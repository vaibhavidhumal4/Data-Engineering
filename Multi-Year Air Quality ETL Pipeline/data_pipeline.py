# Importing the libraries
import creds
import requests
from google.cloud import bigquery
import logging
import pandas as pd
import os
from google.cloud.exceptions import NotFound

# Setup logging to track the pipeline progress
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Importing creds file again
import importlib
import creds
importlib.reload(creds)

# Function to extract data using API 
def extract_data(api_key, resource_id):
  """Extract: Fetch data from Gov data API"""
  #This returns 10 records at a time max
  url = f"https://api.data.gov.in/resource/{resource_id}?api-key={api_key}&format=json"
  
  try:
    response = requests.get(url)
    response.raise_for_status() # catch 404/500 errors
    
    # Data is inside the 'records' list
    data = response.json().get('records',[])
    logging.info(f"Extracted {len(data)} records from API")
    return data

  except Exception as e:
    logging.error(f"Data extraction failed: {e}")
    return []

# Function to make the necessary transformations to the data
def transform_data(raw_records):
  """Transform: Clean the columns"""
  if not raw_records:
    return pd.DataFrame()

  df = pd.DataFrame(raw_records)

  # Mapping
  rename_map = {
      'state': 'state',
        'city_name': 'city',
        '_2022___aqi___200___good_days': 'good_days_2022',
        '_2022___aqi__200___bad_days': 'bad_days_2022',
        '_2023___aqi___200___good_days': 'good_days_2023',
        '_2023___aqi__200___bad_days': 'bad_days_2023',
        '_2024___aqi___200___good_days': 'good_days_2024',
        '_2024___aqi__200___bad_days': 'bad_days_2024'
  }
  df.rename(columns=rename_map, inplace = True)

  # Remove the serial no. column
  if '_sl__no_' in df.columns:
       df.drop(columns=['_sl__no_'], inplace=True)

  # Convert numeric columns to float/int
  numeric_cols= [c for c in df.columns if 'days' in c]
  df[numeric_cols]= df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

  # Engineering new metrics
  df['total_recorded_days'] = df['good_days_2024'] + df['bad_days_2024']
  df['pollution_ratio_2024'] = (df['bad_days_2024'] / df['total_recorded_days']).fillna(0)

  logging.info("Transformation complete.")
  return df

# Load the transformed data to BigQuery table
def load_to_bigquery(df, project, dataset, table_name, credentials_path):
    """
    Loads data using an explicit Service Account JSON file.
    """
    # Initialize client with explicit service account credentials
    client = bigquery.Client.from_service_account_json(
        credentials_path, 
        project=project
    )
    
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table_name)
    full_table_id = f"{project}.{dataset}.{table_name}"

    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_name} exists.")
    except NotFound:
        logging.info(f"Table {table_name} not found. Creating...")
        # Schema definition (Keep this as we had it before)
        schema = [
            bigquery.SchemaField("state_name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("good_days_2022", "INTEGER"),
            bigquery.SchemaField("bad_days_2022", "INTEGER"),
            bigquery.SchemaField("good_days_2023", "INTEGER"),
            bigquery.SchemaField("bad_days_2023", "INTEGER"),
            bigquery.SchemaField("good_days_2024", "INTEGER"),
            bigquery.SchemaField("bad_days_2024", "INTEGER"),
            bigquery.SchemaField("total_recorded_days", "INTEGER"),
            bigquery.SchemaField("pollution_ratio_2024", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("Load Success!")
    except Exception as e:
        logging.error(f"BigQuery Load Failed: {e}")

# Main function 
if __name__ == "__main__":
    try:
        # 1. Extraction
        logging.info("Starting data extraction...")
        data = extract_data(creds.API_KEY, creds.RESOURCE_ID)
        
        if not data:
            logging.error("No data extracted. Exiting.")
        else:
            print("Sample Row from API:", data[0])

            # 2. Transformation
            cleaned_df = transform_data(data)
            print("\nCleaned DataFrame Preview:")
            print(cleaned_df.head())

            # 3. Load (Checks existence -> Creates if missing -> Loads)
            PATH_TO_JSON = "main-486618-b2e03a405716.json" 

            # Pass this to the function we updated in the previous step
            load_to_bigquery(
                cleaned_df, 
                creds.PROJECT_ID, 
                creds.DATASET_ID, 
                creds.TABLE_ID,
                credentials_path=PATH_TO_JSON
            )
            
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
