# Importing the libraries
import creds
import requests
from google.cloud import bigquery
import logging
import pandas as pd
import os
from google.cloud.exceptions import NotFound
import asyncio
import httpx
import nest_asyncio

# Setup logging to track the pipeline progress
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Importing creds file again to reload in Google colab
import importlib
import creds
importlib.reload(creds)

# 1. DATA EXTRACTION: Function to extract data using API using asynchronous approach
async def fetch_page(client, api_key, resource_id, offset, limit=10):
    """Fetch a single page of data."""
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {"api-key": api_key, "format": "json", "offset": offset, "limit": limit}
    try:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error at offset {offset}: {e}")
        return None

async def extract_all_data_async(api_key, resource_id):
    """Extracts EVERY record by calculating total pages first."""
    all_records = []
    limit = 10
    
    async with httpx.AsyncClient() as client:
        # Step 1: Get the first page to see the 'total' records available
        first_resp = await fetch_page(client, api_key, resource_id, offset=0, limit=limit)
        if not first_resp: return []
        
        total_records = int(first_resp.get('total', 0))
        all_records.extend(first_resp.get('records', []))
        logging.info(f"Total records to fetch: {total_records}")

        # Step 2: Create tasks for the remaining records
        tasks = []
        for offset in range(limit, total_records, limit):
            tasks.append(fetch_page(client, api_key, resource_id, offset, limit))
        
        # Step 3: Run all requests concurrently
        responses = await asyncio.gather(*tasks)
        for r in responses:
            if r: all_records.extend(r.get('records', []))
            
    logging.info(f"Successfully extracted {len(all_records)} records.")
    return all_records


# 2. DATA TRANSFORMATION: Function to make the necessary transformations to the data
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

  # Adding IST timestamp
  df['timestamp_IST'] = get_ist_time()

  logging.info("Transformation complete.")
  return df


# 3. DATA LOADING: Load the transformed data to BigQuery table

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
        # Schema definition
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
            bigquery.SchemaField("timestamp_IST", "DATETIME")
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
      

# 4. MAIN FUNCTION 
if __name__ == "__main__":
  async def main():
    # Replace these with your actual path.
    PATH_TO_JSON = "******" 
    
    raw_data = await extract_all_data_async(creds.API_KEY, creds.RESOURCE_ID)
    print("Data extraction done")
    if raw_data:
        cleaned_df = transform_data(raw_data) 
        print("Data transformation done")
        load_to_bigquery(
            cleaned_df, 
            creds.PROJECT_ID, 
            creds.DATASET_ID, 
            creds.TABLE_ID, 
            PATH_TO_JSON
        )
        print("Data loading done")

await main()
