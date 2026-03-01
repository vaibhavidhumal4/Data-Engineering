# Importing the required libraries
import creds
from google.cloud import bigquery                 # To interact with BQ data warehouse
import logging                                    # Logs errors and events
import pandas as pd                               
import asyncio                                    # Manages asynchronous python execution
# Using httpx + asyncio, we fetch paginated API data concurrently with asyncio.gather(), making extraction much faster than sequential requests.
import httpx                                      # Async HTTP client for APIs
import pytz                                       # For time conversion and handling
from datetime import datetime                     # Date and time manipulation

# Importing creds file again to reload in Google colab
import importlib
import creds
importlib.reload(creds)

# Setup logging to track the pipeline progress
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Function to get IST timestamp
def get_ist_time():
    """Returns the current IST time as a naive object for BigQuery display."""
    ist_tz = pytz.timezone('Asia/Kolkata')
    # We get the time in IST, then remove the timezone info so BQ treats the 'hours' as literal/wall-clock time.
    return datetime.now(ist_tz).replace(tzinfo=None)

# 1. DATA EXTRACTION: Function to extract data using API using asynchronous approach
async def fetch_page(client, api_key, resource_id, offset):
    """Fetch a single page of data."""
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {"api-key": api_key, "format": "json", "offset": offset}
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

def load_to_bigquery_with_staging(df, project, dataset, table_name, credentials_path):
    client = bigquery.Client.from_service_account_json(credentials_path, project=project)
    main_table_id = f"{project}.{dataset}.{table_name}"
    staging_table_id = f"{main_table_id}_staging"

    # Check if main table exists
    try:
        client.get_table(main_table_id)
        logging.info(f"Main table '{main_table_id}' already exists.")
    except Exception:
        logging.info(f"Main table '{main_table_id}' does not exist. Creating it...")
        client.load_table_from_dataframe(df, main_table_id).result()
        logging.info("Main table created successfully.")

    # Load to Staging (fresh load every time)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, staging_table_id, job_config=job_config).result()
    logging.info(f"Staging table '{staging_table_id}' populated.")

    # Insert new records into Main 
    merge_query = f"""
    INSERT INTO `{main_table_id}`
    SELECT s.* FROM `{staging_table_id}` AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM `{main_table_id}` AS m
        WHERE m.state = s.state AND m.city = s.city
    )
    """

    try:
        client.query(merge_query).result()
        logging.info("Main table updated successfully.")
    except Exception as e:
        logging.error(f"Insert failed. Ensure schema matches. Error: {e}")

    # Cleanup staging table
    client.delete_table(staging_table_id, not_found_ok=True)
    logging.info("Staging table deleted.")
      

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
        load_to_bigquery_with_staging(
            cleaned_df, 
            creds.PROJECT_ID, 
            creds.DATASET_ID, 
            creds.TABLE_ID, 
            PATH_TO_JSON
        )
        print("Data loading done")

await main()
