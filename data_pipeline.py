# Importing the libraries
import creds
import requests
from google.cloud import bigquery
import logging
import pandas as pd

# Setup logging to track the pipeline progress
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Importing creds file again
import importlib
import creds
importlib.reload(creds)
