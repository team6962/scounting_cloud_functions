import os
import logging
import requests
import pandas as pd
from flask import jsonify
from google.cloud import bigquery
import functions_framework
import base64
import json
from cloudevents.http import CloudEvent

# Initialize BigQuery client
client = bigquery.Client()

@functions_framework.cloud_event
def fetch_and_store_teams(cloud_event: CloudEvent):
    data = base64.b64decode(cloud_event.data["message"]["data"])
    data_json = json.loads(data)
    
    if 'event_key' not in data_json:
        logging.error("Missing 'event_key' in the message")
        return 'Missing event_key', 400

    event_key = data_json['event_key']
    logging.info(f"Received event_key: {event_key}")

    dataset_id = os.environ.get('DATASET_ID')
    api_key = os.environ.get('API_KEY')

    # Fetch teams from the API
    url = f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams"
    headers = {"X-TBA-Auth-Key": api_key}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from API: {response.text}")
        return 'Failed to fetch data from API', 500

    teams = response.json()

    # Convert the JSON data to a Pandas DataFrame
    df = pd.json_normalize(teams)

    # Drop columns where all values are NULL
    df = df.dropna(axis=1, how='all')

    table_id = f"{dataset_id}.{event_key}_teams"
    
    # Define the job configuration for loading the DataFrame into BigQuery
    job_config = bigquery.LoadJobConfig(
        # Autodetect the schema of the DataFrame
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # Overwrites the table if it already exists
    )

    # Attempt to load the DataFrame into BigQuery
    try:
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Data loaded to table {table_id}")
    except Exception as e:
        logging.error(f"Failed to load data to BigQuery: {e}")
        return jsonify({"error": "Failed to load data into BigQuery", "details": str(e)}), 500

    return jsonify({"message": "Successfully loaded data into BigQuery"}), 200
