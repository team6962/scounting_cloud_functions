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
def fetch_and_store_matches(cloud_event: CloudEvent):
    data = base64.b64decode(cloud_event.data["message"]["data"])
    data_json = json.loads(data)

    if 'event_key' not in data_json:
        logging.error("Missing 'event_key' in the message")
        return 'Missing event_key', 400

    event_key = data_json['event_key']
    logging.info(f"Received event_key: {event_key}")

    dataset_id = os.environ.get('DATASET_ID')
    api_key = os.environ.get('API_KEY')

    # Fetch matches from the API
    url = f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches"
    headers = {"X-TBA-Auth-Key": api_key}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from API: {response.text}")
        return 'Failed to fetch data from API', 500

    matches = response.json()

    # Create DataFrame from original data for {event_key}_matches table
    df_original = pd.json_normalize(matches)
    df_original.columns = df_original.columns.str.replace('.', '_', regex=False)
    df_original = df_original.astype(str)
    load_data_to_bigquery(df_original, dataset_id, f"{event_key}_matches", "Original data loaded")

    # Process matches to create a flattened DataFrame
    rows = []
    for match in matches:
        winning_alliance = match['winning_alliance']
        for alliance_color in ['blue', 'red']:
            team_keys = match['alliances'][alliance_color]['team_keys']
            for index, team_key in enumerate(team_keys, start=1):
                alliance_team_key = f"{alliance_color}{index}"  # e.g., red1, blue2
                alliance_is_winner = True if alliance_color == winning_alliance else False
                robot_number = str(index)  # To match with Robot1, Robot2, Robot3

                # Prepare a dictionary to hold score breakdown data with "alliance_" prefix
                score_breakdown_prefixed = {}
                for key, value in match['score_breakdown'][alliance_color].items():
                    if key.endswith(('Robot1', 'Robot2', 'Robot3')):
                        if key.endswith(robot_number):  # Only keep the relevant robot data
                            new_key = key[:-1]  # Remove the number to consolidate
                            score_breakdown_prefixed[new_key] = value
                    else:
                        new_key = f"alliance_{key}"
                        score_breakdown_prefixed[new_key] = value

                row = {
                    'team_key': team_key,
                    'alliance': alliance_color,
                    'alliance_team_key': alliance_team_key,
                    'alliance_is_winner': alliance_is_winner,
                    'match_key': match['key'],
                    'event_key': match['event_key'],
                    'comp_level': match['comp_level'],
                    'set_number': match['set_number'],
                    'match_number': match['match_number'],
                    'alliance_score': match['alliances'][alliance_color]['score'],
                    **score_breakdown_prefixed  # Unpack alliance-specific score breakdown with "alliance_" prefix
                }
                rows.append(row)

    df_flattened = pd.DataFrame(rows)
    df_flattened.columns = df_flattened.columns.str.replace('.', '_', regex=False)
    df_flattened = df_flattened.astype(str)
    load_data_to_bigquery(df_flattened, dataset_id, f"{event_key}_matches_flattened", "Flattened data loaded")

    return jsonify({"message": "Successfully loaded data into BigQuery"}), 200


def load_data_to_bigquery(df, dataset_id, table_name, success_message):
    table_id = f"{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    try:
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"{success_message} to table {table_id}")
    except Exception as e:
        logging.error(f"Failed to load data to BigQuery: {e}")
        raise e