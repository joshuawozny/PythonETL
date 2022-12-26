#!/usr/bin/python3

import requests
import pandas as pd
import sys
from pandas import json_normalize
from datetime import datetime
import gc

###Database Connection Details
# define project, dataset, and table_name variables
project, dataset, table_name = "pythonetl-372721", "citibike_data", "data"
table_id = f"{project}.{dataset}.{table_name}"
url = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
q_check_contents = """SELECT max(last_system_update_date) as last_system_update_date FROM pythonetl-372721.citibike_data.data"""
######################################

def request_data(url):
    r = requests.get(url)
    if r.status_code !=200:
        print("Data Source Server Status Issue ")
        go.collect()
        sys.exit()
    else:
        print("Server Status Shows New Update ")
        pass
    return r

def construct_dataframe(r):ls

    stations = r.json()['data']['stations']
    last_updated = r.json()['last_updated']

    dt_object = datetime.fromtimestamp(last_updated).strftime('%Y-%m-%d %H:%M:%S')
    print("Last Updated: " + str(dt_object))

    df = json_normalize(stations)
    df['last_system_update_date'] = dt_object
    df['insertion_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\nData for insertion constructed ")
    return df

def insert_data(df):
    from google.cloud import bigquery


    client = bigquery.Client()
    job_config = bigquery.job.LoadJobConfig()

    # set write_disposition parameter as WRITE_APPEND for appending to table
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
)

def check_contents(r,df,mq_check_contents=q_check_contents, project_id=project):
    d = pd.read_gbq(q_check_contents, project_id=project)

    last_system_update_date = d['last_system_update_date'][0]

    last_updated = datetime.fromtimestamp(r.json()['last_updated']).strftime('%Y-%m-%d %H:%M:%S')

    if last_system_update_date > last_updated:
        print("\nData not inserted due to duplicate records ")
        gc.collect()
        sys.exit()
    else:
        print("\nData does not contain dulicate records ")
        insert_data(df)
    return

r = request_data(url)
df = construct_dataframe(r)
df.rename(columns = {'rental_uris.android':'rental_uris_android'}, inplace = True)
df.rename(columns = {'rental_uris.ios':'rental_uris_ios'}, inplace = True)

check_contents(r,df)
