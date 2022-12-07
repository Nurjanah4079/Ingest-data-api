import os
import logging
import requests
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import AirflowException

from google.cloud import storage
from google.oauth2 import service_account
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq
import pandas as pd

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'program-data-fellowship-8')
BUCKET = os.environ.get('GCP_GCS_BUCKET', 'datafellowship-8')
GOOGLE_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/home/jee/df8/data_ingestion/airflow/.google/credentials/google_credentials.json')


api_file = "us_sensus.json"
api_url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

file_csv_save = api_file.replace('.json', '.csv')
parquet_file = file_csv_save.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'data_usa')

def call_api_data(url_json: str, local_json: str):
    """
    Call for the api to get json file
    * url_json: retrieve data as json from url
    * local_json: local path to store the file
    """
    r = requests.get(url = url_json)

    data = r.json()
    with open(local_json, 'w') as outfile:
        json.dump(data, outfile)

def format_to_csv(json_file: str):
    
    if not json_file.endswith('.json'):
        logging.error('Must type file JSON')
        return

    dt = jsw.read_json(json_file)
    dt_num = dt['data'].to_numpy()

    dt_list = list(i for i in dt_num[0])
    df = pd.DataFrame.from_dict(dt_list)
    dt_df = pa.Table.from_pandas(df)

    pv.write_csv(dt_df, f"{path_to_local_home}/{file_csv_save}")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Must type file CSV")
        return
    dt = pv.read_csv(src_file)
    pq.write_table(dt, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_api_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    call_dataset_task = PythonOperator(
        task_id="call_dataset_task",
        python_callable=call_api_data,
        op_kwargs={ 
            "url_json": api_url,
            "local_json": f"{path_to_local_home}/{api_file}"
        },
    )


    save_to_csv = PythonOperator(
        task_id="save_to_csv",
        python_callable=format_to_csv,
        op_kwargs={ 
            "json_file": f"{path_to_local_home}/{api_file}",
        }
    )


    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{file_csv_save}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data_raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/data_raw/{parquet_file}"],
            },
        },
    )

    call_dataset_task >> save_to_csv >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
