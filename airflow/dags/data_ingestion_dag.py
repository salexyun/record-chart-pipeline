import os

import pandas as pd
import sqlite3 as sq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.dates import days_ago

from google.cloud import storage


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
STORAGE_BUCKET = os.environ.get("GCS_BUCKET_NAME")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "billboard_200")
URL_TEMPLATE = (
    "https://www.dropbox.com/s/v5in83km14tkhxa/billboard-200-with-segments.db?dl=1"
)
DB_FILE_TEMPLATE = AIRFLOW_HOME + "/billboard-200-with-segments.db"


def format_to_parquet(database):
    """Convert SQLite database file into parquet file.

    Args:
        database: .db file
    """
    con = sq.connect(database)
    cur = con.cursor()

    for table in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall():
        t = table[0]
        df = pd.read_sql("SELECT * FROM " + t, con)
        df.to_parquet(t + ".parquet", index=False)


def upload_to_gcs(bucket, object_name, local_file):
    """Upload local file into the cloud.

    Args:
        bucket: name of the Google cloud storage bucket
        object_name: name and path to the cloud
        local_file: path to the local file
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(
        local_file, timeout=300
    )  # increase timeout depending on your internet upload speed


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["de-billboard-200"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {DB_FILE_TEMPLATE}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={"database": DB_FILE_TEMPLATE},
    )

    local_to_gcs_task0 = PythonOperator(
        task_id="local_to_gcs_task0",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": STORAGE_BUCKET,
            "object_name": "raw/acoustic_features.parquet",
            "local_file": AIRFLOW_HOME + "/acoustic_features.parquet",
        },
    )

    local_to_gcs_task1 = PythonOperator(
        task_id="local_to_gcs_task1",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": STORAGE_BUCKET,
            "object_name": "raw/albums.parquet",
            "local_file": AIRFLOW_HOME + "/albums.parquet",
        },
    )

    local_to_gcs_task2 = PythonOperator(
        task_id="local_to_gcs_task2",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": STORAGE_BUCKET,
            "object_name": "raw/segments.parquet",
            "local_file": AIRFLOW_HOME + "/segments.parquet",
        },
    )

    bq_external_table_task0 = BigQueryCreateExternalTableOperator(
        task_id="bq_external_table_task0",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "acoustic_features_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": f"gs://{STORAGE_BUCKET}/raw/acoustic_features.parquet",
            },
        },
    )

    bq_external_table_task1 = BigQueryCreateExternalTableOperator(
        task_id="bq_external_table_task1",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "albums_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": f"gs://{STORAGE_BUCKET}/raw/albums.parquet",
            },
        },
    )

    bq_external_table_task2 = BigQueryCreateExternalTableOperator(
        task_id="bq_external_table_task2",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "segments_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": f"gs://{STORAGE_BUCKET}/raw/segments.parquet",
            },
        },
    )

    QUERY0 = f"CREATE OR REPLACE TABLE \
                {BIGQUERY_DATASET}.acoustic_features \
            PARTITION BY \
                DATE_TRUNC(date_parsed, YEAR) \
            CLUSTER BY \
                album \
            AS SELECT \
                *, COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', date), SAFE.PARSE_DATE('%Y', date)) as date_parsed \
            FROM {BIGQUERY_DATASET}.acoustic_features_external_table;"

    bq_partitioned_table_task0 = BigQueryInsertJobOperator(
        task_id="bq_partitioned_table_task0",
        configuration={
            "query": {
                "query": QUERY0,
                "useLegacySql": False,
            }
        },
    )

    QUERY1 = f"CREATE OR REPLACE TABLE \
                {BIGQUERY_DATASET}.albums \
            PARTITION BY \
                DATE_TRUNC(date_parsed, YEAR) \
            AS SELECT *, PARSE_DATE('%F', date) AS date_parsed \
            FROM {BIGQUERY_DATASET}.albums_external_table;"

    bq_partitioned_table_task1 = BigQueryInsertJobOperator(
        task_id="bq_partitioned_table_task1",
        configuration={
            "query": {
                "query": QUERY1,
                "useLegacySql": False,
            }
        },
    )

    QUERY2 = f"CREATE OR REPLACE TABLE \
                {BIGQUERY_DATASET}.segments \
            CLUSTER BY \
                album \
            AS SELECT * FROM {BIGQUERY_DATASET}.segments_external_table;"

    bq_partitioned_table_task2 = BigQueryInsertJobOperator(
        task_id="bq_partitioned_table_task2",
        configuration={
            "query": {
                "query": QUERY2,
                "useLegacySql": False,
            }
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> [local_to_gcs_task0, local_to_gcs_task1, local_to_gcs_task2]
    )

    local_to_gcs_task0 >> bq_external_table_task0 >> bq_partitioned_table_task0
    local_to_gcs_task1 >> bq_external_table_task1 >> bq_partitioned_table_task1
    local_to_gcs_task2 >> bq_external_table_task2 >> bq_partitioned_table_task2
