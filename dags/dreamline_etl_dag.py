import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.pipeline_config import (  # noqa: E402
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_TRANSFORMED_PREFIX,
    BQ_TABLES,
    RAW_DATA_PATH,
    TRANSFORMED_DATA_PATH,
)
from scripts.extract import extract_all  # noqa: E402
from scripts.transform import transform_all  # noqa: E402
from scripts.load_gcs import load_all_to_gcs  # noqa: E402
from scripts.load_bigquery import load_all_to_bigquery  # noqa: E402

default_args = {
    "owner": "dreamline-ai",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**kwargs):
    dataframes = extract_all(RAW_DATA_PATH)
    result = {}
    for key, df in dataframes.items():
        filepath = os.path.join(RAW_DATA_PATH, f"{key}_extracted.csv")
        df.to_csv(filepath, index=False)
        result[key] = filepath
    kwargs["ti"].xcom_push(key="extracted_paths", value=result)


def _transform(**kwargs):
    ti = kwargs["ti"]
    extracted_paths = ti.xcom_pull(task_ids="extract", key="extracted_paths")

    import pandas as pd  # noqa: E402

    dataframes = {}
    for key, filepath in extracted_paths.items():
        dataframes[key] = pd.read_csv(filepath)

    transformed = transform_all(dataframes)

    os.makedirs(TRANSFORMED_DATA_PATH, exist_ok=True)
    result = {}
    for key, df in transformed.items():
        filepath = os.path.join(TRANSFORMED_DATA_PATH, f"{key}.csv")
        df.to_csv(filepath, index=False)
        result[key] = filepath
    ti.xcom_push(key="transformed_paths", value=result)


def _load_to_gcs(**kwargs):
    ti = kwargs["ti"]
    transformed_paths = ti.xcom_pull(
        task_ids="transform", key="transformed_paths"
    )

    import pandas as pd  # noqa: E402

    transformed_data = {}
    for key, filepath in transformed_paths.items():
        transformed_data[key] = pd.read_csv(filepath)

    gcs_uris = load_all_to_gcs(
        transformed_data,
        GCS_BUCKET_NAME,
        GCS_TRANSFORMED_PREFIX,
        TRANSFORMED_DATA_PATH,
    )
    ti.xcom_push(key="gcs_uris", value=gcs_uris)


def _load_to_bigquery(**kwargs):
    ti = kwargs["ti"]
    gcs_uris = ti.xcom_pull(task_ids="load_to_gcs", key="gcs_uris")

    load_all_to_bigquery(gcs_uris, BQ_TABLES, GCP_PROJECT_ID)


with DAG(
    dag_id="dreamline_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Dreamline AI - extracts CSV data, transforms, loads to GCS and BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dreamline", "etl", "bigquery", "gcs"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=_transform,
    )

    load_gcs_task = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_load_to_gcs,
    )

    load_bq_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=_load_to_bigquery,
    )

    extract_task >> transform_task >> load_gcs_task >> load_bq_task
