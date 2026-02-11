import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dreamline-ai-project")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "dreamline-etl-data")
BQ_DATASET = os.environ.get("BQ_DATASET", "dreamline_analytics")

GCS_RAW_PREFIX = "raw"
GCS_TRANSFORMED_PREFIX = "transformed"

BQ_TABLES = {
    "property_assessments": f"{GCP_PROJECT_ID}.{BQ_DATASET}.property_assessments",
    "incentive_eligibility": f"{GCP_PROJECT_ID}.{BQ_DATASET}.incentive_eligibility",
    "contractor_matching": f"{GCP_PROJECT_ID}.{BQ_DATASET}.contractor_matching",
}

RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "/opt/airflow/data/raw")
TRANSFORMED_DATA_PATH = os.environ.get("TRANSFORMED_DATA_PATH", "/opt/airflow/data/transformed")

GCS_CONN_ID = "google_cloud_default"
BQ_CONN_ID = "google_cloud_default"
