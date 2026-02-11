import os
import logging

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_to_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    destination_blob_name: str,
    local_temp_path: str,
) -> str:
    local_file = os.path.join(local_temp_path, os.path.basename(destination_blob_name))
    df.to_csv(local_file, index=False)
    logger.info("Saved transformed data locally: %s", local_file)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file)

    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info("Uploaded to GCS: %s", gcs_uri)
    return gcs_uri


def load_all_to_gcs(
    transformed_data: dict[str, pd.DataFrame],
    bucket_name: str,
    gcs_prefix: str,
    local_temp_path: str,
) -> dict[str, str]:
    os.makedirs(local_temp_path, exist_ok=True)
    gcs_uris = {}

    for table_name, df in transformed_data.items():
        destination_blob = f"{gcs_prefix}/{table_name}.csv"
        gcs_uri = upload_to_gcs(df, bucket_name, destination_blob, local_temp_path)
        gcs_uris[table_name] = gcs_uri

    logger.info("All files uploaded to GCS: %s", list(gcs_uris.keys()))
    return gcs_uris
