import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)

BQ_SCHEMAS = {
    "property_assessments": [
        bigquery.SchemaField("assessment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("property_type", "STRING"),
        bigquery.SchemaField("square_feet", "FLOAT64"),
        bigquery.SchemaField("year_built", "INT64"),
        bigquery.SchemaField("heating_type", "STRING"),
        bigquery.SchemaField("cooling_type", "STRING"),
        bigquery.SchemaField("avg_monthly_energy_cost", "FLOAT64"),
        bigquery.SchemaField("zip_code", "STRING"),
        bigquery.SchemaField("assessment_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("property_age", "INT64"),
        bigquery.SchemaField("energy_cost_per_sqft", "FLOAT64"),
        bigquery.SchemaField("energy_cost_category", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ],
    "incentive_eligibility": [
        bigquery.SchemaField("eligibility_check_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("property_id", "STRING"),
        bigquery.SchemaField("incentive_program_id", "STRING"),
        bigquery.SchemaField("incentive_type", "STRING"),
        bigquery.SchemaField("estimated_incentive_amount", "FLOAT64"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("retrofit_type", "STRING"),
        bigquery.SchemaField("check_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("incentive_amount_category", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ],
    "contractor_matching": [
        bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("property_id", "STRING"),
        bigquery.SchemaField("contractor_id", "STRING"),
        bigquery.SchemaField("contractor_rating", "FLOAT64"),
        bigquery.SchemaField("retrofit_type", "STRING"),
        bigquery.SchemaField("estimated_project_cost", "FLOAT64"),
        bigquery.SchemaField("match_status", "STRING"),
        bigquery.SchemaField("request_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("rating_category", "STRING"),
        bigquery.SchemaField("project_cost_category", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ],
}


def load_gcs_to_bigquery(
    gcs_uri: str,
    table_id: str,
    table_name: str,
    project_id: str,
) -> None:
    client = bigquery.Client(project=project_id)

    schema = BQ_SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"No schema defined for table: {table_name}")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )

    logger.info("Loading %s into BigQuery table %s", gcs_uri, table_id)
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    destination_table = client.get_table(table_id)
    logger.info("Loaded %d rows into %s", destination_table.num_rows, table_id)


def load_all_to_bigquery(
    gcs_uris: dict[str, str],
    bq_tables: dict[str, str],
    project_id: str,
) -> None:
    for table_name, gcs_uri in gcs_uris.items():
        table_id = bq_tables.get(table_name)
        if not table_id:
            logger.warning("No BigQuery table mapping for %s, skipping", table_name)
            continue
        load_gcs_to_bigquery(gcs_uri, table_id, table_name, project_id)

    logger.info("All tables loaded to BigQuery successfully")
