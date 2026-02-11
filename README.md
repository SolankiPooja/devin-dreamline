# Dreamline AI ETL Pipeline

An automated ETL (Extract, Transform, Load) data pipeline for Dreamline AI that processes property assessment, incentive eligibility, and contractor matching data. The pipeline extracts data from CSV files, transforms it, loads to Google Cloud Storage (GCS), and then to BigQuery tables, all orchestrated by Apache Airflow.

## Features

- **Property Assessments**: Process property energy assessments with computed metrics like energy cost per square foot and property age
- **Incentive Eligibility**: Track federal, state, and local incentive programs with categorized amounts
- **Contractor Matching**: Manage contractor-property matches with rating and cost categorizations

## Architecture

```
CSV Files → Extract → Transform → Load to GCS → Load to BigQuery
                ↓
         Apache Airflow (Orchestration)
```

## Project Structure

```
dreamline-etl/
├── dags/
│   └── dreamline_etl_dag.py    # Airflow DAG definition
├── scripts/
│   ├── extract.py              # Data extraction from CSV
│   ├── transform.py            # Data transformations
│   ├── load_gcs.py             # Load to Google Cloud Storage
│   └── load_bigquery.py        # Load to BigQuery
├── config/
│   └── pipeline_config.py      # Configuration settings
├── data/
│   ├── raw/                    # Raw CSV input files
│   └── transformed/            # Transformed output files
├── docker-compose.yaml         # Airflow local setup
├── requirements.txt            # Python dependencies
└── .env.example                # Environment variables template
```

## Data Transformations

### Property Assessments
- Standardize timestamps to UTC
- Calculate `property_age` from year built
- Calculate `energy_cost_per_sqft`
- Categorize energy costs (Low/Medium/High/Very High)

### Incentive Eligibility
- Standardize state codes to uppercase
- Categorize incentive amounts
- Clean and normalize text fields

### Contractor Matching
- Categorize contractor ratings (Below Average/Average/Good/Excellent)
- Categorize project costs
- Normalize match status values

## Setup

### Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account with:
  - GCS bucket created
  - BigQuery dataset created
  - Service account with appropriate permissions

### Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

2. Update `.env` with your GCP settings:
   ```
   GCP_PROJECT_ID=your-project-id
   GCS_BUCKET_NAME=your-bucket-name
   BQ_DATASET=your-dataset-name
   ```

3. Place your GCP service account key at `config/gcp-keyfile.json`

### Running Locally

1. Start Airflow:
   ```bash
   docker-compose up -d
   ```

2. Access Airflow UI at http://localhost:8080
   - Username: `admin`
   - Password: `admin`

3. Enable the `dreamline_etl_pipeline` DAG

4. Trigger a manual run or wait for the daily schedule

## BigQuery Tables

The pipeline creates/updates three tables in BigQuery:

| Table | Description |
|-------|-------------|
| `property_assessments` | Property energy assessment data with computed metrics |
| `incentive_eligibility` | Incentive program eligibility checks |
| `contractor_matching` | Contractor-property matching records |

## DAG Schedule

- **Schedule**: Daily (`@daily`)
- **Start Date**: January 1, 2025
- **Retries**: 2 attempts with 5-minute delay

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | Google Cloud project ID | `dreamline-ai-project` |
| `GCS_BUCKET_NAME` | GCS bucket for data storage | `dreamline-etl-data` |
| `BQ_DATASET` | BigQuery dataset name | `dreamline_analytics` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account key | - |
