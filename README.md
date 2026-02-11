# Dreamline AI ETL Pipeline

An automated ETL (Extract, Transform, Load) data pipeline for Dreamline AI that processes property assessment, incentive eligibility, and contractor matching data. The pipeline extracts data from CSV files, transforms it, loads to Google Cloud Storage (GCS), and then to BigQuery tables, all orchestrated by Apache Airflow.

## Features

- **Property Assessments**: Process property energy assessments with computed metrics like energy cost per square foot and property age
- **Incentive Eligibility**: Track federal, state, and local incentive programs with categorized amounts
- **Contractor Matching**: Manage contractor-property matches with rating and cost categorizations

## Project Architecture

The pipeline follows a modular layered architecture designed for scalability and maintainability:

```
+-----------------------------------------------------------+
|                    Apache Airflow                          |
|               (Orchestration Layer)                       |
|                                                           |
|   +-------------+    +--------------+    +--------------+ |
|   | DAG         |    | Scheduler    |    | Web UI       | |
|   | Definition  |    | (Daily @00:00|    | (Port 8080)  | |
|   |             |    |  UTC)        |    |              | |
|   +------+------+    +------+-------+    +--------------+ |
|          |                  |                              |
+-----------------------------------------------------------+
           |                  |
           v                  v
+-----------------------------------------------------------+
|                     ETL Layer                              |
|                                                           |
|  +----------+  +-----------+  +--------+  +------------+  |
|  | Extract  |  | Transform |  |Load GCS|  |Load BigQuery| |
|  | (CSV)    |->| (Pandas)  |->|(GCS API)|->|(BQ API)    | |
|  +----------+  +-----------+  +--------+  +------------+  |
|                                                           |
+-----------------------------------------------------------+
           |                                    |
           v                                    v
+------------------+              +----------------------------+
| Data Sources     |              | Google Cloud Platform       |
|                  |              |                             |
| - property_      |              | +----------+ +-----------+ |
|   assessments    |              | | GCS      | | BigQuery  | |
| - incentive_     |              | | Bucket   | | Dataset   | |
|   eligibility    |              | | (staging)| | (analytics| |
| - contractor_    |              | +----------+ |  tables)  | |
|   matching       |              |              +-----------+ |
+------------------+              +----------------------------+
```

### Component Breakdown

| Layer | Component | Technology | Purpose |
|-------|-----------|------------|---------|
| Orchestration | Airflow DAG | Apache Airflow 2.9 | Schedules and monitors pipeline tasks |
| Orchestration | Scheduler | Airflow Scheduler | Triggers daily runs at midnight UTC |
| Orchestration | Web UI | Airflow Webserver | DAG monitoring, manual triggers, log viewing |
| ETL | Extract | Pandas | Reads raw CSV files from local storage |
| ETL | Transform | Pandas | Cleans, enriches, and categorizes data |
| ETL | Load GCS | google-cloud-storage | Uploads transformed CSVs to GCS bucket |
| ETL | Load BigQuery | google-cloud-bigquery | Loads data from GCS into BigQuery tables |
| Infrastructure | Database | PostgreSQL 15 | Airflow metadata storage |
| Infrastructure | Containers | Docker Compose | Local development environment |

## ETL Workflow

The pipeline executes four sequential tasks orchestrated by Airflow. Each task passes data to the next via XCom (file paths) and local CSV staging.

```
          [Airflow Scheduler triggers DAG daily]
                         |
                         v
+-------------------------------------------------------+
| Task 1: EXTRACT                                       |
|                                                       |
|   data/raw/                                           |
|   +-- property_assessments_raw.csv  (100 rows)        |
|   +-- incentive_eligibility_raw.csv (100 rows)  ---+  |
|   +-- contractor_matching_raw.csv   (100 rows)     |  |
|                                                     |  |
|   scripts/extract.py                                |  |
|   - Validates file existence                        |  |
|   - Reads CSV into DataFrames                       |  |
|   - Saves extracted copies                          |  |
|   - Pushes file paths to XCom                       |  |
+----------------------------+------------------------+  |
                             |                           |
                             v                           |
+-------------------------------------------------------+
| Task 2: TRANSFORM                                     |
|                                                       |
|   scripts/transform.py                                |
|                                                       |
|   Property Assessments:                               |
|   - Timestamps -> UTC                                 |
|   - Calculate property_age = current_year - year_built|
|   - Calculate energy_cost_per_sqft                    |
|   - Categorize energy costs:                          |
|     Low (<$150) | Medium ($150-250) |                 |
|     High ($250-400) | Very High (>$400)               |
|                                                       |
|   Incentive Eligibility:                              |
|   - Standardize state codes (uppercase)               |
|   - Normalize incentive/retrofit types                |
|   - Categorize incentive amounts:                     |
|     Low (<$3k) | Medium ($3k-7k) |                    |
|     High ($7k-12k) | Very High (>$12k)                |
|                                                       |
|   Contractor Matching:                                |
|   - Normalize match status values                     |
|   - Categorize contractor ratings:                    |
|     Below Avg (<3.5) | Average (3.5-4.0) |            |
|     Good (4.0-4.5) | Excellent (4.5-5.0)              |
|   - Categorize project costs:                         |
|     Low (<$10k) | Medium ($10k-20k) |                 |
|     High ($20k-30k) | Very High (>$30k)               |
|                                                       |
|   All tables:                                         |
|   - Add load_timestamp (UTC)                          |
|   - Add source_file metadata                          |
|   - Drop rows with null primary keys                  |
|   - Save to data/transformed/                         |
+----------------------------+--------------------------+
                             |
                             v
+-------------------------------------------------------+
| Task 3: LOAD TO GCS                                  |
|                                                       |
|   scripts/load_gcs.py                                 |
|   - Reads transformed CSVs                            |
|   - Uploads to GCS bucket:                            |
|     gs://{bucket}/transformed/property_assessments.csv|
|     gs://{bucket}/transformed/incentive_eligibility.csv|
|     gs://{bucket}/transformed/contractor_matching.csv |
|   - Pushes GCS URIs to XCom                          |
+----------------------------+--------------------------+
                             |
                             v
+-------------------------------------------------------+
| Task 4: LOAD TO BIGQUERY                             |
|                                                       |
|   scripts/load_bigquery.py                            |
|   - Loads from GCS URIs into BigQuery tables:         |
|     {project}.{dataset}.property_assessments          |
|     {project}.{dataset}.incentive_eligibility         |
|     {project}.{dataset}.contractor_matching           |
|   - Uses explicit schemas with typed columns          |
|   - WRITE_TRUNCATE mode (full table refresh)          |
|   - Skips CSV header row                              |
+-------------------------------------------------------+
                             |
                             v
                    [Pipeline Complete]
```

### Data Flow Summary

```
Raw CSVs (3 files, 300 total rows)
    |
    | extract.py - read & validate
    v
Extracted DataFrames (in-memory)
    |
    | transform.py - clean, enrich, categorize
    v
Transformed CSVs (local staging)
    |
    | load_gcs.py - upload via GCS API
    v
GCS Bucket (gs://dreamline-etl-data/transformed/)
    |
    | load_bigquery.py - load via BigQuery API
    v
BigQuery Tables (dreamline_analytics dataset)
    +-- property_assessments  (15 columns)
    +-- incentive_eligibility (12 columns)
    +-- contractor_matching   (13 columns)
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
