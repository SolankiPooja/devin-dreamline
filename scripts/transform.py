import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def _add_ingestion_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["source_file"] = source_file
    return df


def transform_property_assessments(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming property_assessments: %d rows", len(df))

    df["assessment_timestamp"] = pd.to_datetime(df["assessment_timestamp"], utc=True)
    df["property_type"] = df["property_type"].str.strip().str.title()
    df["heating_type"] = df["heating_type"].str.strip().str.title()
    df["cooling_type"] = df["cooling_type"].str.strip().str.title()
    df["square_feet"] = pd.to_numeric(df["square_feet"], errors="coerce")
    df["avg_monthly_energy_cost"] = pd.to_numeric(
        df["avg_monthly_energy_cost"], errors="coerce"
    )
    df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce").astype(
        "Int64"
    )

    current_year = datetime.now(timezone.utc).year
    df["property_age"] = current_year - df["year_built"]

    df["energy_cost_per_sqft"] = (
        df["avg_monthly_energy_cost"] / df["square_feet"]
    ).round(4)

    df["energy_cost_category"] = pd.cut(
        df["avg_monthly_energy_cost"],
        bins=[0, 150, 250, 400, float("inf")],
        labels=["Low", "Medium", "High", "Very High"],
    )

    df = df.dropna(subset=["assessment_id", "user_id"])
    df = _add_ingestion_metadata(df, "property_assessments_raw.csv")

    logger.info("Transformed property_assessments: %d rows", len(df))
    return df


def transform_incentive_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming incentive_eligibility: %d rows", len(df))

    df["check_timestamp"] = pd.to_datetime(df["check_timestamp"], utc=True)
    df["incentive_type"] = df["incentive_type"].str.strip().str.title()
    df["retrofit_type"] = df["retrofit_type"].str.strip().str.title()
    df["state"] = df["state"].str.strip().str.upper()
    df["estimated_incentive_amount"] = pd.to_numeric(
        df["estimated_incentive_amount"], errors="coerce"
    )

    df["incentive_amount_category"] = pd.cut(
        df["estimated_incentive_amount"],
        bins=[0, 3000, 7000, 12000, float("inf")],
        labels=["Low", "Medium", "High", "Very High"],
    )

    df = df.dropna(subset=["eligibility_check_id", "user_id"])
    df = _add_ingestion_metadata(df, "incentive_eligibility_raw.csv")

    logger.info("Transformed incentive_eligibility: %d rows", len(df))
    return df


def transform_contractor_matching(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming contractor_matching: %d rows", len(df))

    df["request_timestamp"] = pd.to_datetime(df["request_timestamp"], utc=True)
    df["retrofit_type"] = df["retrofit_type"].str.strip().str.title()
    df["match_status"] = df["match_status"].str.strip().str.title()
    df["contractor_rating"] = pd.to_numeric(
        df["contractor_rating"], errors="coerce"
    )
    df["estimated_project_cost"] = pd.to_numeric(
        df["estimated_project_cost"], errors="coerce"
    )

    df["rating_category"] = pd.cut(
        df["contractor_rating"],
        bins=[0, 3.5, 4.0, 4.5, 5.0],
        labels=["Below Average", "Average", "Good", "Excellent"],
    )

    df["project_cost_category"] = pd.cut(
        df["estimated_project_cost"],
        bins=[0, 10000, 20000, 30000, float("inf")],
        labels=["Low", "Medium", "High", "Very High"],
    )

    df = df.dropna(subset=["match_id", "user_id"])
    df = _add_ingestion_metadata(df, "contractor_matching_raw.csv")

    logger.info("Transformed contractor_matching: %d rows", len(df))
    return df


def transform_all(dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    transformers = {
        "property_assessments": transform_property_assessments,
        "incentive_eligibility": transform_incentive_eligibility,
        "contractor_matching": transform_contractor_matching,
    }
    transformed = {}
    for key, df in dataframes.items():
        if key in transformers:
            transformed[key] = transformers[key](df.copy())
        else:
            logger.warning("No transformer found for %s, skipping", key)
    return transformed
