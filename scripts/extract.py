import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

RAW_FILES = [
    "property_assessments_raw.csv",
    "incentive_eligibility_raw.csv",
    "contractor_matching_raw.csv",
]


def extract_csv(raw_data_path: str, filename: str) -> pd.DataFrame:
    filepath = os.path.join(raw_data_path, filename)
    logger.info("Extracting data from %s", filepath)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Raw file not found: {filepath}")

    df = pd.read_csv(filepath)
    logger.info("Extracted %d rows from %s", len(df), filename)
    return df


def extract_all(raw_data_path: str) -> dict[str, pd.DataFrame]:
    dataframes = {}
    for filename in RAW_FILES:
        key = filename.replace("_raw.csv", "")
        dataframes[key] = extract_csv(raw_data_path, filename)
    return dataframes
