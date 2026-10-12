
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2025-01.parquet"

EXPECTED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
}


def test_raw_file_exists():
    assert RAW_FILE.exists(), f"Dataset not found: {RAW_FILE}"


def test_raw_file_is_not_empty():
    df = pd.read_parquet(RAW_FILE)
    assert not df.empty, "Dataset contains no records"


def test_expected_columns_exist():
    df = pd.read_parquet(RAW_FILE)
    missing_columns = EXPECTED_COLUMNS - set(df.columns)

    assert not missing_columns, (
        f"Missing expected columns: {sorted(missing_columns)}"
    )


def test_pickup_and_dropoff_are_datetime():
    df = pd.read_parquet(RAW_FILE)

    assert pd.api.types.is_datetime64_any_dtype(
        df["tpep_pickup_datetime"]
    )
    assert pd.api.types.is_datetime64_any_dtype(
        df["tpep_dropoff_datetime"]
    )