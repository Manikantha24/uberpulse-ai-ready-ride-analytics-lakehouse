
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

VALID_FILE = (
    PROCESSED_DIR / "yellow_tripdata_2025-01_valid.parquet"
)

QUARANTINE_FILE = (
    PROCESSED_DIR / "yellow_tripdata_2025-01_quarantine.parquet"
)

RAW_FILE = (
    PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2025-01.parquet"
)

WARNING_COLUMNS = [
    "warning_zero_distance",
    "warning_negative_fare",
    "warning_negative_total",
    "warning_high_fare",
    "warning_long_distance",
    "warning_long_duration",
]


def test_output_files_exist():
    assert VALID_FILE.exists()
    assert QUARANTINE_FILE.exists()


def test_all_records_are_accounted_for():
    raw_count = len(pd.read_parquet(RAW_FILE))
    valid_count = len(pd.read_parquet(VALID_FILE))
    quarantine_count = len(pd.read_parquet(QUARANTINE_FILE))

    assert valid_count + quarantine_count == raw_count


def test_valid_records_have_positive_duration():
    df = pd.read_parquet(VALID_FILE)

    duration = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds()

    assert duration.gt(0).all()


def test_quarantine_records_have_validation_reasons():
    df = pd.read_parquet(QUARANTINE_FILE)

    assert not df.empty
    assert df["validation_reasons"].notna().all()
    assert df["validation_reasons"].str.len().gt(0).all()


def test_warning_columns_exist_in_both_outputs():
    valid_df = pd.read_parquet(VALID_FILE)
    quarantine_df = pd.read_parquet(QUARANTINE_FILE)

    for column in WARNING_COLUMNS:
        assert column in valid_df.columns
        assert column in quarantine_df.columns


def test_warning_columns_contain_boolean_values():
    valid_df = pd.read_parquet(VALID_FILE)

    for column in WARNING_COLUMNS:
        assert pd.api.types.is_bool_dtype(valid_df[column])


def test_warning_flags_match_their_rules():
    df = pd.read_parquet(VALID_FILE)

    assert df["warning_zero_distance"].equals(
        df["trip_distance"].eq(0)
    )

    assert df["warning_negative_fare"].equals(
        df["fare_amount"].lt(0)
    )

    assert df["warning_negative_total"].equals(
        df["total_amount"].lt(0)
    )

    assert df["warning_high_fare"].equals(
        df["fare_amount"].gt(500)
    )

    assert df["warning_long_distance"].equals(
        df["trip_distance"].gt(100)
    )

    assert df["warning_long_duration"].equals(
        df["trip_duration_minutes"].gt(24 * 60)
    )