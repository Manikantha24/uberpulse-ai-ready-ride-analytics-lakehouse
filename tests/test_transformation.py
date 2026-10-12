
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "yellow_tripdata_2025-01_valid.parquet"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "yellow_tripdata_2025-01_analytics.parquet"
)


def test_analytics_output_exists():
    assert OUTPUT_FILE.exists(), (
        f"Analytics output not found: {OUTPUT_FILE}"
    )


def test_analytics_output_is_not_empty():
    df = pd.read_parquet(OUTPUT_FILE)

    assert not df.empty


def test_input_and_output_record_counts_match():
    input_df = pd.read_parquet(INPUT_FILE)
    output_df = pd.read_parquet(OUTPUT_FILE)

    assert len(output_df) == len(input_df)


def test_expected_derived_columns_exist():
    df = pd.read_parquet(OUTPUT_FILE)

    expected_columns = [
        "trip_duration_minutes",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "average_speed_mph",
        "tip_percentage",
    ]

    for column in expected_columns:
        assert column in df.columns, (
            f"Expected column missing: {column}"
        )


def test_pickup_hour_is_in_valid_range():
    df = pd.read_parquet(OUTPUT_FILE)

    assert df["pickup_hour"].notna().all()
    assert df["pickup_hour"].between(0, 23).all()


def test_pickup_day_of_week_is_in_valid_range():
    df = pd.read_parquet(OUTPUT_FILE)

    assert df["pickup_day_of_week"].notna().all()
    assert df["pickup_day_of_week"].between(0, 6).all()


def test_weekend_flag_matches_day_of_week():
    df = pd.read_parquet(OUTPUT_FILE)

    expected_weekend = df["pickup_day_of_week"].isin([5, 6])

    assert df["is_weekend"].equals(expected_weekend)


def test_positive_fares_have_tip_percentage():
    df = pd.read_parquet(OUTPUT_FILE)

    positive_fare = df["fare_amount"].gt(0)

    assert df.loc[
        positive_fare, "tip_percentage"
    ].notna().all()


def test_nonpositive_fares_have_missing_tip_percentage():
    df = pd.read_parquet(OUTPUT_FILE)

    nonpositive_fare = df["fare_amount"].le(0)

    assert df.loc[
        nonpositive_fare, "tip_percentage"
    ].isna().all()


def test_average_speed_is_nonnegative_when_present():
    df = pd.read_parquet(OUTPUT_FILE)

    speed = df["average_speed_mph"].dropna()

    assert speed.ge(0).all()