
import pandas as pd
import pytest


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


@pytest.fixture
def sample_raw_file(tmp_path):
    """Create a small synthetic raw dataset for testing."""
    raw_file = tmp_path / "sample_raw_trips.parquet"

    df = pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": pd.to_datetime(
                ["2025-01-01 10:00:00", "2025-01-02 12:00:00"]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                ["2025-01-01 10:20:00", "2025-01-02 12:30:00"]
            ),
            "passenger_count": [1, 2],
            "trip_distance": [2.5, 4.0],
            "PULocationID": [100, 200],
            "DOLocationID": [101, 201],
            "payment_type": [1, 2],
            "fare_amount": [12.5, 18.0],
            "total_amount": [15.0, 21.5],
        }
    )

    df.to_parquet(raw_file, index=False)
    return raw_file


def test_sample_raw_file_exists(sample_raw_file):
    assert sample_raw_file.exists()


def test_sample_raw_file_is_not_empty(sample_raw_file):
    df = pd.read_parquet(sample_raw_file)

    assert not df.empty


def test_expected_columns_exist(sample_raw_file):
    df = pd.read_parquet(sample_raw_file)
    missing_columns = EXPECTED_COLUMNS - set(df.columns)

    assert not missing_columns, (
        f"Missing expected columns: {sorted(missing_columns)}"
    )


def test_pickup_and_dropoff_are_datetime(sample_raw_file):
    df = pd.read_parquet(sample_raw_file)

    assert pd.api.types.is_datetime64_any_dtype(
        df["tpep_pickup_datetime"]
    )
    assert pd.api.types.is_datetime64_any_dtype(
        df["tpep_dropoff_datetime"]
    )
