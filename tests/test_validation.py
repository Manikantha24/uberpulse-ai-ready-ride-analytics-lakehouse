import pandas as pd
import pytest

from src.validation.validate_trips import validate_dataset


WARNING_COLUMNS = [
    "warning_zero_distance",
    "warning_negative_fare",
    "warning_negative_total",
    "warning_high_fare",
    "warning_long_distance",
    "warning_long_duration",
]

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
]


def make_trip_data():
    """Return a small dataset containing valid, invalid, and warning records."""
    return pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(
                [
                    "2025-01-01 10:00:00",
                    "2025-01-01 11:00:00",
                    "2025-01-01 12:00:00",
                    "2025-01-01 13:00:00",
                    "2025-01-01 14:00:00",
                    "2025-01-01 15:00:00",
                ]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                [
                    "2025-01-01 10:20:00",  # Valid
                    "2025-01-01 11:00:00",  # Zero duration: quarantine
                    "2025-01-01 12:30:00",  # Valid, warning conditions
                    "2025-01-01 13:15:00",  # Valid, high fare/distance warnings
                    "2025-01-01 14:10:00",  # Valid
                    "2025-01-01 15:30:00",  # Valid
                ]
            ),
            "trip_distance": [2.5, 3.0, 0.0, 150.0, 5.0, 4.0],
            "fare_amount": [15.0, 20.0, -5.0, 600.0, 25.0, 18.0],
            "total_amount": [20.0, 25.0, -5.0, 620.0, 30.0, 22.0],
        }
    )


@pytest.fixture
def sample_data(tmp_path):
    """Write the standard synthetic test dataset to Parquet."""
    input_file = tmp_path / "sample_trips.parquet"
    make_trip_data().to_parquet(input_file, index=False)
    return input_file


@pytest.fixture
def validation_outputs(tmp_path, sample_data):
    """Run validation against the standard fixture dataset."""
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

    validate_dataset(
        input_file=sample_data,
        valid_file=valid_file,
        quarantine_file=quarantine_file,
    )

    return {
        "valid": pd.read_parquet(valid_file),
        "quarantine": pd.read_parquet(quarantine_file),
        "input_count": len(pd.read_parquet(sample_data)),
    }


def test_output_files_are_created(tmp_path, sample_data):
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

    validate_dataset(sample_data, valid_file, quarantine_file)

    assert valid_file.exists()
    assert quarantine_file.exists()


def test_all_records_are_accounted_for(validation_outputs):
    valid_df = validation_outputs["valid"]
    quarantine_df = validation_outputs["quarantine"]

    assert (
        len(valid_df) + len(quarantine_df)
        == validation_outputs["input_count"]
    )


def test_valid_records_have_positive_duration(validation_outputs):
    df = validation_outputs["valid"]

    duration = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds()

    assert duration.gt(0).all()


def test_quarantine_records_have_validation_reasons(validation_outputs):
    df = validation_outputs["quarantine"]

    assert not df.empty
    assert df["validation_reasons"].notna().all()
    assert df["validation_reasons"].str.len().gt(0).all()
    assert df["validation_reasons"].str.contains(
        "non_positive_duration"
    ).all()


def test_warning_columns_exist_in_both_outputs(validation_outputs):
    for output_name in ["valid", "quarantine"]:
        df = validation_outputs[output_name]

        for column in WARNING_COLUMNS:
            assert column in df.columns


def test_warning_columns_contain_boolean_values(validation_outputs):
    for output_name in ["valid", "quarantine"]:
        df = validation_outputs[output_name]

        for column in WARNING_COLUMNS:
            assert pd.api.types.is_bool_dtype(df[column])


def test_warning_flags_match_their_rules(validation_outputs):
    df = validation_outputs["valid"]

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


def test_warning_conditions_do_not_automatically_quarantine(validation_outputs):
    df = validation_outputs["valid"]

    # These records have warning conditions but no hard validation failures.
    assert df["warning_zero_distance"].any()
    assert df["warning_negative_fare"].any()
    assert df["warning_high_fare"].any()
    assert df["warning_long_distance"].any()


def test_missing_required_column_raises_clear_error(tmp_path):
    input_file = tmp_path / "missing_column.parquet"

    pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(
                ["2025-01-01 10:00:00"]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                ["2025-01-01 10:10:00"]
            ),
            "fare_amount": [10.0],
            "total_amount": [12.0],
        }
    ).to_parquet(input_file, index=False)

    with pytest.raises(ValueError, match="Required columns are missing"):
        validate_dataset(
            input_file,
            tmp_path / "valid.parquet",
            tmp_path / "quarantine.parquet",
        )


@pytest.mark.parametrize(
    ("missing_column", "expected_reason"),
    [
        ("tpep_pickup_datetime", "missing_pickup_datetime"),
        ("tpep_dropoff_datetime", "missing_dropoff_datetime"),
    ],
)
def test_missing_timestamp_is_quarantined(
    tmp_path, missing_column, expected_reason
):
    df = make_trip_data().iloc[[0]].copy()
    df.loc[df.index[0], missing_column] = pd.NaT
    input_file = tmp_path / f"{missing_column}_missing.parquet"
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"
    df.to_parquet(input_file, index=False)

    validate_dataset(input_file, valid_file, quarantine_file)

    valid_df = pd.read_parquet(valid_file)
    quarantine_df = pd.read_parquet(quarantine_file)

    assert len(valid_df) == 0
    assert len(quarantine_df) == 1
    assert expected_reason in quarantine_df.iloc[0]["validation_reasons"]


def test_negative_trip_distance_is_quarantined(tmp_path):
    df = make_trip_data().iloc[[0]].copy()
    df.loc[df.index[0], "trip_distance"] = -1.0
    input_file = tmp_path / "negative_distance.parquet"
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"
    df.to_parquet(input_file, index=False)

    validate_dataset(input_file, valid_file, quarantine_file)

    assert len(pd.read_parquet(valid_file)) == 0
    quarantine_df = pd.read_parquet(quarantine_file)
    assert len(quarantine_df) == 1
    assert "negative_trip_distance" in quarantine_df.iloc[0]["validation_reasons"]


def test_all_applicable_failure_reasons_are_preserved(tmp_path):
    df = make_trip_data().iloc[[0]].copy()
    row_index = df.index[0]
    df.loc[row_index, "tpep_pickup_datetime"] = pd.NaT
    df.loc[row_index, "trip_distance"] = -2.0
    df.loc[row_index, "fare_amount"] = float("nan")
    input_file = tmp_path / "multiple_failures.parquet"
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"
    df.to_parquet(input_file, index=False)

    validate_dataset(input_file, valid_file, quarantine_file)

    quarantine_df = pd.read_parquet(quarantine_file)
    assert len(quarantine_df) == 1
    reasons = quarantine_df.iloc[0]["validation_reasons"]

    assert "missing_pickup_datetime" in reasons
    assert "negative_trip_distance" in reasons
    assert "missing_fare_amount" in reasons


def test_warning_flags_are_preserved_on_quarantined_records(tmp_path):
    df = make_trip_data().iloc[[0]].copy()
    row_index = df.index[0]
    df.loc[row_index, "trip_distance"] = -1.0
    df.loc[row_index, "fare_amount"] = -5.0
    input_file = tmp_path / "warning_and_failure.parquet"
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"
    df.to_parquet(input_file, index=False)

    validate_dataset(input_file, valid_file, quarantine_file)

    quarantine_df = pd.read_parquet(quarantine_file)
    assert len(quarantine_df) == 1
    assert bool(quarantine_df.iloc[0]["warning_negative_fare"])
    assert not bool(quarantine_df.iloc[0]["warning_zero_distance"])


def test_empty_dataset_produces_empty_outputs(tmp_path):
    empty_df = pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.Series(dtype="datetime64[ns]"),
            "tpep_dropoff_datetime": pd.Series(dtype="datetime64[ns]"),
            "trip_distance": pd.Series(dtype="float64"),
            "fare_amount": pd.Series(dtype="float64"),
            "total_amount": pd.Series(dtype="float64"),
        }
    )
    input_file = tmp_path / "empty.parquet"
    valid_file = tmp_path / "valid.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"
    empty_df.to_parquet(input_file, index=False)

    validate_dataset(input_file, valid_file, quarantine_file)

    assert valid_file.exists()
    assert quarantine_file.exists()
    assert len(pd.read_parquet(valid_file)) == 0
    assert len(pd.read_parquet(quarantine_file)) == 0


def test_missing_input_file_raises_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="Raw dataset not found"):
        validate_dataset(
            tmp_path / "does_not_exist.parquet",
            tmp_path / "valid.parquet",
            tmp_path / "quarantine.parquet",
        )
