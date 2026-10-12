
import pandas as pd
import pytest

from src.transformation.transform_trips import transform_dataset


@pytest.fixture
def sample_input(tmp_path):
    """Create synthetic trips covering weekdays, weekends, and edge cases."""
    input_file = tmp_path / "sample_valid_trips.parquet"

    df = pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(
                [
                    "2025-01-06 10:00:00",  # Monday
                    "2025-01-11 23:00:00",  # Saturday
                    "2025-01-12 08:00:00",  # Sunday
                    "2025-01-07 12:00:00",  # Tuesday, nonpositive fare
                    "2025-01-08 14:00:00",  # Wednesday, zero distance
                ]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                [
                    "2025-01-06 10:30:00",
                    "2025-01-11 23:30:00",
                    "2025-01-12 09:00:00",
                    "2025-01-07 12:15:00",
                    "2025-01-08 14:20:00",
                ]
            ),
            "trip_distance": [10.0, 15.0, 5.0, 3.0, 0.0],
            "fare_amount": [20.0, 30.0, 10.0, 0.0, -5.0],
            "tip_amount": [4.0, 6.0, 2.0, 1.0, 0.0],
            "validation_reasons": ["", "", "", "", ""],
            "warning_zero_distance": [False, False, False, False, True],
        }
    )

    df.to_parquet(input_file, index=False)
    return input_file


@pytest.fixture
def transformed_data(tmp_path, sample_input):
    """Run the actual transformation and load its output."""
    output_file = tmp_path / "analytics.parquet"

    transform_dataset(
        input_file=sample_input,
        output_file=output_file,
    )

    assert output_file.exists()
    return pd.read_parquet(output_file)


def test_analytics_output_is_created(tmp_path, sample_input):
    output_file = tmp_path / "analytics.parquet"

    transform_dataset(sample_input, output_file)

    assert output_file.exists()
    assert not pd.read_parquet(output_file).empty


def test_input_and_output_record_counts_match(
    sample_input, transformed_data
):
    input_df = pd.read_parquet(sample_input)

    assert len(transformed_data) == len(input_df)


def test_expected_derived_columns_exist(transformed_data):
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
        assert column in transformed_data.columns


def test_pickup_date_hour_and_weekday_are_correct(transformed_data):
    first_trip = transformed_data.iloc[0]

    assert first_trip["pickup_date"] == pd.Timestamp("2025-01-06")
    assert first_trip["pickup_hour"] == 10
    assert first_trip["pickup_day_of_week"] == 0


def test_pickup_hour_and_weekday_are_in_valid_ranges(transformed_data):
    assert transformed_data["pickup_hour"].between(0, 23).all()
    assert transformed_data["pickup_day_of_week"].between(0, 6).all()


def test_weekend_flag_matches_day_of_week(transformed_data):
    expected_weekend = transformed_data["pickup_day_of_week"].isin([5, 6])

    assert transformed_data["is_weekend"].equals(expected_weekend)


def test_average_speed_is_calculated_correctly(transformed_data):
    # 10 miles in 30 minutes equals 20 miles per hour.
    assert transformed_data.loc[0, "average_speed_mph"] == pytest.approx(20.0)

    # Zero distance with positive duration produces zero average speed.
    assert transformed_data.loc[4, "average_speed_mph"] == pytest.approx(0.0)


def test_tip_percentage_is_calculated_correctly(transformed_data):
    # A $4 tip on a $20 fare equals 20%.
    assert transformed_data.loc[0, "tip_percentage"] == pytest.approx(20.0)


def test_nonpositive_fares_have_missing_tip_percentage(transformed_data):
    nonpositive_fare = transformed_data["fare_amount"].le(0)

    assert transformed_data.loc[
        nonpositive_fare, "tip_percentage"
    ].isna().all()


def test_average_speed_is_nonnegative_when_present(transformed_data):
    speed = transformed_data["average_speed_mph"].dropna()

    assert speed.ge(0).all()


def test_existing_columns_and_warning_flags_are_preserved(transformed_data):
    assert "validation_reasons" in transformed_data.columns
    assert "warning_zero_distance" in transformed_data.columns

    assert transformed_data["warning_zero_distance"].tolist() == [
        False,
        False,
        False,
        False,
        True,
    ]


def test_missing_required_column_raises_clear_error(tmp_path):
    input_file = tmp_path / "missing_column.parquet"

    pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(
                ["2025-01-06 10:00:00"]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                ["2025-01-06 10:30:00"]
            ),
            "trip_distance": [5.0],
            "fare_amount": [10.0],
            # tip_amount intentionally omitted
        }
    ).to_parquet(input_file, index=False)

    with pytest.raises(ValueError, match="Required columns are missing"):
        transform_dataset(input_file, tmp_path / "analytics.parquet")
