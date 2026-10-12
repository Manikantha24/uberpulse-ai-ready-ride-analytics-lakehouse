
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

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


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Validated input dataset not found: {INPUT_FILE}"
        )

    df = pd.read_parquet(INPUT_FILE)
    input_count = len(df)

    required_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "tip_amount",
    ]

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    # Calculate trip duration in minutes.
    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Extract pickup date and time attributes.
    pickup = df["tpep_pickup_datetime"]

    df["pickup_date"] = pickup.dt.normalize()
    df["pickup_hour"] = pickup.dt.hour
    df["pickup_day_of_week"] = pickup.dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6])

    # Calculate average speed only for positive durations
    # and nonnegative trip distances.
    valid_speed = (
        df["trip_duration_minutes"].gt(0)
        & df["trip_distance"].ge(0)
    )

    df["average_speed_mph"] = float("nan")
    df.loc[valid_speed, "average_speed_mph"] = (
        df.loc[valid_speed, "trip_distance"]
        / (df.loc[valid_speed, "trip_duration_minutes"] / 60)
    )

    # Calculate tip percentage only when the fare is positive.
    valid_fare = df["fare_amount"].gt(0)

    df["tip_percentage"] = float("nan")
    df.loc[valid_fare, "tip_percentage"] = (
        df.loc[valid_fare, "tip_amount"]
        / df.loc[valid_fare, "fare_amount"]
    ) * 100

    # Keep the original records and all existing validation
    # and warning columns. Do not silently drop any rows.
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)

    # Read the saved file back to verify the output.
    saved_df = pd.read_parquet(OUTPUT_FILE)

    print("--- Analytics Transformation Summary ---")
    print(f"Input records:       {input_count:,}")
    print(f"Output records:      {len(saved_df):,}")
    print(f"Input columns:       {input_count and len(pd.read_parquet(INPUT_FILE).columns):,}")
    print(f"Output columns:      {len(saved_df.columns):,}")
    print(f"Output file:         {OUTPUT_FILE}")
    print(
        f"Missing speed values: "
        f"{saved_df['average_speed_mph'].isna().sum():,}"
    )
    print(
        f"Missing tip percentages: "
        f"{saved_df['tip_percentage'].isna().sum():,}"
    )

    assert len(saved_df) == input_count, (
        "Record count changed during transformation."
    )

    assert saved_df["pickup_hour"].between(0, 23).all(), (
        "Pickup hour contains an invalid value."
    )

    assert saved_df["pickup_day_of_week"].between(0, 6).all(), (
        "Pickup day of week contains an invalid value."
    )

    print("\nRecord count reconciliation: PASSED")
    print("Basic transformation checks: PASSED")


if __name__ == "__main__":
    main()