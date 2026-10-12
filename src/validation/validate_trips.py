
from pathlib import Path
import argparse

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_RAW_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "yellow_tripdata_2025-01.parquet"
)

DEFAULT_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

DEFAULT_VALID_FILE = (
    DEFAULT_PROCESSED_DIR / "yellow_tripdata_2025-01_valid.parquet"
)

DEFAULT_QUARANTINE_FILE = (
    DEFAULT_PROCESSED_DIR / "yellow_tripdata_2025-01_quarantine.parquet"
)


def validate_dataset(
    input_file: Path,
    valid_file: Path,
    quarantine_file: Path,
) -> None:
    """Validate trips and write valid and quarantined records."""

    if not input_file.exists():
        raise FileNotFoundError(f"Raw dataset not found: {input_file}")

    valid_file.parent.mkdir(parents=True, exist_ok=True)
    quarantine_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_file)
    original_count = len(df)

    required_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing from input: {missing_columns}"
        )

    # Calculate trip duration in minutes.
    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Define hard validation rules.
    checks = {
        "missing_pickup_datetime": df["tpep_pickup_datetime"].isna(),
        "missing_dropoff_datetime": df["tpep_dropoff_datetime"].isna(),
        "non_positive_duration": df["trip_duration_minutes"].le(0),
        "negative_trip_distance": df["trip_distance"].lt(0),
        "missing_fare_amount": df["fare_amount"].isna(),
        "missing_total_amount": df["total_amount"].isna(),
    }

    # Collect all failure reasons for each record.
    reasons = pd.Series("", index=df.index, dtype="object")

    for reason, failed in checks.items():
        reasons.loc[failed] = reasons.loc[failed].apply(
            lambda existing: (
                f"{existing};{reason}" if existing else reason
            )
        )

    df["validation_reasons"] = reasons

    # Add warning flags. Warnings do not cause quarantine.
    df["warning_zero_distance"] = df["trip_distance"].eq(0)
    df["warning_negative_fare"] = df["fare_amount"].lt(0)
    df["warning_negative_total"] = df["total_amount"].lt(0)
    df["warning_high_fare"] = df["fare_amount"].gt(500)
    df["warning_long_distance"] = df["trip_distance"].gt(100)
    df["warning_long_duration"] = df["trip_duration_minutes"].gt(24 * 60)

    # Separate records using hard validation failures only.
    quarantine_mask = reasons.ne("")

    valid_df = df.loc[~quarantine_mask].copy()
    quarantine_df = df.loc[quarantine_mask].copy()

    # Write processed outputs.
    valid_df.to_parquet(valid_file, index=False)
    quarantine_df.to_parquet(quarantine_file, index=False)

    # Print processing summary.
    print("--- Silver Validation Summary ---")
    print(f"Input records:     {original_count:,}")
    print(f"Valid records:     {len(valid_df):,}")
    print(f"Quarantined rows:  {len(quarantine_df):,}")
    print(f"Records accounted: {len(valid_df) + len(quarantine_df):,}")

    print(f"\nValid output:      {valid_file}")
    print(f"Quarantine output: {quarantine_file}")

    print("\n--- Failure Counts (rules can overlap) ---")
    for reason, failed in checks.items():
        print(f"{reason}: {failed.sum():,}")

    warning_columns = [
        "warning_zero_distance",
        "warning_negative_fare",
        "warning_negative_total",
        "warning_high_fare",
        "warning_long_distance",
        "warning_long_duration",
    ]

    print("\n--- Warning Counts (rules can overlap) ---")
    for column in warning_columns:
        print(f"{column}: {df[column].sum():,}")

    # Ensure every input record is accounted for.
    assert len(valid_df) + len(quarantine_df) == original_count

    print("\nRecord reconciliation: PASSED")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate taxi trips and separate valid and quarantined records."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_RAW_FILE,
        help="Path to the source Parquet file.",
    )
    parser.add_argument(
        "--valid-output",
        type=Path,
        default=DEFAULT_VALID_FILE,
        help="Path for valid records.",
    )
    parser.add_argument(
        "--quarantine-output",
        type=Path,
        default=DEFAULT_QUARANTINE_FILE,
        help="Path for quarantined records.",
    )

    args = parser.parse_args()

    validate_dataset(
        input_file=args.input,
        valid_file=args.valid_output,
        quarantine_file=args.quarantine_output,
    )


if __name__ == "__main__":
    main()
