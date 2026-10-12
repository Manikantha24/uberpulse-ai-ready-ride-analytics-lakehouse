
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "yellow_tripdata_2025-01.parquet"
)

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

VALID_FILE = (
    PROCESSED_DIR / "yellow_tripdata_2025-01_valid.parquet"
)

QUARANTINE_FILE = (
    PROCESSED_DIR / "yellow_tripdata_2025-01_quarantine.parquet"
)


def main():
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw dataset not found: {RAW_FILE}")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(RAW_FILE)
    original_count = len(df)

    # Calculate trip duration in minutes.
    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Define hard validation rules.
    checks = {
        "missing_pickup_datetime": (
            df["tpep_pickup_datetime"].isna()
        ),
        "missing_dropoff_datetime": (
            df["tpep_dropoff_datetime"].isna()
        ),
        "non_positive_duration": (
            df["trip_duration_minutes"].le(0)
        ),
        "negative_trip_distance": (
            df["trip_distance"].lt(0)
        ),
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

    df["warning_long_duration"] = (
        df["trip_duration_minutes"].gt(24 * 60)
    )

    # Separate records using hard validation failures only.
    quarantine_mask = reasons.ne("")

    valid_df = df.loc[~quarantine_mask].copy()
    quarantine_df = df.loc[quarantine_mask].copy()

    # Write the processed outputs.
    valid_df.to_parquet(VALID_FILE, index=False)
    quarantine_df.to_parquet(QUARANTINE_FILE, index=False)

    # Print processing summary.
    print("--- Silver Validation Summary ---")
    print(f"Input records:     {original_count:,}")
    print(f"Valid records:     {len(valid_df):,}")
    print(f"Quarantined rows:  {len(quarantine_df):,}")
    print(
        f"Records accounted: "
        f"{len(valid_df) + len(quarantine_df):,}"
    )

    print(f"\nValid output:      {VALID_FILE}")
    print(f"Quarantine output: {QUARANTINE_FILE}")

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


if __name__ == "__main__":
    main()