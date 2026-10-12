
from pathlib import Path

import pandas as pd


# Resolve the project root from this script's location.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "yellow_tripdata_2025-01.parquet"


def main():
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw dataset not found: {RAW_FILE}")

    print(f"Reading dataset: {RAW_FILE}")

    df = pd.read_parquet(RAW_FILE)

    print("\n--- Dataset Overview ---")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

    print("\n--- Column Names ---")
    for column in df.columns:
        print(f"- {column}")

    print("\n--- Missing Values ---")
    missing = df.isna().sum()
    missing = missing[missing > 0].sort_values(ascending=False)

    if missing.empty:
        print("No missing values found.")
    else:
        print(missing.to_string())

    print("\n--- Duplicate Rows ---")
    print(f"Exact duplicate rows: {df.duplicated().sum():,}")

    print("\n--- Ingestion Check ---")
    print("Raw dataset read successfully.")


if __name__ == "__main__":
    main()