
from pathlib import Path
import argparse

import pandas as pd


# Resolve the project root from this script's location.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "yellow_tripdata_2025-01.parquet"
)


def inspect_dataset(raw_file: Path) -> None:
    """Inspect a Parquet dataset and print a concise quality summary."""
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw dataset not found: {raw_file}")

    print(f"Reading dataset: {raw_file}")

    df = pd.read_parquet(raw_file)

    print("\n--- Dataset Overview ---")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(
        "Memory usage: "
        f"{df.memory_usage(deep=True).sum() / (1024**2):.2f} MB"
    )

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect a Parquet dataset for basic data-quality issues."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_RAW_FILE,
        help="Path to the source Parquet file.",
    )
    args = parser.parse_args()

    inspect_dataset(args.input)


if __name__ == "__main__":
    main()