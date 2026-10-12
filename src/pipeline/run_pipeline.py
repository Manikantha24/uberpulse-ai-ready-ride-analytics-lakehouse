
from pathlib import Path
import argparse
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PIPELINE_STEPS = [
    (
        "1. Raw data inspection",
        PROJECT_ROOT / "src" / "ingestion" / "inspect_raw_data.py",
    ),
    (
        "2. Data validation",
        PROJECT_ROOT / "src" / "validation" / "validate_trips.py",
    ),
    (
        "3. Analytics transformation",
        PROJECT_ROOT / "src" / "transformation" / "transform_trips.py",
    ),
]


def parse_args() -> argparse.Namespace:
    """Parse optional input and output paths for a pipeline run."""
    parser = argparse.ArgumentParser(
        description="Run the UberPulse local data pipeline."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT
        / "data"
        / "raw"
        / "yellow_tripdata_2025-01.parquet",
        help="Path to the source Parquet file.",
    )
    parser.add_argument(
        "--valid-output",
        type=Path,
        default=PROJECT_ROOT
        / "data"
        / "processed"
        / "yellow_tripdata_2025-01_valid.parquet",
        help="Path for validated records.",
    )
    parser.add_argument(
        "--quarantine-output",
        type=Path,
        default=PROJECT_ROOT
        / "data"
        / "processed"
        / "yellow_tripdata_2025-01_quarantine.parquet",
        help="Path for quarantined records.",
    )
    parser.add_argument(
        "--analytics-output",
        type=Path,
        default=PROJECT_ROOT
        / "data"
        / "processed"
        / "yellow_tripdata_2025-01_analytics.parquet",
        help="Path for transformed analytics records.",
    )

    return parser.parse_args()


def run_step(
    step_name: str,
    script_path: Path,
    extra_args: list[str] | None = None,
) -> None:
    """Run one pipeline script and stop if it fails."""
    print(f"\n{'=' * 70}", flush=True)
    print(f"STARTING: {step_name}", flush=True)
    print(f"Script: {script_path}", flush=True)
    print(f"{'=' * 70}", flush=True)

    if not script_path.exists():
        raise FileNotFoundError(
            f"Pipeline script not found: {script_path}"
        )

    command = [sys.executable, str(script_path)]
    if extra_args:
        command.extend(extra_args)

    start_time = time.perf_counter()

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )

    elapsed = time.perf_counter() - start_time

    if result.returncode != 0:
        raise RuntimeError(
            f"{step_name} failed with exit code "
            f"{result.returncode}. Pipeline stopped."
        )

    print(
        f"\nCOMPLETED: {step_name} "
        f"in {elapsed:.2f} seconds",
        flush=True,
    )


def main() -> None:
    args = parse_args()

    input_file = args.input.resolve()
    valid_output = args.valid_output.resolve()
    quarantine_output = args.quarantine_output.resolve()
    analytics_output = args.analytics_output.resolve()

    step_args = [
        [
            "--input",
            str(input_file),
        ],
        [
            "--input",
            str(input_file),
            "--valid-output",
            str(valid_output),
            "--quarantine-output",
            str(quarantine_output),
        ],
        [
            "--input",
            str(valid_output),
            "--output",
            str(analytics_output),
        ],
    ]

    print("UBERPULSE - LOCAL DATA PIPELINE", flush=True)
    print(f"Project root: {PROJECT_ROOT}", flush=True)
    print(f"Input file: {input_file}", flush=True)

    pipeline_start = time.perf_counter()

    try:
        for (step_name, script_path), extra_args in zip(
            PIPELINE_STEPS, step_args
        ):
            run_step(step_name, script_path, extra_args)

    except (FileNotFoundError, RuntimeError) as error:
        print(f"\nPIPELINE FAILED: {error}", flush=True)
        sys.exit(1)

    total_elapsed = time.perf_counter() - pipeline_start

    print(f"\n{'=' * 70}", flush=True)
    print("PIPELINE COMPLETED SUCCESSFULLY", flush=True)
    print(f"Total runtime: {total_elapsed:.2f} seconds", flush=True)
    print("Expected outputs:", flush=True)
    print(f"  - {valid_output}", flush=True)
    print(f"  - {quarantine_output}", flush=True)
    print(f"  - {analytics_output}", flush=True)


if __name__ == "__main__":
    main()
