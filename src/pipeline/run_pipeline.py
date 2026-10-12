
from pathlib import Path
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


def run_step(step_name: str, script_path: Path) -> None:
    print(f"\n{'=' * 70}", flush=True)
    print(f"STARTING: {step_name}", flush=True)
    print(f"Script: {script_path}", flush=True)
    print(f"{'=' * 70}", flush=True)

    if not script_path.exists():
        raise FileNotFoundError(
            f"Pipeline script not found: {script_path}"
        )

    start_time = time.perf_counter()

    result = subprocess.run(
        [sys.executable, str(script_path)],
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
    print("UBERPULSE — LOCAL DATA PIPELINE", flush=True)
    print(f"Project root: {PROJECT_ROOT}", flush=True)

    pipeline_start = time.perf_counter()

    try:
        for step_name, script_path in PIPELINE_STEPS:
            run_step(step_name, script_path)

    except (FileNotFoundError, RuntimeError) as error:
        print(f"\nPIPELINE FAILED: {error}", flush=True)
        sys.exit(1)

    total_elapsed = time.perf_counter() - pipeline_start

    print(f"\n{'=' * 70}", flush=True)
    print("PIPELINE COMPLETED SUCCESSFULLY", flush=True)
    print(f"Total runtime: {total_elapsed:.2f} seconds", flush=True)
    print("Expected outputs:", flush=True)
    print(
        "  - data/processed/"
        "yellow_tripdata_2025-01_valid.parquet",
        flush=True,
    )
    print(
        "  - data/processed/"
        "yellow_tripdata_2025-01_quarantine.parquet",
        flush=True,
    )
    print(
        "  - data/processed/"
        "yellow_tripdata_2025-01_analytics.parquet",
        flush=True,
    )


if __name__ == "__main__":
    main()