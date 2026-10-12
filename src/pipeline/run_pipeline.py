
from pathlib import Path
import argparse
import subprocess
import sys
import time

import pyarrow.parquet as pq


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


def get_parquet_metrics(file_path: Path) -> dict[str, int | float | str]:
    """Return basic file-size, row-count, and column-count metrics."""
    if not file_path.exists():
        return {"path": str(file_path), "status": "missing"}

    metrics: dict[str, int | float | str] = {
        "path": str(file_path),
        "size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
    }

    try:
        parquet_file = pq.ParquetFile(file_path)
        metrics["rows"] = parquet_file.metadata.num_rows
        metrics["columns"] = len(parquet_file.schema.names)
    except (OSError, ValueError, TypeError) as error:
        metrics["metrics_error"] = str(error)

    return metrics


def run_step(
    step_name: str,
    script_path: Path,
    extra_args: list[str] | None = None,
) -> float:
    """Run one pipeline script and return its duration; raise on failure."""
    print(f"\n{'=' * 70}", flush=True)
    print(f"STARTING: {step_name}", flush=True)
    print(f"Script: {script_path}", flush=True)
    print(
        f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        flush=True,
    )
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
        print(
            f"FAILED: {step_name} after {elapsed:.2f} seconds "
            f"(exit code {result.returncode})",
            flush=True,
        )
        raise RuntimeError(
            f"{step_name} failed with exit code "
            f"{result.returncode}. Pipeline stopped."
        )

    print(
        f"COMPLETED: {step_name} in {elapsed:.2f} seconds",
        flush=True,
    )
    return elapsed


def print_pipeline_summary(
    step_results: list[dict[str, str | float | None]],
    total_elapsed: float,
    success: bool,
) -> None:
    """Print a concise summary of step statuses and durations."""
    print(f"\n{'=' * 70}", flush=True)
    print("PIPELINE RUN SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)

    for result in step_results:
        duration = result["duration_seconds"]

        if isinstance(duration, (int, float)):
            duration_text = f"{duration:.2f} seconds"
        else:
            duration_text = "duration unavailable"

        print(
            f"{result['step']}: {result['status']} "
            f"({duration_text})",
            flush=True,
        )

    print(f"Total runtime: {total_elapsed:.2f} seconds", flush=True)

    if success:
        print("PIPELINE COMPLETED SUCCESSFULLY", flush=True)
    else:
        print("PIPELINE FAILED", flush=True)


def main() -> None:
    args = parse_args()

    input_file = args.input.resolve()
    valid_output = args.valid_output.resolve()
    quarantine_output = args.quarantine_output.resolve()
    analytics_output = args.analytics_output.resolve()

    step_args = [
        ["--input", str(input_file)],
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
    step_results: list[dict[str, str | float | None]] = []

    try:
        for (step_name, script_path), extra_args in zip(
            PIPELINE_STEPS,
            step_args,
            strict=True,
        ):
            step_start = time.perf_counter()

            try:
                elapsed = run_step(
                    step_name,
                    script_path,
                    extra_args,
                )
            except (FileNotFoundError, RuntimeError):
                step_results.append(
                    {
                        "step": step_name,
                        "status": "FAILED",
                        "duration_seconds": (
                            time.perf_counter() - step_start
                        ),
                    }
                )
                raise

            step_results.append(
                {
                    "step": step_name,
                    "status": "SUCCESS",
                    "duration_seconds": elapsed,
                }
            )

    except (FileNotFoundError, RuntimeError) as error:
        print(f"\nPIPELINE FAILED: {error}", flush=True)
        print_pipeline_summary(
            step_results,
            time.perf_counter() - pipeline_start,
            success=False,
        )
        sys.exit(1)

    print_pipeline_summary(
        step_results,
        time.perf_counter() - pipeline_start,
        success=True,
    )

    print("\nOutput metrics:", flush=True)

    for output_file in (
        valid_output,
        quarantine_output,
        analytics_output,
    ):
        metrics = get_parquet_metrics(output_file)
        print(f"  - {metrics['path']}", flush=True)

        if "rows" in metrics:
            print(
                f"    Rows: {metrics['rows']:,} | "
                f"Columns: {metrics['columns']} | "
                f"Size: {metrics['size_mb']:.2f} MB",
                flush=True,
            )
        elif "size_mb" in metrics:
            print(
                f"    Size: {metrics['size_mb']:.2f} MB",
                flush=True,
            )
        else:
            reason = metrics.get(
                "status",
                metrics.get("metrics_error", "unknown error"),
            )
            print(
                f"    Metrics unavailable: {reason}",
                flush=True,
            )


if __name__ == "__main__":
    main()