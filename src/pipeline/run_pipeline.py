
from pathlib import Path
import argparse
import logging
import subprocess
import sys
import time

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_ROOT / "logs" / "pipeline_demo.log"

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


def configure_logging() -> logging.Logger:
    """Configure console and UTF-8 file logging for each pipeline run."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("uberpulse")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        LOG_FILE,
        mode="w",
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def log_child_output(step_name: str, line: str) -> None:
    """Persist child-process output to the log without duplicating console output."""
    logger = logging.getLogger("uberpulse")
    message = line.rstrip("\r\n")

    if not message:
        return

    record = logger.makeRecord(
        logger.name,
        logging.INFO,
        "",
        0,
        "CHILD OUTPUT [%s] %s",
        (step_name, message),
        None,
    )

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.handle(record)


def parse_args() -> argparse.Namespace:
    """Parse configurable pipeline input and output paths."""
    parser = argparse.ArgumentParser(
        description="Run the UberPulse local data pipeline."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "raw"
            / "yellow_tripdata_2025-01.parquet"
        ),
        help="Path to the source Parquet file.",
    )
    parser.add_argument(
        "--valid-output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "processed"
            / "yellow_tripdata_2025-01_valid.parquet"
        ),
        help="Path for validated records.",
    )
    parser.add_argument(
        "--quarantine-output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "processed"
            / "yellow_tripdata_2025-01_quarantine.parquet"
        ),
        help="Path for quarantined records.",
    )
    parser.add_argument(
        "--analytics-output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data"
            / "processed"
            / "yellow_tripdata_2025-01_analytics.parquet"
        ),
        help="Path for transformed analytics records.",
    )

    return parser.parse_args()


def get_parquet_metrics(file_path: Path) -> dict[str, int | float | str]:
    """Return row count, column count, and size for a Parquet file."""
    if not file_path.exists():
        return {
            "path": str(file_path),
            "status": "missing",
        }

    metrics: dict[str, int | float | str] = {
        "path": str(file_path),
        "size_mb": round(
            file_path.stat().st_size / (1024 * 1024),
            2,
        ),
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
    max_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> float:
    """Run a pipeline step, retrying transient process-launch failures."""
    logger = logging.getLogger("uberpulse")

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative.")

    logger.info("Starting step: %s", step_name)
    logger.info("Script: %s", script_path)

    if not script_path.exists():
        logger.error("Pipeline script not found: %s", script_path)
        raise FileNotFoundError(
            f"Pipeline script not found: {script_path}"
        )

    command = [sys.executable, "-u", str(script_path)]

    if extra_args:
        command.extend(extra_args)

    step_start_time = time.perf_counter()

    for attempt in range(1, max_attempts + 1):
        logger.info(
            "Step attempt %s/%s: %s",
            attempt,
            max_attempts,
            step_name,
        )

        try:
            process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError:
            logger.exception(
                "Could not launch step '%s' on attempt %s/%s",
                step_name,
                attempt,
                max_attempts,
            )

            if attempt == max_attempts:
                logger.error(
                    "Step exhausted all %s attempts: %s",
                    max_attempts,
                    step_name,
                )
                raise

            logger.warning(
                "Retrying step '%s' in %.2f seconds",
                step_name,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
            continue

        try:
            if process.stdout is None:
                process.kill()
                process.wait()
                raise RuntimeError(
                    f"Could not capture output from {step_name}."
                )

            with process.stdout:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log_child_output(step_name, line)

            return_code = process.wait()

        except (OSError, RuntimeError):
            if process.poll() is None:
                process.kill()
                process.wait()

            logger.exception(
                "Could not capture output from step '%s'",
                step_name,
            )
            raise

        elapsed = time.perf_counter() - step_start_time

        if return_code != 0:
            logger.error(
                "Step failed: %s | Duration: %.2f seconds | Exit code: %s",
                step_name,
                elapsed,
                return_code,
            )
            raise RuntimeError(
                f"{step_name} failed with exit code "
                f"{return_code}. Pipeline stopped."
            )

        logger.info(
            "Step completed successfully: %s | Duration: %.2f seconds",
            step_name,
            elapsed,
        )
        return elapsed

    raise RuntimeError(
        f"Step failed unexpectedly: {step_name}"
    )


def print_pipeline_summary(
    step_results: list[dict[str, str | float | None]],
    total_elapsed: float,
    success: bool,
) -> None:
    """Print and log the pipeline execution summary."""
    logger = logging.getLogger("uberpulse")

    print(f"\n{'=' * 70}", flush=True)
    print("PIPELINE RUN SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)

    for result in step_results:
        duration = result["duration_seconds"]

        if isinstance(duration, (int, float)):
            duration_text = f"{duration:.2f} seconds"
        else:
            duration_text = "duration unavailable"

        summary_line = (
            f"{result['step']}: {result['status']} "
            f"({duration_text})"
        )

        print(summary_line, flush=True)
        logger.info("Step summary: %s", summary_line)

    print(f"Total runtime: {total_elapsed:.2f} seconds", flush=True)
    logger.info("Total pipeline runtime: %.2f seconds", total_elapsed)

    if success:
        print("PIPELINE COMPLETED SUCCESSFULLY", flush=True)
        logger.info("Pipeline completed successfully")
    else:
        print("PIPELINE FAILED", flush=True)
        logger.error("Pipeline failed")


def main() -> None:
    """Run the pipeline and report execution and output metrics."""
    logger = configure_logging()
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
    print(f"Log file: {LOG_FILE}", flush=True)

    logger.info("=" * 60)
    logger.info("UberPulse pipeline run started")
    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("Input file: %s", input_file)
    logger.info("Log file: %s", LOG_FILE)

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
        logger.exception("Pipeline stopped because a step failed")
        print(f"\nPIPELINE FAILED: {error}", flush=True)

        print_pipeline_summary(
            step_results,
            time.perf_counter() - pipeline_start,
            success=False,
        )

        logger.info("Pipeline run ended with failure")
        sys.exit(1)

    total_elapsed = time.perf_counter() - pipeline_start

    print_pipeline_summary(
        step_results,
        total_elapsed,
        success=True,
    )

    print("\nOutput metrics:", flush=True)
    logger.info("Collecting output file metrics")

    for output_file in (
        valid_output,
        quarantine_output,
        analytics_output,
    ):
        metrics = get_parquet_metrics(output_file)
        print(f"  - {metrics['path']}", flush=True)

        if "rows" in metrics:
            details = (
                f"Rows: {metrics['rows']:,} | "
                f"Columns: {metrics['columns']} | "
                f"Size: {metrics['size_mb']:.2f} MB"
            )
            print(f"    {details}", flush=True)
            logger.info(
                "Output metrics: %s | %s",
                output_file,
                details,
            )
        elif "size_mb" in metrics:
            details = f"Size: {metrics['size_mb']:.2f} MB"
            print(f"    {details}", flush=True)
            logger.info(
                "Output metrics: %s | %s",
                output_file,
                details,
            )
        else:
            reason = metrics.get(
                "status",
                metrics.get("metrics_error", "unknown error"),
            )
            print(f"    Metrics unavailable: {reason}", flush=True)
            logger.warning(
                "Output metrics unavailable for %s: %s",
                output_file,
                reason,
            )

    logger.info("UberPulse pipeline run finished successfully")


if __name__ == "__main__":
    main()