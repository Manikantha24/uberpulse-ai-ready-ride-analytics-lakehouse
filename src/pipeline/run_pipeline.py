
from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import logging
import subprocess
import sys
import time
import uuid

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_ROOT / "logs" / "pipeline_demo.log"
REPORT_FILE = PROJECT_ROOT / "logs" / "pipeline_run_report.json"

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


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


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
    """Write child-process output to the log without duplicating console output."""
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


def write_run_report(report: dict) -> None:
    """Write the latest pipeline run report as formatted JSON."""
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = REPORT_FILE.with_suffix(".json.tmp")
    try:
        with temporary_file.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2, ensure_ascii=False)
            file.write("\n")

        # Replace the previous report only after the new report is complete.
        temporary_file.replace(REPORT_FILE)
    finally:
        if temporary_file.exists():
            temporary_file.unlink()


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
    """Run a step, retrying transient process-launch failures."""
    logger = logging.getLogger("uberpulse")

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative.")

    logger.info("Starting step: %s", step_name)
    logger.info("Script: %s", script_path)

    if not script_path.exists():
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
            message = (
                f"{step_name} failed with exit code "
                f"{return_code}. Pipeline stopped."
            )
            logger.error(
                "Step failed: %s | Duration: %.2f seconds | Exit code: %s",
                step_name,
                elapsed,
                return_code,
            )
            raise RuntimeError(message)

        logger.info(
            "Step completed successfully: %s | Duration: %.2f seconds",
            step_name,
            elapsed,
        )
        return elapsed

    raise RuntimeError(f"Step failed unexpectedly: {step_name}")


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
    """Run the pipeline and write a structured report for the run."""
    logger = configure_logging()
    args = parse_args()

    run_id = str(uuid.uuid4())
    started_at = utc_now()
    pipeline_start = time.perf_counter()

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

    step_results: list[dict[str, str | float | None]] = []
    failure_details: dict[str, str] | None = None

    print("UBERPULSE - LOCAL DATA PIPELINE", flush=True)
    print(f"Run ID: {run_id}", flush=True)
    print(f"Project root: {PROJECT_ROOT}", flush=True)
    print(f"Input file: {input_file}", flush=True)
    print(f"Log file: {LOG_FILE}", flush=True)
    print(f"Run report: {REPORT_FILE}", flush=True)

    logger.info("=" * 60)
    logger.info("UberPulse pipeline run started | Run ID: %s", run_id)
    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("Input file: %s", input_file)
    logger.info("Log file: %s", LOG_FILE)
    logger.info("Run report: %s", REPORT_FILE)

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
            except Exception as error:
                elapsed = time.perf_counter() - step_start
                step_results.append(
                    {
                        "step": step_name,
                        "status": "FAILED",
                        "duration_seconds": round(elapsed, 3),
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    }
                )
                raise

            step_results.append(
                {
                    "step": step_name,
                    "status": "SUCCESS",
                    "duration_seconds": round(elapsed, 3),
                }
            )

    except Exception as error:
        failure_details = {
            "step": (
                str(step_results[-1]["step"])
                if step_results and step_results[-1]["status"] == "FAILED"
                else "Pipeline"
            ),
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        logger.exception("Pipeline stopped because a step failed")
        print(f"\nPIPELINE FAILED: {error}", flush=True)

    total_elapsed = time.perf_counter() - pipeline_start
    success = failure_details is None
    ended_at = utc_now()

    print_pipeline_summary(
        step_results,
        total_elapsed,
        success,
    )

    report = {
        "run_id": run_id,
        "status": "SUCCESS" if success else "FAILED",
        "started_at_utc": started_at,
        "ended_at_utc": ended_at,
        "duration_seconds": round(total_elapsed, 3),
        "input_file": str(input_file),
        "log_file": str(LOG_FILE),
        "steps": step_results,
        "failure": failure_details,
    }

    try:
        write_run_report(report)
        logger.info(
            "Structured run report written: %s",
            REPORT_FILE,
        )
    except OSError:
        logger.exception("Could not write the pipeline run report")
        print(
            f"WARNING: Could not write run report: {REPORT_FILE}",
            flush=True,
        )
        if success:
            sys.exit(1)

    if not success:
        logger.error("Run %s ended with failure", run_id)
        sys.exit(1)

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