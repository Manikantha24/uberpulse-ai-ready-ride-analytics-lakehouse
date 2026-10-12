
import subprocess
import sys
from pathlib import Path
from io import StringIO

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SCRIPT = PROJECT_ROOT / "src" / "pipeline" / "run_pipeline.py"


@pytest.fixture
def sample_input_file(tmp_path):
    """Create a small synthetic dataset for pipeline integration tests."""
    sample_data = pd.DataFrame(
        {
            "VendorID": [1, 2, 1],
            "tpep_pickup_datetime": pd.to_datetime(
                [
                    "2025-01-01 08:00:00",
                    "2025-01-01 09:00:00",
                    "2025-01-04 12:00:00",
                ]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                [
                    "2025-01-01 08:20:00",
                    "2025-01-01 09:30:00",
                    "2025-01-04 12:15:00",
                ]
            ),
            "passenger_count": [1, 2, 1],
            "trip_distance": [2.5, 4.0, 0.0],
            "RatecodeID": [1, 1, 1],
            "store_and_fwd_flag": ["N", "N", "N"],
            "PULocationID": [100, 101, 102],
            "DOLocationID": [200, 201, 202],
            "payment_type": [1, 2, 1],
            "fare_amount": [12.0, 18.0, 0.0],
            "extra": [1.0, 0.0, 0.0],
            "mta_tax": [0.5, 0.5, 0.0],
            "tip_amount": [2.0, 0.0, 0.0],
            "tolls_amount": [0.0, 0.0, 0.0],
            "improvement_surcharge": [1.0, 1.0, 0.0],
            "total_amount": [16.5, 19.5, 0.0],
            "congestion_surcharge": [2.5, 0.0, 0.0],
            "Airport_fee": [0.0, 0.0, 0.0],
            "cbd_congestion_fee": [0.0, 0.0, 0.0],
        }
    )

    input_file = tmp_path / "sample_trips.parquet"
    sample_data.to_parquet(input_file, index=False)
    return input_file


def test_pipeline_script_exists():
    assert PIPELINE_SCRIPT.exists()


def test_pipeline_steps_are_defined_in_correct_order():
    from src.pipeline.run_pipeline import PIPELINE_STEPS

    step_names = [step_name for step_name, _ in PIPELINE_STEPS]

    assert step_names == [
        "1. Raw data inspection",
        "2. Data validation",
        "3. Analytics transformation",
    ]


def test_pipeline_runner_completes_successfully(sample_input_file, tmp_path):
    valid_output = tmp_path / "valid.parquet"
    quarantine_output = tmp_path / "quarantine.parquet"
    analytics_output = tmp_path / "analytics.parquet"

    result = subprocess.run(
        [
            sys.executable,
            str(PIPELINE_SCRIPT),
            "--input",
            str(sample_input_file),
            "--valid-output",
            str(valid_output),
            "--quarantine-output",
            str(quarantine_output),
            "--analytics-output",
            str(analytics_output),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )

    assert result.returncode == 0, (
        f"Pipeline failed.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    assert "PIPELINE COMPLETED SUCCESSFULLY" in result.stdout
    assert valid_output.exists()
    assert quarantine_output.exists()
    assert analytics_output.exists()

    valid_df = pd.read_parquet(valid_output)
    quarantine_df = pd.read_parquet(quarantine_output)
    analytics_df = pd.read_parquet(analytics_output)

    assert len(valid_df) + len(quarantine_df) == 3
    assert len(analytics_df) == len(valid_df)


def test_run_step_retries_after_process_launch_error(monkeypatch, tmp_path):
    from src.pipeline import run_pipeline

    script = tmp_path / "fake_script.py"
    script.write_text("print('success')", encoding="utf-8")

    attempts = []

    class FakeProcess:
        def __init__(self):
            self.stdout = StringIO("success\n")

        def wait(self):
            return 0

        def poll(self):
            return 0

    def fake_popen(*args, **kwargs):
        attempts.append(1)

        if len(attempts) == 1:
            raise OSError("Simulated temporary process-launch failure")

        return FakeProcess()

    monkeypatch.setattr(run_pipeline.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run_pipeline.time, "sleep", lambda _: None)
    monkeypatch.setattr(run_pipeline, "log_child_output", lambda *_: None)

    elapsed = run_pipeline.run_step(
        "Retry test",
        script,
        max_attempts=3,
        retry_delay_seconds=0,
    )

    assert elapsed >= 0
    assert len(attempts) == 2


def test_run_step_stops_after_retry_attempts_are_exhausted(
    monkeypatch,
    tmp_path,
):
    from src.pipeline import run_pipeline

    script = tmp_path / "fake_script.py"
    script.write_text("print('unused')", encoding="utf-8")

    attempts = []

    def fake_popen(*args, **kwargs):
        attempts.append(1)
        raise OSError("Simulated process-launch failure")

    monkeypatch.setattr(run_pipeline.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run_pipeline.time, "sleep", lambda _: None)

    with pytest.raises(OSError, match="Simulated process-launch failure"):
        run_pipeline.run_step(
            "Retry exhaustion test",
            script,
            max_attempts=3,
            retry_delay_seconds=0,
        )

    assert len(attempts) == 3


def test_run_step_rejects_invalid_retry_configuration(tmp_path):
    from src.pipeline import run_pipeline

    script = tmp_path / "fake_script.py"
    script.write_text("print('unused')", encoding="utf-8")

    with pytest.raises(ValueError, match="max_attempts"):
        run_pipeline.run_step(
            "Invalid retry test",
            script,
            max_attempts=0,
        )

    with pytest.raises(ValueError, match="retry_delay_seconds"):
        run_pipeline.run_step(
            "Invalid delay test",
            script,
            retry_delay_seconds=-1,
        )


def test_pipeline_failure_stops_execution(monkeypatch, tmp_path):
    from src.pipeline import run_pipeline

    executed_steps = []

    def fake_run_step(step_name, script_path, extra_args=None):
        executed_steps.append(step_name)

        if len(executed_steps) == 2:
            raise RuntimeError("Simulated validation failure")

        return 0.1

    monkeypatch.setattr(run_pipeline, "run_step", fake_run_step)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(PIPELINE_SCRIPT),
            "--input",
            str(tmp_path / "input.parquet"),
            "--valid-output",
            str(tmp_path / "valid.parquet"),
            "--quarantine-output",
            str(tmp_path / "quarantine.parquet"),
            "--analytics-output",
            str(tmp_path / "analytics.parquet"),
        ],
    )

    with pytest.raises(SystemExit) as error:
        run_pipeline.main()

    assert error.value.code == 1
    assert executed_steps == [
        "1. Raw data inspection",
        "2. Data validation",
    ]