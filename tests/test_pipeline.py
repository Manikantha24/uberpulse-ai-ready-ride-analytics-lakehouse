
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SCRIPT = (
    PROJECT_ROOT / "src" / "pipeline" / "run_pipeline.py"
)


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


def test_pipeline_runner_completes_successfully():
    result = subprocess.run(
        [sys.executable, str(PIPELINE_SCRIPT)],
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


def test_pipeline_failure_stops_execution(monkeypatch):
    from src.pipeline import run_pipeline

    executed_steps = []

    def fake_run_step(step_name, script_path):
        executed_steps.append(step_name)

        if len(executed_steps) == 2:
            raise RuntimeError("Simulated validation failure")

    monkeypatch.setattr(run_pipeline, "run_step", fake_run_step)

    with pytest.raises(SystemExit) as error:
        run_pipeline.main()

    assert error.value.code == 1

    assert executed_steps == [
        "1. Raw data inspection",
        "2. Data validation",
    ]