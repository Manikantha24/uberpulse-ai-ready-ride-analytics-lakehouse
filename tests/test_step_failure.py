
import logging
from io import StringIO

import pytest

from src.pipeline import run_pipeline


def test_run_step_raises_when_child_process_exits_nonzero(
    monkeypatch,
    tmp_path,
    caplog,
):
    script = tmp_path / "fake_script.py"
    script.write_text("raise SystemExit(7)", encoding="utf-8")

    attempts = []

    class FakeProcess:
        def __init__(self):
            self.stdout = StringIO("Simulated child failure\n")

        def wait(self):
            return 7

        def poll(self):
            return 7

    def fake_popen(*args, **kwargs):
        attempts.append(1)
        return FakeProcess()

    monkeypatch.setattr(run_pipeline.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run_pipeline, "log_child_output", lambda *_: None)

    with caplog.at_level(logging.INFO, logger="uberpulse"):
        with pytest.raises(RuntimeError, match="failed with exit code 7"):
            run_pipeline.run_step(
                "Child failure test",
                script,
                max_attempts=3,
                retry_delay_seconds=0,
            )

    assert len(attempts) == 1
    assert "Step failed" in caplog.text
    assert "exit code: 7" in caplog.text.lower()
