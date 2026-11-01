"""Exercise the real entrypoint with a disposable application process, not a DB."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

import pytest


ENTRYPOINT = Path(__file__).resolve().parents[1] / "run.sh"
pytestmark = pytest.mark.skipif(
    os.name != "posix" or shutil.which("bash") is None,
    reason="The container entrypoint requires POSIX signals and bash",
)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_entrypoint_application_owns_pid_and_receives_shutdown(tmp_path, stop_signal):
    bindir = tmp_path / "bin"
    bindir.mkdir(mode=0o700)
    events = tmp_path / "events.jsonl"
    python = bindir / "python"
    python.write_text(f"#!{sys.executable}\n" + r'''
import json, os, signal, sys, time

def record(event, **fields):
    with open(os.environ["ENTRYPOINT_TEST_EVENTS"], "a") as stream:
        stream.write(json.dumps({"event": event, "pid": os.getpid(), **fields}) + "\n")

if len(sys.argv) == 3 and sys.argv[1] == "-c":
    # Do not execute schema initialization or import application/database code.
    record("schema")
    raise SystemExit(0)
if sys.argv[1:] != ["main.py"]:
    raise SystemExit(91)

received = None
def stop(number, frame):
    global received
    received = number
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
try:
    record("ready")
    while True:
        time.sleep(60)
finally:
    record("cleanup", signal=received)
''')
    python.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": str(bindir) + os.pathsep + os.environ.get("PATH", ""),
        "ENTRYPOINT_TEST_EVENTS": str(events),
    }
    child = subprocess.Popen(
        ["bash", str(ENTRYPOINT)], cwd=tmp_path, env=environment,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        recorded = []
        while time.monotonic() < deadline:
            if events.exists():
                # Ignore a partially written final line until its newline arrives.
                data = events.read_text()
                recorded = [json.loads(line) for line in data.splitlines(keepends=True)
                            if line.endswith("\n")]
            if any(event["event"] == "ready" for event in recorded):
                break
            assert child.poll() is None, "Entrypoint exited before the application was ready"
            time.sleep(0.01)
        assert [event["event"] for event in recorded] == ["schema", "ready"]
        assert recorded[-1]["pid"] == child.pid, "Application must replace the entrypoint shell"

        # Docker signals the entrypoint PID, not its entire process group.
        os.kill(child.pid, stop_signal)
        assert child.wait(timeout=5) == 0
        recorded = [json.loads(line) for line in events.read_text().splitlines()]
        assert [event["event"] for event in recorded] == ["schema", "ready", "cleanup"]
        assert recorded[-1] == {"event": "cleanup", "pid": child.pid, "signal": int(stop_signal)}
    finally:
        # Terminate descendants if a regression leaves bash waiting on a child.
        # Signal the private session even when its original leader has exited.
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=5)
