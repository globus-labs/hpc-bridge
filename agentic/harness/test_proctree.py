"""Hermetic: the operator's process tree is stopped — and gone — before the harness tears down (proctree, run.py's exit
path, the CLI operators' turns). A process that would "re-provision" after the teardown stands in for the operator."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import proctree  # noqa: E402

# an "operator" that spawns an "MCP server"; the server writes a marker — the re-provisioning tool call — after a delay
_OPERATOR = (
    "import subprocess, sys, time\n"
    "server = subprocess.Popen([sys.executable, '-c', "
    "'import sys, time; time.sleep(float(sys.argv[2])); open(sys.argv[1], \"w\").write(\"reprovisioned\")', "
    "sys.argv[1], sys.argv[2]])\n"
    "open(sys.argv[3], 'w').write(str(server.pid))\n"
    "time.sleep(60)\n"
)


def _alive(pid: int) -> bool:
    try:
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    st = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False).stdout.strip()
    return bool(st) and not st.startswith("Z")


def _spawn_operator(tmp_path: Path, delay_s: float = 2.5) -> tuple[subprocess.Popen, int, Path]:
    marker, pidfile = tmp_path / "reprovisioned", tmp_path / "server.pid"
    op = subprocess.Popen([sys.executable, "-c", _OPERATOR, str(marker), str(delay_s), str(pidfile)])
    t0 = time.monotonic()
    while not (pidfile.exists() and pidfile.read_text().strip()) and time.monotonic() - t0 < 10:
        time.sleep(0.05)
    return op, int(pidfile.read_text()), marker


def test_stop_tree_stops_the_operator_and_its_server_and_waits(tmp_path):
    op, server, marker = _spawn_operator(tmp_path)
    try:
        assert {op.pid, server} <= set(proctree.descendants())
        found, left = proctree.stop_tree(grace_s=5, kill_s=2)
        assert {op.pid, server} <= set(found) and left == []
        assert not _alive(op.pid) and not _alive(server)          # gone when stop_tree returns, not "signalled"
        time.sleep(3)
        assert not marker.exists()                                # the server never got to re-provision
    finally:
        for p in (op.pid, server):
            try:
                os.kill(p, 9)
            except ProcessLookupError:
                pass
        op.wait()


def test_a_cancelled_cli_turn_takes_its_whole_process_group_down(tmp_path):
    # the CLI operators (Codex, Pi, Hermes-latest) ran in a worker thread: a cancelled turn kept going, and its MCP
    # server's next call re-provisioned the endpoint after the teardown. Now the turn is its own process group,
    # terminated and waited for on cancellation.
    import cli_runner

    pidfile = tmp_path / "grandchild.pid"
    argv = ["bash", "-c", f"sleep 60 & echo $! > {pidfile}; wait"]

    async def go():
        task = asyncio.create_task(cli_runner.run_turn_process(argv, cwd=str(tmp_path), env=dict(os.environ),
                                                               timeout_s=120))
        t0 = time.monotonic()
        while not (pidfile.exists() and pidfile.read_text().strip()) and time.monotonic() - t0 < 10:
            await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return "cancelled"
        return "finished"

    assert asyncio.run(go()) == "cancelled"
    grandchild = int(pidfile.read_text())
    t0 = time.monotonic()
    while _alive(grandchild) and time.monotonic() - t0 < 5:
        time.sleep(0.05)
    assert not _alive(grandchild)


def test_a_cli_turn_that_times_out_is_stopped_with_its_group(tmp_path):
    import cli_runner

    pidfile = tmp_path / "grandchild.pid"
    argv = ["bash", "-c", f"sleep 60 & echo $! > {pidfile}; wait"]
    rc, _out, err = asyncio.run(cli_runner.run_turn_process(argv, cwd=str(tmp_path), env=dict(os.environ), timeout_s=1))
    assert rc == 124 and "timed out" in err
    t0 = time.monotonic()
    while _alive(int(pidfile.read_text())) and time.monotonic() - t0 < 5:
        time.sleep(0.05)
    assert not _alive(int(pidfile.read_text()))


def test_a_timed_out_cli_turn_keeps_what_it_printed(tmp_path):
    # Pi's calls live only in its stdout events: a turn cut off at the deadline used to lose them all, so a run that had
    # acted read as one with no tool call (review of the INFRA verdict, F4)
    import cli_runner

    argv = ["bash", "-c", 'echo \'{"type": "tool_execution_start", "toolName": "bash"}\'; echo warn >&2; sleep 60']
    rc, out, err = asyncio.run(cli_runner.run_turn_process(argv, cwd=str(tmp_path), env=dict(os.environ), timeout_s=1))
    assert rc == 124 and '"tool_execution_start"' in out
    assert err.splitlines() == ["warn", "timed out after 1 s"]


def test_kill_group_takes_eperm_from_an_all_exited_group_as_gone(monkeypatch):
    # macOS answers the liveness probe with EPERM once every member of the group has exited but is not reaped yet: that
    # escaped kill_group and failed a timed-out turn (about 1 run in 25 of the test above, 2026-10-16)
    sent = []

    def killpg(pid, sig):
        sent.append(sig)
        if sig == 0:
            raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(proctree.os, "killpg", killpg)
    proctree.kill_group(12345, grace_s=5)
    assert sent == [proctree.signal.SIGTERM, 0, proctree.signal.SIGKILL]
