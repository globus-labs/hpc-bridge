"""A stop's evidence for a check the Globus Compute service still lists as queued, read on the login node.

Live 2026-10-16 (fake `site`: submit_policy_rejected, slurm_worker_died): globus-compute-endpoint 4.18 acks a task once
it hands it to the engine; at bad state its watcher stops the user endpoint ~0.3 s later and its result publisher stops
without blocking, so the failed check's result never reaches the service (`set_bad_state_and_fail_all` at 07:38:22, then
`ResultPublisher<✗; o:1; t:0> Connection closed`). Every stop then answered `draining` for good, and only
teardown_endpoint reached `down`. A stop now reads that check's OWN compute user endpoint — the one directory whose log
names the check's block request — and its manager's log, twice, and answers `down` only when that user endpoint
received the check (taken before its bad state, in a life that had subscribed), went into bad state, logged its
shutdown, left no process, and its manager logged its stop with no revival or restart after it. Anything less keeps
`draining`.

The logs under tests/uep_log_fixtures/ are those bundles' logs, verbatim: `rejected.*` (submit_policy_rejected
1792136208), `worker_died.*` (slurm_worker_died 1792140090) — both from before the template named the block request, so
the tests insert the `endpoint_setup` lines globus-compute-endpoint 4.18 writes at a user endpoint's start
(endpoint.py:513, :920) — and `live_*` (slurm_worker_died 1792168070, this branch's template, settled live by this
evidence), whose block-request line is the real one. Only the timestamps are shifted, all of a cell's logs together.
"""
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple

import pytest

from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, ShapeRuntime, _ensure_endpoint_up
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _PbsFacility, _PendingFuture, _Res, _settle_clock, _skew

_FIXTURES = Path(__file__).parent / "uep_log_fixtures"
_STAMP = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{6})")

needs_posix_shell = pytest.mark.skipif(sys.platform == "win32", reason="the login node's shell: sh, awk, ps, find, date")


class Cell(NamedTuple):
    """One live cell's logs: its endpoint, the user endpoint that held our check, and the files."""

    eid: str
    uuid: str
    manager: str                 # the manager's own endpoint.log
    compute: str                 # that user endpoint's endpoint.log
    real_block: int | None       # the block-request number its log carries (None: insert the lines)
    login: str | None = None     # the login shape's own user endpoint (a long-lived one, no block request)
    login_uuid: str | None = None


REJECTED = Cell("7a7da153-4643-4b2f-b8a6-ceee6211af97", "a9d3d64e-394f-8ad8-b2fb-aec6c96b1d32",
                "rejected.manager.log", "rejected.endpoint.log", None,
                "login.endpoint.log", "59f3b039-27fb-a42d-6aac-0030bfb5cef3")
WORKER_DIED = Cell("28fae94b-1ea2-47fa-8d5b-40ab9c8c2139", "cfe285ef-7f41-943f-0e9f-a4ae1d904982",
                   "worker_died.manager.log", "worker_died.endpoint.log", None)
LIVE = Cell("08d247d2-9baa-4e31-a591-2690a0b1b966", "215aec48-a89c-29b6-8e58-fa16a3b3f018",
            "live_worker_died.manager.log", "live_worker_died.endpoint.log", 209994360702101,
            "live_login.endpoint.log", "f81aa051-0b5d-d98d-0584-71da4e888bf8")
CELLS = {"rejected": REJECTED, "worker_died": WORKER_DIED, "live": LIVE}


def _stamps(text: str) -> list[datetime]:
    return [datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f") for ln in text.splitlines() if (m := _STAMP.match(ln))]


def _restamp(text: str, shift: timedelta) -> str:
    def one(ln: str) -> str:
        m = _STAMP.match(ln)
        if m is None:
            return ln
        when = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f") + shift
        return when.strftime("%Y-%m-%d %H:%M:%S,%f") + ln[m.end():]
    return "\n".join(one(ln) for ln in text.splitlines()) + "\n"


def _named(text: str, block: int, real: int | None) -> str:
    """The compute user endpoint's log naming `block`: its real line renumbered, or the endpoint_setup lines a 4.18 user
    endpoint logs at its start (globus_compute_endpoint.endpoint.endpoint:_run_command, before the interchange)."""
    if real is not None:
        return text.replace(f"hpcb_block={real}\n", f"hpcb_block={block}\n")
    lines = text.splitlines()
    at = _STAMP.match(next(ln for ln in lines if _STAMP.match(ln))).group(1)
    pid = re.search(r"MainProcess-\d+ MainThread-\d+", text).group(0)
    lines[2:2] = [f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:920 _run_command running endpoint_setup "
                  f"command: true hpcb_block={block}",
                  f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:923 _run_command endpoint_setup stdout: b''",
                  f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:924 _run_command endpoint_setup stderr: b''"]
    return "\n".join(lines) + "\n"


def _put_cell(root: Path, cell: Cell, block: int | None, *, started: datetime, manager: str | None = None,
              compute: str | None = None) -> tuple[Path, Path, timedelta]:
    """Write a cell's logs under `root` (the login node's Globus Compute directory), every timestamp shifted so the
    compute user endpoint's first one is `started` (the login node's local time). `manager` / `compute`: replace a log's
    text (before the shift). Returns the manager's and the user endpoint's directories, and the shift."""
    raw = compute if compute is not None else (_FIXTURES / cell.compute).read_text()
    shift = started - _stamps(raw)[0]
    text = _named(raw, block, cell.real_block) if block is not None else raw
    ud = root / f"uep.{cell.eid}.{cell.uuid}"
    ud.mkdir(parents=True, exist_ok=True)
    (ud / "endpoint.log").write_text(_restamp(text, shift))
    md = root / "hpc-bridge-fake-cell"
    md.mkdir(parents=True, exist_ok=True)
    (md / "endpoint.log").write_text(_restamp(manager if manager is not None else
                                              (_FIXTURES / cell.manager).read_text(), shift))
    if cell.login is not None:
        ld = root / f"uep.{cell.eid}.{cell.login_uuid}"
        ld.mkdir(parents=True, exist_ok=True)
        (ld / "endpoint.log").write_text(_restamp((_FIXTURES / cell.login).read_text(), shift))
    return md, ud, shift


def _run(cmd: str, root: Path, cwd: Path | None = None, env: dict | None = None) -> str:
    """The evidence command as the login shape runs it: a POSIX shell, here, against `root` as the Globus Compute
    directory (never the real one: GLOBUS_COMPUTE_USER_DIR is set, so $HOME is not read)."""
    full = {**os.environ, "GLOBUS_COMPUTE_USER_DIR": str(root), **(env or {})}
    res = subprocess.run(["sh", "-c", cmd], env=full, capture_output=True, text=True, timeout=60, cwd=cwd or root)
    assert res.returncode == 0, res.stderr
    return res.stdout


@pytest.fixture(autouse=True)
def _short_gap(monkeypatch, request):
    """The two samples, 0.3 s apart here (3 s in production); a test marked `real_gap` keeps the real one."""
    if not request.node.get_closest_marker("real_gap"):
        from hpc_bridge import scheduler_ops

        monkeypatch.setattr(scheduler_ops, "_EVIDENCE_GAP_S", 0.3, raising=False)


def _verdict(out: str, cell: Cell, block: int, age: float):
    from hpc_bridge import scheduler_ops

    return scheduler_ops._read_uep_evidence(out, cell.eid, [("k", block, age)])["k"]


# --- the command, run for real on the cells' logs ---------------------------------------------------------------------

@needs_posix_shell
@pytest.mark.parametrize("name", ["rejected", "worker_died", "live"])
def test_the_command_finds_the_checks_user_endpoint_stopped_for_good(tmp_path, name):
    from hpc_bridge import scheduler_ops

    cell = CELLS[name]
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    _put_cell(tmp_path, cell, 207612534, started=started)
    out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [207612534]), tmp_path)
    assert f"HPCB_PS 1 {cell.uuid} 0" in out and f"HPCB_PID 2 {cell.uuid} none" in out
    assert re.search(rf"HPCB_MGR 1 {cell.uuid} \d+ \d+ \d+ \d+ \d+", out)
    assert cell.login_uuid is None or cell.login_uuid not in out           # the login shape's own: never matched
    taken = (datetime.now() - started).total_seconds() + 1        # taken a second before its user endpoint started
    ev = _verdict(out, cell, 207612534, taken)
    assert ev.lost, ev
    assert ev.uep == f"uep.{cell.eid}.{cell.uuid}"
    bad = datetime.strptime(ev.bad_at, "%Y-%m-%d %H:%M:%S")       # the bundle's own line, as far after the start as live
    assert timedelta(seconds=5) < bad - started < timedelta(seconds=70)
    # taken AFTER its user endpoint's bad state: it may have arrived as that endpoint quiesced (never acked, back on the
    # queue for a later life) — not this check's evidence
    late = (datetime.now() - bad).total_seconds() - 1
    assert "before the check had reached it" in _verdict(out, cell, 207612534, late).why
    # too recent: the start request sent with it may not have reached the manager yet (its 30 s grace for a cached one)
    assert "too recent" in _verdict(out, cell, 207612534, 20.0).why


@needs_posix_shell
@pytest.mark.real_gap
def test_a_revival_between_the_two_samples_is_no_evidence(tmp_path):
    # the reviewers' race: the old life has exited (its log ends in shutdown, its pid file is gone); the manager reaps it
    # within ~1 s and revives it from a start request it cached — the new child names the directory only after its fork
    # and render. Sample 1 sees nothing alive; the real 3 s gap later, sample 2 sees the revival.
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    md, _ud, _shift = _put_cell(tmp_path, cell, 42, started=started)
    title = f"Endpoint starting up for u [globus-compute-endpoint _start-user-endpoint uep.{cell.eid}.{cell.uuid}]"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")
    revive = (f"{now} INFO MainProcess-239 MainThread-1 globus_compute_endpoint.endpoint.core_endpoint:350 _revive_child "
              f"User EP stopped within grace period; using cached arguments to start a new instance (name: "
              f"uep.{cell.eid}.{cell.uuid})\n{now} INFO MainProcess-239 MainThread-1 globus_compute_endpoint.endpoint"
              f".core_endpoint:1079 cmd_start_endpoint Creating new user endpoint (pid: 4242) [(u, uep.{cell.eid}."
              f"{cell.uuid}) globus-compute-endpoint _start-user-endpoint uep.{cell.eid}.{cell.uuid}]\n")
    # (what names the directory travels in the environment, so the helper names it only once it has exec'd)
    helper = ("import os, sys, time\ntime.sleep(1.0)\nopen(os.environ['LOG'], 'a').write(os.environ['REV'])\n"
              "os.execv(sys.executable, [sys.executable, '-c', 'import time; time.sleep(20)', os.environ['T']])\n")
    proc = subprocess.Popen([sys.executable, "-c", helper],
                            env={**os.environ, "LOG": str(md / "endpoint.log"), "REV": revive, "T": title})
    try:
        out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path)
    finally:
        proc.kill()
        proc.wait()
    assert f"HPCB_PS 1 {cell.uuid} 0" in out and f"HPCB_PS 2 {cell.uuid} 1" in out   # it really was quiet at first
    ev = _verdict(out, cell, 42, (datetime.now() - started).total_seconds() + 1)
    assert not ev.lost and "changed while hpc-bridge looked" in ev.why


@needs_posix_shell
def test_the_managers_own_log_must_show_the_stop_and_nothing_after_it(tmp_path):
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    base = (_FIXTURES / cell.manager).read_text()
    name = f"uep.{cell.eid}.{cell.uuid}"
    [stop] = [ln for ln in base.splitlines() if "Command stopped normally" in ln and name in ln]
    stamp = stop[:26]

    def verdict(manager: str) -> str:
        _put_cell(tmp_path, cell, 9, started=started, manager=manager)
        ev = _verdict(_run(scheduler_ops._uep_evidence_cmd(cell.eid, [9]), tmp_path), cell, 9, taken)
        assert not ev.lost
        return ev.why

    # not reaped yet (the gap the manager's 1 s poll leaves): no stop logged for that life
    assert "does not show that user endpoint's stop" in verdict(base.replace(stop + "\n", ""))
    # revived from a cached start request (`… is already running … caching arguments`, then the exit within 30 s)
    revived = base.replace(stop + "\n", stop + "\n" + (
        f"{stamp} INFO MainProcess-239 MainThread-1 globus_compute_endpoint.endpoint.core_endpoint:350 _revive_child "
        f"User EP stopped within grace period; using cached arguments to start a new instance (name: {name})\n"))
    assert "started again" in verdict(revived)
    # started afresh for a start request that came after the stop
    again = base.replace(stop + "\n", stop + "\n" + (
        f"{stamp} INFO MainProcess-239 MainThread-1 globus_compute_endpoint.endpoint.core_endpoint:1079 "
        f"cmd_start_endpoint Creating new user endpoint (pid: 999) [(u, {name}) globus-compute-endpoint "
        f"_start-user-endpoint {name}]\n"))
    assert "started again" in verdict(again)
    # another process's stop than the one its log names
    assert "different processes" in verdict(base.replace(f"(886) [(hpcbridge-test-00, {name})",
                                                           f"(887) [(hpcbridge-test-00, {name})")
                                                  .replace(f"(pid: 886) [(hpcbridge-test-00, {name})",
                                                           f"(pid: 887) [(hpcbridge-test-00, {name})"))
    # no manager log naming this endpoint's start — or two of them
    assert "own log on the login node could not be found" in verdict(base.replace("Core Endpoint begins", "Core begins"))
    (tmp_path / "another-manager").mkdir()
    (tmp_path / "another-manager" / "endpoint.log").write_text(base)
    assert "own log on the login node could not be found" in verdict(base)


@needs_posix_shell
def test_the_command_sees_a_process_of_that_user_endpoint_and_a_pid_file_still_refreshed(tmp_path):
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    _md, ud, _ = _put_cell(tmp_path, cell, 5, started=started)
    taken = (datetime.now() - started).total_seconds() + 1
    cmd = scheduler_ops._uep_evidence_cmd(cell.eid, [5])
    # a process whose arguments name the user endpoint's directory (its title does: "Globus Compute Endpoint (<id>,
    # uep.<eid>.<id>) [...]"): still alive, whatever the logs say
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", f"uep.{cell.eid}.{cell.uuid}"])
    try:
        out = _run(cmd, tmp_path)
    finally:
        proc.kill()
        proc.wait()
    assert "still running" in _verdict(out, cell, 5, taken).why
    (ud / "daemon.pid").write_text("4242\n")          # refreshed every 15 s while it lives, from any login node
    out = _run(cmd, tmp_path)
    assert f"HPCB_PS 1 {cell.uuid} 0" in out and "pid file is still being refreshed" in _verdict(out, cell, 5, taken).why
    old = time.time() - 600
    os.utime(ud / "daemon.pid", (old, old))          # a force-killed one's leftover: stale
    out = _run(cmd, tmp_path)
    assert f"HPCB_PID 1 {cell.uuid} old" in out and _verdict(out, cell, 5, taken).lost


@needs_posix_shell
def test_the_command_reads_no_evidence_from_a_log_that_is_not_settled(tmp_path):
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    shift = started - _stamps((_FIXTURES / cell.compute).read_text())[0]
    full = _restamp(_named((_FIXTURES / cell.compute).read_text(), 7, None), shift).splitlines()

    def verdict(lines: list[str]) -> str:
        _put_cell(tmp_path, cell, None, started=started, compute="\n".join(lines) + "\n")
        ev = _verdict(_run(scheduler_ops._uep_evidence_cmd(cell.eid, [7]), tmp_path), cell, 7, taken)
        assert not ev.lost
        return ev.why

    assert "no bad state" in verdict([ln for ln in full if "Setting bad state" not in ln])
    assert "no bad state" in verdict([ln for ln in full if "Awaiting messages from queue" not in ln])  # never subscribed
    assert "not logged its shutdown" in verdict([ln for ln in full if not ln.startswith("---------- Endpoint shutdown")])
    # started again for the same block request after that shutdown: the new life has no bad state, or is starting
    again = _restamp(_named((_FIXTURES / cell.compute).read_text(), 7, None), shift + timedelta(seconds=40)).splitlines()
    assert "no bad state since it last started" in verdict(full + again[:8])
    assert "starting again" in verdict(full + again[:3])            # its endpoint_setup ran, its interchange has not
    # no user endpoint names the block request (not started yet) — or names another one (a prefix of ours is not ours)
    assert "names the check's block request yet" in verdict([ln for ln in full if "hpcb_block=" not in ln])
    assert "names the check's block request yet" in verdict([ln.replace("hpcb_block=7", "hpcb_block=77") for ln in full])


@needs_posix_shell
def test_a_bad_state_in_an_hour_a_clock_change_repeats_is_no_evidence(tmp_path):
    # the log has no zone offset: its local time is turned into epoch seconds on the login node, in its own zone — and
    # the hour a fall-back repeats cannot be placed
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    tz = {"TZ": "America/Chicago"}
    bad = next(ln for ln in (_FIXTURES / cell.compute).read_text().splitlines() if "Setting bad state" in ln)
    to_bad = _stamps(bad)[0] - _stamps((_FIXTURES / cell.compute).read_text())[0]
    started = datetime(2026, 11, 1, 1, 30, 0) - to_bad                    # its bad state at 01:30, the repeated hour
    _put_cell(tmp_path, cell, 3, started=started)
    out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [3]), tmp_path, env=tz)
    row = next(ln.split() for ln in out.splitlines() if ln.startswith("HPCB_UEP 1 "))
    assert row[12:14] == ["2026-11-01", "01:30:00"] and row[15] == "1"
    assert "cannot place" in _verdict(out, cell, 3, 10_000).why
    # an ordinary hour in that zone is placed (and is then judged on the clock as usual)
    _put_cell(tmp_path, cell, 3, started=datetime(2026, 10, 15, 12, 0, 0))
    out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [3]), tmp_path, env=tz)
    assert next(ln.split() for ln in out.splitlines() if ln.startswith("HPCB_UEP 1 "))[15] == "0"


@needs_posix_shell
def test_an_unreadable_log_voids_only_the_check_it_could_belong_to(tmp_path):
    from hpc_bridge import scheduler_ops

    if os.geteuid() == 0:
        pytest.skip("root reads a 0000 file")
    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    _put_cell(tmp_path, cell, 8, started=started)
    other = tmp_path / f"uep.{cell.eid}.0b1c2d3e-0000-4000-8000-000000000001"
    other.mkdir()
    (other / "endpoint.log").write_text("x\n")
    (other / "endpoint.log").chmod(0)
    try:
        out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [8, 9]), tmp_path)
    finally:
        (other / "endpoint.log").chmod(0o600)
    assert "HPCB_FAIL 1 read 0b1c2d3e-0000-4000-8000-000000000001" in out
    assert _verdict(out, cell, 8, taken).lost                                   # its own log was read: settled
    assert "could not be read" in _verdict(out, cell, 9, taken).why             # nothing read names 9: it may be there


@needs_posix_shell
def test_two_user_endpoints_naming_one_block_request_are_no_evidence(tmp_path):
    from hpc_bridge import scheduler_ops

    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    _put_cell(tmp_path, REJECTED, 8, started=started)
    twin = tmp_path / f"uep.{REJECTED.eid}.{WORKER_DIED.uuid}"
    twin.mkdir()
    (twin / "endpoint.log").write_text((tmp_path / f"uep.{REJECTED.eid}.{REJECTED.uuid}" / "endpoint.log").read_text())
    out = _run(scheduler_ops._uep_evidence_cmd(REJECTED.eid, [8]), tmp_path)
    assert "more than one user endpoint" in _verdict(out, REJECTED, 8, taken).why


@needs_posix_shell
def test_the_command_survives_the_real_dispatch_path(tmp_path):
    # as the login shape runs it: the session wrapper (base64, eval in bash), then the ShellFunction's own brace escape
    # and `.format` — every awk program and `${…}` must come out whole
    from hpc_bridge import scheduler_ops, session_shell
    from hpc_bridge.runner import _escape_for_shellfunction

    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    _put_cell(tmp_path / "gc", LIVE, 11, started=started)
    wrapped = session_shell.wrap(scheduler_ops._uep_evidence_cmd(LIVE.eid, [11]),
                                 session_shell.Session("_hpc-bridge-internal", str(tmp_path / "state")))
    script = _escape_for_shellfunction(wrapped).format()
    env = {**os.environ, "GLOBUS_COMPUTE_USER_DIR": str(tmp_path / "gc")}
    res = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert res.returncode == 0, res.stderr
    assert _verdict(res.stdout, LIVE, 11, (datetime.now() - started).total_seconds() + 1).lost


# --- what the command is built from ----------------------------------------------------------------------------------

_EID = REJECTED.eid


@pytest.mark.parametrize("eid", ["fake-eid", "", f"{_EID}; rm -rf ~", f"$(touch pwned){_EID[11:]}", _EID + "\n",
                                 "../../../etc/passwd" + _EID[20:], f"{_EID}*", " " + _EID])
def test_the_command_is_never_built_from_an_endpoint_id_that_is_not_a_uuid(eid):
    from hpc_bridge import scheduler_ops

    with pytest.raises(ValueError):
        scheduler_ops._uep_evidence_cmd(eid, [1])


@pytest.mark.parametrize("blocks", [[], ["1"], [True], [0], [-3], [2 ** 48], [1.5], ["1; rm -rf ~"], [1, "2"]])
def test_the_command_is_never_built_from_a_block_request_that_is_not_one_of_ours(blocks):
    from hpc_bridge import scheduler_ops

    with pytest.raises(ValueError):
        scheduler_ops._uep_evidence_cmd(_EID, blocks)


@pytest.mark.parametrize("gap", [0, -1, 11, "3; rm -rf ~", float("nan")])
def test_the_command_is_never_built_with_an_odd_gap(gap):
    from hpc_bridge import scheduler_ops

    with pytest.raises(ValueError):
        scheduler_ops._uep_evidence_cmd(_EID, [1], gap_s=gap)


@needs_posix_shell
@pytest.mark.parametrize("name", ['x"; touch pwned; "', "$(touch pwned)", "`touch pwned`", "a b", "*",
                                  "11111111-2222-3333-4444-55555555555$(touch pwned)"])
def test_a_hostile_directory_name_on_the_login_node_is_skipped_never_run(tmp_path, name):
    from hpc_bridge import scheduler_ops

    gc = tmp_path / "gc"
    _put_cell(gc, REJECTED, 3, started=datetime.now() - timedelta(minutes=5))
    hostile = gc / f"uep.{REJECTED.eid}.{name}"
    hostile.mkdir()
    (hostile / "endpoint.log").write_text((gc / f"uep.{REJECTED.eid}.{REJECTED.uuid}" / "endpoint.log").read_text())
    out = _run(scheduler_ops._uep_evidence_cmd(REJECTED.eid, [3]), gc, cwd=tmp_path)
    assert [ln.split()[3] for ln in out.splitlines() if ln.startswith("HPCB_UEP ")] == [REJECTED.uuid] * 2
    assert not (tmp_path / "pwned").exists() and not (gc / "pwned").exists()


def _rows(*, s2: dict | None = None, now: str = "1792136400 000000000", uep: str | None = None,
          mgr: str = "8 886 20 886 20") -> str:
    """Both samples of a settled user endpoint (rejected cell, block request 5), as the command prints them."""
    u = REJECTED.uuid
    one = {"now": now, "uep": uep or f"5 {u} 1 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661",
           "ps": f"{u} 0", "pid": f"{u} none", "mgr": f"{u} {mgr}"}
    two = {**one, **(s2 or {})}
    out = ""
    for k, s in (("1", one), ("2", two)):
        out += (f"HPCB_NOW {k} {s['now']}\nHPCB_UEP {k} {s['uep']}\nHPCB_PS {k} {s['ps']}\nHPCB_PID {k} {s['pid']}\n"
                f"HPCB_MGR {k} {s['mgr']}\n")
    return out


def test_the_reader_takes_only_rows_of_the_commands_shape():
    from hpc_bridge import scheduler_ops

    u, d = REJECTED.uuid, WORKER_DIED.uuid
    sent = 1792136400 - 1792136290        # taken 12 s before the bad state, 110 s before the look
    read = lambda text, age=sent: scheduler_ops._read_uep_evidence(text, _EID, [("k", 5, age)])["k"]  # noqa: E731
    assert read(_rows()).lost
    good = _rows()
    for text, why in [
        (_rows(s2={"ps": f"{u} 1"}), "changed while"),
        (_rows(s2={"uep": f"5 {u} 1 6 3 13 23 50 54 886 2026-10-16 07:38:22 1792136302 0 455661"}), "changed while"),
        (_rows(s2={"mgr": f"{u} 8 886 20 886 21"}), "changed while"),
        (good.replace(f"HPCB_PS 1 {u} 0", f"HPCB_PS 1 {u} 2").replace(f"HPCB_PS 2 {u} 0", f"HPCB_PS 2 {u} 2"),
         "still running"),
        (good + "HPCB_FAIL 2 ps\n", "process list"),
        (good + "HPCB_FAIL 1 manager 0\n", "own log on the login node"),
        (good.replace(f"HPCB_PID 1 {u} none", f"HPCB_PID 1 {u} unknown").replace(f"HPCB_PID 2 {u} none",
                                                                                f"HPCB_PID 2 {u} unknown"), "pid file"),
        (good.replace("HPCB_NOW 2", "HPCB_NOPE 2"), "clock"),
        (good + f"HPCB_UEP 1 5 {d} 1 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661\n", "more than one"),
        (good + f"HPCB_UEP 1 6 {u} 1 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661\n", "more than one"),
        (_rows(uep=f"5 {u} 1 6 3 13 23 0 53 886 2026-10-16 07:38:22 1792136302 0 455661"), "shutdown"),
        (_rows(uep=f"5 {u} 1 6 3 13 0 0 53 886 - - - 1 -"), "no bad state"),
        (_rows(uep=f"5 {u} 1 6 3 0 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661"), "no bad state"),
        (_rows(uep=f"5 {u} 0 0 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661"), "no start"),
        (_rows(uep=f"5 {u} 1 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 1 455661"), "cannot place"),
        (_rows(uep=f"5 {u} 1 6 3 13 23 50 53 887 2026-10-16 07:38:22 1792136302 0 455661"), "different processes"),
        (_rows(mgr="8 886 20 886 24"), "started again"),
        (_rows(mgr="8 886 0 - 8"), "does not show"),
        (_rows(now="1792136200 000000000"), "ahead of the login node's clock"),
        # an unreadable log beside the match: settled only if that user endpoint is provably stopped too
        (good + "".join(f"HPCB_FAIL {k} read {d}\nHPCB_PS {k} {d} 0\nHPCB_PID {k} {d} none\nHPCB_MGR {k} {d} 0 - 0 - 0\n"
                        for k in "12"), None),
        (good + "".join(f"HPCB_FAIL {k} read {d}\nHPCB_PS {k} {d} 1\nHPCB_PID {k} {d} none\nHPCB_MGR {k} {d} 0 - 0 - 0\n"
                        for k in "12"), "whose log could not be read"),
        (good + f"HPCB_FAIL 1 read {d}\n", "whose log could not be read"),     # and nothing known of it
        (good + "HPCB_FAIL 1 grep\n", "could not be read"),
    ]:
        ev = read(text)
        assert (ev.lost if why is None else not ev.lost and why in ev.why), (text, ev)
    assert "before the check had reached it" in read(good, 1792136400 - 1792136302 + 0.5).why  # taken 0.5 s before
    assert "too recent" in read(good, 30.0).why


@pytest.mark.parametrize("odd", ["²", "١٢", "1.5", "1e3", "-1", "", "0x10"])
def test_the_reader_never_raises_on_odd_digits(odd):
    # str.isdigit() takes '²' and '١٢', which int() refuses: a stop must never raise on what a login node prints
    from hpc_bridge import scheduler_ops

    u = REJECTED.uuid
    for text in (_rows(uep=f"5 {u} {odd} 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661"),
                 _rows(now=f"{odd} 000000000"), _rows(mgr=f"{odd} 886 20 886 20"), _rows(s2={"ps": f"{u} {odd}"}),
                 f"HPCB_UEP 1 {odd} {u} 1 6 3 13 23 50 53 886 2026-10-16 07:38:22 1792136302 0 455661\n"):
        ev = scheduler_ops._read_uep_evidence(text, _EID, [("k", 5, 110.0)])["k"]
        assert not ev.lost


# --- the stop -------------------------------------------------------------------------------------------------------

class _UuidFacility(FakeFacility):
    """An SSH endpoint of this plugin's (its user endpoints exit when idle and name their block request), whose
    endpoint id is a UUID, as the web service gives it."""

    def __init__(self, eid: str = REJECTED.eid, *, fresh: bool = True, named: bool = True,
                 init_blocks: int | None = 0) -> None:
        super().__init__()
        self.workers = 1
        self.eid = eid
        self.fresh_ueps = fresh
        self.logs_block_requests = named
        self.compute_init_blocks = init_blocks          # 0: batch mode (the default), 1: interactive

    async def provision(self, profile):
        from hpc_bridge.facility.base import EndpointHandle

        self.provisioned = True
        return EndpointHandle(endpoint_id=self.eid, name="fake", reused=self.reused)


class _PbsUuidFacility(_UuidFacility, _PbsFacility):
    pass


class _BlockRunner(_FakeRunner):
    """A compute runner that records, as GlobusRunner does, the block request each check was sent for (its config's
    `hpcb_block` at the send) and when its task id was first seen — `taken_ago` seconds before now."""

    def __init__(self, eid, uec, *, taken_ago=120.0):
        super().__init__(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
        self.user_endpoint_config = uec
        self.blocks, self.taken, self.taken_ago = {}, {}, taken_ago

    async def canary(self, timeout=8.0):
        before = len(self.checks)
        res = await super().canary(timeout)
        for f in self.checks[before:]:
            self.blocks[id(f)] = (self.user_endpoint_config or {}).get("hpcb_block")
            self.taken[id(f)] = time.time() - self.taken_ago
        return res

    def sent_block(self, fut):
        return self.blocks.get(id(fut))

    def taken_at(self, fut):
        return self.taken.get(id(fut))


async def _lost_check_app(monkeypatch, *, eid=REJECTED.eid, pbs=False, fresh=True, named=True, taken_ago=120.0,
                          init_blocks=0):
    """The cells' state: spend confirmed on a fresh user endpoint, its check (canary) timed out and the service lists it
    as pending for good (its failure never left the user endpoint)."""
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    fac = (_PbsUuidFacility if pbs else _UuidFacility)(eid, fresh=fresh, named=named, init_blocks=init_blocks)
    app = AppCtx(facility=fac, profile=Profile())
    built = {}

    def factory(eid, user_endpoint_config=None, **_kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))                       # the login shape: warm
        built["compute"] = _BlockRunner(eid, user_endpoint_config, taken_ago=taken_ago)
        return built["compute"]

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning"
    runner = built["compute"]
    runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}   # what the service says, for good
    [check] = runner.checks
    return app, runner, runner.sent_block(check)


def _login_node(monkeypatch, root: Path, *, releases=(), evidence=None):
    """Script the login channel: a release answers the next of `releases` (else `released none`) — after the facility
    reads the endpoint's template, as the login shape's provision does (warmth._provision -> adopt_endpoint); the
    evidence command RUNS, against `root` as the login node's Globus Compute directory — or answers `evidence` (a
    ShellOutcome) instead. Returns the log of (kind, command)."""
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome

    releases, log = list(releases), []

    async def run(a, command, session_id="default", shape="compute"):
        kind = "evidence" if "HPCB_UEP" in command else "probe" if "HPCB_ERR" in command else "release"
        log.append((kind, command))
        if kind == "evidence":
            if evidence is not None:
                return evidence
            return ShellOutcome(phase="complete", exit_code=0, stdout=_run(command, root), block_state="warm")
        adopt = getattr(a.facility, "adopt_endpoint", None)
        if adopt is not None and a.state.endpoint_id:
            await adopt(a.state.endpoint_id)
        out = (releases.pop(0) if releases else "released none\n") if kind == "release" else ""
        return ShellOutcome(phase="complete", exit_code=0, stdout=out, block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    return log


def _kinds(log):
    return [k for k, _ in log]


def _two_minutes_ago() -> datetime:
    return datetime.now().replace(microsecond=0) - timedelta(seconds=119)   # a second after the check was taken


@needs_posix_shell
@pytest.mark.parametrize("name", ["rejected", "worker_died", "live"])
@pytest.mark.parametrize("pbs", [False, True], ids=["slurm", "pbs"])
async def test_a_check_whose_user_endpoint_failed_it_and_stopped_for_good_is_down(monkeypatch, tmp_path, name, pbs):
    # RED on b04aff8: the service lists the check as pending for good, so every stop answered `draining` and only
    # teardown_endpoint reached `down`
    from hpc_bridge import warmth
    from hpc_bridge.server import _stop_endpoint
    from hpc_bridge.state import SentCheckStore

    cell = CELLS[name]
    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch, eid=cell.eid, pbs=pbs)
    _put_cell(tmp_path, cell, block, started=_two_minutes_ago())
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.spend_stopped is True, res.notice
    assert _kinds(log).count("evidence") == 1 and _kinds(log)[-1] == "evidence"
    n = res.notice
    assert n.startswith("compute block released over AMQP (released none). hpc-bridge's check for the block on the "
                        f"default {'queue' if pbs else 'partition'} (task check-1) cannot run")
    assert (f"its user endpoint on the login node (uep.{cell.eid}.{cell.uuid}) received it and went into bad state "
            "(at 20" in n and "which failed it; it then shut down, nothing of it runs there, and the endpoint has not "
            "started it again, so no block of ours can start for it" in n)
    assert ("The report that it failed was lost on the endpoint's own side (a known globus-compute-endpoint race" in n
            and "the Globus Compute service still lists it as queued" in n)
    # settled for good: off the record, and a later stop does not look again
    await warmth._flush_all(app)
    assert SentCheckStore().load(cell.eid) == [] and cell.eid not in app.sent_checks
    log = _login_node(monkeypatch, tmp_path)
    again = await _stop_endpoint(app)
    assert again.status == "down" and _kinds(log) == ["release"]


@needs_posix_shell
async def test_a_rejected_submission_whose_check_was_lost_says_nothing_was_created_and_why_it_is_settled(
        monkeypatch, tmp_path):
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch)
    app.shapes["compute"].rejected = ("sbatch: error: AssocMaxSubmitJobLimit", None, None)   # its REJECTED answer
    _put_cell(tmp_path, REJECTED, block, started=_two_minutes_ago())
    _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.notice.startswith(
        "nothing to cancel: the scheduler REJECTED the block's submission (sbatch: error: AssocMaxSubmitJobLimit), so "
        "no block was created and none bills (released none). hpc-bridge's check for the block on the default partition "
        "(task check-1) cannot run")
    assert res.notice.endswith("still lists it as queued. The login endpoint stays online for reuse (reconnecting is "
                               "zero-SSH).")


@needs_posix_shell
async def test_a_check_whose_user_endpoint_is_alive_or_unsettled_keeps_the_stop_draining(monkeypatch, tmp_path):
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)

    async def stop(*, other=False, **put):
        app, _runner, block = await _lost_check_app(monkeypatch)
        _put_cell(tmp_path, REJECTED, block + 1 if other else block, started=put.pop("started", _two_minutes_ago()),
                  **put)
        log = _login_node(monkeypatch, tmp_path)
        return app, log, await _stop_endpoint(app)

    raw = (_FIXTURES / REJECTED.compute).read_text()
    app, log, res = await stop(compute="\n".join(ln for ln in raw.splitlines() if "Setting bad state" not in ln) + "\n")
    assert res.status == "draining" and res.spend_stopped is not True and "evidence" in _kinds(log)
    assert ("hpc-bridge also looked on the login node and cannot rule out that it can still start a block: its user "
            "endpoint's log shows no bad state since it last started.") in res.notice
    assert "(task check-1) is still queued on the endpoint" in res.notice
    assert [c.tid for c in app.sent_checks[REJECTED.eid]] == ["check-1"]        # still on the record
    # the bad state was logged before the check was taken (another life's): the check was taken two minutes ago
    _app, _log, res = await stop(started=datetime.now() - timedelta(minutes=10))
    assert res.status == "draining" and "before the check had reached it" in res.notice
    # its process is still alive
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", f"uep.{REJECTED.eid}.{REJECTED.uuid}"])
    try:
        _app, _log, res = await stop()
    finally:
        proc.kill()
        proc.wait()
    assert res.status == "draining" and "a process of its user endpoint is still running there" in res.notice
    # its manager has not reaped it yet (no stop logged), or revived it
    base = (_FIXTURES / REJECTED.manager).read_text()
    unreaped = "\n".join(ln for ln in base.splitlines() if "Command stopped normally (886)" not in ln) + "\n"
    _app, _log, res = await stop(manager=unreaped)
    assert res.status == "draining" and "does not show that user endpoint's stop" in res.notice
    # no user endpoint names the check's block request (not started yet — or its log is another request's)
    _app, _log, res = await stop(other=True)
    assert res.status == "draining" and "no user endpoint there names the check's block request yet" in res.notice


@needs_posix_shell
async def test_a_check_too_recent_for_its_user_endpoints_fate_keeps_the_stop_draining(monkeypatch, tmp_path):
    # a short HPC_BRIDGE_STOP_CHECK_WAIT_S does not loosen it: the check's own age is what counts, past the manager's
    # 30 s grace for a cached start request
    from hpc_bridge.server import _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_STOP_CHECK_WAIT_S", "0")
    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch, taken_ago=20.0)
    _put_cell(tmp_path, REJECTED, block, started=datetime.now() - timedelta(seconds=40))
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "evidence" in _kinds(log) and "too recent" in res.notice


@pytest.mark.parametrize("outcome", ["cold", "busy", "failed"])
async def test_a_cold_or_busy_login_channel_keeps_the_stop_draining(monkeypatch, tmp_path, outcome):
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, _block = await _lost_check_app(monkeypatch)
    answer = {"cold": ShellOutcome(phase="cold_start", block_state="cold", notice="allocating nodes…"),
              "busy": ShellOutcome(phase="running", block_state="warm", task_id="login-9"),
              "failed": ShellOutcome(phase="complete", exit_code=1, stdout="", block_state="warm")}[outcome]
    log = _login_node(monkeypatch, tmp_path, evidence=answer)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "the login channel was cold or busy" in res.notice
    assert _kinds(log).count("evidence") == 1


async def test_a_login_node_that_does_not_answer_in_time_keeps_the_stop_draining(monkeypatch, tmp_path):
    import asyncio

    from hpc_bridge import server
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, _block = await _lost_check_app(monkeypatch)
    _login_node(monkeypatch, tmp_path)
    inner = server._run_shell

    async def slow(a, command, session_id="default", shape="compute"):
        if "HPCB_UEP" in command:
            await asyncio.sleep(5)
        return await inner(a, command, session_id=session_id, shape=shape)

    monkeypatch.setattr(server, "_run_shell", slow)
    monkeypatch.setattr(server, "_EVIDENCE_TIMEOUT_S", 0.05)
    monkeypatch.setattr(server, "_EVIDENCE_MIN_S", 0.01)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "the login node did not answer in time" in res.notice


async def test_a_stop_with_no_time_left_does_not_look(monkeypatch, tmp_path):
    # a slow scheduler ate the call's budget (each release takes 80 s here): the look is skipped, never run past it
    from hpc_bridge import server
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, _block = await _lost_check_app(monkeypatch)
    log = _login_node(monkeypatch, tmp_path)
    inner = server._run_shell

    async def slow_release(a, command, session_id="default", shape="compute"):
        if "HPCB_UEP" not in command:
            skew[0] += 80.0
        return await inner(a, command, session_id=session_id, shape=shape)

    monkeypatch.setattr(server, "_run_shell", slow_release)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "this stop had no time left to look" in res.notice
    assert "evidence" not in _kinds(log) and _kinds(log).count("release") == 2


@pytest.mark.parametrize("fresh, named", [(False, False), (True, False)], ids=["earlier-plugin", "unnamed-template"])
async def test_an_endpoint_whose_template_does_not_name_block_requests_is_not_looked_at(monkeypatch, tmp_path, fresh,
                                                                                       named):
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, _block = await _lost_check_app(monkeypatch, fresh=fresh, named=named)
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "evidence" not in _kinds(log) and "looked on the login node" not in res.notice


async def test_a_lost_command_or_a_check_with_no_block_request_or_no_task_id_is_not_looked_at(monkeypatch, tmp_path):
    # a command runs the user's work: whatever its user endpoint's fate, a stop must not say it ended — and while it
    # waits nothing can make this stop `down`, so nothing is run
    from hpc_bridge import warmth
    from hpc_bridge.context import SentCheck
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, runner, block = await _lost_check_app(monkeypatch)
    warmth._keep_checks(app, REJECTED.eid, [SentCheck(future=None, client=runner.task_client(), task_id="cmd-1",
                                                      sent_at=time.time() - 60, what="command", block=block,
                                                      taken_at=time.time() - 60, ceiling_at=time.time() - 1)])
    log = _login_node(monkeypatch, tmp_path)
    assert (await _stop_endpoint(app)).status == "draining" and "evidence" not in _kinds(log)
    # a check read back with no block request (an older record): nothing names its user endpoint
    app, runner, _block = await _lost_check_app(monkeypatch)
    runner.blocks.clear()
    log = _login_node(monkeypatch, tmp_path)
    assert (await _stop_endpoint(app)).status == "draining" and "evidence" not in _kinds(log)
    # a check whose task id was never seen: no time the service took it by — and it may still be on its way there,
    # where it would have the manager start that user endpoint again
    app, runner, block = await _lost_check_app(monkeypatch)
    _put_cell(tmp_path, REJECTED, block, started=_two_minutes_ago())
    unsent = _PendingFuture()                                   # its submit has not come back: no task id yet
    runner.checks.append(unsent)
    runner.blocks[id(unsent)] = block
    log = _login_node(monkeypatch, tmp_path)
    assert (await _stop_endpoint(app)).status == "draining" and "evidence" not in _kinds(log)
    app, runner, _block = await _lost_check_app(monkeypatch)
    runner.taken.clear()
    log = _login_node(monkeypatch, tmp_path)
    assert (await _stop_endpoint(app)).status == "draining" and "evidence" not in _kinds(log)


@needs_posix_shell
async def test_a_restarted_server_reads_the_block_request_back_and_settles_the_check(monkeypatch, tmp_path):
    # the record carries each check's block request and when its task id was seen, so a server restarted after the
    # check was lost still finds it — its first stop's cancel reads the endpoint's template (the login shape's provision)
    from hpc_bridge import warmth
    from hpc_bridge.server import _stop_endpoint
    from hpc_bridge.state import SentCheckStore

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch)
    await warmth._flush_all(app)
    [row] = SentCheckStore().load(REJECTED.eid)
    assert row["task_id"] == "check-1" and row["block"] == block and abs(row["taken_at"] - (time.time() - 120)) < 5
    fac = _UuidFacility(fresh=False, named=False, init_blocks=None)   # not known yet: the release's provision adopts it

    async def adopt(eid):
        fac.fresh_ueps = fac.logs_block_requests = True
        fac.compute_init_blocks = 0

    fac.adopt_endpoint = adopt
    restarted = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id=REJECTED.eid))
    restarted.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"},
                                             runner=_FakeRunner(REJECTED.eid, _Res(0, "", "")),
                                             warm_confirmed_at=time.monotonic())
    restarted.shapes["login"].runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}
    _put_cell(tmp_path, REJECTED, block, started=_two_minutes_ago())
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(restarted)
    assert res.status == "down" and "cannot run" in res.notice and _kinds(log).count("evidence") == 1
    await warmth._flush_all(restarted)
    assert SentCheckStore().load(REJECTED.eid) == []
    # an older record (no block request, or no time its task id was seen) is not looked at
    for old in ({**row, "block": None}, {**row, "taken_at": None}):
        SentCheckStore().save(REJECTED.eid, [old])
        again = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id=REJECTED.eid))
        again.shapes["login"] = restarted.shapes["login"]
        log = _login_node(monkeypatch, tmp_path)
        assert (await _stop_endpoint(again)).status == "draining" and "evidence" not in _kinds(log)


def test_a_block_request_is_a_whole_number_the_template_renders_as_digits():
    from hpc_bridge import warmth

    rt = ShapeRuntime(user_endpoint_config={"compute": True})
    app = AppCtx(facility=_UuidFacility(), profile=Profile())
    rt.spend_confirmed = True
    warmth._fresh_block(app, "compute", rt)
    n = rt.user_endpoint_config["hpcb_block"]
    assert isinstance(n, int) and 0 < n < 2 ** 48 and warmth._block_number(n) == n
    assert warmth._block_number("3f9a0c1e") is None and warmth._block_number(True) is None
    assert warmth._block_number(0) is None and warmth._block_number(2 ** 48) is None


async def test_a_check_is_dated_by_when_its_task_id_was_first_seen_not_by_its_submit():
    # Executor.submit only queues a task: the SDK sends it later (batched, rate-limited, retried), so the submit's time
    # says nothing of when the service had it. The id is the service's answer.
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge.runner import GlobusRunner
    from tests.test_runner import _CanaryFuture, _QueueExecutor

    queued = _CanaryFuture(pending=True)
    r = GlobusRunner("eid", executor_factory=lambda: _QueueExecutor(queued),
                     user_endpoint_config={"compute": True, "hpcb_block": 77})
    await r.canary(timeout=0.05)
    [(fut, _client, sent_at)] = r.sent_checks()
    assert r.sent_block(fut) == 77 and r.taken_at(fut) is None          # no id yet: no time the service had it
    time.sleep(0.05)
    fut.task_id = "t-1"
    seen = r.taken_at(fut)
    assert seen is not None and seen > sent_at and r.taken_at(fut) == seen   # stamped once, when first seen
    r.user_endpoint_config["hpcb_block"] = 78                           # (the shape's config can be the same dict)
    assert r.sent_block(fut) == 77


# --- round 2 of the reviews ------------------------------------------------------------------------------------------

@pytest.mark.parametrize("init_blocks", [1, None], ids=["interactive", "unknown"])
async def test_an_endpoint_whose_new_user_endpoints_start_a_block_by_themselves_is_not_looked_at(
        monkeypatch, tmp_path, init_blocks):
    # interactive mode renders `init_blocks: 1`: ANY new life of that user endpoint submits a block, task or not, so a
    # `lost` there would rest on the service never restarting a user endpoint it thinks still holds a pending task
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch, init_blocks=init_blocks)
    _put_cell(tmp_path, REJECTED, block, started=_two_minutes_ago())
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "evidence" not in _kinds(log) and "looked on the login node" not in res.notice


def test_the_templates_compute_init_blocks_is_read_from_it():
    from hpc_bridge.facility import remote
    from tests.test_remote_facility import _pbs_profile, _profile

    for prof in (_profile, _pbs_profile):
        f = remote.SlurmFacility(prof(), cli=None)
        assert remote.compute_init_blocks(f.config_template(Profile(mode="batch"))[0]) == 0
        assert remote.compute_init_blocks(f.config_template(Profile(mode="interactive"))[0]) == 1
    tmpl = remote.SlurmFacility(_profile(), cli=None).config_template(Profile())[0]
    assert remote.compute_init_blocks("\n".join(ln for ln in tmpl.splitlines() if "init_blocks |" not in ln)) is None
    assert remote.compute_init_blocks(tmpl + "\n" + tmpl) is None and remote.compute_init_blocks(None) is None


async def test_provision_and_adopt_know_the_compute_init_blocks():
    from hpc_bridge.facility import remote
    from tests.test_env_setup import _Client, _facility
    from tests.test_remote_facility import _FakeRemoteCLI, _profile

    for mode, want in (("batch", 0), ("interactive", 1)):
        fac = remote.SlurmFacility(_profile(), cli=_FakeRemoteCLI(status=None))
        assert fac.compute_init_blocks is None
        await fac.provision(Profile(mode=mode))
        assert fac.compute_init_blocks == want
    import hpc_bridge.facility.endpoint_venv as ev

    wi = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    for mode, want in (("batch", 0), ("interactive", 1)):
        ours = _facility(wi).config_template(Profile(mode=mode))[0]
        fac = _facility(wi, client=lambda t=ours: _Client(t))
        await fac.adopt_endpoint("eid-1")
        assert fac.compute_init_blocks == want
    fac = _facility(wi, client=lambda: _Client(exc=RuntimeError("503")))
    await fac.adopt_endpoint("eid-1")
    assert fac.compute_init_blocks is None


@needs_posix_shell
def test_a_rotated_manager_log_is_read_through_its_rotation(tmp_path):
    # globus-compute-endpoint rotates its log at 100 MB, keeping one copy: the current file then lacks `Core Endpoint
    # begins`, and a life's start may be in the copy while its exit is in the current file
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    md, _ud, _ = _put_cell(tmp_path, cell, 42, started=started)
    lines = (md / "endpoint.log").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines) if f"(pid: 886) [(hpcbridge-test-00, uep.{cell.eid}.{cell.uuid})" in ln)
    (md / "endpoint.log.1").write_text("\n".join(lines[:cut + 1]) + "\n")       # its start, in the rotated copy
    (md / "endpoint.log").write_text("\n".join(lines[cut + 1:]) + "\n")         # its exit, in the current file
    out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path)
    assert _verdict(out, cell, 42, taken).lost
    # rotated twice over (the begin line gone from both): found by its endpoint.json, still read whole
    (md / "endpoint.log.1").write_text("\n".join(ln for ln in lines[:cut + 1] if "Core Endpoint begins" not in ln) + "\n")
    assert "could not be found" in _verdict(_run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path), cell, 42,
                                            taken).why
    (md / "endpoint.json").write_text(f'{{"endpoint_id": "{cell.eid}"}}')
    assert _verdict(_run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path), cell, 42, taken).lost
    # the start rotated out entirely: no evidence
    (md / "endpoint.log.1").unlink()
    assert "does not show that user endpoint's stop" in _verdict(
        _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path), cell, 42, taken).why


@needs_posix_shell
def test_an_unreadable_twin_naming_the_same_block_request_and_alive_is_no_evidence(tmp_path):
    # the reviewer's probe P2: a second user endpoint naming the same block request, its log unreadable, a process of
    # it alive — beside the readable, dead one
    from hpc_bridge import scheduler_ops

    if os.geteuid() == 0:
        pytest.skip("root reads a 0000 file")
    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    _put_cell(tmp_path, cell, 42, started=started)
    twin_id = "0b1c2d3e-0000-4000-8000-000000000001"
    twin = tmp_path / f"uep.{cell.eid}.{twin_id}"
    twin.mkdir()
    (twin / "endpoint.log").write_text((tmp_path / f"uep.{cell.eid}.{cell.uuid}" / "endpoint.log").read_text())
    (twin / "endpoint.log").chmod(0)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", f"uep.{cell.eid}.{twin_id}"])
    try:
        out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path)
    finally:
        proc.kill()
        proc.wait()
        (twin / "endpoint.log").chmod(0o600)
    ev = _verdict(out, cell, 42, taken)
    assert not ev.lost and "whose log could not be read is still running" in ev.why
    assert f"HPCB_PS 1 {twin_id} 1" in out
    # the same twin, not running and never started by the manager: it cannot start a block — settled
    (twin / "endpoint.log").chmod(0)
    try:
        out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path)
    finally:
        (twin / "endpoint.log").chmod(0o600)
    assert _verdict(out, cell, 42, taken).lost
    # …but one its manager started and has not seen exit: not
    md = tmp_path / "hpc-bridge-fake-cell"
    with (md / "endpoint.log").open("a") as fh:
        fh.write(f"{datetime.now():%Y-%m-%d %H:%M:%S,%f} INFO MainProcess-239 MainThread-1 globus_compute_endpoint"
                 f".endpoint.core_endpoint:1079 cmd_start_endpoint Creating new user endpoint (pid: 5150) [(u, uep."
                 f"{cell.eid}.{twin_id}) globus-compute-endpoint _start-user-endpoint uep.{cell.eid}.{twin_id}]\n")
    (twin / "endpoint.log").chmod(0)
    try:
        out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path)
    finally:
        (twin / "endpoint.log").chmod(0o600)
    assert "may start again" in _verdict(out, cell, 42, taken).why


@needs_posix_shell
def test_a_read_error_grep_meets_is_said_never_an_empty_match(tmp_path):
    # `[ -r ]` checks permissions only: an EIO or a stale NFS handle shows up as grep's own error (exit 2), which must
    # read as "could not be read", never as "no user endpoint names it"
    from hpc_bridge import scheduler_ops

    cell = REJECTED
    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    taken = (datetime.now() - started).total_seconds() + 1
    _put_cell(tmp_path / "gc", cell, 42, started=started)
    shim = tmp_path / "bin"
    shim.mkdir()
    real = subprocess.run(["sh", "-c", "command -v grep"], capture_output=True, text=True).stdout.strip()
    (shim / "grep").write_text(f'#!/bin/sh\ncase " $* " in *" -lF "*) "{real}" "$@"; echo "grep: x: Input/output error" >&2; '
                               f'exit 2;; esac\nexec "{real}" "$@"\n')
    (shim / "grep").chmod(0o755)
    out = _run(scheduler_ops._uep_evidence_cmd(cell.eid, [42]), tmp_path / "gc",
               env={"PATH": f"{shim}:{os.environ['PATH']}"})
    assert "HPCB_FAIL 1 grep" in out
    assert "could not be read" in _verdict(out, cell, 42, taken).why


async def test_an_age_is_measured_on_the_monotonic_clock_so_a_wall_clock_step_cannot_stretch_it(monkeypatch, tmp_path):
    # the check was truly taken AFTER its user endpoint's bad state; then this machine's wall clock stepped forward an
    # hour. Measured on the wall clock its age would place it an hour earlier, before the bad state — a false `lost`.
    from hpc_bridge import server
    from hpc_bridge.context import SentCheck

    started = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    _put_cell(tmp_path, REJECTED, 42, started=started)
    app = AppCtx(facility=_UuidFacility(), profile=Profile(), state=EndpointState(endpoint_id=REJECTED.eid))
    _login_node(monkeypatch, tmp_path)
    taken_wall, taken_mono = time.time() - 60, time.monotonic() - 60       # a minute ago: after the bad state
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 3600)               # the wall clock steps an hour forward
    check = SentCheck(future=None, task_id="check-1", block=42, sent_at=taken_wall, taken_at=taken_wall,
                      taken_mono=taken_mono)
    states, _found, why = await server._settle_by_evidence(app, REJECTED.eid, [check], ["pending"],
                                                          time.monotonic() + 120)
    assert states == ["pending"] and why is not None and "before the check had reached it" in why
    # read back from disk there is only the wall time: that is what the record carries
    check = SentCheck(future=None, task_id="check-1", block=42, sent_at=taken_wall, taken_at=taken_wall)
    states, _found, _why = await server._settle_by_evidence(app, REJECTED.eid, [check], ["pending"],
                                                            time.monotonic() + 120)
    assert states == ["lost"]          # (the stepped wall clock fools it: why the in-process age is monotonic)
