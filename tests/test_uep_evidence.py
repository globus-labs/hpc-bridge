"""A stop's evidence for a check the Globus Compute service still lists as queued, read on the login node.

Live 2026-10-16 (fake `site`: submit_policy_rejected, slurm_worker_died): globus-compute-endpoint 4.18 acks a task once
it hands it to the engine; at bad state its watcher stops the user endpoint ~0.3 s later and its result publisher stops
without blocking, so the failed check's result never reaches the service (`set_bad_state_and_fail_all` at 07:38:22, then
`ResultPublisher<✗; o:1; t:0> Connection closed`). Every stop then answered `draining` for good, and only
teardown_endpoint reached `down`. A stop now reads that check's OWN compute user endpoint — the one directory whose log
names the check's block request — and answers `down` only when it went into bad state AFTER the check was sent, logged
its shutdown, and left no process. Anything less keeps `draining`.

The logs under tests/uep_log_fixtures/ are those two bundles' user endpoint logs, verbatim; the tests insert the
`endpoint_setup` lines globus-compute-endpoint 4.18 writes at a user endpoint's start (endpoint.py:513, :920) and shift
the timestamps to the test's clock — never the lines themselves.
"""
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, ShapeRuntime, _ensure_endpoint_up
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _PbsFacility, _Res, _settle_clock, _skew

_FIXTURES = Path(__file__).parent / "uep_log_fixtures"
EID = "7a7da153-4643-4b2f-b8a6-ceee6211af97"            # the submit_policy_rejected cell's endpoint
LOST = "a9d3d64e-394f-8ad8-b2fb-aec6c96b1d32"            # its user endpoint whose failed check never reached the service
DIED = "cfe285ef-7f41-943f-0e9f-a4ae1d904982"            # slurm_worker_died's (its pilot exited 42 before it connected)
LOGIN = "59f3b039-27fb-a42d-6aac-0030bfb5cef3"           # the login shape's own user endpoint (long-lived, no block)
_STAMP = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{6})")

needs_posix_shell = pytest.mark.skipif(sys.platform == "win32", reason="the login node's shell: sh, awk, ps, find, date")


def _uep_log(name: str, block: int | None, *, first: datetime) -> str:
    """A fixture log as the user endpoint would write it under this plugin's template: the bundle's lines verbatim, with
    the `endpoint_setup` lines a 4.18 user endpoint logs at its start (before `Endpoint begins`) naming `block`, and
    every timestamp shifted so the log's first one is `first` (the login node's local time, as its logger writes)."""
    lines = (_FIXTURES / name).read_text().splitlines()
    stamps = [datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f") for ln in lines if (m := _STAMP.match(ln))]
    shift = first - stamps[0]

    def restamp(ln: str) -> str:
        m = _STAMP.match(ln)
        if m is None:
            return ln
        when = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f") + shift
        return when.strftime("%Y-%m-%d %H:%M:%S,%f") + ln[m.end():]

    out = [restamp(ln) for ln in lines]
    if block is not None:  # globus_compute_endpoint.endpoint.endpoint:_run_command, at the start, before the interchange
        pid = re.search(r"MainProcess-\d+ MainThread-\d+", "\n".join(lines)).group(0)
        at = (stamps[0] + shift - timedelta(milliseconds=5)).strftime("%Y-%m-%d %H:%M:%S,%f")
        out[2:2] = [f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:920 _run_command running "
                    f"endpoint_setup command: true hpcb_block={block}",
                    f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:923 _run_command endpoint_setup stdout: "
                    "b''",
                    f"{at} INFO {pid} globus_compute_endpoint.endpoint.endpoint:924 _run_command endpoint_setup stderr: "
                    "b''"]
    return "\n".join(out) + "\n"


def _put(root: Path, uuid: str, text: str, eid: str = EID) -> Path:
    d = root / f"uep.{eid}.{uuid}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "endpoint.log").write_text(text)
    return d


def _run(cmd: str, root: Path, cwd: Path | None = None) -> str:
    """The evidence command as the login shape runs it: a POSIX shell, here, against `root` as the Globus Compute
    directory (never the real one: GLOBUS_COMPUTE_USER_DIR is set, so $HOME is not read)."""
    env = {**os.environ, "GLOBUS_COMPUTE_USER_DIR": str(root)}
    res = subprocess.run(["sh", "-c", cmd], env=env, capture_output=True, text=True, timeout=30, cwd=cwd or root)
    assert res.returncode == 0, res.stderr
    return res.stdout


def _now_local(out: str) -> datetime:
    w = next(ln.split() for ln in out.splitlines() if ln.startswith("HPCB_NOW "))
    return datetime.strptime(f"{w[2]} {w[3]}", "%Y-%m-%d %H:%M:%S")


# --- the command, run for real on the bundles' log lines --------------------------------------------------------------

@needs_posix_shell
@pytest.mark.parametrize("fixture, uuid", [("rejected.endpoint.log", LOST), ("worker_died.endpoint.log", DIED)],
                         ids=["rejected", "worker_died"])
def test_the_command_finds_the_checks_user_endpoint_in_bad_state_and_gone(tmp_path, fixture, uuid):
    from hpc_bridge import scheduler_ops

    start = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    _put(tmp_path, uuid, _uep_log(fixture, 207612534, first=start))
    _put(tmp_path, LOGIN, _uep_log("login.endpoint.log", None, first=start - timedelta(minutes=1)))
    _put(tmp_path, "0b1c2d3e-0000-4000-8000-000000000001", _uep_log("rejected.endpoint.log", 99, first=start))
    out = _run(scheduler_ops._uep_evidence_cmd(EID, [207612534]), tmp_path)
    assert f"HPCB_PS {uuid} 0" in out and f"HPCB_PID {uuid} none" in out and LOGIN not in out
    age = (_now_local(out) - start).total_seconds() + 1          # sent a second before its user endpoint started
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("check-1", 207612534, age)]).values()
    assert ev.lost and ev.uep == f"uep.{EID}.{uuid}" and ev.bad_at is not None
    # the bad state in the log is the bundle's own line, as far after the start as it was live
    bad = datetime.strptime(ev.bad_at, "%Y-%m-%d %H:%M:%S")
    assert timedelta(seconds=5) < bad - start < timedelta(seconds=70)
    # bad state logged BEFORE the check was sent: an earlier life's, not this check's
    late = (_now_local(out) - bad).total_seconds() - 30
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("check-1", 207612534, late)]).values()
    assert not ev.lost and "before the check was sent" in ev.why


@needs_posix_shell
def test_the_command_sees_a_process_of_that_user_endpoint_and_a_pid_file_still_refreshed(tmp_path):
    from hpc_bridge import scheduler_ops

    start = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    d = _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", 5, first=start))
    age = (datetime.now() - start).total_seconds() + 1
    cmd = scheduler_ops._uep_evidence_cmd(EID, [5])
    # a process whose arguments name the user endpoint's directory (its title does: "Globus Compute Endpoint (<id>,
    # uep.<eid>.<id>) [...]"): still alive, whatever its log says
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", f"uep.{EID}.{LOST}"])
    try:
        out = _run(cmd, tmp_path)
    finally:
        proc.kill()
        proc.wait()
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 5, age)]).values()
    assert not ev.lost and "still running" in ev.why
    # gone: lost — unless its pid file is still being refreshed (every 15 s while it lives, from any login node)
    (d / "daemon.pid").write_text("4242\n")
    out = _run(cmd, tmp_path)
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 5, age)]).values()
    assert f"HPCB_PS {LOST} 0" in out and not ev.lost and "pid file is still being refreshed" in ev.why
    old = time.time() - 600
    os.utime(d / "daemon.pid", (old, old))          # a force-killed one's leftover: stale
    out = _run(cmd, tmp_path)
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 5, age)]).values()
    assert f"HPCB_PID {LOST} old" in out and ev.lost


@needs_posix_shell
def test_the_command_reads_no_evidence_from_a_log_that_is_not_settled(tmp_path):
    from hpc_bridge import scheduler_ops

    start = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    age = (datetime.now() - start).total_seconds() + 1
    full = _uep_log("rejected.endpoint.log", 7, first=start).splitlines()

    def verdict(lines: list[str]) -> str:
        _put(tmp_path, LOST, "\n".join(lines) + "\n")
        out = _run(scheduler_ops._uep_evidence_cmd(EID, [7]), tmp_path)
        [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 7, age)]).values()
        assert not ev.lost
        return ev.why

    # no bad state: a user endpoint whose block simply had not come up yet
    assert "no bad state" in verdict([ln for ln in full if "Setting bad state" not in ln])
    # bad state, but no shutdown logged (yet)
    assert "not logged its shutdown" in verdict([ln for ln in full if not ln.startswith("---------- Endpoint shutdown")])
    # it was started again for the same block request after that shutdown: the new life has no bad state
    again = _uep_log("rejected.endpoint.log", 7, first=start + timedelta(seconds=40)).splitlines()
    assert "no bad state since it last started" in verdict(full + again[:8])
    assert "starting again" in verdict(full + again[:3])            # its endpoint_setup ran, its interchange has not
    # no user endpoint names the block request: not started yet, or this plugin's template is not the endpoint's
    assert "names the check's block request yet" in verdict([ln for ln in full if "hpcb_block=" not in ln])
    # bad state on another block request's user endpoint (a prefix of ours) is not ours
    assert "names the check's block request yet" in verdict([ln.replace("hpcb_block=7", "hpcb_block=77") for ln in full])


@needs_posix_shell
def test_an_unreadable_or_ambiguous_login_node_is_no_evidence(tmp_path):
    from hpc_bridge import scheduler_ops

    start = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    age = (datetime.now() - start).total_seconds() + 1
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", 8, first=start))
    other = _put(tmp_path, DIED, _uep_log("worker_died.endpoint.log", 8, first=start))
    out = _run(scheduler_ops._uep_evidence_cmd(EID, [8]), tmp_path)
    [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 8, age)]).values()
    assert not ev.lost and "more than one user endpoint" in ev.why
    if os.geteuid() != 0:  # (root reads a 0000 file)
        (other / "endpoint.log").write_text(_uep_log("worker_died.endpoint.log", 9, first=start))
        (other / "endpoint.log").chmod(0)
        try:
            out = _run(scheduler_ops._uep_evidence_cmd(EID, [8]), tmp_path)
        finally:
            (other / "endpoint.log").chmod(0o600)
        [ev] = scheduler_ops._read_uep_evidence(out, EID, [("k", 8, age)]).values()
        assert f"HPCB_FAIL read {DIED}" in out and not ev.lost and "could not be read" in ev.why
    # another endpoint's user endpoints are never read
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", 8, first=start), eid="00000000-1111-4222-8333-444444444444")
    out = _run(scheduler_ops._uep_evidence_cmd("00000000-1111-4222-8333-444444444444", [8]), tmp_path)
    assert "HPCB_UEP 8" in out and EID not in out


# --- what the command is built from ----------------------------------------------------------------------------------

@pytest.mark.parametrize("eid", ["fake-eid", "", f"{EID}; rm -rf ~", f"$(touch pwned){EID[11:]}", EID + "\n",
                                 "../../../etc/passwd" + EID[20:], f"{EID}*", " " + EID])
def test_the_command_is_never_built_from_an_endpoint_id_that_is_not_a_uuid(eid):
    from hpc_bridge import scheduler_ops

    with pytest.raises(ValueError):
        scheduler_ops._uep_evidence_cmd(eid, [1])


@pytest.mark.parametrize("blocks", [[], ["1"], [True], [0], [-3], [2 ** 48], [1.5], ["1; rm -rf ~"], [1, "2"]])
def test_the_command_is_never_built_from_a_block_request_that_is_not_one_of_ours(blocks):
    from hpc_bridge import scheduler_ops

    with pytest.raises(ValueError):
        scheduler_ops._uep_evidence_cmd(EID, blocks)


@needs_posix_shell
@pytest.mark.parametrize("name", ['x"; touch pwned; "', "$(touch pwned)", "`touch pwned`", "a b", "*",
                                  "11111111-2222-3333-4444-55555555555$(touch pwned)"])
def test_a_hostile_directory_name_on_the_login_node_is_skipped_never_run(tmp_path, name):
    from hpc_bridge import scheduler_ops

    start = datetime.now().replace(microsecond=0) - timedelta(minutes=5)
    gc = tmp_path / "gc"
    _put(gc, name, _uep_log("rejected.endpoint.log", 3, first=start))
    out = _run(scheduler_ops._uep_evidence_cmd(EID, [3]), gc, cwd=tmp_path)
    assert "HPCB_UEP" not in out and not (tmp_path / "pwned").exists() and not (gc / "pwned").exists()


def test_the_reader_takes_only_rows_of_the_commands_shape():
    from hpc_bridge import scheduler_ops

    now = datetime(2026, 10, 16, 7, 40, 0)
    epoch = 1_792_136_400
    good = (f"HPCB_NOW {epoch} 2026-10-16 07:40:00\nHPCB_UEP 5 {LOST} 1 6 3 23 50 2026-10-16 07:38:22\n"
            f"HPCB_PS {LOST} 0\nHPCB_PID {LOST} none\n")
    sent = (now - datetime(2026, 10, 16, 7, 38, 10)).total_seconds()  # 07:38:10, 12 s before the bad state
    assert scheduler_ops._read_uep_evidence(good, EID, [("k", 5, sent)])["k"].lost
    for bad, why in [
        (good.replace(f"HPCB_PS {LOST} 0", f"HPCB_PS {LOST} 2"), "still running"),
        (good.replace(f"HPCB_PS {LOST} 0\n", "") + "HPCB_FAIL ps\n", "process list"),
        (good.replace(f"HPCB_PS {LOST} 0", "HPCB_PS ../x 0"), "still running"),           # not a row: no answer
        (good.replace(f"HPCB_PID {LOST} none", f"HPCB_PID {LOST} unknown"), "could not be read"),
        (good.replace("HPCB_NOW", "HPCB_NOPE"), "clock"),
        (good + f"HPCB_UEP 5 {DIED} 1 6 3 23 50 2026-10-16 07:38:22\n", "more than one"),
        (good + f"HPCB_UEP 6 {LOST} 1 6 3 23 50 2026-10-16 07:38:22\n", "more than one"),   # it names two requests
        (good.replace(" 50 2026", " 0 2026"), "shutdown"),
        (good.replace("1 6 3 23 50 2026-10-16 07:38:22", "1 6 3 0 0 -"), "no bad state"),
        (good.replace("1 6 3 23", "0 0 3 23"), "no start"),
        (good + f"HPCB_FAIL read {DIED}\n", "could not be read"),
        (good.replace(f"HPCB_UEP 5 {LOST}", f"HPCB_UEP 5 {LOST}; rm"), "names the check's block request yet"),
        (good.replace("07:40:00", "07:30:00"), "ahead of the login node's clock"),
    ]:
        ev = scheduler_ops._read_uep_evidence(bad, EID, [("k", 5, sent)])["k"]
        assert not ev.lost and why in ev.why, (bad, ev)
    # bad state at the very moment of the send (within whole-second clocks), or before it: not this check's
    assert not scheduler_ops._read_uep_evidence(good, EID, [("k", 5, sent - 11)])["k"].lost


# --- the stop -------------------------------------------------------------------------------------------------------

class _UuidFacility(FakeFacility):
    """An SSH endpoint of this plugin's (its user endpoints exit when idle and name their block request), whose
    endpoint id is a UUID, as the web service gives it."""

    def __init__(self, *, fresh: bool = True, named: bool = True) -> None:
        super().__init__()
        self.workers = 1
        self.fresh_ueps = fresh
        self.logs_block_requests = named

    async def provision(self, profile):
        from hpc_bridge.facility.base import EndpointHandle

        self.provisioned = True
        return EndpointHandle(endpoint_id=EID, name="fake", reused=self.reused)


class _PbsUuidFacility(_UuidFacility, _PbsFacility):
    pass


class _BlockRunner(_FakeRunner):
    """A compute runner that records, as GlobusRunner does, the block request each check was sent for (its config's
    `hpcb_block` at the send) and when it was sent — `sent_ago` seconds before now, as a check the stop has waited on."""

    def __init__(self, eid, uec, *, sent_ago=60.0):
        super().__init__(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
        self.user_endpoint_config = uec
        self.blocks, self.sent, self.sent_ago = {}, {}, sent_ago

    async def canary(self, timeout=8.0):
        before = len(self.checks)
        res = await super().canary(timeout)
        for f in self.checks[before:]:
            self.blocks[id(f)] = (self.user_endpoint_config or {}).get("hpcb_block")
            self.sent[id(f)] = time.time() - self.sent_ago
        return res

    def sent_checks(self):
        return [(f, c, self.sent.get(id(f), 0.0)) for f, c, _ in super().sent_checks()]

    def sent_block(self, fut):
        return self.blocks.get(id(fut))


async def _lost_check_app(monkeypatch, *, pbs=False, fresh=True, named=True, sent_ago=60.0):
    """The cells' state: spend confirmed on a fresh user endpoint, its check (canary) timed out and the service lists it
    as pending for good (its failure never left the user endpoint)."""
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    fac = _PbsUuidFacility(fresh=fresh, named=named) if pbs else _UuidFacility(fresh=fresh, named=named)
    app = AppCtx(facility=fac, profile=Profile())
    built = {}

    def factory(eid, user_endpoint_config=None, **_kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))                       # the login shape: warm
        built["compute"] = _BlockRunner(eid, user_endpoint_config, sent_ago=sent_ago)
        return built["compute"]

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning"
    runner = built["compute"]
    runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}   # what the service says, for good
    [check] = runner.checks
    return app, runner, runner.sent_block(check)


def _login_node(monkeypatch, root: Path, *, releases=(), evidence=None):
    """Script the login channel: a release answers the next of `releases` (else `released none`); the evidence command
    RUNS, against `root` as the login node's Globus Compute directory — or answers `evidence` (a ShellOutcome) instead.
    Returns the log of (kind, command)."""
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
        out = (releases.pop(0) if releases else "released none\n") if kind == "release" else ""
        return ShellOutcome(phase="complete", exit_code=0, stdout=out, block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    return log


def _kinds(log):
    return [k for k, _ in log]


@needs_posix_shell
@pytest.mark.parametrize("fixture, uuid", [("rejected.endpoint.log", LOST), ("worker_died.endpoint.log", DIED)],
                         ids=["rejected", "worker_died"])
@pytest.mark.parametrize("pbs", [False, True], ids=["slurm", "pbs"])
async def test_a_check_whose_user_endpoint_went_into_bad_state_and_is_gone_is_down(monkeypatch, tmp_path, fixture,
                                                                                     uuid, pbs):
    # RED on b04aff8: the service lists the check as pending for good, so every stop answered `draining` and only
    # teardown_endpoint reached `down`
    from hpc_bridge import warmth
    from hpc_bridge.server import _stop_endpoint
    from hpc_bridge.state import SentCheckStore

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch, pbs=pbs, sent_ago=120.0)
    started = datetime.now().replace(microsecond=0) - timedelta(seconds=118)  # 2 s after the check was sent
    _put(tmp_path, uuid, _uep_log(fixture, block, first=started))
    _put(tmp_path, LOGIN, _uep_log("login.endpoint.log", None, first=started - timedelta(minutes=2)))
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.spend_stopped is True, res.notice
    assert _kinds(log).count("evidence") == 1 and _kinds(log)[-1] == "evidence"
    n = res.notice
    assert n.startswith("compute block released over AMQP (released none). hpc-bridge's check for the block on the "
                        f"default {'queue' if pbs else 'partition'} (task check-1) cannot run")
    assert (f"its user endpoint on the login node (uep.{EID}.{uuid}) went into bad state after it was sent" in n
            and "and shut down, and no process of it is left there, so no block of ours can start for it" in n)
    assert ("The report that it failed was lost on the endpoint's own side (a known globus-compute-endpoint race" in n
            and "the Globus Compute service still lists it as queued" in n)
    # settled for good: off the record, and a later stop does not look again
    await warmth._flush_all(app)
    assert SentCheckStore().load(EID) == [] and EID not in app.sent_checks
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
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", block, first=datetime.now() - timedelta(seconds=58)))
    _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.notice.startswith(
        "nothing to cancel: the scheduler REJECTED the block's submission, so no block was created and none bills "
        "(released none). hpc-bridge's check for the block on the default partition (task check-1) cannot run")
    assert res.notice.endswith("still lists it as queued. The login endpoint stays online for reuse (reconnecting is "
                               "zero-SSH).")


@needs_posix_shell
async def test_a_check_whose_user_endpoint_is_still_alive_or_not_settled_keeps_the_stop_draining(monkeypatch, tmp_path):
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    started = datetime.now().replace(microsecond=0) - timedelta(seconds=58)

    async def stop(text_for):
        app, _runner, block = await _lost_check_app(monkeypatch)
        for p in tmp_path.glob("uep.*"):
            for f in p.iterdir():
                f.unlink()
            p.rmdir()
        d = _put(tmp_path, LOST, text_for(block))
        log = _login_node(monkeypatch, tmp_path)
        return app, d, log, await _stop_endpoint(app)

    # no bad-state line
    app, _d, log, res = await stop(lambda b: "\n".join(
        ln for ln in _uep_log("rejected.endpoint.log", b, first=started).splitlines() if "Setting bad state" not in ln))
    assert res.status == "draining" and res.spend_stopped is not True and "evidence" in _kinds(log)
    assert ("hpc-bridge also looked on the login node and cannot rule out that it can still start a block: its user "
            "endpoint's log shows no bad state since it last started.") in res.notice
    assert "(task check-1) is still queued on the endpoint" in res.notice
    assert [c.tid for c in app.sent_checks[EID]] == ["check-1"]                # still on the record
    # the bad state was logged BEFORE the check was sent (another life's): the check was sent 60 s ago
    app, _d, log, res = await stop(lambda b: _uep_log("rejected.endpoint.log", b,
                                                      first=started - timedelta(minutes=10)))
    assert res.status == "draining" and "bad state was logged before the check was sent" in res.notice
    # its process is still alive
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", f"uep.{EID}.{LOST}"])
    try:
        app, _d, log, res = await stop(lambda b: _uep_log("rejected.endpoint.log", b, first=started))
    finally:
        proc.kill()
        proc.wait()
    assert res.status == "draining" and "a process of its user endpoint is still running there" in res.notice
    # no user endpoint names the check's block request (not started yet — or its log is another request's)
    app, _d, log, res = await stop(lambda b: _uep_log("rejected.endpoint.log", b + 1, first=started))
    assert res.status == "draining" and "no user endpoint there names the check's block request yet" in res.notice


@needs_posix_shell
async def test_an_unreadable_log_keeps_the_stop_draining(monkeypatch, tmp_path):
    from hpc_bridge.server import _stop_endpoint

    if os.geteuid() == 0:
        pytest.skip("root reads a 0000 file")
    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch)
    started = datetime.now().replace(microsecond=0) - timedelta(seconds=58)
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", block, first=started))
    other = _put(tmp_path, DIED, _uep_log("worker_died.endpoint.log", 3, first=started))
    (other / "endpoint.log").chmod(0)
    _login_node(monkeypatch, tmp_path)
    try:
        res = await _stop_endpoint(app)
    finally:
        (other / "endpoint.log").chmod(0o600)
    assert res.status == "draining" and "a user endpoint's log there could not be read" in res.notice


@pytest.mark.parametrize("outcome, why", [
    ("cold", "the login channel was cold or busy"),
    ("busy", "the login channel was cold or busy"),
    ("failed", "the login channel was cold or busy"),
])
async def test_a_cold_or_busy_login_channel_keeps_the_stop_draining(monkeypatch, tmp_path, outcome, why):
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
    assert res.status == "draining" and why in res.notice and _kinds(log).count("evidence") == 1


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


async def test_a_lost_command_or_a_check_with_no_block_request_is_not_looked_at(monkeypatch, tmp_path):
    # a command runs the user's work: whatever its user endpoint's fate, a stop must not say it ended — and while it
    # waits nothing can make this stop `down`, so nothing is run
    from hpc_bridge import warmth
    from hpc_bridge.context import SentCheck
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, runner, block = await _lost_check_app(monkeypatch)
    warmth._keep_checks(app, EID, [SentCheck(future=None, client=runner.task_client(), task_id="cmd-1",
                                             sent_at=time.time() - 60, what="command", block=block,
                                             ceiling_at=time.time() - 1)])
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "evidence" not in _kinds(log)
    # a check read back with no block request (an older record): nothing names its user endpoint
    app, _runner, _block = await _lost_check_app(monkeypatch)
    app.shapes["compute"].runner.blocks.clear()
    log = _login_node(monkeypatch, tmp_path)
    assert (await _stop_endpoint(app)).status == "draining" and "evidence" not in _kinds(log)
    # a check with no task id may still be on its way to the service: taken after its user endpoint shut down, it would
    # have the manager start that user endpoint afresh — and a new life of it starts a new block. Never settled so.
    from tests.test_server import _PendingFuture

    app, runner, block = await _lost_check_app(monkeypatch)
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", block, first=datetime.now() - timedelta(seconds=58)))
    unsent = _PendingFuture()                                   # its submit has not come back: no task id yet
    runner.checks.append(unsent)
    runner.blocks[id(unsent)], runner.sent[id(unsent)] = block, time.time() - 60
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "evidence" not in _kinds(log)


@needs_posix_shell
async def test_a_restarted_server_reads_the_block_request_back_and_settles_the_check(monkeypatch, tmp_path):
    # the record carries each check's block request, so a server restarted after the check was lost still finds it
    from hpc_bridge import warmth
    from hpc_bridge.server import _stop_endpoint
    from hpc_bridge.state import SentCheckStore

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, _runner, block = await _lost_check_app(monkeypatch)
    await warmth._flush_all(app)
    [row] = SentCheckStore().load(EID)
    assert row["task_id"] == "check-1" and row["block"] == block
    fresh = AppCtx(facility=_UuidFacility(), profile=Profile(), state=EndpointState(endpoint_id=EID))
    fresh.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"},
                                         runner=_FakeRunner(EID, _Res(0, "", "")), warm_confirmed_at=time.monotonic())
    fresh.shapes["login"].runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}
    _put(tmp_path, LOST, _uep_log("rejected.endpoint.log", block, first=datetime.now() - timedelta(seconds=58)))
    log = _login_node(monkeypatch, tmp_path)
    res = await _stop_endpoint(fresh)
    assert res.status == "down" and "cannot run" in res.notice and _kinds(log).count("evidence") == 1
    await warmth._flush_all(fresh)
    assert SentCheckStore().load(EID) == []
    # an older record (no block request on the row) is not looked at
    SentCheckStore().save(EID, [{**row, "block": None}])
    again = AppCtx(facility=_UuidFacility(), profile=Profile(), state=EndpointState(endpoint_id=EID))
    again.shapes["login"] = fresh.shapes["login"]
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
