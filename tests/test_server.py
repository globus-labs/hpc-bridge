from pathlib import Path

from hpc_bridge import binding
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _run_shell, _shape_runtime, mcp
from tests.fakes import FakeFacility

# The Anvil SSH entry left the registry 2026-09-04 (Anvil is its multi-user endpoint there); the SSH startup-path
# tests below keep using the retired entry as a fixture.
_ANVIL_SSH_SEED = Path(__file__).parent / "catalog_fixtures" / "anvil-ssh.yaml"


def _confirm_slurm(app):
    """Acknowledge spend for the default billed (slurm) shape, as the budget gate would — so
    run_shell/reset tests exercise dispatch rather than tripping the deterministic spend floor."""
    _shape_runtime(app, "compute").spend_confirmed = True


class _Res:
    def __init__(self, rc, out, err):
        self.returncode = rc
        self.stdout = out
        self.stderr = err


class _DoneFuture:
    """An already-resolved future — the fast-task path: _run_shell gets the result within the
    sync-wait and returns 'complete'."""

    def __init__(self, res=None, exc=None):
        self._res, self._exc = res, exc

    def done(self):
        return True

    def cancelled(self):
        return False

    def result(self, timeout=None):
        if self._exc is not None:
            raise self._exc
        return self._res


class _PendingFuture:
    """A controllable future: starts not-done so result(timeout) raises TimeoutError (so _run_shell
    hands back a poll handle). finish()/fail()/cancel() resolve it for a later poll."""

    def __init__(self):
        self._res = None
        self._exc = None
        self._done = False
        self._cancelled = False

    def done(self):
        return self._done or self._cancelled

    def cancelled(self):
        return self._cancelled

    def result(self, timeout=None):
        if self._cancelled:
            from concurrent.futures import CancelledError

            raise CancelledError()
        if not self._done:
            raise TimeoutError()
        if self._exc is not None:
            raise self._exc
        return self._res

    def finish(self, res):
        self._res, self._done = res, True

    def fail(self, exc):
        self._exc, self._done = exc, True

    def cancel(self):
        self._cancelled = True


class _FakeRunner:
    def __init__(
        self, endpoint_id, res, *, canary_result=None, walltime=1780.0, timeout=120.0, pending=False
    ):
        self.endpoint_id = endpoint_id
        self._res = res
        self.walltime = walltime  # the per-task ceiling _run_shell records on a poll handle
        self.timeout = timeout  # the client sync-wait _run_shell blocks for before handing a handle
        self.pending = pending  # True -> submit() returns a not-done future (long task -> poll handle)
        # default: a live worker (so existing warm-path tests stay warm); pass a not-ok
        # canary_result to simulate the cold-start gap (manager up, no worker yet).
        self._canary = canary_result or CanaryResult(
            ok=True, worker_host="a070", worker_python="3.11.7", worker_dill="0.3.9"
        )
        self.closed = False
        self.commands = []
        self.futures = []
        self.canaries = 0

    def submit(self, command):
        self.commands.append(command)
        fut = _PendingFuture() if self.pending else _DoneFuture(self._res)
        self.futures.append(fut)
        return fut

    async def run(self, command):
        self.commands.append(command)
        return self._res

    async def canary(self, timeout=8.0):
        self.canaries += 1
        return self._canary

    def close(self):
        self.closed = True


async def test_ensure_endpoint_up_reports_up_when_warm():
    f = FakeFacility()
    f.workers = 1  # manager online; the canary (below) confirms a live worker
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and res.block_state == "warm"
    assert res.endpoint_id == "fake-eid"
    assert res.notice and "worker live" in res.notice  # warm => a worker answered, not just the manager


async def test_billed_compute_result_surfaces_its_idle_and_task_bounds():
    # #21 item 2 (prevention). A billed `compute` block still idle-releases after ~600s
    # (max_idletime, min_blocks=0), but a task is no longer capped at ~110s — it runs up to the block
    # walltime, and one that outlives the sync-wait returns a poll handle (poll_task) rather than being
    # cut. The warm result names the surviving bounds and the run-foreground-don't-detach guidance.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and res.block_state == "warm"  # a billed block is live
    low = (res.notice or "").lower()
    assert "idle" in low, f"warm compute notice must still name the idle-release; got {res.notice!r}"
    assert "poll" in low, f"warm compute notice must name the poll-handle behaviour; got {res.notice!r}"
    assert "detach" in low, f"warm compute notice must warn against detaching long work; got {res.notice!r}"
    assert "110" not in low, f"the ~110s per-task cap is gone in item 2; got {res.notice!r}"


def test_pilot_status_cmd_and_summarize():
    # #32: the read-only pilot query is scheduler-specific and marker-scoped; the summary maps the
    # scheduler state to a category (a rejected pilot -> "no pilot in the scheduler").
    from hpc_bridge.server import _pilot_status_cmd, _summarize_pilot
    assert "qstat -x -f" in _pilot_status_cmd("pbs", "E") and "uep.E" in _pilot_status_cmd("pbs", "E")
    slurm_cmd = _pilot_status_cmd("slurm", "E")
    assert "squeue" in slurm_cmd and "uep.E" in slurm_cmd
    # awk filters the marker (exits 0 on no-match) — no `grep`, whose non-zero no-match exit could
    # mask the empty "no pilot -> rejected" result under a pipefail shell.
    assert "grep" not in slurm_cmd and "index($0" in slurm_cmd
    from hpc_bridge.server import PROVISION_GRACE_S
    past = PROVISION_GRACE_S + 5
    # a VISIBLE pilot is categorized at once, regardless of the grace clock
    assert _summarize_pilot("R 42.aurora\n", 0)[0] == "starting"
    assert _summarize_pilot("Q 42.aurora\n", 0)[0] == "queued"
    assert _summarize_pilot("H 42.aurora\n", 0)[0] == "held"
    assert _summarize_pilot("PENDING 42\n", 0)[0] == "queued"   # slurm long-form
    assert _summarize_pilot("RUNNING 42\n", 0)[0] == "starting"
    # NO pilot: quiet during the cold-start grace (no false alarm), 'rejected' only past it
    early = _summarize_pilot("", 0)
    assert early[0] == "starting" and early[1] == ""
    late = _summarize_pilot("", past)
    assert late[0] == "rejected" and "REJECTED" in late[1] and "qstat" in late[1]


_REJECTED = CanaryResult(ok=False, error="GlobusAPIError: 400 user_endpoint_config failed validation")


async def test_rejected_submit_marks_runner_stale_and_surfaces_error():
    # A NON-timeout canary failure (the web service refused the submit — e.g. a user_endpoint_config
    # the endpoint's schema rejects) leaves the SDK Executor shut down; the runner must be marked
    # stale so the next call REBUILDS it rather than re-raising `Executor is shutdown` forever, and the
    # refusal must reach the caller — not vanish into a silent "allocating nodes…".
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        if not (user_endpoint_config or {}).get("compute"):  # the pilot-query login runner: warm, ignore
            return _FakeRunner(eid, _Res(0, "", ""))
        r = _FakeRunner(eid, _Res(0, "", ""), canary_result=_REJECTED)
        built.append(r)
        return r

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "failed validation" in (res.notice or ""), f"the refusal must be surfaced; got {res.notice!r}"
    assert "not a queue wait" in (res.notice or "").lower()
    rt = _shape_runtime(app, "compute")
    assert rt.last_canary is _REJECTED  # failures are kept, not dropped
    assert rt.runner_stale is True
    # next call: the bricked runner is closed and a fresh one built (not the same shut-down Executor)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert len(built) == 2 and built[0].closed is True


async def test_timeout_canary_stays_a_quiet_cold_start():
    # a plain timeout IS the normal cold-start wait: no error suffix, no runner rebuild
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        if not (user_endpoint_config or {}).get("compute"):  # the pilot-query login runner: warm, ignore
            return _FakeRunner(eid, _Res(0, "", ""))
        r = _FakeRunner(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
        built.append(r)
        return r

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "last dispatch failed" not in (res.notice or "")
    assert _shape_runtime(app, "compute").runner_stale is False
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert len(built) == 1  # same runner reused — a cold block is not a broken runner


async def test_run_shell_cold_outcome_carries_dispatch_error():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=_REJECTED
    )
    _shape_runtime(app, "compute").spend_confirmed = True
    out = await _run_shell(app, "echo hi", shape="compute")
    assert out.phase == "cold_start"
    assert "failed validation" in (out.notice or "")


def _split_shape_factory(login_res):
    """A runner_factory whose COMPUTE runner fails the canary (-> provisioning) while its LOGIN
    runner is warm and returns `login_res` from run() — so the #32 pilot query has something to read."""
    def factory(eid, user_endpoint_config=None, **_kw):  # **_kw absorbs walltime/timeout (#31 _runner_for)
        if user_endpoint_config and user_endpoint_config.get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
        return _FakeRunner(eid, login_res)
    return factory


async def test_provisioning_billed_block_surfaces_pilot_rejection():
    # #32: PAST the cold-start grace, a billed block with NO pilot in the scheduler gets the rejection
    # hint (not a silent "allocating…"). We pre-age the grace clock to land past the window.
    import time as _time

    from hpc_bridge.server import PROVISION_GRACE_S
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = _split_shape_factory(_Res(0, "", ""))  # empty pilot query -> no pilot
    _shape_runtime(app, "compute").provisioning_since = _time.monotonic() - (PROVISION_GRACE_S + 10)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "REJECTED" in (res.notice or ""), f"expected a rejection hint past grace; got {res.notice!r}"


async def test_provisioning_within_grace_does_not_cry_rejection():
    # #32 (the globus1 false-alarm fix): during the normal cold-start window a not-yet-visible pilot
    # must NOT be called rejected — the notice stays the plain "allocating…".
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = _split_shape_factory(_Res(0, "", ""))  # no pilot visible yet
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)  # fresh -> elapsed ~0
    assert res.status == "provisioning"
    assert "REJECTED" not in (res.notice or "") and "allocating nodes" in (res.notice or "")


async def test_provisioning_notice_reports_queued_pilot():
    # When the pilot IS queued, the notice says so (an honest wait, not a false rejection).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = _split_shape_factory(_Res(0, "PENDING 8675991\n", ""))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "queued" in (res.notice or "") and "8675991" in (res.notice or "")


async def test_ensure_endpoint_up_provisioning_when_manager_up_but_worker_cold():
    # The canary gap: manager_online() True but no worker answers -> NOT warm. Without the
    # canary this wrongly reported 'up' and the next run_shell 124'd on a cold start.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout")
    )
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning" and res.block_state == "provisioning"
    assert res.notice and "allocating" in res.notice.lower()


async def test_ensure_endpoint_up_reports_provisioning_when_cold():
    f = FakeFacility()
    f.workers = 0
    app = AppCtx(facility=f, profile=Profile())
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning"
    assert res.notice and "allocating" in res.notice.lower()


async def test_server_registers_ensure_endpoint_up_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "ensure_endpoint_up" for t in tools)


async def test_run_shell_warm_returns_complete_outcome():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "hi\n", ""))
    _confirm_slurm(app)
    out = await _run_shell(app, "echo hi")
    assert out.phase == "complete"
    assert out.exit_code == 0 and out.stdout == "hi\n"
    assert out.block_state == "warm"


async def test_run_shell_cold_returns_cold_start():
    f = FakeFacility()
    f.workers = 0
    app = AppCtx(facility=f, profile=Profile())
    _confirm_slurm(app)  # spend ack'd, so we reach the cold-block path (not the spend floor)
    out = await _run_shell(app, "echo hi")
    assert out.phase == "cold_start"
    assert out.notice and "allocating" in out.notice.lower()


async def test_run_shell_cold_start_when_worker_not_registered():
    # Manager online but the canary fails -> cold_start, and the command must NOT be dispatched
    # into the void (no run() call) where it would hang for the full dispatch timeout.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    out = await _run_shell(app, "echo hi")
    assert out.phase == "cold_start"
    assert runner.canaries == 1 and runner.commands == []  # canaried, never dispatched


async def test_canary_ttl_skips_repeat_canary_on_hot_path():
    # Two run_shells in quick succession: the first canaries, the second trusts the <45s TTL
    # (and a successful dispatch refreshes it) so interactive bursts don't pay the round-trip.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "ok\n", ""))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    await _run_shell(app, "echo a")
    await _run_shell(app, "echo b")
    assert runner.canaries == 1  # second call skipped the canary
    assert len(runner.commands) == 2  # both commands still dispatched


async def test_run_shell_login_shape_uses_localprovider_config():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    seen = {}

    def factory(eid, user_endpoint_config=None, **_kw):
        seen["uec"] = user_endpoint_config
        return _FakeRunner(eid, _Res(0, "", ""))

    app.runner_factory = factory
    await _run_shell(app, "echo hi", shape="login")
    assert seen["uec"]["provider_type"] == "LocalProvider"


async def test_two_shapes_keep_independent_runners():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    await _run_shell(app, "echo a", shape="login")
    await _run_shell(app, "echo b", shape="compute")
    assert set(app.shapes) == {"login", "compute"}
    assert app.shapes["login"].runner is not app.shapes["compute"].runner


async def test_server_registers_run_shell_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "run_shell" for t in tools)


async def test_run_shell_wraps_command_with_session_shim():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    await _run_shell(app, "make", session_id="s1")
    sent = runner.commands[-1]
    assert "sessions/s1" in sent  # routed through the session dir
    assert ".cwd" in sent  # shim rehydrates/persists cwd
    assert "base64 -d" in sent  # command carried inertly, not raw


async def test_reset_session_dispatches_reset_command():
    from hpc_bridge.server import _reset_session

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    await _reset_session(app, "s1")
    sent = runner.commands[-1]
    assert sent.startswith("rm -f")
    assert "sessions/s1" in sent


async def test_server_registers_reset_session_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "reset_session" for t in tools)


async def test_run_shell_rejects_traversal_session_id():
    import pytest

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    with pytest.raises(ValueError):
        await _run_shell(app, "echo hi", session_id="../../etc")


async def test_byo_endpoint_skips_provisioning():
    # HPC_BRIDGE_ENDPOINT_ID seeds the state, so the server dispatches to an existing
    # endpoint and never provisions a local one (the macOS / remote-endpoint path).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile(), state=EndpointState(endpoint_id="byo-uuid"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and res.endpoint_id == "byo-uuid"
    assert f.provisioned is False


def test_env_endpoint_id_reads_and_trims(monkeypatch):
    from hpc_bridge.server import _env_endpoint_id

    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_ID", raising=False)
    assert _env_endpoint_id() is None
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "  ep-42  ")
    assert _env_endpoint_id() == "ep-42"
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "   ")
    assert _env_endpoint_id() is None


async def test_ensure_endpoint_up_reports_down_on_provision_failure():
    # A non-Linux host (or any provisioning error) yields a structured 'down', not a crash.
    class BoomFacility(FakeFacility):
        async def provision(self, profile):
            raise RuntimeError("globus-compute-endpoint runs only on Linux")

    app = AppCtx(facility=BoomFacility(), profile=Profile())  # cold -> provisions -> boom
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "down"
    assert res.notice and "Linux" in res.notice


async def test_stop_releases_block_over_login_and_keeps_endpoint(monkeypatch):
    # Option A: stop `scancel`s the block over the LOGIN shape (AMQP, no SSH) and LEAVES the manager
    # online for reuse — it must NOT call the facility teardown / gce stop, and must keep the
    # endpoint_id + login shape so a reconnect is zero-SSH.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    class _NoTeardown(FakeFacility):
        async def teardown(self, eid):  # must NOT be called under Option A
            raise AssertionError("stop must not tear the endpoint down")

    f = _NoTeardown()
    app = AppCtx(facility=f, profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=slurm_runner)
    app.shapes["login"] = ShapeRuntime(
        user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0
    )
    seen = {}

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        seen["shape"], seen["cmd"] = shape, command
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 123\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    res = await _stop_endpoint(app)
    assert seen["shape"] == "login"  # the scancel rode AMQP, not SSH
    assert "scancel" in seen["cmd"] and "uep.eid-1" in seen["cmd"]
    assert "compute" not in app.shapes  # billed shape dropped -> a later run re-provisions fresh
    assert slurm_runner.closed  # its (now-dead) runner was closed
    assert "login" in app.shapes  # login shape kept (warm, free, for cheap reconnect)
    assert app.state.endpoint_id == "eid-1"  # endpoint NOT torn down
    assert res.status == "down"  # cancel CONFIRMED (login channel was warm) -> honest "down"
    assert res.endpoint_id == "eid-1" and res.block_state == "cold"
    assert "online for reuse" in (res.notice or "")


async def test_stop_is_honest_when_release_channel_is_cold(monkeypatch):
    # Issue #24: if the login release channel is cold, the scancel dispatch comes back non-complete
    # ("allocating nodes…"), so the cancel is NOT confirmed. stop_endpoint must NOT then claim
    # status="down" (an agent reading that walks away while the block keeps burning). It reports the
    # honest status="draining" and says spend is not confirmed stopped — idle-release backstops it.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")  # no real sleeps in the retry loop
    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.facility.manager_up = True  # the MANAGER is online; only the login WORKER is cold (not a gone endpoint)
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=slurm_runner)
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    calls = {"n": 0}

    async def cold_run_shell(a, command, session_id="default", shape="compute"):
        calls["n"] += 1  # login worker never warms -> every dispatch is a cold_start (not complete)
        return ShellOutcome(phase="cold_start", block_state="cold", notice="allocating nodes…")

    monkeypatch.setattr(server, "_run_shell", cold_run_shell)
    res = await _stop_endpoint(app)
    assert res.status == "draining"  # honest: NOT "down" while the cancel is unconfirmed
    assert res.status not in ("down", "stopped")
    assert "not confirmed" in (res.notice or "").lower()
    assert calls["n"] >= 2  # it RETRIED the cold channel rather than giving up on the first miss
    assert "compute" not in app.shapes and slurm_runner.closed  # billed shape still dropped (spend clock banked)


async def test_stop_during_provisioning_is_draining_not_down(monkeypatch):
    # The stop-during-provisioning race (spend_revoked, 2026-09-05): the block was REQUESTED
    # (spend_confirmed) but never CONFIRMED running (warm_confirmed_at is None), and the scancel finds
    # no job yet ("released none") because parsl's sbatch is still in flight. Claiming "down" would leak
    # a pilot that lands a moment later; stop_endpoint must report "draining" so the agent re-stops.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")  # no real sleeps in the provisioning poll
    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.facility.manager_up = True  # the endpoint is up (not orphaned) — only the block isn't visible yet
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(
        user_endpoint_config={"compute": True}, runner=slurm_runner,
        spend_confirmed=True, warm_confirmed_at=None,  # requested, never confirmed running
    )
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)

    async def released_nothing(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released none\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", released_nothing)
    res = await _stop_endpoint(app)
    assert res.status == "draining"  # NOT "down": the pilot never appeared in the window, may still be landing
    assert "still being submitted" in (res.notice or "") and "stop_endpoint again" in (res.notice or "")

    # RECOVERY: the release POLLS during provisioning — "released none" first, then the pilot lands and is cancelled
    app3 = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-3"))
    app3.shapes["compute"] = ShapeRuntime(
        user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-3", _Res(0, "", "")),
        spend_confirmed=True, warm_confirmed_at=None,
    )
    app3.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    seq = ["released none\n", "released none\n", "released 12345\n"]  # pilot lands on the 3rd poll

    async def lands_then_cancelled(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout=seq.pop(0) if seq else "released none\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", lands_then_cancelled)
    res3 = await _stop_endpoint(app3)
    assert res3.status == "down" and "released 12345" in (res3.notice or "")  # caught the late pilot in ONE stop call

    # control: a block CONFIRMED running (warm) whose scancel finds nothing (it already finished) is honest "down"
    app2 = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-2"))
    app2.shapes["compute"] = ShapeRuntime(
        user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-2", _Res(0, "", "")),
        spend_confirmed=True, warm_confirmed_at=1.0,  # was confirmed running
    )
    app2.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    monkeypatch.setattr(server, "_run_shell", released_nothing)
    assert (await _stop_endpoint(app2)).status == "down"


async def test_stop_retries_cold_channel_then_confirms(monkeypatch):
    # The first dispatch wakes the cold login worker (returns cold_start); a bounded retry catches
    # it once warm and CONFIRMS the cancel -> honest "down". This is the common, recoverable case.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-1", _Res(0, "", "")))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    calls = {"n": 0}

    async def warming_run_shell(a, command, session_id="default", shape="compute"):
        calls["n"] += 1
        if calls["n"] == 1:  # cold on the first hit (worker scaled in) ...
            return ShellOutcome(phase="cold_start", block_state="cold", notice="allocating nodes…")
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 456\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", warming_run_shell)
    res = await _stop_endpoint(app)
    assert res.status == "down"  # ... confirmed on retry
    assert calls["n"] == 2 and "online for reuse" in (res.notice or "")


# --- cancel: qdel/qstat on the cost-critical stop path (PBS) --------------------------------


def test_release_cmd_pbs_uses_qstat_and_qdel():
    from hpc_bridge.server import _release_cmd

    cmd = _release_cmd("pbs", "abc-123")
    assert "qstat -f" in cmd and "qdel" in cmd
    assert "uep.abc-123" in cmd
    assert "scancel" not in cmd and "squeue" not in cmd
    # Must NOT filter qstat -f by -u: PBS Pro yields empty full-format output with -u, silently
    # no-opping the cancel (live Polaris bug). Bare `qstat -f` + the unique marker scopes it.
    assert "-u" not in cmd


def test_release_cmd_slurm_uses_squeue_and_scancel():
    from hpc_bridge.server import _release_cmd

    cmd = _release_cmd("slurm", "abc-123")
    assert "squeue" in cmd and "scancel" in cmd and "uep.abc-123" in cmd


def test_release_cmd_slurm_matches_prior_inline_command():
    # Byte-for-byte: the extracted helper must build the EXACT string the old inline
    # marker=...+cmd=(...) block in _release_blocks_over_login used to build.
    import shlex

    from hpc_bridge.server import _release_cmd

    eid = "eid-1"
    marker = shlex.quote(f"uep.{eid}")
    expected = (
        'ids=$(squeue -u "$USER" -h -O "JobID:30,StdOut:1024" 2>/dev/null '
        f"| grep -F {marker} | awk '{{print $1}}'); "
        '[ -n "$ids" ] && scancel $ids; echo "released ${ids:-none}"'
    )
    assert _release_cmd("slurm", eid) == expected


async def test_stop_dispatches_pbs_release_cmd_when_facility_scheduler_is_pbs(monkeypatch):
    # _release_blocks_over_login must branch on the facility's scheduler (PBS uses
    # qstat/qdel, never squeue/scancel) so a PBS block is actually cancelled, not silently
    # missed by a Slurm-only command.
    from types import SimpleNamespace

    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    class _PbsFacility(FakeFacility):
        def __init__(self):
            super().__init__()
            self.profile = SimpleNamespace(scheduler="pbs")

    app = AppCtx(facility=_PbsFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-1", _Res(0, "", "")))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    seen = {}

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        seen["cmd"] = command
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 123\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    await _stop_endpoint(app)
    assert "qstat -f" in seen["cmd"] and "qdel" in seen["cmd"]
    assert "scancel" not in seen["cmd"] and "squeue" not in seen["cmd"]
    assert "uep.eid-1" in seen["cmd"]


async def test_teardown_endpoint_stops_manager_and_clears_state(monkeypatch):
    # teardown_endpoint (the explicit "destroy it") releases the block, calls the facility teardown
    # (gce stop + delete), and clears ALL shape/state so a stray run_shell can't revive a stale endpoint.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    torn = []
    wiped = []

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(eid)
            wiped.append(wipe_credentials)

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=slurm_runner)
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 1\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    res = await _teardown_endpoint(app)
    assert torn == ["eid-1"]  # the facility teardown (gce stop + delete) was invoked
    assert wiped == [True]  # and the seeded Globus token copy on the login node goes with it (review 2026-09-04, B-03)
    assert res.status == "down" and "torn down" in (res.notice or "")
    assert app.shapes == {} and app.state.endpoint_id is None  # ALL state cleared (no stale revive)
    assert slurm_runner.closed


async def test_teardown_on_a_one_time_code_facility_asks_for_the_code_before_any_ssh(monkeypatch):
    # Teardown is the one post-bootstrap op that must SSH the login node. On an mfa-otp facility with no shared
    # connection open it must ask for the code FIRST (the connect handoff), not let stop/delete fail their
    # BatchMode logins and then report "DELETE FAILED" about an endpoint that is still running.
    from hpc_bridge import connect, server
    from hpc_bridge.facility.remote import SshTarget
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    torn = []
    target = SshTarget(host="login02.expanse.sdsc.edu", user="u", control_dir="/tmp/cm",
                       host_key_alias="login.expanse.sdsc.edu")

    class _F(FakeFacility):
        auth_method = "mfa-otp"

        class cli:  # mirrors RemoteEndpointCLI's attribute shape (lower-case on purpose)
            pass

        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(eid)

    _F.cli.target = target
    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.machine = "expanse"
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=slurm_runner)
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 1\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    monkeypatch.setattr(connect, "_master_alive", lambda t: False)
    res = await _teardown_endpoint(app)
    assert torn == []  # no SSH op was attempted
    assert res.status == "up" and res.endpoint_id == "eid-1" and "STILL RUNNING" in res.notice
    assert "complete_preauth" in res.notice and "teardown_endpoint again" in res.notice
    assert app.pending_preauth == ("expanse", target) and app.preauth_resume == "teardown_endpoint()"
    assert app.state.endpoint_id == "eid-1" and "login" in app.shapes  # nothing cleared: the endpoint is still up
    # with the connection open (the code went through) the same call finishes the job
    monkeypatch.setattr(connect, "_master_alive", lambda t: True)
    res = await _teardown_endpoint(app)
    assert torn == ["eid-1"] and res.status == "down" and app.state.endpoint_id is None


async def test_teardown_hands_back_tearing_down_when_the_login_node_ops_outlive_the_wait(monkeypatch):
    # Expanse live 2026-09-04: gce stop + delete took ~3 min and the tool call fell into the client's 120 s
    # background rescue. The ops now run in a server-side task: one call waits a bounded time and returns
    # `tearing_down`; the next call reports the finished result and the state is cleared only then.
    import asyncio

    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    release = asyncio.Event()
    torn = []

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            await release.wait()
            torn.append(eid)
            return {"deleted": True, "credentials_wiped": True, "ssh_closed": True}

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 0\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    monkeypatch.setattr(server, "_TEARDOWN_SYNC_WAIT_S", 0.05)
    res = await _teardown_endpoint(app)
    assert res.status == "tearing_down" and res.endpoint_id == "eid-1" and "call teardown_endpoint again" in res.notice.lower()
    assert torn == [] and app.teardown_task is not None and app.state.endpoint_id == "eid-1"  # in flight; nothing cleared
    res = await _teardown_endpoint(app)  # still running: waits again, same answer, does NOT start a second teardown
    assert res.status == "tearing_down" and torn == []
    release.set()
    res = await _teardown_endpoint(app)
    assert res.status == "down" and torn == ["eid-1"] and app.teardown_task is None
    assert "SSH connection to the login node was closed" in res.notice
    assert app.shapes == {} and app.state.endpoint_id is None  # cleared once the ops finished


async def test_finished_teardown_does_not_clear_a_facility_bound_meanwhile(monkeypatch):
    # the agent was told not to connect meanwhile; if it does anyway, the late teardown must not wipe the NEW binding
    import asyncio

    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    release = asyncio.Event()

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            await release.wait()
            return {"deleted": True, "credentials_wiped": False}

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 0\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    monkeypatch.setattr(server, "_TEARDOWN_SYNC_WAIT_S", 0.05)
    assert (await _teardown_endpoint(app)).status == "tearing_down"
    app.state = EndpointState(endpoint_id="eid-2")  # a connect_facility meanwhile bound a fresh endpoint
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    release.set()
    res = await _teardown_endpoint(app)
    assert res.status == "down" and res.endpoint_id == "eid-1"
    assert app.state.endpoint_id == "eid-2" and "login" in app.shapes  # the new binding survives


def _ssh_fake_facility(teardown_report=None, *, auth_method="ssh-key", torn=None):
    from hpc_bridge.facility.remote import SshTarget

    target = SshTarget(host="login02.example.edu", user="u", control_dir="/tmp/cm", host_key_alias="login.example.edu")

    class _F(FakeFacility):
        class cli:  # mirrors RemoteEndpointCLI's attribute shape
            pass

        async def teardown(self, eid, *, wipe_credentials=False):
            if torn is not None:
                torn.append(eid)
            return teardown_report

    _F.cli.target = target
    _F.auth_method = auth_method
    return _F(), target


async def _teardown_app(monkeypatch, fac):
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime

    app = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.machine = "byo-mfa"
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 0\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    return app


async def test_teardown_ssh_failure_is_reported_as_failed_and_keeps_the_endpoint_bound(monkeypatch):
    # review 2026-09-05 Fix-now #1: a BYO MFA facility whose shared connection expired used to be reported
    # "down … token copy removed" from booleans nobody measured, and the record was deleted. Now: the master
    # is dead, the probe's denial offers a second factor -> the one-time-code handoff, nothing torn down.
    from hpc_bridge import connect, server
    from hpc_bridge.server import _teardown_endpoint

    torn = []
    fac, target = _ssh_fake_facility({"stopped": True, "deleted": True, "credentials_wiped": True,
                                      "ssh_closed": True, "ssh_failed": False, "error": ""}, torn=torn)
    app = await _teardown_app(monkeypatch, fac)
    monkeypatch.setattr(connect, "_master_alive", lambda t: False)

    async def denied(t):
        return 255, "u@login02.example.edu: Permission denied (gssapi-with-mic,keyboard-interactive,hostbased)."

    monkeypatch.setattr(server, "_probe_login_node", denied)
    res = await _teardown_endpoint(app)
    assert torn == [] and res.status == "up" and "STILL RUNNING" in res.notice and "complete_preauth" in res.notice
    assert app.pending_preauth == ("byo-mfa", target) and app.preauth_resume == "teardown_endpoint()"
    assert app.state.endpoint_id == "eid-1" and "login" in app.shapes  # nothing cleared
    # a denial WITHOUT a second factor (key refused / host down) is a plain failed teardown, no code asked
    app.pending_preauth = None

    async def down(t):
        return 255, "ssh: connect to host login02.example.edu port 22: Connection timed out"

    monkeypatch.setattr(server, "_probe_login_node", down)
    res = await _teardown_endpoint(app)
    assert torn == [] and res.status == "up" and res.notice.startswith("TEARDOWN FAILED") and "CANNOT REACH" in res.notice
    assert app.pending_preauth is None and app.state.endpoint_id == "eid-1"

    async def ok(t):  # the key works: no gate, teardown proceeds and finishes
        return 0, ""

    monkeypatch.setattr(server, "_probe_login_node", ok)
    res = await _teardown_endpoint(app)
    assert torn == ["eid-1"] and res.status == "down" and "SSH connection to the login node was closed" in res.notice


async def test_teardown_report_ssh_failed_from_the_facility_is_honest(monkeypatch):
    # the gate passed (master alive) but the facility's own stop hit rc 255 anyway (connection died in between)
    from hpc_bridge import connect
    from hpc_bridge.server import _teardown_endpoint

    fac, _target = _ssh_fake_facility({"stopped": False, "deleted": False, "credentials_wiped": False,
                                      "ssh_closed": False, "ssh_failed": True,
                                      "error": "Permission denied (keyboard-interactive)."})
    app = await _teardown_app(monkeypatch, fac)
    monkeypatch.setattr(connect, "_master_alive", lambda t: True)
    res = await _teardown_endpoint(app)
    assert res.status == "up" and res.notice.startswith("TEARDOWN FAILED") and "still in place" in res.notice
    assert app.state.endpoint_id == "eid-1" and "login" in app.shapes  # bound: a retry can finish it
    assert app.pending_preauth is not None  # the denial offered a second factor -> the handoff is armed


async def test_teardown_stop_failure_is_not_reported_as_down(monkeypatch):
    from hpc_bridge import connect
    from hpc_bridge.server import _teardown_endpoint

    fac, _t = _ssh_fake_facility({"stopped": False, "deleted": False, "credentials_wiped": False,
                                  "ssh_closed": True, "ssh_failed": False, "error": "psutil traceback"})
    app = await _teardown_app(monkeypatch, fac)
    monkeypatch.setattr(connect, "_master_alive", lambda t: True)
    res = await _teardown_endpoint(app)
    assert res.status == "up" and "still reports running" in res.notice and app.state.endpoint_id == "eid-1"


async def test_teardown_on_a_key_facility_never_gates(monkeypatch):
    # ssh-key facilities (the default) tear down straight away — no master check, no handoff
    from hpc_bridge import connect, server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    torn = []

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(eid)

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 0\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    monkeypatch.setattr(connect, "_master_alive", lambda t: (_ for _ in ()).throw(AssertionError("must not probe")))
    res = await _teardown_endpoint(app)
    assert torn == ["eid-1"] and res.status == "down"


# --- partition loop: the discovery gate's selection -> provisioning -------------------------


async def test_ensure_endpoint_up_provisions_onto_selected_partition():
    # The gate's selection flows into the shape's user_endpoint_config (the per-task render var)
    # and is echoed back on the status.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, partition="shared", confirm_spend=True)
    assert res.partition == "shared"
    assert app.shapes["compute"].user_endpoint_config["partition"] == "shared"


async def test_partition_change_invalidates_runner():
    # Changing partition means a different Slurm block: the cached Executor captured the old
    # partition at build time, so the runner must be rebuilt (and the old one torn down).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "", ""))
        built.append(r)
        return r

    app.runner_factory = factory
    await _ensure_endpoint_up(app, partition="shared", confirm_spend=True)
    r1 = app.shapes["compute"].runner
    await _ensure_endpoint_up(app, partition="gpu", confirm_spend=True)
    r2 = app.shapes["compute"].runner
    assert r2 is not r1  # runner rebuilt for the new partition
    assert r1.closed  # old runner torn down (its block idle-releases via min_blocks=0)
    assert app.shapes["compute"].user_endpoint_config["partition"] == "gpu"


async def test_no_partition_is_noop_and_persists_previous_selection():
    # Omitting partition keeps the prior selection (or facility default) and does NOT churn the
    # runner — the selection persists for the session.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)
    r1 = app.shapes["compute"].runner
    res = await _ensure_endpoint_up(app)  # no partition, already confirmed -> no-op
    assert app.shapes["compute"].runner is r1  # runner NOT rebuilt
    assert not r1.closed
    assert app.shapes["compute"].user_endpoint_config["partition"] == "debug"  # selection persisted
    assert res.partition == "debug"


async def test_ensure_endpoint_up_rejects_invalid_partition():
    # A partition is agent/user-supplied and renders into a remote Jinja template -> reject any
    # token with shell/YAML metacharacters at the boundary, before touching any state.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    res = await _ensure_endpoint_up(app, partition="bad; rm -rf /")
    assert res.status == "down"
    assert res.notice and "invalid partition" in res.notice
    assert not f.provisioned  # rejected before any provisioning
    assert "compute" not in app.shapes  # no shape state was mutated


async def test_login_shape_ignores_partition():
    # A LocalProvider (login) shape has no partition: a supplied one is ignored (not forced onto
    # the config) and the status says so.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, shape="login", partition="shared")
    uec = app.shapes["login"].user_endpoint_config
    assert "partition" not in uec  # not forced onto a LocalProvider config
    assert res.partition is None
    assert res.notice and "login shape has no partition" in res.notice


async def test_stop_endpoint_noop_when_nothing_up():
    from hpc_bridge.server import _stop_endpoint

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await _stop_endpoint(app)
    assert res.status == "down" and "no endpoint" in (res.notice or "")


async def test_server_registers_stop_endpoint_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "stop_endpoint" for t in tools)


async def test_make_facility_rejects_removed_facility_env(monkeypatch):
    import pytest

    from hpc_bridge.server import make_facility

    # HPC_BRIDGE_FACILITY was removed — machines are catalog data now. Setting it without
    # HPC_BRIDGE_MACHINE fails loudly (a migration aid), not a silent fallback to local.
    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_FACILITY", "anvil")
    with pytest.raises(RuntimeError, match="HPC_BRIDGE_FACILITY was removed"):
        await make_facility()


async def test_lifespan_boots_unbound_when_facility_setup_fails(monkeypatch, capsys):
    # A startup crash would silently register no tools (the agent just sees "no hpc-bridge tools"),
    # so lifespan must catch a failed make_facility (here: the removed HPC_BRIDGE_FACILITY) and boot
    # local/unbound instead — the agent then binds a machine via connect_facility.
    from hpc_bridge.server import lifespan, mcp

    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_FACILITY", "anvil")
    async with lifespan(mcp) as app:
        assert app.facility.name == "local"
    assert "facility setup failed at startup" in capsys.readouterr().err


async def test_make_facility_sources_ssh_user_from_config_when_env_absent(monkeypatch):
    # SSH creds are NO LONGER required boot-env vars (they don't reach an already-running server):
    # with HPC_BRIDGE_SSH_USER/KEY absent, the login name comes from ~/.ssh/config (`ssh -G`) and the
    # key defers to the config's IdentityFile. (Account is separate — still env-pinned at startup.)
    import hpc_bridge.state as state_mod
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.server import make_facility

    class _NoPinStore:
        def __init__(self, *a, **k):
            pass

        def get(self, *, alias, name):
            return None

    monkeypatch.setattr(state_mod, "LoginNodeStore", _NoPinStore)
    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.setattr(binding, "_ssh_config_user", lambda host: "cfg-user")  # from ~/.ssh/config
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "anvil")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "ACC")  # the one boot value still required (billing)
    for v in ("HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY", "HPC_BRIDGE_SSH_HOST"):
        monkeypatch.delenv(v, raising=False)
    fac = await make_facility()
    assert fac.name == "anvil"
    assert fac.cli.target.user == "cfg-user"  # login name from ssh_config, not an env var
    assert fac.cli.target.key_path is None  # key deferred to ~/.ssh/config IdentityFile
    assert "/home/cfg-user/hpc-bridge/gce-venv/bin/activate" in fac.profile.env_setup  # templated


def test_ssh_config_user_parses_ssh_dash_g(monkeypatch):
    import subprocess

    from hpc_bridge.server import _ssh_config_user

    class _R:
        stdout = "hostname globus1.cs.uchicago.edu\nuser glabs\nidentityfile ~/.ssh/globus\n"

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _R())
    assert _ssh_config_user("globus1") == "glabs"  # read the User ~/.ssh/config resolves


def test_ssh_config_user_falls_back_to_local_user(monkeypatch):
    import getpass
    import subprocess

    from hpc_bridge.server import _ssh_config_user

    def _boom(*a, **k):
        raise FileNotFoundError("no ssh")

    monkeypatch.setattr(subprocess, "run", _boom)
    assert _ssh_config_user("whatever") == getpass.getuser()  # ssh missing -> local username


async def test_make_facility_defaults_local(monkeypatch):
    from hpc_bridge.server import make_facility

    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    assert (await make_facility()).name == "local"


async def test_make_facility_reconnects_to_pinned_login_node(monkeypatch):
    import hpc_bridge.state as state_mod
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.server import make_facility
    from hpc_bridge.state import EndpointRecord

    rec = EndpointRecord(
        endpoint_id="eid", login_host="login05.anvil.rcac.purdue.edu",
        alias="anvil.rcac.purdue.edu", user="x-u", key_path="/tmp/k",
        name="hpc-bridge", provisioned_at="2026-06-06T00:00:00Z",
    )

    class _FakeStore:
        def __init__(self, *a, **k):
            pass

        def get(self, *, alias, name):
            return rec

    monkeypatch.setattr(state_mod, "LoginNodeStore", _FakeStore)
    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "purdue:anvil")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "x-u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/tmp/k")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "ACC")
    fac = await make_facility()
    assert fac.cli.target.host == "login05.anvil.rcac.purdue.edu"  # rebound to pinned node


async def test_make_facility_builds_from_catalog_when_machine_set(monkeypatch):
    # HPC_BRIDGE_MACHINE sources the profile from the catalog (bundled seed here; the live
    # Globus Search index when HPC_BRIDGE_SEARCH_INDEX is set). FACILITY is unset, so a slurm
    # "anvil" facility can ONLY come from the catalog branch.
    import hpc_bridge.state as state_mod
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.server import make_facility

    class _NoPinStore:  # isolate from any real ~/.hpc-bridge/endpoints.json on this machine
        def __init__(self, *a, **k):
            pass

        def get(self, *, alias, name):
            return None

    monkeypatch.setattr(state_mod, "LoginNodeStore", _NoPinStore)
    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "purdue:anvil")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "x-u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/tmp/k")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "ACC")
    fac = await make_facility()
    assert fac.name == "anvil"  # profile.name == entry.id
    assert fac.cli.target.host == "anvil.rcac.purdue.edu"  # from entry.ssh_host
    assert fac.cli.target.user == "x-u"
    assert "{venv}" not in fac.profile.env_setup  # template resolved
    assert "/home/x-u/hpc-bridge/gce-venv/bin/activate" in fac.profile.env_setup
    assert fac.profile.scratch_root == "/anvil/scratch/x-u/.hpc-bridge"


async def test_make_facility_catalog_unknown_machine_errors(monkeypatch):
    import pytest

    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.server import make_facility

    # Inject the seed loader as the catalog: an unknown machine is a hard "not found", not a
    # silent fallback.
    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "nope:nope")
    with pytest.raises(RuntimeError, match="not found"):
        await make_facility()


def test_billing_banks_warm_interval_across_idle_release(monkeypatch):
    # The canary makes warm_since track a TRUE worker, so the clock stops on idle release.
    # Spend must (a) exclude the idle gap (no over-report) and (b) retain prior warm time
    # (no under-report) — i.e. accrue across intervals.
    import hpc_bridge.server as srv
    from hpc_bridge.server import ShapeRuntime

    clock = {"t": 1000.0}
    monkeypatch.setattr(srv.time, "monotonic", lambda: clock["t"])
    app = AppCtx(facility=FakeFacility(), profile=Profile(nodes_per_block=1), charge_factor=1.0)
    rt = ShapeRuntime(user_endpoint_config={})

    srv._settle_billing(rt, app, "warm")  # worker confirmed at t=1000
    assert rt.warm_since == 1000.0
    clock["t"] += 3600  # held warm for 1h
    srv._settle_billing(rt, app, "provisioning")  # idle release: bank 1.0 node-hour, stop the clock
    assert rt.warm_since is None and abs(rt.spend_accrued - 1.0) < 1e-9
    clock["t"] += 7200  # 2h cold — must NOT be billed
    srv._settle_billing(rt, app, "warm")  # warm again
    clock["t"] += 1800  # +0.5h
    assert abs(srv._session_spend(rt, app) - 1.5) < 1e-9  # 1.0 banked + 0.5 current; idle gap excluded


def test_login_shape_is_not_billed(monkeypatch):
    # LocalProvider (login) shapes consume no allocation: the spend clock never starts.
    import hpc_bridge.server as srv
    from hpc_bridge.server import ShapeRuntime

    clock = {"t": 1000.0}
    monkeypatch.setattr(srv.time, "monotonic", lambda: clock["t"])
    app = AppCtx(facility=FakeFacility(), profile=Profile(nodes_per_block=1), charge_factor=1.0)
    rt = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})

    srv._settle_billing(rt, app, "warm")
    assert rt.warm_since is None  # login never starts the spend clock
    clock["t"] += 3600
    assert srv._session_spend(rt, app) == 0.0  # no allocation -> no spend


def test_total_session_spend_sums_only_billable_shapes(monkeypatch):
    import hpc_bridge.server as srv
    from hpc_bridge.server import ShapeRuntime

    clock = {"t": 1000.0}
    monkeypatch.setattr(srv.time, "monotonic", lambda: clock["t"])
    app = AppCtx(facility=FakeFacility(), profile=Profile(nodes_per_block=1), charge_factor=1.0)
    slurm = ShapeRuntime(user_endpoint_config={"provider_type": "SlurmProvider"})
    login = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    app.shapes = {"compute": slurm, "login": login}

    srv._settle_billing(slurm, app, "warm")
    srv._settle_billing(login, app, "warm")
    clock["t"] += 3600  # 1h warm
    # only the slurm shape bills: 1 node * 1h * charge_factor 1.0 = 1.0
    assert abs(srv._total_session_spend(app) - 1.0) < 1e-9


def test_worker_notice_flags_dill_skew(monkeypatch):
    import hpc_bridge.server as srv

    monkeypatch.setattr("hpc_bridge.notices._local_dill", lambda: "0.3.9")
    skewed = srv._worker_notice(
        CanaryResult(ok=True, worker_host="a070", worker_python="3.11.7", worker_dill="0.3.8")
    )
    assert "a070" in skewed and "skew" in skewed and "0.3.8" in skewed and "0.3.9" in skewed
    matched = srv._worker_notice(CanaryResult(ok=True, worker_dill="0.3.9"))
    assert matched and "skew" not in matched


def test_note_dispatch_refreshes_on_complete_and_voids_on_timeout(monkeypatch):
    import hpc_bridge.server as srv
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime

    rt = ShapeRuntime(user_endpoint_config={})
    rt.warm_confirmed_at = 5.0
    srv._note_dispatch(rt, ShellOutcome(phase="failed", block_state="warm", exit_code=124))
    assert rt.warm_confirmed_at is None  # a dispatch timeout forces a re-canary next call
    monkeypatch.setattr(srv.time, "monotonic", lambda: 999.0)
    srv._note_dispatch(rt, ShellOutcome(phase="complete", block_state="warm", exit_code=0))
    assert rt.warm_confirmed_at == 999.0  # a real result refreshes liveness


async def test_concurrent_run_shell_serializes_runner_creation():
    # The lock must serialize provision + runner-swap: two run_shells racing on a fresh app
    # create exactly ONE runner (without it, both could see app.runner is None and double up).
    import asyncio

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    created = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "ok\n", ""))
        created.append(r)
        return r

    app.runner_factory = factory
    _confirm_slurm(app)
    outs = await asyncio.gather(_run_shell(app, "echo a"), _run_shell(app, "echo b"))
    assert all(o.phase == "complete" for o in outs)
    assert len(created) == 1  # the second call reused the runner instead of racing a new one


async def test_login_shell_runs_on_ssh_facility():
    from hpc_bridge.server import _login_shell

    class _SshFacility(FakeFacility):  # an SSH facility exposes login_exec; local does not
        async def login_exec(self, command):
            return (0, "shared*|up|infinite|250|128|257400|226/12/12/250\n", "")

    res = await _login_shell(AppCtx(facility=_SshFacility(), profile=Profile()), "sinfo -h")
    assert res.exit_code == 0 and "shared" in res.stdout


async def test_login_shell_unavailable_on_local_facility():
    from hpc_bridge.server import _login_shell

    res = await _login_shell(AppCtx(facility=FakeFacility(), profile=Profile()), "sinfo")
    assert res.exit_code == 1 and "connect_facility" in (res.notice or "")  # nudges the entry point, not login_shell


async def test_server_registers_login_shell_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "login_shell" for t in tools)


async def test_stop_keeps_login_node_pin_for_reuse(tmp_path, monkeypatch):
    # Option A: stop leaves the endpoint online, so the login-node pin MUST survive — a reconnect
    # rebinds straight to the pinned node (zero SSH). Stop never removes it.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import _stop_endpoint
    from hpc_bridge.state import EndpointRecord, LoginNodeStore

    store = LoginNodeStore(tmp_path / "endpoints.json")
    store.put(EndpointRecord(
        endpoint_id="eid-1", login_host="login03.x", alias="anvil.x", user="u",
        key_path="/k", name="hpc-bridge", provisioned_at="2026-06-06T00:00:00Z",
    ))

    class _Fac(FakeFacility):
        def __init__(self):
            super().__init__()
            self.store = store
            self.alias = "anvil.x"

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released none\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    app = AppCtx(facility=_Fac(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    await _stop_endpoint(app)
    rec = store.get(alias="anvil.x", name="hpc-bridge")
    assert rec is not None and rec.login_host == "login03.x"  # pin kept for cheap reconnect


# --- budget gate: the deterministic spend floor (confirm before a billed block) -------------


async def test_billed_provision_needs_confirmation():
    # A billed (Slurm) shape must not start a block until spend is acknowledged. Without
    # confirm_spend the call returns needs_confirmation and provisions NOTHING.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    created = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "", ""))
        created.append(r)
        return r

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app)  # no confirm_spend
    assert res.status == "needs_confirmation" and res.block_state == "cold"
    assert res.notice and "confirm_spend=True" in res.notice and "balance" in res.notice
    assert f.provisioned is False  # nothing started
    assert created == []  # no runner built, no canary, no block kicked
    assert app.shapes["compute"].spend_confirmed is False


async def test_confirm_spend_provisions_and_persists_for_session():
    # confirm_spend=True provisions and records the ack; a later call needs no re-confirmation.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up"
    assert app.shapes["compute"].spend_confirmed is True
    res2 = await _ensure_endpoint_up(app)  # no confirm_spend, but ack persists
    assert res2.status == "up"  # NOT needs_confirmation


async def test_login_shape_never_needs_confirmation():
    # A login (LocalProvider) shape is free: it provisions without a spend ack.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, shape="login")  # no confirm_spend
    assert res.status == "up"


async def test_run_shell_blocked_until_spend_confirmed():
    # The floor covers run_shell too (its canary submit would otherwise kick a billed block):
    # an unconfirmed billed shape returns needs_confirmation and dispatches nothing.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    created = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "", ""))
        created.append(r)
        return r

    app.runner_factory = factory
    out = await _run_shell(app, "echo hi")
    assert out.phase == "needs_confirmation"
    assert created == []  # no runner, no canary, no block — the command never dispatched
    assert f.provisioned is False


async def test_run_shell_runs_after_spend_confirmed():
    # Once ensure_endpoint_up(confirm_spend=True) acknowledges spend, run_shell dispatches.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "hi\n", ""))
    await _ensure_endpoint_up(app, confirm_spend=True)
    out = await _run_shell(app, "echo hi")
    assert out.phase == "complete" and out.stdout == "hi\n"


def test_pbs_entry_is_supported():
    import datetime

    from hpc_bridge.catalog.entry import CatalogEntry, Compute, Defaults
    from hpc_bridge.server import _unsupported_entry_reason
    entry = CatalogEntry(
        id="polaris", facility_key="alcf", facility="ALCF", description="d",
        display_name="Polaris", ssh_host="polaris",
        compute=Compute(scheduler="pbs", interface="hsn0",
                        env_setup="x", scratch_root="/home/{user}/.hpc-bridge"),
        defaults=Defaults(partition="debug"),
        last_validated=datetime.date(2026, 7, 10),
    )
    assert _unsupported_entry_reason(entry) is None


# --- #21 item 2: long-task submit/poll -------------------------------------------------------


async def test_server_registers_poll_task_tool():
    tools = await mcp.list_tools()
    assert any(t.name == "poll_task" for t in tools)


async def test_long_task_returns_handle_then_polls_to_complete():
    # A command that outlives the sync-wait comes back phase="running" with a registered task_id
    # (NOT cut); once it finishes, poll_task returns the full result and reaps the handle.
    from hpc_bridge.server import _poll_task

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    out = await _run_shell(app, "sleep 999")
    assert out.phase == "running" and out.task_id  # handed a poll handle, not cut at ~110s
    assert out.task_id in app.tasks  # registered
    assert await _poll_task(app, out.task_id) and (await _poll_task(app, out.task_id)).phase == "running"
    runner.futures[0].finish(_Res(0, "done\n", ""))  # the task finishes on the worker
    done = await _poll_task(app, out.task_id)
    assert done.phase == "complete" and done.exit_code == 0 and done.stdout == "done\n"
    assert out.task_id not in app.tasks  # handle dropped after retrieval


async def test_completed_exit_124_does_not_void_warmth():
    # A task hitting its ceiling returns a COMPLETED exit-124 (the worker enforced it and is alive),
    # so warmth is NOT voided (the old timeout==124 heuristic is gone) and the spend clock runs on.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(124, "partial", ""))
    _confirm_slurm(app)
    out = await _run_shell(app, "sleep 999")
    assert out.phase == "complete" and out.exit_code == 124
    assert _shape_runtime(app, "compute").warm_confirmed_at is not None  # alive -> warmth kept


async def test_partition_change_rejected_while_task_running():
    # Repointing the block while a task runs would tear it down and cancel the task -> the change is
    # rejected with a clear notice, the runner is NOT swapped, and the live task is untouched.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    assert (await _run_shell(app, "sleep 999")).phase == "running"
    before = app.shapes["compute"].runner
    res = await _ensure_endpoint_up(app, partition="gpu", confirm_spend=True)
    assert "still running" in (res.notice or "")  # rejected, not applied
    assert app.shapes["compute"].runner is before and not before.closed  # runner NOT swapped/cancelled
    assert app.shapes["compute"].user_endpoint_config.get("partition") != "gpu"


async def test_stop_endpoint_drains_task_registry(monkeypatch):
    # A close site (stop_endpoint here) drops the released block's poll handles so no dead future is
    # polled; poll_task then reports the task ended.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, TaskHandle, _poll_task, _stop_endpoint

    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=slurm_runner)
    app.shapes["login"] = ShapeRuntime(
        user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0
    )
    app.tasks["compute-1"] = TaskHandle(
        future=_PendingFuture(), shape="compute", session_id="default",
        command="sleep 999", submitted_at=0.0, ceiling_s=1780.0,
    )

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    # 2026-09-05 (fake-cluster chaos): a stop under a RUNNING task no longer drains it — releasing the block does not
    # end the task, the endpoint relaunches a block for it. The stop REFUSES and the handle stays pollable.
    res = await _stop_endpoint(app)
    assert res.status == "up" and "compute-1" in app.tasks and "can't stop yet" in res.notice
    # once the task has finished the stop proceeds; a FINISHED handle is kept (its result stays retrievable)
    app.tasks["compute-1"].future.finish(_Res(0, "done", ""))
    res = await _stop_endpoint(app)
    assert res.status == "down" and "compute-1" in app.tasks
    got = await _poll_task(app, "compute-1")
    assert got.phase == "complete" and got.stdout == "done"


async def test_second_command_on_busy_session_is_rejected():
    # A session whose task is still running can't take a second command (they'd race the same cwd/env);
    # a DIFFERENT session_id runs fine.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    assert (await _run_shell(app, "sleep 999", session_id="s1")).phase == "running"
    second = await _run_shell(app, "echo hi", session_id="s1")  # same session -> refused
    assert second.phase == "failed" and "still has a task running" in (second.notice or "")
    assert len(runner.futures) == 1  # the refused command was NOT submitted
    third = await _run_shell(app, "echo hi", session_id="s2")  # a different session is fine
    assert third.phase == "running" and len(runner.futures) == 2


async def test_double_poll_on_done_task_is_claimed_once():
    # The pop-under-lock is the atomic claim: the first poll of a finished task gets the result, a
    # second gets a benign miss (never a double spend-count / KeyError).
    from hpc_bridge.server import _poll_task

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    running = await _run_shell(app, "sleep 999")
    runner.futures[0].finish(_Res(0, "out\n", ""))
    first = await _poll_task(app, running.task_id)
    second = await _poll_task(app, running.task_id)
    assert first.phase == "complete" and first.stdout == "out\n"
    assert second.phase == "failed" and "no task" in (second.notice or "").lower()
    assert running.task_id not in app.tasks


async def test_poll_with_wait_times_out_to_running():
    # poll_task(wait=W) that doesn't resolve within W returns phase="running" (never a 124 failure),
    # and the handle stays registered for the next poll.
    from hpc_bridge.server import _poll_task

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    running = await _run_shell(app, "sleep 999")
    out = await _poll_task(app, running.task_id, wait=0.01)
    assert out.phase == "running" and out.task_id == running.task_id
    assert running.task_id in app.tasks


def test_parse_hhmmss_and_task_ceiling():
    import os

    from hpc_bridge.server import _parse_hhmmss, _task_ceiling_s

    assert _parse_hhmmss("00:30:00") == 1800
    assert _parse_hhmmss("48:00:00") == 172800
    assert _parse_hhmmss("90") == 5400 and _parse_hhmmss("5:00") == 300  # a bare number is Slurm minutes
    assert _parse_hhmmss("") == 0 and _parse_hhmmss(None) == 0 and _parse_hhmmss("abc") == 0
    assert _task_ceiling_s({"walltime": "00:30:00"}) == 1780.0  # block walltime - margin
    assert _task_ceiling_s({}) >= 300.0  # missing walltime -> safe non-zero fallback, not 0/crash
    os.environ["HPC_BRIDGE_MAX_TASK_S"] = "600"  # opt-in cap clamps a long-walltime facility
    try:
        assert _task_ceiling_s({"walltime": "48:00:00"}) == 600.0
    finally:
        del os.environ["HPC_BRIDGE_MAX_TASK_S"]


async def test_runner_gets_ceiling_walltime_and_sync_wait_below_it():
    # _runner_for passes the block-walltime ceiling as the runner walltime, and clamps the client
    # sync-wait strictly below it (so a task finishing near the boundary still returns, never a race).
    from hpc_bridge.server import _runner_for

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    seen = {}

    def factory(eid, user_endpoint_config=None, **kw):
        seen.clear()
        seen.update(kw)
        return _FakeRunner(eid, _Res(0, "", ""))

    app.runner_factory = factory
    rt = _shape_runtime(app, "compute")
    rt.user_endpoint_config["walltime"] = "00:30:00"
    _runner_for(app, "compute")
    assert seen["walltime"] == 1780.0 and seen["timeout"] < seen["walltime"]
    rt.runner = None  # force a rebuild with a tiny walltime
    rt.user_endpoint_config["walltime"] = "00:01:00"
    _runner_for(app, "compute")
    assert seen["timeout"] < seen["walltime"]  # invariant holds even for a 60s block


async def test_running_task_short_circuits_the_canary():
    # A live task IS liveness: a status probe while it runs must NOT fire a canary (which would queue
    # behind the sole worker and, on timeout, bank/stop the spend clock while the block still burns).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), pending=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    assert (await _run_shell(app, "sleep 999")).phase == "running"
    canaries = runner.canaries  # the one canary paid during provisioning
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and res.block_state == "warm"
    assert runner.canaries == canaries  # no extra canary behind the busy worker


async def test_drain_keeps_finished_handles_drops_running():
    # review nit: a stale-runner rebuild / stop dropped DONE-but-unpolled handles too, losing a delivered
    # result. Only still-running futures are moot when the block goes away.
    from hpc_bridge import server as srv
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    done_id = srv._register_task(app, "compute", "s1", "echo", _DoneFuture(_Res(0, "out", "")), 100.0)
    live_id = srv._register_task(app, "compute", "s2", "sleep", _PendingFuture(), 100.0)
    srv._drain_shape_tasks(app, "compute")
    assert done_id in app.tasks and live_id not in app.tasks


async def test_ssh_teardown_reports_the_spend_it_ended(monkeypatch):
    # review nit: teardown cleared app.shapes and THEN summed session spend -> always 0.0
    from hpc_bridge import server as srv
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    _shape_runtime(app, "compute").spend_accrued = 3.5

    async def _released(app_, eid, *rest, **kw):
        return True, "released"

    monkeypatch.setattr("hpc_bridge.scheduler_ops._release_blocks_over_login", _released)
    res = await srv._teardown_endpoint(app)
    assert res.status == "down" and res.session_spend >= 3.5


async def test_poll_on_a_dead_endpoint_is_terminal_not_running():
    # 2026-08-19: another process deleted the endpoint under a running task; the agent polled 25× for
    # 20 minutes because a pending future always read 'running'. A pending task whose endpoint is
    # offline/gone can never resolve -> terminal failed (ORPHANED), handle dropped.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""), pending=True)
    _shape_runtime(app, "compute").spend_confirmed = True
    out = await _run_shell(app, "sleep 3000", shape="compute")
    assert out.phase == "running" and out.task_id
    # endpoint alive, block merely slow -> still running (Parsl would relaunch a killed block; keep polling)
    still = await _poll_task_for_test(app, out.task_id)
    assert still.phase == "running" and out.task_id in app.tasks
    # the endpoint goes away (torn down / facility outage) -> orphaned, terminal, handle gone
    f.workers = 0
    dead = await _poll_task_for_test(app, out.task_id)
    assert dead.phase == "failed" and "ORPHANED" in dead.notice and "stop polling" in dead.notice
    assert out.task_id not in app.tasks
    # a second poll is the benign 'no task' miss, not another 20-minute loop
    again = await _poll_task_for_test(app, out.task_id)
    assert again.phase == "failed" and "no task" in again.notice


async def test_poll_status_hiccup_does_not_orphan_a_live_task():
    # a status-API error must not condemn a live task (best-effort: keep polling)
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""), pending=True)
    _shape_runtime(app, "compute").spend_confirmed = True
    out = await _run_shell(app, "sleep 3000", shape="compute")

    async def boom(eid):
        raise RuntimeError("status API 503")

    f.manager_online = boom
    assert (await _poll_task_for_test(app, out.task_id)).phase == "running"
    assert out.task_id in app.tasks


async def _poll_task_for_test(app, task_id, wait=0.0):
    from hpc_bridge.server import _poll_task
    return await _poll_task(app, task_id, wait)


async def test_ssh_stop_refuses_while_a_task_runs_and_keeps_its_handle(monkeypatch):
    # Fake-cluster chaos run 2026-09-05 (stop_while_running): stop_endpoint under a RUNNING compute task answered
    # "down, released 7" while the endpoint relaunched block-1 for the orphaned task and poll_task then said the
    # task was gone. Releasing a block does not end the task it hosts. Refuse, like the facility-endpoint stop.
    from concurrent.futures import Future

    from hpc_bridge import server
    from hpc_bridge.context import TaskHandle
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True, "walltime": "00:30:00"},
                                         runner=_FakeRunner("eid-1", _Res(0, "", "")))
    app.tasks["compute-1"] = TaskHandle(future=Future(), shape="compute", session_id="default", command="sleep 180",
                                        submitted_at=0.0, ceiling_s=1780.0)  # not done: still RUNNING

    released = []

    async def fake_release(a, eid, runner, **kw):
        released.append(eid)
        return True, "released 7"

    monkeypatch.setattr(server.scheduler_ops, "_release_blocks_over_login", fake_release)
    res = await _stop_endpoint(app)
    assert res.status == "up" and res.block_state == "warm"
    assert "can't stop yet" in res.notice and "compute-1" in res.notice and "poll_task" in res.notice and "teardown_endpoint" in res.notice
    assert released == [] and "compute-1" in app.tasks and "compute" in app.shapes  # nothing released, handle kept
    app.tasks["compute-1"].future.set_result(None)  # the task finished: the same stop now releases
    res = await _stop_endpoint(app)
    assert res.status == "down" and released == ["eid-1"]


def test_summarize_pilot_names_a_pilot_that_ran_and_died():
    # PBS with -x lists finished jobs: every row F/E ⇒ the block ran and its worker exited — not "never submitted"
    # (fake OpenPBS 2026-09-06: an empty-account submit became an empty STDIN job, exit 127, read as REJECTED).
    from hpc_bridge.server import _summarize_pilot
    cat, why = _summarize_pilot("F 1.pbsserver 127\n", 200)
    assert cat == "finished" and "1.pbsserver" in why and "exit status 127" in why and "submit_scripts" in why
    # a live pilot beside an old finished one: the live state wins
    assert _summarize_pilot("F 1.pbsserver 127\nR 3.pbsserver -\n", 200)[0] == "starting"
    assert _summarize_pilot("F 1.pbsserver 127\nQ 3.pbsserver -\n", 200)[0] == "queued"
    # leftovers are NOT a diagnosis: a pilot deleted before it ran (no exit status — a held pilot cancelled by a re-bind)
    # and one killed/qdel'd (271) read as "no pilot": starting within the grace, rejected after it
    assert _summarize_pilot("F 4.pbsserver - HELD by the site\n", 10)[0] == "starting"
    assert _summarize_pilot("F 4.pbsserver - HELD by the site\nF 5.pbsserver 271 Job run … terminated\n", 200)[0] == "rejected"
    assert _summarize_pilot("F 5.pbsserver 271 terminated\nQ 6.pbsserver -\n", 200)[0] == "queued"


def test_summarize_pilot_relays_a_held_jobs_comment():
    # PBS: the probe prints `STATE JOBID comment…`; a Polaris-style hook's comment is the site's own explanation
    from hpc_bridge.server import _pilot_status_cmd, _summarize_pilot
    assert "comment = " in _pilot_status_cmd("pbs", "E")
    cat, why = _summarize_pilot("H 5.pbsserver - HELD by the site: every job must request -l filesystems=home:eagle\n", 30)
    assert cat == "held" and "5.pbsserver" in why and "filesystems=home:eagle" in why and "scheduler_options" in why
    cat2, why2 = _summarize_pilot("H 42.aurora\n", 0)          # no comment: the generic hint
    assert cat2 == "held" and "bad scheduler directive" in why2
    assert _summarize_pilot("Q 7.pbsserver - \n", 10)[0] == "queued"   # a trailing empty comment is fine
    assert _summarize_pilot("PD 12345\nR 12346\n", 10)[0] == "starting"       # Slurm rows have no exit column


def test_slurm_pilot_probe_reads_finished_pilots_and_pending_reasons():
    # the Slurm twin of the PBS fix (0.1.13): sacct rows (SubmitLine carries the uep marker) for pilots that ran and
    # died, and squeue's Reason for a PENDING job the scheduler will never start
    from hpc_bridge.server import _pilot_status_cmd, _summarize_pilot
    cmd = _pilot_status_cmd("slurm", "E")
    assert "sacct -X -n -P" in cmd and "SubmitLine" in cmd and "Reason:60" in cmd and "uep.E" in cmd
    assert "COMPLETED" in cmd and "CANCELLED" in cmd   # a clean/cancelled end is not a diagnosis
    cat, why = _summarize_pilot("F 12 42 FAILED\n", 200)
    assert cat == "finished" and "12" in why and "exit status 42" in why
    assert _summarize_pilot("F 12 - CANCELLED\n", 200)[0] == "rejected"         # our own scancel: ignored past the grace
    assert _summarize_pilot("F 12 - CANCELLED\n", 10)[0] == "starting"
    cat, why = _summarize_pilot("PENDING 13 - PartitionTimeLimit\n", 30)
    assert cat == "held" and "PartitionTimeLimit" in why and "will not start" in why
    assert _summarize_pilot("PENDING 13 - AssocMaxJobsLimit\n", 30)[0] == "held"
    cat, why = _summarize_pilot("PENDING 13 - Priority\n", 30)
    assert cat == "queued" and "reason Priority" in why
    assert _summarize_pilot("PENDING 13 - None\n", 30) == ("queued", "— pilot 13 is queued (PENDING); waiting on the scheduler.")
    assert _summarize_pilot("RUNNING 14 - None\nF 12 42 FAILED\n", 30)[0] == "starting"   # a live pilot wins


async def test_reaped_block_under_a_live_session_cold_starts_then_recovers():
    # The return-later flow (agentic: block_reaped_resume): the server process lives on with its in-memory state,
    # the pilot block is reaped underneath it (idle-release / walltime / scancel), the canary TTL expires, and the
    # user comes back and runs again. Warm -> honest cold_start (nothing dispatched into the void, spend clock
    # stopped) -> complete on the new block, without a second spend ask (the acknowledgement persists by design).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "HPCB_REAP marker", ""))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    _confirm_slurm(app)
    rt = _shape_runtime(app, "compute")

    out = await _run_shell(app, "cat marker")
    assert out.phase == "complete" and out.block_state == "warm"
    assert rt.warm_since is not None and len(runner.commands) == 1

    # the block is gone; the last good canary is older than CANARY_TTL_S, so the next call must re-verify
    runner._canary = CanaryResult(ok=False, error="timeout")
    rt.warm_confirmed_at -= 10_000
    out = await _run_shell(app, "cat marker")
    assert out.phase == "cold_start" and out.block_state == "provisioning"
    assert len(runner.commands) == 1        # not dispatched to a block that is not there
    assert rt.warm_since is None            # the spend clock stopped (banked), not left running through the gap
    assert rt.spend_confirmed is True       # no re-ask on the way back: the session's acknowledgement persists

    # the canary's submit re-kicked a block; a worker answers again and the same session carries on
    runner._canary = CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9")
    out = await _run_shell(app, "cat marker")
    assert out.phase == "complete" and out.block_state == "warm"
    assert "HPCB_REAP" in out.stdout and len(runner.commands) == 2
    assert rt.warm_since is not None        # a fresh clock for the new block
