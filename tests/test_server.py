import time
from pathlib import Path

import pytest

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

    def add_done_callback(self, fn):
        fn(self)

    def cancelled(self):
        return False

    def exception(self, timeout=None):
        return self._exc

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
        self._callbacks = []

    def done(self):
        return self._done or self._cancelled

    def add_done_callback(self, fn):
        if self.done():
            fn(self)
        else:
            self._callbacks.append(fn)

    def _resolved(self):
        for fn in self._callbacks:
            fn(self)
        self._callbacks = []

    def cancelled(self):
        return self._cancelled

    def exception(self, timeout=None):
        if not self.done():
            raise TimeoutError()
        return None if self._cancelled else self._exc

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
        self._resolved()

    def fail(self, exc):
        self._exc, self._done = exc, True
        self._resolved()

    def cancel(self):
        self._cancelled = True
        self._resolved()


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
        # the checks this runner "sent" (as GlobusRunner.sent_checks): a canary that timed out stays queued on the
        # endpoint (one at a time, reused while pending, as the real runner does); an answer resolves it, a failure
        # fails it. `lookup` (task id -> {"pending", "status"}) stands in for the SDK client's get_task; None: unset.
        self.checks = []
        self.lookup = None

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
        c = self._canary
        live = self.checks[-1] if self.checks and not self.checks[-1].done() else None
        if not c.ok and live is None:  # a check that does not answer is a task sent: queued (timeout), or failed
            live = _PendingFuture()
            live.task_id = f"check-{len(self.checks) + 1}"
            self.checks.append(live)
        if live is not None and c.ok:
            live.finish(_Res(0, "HPCB_CANARY", ""))  # a worker answered the check still queued
        elif live is not None and c.error != "timeout":
            live.fail(RuntimeError(c.error or "failed"))
        return c

    def sent_checks(self):
        # late-bound, as the SDK client's get_task is: a test may change what the service answers after a retire
        look = (lambda tid: self.lookup(tid)) if self.lookup is not None else None
        return [(f, look) for f in self.checks
                if not (f.done() and not f.cancelled() and f.exception() is None)]

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


class _Ctx:  # the slice of an MCP Context a tool wrapper reads
    def __init__(self, app):
        self.request_context = type("_Req", (), {"lifespan_context": app})()

    async def report_progress(self, *a, **k):
        pass


def _warm_app(stdout, stderr="", rc=0):
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(rc, stdout, stderr))
    _confirm_slurm(app)
    return app


async def test_run_shell_tool_hands_the_agent_the_marked_end_and_keeps_the_internal_result_whole():
    from hpc_bridge import server
    from hpc_bridge.runner import MAX_OUTPUT_CHARS

    long = "".join(f"row {i}\n" for i in range(1, 5001))  # ~38,900 chars: over the cap
    app = _warm_app(long, "boom\n", rc=1)
    internal = await _run_shell(app, "cat rows")
    assert internal.stdout == long and internal.notice is None  # what the pilot probe / allocation parsers read
    out = await server.run_shell("cat rows", _Ctx(app))
    assert out.phase == "complete" and out.exit_code == 1
    assert out.stdout.startswith("[hpc-bridge: stdout too long — showing only its last ")
    assert out.stdout.endswith("row 5000\n") and len(out.stdout) < MAX_OUTPUT_CHARS + 200
    assert out.stderr_snippet == "boom\n"
    assert out.notice and out.notice.startswith("stdout too long for one result")  # reaches the agent


@pytest.mark.parametrize("tool,impl,args", [
    ("poll_task", "_poll_task", ("t1",)), ("reset_session", "_reset_session", ()),
])
async def test_poll_task_and_reset_session_tools_hand_the_agent_the_marked_end(monkeypatch, tool, impl, args):
    from hpc_bridge import server

    long = "".join(f"row {i}\n" for i in range(1, 5001))

    async def fake(app, *a, **k):
        return server.ShellOutcome(phase="complete", exit_code=0, stdout=long, block_state="warm")

    monkeypatch.setattr(server, impl, fake)
    out = await getattr(server, tool)(*args, ctx=_Ctx(object()))
    assert out.stdout.startswith("[hpc-bridge: stdout too long") and out.stdout.endswith("row 5000\n")
    assert out.notice and out.notice.startswith("stdout too long for one result")


async def test_pilot_probe_reads_a_long_scheduler_history_uncut():
    # PBS `qstat -x` keeps finished jobs for weeks; a reused endpoint's leftover pilots (271 = our own release) pass
    # 16,000 chars at ~115 rows. A cut result's marker line read as a LIVE pilot turned "rejected" into "queued".
    from hpc_bridge import scheduler_ops, server
    from hpc_bridge.runner import MAX_OUTPUT_CHARS

    rows = "".join(f"F {6200000 + i}.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov 271 Job run at Mon Oct 05 at 14:02 "
                   "on (x3005c0s37b1n0:ncpus=64:ngpus=4) and finished\n" for i in range(130))
    assert len(rows) > MAX_OUTPUT_CHARS
    app = _warm_app(rows)
    cat, _ = await scheduler_ops._pilot_status_over_login(app, "eid", 10_000, server._login_runner(app))
    assert cat == "rejected"  # no live pilot, past the grace
    died = rows + "F 6300000.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov 1 Job run and failed\n"
    app = _warm_app(died)
    cat, _ = await scheduler_ops._pilot_status_over_login(app, "eid", 10, server._login_runner(app))
    assert cat == "finished"


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


def _queued_check(runner, task_id="check-1"):
    """A check (canary) `runner` sent that no worker has answered: still queued on the compute user endpoint."""
    fut = _PendingFuture()
    fut.task_id = task_id
    runner.checks.append(fut)
    return fut


async def test_stop_during_provisioning_is_draining_not_down(monkeypatch):
    # The stop-during-provisioning race (spend_revoked, 2026-09-05): the block was REQUESTED (spend_confirmed) but never
    # CONFIRMED running, and the scancel finds no job yet ("released none"). Our check is still queued on the endpoint,
    # which can start a block for it after this stop: claiming "down" would leak that pilot (live 2026-10-16,
    # queued_check_relaunch), so the stop says `draining` — from the check, not from a clock.
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.facility.manager_up = True  # the endpoint is up (not orphaned) — only the block isn't visible yet
    slurm_runner = _FakeRunner("eid-1", _Res(0, "", ""))
    _queued_check(slurm_runner)
    app.shapes["compute"] = ShapeRuntime(
        user_endpoint_config={"compute": True}, runner=slurm_runner,
        spend_confirmed=True, warm_confirmed_at=None,  # requested, never confirmed running
    )
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)

    async def released_nothing(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released none\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", released_nothing)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and res.spend_stopped is not True
    assert "is still queued on the endpoint" in (res.notice or "") and "stop_endpoint again" in (res.notice or "")

    # RECOVERY on a fresh compute user endpoint: the stop looks again while our check is queued — "released none", then
    # the pilot lands and is cancelled, and the endpoint gives the check up: `down` in ONE call
    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app3 = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-3"))
    app3.facility.fresh_ueps = True
    runner3 = _FakeRunner("eid-3", _Res(0, "", ""))
    _queued_check(runner3)
    app3.shapes["compute"] = ShapeRuntime(
        user_endpoint_config={"compute": True}, runner=runner3, spend_confirmed=True, warm_confirmed_at=None,
    )
    app3.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    seq = ["released none\n", "released none\n", "released 12345\n"]  # pilot lands on the 3rd look
    runner3.lookup = lambda tid: ({"pending": True, "status": "waiting-for-nodes"} if seq else
                                  {"pending": False, "status": "failed"})

    async def lands_then_cancelled(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout=seq.pop(0) if seq else "released none\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", lands_then_cancelled)
    res3 = await _stop_endpoint(app3)
    assert res3.status == "down" and "released 12345" in (res3.notice or "")  # caught the late pilot in ONE stop call

    # control: a block CONFIRMED running (warm), no check of ours queued, whose scancel finds nothing: honest "down"
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


# --- a stop decided by its evidence: our checks still queued on a compute user endpoint (#24) ------------------------
# Live 2026-10-15 (submit_policy_rejected, slurm_worker_died): ensure_endpoint_up said REJECTED / pilot FINISHED, then
# stop_endpoint said "still being submitted … one may land shortly", and the agents believed the earlier answer. Live
# 2026-10-16 (queued_check_relaunch): on main a stop answered `down` "released 4" while our check was still queued, and
# the endpoint submitted job 5 for it 53 s later. Now a stop answers `down` only once every check of ours has resolved.

_SBATCH_REFUSED = ("Failed to start block 1: Cannot launch job parsl.slurm.1: Could not read job ID from submit command "
                   "standard output; recode=1, stderr='sbatch: error: Batch job submission failed: Invalid account or "
                   "account/partition combination specified'")


def _scripted_login(monkeypatch, releases=(), probes=()):
    """Script the login channel the release and the pilot probe ride (server._run_shell): each release attempt answers
    the next of `releases` (its stdout; None: a cold channel; when they run out, `released none`), each pilot probe the
    next of `probes` (its stdout; None: unreadable; when they run out, nothing listed). Returns the log of calls."""
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome

    releases, probes, log = list(releases), list(probes), []

    async def run(a, command, session_id="default", shape="compute"):
        kind = "probe" if "HPCB_ERR" in command else "release"
        log.append((kind, command))
        queue = probes if kind == "probe" else releases
        out = queue.pop(0) if queue else ("released none\n" if kind == "release" else "")
        if out is None:
            return ShellOutcome(phase="cold_start", block_state="cold", notice="allocating nodes…")
        return ShellOutcome(phase="complete", exit_code=0, stdout=out, block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    return log


def _kinds(log):
    return [k for k, _ in log]


class _PbsFacility(FakeFacility):
    def __init__(self):
        from types import SimpleNamespace

        super().__init__()
        self.profile = SimpleNamespace(scheduler="pbs")


def _ssh_app(pbs=False):
    f = _PbsFacility() if pbs else FakeFacility()
    f.workers = 1
    return AppCtx(facility=f, profile=Profile())


def _job_answer(job):
    return CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9", worker_job=job)


def _skew(monkeypatch):
    """time.monotonic, shifted by skew[0] seconds — to let time pass between calls."""
    real, skew = time.monotonic, [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: real() + skew[0])
    return skew


def _queued_app(monkeypatch, *, pbs=False, fresh=False):
    """An SSH endpoint whose confirmed block never came up: its check (canary) timed out, so it is still queued on the
    compute user endpoint — task "check-1" (the fake runner's). `fresh`: the endpoint's template gives each block
    request its own user endpoint (this plugin's), so a stop waits for that check to be given up."""
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(pbs)
    app.facility.fresh_ueps = fresh
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    return app, runner


async def _dead_pilot_app(monkeypatch, *, pbs=False, fresh=False):
    """A billed block, spend confirmed and still cold, whose provisioning probe reported its pilot FINISHED — the
    slurm_worker_died cell's state: pilot 12, exit status 42; our check still queued."""
    app, runner = _queued_app(monkeypatch, pbs=pbs, fresh=fresh)
    rows = "F 12.pbsserver 42 Job failed\n" if pbs else "F 12 42 FAILED\n"
    _scripted_login(monkeypatch, probes=[rows])
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning" and "already FINISHED (exit status 42)" in res.notice
    return app, runner


def _settle_clock(monkeypatch, skew):
    """The stop's settle loop sleeps on this: advance the fake clock instead of waiting."""
    from hpc_bridge import server

    async def sleep(s):
        skew[0] += s

    monkeypatch.setattr(server, "_sleep", sleep, raising=False)


async def test_a_stop_that_cancelled_the_pilot_of_a_check_still_queued_is_not_down(monkeypatch):
    # the live hazard (2026-10-16, queued_check_relaunch phase 1, on main): our check queued, its pilot (job 4) in the
    # queue; the stop cancelled it — "released 4" — and answered `down`; the endpoint submitted job 5 for the check 53 s
    # later. An endpoint of an earlier plugin (one compute user endpoint) is not waited on: the answer is `draining`.
    from hpc_bridge.server import _stop_endpoint

    app, _runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    _scripted_login(monkeypatch, releases=["released 4\n"])
    res = await _stop_endpoint(app)
    assert res.status == "draining" and res.spend_stopped is not True
    assert res.notice.startswith("compute block released over AMQP (released 4).")
    assert "is still queued on the endpoint, which can start a block to run it" in res.notice


async def test_a_check_still_queued_keeps_the_stop_draining(monkeypatch):
    # the hazard: the scheduler holds none of ours, but our check is still queued — the endpoint can start a block for
    # it (after a cancel parsl submits another while a task is outstanding). Main answered `down` here.
    from hpc_bridge.server import _stop_endpoint

    app, runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    log = _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and res.spend_stopped is not True and _kinds(log) == ["release"]
    assert ("hpc-bridge's check for the block on the default partition (task check-1) is still queued on the endpoint, "
            "which can start a block to run it") in res.notice
    assert "teardown_endpoint ends it" in res.notice and "about a minute" in res.notice
    assert "still being submitted" not in res.notice and "idle-release" not in res.notice
    assert [getattr(c.future, "task_id", None) for c in app.sent_checks[app.state.endpoint_id]] == ["check-1"]
    # a second stop (the shape is gone) still sees it: the endpoint kept it
    _scripted_login(monkeypatch)
    assert (await _stop_endpoint(app)).status == "draining"
    # it resolves (failed: the endpoint gave it up) — the next stop is `down`, and forgets it
    runner.checks[0].fail(RuntimeError("bad state"))
    _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.spend_stopped is True and "has given up hpc-bridge's check" in res.notice
    assert app.state.endpoint_id not in app.sent_checks


async def test_a_check_the_service_says_is_still_pending_is_pending_whatever_the_local_future_says(monkeypatch):
    # the local future can fail on the client's side alone (the SDK's result watcher quitting) while the task lives on:
    # a registered task is asked about by id
    from hpc_bridge.server import _stop_endpoint

    app, runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner.checks[0].fail(RuntimeError("SDK thread quit unexpectedly."))
    runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}
    for _ in range(2):   # the shape is gone after the first: the second stop still asks the service
        _scripted_login(monkeypatch)
        assert (await _stop_endpoint(app)).status == "draining"
    # the service answers it failed (given up): `down`
    runner.lookup = lambda tid: {"pending": False, "status": "failed", "task_id": tid}
    _scripted_login(monkeypatch)
    assert (await _stop_endpoint(app)).status == "down"


async def test_a_check_whose_task_cannot_be_looked_up_keeps_the_stop_draining(monkeypatch):
    from hpc_bridge.server import _stop_endpoint

    app, runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"

    def broken(tid):
        raise RuntimeError("service unreachable")

    runner.lookup = broken
    _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "its state could not be read from the service" in res.notice


@pytest.mark.parametrize("pbs", [False, True])
async def test_a_fresh_user_endpoints_first_pilot_cancelled_ends_its_check_and_one_stop_is_down(monkeypatch, pbs):
    # the design: a fresh compute user endpoint per block request, so the pilot this stop cancels is that endpoint's
    # first — parsl reads it MISSING at its next status poll and fails our check (bad state). The stop waits (bounded)
    # for that, releasing again as it looks, so one call usually ends `down`.
    from hpc_bridge import config
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, runner = _queued_app(monkeypatch, pbs=pbs, fresh=True)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    job = "4.pbs" if pbs else "4"
    looks = {"n": 0}

    def lookup(tid):  # given up ~60 s after the cancel: the first look sees it queued, a later one failed
        looks["n"] += 1
        return ({"pending": True, "status": "waiting-for-nodes"} if skew[0] < 60 else
                {"pending": False, "status": "failed", "task_id": tid})

    runner.lookup = lookup
    log = _scripted_login(monkeypatch, releases=[f"released {job}\n"])
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.spend_stopped is True
    assert f"compute block released over AMQP (released {job})" in res.notice
    assert "has given up hpc-bridge's check" in res.notice and looks["n"] >= 2
    assert skew[0] <= config.stop_check_wait_s() and _kinds(log).count("release") >= 2


async def test_the_stops_wait_for_a_check_is_bounded_and_says_what_it_waited(monkeypatch):
    from hpc_bridge import config
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, runner = _queued_app(monkeypatch, fresh=True)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner.lookup = lambda tid: {"pending": True, "status": "waiting-for-nodes"}   # never given up
    _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and f"This stop waited {int(config.stop_check_wait_s())} s" in res.notice
    assert config.stop_check_wait_s() - 1 < skew[0] <= config.stop_check_wait_s()
    # an endpoint of an earlier plugin (one user endpoint, which relaunches instead) is not waited on
    skew2 = skew[0]
    app2, _runner2 = _queued_app(monkeypatch, fresh=False)
    assert (await _ensure_endpoint_up(app2, confirm_spend=True)).status == "provisioning"
    _scripted_login(monkeypatch)
    assert (await _stop_endpoint(app2)).status == "draining" and skew[0] == skew2


async def test_a_check_that_ran_on_a_block_that_came_up_during_the_stop_gets_that_block_cancelled(monkeypatch):
    # the check resolves OK during the stop: a block came up for it and ran it — still up, so one more release cancels
    # it, and only then `down`
    from hpc_bridge.server import _stop_endpoint

    skew = _skew(monkeypatch)
    _settle_clock(monkeypatch, skew)
    app, runner = _queued_app(monkeypatch, fresh=True)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner.lookup = lambda tid: ({"pending": True, "status": "waiting-for-launch"} if skew[0] < 10 else
                                 {"pending": False, "status": "success", "task_id": tid})
    log = _scripted_login(monkeypatch, releases=["released none\n", "released none\n", "released 5\n"])
    res = await _stop_endpoint(app)
    assert res.status == "down" and "released 5" in res.notice and "ran hpc-bridge's last check" in res.notice
    assert _kinds(log).count("release") == 3


async def test_a_check_sent_while_the_stop_released_is_counted_as_queued(monkeypatch):
    # the release runs off the lock: a concurrent ensure_endpoint_up can send a new check meanwhile, which this stop
    # never looked at — not a `down`
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.server import _stop_endpoint

    app, runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner.checks[0].fail(RuntimeError("bad state"))   # the endpoint gave the first check up
    busy = {}

    async def run(a, command, session_id="default", shape="compute"):
        if "HPCB_ERR" not in command and not busy:
            busy["ensure"] = await _ensure_endpoint_up(app)   # the concurrent call: it sends check-2
        return ShellOutcome(phase="complete", exit_code=0, stdout="released none\n" if "HPCB_ERR" not in command
                            else "", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    res = await _stop_endpoint(app)
    assert busy["ensure"].status == "provisioning" and [c.task_id for c in runner.checks] == ["check-1", "check-2"]
    assert res.status == "draining" and "(task check-2) is still queued on the endpoint" in res.notice


async def test_a_task_of_ours_on_the_block_is_still_refused_before_any_check_is_looked_at(monkeypatch):
    # the busy refusal comes first: releasing would not end our work (the endpoint relaunches for it)
    from concurrent.futures import Future

    from hpc_bridge.context import TaskHandle
    from hpc_bridge.server import _stop_endpoint

    app, _runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    app.tasks["compute-1"] = TaskHandle(future=Future(), shape="compute", session_id="default", command="sleep 180",
                                        submitted_at=time.monotonic(), ceiling_s=1780.0)
    log = _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "up" and "can't stop yet" in res.notice and log == []


async def test_stop_after_a_rejected_submission_is_down_and_says_nothing_was_created(monkeypatch):
    from hpc_bridge.server import _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app()
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_SBATCH_REFUSED))
    res = await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)
    assert res.status == "down" and "REJECTED" in res.notice and "The new block was not created" in res.notice
    log = _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    # the check failed with the rejection: nothing queued, nothing created — one look, and the words agree with
    # the REJECTED answer instead of saying a block was released or may still land
    assert stop.status == "down" and stop.spend_stopped is True and _kinds(log) == ["release"]
    assert "nothing to cancel" in stop.notice and "REJECTED" in stop.notice and "no block was created" in stop.notice
    assert "released over AMQP" not in stop.notice and "may land" not in stop.notice
    assert "compute" not in app.shapes


async def test_rejected_submission_on_pbs_is_down_with_one_look(monkeypatch):
    # the PBS twin of the REJECTED → `down` (qsub refused: no job id)
    from hpc_bridge.server import _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=(
            "Failed to start block 0: Cannot launch job parsl.pbs.0: Could not read job ID from submit command "
            "standard output; recode=38, stderr='qsub: Job violates queue and/or server resource limits'")))
    res = await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)
    assert res.status == "down" and "REJECTED" in res.notice
    log = _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "down" and stop.spend_stopped is True and "nothing to cancel" in stop.notice
    assert [c for k, c in log] and all("qdel" in c for k, c in log) and _kinds(log) == ["release"]


@pytest.mark.parametrize("pbs", [False, True])
async def test_a_dead_pilot_whose_check_is_still_queued_is_draining_naming_it(monkeypatch, pbs):
    from hpc_bridge.server import _stop_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch, pbs=pbs)
    job = "12.pbsserver" if pbs else "12"
    _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and f"Pilot {job} FINISHED (exit status 42) before it ran it." in res.notice
    assert "is still queued on the endpoint" in res.notice
    assert "still being submitted" not in res.notice and "idle-release" not in res.notice


@pytest.mark.parametrize("pbs", [False, True])
async def test_a_dead_pilot_whose_check_was_given_up_is_down_naming_it(monkeypatch, pbs):
    # a fresh user endpoint's first pilot died before it connected: parsl reads it MISSING and fails our check — the
    # stop's look sees that, and says so with the exit status the provisioning answer reported
    from hpc_bridge.server import _stop_endpoint

    app, runner = await _dead_pilot_app(monkeypatch, pbs=pbs)
    job = "12.pbsserver" if pbs else "12"
    runner.checks[0].fail(RuntimeError("TooManyJobFailuresError: Error 1: EXIT CODE: 42"))
    log = _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "down" and res.spend_stopped is True and _kinds(log) == ["release"]
    assert (f"Pilot {job} FINISHED (exit status 42) before it ran hpc-bridge's check for the block on the default "
            f"{'queue' if pbs else 'partition'} (task check-1), and the endpoint has given it up, so it starts no block "
            "for it") in res.notice
    assert all("qdel" in c for _, c in log) if pbs else all("scancel" in c for _, c in log)


async def test_a_replacement_pilot_cancelled_while_the_check_is_still_queued_is_not_down(monkeypatch):
    # an endpoint of an earlier plugin (one user endpoint): cancelling a replacement does not end our check, so the
    # endpoint submits another — `draining`, saying what this stop cancelled
    from hpc_bridge.server import _stop_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch)
    _scripted_login(monkeypatch, releases=["released 13\n"])
    res = await _stop_endpoint(app)
    assert res.status == "draining" and res.notice.startswith("compute block released over AMQP (released 13).")
    assert "is still queued on the endpoint" in res.notice


async def test_ensure_records_a_dead_pilot_and_a_worker_answering_resolves_the_check(monkeypatch):
    from hpc_bridge.server import _stop_endpoint

    app, runner = await _dead_pilot_app(monkeypatch)
    rt = app.shapes["compute"]
    assert (rt.dead_pilot.job, rt.dead_pilot.exit_status) == ("12", "42") and "12" in rt.dead_pilot.known
    runner._canary = _job_answer("14")   # a block comes up and a worker answers: the queued check ran
    assert (await _ensure_endpoint_up(app)).status == "up"
    assert rt.dead_pilot is None and runner.sent_checks() == []
    log = _scripted_login(monkeypatch, releases=["released 14\n"])
    res = await _stop_endpoint(app)
    assert res.status == "down" and _kinds(log) == ["release"]


async def test_a_switch_keeps_the_old_terms_check_and_the_stop_names_it(monkeypatch):
    # confirm on debug (its check queued), switch to compute: the old runner is retired with its check, labelled with
    # the terms it asked for — a stop still waits for it and names that block
    from hpc_bridge.server import _stop_endpoint

    app, _runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "provisioning"
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    eid = app.state.endpoint_id
    assert [c.terms for c in app.sent_checks[eid]] == [("debug", None)]
    _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "check for the block on partition 'debug' (task check-1)" in res.notice


async def test_a_reap_found_by_a_check_then_a_switch_and_a_rejection_keeps_the_stop_honest(monkeypatch):
    # Review R1: a warm block on A (job 112) stops answering — the timeout reap says its check "may already have asked
    # the facility for a new block". The switch to B and its confirmation come in one call, and B is REJECTED. A's check
    # is still queued on A's user endpoint: the stop must not answer `down`, and names A's block.
    from hpc_bridge.server import _stop_endpoint

    app, runner = _queued_app(monkeypatch)
    runner._canary = _job_answer("112")
    assert (await _ensure_endpoint_up(app, partition="A", confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.warm_confirmed_at -= 100
    runner._canary = CanaryResult(ok=False, error="timeout")
    reap = await _ensure_endpoint_up(app)
    assert reap.status == "needs_confirmation" and "did not answer a check" in reap.notice
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_SBATCH_REFUSED))
    assert (await _ensure_endpoint_up(app, partition="B", confirm_spend=True)).status == "down"
    _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "draining" and "check for the block on partition 'A'" in stop.notice


async def test_a_switch_then_a_rejection_says_what_exists_and_the_stop_names_the_old_block(monkeypatch):
    # Review R6: the ensure said "The new block was not created", the stop then "the compute block was still being
    # submitted … one may land shortly"; and the receipt before the rejection said "…this new one included"
    from hpc_bridge.server import _stop_endpoint

    app, _runner = _queued_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "provisioning"
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_SBATCH_REFUSED))
    res = await _ensure_endpoint_up(app, partition="compute", confirm_spend=True)
    assert res.status == "down" and "this new one included" not in res.notice
    assert res.notice.startswith("The partition or account changed from partition 'debug' in this call. Nothing cancels")
    assert "The scheduler REJECTED the block submission on 'compute'" in res.notice
    _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "draining" and "the compute block was still being submitted" not in stop.notice
    assert "check for the block on partition 'debug' (task check-1) is still queued" in stop.notice
    assert "partition 'compute'" not in stop.notice                    # nothing of the rejected terms was created


async def test_a_probe_that_races_a_switch_never_records_the_old_terms_dead_pilot_on_the_new_terms(monkeypatch):
    # Review R5: the provisioning probe runs off the lock; a switch meanwhile — the old terms' check (with its terms)
    # is the endpoint's to keep, not the new shape's dead pilot to name
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome

    app, _runner = _queued_app(monkeypatch)
    switched = {}

    async def run(a, command, session_id="default", shape="compute"):
        if "HPCB_ERR" in command and not switched:
            switched["r"] = await _ensure_endpoint_up(app, partition="B")   # the concurrent call, while the probe runs
            return ShellOutcome(phase="complete", exit_code=0, stdout="F 12 42 FAILED\n", block_state="warm")
        return ShellOutcome(phase="complete", exit_code=0, stdout="", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    await _ensure_endpoint_up(app, partition="A", confirm_spend=True)
    assert switched["r"].status == "needs_confirmation" and app.shapes["compute"].dead_pilot is None
    assert [c.terms for c in app.sent_checks[app.state.endpoint_id]] == [("A", None)]


async def test_a_reconnect_to_the_same_endpoint_keeps_our_queued_check(monkeypatch):
    # Review R2: a re-bind (connect_facility to the same endpoint — what the FINISHED notice of a facility you brought
    # points at) dropped the shape; its check is still queued there, and the next stop must still see it
    from hpc_bridge import connect
    from hpc_bridge.server import _stop_endpoint
    from tests.fakes import FakeCatalog, fake_entry

    app, _runner = await _dead_pilot_app(monkeypatch, pbs=True)
    eid = app.state.endpoint_id
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: app.facility)

    async def run_login(cmd):
        from hpc_bridge.models import ShellOutcome
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    await connect._connect_facility(app, "anvil", run_login=run_login)
    assert app.state.endpoint_id == eid and "compute" not in app.shapes
    _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "draining" and "Pilot 12.pbsserver FINISHED (exit status 42)" in stop.notice


async def _none():
    return None


async def test_a_teardown_stopped_at_its_code_gate_keeps_our_check_and_a_finished_one_drops_it(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.models import EndpointStatus
    from hpc_bridge.server import _stop_endpoint, _teardown_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch)
    eid = app.state.endpoint_id

    async def gate(a, e, **kw):   # the one-time-code facility: the teardown asks for a code after its release step
        return EndpointStatus(status="up", block_state="cold", endpoint_id=e, notice="needs a one-time code")

    monkeypatch.setattr(server, "_teardown_preauth_gate", gate)
    _scripted_login(monkeypatch)
    assert (await _teardown_endpoint(app)).status == "up"
    assert [getattr(c.future, "task_id", None) for c in app.sent_checks[eid]] == ["check-1"]   # kept by the drop
    _scripted_login(monkeypatch)
    assert (await _stop_endpoint(app)).status == "draining"
    monkeypatch.setattr(server, "_teardown_preauth_gate", lambda *a, **k: _none())
    _scripted_login(monkeypatch)
    assert (await _teardown_endpoint(app)).status == "down" and eid not in app.sent_checks


async def test_a_cold_release_with_a_queued_check_stays_draining_and_an_offline_endpoint_ends_it(monkeypatch):
    from hpc_bridge.server import _stop_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch)
    eid = app.state.endpoint_id
    _scripted_login(monkeypatch, releases=[None] * 3)   # a cold channel: the release could not look
    cold = await _stop_endpoint(app)
    assert cold.status == "draining" and "the login release channel was cold, and hpc-bridge's check" in cold.notice
    assert "this stop cancelled" not in cold.notice and eid in app.sent_checks   # R7: never a cancel it did not make
    app.facility.manager_up = False   # the endpoint went offline: nothing of it can start a block any more
    _scripted_login(monkeypatch, releases=[None] * 3)
    gone = await _stop_endpoint(app)
    assert gone.status == "down" and "ORPHANED" in gone.notice and eid not in app.sent_checks


async def test_a_switch_away_from_a_dead_pilot_then_a_rejection_still_names_the_dead_pilot(monkeypatch):
    from hpc_bridge.server import _stop_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_SBATCH_REFUSED))
    assert (await _ensure_endpoint_up(app, partition="other", confirm_spend=True)).status == "down"
    _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    # the rejection ends nothing of the old terms' user endpoint, where our check is still queued
    assert stop.status == "draining" and "Pilot 12 FINISHED (exit status 42) before it ran it." in stop.notice


async def test_a_rejected_submission_voids_the_spend_confirmation(monkeypatch):
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_SBATCH_REFUSED))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "down"
    canaries = runner.canaries
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and runner.canaries == canaries     # asked again; nothing resubmitted
    assert "REJECTED the last block submission, on partition 'debug'" in res.notice
    assert "repeats a request the scheduler refuses" in res.notice
    res2 = await _ensure_endpoint_up(app, confirm_spend=True)                    # the user confirms: a resubmit
    assert runner.canaries == canaries + 1 and res2.status == "down" and "REJECTED" in res2.notice


async def test_run_shell_surfaces_a_rejection_and_voids_the_confirmation_too(monkeypatch):
    # run_shell's own provisioning can meet the rejection first: it answers REJECTED (failed), and then asks again too
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner._canary = CanaryResult(ok=False, error=_SBATCH_REFUSED)
    out = await _run_shell(app, "hostname")
    assert out.phase == "failed" and "REJECTED" in out.notice and runner.commands == []
    canaries = runner.canaries
    again = await _run_shell(app, "hostname")
    assert again.phase == "needs_confirmation" and "REJECTED the last block submission" in again.notice
    assert runner.canaries == canaries and runner.commands == []


@pytest.mark.parametrize("pbs,query", [(False, "squeue"), (True, "qstat")])
async def test_a_failed_scheduler_command_is_said_as_one_not_a_cold_channel(monkeypatch, pbs, query):
    # Review r2, S1: "release failed: squeue could not be read" was answered as "the login release channel was cold …
    # (the channel is warming)"
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(pbs)
    app.state = EndpointState(endpoint_id="eid-1")
    now = time.monotonic()
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-1", _Res(0, "", "")),
                                         spend_confirmed=True, warm_confirmed_at=now, block_since=now, block_job="112")
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    _scripted_login(monkeypatch, releases=[f"release failed: {query} could not be read\n"] * 3)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and res.spend_stopped is not True
    assert f"a scheduler command on the login node failed — 'release failed: {query} could not be read'" in res.notice
    assert "cold" not in res.notice and "warming" not in res.notice


async def test_a_pbs_release_that_could_not_cancel_an_exiting_job_keeps_what_it_cancelled(monkeypatch):
    # PBS lists a job already EXITING (E) in qstat -f and refuses its qdel: the release used to read as a whole failure
    # (losing the ids it did delete); now it names both, and is not a `down` while one of ours is left
    from hpc_bridge.server import ShapeRuntime, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(True)
    app.state = EndpointState(endpoint_id="eid-1")
    now = time.monotonic()
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-1", _Res(0, "", "")),
                                         spend_confirmed=True, warm_confirmed_at=now, block_since=now, block_job="43.pbs")
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    _scripted_login(monkeypatch, releases=["release failed: qdel refused 41.pbs; cancelled: 43.pbs\n"] * 3)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "'release failed: qdel refused 41.pbs; cancelled: 43.pbs'" in res.notice
    from hpc_bridge import scheduler_ops
    assert scheduler_ops._released_jobs("cancel not confirmed (release failed: qdel refused 41.pbs; cancelled: "
                                        "43.pbs); idle-release will reclaim it") == ["43.pbs"]


@pytest.mark.parametrize("pbs", [False, True])
async def test_a_dead_pilot_after_a_check_failed_with_an_error_is_recorded_and_named(monkeypatch, pbs):
    # Review r2, S2: block 112 up; a check fails with an ERROR (ManagerLost: the block keeps its identity) and the call
    # answers `provisioning`; the probe finds pilot 113 dead. It is recorded (to name), and the stop — the failed check
    # being resolved, nothing queued — names it in its `down`.
    from hpc_bridge.server import _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(pbs)
    j112, j113 = ("112.pbs", "113.pbs") if pbs else ("112", "113")
    r = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=_job_answer(j112))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: r
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.warm_confirmed_at -= 100   # past the TTL
    r._canary = CanaryResult(ok=False, error="ManagerLost: the worker's manager was lost")
    _scripted_login(monkeypatch, probes=[f"F {j112} 1 x\nF {j113} 42 Job failed\n"])
    res = await _ensure_endpoint_up(app)
    assert res.status == "provisioning" and f"pilot {j113} already FINISHED (exit status 42)" in res.notice
    assert rt.dead_pilot is not None and rt.dead_pilot.job == j113
    _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "down" and f"Pilot {j113} FINISHED (exit status 42)" in stop.notice


@pytest.mark.parametrize("pbs", [False, True])
async def test_a_slow_probe_never_reads_this_blocks_dead_pilot_as_an_earlier_sessions(monkeypatch, pbs):
    # Review r2, S3: the confirmation's age was read BEFORE the probe, the login clock while it ran — a probe that
    # waited ~100 s for the login worker moved the cutoff past a pilot submitted 20 s after the confirmation
    from hpc_bridge import server
    from hpc_bridge.models import ShellOutcome

    skew = _skew(monkeypatch)
    app = _ssh_app(pbs)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    t0, base = {}, 1_000_000

    async def run(a, command, session_id="default", shape="compute"):
        if "HPCB_ERR" not in command:
            return ShellOutcome(phase="complete", exit_code=0, stdout="released none\n", block_state="warm")
        if "late" in t0:
            skew[0] += 100.0   # queued behind other login-node work
        login_now = base + int(time.monotonic() - t0["t"])
        rows = f"F 12 42 FAILED\nHPCB_SUB 12 {base + 20}\n" if "late" in t0 else ""
        return ShellOutcome(phase="complete", exit_code=0, stdout=f"HPCB_NOW {login_now}\n{rows}", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", run)
    t0["t"] = time.monotonic()
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    skew[0] += 30.0
    t0["late"] = True
    res = await _ensure_endpoint_up(app)
    assert "pilot 12 already FINISHED (exit status 42)" in res.notice and app.shapes["compute"].dead_pilot.job == "12"


async def test_an_earlier_sessions_dead_pilot_is_not_this_blocks(monkeypatch):
    # the scheduler's history (sacct 6 h, PBS's qstat -x about two weeks) still lists a pilot that died long before this
    # block's spend was confirmed: no FINISHED verdict for it, nothing recorded
    app, _runner = _queued_app(monkeypatch)
    _scripted_login(monkeypatch, probes=["HPCB_NOW 1000000\nF 9 42 FAILED\nHPCB_SUB 9 992800\n"])
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning" and "FINISHED" not in res.notice
    assert app.shapes["compute"].dead_pilot is None
    _scripted_login(monkeypatch, probes=["HPCB_NOW 1000000\nF 9 42 FAILED\nHPCB_SUB 9 992800\nF 12 42 FAILED\n"
                                         "HPCB_SUB 12 999990\n"])
    res = await _ensure_endpoint_up(app)
    assert "pilot 12 already FINISHED (exit status 42)" in res.notice and app.shapes["compute"].dead_pilot.job == "12"


@pytest.mark.parametrize("pbs", [False, True])
async def test_with_several_blocks_a_rejection_stops_the_clock_and_the_stop_is_down(monkeypatch, pbs):
    # Review R4 with its pair: the single-block rule left both the stop's contradiction ("still being submitted" after
    # REJECTED) and, dropped from the void alone, a clock running for an unconfirmed block with no identity
    from hpc_bridge.server import _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(pbs)
    r = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=_job_answer("112"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: r
    _shape_runtime(app, "compute").user_endpoint_config["max_blocks"] = 2
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.warm_confirmed_at -= 100
    r._canary = CanaryResult(ok=False, error=_SBATCH_REFUSED)
    res = await _ensure_endpoint_up(app)
    assert res.status == "down" and rt.warm_since is None and not rt.spend_confirmed and rt.block_since is None
    log = _scripted_login(monkeypatch)
    stop = await _stop_endpoint(app)
    assert stop.status == "down" and "nothing to cancel" in stop.notice and _kinds(log) == ["release"]


# --- a fresh compute user endpoint per block request (warmth._fresh_block) -------------------------------------------

def _fresh_app(*, fresh=True):
    f = FakeFacility()
    f.workers = 1
    f.fresh_ueps = fresh
    app = AppCtx(facility=f, profile=Profile())
    app.built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))

    app.runner_factory = factory
    return app


async def test_each_block_request_gets_its_own_compute_user_endpoint():
    from hpc_bridge import warmth

    app = _fresh_app()
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    rt = app.shapes["compute"]
    first = rt.user_endpoint_config["hpcb_block"]
    compute = [b for b in app.built if b.get("compute")]
    assert compute[-1]["hpcb_block"] == first                         # the runner sends it with every task
    # asking again while that block is still the one requested: the same user endpoint
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    assert rt.user_endpoint_config["hpcb_block"] == first
    # a new block request (the spend asked again: a rejection, a reap, a switch, a stop) — a new one
    warmth._void_rejected(app, rt)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    compute = [b for b in app.built if b.get("compute")]
    assert rt.user_endpoint_config["hpcb_block"] != first and compute[-1]["hpcb_block"] != first
    # the login shape never moves
    login = _shape_runtime(app, "login")
    assert "hpcb_block" not in login.user_endpoint_config


async def test_a_confirmation_over_a_block_already_up_keeps_its_user_endpoint():
    # a warm block (or a replacement already answering after a reap) must not be abandoned for a new user endpoint
    app = _fresh_app()
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=_job_answer("112"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    block = rt.user_endpoint_config["hpcb_block"]
    rt.warm_confirmed_at -= 100
    runner._canary = _job_answer("113")                                  # a different job answers: a kicked reap
    assert (await _ensure_endpoint_up(app)).status == "needs_confirmation"
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.user_endpoint_config["hpcb_block"] == block and rt.block_job == "113"


async def test_a_block_that_idled_out_gets_a_fresh_user_endpoint_on_the_same_endpoint():
    # its compute user endpoint exits with it (idle_heartbeats_soft): the next block request is a new one, on the same
    # manager — an idle-exited user endpoint is not a dead endpoint, nothing is stood up again
    app = _fresh_app()
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=_job_answer("112"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    block, eid = rt.user_endpoint_config["hpcb_block"], app.state.endpoint_id
    rt.warm_confirmed_at -= 10_000
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and "idle-released" in res.notice
    runner._canary = _job_answer("113")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.user_endpoint_config["hpcb_block"] != block and app.state.endpoint_id == eid and rt.block_job == "113"


async def test_an_endpoint_whose_template_is_older_keeps_one_compute_user_endpoint():
    app = _fresh_app(fresh=False)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    assert "hpcb_block" not in app.shapes["compute"].user_endpoint_config


async def test_a_facility_endpoint_never_gets_a_block_number(monkeypatch):
    from hpc_bridge.facility.mep import MEPFacility
    from tests.fakes import fake_mep_entry
    from tests.test_mep_server import _Status

    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))
    fac.fresh_ueps = True   # even if something set it: a facility's template is its own
    app = AppCtx(facility=fac, profile=Profile())
    app.state = EndpointState(endpoint_id=fac.endpoint_id, reused=True)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert "hpcb_block" not in app.shapes["compute"].user_endpoint_config


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


def _stub_scheduler(tmp_path, **cmds):
    """A PATH whose scheduler commands are stand-ins: each name maps to (stdout, exit code); `date` answers `+%s` with
    1000000 and `-d '<qstat time>' +%s` like GNU date (macOS's cannot). Returns an env for `bash -c`."""
    import os
    import sys

    d = tmp_path / "bin"
    d.mkdir(parents=True, exist_ok=True)
    for name, (out, rc) in cmds.items():
        (d / name).write_text(f"#!{sys.executable}\nimport sys\nsys.stdout.write({out!r})\n"
                              f"open({str(tmp_path / 'calls')!r}, 'a').write({name!r} + ' ' + ' '.join(sys.argv[1:]) + '\\n')\n"
                              f"sys.exit({rc})\n")
    (d / "date").write_text(f"#!{sys.executable}\nimport sys, datetime\na = sys.argv[1:]\n"
                            "if '-d' in a:\n"
                            "    t = datetime.datetime.strptime(a[a.index('-d') + 1], '%a %b %d %H:%M:%S %Y')\n"
                            "    print(int(t.replace(tzinfo=datetime.timezone.utc).timestamp()))\n"
                            "else:\n    print(1000000)\n")
    for f in d.iterdir():
        f.chmod(0o755)
    return {**os.environ, "PATH": f"{d}:/usr/bin:/bin", "USER": "u", "HOME": str(tmp_path)}


def _bash(cmd, env):
    import subprocess

    return subprocess.run(["bash", "-c", cmd], env=env, capture_output=True, text=True, timeout=30).stdout


@pytest.mark.parametrize("scheduler,query,cancel", [("slurm", "squeue", "scancel"), ("pbs", "qstat", "qdel")])
def test_the_release_says_a_failed_scheduler_command_never_released_none(tmp_path, scheduler, query, cancel):
    # `squeue … 2>/dev/null | …` turned an unreachable scheduler into an empty listing, i.e. "released none": a
    # confirmed release, which now feeds the `down` after a rejected submission and a pilot watch's end
    from hpc_bridge.server import _release_cmd

    eid = "eid-1"
    if scheduler == "slurm":
        two = f"41 /h/uep.{eid}.a/x.stdout\n43 /h/uep.{eid}.a/y.stdout\n42 /h/uep.other.a/z.stdout\n"
    else:
        two = "".join(f"Job Id: {j}.pbs\n    Output_Path = login:/h/uep.{e}.a/x.stdout\n"
                      for j, e in (("41", eid), ("43", eid), ("42", "other")))
    cmd = _release_cmd(scheduler, eid)
    jobs = "41 43" if scheduler == "slurm" else "41.pbs 43.pbs"
    assert _bash(cmd, _stub_scheduler(tmp_path / "a", **{query: ("", 1)})).strip() == \
        f"release failed: {query} could not be read"
    assert _bash(cmd, _stub_scheduler(tmp_path / "b", **{query: (two, 0), cancel: ("", 1)})).strip() == \
        f"release failed: {cancel} refused {jobs}; cancelled: none"
    assert _bash(cmd, _stub_scheduler(tmp_path / "c", **{query: ("", 0)})).strip() == "released none"
    assert _bash(cmd, _stub_scheduler(tmp_path / "d", **{query: (two, 0), cancel: ("", 0)})).strip() == \
        f"released {jobs}"                                   # every id on the one line, never another endpoint's
    calls = (tmp_path / "d" / "calls").read_text()
    assert all(f"{cancel} {j}\n" in calls for j in jobs.split())               # one cancel per job
    # the scheduler refuses ONE (PBS: a job already exiting, still listed): the line says which, and keeps the other
    env = _stub_scheduler(tmp_path / "e", **{query: (two, 0)})
    first, second = jobs.split()
    stub = tmp_path / "e" / "bin" / cancel
    stub.write_text(f"#!/bin/sh\n[ \"$1\" = {first} ] && exit 35\nexit 0\n")
    stub.chmod(0o755)
    assert _bash(cmd, env).strip() == f"release failed: {cancel} refused {first}; cancelled: {second}"


@pytest.mark.parametrize("scheduler", ["slurm", "pbs"])
def test_the_pilot_probe_says_a_failed_scheduler_query_and_when_each_finished_pilot_was_submitted(tmp_path, scheduler):
    from hpc_bridge.scheduler_ops import _pilot_status_cmd

    eid = "eid-1"
    cmd = _pilot_status_cmd(scheduler, eid)
    if scheduler == "slurm":
        ok = {"squeue": ("", 0), "sacct": (f"FAILED|1108|42:0|sbatch /h/uep.{eid}.a/s/p.1|999000\n", 0)}
        out = _bash(cmd, _stub_scheduler(tmp_path / "ok", **ok))
        assert "HPCB_NOW 1000000" in out and "F 1108 42 FAILED" in out and "HPCB_SUB 1108 999000" in out
        assert "HPCB_FAIL" not in out
        out = _bash(cmd, _stub_scheduler(tmp_path / "q", squeue=("", 1), sacct=("", 0)))
        assert "HPCB_FAIL squeue" in out and "HPCB_FAIL sacct" not in out
        out = _bash(cmd, _stub_scheduler(tmp_path / "a", squeue=("", 0), sacct=("", 1)))
        assert "HPCB_FAIL sacct" in out and "HPCB_FAIL squeue" not in out
    else:
        rec = (f"Job Id: 1108.pbs\n    job_state = F\n    Exit_status = 42\n    ctime = Thu Jan 01 12:00:00 1970\n"
               f"    Output_Path = login:/h/uep.{eid}.a/s/y.stdout\n")
        out = _bash(cmd, _stub_scheduler(tmp_path / "ok", qstat=(rec, 0)))
        assert "HPCB_NOW 1000000" in out and "F 1108.pbs 42" in out and "HPCB_SUB 1108.pbs 43200" in out
        assert "HPCB_QT" not in out and "HPCB_FAIL" not in out
        assert "HPCB_FAIL qstat" in _bash(cmd, _stub_scheduler(tmp_path / "q", qstat=("", 2)))


async def test_the_probe_reads_a_failed_listing_as_no_verdict_and_leaves_out_an_earlier_sessions_dead_pilot():
    from hpc_bridge import scheduler_ops
    from hpc_bridge.models import ShellOutcome

    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))

    def runner(stdout):
        async def run(cmd):
            return ShellOutcome(phase="complete", exit_code=0, stdout=stdout, block_state="warm")
        return run

    # the live listing failed: no verdict (never "no pilot … likely REJECTED"), and the notice is left as it was
    failed = "HPCB_NOW 1000000\nHPCB_FAIL squeue\n"
    assert await scheduler_ops._pilot_status_over_login(app, "eid-1", 999, runner(failed)) is None
    notice, probe = await scheduler_ops._augment_provisioning(app, "eid-1", "allocating nodes…", 999, runner(failed))
    assert notice == "allocating nodes…" and probe.failed == {"squeue"} and probe.dead is None
    # a pilot that died 2 h before this block's spend was confirmed is an earlier session's: not this block's verdict
    old = "HPCB_NOW 1000000\nF 1108 42 FAILED\nHPCB_SUB 1108 992000\n"
    probe = await scheduler_ops._probe_pilots_over_login(app, "eid-1", 999, runner(old), confirmed_at=time.monotonic() - 600)
    assert probe.category != "finished" and probe.dead is None and "1108" in probe.jobs
    fresh = "HPCB_NOW 1000000\nF 1108 42 FAILED\nHPCB_SUB 1108 999700\n"
    probe = await scheduler_ops._probe_pilots_over_login(app, "eid-1", 999, runner(fresh), confirmed_at=time.monotonic() - 600)
    assert probe.dead == ("1108", "42")
    unknown = "HPCB_NOW 1000000\nF 1108 42 FAILED\n"            # no submit time: it counts (the safe side)
    probe = await scheduler_ops._probe_pilots_over_login(app, "eid-1", 999, runner(unknown), confirmed_at=time.monotonic() - 600)
    assert probe.dead == ("1108", "42")


async def test_a_failed_release_is_never_a_confirmed_one(monkeypatch):
    # the line the one-liner prints for a failed scheduler command is unconfirmed
    from hpc_bridge import scheduler_ops
    from hpc_bridge.models import ShellOutcome

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = AppCtx(facility=FakeFacility(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))

    async def failing(cmd):
        return ShellOutcome(phase="complete", exit_code=0, stdout="release failed: squeue could not be read\n",
                            block_state="warm")

    confirmed, detail = await scheduler_ops._release_blocks_over_login(app, "eid-1", failing)
    assert confirmed is False and "release failed: squeue could not be read" in detail


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


async def test_stop_after_a_dead_pbs_pilot_is_draining_naming_it(monkeypatch):
    # the PBS twin: the release rides qdel, and the words come from our check — still queued, so `draining`, naming the
    # pilot that died before it ran it (no scheduler-history read at the stop: the check is the evidence)
    from hpc_bridge.server import _stop_endpoint

    app, _runner = await _dead_pilot_app(monkeypatch, pbs=True)
    log = _scripted_login(monkeypatch)
    res = await _stop_endpoint(app)
    assert res.status == "draining" and "Pilot 12.pbsserver FINISHED (exit status 42) before it ran it." in res.notice
    assert "check for the block on the default queue (task check-1) is still queued" in res.notice
    assert "still being submitted" not in res.notice and "idle-release" not in res.notice
    assert "keeps resubmitting" not in res.notice and "only teardown" not in res.notice
    assert _kinds(log) == ["release"] and all("qdel" in c for _, c in log)


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
    monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", "0.05")
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
    monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", "0.05")
    assert (await _teardown_endpoint(app)).status == "tearing_down"
    app.state = EndpointState(endpoint_id="eid-2")  # a connect_facility meanwhile bound a fresh endpoint
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    release.set()
    res = await _teardown_endpoint(app)
    assert res.status == "down" and res.endpoint_id == "eid-1"
    assert app.state.endpoint_id == "eid-2" and "login" in app.shapes  # the new binding survives


# -- the HPC_BRIDGE_TEARDOWN_WAIT_S test seam (config.teardown_wait_s) -------------------------------------------------
# The agentic scenario `teardown_slow_fs` shortens the wait so the server-task path (`tearing_down`, then a call again)
# shows on the fake cluster, whose whole teardown takes ~5 s — inside the 60 s default, so it never answered
# `tearing_down` live.

@pytest.mark.parametrize(("raw", "want", "said"), [
    (None, 60.0, False), ("", 60.0, False), ("  ", 60.0, False), ("1", 1.0, False), (" 2.5 ", 2.5, False),
    ("0", 0.0, False), ("60", 60.0, False),
    ("abc", 60.0, True), ("0x10", 60.0, True),    # not a number: the default, said once (not at every call)
    ("-1", 60.0, True),                           # below the range: the default
    ("600", 60.0, True),                          # longer would risk the MCP client's tool window: capped
    ("inf", 60.0, True), ("nan", 60.0, True)])
def test_the_teardown_wait_seam_only_shortens_the_wait_within_bounds(monkeypatch, capsys, raw, want, said):
    from hpc_bridge import config
    from hpc_bridge.config import TEARDOWN_WAIT_S, teardown_wait_s
    monkeypatch.setattr(config, "_TEARDOWN_SEAM_NOTED", set())
    if raw is None:
        monkeypatch.delenv("HPC_BRIDGE_TEARDOWN_WAIT_S", raising=False)
    else:
        monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", raw)
    assert TEARDOWN_WAIT_S == 60.0 and teardown_wait_s() == want
    err = capsys.readouterr().err
    assert ("is not a number of seconds in [0, 60]" in err) is said and len(err.splitlines()) == int(said)
    teardown_wait_s()
    assert capsys.readouterr().err == ""   # said once, not at every teardown call


@pytest.mark.parametrize(("raw", "want"), [(None, 60.0), ("1", 1.0), ("0.05", 0.05)])
async def test_one_teardown_call_waits_as_long_as_the_seam_says(monkeypatch, raw, want):
    # what the wait IS: the bound on asyncio.wait over the server-side teardown task, read at each call
    import asyncio

    from hpc_bridge import server
    from hpc_bridge.server import ShapeRuntime, _teardown_endpoint

    release = asyncio.Event()

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            await release.wait()
            return {"deleted": True, "credentials_wiped": True}

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    waits: list[float | None] = []
    real_wait = asyncio.wait

    async def spy_wait(fs, *, timeout=None, **kw):
        waits.append(timeout)
        return await real_wait(fs, timeout=0, **kw)   # never actually wait the default minute

    if raw is None:
        monkeypatch.delenv("HPC_BRIDGE_TEARDOWN_WAIT_S", raising=False)
    else:
        monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", raw)
    monkeypatch.setattr(server.asyncio, "wait", spy_wait)
    try:
        assert (await _teardown_endpoint(app)).status == "tearing_down"
        monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", "2")   # read at EACH call, not once at import
        assert (await _teardown_endpoint(app)).status == "tearing_down"
    finally:
        release.set()
        await real_wait({app.teardown_task}, timeout=5)
    assert waits == [want, 2.0]


async def test_a_short_teardown_wait_hands_back_tearing_down_and_the_next_call_the_result(monkeypatch):
    # the path the scenario exercises, end to end at the seam: a 1 s wait outlived by the login-node ops answers
    # `tearing_down` (nothing cleared yet); the ops finish with nobody waiting; the next call replays THAT teardown's
    # `down` for the same endpoint — the state cleared by the `ours` branch — and starts no second teardown
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
    monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", "0.05")
    first = await asyncio.wait_for(_teardown_endpoint(app), 5)   # the 60 s default would time this out
    assert first.status == "tearing_down" and first.endpoint_id == "eid-1" and torn == []
    release.set()
    await asyncio.wait({app.teardown_task}, timeout=5)           # the ops finish with nobody waiting
    assert app.state.endpoint_id is None and app.shapes == {}     # ...and cleared the state: it was still ours
    res = await asyncio.wait_for(_teardown_endpoint(app), 5)
    assert res.status == "down" and res.endpoint_id == "eid-1" and "fully torn down" in res.notice
    assert torn == ["eid-1"] and app.teardown_task is None        # the finished result, replayed; no second teardown


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


def test_note_dispatch_counts_a_result_too_big_to_return_as_the_worker_answering(monkeypatch):
    # the worker ran the command; only its result was over Compute's size limit — not a lost worker
    from globus_compute_sdk.errors import TaskExecutionFailed

    import hpc_bridge.server as srv
    from hpc_bridge.dispatch import failure_outcome
    from hpc_bridge.server import ShapeRuntime

    rt = ShapeRuntime(user_endpoint_config={})
    rt.warm_confirmed_at = 5.0
    monkeypatch.setattr(srv.time, "monotonic", lambda: 999.0)
    srv._note_dispatch(rt, failure_outcome(TaskExecutionFailed("MaxResultSizeExceeded(12000000, 10485760)"), "warm"))
    assert rt.warm_confirmed_at == 999.0
    srv._note_dispatch(rt, failure_outcome(TaskExecutionFailed("parsl...WorkerLost: worker died"), "warm"))
    assert rt.warm_confirmed_at is None  # any other remote failure still forces a re-canary


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


async def test_login_shell_tool_hands_the_agent_the_marked_end():
    from hpc_bridge import server

    class _SshFacility(FakeFacility):
        async def login_exec(self, command):
            return (0, "".join(f"job {i}\n" for i in range(1, 5001)), "")

    res = await server.login_shell("squeue", _Ctx(AppCtx(facility=_SshFacility(), profile=Profile())))
    assert res.stdout.startswith("[hpc-bridge: stdout too long") and res.stdout.endswith("job 5000\n")
    assert "at least" not in res.stdout  # SSH output: no SDK line limit before ours
    assert res.notice and res.notice.startswith("stdout too long for one result")


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
        command="sleep 999", submitted_at=time.monotonic(), ceiling_s=1780.0,
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
                                        submitted_at=time.monotonic(), ceiling_s=1780.0)  # not done: still RUNNING

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


async def test_reaped_block_under_a_live_session_reasks_spend_then_recovers():
    # The return-later flow (agentic: block_reaped_resume): the server process lives on with its in-memory state,
    # the pilot block is reaped underneath it (idle-release / walltime / scancel), and the user comes back and runs
    # again. Warm -> needs_confirmation with the reason (nothing dispatched, no canary, spend clock stopped) -> a
    # fresh confirmation for the new block -> honest provisioning -> complete on the new block, same session.
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

    # long past the idle window: the block idle-released, and the server knows it without looking
    rt.warm_confirmed_at -= 10_000
    canaries = runner.canaries
    out = await _run_shell(app, "cat marker")
    assert out.phase == "needs_confirmation" and out.block_state == "cold"
    assert "idle-released" in out.notice and "Confirm spend again" in out.notice
    assert len(runner.commands) == 1 and runner.canaries == canaries  # nothing submitted: no new block requested
    assert rt.warm_since is None            # the spend clock stopped (banked), not left running through the gap
    assert rt.spend_confirmed is False      # the acknowledgement covered the reaped block only

    # the user confirms the new block; it cold-starts honestly, then a worker answers and the session carries on
    runner._canary = CanaryResult(ok=False, error="timeout")
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning"
    runner._canary = CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9")
    out = await _run_shell(app, "cat marker")
    assert out.phase == "complete" and out.block_state == "warm"
    assert "HPCB_REAP" in out.stdout and len(runner.commands) == 2
    assert rt.warm_since is not None        # a fresh clock for the new block


# --- a scheduler job submitted by hand (2026-10-10) ------------------------------------------------------------------
# A job submitted from the free login shape, or from inside a compute block (a new job beside it), bills the
# allocation, yet the spend floor never sees it; the result says so. The shared corpus (tests/submission_corpus.py)
# holds the commands — the harness grader is held to the same one.


def test_scheduler_submission_matches_the_shared_corpus():
    from hpc_bridge.scheduler_ops import _scheduler_submission
    from tests.submission_corpus import CORPUS, KNOWN_LIMITS

    for cmd, inside_job, want in CORPUS:
        assert _scheduler_submission(cmd, inside_job=inside_job) == want, (cmd, inside_job)
    for cmd, inside_job in KNOWN_LIMITS:  # documented misses: if one starts matching, move it into the corpus
        assert _scheduler_submission(cmd, inside_job=inside_job) is None, cmd


def test_scheduler_submission_is_linear_and_bounded():
    import time

    from hpc_bridge.scheduler_ops import _scheduler_submission

    for cmd in ("echo " + "A" * 1_000_000 + " | base64 -d > f", 'echo "' + "A" * 1_000_000 + '" > f',
                'echo "' + "$(" * 20_000, 'echo "' + "`a" * 20_000 + '"', '"$(' * 2_000 + "sbatch x" + ')"' * 2_000):
        t0 = time.perf_counter()
        _scheduler_submission(cmd)  # nesting past the depth cap is not analysed; nothing raises
        assert time.perf_counter() - t0 < 1.0, cmd[:40]


def _login_app(res, *, pending=False):
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", res, pending=pending)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    return app


async def test_login_shape_submission_says_it_bills_and_is_not_tracked():
    app = _login_app(_Res(0, "Submitted batch job 42\n", ""))
    out = await _run_shell(app, "cd ~/x && sbatch job.sh", shape="login")
    assert out.phase == "complete" and out.stdout.startswith("Submitted")  # never blocked: the user may have asked
    assert "submitted a scheduler job from the login node" in out.notice and "bills the user's allocation" in out.notice
    assert "stop_endpoint does not cancel it" in out.notice and "spend confirmation" in out.notice
    assert "`scancel <jobid>`" in out.notice
    out = await _run_shell(app, "qsub -q debug job.pbs", shape="login")
    assert "`qdel <jobid>`" in out.notice
    assert (await _run_shell(app, "squeue -u $USER; command -v sbatch", shape="login")).notice is None  # a mention


async def test_a_refused_submission_says_no_job_was_created():
    app = _login_app(_Res(1, "", "sbatch: error: invalid partition specified: nope"))
    out = await _run_shell(app, "cd run && sbatch -p nope job.sh", shape="login")
    assert "exited 1 with no job id" in out.notice and "none was created and nothing is billed" in out.notice
    assert "Cancel" not in out.notice and "spend" in out.notice


async def test_a_nonzero_exit_claims_a_refusal_only_when_it_is_the_schedulers_own_answer():
    # Review round 2 (2026-10-10): the line's exit code is the scheduler's answer only when the submit is the LAST
    # command, prints no receipt, and does not wait for the job — else jobs may well be queued and billing.
    for res, cmd in (
        (_Res(1, "Submitted batch job 41\nSubmitted batch job 42\n", "sbatch: error: QOSMaxSubmitJobPerUserLimit"),
         "for f in a.sh b.sh c.sh; do sbatch $f; done"),                       # two queued, the third refused
        (_Res(1, "Submitted batch job 42\n", ""), "sbatch job.sh; squeue -u $USER | grep -q RUNNING"),
        (_Res(3, "", ""), "sbatch --wait job.sh"),                              # the JOB's exit code
        (_Res(2, "", ""), "qsub -W block=true job.pbs"),
        (_Res(1, "", ""), "sbatch job.sh && tail -1 out.log"),                  # the exit code may be tail's
        (_Res(123, "Submitted batch job 41\nSubmitted batch job 42\n", "sbatch: error: QOSMaxSubmitJobPerUserLimit"),
         "ls *.sh | xargs -n1 sbatch"),                                         # last, but receipts were printed
        # dependency chains: the first job's id is CAPTURED (no receipt in stdout) and only the second is refused
        (_Res(1, "", "sbatch: error: Job dependency problem"),
         "JID=$(sbatch --parsable a.sh); sbatch --dependency=afterok:$JID b.sh"),
        (_Res(1, "", "sbatch: error: QOSMaxSubmitJobPerUserLimit"),
         "sbatch --parsable a.sh > a.jid && sbatch -d afterok:$(cat a.jid) b.sh"),
        (_Res(1, "", "qsub: illegal -W value"), "J1=$(qsub a.pbs); qsub -W depend=afterok:$J1 b.pbs"),
        (_Res(124, "", ""), "timeout 60 sbatch job.sh"),                          # 124 is timeout's, not sbatch's
    ):
        out = await _run_shell(_login_app(res), cmd, shape="login")
        assert "nothing is billed" not in out.notice and "may have submitted" in out.notice, cmd
        assert "lists what was queued" in out.notice and "Cancel it with" in out.notice, cmd


async def test_a_job_receipt_in_the_output_means_a_job_may_exist():
    # The one direct submit exited non-zero but printed a receipt: Slurm --parsable prints the bare id, PBS its job id.
    out = await _run_shell(_login_app(_Res(1, "4242\n", "")), "sbatch --parsable job.sh", shape="login")
    assert "nothing is billed" not in out.notice and "`squeue -u $USER` lists what was queued" in out.notice
    out = await _run_shell(_login_app(_Res(1, "12345.pbs01.example.org\n", "")), "qsub job.pbs", shape="login")
    assert "nothing is billed" not in out.notice and "`qstat -u $USER` lists what was queued" in out.notice
    assert "`qdel <jobid>`" in out.notice
    out = await _run_shell(_login_app(_Res(1, "", "")), "qsub job.pbs", shape="login")
    assert "nothing is billed" in out.notice     # no receipt, the only submit, its own exit: refused


async def test_login_shell_reads_the_receipt_too():
    from hpc_bridge.server import _login_shell

    class _Ssh(FakeFacility):
        async def login_exec(self, command):
            return (1, "Submitted batch job 9\n", "sbatch: warning: something")

    res = await _login_shell(AppCtx(facility=_Ssh(), profile=Profile()), "sbatch job.sh")
    assert "nothing is billed" not in res.notice and "may have submitted" in res.notice


async def test_the_submission_is_recognised_before_the_command_is_dispatched(monkeypatch):
    # Detection must not sit between the submit and its poll handle (a cancel there would orphan a running task).
    from hpc_bridge import scheduler_ops

    events = []
    real = scheduler_ops._submission

    def spy(command, **kw):
        events.append("detect")
        return real(command, **kw)

    monkeypatch.setattr(scheduler_ops, "_submission", spy)
    app = _login_app(_Res(0, "", ""), pending=True)
    runner = app.runner_factory("fake-eid")
    submit = runner.submit
    runner.submit = lambda cmd: (events.append("submit"), submit(cmd))[1]
    out = await _run_shell(app, "sbatch job.sh", shape="login")
    assert out.phase == "running" and events == ["detect", "submit"]


async def test_a_finished_srun_is_reported_in_the_past_without_a_cancel():
    app = _login_app(_Res(0, "c001\n", ""))
    out = await _run_shell(app, "srun -N1 hostname", shape="login")
    assert "ran as a scheduler job from the login node and has finished" in out.notice
    assert "it billed the user's allocation" in out.notice and "Cancel" not in out.notice


async def test_compute_shape_sbatch_is_a_new_job_but_srun_is_the_blocks_own_step():
    app = _login_app(_Res(0, "Submitted batch job 77\n", ""))
    _confirm_slurm(app)  # the user approved a block, not a 48 h job beside it
    out = await _run_shell(app, "sbatch -t 48:00:00 long.sh")  # shape omitted -> compute
    assert "from inside the compute block (a new job beside this block)" in out.notice
    assert "stop_endpoint does not cancel it" in out.notice
    assert (await _run_shell(app, "srun -n 4 ./mpi_app")).notice is None


async def test_login_shape_submission_still_running_keeps_both_notices():
    app = _login_app(_Res(0, "", ""), pending=True)
    out = await _run_shell(app, "srun -N1 ./long", shape="login")
    assert out.phase == "running" and "poll_task" in out.notice
    assert "`srun` here is running as a scheduler job" in out.notice and "`scancel <jobid>`" in out.notice
    out = await _run_shell(_login_app(_Res(0, "", ""), pending=True), "sleep 60; sbatch x.sh", shape="login")
    assert "submits (or has already submitted) a scheduler job" in out.notice   # not "submitted" while it runs


async def test_poll_task_result_carries_the_submission_notice():
    from hpc_bridge.server import _poll_task

    app = _login_app(_Res(0, "", ""), pending=True)
    runner = app.runner_factory("fake-eid")
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    out = await _run_shell(app, "sleep 300; sbatch long.sh", shape="login")
    assert out.phase == "running"
    runner.futures[0].finish(_Res(0, "Submitted batch job 7\n", ""))
    done = await _poll_task(app, out.task_id)
    assert done.phase == "complete" and "submitted a scheduler job from the login node" in done.notice
    assert "`scancel <jobid>`" in done.notice


async def test_login_shell_submission_carries_the_notice():
    from hpc_bridge.server import _login_shell

    class _Ssh(FakeFacility):
        async def login_exec(self, command):
            return (0, "Submitted batch job 42\n", "")

    app = AppCtx(facility=_Ssh(), profile=Profile())
    res = await _login_shell(app, "sbatch -A lab job.sh")
    assert res.exit_code == 0 and "from the login node" in res.notice and "bills the user's allocation" in res.notice
    assert (await _login_shell(app, "sinfo -h")).notice is None


async def test_submission_notice_never_fails_the_command(monkeypatch):
    from hpc_bridge import scheduler_ops

    def boom(_command, **_kw):
        raise RecursionError("pathological input")

    monkeypatch.setattr(scheduler_ops, "_submission", boom)
    out = await _run_shell(_login_app(_Res(0, "ok", "")), "sbatch job.sh", shape="login")
    assert out.phase == "complete" and out.stdout == "ok" and out.notice is None


def test_long_work_advice_puts_the_spend_on_a_batch_job():
    from hpc_bridge.dispatch import failure_outcome
    from hpc_bridge.notices import _running_outcome

    mep = FakeFacility()
    mep.supported_shapes = ("compute",)  # a compute-only facility: a batch job comes from inside the block, same rule
    for fac in (FakeFacility(), mep):
        notice = _running_outcome(AppCtx(facility=fac, profile=Profile()), "compute-1", 1780).notice
        assert "checkpoint and resume" in notice and "only with the user's spend confirmation" in notice
    assert "spend confirmation" in failure_outcome(TimeoutError(), "warm").notice
