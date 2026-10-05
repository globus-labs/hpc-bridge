"""Spend is asked again after the block it covered is gone (0.1.18).

The `confirm_spend` acknowledgement covers ONE block. When that block is gone — idle-released, past its walltime, or
found dead by a check — the next call answers `needs_confirmation` with the reason, even one that carries
`confirm_spend=True`, and the spend clock stops at the estimated release time.
"""
import time
from concurrent.futures import Future

from hpc_bridge.context import TaskHandle
from hpc_bridge.cost import _block_nodes
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _run_shell, _shape_runtime
from hpc_bridge.warmth import _presumed_reaped
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _Res

_OK = CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9")
_TIMEOUT = CanaryResult(ok=False, error="timeout")


async def _warm_app(facility=None):
    """A billed compute shape, spend confirmed, one command run on a warm block."""
    f = facility or FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "ok", ""))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    rt = _shape_runtime(app, "compute")
    rt.spend_confirmed = True
    out = await _run_shell(app, "true")
    assert out.phase == "complete" and rt.block_since is not None
    return app, rt, runner


async def test_idle_release_is_presumed_before_anything_is_submitted():
    app, rt, runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 60
    canaries, commands = runner.canaries, len(runner.commands)

    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "cold"
    assert "idle-released" in out.notice and "Confirm spend again" in out.notice
    assert runner.canaries == canaries and len(runner.commands) == commands  # no task: no new block requested
    assert rt.spend_confirmed is False and rt.warm_confirmed_at is None and rt.warm_since is None

    # an unconfirmed retry keeps the reason in front of the agent until the user answers
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "idle-released" in out.notice

    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and rt.reaped is None and rt.spend_confirmed is True
    assert (await _run_shell(app, "true")).phase == "complete"


async def test_a_confirmation_sent_before_the_reap_was_known_is_not_taken():
    # the agent pre-emptively passes confirm_spend=True on the call that finds the reap: that call must still stop,
    # so the user is asked AFTER learning the old block is gone; the next confirmed call proceeds
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 60

    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "needs_confirmation" and "idle-released" in res.notice
    assert rt.spend_confirmed is False

    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and rt.spend_confirmed is True and rt.reaped is None


async def test_walltime_is_presumed_even_while_tasks_keep_the_block_busy():
    app, rt, _runner = await _warm_app()
    rt.user_endpoint_config["walltime"] = "00:10:00"
    rt.block_since -= 601  # the block is older than its walltime; the last task was just now
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "walltime (00:10:00)" in out.notice


async def test_a_check_that_finds_a_warm_block_gone_reasks_and_says_a_block_may_be_coming():
    # cancelled or preempted inside the idle window: only the canary can tell, and the canary is itself a task
    # that may already be bringing a new block up — the result must say so rather than "cold"
    app, rt, runner = await _warm_app()
    rt.warm_confirmed_at -= 100  # past the canary TTL, well inside the 600 s idle window
    runner._canary = _TIMEOUT
    commands = len(runner.commands)

    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "provisioning"
    assert "did not answer a check" in out.notice and "stop_endpoint" in out.notice
    assert len(runner.commands) == commands and rt.spend_confirmed is False

    runner._canary = _OK
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and rt.reap_kicked is False


async def test_a_cold_start_timeout_is_not_a_reap():
    # a canary timeout on a block never confirmed warm is the ordinary cold-start wait, not a reap
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "ok", ""), canary_result=_TIMEOUT)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "provisioning"
    res = await _ensure_endpoint_up(app)
    assert res.status == "provisioning" and _shape_runtime(app, "compute").spend_confirmed is True


async def test_an_unknown_facility_idle_window_presumes_nothing():
    # a facility endpoint whose idle window could not be read: no clock-only guess — the canary decides
    f = FakeFacility()
    f.max_idletime_s = None
    app, rt, _runner = await _warm_app(f)
    rt.warm_confirmed_at -= 10_000
    out = await _run_shell(app, "true")
    assert out.phase == "complete" and rt.spend_confirmed is True  # the block answered: nothing was reaped


async def test_a_task_handle_suspends_the_clock_only_guess():
    # a running or finished-but-unpolled task has no known end time, so the idle clock cannot be trusted
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= 10_000
    fut: Future = Future()
    fut.set_result(_Res(0, "", ""))
    app.tasks["t1"] = TaskHandle(future=fut, shape="compute", session_id="default", command="sleep 1",
                                 submitted_at=time.monotonic(), ceiling_s=60.0)
    assert _presumed_reaped(app, "compute", rt) is None
    app.tasks.clear()
    assert _presumed_reaped(app, "compute", rt) is not None


async def test_the_reaped_block_is_billed_to_its_estimated_release_not_to_the_next_call():
    app, rt, _runner = await _warm_app()
    app.charge_factor = 1.0
    now = time.monotonic()
    rt.warm_since = now - 3600          # warm for an hour by the clock
    rt.block_since = now - 3600
    rt.warm_confirmed_at = now - 1800   # last activity 30 min ago; released 600 s after it
    rt.spend_accrued = 0.0
    await _run_shell(app, "true")
    expected = (3600 - 1800 + app.profile.max_idletime_s) / 3600 * _block_nodes(rt, app)
    assert abs(rt.spend_accrued - expected) < 0.01


async def test_stop_clears_a_pending_reap():
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= 10_000
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    from hpc_bridge.warmth import _drop_all_shapes
    _drop_all_shapes(app, bank=True)
    assert _shape_runtime(app, "compute").reaped is None
