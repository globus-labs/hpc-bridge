"""Spend is asked again after the block it covered is gone (0.1.18).

The `confirm_spend` acknowledgement covers ONE block. When that block is gone — idle-released, past its walltime, or
found dead by a check — the next call answers `needs_confirmation` with the reason, even one that carries
`confirm_spend=True`, and the spend clock stops at the estimated release time.
"""
import time
from concurrent.futures import Future

import pytest

from hpc_bridge import scheduler_ops
from hpc_bridge.context import ShapeRuntime, TaskHandle
from hpc_bridge.cost import _block_nodes
from hpc_bridge.facility.local import LocalFacility
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.models import ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _reset_session, _run_shell, _shape_runtime, _stop_endpoint
from hpc_bridge.shapes import shape_config
from hpc_bridge.warmth import (
    _IDLE_GRACE_S,
    _apply_partition,
    _drop_compute_shape,
    _idle_window,
    _note_dispatch,
    _presumed_reaped,
)
from tests.fakes import FakeFacility
from tests.test_remote_facility import _pbs_profile, _render
from tests.test_remote_facility import _profile as _slurm_profile
from tests.test_server import _FakeRunner, _Res

_OK = CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9")
_TIMEOUT = CanaryResult(ok=False, error="timeout")
# a scaling pass's own work (a squeue/sacct poll, the scancel) delays the next pass; the grace must leave room for it
_PASS_WORK_S = 5.0


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
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 120
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
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 120

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


async def test_a_running_task_suspends_the_clock_and_a_finished_one_counts_from_its_end():
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= 10_000
    fut: Future = Future()
    handle = TaskHandle(future=fut, shape="compute", session_id="default", command="sleep 1",
                        submitted_at=time.monotonic(), ceiling_s=60.0)
    app.tasks["t1"] = handle
    assert _presumed_reaped(app, "compute", rt) is None           # still running: work is on the block
    handle.done_at = time.monotonic() - 30                         # ended 30 s ago, unpolled
    assert _presumed_reaped(app, "compute", rt) is None           # its end is recent activity
    handle.done_at = time.monotonic() - 10_000                     # ended long ago, unpolled
    assert _presumed_reaped(app, "compute", rt) is not None


async def test_a_late_poll_does_not_pass_for_recent_activity():
    # a long task ends, the block idles out while nobody polls, then the agent polls and runs again at once: the
    # poll must not reset the idle clock to now (and so skip the check via the canary TTL)
    app, rt, runner = await _warm_app()
    runner.pending, runner.timeout = True, 0.01
    out = await _run_shell(app, "sleep 3000")
    assert out.phase == "running"
    runner.futures[-1].finish(_Res(0, "done", ""))
    handle = app.tasks[out.task_id]
    handle.done_at -= 10_000                                       # it finished long ago
    rt.warm_confirmed_at -= 10_000
    from hpc_bridge.server import _poll_task
    polled = await _poll_task(app, out.task_id)
    assert polled.phase == "complete"
    runner.pending = False
    again = await _run_shell(app, "true")
    assert again.phase == "needs_confirmation" and "idle-released" in again.notice


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


# -- the 0.1.18 review of this change: paths that hid the reap, or invented one --------------------------------------

async def test_a_block_that_died_under_a_failed_task_is_still_a_reap():
    # a failed dispatch voids warm_confirmed_at; the block it ran on was still confirmed, so a canary that then
    # times out is the block gone — not a cold start that keeps the old acknowledgement
    app, rt, runner = await _warm_app()
    _note_dispatch(rt, ShellOutcome(phase="failed", block_state="warm", exit_code=None, notice="ManagerLost"))
    runner._canary = _TIMEOUT
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "provisioning" and rt.spend_confirmed is False


async def test_a_runner_rebuild_does_not_hide_the_reap():
    app, rt, runner = await _warm_app()
    rt.warm_confirmed_at -= 100
    rt.runner_stale = True  # e.g. a new Globus login: the rebuild voids warm_confirmed_at before the canary
    runner._canary = _TIMEOUT
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "did not answer a check" in out.notice


async def test_status_reports_the_block_a_reap_check_may_have_requested():
    app, _rt, runner = await _warm_app()
    _rt.warm_confirmed_at -= 100
    runner._canary = _TIMEOUT
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and res.block_state == "provisioning"


async def test_a_canary_queued_behind_synchronous_work_is_not_a_reap():
    # a run_shell inside its sync-wait holds no handle; a concurrent forced canary on a one-worker block queues
    # behind it and times out — the block is busy with our work, not gone
    app, rt, runner = await _warm_app()
    rt.inflight = 1
    rt.warm_confirmed_at -= 10_000
    runner._canary = _TIMEOUT
    assert _presumed_reaped(app, "compute", rt) is None
    res = await _ensure_endpoint_up(app)
    assert res.status == "up" and rt.spend_confirmed is True and rt.reaped is None


async def test_the_in_flight_count_is_handed_back_on_every_path():
    app, rt, runner = await _warm_app()
    assert rt.inflight == 0
    await _reset_session(app)
    assert rt.inflight == 0
    runner.pending, runner.timeout = True, 0.01  # past the sync-wait: a poll handle takes over
    out = await _run_shell(app, "sleep 100")
    assert out.phase == "running" and rt.inflight == 0


async def test_a_partition_switch_does_not_hide_a_reap_or_inherit_the_old_block_age():
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= 10_000
    res = await _ensure_endpoint_up(app, partition="other")
    assert res.status == "needs_confirmation" and "idle-released" in res.notice

    app, rt, _runner = await _warm_app()
    rt.block_since -= 3000
    assert _apply_partition(app, "compute", rt, "other") is None
    assert rt.block_since is None and rt.spend_confirmed is True  # a live block: no reap, and a fresh age


async def test_stop_after_a_reap_check_waits_for_the_pilot_that_check_may_have_requested(monkeypatch):
    app, rt, runner = await _warm_app()
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, runner=runner)
    rt.warm_confirmed_at -= 100
    runner._canary = _TIMEOUT
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["expect_block"] = expect_block
        return True, "released"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    await _stop_endpoint(app)
    assert seen["expect_block"] is True


async def test_on_a_facility_endpoint_the_reap_notice_does_not_promise_a_cancel():
    f = FakeFacility()
    f.supported_shapes = ("compute",)
    app, rt, runner = await _warm_app(f)
    rt.warm_confirmed_at -= 100
    runner._canary = _TIMEOUT
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "nothing can cancel it" in out.notice
    assert "stop_endpoint releases" not in out.notice


async def test_releasing_a_long_idle_block_bills_it_to_its_presumed_release():
    app, rt, _runner = await _warm_app()
    app.charge_factor = 1.0
    now = time.monotonic()
    rt.warm_since = rt.block_since = now - 3600
    rt.warm_confirmed_at = now - 3000
    rt.spend_accrued = 0.0
    spent = await _drop_compute_shape(app)
    assert abs(spent - (600 + app.profile.max_idletime_s) / 3600 * _block_nodes(rt, app)) < 0.01


def test_a_local_block_held_warm_never_idles_out():
    app = AppCtx(facility=LocalFacility(cli=None), profile=Profile(mode="interactive"))
    assert _idle_window(app) is None
    app = AppCtx(facility=LocalFacility(cli=None), profile=Profile(mode="batch"))
    assert _idle_window(app) == app.profile.max_idletime_s


async def test_just_past_the_idle_window_the_block_is_not_yet_presumed_gone():
    # the facility releases on its next scale-in pass after the window, so a call right at the edge still checks
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 5
    assert _presumed_reaped(app, "compute", rt) is None


@pytest.mark.parametrize("past_window", [20, 30, 59])
async def test_once_a_5s_pass_has_released_the_block_the_reap_is_presumed_without_a_canary(past_window):
    # a 5 s scaling pass has cancelled the block by ~idle+10; with the old 60 s grace these calls sent a canary to the
    # released endpoint, which submitted a NEW billed block before the user was asked (review of 49a942a)
    app, rt, runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + past_window
    runner._canary = _TIMEOUT  # what a check would find: nothing answers
    canaries, commands = runner.canaries, len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "cold" and "idle-released" in out.notice
    assert runner.canaries == canaries and len(runner.commands) == commands and rt.reap_kicked is False


@pytest.mark.parametrize("profile", [_slurm_profile, _pbs_profile], ids=["slurm", "pbs"])
def test_the_idle_grace_covers_two_scaling_passes_of_the_templates_we_write(profile):
    # raise strategy_period in facility/remote.py and this fails until _IDLE_GRACE_S follows: a grace shorter than
    # the release lag sends canaries to released blocks (each one a new billed block before the user is asked)
    tmpl, defaults = SlurmFacility(profile(), cli=None).config_template(Profile())
    for shape in ("login", "compute"):
        period = _render(tmpl, {**defaults, **shape_config(shape)})["engine"]["job_status_kwargs"]["strategy_period"]
        assert 2 * period + _PASS_WORK_S <= _IDLE_GRACE_S


async def test_a_switch_is_refused_while_a_synchronous_command_runs():
    # the in-flight count is the OLD block's work: it must not vouch for the new partition's block as warm
    app, rt, _runner = await _warm_app()
    rt.inflight = 1
    assert "still running" in (_apply_partition(app, "compute", rt, "other") or "")
    assert rt.user_endpoint_config.get("partition") != "other"
    res = await _ensure_endpoint_up(app, partition="other")  # reports the OLD, busy block — unchanged
    assert res.partition != "other" and "still running" in res.notice and runner_untouched(rt)


def runner_untouched(rt):
    return rt.runner_stale is False and rt.block_since is not None


def test_a_cancelled_task_is_not_activity():
    from hpc_bridge.warmth import _register_task
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    fut: Future = Future()
    tid = _register_task(app, "compute", "default", "sleep 1", fut, 60.0)
    fut.cancel()
    assert app.tasks[tid].done_at is None
