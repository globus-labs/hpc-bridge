"""Spend is asked again after the block it covered is gone (0.1.18).

The `confirm_spend` acknowledgement covers ONE block. When that block is gone — idle-released, past its walltime, or
found dead by a check — the next call answers `needs_confirmation` with the reason, even one that carries
`confirm_spend=True`, and the spend clock stops at the latest the block could have lived (an upper bound).
"""
import time
from concurrent.futures import Future

import pytest

from hpc_bridge import scheduler_ops
from hpc_bridge.context import ShapeRuntime, TaskHandle
from hpc_bridge.cost import _block_nodes, _total_session_spend
from hpc_bridge.facility.local import LocalFacility
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.models import ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _reset_session, _run_shell, _shape_runtime, _stop_endpoint
from hpc_bridge.shapes import shape_config
from hpc_bridge.warmth import (
    _IDLE_GRACE_S,
    _RELEASE_BOUND_S,
    _apply_account,
    _apply_partition,
    _drop_compute_shape,
    _idle_window,
    _note_dispatch,
    _presumed_reaped,
    _release_bound,
)
from tests.fakes import FakeFacility
from tests.test_remote_facility import _pbs_profile, _render
from tests.test_remote_facility import _profile as _slurm_profile
from tests.test_server import _FakeRunner, _Res

_OK = CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9")
_TIMEOUT = CanaryResult(ok=False, error="timeout")
# a scaling pass's own work (a squeue/sacct poll, the scancel) delays the next pass; the grace must leave room for it
_PASS_WORK_S = 5.0


def _job(job):
    """A canary answered from scheduler job `job` (None: the worker reported none)."""
    return CanaryResult(ok=True, worker_host="b002", worker_python="3.11.7", worker_dill="0.3.9", worker_job=job)


async def _warm_app(facility=None, job=None):
    """A billed compute shape, spend confirmed, one command run on a warm block (scheduler job `job`, if given)."""
    f = facility or FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "ok", ""), canary_result=_job(job) if job else None)
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


async def test_the_reaped_block_is_billed_to_the_latest_it_could_have_lived_not_to_the_next_call():
    app, rt, _runner = await _warm_app()
    app.charge_factor = 1.0
    now = time.monotonic()
    rt.warm_since = now - 3600          # warm for an hour by the clock
    rt.block_since = now - 3600
    rt.warm_confirmed_at = now - 1800   # last activity 30 min ago; released by 600 s + the slowest release after it
    rt.spend_accrued = 0.0
    await _run_shell(app, "true")
    expected = (3600 - 1800 + app.profile.max_idletime_s + _RELEASE_BOUND_S) / 3600 * _block_nodes(rt, app)
    assert abs(rt.spend_accrued - expected) < 0.001


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
    expected = (600 + app.profile.max_idletime_s + _RELEASE_BOUND_S) / 3600 * _block_nodes(rt, app)
    assert abs(spent - expected) < 0.001


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
    # a 5 s scaling pass has cancelled the block by ~idle+10; a canary sent now would reach the released endpoint and
    # make it submit a NEW billed block before the user is asked — so these calls presume, and send nothing
    app, rt, runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + past_window
    runner._canary = _TIMEOUT  # what a check would find: nothing answers
    canaries, commands = runner.canaries, len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "cold" and "idle-released" in out.notice
    assert runner.canaries == canaries and len(runner.commands) == commands and rt.reap_kicked is False


@pytest.mark.parametrize("profile", [_slurm_profile, _pbs_profile], ids=["slurm", "pbs"])
def test_the_idle_grace_is_two_scaling_passes_of_the_templates_we_write(profile):
    # The grace is exactly two of our templates' scaling passes plus a pass's own work: parsl cancels an idle block
    # within that of the window. A longer period without a longer grace presumes blocks gone while they are still up
    # (an extra spend question); a shorter period, or a longer grace, widens the span in which a call sends a canary to
    # an already-released block (a new billed block before the user is asked). Either way: fail.
    tmpl, defaults = SlurmFacility(profile(), cli=None).config_template(Profile())
    for shape in ("login", "compute"):
        period = _render(tmpl, {**defaults, **shape_config(shape)})["engine"]["job_status_kwargs"]["strategy_period"]
        assert 2 * period + _PASS_WORK_S == _IDLE_GRACE_S


async def test_the_release_bound_counts_an_unpolled_tasks_end_and_bills_up_to_the_slowest_release():
    app, rt, _runner = await _warm_app()
    now = time.monotonic()
    rt.warm_confirmed_at = now - 1300                 # the long task's dispatch
    fut: Future = Future()
    fut.set_result(None)
    app.tasks["t1"] = TaskHandle(future=fut, shape="compute", session_id="default", command="sleep 1200",
                                 submitted_at=now - 1300, ceiling_s=3600.0, done_at=now - 100)
    # the block ran that task until 100 s ago: bill to its end + the idle window + the slowest release (two 30 s passes
    # and their work)...
    assert _release_bound(app, "compute", rt) == now - 100 + app.profile.max_idletime_s + 65.0
    # ...but never past its walltime's end
    rt.user_endpoint_config["walltime"] = "00:30:00"
    rt.block_since = now - 1500
    assert _release_bound(app, "compute", rt) == now - 1500 + 1800


async def test_the_reason_says_or_is_about_to_be_only_while_the_block_may_still_be_up():
    app, rt, _runner = await _warm_app()
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 20     # a slower pass may not have released it yet
    assert "has been idle-released, or is about to be" in _presumed_reaped(app, "compute", rt)[0]
    rt.warm_confirmed_at -= 100                                  # past the latest any of our endpoints releases
    assert _presumed_reaped(app, "compute", rt)[0].startswith("the previous block idle-released (")


@pytest.mark.parametrize(("old", "new"), [("101", "101"), (None, None), (None, "101"), ("101", None)],
                         ids=["same-job", "no-ids", "old-id-unknown", "new-id-unknown"])
async def test_a_block_presumed_early_that_answers_keeps_its_walltime_age(old, new):
    # An endpoint still on the 30 s pass (configured before this change; reuse never rewrites the template) can be
    # presumed idle-released while its block is up. If the re-confirm's canary reaches that block — the same scheduler
    # job, or (ids unknown) an answer before the presumed block's latest end — it keeps the older age, so its real
    # walltime is still presumed without a canary, not found later by a canary that kicks a new block.
    app, rt, runner = await _warm_app(job=old)
    rt.user_endpoint_config["walltime"] = "00:30:00"
    now = time.monotonic()
    rt.block_since = now - 1200                                  # 20 min into its 30 min walltime
    rt.warm_confirmed_at = now - (app.profile.max_idletime_s + 20)
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    runner._canary = _job(new)                                   # the old block answers
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert abs(rt.block_since - (now - 1200)) < 1e-6             # its age is kept
    rt.block_since -= 700                                        # the user kept working past its real walltime...
    rt.warm_confirmed_at = time.monotonic() - 100
    runner._canary = _TIMEOUT                                    # ...and Slurm ended it there
    canaries = runner.canaries
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "reached its walltime" in out.notice
    assert runner.canaries == canaries and rt.reap_kicked is False


async def test_a_new_block_answering_after_a_presumption_is_aged_from_now_by_its_job_id():
    # A 5 s endpoint: the presumed block really was released, and the re-confirm brought up a NEW block (another
    # scheduler job) that answers within seconds. It must not inherit the old block's age: that made its walltime
    # question come early, after which it was dated afresh and its real walltime was missed — a canary then kicked.
    app, rt, runner = await _warm_app(job="101")
    rt.user_endpoint_config["walltime"] = "00:30:00"
    now = time.monotonic()
    rt.block_since = now - 1200                                  # the old block was 20 min old
    rt.warm_confirmed_at = now - (app.profile.max_idletime_s + 20)
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    runner._canary = _job("202")                                 # a new block answers
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.block_since > now - 1 and rt.block_job == "202"    # aged from now

    rt.block_since -= 600                                        # 10 min of work on it: no early walltime question
    rt.warm_confirmed_at = time.monotonic() - 10
    assert (await _run_shell(app, "true")).phase == "complete"

    rt.block_since -= 1260                                       # 21 more minutes: past ITS walltime
    rt.warm_confirmed_at = time.monotonic() - 60
    runner._canary = _TIMEOUT                                    # Slurm ended it there
    canaries = runner.canaries
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and "reached its walltime" in out.notice
    assert runner.canaries == canaries and rt.reap_kicked is False


async def test_the_same_job_keeps_its_age_even_past_the_presumed_blocks_latest_end():
    # a facility MEP's scaling pass can be slower than any we write: the job id, not the bound, says it is that block
    app, rt, runner = await _warm_app(job="101")
    now = time.monotonic()
    rt.block_since = now - 1200
    rt.warm_confirmed_at = now - (app.profile.max_idletime_s + 200)  # past the 65 s bound
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    runner._canary = _job("101")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert abs(rt.block_since - (now - 1200)) < 1e-6


async def test_a_block_answering_after_the_presumed_ones_latest_end_is_dated_from_now():
    app, rt, _runner = await _warm_app()
    now = time.monotonic()
    rt.block_since = now - 1200
    rt.warm_confirmed_at = now - (app.profile.max_idletime_s + 200)  # long past any release: a new block answers
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.block_since > now - 1 and rt.presumed_block_since is None


@pytest.mark.parametrize(("new", "kept"), [("101", True), ("202", False)], ids=["same-job", "new-job"])
async def test_the_spend_clock_of_a_presumed_block_continues_when_it_answers(new, kept):
    # The presumption stops the clock (the block may be gone). If the SAME job then answers, it was billing all along:
    # its clock continues from where it stopped. A different job is a new block, billed from when it answers.
    app, rt, runner = await _warm_app(job="101")
    app.charge_factor = 1.0
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 20
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    assert rt.warm_since is None and rt.presumed_billed_until is not None
    rt.presumed_billed_until -= 300  # the user answered five minutes after the presumption
    stopped_at = rt.presumed_billed_until
    runner._canary = _job(new)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert (rt.warm_since == stopped_at) is kept
    if not kept:
        assert rt.warm_since > stopped_at + 299


@pytest.mark.parametrize("switch", ["partition", "account"])
async def test_a_switch_forgets_the_presumed_block(switch):
    # a different partition or account is a different block: it must not be dated or billed as the presumed one
    app, rt, _runner = await _warm_app(job="101")
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 20
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    assert rt.presumed_block_since is not None and rt.presumed_billed_until is not None
    apply = _apply_partition if switch == "partition" else _apply_account
    assert apply(app, "compute", rt, "other") is None
    assert (rt.presumed_block_since, rt.presumed_block_job, rt.presumed_block_until, rt.presumed_billed_until) == (
        None, None, None, None)


# -- a canary answered by a DIFFERENT scheduler job while the block was believed up ----------------------------------
# With the 5 s pass a block that replaced the confirmed one (after a release, a preemption, an scancel) can answer the
# canary inside CANARY_TIMEOUT_S, so a timeout no longer reveals it. The job id does: that is a reap found by a check.

async def test_a_new_job_answering_a_check_before_the_presumption_is_a_kicked_reap():
    app, rt, runner = await _warm_app(job="101")
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 8   # the 5 s pass released it; the 15 s grace has not presumed
    runner._canary = _job("202")                             # the check made the endpoint start job 202, which answered
    commands = len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "provisioning"
    assert "a check was answered by a new block (job 202)" in out.notice and "stop_endpoint releases it" in out.notice
    assert len(runner.commands) == commands                  # nothing ran on the new block before the re-ask
    assert rt.reap_kicked is True and rt.spend_confirmed is False
    # left exactly like a timeout-found reap: the new block is not adopted until the user confirms it
    assert rt.block_since is None and rt.block_job is None and rt.warm_since is None and rt.warm_confirmed_at is None


async def test_a_cancelled_block_replaced_by_the_check_is_asked_for_before_work_runs_on_it():
    # block_reaped_resume's sequence: the pilot is scancel'd, the session sits past the canary trust window (inside
    # the idle window), then the user asks for more work; the check's canary is answered by the block it started
    app, rt, runner = await _warm_app(job="101")
    app.charge_factor = 1.0
    rt.warm_confirmed_at -= 55
    runner._canary = _job("202")
    commands = len(runner.commands)
    out = await _run_shell(app, "cat marker")
    assert out.phase == "needs_confirmation" and "previous block (job 101) is gone" in out.notice
    assert len(runner.commands) == commands and rt.reap_kicked is True
    res = await _ensure_endpoint_up(app, confirm_spend=True)  # the user confirms; the confirm's canary dates job 202
    assert res.status == "up" and rt.spend_confirmed is True and rt.reaped is None and rt.block_job == "202"
    assert rt.block_since > time.monotonic() - 1 and rt.warm_since is not None
    assert (await _run_shell(app, "cat marker")).phase == "complete"


@pytest.mark.parametrize("answer", ["timeout", "303"])
async def test_a_late_yes_after_a_kicked_reap_is_asked_for_once(answer):
    # The user answers the kicked re-ask later, when the block the check started may have idled out. The yes covers
    # whatever block comes up now: it must be taken (provisioning, or job 303 warm), not reaped and asked again. Nothing
    # of the unconfirmed block is kept to be reaped again — no age, no job, no running clock.
    app, rt, runner = await _warm_app(job="101")
    rt.warm_confirmed_at -= 55
    runner._canary = _job("202")
    assert (await _run_shell(app, "true")).phase == "needs_confirmation" and rt.reap_kicked is True
    assert (rt.block_since, rt.block_job, rt.warm_since, rt.warm_confirmed_at) == (None, None, None, None)
    assert (await _ensure_endpoint_up(app)).status == "needs_confirmation"  # an unconfirmed poll meanwhile
    runner._canary = _TIMEOUT if answer == "timeout" else _job("303")
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == ("provisioning" if answer == "timeout" else "up")
    assert rt.spend_confirmed is True and rt.reaped is None and rt.reap_kicked is False
    if answer == "303":
        assert rt.block_job == "303" and rt.block_since > time.monotonic() - 1


async def test_spend_does_not_run_on_for_a_block_nobody_confirmed():
    # after a kicked reap the new block is not billed while the question is open (it is not ours yet)
    app, rt, runner = await _warm_app(job="101")
    app.charge_factor = 1.0
    rt.warm_confirmed_at -= 55
    runner._canary = _job("202")
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    assert rt.warm_since is None                                  # no clock runs for the unconfirmed block
    banked = _total_session_spend(app)
    assert (await _ensure_endpoint_up(app)).status == "needs_confirmation"
    assert rt.warm_since is None and _total_session_spend(app) == banked  # and an unconfirmed poll starts none


async def test_a_billed_block_is_dated_and_billed_only_once_spend_is_confirmed_for_it(monkeypatch):
    # the general rule behind the kicked reaps: a check that voided the acknowledgement and still reports a worker
    # (a block it started) must not have that block adopted — no age, no spend clock — until the user confirms
    from hpc_bridge import warmth

    app, rt, _runner = await _warm_app(job="101")

    async def unconfirming_check(app_, shape, *, force):
        rt.spend_confirmed = False
        rt.block_since = rt.block_job = rt.warm_since = None
        return "warm"

    monkeypatch.setattr(warmth, "_confirm_worker", unconfirming_check)
    assert await warmth._provision(app, "compute") == "provisioning"
    assert rt.block_since is None and rt.block_job is None and rt.warm_since is None


@pytest.mark.parametrize("max_blocks", [2, "2"])
async def test_with_several_blocks_a_canary_on_a_sibling_is_not_a_reap(max_blocks):
    # with max_blocks > 1 a canary can land on any of the shape's blocks: a different job id proves nothing
    app, rt, runner = await _warm_app(job="101")
    rt.user_endpoint_config["max_blocks"] = max_blocks
    rt.warm_confirmed_at -= 55
    runner._canary = _job("102")
    out = await _run_shell(app, "true")
    assert out.phase == "complete" and rt.reaped is None and rt.spend_confirmed is True


async def test_max_blocks_given_as_the_string_1_is_one_block():
    app, rt, runner = await _warm_app(job="101")
    rt.user_endpoint_config["max_blocks"] = "1"
    rt.warm_confirmed_at -= 55
    runner._canary = _job("202")
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and rt.reap_kicked is True


async def test_a_facility_endpoint_whose_strict_schema_drops_max_blocks_is_one_block():
    # a facility MEP whose published schema has no max_blocks drops the key from the config (its template's own
    # default, 1 on every template we know, applies) — the job-id comparison still runs
    from hpc_bridge.facility.mep import MEPFacility
    from tests.fakes import fake_mep_entry
    from tests.test_mep_server import _Status

    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))
    fac.schema = {"additionalProperties": False, "properties": {"partition": {}, "walltime": {}, "account": {}}}
    app = AppCtx(facility=fac, profile=Profile())
    runner = _FakeRunner(fac.endpoint_id, _Res(0, "ok", ""), canary_result=_job("101"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = _shape_runtime(app, "compute")
    assert "max_blocks" not in rt.user_endpoint_config and rt.block_job == "101"
    rt.warm_confirmed_at -= 55
    runner._canary = _job("202")
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and rt.reap_kicked is True and "nothing can cancel it" in out.notice


@pytest.mark.parametrize(("old", "new"), [(None, "202"), ("101", None)], ids=["old-id-unknown", "new-id-unknown"])
async def test_with_a_job_id_unknown_a_canary_answer_is_taken_as_before(old, new):
    # no identity to compare: only a canary timeout reveals a replaced block, as before
    app, rt, runner = await _warm_app(job=old)
    rt.warm_confirmed_at -= 55
    runner._canary = _job(new)
    out = await _run_shell(app, "true")
    assert out.phase == "complete" and rt.reaped is None and rt.spend_confirmed is True


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
