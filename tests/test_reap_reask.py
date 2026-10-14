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
from tests.test_server import _DoneFuture, _FakeRunner, _Res

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
    # a live block: no reap, and a fresh age — and a different block, so spend is asked again
    assert rt.block_since is None and rt.reaped is None and rt.spend_confirmed is False and rt.switched_terms


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
async def test_a_switch_never_dates_or_bills_a_block_as_the_presumed_one_of_other_terms(switch):
    # a different partition or account is a different block: it must not be dated or billed as the presumed one, even
    # when it reports the same job id (the presumed block is kept to its own terms, for a switch back to them)
    app, rt, _runner = await _warm_app(job="101")
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 20
    rt.block_since -= app.profile.max_idletime_s + 20                  # an old block: its age would show
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    presumed_since, presumed_billed = rt.presumed_block_since, rt.presumed_billed_until
    assert presumed_since is not None and presumed_billed is not None
    apply = _apply_partition if switch == "partition" else _apply_account
    assert apply(app, "compute", rt, "other") is None
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    now = time.monotonic()
    assert res.status == "up" and rt.block_job == "101"
    assert rt.block_since == pytest.approx(now, abs=5) and rt.warm_since == pytest.approx(now, abs=5)


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


# -- a replaced block found after something voided the warm confirmation (reap_replaced_block, 2026-10-13) -----------
# A failed dispatch, a runner rebuild and a check that failed with an error each void warm_confirmed_at (forcing a
# check) without finding the block gone. The block the spend was confirmed for must stay the one the next answer is
# compared against. A check that failed with an error used to erase it, and the block that replaced it was taken under
# the old confirmation.

_TEF = CanaryResult(ok=False, error="TaskExecutionFailed: Task failed due to loss of manager (ManagerLost)")


async def test_reap_replaced_block_a_failed_check_then_a_new_job_answering_is_asked_for():
    # The live journal (fake cluster, Pi operator): warm on job 112; a hook scancels it; `sleep 75` completes on the
    # block the endpoint started for it (113); the next call's check fails with TaskExecutionFailed; the status call
    # after it is answered by job 113. That read "up", and the next command ran on job 113 without asking.
    app, rt, runner = await _warm_app(job="112")                       # 1. warm on job 112, a command ran
    # 2. job 112 is scancel'd: nothing the fake can see
    assert (await _run_shell(app, "sleep 75")).phase == "complete"     # 3. inside the check's TTL: no check
    rt.warm_confirmed_at -= 55                                          # past the TTL, well inside the idle window
    runner._canary = _TEF
    out = await _run_shell(app, "echo JOB=$SLURM_JOB_ID")               # 4. the check fails with an error
    assert out.phase == "cold_start" and "last dispatch failed: TaskExecutionFailed" in out.notice
    assert rt.spend_confirmed is True and rt.block_job == "112" and rt.reaped is None  # not a reap; 112 is still it
    runner._canary = _job("113")
    commands = len(runner.commands)
    res = await _ensure_endpoint_up(app)                                # 5. answered by job 113
    assert res.status == "needs_confirmation" and res.block_state == "provisioning" and rt.reap_kicked is True
    assert "previous block (job 112) is gone" in res.notice and "new block (job 113)" in res.notice
    assert "already running and billing the allocation" in res.notice   # not "a new block bills again": it is
    assert "started it for work sent after the old block went" in res.notice
    assert "Nothing more runs on job 113 until the user confirms" in res.notice
    assert "bills from the moment it starts, confirmed or not" in res.notice and "bills again" not in res.notice
    out = await _run_shell(app, "echo JOB=$SLURM_JOB_ID")               # 6. nothing runs on job 113 before the re-ask
    assert out.phase == "needs_confirmation" and len(runner.commands) == commands
    assert (rt.spend_confirmed, rt.block_job, rt.block_since, rt.warm_since) == (False, None, None, None)
    res = await _ensure_endpoint_up(app, confirm_spend=True)            # the user says yes: job 113 is taken now
    assert res.status == "up" and rt.block_job == "113" and rt.spend_confirmed is True and rt.reaped is None
    assert (await _run_shell(app, "echo JOB=$SLURM_JOB_ID")).phase == "complete"


@pytest.mark.parametrize("voided_by", ["failed-dispatch", "runner-rebuild", "failed-check"])
async def test_a_new_job_answering_after_the_warm_confirmation_was_voided_is_a_kicked_reap(voided_by, monkeypatch):
    # whatever voided the warm confirmation, a different job answering means the confirmed block was replaced
    from globus_compute_sdk.errors import TaskExecutionFailed

    app, rt, runner = await _warm_app(job="112")
    if voided_by == "failed-dispatch":  # the task fails with its worker: _note_dispatch voids the confirmation
        monkeypatch.setattr(runner, "submit", lambda command: _DoneFuture(exc=TaskExecutionFailed("ManagerLost", "0")))
        out = await _run_shell(app, "true")
        assert out.phase == "failed" and rt.warm_confirmed_at is None
        monkeypatch.undo()
    elif voided_by == "runner-rebuild":  # e.g. a new Globus login: the rebuild voids it before the check
        rt.runner_stale = True
    else:
        rt.warm_confirmed_at -= 55
        runner._canary = _TEF
        assert (await _run_shell(app, "true")).phase == "cold_start"
    runner._canary = _job("113")
    commands = len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "provisioning" and rt.reap_kicked is True
    assert "previous block (job 112) is gone" in out.notice and len(runner.commands) == commands


# Every non-timeout check failure takes the same path (TaskExecutionFailed, "Executor is shutdown", a task-side
# TimeoutError, a cancelled canary...): `worker-lost` stands for them all. A transient conflict takes it too, and also
# counts the conflict streak. A no-account verdict is a different path: its own test below.
_CHECK_ERRORS = {
    "worker-lost": _TEF.error,
    "transient-conflict": ("ComputeAPIError[RESOURCE_CONFLICT]: Endpoint fake-eid is already in use: possibly due to "
                           "concurrent requests -- please try again"),
}


@pytest.mark.parametrize("then", ["new-job", "timeout", "same-job"])
@pytest.mark.parametrize("error", list(_CHECK_ERRORS.values()), ids=list(_CHECK_ERRORS))
async def test_a_check_that_fails_with_an_error_keeps_the_confirmed_block_to_compare_against(error, then):
    # The confirmed block keeps its job and age through the failed check: a new job answering is a kicked reap, a
    # timeout is that block gone (not a cold start), and the same job answering is that block, still dated from when it
    # started (so its walltime is still presumed without a check).
    app, rt, runner = await _warm_app(job="112")
    rt.user_endpoint_config["walltime"] = "00:30:00"
    rt.block_since -= 600                                       # 10 minutes into its walltime
    age = rt.block_since
    rt.warm_confirmed_at -= 55
    runner._canary = CanaryResult(ok=False, error=error)
    assert (await _run_shell(app, "true")).phase == "cold_start"
    assert (rt.spend_confirmed, rt.block_job, rt.block_since, rt.reaped) == (True, "112", age, None)
    runner._canary = {"new-job": _job("113"), "timeout": _TIMEOUT, "same-job": _job("112")}[then]
    commands = len(runner.commands)
    out = await _run_shell(app, "true")
    if then == "same-job":
        assert out.phase == "complete" and rt.block_since == age and rt.spend_confirmed is True and rt.reaped is None
    else:
        assert out.phase == "needs_confirmation" and out.block_state == "provisioning" and rt.reap_kicked is True
        assert len(runner.commands) == commands and rt.spend_confirmed is False
        assert ("new block (job 113)" if then == "new-job" else "did not answer a check") in out.notice


@pytest.mark.parametrize("voided_by", ["failed-check", "failed-dispatch", "rebuild-then-failed-check"])
async def test_after_the_confirmation_is_voided_the_idle_presumption_still_counts_from_the_last_proof(voided_by,
                                                                                                     monkeypatch):
    # A failed check, a failed dispatch and a runner rebuild (a new login) each void the warm confirmation; the idle
    # clock must not lose the block's last proof of life with it (_void_warm keeps it as last_proof_at). Past the idle
    # window the next call presumes the block released and asks first, sending nothing: a canary to a released endpoint
    # would have it start a new billed block before the user is asked.
    from globus_compute_sdk.errors import TaskExecutionFailed

    app, rt, runner = await _warm_app(job="112")
    proof = rt.warm_confirmed_at
    if voided_by == "failed-dispatch":
        monkeypatch.setattr(runner, "submit", lambda command: _DoneFuture(exc=TaskExecutionFailed("ManagerLost", "0")))
        assert (await _run_shell(app, "true")).phase == "failed"
        monkeypatch.undo()
    else:
        if voided_by == "rebuild-then-failed-check":
            rt.runner_stale = True                                  # the rebuild voids it before the check fails
        else:
            rt.warm_confirmed_at -= 55
            proof = rt.warm_confirmed_at
        runner._canary = CanaryResult(ok=False, error=_CHECK_ERRORS["transient-conflict"])
        assert (await _run_shell(app, "true")).phase == "cold_start"
    assert rt.warm_confirmed_at is None and rt.last_proof_at == proof
    rt.last_proof_at -= app.profile.max_idletime_s + 300           # the idle window and more, with no call
    runner._canary = _job("113")                                    # what a check would find
    canaries, commands = runner.canaries, len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and out.block_state == "cold" and "idle-released" in out.notice
    assert runner.canaries == canaries and len(runner.commands) == commands and rt.reap_kicked is False
    assert rt.presumed_block_job == "112"                           # the answer after the yes is dated by its job


async def test_a_late_poll_of_an_older_task_does_not_hide_a_newer_proof():
    # After a failed check, polling a task that ended long before it sets that older end as the confirmation: the idle
    # clock must still count from the newer proof (the max of the two), not presume the block idle-released
    from hpc_bridge.server import _poll_task
    from hpc_bridge.warmth import _register_task

    app, rt, runner = await _warm_app(job="112")
    tid = _register_task(app, "compute", "s2", "make", _DoneFuture(_Res(0, "built", "")), 60.0, runtime=rt)
    app.tasks[tid].done_at = time.monotonic() - (app.profile.max_idletime_s + 300)  # it ended long ago
    rt.warm_confirmed_at -= 55
    runner._canary = _TEF
    assert (await _run_shell(app, "true")).phase == "cold_start"    # the failed check: last proof 55 s ago
    assert (await _poll_task(app, tid)).phase == "complete"         # sets the older end as the confirmation
    assert (await _run_shell(app, "true")).phase == "cold_start"    # the next check fails too: its rebuild re-voids
    assert rt.reaped is None and rt.last_proof_at > time.monotonic() - 60  # the newer proof is the one kept
    runner._canary = _job("112")
    out = await _run_shell(app, "true")
    assert out.phase == "complete" and rt.reaped is None


@pytest.mark.parametrize("failed_checks", [1, 2])
@pytest.mark.parametrize("then", ["same-job", "new-job"])
async def test_the_clock_of_the_confirmed_block_runs_on_through_a_failed_check(then, failed_checks, monkeypatch):
    # A failed check is not the block gone: it may well be up and billing. If the same job answers 5 minutes later
    # those 5 minutes are billed; if a new job answers, the old block is billed through them too (its reap banks it to
    # the latest it could have lived: here, now, inside its idle window). Two failed checks in a row (a transient
    # conflict streak): the second call's runner rebuild banks the clock, which must start again at once.
    app, rt, runner = await _warm_app(job="112")
    app.charge_factor = 1.0
    rt.warm_confirmed_at -= 55
    runner._canary = _TEF
    real, skew = time.monotonic, {"s": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: real() + skew["s"])
    for _ in range(failed_checks):
        assert (await _run_shell(app, "true")).phase == "cold_start"
        assert rt.warm_since is not None                            # not stopped at the error
        skew["s"] += 300 / failed_checks
    runner._canary = _job("112" if then == "same-job" else "113")
    res = await _ensure_endpoint_up(app)
    gap = 300 / 3600 * _block_nodes(rt, app)
    if then == "same-job":  # (a runner rebuild banks the interval and restarts the clock: the total is what counts)
        assert res.status == "up" and rt.warm_since is not None and _total_session_spend(app) >= gap
    else:
        assert res.status == "needs_confirmation" and rt.warm_since is None and rt.spend_accrued >= gap


_REJECTED = ("Failed to start block 1: Cannot launch job parsl.slurm.1: Could not read job ID from submit command "
             "standard output; recode=1, stderr='sbatch: error: Batch job submission failed: Invalid account or "
             "account/partition combination specified'")


def _mep_app(job, idle_s=None):
    from hpc_bridge.facility.mep import MEPFacility
    from tests.fakes import fake_mep_entry
    from tests.test_mep_server import _Status

    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))
    fac.max_idletime_s = idle_s  # the facility's own idle window, when its template publishes one
    app = AppCtx(facility=fac, profile=Profile())
    runner = _FakeRunner(fac.endpoint_id, _Res(0, "ok", ""), canary_result=_job(job))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    return app, runner


@pytest.mark.parametrize("facility", ["ssh", "mep"])
@pytest.mark.parametrize("verdict", ["rejected", "conflict-limit", "no-account"])
async def test_a_down_never_leaves_a_clock_running(verdict, facility, monkeypatch):
    # A terminal verdict after a confirmed block: the scheduler rejected the block submission (with one block, the
    # endpoint submits only once the confirmed block is gone), the conflict streak hit its limit, or no account. The
    # status is `down`; its clock must stop there (banked to the latest the block could have lived), not run on. (The
    # MEP here publishes its idle window, so its failed checks are held like the SSH endpoint's.)
    from tests.test_mep_server import _LIVE_409, _LIVE_422

    if facility == "ssh":
        app, rt, runner = await _warm_app(job="112")
    else:
        app, runner = _mep_app("112", idle_s=600)
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
        rt = _shape_runtime(app, "compute")
    app.charge_factor = 1.0
    real, skew = time.monotonic, {"s": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: real() + skew["s"])
    skew["s"] += 60
    runner._canary = CanaryResult(ok=False, error={"rejected": _REJECTED, "conflict-limit": _LIVE_409,
                                                   "no-account": _LIVE_422}[verdict])
    res = await _ensure_endpoint_up(app)
    for _ in range(2 if verdict == "conflict-limit" else 0):       # the first two conflicts are transient
        assert res.status == "provisioning" and rt.warm_since is not None
        res = await _ensure_endpoint_up(app)
    assert res.status == "down" and rt.warm_since is None
    if verdict == "rejected":  # "nothing was billed" was true of the new block only
        assert "The new block was not created" in res.notice and "stays in session_spend" in res.notice
    spent = _total_session_spend(app)
    assert spent >= 60 / 3600 * _block_nodes(rt, app)              # billed up to the verdict, not short of it
    skew["s"] += 300
    res = await _ensure_endpoint_up(app)
    assert res.status == "down" and _total_session_spend(app) == spent and rt.warm_since is None


async def test_with_no_idle_window_known_a_failed_check_stops_the_clock():
    # a facility endpoint whose idle window could not be read: nothing would end a hold, so the clock stops at the
    # failed check, as it did before holds existed (the block keeps its identity: the next answer is still compared)
    from tests.test_mep_server import _LIVE_409

    app, runner = _mep_app("112", idle_s=None)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = _shape_runtime(app, "compute")
    runner._canary = CanaryResult(ok=False, error=_LIVE_409)
    assert (await _ensure_endpoint_up(app)).status == "provisioning"
    assert rt.warm_since is None and rt.block_job == "112"


def _skewed(monkeypatch):
    """time.monotonic, shifted by skew["s"] — to let minutes pass between calls."""
    real, skew = time.monotonic, {"s": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: real() + skew["s"])
    return skew


@pytest.mark.parametrize("gap", [30, 400])
@pytest.mark.parametrize("window", ["unknown", "known"])
async def test_the_same_job_answering_after_its_clock_stopped_bills_the_gap(window, gap, monkeypatch):
    # With no idle window known (the registry's Anvil and Delta MEPs publish none) a failed check is not held: the clock
    # stops there. If the same job answers later it was billing all along, so the clock resumes where it stopped (with a
    # known window the hold already kept it running). Before, the gap billed 0 s.
    from tests.test_mep_server import _LIVE_409

    app, runner = _mep_app("112", idle_s=None if window == "unknown" else 600)
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = _shape_runtime(app, "compute")
    app.charge_factor = 1.0
    skew = _skewed(monkeypatch)
    runner._canary = CanaryResult(ok=False, error=_LIVE_409)
    assert (await _ensure_endpoint_up(app)).status == "provisioning"
    assert (rt.warm_since is None) is (window == "unknown")         # stopped only where nothing would end a hold
    skew["s"] += gap
    runner._canary = _job("112")
    assert (await _ensure_endpoint_up(app)).status == "up"
    assert _total_session_spend(app) >= gap / 3600 * _block_nodes(rt, app) and rt.clock_stopped_at is None


@pytest.mark.parametrize(("verdict", "then"), [("conflict-limit", "answer"), ("conflict-limit", "presumed-then-answer"),
                                               ("no-account-then-login", "answer")])  # (no presumption under no-account)
async def test_a_block_that_answers_after_a_down_is_billed_through_it(verdict, then, monkeypatch):
    # A `down` stops the clock at the verdict, the identity kept. If that job answers later (the conflicts cleared; a
    # new login maps the identity again), the gap is billed from where the clock stopped, also when the idle
    # presumption came in between (it hands the stop point on as presumed_billed_until).
    from hpc_bridge.warmth import _forget_identity_verdicts
    from tests.test_mep_server import _LIVE_409, _LIVE_422

    app, rt, runner = await _warm_app(job="112")
    app.charge_factor = 1.0
    skew = _skewed(monkeypatch)
    skew["s"] += 60
    runner._canary = CanaryResult(ok=False, error=_LIVE_409 if verdict == "conflict-limit" else _LIVE_422)
    res = await _ensure_endpoint_up(app)
    for _ in range(2 if verdict == "conflict-limit" else 0):
        res = await _ensure_endpoint_up(app)
    assert res.status == "down" and rt.warm_since is None and rt.clock_stopped_at is not None
    stopped = rt.clock_stopped_at
    if verdict != "conflict-limit":
        _forget_identity_verdicts(app)                                # a new login
    runner._canary = _job("112")
    if then == "presumed-then-answer":
        skew["s"] += app.profile.max_idletime_s + 300               # past the idle window: presumed released
        assert (await _run_shell(app, "true")).phase == "needs_confirmation"
        assert rt.presumed_billed_until == stopped
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up" and rt.warm_since == stopped
        return
    skew["s"] += 300
    assert (await _ensure_endpoint_up(app)).status == "up"
    assert rt.warm_since == stopped and _total_session_spend(app) >= 360 / 3600 * _block_nodes(rt, app) - 1e-9


@pytest.mark.parametrize("path", ["ensure_endpoint_up", "run_shell"])
async def test_an_error_while_checking_stops_the_clock_and_reports_the_spend(path, monkeypatch):
    # A check that raises (the status API unreachable) reads `down` (or an error from run_shell): the clock stops there,
    # the status carries the session spend, and if the block answers again its clock resumes where it stopped.
    app, rt, _runner = await _warm_app(job="112")
    app.charge_factor = 1.0
    rt.warm_since -= 600

    async def unreachable(endpoint_id):
        raise RuntimeError("status API unreachable")

    monkeypatch.setattr(app.facility, "manager_online", unreachable)
    if path == "ensure_endpoint_up":
        res = await _ensure_endpoint_up(app)
        assert res.status == "down" and res.session_spend == _total_session_spend(app) > 0.1
    else:
        with pytest.raises(RuntimeError):
            await _run_shell(app, "true")
    assert rt.warm_since is None and rt.clock_stopped_at is not None
    stopped = rt.clock_stopped_at
    monkeypatch.undo()
    assert (await _ensure_endpoint_up(app)).status == "up" and rt.warm_since == stopped


async def test_with_several_blocks_a_rejected_submission_does_not_stop_the_confirmed_blocks_clock():
    # with max_blocks > 1 a rejected submission is a NEW block refused; the confirmed one may still be up and billing
    app, rt, runner = await _warm_app(job="112")
    rt.user_endpoint_config["max_blocks"] = 2
    rt.warm_confirmed_at -= 55
    runner._canary = CanaryResult(ok=False, error=_REJECTED)
    res = await _ensure_endpoint_up(app)
    assert res.status == "down" and rt.warm_since is not None and rt.block_job == "112"


async def test_an_orphaned_task_stops_its_blocks_clock():
    # poll_task finds the endpoint gone: the task is ORPHANED (terminal), and its block's clock stops with it
    from hpc_bridge.server import _poll_task

    app, rt, runner = await _warm_app()
    app.charge_factor = 1.0
    runner.pending, runner.timeout = True, 0.01
    out = await _run_shell(app, "sleep 3000")
    assert out.phase == "running" and rt.warm_since is not None
    app.facility.manager_up = False
    polled = await _poll_task(app, out.task_id)
    assert polled.phase == "failed" and "ORPHANED" in polled.notice and rt.warm_since is None


@pytest.mark.parametrize("case", ["first-check-error", "first-check-timeout", "switch-then-timeout"])
async def test_a_check_that_fails_with_no_confirmed_block_starts_no_clock(case):
    # Only a CONFIRMED block whose check failed is billed as still up. A failed first check, a cold start, or the first
    # check after a switch (even with a recently finished task on the shape) bills nothing.
    if case == "switch-then-timeout":
        app, rt, runner = await _warm_app(job="112")
        fut: Future = Future()
        fut.set_result(None)
        app.tasks["t1"] = TaskHandle(future=fut, shape="compute", session_id="s2", command="make",
                                     submitted_at=time.monotonic(), ceiling_s=60.0, done_at=time.monotonic())
        runner._canary = _TIMEOUT
        res = await _ensure_endpoint_up(app, partition="other", confirm_spend=True)  # the user's yes for the new terms
    else:
        f = FakeFacility()
        f.workers = 1
        app = AppCtx(facility=f, profile=Profile())
        runner = _FakeRunner("fake-eid", _Res(0, "ok", ""), canary_result=_TEF if case == "first-check-error" else _TIMEOUT)
        app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
        res = await _ensure_endpoint_up(app, confirm_spend=True)
        rt = _shape_runtime(app, "compute")
    assert res.status == "provisioning" and rt.block_since is None and rt.warm_since is None


async def test_a_manager_reported_offline_stops_the_clock():
    # no check ran (the manager is offline): nothing says the block is up, so its clock stops as before
    app, rt, _runner = await _warm_app(job="112")
    app.facility.manager_up = False
    res = await _ensure_endpoint_up(app)
    assert res.status == "provisioning" and rt.warm_since is None and rt.block_job == "112"


async def test_the_spend_floors_report_the_spend_a_reap_just_banked():
    # needs_confirmation (and needs_account after it) carried no session_spend, so they read 0 right after a reap
    # banked the old block
    app, rt, _runner = await _warm_app(job="112")
    app.charge_factor = 1.0
    rt.warm_since -= 3600
    rt.warm_confirmed_at -= app.profile.max_idletime_s + 120        # idle-released: this call's reap banks the hour
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and res.session_spend == _total_session_spend(app) > 0.5
    app.facility.account_required = True
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "needs_account" and res.session_spend == _total_session_spend(app) > 0.5


async def test_a_no_account_verdict_keeps_the_block_but_asks_for_no_spend():
    # A no-account verdict (after a new Globus login, say) is terminal for this identity: no block can be started, so
    # the walltime presumption must not turn it into a spend question whose yes only meets the same refusal. The clock
    # stops at the verdict. The block's identity is kept, so after a login that clears the verdict the next answer is
    # still compared against it.
    from hpc_bridge.warmth import _forget_identity_verdicts
    from tests.test_mep_server import _LIVE_422

    app, rt, runner = await _warm_app(job="112")
    rt.user_endpoint_config["walltime"] = "00:30:00"
    rt.warm_confirmed_at -= 55
    runner._canary = CanaryResult(ok=False, error=_LIVE_422)
    out = await _run_shell(app, "true")
    assert out.phase == "failed" and rt.no_account and rt.block_job == "112" and rt.warm_since is None
    rt.block_since -= 1900                                          # past its walltime
    out = await _run_shell(app, "true")
    assert out.phase == "failed" and rt.reaped is None and "Confirm spend again" not in (out.notice or "")
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "down" and rt.reaped is None
    rt.block_since += 1900                                          # (inside its walltime again)
    _forget_identity_verdicts(app)                                  # a new login
    runner._canary = _job("113")
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and rt.reap_kicked is True and "previous block (job 112)" in out.notice


async def test_on_a_facility_endpoint_a_block_replaced_after_a_transient_conflict_is_asked_for():
    # a facility MEP's canaries carry the job too; RESOURCE_CONFLICT is common there, and nothing can cancel the block
    from tests.test_mep_server import _LIVE_409

    app, runner = _mep_app("101")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = _shape_runtime(app, "compute")
    runner._canary = CanaryResult(ok=False, error=_LIVE_409)
    res = await _ensure_endpoint_up(app)
    assert res.status == "provisioning" and "TRANSIENT" in res.notice and rt.block_job == "101"
    runner._canary = _job("202")
    commands = len(runner.commands)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and rt.reap_kicked is True and "nothing can cancel it" in out.notice
    assert "previous block (job 101) is gone" in out.notice and len(runner.commands) == commands


# The comparison needs a block this spend was confirmed for. Where there is none, the first block to answer is taken.

@pytest.mark.parametrize("switch", ["partition", "account"])
async def test_after_a_switch_the_first_block_to_answer_is_taken_not_compared(switch):
    # a switch is a different block, confirmed with the switch itself: no job to compare it against
    app, rt, runner = await _warm_app(job="112")
    runner._canary = _job("113")
    res = await _ensure_endpoint_up(app, **{switch: "other"}, confirm_spend=True)
    assert res.status == "up" and rt.block_job == "113" and rt.spend_confirmed is True and rt.reaped is None


async def test_a_first_confirmation_has_no_block_to_compare_against():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "ok", ""), canary_result=_TEF)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    runner._canary = _job("113")
    res = await _ensure_endpoint_up(app)
    assert res.status == "up" and _shape_runtime(app, "compute").block_job == "113"


@pytest.mark.parametrize(("new", "kept"), [("112", True), ("113", False)], ids=["same-job", "new-job"])
async def test_after_an_idle_presumption_a_failed_check_still_dates_the_answer_by_its_job(new, kept):
    # the user's yes after a presumption covers the block that answers next: it is dated by its job against the
    # presumed one (presumed_block_job), never compared as a replacement, even after a check that failed with an error
    app, rt, runner = await _warm_app(job="112")
    now = time.monotonic()
    rt.block_since = now - 1200
    rt.warm_confirmed_at = now - (app.profile.max_idletime_s + 20)
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    runner._canary = _TEF
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    assert rt.block_job is None and rt.presumed_block_job == "112"
    runner._canary = _job(new)
    assert (await _ensure_endpoint_up(app)).status == "up" and rt.reaped is None and rt.spend_confirmed is True
    assert (abs(rt.block_since - (now - 1200)) < 1e-6) is kept and rt.block_job == new


# -- the HPC_BRIDGE_CANARY_TIMEOUT_S test seam (config.canary_timeout_s) -----------------------------------------------
# The agentic scenario `reap_replaced_block` lengthens the check so the block that replaced a cancelled one answers
# INSIDE it — the job-id reap above, which on the fake cluster's 8 s default only a timeout ever found live.

@pytest.mark.parametrize(("raw", "want", "said"), [
    (None, 8.0, False), ("", 8.0, False), ("  ", 8.0, False), ("30", 30.0, False), (" 12.5 ", 12.5, False),
    ("8", 8.0, False), ("120", 120.0, False),
    ("abc", 8.0, False),                          # invalid: _env_float says so itself
    ("0", 8.0, True), ("-5", 8.0, True), ("5", 8.0, True),    # shorter would read a live block as reaped
    ("500", 120.0, True),                         # capped: the check holds app.lock
    ("inf", 8.0, True), ("nan", 8.0, True)])
def test_the_canary_timeout_seam_only_lengthens_the_wait_within_bounds(monkeypatch, capsys, raw, want, said):
    from hpc_bridge import config
    from hpc_bridge.config import CANARY_TIMEOUT_MAX_S, CANARY_TIMEOUT_S, canary_timeout_s
    monkeypatch.setattr(config, "_CANARY_SEAM_NOTED", set())
    if raw is None:
        monkeypatch.delenv("HPC_BRIDGE_CANARY_TIMEOUT_S", raising=False)
    else:
        monkeypatch.setenv("HPC_BRIDGE_CANARY_TIMEOUT_S", raw)
    assert (CANARY_TIMEOUT_S, CANARY_TIMEOUT_MAX_S) == (8.0, 120.0) and canary_timeout_s() == want
    assert ("outside [8, 120] s" in capsys.readouterr().err) is said
    canary_timeout_s()
    assert "outside" not in capsys.readouterr().err   # said once, not at every check


def _timed(runner):
    """Record the timeout each canary was given."""
    seen = []
    inner = runner.canary

    async def canary(timeout=8.0):
        seen.append(timeout)
        return await inner(timeout=timeout)

    runner.canary = canary
    return seen


@pytest.mark.parametrize(("raw", "want"), [(None, 8.0), ("30", 30.0)])
async def test_the_check_waits_as_long_as_the_seam_says(monkeypatch, raw, want):
    app, rt, runner = await _warm_app(job="101")
    seen = _timed(runner)
    if raw is None:
        monkeypatch.delenv("HPC_BRIDGE_CANARY_TIMEOUT_S", raising=False)
    else:
        monkeypatch.setenv("HPC_BRIDGE_CANARY_TIMEOUT_S", raw)
    rt.warm_confirmed_at -= 55          # past the canary's trust window: the next call checks
    runner._canary = _job("202")        # the block that replaced job 101 answers inside the (longer) check
    out = await _run_shell(app, "cat marker")
    assert seen == [want]
    assert out.phase == "needs_confirmation" and "previous block (job 101) is gone" in out.notice and "job 202" in out.notice


async def test_a_timeout_reap_names_the_wait_the_check_actually_had(monkeypatch):
    app, rt, runner = await _warm_app(job="101")
    seen = _timed(runner)
    monkeypatch.setenv("HPC_BRIDGE_CANARY_TIMEOUT_S", "12.5")
    rt.warm_confirmed_at -= 55
    runner._canary = _TIMEOUT
    out = await _run_shell(app, "true")
    assert seen == [12.5] and out.phase == "needs_confirmation"
    assert "did not answer a check within 12.5 s" in out.notice
