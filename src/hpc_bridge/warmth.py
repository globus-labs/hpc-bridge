"""The warmth state machine and task-handle bookkeeping (split steps 7–8, 2026-09-03).

Per resource shape: build/reuse the Executor (`_runner_for`), prove a worker is live with the canary
(`_confirm_worker`), gate a billed block behind the spend floor (`_provision`, `_apply_partition`,
`_apply_account`), record the sticky no-account verdict and the transient-conflict count, and drop
shapes (`_drop_compute_shape`, `_drop_all_shapes`, `_forget_identity_verdicts`). Task handles — a command
still running past the sync-wait — are registered, resolved and drained here too, because the runner
rebuild and the canary consult them (a live task IS warmth), which is why steps 7 and 8 ship together.

Every function here runs under `app.lock` held by the caller in `server` — except `_drop_compute_shape`, which
takes the lock itself, `_endpoint_gone`, a web call deliberately made OFF the lock (don't add one inside
either), and `_register_task` when a cancelled call registers its still-running command in one synchronous step
without the lock (nothing can interleave without an await). Tests patch
`warmth._provision` / `warmth._drop_compute_shape`; `server` calls those two through the module and
re-exports every name for imports.
"""
from __future__ import annotations

import dataclasses
import inspect
import re
import time
from dataclasses import replace

from . import dispatch, session_shell
from .config import (
    CANARY_TTL_S,
    SYNC_WAIT_S,
    TASK_CEILING_MARGIN_S,
    TRANSIENT_CONFLICT_LIMIT,
    _parse_hhmmss,
    _task_ceiling_s,
    canary_timeout_s,
)
from .context import (
    DEFAULT_SHAPE,
    AppCtx,
    LeftBlock,
    ShapeRuntime,
    TaskHandle,
    _has_login_shape,
    _idle_release_s,
    _supported_shapes,
)
from .cost import (
    _bank_warm_interval,
    _billable,
    _block_nodes,
    _settle_billing,
    _total_session_spend,
    _with_spend,
    estimate_spend,
)
from .lifecycle import BlockState, EndpointState, ProvisionResult, ensure_warm
from .models import ShellOutcome, Submission
from .notices import (
    _no_account_failure,
    _spend_floor_guidance,
    _submit_rejected,
    _transient_dispatch_failure,
    _with_submission_notice,
)
from .runner import CanaryResult, GlobusRunner
from .shapes import SHAPES, shape_config


def _shape_reject(app: AppCtx, shape: str) -> str | None:
    """A notice if `shape` isn't served by the bound facility, else None. Checked BEFORE any
    _shape_runtime(app, shape) so an unsupported shape never gets a ShapeRuntime/runner (a submit
    with it would be refused server-side and would shut the Executor down — see _confirm_worker)."""
    if shape in SHAPES and shape not in _supported_shapes(app):
        return (
            f"shape {shape!r} isn't available on this facility (a compute-only multi-user endpoint: "
            "no free login node; its schema refuses a LocalProvider block). Use shape='compute' — a "
            "block stays warm between calls (init_blocks=1 + the facility's idle-release), so cheap "
            "follow-up commands don't re-queue."
        )
    return None

def _shape_runtime(app: AppCtx, shape: str) -> ShapeRuntime:
    """Resolve (and lazily build) the per-shape runtime, seeding its user_endpoint_config
    from facility defaults (SlurmFacility) merged with the shape's template vars."""
    if shape not in SHAPES:
        raise ValueError(f"unknown shape {shape!r}")
    rt = app.shapes.get(shape)
    if rt is None:
        defaults: dict = {}
        ct = getattr(app.facility, "config_template", None)
        if ct is not None:
            result = ct(app.profile)
            if isinstance(result, tuple):  # SlurmFacility -> (template_str, defaults)
                defaults = result[1]
            # LocalFacility/FakeFacility return a plain dict (rendered engine) -> no UEP defaults
        if not isinstance(defaults, dict):
            defaults = {}
        uec = {**defaults, **shape_config(shape)}
        # a facility MEP fits the config to ITS published schema (drops keys it rejects, pins worker versions)
        uec = getattr(app.facility, "sanitize_uec", lambda d: d)(uec)
        if uec.get("compute"):  # the session's partition/account outlive a stop, written as _repoint writes them
            uec.update(app.compute_choice)
        rt = ShapeRuntime(user_endpoint_config=uec)
        app.shapes[shape] = rt
    return rt

# A task's ceiling runs from when it STARTS on a worker; its handle's clock runs from the submit. So a pending task past
# its ceiling (measured from the submit) may be lost — or only queued (behind a block relaunched at walltime, or behind
# another task), and a relaunching block still reads `running` (Parsl relaunches it). Every guard keeps treating it as
# live, with ONE exception: on a facility multi-user endpoint nothing can cancel a task and its manager stays online
# (it is never ORPHANED), so stop and the detach would wait on a lost one forever. There, a task pending past its
# ceiling plus this grace is presumed lost and no longer holds them (server._block_work); everything else still waits.
_LOST_TASK_GRACE_S = 600.0

def _past_ceiling(h: TaskHandle, now: float | None = None) -> bool:
    """A task still pending past its ceiling, counted from its submit: queued (behind another task or a relaunching
    block), or lost."""
    now = time.monotonic() if now is None else now
    return not h.future.done() and now - h.submitted_at > h.ceiling_s

def _overdue(h: TaskHandle, now: float | None = None) -> bool:
    """Pending past its ceiling plus _LOST_TASK_GRACE_S: presumed lost — for a facility endpoint's stop/detach only."""
    now = time.monotonic() if now is None else now
    return not h.future.done() and now - h.submitted_at > h.ceiling_s + _LOST_TASK_GRACE_S

def _live_task_handles(app: AppCtx, shape: str) -> list[tuple[str, TaskHandle]]:
    """(task_id, handle) for this shape whose task is still RUNNING (future not yet done) — i.e. still
    holding the block. The warmth signal and the stop/swap/session-busy guards all key off this."""
    return [(tid, h) for tid, h in app.tasks.items() if h.shape == shape and not h.future.done()]

def _drain_shape_tasks(app: AppCtx, shape: str) -> None:
    """Drop a shape's still-RUNNING task handles — its block is going away (endpoint swap/stop/
    connect/teardown), so those futures are moot; poll_task on a drained id reports it ended rather
    than polling a dead future. A FINISHED task's handle is kept: its result is already delivered and
    stays retrievable via poll_task whatever happens to the Executor — dropping it would lose a
    completed result the agent simply hadn't polled yet."""
    for tid in [tid for tid, h in app.tasks.items() if h.shape == shape and not h.future.done()]:
        app.tasks.pop(tid, None)

def _runner_for(app: AppCtx, shape: str) -> GlobusRunner:
    """Reuse the shape's runner if it's bound to the current endpoint, else (re)create it. A
    new endpoint voids the prior worker confirmation and banks the old endpoint's spend."""
    rt = _shape_runtime(app, shape)
    eid = app.state.endpoint_id
    if rt.runner is None or rt.runner.endpoint_id != eid or rt.runner_stale:
        if rt.runner is not None and rt.runner.endpoint_id == eid and (_live_task_handles(app, shape) or rt.inflight):
            # A credential/config-only swap (runner_stale, e.g. a new Globus login) must WAIT: closing
            # this Executor would drop the live task's future and poll_task would report "no task"
            # (found in review). The rebuild happens once the task has been polled — or, for a call still
            # inside its sync-wait, once it returns (its command must stay trackable on this runner).
            return rt.runner
        if rt.runner is not None:
            # A config-only swap (runner_stale) is barred while a task runs (see _apply_partition/
            # _apply_account), so reaching here with a live task means the ENDPOINT changed — that
            # block (and its tasks) is gone; drop the handles before closing so no dead future is polled.
            _drain_shape_tasks(app, shape)
            rt.runner.close()
            _bank_warm_interval(rt, app)
        ceiling_s = _task_ceiling_s(rt.user_endpoint_config)
        sync_wait_s = max(min(SYNC_WAIT_S, ceiling_s - TASK_CEILING_MARGIN_S), 5.0)
        rt.runner = app.runner_factory(
            eid, user_endpoint_config=getattr(app.facility, "dispatch_uec", lambda d: d)(rt.user_endpoint_config),
            walltime=ceiling_s, timeout=sync_wait_s
        )
        rt.runner_stale = False
        _void_warm(rt)
    return rt.runner

def _void_warm(rt: ShapeRuntime) -> None:
    """Force a check on the next call without saying the block is gone: void the warm confirmation, and keep it as the
    block's last proof of life (`last_proof_at`), which the idle presumption and the billing bound still count from."""
    if rt.warm_confirmed_at is not None:  # the latest wins: a late poll can set an older confirmation over a newer one
        rt.last_proof_at = max(rt.warm_confirmed_at, rt.last_proof_at or rt.warm_confirmed_at)
    rt.warm_confirmed_at = None

def _single_block(rt: ShapeRuntime) -> bool:
    """One block at most (max_blocks 1: every template and registry entry we know, and a facility template's own
    default when its schema drops the key): a different scheduler job answering, or a new block being submitted at all,
    means the confirmed block is gone. With more, it may be a sibling."""
    return rt.user_endpoint_config.get("max_blocks") in (None, 1, "1")

def _terminal_failure(rt: ShapeRuntime) -> bool:
    """The last failed check is a terminal verdict, which the tools report as `down` (or a failed run_shell): the
    scheduler rejected a block submission — which, with one block, the endpoint makes only once the confirmed block is
    gone (with more, another block may still be up) — the identity has no account, or the conflict streak hit its
    limit."""
    error = rt.last_canary.error if rt.last_canary is not None and not rt.last_canary.ok else None
    return (bool(rt.no_account) or (_submit_rejected(error) and _single_block(rt))
            or rt.transient_conflicts >= TRANSIENT_CONFLICT_LIMIT)

def _kicked_release_note(app: AppCtx) -> str:
    """What happens to a block a check started if the user declines it."""
    return ("stop_endpoint releases it if the user declines" if _has_login_shape(app) else
            "nothing can cancel it on this facility endpoint, so it idles out after the facility's idle window if no "
            "work is sent")

async def _confirm_worker(app: AppCtx, shape: str, *, force: bool) -> BlockState:
    """Upgrade a manager-online endpoint to truly 'warm' by confirming a worker answers a
    canary. Returns 'warm' if a worker is live, else 'provisioning' — the manager is up but the
    compute block is still cold-starting (the gap manager_online cannot see; the canary submit
    also kicks that block). Within CANARY_TTL_S of the last success we trust warmth and skip
    the round-trip so an interactive burst doesn't pay it on every call."""
    rt = _shape_runtime(app, shape)
    if rt.no_account:  # terminal for this identity: no canary, no runner rebuild — keep last_canary as the evidence
        return "provisioning"
    # A block confirmed warm and not since found gone. Its identity (block_since, block_job) survives a failed dispatch,
    # a runner rebuild and a check that failed with an error, which void warm_confirmed_at, so the next answer is
    # compared against it. Only a reap, or a partition or account switch (a different block), clears it.
    was_warm = rt.block_since is not None
    runner = _runner_for(app, shape)
    now = time.monotonic()
    # A task still running on this shape IS liveness — the worker is demonstrably executing our work.
    # Trust it and skip the canary, which would otherwise queue behind the sole worker and (on timeout)
    # flip us to 'not warm', banking the spend clock while the block is still burning (#21). A synchronous
    # dispatch still inside its sync-wait holds no handle but counts the same (rt.inflight).
    if _live_task_handles(app, shape) or rt.inflight:
        rt.warm_confirmed_at = now
        rt.provisioning_since = None
        return "warm"
    if not force and rt.warm_confirmed_at is not None and now - rt.warm_confirmed_at < CANARY_TTL_S:
        rt.provisioning_since = None
        return "warm"
    timeout = canary_timeout_s()
    result = await runner.canary(timeout=timeout)
    rt.last_canary = result  # keep failures too: the error text is the diagnosis the caller needs
    if result.ok and result.worker_user:  # the account the work runs as (a facility's identity mapping): for squeue -u
        app.worker_user = result.worker_user
    if result.ok:  # its job is up: the first proof dates it (for its walltime), on whatever terms it is met again
        _saw_job(app, result.worker_job, min(now, result.answered_at) if result.answered_at else now)
    # A canary can land on any of the shape's blocks; only with one block at most (max_blocks 1: every template and
    # registry entry we know, and a facility template's own default when its schema drops the key) does a different
    # scheduler job mean the confirmed block was replaced.
    single_block = _single_block(rt)
    # The block this spend was confirmed for, by its job. (A set block_job implies a set block_since, so this asks no
    # less than was_warm did; what changed is that a failed check no longer clears the job.)
    if (result.ok and single_block and _billable(rt) and rt.spend_confirmed and rt.block_job is not None
            and result.worker_job is not None and result.worker_job != rt.block_job):
        # A DIFFERENT scheduler job answered while this block was believed up: the block is gone (idle-released,
        # cancelled, failed, or preempted — though a preempted job REQUEUED keeps its id, which only a timeout can
        # then reveal) and a task — this check, or work sent after the block went — had the endpoint start a new one
        # without the user's confirmation. With the 5 s scaling pass that block can answer inside the canary timeout,
        # so a timeout alone no longer catches it. A reap found by a check, left exactly like the timeout's below:
        # the answering block is not adopted (no age, no job, no spend clock) until the user confirms; the confirm's
        # canary then dates it.
        _mark_reaped(app, rt, f"the previous block (job {rt.block_job}) is gone — idle-released, cancelled, "
                     f"preempted or failed — and a check was answered by a new block (job {result.worker_job}), which "
                     "is already running and billing the allocation. The facility started it for work sent after the "
                     "old block went (a task left waiting for a block) or for the check itself. Nothing more runs on "
                     f"job {result.worker_job} until the user confirms; {_kicked_release_note(app)}",
                     _release_bound(app, shape, rt), kicked=True)
        rt.transient_conflicts = 0
        return "provisioning"
    if result.ok:
        # dated from when the worker actually answered (it can predate this call by up to the runner's freshness
        # window), so the TTL, the idle clock and the reap estimate all measure from real proof
        rt.warm_confirmed_at = min(now, result.answered_at) if result.answered_at else now
        rt.transient_conflicts = 0
        rt.provisioning_since = None  # warm by any route: a later cold start must not inherit a stale clock
        return "warm"
    if was_warm and result.error == "timeout" and _billable(rt) and rt.spend_confirmed:
        # A block we had confirmed warm, with no task of ours on it, no longer answers: cancelled, preempted or
        # failed before its idle window or walltime ran out. The check itself was a task, so the endpoint may
        # already be asking for a new block — say so, and ask before any work runs on it.
        _mark_reaped(app, rt, f"the previous block did not answer a check within {timeout:g} s — most "
                     "likely cancelled, preempted or failed. That check may already have asked the facility for a "
                     f"new block: {_kicked_release_note(app)}", _release_bound(app, shape, rt), kicked=True)
    # A check that failed with an ERROR (a task that failed with its worker, a refused or broken submit, a transient
    # conflict) says nothing about which block is up, so the confirmed block keeps its identity (only the reap above
    # clears it): the next answer is compared against its job (a different one is a kicked reap; the same one keeps its
    # age), a timeout is that block gone, not a cold start, and its idle clock and bill keep counting from its last
    # proof of life. Clearing it here took the replacement under the old confirmation (live 2026-10-13: job 112
    # scancel'd, a check failed with TaskExecutionFailed, job 113 then used without asking).
    _void_warm(rt)
    if result.error == "timeout":
        rt.transient_conflicts = 0  # the submit was ACCEPTED (a normal cold-start wait) — not a conflict streak
    if result.error and result.error != "timeout":
        # A NON-timeout failure means the dispatch path itself broke — e.g. the web service rejected
        # the submit (a user_endpoint_config the endpoint's schema refuses, a bad partition), after
        # which the SDK Executor shuts ITSELF down and every later submit raises `Executor is
        # shutdown`. Left alone, the runner is bricked while the caller sees "allocating nodes…"
        # forever (the #37 dead-end in a new guise). Rebuild it on the next call; the failure text
        # rides `last_canary` into the provisioning notice so the cause is visible, not buried.
        rt.runner_stale = True
        if _no_account_failure(result.error):
            rt.no_account = result.error
        rt.transient_conflicts = rt.transient_conflicts + 1 if _transient_dispatch_failure(result.error) else 0
    return "provisioning"

def _retain_inflight(app: AppCtx, rt: ShapeRuntime | None) -> None:
    """A compute runtime dropped while a dispatch is still inside its sync-wait on it: that command is on the endpoint,
    so keep the runtime where a stop looks (`AppCtx.released_inflight`; server._block_work counts it while the endpoint
    it was sent to is the bound one — after a teardown or a re-bind elsewhere it is abandoned, as its tasks are).
    Runtimes whose calls have all returned are pruned here."""
    app.released_inflight = [r for r in app.released_inflight if r.inflight] + (
        [rt] if rt is not None and rt.inflight else [])

def _drop_all_shapes(app: AppCtx, *, bank: bool) -> float:
    """Forget every task handle, close every shape's runner, and unbind the endpoint. With `bank`, fold
    each shape's running warm interval into the spend FIRST and return the session total as it stood —
    the four inline copies of this block disagreed, and the connect re-bind's copy silently dropped a
    warm block's interval (found in review). A block the clock presumes released is banked to that time, not to
    now. Callers hold app.lock."""
    untils = {name: _presumed_release_at(app, name, rt) for name, rt in app.shapes.items()} if bank else {}
    app.tasks.clear()
    _retain_inflight(app, app.shapes.get(DEFAULT_SHAPE))  # counted only if the same endpoint is bound again
    for name, rt in app.shapes.items():
        if bank:
            _bank_warm_interval(rt, app, until=untils.get(name))
        if rt.runner is not None:
            rt.runner.close()
    spent = _total_session_spend(app) if bank else 0.0
    app.shapes.clear()
    app.compute_choice.clear()  # chosen for this binding: a stop keeps them, a new binding or a teardown does not
    app.job_seen_since.clear()
    app.released_spend = 0.0  # the binding's released blocks are in `spent`; a new binding starts from zero
    app.state = EndpointState()
    return spent

def _note_dispatch(rt: ShapeRuntime, out: ShellOutcome, *, at: float | None = None) -> None:
    """A real result — or a task still running — is the strongest liveness proof, so refresh the canary
    TTL. A dispatch FAILURE (transport timeout/error) means the worker may be gone, so void the
    confirmation to force a re-canary. A completed exit-124 is the worker ENFORCING the task ceiling
    (it answered — it's alive), so it no longer voids (the old timeout==124 heuristic is obsolete now
    that a slow task returns a poll handle, not a 124 failure). A result too big to come back is the worker
    answering too (it ran the command), so it refreshes rather than voids."""
    if out.phase in ("complete", "running") or out._worker_answered:
        # `at`: when a polled task actually finished — the block's last proof of life, not the poll's time
        seen = time.monotonic() if at is None else at
        rt.warm_confirmed_at = seen if rt.warm_confirmed_at is None else max(rt.warm_confirmed_at, seen)
        rt.transient_conflicts = 0
    elif out.phase == "failed":
        _void_warm(rt)

# How long past the idle window the clock waits before presuming the block idle-released, without looking (a canary to a
# released block makes the endpoint submit a new billed one before the user is asked). parsl cancels an idle block
# within two scaling passes of the window, plus each pass's own work (a squeue/sacct poll, the scancel): 2 × 5 s + 5 s
# on endpoints configured with the 5 s period (facility/remote.py). This NARROWS the span in which a call can still
# send a canary to an already-released block, to about 0–15 s past the window, where the kicked notice says what
# happened; it does not close it. Endpoints still on 30 s (configured before this change — reuse never rewrites the
# template) and facility MEPs with a published idle window can be presumed while the block is still up: that costs an
# extra spend question, and the block that answers next is dated by its scheduler job id (_answering_block_since).
_IDLE_GRACE_S = 15.0
# The latest an idle block outlives its window on any endpoint hpc-bridge has configured (two 30 s passes + their work);
# approximate on a facility MEP, whose period is unknown. A block presumed or found gone is billed up to here (capped at
# its walltime's end): an upper bound, the honest side of a spend estimate.
_RELEASE_BOUND_S = 65.0

def _idle_window(app: AppCtx) -> int | None:
    """The idle-release window the clock may presume from: None when it is unknown, or when the facility holds its
    block warm (local dev's min_blocks=1) and nothing idle-releases."""
    keeps = getattr(app.facility, "keeps_block_warm", None)
    if keeps is not None and keeps(app.profile):
        return None
    return _idle_release_s(app)

def _last_activity(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """The block's last proof of work: its last confirmation (warm_confirmed_at, and `last_proof_at`, the one a failed
    dispatch or check or a rebuild voided — a late poll of an older task can set an older confirmation after it) or the
    end of a task nobody has polled yet (`done_at`), whichever is latest. One source for the presumption and the billing
    bound — the bound used to read the confirmation alone, and billed a block short after a long task; and a voided
    confirmation used to stop the idle presumption for good."""
    ends = [h.done_at for h in app.tasks.values() if h.shape == shape]
    stamps = [t for t in (rt.warm_confirmed_at, rt.last_proof_at, *ends) if t is not None]
    return max(stamps) if stamps else None

def _no_idle_block(app: AppCtx, shape: str, rt: ShapeRuntime) -> bool:
    """No confirmed block to presume about, or work may be on it: a task still running (or its end not yet stamped),
    or a synchronous dispatch in flight."""
    return (rt.block_since is None or bool(rt.inflight)
            or any(h.done_at is None for h in app.tasks.values() if h.shape == shape))

def _presumed_idle_released(app: AppCtx, shape: str, rt: ShapeRuntime) -> tuple[str, float] | None:
    """(why, billed_until) when the block has had no task for its idle window plus _IDLE_GRACE_S. Until the latest it
    could have lived the reason says "or is about to be": on an endpoint with a slower scaling pass it may still be
    up."""
    if _no_idle_block(app, shape, rt):
        return None
    last, idle, bound = _last_activity(app, shape, rt), _idle_window(app), _release_bound(app, shape, rt)
    now = time.monotonic()
    if not idle or last is None or bound is None or now - last < idle + _IDLE_GRACE_S:
        return None
    how = "has been idle-released, or is about to be" if now < last + idle + _RELEASE_BOUND_S else "idle-released"
    return f"the previous block {how} (no task for ~{int(now - last)} s, past the {idle} s idle window)", bound

def _presumed_past_walltime(app: AppCtx, shape: str, rt: ShapeRuntime) -> tuple[str, float] | None:
    """(why, its walltime's end) when the block is older than its walltime."""
    if _no_idle_block(app, shape, rt):
        return None
    wall = _parse_hhmmss(rt.user_endpoint_config.get("walltime"))
    if rt.block_since is not None and wall and time.monotonic() - rt.block_since >= wall:
        return (f"the previous block reached its walltime ({rt.user_endpoint_config.get('walltime')})",
                rt.block_since + wall)
    return None

def _presumed_reaped(app: AppCtx, shape: str, rt: ShapeRuntime) -> tuple[str, float] | None:
    """(why, billed_until) when the shape's last confirmed block has, by the clock alone, been released by the
    facility: idle-released (_presumed_idle_released) or past its walltime. Decided WITHOUT submitting anything — the
    only way to look (a canary) is itself a task, and a task on a released block starts a new billed one. None when
    there is no such block, or while work may be on it."""
    return _presumed_idle_released(app, shape, rt) or _presumed_past_walltime(app, shape, rt)

def _release_bound(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """The latest a block with no task of ours could have lived: its last activity, plus its idle window, plus
    _RELEASE_BOUND_S — and never past its walltime's end. An upper bound, the honest side of a spend estimate. None
    when either is unknown (bill to now)."""
    idle, last = _idle_window(app), _last_activity(app, shape, rt)
    if not idle or last is None:
        return None
    bound = last + idle + _RELEASE_BOUND_S
    wall = _parse_hhmmss(rt.user_endpoint_config.get("walltime"))
    return min(bound, rt.block_since + wall) if wall and rt.block_since is not None else bound

def _presumed_release_at(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """The latest the clock says the shape's block lived (the bill's end), for banking on a stop/teardown that never
    reaches _provision; None when it is presumed alive (bill to now)."""
    gone = _presumed_reaped(app, shape, rt)
    return gone[1] if gone is not None else None

def _mark_reaped(app: AppCtx, rt: ShapeRuntime, why: str, released_at: float | None, *, kicked: bool = False) -> None:
    """The block this shape's spend acknowledgement covered is gone: stop its clock (at the latest it could have
    lived, not now), forget its warmth, and require a fresh acknowledgement for the next block. `kicked`: the reap
    was found by a check that may already have asked for a new block."""
    _bank_warm_interval(rt, app, until=released_at)
    rt.warm_confirmed_at = rt.last_proof_at = rt.clock_stopped_at = None
    rt.block_since = rt.block_job = None
    rt.provisioning_since = None
    rt.spend_confirmed = False
    rt.reaped, rt.reap_told, rt.reap_kicked = why, False, kicked

# -- the wrong-block guard (session_shell) -----------------------------------------------------------------------------
# A check (the canary) runs only once the block's last proof of life is CANARY_TTL_S old, and every completed command
# refreshes it, so commands sent less than that apart used to run on a replacement block unasked for as long as they
# kept coming (live 2026-10-13: job 112 scancel'd, `sleep 75` ran on job 113; a probe sent 20 commands 40 s apart to a
# replaced block over 13 minutes, and no check ever ran). Now each command on a confirmed block carries the block's job,
# and the worker refuses it anywhere else.

def _guard_job(rt: ShapeRuntime) -> str | None:
    """The scheduler job a command sent now must run in, or None for no guard: only on a billed shape with one block at
    most (a different job is then a replacement, not a sibling), spend confirmed for a block whose job is known."""
    if _billable(rt) and _single_block(rt) and rt.spend_confirmed:
        return rt.block_job
    return None

def _new_guard(rt: ShapeRuntime) -> session_shell.Guard | None:
    """The guard for one dispatch (its job, and a fresh nonce), or None for no guard."""
    job = _guard_job(rt)
    return session_shell.Guard.new(job) if job is not None else None

def _guard_refusal(out: ShellOutcome, guard: session_shell.Guard) -> session_shell.Refusal | None:
    """The guard's refusal of the command it was sent with, or None when the command ran."""
    if out.phase != "complete":
        return None
    return session_shell.refusal(out.exit_code, out.stderr_snippet, guard)

def _wrong_block_outcome(app: AppCtx, shape: str, runtime: ShapeRuntime | None, guard: session_shell.Guard,
                         refused: session_shell.Refusal, *, reset: bool = False) -> ShellOutcome:
    """A command the guard refused: it reached another scheduler job than the block it was sent for (`guard.job`), and
    did NOT run. While that block is still the confirmed one, this is the reap `_confirm_worker` makes when a different
    job answers a check, found by the command instead: kicked (the replacement is up and billing), the old block banked
    to the latest it could have lived, spend asked again, the replacement not adopted until the user confirms it.
    `runtime`: the shape runtime the command was sent on (None: the current one) — a released one is never marked."""
    rt = app.shapes.get(shape)
    what = "reset_session" if reset else "command"
    old = guard.job
    new = f"job {refused.job}" if refused.job is not None else "a job whose id hpc-bridge cannot read"
    if rt is None or (runtime is not None and runtime is not rt):  # released under it: nothing here to mark or ask
        return _with_spend(app, ShellOutcome(
            phase="failed", block_state="cold", exit_code=None,
            notice=(f"Your {what} did NOT run: it reached {new}, not the block it was sent to (job {old}), and that "
                    "block was released meanwhile (a stop, teardown or re-connect). The next command starts on a new "
                    "block, and a billed one asks for spend again.")))
    if rt.spend_confirmed and rt.block_job == old and _single_block(rt):
        _mark_reaped(app, rt, f"the previous block (job {old}) is gone — idle-released, cancelled, preempted or failed "
                     f"— and a command sent for it reached a new block ({new}), started for work sent after the old "
                     "block went; the command was refused there, so nothing has run on it; "
                     f"{_kicked_release_note(app)}", _release_bound(app, shape, rt), kicked=True)
        rt.reap_told = True  # this result tells it
        rt.transient_conflicts = 0
        # The last worker to answer is the replacement's. A confirm that skips its canary (a task of ours still holds
        # the block) dates the block by the last answer's job (_provision): make that this job, not the old block's,
        # or the next guard would expect the block that is gone.
        prev = rt.last_canary if rt.last_canary is not None and rt.last_canary.ok else CanaryResult(ok=True)
        rt.last_canary = dataclasses.replace(prev, worker_host=None, worker_job=refused.job,
                                             answered_at=time.monotonic())
        _saw_job(app, refused.job, rt.last_canary.answered_at or time.monotonic())
    if not rt.spend_confirmed:  # this reap, or one a check or another command found first
        return _with_spend(app, ShellOutcome(
            phase="needs_confirmation", block_state="provisioning" if rt.reap_kicked else "cold",
            notice=(f"Your {what} did NOT run: the previous block (job {old}) is gone — idle-released, cancelled, "
                    f"preempted or failed — and the {what} reached a new block ({new}) instead, which is already "
                    "running and billing the allocation. Confirm spend with the user before anything runs on it; "
                    f"{_kicked_release_note(app)}. The earlier confirmation covered job {old} only. "
                    + _spend_floor_guidance(app))))
    # a late result: spend was confirmed again after this command was sent — for whichever block is up now
    now_on = f" (now for job {rt.block_job})" if rt.block_job is not None else ""
    return _with_spend(app, ShellOutcome(
        phase="failed", block_state="warm" if rt.block_since is not None else "provisioning", exit_code=None,
        notice=(f"Your {what} did NOT run: it reached {new}, not the block it was sent to (job {old}). Spend has been "
                f"confirmed again since it was sent{now_on}: send it again if it is still wanted.")))

def _check_reaped(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """Before anything voids the evidence (a provision, a partition or account switch): if the clock says the block
    the spend acknowledgement covered is gone, record the reap. Not under a no-account verdict: no block can be started
    for this identity, so asking for spend would only lead to the same refusal."""
    if _billable(rt) and rt.spend_confirmed and rt.reaped is None and not rt.no_account:
        idle_gone = _presumed_idle_released(app, shape, rt)
        gone = idle_gone or _presumed_past_walltime(app, shape, rt)
        if gone is not None:
            if idle_gone is not None:  # it may still be up: remember which block it was, its age, and its latest end
                rt.presumed_block_since, rt.presumed_block_job = rt.block_since, rt.block_job
                rt.presumed_block_until = idle_gone[1]
                # ...and where the reap below stops its spend clock, so the same block can resume it from there
                rt.presumed_billed_until = (min(time.monotonic(), idle_gone[1]) if rt.warm_since is not None
                                            else rt.clock_stopped_at)  # (already stopped there, by a failure)
            _mark_reaped(app, rt, *gone)

def _forget_presumed(rt: ShapeRuntime) -> None:
    rt.presumed_block_since = rt.presumed_block_job = rt.presumed_block_until = rt.presumed_billed_until = None

# How many jobs `AppCtx.job_seen_since` remembers per binding: far more than one session meets; the oldest go first.
_JOBS_SEEN_MAX = 256

def _saw_job(app: AppCtx, job: str | None, at: float) -> None:
    """Scheduler job `job` was proven up at `at` (a check or a command reported it): remember the first such time on
    this endpoint, never a later one."""
    if job is None:
        return
    app.job_seen_since.setdefault((app.state.endpoint_id, job), at)
    while len(app.job_seen_since) > _JOBS_SEEN_MAX:
        del app.job_seen_since[min(app.job_seen_since, key=app.job_seen_since.__getitem__)]

def _answering_block_since(app: AppCtx, rt: ShapeRuntime, job: str | None, now: float) -> float:
    """When the block that has just answered started, for its walltime: when its scheduler job was first proven up on
    this endpoint (`AppCtx.job_seen_since`), whatever partition or account it is on — a job that answers again after an
    idle presumption, a stop or a switch away and back keeps its real age. A job not seen before started now. With
    either id unknown (not expected: every Slurm or PBS block has one), a block answering before the idle-presumed
    one's latest possible end keeps the older age, so its walltime is presumed early (a question) rather than missed."""
    old, old_job, until = rt.presumed_block_since, rt.presumed_block_job, rt.presumed_block_until
    if job is not None and (old is None or old_job is not None):  # by job: the earliest proof of it
        ages = [app.job_seen_since.get((app.state.endpoint_id, job), now)]
        if old is not None and old_job == job:
            ages.append(old)
        return min(ages)
    return old if old is not None and until is not None and now < until else now

async def _provision(
    app: AppCtx, shape: str, *, force_canary: bool = False, confirm_spend: bool = False
) -> ProvisionResult:
    """Provision/probe under the session profile and update the spend clock. Returns the
    block state. 'warm' means a WORKER answered a canary — not merely that the manager is
    online; that distinction is the cold-start gap this closes.

    Deterministic spend floor: a scheduler compute shape returns 'needs_confirmation' and starts
    NOTHING until spend is acknowledged (confirm_spend=True, or confirmed for the block that is still up — a reaped
    block voids it, and the call that finds the reap stops even with confirm_spend=True).
    The carve-out only applies to billable shapes — a login (LocalProvider) shape is free and
    provisions straight through."""
    rt = _shape_runtime(app, shape)
    _check_reaped(app, shape, rt)
    if rt.reaped is not None and not rt.reap_told:
        # The call that finds the reap answers needs_confirmation even if it carried confirm_spend=True: that
        # acknowledgement was given without knowing the block was gone. The user is asked for the NEW block.
        rt.reap_told = True
        return "needs_confirmation"
    if _billable(rt) and not rt.spend_confirmed:
        if not confirm_spend:
            return "needs_confirmation"  # gate BEFORE bootstrap/probe/canary — no block, no charge
        # The ACCOUNT floor, beside the spend floor: a facility whose catalog entry says account_required gets
        # NOTHING until an allocation account is set (passed now, or sticky from an earlier call). Live 2026-09-09:
        # on NCSA Delta (account_required) the agent confirmed spend with no account — the user had offered a
        # LOGIN NAME — and the MEP submitted a GPU block Slurm could only reject, which a MEP has no channel to see,
        # so it read "allocating nodes…" for five minutes. Every channel carries the flag (an SSH facility in its
        # profile), and the account read is the one the block would be charged to: this session's choice or the
        # facility's configured default (the startup pin's), both in the shape's config.
        if getattr(app.facility, "account_required", False) and not rt.user_endpoint_config.get("account"):
            return "needs_account"  # spend stays unconfirmed: the re-call with account= re-gates cleanly
        rt.spend_confirmed = True  # ack persists for this block (cleared when it is reaped, or its terms switched)
        rt.reaped, rt.reap_told, rt.reap_kicked = None, False, False
        rt.switched_terms, rt.switch_left, rt.switch_unseen = None, None, False
    if app.state.endpoint_id is None:
        bootstrap = getattr(app.facility, "bootstrap", None)
        if bootstrap is not None:
            handle = await bootstrap(app.profile)
            app.state = EndpointState(endpoint_id=handle.endpoint_id, reused=handle.reused)
    try:
        block, app.state = await ensure_warm(app.facility, app.profile, app.state)
        # Before the first runner for this endpoint exists: learn the worker_init the endpoint was configured with, so
        # its tasks carry that one (an SSH facility; once per endpoint id). Every path to an endpoint id passes here.
        adopt = getattr(app.facility, "adopt_endpoint", None)
        if app.state.endpoint_id and inspect.iscoroutinefunction(adopt):
            await adopt(app.state.endpoint_id)
        checked = block == "warm"
        if checked:  # manager online -> confirm a worker is actually live
            block = await _confirm_worker(app, shape, force=force_canary)
    except Exception:
        _bank_warm_interval(rt, app)  # nothing could be checked: the caller reports `down` / an error — clock stopped
        raise
    if block == "warm" and _billable(rt) and not rt.spend_confirmed:
        # Defensive invariant: a billed block is dated and billed only once spend is confirmed for it. Today every
        # path in _confirm_worker that voids the confirmation (its reaps) already answers "provisioning"; this keeps a
        # future path from adopting a block the user has not confirmed.
        block = "provisioning"
    billed_as = block
    if checked and block != "warm" and _billable(rt) and rt.spend_confirmed and rt.block_since is not None:
        # A check that failed with an error left the confirmed block's identity in place (_confirm_worker). It is
        # billed as warm while it could still be up: its clock runs on, or restarts at once if this call's runner
        # rebuild banked it, so a same-job answer leaves no unbilled gap, and the idle presumption above ends it at its
        # release bound. A terminal verdict (a `down`: a rejected submit, which with one block means the confirmed one
        # is gone; no account; the conflict limit), or a block with no bound (an unknown idle window: nothing would
        # end the hold) stops the clock instead, at the latest the block could have lived. A `down` never leaves a
        # clock running.
        bound = _release_bound(app, shape, rt)
        if bound is not None and not _terminal_failure(rt):
            billed_as = "warm"
        else:
            _bank_warm_interval(rt, app, until=bound)
    if billed_as == "warm":
        if rt.block_since is not None and rt.warm_since is None and rt.clock_stopped_at is not None:
            # the block that kept its identity is up (or held) again after its clock was stopped without it being found
            # gone: the same block was billing all along, so the clock resumes where it stopped and the gap is billed.
            # (A different job would have been a reap; a block first confirmed now has no stop point to resume.)
            rt.warm_since = rt.clock_stopped_at
        rt.clock_stopped_at = None
    _settle_billing(rt, app, billed_as)
    if block == "warm" and rt.block_since is None:  # a block first confirmed warm: its age (for its walltime) and job
        job = rt.last_canary.worker_job if rt.last_canary is not None and rt.last_canary.ok else None
        since = _answering_block_since(app, rt, job, time.monotonic())
        if (rt.presumed_block_since is not None and since == rt.presumed_block_since
                and rt.presumed_billed_until is not None):
            rt.warm_since = rt.presumed_billed_until  # the presumed block lived on: its spend clock continues
        _take_left(app, rt, job)  # names only: a block a facility stop left is this one, or gone (#178)
        rt.block_since, rt.block_job = since, job
    if block == "warm":
        _forget_presumed(rt)
    if rt.reaped is not None and not rt.reap_told:  # the check just found the block gone (_confirm_worker)
        rt.reap_told = True
        return "needs_confirmation"
    return block

# Partition names come from the discovery gate (agent/user-supplied), then flow into a Jinja
# template rendered on the login node — so validate the token at the boundary (no shell/YAML
# metacharacters). Scheduler partition/queue names are short identifiers; this allowlist covers
# real ones (letters, digits, '_', '-', '.', ':') without admitting an injection vector.
_VALID_PARTITION = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_VALID_ACCOUNT = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")

def _apply_partition(app: AppCtx, shape: str, rt: ShapeRuntime, partition: str | None) -> str | None:
    """Point this shape's next provision at `partition`, invalidating a stale runner. Returns a
    rejection notice (and applies nothing) when a task is still running on the shape: the change would
    mark the runner stale, and the next _runner_for would close its Executor and cancel that task — so
    make the caller poll_task/stop_endpoint first. Otherwise returns None.

    No-op when `partition` is None (keep the facility/profile default) or unchanged, or for the
    login shape, which has no partition. A real change means a different scheduler block (_repoint): the
    confirmation covered the old block only, the cached runner is stale (its Executor captured the old partition at
    build time — _runner_for rebuilds it), and nothing cancels the old block (an SSH endpoint's idle-releases on its
    own, min_blocks=0; a facility endpoint's is left billing and named). The selection persists for the session, a
    stop included."""
    if partition is None or not rt.user_endpoint_config.get("compute"):
        return None
    if rt.user_endpoint_config.get("partition") == partition:
        return None
    live = _live_task_handles(app, shape)
    if live:
        return (f"can't change partition to {partition!r}: a task is still running "
                f"(task_id={live[0][0]!r}) on shape {shape!r}. poll_task it to completion first (stop_endpoint "
                "refuses while it runs).")
    # a command inside its sync-wait: the swap would orphan it, and its count would vouch for the new block
    if rt.inflight:
        return (f"can't change partition to {partition!r}: a command is still running on shape {shape!r} "
                "(inside run_shell's wait). Let it return, then change it.")
    _repoint(app, shape, rt, "partition", partition)
    return None

def _apply_account(app: AppCtx, shape: str, rt: ShapeRuntime, account: str | None) -> str | None:
    """Point this shape's next provision at `account` (the chosen allocation) — the account
    analogue of _apply_partition. Returns a rejection notice (and applies nothing) when a task is still
    running on the shape (the runner swap would cancel it). compute-shape only; the config_template
    renders `account` from user_endpoint_config with the profile default, so a selection here overrides
    it. A change is a different block (_repoint); the selection persists for the session, a stop included."""
    if account is None or not rt.user_endpoint_config.get("compute"):
        return None
    if rt.user_endpoint_config.get("account") == account:
        return None
    live = _live_task_handles(app, shape)
    if live:
        return (f"can't change account to {account!r}: a task is still running "
                f"(task_id={live[0][0]!r}) on shape {shape!r}. poll_task it to completion first (stop_endpoint "
                "refuses while it runs).")
    # a command inside its sync-wait: the swap would orphan it, and its count would vouch for the new block
    if rt.inflight:
        return (f"can't change account to {account!r}: a command is still running on shape {shape!r} "
                "(inside run_shell's wait). Let it return, then change it.")
    _repoint(app, shape, rt, "account", account)
    return None

def _repoint(app: AppCtx, shape: str, rt: ShapeRuntime, key: str, value: str) -> None:
    """A real partition or account change, once nothing of ours holds the block: a different block.

    The spend confirmation covered the old terms' block only, so it is dropped: the call answers needs_confirmation
    unless it carries confirm_spend=True itself, and the answer says what became of the old block
    (`rt.switched_terms`, `rt.switch_left`; notices._reask_prefix). Nothing here cancels that block: on an SSH endpoint
    it idle-releases on its own (min_blocks=0) and stop_endpoint cancels it; on a facility endpoint it is left billing
    and remembered for naming (_leave_switched, #178). Either way whatever block of ours is known up there is billed
    once, to the latest it can bill (_leave_block). A finished command the wrong-block guard refused is taken in first
    (`_settle_refusals`): the replacement it found is such a block. If the old job is met again later, it keeps its age
    (dated by its job) and bills afresh from that confirmation: switching back and forth over-counts, never under.
    Task handles from before the switch stop vouching for this shape's block (`block_epoch`). The new value is the
    session's choice (`app.compute_choice`)."""
    _settle_refusals(app, shape, rt)
    _check_reaped(app, shape, rt)  # a reap that already happened must not hide behind the switch
    uec = rt.user_endpoint_config
    terms = (uec.get("partition"), uec.get("account"))
    confirmed = _billable(rt) and rt.spend_confirmed
    unseen = rt.block_since is None and rt.warm_confirmed_at is None  # asked for, never seen up: its pilot may land
    if _has_login_shape(app):  # an SSH endpoint: a block idles out — banked to that latest end, not named
        _leave_block(app, shape, rt, how="switch", remember=False)
    elif (left := _leave_switched(app, shape, rt)) is not None:  # a facility endpoint: it bills on, and is named
        rt.switch_left = left
    if confirmed:
        rt.spend_confirmed = False
        rt.switched_terms, rt.switch_unseen = terms, unseen
    _bank_warm_interval(rt, app)  # (a no-op once the block was left)
    uec[key] = value
    app.compute_choice[key] = value
    rt.runner_stale = True
    rt.warm_confirmed_at = rt.last_proof_at = rt.clock_stopped_at = None
    rt.block_since = rt.block_job = None  # a different partition or account is a different block: not the old one's age
    _forget_presumed(rt)
    rt.block_epoch += 1

def _settle_refusals(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """Before a switch lets go of the block: a finished command of this runtime that the wrong-block guard refused, not
    polled yet, found a replacement block — take that in now, as its poll would have (`_wrong_block_outcome`: a kicked
    reap), so the switch leaves the replacement billed and named, not the block that is gone. The handle stays: its
    poll still says the command did not run."""
    for h in list(app.tasks.values()):
        if (h.shape == shape and h.guard is not None and h.future.done() and not h.future.cancelled()
                and (h.runtime is None or h.runtime is rt)
                and (refused := _guard_refusal(_finished_outcome(h), h.guard)) is not None):
            _wrong_block_outcome(app, shape, rt, h.guard, refused, reset=h.reset)

def _check_queued(rt: ShapeRuntime) -> bool:
    """A check (canary) of ours is still queued on the endpoint: its runner holds one unresolved, or the last check
    timed out and nothing has proved a block live since. On a facility endpoint the facility starts (or keeps) a
    billed block for it, whatever this session does next — nothing here can cancel it (#178)."""
    if getattr(rt.runner, "canary_pending", False) is True:
        return True
    c = rt.last_canary
    return c is not None and c.error == "timeout" and rt.warm_confirmed_at is None

def _work_on_block(app: AppCtx, shape: str, rt: ShapeRuntime) -> bool:
    """Work of ours may still be on the block: a task whose end is not stamped yet (running, or presumed lost but never
    resolved), or a call still inside its sync-wait. Its idle window cannot have started, so the idle bound says
    nothing about when it ends."""
    return bool(rt.inflight) or any(h.done_at is None for h in app.tasks.values() if h.shape == shape)

def _resolve_left(app: AppCtx, shape: str, rt: ShapeRuntime, now: float,
                  how: str) -> tuple[LeftBlock, float | None] | None:
    """The block this shape leaves running on a facility endpoint — and where its spend clock stands, from which a stop
    banks it to its latest end — or None when there is none to name. Read BEFORE the shape's state is dropped, in this
    order (#178):
    1. the block confirmed warm (`block_job`; its identity survives a failed check);
    2. else the new block a check found answering after a reap (`reap_kicked`: the check's own job), from its answer;
    3. else a block whose check answered after its caller stopped waiting (`canary_answer`: read now, never waited on);
    4. else the block the idle presumption took for gone (`presumed_block_job`), to its latest end;
    5. else a check still queued (`_check_queued`): no block seen yet, no job id.
    A check still queued is flagged on 1, 2 and 4. The latest end (`until`, `bound`) is the idle bound — last activity +
    idle window + _RELEASE_BOUND_S, capped at the walltime — or the walltime's end alone when the idle window is
    unknown, or when work of ours may still be on the block (`_work_on_block`: no idle window has started)."""
    if not _billable(rt):
        return None
    uec = rt.user_endpoint_config
    wall = _parse_hhmmss(uec.get("walltime"))
    idle = _idle_window(app)
    queued = _check_queued(rt)
    base = LeftBlock(endpoint_id=app.state.endpoint_id, job=None, until=None, left_at=now, how=how,
                     check_queued=queued, partition=uec.get("partition"), account=uec.get("account"),
                     multi=not _single_block(rt))

    def answered(seen: float, job: str | None, check_queued: bool) -> LeftBlock:  # a block seen only by a check
        until = seen + idle + _RELEASE_BOUND_S if idle else None
        bound = "idle" if idle else ("walltime" if wall else None)
        ends = seen + wall if wall else None  # it started before it answered: its walltime ends before this
        if ends is not None:
            until = min(until, ends) if until is not None else ends
        return replace(base, job=job, until=until, bound=bound, ends_by=ends, check_queued=check_queued)

    if rt.block_since is not None:
        ends = rt.block_since + wall if wall else None
        if _work_on_block(app, shape, rt):
            until, bound = ends, ("busy" if ends is not None else None)
        else:
            until, bound = _release_bound(app, shape, rt), "idle"
            if until is None:
                until, bound = ends, ("walltime" if ends is not None else None)
        # the clock is banked first (_leave_block): from where it stopped
        return replace(base, job=rt.block_job, until=until, bound=bound, ends_by=ends, verified=True), None
    canary = rt.last_canary
    if rt.reap_kicked and canary is not None and canary.ok:
        # not adopted (no age, no clock) until the user confirms it — but it has been billing since it answered
        seen = min(now, canary.answered_at) if canary.answered_at else now
        return answered(seen, canary.worker_job, queued), seen
    late = getattr(rt.runner, "canary_answer", lambda: None)()
    if isinstance(late, CanaryResult) and late.ok:
        # our check answered after its caller stopped waiting: the block it started (or ran on) is up, unbilled so far
        if late.worker_user:
            app.worker_user = late.worker_user
        seen = min(now, late.answered_at) if late.answered_at else now
        _saw_job(app, late.worker_job, seen)
        return answered(seen, late.worker_job, False), seen
    if rt.presumed_block_since is not None:
        ends = rt.presumed_block_since + wall if wall else None
        return (replace(base, job=rt.presumed_block_job, until=rt.presumed_block_until, bound="idle", ends_by=ends,
                        verified=True), rt.presumed_billed_until)
    if queued:
        return replace(base, kind="check"), None  # a queued check never expires: the queue wait is unbounded
    return None

def _leave_block(app: AppCtx, shape: str, rt: ShapeRuntime, *, how: str, remember: bool = True) -> LeftBlock | None:
    """A facility-endpoint stop, detach, reconnect or switch lets go of this shape's block, which keeps billing (no
    cancel channel there, #178): resolve it (`_resolve_left`), stop its spend clock and bank it ONCE to its latest end —
    the honest upper bound, beyond now when that is still ahead — and remember it in `app.left_blocks`, where it is
    only ever NAMED again (a later stop, a teardown, a re-bind note), never billed or dated. Callers hold app.lock."""
    now = time.monotonic()
    _prune_left(app, now)
    found = _resolve_left(app, shape, rt, now, how)
    if found is None:
        return None
    left, billed = found
    if rt.block_since is not None:  # the confirmed block: stop its clock where a drop would (now, or its presumed end)
        _bank_warm_interval(rt, app, until=_presumed_release_at(app, shape, rt))
        billed = rt.clock_stopped_at
    # The job's high-water mark: how far a bank of ours has already counted this job. The tail adds only the part
    # beyond it, so stop/re-confirm cycles of one block never recount a span its earlier tail counted (each cycle can
    # still count its own confirmation-to-stop clock twice: session_spend is an upper bound).
    key = (left.endpoint_id, left.job) if left.job else None
    mark = app.billed_to.get(key) if key is not None else None
    start = billed if billed is None or mark is None else max(billed, mark)
    counted = billed
    if left.until is not None and start is not None and left.until > start:
        rt.spend_accrued += estimate_spend(left.until - start, _block_nodes(rt, app), app.charge_factor)
        counted = left.until
    if key is not None and counted is not None:
        app.billed_to[key] = counted if mark is None else max(mark, counted)
    if remember:  # an SSH switch is banked the same way, but never named: a stop there cancels it
        _remember_left(app, left)
    return left

def _remember_left(app: AppCtx, left: LeftBlock) -> None:
    """Keep `left` in `app.left_blocks`, replacing an entry for the same job — or, unnamed, the same kind on the same
    partition and account: the most recent queued check per partition and account is kept (one facility user endpoint
    each), so checks left on other partitions stay named."""
    def same(b: LeftBlock) -> bool:
        if b.endpoint_id != left.endpoint_id or b.kind != left.kind:
            return False
        if left.job is not None or b.job is not None:
            return b.job == left.job
        return (b.partition, b.account) == (left.partition, left.account)
    app.left_blocks = [b for b in app.left_blocks if not same(b)] + [left]

def _leave_switched(app: AppCtx, shape: str, rt: ShapeRuntime) -> LeftBlock | None:
    """A partition or account switch on a facility endpoint: leave the old block (`_leave_block`, banked once), then let
    go of everything that still points at it — the old runner (closed: its unread late answer belongs to the old
    partition), the last check and a reap's kick — so no later stop reads that block again and files it under the new
    partition (review of a6de41e, X). Returns the block left, for the spend question to name."""
    left = _leave_block(app, shape, rt, how="switch")
    _bank_warm_interval(rt, app)  # (a no-op once _leave_block banked it)
    if rt.runner is not None:
        rt.runner.close()
    rt.runner, rt.runner_stale = None, False
    rt.last_canary, rt.reap_kicked = None, False
    return left

def _prune_left(app: AppCtx, now: float | None = None) -> list[LeftBlock]:
    """Drop the left BLOCKS that cannot still be running — past their walltime's end — and return the rest. A queued
    check is never dropped: the queue wait is unbounded, and the facility keeps starting blocks while it waits; its
    entry stays dated instead ("left queued at …; it may have run since")."""
    now = time.monotonic() if now is None else now
    app.left_blocks = [b for b in app.left_blocks if b.ends_by is None or b.ends_by > now]
    return app.left_blocks

def _take_left(app: AppCtx, rt: ShapeRuntime, job: str | None) -> None:
    """A block first confirmed warm answers for the entries a facility stop, detach, reconnect or switch left on this
    endpoint (#178) — for NAMING only: the same job is that block, now this shape's again; with one block at most, an
    entry on the same partition and account (the same facility user endpoint, so a queued check of it too) is gone,
    since the endpoint started this block only after it went. Either way it leaves `app.left_blocks`, so no later
    notice names a stale job. Nothing is billed or dated from it: the block confirmed now bills from its own
    confirmation (session_spend is an upper bound, and may count an overlap with the left block's bank twice)."""
    eid, uec = app.state.endpoint_id, rt.user_endpoint_config
    app.left_blocks = [
        b for b in app.left_blocks
        if not (b.endpoint_id == eid and ((job is not None and b.job == job) or (
            _single_block(rt) and (b.partition, b.account) == (uec.get("partition"), uec.get("account")))))
    ]

def _known_block_job(app: AppCtx, rt: ShapeRuntime | None) -> tuple[str | None, bool]:
    """The scheduler job of a CONFIRMED block of this shape's partition and account, not presumed gone, for the
    five-minute allocating notice to name — and whether it is this shape's own block (`block_job`, whose cancel the
    notice gives); else the newest confirmed block a stop or switch left on the same partition and account that may
    still bill (named, never given a cancel: it may be gone). (None, False): none. Never another partition's job, a
    presumed-gone one, or one only a check saw — the user could cancel someone else's job (#178)."""
    if rt is None:
        return None, False
    if rt.block_job:
        return rt.block_job, True
    now, uec = time.monotonic(), rt.user_endpoint_config
    for b in reversed(_prune_left(app, now)):
        if (b.endpoint_id == app.state.endpoint_id and b.job and b.verified
                and (b.partition, b.account) == (uec.get("partition"), uec.get("account"))
                and (b.until is None or b.until > now)):
            return b.job, False
    return None, False

async def _drop_compute_shape(app: AppCtx) -> float:
    """Drop the billed (compute) shape so a later run re-provisions a FRESH block (its runner now
    points at the released block) and stop its spend clock. Keep the login shape (if any), the
    manager, the endpoint_id, and the login-node pin — the endpoint stays online and reusable. Done
    regardless of cancel confirmation: the runner is dead either way, and banking must stop now.
    Returns the spend the dropped shape had accrued; it is ALSO folded into `app.released_spend`, which
    _total_session_spend() counts — so callers must not add the return value again (0.1.18)."""
    async with app.lock:
        return _drop_compute_shape_locked(app)

def _drop_compute_shape_locked(app: AppCtx) -> float:
    """_drop_compute_shape for a caller that already holds app.lock — a stop that must check what holds the block and
    drop it in ONE locked step, so no dispatch can be counted in between."""
    compute = app.shapes.get(DEFAULT_SHAPE)
    until = _presumed_release_at(app, DEFAULT_SHAPE, compute) if compute is not None else None
    _drain_shape_tasks(app, DEFAULT_SHAPE)  # the released block's poll handles are now dead
    compute = app.shapes.pop(DEFAULT_SHAPE, None)
    _retain_inflight(app, compute)
    if compute is None:
        return 0.0
    # stop the spend clock — at the presumed release if the block idled out or hit its walltime long ago
    _bank_warm_interval(compute, app, until=until)
    if compute.runner is not None:
        compute.runner.close()
    app.released_spend += compute.spend_accrued  # still part of the session: _total_session_spend counts it
    return compute.spend_accrued

def _forget_identity_verdicts(app: AppCtx) -> None:
    """A new Globus login may be a different identity: drop every sticky no-account verdict and make the
    runners rebuild (their Executors were built on the old credential)."""
    from .login import reset_identity_label

    reset_identity_label()  # the cached "who am I" may name the previous identity
    app.worker_user = None  # ...and so may the local account the facility mapped it to
    for rt in app.shapes.values():
        if rt.no_account:
            rt.no_account = None
            rt.last_canary = None
        rt.runner_stale = True

async def _ensure_warm_runner(app: AppCtx, shape: str) -> ProvisionResult | None:
    """Ensure a worker is live and the shape's runner is bound to it; returns the block state
    if NOT warm (caller returns a cold_start), else None. _provision -> _confirm_worker
    (re)creates the runner and proves a worker answered, so on 'warm' the runner is ready."""
    block = await _provision(app, shape, force_canary=False)
    return None if block == "warm" else block

def _busy_session(app: AppCtx, shape: str, session_id: str) -> str | None:
    """task_id of a task still running on this (shape, session_id), else None. A busy session can't
    take a second command: the two would concurrently mutate the same on-disk cwd/env on the worker.
    (Covers the sequential case — a prior command that became a poll handle; two *simultaneously*
    submitted commands on one session is a pre-existing race, unchanged here.)"""
    for tid, h in _live_task_handles(app, shape):
        if h.session_id == session_id:
            return tid
    return None

def _register_task(app: AppCtx, shape: str, session_id: str, command: str, fut, ceiling_s: float, *,
                   client_cancelled: bool = False, runtime: ShapeRuntime | None = None, reset: bool = False,
                   submission: Submission | None = None, guard: session_shell.Guard | None = None) -> str:
    """Register a still-running task as a poll handle and return its id. Caller holds app.lock — or calls it in one
    synchronous step with no await around it (a cancelled call, which must not wait for the lock), which nothing can
    interleave with either. `runtime`: the shape runtime it was dispatched on (its result is noted there only while
    that runtime is still the shape's — a released block's runtime is never rebuilt for it). `guard`: its wrong-block
    guard (the job and nonce), so a refusal found when it is polled is still the replacement found."""
    app.task_seq += 1
    task_id = f"{shape}-{app.task_seq}"
    sent_on = runtime if runtime is not None else app.shapes.get(shape)
    handle = TaskHandle(
        future=fut,
        shape=shape,
        session_id=session_id,
        command=command,
        submitted_at=time.monotonic(),
        ceiling_s=ceiling_s,
        client_cancelled=client_cancelled,
        runtime=runtime,
        reset=reset,
        submission=submission,
        guard=guard,
        epoch=sent_on.block_epoch if sent_on is not None else 0,
    )

    def _stamp(_f: object) -> None:  # runs on the SDK's thread when the task resolves; one float assignment
        if handle.done_at is None and not fut.cancelled():  # a cancel (teardown) is not activity on the block
            handle.done_at = time.monotonic()

    fut.add_done_callback(_stamp)  # an already-done future calls it at once
    app.tasks[task_id] = handle
    return task_id

def _resolve_task(app: AppCtx, task_id: str) -> ShellOutcome | None:
    """Under the caller's app.lock: shape a terminal outcome if the task is gone/cancelled/finished
    (popping it — the atomic claim, so a concurrent poll gets a benign miss), else None if it's still
    running. Refreshes worker liveness / spend on a finished task."""
    handle = app.tasks.get(task_id)
    if handle is None:
        return ShellOutcome(
            phase="failed", block_state="warm", exit_code=None,
            notice=f"no task {task_id!r} — already retrieved, or its block ended (stop / partition / switch).",
        )
    fut = handle.future
    if fut.cancelled():
        app.tasks.pop(task_id, None)
        return _with_spend(app, ShellOutcome(
            phase="failed", block_state="warm", exit_code=None,
            notice=f"task {task_id!r} was cancelled when its block was torn down.",
        ))
    if not fut.done():
        return None  # still running
    app.tasks.pop(task_id, None)  # atomic claim under the lock
    out = _finished_outcome(handle)
    if handle.guard is not None and (refused := _guard_refusal(out, handle.guard)) is not None:
        # refused on a replacement block, however late it is polled (a client-cancelled call's command included)
        return _wrong_block_outcome(app, handle.shape, handle.runtime, handle.guard, refused, reset=handle.reset)
    current = app.shapes.get(handle.shape)
    # proof of life for the block it ran on — not a reason to rebuild a released shape, nor news about a newer block
    # (a released runtime, or a switch since it was sent: the same runtime now stands for another block)
    if (current is not None and (handle.runtime is None or handle.runtime is current)
            and handle.epoch == current.block_epoch):
        _note_dispatch(current, out, at=handle.done_at)
    else:
        out.block_state = "cold"  # its block was released, or switched away from, while it ran
    return _with_spend(app, _with_submission_notice(out, handle.submission, handle.shape))

def _finished_outcome(handle: TaskHandle) -> ShellOutcome:
    """A finished handle's result, shaped exactly as execute() would shape it."""
    try:
        res = handle.future.result()  # done -> returns at once (or raises the task's own exception)
    except Exception as exc:  # noqa: BLE001 - a failed task is an outcome, not a raise
        return dispatch.failure_outcome(exc, "warm")
    return dispatch.complete_outcome(res, "warm")

async def _endpoint_gone(app: AppCtx) -> bool:
    """True when nothing can resolve a pending task any more: the endpoint the task was dispatched
    to is unbound, or its manager reports offline/deleted. A KILLED BLOCK under a LIVE endpoint is
    not this — Parsl relaunches the block and the task eventually runs, so polling stays correct."""
    eid = app.state.endpoint_id
    if eid is None:
        return True
    try:
        return not await app.facility.manager_online(eid)
    except Exception:  # noqa: BLE001 - a status hiccup must not condemn a live task; keep polling
        return False
