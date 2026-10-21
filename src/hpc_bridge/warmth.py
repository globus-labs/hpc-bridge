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

import asyncio
import dataclasses
import inspect
import queue
import re
import secrets
import sys
import threading
import time
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal

from globus_compute_sdk.errors import SerdeError, TaskExecutionFailed

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
    DeadPilot,
    LeftBlock,
    SeenPilots,
    SentCheck,
    ShapeRuntime,
    SshLeft,
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
from .facility.remote import BLOCK_MAX as _BLOCK_MAX  # the block-request numbers `_fresh_block` draws
from .facility.remote import block_number as _block_number
from .lifecycle import BlockState, EndpointState, ProvisionResult, ensure_warm
from .login import LoginExpiredError
from .models import ShellOutcome, Submission
from .notices import (
    _block_terms,
    _no_account_failure,
    _queue_word,
    _spend_floor_guidance,
    _submit_rejected,
    _submit_rejection_cause,
    _terms_line,
    _transient_dispatch_failure,
    _with_submission_notice,
)
from .runner import CanaryResult, GlobusRunner, never_sent
from .shapes import SHAPES, shape_config
from .state import SentCheckStore


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

def _seed_uec(app: AppCtx, shape: str) -> dict:
    """The user_endpoint_config a NEW runtime of `shape` starts with: the facility's defaults (SlurmFacility) merged
    with the shape's template vars, fitted to a facility MEP's schema, and — on the compute shape — the session's
    partition/account choice. Builds nothing: connect_facility reads the next block's terms from it too."""
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
    return uec

def _shape_runtime(app: AppCtx, shape: str) -> ShapeRuntime:
    """Resolve (and lazily build) the per-shape runtime, seeded by `_seed_uec`."""
    if shape not in SHAPES:
        raise ValueError(f"unknown shape {shape!r}")
    rt = app.shapes.get(shape)
    if rt is None:
        rt = ShapeRuntime(user_endpoint_config=_seed_uec(app, shape))
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
            _retire_runner(app, rt)
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
    scheduler rejected a block submission, the identity has no account, or the conflict streak hit its limit. A
    rejection is terminal whatever the block count: it reaches our check only through parsl's block-error handler
    (`set_bad_state_and_fail_all`), which needs every block that user endpoint ever had to be FAILED or MISSING (its
    threshold is max(1, init_blocks)) — so no block of it is still up, and none will be started (`_void_rejected`)."""
    error = rt.last_canary.error if rt.last_canary is not None and not rt.last_canary.ok else None
    return (bool(rt.no_account) or _submit_rejected(error)
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
    # On the record as soon as it is submitted (before the wait), and again when the wait ends — cancelled too (Esc, a
    # shutdown): a server that restarts must still see a check still queued.
    runner.on_sent = lambda: _note_checks_sent(app, shape, rt)
    try:
        result = await runner.canary(timeout=timeout)
    finally:
        runner.on_sent = None
        _note_checks_sent(app, shape, rt)
    rt.last_canary = result  # keep failures too: the error text is the diagnosis the caller needs
    if result.ok and result.worker_user:  # the account the work runs as (a facility's identity mapping): for squeue -u
        app.worker_user = result.worker_user
    if result.ok:  # its job is up: the first proof dates it (for its walltime), on whatever terms it is met again
        _saw_job(app, result.worker_job, min(now, result.answered_at) if result.answered_at else now)
        _check_resolved(app, rt)  # a worker ran our check: no pilot is dying before it any more, none waits to land
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
        _retire_runner(app, rt)  # its checks stay with the endpoint: a re-bind to it keeps them
    spent = _total_session_spend(app) if bank else 0.0
    app.shapes.clear()
    app.compute_choice.clear()  # chosen for this binding: a stop keeps them, a new binding or a teardown does not
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
    if rt.pilots is not None:  # that block's pilot is gone with it: never named as still in the queue
        rt.pilots = rt.pilots.without({rt.pilots.subject, rt.block_job})
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
                         refused: session_shell.Refusal, *, reset: bool = False,
                         at: float | None = None, epoch: int | None = None) -> ShellOutcome:
    """A command the guard refused: it reached another scheduler job than the block it was sent for (`guard.job`), and
    did NOT run. While that block is still the confirmed one, this is the reap `_confirm_worker` makes when a different
    job answers a check, found by the command instead: kicked (the replacement is up and billing), the old block banked
    to the latest it could have lived, spend asked again, the replacement not adopted until the user confirms it.
    `runtime`: the shape runtime the command was sent on (None: the current one) — a released one is never marked.
    `epoch`: its runtime's block_epoch when it was sent — a partition/account switch since means the terms the answer
    names are the next block's, not the replacement's it reached."""
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
        seen = at if at is not None else time.monotonic()  # when it refused (a polled handle's end), not when read
        rt.last_canary = dataclasses.replace(prev, worker_host=None, worker_job=refused.job, answered_at=seen)
        _saw_job(app, refused.job, seen)
    if not rt.spend_confirmed:  # this reap, or one a check or another command found first
        terms = _block_terms(app, rt)
        head = (f"Your {what} did NOT run: the previous block (job {old}) is gone — idle-released, cancelled, "
                f"preempted or failed — and the {what} reached a new block ({new}) instead, which is already running "
                "and billing the allocation")
        if epoch is not None and epoch != rt.block_epoch:  # switched since it was sent: these terms are another block
            said = (f"{head}; {_kicked_release_note(app)}. The earlier confirmation covered job {old} only, and the "
                    f"{_queue_word(app)} or account has been switched since: a confirmation now starts a block on the "
                    "new terms. "
                    + _terms_line(app, terms, lead="The new block's terms"))
        else:
            said = (f"{head}. Confirm spend with the user before anything runs on it; {_kicked_release_note(app)}. "
                    f"The earlier confirmation covered job {old} only. " + _terms_line(app, terms))
        return _with_spend(app, ShellOutcome(
            phase="needs_confirmation", block_state="provisioning" if rt.reap_kicked else "cold", block=terms,
            notice=said + _spend_floor_guidance(app, terms)))
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

# How many jobs `AppCtx.job_seen_since` remembers: far more than one session meets. The oldest go first, never the job
# of a shape's confirmed or presumed block.
_JOBS_SEEN_MAX = 256

def _saw_job(app: AppCtx, job: str | None, at: float) -> None:
    """Scheduler job `job` was proven up at `at` (a check or a command reported it): remember the first such time on
    this endpoint, never a later one."""
    if job is None:
        return
    app.job_seen_since.setdefault((app.state.endpoint_id, job), at)
    if len(app.job_seen_since) <= _JOBS_SEEN_MAX:
        return
    live = {(app.state.endpoint_id, j)
            for rt in app.shapes.values() for j in (rt.block_job, rt.presumed_block_job) if j}
    while len(app.job_seen_since) > _JOBS_SEEN_MAX and (spare := [k for k in app.job_seen_since if k not in live]):
        del app.job_seen_since[min(spare, key=app.job_seen_since.__getitem__)]

# How long past its walltime a job may still answer before the scheduler kills it (Slurm KillWait / OverTimeLimit, PBS's
# kill grace): only an answer later than this is a requeue. Safety first: a real requeue is then recognised one confirm
# later at worst (one more re-ask), never a job at its hard end taken for a fresh one.
_REQUEUE_GRACE_S = 30.0

def _answering_block_since(app: AppCtx, rt: ShapeRuntime, job: str | None, now: float) -> float:
    """When the block that has just answered started, for its walltime: when its scheduler job was first proven up on
    this endpoint (`AppCtx.job_seen_since`), whatever partition or account it is on — a job that answers again after an
    idle presumption, a stop or a switch away and back keeps its real age. A job not seen before started now. With
    either id unknown (not expected: every Slurm or PBS block has one), a block answering before the idle-presumed
    one's latest possible end keeps the older age, so its walltime is presumed early (a question) rather than missed.
    No job outlives its walltime: one that answers past it plus _REQUEUE_GRACE_S, counted from its first proof, was
    requeued under the same id (a preemption, a node failure, a PBS rerun) and runs a fresh walltime from that answer.
    `now` is when the block answered (a check's answer may be up to the runner's freshness window old), not the call's
    clock."""
    old, old_job, until = rt.presumed_block_since, rt.presumed_block_job, rt.presumed_block_until
    if job is not None and (old is None or old_job is not None):  # by job: the earliest proof of it
        ages = [app.job_seen_since.get((app.state.endpoint_id, job), now)]
        if old is not None and old_job == job:
            ages.append(old)
        wall = _parse_hhmmss(rt.user_endpoint_config.get("walltime"))
        if wall and now - min(ages) > wall + _REQUEUE_GRACE_S:  # requeued: this proof is its new first one
            app.job_seen_since[(app.state.endpoint_id, job)] = now
            return now
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
        # a replacement already up — on these terms: a kick met before a switch is the other terms' block
        kicked_up = (rt.reap_kicked and rt.last_canary is not None and rt.last_canary.ok
                     and (rt.reap_terms is None or rt.reap_terms == _terms_of(rt)))
        rt.spend_confirmed = True  # ack persists for this block (cleared when it is reaped, rejected or switched)
        rt.reaped, rt.reap_told, rt.reap_kicked = None, False, False
        rt.switched_terms, rt.switch_left, rt.reap_terms = None, None, None
        rt.switch_left_up, rt.switch_left_pilots = None, None
        new_request = not kicked_up  # this confirmation asks for a block (not one already answering)
        rt.rejected = None  # a new request: the rejection it follows is history (its re-ask has been answered)
        rt.confirmed_at = time.monotonic()  # a pilot that died before this is an earlier session's, not this block's
        rt.confirmed_wall = time.time()     # the same on the wall clock (the monotonic one stops while asleep)
        if app.state.endpoint_id is not None:  # its probes list this endpoint's pilots now; a re-bind reads them there
            app.ssh_left.pop(app.state.endpoint_id, None)
    else:
        new_request = False
    if app.state.endpoint_id is None:
        bootstrap = getattr(app.facility, "bootstrap", None)
        if bootstrap is not None:
            # A bootstrap may copy the stored login to the login node: never one Globus refuses to renew (a startup
            # pin or a call after a teardown reaches here without connect's gate). Forced, so a refresh token revoked
            # while its access token is still valid is caught too; a check that cannot reach Globus carries on.
            if app.login_flow is not None:
                verdict, why = await asyncio.to_thread(app.login_flow.check_stored_login, force_refresh=True)
                if verdict == "expired":
                    raise LoginExpiredError(why)  # no SSH: the caller reports it (notices._hpc_bridge_error)
            handle = await bootstrap(app.profile)
            app.state = EndpointState(endpoint_id=handle.endpoint_id, reused=handle.reused)
    try:
        block, app.state = await ensure_warm(app.facility, app.profile, app.state)
        # Before the first runner for this endpoint exists: learn the worker_init the endpoint was configured with, so
        # its tasks carry that one (an SSH facility; once per endpoint id). Every path to an endpoint id passes here.
        adopt = getattr(app.facility, "adopt_endpoint", None)
        if app.state.endpoint_id and inspect.iscoroutinefunction(adopt):
            await adopt(app.state.endpoint_id)
        if new_request:
            _fresh_block(app, shape, rt)  # after adopt: whether this endpoint's template allows it is known now
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
        answer = rt.last_canary if rt.last_canary is not None and rt.last_canary.ok else None
        job = answer.worker_job if answer is not None else None
        now = time.monotonic()  # dated by its answer: a cached one may be older than this call
        answered = min(now, answer.answered_at) if answer is not None and answer.answered_at else now
        since = _answering_block_since(app, rt, job, answered)
        if (rt.presumed_block_since is not None and since == rt.presumed_block_since
                and rt.presumed_billed_until is not None):
            rt.warm_since = rt.presumed_billed_until  # the presumed block lived on: its spend clock continues
        _take_left(app, rt, job)  # names only: a block a facility stop left is this one, or gone (#178)
        rt.block_since, rt.block_job = since, job
        # the pilot the probes were about is this block now: a later re-bind names the block, never that pilot as queued
        if rt.pilots is not None:
            rt.pilots = rt.pilots.without({rt.pilots.subject, job})
    if block == "warm":
        _forget_presumed(rt)
    if checked and block != "warm" and _rejected_now(rt):  # this call's check: the scheduler refused the submission
        _void_rejected(app, rt)
    if rt.reaped is not None and not rt.reap_told:  # the check just found the block gone (_confirm_worker)
        rt.reap_told = True
        return "needs_confirmation"
    return block

def _rejected_now(rt: ShapeRuntime) -> bool:
    """The check just made (the last one) failed because the scheduler REJECTED the block's submission, on a billed
    shape whose spend was confirmed: a terminal verdict (`_terminal_failure`) for every block of that user endpoint,
    whatever its block count — the rejection reaches us only once all of them have failed."""
    c = rt.last_canary
    return c is not None and not c.ok and _submit_rejected(c.error) and _billable(rt) and rt.spend_confirmed

def _void_rejected(app: AppCtx, rt: ShapeRuntime) -> None:
    """The scheduler REJECTED the block's submission (`_rejected_now`): no job id was issued, so no block was created
    and none will be from it — parsl's block-error handler failed every task of that user endpoint (our check among
    them: that failure is how the rejection reached us), a bad-state executor is never scaled out again, and the user
    endpoint stops itself. The spend confirmation covered that block, so it is dropped as a reap or a switch drops it:
    the next call without confirm_spend asks again (`needs_confirmation`), and the re-ask says why (`rt.rejected`,
    notices._reask_prefix) — the same terms are refused again. A block confirmed earlier is gone too (the handler
    fires only once every block of that user endpoint has failed): `_provision` stopped its clock at its latest end
    above, and its identity goes here, so the block a new confirmation starts is not taken for an unasked replacement
    of it (a kicked reap). No pilot dies before our check any more: the check failed with the rest
    (`_check_resolved`). Callers hold app.lock."""
    uec = rt.user_endpoint_config
    c = rt.last_canary
    # what the re-ask and the stop refer back to: the submit command's answer the check carried, in brief
    cause = _submit_rejection_cause(c.error if c is not None else None, c.submit if c is not None else None)
    rt.rejected = (cause, uec.get("partition"), uec.get("account"))
    rt.spend_confirmed = False
    rt.warm_confirmed_at = rt.last_proof_at = rt.clock_stopped_at = None
    rt.block_since = rt.block_job = None
    rt.provisioning_since = None
    _forget_presumed(rt)
    _check_resolved(app, rt)

# -- our checks still queued on a compute user endpoint (#24's evidence) -----------------------------------------------

def _terms_of(rt: ShapeRuntime) -> tuple[str | None, str | None]:
    uec = rt.user_endpoint_config
    return uec.get("partition"), uec.get("account")

def _runner_terms(runner: object, rt: ShapeRuntime) -> tuple[str | None, str | None]:
    """The partition and account a runner's checks asked for: its own config's (what it was built with — the shape's
    may have been switched since), else the shape's."""
    uec = getattr(runner, "user_endpoint_config", None)
    if isinstance(uec, dict) and uec.get("compute"):
        return uec.get("partition"), uec.get("account")
    return _terms_of(rt)

def _check_resolved(app: AppCtx, rt: ShapeRuntime) -> None:
    """Our check is no longer waiting for a pilot (a worker answered it, or the endpoint failed it with a rejected
    submission): the pilot that died before it is history."""
    rt.dead_pilot = None

def _note_dead_pilot(rt: ShapeRuntime, job: str, exit_status: str, jobs: frozenset[str]) -> None:
    """The pilot probe reported pilot `job` FINISHED (exit status `exit_status`) before any worker answered: record it
    on the shape, to name (the newest; every pilot job listed so far is known). Callers hold app.lock, on the runtime
    the probe was made for."""
    known = jobs | (rt.dead_pilot.known if rt.dead_pilot is not None else frozenset())
    rt.dead_pilot = DeadPilot(job=job, exit_status=exit_status, known=known | {job})

def _record_dead_pilot(app: AppCtx, shape: str, rt: ShapeRuntime, eid: str, epoch: int,
                       answered: float | None, job: str, exit_status: str, jobs: frozenset[str]) -> None:
    """The provisioning probe — run off the lock — found pilot `job` dead: record it, to name, on the runtime and terms
    the probe was for (the same shape, `block_epoch` unchanged) — whether or not the shape kept a block's identity (a
    check that failed with an error keeps it: review r2, S2) — unless a worker answered DURING the probe
    (`warm_confirmed_at` past `answered`, read before it): our check ran, so that pilot is history. After a switch or a
    drop meanwhile it belongs to the old terms' runner, whose checks the endpoint already keeps: nothing to record on
    this shape (it names a pilot; the checks are the evidence). Callers hold app.lock."""
    if app.state.endpoint_id != eid or app.shapes.get(shape) is not rt or rt.block_epoch != epoch:
        return
    if rt.warm_confirmed_at is None or (answered is not None and rt.warm_confirmed_at <= answered):
        _note_dead_pilot(rt, job, exit_status, jobs)

def _record_pilots(app: AppCtx, shape: str, rt: ShapeRuntime, eid: str, epoch: int,
                   live: tuple[tuple[str, str, str], ...], subject: str | None) -> None:
    """The provisioning probe's live pilots (scheduler_ops.PilotProbe.live, and the one its verdict is about),
    recorded on the runtime and terms it was for — as `_record_dead_pilot` guards it — so a switch (`_repoint`) or a
    reconnect (`_ssh_left`) can say what is still queued or held. Callers hold app.lock."""
    if app.state.endpoint_id != eid or app.shapes.get(shape) is not rt or rt.block_epoch != epoch:
        return
    rt.pilots = SeenPilots(jobs=live, at=time.monotonic(), subject=subject)

def _came_up(rt: ShapeRuntime) -> bool:
    """A worker answered on this shape's block (it was up), and no reap has been found since."""
    return rt.reaped is None and (rt.block_since is not None or rt.warm_confirmed_at is not None
                                  or rt.last_proof_at is not None)

def _ssh_left(rt: ShapeRuntime | None) -> SshLeft | None:
    """What a re-bind leaves of an SSH endpoint's compute shape, to say (connect._left_checks_note): nothing cancels a
    block on a re-bind. The block, if it came up (it idle-releases after its last task), and the pilots the last probe
    saw live other than that block's own job (a block's pilot leaves the record once a worker answers on it, and when
    the block is found gone). None: nothing of it is known in the scheduler."""
    if rt is None or not _billable(rt):
        return None
    up = rt.spend_confirmed and _came_up(rt)
    seen = rt.pilots.without({rt.block_job} if up else set()) if rt.pilots is not None else None
    if not up and seen is None:
        return None
    return SshLeft(up=up, job=rt.block_job if up else None, pilots=seen)

def _check_key(c: SentCheck) -> str:
    """One task, however it was found: its task id once registered, else the key of its row on disk — for a future of
    this process, one derived from it (the same while it lives), so the row written at the submit and the future
    match."""
    return c.tid or c.row_key or f"{_OWN_NO_ID}{id(c.future)}"

# The prefix of this process's no-id row keys: a process token, not the pid (a restarted server in a container can get
# the same pid), so another process's never collides with this one's.
_OWN_NO_ID = f"no-id-{secrets.token_hex(6)}-"

def _locally_resolved(c: SentCheck) -> bool:
    """Known resolved without asking the service: a worker answered it, the service failed it (TaskExecutionFailed is
    the service's own verdict), or it was certainly never sent."""
    f = c.future
    if f is None or not f.done():
        return False
    if f.cancelled():
        return c.tid is None
    exc = f.exception()
    return exc is None or isinstance(exc, TaskExecutionFailed) or never_sent(f)

def _retire_runner(app: AppCtx, rt: ShapeRuntime) -> None:
    """Close a shape's runner — and, for a billed shape, first hand the checks it sent that no worker has answered to
    the endpoint (`AppCtx.sent_checks`), with the terms they asked for and the pilot that died there: closing it drops
    only tasks never handed to the web service, so one still queued on a user endpoint can start a block there after
    this runtime is gone, and a stop must still see it. Callers hold app.lock."""
    runner = rt.runner
    if runner is None:
        return
    eid = getattr(runner, "endpoint_id", None)
    if _billable(rt) and eid is not None and _has_login_shape(app):  # (a facility endpoint's stop never looks)
        sent: list[tuple[Any, Any, float]] = getattr(runner, "sent_checks", list)()
        if sent:
            _keep_checks(app, eid, [SentCheck(future=f, client=client, terms=_runner_terms(runner, rt),
                                              dead=rt.dead_pilot, task_id=getattr(f, "task_id", None), sent_at=at,
                                              block=_sent_block(runner, f), taken_at=_taken_at(runner, f),
                                              taken_mono=_taken_at(runner, f, mono=True))
                                    for f, client, at in sent])
    runner.close()

# Past this many kept per endpoint, the ones KNOWN resolved (answered, failed by the service, never sent) go first,
# oldest first. One still unresolved is never dropped: a dropped one could make a later stop's `down` a lie.
_SENT_CHECKS_MAX = 64

def _keep_checks(app: AppCtx, eid: str, new: list[SentCheck]) -> None:
    """Add tasks to the endpoint's evidence (not twice), bound it without ever dropping an unresolved one, and write
    it to disk. Callers hold app.lock."""
    kept = app.sent_checks.setdefault(eid, [])
    known = {_check_key(c) for c in kept}
    for c in new:
        if _check_key(c) not in known:
            kept.append(c)
            known.add(_check_key(c))
    excess = len(kept) - _SENT_CHECKS_MAX
    if excess > 0:  # oldest first (kept is in arrival order), and only what is known resolved
        gone = {id(c) for c in [c for c in kept if _locally_resolved(c)][:excess]}
        kept[:] = [c for c in kept if id(c) not in gone]
    _persist_checks(app, eid)

def _persist_checks(app: AppCtx, eid: str) -> None:
    """Write the endpoint's unresolved tasks with a task id — the ones kept, and the current compute runner's — to disk,
    so a restarted server still sees them (state.SentCheckStore). The store MERGES: it adds these and removes only the
    tasks this process has seen resolve (the ones it wrote that are no longer unresolved here, and those its lookups
    resolved), so another process's rows on the same endpoint are kept. Written when that changed, or when the last
    write failed (said on stderr, and tried again on the next change). Only for an SSH endpoint: a facility endpoint's
    stop never looks. Callers hold app.lock."""
    if not _has_login_shape(app):
        return
    rt = app.shapes.get(DEFAULT_SHAPE)
    current = _current_checks(app, rt) if getattr(getattr(rt, "runner", None), "endpoint_id", None) == eid else []
    inflight = app.commands_sent.get(eid, [])
    inflight[:] = [c for c in inflight if not _locally_resolved(c)]  # finished: no longer on the record
    rows: dict[str, dict] = {}
    for c in [*app.sent_checks.get(eid, []), *current, *inflight]:
        key = _check_key(c)
        if c.future is None or _locally_resolved(c) or key in app.resolved_checks:
            continue  # (one read back from the record is on it already: the process that sent it removes it)
        # a task with no id yet (just submitted, or a submit that may have been taken) is written too, under a key of
        # its own: a server that restarts reads it as unknown (fail closed) — replaced by its id once the SDK has one
        rows[key] = {"task_id": key, "no_id": c.tid is None, "partition": c.terms[0], "account": c.terms[1],
                     "sent_at": c.sent_at, "what": c.what, "ceiling_at": c.ceiling_at, "block": c.block,
                     "taken_at": c.taken_at}
    mine = frozenset(rows)
    wrote, dropped = app.persisted_checks.get(eid, (frozenset(), frozenset()))
    drop = ((wrote | frozenset(app.resolved_checks)) - mine)
    if mine == wrote and drop <= dropped and eid not in app.checks_pending:
        return
    app.checks_pending[eid] = _RecordJob(list(rows.values()), mine, SentCheckStore().root)
    _schedule_flush(app, eid)

@dataclasses.dataclass(frozen=True)
class _RecordJob:
    """A write of an endpoint's record, as decided on the loop: its rows (None: the endpoint is gone — remove the
    record), their keys, and the record's directory AS IT WAS when the write was decided — never re-read in the worker,
    where the environment may have changed since (a later test, a later binding)."""
    rows: list[dict] | None
    mine: frozenset[str]
    root: Path

class _RecordThread:
    """The record's I/O runs on ONE thread of its own, never the loop's shared pool: a home directory that hangs (NFS)
    can hold that thread, but nothing else waits on it — reads are bounded (`_read_checks`), and the rest of
    hpc-bridge's blocking calls keep their pool. A DAEMON thread, started on first use: a write hung at exit never keeps
    the interpreter alive (the shutdown's flush is bounded)."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._jobs: queue.SimpleQueue = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._guard = threading.Lock()

    def submit(self, fn: Any, *args: Any) -> Future:
        fut: Future = Future()
        self._jobs.put((fut, fn, args))
        with self._guard:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
                self._thread.start()
        return fut

    def _run(self) -> None:
        while True:
            fut, fn, args = self._jobs.get()
            if not fut.set_running_or_notify_cancel():
                continue
            try:
                fut.set_result(fn(*args))
            except BaseException as exc:  # noqa: BLE001 - handed to the waiting coroutine
                fut.set_exception(exc)

_STORE_POOL = _RecordThread("hpc-bridge-record")

async def _on_record_thread(fn: Any, *args: Any) -> Any:
    return await asyncio.wrap_future(_STORE_POOL.submit(fn, *args))

def _schedule_flush(app: AppCtx, eid: str) -> None:
    """The file is written OFF the event loop (a thread), by one flusher per endpoint, latest state first: a lock
    service or a home directory that hangs (NFS) must never freeze the server. With no running loop (a synchronous
    caller), it is written here."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _write_pending(app, eid)
        return
    task = app.check_flushers.get(eid)
    if task is None or task.done():
        app.check_flushers[eid] = loop.create_task(_flush_checks(app, eid))

def _write_job(eid: str, job: _RecordJob, drop: frozenset[str]) -> bool:
    """One pending write, in the record's own directory as decided (`job.root`). Blocking."""
    store = SentCheckStore(job.root)
    if job.rows is None:
        store.remove(eid)
        return True
    return store.update(eid, job.rows, drop)

# A failed write is tried again by itself — 1 s, 2 s, 4 s … at most 60 s apart, 20 times — unless a newer one is
# pending by then (which replaces it): a change skipped while it was in flight must not wait for another change.
_RETRY_BASE_S = 1.0
_RETRY_MAX_S = 60.0
_RETRY_LIMIT = 20

def _done_job(app: AppCtx, eid: str, job: _RecordJob, drop: frozenset[str], ok: bool,
              before: tuple[frozenset[str], frozenset[str]] | None) -> None:
    if job.rows is None:
        return
    if ok:
        app.record_retries.pop(eid, None)
        return
    if before is None:  # not written: back to what is on the record
        app.persisted_checks.pop(eid, None)
    else:
        app.persisted_checks[eid] = before
    n = app.record_retries.get(eid, 0) + 1
    app.record_retries[eid] = n
    delay = min(_RETRY_MAX_S, _RETRY_BASE_S * 2 ** (n - 1))
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None or n > _RETRY_LIMIT:
        print(f"hpc-bridge: could not write the record of checks sent to endpoint {eid} (under {job.root}); tried "
              "again on the next change", file=sys.stderr)
        return
    print(f"hpc-bridge: could not write the record of checks sent to endpoint {eid} (under {job.root}); trying "
          f"again in {delay:g} s", file=sys.stderr)
    loop.call_later(delay, _retry_job, app, eid, job)

def _retry_job(app: AppCtx, eid: str, job: _RecordJob) -> None:
    if eid not in app.checks_pending:  # a newer write pending replaces this one
        app.checks_pending[eid] = job
    _schedule_flush(app, eid)

def _take_job(app: AppCtx, eid: str) -> tuple[_RecordJob, frozenset[str], tuple[frozenset[str], frozenset[str]] | None]:
    """The next write, and its removals computed NOW — against what the writes before it put on the record — so a row
    an earlier write added is removed by a later one that no longer has it, however the two were queued. What it
    writes counts as written from now (`persisted_checks`), so a change while it is in flight is not queued twice;
    a write that fails puts back what was there before (`_done_job`)."""
    job = app.checks_pending.pop(eid)
    before = app.persisted_checks.get(eid)
    wrote = before[0] if before is not None else frozenset()
    drop = (wrote | frozenset(app.resolved_checks)) - job.mine
    if job.rows is not None:
        app.persisted_checks[eid] = (job.mine, drop)
    return job, drop, before

def _write_pending(app: AppCtx, eid: str) -> None:
    while eid in app.checks_pending:
        job, drop, before = _take_job(app, eid)
        _done_job(app, eid, job, drop, _write_job(eid, job, drop), before)

async def _flush_checks(app: AppCtx, eid: str) -> None:
    """Write the endpoint's pending record state, on the record's thread, until none is left (a newer one replaces an
    older)."""
    while eid in app.checks_pending:
        job, drop, before = _take_job(app, eid)
        _done_job(app, eid, job, drop, await _on_record_thread(_write_job, eid, job, drop), before)

async def _flush_all(app: AppCtx, timeout: float = 10.0) -> None:
    """Wait — at most `timeout` — for every pending write of the record: at a teardown's end and the server's shutdown
    (and in tests)."""
    tasks = [t for t in app.check_flushers.values() if not t.done()]
    if tasks:
        await asyncio.wait(tasks, timeout=timeout)

async def _read_checks(eid: str, timeout: float = 5.0) -> tuple[list[dict], str | None]:
    """The record on disk, read in a thread, within `timeout`: out of time, nothing is known — fail closed."""
    try:
        return await asyncio.wait_for(_on_record_thread(SentCheckStore().read, eid), timeout=max(timeout, 0.1))
    except TimeoutError:
        return [], "it could not be read in time"

# Resolutions seen this session, by task id: never asked again (AppCtx.resolved_checks), bounded — oldest out first.
_RESOLVED_MAX = 4096

def _trim_resolved(app: AppCtx) -> None:
    excess = len(app.resolved_checks) - _RESOLVED_MAX
    for tid in list(app.resolved_checks)[:max(0, excess)]:
        app.resolved_checks.pop(tid, None)

def _forget_checks(app: AppCtx, eid: str) -> None:
    """The endpoint is gone (a teardown): nothing of it can start a block any more — its record goes too (written off
    the loop, like every write). Callers hold app.lock."""
    app.sent_checks.pop(eid, None)
    app.persisted_checks.pop(eid, None)
    app.checks_pending[eid] = _RecordJob(None, frozenset(), SentCheckStore().root)
    _schedule_flush(app, eid)

def _current_checks(app: AppCtx, rt: ShapeRuntime | None) -> list[SentCheck]:
    """The current compute runner's sent, unanswered checks, as evidence items."""
    if rt is None or rt.runner is None or not _billable(rt):
        return []
    return [SentCheck(future=f, client=client, terms=_runner_terms(rt.runner, rt), dead=rt.dead_pilot,
                      task_id=getattr(f, "task_id", None), sent_at=at, block=_sent_block(rt.runner, f),
                      taken_at=_taken_at(rt.runner, f), taken_mono=_taken_at(rt.runner, f, mono=True))
            for f, client, at in getattr(rt.runner, "sent_checks", list)()]


def _sent_block(runner: object, fut: Any) -> int | None:
    """The block request a runner's check was sent for (GlobusRunner.sent_block, read at its send), if it is one."""
    get = getattr(runner, "sent_block", None)
    return _block_number(get(fut)) if callable(get) else None

def _taken_at(runner: object, fut: Any, *, mono: bool = False) -> float | None:
    """When this process first saw a runner's check's task id (GlobusRunner.taken_at, or `taken_mono` on the monotonic
    clock), if it has."""
    get = getattr(runner, "taken_mono" if mono else "taken_at", None)
    at = get(fut) if callable(get) else None
    return float(at) if isinstance(at, (int, float)) and not isinstance(at, bool) else None

def _note_checks_sent(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """A check went out (called at its submit, and again once its wait ends — cancelled too): write the endpoint's
    evidence, the current runner's included, to disk — a server that restarts before this runner is retired must still
    see it — and watch for its task id. Callers hold app.lock."""
    eid = app.state.endpoint_id
    if eid is not None and shape == DEFAULT_SHAPE and _has_login_shape(app):
        _persist_checks(app, eid)
        _watch_ids(app, eid)

def _note_command_sent(app: AppCtx, shape: str, rt: ShapeRuntime, fut: Any, client: Any, ceiling_s: float) -> None:
    """A command went to the compute block: on the record until it finishes, so a server that restarts while it runs
    still sees it (its stop then asks the service, and refuses while it is running). Only for a billed shape on an SSH
    endpoint."""
    eid = app.state.endpoint_id
    if eid is None or shape != DEFAULT_SHAPE or not _billable(rt) or not _has_login_shape(app):
        return
    at = time.time()
    app.commands_sent.setdefault(eid, []).append(
        SentCheck(future=fut, client=client, terms=_terms_of(rt), task_id=None, sent_at=at, what="command",
                  ceiling_at=at + ceiling_s + _LOST_TASK_GRACE_S))
    _persist_checks(app, eid)
    _watch_ids(app, eid)

def _note_command_done(app: AppCtx, fut: Any) -> None:
    """A command's result arrived (or was given up): off the record."""
    eid = app.state.endpoint_id
    if eid is not None and eid in app.commands_sent:
        app.commands_sent[eid] = [c for c in app.commands_sent[eid] if c.future is not fut]
        _persist_checks(app, eid)

def _watch_ids(app: AppCtx, eid: str) -> None:
    """Write a task's id to the record as soon as the SDK has it (it arrives from the Executor's submit thread, after
    the submit call returned): looked for every 50 ms, then less often, for two minutes at most."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = app.id_watchers.get(eid)
    if task is None or task.done():
        app.id_watchers[eid] = loop.create_task(_watch_ids_loop(app, eid))

async def _watch_ids_loop(app: AppCtx, eid: str) -> None:
    delay, give_up = 0.05, time.monotonic() + 120.0
    while time.monotonic() < give_up:
        await asyncio.sleep(delay)
        delay = min(delay * 2, 2.0)
        rt = app.shapes.get(DEFAULT_SHAPE)
        waiting = [c for c in [*_current_checks(app, rt), *app.commands_sent.get(eid, [])]
                   if c.tid is None and c.future is not None and not c.future.done()]
        _persist_checks(app, eid)
        if not waiting:
            return

def _our_checks(app: AppCtx, eid: str, rt: ShapeRuntime | None,
                disk: tuple[list[dict], str | None]) -> tuple[list[SentCheck], str | None]:
    """Every task of ours a stop must see resolve before `down`: the ones the endpoint kept (from retired runners, lost
    commands, and — after a restart or from another process — read back from disk by task id), and the ones the current
    compute runner sent; and why the record on disk cannot be trusted to list them all, if it cannot (it could not be
    read: fail closed). `disk`: the record as `_read_checks` read it (off the loop). Callers hold app.lock."""
    kept = app.sent_checks.setdefault(eid, [])
    current = _current_checks(app, rt)
    known = {_check_key(c) for c in [*kept, *current]}
    rows, problem = disk
    wrote = app.persisted_checks.get(eid, (frozenset(), frozenset()))[0]
    for row in rows:  # a restart (or another process) left them: adopt what is not known, nor known resolved
        if row["task_id"] in wrote:  # this process wrote it: what it no longer holds has resolved
            continue
        if row["task_id"] not in known and row["task_id"] not in app.resolved_checks:
            ceiling = row.get("ceiling_at")
            no_id = row.get("no_id") is True  # written at the submit, before the service answered with an id
            kept.append(SentCheck(future=None, terms=(row.get("partition"), row.get("account")),
                                  task_id=None if no_id else row["task_id"],
                                  row_key=row["task_id"] if no_id else None,
                                  sent_at=float(row.get("sent_at") or 0.0),
                                  what="command" if row.get("what") == "command" else "check",
                                  ceiling_at=float(ceiling) if isinstance(ceiling, (int, float)) else None,
                                  block=_block_number(row.get("block")),
                                  taken_at=(float(taken) if isinstance(taken := row.get("taken_at"), (int, float))
                                            and not isinstance(taken, bool) else None)))
            known.add(row["task_id"])
    if not kept:
        app.sent_checks.pop(eid, None)
    in_kept = {_check_key(c) for c in kept}
    return [*kept, *[c for c in current if _check_key(c) not in in_kept]], problem

def _unresolved_count(app: AppCtx, eid: str | None, disk: tuple[list[dict], str | None]) -> int:
    """How many tasks of ours an endpoint may still hold, without asking the service: the ones kept in memory not known
    resolved, plus those on disk (`disk`, as `_read_checks` read it) not among them — and one more when the record
    cannot be trusted. Callers hold app.lock."""
    if eid is None:
        return 0
    kept = app.sent_checks.get(eid, [])
    known = {_check_key(c) for c in kept}
    rows, problem = disk
    return (sum(1 for c in kept if not _locally_resolved(c)) + (1 if problem else 0)
            + sum(1 for row in rows if row["task_id"] not in known and row["task_id"] not in app.resolved_checks))

def _keep_lost_command(app: AppCtx, shape: str, rt: ShapeRuntime | None, fut: Any, client: Any, *,
                       ceiling_s: float | None = None, sent_at: float | None = None) -> None:
    """A command whose future FAILED on the client's side alone — not the service's verdict (TaskExecutionFailed), on
    a task the web service registered (a task id): e.g. "SDK thread quit unexpectedly.", which fails every open future
    while the tasks live on. Its handle is gone, but the endpoint may still run it — or start a block for it after a
    stop — so it is kept as evidence like a check. Only for a billed shape on an SSH endpoint. Callers hold app.lock."""
    eid = app.state.endpoint_id
    if eid is None or rt is None or not _billable(rt) or not _has_login_shape(app) or shape != DEFAULT_SHAPE:
        return
    if not fut.done() or fut.cancelled() or getattr(fut, "task_id", None) is None:
        return
    exc = fut.exception()
    if exc is None or isinstance(exc, TaskExecutionFailed):
        return
    at = sent_at if sent_at is not None else time.time()
    # past its ceiling (it is killed at its walltime once running) plus the grace a queued one gets, it can no longer
    # make a stop refuse: it still counts as evidence while the service says it is queued
    ceiling = at + ceiling_s + _LOST_TASK_GRACE_S if ceiling_s is not None else None
    if eid in app.commands_sent:
        app.commands_sent[eid] = [c for c in app.commands_sent[eid] if c.future is not fut]
    _keep_checks(app, eid, [SentCheck(future=fut, client=client, terms=_terms_of(rt), task_id=str(fut.task_id),
                                      sent_at=at, what="command", ceiling_at=ceiling)])

# `lost`: the service still says pending, but the check's own compute user endpoint is gone for good — it went into
# bad state after the check was sent and shut down, and its report of the failure never reached the service (a
# globus-compute-endpoint race): it can never run, nor start a block (server._settle_by_evidence). Only a stop sets it.
CheckState = Literal["ok", "failed", "pending", "unknown", "unsent", "lost"]

# A task whose submit failed in a way that leaves it unclear whether the web service took it (no task id: a timeout, a
# 5xx, a malformed answer) reads `unknown` for this long after it was sent, then is dropped with a line on stderr — an
# entry with no id can never be looked up. Taken, it reaches its user endpoint within seconds; a fresh user endpoint
# scales out on its first 5 s pass, and once its first pilot is cancelled or dies parsl gives the task up at its next
# 60 s status poll (bad state). 300 s covers the user endpoint's start (tens of seconds), that poll and pass, and the
# failure reaching the service, with margin. Only where that holds — an endpoint that gives each block request its own
# compute user endpoint (`drop_no_id`): on one of an earlier plugin, which keeps one user endpoint, nothing gives such
# a task up, so it stays `unknown` (that endpoint's stop names teardown_endpoint as the only way to end it anyway).
_NO_ID_BOUND_S = 300.0
_DROPPED_NO_ID: set[int] = set()

def _local_state(c: SentCheck, now: float, *, drop_no_id: bool = True) -> CheckState | None:
    """What a task's own future says, or None when the service must be asked (by task id). `drop_no_id`: an id-less task
    whose submit may have been taken is dropped after `_NO_ID_BOUND_S` (a fresh-user-endpoint endpoint only)."""
    f = c.future
    if f is not None and f.done() and not f.cancelled():
        exc = f.exception()
        if exc is None:
            return "ok"
        if isinstance(exc, TaskExecutionFailed):
            return "failed"  # the service's verdict, delivered to the future
    if c.tid is not None:
        return None
    if f is None and c.row_key is not None:  # a submit's row read back with no id: whether it was taken is unknown
        if not drop_no_id or now - c.sent_at < _NO_ID_BOUND_S:
            return "unknown"
        return "unsent"
    if f is None or never_sent(f):
        return "unsent"
    if not f.done():
        return "pending"  # still being submitted
    if not drop_no_id or now - c.sent_at < _NO_ID_BOUND_S:
        return "unknown"  # the submit failed after the service may have taken it
    if id(f) not in _DROPPED_NO_ID:
        _DROPPED_NO_ID.add(id(f))
        print(f"hpc-bridge: a {c.what} whose submit failed without a task id ({type(f.exception()).__name__}) is no "
              f"longer counted by stop_endpoint, {int(now - c.sent_at)} s after it was sent", file=sys.stderr)
    return "unsent"

def _status_state(task: object) -> CheckState:
    """A task dict as the SDK returns it (`{"pending", "status", …}`): pending while `pending` is not False; then
    `success` (with a result, or with none: it finished) is `ok`, anything else `failed`."""
    if not isinstance(task, dict):
        return "unknown"
    if task.get("pending", True) is not False:
        return "pending"
    return "ok" if str(task.get("status", "")).lower() == "success" else "failed"

def _task_state(client: Any, tid: str) -> CheckState:
    """One task, by id (`Client.get_task`). SDK 4.18 RAISES `TaskExecutionFailed` for a task the endpoint failed with an
    exception — a check failed by bad state, a REJECTED submission, a dead first pilot: that is the service's verdict,
    `failed`. A `SerdeError` arises only for a finished task's result this process cannot read: it finished, `ok`.
    Anything else (transport, auth, an unexpected answer) says nothing: `unknown`, read as still queued."""
    try:
        task = client.get_task(tid)
    except TaskExecutionFailed:
        return "failed"
    except SerdeError:
        return "ok"
    except Exception:  # noqa: BLE001 - a lookup that fails says nothing: the honest side is "still queued"
        return "unknown"
    return _status_state(task)

# The most tasks one round asks about one by one (after the batch call), per client: the SDK serialises its requests
# (Client._request_lock), at 0.2-0.5 s each, so 32 fit in a round's time. The rest are asked in the next round — a
# resolution is never asked again (AppCtx.resolved_checks), so each round makes progress.
_MAX_SINGLE_LOOKUPS = 32

def _lookup_into(client: Any, tids: list[str], out: dict[str, CheckState], deadline: float,
                 resolved: dict[str, str] | None = None) -> None:
    """Several tasks of one client, written into `out` AS THEY ARE READ — so an answer read before the round's deadline
    is kept whatever happens after: one batch call where the SDK can (`get_batch_result`), then, for a task it leaves
    out (it drops one the service failed with an exception, logging it), `get_task` alone, at most
    `_MAX_SINGLE_LOOKUPS` of them and none once `deadline` (monotonic) has passed — so a round outliving its wait
    leaves at most one request of ours holding the SDK's lock. `resolved`: where each resolution goes too, as it is
    read — one that arrives after the round's caller stopped waiting is still never asked again. Blocking: run it off
    the loop."""
    if client is None:
        return

    def put(t: str, st: CheckState) -> None:
        out[t] = st
        if resolved is not None and st in ("ok", "failed"):
            resolved[t] = st

    batch = getattr(client, "get_batch_result", None)
    if callable(batch) and len(tids) > 1:
        try:
            got = batch(list(tids))
        except Exception:  # noqa: BLE001 - transport / auth: nothing to read for any of them this round
            return
        if isinstance(got, dict):
            for t in tids:
                if t in got:
                    put(t, _status_state(got[t]))
    asked = 0
    for t in tids:
        if t in out:
            continue
        if asked >= _MAX_SINGLE_LOOKUPS or time.monotonic() >= deadline:
            return
        asked += 1
        put(t, _task_state(client, t))

def _lookup_states(client: Any, tids: list[str]) -> dict[str, CheckState]:
    """`_lookup_into`, whole: every task's state (`unknown` for one not read)."""
    out: dict[str, CheckState] = {}
    _lookup_into(client, tids, out, time.monotonic() + _LOOKUP_TIMEOUT_S)
    return {t: out.get(t, "unknown") for t in tids}

# The most one round of lookups may take (a web call per client; the SDK's own timeouts are longer).
_LOOKUP_TIMEOUT_S = 20.0

async def _check_states(checks: list[SentCheck], fallback: Any = None, timeout: float = _LOOKUP_TIMEOUT_S, *,
                        known: dict[str, str] | None = None, drop_no_id: bool = True) -> list[CheckState]:
    """Where each task of ours stands: `ok` (it ran), `failed` (the endpoint gave it up: a rejection, a bad state after
    its first pilot died or was cancelled), `pending` (still queued: the endpoint can start a block for it), `unsent`
    (the service never took it, or an id-less one past `_NO_ID_BOUND_S` where `drop_no_id`), or `unknown` (it could not
    be read: read as pending). First each task's own future, then `known` (resolutions seen before: never asked again),
    then the service by task id — batched per client (`fallback`: the client for a task read back from disk), all
    within `timeout`; an answer read in that time is kept, one not read is `unknown`. Resolutions found are added to
    `known`."""
    now = time.time()
    states: list[CheckState | None] = [_local_state(c, now, drop_no_id=drop_no_id) for c in checks]
    if known is not None:
        states = [st if st is not None or c.tid not in known else known[c.tid]  # type: ignore[misc]
                  for c, st in zip(checks, states, strict=True)]
    groups: dict[int, tuple[Any, list[str]]] = {}
    for c, st in zip(checks, states, strict=True):
        if st is None and c.tid is not None:
            client = c.client if c.client is not None else fallback
            groups.setdefault(id(client), (client, []))[1].append(c.tid)
    found: dict[str, CheckState] = {}
    if groups:
        deadline = time.monotonic() + max(timeout, 0.01)
        tasks = [asyncio.ensure_future(asyncio.to_thread(_lookup_into, cl, list(dict.fromkeys(ts)), found, deadline,
                                                         known))
                 for cl, ts in groups.values()]
        await asyncio.wait(tasks, timeout=max(timeout, 0.01))  # what arrived in time is kept; the rest stay unknown
    out = [st if st is not None else found.get(c.tid or "", "unknown") for c, st in zip(checks, states, strict=True)]
    if known is not None:
        for c, st in zip(checks, out, strict=True):
            if c.tid is not None and st in ("ok", "failed"):
                known[c.tid] = st
    return out

def _settle_checks(app: AppCtx, eid: str, checks: list[SentCheck], states: list[CheckState]) -> None:
    """After a stop looked: the endpoint keeps only the tasks still queued (or unknown), on disk too. Callers hold
    app.lock."""
    done = ("ok", "failed", "unsent", "lost")
    resolved = {_check_key(c) for c, s in zip(checks, states, strict=True) if s in done}
    for c, st in zip(checks, states, strict=True):  # removed from the record by this process (it saw them resolve)
        if st in done and (c.tid or c.row_key):
            app.resolved_checks[_check_key(c)] = st
    kept = [c for c in app.sent_checks.get(eid, []) if _check_key(c) not in resolved]
    if kept:
        app.sent_checks[eid] = kept
    else:
        app.sent_checks.pop(eid, None)
    _persist_checks(app, eid)
    _trim_resolved(app)

def _fresh_block(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """A confirmation that asks for a NEW block gets its own compute user endpoint: a fresh `hpcb_block` token in the
    config, which the web service keys user endpoints on, so this block's pilot is that endpoint's first. A first
    pilot cancelled by a stop, or dead before it connects, reads MISSING to parsl, which then fails our queued check
    (bad state) instead of starting another block for it; a later pilot of an old user endpoint does not (live
    2026-10-16, `queued_check_relaunch`). Only where it is safe and ours: an endpoint hpc-bridge stood up whose template
    lets an idle compute user endpoint exit (`fresh_ueps`; a facility's own endpoint keeps its template, and one of an
    earlier plugin would keep every such endpoint running), a billed shape, and no block of ours up on it — a
    confirmation over a block already answering, or with our work on it, must not move to a new user endpoint and
    abandon that block. The runner is rebuilt for the new config. Callers hold app.lock."""
    if not (getattr(app.facility, "fresh_ueps", False) is True and _has_login_shape(app) and _billable(rt)):
        return
    if rt.block_since is not None or rt.inflight or _live_task_handles(app, shape):
        return
    # a whole number (digits only once the template renders it `| int`, whatever a config carries): the user endpoint's
    # own log names it from its start (the template's endpoint_setup line), which is how a stop finds that endpoint
    rt.user_endpoint_config["hpcb_block"] = secrets.randbelow(_BLOCK_MAX - 1) + 1
    rt.runner_stale = True

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
        app.compute_choice["partition"] = partition  # named, though unchanged: the session's choice now (its source)
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
        app.compute_choice["account"] = account  # named, though unchanged: the session's choice now (its source)
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
    if _has_login_shape(app):  # an SSH endpoint: a block idles out — banked to that latest end, not named
        _leave_block(app, shape, rt, how="switch", remember=False)
    elif (left := _leave_switched(app, shape, rt)) is not None:  # a facility endpoint: it bills on, and is named
        rt.switch_left = left
    if confirmed:
        rt.spend_confirmed = False
        rt.switched_terms = terms
        # what the old terms' block does now (notices._switch_fate): a block that came up idle-releases after its last
        # task; one that never did may sit HELD, which never starts or idle-releases
        rt.switch_left_up, rt.switch_left_pilots = _came_up(rt), rt.pilots
    elif rt.reaped is not None and rt.reap_terms is None:  # spend is asked anyway (a reap): remember the reap's terms
        rt.reap_terms = terms
    # A check of the old terms still queued may have asked their user endpoint for a block that can still land: the
    # old runner is retired NOW, before the terms change, and the endpoint keeps its checks (`_retire_runner`) — with
    # the old terms and the pilot that died there, if one did — so a stop still sees, cancels and names them. (A switch
    # is refused while our work is on the block, so no task of ours is cut off by it.)
    if _has_login_shape(app):
        _retire_runner(app, rt)
        rt.runner = None
    _bank_warm_interval(rt, app)  # (a no-op once the block was left)
    uec[key] = value
    app.compute_choice[key] = value
    rt.runner_stale = True
    rt.warm_confirmed_at = rt.last_proof_at = rt.clock_stopped_at = None
    rt.block_since = rt.block_job = None  # a different partition or account is a different block: not the old one's age
    rt.pilots = None  # the new terms' block request gets a probe of its own
    _forget_presumed(rt)
    rt.block_epoch += 1

def _settle_refusals(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """Before a switch lets go of the block: a finished command of this runtime that the wrong-block guard refused, not
    polled yet, found a replacement block — take that in now, as its poll would have (`_wrong_block_outcome`: a kicked
    reap), so the switch leaves the replacement billed and named, not the block that is gone. The handle stays: its
    poll still says the command did not run. A reap found here was told to no one: the calling call answers
    needs_confirmation with it (`reap_told` left False), as any call that finds a reap does."""
    for h in list(app.tasks.values()):
        if (h.shape == shape and h.guard is not None and h.future.done() and not h.future.cancelled()
                and (h.runtime is None or h.runtime is rt)
                and (refused := _guard_refusal(_finished_outcome(h), h.guard)) is not None):
            was = rt.reaped
            _wrong_block_outcome(app, shape, rt, h.guard, refused, reset=h.reset, at=h.done_at, epoch=h.epoch)
            if was is None and rt.reaped is not None:
                rt.reap_told = False

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
    only ever NAMED again (a later stop, a teardown, a re-bind note), never billed or dated. A refused command not yet
    polled is taken in first (`_settle_refusals`): the block left is then the replacement it found. Callers hold
    app.lock."""
    _settle_refusals(app, shape, rt)
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
    _retire_runner(app, rt)
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
    _retire_runner(app, compute)  # its checks still queued stay with the endpoint, for the stop that follows
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
    # its future failed on the client's side alone (not the service's verdict): the task may live on — evidence
    runner = getattr(handle.runtime, "runner", None)
    _keep_lost_command(app, handle.shape, handle.runtime, fut, getattr(runner, "task_client", lambda: None)(),
                       ceiling_s=handle.ceiling_s, sent_at=time.time() - (time.monotonic() - handle.submitted_at))
    _note_command_done(app, fut)
    if handle.guard is not None and (refused := _guard_refusal(out, handle.guard)) is not None:
        # refused on a replacement block, however late it is polled (a client-cancelled call's command included)
        return _wrong_block_outcome(app, handle.shape, handle.runtime, handle.guard, refused, reset=handle.reset,
                                    at=handle.done_at, epoch=handle.epoch)
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
