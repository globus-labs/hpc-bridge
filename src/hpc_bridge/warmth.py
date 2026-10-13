"""The warmth state machine and task-handle bookkeeping (split steps 7–8, 2026-09-03).

Per resource shape: build/reuse the Executor (`_runner_for`), prove a worker is live with the canary
(`_confirm_worker`), gate a billed block behind the spend floor (`_provision`, `_apply_partition`,
`_apply_account`), record the sticky no-account verdict and the transient-conflict count, and drop
shapes (`_drop_compute_shape`, `_drop_all_shapes`, `_forget_identity_verdicts`). Task handles — a command
still running past the sync-wait — are registered, resolved and drained here too, because the runner
rebuild and the canary consult them (a live task IS warmth), which is why steps 7 and 8 ship together.

Every function here runs under `app.lock` held by the caller in `server` — except `_drop_compute_shape`, which
takes the lock itself, and `_endpoint_gone`, a web call deliberately made OFF the lock (don't add one inside
either). Tests patch
`warmth._provision` / `warmth._drop_compute_shape`; `server` calls those two through the module and
re-exports every name for imports.
"""
from __future__ import annotations

import re
import time

from . import dispatch
from .config import CANARY_TIMEOUT_S, CANARY_TTL_S, SYNC_WAIT_S, TASK_CEILING_MARGIN_S, _parse_hhmmss, _task_ceiling_s
from .context import (
    DEFAULT_SHAPE,
    AppCtx,
    ShapeRuntime,
    TaskHandle,
    _has_login_shape,
    _idle_release_s,
    _supported_shapes,
)
from .cost import _bank_warm_interval, _billable, _settle_billing, _total_session_spend, _with_spend
from .lifecycle import BlockState, EndpointState, ProvisionResult, ensure_warm
from .models import ShellOutcome
from .notices import _no_account_failure, _transient_dispatch_failure
from .runner import GlobusRunner
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
        rt = ShapeRuntime(user_endpoint_config=uec)
        app.shapes[shape] = rt
    return rt

def _live_task_handles(app: AppCtx, shape: str) -> list[tuple[str, TaskHandle]]:
    """(task_id, handle) for this shape whose task is still RUNNING (future not yet done) — i.e. still
    holding the block. The warmth signal and the swap/session-busy guards all key off this."""
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
        if rt.runner is not None and rt.runner.endpoint_id == eid and _live_task_handles(app, shape):
            # A credential/config-only swap (runner_stale, e.g. a new Globus login) must WAIT: closing
            # this Executor would drop the live task's future and poll_task would report "no task"
            # (found in review). The rebuild happens once the task has been polled.
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
        rt.warm_confirmed_at = None
    return rt.runner

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
    # A block confirmed warm and not since found gone (block_since survives a failed dispatch and a runner rebuild,
    # both of which void warm_confirmed_at) — read BEFORE _runner_for, which voids it on a rebuild.
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
    result = await runner.canary(timeout=CANARY_TIMEOUT_S)
    rt.last_canary = result  # keep failures too: the error text is the diagnosis the caller needs
    # A canary can land on any of the shape's blocks; only with one block at most (max_blocks 1: every template and
    # registry entry we know, and a facility template's own default when its schema drops the key) does a different
    # scheduler job mean the confirmed block was replaced.
    single_block = rt.user_endpoint_config.get("max_blocks") in (None, 1, "1")
    if (result.ok and single_block and was_warm and _billable(rt) and rt.spend_confirmed and rt.block_job is not None
            and result.worker_job is not None and result.worker_job != rt.block_job):
        # A DIFFERENT scheduler job answered while this block was believed up: the block is gone (idle-released,
        # cancelled, failed, or preempted — though a preempted job REQUEUED keeps its id, which only a timeout can
        # then reveal) and this check, a task, had the endpoint start a new one without the user's confirmation.
        # With the 5 s scaling pass that block can answer inside CANARY_TIMEOUT_S, so a timeout alone no longer
        # catches it. A reap found by a check, left exactly like the timeout's below: the answering block is not
        # adopted (no age, no job, no spend clock) until the user confirms; the confirm's canary then dates it.
        _mark_reaped(app, rt, f"the previous block (job {rt.block_job}) is gone — idle-released, cancelled, "
                     f"preempted or failed — and a check was answered by a new block (job {result.worker_job}) "
                     f"that the check had the facility start: {_kicked_release_note(app)}",
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
        _mark_reaped(app, rt, f"the previous block did not answer a check within {CANARY_TIMEOUT_S:g} s — most "
                     "likely cancelled, preempted or failed. That check may already have asked the facility for a "
                     f"new block: {_kicked_release_note(app)}", _release_bound(app, shape, rt), kicked=True)
    rt.warm_confirmed_at = None
    rt.block_since = rt.block_job = None
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

def _drop_all_shapes(app: AppCtx, *, bank: bool) -> float:
    """Forget every task handle, close every shape's runner, and unbind the endpoint. With `bank`, fold
    each shape's running warm interval into the spend FIRST and return the session total as it stood —
    the four inline copies of this block disagreed, and the connect re-bind's copy silently dropped a
    warm block's interval (found in review). A block the clock presumes released is banked to that time, not to
    now. Callers hold app.lock."""
    untils = {name: _presumed_release_at(app, name, rt) for name, rt in app.shapes.items()} if bank else {}
    app.tasks.clear()
    for name, rt in app.shapes.items():
        if bank:
            _bank_warm_interval(rt, app, until=untils.get(name))
        if rt.runner is not None:
            rt.runner.close()
    spent = _total_session_spend(app) if bank else 0.0
    app.shapes.clear()
    app.released_spend = 0.0  # the binding's released blocks are in `spent`; a new binding starts from zero
    app.state = EndpointState()
    return spent

def _note_dispatch(rt: ShapeRuntime, out: ShellOutcome, *, at: float | None = None) -> None:
    """A real result — or a task still running — is the strongest liveness proof, so refresh the canary
    TTL. A dispatch FAILURE (transport timeout/error) means the worker may be gone, so void the
    confirmation to force a re-canary. A completed exit-124 is the worker ENFORCING the task ceiling
    (it answered — it's alive), so it no longer voids (the old timeout==124 heuristic is obsolete now
    that a slow task returns a poll handle, not a 124 failure)."""
    if out.phase in ("complete", "running"):
        # `at`: when a polled task actually finished — the block's last proof of life, not the poll's time
        seen = time.monotonic() if at is None else at
        rt.warm_confirmed_at = seen if rt.warm_confirmed_at is None else max(rt.warm_confirmed_at, seen)
        rt.transient_conflicts = 0
    elif out.phase == "failed":
        rt.warm_confirmed_at = None

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
    """The block's last proof of work: its last confirmation (warm_confirmed_at, voided by a failed dispatch) or the
    end of a task nobody has polled yet (`done_at`), whichever is later. One source for the presumption and the
    billing bound — the bound used to read the confirmation alone, and billed a block short after a long task."""
    stamps = [t for t in (rt.warm_confirmed_at, *(h.done_at for h in app.tasks.values() if h.shape == shape))
              if t is not None]
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
    rt.warm_confirmed_at = None
    rt.block_since = rt.block_job = None
    rt.provisioning_since = None
    rt.spend_confirmed = False
    rt.reaped, rt.reap_told, rt.reap_kicked = why, False, kicked

def _check_reaped(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """Before anything voids the evidence (a provision, a partition or account switch): if the clock says the block
    the spend acknowledgement covered is gone, record the reap."""
    if _billable(rt) and rt.spend_confirmed and rt.reaped is None:
        idle_gone = _presumed_idle_released(app, shape, rt)
        gone = idle_gone or _presumed_past_walltime(app, shape, rt)
        if gone is not None:
            if idle_gone is not None:  # it may still be up: remember which block it was, its age, and its latest end
                rt.presumed_block_since, rt.presumed_block_job = rt.block_since, rt.block_job
                rt.presumed_block_until = idle_gone[1]
                # ...and where the reap below stops its spend clock, so the same block can resume it from there
                rt.presumed_billed_until = min(time.monotonic(), idle_gone[1]) if rt.warm_since is not None else None
            _mark_reaped(app, rt, *gone)

def _forget_presumed(rt: ShapeRuntime) -> None:
    rt.presumed_block_since = rt.presumed_block_job = rt.presumed_block_until = rt.presumed_billed_until = None

def _answering_block_since(rt: ShapeRuntime, job: str | None, now: float) -> float:
    """When the block that has just answered started, for its walltime. After an idle presumption, which can come while
    the block is still up (on a slower scaling pass), the answer is dated by the scheduler job id: the same job is the
    presumed block and keeps its older age; a different job is a new block, aged from now. With either id unknown (not
    expected: every Slurm or PBS block has one), a block answering before the presumed one's latest possible end keeps
    the older age, so its walltime is presumed early (a question) rather than missed."""
    old, old_job, until = rt.presumed_block_since, rt.presumed_block_job, rt.presumed_block_until
    if old is None:
        return now
    if job is not None and old_job is not None:
        return old if job == old_job else now
    return old if until is not None and now < until else now

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
        # so it read "allocating nodes…" for five minutes. The flag was stored and never enforced.
        if getattr(app.facility, "account_required", False) and not rt.user_endpoint_config.get("account"):
            return "needs_account"  # spend stays unconfirmed: the re-call with account= re-gates cleanly
        rt.spend_confirmed = True  # ack persists for this block (cleared when it is reaped)
        rt.reaped, rt.reap_told, rt.reap_kicked = None, False, False
    if app.state.endpoint_id is None:
        bootstrap = getattr(app.facility, "bootstrap", None)
        if bootstrap is not None:
            handle = await bootstrap(app.profile)
            app.state = EndpointState(endpoint_id=handle.endpoint_id, reused=handle.reused)
    block, app.state = await ensure_warm(app.facility, app.profile, app.state)
    if block == "warm":  # manager online -> confirm a worker is actually live
        block = await _confirm_worker(app, shape, force=force_canary)
    if block == "warm" and _billable(rt) and not rt.spend_confirmed:
        # Defensive invariant: a billed block is dated and billed only once spend is confirmed for it. Today every
        # path in _confirm_worker that voids the confirmation (its reaps) already answers "provisioning"; this keeps a
        # future path from adopting a block the user has not confirmed.
        block = "provisioning"
    _settle_billing(rt, app, block)
    if block == "warm" and rt.block_since is None:  # a block first confirmed warm: its age (for its walltime) and job
        job = rt.last_canary.worker_job if rt.last_canary is not None and rt.last_canary.ok else None
        since = _answering_block_since(rt, job, time.monotonic())
        if (rt.presumed_block_since is not None and since == rt.presumed_block_since
                and rt.presumed_billed_until is not None):
            rt.warm_since = rt.presumed_billed_until  # the presumed block lived on: its spend clock continues
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
    login shape, which has no partition. A real change means a different scheduler block,
    so we mark the cached runner stale (its Executor captured the old partition at build time —
    _runner_for rebuilds it and banks the prior warm interval) and drop the warm confirmation;
    the old block idle-releases on its own (min_blocks=0). The selection persists in
    user_endpoint_config for the rest of the session."""
    if partition is None or not rt.user_endpoint_config.get("compute"):
        return None
    if rt.user_endpoint_config.get("partition") == partition:
        return None
    live = _live_task_handles(app, shape)
    if live:
        return (f"can't change partition to {partition!r}: a task is still running "
                f"(task_id={live[0][0]!r}) on shape {shape!r}. poll_task it or stop_endpoint first.")
    # a command inside its sync-wait: the swap would orphan it, and its count would vouch for the new block
    if rt.inflight:
        return (f"can't change partition to {partition!r}: a command is still running on shape {shape!r} "
                "(inside run_shell's wait). Let it return, then change it.")
    _check_reaped(app, shape, rt)  # a reap that already happened must not hide behind the switch
    rt.user_endpoint_config["partition"] = partition
    rt.runner_stale = True
    rt.warm_confirmed_at = None
    rt.block_since = rt.block_job = None  # a different partition is a different block: not the old one's age
    _forget_presumed(rt)
    return None

def _apply_account(app: AppCtx, shape: str, rt: ShapeRuntime, account: str | None) -> str | None:
    """Point this shape's next provision at `account` (the chosen allocation) — the account
    analogue of _apply_partition. Returns a rejection notice (and applies nothing) when a task is still
    running on the shape (the runner swap would cancel it). compute-shape only; the config_template
    renders `account` from user_endpoint_config with the profile default, so a selection here overrides
    it. A change invalidates the cached runner (banking the prior warm interval) and drops the warm
    confirmation; the selection persists for the session."""
    if account is None or not rt.user_endpoint_config.get("compute"):
        return None
    if rt.user_endpoint_config.get("account") == account:
        return None
    live = _live_task_handles(app, shape)
    if live:
        return (f"can't change account to {account!r}: a task is still running "
                f"(task_id={live[0][0]!r}) on shape {shape!r}. poll_task it or stop_endpoint first.")
    # a command inside its sync-wait: the swap would orphan it, and its count would vouch for the new block
    if rt.inflight:
        return (f"can't change account to {account!r}: a command is still running on shape {shape!r} "
                "(inside run_shell's wait). Let it return, then change it.")
    _check_reaped(app, shape, rt)  # a reap that already happened must not hide behind the switch
    rt.user_endpoint_config["account"] = account
    rt.runner_stale = True
    rt.warm_confirmed_at = None
    rt.block_since = rt.block_job = None  # a different account is a different block: not the old one's age
    _forget_presumed(rt)
    return None

async def _drop_compute_shape(app: AppCtx) -> float:
    """Drop the billed (compute) shape so a later run re-provisions a FRESH block (its runner now
    points at the released block) and stop its spend clock. Keep the login shape (if any), the
    manager, the endpoint_id, and the login-node pin — the endpoint stays online and reusable. Done
    regardless of cancel confirmation: the runner is dead either way, and banking must stop now.
    Returns the spend the dropped shape had accrued; it is ALSO folded into `app.released_spend`, which
    _total_session_spend() counts — so callers must not add the return value again (0.1.18)."""
    async with app.lock:
        compute = app.shapes.get(DEFAULT_SHAPE)
        until = _presumed_release_at(app, DEFAULT_SHAPE, compute) if compute is not None else None
        _drain_shape_tasks(app, DEFAULT_SHAPE)  # the released block's poll handles are now dead
        compute = app.shapes.pop(DEFAULT_SHAPE, None)
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

def _register_task(app: AppCtx, shape: str, session_id: str, command: str, fut, ceiling_s: float) -> str:
    """Register a still-running task as a poll handle and return its id. Caller holds app.lock."""
    app.task_seq += 1
    task_id = f"{shape}-{app.task_seq}"
    handle = TaskHandle(
        future=fut,
        shape=shape,
        session_id=session_id,
        command=command,
        submitted_at=time.monotonic(),
        ceiling_s=ceiling_s,
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
    try:
        res = fut.result()  # done -> returns at once (or raises the task's own exception)
    except Exception as exc:  # noqa: BLE001 - shape a failed task exactly as execute() would
        out = dispatch.failure_outcome(exc, "warm", app.max_output_chars)
    else:
        out = dispatch.complete_outcome(res, "warm", app.max_output_chars)
    _note_dispatch(_shape_runtime(app, handle.shape), out, at=handle.done_at)
    return _with_spend(app, out)

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
