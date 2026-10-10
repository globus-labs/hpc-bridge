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
    # both of which void warm_confirmed_at) — read BEFORE _runner_for, which voids it on a rebuild. So is the latest
    # the block could have lived, which bills it if this check finds it gone.
    was_warm = rt.block_since is not None
    bound = _release_bound(app, shape, rt)
    # The confirm that follows "the block may have idle-released" (reap_tentative): this canary decides whether that
    # block is still up. It was sent WITH the user's confirmation, so whatever it finds is never the kicked path below.
    resuming = rt.reap_tentative and rt.spend_confirmed
    runner = _runner_for(app, shape)
    now = time.monotonic()
    # A task still running on this shape IS liveness — the worker is demonstrably executing our work.
    # Trust it and skip the canary, which would otherwise queue behind the sole worker and (on timeout)
    # flip us to 'not warm', banking the spend clock while the block is still burning (#21). A synchronous
    # dispatch still inside its sync-wait holds no handle but counts the same (rt.inflight).
    if _live_task_handles(app, shape) or rt.inflight:
        rt.warm_confirmed_at = now
        rt.provisioning_since = None
        rt.reap_tentative = False
        return "warm"
    if not force and not resuming and rt.warm_confirmed_at is not None and now - rt.warm_confirmed_at < CANARY_TTL_S:
        rt.provisioning_since = None
        return "warm"
    result = await runner.canary(timeout=CANARY_TIMEOUT_S)
    rt.last_canary = result  # keep failures too: the error text is the diagnosis the caller needs
    if resuming:
        rt.reap_tentative = False
        was_warm = False
        if not (result.ok and result.worker_job is not None and result.worker_job == rt.block_job):
            # Not the same scheduler job (a different one, no answer, or no way to tell): the block that may have
            # idle-released is gone, and its spend ends at the latest it could have lived. A block that answers now
            # is one this confirm brought up. Its own age starts now when the job ids differ; when either is unknown
            # the older age is kept, so the walltime presumption errs early (a question), never late (a canary to a
            # block past its walltime).
            _bank_warm_interval(rt, app, until=bound)
            if result.ok and result.worker_job is not None and rt.block_job is not None:
                rt.block_since = None
            rt.block_job = result.worker_job if result.ok else None
        # else: the same job — the block was still up. Its age and its spend clock simply continue.
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
        release = ("stop_endpoint releases it if the user declines" if _has_login_shape(app) else
                   "nothing can cancel it on this facility endpoint, so it idles out after the facility's idle "
                   "window if no work is sent")
        _mark_reaped(app, rt, f"the previous block did not answer a check within {CANARY_TIMEOUT_S:g} s — most "
                     "likely cancelled, preempted or failed. That check may already have asked the facility for a "
                     f"new block: {release}", bound, kicked=True)
    rt.warm_confirmed_at = None
    rt.block_since = None
    rt.block_job = None
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

# Past its idle window a block MAY already be released. How soon after the window depends on the facility's scaling
# pass (5 s on endpoints configured since 2026-10, 30 s on one configured earlier and still running — reuse never
# rewrites the template — a facility MEP's own), and hpc-bridge cannot know which for a given endpoint. A canary to a
# released block makes the endpoint submit a new billed one, so from the window on NOTHING is submitted until spend is
# confirmed again. Up to this grace past the window the presumption is TENTATIVE: the block keeps its age and its
# spend clock, and the canary after the confirm decides by the scheduler job id it reports (_confirm_worker). After
# it, the release is taken as CERTAIN: two passes of 30 s, the slowest hpc-bridge has written into a template.
_CERTAIN_RELEASE_GRACE_S = 60.0

def _idle_window(app: AppCtx) -> int | None:
    """The idle-release window the clock may presume from: None when it is unknown, or when the facility holds its
    block warm (local dev's min_blocks=1) and nothing idle-releases."""
    keeps = getattr(app.facility, "keeps_block_warm", None)
    if keeps is not None and keeps(app.profile):
        return None
    return _idle_release_s(app)

def _work_on_block(app: AppCtx, shape: str, rt: ShapeRuntime) -> bool:
    """A task still running on the shape (or its end not yet stamped), or a synchronous dispatch in flight."""
    return bool(rt.inflight) or any(h.done_at is None for h in app.tasks.values() if h.shape == shape)

def _last_activity(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """The block's last proof of work: its last confirmation (warm_confirmed_at, voided by a failed dispatch) or the
    end of a task nobody has polled yet (`done_at`), whichever is later. One source for the presumptions and the
    billing bound — the bound used to read the confirmation alone and bill a block short."""
    stamps = [t for t in (rt.warm_confirmed_at, *(h.done_at for h in app.tasks.values() if h.shape == shape))
              if t is not None]
    return max(stamps) if stamps else None

def _presumed_reaped(app: AppCtx, shape: str, rt: ShapeRuntime) -> tuple[str, float] | None:
    """(why, released_at) when the shape's last confirmed block has, by the clock alone, CERTAINLY been released by
    the facility: no task for longer than its idle window plus _CERTAIN_RELEASE_GRACE_S, or older than its walltime.
    Decided WITHOUT submitting anything — the only way to look (a canary) is itself a task, and a task on a released
    block starts a new billed one. None when there is no such block, or while work may be on it. The idle check needs
    the last activity; the walltime check needs only the block's age."""
    if rt.block_since is None or _work_on_block(app, shape, rt):
        return None
    last = _last_activity(app, shape, rt)
    now = time.monotonic()
    idle = _idle_window(app)
    if idle and last is not None and now - last >= idle + _CERTAIN_RELEASE_GRACE_S:
        return (f"the previous block idle-released (no task for ~{int(now - last)} s, past the "
                f"{idle} s idle window)", last + idle)
    wall = _parse_hhmmss(rt.user_endpoint_config.get("walltime"))
    if wall and now - rt.block_since >= wall:
        return (f"the previous block reached its walltime ({rt.user_endpoint_config.get('walltime')})",
                rt.block_since + wall)
    return None

def _maybe_released(app: AppCtx, shape: str, rt: ShapeRuntime) -> str | None:
    """Why the block MAY be gone: no task for at least its idle window, but not yet past the certain grace. None
    otherwise, while work may be on it, or when the window is unknown (the canary decides, as before)."""
    if rt.block_since is None or _work_on_block(app, shape, rt):
        return None
    last, idle = _last_activity(app, shape, rt), _idle_window(app)
    quiet = time.monotonic() - last if last is not None else None
    if not idle or quiet is None or quiet < idle:
        return None
    return (f"the previous block has had no task for ~{int(quiet)} s, past the {idle} s idle window, so it has been "
            "or is about to be idle-released")

def _release_bound(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """The latest a block with no task of ours could have lived: its idle window plus the certain grace after its last
    activity — an upper bound, the honest side of a spend estimate. None when either is unknown (bill to now)."""
    idle, last = _idle_window(app), _last_activity(app, shape, rt)
    if idle and last is not None:
        return last + idle + _CERTAIN_RELEASE_GRACE_S
    return None

def _presumed_release_at(app: AppCtx, shape: str, rt: ShapeRuntime) -> float | None:
    """When the clock says the shape's block was released, for banking on a stop/teardown that never reaches
    _provision; None when it is presumed alive (bill to now)."""
    gone = _presumed_reaped(app, shape, rt)
    return gone[1] if gone is not None else None

def _forget_block(app: AppCtx, rt: ShapeRuntime, released_at: float | None) -> None:
    """The block is gone: stop its clock (at the estimated release time, not now) and forget its warmth and age."""
    _bank_warm_interval(rt, app, until=released_at)
    rt.warm_confirmed_at = None
    rt.block_since = None
    rt.block_job = None
    rt.provisioning_since = None
    rt.reap_tentative = False

def _mark_reaped(app: AppCtx, rt: ShapeRuntime, why: str, released_at: float | None, *, kicked: bool = False) -> None:
    """The block this shape's spend acknowledgement covered is gone: stop its clock (at the estimated release
    time, not now), forget its warmth, and require a fresh acknowledgement for the next block. `kicked`: the reap
    was found by a check that may already have asked for a new block."""
    _forget_block(app, rt, released_at)
    rt.spend_confirmed = False
    rt.reaped, rt.reap_told, rt.reap_kicked = why, False, kicked

def _check_reaped(app: AppCtx, shape: str, rt: ShapeRuntime) -> None:
    """Before anything voids the evidence (a provision, a partition or account switch): if the clock says the block
    the spend acknowledgement covered is gone, record the reap — CERTAIN (forget the block) or, just past its idle
    window, TENTATIVE (ask again, keep the block's age and spend clock; the confirm's canary decides)."""
    if not _billable(rt):
        return
    if rt.reap_tentative:
        gone = _presumed_reaped(app, shape, rt)
        if gone is not None:
            # Already asked whether to go on, and now the block is surely gone: end its spend and its age. The
            # question is not asked twice, and a confirmation given after it stands (it covered a new block too).
            _forget_block(app, rt, gone[1])
            if rt.reaped is not None:
                rt.reaped = gone[0]
        return
    if rt.spend_confirmed and rt.reaped is None:
        gone = _presumed_reaped(app, shape, rt)
        if gone is not None:
            _mark_reaped(app, rt, *gone)
            return
        why = _maybe_released(app, shape, rt)
        if why is not None:
            rt.spend_confirmed = False
            rt.reaped, rt.reap_told, rt.reap_kicked, rt.reap_tentative = why, False, False, True

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
    _settle_billing(rt, app, block)
    if block == "warm" and rt.block_since is None:
        rt.block_since = time.monotonic()  # the block's age, for its walltime
        # ...and which scheduler job it is, so a later "may have idle-released" can tell it from a new block
        rt.block_job = rt.last_canary.worker_job if rt.last_canary is not None and rt.last_canary.ok else None
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
    rt.block_since = None  # a different partition is a different block: it must not inherit the old one's age
    rt.block_job, rt.reap_tentative = None, False  # nor be taken for the old block by the confirm's canary
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
    rt.block_since = None  # a different account is a different block: it must not inherit the old one's age
    rt.block_job, rt.reap_tentative = None, False  # nor be taken for the old block by the confirm's canary
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
