"""The server's runtime state (split step 1, 2026-09-03): data plus the derived reads on it
(`_supported_shapes`, `_has_login_shape`, `_no_facility_bound`, `_idle_release_s`).

`AppCtx` is the one object every tool call shares (installed by the server's lifespan); a `ShapeRuntime`
per resource shape carries that shape's Executor, warmth/canary state and spend clock; a `TaskHandle`
is a dispatched command still running past the sync-wait. The behaviour that mutates these lives in
`server.py` (and, as the split proceeds, in the modules it delegates to); `server` re-exports these
names, so `from hpc_bridge.server import AppCtx` keeps working.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Literal

from .catalog.entry import CatalogEntry
from .facility.base import Facility
from .lifecycle import EndpointState
from .login import LoginFlow
from .models import FacilityDetails, Submission, TermsOrigin
from .profile import Profile
from .runner import CanaryResult, GlobusRunner
from .session_shell import Guard
from .shapes import SHAPES

DEFAULT_SHAPE = "compute"


@dataclass
class ShapeRuntime:
    """Warm/canary/spend state for ONE resource shape (its own Executor + AMQP sub)."""

    user_endpoint_config: dict
    runner: GlobusRunner | None = None
    warm_since: float | None = None
    warm_confirmed_at: float | None = None
    # When this block first went 'provisioning' (cleared on warm) — the grace clock for the #32
    # pilot-rejection hint, so a not-yet-visible pilot during normal cold-start isn't cried as rejected.
    provisioning_since: float | None = None
    spend_accrued: float = 0.0
    last_canary: CanaryResult | None = None
    # Set when user_endpoint_config changed under a live runner (e.g. a new partition): the
    # cached Executor captured the old config at build time, so _runner_for must rebuild it.
    runner_stale: bool = False
    # A recorded NO-ACCOUNT refusal (the MEP could not map our identity). Sticky: later calls return
    # it without re-submitting — a rapid re-submit got a transient RESOURCE_CONFLICT from the web
    # service (live, 2026-09-03) that flipped the verdict back to 'allocating nodes…'. Cleared when the
    # runtime is dropped (re-bind/teardown) or a new login lands (_forget_identity_verdicts).
    no_account: str | None = None
    # Consecutive TRANSIENT dispatch refusals (RESOURCE_CONFLICT). One is a race; three in a row means
    # another session with the same Globus identity holds/starts this endpoint, or the manager is
    # wedged — the 'call again' hint must stop (a model sweep showed Sonnet retrying 7× on it).
    transient_conflicts: int = 0
    # Deterministic spend floor: a scheduler compute shape may not start a block until spend is
    # explicitly acknowledged via ensure_endpoint_up(confirm_spend=True). It covers ONE block: it persists
    # while that block lives, is cleared on stop/reset when the shape state is dropped, and is cleared when
    # the block is reaped (idle-release, walltime, or found gone) or its partition or account is switched, so the NEXT
    # block is asked for again.
    spend_confirmed: bool = False
    # When the current block was first confirmed warm (for its walltime), and why it is presumed or known gone.
    # `reap_told`: the agent has been handed that reason once — the call that detects a reap always answers
    # needs_confirmation, even if it carried confirm_spend=True, so the user is asked AFTER learning of the reap.
    # `reap_kicked`: the reap was found by a check (a task), which may already have asked for a new block.
    block_since: float | None = None
    # The scheduler job id the block's canary reported when it was first confirmed warm (None: unknown).
    block_job: str | None = None
    # The block's last proof of life when something voided warm_confirmed_at (a failed dispatch or check, a runner
    # rebuild) without finding it gone: the idle clock and the billing bound keep counting from it (warmth._void_warm).
    last_proof_at: float | None = None
    # Where the spend clock stopped while the block kept its identity (not found gone): a terminal verdict, a failed
    # check with no idle window to bound a hold, the manager offline, an orphaned task, a runner rebuild, an error. If
    # that block answers again, its clock resumes from here, so the gap is billed (cost._bank_warm_interval records it;
    # warmth._provision resumes it).
    clock_stopped_at: float | None = None
    reaped: str | None = None
    reap_told: bool = False
    reap_kicked: bool = False
    # A partition or account switch on a block spend was confirmed for: that confirmation covered the old terms' block
    # only, so the switch drops it and the next start asks again (warmth._repoint). `switched_terms`: the (partition,
    # account) it covered; `switch_left`: on a facility endpoint, the block or check the switch left billing, to name;
    # `switch_unseen`: that block was asked for but never seen up, so a stop must still look for its pilot
    # (server._stop_endpoint's expect_block) — also after a reap a check found (`reap_kicked`), whose check may have
    # asked the old terms' user endpoint for a block; `unseen_terms` are that block's partition and account, which a
    # stop names and watches for. A confirmation clears them — `switch_unseen` / `unseen_terms` only once a block is
    # up (a submission on the new terms that the scheduler rejects leaves the old terms' pilot still to look for).
    switched_terms: tuple[str | None, str | None] | None = None
    switch_left: LeftBlock | None = None
    switch_unseen: bool = False
    unseen_terms: tuple[str | None, str | None] | None = None
    # A switch while spend was asked anyway (a reap): the terms the reap was on. The re-ask says the terms changed since
    # while they differ from these — back on them, the block the reap found is that confirmation's again.
    reap_terms: tuple[str | None, str | None] | None = None
    # Bumped by a switch: a task handle records the epoch it was sent in, and its result is proof of life only for a
    # block of that epoch — a task that finished on the old block, polled later, must not vouch for the new one
    # (warmth._resolve_task). Its end is still the old block's last activity, which every stamp of the new block passes.
    block_epoch: int = 0
    # An idle presumption can come while the block is still up (an endpoint on a slower scaling pass): that block's
    # age, job id, the latest it could have lived, and where the presumption stopped its spend clock — so the block
    # that answers next is dated and billed right (warmth._provision).
    presumed_block_since: float | None = None
    presumed_block_job: str | None = None
    presumed_block_until: float | None = None
    presumed_billed_until: float | None = None
    # Synchronous dispatches in flight on this shape (run_shell / reset_session inside their sync-wait). They hold
    # no poll handle, yet they ARE the worker's work: a canary queued behind one is not evidence the block is gone.
    inflight: int = 0
    # The scheduler REJECTED this shape's block submission (one block): (the cause, the partition and account it was
    # on). Set with the confirmation it voids (warmth._void_rejected), cleared by the next confirmation. The re-ask says
    # why (notices._reask_prefix), and a stop expects no pilot from it (server._stop_endpoint).
    rejected: tuple[str, str | None, str | None] | None = None
    # A pilot of this shape that ran and died before any worker answered: the pilot probe reported it FINISHED with an
    # exit status of its own (scheduler_ops.PilotProbe.dead; recorded by server._ensure_endpoint_up). Cleared when a
    # worker answers a check (warmth._check_resolved) or the scheduler rejects a submission; a switch hands it to the
    # endpoint's pilot watch (warmth._repoint). NOT cleared by a fresh confirm_spend: a confirmation is the user's
    # assent, not news about the endpoint — the check those pilots were started for is still queued (the runner waits
    # on it again rather than sending another), so the next pilot is for the same check and likely dies the same way.
    dead_pilot: DeadPilot | None = None
    # When spend was last confirmed for this shape (time.monotonic()): a pilot that FINISHED and was submitted before it
    # is an earlier session's, not this block's (scheduler_ops._probe_pilots_over_login's `since_s`).
    confirmed_at: float | None = None


@dataclass
class TaskHandle:
    """A dispatched command still running past the client sync-wait — a poll handle (phase="running").
    Its future lives on the shape's long-lived Executor, so poll_task can retrieve the result whenever
    it resolves; the running task also keeps the block busy (a warmth signal) until it finishes."""

    future: Future[Any]  # a stdlib concurrent.futures.Future from the Executor (no SDK import needed)
    shape: str
    session_id: str
    command: str
    submitted_at: float
    ceiling_s: float
    # When the future resolved (stamped by a done-callback), so a poll long after the task ended does not pass
    # for recent activity on the idle clock (0.1.18).
    done_at: float | None = None
    # The client cancelled the call that dispatched it, inside its sync-wait (Esc in Claude Code → MCP
    # notifications/cancelled). Cancelling a call does not cancel the command — it runs on the block — so it is
    # tracked like any running task; the flag only makes the notices say where a task id the agent never saw came from.
    client_cancelled: bool = False
    # The shape runtime it was dispatched on. A stop can release the block (and drop that runtime) while a sent task
    # still runs; its result is then noted nowhere — never on a rebuilt or newer runtime. None: the shape's current one.
    runtime: ShapeRuntime | None = field(default=None, repr=False, compare=False)
    reset: bool = False  # a reset_session, not a command: its notices say so
    # The scheduler job the command submits by hand, if any — so the result poll_task hands back says so (2026-10-10).
    submission: Submission | None = None
    # Its wrong-block guard (session_shell.Guard: the block job it must run in, and the dispatch's nonce); None: sent
    # unguarded. A result the guard refused is the replacement found (warmth._wrong_block_outcome), whenever it is
    # polled.
    guard: Guard | None = None
    epoch: int = 0  # its runtime's block_epoch when it was sent: a switch since then means it ran on the old block


@dataclass
class LeftBlock:
    """What a facility-endpoint stop, detach, reconnect or partition/account switch LEFT running (#178) — a block, or a
    check of ours still queued: a facility multi-user endpoint gives hpc-bridge no cancel channel, so a block bills
    until the facility's idle-release or its walltime. Kept in `AppCtx.left_blocks` ONLY to NAME it in later notices (a
    second stop, a teardown, a re-bind) — never to bill it again or to date another block. Times: time.monotonic()."""

    endpoint_id: str | None
    job: str | None              # its scheduler job id (from our own check's HPCB_JOB); None: unknown here
    until: float | None          # the latest it can bill to (see `bound`); None: unknown
    left_at: float               # when it was left
    how: str                     # what left it: "stop" | "detach" | "reconnect" | "switch"
    kind: Literal["block", "check"] = "block"  # "check": no block seen, only a check of ours still queued
    # What `until` is: "idle" — the facility's idle window after our last task, capped at its walltime; "walltime" —
    # its walltime alone (the idle window is unknown); "busy" — its walltime, because work of ours may still be on it.
    bound: str | None = None
    # A block's walltime end, past which it cannot still run and the entry is dropped; None: unknown, or a queued check
    # (kept, dated: the queue wait is unbounded and the facility keeps starting blocks while it waits).
    ends_by: float | None = None
    verified: bool = False       # the job is a block hpc-bridge confirmed warm (not only one a check saw)
    check_queued: bool = False   # a check of ours was also still queued: the facility may keep a block or start one
    partition: str | None = None  # the block's partition and account: the same pair is the same facility user
    account: str | None = None    # endpoint process, whose next block supersedes this one (with one block at most)
    multi: bool = False          # the shape could run more than one block (max_blocks > 1): "at least" this one


@dataclass(frozen=True)
class DeadPilot:
    """A pilot that ran and died before it ran hpc-bridge's check: the block started and its worker exited (a failed
    worker_init, an environment the compute node lacks). The check is still queued on the endpoint, and parsl submits
    another pilot for it on its next scaling pass — on PBS forever (a finished job reads COMPLETED, which never trips
    parsl's block-error handler), on Slurm until every block that user endpoint ever had has FAILED. Idle-release never
    ends that: the endpoint is not idle while the check waits."""

    job: str                     # the pilot's scheduler job, as the FINISHED notice named it
    exit_status: str             # its exit status, as reported
    # every pilot job of this endpoint the probe listed with it (live or finished): a stop tells a pilot the endpoint
    # submitted since from these
    known: frozenset[str] = frozenset()


@dataclass(frozen=True)
class PilotWatch:
    """A pilot of ours that may still land on an SSH endpoint after a stop let the compute shape go (or a switch moved
    it to other terms), kept per endpoint in `AppCtx.pilot_watch` so a later stop looks again instead of reading
    "released none" as done (#24): a block still being submitted when a stop ended `draining`, or a dead pilot whose
    check is still queued (`dead`). A stop that finds nothing within `config.pilot_watch_s()` of `since` answers `down`;
    one that finds a pilot refreshes it. Times: time.monotonic()."""

    since: float                                  # the last sign of a pilot: when a stop looked, or found one
    terms: tuple[str | None, str | None]          # the partition and account of the block it is for
    dead: DeadPilot | None = None                 # None: a block still being submitted
    # Not built — the seam for the queued-check fix: the watch only infers from the scheduler that our check may still
    # be queued. The check's own future (GlobusRunner's pending canary, which the SDK's result watcher keeps resolving
    # after the runner closes) would SAY so; it would be carried here, taken before the drop closes the runner.


@dataclass
class AppCtx:
    facility: Facility
    profile: Profile
    # Catalog machine id bound by connect_facility (the agentic path); None when the facility was
    # fixed at startup (HPC_BRIDGE_MACHINE/FACILITY) or is local dev.
    machine: str | None = None
    state: EndpointState = field(default_factory=EndpointState)
    scratch_root: str = "~/.hpc-bridge"
    charge_factor: float = 0.0
    shapes: dict[str, ShapeRuntime] = field(default_factory=dict)
    # Live long-task handles (phase="running") keyed by task_id. The future lives on the shape's
    # Executor; poll_task resolves it. Drained when the block goes away (swap/stop/connect/teardown).
    tasks: dict[str, TaskHandle] = field(default_factory=dict)
    task_seq: int = 0  # monotonic task-id counter (bumped under app.lock)
    # Session-local facilities the agent supplied for machines NOT in the catalog (the Socratic
    # fallback) — keyed by the id passed to connect_facility. Never written to the shared index.
    session_facilities: dict[str, CatalogEntry] = field(default_factory=dict)
    # BYO configs supplied this session but not yet PROVEN (login-shape canary answered) — written to
    # facilities.json only then (decision 2026-09-03: "proven", not "accepted"): facility id -> (ssh_host, details)
    pending_facility_cache: dict[str, tuple[str, dict]] = field(default_factory=dict)
    # The facility id the LAST connect_facility answered with proposed_facility_details (a draft config, nothing bound).
    # While no facility is bound, the "No facility connected" notice tells the agent to FINISH that connect with
    # details=… rather than start over. Every connect_facility clears it first; only a new proposal sets it again.
    proposed_facility: str | None = None
    # The LAST draft config each facility id was proposed (connect_facility's proposed_facility_details): a details=
    # that omits env_setup takes the proposal's, so the agent never has to echo hpc-bridge's env_setup line back
    # (gpt-oss corrupted a 3.8 KB one doing that, 2026-10-10).
    proposed_drafts: dict[str, FacilityDetails] = field(default_factory=dict)
    # Facilities THIS session bootstrapped over SSH (a fresh start, not a reattach). The very next connect
    # re-finds that endpoint online and reads as `reused` — which used to (a) skip the proven-cache commit
    # forever and (b) tell the agent it 'reused an already-online endpoint' (live, 2026-09-04).
    bootstrapped_facilities: set[str] = field(default_factory=set)
    runner_factory: Callable[..., GlobusRunner] = GlobusRunner
    # The in-terminal Globus login (login.py). None ⇒ no login gating (hermetic tests / unbound dev);
    # lifespan installs the real one, which rides the Compute SDK's own client id + token storage.
    login_flow: LoginFlow | None = None
    # The SSH target a connect_facility just refused with needs_preauth (facility id, target): what
    # complete_preauth(code) opens the shared connection for. Cleared once opened.
    pending_preauth: tuple[str, Any] | None = None
    # The call that asked for the code, for complete_preauth's "now call X again" — connect_facility by default;
    # teardown_endpoint when the teardown gate asked (it is the one post-bootstrap op that must SSH).
    preauth_resume: str | None = None
    # A teardown whose login-node ops (gce stop + delete over SSH) outlived one tool call's wait: the ops run on in
    # this task; the next teardown_endpoint call reports its result (or waits again). See server._teardown_endpoint.
    teardown_task: asyncio.Task[Any] | None = None
    # The endpoint `teardown_task` was started for, and how its block release went (None = still releasing,
    # True = confirmed, False = dispatched but NOT confirmed) — a finished result is replayed only for the endpoint
    # it belongs to, and the interim `tearing_down` notice says what is true of the release (review 2026-10-05).
    teardown_eid: str | None = None
    teardown_release: bool | None = None
    # Spend of blocks already RELEASED on this binding: their shapes are gone from `shapes`, so without this every
    # later result (a teardown resumed after its one-time code, a retry) under-reported the session. Cleared with
    # the binding (`warmth._drop_all_shapes`).
    released_spend: float = 0.0
    # Compute runtimes dropped (a teardown's release step, a re-bind) while a dispatch was still inside its sync-wait on
    # them: the dispatch's command is on the endpoint, so a stop must still see it — while that endpoint is the bound
    # one (warmth._retain_inflight; server._block_work).
    released_inflight: list[ShapeRuntime] = field(default_factory=list)
    # Billed blocks a facility-endpoint stop, detach or switch left running (no cancel channel there; #178), so a
    # second stop or a stop after a reconnect still names them. Kept across a detach and a reconnect to the same
    # endpoint, dropped by a bind to another one (connect) and when a block on the same endpoint takes one over
    # (warmth._take_left).
    left_blocks: list[LeftBlock] = field(default_factory=list)
    # Per (endpoint, scheduler job): how far a facility stop's bank has already counted that job (its latest end, which
    # may be ahead of now). A later bank for the same job adds only the time beyond it (warmth._leave_block), so stop /
    # re-confirm cycles of one block never recount a span a tail already counted. Kept with left_blocks.
    billed_to: dict[tuple[str | None, str], float] = field(default_factory=dict)
    # The local account the last check (canary) ran as — on a facility endpoint, the identity mapping's: a stop's
    # `squeue -u` names it. Cleared with left_blocks on a bind elsewhere, and on a new Globus login.
    worker_user: str | None = None
    # The partition and account this session chose for the compute shape (ensure_endpoint_up partition= / account=,
    # warmth._repoint). A stop drops the compute shape whole; the next one starts with these (warmth._shape_runtime), so
    # the choice persists for the session as the tool says: the spend question after a stop names the partition, and
    # its result carries both. Forgotten with the binding's shapes (warmth._drop_all_shapes: a connect_facility, a
    # teardown or a detach); a new Globus login keeps them, as it keeps a live shape's.
    compute_choice: dict[str, str] = field(default_factory=dict)
    # Where the bound facility's DEFAULT partition and account came from (the registry, an env preset, a facility you
    # bring yourself), whether its entry says an account is needed, and its account hint: recorded with the binding
    # (connect_facility, the startup pin), read for every block's terms (notices._block_terms). None: unknown (local
    # dev).
    terms_origin: TermsOrigin | None = None
    # When each scheduler job on the bound endpoint was first proven up (a check or a command reporting it), keyed
    # (endpoint id, job): never moved later, so a block dated by its job keeps its real age (for its walltime) however
    # often a switch, a stop or a reconnect let go of it and it answered again (warmth._answering_block_since).
    # Bounded; kept for the endpoint a reconnect binds again, like `billed_to`, and dropped by a bind to another one.
    job_seen_since: dict[tuple[str | None, str], float] = field(default_factory=dict)
    # Per SSH endpoint id: a pilot of ours that may still land there though the compute shape is gone (PilotWatch). It
    # outlives the stop's drop of the shape, so a second stop polls and checks again (server._stop_endpoint). Set by a
    # stop that answered `draining` waiting for a pilot, and by a switch away from a dead pilot (warmth._repoint);
    # dropped by a stop that answers `down`, a worker answering a check on its terms or a rejected submission there
    # (warmth._check_resolved), and the teardown of that endpoint. Keyed by endpoint, so a reconnect to it keeps it.
    pilot_watch: dict[str, PilotWatch] = field(default_factory=dict)
    # serializes provision / runner-swap / teardown so concurrent tool calls can't race AppCtx state
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _facility_shapes(facility: object) -> tuple[str, ...]:
    """The shapes `facility` can serve (bound or not): its `supported_shapes`, else every shape."""
    return tuple(getattr(facility, "supported_shapes", None) or SHAPES)

def _supported_shapes(app: AppCtx) -> tuple[str, ...]:
    """The shapes the bound facility can serve. Default: every shape (a personal endpoint renders
    our own template, which has both). A facility-run multi-user endpoint declares
    `supported_shapes=("compute",)` — its schema REJECTS the LocalProvider login shape — and the
    server derives the rest from that single fact: no login shape ⇒ no free channel for the
    allocation listing / the pilot query / the scancel release ⇒ stop is draining-only, teardown is
    a no-op, every shape is billed."""
    return _facility_shapes(app.facility)

def _has_login_shape(app: AppCtx) -> bool:
    return "login" in _supported_shapes(app)

def _no_facility_bound(app: AppCtx) -> bool:
    """Nothing is bound: the facility is the server's IMPLICIT local default (no machine pinned or connected, local
    dev not opted into — binding._default_local_facility marks it). Read from that mark, never from the facility's
    type: an explicitly chosen LocalFacility (local dev, the tests) is bound and works as before."""
    return getattr(app.facility, "implicit", False) is True

def _idle_release_s(app: AppCtx) -> int | None:
    """The block's idle-release window in seconds, or None when it is the FACILITY's and unknown.
    A facility that declares the attribute (a MEP) owns the window: its template's value when hpc-bridge could read
    it, else None — never our profile's 600 s, which is what we WRITE into our own endpoints' templates, not theirs
    (review 2026-09-05 #6a). Every other facility runs the template we wrote, so the profile value is the truth.
    One source — the warm-block bounds note and the MEP stop notice used to read different ones."""
    if hasattr(app.facility, "max_idletime_s"):
        val = app.facility.max_idletime_s
        return int(val) if val else None
    return int(app.profile.max_idletime_s)
