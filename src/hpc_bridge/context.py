"""The server's runtime state (split step 1, 2026-09-03): data plus the three derived reads on it
(`_supported_shapes`, `_has_login_shape`, `_idle_release_s`).

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
from typing import Any

from .catalog.entry import CatalogEntry
from .facility.base import Facility
from .lifecycle import EndpointState
from .login import LoginFlow
from .profile import Profile
from .runner import CanaryResult, GlobusRunner
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
    # the block is reaped (idle-release, walltime, or found gone) so the NEXT block is asked for again (0.1.18).
    spend_confirmed: bool = False
    # When the current block was first confirmed warm (for its walltime), and why it is presumed or known gone.
    # `reap_told`: the agent has been handed that reason once — the call that detects a reap always answers
    # needs_confirmation, even if it carried confirm_spend=True, so the user is asked AFTER learning of the reap.
    # `reap_kicked`: the reap was found by a check (a task), which may already have asked for a new block.
    block_since: float | None = None
    reaped: str | None = None
    reap_told: bool = False
    reap_kicked: bool = False
    # Synchronous dispatches in flight on this shape (run_shell / reset_session inside their sync-wait). They hold
    # no poll handle, yet they ARE the worker's work: a canary queued behind one is not evidence the block is gone.
    inflight: int = 0


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
    max_output_chars: int = 1_000_000
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
    # serializes provision / runner-swap / teardown so concurrent tool calls can't race AppCtx state
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _supported_shapes(app: AppCtx) -> tuple[str, ...]:
    """The shapes the bound facility can serve. Default: every shape (a personal endpoint renders
    our own template, which has both). A facility-run multi-user endpoint declares
    `supported_shapes=("compute",)` — its schema REJECTS the LocalProvider login shape — and the
    server derives the rest from that single fact: no login shape ⇒ no free channel for the
    allocation listing / the pilot query / the scancel release ⇒ stop is draining-only, teardown is
    a no-op, every shape is billed."""
    return tuple(getattr(app.facility, "supported_shapes", None) or SHAPES)

def _has_login_shape(app: AppCtx) -> bool:
    return "login" in _supported_shapes(app)

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
