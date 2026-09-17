from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from mcp.server.fastmcp import Context, FastMCP

from . import binding, config, connect, dispatch, login_gate, scheduler_ops, session_shell, warmth
from .binding import (  # noqa: F401 - re-exported for imports; PATCH binding.<name>, not server.<name>
    _catalog_facility,
    _entry_from_details,
    _facility_from_entry,
    _facility_store,
    _make_search_client,
    _resolve_scratch_root,
    _session_endpoint_name,
    _slurm_facility,
    _ssh_config_user,
    _unsupported_entry_reason,
    make_catalog,
    make_facility,
)
from .catalog.entry import CatalogSummary
from .config import (  # noqa: F401 - re-exported: tests patch/import these on server
    CANARY_TIMEOUT_S,
    CANARY_TTL_S,
    PROVISION_GRACE_S,
    SYNC_WAIT_S,
    TASK_CEILING_MARGIN_S,
    TRANSIENT_CONFLICT_LIMIT,
    _control_settings,
    _env_endpoint_id,
    _env_float,
    _env_mode,
    _parse_hhmmss,
    _require_env,
    _short_control_dir,
    _task_ceiling_s,
)
from .connect import (  # noqa: F401 - re-exported for imports; PATCH connect.discover_facility_details / connect._propose_or_ask
    _commit_proven_facility,
    _connect_mep,
    _drop_dead_pin,
    _propose_or_ask,
)
from .context import (  # noqa: F401 - re-exported: tools + tests import them from here
    DEFAULT_SHAPE,
    AppCtx,
    ShapeRuntime,
    TaskHandle,
    _has_login_shape,
    _idle_release_s,
    _supported_shapes,
)
from .cost import (  # noqa: F401 - re-exported
    _bank_warm_interval,
    _billable,
    _session_spend,
    _settle_billing,
    _total_session_spend,
    _with_spend,
    cap_output,
)
from .endpoint import EndpointCLI
from .facility.local import LocalFacility
from .lifecycle import EndpointState
from .login import LoginFlow, LoginMode
from .login_gate import (  # noqa: F401 - re-exported for imports
    _authenticate,
    _complete_login,
    _start_login_and_wait,
)
from .models import (
    ConnectFacilityResult,
    EndpointStatus,
    FacilityDetails,
    LoginShellResult,
    LoginStatus,
    PreauthStatus,
    ShellOutcome,
)
from .notices import (  # noqa: F401 - re-exported
    _GLOBUS_USERNAME_RE,
    _NO_ACCOUNT_MARKERS,
    _SSH_AUTH_DENIED,
    _allocating_notice,
    _billed_bounds_note,
    _busy_session_outcome,
    _cold_outcome,
    _dispatch_error_suffix,
    _error_outcome,
    _explain_provision_error,
    _identity_from_error,
    _local_dill,
    _login_notice,
    _needs_account_notice,
    _needs_account_outcome,
    _needs_confirmation_notice,
    _needs_confirmation_outcome,
    _needs_login_result,
    _needs_preauth_result,
    _no_account_failure,
    _no_account_notice,
    _orphaned_outcome,
    _running_outcome,
    _shape_reject_outcome,
    _spend_floor_guidance,
    _submit_rejected,
    _submit_rejected_notice,
    _transient_dispatch_failure,
    _worker_notice,
)
from .profile import Profile
from .runner import GlobusRunner
from .scheduler_ops import (  # noqa: F401 - re-exported for imports; PATCH scheduler_ops.<name>
    _augment_provisioning_notice,
    _pilot_status_cmd,
    _pilot_status_over_login,
    _release_blocks_over_login,
    _release_cmd,
    _summarize_pilot,
)
from .session_shell import Session
from .warmth import (  # noqa: F401 - re-exported for imports; PATCH warmth._provision / warmth._drop_compute_shape
    _VALID_ACCOUNT,
    _VALID_PARTITION,
    _apply_account,
    _apply_partition,
    _busy_session,
    _confirm_worker,
    _drain_shape_tasks,
    _drop_all_shapes,
    _drop_compute_shape,
    _endpoint_gone,
    _ensure_warm_runner,
    _forget_identity_verdicts,
    _live_task_handles,
    _note_dispatch,
    _provision,
    _register_task,
    _resolve_task,
    _runner_for,
    _shape_reject,
    _shape_runtime,
)


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppCtx]:
    try:
        facility = await binding.make_facility()
    except Exception as exc:  # noqa: BLE001 - a config error must NOT brick the MCP server at boot
        # (a startup crash = the agent silently sees no tools). Start unbound/local and let the
        # catalog tools surface/bind: list_facilities / connect_facility.
        print(
            f"hpc-bridge: facility setup failed at startup ({type(exc).__name__}: {exc}); starting "
            "unbound — use list_facilities / connect_facility to bind a machine.",
            file=sys.stderr,
        )
        user_dir = config.user_dir()
        facility = LocalFacility(EndpointCLI(user_dir=user_dir))
    scratch = binding._resolve_scratch_root(facility)
    app = AppCtx(
        facility=facility,
        profile=Profile(mode=_env_mode()),  # type: ignore[arg-type]
        state=EndpointState(endpoint_id=_env_endpoint_id()),
        scratch_root=scratch,
        charge_factor=config.charge_factor(),
        login_flow=LoginFlow(),
    )
    try:
        yield app
    finally:
        app.tasks.clear()  # drop any live poll handles — their blocks are going away with the process
        for rt in app.shapes.values():
            if rt.runner is not None:
                rt.runner.close()


# --- operational guidance over MCP (cross-harness) --------------------------------------------------------------
# hpc-bridge's operating guidance lives in the driving-hpc SKILL.md. Claude Code auto-loads it via its skill system;
# other MCP hosts (hermes-agent, …) have no skill system, so we surface the same guidance over MCP itself:
#   • a small always-on POINTER in `instructions=` (hosts that surface serverInfo.instructions inject it) telling the
#     model to read the guidance RESOURCE before consequential actions — the lazy, Claude-Code-like path (a host loads
#     the full text only when it's relevant), proven on hermes-on-ALCF and Claude Code (2026-09-05);
#   • the full SKILL.md served VERBATIM as an @mcp.resource (one source, zero drift, paid for only when fetched).
# Claude Code sets HPC_BRIDGE_OMIT_INSTRUCTIONS=1 in .mcp.json → no pointer for it (it has the skill; no duplication).
_GUIDANCE_URI = "hpcbridge://guidance/operations"
# SKILL.md is found in either of two layouts: bundled into the wheel at hpc_bridge/_guidance/ (an installed uvx/pip
# server — see the pyproject force-include), or the source tree's skills/driving-hpc/ when running from the repo
# (`uv run --directory <repo>`, which both Claude Code's .mcp.json and the hermes config use today).
_SKILL_CANDIDATES = (
    Path(__file__).resolve().parent / "_guidance" / "SKILL.md",                     # installed wheel
    Path(__file__).resolve().parents[2] / "skills" / "driving-hpc" / "SKILL.md",    # source tree
)
_INSTRUCTIONS_POINTER = (
    "These tools drive real HPC: stand up (or reuse) a Globus Compute endpoint on a login node, then run shell work "
    "over it. Before you provision a billed compute block, present a spend gate, or handle a Globus/MFA login, READ "
    f"the resource {_GUIDANCE_URI} and follow it — it carries the operating rules (select → discover → gate → "
    "provision → wait; compute-only facilities; stop = draining vs down; never detach long jobs). If you cannot read "
    "the resource, each tool's own description is the fallback."
)


def _skill_path() -> Path | None:
    """The first SKILL.md that exists — the bundled wheel copy, else the source-tree copy."""
    return next((p for p in _SKILL_CANDIDATES if p.is_file()), None)


def _guidance_text() -> str:
    """The full driving-hpc guidance (SKILL.md) served verbatim as an MCP resource, for hosts without a skill system.
    Found whether the server is installed (wheel) or run from the source tree; a graceful note if neither exists."""
    p = _skill_path()
    if p is not None:
        try:
            return p.read_text()
        except OSError:
            pass
    return ("hpc-bridge operational guidance is unavailable in this installation — rely on each tool's own "
            "description. (The driving-hpc SKILL.md could not be located.)")


def _server_instructions() -> str | None:
    """The pointer, unless the host opts out (Claude Code, which loads the skill itself)."""
    return None if config.omit_instructions() else _INSTRUCTIONS_POINTER


# Named "endpoint", not "hpc-bridge" (the plugin/CLI name): Claude Code namespaces a plugin's MCP
# tools as plugin:<plugin>:<server>, so matching names would read the doubled plugin:hpc-bridge:hpc-bridge.
# Keep in sync with the mcpServers key in .mcp.json — CC namespaces by that key, this name just mirrors it.
mcp = FastMCP("endpoint", lifespan=lifespan, instructions=_server_instructions())


@mcp.resource(_GUIDANCE_URI, name="driving-hpc operational guidance", mime_type="text/markdown")
def _operations_guidance() -> str:
    """The full hpc-bridge operating guidance (the driving-hpc skill) — how to select → discover → gate → provision →
    wait, compute-only facilities, stop semantics, and the long-job rule. Read it before consequential actions."""
    return _guidance_text()


async def _ensure_endpoint_up(
    app: AppCtx,
    shape: str = DEFAULT_SHAPE,
    partition: str | None = None,
    confirm_spend: bool = False,
    account: str | None = None,
) -> EndpointStatus:
    if reject := _shape_reject(app, shape):  # compute-only facility: never build a login runtime
        return EndpointStatus(
            status="down", block_state="cold", endpoint_id=app.state.endpoint_id, notice=reject,
        )
    if partition is not None and not _VALID_PARTITION.match(partition):
        return EndpointStatus(
            status="down",
            block_state="cold",
            endpoint_id=app.state.endpoint_id,
            notice=f"invalid partition {partition!r}: must match [A-Za-z0-9_.:-]{{1,64}}",
        )
    if account is not None and not _VALID_ACCOUNT.match(account):
        return EndpointStatus(
            status="down",
            block_state="cold",
            endpoint_id=app.state.endpoint_id,
            notice=f"invalid account {account!r}: must match [A-Za-z0-9_.:-]{{1,64}}",
        )
    async with app.lock:  # serialize provisioning/state mutation across concurrent tool calls
        rt = _shape_runtime(app, shape)
        # A login shape has no partition; surface that we ignored a supplied one rather than
        # silently dropping the user's selection.
        ignored = partition is not None and not rt.user_endpoint_config.get("compute")
        reject = _apply_partition(app, shape, rt, partition) or _apply_account(app, shape, rt, account)
        if reject:  # a live task blocks repointing the block (the swap would cancel it) — change nothing
            return EndpointStatus(
                status="up",
                block_state="warm",
                endpoint_id=app.state.endpoint_id,
                session_spend=_total_session_spend(app),
                partition=rt.user_endpoint_config.get("partition"),
                account=rt.user_endpoint_config.get("account"),
                notice=reject,
            )
        active_partition = rt.user_endpoint_config.get("partition")
        active_account = rt.user_endpoint_config.get("account")
        try:
            # force_canary: a status probe must re-verify the worker (and kick a cold block),
            # never trust the TTL — that's exactly the cold-start gap callers are asking about.
            block = await warmth._provision(app, shape, force_canary=True, confirm_spend=confirm_spend)
        except Exception as exc:  # noqa: BLE001 - provisioning unavailable (e.g. non-Linux host)
            return EndpointStatus(
                status="down",
                block_state="cold",
                endpoint_id=app.state.endpoint_id,
                partition=active_partition,
                account=active_account,
                notice=f"hpc-bridge error: {type(exc).__name__}: {exc}"[:500],
            )
        if block == "needs_confirmation":  # the deterministic spend floor — nothing was started
            where = f" on {active_partition!r}" if active_partition else ""
            return EndpointStatus(
                status="needs_confirmation",
                block_state="cold",
                endpoint_id=app.state.endpoint_id,
                partition=active_partition,
                account=active_account,
                notice=_needs_confirmation_notice(app, where),
            )
        if block == "needs_account":  # the account floor (account_required facility, no account) — nothing was started
            return EndpointStatus(
                status="needs_account",
                block_state="cold",
                endpoint_id=app.state.endpoint_id,
                partition=active_partition,
                account=None,
                notice=_needs_account_notice(app),
            )
        billable = _billable(rt)
        eid = app.state.endpoint_id
        spend = _total_session_spend(app)
        provisioning_elapsed = 0.0
        status: Literal["up", "provisioning"]
        if block == "warm":
            status, notice = "up", _worker_notice(rt.last_canary) or "worker live"
            rt.provisioning_since = None  # warm -> the cold-start grace clock resets (#32)
            if shape == "login" and app.machine:  # the login shape is PROVEN here too, not only inside connect
                connect._commit_proven_facility(app, app.machine)
            if billable:  # #21: name the block's bounds so a caller runs long work as a task
                bounds = _billed_bounds_note(app, rt)
                notice = f"{notice}. {bounds}" if notice else bounds
                if not app.charge_factor:  # walk finding: "session_spend: 0" on a billed block misleads
                    notice += (" (session_spend stays 0 here because no charge factor is configured for this "
                               "facility — the block is still a billed allocation, not a free tier)")
        else:
            status = "provisioning"
            if rt.provisioning_since is None:  # start the grace clock on the first cold poll
                rt.provisioning_since = time.monotonic()
            provisioning_elapsed = time.monotonic() - rt.provisioning_since
            notice = _allocating_notice(active_partition, provisioning_elapsed, facility_mep=not _has_login_shape(app))
            if rt.transient_conflicts >= TRANSIENT_CONFLICT_LIMIT:
                rt.provisioning_since = None
                return EndpointStatus(
                    status="down", block_state="cold", endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account,
                    notice=(f"the endpoint refused to start for this identity {rt.transient_conflicts} times in a row "
                            f"(RESOURCE_CONFLICT: 'already in use … concurrent requests'). This is NO LONGER transient: "  # noqa: E501
                            "another session with the SAME Globus identity is starting or holding a user endpoint here "
                            "(a concurrent hpc-bridge run?), or the facility's manager is wedged. Stop retrying: end the "  # noqa: E501
                            "other session or wait a few minutes, then call ensure_endpoint_up again. Nothing was started."),  # noqa: E501
                )
            if rt.last_canary is not None and _no_account_failure(rt.last_canary.error):
                # The manager refused to start a user endpoint for THIS identity: no local account. Not
                # 'allocating nodes' — a terminal `down`, so the agent stops polling and tells the user.
                from .login import globus_identity_label

                identity = globus_identity_label(fetch=False)  # never a network call under app.lock
                rt.provisioning_since = None
                return EndpointStatus(
                    status="down", block_state="cold", endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account,
                    notice=_no_account_notice(app, rt.last_canary.error, identity),
                )
            if rt.last_canary is not None and _submit_rejected(rt.last_canary.error):
                # The scheduler refused the submission (bad account/partition/QOS, missing resource request): a
                # terminal `down` for THIS config, so the agent stops polling and changes it — not "allocating
                # nodes…" with the cause buried in a suffix (live 2026-09-09: five polls before anyone read it).
                rt.provisioning_since = None
                return EndpointStatus(
                    status="down", block_state="cold", endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account,
                    notice=_submit_rejected_notice(active_partition, active_account, rt.last_canary.error),
                )
            if not _has_login_shape(app) and rt.last_canary is None:
                # On a MEP a canary runs on EVERY poll whose manager gate passes (and is recorded even
                # when it fails), so "provisioning with no canary ever recorded" means the manager
                # itself reported OFFLINE — a facility outage, not a queue wait. "allocating nodes…"
                # would have the agent wait on a queue that doesn't exist (the #32 pilot query that
                # normally disambiguates rides the login shape, which a MEP hasn't got).
                notice = (
                    f"the facility's multi-user endpoint {eid} reports OFFLINE — not a queue wait. It is run "
                    "by the facility (not hpc-bridge), so nothing here restarts it: contact the facility / "
                    "check its status page, then try again."
                )
            notice += _dispatch_error_suffix(rt.last_canary)
        if ignored:
            notice = f"{notice or ''} (login shape has no partition; ignored {partition!r})".strip()
    # OUTSIDE the lock (dispatch takes it): for a still-cold BILLED block, ask the scheduler whether
    # the pilot actually submitted. A rejected/held qsub is otherwise indistinguishable from a normal
    # queue wait, leaving the caller stuck on "allocating nodes…" forever ([#32]). The grace clock
    # keeps a not-yet-visible pilot during normal cold-start from being cried as rejected.
    # (The query rides the free login shape — a compute-only facility has none, so skip it there; the
    # #37 failure-signal path for a MEP is the dispatch-error suffix already on the notice.)
    if status == "provisioning" and billable and eid and _has_login_shape(app):
        notice = await scheduler_ops._augment_provisioning_notice(app, eid, notice, provisioning_elapsed, _login_runner(app))  # noqa: E501
    return EndpointStatus(
        status=status,
        block_state=block,
        endpoint_id=eid,
        session_spend=spend,
        partition=active_partition,
        account=active_account,
        notice=notice,
    )


@mcp.tool()
async def ensure_endpoint_up(
    ctx: Context,
    shape: str = DEFAULT_SHAPE,
    partition: str | None = None,
    confirm_spend: bool = False,
    account: str | None = None,
) -> EndpointStatus:
    """Ensure the personal HPC endpoint is up; report whether its pilot block is warm.

    Pass `partition` (from the discovery selection gate) to provision the scheduler block onto that
    partition; the choice persists for the session until changed. Omit it to keep the facility
    default. Ignored for shape="login" (a login-node LocalProvider has no partition).

    Pass `account` (the allocation chosen from connect_facility's options) to charge the scheduler
    block to it; like `partition`, it persists for the session and is ignored for shape="login".

    `confirm_spend` is the deterministic budget floor: a scheduler compute block will not start until
    you pass confirm_spend=True (after surfacing the allocation balance to the user — see the
    driving-hpc skill). Without it the call returns status="needs_confirmation" and provisions
    nothing. The acknowledgement persists for the session. Not needed for shape="login" (free)."""
    return await _ensure_endpoint_up(
        ctx.request_context.lifespan_context, shape, partition, confirm_spend, account
    )


def _registry_transport_error(exc: BaseException) -> bool:
    """Network / Globus API / OS failures reaching the registry — the cases an empty list may stand for."""
    if isinstance(exc, (OSError, TimeoutError)) or hasattr(exc, "http_status"):
        return True
    return type(exc).__name__ in ("GlobusAPIError", "GlobusConnectionError", "GlobusTimeoutError",
                                  "NetworkError", "SearchAPIError", "GlobusConnectionTimeoutError")


async def _list_facilities(query: str = "") -> list[CatalogSummary]:
    try:
        return await binding.make_catalog().discover(query)
    except Exception as exc:
        # The registry id is built in, so what lands here is either the network (an empty list is
        # honest: the agent can still BYO) or a BUG — which must not hide behind "no facilities"
        # (found in review: the old blanket net reported an AttributeError as an empty registry).
        print(f"hpc-bridge: list_facilities failed ({type(exc).__name__}: {exc})", file=sys.stderr)
        if _registry_transport_error(exc):
            return []
        raise


@mcp.tool()
async def authenticate(ctx: Context, force: bool = False, mode: LoginMode | None = None) -> LoginStatus:
    """Log in to Globus from the terminal — the ONE credential hpc-bridge needs (it covers computing,
    starting an endpoint, and reading the facility registry). Normally you don't call this: a
    connect_facility that needs it returns phase="needs_login" with the same link. Call it to log in
    proactively, to get a FRESH link after one expired (~10 min), or with force=True to re-login.

    Returns `login_url` for the USER to open. `login_mode="browser"`: their browser completes it and
    this process receives the result — nothing to paste; just call connect_facility again afterwards.
    `login_mode="paste"` (remote/headless sessions): Globus shows a one-time code — ask the user to
    paste it and call complete_login(code). `mode="paste"` forces paste mode (e.g. no browser on this
    machine). Never ask for a Globus password."""
    return await login_gate._authenticate(ctx.request_context.lifespan_context, force=force, mode=mode)


@mcp.tool()
async def complete_login(code: str, ctx: Context) -> LoginStatus:
    """Finish a paste-mode Globus login with the one-time authorization code the user pasted (from
    the page Globus showed after they approved). Single-use and short-lived — not a password, not a
    token. Only needed when authenticate()/connect_facility reported login_mode="paste"."""
    return await login_gate._complete_login(ctx.request_context.lifespan_context, code)


@mcp.tool()
async def complete_preauth(code: str, ctx: Context) -> PreauthStatus:
    """Open the shared SSH connection to a facility that asked for a ONE-TIME CODE (TOTP / Duo passcode) — the
    step after connect_facility returned needs_preauth with preauth_code_ok=true. Ask the USER for the current
    code from their authenticator and pass it here; it is single-use and expires in seconds. NEVER pass a
    password: this tool refuses password prompts and then the user opens the session in their own terminal
    with the preauth_command. On success, call connect_facility again."""
    return await _complete_preauth(ctx.request_context.lifespan_context, code)


async def _complete_preauth(app: AppCtx, code: str) -> PreauthStatus:
    from . import preauth as _pre
    from .state import _state_dir

    pending = app.pending_preauth
    if pending is None:
        return PreauthStatus(phase="failed",
                             notice="no facility is waiting for a code — call connect_facility first (it reports "
                                    "needs_preauth with the host).")
    facility, target = pending
    if not _pre.looks_like_code(code):
        return PreauthStatus(phase="failed", preauth_command=target.preauth_command(),
                             notice="that is not a one-time code (4–16 letters/digits). hpc-bridge never sends "
                                    "passwords; ask the user for the CURRENT authenticator code, or have them open "
                                    "the session in their own terminal with preauth_command.")
    state = _state_dir()
    state.mkdir(parents=True, exist_ok=True)
    ok, why = await asyncio.to_thread(_pre.open_master_with_code, target, code, state_dir=state)
    if ok:
        resume = app.preauth_resume or f"connect_facility({facility!r})"
        app.pending_preauth = None
        app.preauth_resume = None
        return PreauthStatus(phase="opened",
                             notice=f"{why}. Call {resume} again — it rides this connection with no further auth.")
    if "PASSWORD" in why:
        return PreauthStatus(phase="needs_terminal", preauth_command=target.preauth_command(),
                             notice=why + f"\n    {target.preauth_command()}")
    return PreauthStatus(phase="failed", preauth_command=target.preauth_command(), notice=why)


@mcp.tool()
async def list_facilities(query: str = "") -> list[CatalogSummary]:
    """List the HPC machines hpc-bridge can stand up, from the public facility registry (a Globus
    Search index, read anonymously — works with no login). Empty query lists all; a query filters by
    name/description.

    Returns agent-safe summaries (no executable config or raw UUIDs). Pick one and call
    connect_facility(facility=…) to bring up its login node and see your allocations. No SSH, no
    provisioning, no spend."""
    return await _list_facilities(query)


async def _connect_facility(
    app: AppCtx, facility: str, ssh_host: str | None = None, details: FacilityDetails | None = None
) -> ConnectFacilityResult:
    """The connect flow lives in connect.py; this wrapper injects the login-shape runner (resolved from
    this module at call time, so a test that patches server._run_shell reaches the allocation listing)."""
    return await connect._connect_facility(app, facility, ssh_host, details, run_login=_login_runner(app))


@mcp.tool()
async def connect_facility(
    facility: str, ctx: Context, ssh_host: str | None = None, details: FacilityDetails | None = None
) -> ConnectFacilityResult:
    """Select an HPC facility and bring up its (free) login node, then list the allocations a scheduler
    block can be charged to.

    **This is the ENTRY POINT for reaching any facility — ALWAYS call it first** (before login_shell,
    and before reasoning about SSH/Duo yourself): it decides whether SSH is even needed. Don't
    pre-check for an SSH master or assume a password/Duo is required — call this and let it tell you.

    **Reconnecting to a facility you've used before? Pass its `ssh_host`.** connect_facility resolves
    the config from the LOCAL cache (a previously-confirmed BYO facility) with **no SSH probe**, then
    reuses the still-online endpoint over the web (`reused: true`) — a **fully zero-SSH reconnect, no
    re-auth**. So a known MFA facility reconnects with NO Duo prompt while its endpoint is up.

    `facility` is an id/subject/alias from list_facilities() (e.g. "anvil"). This binds the facility,
    stands up the login shape (SSH cold-bootstrap once, or reuse an online endpoint — no scheduler
    account needed), runs the allocation command over Compute, and returns phase="needs_account".
    Pick one, then ensure_endpoint_up(account=…, partition=…, confirm_spend=True). phase=
    "provisioning" ⇒ login node still warming — call again shortly.

    NOT in the catalog and not cached → discover, don't interrogate. Pass `ssh_host` (login
    host/alias; SSH user+key come from the environment) and the tool PROBES the login node →
    phase="proposed_facility_details" with a draft — review/correct it with the user (above all
    `interface`), then call again with details=… to register the session facility (then CACHED for
    zero-SSH reconnects; the canary validates). phase="needs_preauth" ⇒ the host needs a one-time
    interactive login (password/MFA) — relay its `preauth_command` for the user to run in THEIR OWN
    terminal; never handle the secret. neither ssh_host nor details ⇒ needs_facility_details."""
    app = ctx.request_context.lifespan_context
    return await _connect_facility(app, facility, ssh_host=ssh_host, details=details)


async def _stop_mep(app: AppCtx, eid: str) -> EndpointStatus:
    """Stop on a facility-run multi-user endpoint: **draining-only, never 'down'**.

    We own neither the manager nor a login channel, and the Globus SDK offers no foreign-endpoint
    cancel (ComputeFuture.cancel is pre-run only; stop/delete_endpoint act on OUR registration). So
    the honest thing (#24) is: stop submitting, drop the shape so no further work lands on the
    block, and rely on the facility template's idle-release (max_idletime) to reclaim it. The block
    keeps burning for up to that idle window after our last task — we report that tail rather than
    pretend it's gone. `draining` here is TERMINAL: re-polling stop will never yield `down`."""
    live = _live_task_handles(app, DEFAULT_SHAPE)
    if live:
        # A running task is exactly what "stop" cannot touch here: there is no cancel channel, so
        # the block stays BUSY (billing) until the task ends — not idle-releasing in ~600s. Refuse,
        # like _apply_partition does for a live task, rather than drain the handles (which would
        # make the result unretrievable) while claiming the block is idle.
        rt = app.shapes[DEFAULT_SHAPE]
        ceiling = int(_task_ceiling_s(rt.user_endpoint_config))
        ids = ", ".join(tid for tid, _ in live)
        return EndpointStatus(
            status="up",
            block_state="warm",
            endpoint_id=eid,
            session_spend=_total_session_spend(app),
            notice=(
                f"can't stop yet: task(s) {ids} are still running on the block, and on a facility "
                "multi-user endpoint hpc-bridge has NO cancel channel — nothing here can end them. The "
                f"block stays busy (billing) until they finish, at most ~{ceiling}s more. poll_task them "
                "to completion (their results stay retrievable), then call stop_endpoint."
            ),
        )
    dropped = await warmth._drop_compute_shape(app)
    idle = _idle_release_s(app)
    return EndpointStatus(
        status="draining",
        block_state="cold",
        endpoint_id=eid,
        session_spend=_total_session_spend(app) + dropped,
        notice=(
            "stopped submitting; the block is DRAINING. On a facility multi-user endpoint hpc-bridge "
            "has no cancel channel, so the block cannot be released or confirmed from here — the "
            f"facility's idle-release reclaims it after ~{int(idle)}s of no tasks (or at walltime). "
            f"Spend may accrue for up to that tail. 'draining' is FINAL on this facility: do NOT "
            "re-poll stop_endpoint waiting for 'down'. The endpoint stays available (it's the facility's)."
        ),
    )


def _login_runner(app: AppCtx):
    """The free login-shape channel the scheduler ops ride, INJECTED into scheduler_ops so it needn't
    import server. Resolves `_run_shell` at call time from this module's namespace, so a test that
    patches `server._run_shell` still reaches every scheduler op."""
    async def run(cmd: str) -> ShellOutcome:
        return await _run_shell(app, cmd, shape="login")
    return run


async def _stop_endpoint(app: AppCtx) -> EndpointStatus:
    """Release the compute block over the **login endpoint (AMQP)** and LEAVE the manager online for
    reuse. "Stop" means *stop spending*, not destroy the endpoint: the login-node manager is the
    whole point — it persists so the next session reuses it with **zero SSH** ([[Standing up the
    endpoint|SSH-once]], #12). Fully pulling the endpoint down (`gce stop`, the facility's
    `teardown()`) is a separate, rarer operation, not done here."""
    eid = app.state.endpoint_id
    if eid is None:
        return EndpointStatus(status="down", block_state="cold", notice="no endpoint was up")
    if not _has_login_shape(app):  # a facility MEP: no release channel exists — drain honestly
        return await _stop_mep(app, eid)
    live = _live_task_handles(app, DEFAULT_SHAPE)
    if live:
        # A RUNNING task holds the block. Cancelling the block under it does not end the task: the endpoint's own
        # scheduler still has it outstanding and relaunches a fresh block to run it — so "down, released" would be
        # false and spend continues (fake-cluster chaos run 2026-09-05: `block-1` appeared 60 s after a `down`).
        # Refuse, as _stop_mep does; the results stay retrievable and the agent has two honest ways out.
        rt = app.shapes[DEFAULT_SHAPE]
        ceiling = int(_task_ceiling_s(rt.user_endpoint_config))
        ids = ", ".join(tid for tid, _ in live)
        return EndpointStatus(
            status="up", block_state="warm", endpoint_id=eid, session_spend=_total_session_spend(app),
            notice=(f"can't stop yet: task(s) {ids} are still running on the compute block. Releasing the block now "
                    "would not end them — the endpoint would relaunch a block to run them and spend would continue "
                    f"after a false 'down'. poll_task them to completion (at most ~{ceiling}s more; their results stay "
                    "retrievable), then call stop_endpoint. To abandon them and remove everything, teardown_endpoint."),
        )
    # Was a block REQUESTED but never CONFIRMED running? Then a scheduler submit may still be in flight — a
    # one-shot scancel that finds nothing has NOT confirmed the block gone: the pilot's sbatch can land a moment
    # after (the stop-during-provisioning race — a user revoking mid-bring-up, spend_revoked 2026-09-05). The
    # release then POLLS for the pilot to land and cancels it (expect_block); captured before the shape is dropped.
    rt_pre = app.shapes.get(DEFAULT_SHAPE)
    expect_block = rt_pre is not None and rt_pre.spend_confirmed and rt_pre.warm_confirmed_at is None
    # Cancel the scheduler block over the login shape (AMQP) — no SSH.
    confirmed, detail = await scheduler_ops._release_blocks_over_login(
        app, eid, _login_runner(app), expect_block=expect_block)
    dropped = await warmth._drop_compute_shape(app)
    if confirmed:
        return EndpointStatus(
            status="down",  # cancel CONFIRMED: a block was found + cancelled, or none was ever requested
            block_state="cold",
            endpoint_id=eid,
            session_spend=_total_session_spend(app) + dropped,
            notice=f"compute block released over AMQP ({detail}); the login endpoint stays online for "
            "reuse (reconnecting is zero-SSH).",
        )
    if await _endpoint_gone(app):
        # The login endpoint is OFFLINE/gone (the #44 liveness check poll_task got, applied here — found
        # in review): "call again in a few seconds" would loop forever. A block whose manager is gone
        # exits on its own (workers lose their manager), so nothing spends through hpc-bridge.
        return EndpointStatus(
            status="down", block_state="cold", endpoint_id=eid,
            session_spend=_total_session_spend(app) + dropped,
            notice=("the login endpoint is OFFLINE — the cancel cannot be dispatched through it (ORPHANED). "
                    "A block without its manager exits on its own; nothing is spending through hpc-bridge. "
                    "Do not call stop_endpoint again; connect_facility stands the endpoint up afresh."),
        )
    # HONEST unconfirmed release (#24): NEVER "down" here — the agent must know spend may still be running.
    if expect_block:
        # the block was still being submitted and no pilot appeared in the scheduler within the release window;
        # one may land shortly (the stop-during-provisioning race, 0.1.14).
        notice = (f"{detail}. Spend is NOT confirmed stopped — the compute block was still being submitted and no "
                  "pilot had appeared in the scheduler yet, so there was nothing to cancel; one may land shortly. "
                  "Call stop_endpoint again in a few seconds to cancel it once it lands (idle-release, ~10 min, "
                  "min_blocks=0, is the backstop). The login endpoint stays online for reuse.")
    else:
        notice = (f"{detail}. Spend is NOT confirmed stopped — the login release channel was cold. "
                  "idle-release (~10 min, min_blocks=0) is the backstop; call stop_endpoint again in a few "
                  "seconds (the channel is warming) to confirm the cancel. The login endpoint stays online for reuse.")
    return EndpointStatus(
        status="draining",
        block_state="cold",
        endpoint_id=eid,
        session_spend=_total_session_spend(app) + dropped,
        notice=notice,
    )


@mcp.tool()
async def stop_endpoint(ctx: Context) -> EndpointStatus:
    """Release the HPC compute block so the allocation stops being charged. Cancels the billed
    scheduler block over the login endpoint (no SSH) and **leaves the login-node endpoint online** so a
    later reconnect reuses it with zero SSH — "stop" means stop spending, not tear the endpoint
    down. Call when you're done with a compute block."""
    return await _stop_endpoint(ctx.request_context.lifespan_context)


# How long ONE teardown_endpoint call waits for the login-node ops (gce stop + delete over SSH) before handing
# back `tearing_down`. Well inside any MCP client's tool window: Expanse's stop + delete take ~3 min on its
# filesystem (live 2026-09-04) and the call used to fall into the client's 120 s background rescue — a client
# without one would cancel the request and could interrupt the ops half-way. The ops now run in a server-side
# task; a later call reports the result.
_TEARDOWN_SYNC_WAIT_S = 60.0


async def _teardown_endpoint(app: AppCtx) -> EndpointStatus:
    """FULLY tear the endpoint down: release the billed block, then `gce stop` + delete the login
    manager over SSH (the facility's `teardown()`), and clear ALL shape/state so nothing lingers.
    The rare, explicit 'destroy it' op — normally the login endpoint STAYS ONLINE for zero-SSH reuse
    and costs nothing; a later run_shell would re-bootstrap a fresh endpoint from scratch.

    The SSH ops run in a task (`app.teardown_task`): this call waits `_TEARDOWN_SYNC_WAIT_S` for them and
    otherwise returns `tearing_down`; calling again waits again / reports the finished result."""
    if app.teardown_task is not None:  # an earlier call started the ops: report them, don't start again
        return await _await_teardown(app)
    eid = app.state.endpoint_id
    if eid is None:
        return EndpointStatus(status="down", block_state="cold", notice="no endpoint was up")
    if not _has_login_shape(app):
        # A facility MEP is NOT ours to destroy (and there's no release channel): detach — drop our
        # shapes/state so nothing of ours lingers — and say exactly that. The facility's endpoint
        # stays online; a block we left is reclaimed by its idle-release (see _stop_mep).
        dropped = await warmth._drop_compute_shape(app)
        async with app.lock:
            spent = _drop_all_shapes(app, bank=True)
        return EndpointStatus(
            status="down",  # OUR state is fully cleared (nothing of ours remains); the facility's endpoint is untouched
            block_state="cold",
            endpoint_id=eid,
            session_spend=spent + dropped,
            notice=(
                "detached from the facility's multi-user endpoint (nothing of ours to tear down — the "
                "facility runs it). Any block still draining is reclaimed by the facility's idle-release; "
                "it cannot be cancelled from here. Do NOT call run_shell now (it would re-attach); "
                "connect_facility re-attaches with zero SSH."
            ),
        )
    await scheduler_ops._release_blocks_over_login(app, eid, _login_runner(app))  # halt spend first (a confirmed stop is stop_endpoint's job)  # noqa: E501
    gate = await _teardown_preauth_gate(app, eid)
    if gate is not None:
        return gate
    app.teardown_task = asyncio.create_task(_finish_teardown(app, eid))
    return await _await_teardown(app)


async def _await_teardown(app: AppCtx) -> EndpointStatus:
    """Wait a bounded time for the in-flight teardown; its result when it finished, else `tearing_down`."""
    task = app.teardown_task
    assert task is not None
    done, _pending = await asyncio.wait({task}, timeout=_TEARDOWN_SYNC_WAIT_S)
    if task in done:
        app.teardown_task = None
        return task.result()  # _finish_teardown never raises: every failure is folded into the notice
    return EndpointStatus(
        status="tearing_down",
        block_state="cold",
        endpoint_id=app.state.endpoint_id,
        session_spend=_total_session_spend(app),
        notice=("teardown is still running on the login node — the manager's stop + delete take a few minutes on a "
                "slow filesystem; the block release already went through. Call teardown_endpoint again in about a "
                "minute to confirm 'down'. Do NOT call run_shell or connect_facility meanwhile."),
    )


async def _finish_teardown(app: AppCtx, eid: str) -> EndpointStatus:
    """The login-node half of teardown (the facility's `teardown()`), then clear ALL shape/state. Runs as
    `app.teardown_task` so a slow login node cannot hold the MCP call — or be interrupted by its client."""
    notice = "endpoint fully torn down (block released; manager gce-stopped + deleted)"
    teardown = getattr(app.facility, "teardown", None)
    if teardown is not None:
        try:
            # the seeded token store leaves with the endpoint (B-03)
            report = await teardown(eid, wipe_credentials=True)
        except Exception as exc:  # noqa: BLE001 - report, don't crash the tool
            return _teardown_failed(app, eid, f"the login-node teardown raised {type(exc).__name__}: {exc}"[:300])
        else:
            if isinstance(report, dict):  # say what actually happened, not what was intended (live 2026-09-04)
                if report.get("ssh_failed"):
                    return _teardown_failed(app, eid, report.get("error") or "ssh failed", ssh_denial=True)
                if not report.get("stopped", True):
                    return _teardown_failed(
                        app, eid, "`globus-compute-endpoint stop` failed and the manager still reports running"
                        + (f": {report.get('error')}" if report.get("error") else ""))
                deleted = ("manager gce-stopped + deleted" if report.get("deleted") else
                           "manager gce-stopped, but DELETE FAILED: the endpoint directory and its registration "
                           "remain on the login node (the next connect will re-adopt them)")
                creds = ("the Globus token copy hpc-bridge placed on the login node removed"
                         if report.get("credentials_wiped") else "no token store of ours was removed")
                ssh = ("; the shared SSH connection to the login node was closed too — nothing of this session "
                       "stays open on the user's machine" if report.get("ssh_closed") else "")
                notice = f"endpoint fully torn down (block released; {deleted}; {creds}{ssh})"
    async with app.lock:  # clear everything so a stray run_shell can't silently revive a stale endpoint
        # — unless a connect meanwhile bound a NEW endpoint: that state is its, not this teardown's to clear
        ours = app.state.endpoint_id in (eid, None)
        spent = _drop_all_shapes(app, bank=True) if ours else _total_session_spend(app)
    return EndpointStatus(
        status="down",
        block_state="cold",
        endpoint_id=eid,
        session_spend=spent,
        notice=notice + ". It will NOT be reused — a fresh connect_facility re-bootstraps over SSH. "
        "Do NOT call run_shell now (it would provision a new endpoint).",
    )


def _teardown_failed(app: AppCtx, eid: str, why: str, *, ssh_denial: bool = False) -> EndpointStatus:
    """Teardown did NOT happen: the endpoint stays bound (so a retry can finish the job) and the notice says
    what is still there. An SSH denial that offers a second factor becomes the one-time-code handoff, so a
    bring-your-own MFA facility gets the same treatment as a curated one (review 2026-09-05, Fix-now #1)."""
    from .facility.remote import key_accepted_second_factor_pending

    target = getattr(getattr(app.facility, "cli", None), "target", None)
    facility = app.machine or "the facility"
    head = ("TEARDOWN FAILED — nothing was removed: the login-node manager is STILL RUNNING and any token copy "
            "hpc-bridge placed there is still in place. ")
    if ssh_denial and target is not None and key_accepted_second_factor_pending(why):
        app.pending_preauth = (facility, target)
        app.preauth_resume = "teardown_endpoint()"
        handoff = _needs_preauth_result(facility, target, otp_ok=True)
        detail = f"The SSH connection to the login node needs its one-time code again. {handoff.notice} "
    elif ssh_denial:
        detail = _explain_provision_error(RuntimeError(why), app.facility) + " "
    else:
        detail = why + ". "
    return EndpointStatus(
        status="up", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
        notice=head + detail + "Then call teardown_endpoint again to finish.",
    )


async def _probe_login_node(target) -> tuple[int, str]:
    """One cheap BatchMode SSH (`true`) to learn whether the login node will take our key right now.
    (rc, stderr): 0 = yes; 255 = ssh failed (the stderr names why: a second factor pending, host down, key
    refused). Used by the teardown gate when no shared connection is open, so the gate rests on EVIDENCE
    rather than the curated `auth_method` flag — a bring-your-own facility has none."""
    from .facility.remote import ssh_exec

    try:
        rc, _out, err = await ssh_exec(target, "true", timeout=20.0)
    except Exception as exc:  # noqa: BLE001 - timeout / no ssh binary: read as unreachable
        return 255, f"ssh: connect to host {getattr(target, 'host', '?')}: {type(exc).__name__}: {exc}"
    return rc, (err or "").strip()


async def _teardown_preauth_gate(app: AppCtx, eid: str) -> EndpointStatus | None:
    """Teardown is the one post-bootstrap op that MUST SSH the login node (`gce stop` + delete run there).
    On a one-time-code facility with no shared connection open, ask for the code BEFORE any SSH — the same
    handoff connect_facility uses — instead of letting `stop`/`delete` fail their BatchMode logins and then
    reporting "DELETE FAILED" about an endpoint that is in fact still running. The block release above has
    already gone over AMQP, so spend is halted before the user is asked for anything. None = proceed."""
    from .connect import _master_alive

    fac = app.facility
    target = getattr(getattr(fac, "cli", None), "target", None)
    if target is None or not getattr(target, "control_dir", None):
        return None  # no SSH control plane (a MEP), or multiplexing off: nothing to gate on
    if await asyncio.to_thread(_master_alive, target):
        return None  # the shared connection is open: every op below rides it
    if getattr(fac, "auth_method", None) != "mfa-otp":
        # Not flagged as a one-time-code facility — but the flag exists only on curated entries. Ask the login
        # node itself, once: a key that works means proceed; a denial that offers a second factor is the same
        # handoff; anything else is reported as a failed teardown with the endpoint still bound.
        rc, err = await _probe_login_node(target)
        if rc == 0:
            return None
        from .facility.remote import key_accepted_second_factor_pending

        if not key_accepted_second_factor_pending(err):
            return _teardown_failed(app, eid, err or "ssh failed", ssh_denial=True)
    facility = app.machine or "the facility"
    app.pending_preauth = (facility, target)
    app.preauth_resume = "teardown_endpoint()"
    handoff = _needs_preauth_result(facility, target, otp_ok=True)
    return EndpointStatus(
        status="up",  # the login-node manager is still running — nothing has been torn down yet
        block_state="cold",
        endpoint_id=eid,
        session_spend=_total_session_spend(app),
        notice=("block release dispatched; the login-node manager is STILL RUNNING — tearing it down needs an SSH "
                f"connection to the login node, which is not open. {handoff.notice} Then call teardown_endpoint "
                "again to finish (it may answer 'tearing_down' first: the login-node ops take a few minutes)."),
    )


@mcp.tool()
async def teardown_endpoint(ctx: Context) -> EndpointStatus:
    """FULLY tear down the login-node endpoint (gce stop + delete over SSH) — the rare 'destroy it'
    operation. **Normally do NOT call this.** The login endpoint is DESIGNED to stay online for
    zero-SSH reuse and costs nothing (a free login-node process, no allocation); `stop_endpoint`
    already halts ALL spend by releasing the billed block. Only call this when the user EXPLICITLY
    insists on removing the endpoint entirely. Afterwards, do not call run_shell (it re-provisions).
    The login-node ops can take a few minutes on a slow filesystem: a `tearing_down` status means they are
    still running — call teardown_endpoint again in about a minute to confirm `down`; call nothing else
    meanwhile. On a one-time-code facility the first call may instead ask for a code (`complete_preauth`)."""
    return await _teardown_endpoint(ctx.request_context.lifespan_context)


async def _login_shell(app: AppCtx, command: str) -> LoginShellResult:
    # No lock: read-only login-node command, independent of the provision/runner state machine.
    login_exec = getattr(app.facility, "login_exec", None)
    if login_exec is None and not _has_login_shape(app):
        return LoginShellResult(
            exit_code=1,
            notice="This facility is a compute-only multi-user endpoint: there is no SSH and no login "
            "node to shell into (the facility maps your Globus identity to a local account over "
            "AMQP). Use run_shell(shape='compute') — the block stays warm between calls.",
        )
    if login_exec is None:
        return LoginShellResult(
            exit_code=1,
            notice="No facility connected. Call connect_facility(facility, ssh_host=…) FIRST — it's "
            "the entry point: for a facility you've used before it reuses the endpoint over the web "
            "with ZERO SSH (no re-auth), and it decides whether SSH is even needed. Don't reach for "
            "login_shell or a manual SSH before that. (Or pin one via HPC_BRIDGE_MACHINE=<id>.)",
        )
    try:
        rc, out, err = await login_exec(command)
    except Exception as exc:  # noqa: BLE001 - never crash the tool; report structurally
        return LoginShellResult(exit_code=1, notice=f"login_shell error: {type(exc).__name__}: {exc}"[:300])
    return LoginShellResult(
        exit_code=rc,
        stdout=cap_output(out, app.max_output_chars),
        stderr_snippet=cap_output(err, app.max_output_chars),
    )


@mcp.tool()
async def login_shell(command: str, ctx: Context) -> LoginShellResult:
    """Run a READ-ONLY command on the HPC login node over a FRESH SSH connection — the
    cold-start discovery escape hatch (`sinfo`, `sacctmgr`, `echo $SCRATCH`) for when no
    endpoint exists yet. It provisions nothing, starts no scheduler job, costs no allocation.

    Prefer `run_shell(command, shape="login")` once an endpoint is up: that runs the same
    login-node command THROUGH the endpoint (over the network), avoiding a fresh SSH — which
    on an MFA facility can force a re-auth. SSH is meant to be a one-time bootstrap, not a
    channel. Only available for an SSH facility (a catalog machine via HPC_BRIDGE_MACHINE or
    connect_facility), not local dev."""
    return await _login_shell(ctx.request_context.lifespan_context, command)


async def _ready_session(app: AppCtx, shape: str, session_id: str) -> tuple[GlobusRunner, Session] | ShellOutcome:
    """The shared preamble of run_shell and reset_session: reject an unsupported shape, validate the
    session id, provision + bind the runner atomically under app.lock, and refuse to dispatch when
    the block is cold, the spend is unconfirmed, or a live task owns the session. Returns the runner
    and session, or the outcome to hand back."""
    if reject := _shape_reject(app, shape):
        return _shape_reject_outcome(reject)
    session = Session(session_id, app.scratch_root)  # validates session_id before provisioning
    busy = None
    async with app.lock:  # provision + bind the runner atomically (no race with a concurrent stop)
        not_warm = await _ensure_warm_runner(app, shape)
        runner = _shape_runtime(app, shape).runner
        if not_warm is None:
            busy = _busy_session(app, shape, session_id)
    if not_warm == "needs_confirmation":  # billed shape, spend not acknowledged -> don't dispatch
        return _needs_confirmation_outcome(app)
    if not_warm == "needs_account":  # account-required facility, no account -> don't dispatch, nothing started
        return _needs_account_outcome(app)
    if not_warm is not None:
        return _cold_outcome(not_warm, _shape_runtime(app, shape).last_canary)
    if busy is not None:  # a live task owns this session's cwd/env -> don't dispatch a second command
        return _busy_session_outcome(busy, shape, session_id)
    assert runner is not None  # _ensure_warm_runner returns None only after binding the runner
    return runner, session


async def _run_shell(
    app: AppCtx, command: str, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    ready = await _ready_session(app, shape, session_id)
    if isinstance(ready, ShellOutcome):
        return ready
    runner, session = ready
    wrapped = session_shell.wrap(command, session)
    fut = runner.submit(wrapped)  # submit; wait a bounded time OFF the lock, else hand back a handle
    try:
        res = await asyncio.to_thread(fut.result, runner.timeout)
    except TimeoutError:  # still running past the sync-wait -> a poll handle, NOT a kill
        async with app.lock:
            task_id = _register_task(app, shape, session_id, command, fut, runner.walltime)
            out = _running_outcome(app, task_id, runner.walltime)
            _note_dispatch(_shape_runtime(app, shape), out)  # the worker took our task -> it's alive
            return out
    except Exception as exc:  # noqa: BLE001 - translate ALL dispatch failures to a structured outcome
        out = dispatch.failure_outcome(exc, "warm", app.max_output_chars)
    else:
        out = dispatch.complete_outcome(res, "warm", app.max_output_chars)
    async with app.lock:
        _note_dispatch(_shape_runtime(app, shape), out)
        return _with_spend(app, out)


async def _reset_session(
    app: AppCtx, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    ready = await _ready_session(app, shape, session_id)
    if isinstance(ready, ShellOutcome):
        return ready
    runner, session = ready
    cmd = session_shell.reset_command(session)
    out = await dispatch.execute(
        cmd, runner, block_state="warm", max_output_chars=app.max_output_chars
    )
    async with app.lock:
        _note_dispatch(_shape_runtime(app, shape), out)
    return out


async def _poll_task(app: AppCtx, task_id: str, wait: float = 0.0) -> ShellOutcome:
    """Retrieve a running task's result (or report it still running). Optionally block up to `wait`
    seconds for it OFF the lock, then re-check under the lock.

    A pending future is only 'running' if something can still resolve it. If the endpoint behind the
    task is offline/gone (torn down by us or by someone else, a facility outage), the future never
    resolves and an agent would poll forever — seen live 2026-08-19: 25 polls over 20 minutes after
    another process deleted the endpoint. So a pending task on a dead endpoint is reported as a
    terminal `failed` (orphaned) and its handle dropped."""
    wait = max(0.0, min(wait, 600.0))  # a bounded courtesy wait; never an unbounded tool hang
    async with app.lock:
        resolved = _resolve_task(app, task_id)
        if resolved is not None:
            return resolved
        handle = app.tasks[task_id]  # resolved is None => the still-running handle is present
        fut, ceiling_s = handle.future, handle.ceiling_s
    if wait > 0:
        try:
            await asyncio.to_thread(fut.result, wait)
        except Exception:  # noqa: BLE001 - the re-resolve reads the true state (done / failed / timeout)
            pass
        async with app.lock:
            resolved = _resolve_task(app, task_id)
            if resolved is not None:
                return resolved
            still = app.tasks.get(task_id)  # re-read: a rebuild may have re-registered the task
            if still is not None:
                ceiling_s = still.ceiling_s
    # Still pending: can anything resolve it? (web call — off the lock; then claim under the lock)
    if await _endpoint_gone(app):
        async with app.lock:
            resolved = _resolve_task(app, task_id)  # it may have raced to done in the meantime
            if resolved is not None:
                return resolved
            if app.tasks.pop(task_id, None) is not None:
                return _orphaned_outcome(app, task_id)
    return _running_outcome(app, task_id, ceiling_s)


@mcp.tool()
async def run_shell(
    command: str, ctx: Context, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    """Run a shell command on the warm HPC compute block.

    `shape` picks the execution target on the same endpoint: "compute" runs on a
    scheduler block (heavy compute, billed, idle-released); "login" runs on the login
    node via a LocalProvider (lightweight, no allocation). Sessions (cwd/env) persist
    per session_id within a shape.

    LONG WORK: run it as a normal (foreground) command — do NOT background/detach it. A command
    still running past the sync-wait comes back phase="running" with a task_id; poll it with
    poll_task(task_id) until phase="complete". The task runs up to the block walltime and keeps the
    block warm while it runs, so it won't be cut or idle-released — but a *detached* process is not a
    task, so the block would idle-release out from under it (issue #21)."""
    try:
        return await _run_shell(
            ctx.request_context.lifespan_context, command, session_id, shape
        )
    except Exception as exc:  # noqa: BLE001
        return _error_outcome(exc)


@mcp.tool()
async def poll_task(task_id: str, ctx: Context, wait: float = 0.0) -> ShellOutcome:
    """Retrieve the result of a long task that run_shell returned as phase="running" (with a task_id).

    Returns phase="complete" (exit_code, stdout, stderr) once the task finishes, or phase="running"
    if it's still going — poll again. `wait` optionally blocks up to that many seconds for the result
    before returning (default 0 = check once and return now). The task runs up to the block walltime
    and the block stays warm while it runs, so a long job never needs detaching. An unknown or ended
    task_id returns a failed outcome explaining why (already retrieved, or the block was
    stopped/repointed)."""
    try:
        return await _poll_task(ctx.request_context.lifespan_context, task_id, wait)
    except Exception as exc:  # noqa: BLE001 - never crash the tool; return a structured failure
        return _error_outcome(exc)


@mcp.tool()
async def reset_session(
    ctx: Context, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    """Clear a session's persisted working directory and environment (fresh slate)."""
    try:
        return await _reset_session(
            ctx.request_context.lifespan_context, session_id, shape
        )
    except Exception as exc:  # noqa: BLE001 - never crash the tool; return a structured failure
        return _error_outcome(exc)


def main() -> None:
    mcp.run()
