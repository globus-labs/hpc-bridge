from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import os
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, NamedTuple, TypeVar

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations

from . import binding, config, connect, dispatch, login_gate, notices, resources, scheduler_ops, session_shell, warmth
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
    LeftBlock,
    SentCheck,
    ShapeRuntime,
    TaskHandle,
    _has_login_shape,
    _idle_release_s,
    _no_facility_bound,
    _supported_shapes,
)
from .cost import (  # noqa: F401 - re-exported
    _bank_warm_interval,
    _billable,
    _session_spend,
    _settle_billing,
    _total_session_spend,
    _with_spend,
)
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
    Submission,
)
from .notices import (  # noqa: F401 - re-exported
    _CLIENT_CANCELLED,
    _GLOBUS_USERNAME_RE,
    _NO_ACCOUNT_MARKERS,
    _SSH_AUTH_DENIED,
    _allocating_notice,
    _billed_bounds_note,
    _block_terms,
    _busy_session_outcome,
    _check_pending_notice,
    _check_words,
    _cold_outcome,
    _dispatch_error_suffix,
    _dropped_term_notice,
    _error_outcome,
    _explain_provision_error,
    _failed_release_notice,
    _hpc_bridge_error,
    _identity_from_error,
    _idle_window_text,
    _left_block_state,
    _left_job,
    _left_spend_stopped,
    _local_dill,
    _login_expired_notice,
    _login_notice,
    _needs_account_notice,
    _needs_account_outcome,
    _needs_confirmation_notice,
    _needs_confirmation_outcome,
    _needs_login_result,
    _needs_preauth_result,
    _never_sent_outcome,
    _no_account_failure,
    _no_account_notice,
    _no_cancel_notice,
    _no_facility_notice,
    _no_facility_outcome,
    _orphaned_outcome,
    _past_ceiling_outcome,
    _queue_word,
    _rejected_notice,
    _rejected_stop_notice,
    _rejected_switch_note,
    _released_under_outcome,
    _reserved_session_outcome,
    _running_outcome,
    _scheduler_submit_notice,
    _shape_reject_outcome,
    _size_refusal,
    _spend_floor_guidance,
    _stop_down_notice,
    _submit_rejected,
    _submit_rejected_notice,
    _switch_receipt,
    _terms_line,
    _transient_dispatch_failure,
    _with_submission_notice,
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
        endpoint_id = _env_endpoint_id()  # HPC_BRIDGE_ENDPOINT_ID: dispatch to that endpoint, never provision one
    except Exception as exc:  # noqa: BLE001 - a config error must NOT brick the MCP server at boot
        # (a startup crash = the agent silently sees no tools). Start unbound and let the
        # catalog tools surface/bind: list_facilities / connect_facility.
        print(
            f"hpc-bridge: facility setup failed at startup ({type(exc).__name__}: {exc}); starting "
            "unbound — use list_facilities / connect_facility to bind a machine.",
            file=sys.stderr,
        )
        # Unbound means unbound: no facility AND no endpoint. HPC_BRIDGE_ENDPOINT_ID may be the very UUID the failed pin
        # refused (a MEP entry's conflict) — dispatching to it, or stop/teardown acting on it, would be a silent guess.
        facility, endpoint_id = binding._default_local_facility(unbound=True), None
    scratch = binding._resolve_scratch_root(facility)
    app = AppCtx(
        facility=facility,
        profile=Profile(mode=_env_mode(), uep_idle_heartbeats=config.uep_idle_heartbeats()),  # type: ignore[arg-type]
        state=EndpointState(endpoint_id=endpoint_id),
        scratch_root=scratch,
        charge_factor=config.charge_factor(),
        login_flow=LoginFlow(),
        # the startup pin's default terms (registry or preset; recorded only by a pin that succeeded); None unpinned
        terms_origin=binding._PINNED_ORIGIN.get(config.machine() or ""),
    )
    try:
        yield app
    finally:
        await _shutdown(app)


async def _shutdown(app: AppCtx) -> None:
    """The server is going away: write what may still be queued on the bound endpoint to the record and wait (bounded)
    for every pending write — a server that starts again must see it (#24) — then close the runners."""
    eid = app.state.endpoint_id
    if eid is not None:
        warmth._persist_checks(app, eid)
    await warmth._flush_all(app, timeout=5.0)
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


# The tool-call journal (opt-in: HPC_BRIDGE_JOURNAL=<path>). One JSON line per call — tool, arguments, the result the
# host received, duration — written by the SERVER, so it is the same record whatever MCP host drives it (Codex, Pi,
# Hermes, Claude Code…). Grading a host from its own session format meant one fragile reader per host; this is also
# what to ask a user for when a session went wrong. One-time codes are redacted; the file is created 0600.
_JOURNAL_REDACT = {"complete_preauth": ("code",), "complete_login": ("code",)}


def _journal_result(res: Any) -> Any:
    if isinstance(res, tuple) and len(res) == 2 and isinstance(res[1], dict):
        res = res[1]  # (content, structured): the structured payload is the tool's own model
    if isinstance(res, dict):
        return res.get("result", res) if set(res) == {"result"} else res
    texts = [getattr(b, "text", None) for b in (res or [])]
    return "\n".join(t for t in texts if t)


def _journal_write(path: str, record: dict) -> None:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except OSError as exc:  # the journal is a record, never a reason a call fails
        print(f"hpc-bridge: journal write failed ({exc})", file=sys.stderr)


_JOURNAL_SEQ = itertools.count(1)


def _install_journal(server: FastMCP) -> None:
    tm = server._tool_manager
    inner = tm.call_tool

    async def call_tool(name: str, arguments: dict, *args: Any, **kwargs: Any) -> Any:
        path = os.environ.get("HPC_BRIDGE_JOURNAL", "").strip()
        if not path:
            return await inner(name, arguments, *args, **kwargs)
        safe = {k: ("<redacted>" if k in _JOURNAL_REDACT.get(name, ()) else v) for k, v in (arguments or {}).items()}
        t0 = time.monotonic()
        # `ts` / `seq` order calls by when they STARTED (the line is written when the call ends, and concurrent calls
        # finish out of order); `ts` also orders rows from several server processes sharing one journal
        record: dict[str, Any] = {"t": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "ts": round(time.time(), 6),
                                  "seq": next(_JOURNAL_SEQ), "pid": os.getpid(), "tool": name, "args": safe}
        try:
            res = await inner(name, arguments, *args, **kwargs)
        except BaseException as exc:
            record.update(error=f"{type(exc).__name__}: {exc}"[:1000], ms=int((time.monotonic() - t0) * 1000))
            _journal_write(path, record)
            raise
        record.update(result=_journal_result(res), ms=int((time.monotonic() - t0) * 1000))
        _journal_write(path, record)
        return res

    tm.call_tool = call_tool  # type: ignore[method-assign]


_install_journal(mcp)


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
    cores: int | None = None,
    memory_gb: int | None = None,
    gpus: int | None = None,
    nodes: int | None = None,
    whole_node: bool | None = None,
) -> EndpointStatus:
    if _no_facility_bound(app):  # nothing pinned or connected: never stand up a local endpoint in its place
        return EndpointStatus(status="down", block_state="cold", endpoint_id=app.state.endpoint_id,
                              notice=_no_facility_notice(app))
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
    # the block's size (hpc_bridge.resources): cores, memory and GPUs per node, the node count, whole nodes or not
    sizing, bad_size = resources.requested(cores, memory_gb, gpus, nodes, whole_node)
    login_only = shape == "login"  # the login shape is free and has no size: a size named for it is said ignored
    if bad_size and not login_only:
        async with app.lock:
            return _dropped_term_status(app, shape, f"{bad_size}. Nothing in this call was set or started.")
    if login_only:
        sizing = {}
    if partition is not None or account is not None or sizing:
        # a facility endpoint whose schema has no such setting drops it at the wire: never set what is not sent. A
        # startup-pinned one has not read its template yet (that happens at its first block): read it first.
        load = getattr(app.facility, "load_template", None)
        if inspect.iscoroutinefunction(load):
            await load()
        dropped = next((n for k, v in (("partition", partition), ("account", account))
                        if v is not None and (n := _dropped_term_notice(app, k, v))), None)  # all or nothing
        takes_key = getattr(app.facility, "takes_size_key", None)
        if (not _has_login_shape(app) and callable(takes_key) and sizing.get(resources.WHOLE, False) is None
                and (sizing.get(resources.CORES) or sizing.get(resources.MEM)) and takes_key(resources.WHOLE)):
            # on a facility endpoint a slice is only a slice when `exclusive: false` reaches its template (parsl's
            # default is the whole node): cores= / memory_gb= send it where the template takes the key
            sizing[resources.WHOLE] = False
        if not dropped and (sizing or partition is not None) and not login_only:
            # the size this call asks for, on the partition it names (or the session's), checked against what
            # discovery or the registry saw of it — a slice of a whole-node partition, more than a node has, over a QOS
            # per-job cap — and against what the facility's template renders; nothing is set unless all of it holds
            base = app.shapes[shape].user_endpoint_config if shape in app.shapes else warmth._seed_uec(app, shape)
            after = {**base, **({"partition": partition} if partition is not None else {})}
            for k, v in sizing.items():
                if v is None:
                    after.pop(k, None)
                else:
                    after[k] = v
            if after.get("compute"):
                named_now = set(sizing) | ({"partition"} if partition is not None else set())
                dropped = _size_refusal(app, after, sizing, named_now)
        if dropped:
            async with app.lock:
                return _dropped_term_status(app, shape, dropped)
    async with app.lock:  # serialize provisioning/state mutation across concurrent tool calls
        rt = _shape_runtime(app, shape)
        # A login shape has no partition; surface that we ignored a supplied one rather than
        # silently dropping the user's selection.
        ignored = partition is not None and not rt.user_endpoint_config.get("compute")
        ignored_size = [a for a, v in (("cores", cores), ("memory_gb", memory_gb), ("gpus", gpus),
                                       ("nodes", nodes), ("whole_node", whole_node))
                        if v is not None and not rt.user_endpoint_config.get("compute")]
        # what THIS call named for the block (BlockTerms' "this_call"); the login shape has neither
        named = {k for k, v in (("partition", partition), ("account", account))
                 if v is not None and rt.user_endpoint_config.get("compute")}
        if rt.user_endpoint_config.get("compute"):
            named |= set(sizing)
        epoch = rt.block_epoch
        reject = (_apply_partition(app, shape, rt, partition) or _apply_account(app, shape, rt, account)
                  or warmth._apply_resources(app, shape, rt, sizing))
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
        # A switch confirmed in this same call: no needs_confirmation answer said what became of the old block, so the
        # answer of the call that confirms the new one does, once.
        receipt = _switch_receipt(app, rt) if confirm_spend and rt.block_epoch != epoch else ""
        switched_from = rt.switched_terms  # the terms that receipt moved from, before a confirmation clears them

        def said(notice: str | None) -> str | None:
            if receipt and rt.spend_confirmed:  # the confirmation held: what it is for, and the old block's fate
                return receipt + (notice or "")
            if receipt and rt.rejected is not None and notice:
                # this call's own check met a rejection (warmth._void_rejected): no new block exists, so not the
                # receipt ("…this new one included") — only what became of the old terms' block, then the rejection
                return _rejected_switch_note(app, rt, switched_from) + notice[:1].upper() + notice[1:]
            return notice

        was_confirmed = rt.spend_confirmed  # after a switch dropped it: this call's own confirmation is what is new
        if confirm_spend and named and rt.user_endpoint_config.get("compute"):
            rt.question_size = notices._size_key(app, rt.user_endpoint_config)  # the terms this call named itself
        try:
            # force_canary: a status probe must re-verify the worker (and kick a cold block),
            # never trust the TTL — that's exactly the cold-start gap callers are asking about.
            block = await warmth._provision(app, shape, force_canary=True, confirm_spend=confirm_spend)
        except Exception as exc:  # noqa: BLE001 - provisioning unavailable (e.g. non-Linux host)
            return EndpointStatus(
                status="down",
                block_state="cold",
                endpoint_id=app.state.endpoint_id,
                session_spend=_total_session_spend(app),  # _provision stopped the clock: a down leaves none running
                partition=active_partition,
                account=active_account,
                notice=said(_hpc_bridge_error(exc)),
            )
        if block == "needs_confirmation":  # the spend floor — nothing started by this call (a reap's check may have)
            terms = _block_terms(app, rt, named=named)
            rt.question_size = notices._size_key(app, rt.user_endpoint_config)  # the size this question names
            return EndpointStatus(
                status="needs_confirmation",
                block_state="provisioning" if rt.reap_kicked else "cold",
                endpoint_id=app.state.endpoint_id,
                session_spend=_total_session_spend(app),  # a reap this call found has just banked the old block
                partition=active_partition,
                account=active_account,
                block=terms,
                notice=_needs_confirmation_notice(app, rt, terms),
            )
        if block == "needs_account":  # the account floor (account_required facility, no account) — nothing was started
            terms = _block_terms(app, rt, named=named)
            return EndpointStatus(
                status="needs_account",
                block_state="cold",
                endpoint_id=app.state.endpoint_id,
                session_spend=_total_session_spend(app),
                partition=active_partition,
                account=None,
                block=terms,
                notice=_needs_account_notice(app, terms),
            )
        billable = _billable(rt)
        # The receipt: the call that newly confirmed a billed block names what it confirmed, so a one-call start (no
        # needs_confirmation answer before it) still shows its queue, account and size. A poll that passes
        # confirm_spend=True again on a block already confirmed confirmed nothing new: no receipt.
        receipt_terms = (_block_terms(app, rt, named=named)
                         if confirm_spend and billable and not was_confirmed and rt.spend_confirmed else None)
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
            stale = getattr(app.facility, "stale_worker_note", lambda: None)()
            job, verified = warmth._known_block_job(app, rt)
            notice = _allocating_notice(active_partition, provisioning_elapsed, facility_mep=not _has_login_shape(app),
                                        stale=stale, job=job, job_verified=verified,
                                        scheduler=getattr(app.facility, "scheduler", None), user=app.worker_user)
            if rt.transient_conflicts >= TRANSIENT_CONFLICT_LIMIT:
                rt.provisioning_since = None
                return EndpointStatus(
                    status="down", block_state="cold", endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account,
                    notice=said(f"the endpoint refused to start for this identity {rt.transient_conflicts} times in a row "  # noqa: E501
                            f"(RESOURCE_CONFLICT: 'already in use … concurrent requests'). This is NO LONGER transient: "  # noqa: E501
                            "another session with the SAME Globus identity is starting or holding a user endpoint here "
                            "(a concurrent hpc-bridge run?), or the facility's manager is wedged. Stop retrying: end the "  # noqa: E501
                            "other session or wait a few minutes, then call ensure_endpoint_up again. Nothing was started."),  # noqa: E501
                )
            if rt.last_canary is not None and rt.last_canary.login_expired:
                # Globus Auth refused to refresh the stored login: nothing can be sent until the user logs in again.
                # Not "allocating nodes…", not a config fault (live 2026-10-14). The block's state is not known, so it
                # stays `provisioning`, never a `down` that could hide a block still billing.
                rt.provisioning_since = None  # the cold-start clock restarts once work can be sent again
                return EndpointStatus(
                    status="provisioning", block_state=block, endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account, block=receipt_terms,  # what it confirmed
                    notice=said(_login_expired_notice(rt.last_canary.error)),
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
                    notice=said(_no_account_notice(app, rt.last_canary.error, identity)),
                )
            if rt.last_canary is not None and _submit_rejected(rt.last_canary.error):
                # The scheduler refused the submission (bad account/partition/QOS, missing resource request): a
                # terminal `down` for THIS config, so the agent stops polling and changes it — not "allocating
                # nodes…" with the cause buried in a suffix (live 2026-09-09: five polls before anyone read it).
                # With one block, _provision has voided the spend confirmation (warmth._void_rejected): the next
                # call without confirm_spend asks again, and a stop expects no pilot from it.
                rt.provisioning_since = None
                return EndpointStatus(
                    status="down", block_state="cold", endpoint_id=eid, session_spend=spend,
                    partition=active_partition, account=active_account,
                    notice=said(_rejected_notice(app, rt, rt.last_canary)),
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
        if ignored_size:
            notice = (f"{notice or ''} (the login shape is the login node itself, with no block to size; ignored "
                      f"{', '.join(ignored_size)})").strip()
    # OUTSIDE the lock (dispatch takes it): for a still-cold BILLED block, ask the scheduler whether
    # the pilot actually submitted. A rejected/held qsub is otherwise indistinguishable from a normal
    # queue wait, leaving the caller stuck on "allocating nodes…" forever ([#32]). The grace clock
    # keeps a not-yet-visible pilot during normal cold-start from being cried as rejected.
    # (The query rides the free login shape — a compute-only facility has none, so skip it there; the
    # #37 failure-signal path for a MEP is the dispatch-error suffix already on the notice.)
    # the block's scheduler job: the one its check answered from, once it is up; while it provisions, the newest pilot
    # the probe lists for this endpoint (the one its verdict names)
    job_id = rt.block_job if status == "up" and billable else None
    if status == "provisioning" and billable and eid and _has_login_shape(app):
        # what the probe is for, read before it runs off the lock: a concurrent switch bumps `block_epoch`
        # (and when a worker last answered, so the record can tell one that answered DURING the probe)
        probe_epoch, answered = rt.block_epoch, rt.warm_confirmed_at
        # the block request names its user endpoint's log, where the probe may read a refusal's own words (quoted only:
        # a log read is not conclusive, so nothing is voided on it — the block stays `provisioning`)
        notice, probe = await scheduler_ops._augment_provisioning(app, eid, notice, provisioning_elapsed,
                                                                  _login_runner(app), confirmed_at=rt.confirmed_at,
                                                                  block=rt.user_endpoint_config.get("hpcb_block"),
                                                                  confirmed_wall=rt.confirmed_wall)
        if probe is not None and probe.category != "unknown":
            job_id = probe.subject
            async with app.lock:
                # what is still queued or HELD, for a switch or a reconnect to say (a held pilot never idle-releases)
                warmth._record_pilots(app, shape, rt, eid, probe_epoch, probe.live, probe.subject)
                if probe.dead is not None:
                    # A pilot that ran and died before any worker answered: the check it was started for is still
                    # queued, and the endpoint can submit pilots for it. Recorded to NAME it (a stop says what became of
                    # that check, which its own state decides) — on the runtime and terms the probe was for (live
                    # 2026-10-15, slurm_worker_died).
                    warmth._record_dead_pilot(app, shape, rt, eid, probe_epoch, answered, *probe.dead, probe.jobs)
    return EndpointStatus(
        status=status,
        block_state=block,
        endpoint_id=eid,
        session_spend=spend,
        partition=active_partition,
        account=active_account,
        block_job_id=job_id,
        block=receipt_terms,
        notice=said(notice),
    )


def _dropped_term_status(app: AppCtx, shape: str, dropped: str) -> EndpointStatus:
    """A refused partition/account (the facility would drop it at the wire): nothing changed, so the answer is the
    shape's standing state, never a `down` that would read as the block gone while it bills. A confirmed block is `up`
    (or `provisioning` until first seen warm) on its current terms; an unconfirmed shape is the spend question for
    those terms. No probe: a refusal sends nothing."""
    rt = _shape_runtime(app, shape)
    terms = _block_terms(app, rt) if _billable(rt) else None
    uec = rt.user_endpoint_config
    # the refusal says what to send again; the current terms follow — never "ask which account": the call carried it
    stood = f" {_terms_line(app, terms, lead='The current terms')}" if terms is not None else ""
    if rt.spend_confirmed or terms is None:
        warm = rt.block_since is not None or rt.warm_confirmed_at is not None
        return EndpointStatus(status="up" if warm else "provisioning", block_state="warm" if warm else "provisioning",
                              endpoint_id=app.state.endpoint_id, session_spend=_total_session_spend(app),
                              partition=uec.get("partition"), account=uec.get("account"),
                              notice=(dropped + stood).strip())
    return EndpointStatus(status="needs_confirmation", block_state="provisioning" if rt.reap_kicked else "cold",
                          endpoint_id=app.state.endpoint_id, session_spend=_total_session_spend(app),
                          partition=uec.get("partition"), account=uec.get("account"), block=terms,
                          notice=(dropped + stood).strip())


_T = TypeVar("_T")
# How often a long tool call reports progress. Pi's MCP client gives EVERY request 60 s and restarts that timer on a
# progress notification for it (pi-mcp client.js handleProgress); without one, a 120 s run_shell sync-wait or a
# first SSH bootstrap was cut off by default. Other clients (Codex, Hermes) send no progress token or only log it —
# for them `report_progress` is a no-op.
_HEARTBEAT_S = 15.0


async def _heartbeat(ctx: Context, work: Awaitable[_T], label: str) -> _T:
    """Await `work`, sending an MCP progress notification every `_HEARTBEAT_S` while it runs. A cancelled call cancels
    the work coroutine (the semantics of awaiting it directly) — which does not cancel a command it already sent to the
    endpoint: an agent's run_shell / reset_session keeps tracking that command (`_dispatch_counted`)."""
    task = asyncio.ensure_future(work)
    t0 = time.monotonic()
    beats = 0
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=_HEARTBEAT_S)
            if done:
                return task.result()
            beats += 1
            try:
                await ctx.report_progress(beats, None, f"{label}: still working ({int(time.monotonic() - t0)} s)")
            except Exception:  # noqa: BLE001 - progress is a courtesy; it must never fail the call
                pass
    except asyncio.CancelledError:
        task.cancel()
        raise


@mcp.tool()
async def ensure_endpoint_up(
    ctx: Context,
    shape: str = DEFAULT_SHAPE,
    partition: str | None = None,
    confirm_spend: bool = False,
    account: str | None = None,
    cores: int | None = None,
    memory_gb: int | None = None,
    gpus: int | None = None,
    nodes: int | None = None,
    whole_node: bool | None = None,
) -> EndpointStatus:
    """Ensure the personal HPC endpoint is up; report whether its pilot block is warm.

    Pass `partition` (from the discovery selection gate) to provision the scheduler block onto that
    partition; the choice persists for the session until changed (stop_endpoint keeps it; a new
    connect_facility or a teardown forgets it). Omit it to keep the facility default. Ignored for
    shape="login" (a login-node LocalProvider has no partition).

    Pass `account` (the allocation the user named, or chose from connect_facility's listing where it had one)
    to charge the scheduler block to it; like `partition`, it persists for the session and is ignored for
    shape="login".

    `confirm_spend` is the deterministic budget floor: a scheduler compute block will not start until
    you pass confirm_spend=True (once the user has agreed to the block's terms — see the
    driving-hpc skill). Without it the call returns status="needs_confirmation" and provisions
    nothing. Every spend answer carries `block`: the block's queue and account (each with where it came from),
    nodes and walltime — name them to the user; a confirming call's result carries it too, as a receipt.
    The acknowledgement covers the block it starts: it holds while that block lives, and once the
    block is gone (idle-released, past its walltime, or cancelled) the next call returns needs_confirmation
    with the reason — even one that passes confirm_spend=True — so the user is asked again for the new block.
    A different partition or account is a different block too: the call that changes them answers
    needs_confirmation (saying what becomes of the old block) unless it passes confirm_spend=True itself.
    Not needed for shape="login" (free).

    The block's size, per node — `cores`, `memory_gb` (GB), `gpus` — and `nodes` ask for what the work needs: a slice of
    a node, not a whole one, unless `whole_node=True` or the partition hands out only whole nodes. With none of them on
    a per-core partition the terms PROPOSE a modest slice (a few cores and their memory) for the user to confirm or
    change. `nodes` > 1 is one shell over the whole allocation, on the first node, like salloc: run work across the
    nodes with srun from it; nodes not used still bill. Like `partition`, the size persists for the session, and a
    change is a different block (spend is asked again). The terms never claim a slice they cannot show: where nothing
    shows a partition shares nodes they say a whole node (parsl's default), and its cost. A size the partition cannot
    give (more than a node has, over a QOS per-job cap, a slice of a whole-node partition — on a facility's endpoint
    too) or that the facility's template does not take, and several nodes on a facility's endpoint, are refused before
    anything is submitted, saying why. Ignored for shape="login"."""
    return await _heartbeat(ctx, _ensure_endpoint_up(
        ctx.request_context.lifespan_context, shape, partition, confirm_spend, account, cores, memory_gb, gpus, nodes,
        whole_node,
    ), "ensure_endpoint_up")


class RegistryUnavailable(RuntimeError):
    """list_facilities could not read the registry — the network or the Search service, not an empty index."""


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
            # an error, never an empty list: `[]` read as "no facilities" while Globus Search was down for
            # maintenance (2026-10-24) — the agent had to probe the index itself to tell the two apart
            raise RegistryUnavailable(
                f"the facility registry (a Globus Search index) is unavailable — {binding.registry_outage(exc)}. "
                "This is not an empty registry: list_facilities again in a few minutes. A cluster you bring still "
                "works meanwhile (connect_facility with ssh_host=…).") from exc
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
    return await _heartbeat(ctx, login_gate._authenticate(ctx.request_context.lifespan_context, force=force, mode=mode),
                            "authenticate (waiting for the Globus login)")


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
    return await _heartbeat(ctx, _complete_preauth(ctx.request_context.lifespan_context, code), "complete_preauth")


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
                                    "the session in their own terminal with preauth_command. If it was their "
                                    "password, it is now in the transcript: tell them to change it, and never "
                                    "repeat it.")
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


# MCP tool annotations. Codex lets a tool skip its approval prompt only when it is read-only (or non-destructive AND
# closed-world, which no hpc-bridge tool is: they all reach a remote facility). Only the two that just read say so;
# the spend gate is enforced by the server regardless of a host's approvals.
_READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
_DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=True)


@mcp.tool(annotations=_READ_ONLY)
async def list_facilities(query: str = "") -> list[CatalogSummary]:
    """List the HPC machines hpc-bridge can stand up, from the public facility registry (a Globus
    Search index, read anonymously — works with no login). Empty query lists all; a query filters by
    name/description.

    When the registry cannot be read (the network, or a Globus Search outage), it fails with the service's reason
    rather than an empty list — an outage is not "no facilities".

    Returns agent-safe summaries (no executable config or raw UUIDs). Pick one and call
    connect_facility(facility=…) to bind it and see the terms its first block would start on. No SSH, no
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
    """Select an HPC facility and bring up its (free) login node, and state the terms its first billed block would
    start on (`block`: the default queue and its source, whether an account is needed and where to find it, size and
    walltime) — nothing is billed here.

    **This is the ENTRY POINT for reaching any facility — ALWAYS call it first** (before login_shell,
    and before reasoning about SSH/Duo yourself): it decides whether SSH is even needed. Don't
    pre-check for an SSH master or assume a password/Duo is required — call this and let it tell you.

    **Reconnecting to a facility you've used before? Pass its `ssh_host`.** connect_facility resolves
    the config from the LOCAL cache (a previously-confirmed BYO facility) with **no SSH probe**, then
    reuses the still-online endpoint over the web (`reused: true`) — a **fully zero-SSH reconnect, no
    re-auth**. So a known MFA facility reconnects with NO Duo prompt while its endpoint is up.

    `facility` is an id/subject/alias from list_facilities() (e.g. "anvil"). This binds the facility,
    stands up the login shape (SSH cold-bootstrap once, or reuse an online endpoint — no scheduler
    account needed), lists your allocations where the facility has a listing it can read (otherwise the
    account is the user's to name), and returns phase="needs_account". Before the first block ask the
    user, in one question, for what they have not said (the account, whether the queue is OK): their yes
    is the spend confirmation, sent with their answer in one ensure_endpoint_up call.
    phase="provisioning" ⇒ login node still warming — call again shortly.

    NOT in the catalog and not cached → discover, don't interrogate. Pass `ssh_host` (login
    host/alias; SSH user+key come from the environment) and the tool PROBES the login node →
    phase="proposed_facility_details" with a draft — review/correct it with the user (above all
    `interface`), then call again with details=… (leave env_setup out: the server keeps the proposed one) to
    register the session facility (then CACHED for
    zero-SSH reconnects; the canary validates). phase="needs_preauth" ⇒ the host needs a one-time
    interactive login (password/MFA) — relay its `preauth_command` for the user to run in THEIR OWN
    terminal; never handle the secret. neither ssh_host nor details ⇒ needs_facility_details."""
    app = ctx.request_context.lifespan_context
    return await _heartbeat(ctx, _connect_facility(app, facility, ssh_host=ssh_host, details=details),
                            "connect_facility")


def _block_work(app: AppCtx, *, then: str) -> tuple[str, str] | None:
    """What of ours holds the compute block, and what to do about it — or None when nothing does.

    Holding it: a task running behind a poll handle (including one whose call the client cancelled, and one whose
    block was released under it — a sent task runs on), or a run_shell / reset_session still inside its sync-wait
    (`rt.inflight`: no handle yet, but its command is on the block now). A task past its ceiling still holds it: it may
    only be queued behind a relaunching block. Except on a facility endpoint, where nothing could ever end a lost one:
    there a task presumed lost (`warmth._overdue`) does not hold it. Returns (`what`, `action`): what holds it, and the
    steps that end with "call {then}"."""
    rt = app.shapes.get(DEFAULT_SHAPE)
    mep = not _has_login_shape(app)
    now = time.monotonic()
    live = [(tid, h) for tid, h in _live_task_handles(app, DEFAULT_SHAPE) if not (mep and warmth._overdue(h, now))]
    # calls in their sync-wait: on the shape's runtime, or on one dropped under them (a teardown's release, a re-bind)
    # whose endpoint is still the bound one — after a teardown or a re-bind elsewhere that work is abandoned
    eid = app.state.endpoint_id
    released = [r for r in app.released_inflight if r.runner is not None and r.runner.endpoint_id == eid]
    waiting = [r for r in ([rt] if rt is not None else []) + released if r.inflight]
    inflight = sum(r.inflight for r in waiting)
    if not live and not inflight:
        return None
    block = "the block" if mep else "the compute block"
    # the latest any of it can still be running: a task is killed at its ceiling (counted from its start — it may queue
    # first); a call in its sync-wait has at most its full ceiling ahead of it
    left = max([h.ceiling_s - (now - h.submitted_at) for _, h in live]
               + [_task_ceiling_s(r.user_endpoint_config) for r in waiting])
    if left >= 1:
        bound = f"at most ~{int(left)}s more"
    else:
        bound = "past its ceiling with no result — queued behind another task or a relaunching block, or lost"
        if mep:
            lost_in = max(h.ceiling_s + warmth._LOST_TASK_GRACE_S - (now - h.submitted_at) for _, h in live)
            bound += f"; with none within ~{int(lost_in)}s it is presumed lost and no longer holds the block"
    held = []
    if live:
        held.append(f"task(s) {', '.join(tid for tid, _ in live)} are still running on {block}")
        cancelled = [tid for tid, h in live if h.client_cancelled]
        if cancelled:
            held[-1] += f" ({', '.join(cancelled)}: {_CLIENT_CANCELLED})"
    if inflight:
        held.append(f"a run_shell or reset_session call is still inside its sync-wait, its command on {block} now"
                    if inflight == 1 else
                    f"{inflight} run_shell / reset_session calls are still inside their sync-wait, their commands on "
                    f"{block} now")
    steps = []
    if inflight:
        steps.append(f"Let {'that call' if inflight == 1 else 'those calls'} return (within ~{int(SYNC_WAIT_S)}s "
                     f"{'it' if inflight == 1 else 'each'} hands back its result, or a task_id to poll_task)")
    if live:  # a tool name opens the sentence in lower case, as everywhere else
        steps.append("poll_task " + ("them" if len(live) > 1 else "it") + " to completion (results stay retrievable)")
    what = " and ".join(held)
    action = f"{' and '.join(steps)}, then call {then}."
    if mep:
        what += (". On a facility multi-user endpoint hpc-bridge has NO cancel channel — nothing here can end that "
                 "work: the block stays busy (billing) until it finishes" + (f", {bound}" if left >= 1 else
                                                                            f". It is {bound}"))
    else:
        what += f", {bound}" if left >= 1 else f"; it is {bound}"
        action += " To abandon that work and remove everything, teardown_endpoint."
    return what, action


def _lost_note(app: AppCtx, *, op: str) -> str:
    """On a facility endpoint: the tasks a stop or detach abandons because they are presumed lost (`warmth._overdue`),
    said plainly — their handles go with the shape, and one that was only queued may keep the facility relaunching."""
    lost = [tid for tid, h in _live_task_handles(app, DEFAULT_SHAPE) if warmth._overdue(h)]
    if not lost:
        return ""
    return (f"Task(s) {', '.join(lost)} had no result long past their ceiling and are presumed lost, so they did not "
            f"hold this {op}: they are abandoned (their results can no longer be retrieved here), and one that was "
            "only queued counts as a task of ours still queued there. ")


def _busy_block_refusal(app: AppCtx, eid: str, *, op: Literal["stop", "detach"] = "stop") -> EndpointStatus | None:
    """`stop_endpoint` (either kind) and a facility-MEP detach REFUSE while our work holds the compute block
    (`_block_work`). On an SSH endpoint releasing the block does not end that work — the endpoint relaunches a block for
    it (fake-cluster chaos `stop_while_running`, 2026-09-05); on a MEP nothing here can end it. Either way, dropping the
    shape would lose the result and leave the block billing behind a 'down'/'draining'. Callers hold app.lock, so a
    dispatch holding it (provisioning, its canary) has been counted before the look. None = nothing holds the block."""
    work = _block_work(app, then="stop_endpoint" if op == "stop" else "teardown_endpoint")
    if work is None:
        return None
    what, action = work
    why = ("" if not _has_login_shape(app) else
           ". Releasing the block now would not end that work — the endpoint would relaunch a block to run it and "
           "spend would continue after a false 'down'")
    rt = app.shapes.get(DEFAULT_SHAPE)
    return EndpointStatus(  # "cold" when the shape holding the work was already released under it
        status="up", block_state="warm" if rt is not None else "cold", endpoint_id=eid,
        session_spend=_total_session_spend(app), block_job_id=rt.block_job if rt is not None else None,
        spend_stopped=False,  # refused: the block runs on with our work on it
        notice=f"can't {op} yet: {what}{why}. {action}",
    )


async def _stop_mep(app: AppCtx, eid: str | None) -> EndpointStatus:
    """Stop on a facility-run multi-user endpoint: **draining-only, never 'down' — and NO cancel is issued**.

    We own neither the manager nor a login channel, and the Globus SDK offers no foreign-endpoint
    cancel (ComputeFuture.cancel is pre-run only; stop/delete_endpoint act on OUR registration). A block cancelling
    itself (a last task running `scancel $SLURM_JOB_ID`) was rejected (#178): the task stays outstanding until the
    manager notices the worker is gone, so parsl starts a replacement block, which then idles out its window — and it
    could kill a sibling session's block. So the honest thing (#24, #178) is: stop submitting, drop the shape so no
    further work lands on the block, and SAY what is left billing — the block's scheduler job (resolved in this
    locked step, before the shape goes: `warmth._leave_block`), the latest it can bill until, and the user's own
    command to end it now — remembering it (`app.left_blocks`) so a second stop, or a stop after a reconnect, still
    names it. `draining` here is TERMINAL: re-polling stop will never yield `down`. `eid` None: nothing is bound any
    more (a detach), but blocks it left may still bill."""
    async with app.lock:  # look and drop in ONE step: no dispatch can be counted in between
        lost, current = "", None
        if eid is not None:
            if busy := _busy_block_refusal(app, eid):  # our work holds the block, and nothing here can end it
                return busy
            lost = _lost_note(app, op="stop")
            rt = app.shapes.get(DEFAULT_SHAPE)
            current = warmth._leave_block(app, DEFAULT_SHAPE, rt, how="stop") if rt is not None else None
            warmth._drop_compute_shape_locked(app)  # its spend stays in the session total (app.released_spend)
        return _left_blocks_status(app, eid, current, lost, op="stop", spent=_total_session_spend(app))


def _left_blocks_here(app: AppCtx, eid: str | None, current: LeftBlock | None) -> list[LeftBlock]:
    """The blocks a facility stop or detach reports: this call's own first, then the ones earlier stops, detaches,
    reconnects and switches left on the same endpoint (every one, with nothing bound), newest first — those that can
    still be running (`warmth._prune_left`)."""
    rest = [b for b in reversed(warmth._prune_left(app)) if b is not current and (eid is None or b.endpoint_id == eid)]
    return ([current] if current is not None else []) + rest


def _left_blocks_status(app: AppCtx, eid: str | None, current: LeftBlock | None, lost: str, *,
                        op: Literal["stop", "detach"], spent: float) -> EndpointStatus:
    """The answer of a facility-endpoint stop (`draining`, final) or detach (`down` — for OUR state only): no cancel was
    issued, and the notice and fields say what is left billing (#178). Callers hold app.lock."""
    now = time.monotonic()
    blocks = _left_blocks_here(app, eid, current)
    return EndpointStatus(
        status="draining" if op == "stop" else "down",
        block_state=_left_block_state(blocks, now),  # its presumed state: never 'cold' while it may run
        endpoint_id=eid,
        session_spend=spent,
        block_job_id=_left_job(blocks, now),
        spend_stopped=_left_spend_stopped(blocks, now),
        notice=_no_cancel_notice(app, blocks, now, current=current, lost=lost, op=op),
    )


# The login-shape session the scheduler ops (block release, pilot probe, allocation listing) run in — never one of the
# agent's: an agent command still running in its own session must not make the stop's scancel "busy", and an internal
# op that outlives its sync-wait must not occupy the agent's session. The prefix is reserved: an agent call may not
# use it (`_reserved_session_outcome`), or its command could make the scheduler ops wait.
_RESERVED_SESSION_PREFIX = "_hpc-bridge"
_INTERNAL_SESSION = f"{_RESERVED_SESSION_PREFIX}-internal"


def _login_runner(app: AppCtx):
    """The free login-shape channel the scheduler ops ride, INJECTED into scheduler_ops so it needn't
    import server. Resolves `_run_shell` at call time from this module's namespace, so a test that
    patches `server._run_shell` still reaches every scheduler op. Runs in `_INTERNAL_SESSION`, and is not
    an agent call: cancelled, its command is not tracked."""
    async def run(cmd: str) -> ShellOutcome:
        return await _run_shell(app, cmd, session_id=_INTERNAL_SESSION, shape="login")
    return run


async def _stop_endpoint(app: AppCtx) -> EndpointStatus:
    """Release the compute block over the **login endpoint (AMQP)** and LEAVE the manager online for
    reuse. "Stop" means *stop spending*, not destroy the endpoint: the login-node manager is the
    whole point — it persists so the next session reuses it with **zero SSH** ([[Standing up the
    endpoint|SSH-once]], #12). Fully pulling the endpoint down (`gce stop`, the facility's
    `teardown()`) is a separate, rarer operation, not done here."""
    eid = app.state.endpoint_id
    if eid is None and not _has_login_shape(app) and warmth._prune_left(app):
        return await _stop_mep(app, None)  # detached, but a block it left may still bill: name it again (#178)
    if eid is None:
        return EndpointStatus(status="down", block_state="cold", notice="no endpoint was up")
    if not _has_login_shape(app):  # a facility MEP: no release channel exists — drain honestly
        return await _stop_mep(app, eid)
    # The whole call is bounded (config.STOP_CALL_BUDGET_S): under the tool timeouts of the clients that drive it —
    # the waits for the lock, the releases, the lookups and the liveness check included.
    call_deadline = time.monotonic() + config.STOP_CALL_BUDGET_S
    # Our own work on the block refuses the stop at once — before any wait on the record below (a slow home directory
    # must not let a command still in its sync-wait turn into a handle first, nor delay the answer that names it).
    if not await _acquire_within(app, call_deadline):
        return _lock_busy_status(app, eid, released=None)
    try:
        if busy := _busy_block_refusal(app, eid):
            return busy
    finally:
        app.lock.release()
    # the record of our tasks on disk, read off the loop (a hung home directory must not freeze the server) — after this
    # process's own pending writes, so it reads what this process knows
    await warmth._flush_all(app, timeout=min(5.0, call_deadline - time.monotonic()))
    disk = await warmth._read_checks(eid, min(5.0, call_deadline - time.monotonic()))
    if not await _acquire_within(app, call_deadline):
        return _lock_busy_status(app, eid, released=None)
    try:  # under the lock: a run_shell provisioning right now has counted its dispatch before we look
        if busy := _busy_block_refusal(app, eid):  # releasing would not end our work: the endpoint relaunches for it
            return busy
        rt_pre = app.shapes.get(DEFAULT_SHAPE)
        rejected = rt_pre.rejected[0] if rt_pre is not None and rt_pre.rejected is not None else None  # its cause
        # The evidence (#24): every task of ours sent to a compute user endpoint that has not been seen to finish — the
        # current runner's checks, the ones the endpoint kept from runners a stop, switch, rebuild or re-bind retired, a
        # command whose result the client lost, and those a restarted server reads back from disk. While one is queued
        # the endpoint can start a block to run it: after a cancel parsl submits another block for a task still
        # outstanding (live 2026-10-16, `queued_check_relaunch`: "released 4", `down`, job 5 submitted 53 s later). So
        # `down` needs the release confirmed AND every one resolved (or never sent), whatever the shape's own state
        # says. (Our own commands with a live handle are the busy refusal's: a stop never releases under them.)
        checks, store_problem = warmth._our_checks(app, eid, rt_pre, disk)
        # On a fresh compute user endpoint (warmth._fresh_block) a cancelled or dead first pilot makes parsl give our
        # check up (bad state) within one scheduler status poll: worth waiting for, so one call usually ends `down`. An
        # endpoint of an earlier plugin keeps one user endpoint, where a cancel is followed by another block instead.
        fresh = getattr(app.facility, "fresh_ueps", False) is True
        wait = config.stop_check_wait_s() if fresh else 0.0
        # an earlier scheduler command of ours already occupying the internal login session (the cancel would wait)
        internal_before = _busy_session(app, "login", _INTERNAL_SESSION)
    finally:
        app.lock.release()
    # A command whose result the client lost (its handle is gone) that may still be running: like a live task, a stop
    # refuses — releasing the block would not end it (the endpoint starts another block to run it).
    if refusal := await _lost_command_refusal(app, eid, checks, fresh, call_deadline):
        return refusal
    # Cancel the scheduler block over the login shape (AMQP) — no SSH. OFF the lock: the release rides run_shell.
    started = time.monotonic()
    settled = await _release_until_settled(app, eid, checks, wait, call_deadline, fresh=fresh)
    confirmed, detail, states, ran = settled.confirmed, settled.detail, settled.states, settled.ran
    waited = time.monotonic() - started
    # A check the service still lists as queued after that wait may be one whose failure its own user endpoint never
    # reported (the globus-compute-endpoint race, scheduler_ops): read that user endpoint's fate on the login node —
    # once, only where its log names the check's block request, and only for a confirmed release that a `down` could
    # follow. `looked`: why the evidence did not settle it, for the `draining`.
    # The endpoint's template is known by now even after a restart: the release rode the login shape, whose provision
    # read it (warmth._provision -> adopt_endpoint).
    fresh = fresh or getattr(app.facility, "fresh_ueps", False) is True
    evidence: dict[str, scheduler_ops.UepEvidence] = {}
    looked: str | None = None
    # Only where a new life of a user endpoint starts no block by itself (`init_blocks` 0: batch mode, the default):
    # with 1 (interactive) any restart bills, task or not, and that the Globus Compute service never restarts a user
    # endpoint it thinks still holds a pending task cannot be shown from here. Unknown: not either.
    if (confirmed and fresh and not store_problem and getattr(app.facility, "logs_block_requests", False) is True
            and getattr(app.facility, "compute_init_blocks", None) == 0):
        states, evidence, looked = await _settle_by_evidence(app, eid, checks, states, call_deadline)
    if not await _acquire_within(app, call_deadline):
        return _lock_busy_status(app, eid, released=detail if confirmed else None)
    try:  # look again and drop in ONE step: a dispatch may have reached the block during the release
        if work := _block_work(app, then="stop_endpoint again"):
            # Its command was sent to a block being cancelled: the endpoint may relaunch one to run it. Keep the shape
            # (its handle or returning call stays tracked there, and the next stop refuses or releases honestly).
            what, action = work
            return EndpointStatus(
                status="draining", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
                notice=(f"{detail}. Spend is NOT confirmed stopped: {what} — it reached the block while it was being "
                        f"released, so the endpoint may relaunch a block to run it. {action}"),
            )
        # A worker answered a check of another call AFTER this stop's last cancel began: that block is up now.
        up_late = _answered_since(app.shapes.get(DEFAULT_SHAPE), settled.released_at)
        warmth._drop_compute_shape_locked(app)  # its spend stays in the session total, its checks with the endpoint
        if confirmed:  # the release cancelled every pilot of this endpoint: a re-bind names none (an unconfirmed one —
            app.ssh_left.pop(eid, None)  # a cold channel, a refused qdel, out of time — may have left a held one)
        warmth._settle_checks(app, eid, checks, states)
        # a check sent while the release ran (a concurrent ensure_endpoint_up) was not looked at: what its own future
        # says, else queued
        seen = {warmth._check_key(c) for c in checks}
        late = [c for c in app.sent_checks.get(eid, []) if warmth._check_key(c) not in seen
                and warmth._local_state(c, time.time(), drop_no_id=fresh) not in ("ok", "failed", "unsent")]
    finally:
        app.lock.release()
    if up_late and confirmed:
        return EndpointStatus(
            status="draining", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
            notice=(f"{detail}. Spend is NOT confirmed stopped: a block came up and answered a check of hpc-bridge's "
                    "after this stop's last cancel began — it is up now. Call stop_endpoint again to cancel it. The "
                    "login endpoint stays online for reuse."),
        )
    pending = [c for c, st in zip(checks, states, strict=True) if st in ("pending", "unknown")] + late
    unknown = "unknown" in states
    gave_up = [c for c, st in zip(checks, states, strict=True) if st == "failed"]
    lost = [(c, ev.uep or "", ev.bad_at or "") for c, st in zip(checks, states, strict=True)
            if st == "lost" and (ev := evidence.get(warmth._check_key(c))) is not None]
    if confirmed and not pending and store_problem:  # fail closed: a task we cannot see may still be queued
        return EndpointStatus(
            status="draining", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
            notice=_store_problem_notice(detail, store_problem))
    if confirmed and not pending:
        return EndpointStatus(
            status="down",  # the cancel CONFIRMED, and no task of ours is left that could start a block
            block_state="cold",
            endpoint_id=eid,
            session_spend=_total_session_spend(app),
            spend_stopped=True,  # the one result that may say so
            notice=_stop_down_notice(app, detail, rejected=rejected, gave_up=gave_up, ran=ran, lost=lost),
        )
    if confirmed:  # the scheduler holds nothing of ours now, but a task still queued can start a block
        released = detail if detail != "released none" else None
        return EndpointStatus(
            status="draining", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
            notice=_check_pending_notice(app, pending, released=released, unknown=unknown, waited_s=waited,
                                         fresh=fresh, looked=looked) + _store_problem_note(store_problem),
        )
    if await _gone_within(app, call_deadline):
        return await _orphaned_stop(app, eid, queued=bool(pending))
    # HONEST unconfirmed release (#24): NEVER "down" here — the agent must know spend may still be running.
    # idle-release (min_blocks=0) ends a block ~10 min after its last task ONLY while no task of ours is outstanding
    # there: never offered as the backstop while one may be queued (it is named instead), nor while the record of our
    # tasks cannot be trusted
    if pending:
        what, plural = _check_words(pending, _queue_word(app))
        backstop = (f" {what} {'are' if plural else 'is'} also not seen to finish, and the endpoint can start a block "
                    "to run " + ("them" if plural else "it") + ": idle-release does not end that.")
    elif store_problem:
        backstop = _store_problem_note(store_problem)
    else:
        backstop = (" No task of hpc-bridge's is queued there, so idle-release (min_blocks=0) ends the block ~10 min "
                    "after its last task.")
    internal = _busy_session(app, "login", _INTERNAL_SESSION)
    held = app.tasks.get(internal) if internal is not None else None
    if held is not None and internal != internal_before and held.command == scheduler_ops._release_cmd_for(app, eid):
        # this stop's own cancel went out but outlived its sync-wait: queued behind other login-node work, or a slow
        # scheduler — sent, not confirmed
        notice = (f"Spend is NOT confirmed stopped: the cancel was sent (task_id={internal!r}) but has not finished — "
                  "it is queued behind other work on the login node, or the scheduler is slow. Call stop_endpoint "
                  "again in a little while to confirm." + backstop + " The login endpoint stays online for reuse.")
    elif internal is not None:
        # the cancel never left: another scheduler command of ours (an earlier one, or one started meanwhile)
        # occupies the internal login session
        notice = ("Spend is NOT confirmed stopped: the cancel could not be sent — the login release channel is busy, "
                  f"not cold, with another scheduler command of hpc-bridge's own (task_id={internal!r}) that is "
                  "still running. Call stop_endpoint again in a little while." + backstop + " The login endpoint stays "
                  "online for reuse.")
    elif detail == _OUT_OF_TIME:
        # this call's time ran out before the scheduler answered (the login node or the scheduler is slow)
        notice = ("Spend is NOT confirmed stopped: the cancel did not finish within this call — the login node or the "
                  "scheduler is slow. Call stop_endpoint again in a little while to confirm." + backstop + " The login "
                  "endpoint stays online for reuse.")
    elif (failure := scheduler_ops._release_failure(detail)) is not None:
        # a scheduler command itself failed (unreachable, or a cancel refused): not a cold channel, quoted as it is
        notice = _failed_release_notice(failure, queued=bool(pending))
    elif pending:  # the release could not look (a cold channel), and a task of ours is still queued
        notice = _check_pending_notice(app, pending, released=None, unknown=unknown, waited_s=0.0, fresh=fresh,
                                       reason="the login release channel was cold, and ") + _store_problem_note(
                                           store_problem)
    else:
        notice = (f"{detail}. Spend is NOT confirmed stopped — the login release channel was cold. Call stop_endpoint "
                  "again in a few seconds (the channel is warming) to confirm the cancel." + backstop + " The login "
                  "endpoint stays online for reuse.")
    return EndpointStatus(
        status="draining",
        block_state="cold",
        endpoint_id=eid,
        session_spend=_total_session_spend(app),
        notice=notice,
    )


def _answered_since(rt: ShapeRuntime | None, since: float | None) -> bool:
    """A worker answered a check on this shape at or after `since` (monotonic; None: no confirmed release): the block it
    ran on came up after that cancel began, so it may still be up. Read from the shape's last confirmation, and from a
    check that answered after its caller stopped waiting (`canary_answer`, not read yet)."""
    if rt is None or since is None:
        return False
    if rt.warm_confirmed_at is not None and rt.warm_confirmed_at >= since:
        return True
    late = getattr(rt.runner, "canary_answer", lambda: None)() if rt.runner is not None else None
    return late is not None and late.answered_at is not None and late.answered_at >= since


# How often a stop that waits for our checks looks again: each look is a release (cancelling whatever pilot landed) and
# a lookup of each check.
_SETTLE_STEP_S = 10.0
# Kept back from the call's budget while waiting: one more release and one more round of lookups.
_SETTLE_RESERVE_S = 40.0
_sleep = asyncio.sleep  # indirection so tests can run the settle loop without waiting
# The detail of a release that this call's budget cut short.
_OUT_OF_TIME = "cancel not confirmed (this call ran out of time before the scheduler answered)"


class _Settled(NamedTuple):
    confirmed: bool               # the last release was confirmed
    detail: str                   # every job cancelled on the way, or the last attempt's words
    states: list[warmth.CheckState]
    ran: bool                     # a check ran during the stop (a block came up for it): it was cancelled after
    released_at: float | None     # when the last confirmed release began (monotonic)


async def _acquire_within(app: AppCtx, deadline: float) -> bool:
    """Take app.lock, waiting no later than `deadline` (monotonic): another operation can hold it for long (a bring-up
    over SSH, a provisioning call), and the stop must answer within its budget. A free lock is taken at once, even
    with no time left (its final locked step must still run). True: the caller holds it now."""
    if not app.lock.locked():
        await app.lock.acquire()  # free: no suspension, so nothing can take it in between
        return True
    try:
        await asyncio.wait_for(app.lock.acquire(), timeout=max(0.5, deadline - time.monotonic()))
    except TimeoutError:
        return False
    return True


def _lock_busy_status(app: AppCtx, eid: str, *, released: str | None) -> EndpointStatus:
    """The stop could not take the endpoint within its budget: another hpc-bridge operation holds it. Nothing is
    confirmed — `released`: what this stop's cancel did before that."""
    head = f"compute block released over AMQP ({released}), but " if released else ""
    return EndpointStatus(
        status="draining", block_state="cold", endpoint_id=eid, session_spend=_total_session_spend(app),
        notice=(f"{head}Spend is NOT confirmed stopped: another hpc-bridge operation holds this endpoint (for example "
                "a bring-up over SSH, or a long provisioning call), and this stop could not finish within its time. "
                "Call stop_endpoint again in a little while. The login endpoint stays online for reuse."),
    )


async def _gone_within(app: AppCtx, deadline: float) -> bool:
    """`_endpoint_gone`, within the stop's budget (out of time: not known gone — the honest `draining` follows)."""
    try:
        return await asyncio.wait_for(_endpoint_gone(app), timeout=max(0.01, deadline - time.monotonic()))
    except TimeoutError:
        return False


async def _lost_command_refusal(app: AppCtx, eid: str, checks: list[SentCheck], fresh: bool,
                                deadline: float) -> EndpointStatus | None:
    """A command of ours whose result the client lost (its future failed on the client's side alone) that the SERVICE
    says is still queued or running: like a live task, releasing the block would not end it — the endpoint starts
    another block to run it, and spend continues after a false `down` — so the stop refuses, naming it. Only on that
    answer: one whose state cannot be read (no client yet after a restart — the release builds the login runner that
    looks it up —, or a record the service no longer returns) never blocks a stop: the stop releases, and names it in
    its `draining` while it cannot be read. Nor does one past its ceiling (`SentCheck.ceiling_at`: killed at its
    walltime once running, plus the grace a queued one gets). None: nothing holds the stop."""
    now = time.time()
    cmds = [c for c in checks if c.what == "command" and (c.ceiling_at is None or now < c.ceiling_at)]
    if not cmds:
        return None
    states = await warmth._check_states(cmds, _lookup_client(app),
                                        min(warmth._LOOKUP_TIMEOUT_S, deadline - time.monotonic()),
                                        known=app.resolved_checks, drop_no_id=fresh)
    live = [c for c, st in zip(cmds, states, strict=True) if st == "pending"]
    if not live:
        return None
    ids = ", ".join(t for c in live if (t := c.tid)) or "unknown id"
    read = ""
    rt = app.shapes.get(DEFAULT_SHAPE)
    return EndpointStatus(
        status="up", block_state="warm" if rt is not None else "cold", endpoint_id=eid,
        session_spend=_total_session_spend(app), block_job_id=rt.block_job if rt is not None else None,
        spend_stopped=False,
        notice=(f"can't stop yet: hpc-bridge's command whose result the client lost (task {ids}) may still be "
                f"running on the block{read}. Releasing the block now would not end it — the endpoint would start "
                "another block to run it and spend would continue after a false 'down'. Its result cannot be fetched "
                "(poll_task cannot return it): call stop_endpoint again once it has had time to finish, or "
                "teardown_endpoint ends it now (the user's call)."),
    )


def _store_problem_note(problem: str | None) -> str:
    """The sentence a stop adds when the record of our checks on disk cannot be trusted (state.SentCheckStore)."""
    if not problem:
        return ""
    return (f" hpc-bridge's record of the checks it sent to this endpoint cannot be trusted — {problem} — so a check "
            "still queued there may go unseen; teardown_endpoint ends every one of them (the user's call).")


def _store_problem_notice(detail: str, problem: str) -> str:
    """A confirmed release and no task of ours seen queued — but the record on disk could not be read: fail closed."""
    return (f"compute block released over AMQP ({detail}). Spend is NOT confirmed stopped: hpc-bridge's record of the "
            f"checks it sent to this endpoint cannot be trusted — {problem} — so a check of an earlier session may "
            "still be queued there, which can start a block to run it. teardown_endpoint ends every one of them (the "
            "user's call). The login endpoint stays online for reuse.")


def _lookup_client(app: AppCtx) -> Any:
    """An SDK Client to look up a task read back from disk (it has none of its own): any runner of this binding's —
    the login shape's first, which the release itself just used."""
    for name in ("login", DEFAULT_SHAPE):
        runner = getattr(app.shapes.get(name), "runner", None)
        client = getattr(runner, "task_client", lambda: None)() if runner is not None else None
        if client is not None:
            return client
    return None


async def _bounded_release(app: AppCtx, eid: str, run_login: Any, deadline: float) -> tuple[bool, str]:
    """One release, within what is left of the call's budget."""
    left = deadline - time.monotonic()
    if left <= 0:
        return False, _OUT_OF_TIME
    try:
        return await asyncio.wait_for(scheduler_ops._release_blocks_over_login(app, eid, run_login), timeout=left)
    except TimeoutError:
        return False, _OUT_OF_TIME


async def _release_until_settled(app: AppCtx, eid: str, checks: list[SentCheck], wait_s: float,
                                 deadline: float, *, fresh: bool = False) -> _Settled:
    """Release this endpoint's blocks, and — for up to `wait_s` while a task of ours is still queued — release again
    and look again every `_SETTLE_STEP_S`: a pilot that lands is cancelled, and a fresh user endpoint whose first pilot
    was cancelled (or died) gives our check up. A check that RAN meanwhile ran on a block that came up for it: one more
    release cancels that block. Everything — releases and lookups — within the call's `deadline`."""
    run_login = _login_runner(app)
    wait_until = min(time.monotonic() + wait_s, deadline - _SETTLE_RESERVE_S)
    cancelled: list[str] = []
    ran = False
    released_at: float | None = None
    while True:
        began = time.monotonic()
        confirmed, detail = await _bounded_release(app, eid, run_login, deadline)
        if confirmed:
            released_at = began
        cancelled += [j for j in scheduler_ops._released_jobs(detail) if j not in cancelled]
        states = await warmth._check_states(checks, _lookup_client(app),
                                            min(warmth._LOOKUP_TIMEOUT_S, deadline - time.monotonic()),
                                            known=app.resolved_checks, drop_no_id=fresh)
        ran = ran or "ok" in states
        if not confirmed or not any(st in ("pending", "unknown") for st in states) or time.monotonic() >= wait_until:
            break
        await _sleep(min(_SETTLE_STEP_S, max(0.0, wait_until - time.monotonic())))
    if ran and confirmed:  # a block came up for a check during the stop: it ran it, and is cancelled now
        began = time.monotonic()
        confirmed, detail = await _bounded_release(app, eid, run_login, deadline)
        if confirmed:
            released_at = began
        cancelled += [j for j in scheduler_ops._released_jobs(detail) if j not in cancelled]
    if confirmed:
        detail = f"released {' '.join(cancelled)}" if cancelled else "released none"
    return _Settled(confirmed, detail, states, ran, released_at)


# One look at the login node for checks the service still lists as queued (`_settle_by_evidence`): at most this long,
# only with at least `_EVIDENCE_MIN_S` of it to spend (the command takes two samples `scheduler_ops._EVIDENCE_GAP_S`
# apart), and never into the last `_EVIDENCE_RESERVE_S` of the call's budget (its final locked step and the answer).
_EVIDENCE_TIMEOUT_S = 25.0
_EVIDENCE_MIN_S = 10.0
_EVIDENCE_RESERVE_S = 10.0


async def _settle_by_evidence(app: AppCtx, eid: str, checks: list[SentCheck], states: list[warmth.CheckState],
                              deadline: float) -> tuple[list[warmth.CheckState], dict[str, scheduler_ops.UepEvidence],
                                                        str | None]:
    """The checks a stop's wait left `pending` (or `unknown`), read on the login node: a check is `lost` when its own
    compute user endpoint received it and failed it with bad state, and that user endpoint has stopped for good —
    gone, and its manager neither reviving nor restarting it (scheduler_ops: what the evidence rules out, and how). The
    service lists such a check as queued only because that endpoint's report of its failure never left
    (globus-compute-endpoint 4.18 stops its result publisher without blocking, right after bad state). Nothing else
    changes a state.

    Only when EVERY task still waiting is a check the service registered (a task id first seen here at a known time)
    that names its block request. Otherwise nothing can make this stop `down`, so nothing is run: a command whose result
    the client lost runs the user's work — whatever its user endpoint's fate, a stop must not say it ended (the stop
    names it, and teardown_endpoint ends it); a check with no block request (an endpoint configured before this change,
    an older record) has no user endpoint to find; and one with no task id may still be on its way to the service,
    where it would have the manager start that user endpoint again. The checks are dated by when their task id was
    first seen (`SentCheck.taken_at`), never by the submit, which only queues them in the SDK. One command for all of
    them over the free login shape (AMQP, no SSH), bounded by what is left of the call's budget; a stop's wait need not
    have run first — the command itself asks that each check be old enough (`scheduler_ops._EVIDENCE_SETTLED_S`), so a
    short `HPC_BRIDGE_STOP_CHECK_WAIT_S` does not loosen it. An age is measured on this process's monotonic clock from
    when the task id was seen here (`SentCheck.taken_mono`), else — read back from disk — on the wall clock.

    What it still takes on trust, from the Globus Compute service: a wrong `down` would need BOTH the service
    delivering the task to that user endpoint's queue more than about a second after returning its task id (so it could
    arrive while that endpoint quiesced, and not be acked) AND its start request reaching the manager more than 35 s
    late (so the restart it causes comes after this look). The caller looks only where a new life starts no block by
    itself (`init_blocks` 0), so a restart without our task bills nothing. Returns the states, each check's evidence by
    its key, and why the evidence settled nothing (None: not looked, or all settled)."""
    waiting = [c for c, st in zip(checks, states, strict=True) if st in ("pending", "unknown")]
    if not waiting or any(c.what != "check" or c.block is None or c.tid is None or c.taken_at is None
                          for c in waiting):
        return states, {}, None
    budget = min(_EVIDENCE_TIMEOUT_S, deadline - time.monotonic() - _EVIDENCE_RESERVE_S)
    if budget < _EVIDENCE_MIN_S:
        return states, {}, "this stop had no time left to look"
    # each check's age, read BEFORE the command is sent: the login node's clock is read after it, so the check is
    # never placed earlier on it than the service took it
    now, mono = time.time(), time.monotonic()
    wanted = [(warmth._check_key(c), c.block, (mono - c.taken_mono) if c.taken_mono is not None else (now - c.taken_at))
              for c in waiting if c.block is not None and c.taken_at is not None]
    try:
        cmd = scheduler_ops._uep_evidence_cmd(eid, [b for _, b, _ in wanted])
    except ValueError:  # an endpoint id that is not a UUID: no command is built from it
        return states, {}, None
    try:
        out = await asyncio.wait_for(_login_runner(app)(cmd), timeout=budget)
    except TimeoutError:
        return states, {}, "the login node did not answer in time"
    if out.phase != "complete" or out.exit_code != 0:
        return states, {}, "the login channel was cold or busy"
    found = scheduler_ops._read_uep_evidence(out.stdout or "", eid, wanted)
    new = [("lost" if st in ("pending", "unknown") and (ev := found.get(warmth._check_key(c))) is not None and ev.lost
            else st) for c, st in zip(checks, states, strict=True)]
    why = next((ev.why for ev in found.values() if not ev.lost), None)
    return new, found, why


async def _orphaned_stop(app: AppCtx, eid: str, *, queued: bool = False) -> EndpointStatus:
    """The login endpoint is OFFLINE/gone (the #44 liveness check poll_task got, applied here — found in review): "call
    again in a few seconds" would loop forever. A block whose manager is gone exits on its own (workers lose their
    manager), so nothing spends through hpc-bridge now. Our tasks are KEPT (in memory and on disk): an OFFLINE status
    can be transient, and a task still queued there can start a block once the endpoint is back — a later stop sees it.
    `queued`: one was not known resolved."""
    note = (" hpc-bridge's last check (or command) there was not seen to finish: if the endpoint comes back online it "
            "can start a block for it — a stop_endpoint then cancels it, and teardown_endpoint ends it."
            if queued else "")
    return EndpointStatus(
        status="down", block_state="cold", endpoint_id=eid,
        session_spend=_total_session_spend(app),
        notice=("the login endpoint is OFFLINE — the cancel cannot be dispatched through it (ORPHANED). "
                "A block without its manager exits on its own; nothing is spending through hpc-bridge." + note
                + " Do not call stop_endpoint again while it is offline; connect_facility stands the endpoint up "
                "afresh."),
    )


@mcp.tool()
async def stop_endpoint(ctx: Context) -> EndpointStatus:
    """Stop spending on the HPC compute block. On an endpoint hpc-bridge stood up over SSH it cancels the billed
    scheduler block over the login endpoint (no SSH) and **leaves the login-node endpoint online** so a later
    reconnect reuses it with zero SSH — "stop" means stop spending, not tear the endpoint down; `down` (with
    `spend_stopped` true) means nothing of ours is left to bill there — the cancel was confirmed (or there was nothing
    to cancel) and no check of hpc-bridge's can still start a block (none is queued; or the service still lists one,
    but its user endpoint shut down for good, which the notice says) — and `draining` that this is not confirmed yet:
    the cancel was not confirmed, or such a check is still queued and the endpoint can start a block to run it.
    Call it again when its notice says; a check that stays queued ends with teardown_endpoint. On a facility multi-user
    endpoint NO cancel is issued (hpc-bridge has no cancel channel there): it stops submitting and answers `draining`,
    which is final, naming the block's scheduler job (`block_job_id`), the latest it can bill until, and the command
    the user runs to end it now — relay those; do not check from here afterwards. Call when you're done with a
    compute block."""
    return await _heartbeat(ctx, _stop_endpoint(ctx.request_context.lifespan_context), "stop_endpoint")


async def _teardown_endpoint(app: AppCtx) -> EndpointStatus:
    """FULLY tear the endpoint down: release the billed block, then `gce stop` + delete the login
    manager over SSH (the facility's `teardown()`), and clear ALL shape/state so nothing lingers.
    The rare, explicit 'destroy it' op — normally the login endpoint STAYS ONLINE for zero-SSH reuse
    and costs nothing; a later run_shell would re-bootstrap a fresh endpoint from scratch.

    The SSH ops run in a task (`app.teardown_task`): this call waits `config.teardown_wait_s()` (60 s) for them and
    otherwise returns `tearing_down`; calling again waits again / reports the finished result.

    Everything that decides WHAT is torn down happens in ONE locked section with no await before it: the SSH
    teardown is claimed and snapshotted there (the task then runs on that facility and endpoint, whatever a
    parallel connect_facility binds), and a facility MEP is detached there outright. A teardown awaiting
    anything before claiming could be retargeted at the facility a parallel connect binds (review 2026-09-05 #3)."""
    async with app.lock:
        task = app.teardown_task
        eid = app.state.endpoint_id
        if task is not None and task.done() and not _replayable_teardown(app, task):
            task = app.teardown_task = None  # a stale result (another endpoint, or a code gate since answered)
        if task is None and eid is None and not _has_login_shape(app) and warmth._prune_left(app):
            # already detached, but a block it left may still bill: say so again, never a bare 'down' (#178)
            return _left_blocks_status(app, None, None, "", op="detach", spent=_total_session_spend(app))
        if task is None and eid is None:
            return EndpointStatus(status="down", block_state="cold", notice="no endpoint was up")
        if task is None and not _has_login_shape(app):
            # A facility MEP is NOT ours to destroy (and there's no release channel): detach — drop our
            # shapes/state so nothing of ours lingers — and say exactly that. The facility's endpoint
            # stays online; a block we left is reclaimed by its idle-release (see _stop_mep).
            assert eid is not None  # the no-endpoint case returned above
            if busy := _busy_block_refusal(app, eid, op="detach"):  # detaching would lose results, not idle it
                return busy
            lost = _lost_note(app, op="detach") or "No task of ours is known to be running there."
            # the block it leaves, resolved before the shapes go, banked to its latest end and remembered (#178)
            rt = app.shapes.get(DEFAULT_SHAPE)
            current = warmth._leave_block(app, DEFAULT_SHAPE, rt, how="detach") if rt is not None else None
            spent = _drop_all_shapes(app, bank=True)  # banks the compute shape too; app.tasks cleared
            # `down`: OUR state is fully cleared; the facility's endpoint is untouched — and so is its block
            return _left_blocks_status(app, eid, current, lost, op="detach", spent=spent)
        if task is None:
            assert eid is not None  # the no-endpoint case returned above
            app.teardown_eid, app.teardown_release = eid, None
            task = app.teardown_task = asyncio.create_task(_run_teardown(app, app.facility, app.machine, eid))
    return await _await_teardown(app, task)


def _replayable_teardown(app: AppCtx, task: asyncio.Task) -> bool:
    """A FINISHED teardown's result is worth replaying only if it is the 'down' of the endpoint still in question.
    A one-time-code gate or a failure is a request to act and call again — answering the next call with it again
    would loop (the code may have been completed since); a 'down' for an endpoint a later run_shell replaced
    would leave the new one untouched (review 2026-10-05)."""
    if task.cancelled():  # CancelledError is a BaseException: check before result()
        return False
    try:
        res = task.result()
    except Exception:  # noqa: BLE001 - _run_teardown never raises
        return False
    # A real `down` always cleared the state; a bound endpoint (even with the same id — `gce start` re-adopts its
    # endpoint.json after a failed delete) means there is something to tear down now, not a result to replay.
    return res.status == "down" and app.state.endpoint_id is None


async def _run_teardown(app: AppCtx, fac, machine: str | None, eid: str) -> EndpointStatus:
    """The SSH teardown, start to finish, as ONE task on the facility/endpoint snapshotted when it was claimed:
    release the block, stop its spend clock, gate on a one-time code if needed, then the login-node ops. Never
    raises — _await_teardown returns its result to whichever call is waiting."""
    try:
        # halt spend first (a confirmed stop is stop_endpoint's job); what the release achieved is reported
        confirmed, _detail = await scheduler_ops._release_blocks_over_login(app, eid, _login_runner(app))
        app.teardown_release = bool(confirmed)
        # The block is being released: stop its spend clock NOW, as stop_endpoint does — not after the login-node
        # ops (minutes on Expanse), and not never on the one-time-code path (review 2026-09-05 #6c). The spend
        # stays in the session total (app.released_spend).
        await warmth._drop_compute_shape(app)
        gate = await _teardown_preauth_gate(app, eid, fac=fac, machine=machine)
        if gate is not None:
            return gate
        return await _finish_teardown(app, eid, fac=fac)
    except Exception as exc:  # noqa: BLE001 - a waiting call must get a structured answer, never an exception
        return _teardown_failed(app, eid, f"teardown raised {type(exc).__name__}: {exc}"[:300], fac=fac)


def _release_words(app: AppCtx, *, sentence: bool = False) -> str:
    """What is TRUE of this teardown's block release, for the agent to relay (`sentence`: capitalised to open one)."""
    if app.teardown_release is None:
        words = "the block release is still in progress (the login channel may be warming)"
    elif app.teardown_release:
        words = "the block release went through"
    else:
        words = ("the block release was dispatched but NOT confirmed — the login channel stayed cold, so the block may "
                 "run until the facility's idle-release or its walltime")
    return words[:1].upper() + words[1:] if sentence else words


async def _await_teardown(app: AppCtx, task: asyncio.Task) -> EndpointStatus:
    """Wait a bounded time for the in-flight teardown; its result when it finished, else `tearing_down`. Two calls
    may wait on the same task: each gets the result, and only the slot still holding THIS task is cleared. A task
    that finishes with nobody waiting stays in the slot for the next call (`_replayable_teardown` decides)."""
    done, _pending = await asyncio.wait({task}, timeout=config.teardown_wait_s())
    if task in done:
        if app.teardown_task is task:
            app.teardown_task = None  # reported to a caller: nothing left to confirm or replay
        return task.result()  # _run_teardown never raises: every failure is folded into the notice
    return EndpointStatus(
        status="tearing_down",
        block_state="cold",
        endpoint_id=app.state.endpoint_id,
        session_spend=_total_session_spend(app),
        notice=(f"teardown is still running — {_release_words(app)}; the login-node manager's stop + delete take a "
                "few minutes on a slow filesystem. Call teardown_endpoint again in about a minute to confirm 'down'. "
                "Do NOT call run_shell or connect_facility meanwhile."),
    )


async def _finish_teardown(app: AppCtx, eid: str, *, fac=None) -> EndpointStatus:
    """The login-node half of teardown (the facility's `teardown()`), then clear ALL shape/state. Runs inside
    `app.teardown_task` so a slow login node cannot hold the MCP call — or be interrupted by its client. `fac` is
    the facility snapshotted when the teardown was claimed, never whatever is bound by the time this runs."""
    fac = fac if fac is not None else app.facility
    released = ("block released" if app.teardown_release is not False else
                "block release dispatched but NOT confirmed — the facility's idle-release is the backstop")
    notice = f"endpoint fully torn down ({released}; manager gce-stopped + deleted)"
    teardown = getattr(fac, "teardown", None)
    if teardown is not None:
        try:
            # the seeded token store leaves with the endpoint (B-03)
            report = await teardown(eid, wipe_credentials=True)
        except Exception as exc:  # noqa: BLE001 - report, don't crash the tool
            return _teardown_failed(app, eid, f"the login-node teardown raised {type(exc).__name__}: {exc}"[:300],
                                    fac=fac)
        else:
            if isinstance(report, dict):  # say what actually happened, not what was intended (live 2026-09-04)
                if report.get("ssh_failed"):
                    return _teardown_failed(app, eid, report.get("error") or "ssh failed", ssh_denial=True, fac=fac)
                unconfirmed = "stopped" in report and report["stopped"] is None  # measured (absent = not reported)
                if unconfirmed and not report.get("deleted"):
                    done = [w for k, w in (("credentials_wiped", "the Globus token copy hpc-bridge placed there was "
                                            "removed"), ("ssh_closed", "the shared SSH connection was closed"))
                            if report.get(k)]
                    return _teardown_failed(
                        app, eid, "the manager's stop could not be confirmed — the login node did not answer in time"
                        + (f" ({report.get('error')})" if report.get("error") else "")
                        + (f"; {'; '.join(done)}" if done else ""), fac=fac, unconfirmed=True)
                if not unconfirmed and not report.get("stopped", True):
                    return _teardown_failed(
                        app, eid, "`globus-compute-endpoint stop` failed and the manager still reports running"
                        + (f": {report.get('error')}" if report.get("error") else ""), fac=fac)
                deleted = ("manager deleted (its stop was not confirmed in time, but the delete went through)"
                           if unconfirmed else
                           "manager gce-stopped + deleted" if report.get("deleted") else
                           "manager gce-stopped, but DELETE FAILED: the endpoint directory and its registration "
                           "remain on the login node (the next connect will re-adopt them)"
                           + (f" — {report.get('delete_error')}" if report.get("delete_error") else ""))
                creds = ("the Globus token copy hpc-bridge placed on the login node removed"
                         if report.get("credentials_wiped") else "no token store of ours was removed")
                ssh = ("; the shared SSH connection to the login node was closed too — nothing of this session "
                       "stays open on the user's machine" if report.get("ssh_closed") else "")
                notice = f"endpoint fully torn down ({released}; {deleted}; {creds}{ssh})"
    async with app.lock:  # clear everything so a stray run_shell can't silently revive a stale endpoint
        # — unless a connect meanwhile bound a NEW endpoint: that state is its, not this teardown's to clear
        ours = app.state.endpoint_id in (eid, None)
        spent = _drop_all_shapes(app, bank=True) if ours else _total_session_spend(app)
        warmth._forget_checks(app, eid)  # the endpoint is deleted, and its user endpoints with our checks with it
        app.ssh_left.pop(eid, None)
    await warmth._flush_all(app, timeout=5.0)  # its record goes before the answer says it is gone
    return EndpointStatus(
        status="down",
        block_state="cold",
        endpoint_id=eid,
        session_spend=spent,
        notice=notice + ". It will NOT be reused — a fresh connect_facility re-bootstraps over SSH. "
        "Do NOT call run_shell now (it would provision a new endpoint).",
    )


def _teardown_failed(app: AppCtx, eid: str, why: str, *, ssh_denial: bool = False, fac=None,
                     unconfirmed: bool = False) -> EndpointStatus:
    """Teardown did NOT happen: the endpoint stays bound (so a retry can finish the job) and the notice says
    what is still there. An SSH denial that offers a second factor becomes the one-time-code handoff, so a
    bring-your-own MFA facility gets the same treatment as a curated one (review 2026-09-05, Fix-now #1)."""
    from .facility.remote import key_accepted_second_factor_pending

    fac = fac if fac is not None else app.facility
    target = getattr(getattr(fac, "cli", None), "target", None)
    facility = app.machine or "the facility"
    head = ("TEARDOWN NOT CONFIRMED — the manager may or may not have stopped, and its endpoint was not deleted. "
            if unconfirmed else
            "TEARDOWN FAILED — nothing was removed: the login-node manager is STILL RUNNING and any token copy "
            "hpc-bridge placed there is still in place. ")
    if ssh_denial and target is not None and key_accepted_second_factor_pending(why):
        app.pending_preauth = (facility, target)
        app.preauth_resume = "teardown_endpoint()"
        handoff = _needs_preauth_result(facility, target, otp_ok=True)
        detail = f"The SSH connection to the login node needs its one-time code again. {handoff.notice} "
    elif ssh_denial:
        detail = _explain_provision_error(RuntimeError(why), fac) + " "
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


async def _teardown_preauth_gate(app: AppCtx, eid: str, *, fac=None, machine: str | None = None,
                                 ) -> EndpointStatus | None:
    """Teardown is the one post-bootstrap op that MUST SSH the login node (`gce stop` + delete run there).
    On a one-time-code facility with no shared connection open, ask for the code BEFORE any SSH — the same
    handoff connect_facility uses — instead of letting `stop`/`delete` fail their BatchMode logins and then
    reporting "DELETE FAILED" about an endpoint that is in fact still running. The block release above has
    already gone over AMQP, so spend is halted before the user is asked for anything. None = proceed."""
    from .connect import _master_alive

    fac = fac if fac is not None else app.facility
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
            return _teardown_failed(app, eid, err or "ssh failed", ssh_denial=True, fac=fac)
    facility = machine or app.machine or "the facility"
    app.pending_preauth = (facility, target)
    app.preauth_resume = "teardown_endpoint()"
    handoff = _needs_preauth_result(facility, target, otp_ok=True)
    return EndpointStatus(
        status="up",  # the login-node manager is still running — nothing has been torn down yet
        block_state="cold",
        endpoint_id=eid,
        session_spend=_total_session_spend(app),
        notice=(f"{_release_words(app, sentence=True)}; the login-node manager is STILL RUNNING — tearing it down "
                "needs an SSH "
                f"connection to the login node, which is not open. {handoff.notice} Then call teardown_endpoint "
                "again to finish (it may answer 'tearing_down' first: the login-node ops take a few minutes)."),
    )


@mcp.tool(annotations=_DESTRUCTIVE)
async def teardown_endpoint(ctx: Context) -> EndpointStatus:
    """FULLY tear down the login-node endpoint (gce stop + delete over SSH) — the rare 'destroy it'
    operation. **Normally do NOT call this.** The login endpoint is DESIGNED to stay online for
    zero-SSH reuse and costs nothing (a free login-node process, no allocation); `stop_endpoint`
    already halts ALL spend by releasing the billed block. Only call this when the user EXPLICITLY
    insists on removing the endpoint entirely. Afterwards, do not call run_shell (it re-provisions).
    The login-node ops can take a few minutes on a slow filesystem: a `tearing_down` status means they are
    still running — call teardown_endpoint again in about a minute to confirm `down`; call nothing else
    meanwhile. On a one-time-code facility the first call may instead ask for a code (`complete_preauth`)."""
    return await _heartbeat(ctx, _teardown_endpoint(ctx.request_context.lifespan_context), "teardown_endpoint")


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
        return LoginShellResult(exit_code=1, notice=_no_facility_notice(app))
    sub = await _detect_submission(command, "login")  # the same login node, over SSH: a job submitted here is billed
    try:
        rc, out, err = await login_exec(command)
    except Exception as exc:  # noqa: BLE001 - never crash the tool; report structurally
        return LoginShellResult(exit_code=1, notice=f"login_shell error: {type(exc).__name__}: {exc}"[:300])
    return LoginShellResult(
        exit_code=rc, stdout=out, stderr_snippet=err,
        notice=(_scheduler_submit_notice(sub, inside_block=False, phase="complete", exit_code=rc, stdout=out)
                if sub else None),
    )


@mcp.tool()
async def login_shell(command: str, ctx: Context) -> LoginShellResult:
    """Run a READ-ONLY command on the HPC login node over a FRESH SSH connection — the
    cold-start discovery escape hatch (`sinfo`, `sacctmgr`, `echo $SCRATCH`) for when no
    endpoint exists yet. It provisions nothing and costs no allocation — unless the command itself
    submits a scheduler job (sbatch/qsub/srun/salloc), which bills the allocation like a block and
    needs the user's spend confirmation first.

    Prefer `run_shell(command, shape="login")` once an endpoint is up: that runs the same
    login-node command THROUGH the endpoint (over the network), avoiding a fresh SSH — which
    on an MFA facility can force a re-auth. SSH is meant to be a one-time bootstrap, not a
    channel. Only available for an SSH facility (a catalog machine via HPC_BRIDGE_MACHINE or
    connect_facility), not local dev."""
    res = await _heartbeat(ctx, _login_shell(ctx.request_context.lifespan_context, command), "login_shell")
    return dispatch.for_agent(res)  # the agent gets each stream's marked end


async def _ready_session(
    app: AppCtx, shape: str, session_id: str
) -> tuple[GlobusRunner, Session, ShapeRuntime, session_shell.Guard | None] | ShellOutcome:
    """The shared preamble of run_shell and reset_session: reject an unsupported shape, validate the
    session id, provision + bind the runner atomically under app.lock, and refuse to dispatch when
    the block is cold, the spend is unconfirmed, or a live task owns the session. Returns the runner,
    session and shape state — with the dispatch counted in `rt.inflight` under the lock, which the caller
    MUST undo when its sync-wait ends — and the dispatch's wrong-block guard (the block job it must run in, read in
    the same locked step, and a fresh nonce; None: unguarded), or the outcome to hand back. With no facility bound it
    refuses first: the implicit local default must never be provisioned in place of the facility the agent forgot to
    connect."""
    if _no_facility_bound(app):
        return _no_facility_outcome(app)
    if reject := _shape_reject(app, shape):
        return _shape_reject_outcome(reject)
    session = Session(session_id, app.scratch_root)  # validates session_id before provisioning
    busy = None
    busy_cancelled = False
    guard: session_shell.Guard | None = None
    async with app.lock:  # provision + bind the runner atomically (no race with a concurrent stop)
        not_warm = await _ensure_warm_runner(app, shape)
        rt = _shape_runtime(app, shape)
        runner = rt.runner
        if not_warm is None:
            busy = _busy_session(app, shape, session_id)
            if busy is None:
                rt.inflight += 1  # the worker is about to be busy with this: a canary meanwhile is not a reap probe
                # The confirmed block's job: the command runs only there. Within CANARY_TTL_S no check ran first, so
                # this guard is what keeps it off a replacement the user never confirmed (session_shell).
                guard = warmth._new_guard(rt)
            else:
                busy_cancelled = app.tasks[busy].client_cancelled
    # The spend floors report the session spend, as ensure_endpoint_up's do: a reap this call found has just banked the
    # old block.
    if not_warm == "needs_confirmation":  # billed shape, spend not acknowledged (or its block reaped) -> don't dispatch
        return _with_spend(app, _needs_confirmation_outcome(app, rt))
    if not_warm == "needs_account":  # account-required facility, no account -> don't dispatch, nothing started
        return _with_spend(app, _needs_account_outcome(app, rt))
    if not_warm is not None:
        return _cold_outcome(not_warm, rt.last_canary, app=app, rt=rt)
    if busy is not None:  # a live task owns this session's cwd/env -> don't dispatch a second command
        return _busy_session_outcome(busy, shape, session_id, client_cancelled=busy_cancelled,
                                     internal=session_id == _INTERNAL_SESSION)
    assert runner is not None  # _ensure_warm_runner returns None only after binding the runner
    return runner, session, rt, guard


async def _detect_submission(command: str, shape: str) -> Submission | None:
    """The scheduler job `command` starts by hand (sbatch/qsub/salloc, or srun outside a block), or None. The spend
    floor never sees one: the login shape is free, and from inside a block it is a new job beside the block. Worked
    out BEFORE the dispatch is counted (never between the count and the submit), off the event loop and outside
    app.lock, and never raising — recognising a submission must not fail or stall the command."""
    try:
        return await asyncio.to_thread(scheduler_ops._submission, command, inside_job=shape != "login")
    except Exception:  # noqa: BLE001 - advisory: a lexer surprise leaves the outcome as it was
        return None


async def _dispatch_counted(
    app: AppCtx, rt: ShapeRuntime, runner: GlobusRunner, shape: str, session_id: str, command: str,
    payload: Callable[[], str], *, agent_call: bool, reset: bool = False, submission: Submission | None = None,
    guard: session_shell.Guard | None = None,
) -> ShellOutcome:
    """Submit `payload()` — a dispatch _ready_session counted in `rt.inflight` — wait the bounded sync-wait OFF the
    lock, and hand the count back on EVERY path. A failed submit is a dispatch failure like any other. A command still
    running past the wait comes back as a poll handle (`command` is what the handle records).

    A SENT task stays tracked while it can still report: the endpoint it was sent to is still bound and its shape's
    runner is the one it went out on (`tracked`). That holds even when a stop released the block and dropped the shape
    meanwhile — Executor.shutdown cancels only UNSENT tasks, and the SDK's result watcher still resolves sent ones —
    so the task keeps its handle (poll_task returns it; stop refuses honestly while it runs), but the dropped runtime is
    never rebuilt for it and nothing is noted on it. A teardown or re-bind abandons tasks (`_drop_all_shapes`), and a
    runner swapped for a new endpoint drains them (`_runner_for`); there it is reported, not tracked.

    `agent_call`: the agent's own run_shell / reset_session. If the CLIENT cancels it (Esc in Claude Code →
    notifications/cancelled → `_heartbeat` cancels this coroutine), in the sync-wait or queued for the lock after it,
    its command is not cancelled with it: untracked, a stop would answer a false 'down', a forced canary would queue
    behind it and read as a reap, and a second command could clobber the session. So the count is handed to a poll
    handle marked `client_cancelled` in ONE synchronous step (no await: a cancelled call must not wait for the lock, and
    nothing interleaves without one), and the cancellation propagates. An internal call (the scheduler ops, which run
    in their own login session, `_INTERNAL_SESSION`) that is cancelled is not tracked, as before. `reset`: the dispatch
    is a reset_session (its running notice says so).

    `guard`: the wrong-block guard `payload` carries (session_shell: the block job and this dispatch's nonce). A result
    the guard refused did NOT run: it becomes the reap of the confirmed block (warmth._wrong_block_outcome), here or —
    through the handle, which records the guard — whenever a poll resolves it, a client-cancelled call's command
    included.

    (A cancel BEFORE the submit — still in _ready_session — propagates from there with nothing counted or sent; the
    submit is synchronous, so once anything was sent the future is in hand.)"""
    counted: ShapeRuntime | None = rt  # the in-flight count _ready_session took, until it is handed back
    fut: Any = None  # the SDK future, once sent
    sub = submission  # the scheduler job the command submits by hand, if any (_run_shell finds it before the count)

    def tracked() -> bool:
        return rt.runner is runner and runner.endpoint_id == app.state.endpoint_id

    try:
        try:
            fut = runner.submit(payload())  # submit; wait a bounded time OFF the lock, else hand back a handle
            # on the record while it runs (#24): a server that restarts mid-command must still see it
            warmth._note_command_sent(app, shape, rt, fut, getattr(runner, "task_client", lambda: None)(),
                                      runner.walltime)
            res = await asyncio.to_thread(fut.result, runner.timeout)
        except asyncio.CancelledError:
            me = asyncio.current_task()
            if fut is None or not fut.cancelled() or (me is not None and me.cancelling()):
                raise  # the CLIENT cancelled the call: handled below (a cancelled future here is the SDK's)
            # NOT this call: the SDK cancelled the task before sending it — its runner was closed under the call
            out = _never_sent_outcome(app)
        except TimeoutError as exc:
            if fut is None:  # the submit itself timed out: nothing was sent
                out = dispatch.failure_outcome(exc, "warm")
            else:  # still running past the sync-wait -> a poll handle, NOT a kill
                async with app.lock:
                    rt.inflight -= 1  # the handle takes over as the liveness signal, in the same locked step
                    counted = None
                    if not tracked():
                        return _released_under_outcome(app, None, runner.walltime)
                    task_id = _register_task(app, shape, session_id, command, fut, runner.walltime, runtime=rt,
                                             reset=reset, submission=sub, guard=guard)
                    released = app.shapes.get(shape) is not rt
                    out = _with_submission_notice(
                        _running_outcome(app, task_id, runner.walltime, reset=reset,
                                         block_state="cold" if released else "warm"), sub, shape)
                    if released:  # released under the call: tracked, but don't revive the shape
                        return _released_under_outcome(app, out, runner.walltime)
                    _note_dispatch(rt, out)  # the worker took our task -> it's alive
                    return out
        except Exception as exc:  # noqa: BLE001 - the submit or the task failed: a structured outcome, never a raise
            out = dispatch.failure_outcome(exc, "warm")
        else:
            out = dispatch.complete_outcome(res, "warm")
        async with app.lock:
            rt.inflight -= 1
            counted = None
            if fut is not None:  # a future that failed on the client's side alone: the task may live on — evidence
                warmth._keep_lost_command(app, shape, rt, fut, getattr(runner, "task_client", lambda: None)(),
                                          ceiling_s=runner.walltime)
                warmth._note_command_done(app, fut)  # its result is in: off the record
            if guard is not None and (refused := warmth._guard_refusal(out, guard)) is not None:
                # refused on a replacement block: nothing ran, so no submission and no proof of life — the reap
                return warmth._wrong_block_outcome(app, shape, rt, guard, refused, reset=reset)
            out = _with_submission_notice(out, sub, shape)  # the command ran (or may have): its job is spend
            if app.shapes.get(shape) is not rt:  # released under the call: say so, and don't revive the shape
                sent = fut is not None and not fut.cancelled()
                return _released_under_outcome(app, out, runner.walltime) if sent else _with_spend(app, out)
            _note_dispatch(rt, out)
            return _with_spend(app, out)
    except asyncio.CancelledError:
        if counted is not None:  # cancelled before the count was handed back
            rt.inflight -= 1
            counted = None
            if agent_call and fut is not None and not fut.done() and tracked():  # still running and reachable
                _register_task(app, shape, session_id, command, fut, runner.walltime, client_cancelled=True,
                               runtime=rt, reset=reset, submission=sub, guard=guard)
        raise
    finally:
        if counted is not None:  # an exception escaped before the count was handed back
            counted.inflight -= 1


async def _run_shell(
    app: AppCtx, command: str, session_id: str = "default", shape: str = DEFAULT_SHAPE, *, agent_call: bool = False
) -> ShellOutcome:
    """`agent_call`: the agent's own command (the run_shell tool) — a client cancel keeps it tracked, and the internal
    session is off limits. Internal callers (the scheduler ops on the login shape) leave it False."""
    if agent_call and session_id.startswith(_RESERVED_SESSION_PREFIX):
        return _reserved_session_outcome(session_id)
    # Before the dispatch is counted: once it is, nothing may await before its submit (a stop queued behind it must
    # find the command sent), and a cancel here propagates with nothing counted or sent.
    sub = await _detect_submission(command, shape)
    ready = await _ready_session(app, shape, session_id)
    if isinstance(ready, ShellOutcome):
        return ready
    runner, session, rt, guard = ready
    return await _dispatch_counted(app, rt, runner, shape, session_id, command,
                                   lambda: session_shell.wrap(command, session, guard=guard),
                                   agent_call=agent_call, submission=sub, guard=guard)


async def _reset_session(
    app: AppCtx, session_id: str = "default", shape: str = DEFAULT_SHAPE, *, agent_call: bool = False
) -> ShellOutcome:
    if agent_call and session_id.startswith(_RESERVED_SESSION_PREFIX):
        return _reserved_session_outcome(session_id)
    ready = await _ready_session(app, shape, session_id)
    if isinstance(ready, ShellOutcome):
        return ready
    runner, session, rt, guard = ready
    # A reset is a task like any other: queued behind work on the worker it can outlive the sync-wait (a poll handle)
    # or be cancelled mid-wait (tracked) — untracked, it would keep the endpoint asking for a block after a stop. It is
    # guarded like a command: on a replacement block it would be work there unasked, and a reset that "succeeded" there
    # would refresh the trust window and hide the replacement from the next check.
    return await _dispatch_counted(app, rt, runner, shape, session_id, "reset_session",
                                   lambda: session_shell.reset_command(session, guard=guard),
                                   agent_call=agent_call, reset=True, guard=guard)


async def _poll_task(app: AppCtx, task_id: str, wait: float = 0.0) -> ShellOutcome:
    """Retrieve a running task's result (or report it still running). Optionally block up to `wait`
    seconds for it OFF the lock, then re-check under the lock.

    A pending future is only 'running' if something can still resolve it. If the endpoint behind the
    task is offline/gone (torn down by us or by someone else, a facility outage), the future never
    resolves and an agent would poll forever — seen live 2026-08-19: 25 polls over 20 minutes after
    another process deleted the endpoint. So a pending task on a dead endpoint is reported as a
    terminal `failed` (orphaned) and its handle dropped. One still pending past its ceiling (counted from its
    submit) is still `running` — it may be queued behind another task or a relaunching block, or lost — and the
    notice says so and what, for its shape, can abandon it."""
    wait = max(0.0, min(wait, 600.0))  # a bounded courtesy wait; never an unbounded tool hang
    async with app.lock:
        resolved = _resolve_task(app, task_id)
        if resolved is not None:
            return resolved
        handle = app.tasks[task_id]  # resolved is None => the still-running handle is present
        fut, ceiling_s, cancelled, reset = handle.future, handle.ceiling_s, handle.client_cancelled, handle.reset
    # A poll_task call cancelled anywhere below drops nothing: the handle is only ever popped once the task resolved
    # (or its endpoint is gone), and a cancellation propagates past every await here.
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
            orphan = app.tasks.pop(task_id, None)
            if orphan is not None:
                rt = app.shapes.get(orphan.shape)
                if rt is not None and (orphan.runtime is None or orphan.runtime is rt):
                    _bank_warm_interval(rt, app)  # its endpoint is gone: the block's clock stops with the verdict
                return _orphaned_outcome(app, task_id)
    still = app.tasks.get(task_id)
    # its block was released under it (its runtime is no longer the shape's): what it ran on reads cold, as in the
    # call's own result
    current = app.shapes.get(handle.shape)
    block: Literal["warm", "cold"] = (
        "warm" if current is not None and (handle.runtime is None or handle.runtime is current) else "cold")
    if still is not None and warmth._past_ceiling(still):
        return _past_ceiling_outcome(app, task_id, ceiling_s, shape=still.shape, session_id=still.session_id,
                                     facility_mep=not _has_login_shape(app), block_state=block,
                                     lost_in=still.ceiling_s + warmth._LOST_TASK_GRACE_S
                                     - (time.monotonic() - still.submitted_at))
    return _running_outcome(app, task_id, ceiling_s, client_cancelled=cancelled, reset=reset, block_state=block)


@mcp.tool()
async def run_shell(
    command: str, ctx: Context, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    """Run a shell command on the warm HPC compute block.

    `shape` picks the execution target on the same endpoint: "compute" runs on a
    scheduler block (heavy compute, billed, idle-released); "login" runs on the login
    node via a LocalProvider (lightweight, no allocation). A batch job submitted from either shape
    (sbatch/qsub/salloc, or srun from the login node) bills the allocation like a block, outside
    hpc-bridge's tracking, so it needs the user's spend confirmation first. Sessions (cwd/env)
    persist per session_id within a shape. A billed shape whose spend is not confirmed answers
    phase="needs_confirmation" (nothing runs), with `block`: the block's queue and account, each with
    where it came from, and its size — name them to the user.

    LONG WORK: run it as a normal (foreground) command — do NOT background/detach it. A command
    still running past the sync-wait comes back phase="running" with a task_id; poll it with
    poll_task(task_id) until phase="complete". The task runs up to the block walltime and keeps the
    block warm while it runs, so it won't be cut or idle-released — but a *detached* process is not a
    task, so the block would idle-release out from under it (issue #21)."""
    try:
        return dispatch.for_agent(await _heartbeat(ctx, _run_shell(
            ctx.request_context.lifespan_context, command, session_id, shape, agent_call=True
        ), "run_shell"))  # cut only here: _run_shell's own callers (the pilot probe, allocations) read it whole
    except Exception as exc:  # noqa: BLE001
        return _error_outcome(exc)


@mcp.tool(annotations=_READ_ONLY)
async def poll_task(task_id: str, ctx: Context, wait: float = 0.0) -> ShellOutcome:
    """Retrieve the result of a long task that run_shell returned as phase="running" (with a task_id).

    Returns phase="complete" (exit_code, stdout, stderr) once the task finishes, or phase="running"
    if it's still going — poll again. `wait` optionally blocks up to that many seconds for the result
    before returning (default 0 = check once and return now). The task runs up to the block walltime
    and the block stays warm while it runs, so a long job never needs detaching. An unknown or ended
    task_id returns a failed outcome explaining why (already retrieved, or the block was
    stopped/repointed). One with no result past its ceiling still reads phase="running" — it may only be queued
    behind a relaunching block — and its notice says how to abandon it instead."""
    try:
        return dispatch.for_agent(
            await _heartbeat(ctx, _poll_task(ctx.request_context.lifespan_context, task_id, wait), "poll_task")
        )
    except Exception as exc:  # noqa: BLE001 - never crash the tool; return a structured failure
        return _error_outcome(exc)


@mcp.tool()
async def reset_session(
    ctx: Context, session_id: str = "default", shape: str = DEFAULT_SHAPE
) -> ShellOutcome:
    """Clear a session's persisted working directory and environment (fresh slate). Like run_shell, a reset still
    waiting past the sync-wait (queued behind other work on the block) comes back phase="running" with a task_id."""
    try:
        return dispatch.for_agent(await _heartbeat(ctx, _reset_session(
            ctx.request_context.lifespan_context, session_id, shape, agent_call=True
        ), "reset_session"))
    except Exception as exc:  # noqa: BLE001 - never crash the tool; return a structured failure
        return _error_outcome(exc)


def main() -> None:
    mcp.run()
