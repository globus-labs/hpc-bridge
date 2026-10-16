"""The connect flow (split step 10, 2026-09-03): bind a facility and bring up its free login shape.

`_connect_facility` is the entry point the agent drives: the Globus login gate FIRST (before the registry
read and any SSH), then the resolution ladder — explicit details this session → the registry → the local
BYO cache → the SSH probe (`_propose_or_ask`, which proposes a config for the user to confirm) — then the
switch of shapes/state to the bound facility, the attach (`_connect_mep`, identity-blind) or the
bootstrap, and the allocation listing over the login shape. `_commit_proven_facility` writes a BYO
config to the local cache only once the login-shape canary has answered; `_drop_dead_pin` forgets a
login-node pin whose host is unreachable.

The login-shape channel is INJECTED (`run_login`), as in scheduler_ops: `server._connect_facility` is a
thin wrapper that supplies `server._login_runner(app)`, so tests that call `server._connect_facility` and
patch `server._run_shell` keep working. Tests that patch the probe patch `connect.discover_facility_details`
/ `connect._propose_or_ask`.
"""
from __future__ import annotations

import asyncio
import os

from . import binding, config, login_gate, warmth
from .catalog.entry import CatalogEntry
from .catalog.parsers import PARSERS
from .context import DEFAULT_SHAPE, AppCtx, LeftBlock, ShapeRuntime, _facility_shapes, _has_login_shape
from .discovery import discover_facility_details
from .dispatch import for_agent
from .facility.remote import NeedsPreauth, SshTarget, key_accepted_second_factor_pending
from .lifecycle import EndpointState, ensure_warm
from .models import BlockTerms, ConnectFacilityResult, FacilityDetails, validate_host
from .notices import (
    _block_terms,
    _connect_terms_note,
    _explain_provision_error,
    _first_contact_note,
    _hpc_bridge_error,
    _left_block_clause,
    _needs_login_result,
    _needs_preauth_result,
    _unapplied_presets_note,
)
from .scheduler_ops import LoginRunner
from .warmth import _drop_all_shapes


def _commit_proven_facility(app: AppCtx, facility: str) -> None:
    """PROVEN: the login shape's canary answered — the only step that exercises the network interface
    the probe flags as its riskiest guess. Only now does a BYO config earn a zero-probe reconnect
    (decision 2026-09-03; caching on acceptance remembered a wrong interface every session)."""
    pending = app.pending_facility_cache.pop(facility, None)
    if pending is None:
        return
    if app.state.reused and facility not in app.bootstrapped_facilities:
        # The canary ran on an endpoint that was ALREADY online under an earlier config — it proves nothing
        # about these details (review 2: a bogus interface got committed this way). Leave the old proven
        # entry as it is. An endpoint THIS session bootstrapped and then re-found online is ours: the
        # canary did run on these details (live 2026-09-04: a fresh BYO bring-up was never cached).
        return
    binding._facility_store().put(*pending)


def _reuse_note(app: AppCtx, facility: str, reused: bool, fac) -> str:
    if reused and facility in app.bootstrapped_facilities:
        return "reconnected to the endpoint this session started (zero-SSH). "
    if reused:
        return "reused the already-online endpoint (zero-SSH reconnect). "
    return _first_contact_note(fac)

def _venv_note(fac) -> str:
    """What this bring-up did to hpc-bridge's endpoint venv, when it built one (a first build, or a repair)."""
    res = getattr(getattr(fac, "cli", None), "venv_built", None)
    if res is None or not getattr(res, "built", False):
        return ""
    aside = f"; the broken one was moved to {res.moved_aside}" if getattr(res, "moved_aside", None) else ""
    return f"built hpc-bridge's endpoint venv {res.path} ({res.reason}){aside}. "


def _master_alive(target) -> bool:
    """Is a shared SSH connection (ControlMaster) already open for `target`? `ssh -O check` against its socket."""
    import subprocess

    if not getattr(target, "control_dir", None):
        return False
    try:
        r = subprocess.run(target.control_argv("check"), capture_output=True, text=True, timeout=10, check=False)
    except Exception:  # noqa: BLE001 - no ssh / odd target: treat as no master
        return False
    return r.returncode == 0


def _tcp_answers(host: str, port: int = 22, timeout_s: float = 3.0) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


_ALIAS_PROBE = _tcp_answers  # injectable: tests replace it; production probes the alias's sshd port


async def _online_endpoint(fac) -> str | None:
    """UUID of an online endpoint of ours at `fac`, asked of the Globus web service only — NO SSH. None when
    there is none, or the facility cannot say (no finder, no profile, a web error: `find_online_endpoint`
    already swallows those to None so the caller falls through to the path that surfaces them)."""
    finder = getattr(fac, "find_online_endpoint", None)
    name = getattr(getattr(fac, "profile", None), "endpoint_name", None)
    if finder is None or not name:
        return None
    try:
        return await finder(name)
    except Exception:  # noqa: BLE001 - "can't tell" reads as "none online"; the bootstrap path reports the cause
        return None


def _drop_dead_pin(fac) -> bool:
    """A login-node PIN (endpoints.json) that no longer answers is dropped, so the next connect goes
    back to the facility's canonical host — pins used to be permanent (vault audit 2026-09-03). Only
    an UNREACHABLE host qualifies: a refused login keeps the pin (the host is fine; the access isn't).
    True when a pin was in use and got dropped."""
    store, alias = getattr(fac, "store", None), getattr(fac, "alias", None)
    target = getattr(getattr(fac, "cli", None), "target", None)
    name = getattr(getattr(fac, "profile", None), "endpoint_name", None)
    if store is None or not alias or target is None or not name or getattr(target, "host", alias) == alias:
        return False
    if not _ALIAS_PROBE(alias):
        # The facility's canonical host doesn't answer either: a CLIENT-side outage (VPN off, no network),
        # not a dead pin. Dropping it would send teardown/login_shell to the round-robin alias later
        # and orphan the endpoint the pin protects (review 2). Keep the pin.
        return False
    try:
        store.remove(alias=alias, name=name)
    except Exception:  # noqa: BLE001 - best-effort hygiene; the structured failure is what matters
        return False
    return True

def _discovered_partition(app: AppCtx, facility: str, entry: CatalogEntry) -> bool:
    """A facility you bring yourself whose partition is the one the login-node probe proposed for it this session
    (kept by the details= the user confirmed): its default partition was discovered, not named."""
    draft = app.proposed_drafts.get(facility)
    return (entry.facility_key == binding.SESSION_KEY and draft is not None and draft.ssh_host == entry.ssh_host
            and draft.partition == entry.defaults.partition)

def _next_block_terms(app: AppCtx) -> BlockTerms | None:
    """The terms the bound facility's next billed block would start on — read from the config a new compute runtime
    would get (`warmth._seed_uec`); nothing is built or started. None if it cannot be read (the connect goes on)."""
    if DEFAULT_SHAPE not in _facility_shapes(app.facility):
        return None
    try:
        return _block_terms(app, ShapeRuntime(user_endpoint_config=warmth._seed_uec(app, DEFAULT_SHAPE)))
    except Exception:  # noqa: BLE001 - a preview must never fail the connect it describes
        return None

async def _connect_facility(
    app: AppCtx, facility: str, ssh_host: str | None = None, details: FacilityDetails | None = None,
    *, run_login: LoginRunner,
) -> ConnectFacilityResult:
    # Globus login gate — FIRST, before the catalog read and before any SSH. Every non-`unsupported`
    # outcome needs Globus (the SSH path seeds the endpoint's credential from our token storage; the
    # MEP path dispatches with it) — and, found in review, constructing the Compute SDK Client for
    # the catalog on a fresh install would run the SDK's OWN command-line login: a URL on stdout and
    # input() on stdin — i.e. the MCP transport. A phase, not a prompt: the agent shows the link, the
    # user's browser completes it, the next call proceeds. (login_required() is a local SQLite read; the check that
    # the stored login still works adds a token refresh only for an access token past its expiry.)
    app.proposed_facility = None  # a draft belongs to the connect that made it: this one proposes again, or doesn't
    if ssh_host:
        try:
            ssh_host = validate_host(ssh_host)
        except ValueError as exc:
            return ConnectFacilityResult(phase="failed", facility=facility, notice=f"hpc-bridge error: {exc}")
    # A stored login Globus refuses to renew is gated like a missing one (live 2026-10-14: it passed, the MEP attach
    # said only "template not readable", and an SSH bootstrap would copy the dead token to the login node); a check
    # that cannot reach Globus carries on as before.
    verdict = (await login_gate._stored_login(app.login_flow))[0] if app.login_flow is not None else "works"
    if app.login_flow is not None and verdict in ("missing", "expired"):
        why = login_gate.EXPIRED_WHY if verdict == "expired" else None
        start, status = await login_gate._start_login_and_wait(app.login_flow)
        if status != "done":
            return _needs_login_result(facility, start, app.login_flow.error,
                                       waited_s=config.login_wait_s() if status == "waiting" else None, why=why)
        # the browser flow completed while we waited — carry straight on with the connection, but NAME the
        # identity that landed: on this (most common) path nothing else would (security review B-02). A new login may
        # be another identity, and runners hold the old token: forget both first, as authenticate() does.
        warmth._forget_identity_verdicts(app)
        login_note = ("Globus login landed" + await login_gate._as_identity() + (login_gate.REPLACED if why else "")
                      + ". ")
    else:
        login_note = ""
    entry: CatalogEntry | None = None
    # Resolve the entry: a session-local one the agent already supplied wins; else the catalog. An
    # index error is treated as "unresolved" (the agent can still supply details), not a hard fail.
    if details is not None:
        # An explicit details= is a (re)definition — it OVERRIDES any cached session entry or catalog
        # match, so a correction after discovery actually takes effect. Previously the cached entry
        # (frozen on the FIRST call — even one that later failed) silently won, so a wrong field could
        # never be fixed and stranded the whole session (seen live on Midway).
        if details.env_setup is None:
            # env_setup is optional after a proposal: the proposal's line is used, never an agent's copy of it
            draft = app.proposed_drafts.get(facility)
            if draft is None or draft.env_setup is None:
                return ConnectFacilityResult(
                    phase="failed", facility=facility,
                    notice=("details= has no env_setup and there is no proposal for this facility to take it from. "
                            f"Call connect_facility({facility!r}, ssh_host=…) without details for a proposed config, "
                            "or give env_setup. Nothing was started."))
            if draft.ssh_host != details.ssh_host:
                return ConnectFacilityResult(
                    phase="failed", facility=facility,
                    notice=(f"details= has no env_setup, and the proposal for {facility!r} was for {draft.ssh_host!r}, "
                            f"not {details.ssh_host!r}: its env_setup may not fit. Give env_setup, or call "
                            f"connect_facility({facility!r}, ssh_host={details.ssh_host!r}) for a proposal for that "
                            "host. Nothing was started."))
            details = details.model_copy(update={"env_setup": draft.env_setup})
        try:
            entry = binding._entry_from_details(facility, details)
        except Exception as exc:  # noqa: BLE001 - bad details -> structured failure, not a crash
            return ConnectFacilityResult(
                phase="failed",
                facility=facility,
                notice=f"invalid facility details: {type(exc).__name__}: {exc}"[:300],
            )
        app.session_facilities[facility] = entry  # this session's config; on disk only once PROVEN (below)
        if details.ssh_host:  # persist for LOCAL DISCOVERY — but only once the login shape is PROVEN warm
            app.pending_facility_cache[facility] = (details.ssh_host, details.model_dump(mode="json"))
    else:
        entry = app.session_facilities.get(facility)
        registry_error: Exception | None = None
        if entry is None:
            # THE REGISTRY WINS for any catalogued id (decision 2026-09-03: curated entries are the stable
            # ones). Found live: the maintainer's local cache held an SSH-era `globus1` config that would
            # have shadowed the registry's MEP entry and silently taken the SSH path.
            try:
                entry = await binding.make_catalog().get(facility)
            except Exception as exc:  # noqa: BLE001 - registry unreachable -> the cache may still serve
                registry_error = exc
        if entry is None:
            # LOCAL DISCOVERY: a previously-confirmed BYO config for this host, cached to disk (keyed on
            # ssh_host, canonical; facility id as fallback) — only for facilities the registry does NOT
            # know (or when it is unreachable). Used with NO SSH probe; bootstrap then reuses the online
            # endpoint over the web. A stale/invalid cache falls through to the probe.
            cached = binding._facility_store().get(ssh_host or facility)
            if cached is not None and ssh_host and cached.get("ssh_host") != ssh_host:
                # a record whose host is not the host it is filed under must never redirect the bootstrap (C-5)
                cached = None
            if cached is not None:
                try:
                    entry = binding._entry_from_details(facility, FacilityDetails(**cached))
                    app.session_facilities[facility] = entry
                except binding.TermsDirectiveError as exc:  # cached before such directives were refused: say so
                    return ConnectFacilityResult(
                        phase="failed", facility=facility,
                        notice=(f"the configuration cached for {facility!r} from an earlier session is no longer "
                                f"accepted: {exc}. Connect with details= (without that directive), or with ssh_host= "
                                "for a fresh proposal. Nothing was started."))
                except Exception:  # noqa: BLE001 - stale/invalid cached config
                    entry = None
        if entry is None and registry_error is not None:
            return await _propose_or_ask(
                facility, ssh_host,
                f"registry unavailable ({type(registry_error).__name__}); give me this facility's SSH "
                "host (ssh_host=… or HPC_BRIDGE_SSH_HOST) to probe it, or supply details= directly.", app=app)
        if entry is None:
            return await _propose_or_ask(
                facility, ssh_host,
                f"{facility!r} isn't in the catalog. Give me its SSH host (ssh_host=… or "
                "HPC_BRIDGE_SSH_HOST) and I'll probe the login node to propose a config, or supply "
                "details= directly (or list_facilities() if you meant a catalogued one).", app=app)
    reason = binding._unsupported_entry_reason(entry)
    if reason is None and entry.allocation is not None and entry.allocation.parser not in PARSERS:
        reason = (
            f"allocation parser {entry.allocation.parser!r} not implemented yet "
            f"(have: {sorted(PARSERS)})"
        )
    if reason:
        return ConnectFacilityResult(phase="unsupported", facility=facility, notice=reason)
    # HPC_BRIDGE_ACCOUNT is the account of the facility HPC_BRIDGE_MACHINE names, never of another one; a connect where
    # a preset goes unused is told so in one clause
    named = binding._names_machine(entry)
    preset = config.account() if named else None
    presets_note = _unapplied_presets_note(named=named, facility_endpoint=entry.compute_mep_uuid is not None,
                                           own_partition=entry.facility_key == binding.SESSION_KEY)
    try:
        # off the loop: it may run `ssh -G` (a subprocess with a 10 s timeout) to read ~/.ssh/config
        fac = await asyncio.to_thread(binding._facility_from_entry, entry, account=(preset or ""))
    except Exception as exc:  # noqa: BLE001 - a facility that cannot be built (bad entry, store I/O) is a structured failure
        return ConnectFacilityResult(
            phase="failed",
            facility=facility,
            notice=f"hpc-bridge error: {type(exc).__name__}: {exc}"[:500],
        )
    async with app.lock:  # switch facilities: drop the old shapes/endpoint, bind the new one
        if app.teardown_task is not None and not app.teardown_task.done():
            # The teardown runs on the facility it snapshotted, but re-binding under it would drop the shapes its
            # spend report and state-clearing read — finish it first (review 2026-09-05 #3).
            return ConnectFacilityResult(
                phase="failed", facility=facility,
                notice=("a teardown of the current endpoint is still running on its login node. Call "
                        "teardown_endpoint to wait for it (it reports 'down' when done), then connect_facility again."),
            )
        app.teardown_task = None  # a FINISHED teardown's result belongs to the endpoint this connect replaces
        # A pending one-time-code handoff belongs to the previous binding: complete_preauth must not open a
        # connection to it and tell the agent to re-connect there, dropping this facility (review 2026-09-05 #3).
        app.pending_preauth = None
        app.preauth_resume = None
        compute, old_eid, old_mep = app.shapes.get(DEFAULT_SHAPE), app.state.endpoint_id, not _has_login_shape(app)
        left = None
        if compute is not None and old_mep:
            # the old binding is a facility endpoint: nothing cancels its block, which bills on — remember it (#178)
            left = warmth._leave_block(app, DEFAULT_SHAPE, compute, how="reconnect")
        sched = getattr(app.facility, "scheduler", None)
        prior_spend = _drop_all_shapes(app, bank=True)  # the old endpoint's blocks/handles are gone
        # tasks of ours the old SSH endpoint may still hold (#24): they can start a block there after this connect
        queued_left = 0 if old_mep or old_eid is None else warmth._unresolved_count(
            app, old_eid, await warmth._read_checks(old_eid, 2.0))
        # Blocks a facility stop, detach or switch left belong to THAT endpoint: a reconnect to it keeps them (the next
        # stop still names them); a bind to any other endpoint drops them, and the account its identity was mapped to.
        here = None if "login" in _facility_shapes(fac) else getattr(fac, "endpoint_id", None)
        app.left_blocks = [b for b in app.left_blocks if here is not None and b.endpoint_id == here]
        app.billed_to = {k: t for k, t in app.billed_to.items() if here is not None and k[0] == here}
        app.job_seen_since = {k: t for k, t in app.job_seen_since.items() if here is not None and k[0] == here}
        if not app.left_blocks and (here is None or here != old_eid):
            app.worker_user = None
        app.facility = fac
        app.machine = facility
        # where this binding's default partition and account come from, for every block's terms (BlockTerms)
        app.terms_origin = binding._terms_origin(entry, discovered=_discovered_partition(app, facility, entry))
        # The session-shell root follows the bound facility — else run_shell would use the local
        # ~/.hpc-bridge path on the remote node (same resolution as lifespan's).
        app.scratch_root = binding._resolve_scratch_root(fac)
        if not _has_login_shape(app):  # a facility-run multi-user endpoint: attach, don't provision
            res = await _connect_mep(app, facility, fac)
            if prior_spend > 0 or left is not None:  # the number, and a block left billing, must not vanish
                res.notice = _prior_note(prior_spend, left, sched, same=here is not None and here == old_eid, old_mep=old_mep) + (res.notice or "")  # noqa: E501
            res.notice = login_note + presets_note + _left_checks_note(app, old_eid, queued_left) + (res.notice or "")
            return res
        # An MFA facility (curated `auth_method: mfa-otp`) with no shared connection yet: don't burn a failing
        # SSH attempt (and a fail2ban strike) to learn what the entry already says — hand off first. But ONLY
        # a bootstrap needs SSH: reuse asks the web service whether our endpoint is online and the login shape
        # runs over AMQP. So the reuse check comes first — live 2026-09-04 the code was requested, typed, and
        # then never used, because the very next step found the endpoint online (zero-SSH reconnect).
        target = getattr(getattr(fac, "cli", None), "target", None)
        if getattr(entry, "auth_method", None) == "mfa-otp" and target is not None \
                and not await asyncio.to_thread(_master_alive, target):
            online = await _online_endpoint(fac)
            if online is None:
                app.pending_preauth = (facility, target)
                app.preauth_resume = f"connect_facility({facility!r})"
                return _needs_preauth_result(facility, target, otp_ok=True)
            app.state = EndpointState(endpoint_id=online, reused=True)  # _provision then skips the bootstrap
        try:
            block = await warmth._provision(app, "login", force_canary=True)
            if not app.state.reused:  # a FRESH bootstrap: later calls re-find it online and read as reused
                app.bootstrapped_facilities.add(facility)
        except Exception as exc:  # noqa: BLE001 - provisioning unavailable (e.g. non-Linux host)
            # A denial that OFFERS an interactive method is the pre-auth handoff, not "no access": the
            # discovery probe knew this; the registry/cached bootstrap path did not (Expanse, live 2026-09-04 —
            # a curated MFA entry came back NO SSH ACCESS and the agent went hunting in ~/.ssh/config).
            if target is not None and key_accepted_second_factor_pending(str(exc)):
                app.pending_preauth = (facility, target)
                app.preauth_resume = f"connect_facility({facility!r})"
                return _needs_preauth_result(facility, target, otp_ok=True)
            notice = _explain_provision_error(exc, fac)
            if notice.startswith(("CANNOT REACH", "UNKNOWN HOST KEY")) and await asyncio.to_thread(_drop_dead_pin, fac):
                notice += " (The remembered login-node pin was dropped: the next connect resolves the facility's host afresh.)"  # noqa: E501
            return ConnectFacilityResult(phase="failed", facility=facility, notice=notice)
        if block == "warm":
            _commit_proven_facility(app, facility)
    wi_note = getattr(fac, "worker_init_note", "")  # an edited env_setup that a reused endpoint does not run
    reuse_note = (login_note + presets_note + _reuse_note(app, facility, app.state.reused, fac)
                  + (wi_note if isinstance(wi_note, str) else "") + _venv_note(fac))
    # `reused` in the RESULT means "reattached to an endpoint from before this session" — our own
    # just-started endpoint re-found online is not a reuse the agent should report as one
    reused = app.state.reused and facility not in app.bootstrapped_facilities
    if prior_spend > 0 or left is not None:  # say what it cost, and name a block left billing, rather than lose them
        reuse_note = _prior_note(prior_spend, left, sched, same=False, old_mep=old_mep) + reuse_note
    reuse_note = _left_checks_note(app, old_eid, queued_left) + reuse_note
    terms = _next_block_terms(app)
    if block != "warm":  # login node still coming up — nothing to read yet
        return ConnectFacilityResult(
            phase="provisioning",
            facility=facility,
            reused=reused,
            block=terms,
            notice=reuse_note + "bringing up the login node; call connect_facility again shortly"
            + (" to read your allocations" if entry.allocation is not None else "") + ".",
        )
    if entry.allocation is None:  # no auto-listable allocations -> the human supplies the account
        return ConnectFacilityResult(
            phase="needs_account",
            facility=facility,
            reused=reused,
            allocations=[],
            block=terms,
            notice=reuse_note + "login node is up; hpc-bridge reads no allocation listing here. "
            + _connect_terms_note(app, terms),
        )
    out = await run_login(entry.allocation.command)
    if out.phase != "complete" or out.exit_code != 0:
        return ConnectFacilityResult(
            phase="failed",
            facility=facility,
            notice=f"allocation discovery ({entry.allocation.command!r}) failed: "
            f"{out.notice or for_agent(out).stderr_snippet or out.phase}",  # its stderr, cut as run_shell's
        )
    allocations = PARSERS[entry.allocation.parser](out.stdout)
    return ConnectFacilityResult(
        phase="needs_account",
        facility=facility,
        reused=reused,
        allocations=allocations,
        block=terms,
        notice=reuse_note + ("login node is up; your allocations are listed (`allocations`). " if allocations else
                             "login node is up; the allocation listing returned none. ")
        + _connect_terms_note(app, terms, listed=bool(allocations)),
    )

def _left_checks_note(app: AppCtx, old_eid: str | None, queued: int) -> str:
    """A connect that LEFT an SSH endpoint where tasks of ours were not seen to finish (#24): while one is queued
    there, that endpoint can start a block for it — nothing of this connect ends it. Silent on a reconnect to it."""
    if not queued or old_eid is None or app.state.endpoint_id == old_eid:
        return ""
    things = "a check (or command) of hpc-bridge's" if queued == 1 else f"{queued} checks (or commands) of hpc-bridge's"
    return (f"On the endpoint this connect left ({old_eid}), {things} not seen to finish may still be queued: while "
            "one is, that endpoint can start a billed block to run it. Connecting back to it and calling "
            "stop_endpoint cancels such a block and says when none is left; teardown_endpoint there ends them. ")


def _prior_note(spent: float, left: LeftBlock | None, scheduler: str | None, *, same: bool, old_mep: bool) -> str:
    """What a re-bind did to the previous binding's blocks, with the session spend so far. An SSH facility's shapes are
    released; a facility endpoint's block is NOT — nothing here cancels it (#178): it bills until that facility's
    idle-release, so the note names it (and a stop on the same endpoint names it again)."""
    if not old_mep:
        return f"the previous facility's shapes were released (session spend so far ≈ {spent:.2f}). "
    where = "this endpoint" if same else "the previous facility's endpoint"
    note = f"the previous session on {where} was closed (session spend so far ≈ {spent:.2f})"
    if left is None:
        return note + ". "
    return f"{note}, but {_left_block_clause(left, scheduler)}" + ("; stop_endpoint names it. " if same else ". ")


async def _connect_mep(app: AppCtx, facility: str, fac) -> ConnectFacilityResult:
    """Bind a facility-run multi-user endpoint (MEP). Called under app.lock, after the bind.

    There is nothing to stand up: the facility runs the manager and its identity mapping makes our
    Globus identity a local account — zero SSH. So we only ATTACH the catalogued UUID (free) and
    read the manager's status. We deliberately do NOT warm a block here: on a MEP every shape is a
    billed scheduler block, so warming belongs behind the spend gate (ensure_endpoint_up
    confirm_spend=True), not inside connect. There is no login SHAPE through this channel (the facility's login
    nodes exist, but a MEP does not expose them), hence no allocation listing — the account (if the facility needs
    one) is passed directly."""
    try:
        block, app.state = await ensure_warm(app.facility, app.profile, app.state)
    except Exception as exc:  # noqa: BLE001 - e.g. the SDK can't reach the status API
        return ConnectFacilityResult(phase="failed", facility=facility, notice=_hpc_bridge_error(exc))
    if block != "warm":
        # The manager reported OFFLINE. It isn't ours: no retry on our side brings it up, so don't
        # say "call again shortly" — name the facility as the owner.
        return ConnectFacilityResult(
            phase="failed", facility=facility, reused=True,
            notice=(
                f"the facility's multi-user endpoint {app.state.endpoint_id} reports offline. It is "
                "run by the facility (not by hpc-bridge), so there is nothing to restart here — "
                "contact the facility / check its status page, then connect_facility again."
            ),
        )
    tmpl = ""
    if getattr(fac, "display_name", None) or getattr(fac, "endpoint_version", None):
        tmpl = (f"Facility endpoint: {getattr(fac, 'display_name', None) or 'unnamed'}"
                + (f" (v{fac.endpoint_version})" if getattr(fac, "endpoint_version", None) else "") + ". ")
    # Read the terms first: fitting the config to the facility's schema notes what it drops (a key the schema has no
    # place for) and how the worker is pinned — said here, before any spend, with the facility's other notes.
    terms = _next_block_terms(app)
    notes = list(getattr(fac, "template_notes", []) or [])
    if notes:
        tmpl += "; ".join(notes) + ". "
    where = ("Allocation names and balances come from the facility's own allocation tool/portal or your own SSH "
             "session, not from here. " if getattr(fac, "account_required", True) else "")
    return ConnectFacilityResult(
        phase="needs_account",
        facility=facility,
        reused=True,  # attached to the facility's always-on endpoint: zero SSH, nothing bootstrapped
        allocations=[],
        block=terms,
        notice=tmpl + (
            "attached to the facility's multi-user endpoint (zero SSH, nothing to bootstrap). Attaching "
            "does NOT test your identity mapping — the first block start does (no account there ⇒ a "
            "terminal NO ACCOUNT then, nothing billed). This "
            "channel is COMPUTE-ONLY: hpc-bridge reaches the facility only through its compute endpoint — the "
            "facility's login nodes exist but are outside this channel, so there is no free login shape here and "
            "every command runs on a billed scheduler block that stays warm between calls. "
            + where + _connect_terms_note(app, terms)
        ),
    )

async def _propose_or_ask(
    facility: str, ssh_host: str | None, ask_notice: str
, *, app: AppCtx | None = None) -> ConnectFacilityResult:
    """Index miss + no details: if we have an SSH host, probe the login node and PROPOSE a draft
    config; otherwise ask the agent for the host (SSH access is the one irreducible input). The
    discovery target carries the same ControlMaster socket as the later bootstrap, so probing warms
    the master the bootstrap then rides — no extra authentication."""
    host = (ssh_host or config.ssh_host() or "").strip()
    if not host:
        return ConnectFacilityResult(
            phase="needs_facility_details", facility=facility, notice=ask_notice
        )

    try:
        control_dir, persist = config._control_settings()
        key = (config.ssh_key() or "")
        target = SshTarget(
            host=host,
            user=config.ssh_user(),  # else ~/.ssh/config User
            key_path=os.path.expanduser(key) if key else None,  # else config's IdentityFile
            control_dir=control_dir,
            control_persist=persist,
        )
        draft, notes = await discover_facility_details(target)
    except NeedsPreauth as pre:  # host wants an interactive login (password/MFA) — hand off to the user
        if app is not None:
            app.pending_preauth = (facility, pre.target)  # what complete_preauth(code) will open
            # never a stale "teardown_endpoint()"; and WITH the host: an un-catalogued facility resumed without it
            # resolves to nothing and answers needs_facility_details (review 2026-10-10)
            app.preauth_resume = f"connect_facility({facility!r}, ssh_host={host!r})"
        return _needs_preauth_result(facility, pre.target, otp_ok=pre.otp_ok)
    except Exception as exc:  # noqa: BLE001 - probe/connect/creds failure -> structured result
        # The same first-contact explanation the bootstrap gives (stranger's walk): a refused SSH is
        # "NO SSH ACCESS to <host> as <user>" with the remedies, not a raw rc=255 dump.
        return ConnectFacilityResult(
            phase="failed",
            facility=facility,
            notice=_explain_provision_error(
                exc, host=host, user=config.ssh_user(),
                fallback=f"discovery over SSH to {host!r} failed: {type(exc).__name__}: {exc}"[:400],
            ),
        )
    notice = (
        "probed the login node and proposed this config — review/correct it WITH THE USER "
        "(confirm the flagged fields, above all `interface`), then call connect_facility(details=…). "
        "Leave env_setup OUT of details= unless the user wants to change it: the server keeps the proposed one. "
        "The probe has already run: do NOT re-probe the host over your own ssh. The confirmation must come "
        "from the user in THIS conversation — a remembered or previously cached confirmation is not consent. "
        "Notes: " + " | ".join(_fit_notes(notes, _PROPOSAL_CAP - 600))
    )
    if app is not None:  # nothing is bound by a proposal: a run_shell meanwhile is told to finish this connect
        app.proposed_facility = facility
        app.proposed_drafts[facility] = draft  # a details= without env_setup takes this one
    return ConnectFacilityResult(
        phase="proposed_facility_details",
        facility=facility,
        proposed_details=draft,
        notice=notice[:_PROPOSAL_CAP],
    )


_PROPOSAL_CAP = 1800  # the proposal's notice; its fixed head is under 600 chars


def _fit_notes(notes: list[str], budget: int) -> list[str]:
    """Every note keeps its start: when they run over `budget`, the longest are clipped first (never below 120 chars),
    instead of the notice losing its last notes to a blind cut. Bounded: each note is clipped at most once."""
    out = list(notes)
    for i in sorted(range(len(out)), key=lambda j: -len(out[j])):
        over = sum(len(n) for n in out) + 3 * max(len(out) - 1, 0) - budget
        if over <= 0:
            break
        if len(out[i]) > 120:
            out[i] = out[i][: max(120, len(out[i]) - over - 1)] + "…"
    return out
