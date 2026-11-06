"""Billed blocks an EARLIER hpc-bridge server process started: recorded, checked, named, reused, released.

Each chat session typically runs a fresh server process against the same state directory, so a restart is the normal
case between sessions — and what a process knows of its blocks (its shape runtimes, `left_blocks`, `ssh_left`) ends
with it. A block it confirmed runs on and bills until it idle-releases or its walltime ends. Free agent chains on the
fake cluster (2026-11) showed what that cost: a new session's connect said "Nothing is started or billed yet" while
job 40 ran; its yes added a second block (40+41, 9+10, 25+26); its stop before any connect said `down`, "no endpoint
was up", and released nothing — only a stop after a connect released both, its release reading every block of the
endpoint by the `uep.<eid>` marker.

So every billed block request is RECORDED in the ledger (`Ledger.record_block`, the `block` table) when its spend is
confirmed, before its check is sent (`record_request`), with its scheduler job once a check reports it (`record_up`).
A later process:
- reads the records of the endpoint it binds and checks them against the scheduler (`verify`: the endpoint's own
  pilots by its marker, through the free login endpoint — at connect, and before a spend question or a billed start):
  a record whose job is gone is closed quietly; what is still there is named, never "Nothing is started or billed yet".
- reuses one on the same terms (`reuse_for` / `adopt`): a yes whose block config would be the recorded one (the hash of
  the config it would send) takes that block request, so its check lands on that block — no second block.
- asks before a second one (`must_ask`): a yes while such a block bills, on other terms or one it cannot reuse (held,
  unverified), and that no spend question of this process named, answers `needs_confirmation` naming it.
- with no endpoint bound, a stop reaches each recorded endpoint again (`stop_cold`: the facility from the recorded
  catalog entry, a scratch context, the stop every bound SSH endpoint gets) and is `down` only once that stop is. A
  facility multi-user endpoint is named from its record (no scheduler channel there), never claimed stopped.
Ownership: only this hpc-bridge's records (the ledger is this machine's local disk), and only a server that is gone —
or this one: a block of another server still running is that session's, never reused or released from a cold process
(its own stop releases it; a stop of a session bound to the same endpoint releases every block there, as it always
has). The release itself is every stop's: by the endpoint's own marker, never a job hpc-bridge did not start.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any

from . import binding, config, notices, scheduler_ops
from .catalog.entry import CatalogEntry
from .context import (
    AppCtx,
    EarlierBlock,
    EarlierView,
    LeftBlock,
    ShapeRuntime,
    _has_login_shape,
    _no_facility_bound,
)
from .facility.remote import block_number
from .ledger import Ledger, NetworkFilesystemError, Owner
from .lifecycle import EndpointState
from .models import EndpointStatus, ShellOutcome

# A read of the scheduler this recent stands for the spend question and the billed start that follow it.
VIEW_TTL_S = 60.0
# A request with no job seen and no pilot listed, this young, may still have its check queued: named, kept. Older: gone.
YOUNG_S = 600.0
# The scheduler read's own bound: the login worker may be cold, and a connect or a spend question must not hang on it.
VERIFY_WAIT_S = 30.0
# With no scheduler read to say otherwise (a facility endpoint's record, or an endpoint this process cannot reach), a
# record is closed only once its block cannot still run: past when it came up plus its walltime, or — never seen up —
# past when it was asked for plus this queue allowance plus its walltime (a block can wait days in a busy queue; three
# days is the most hpc-bridge presumes). A walltime not on the record is taken as UNKNOWN_WALLTIME_S. On an SSH endpoint
# it can read, a scheduler read closes a record whose block is gone long before (`verify`).
QUEUE_ALLOWANCE_S = 3 * 86400.0
UNKNOWN_WALLTIME_S = 2 * 86400.0
# A block past its walltime's end by this much cannot still run (the scheduler's kill grace, a requeue aside).
WALLTIME_GRACE_S = 120.0

LoginRunner = Callable[[str], Awaitable[ShellOutcome]]

_OWNER: Owner | None = None
_WARNED: set[str] = set()


def _warn(what: str, exc: BaseException) -> None:
    """A record that cannot be written is never a reason a call fails: said once per kind on stderr."""
    if what not in _WARNED:
        _WARNED.add(what)
        print(f"hpc-bridge: the record of billed blocks could not be {what} ({type(exc).__name__}: {exc})",
              file=sys.stderr)


def _me() -> Owner:
    """This process (cached: its start time is read once — `ps` on macOS)."""
    global _OWNER
    import os

    if _OWNER is None or _OWNER.pid != os.getpid():
        _OWNER = Owner.here()
    return _OWNER


_MACHINE: str | None = None   # this machine's identity, read once ("" once read and unknown)


def _read_machine_id() -> str | None:
    """A STABLE identity of this machine — never its hostname, which a laptop's network changes (review round 2: a
    macOS hostname with no `scutil HostName` set follows the network, and every record of this machine read as another
    host's): Linux's /etc/machine-id (or D-Bus's copy), macOS's IOPlatformUUID; None when there is none to read."""
    import re
    import subprocess
    from pathlib import Path

    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            value = Path(path).read_text().strip()
        except OSError:
            continue
        if value:
            return value
    if sys.platform == "darwin":
        try:
            out = subprocess.run(["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"], capture_output=True, text=True,
                                 timeout=5, stdin=subprocess.DEVNULL).stdout
        except (OSError, subprocess.SubprocessError):
            return None
        m = re.search(r'"IOPlatformUUID"\s*=\s*"([^"]+)"', out or "")
        return m.group(1) if m else None
    return None


def _machine_id() -> str | None:
    """`_read_machine_id`, cached for the process (ioreg is a subprocess)."""
    global _MACHINE
    if _MACHINE is None:
        _MACHINE = _read_machine_id() or ""
    return _MACHINE or None


def _me_ns() -> str | None:
    """This process's pid namespace (Linux), so a pid of another namespace sharing the state dir (a container) is never
    read here as dead or alive; None where there is none to read (macOS)."""
    import os

    try:
        return os.readlink("/proc/self/ns/pid")
    except OSError:
        return None


def _ledger(app: AppCtx) -> Ledger:
    """The ledger, opened on first use (as batch's). Raises when it cannot be opened."""
    if app.ledger is None:
        app.ledger = Ledger.open()
    return app.ledger


def _scheduler(app: AppCtx) -> str | None:
    return (getattr(getattr(app.facility, "profile", None), "scheduler", None)
            or getattr(app.facility, "scheduler", None))


def wire_hash(app: AppCtx, uec: dict) -> str:
    """The identity of the user endpoint a block config asks for: the hash of the config as it is SENT
    (`dispatch_uec`) — the same config reaches the same user endpoint, and its block."""
    sent = getattr(app.facility, "dispatch_uec", lambda d: d)(dict(uec))
    return hashlib.sha256(json.dumps(sent, sort_keys=True, default=str).encode()).hexdigest()[:24]


def _request(app: AppCtx, uec: dict) -> str:
    """A block request's key: its `hpcb_block` (an endpoint this plugin stood up: one user endpoint per request), else
    the hash of its config (a facility endpoint, an endpoint of an earlier plugin: the config is the user endpoint)."""
    n = block_number(uec.get("hpcb_block"))
    return str(n) if n is not None else "cfg:" + wire_hash(app, uec)


def _wall(mono: float) -> float:
    return time.time() - (time.monotonic() - mono)


def _mono(wall: float) -> float:
    return time.monotonic() - (time.time() - wall)


# --- the record ------------------------------------------------------------------------------------------------------


def record_request(app: AppCtx, rt: ShapeRuntime) -> None:
    """Spend was confirmed for a NEW block request on the bound endpoint (warmth._provision, once its block request is
    drawn and before its check is sent): recorded, with how to reach the endpoint again — the facility id and the
    catalog entry it was bound from — its terms and this server. Best-effort: a ledger that cannot be written is said
    on stderr, never a failed call."""
    eid = app.state.endpoint_id
    if eid is None:
        return
    uec = rt.user_endpoint_config
    pin = config.machine() or ""
    # the entry the binding was built from: connect's, else a startup pin's (review round 1: a pinned session's records
    # carried none, so a later cold stop could not reach them)
    bound = app.bound_entry if app.bound_entry is not None else (
        binding._PINNED_ENTRY.get(pin) if app.machine is None else None)
    entry = bound.model_dump_json() if bound is not None else None
    me, request = _me(), _request(app, uec)
    try:
        _ledger(app).record_block(
            eid, request, facility=app.machine or pin or None, entry=entry,
            mep=not _has_login_shape(app), scheduler=_scheduler(app), uec_hash=wire_hash(app, uec),
            partition=uec.get("partition"), account=uec.get("account"),
            walltime_s=config._parse_hhmmss(uec.get("walltime")) or None, session=app.session_token,
            owner_host=me.host, owner_pid=me.pid, owner_start=me.started, owner_ns=_me_ns(),
            owner_machine=_machine_id())
    except NetworkFilesystemError:
        return   # the ledger cannot live there (batch says so); nothing can be recorded
    except Exception as exc:  # noqa: BLE001 - a record is never a reason a call fails
        _warn("written", exc)
    _forget(app, eid, request)   # this server's request now: never named as an earlier one's again


def record_up(app: AppCtx, rt: ShapeRuntime, job: str | None, since: float) -> None:
    """The block first confirmed warm, as scheduler job `job`, up since `since` (time.monotonic()). A confirmation that
    asked for no new request (a replacement a check found) is recorded here too."""
    eid = app.state.endpoint_id
    if eid is None or not job:
        return
    try:
        led = _ledger(app)
        req = _request(app, rt.user_endpoint_config)
        if not any(r["request"] == req for r in led.open_blocks(eid)):
            record_request(app, rt)
        led.block_up(eid, req, job, at=_wall(since))
    except NetworkFilesystemError:
        return
    except Exception as exc:  # noqa: BLE001
        _warn("updated", exc)


def end_endpoint(app: AppCtx, eid: str, how: str) -> None:
    """Every block of the endpoint is gone (a stop's confirmed `down`: its release cancelled every block of the
    endpoint, by its marker; a teardown): its records close."""
    app.earlier.pop(eid, None)
    try:
        _ledger(app).end_blocks(eid, how=how)
    except NetworkFilesystemError:
        return
    except Exception as exc:  # noqa: BLE001
        _warn("closed", exc)


def end_request(app: AppCtx, rt: ShapeRuntime, how: str) -> None:
    """This block request created no block (the scheduler rejected its submission): its record closes."""
    eid = app.state.endpoint_id
    if eid is None:
        return
    try:
        _ledger(app).end_blocks(eid, [_request(app, rt.user_endpoint_config)], how=how)
    except NetworkFilesystemError:
        return
    except Exception as exc:  # noqa: BLE001
        _warn("closed", exc)


def _latest_end(r: dict) -> float:
    """The latest a record's block can still run until, with no scheduler read (wall clock): when it came up plus its
    walltime; never seen up, when it was asked for plus QUEUE_ALLOWANCE_S plus its walltime."""
    wall = float(r["walltime_s"] or UNKNOWN_WALLTIME_S)
    if r["up_at"]:
        return float(r["up_at"]) + wall
    return float(r["confirmed_at"]) + QUEUE_ALLOWANCE_S + wall


def _records(app: AppCtx, eid: str | None = None) -> tuple[list[dict], str | None]:
    """The open records (of `eid`, or every endpoint), those whose block cannot still run closed first (`_latest_end`;
    never on a guess shorter than that). (rows, problem): `problem` says why the record could not be read (fail
    closed)."""
    try:
        led = _ledger(app)
        rows = led.open_blocks(eid)
    except NetworkFilesystemError:
        return [], None   # no ledger can live there: nothing was ever recorded
    except Exception as exc:  # noqa: BLE001
        return [], f"{type(exc).__name__}: {exc}"[:300]
    now, keep = time.time(), []
    for r in rows:
        how = None
        if _latest_end(r) + WALLTIME_GRACE_S < now:
            how = "walltime" if r["up_at"] else "never up"
        if how is None:
            keep.append(r)
            continue
        try:
            led.end_blocks(r["endpoint_id"], [r["request"]], how=how)
        except Exception as exc:  # noqa: BLE001
            _warn("closed", exc)
    return keep, None


def _whose(app: AppCtx, r: dict) -> str:
    """"mine" (this server's), "live" (another server that still runs here: its own session's), "elsewhere" (a server
    of another MACHINE — its stable identity known and different —, or of another pid namespace sharing the state dir:
    whether it runs cannot be told from here, so it is never reused or released; it is still read against the
    scheduler, and closed when gone), or "earlier" (a server of this machine that is gone). The hostname is never
    consulted (review round 2: a laptop's network renames it, and round 1's host test hid every record of this machine
    after a network change); an identity unknown on either side is this machine's, so a record it can verify through
    the scheduler is verified."""
    if r.get("session") == app.session_token:
        return "mine"
    theirs, ours = r.get("owner_machine"), _machine_id()
    if theirs and ours and theirs != ours:
        return "elsewhere"
    ns, my_ns = r.get("owner_ns"), _me_ns()
    if ns is not None and my_ns is not None and ns != my_ns:
        return "elsewhere"
    holder = Owner(str(r.get("owner_host") or ""), int(r.get("owner_pid") or 0), r.get("owner_start"))
    try:
        return "live" if _ledger(app).holder_alive(holder) else "earlier"
    except Exception:  # noqa: BLE001 - unknown: never another session's block taken for gone
        return "live"


def _block(r: dict, *, jobs: tuple = (), verified: bool = True) -> EarlierBlock:
    return EarlierBlock(
        endpoint_id=str(r["endpoint_id"]), request=str(r["request"]), facility=r.get("facility"),
        partition=r.get("partition"), account=r.get("account"), uec_hash=r.get("uec_hash"),
        scheduler=r.get("scheduler"), confirmed_at=float(r["confirmed_at"]), job=r.get("job"), up_at=r.get("up_at"),
        walltime_s=r.get("walltime_s"), jobs=tuple(jobs), verified=verified, mep=bool(r.get("mep")),
        owner=f"pid {r.get('owner_pid')} on {r.get('owner_host')}", owner_host=str(r.get("owner_host") or ""))


# --- checked against the scheduler -----------------------------------------------------------------------------------


async def verify(app: AppCtx, eid: str, run_login: LoginRunner) -> EarlierView:
    """Read what earlier servers left on endpoint `eid`, check it against the scheduler and keep it
    (`app.earlier[eid]`). On an SSH endpoint ONE login-shape command lists the endpoint's pilots by its marker
    (scheduler_ops._pilot_status_cmd: live and finished, each with its user endpoint and the block request that user
    endpoint was started for); a record matches its job, or a pilot of its block request (a pilot the endpoint started
    again for it). A record whose block is gone — its job no longer listed, a pilot of it finished, or no pilot of it
    long after it was asked for — is closed quietly. A facility endpoint is named from its records alone (no scheduler
    channel there). A listing that cannot be read names the records unverified."""
    rows, problem = _records(app, eid)
    now = time.monotonic()
    if problem is not None:
        view = EarlierView(at=now, unread=f"the record of earlier blocks could not be read ({problem})")
        app.earlier[eid] = view
        return view
    whose = {str(r["request"]): _whose(app, r) for r in rows}
    # an "elsewhere" record (another machine's) is read too, so one whose block is gone is closed — never named, reused
    # or released here (review round 2: they lingered for days and kept every cold stop draining)
    rows = [r for r in rows if whose[str(r["request"])] in ("earlier", "elsewhere")]
    named = {str(r["request"]) for r in rows if whose[str(r["request"])] == "earlier"}
    if not rows:
        view = EarlierView(at=now)
    elif not _has_login_shape(app):
        view = EarlierView(at=now, blocks=tuple(_block(r, verified=False) for r in rows
                                                if str(r["request"]) in named))
    else:
        view = await _read_scheduler(app, eid, rows, now, run_login)
        view = replace(view, blocks=tuple(b for b in view.blocks if b.request in named))
    app.earlier[eid] = view
    return view


async def _read_scheduler(app: AppCtx, eid: str, rows: list[dict], now: float, run_login: LoginRunner) -> EarlierView:
    sched = _scheduler(app) or next((r["scheduler"] for r in rows if r.get("scheduler")), None) or "slurm"
    n = next((int(r["request"]) for r in rows if str(r["request"]).isdigit()), None)
    cmd = scheduler_ops._pilot_status_cmd(sched, eid, request=n)
    why = None
    try:
        out = await asyncio.wait_for(run_login(cmd), VERIFY_WAIT_S)
        if out.phase != "complete" or out.exit_code != 0:
            why = f"the login endpoint answered {out.phase}"
    except TimeoutError:
        why, out = f"the login endpoint did not answer within {VERIFY_WAIT_S:.0f} s", None
    except Exception as exc:  # noqa: BLE001 - a read that failed names the records unverified
        why, out = f"{type(exc).__name__}: {str(exc)[:160]}", None
    stdout = (out.stdout or "") if out is not None else ""
    meta = [ln.split() for ln in stdout.split("\n") if ln.startswith(("HPCB_FAIL ", "HPCB_DIR ", "HPCB_BREQ "))]
    if why is None and {w[1] for w in meta if w[0] == "HPCB_FAIL" and len(w) > 1} & {"squeue", "qstat"}:
        why = "the scheduler's listing failed"
    if why is not None:
        return EarlierView(at=now, blocks=tuple(_block(r, verified=False) for r in rows), unread=why)
    listed = scheduler_ops._pilot_rows(stdout)
    done = scheduler_ops._DONE_STATES
    live = scheduler_ops._live_pilots([r for r in listed if r and r[0][:1].upper() not in done])
    finished = [r[1] for r in listed if len(r) > 1 and r[0][:1].upper() in done]
    uep = {w[1]: w[2] for w in meta if w[0] == "HPCB_DIR" and len(w) > 2}
    asked = {w[1]: w[2] for w in meta if w[0] == "HPCB_BREQ" and len(w) > 2}

    def request_of(job: str) -> str | None:
        u = uep.get(job)
        return asked.get(u) if u is not None else None

    blocks, gone = [], []
    for r in rows:
        req = str(r["request"])
        jobs = tuple(p for p in live if (r["job"] and p[0] == r["job"]) or request_of(p[0]) == req)
        if jobs:
            blocks.append(_block(r, jobs=jobs))
        elif (r["job"] or any(request_of(j) == req for j in finished)
              or time.time() - float(r["confirmed_at"]) > YOUNG_S):
            gone.append(req)    # its block is gone: closed quietly
        else:
            blocks.append(_block(r))   # asked for recently, no pilot listed yet: its check may still be queued
    if gone:
        try:
            _ledger(app).end_blocks(eid, gone, how="gone")
        except Exception as exc:  # noqa: BLE001
            _warn("closed", exc)
    return EarlierView(at=now, blocks=tuple(blocks))


async def refresh(app: AppCtx, eid: str | None, run_login: LoginRunner | None, *,
                  force: bool = False) -> EarlierView | None:
    """Before a spend question or a billed start: the view of `eid`, read again when it is older than VIEW_TTL_S or
    was never read (`force`: a connect, which reads it afresh) — only when a record of an earlier server is open there
    (a ledger read; nothing else otherwise)."""
    if eid is None:
        return None
    view = app.earlier.get(eid)
    if not force and view is not None and time.monotonic() - view.at < VIEW_TTL_S:
        return view
    rows, problem = _records(app, eid)
    if problem is None and not any(_whose(app, r) in ("earlier", "elsewhere") for r in rows):
        app.earlier[eid] = EarlierView(at=time.monotonic())
        return app.earlier[eid]
    if run_login is None and _has_login_shape(app):
        return view
    return await verify(app, eid, run_login or _no_runner)


async def _no_runner(cmd: str) -> ShellOutcome:   # a facility endpoint's verify reads no scheduler
    raise RuntimeError("no login endpoint")


def bound_eid(app: AppCtx) -> str | None:
    """The endpoint the session's blocks go to: the bound one, else — a facility endpoint pinned at startup, before its
    first block — the facility's own (known before any bring-up)."""
    if app.state.endpoint_id is not None:
        return app.state.endpoint_id
    eid = getattr(app.facility, "endpoint_id", None) if not _has_login_shape(app) else None
    return eid if isinstance(eid, str) else None


def has_open(app: AppCtx) -> bool:
    """A record of an earlier server (gone, of this host) is open on any endpoint: a session that does not know its
    endpoint yet (a startup pin) learns it before deciding a block. A ledger read; never a scheduler one."""
    rows, _problem = _records(app)
    return any(_whose(app, r) == "earlier" for r in rows)


def _view(app: AppCtx) -> EarlierView | None:
    eid = bound_eid(app)
    return app.earlier.get(eid) if eid is not None else None


def _forget(app: AppCtx, eid: str, request: str) -> None:
    """That block request is this server's now (a confirmation reused it): no longer an earlier one."""
    view = app.earlier.get(eid)
    if view is not None:
        app.earlier[eid] = replace(view, blocks=tuple(b for b in view.blocks if b.request != request))


# --- reused on the same terms, else asked about ---------------------------------------------------------------------


def _same_config(app: AppCtx, b: EarlierBlock, uec: dict) -> bool:
    """`uec` would ask for b's user endpoint: the config it would send, with b's block request, is b's."""
    if not b.uec_hash:
        return False
    cand = dict(uec)
    n = block_number(int(b.request)) if b.request.isdigit() else None
    if n is not None:
        cand["hpcb_block"] = n
    return wire_hash(app, cand) == b.uec_hash


def _reusable(app: AppCtx, b: EarlierBlock, uec: dict) -> bool:
    """A yes on `uec` can take b's block: read from the scheduler on an SSH endpoint, no pilot of it held or refused
    as submitted (never starts), and the same config (with b's block request in it: an endpoint of this plugin; an
    endpoint of an earlier plugin keys its user endpoints on the config alone, so the same config IS b's)."""
    if b.mep or not b.verified:
        return False
    if any(state in ("H", "N") for _job, state, _r, _w in b.jobs):
        return False
    return _same_config(app, b, uec)


def _keys(b: EarlierBlock) -> set[str]:
    return {f"job:{j}" for j in b.job_ids} or {f"req:{b.request}"}


def _told(app: AppCtx, b: EarlierBlock) -> bool:
    return all((b.endpoint_id, k) in app.earlier_told for k in _keys(b))


def _billing(app: AppCtx) -> list[EarlierBlock]:
    view = _view(app)
    return list(view.blocks) if view is not None else []


def reuse_for(app: AppCtx, rt: ShapeRuntime) -> EarlierBlock | None:
    """The earlier block a NEW block request on these terms takes over, if any (warmth._provision, in place of a fresh
    block request)."""
    return next((b for b in _billing(app) if _reusable(app, b, rt.user_endpoint_config)), None)


def adopt(app: AppCtx, rt: ShapeRuntime, b: EarlierBlock) -> None:
    """Take block `b` over: its block request (so the check lands on its user endpoint, and its block), its age (a block
    dated by its scheduler job keeps its walltime: `AppCtx.job_seen_since`), and the record (this server's now: the
    caller records the request). Callers hold app.lock."""
    eid = app.state.endpoint_id
    assert eid is not None
    if b.request.isdigit():   # (a config-keyed request is the same config already: nothing to set)
        rt.user_endpoint_config["hpcb_block"] = int(b.request)
        rt.runner_stale = True
    job = b.jobs[0][0] if b.jobs else b.job
    if job and job == b.job and b.up_at is not None:
        app.job_seen_since.setdefault((eid, job), _mono(float(b.up_at)))
    rt.earlier_reused = b
    _forget(app, eid, b.request)


def _second(app: AppCtx, b: EarlierBlock, uec: dict) -> bool:
    """A yes on `uec` would start a block beside b: not one it reuses, nor — on a facility endpoint — one on the same
    config (the facility sends the check to that block itself)."""
    return not (_reusable(app, b, uec) or (b.mep and _same_config(app, b, uec)))


def must_ask(app: AppCtx, rt: ShapeRuntime) -> bool:
    """A yes that would start a block while an earlier server's block bills on this endpoint, and no spend question of
    this process has named that block: ask first (never a second block unasked)."""
    return any(_second(app, b, rt.user_endpoint_config) and not _told(app, b) for b in _billing(app))


def spend_lead(app: AppCtx, rt: ShapeRuntime, *, yes: bool = False) -> str:
    """What a spend question (needs_confirmation) says first about earlier blocks still billing here, marking them
    named. `yes`: the call carried confirm_spend=True, and a block it did not know of made it ask."""
    blocks = _billing(app)
    if not blocks:
        return ""
    view = _view(app)
    asked = yes and must_ask(app, rt)
    note = _note(app, blocks, rt.user_endpoint_config, unread=view.unread if view is not None else None, ask=asked)
    for b in blocks:
        app.earlier_told.update((b.endpoint_id, k) for k in _keys(b))
    return note


def _why_not(app: AppCtx, b: EarlierBlock, uec: dict) -> str | None:
    """Why a yes on `uec` would not reuse b (None: it would): "unverified" (the scheduler could not be read), "held"
    (a pilot of it never starts as it is), "terms" (another config)."""
    if _reusable(app, b, uec):
        return None
    if not b.verified:
        return "unverified"
    if any(state in ("H", "N") for _job, state, _r, _w in b.jobs):
        return "held"
    return "terms"


def _note(app: AppCtx, blocks: list[EarlierBlock], uec: dict, *, unread: str | None = None, ask: bool = False) -> str:
    ssh = [(b, _why_not(app, b, uec)) for b in blocks if not b.mep]
    mep = [b for b in blocks if b.mep]
    out = notices._earlier_note(app, ssh, unread=unread) if ssh else ""
    out += "".join(notices._earlier_mep_note(b, b.scheduler or _scheduler(app)) for b in mep)
    if ask and out:
        out += notices._EARLIER_ASK
    return out


def connect_note(app: AppCtx) -> str:
    """connect_facility's words for what earlier servers left on the endpoint it bound (after `verify`), against the
    next block's terms (`warmth._seed_uec`: what a new compute runtime would get)."""
    from . import warmth  # warmth imports this module

    view = _view(app)
    if view is None or not view.blocks:
        return ""
    return _note(app, list(view.blocks), warmth._seed_uec(app, "compute"), unread=view.unread)


def receipt(app: AppCtx, rt: ShapeRuntime) -> str:
    """The answer that newly confirmed a block: the earlier block it reused (said once), or the earlier ones still
    billing beside it."""
    if rt.earlier_reused is not None:
        b, rt.earlier_reused = rt.earlier_reused, None
        return notices._earlier_reused_note(b)
    # (not the block this confirmation landed on — a facility endpoint's same config is that block)
    beside = [b for b in _billing(app) if b.job_ids and rt.block_job not in b.job_ids
              and _second(app, b, rt.user_endpoint_config)]
    return notices._earlier_beside_note(beside) if beside else ""


def handoff_note(app: AppCtx) -> str:
    """export_handoff's words for an earlier server's block still billing on the endpoint: its billed canary starts a
    block beside it (review round 1, as the spend question names it)."""
    blocks = [b for b in _billing(app) if b.job_ids]
    return notices._earlier_handoff_note(blocks) if blocks else ""


def released_note(app: AppCtx, eid: str, detail: str) -> str:
    """A stop's release cancelled jobs an earlier server left: which ones, for its answer."""
    view = app.earlier.get(eid)
    if view is None:
        return ""
    ids = set(scheduler_ops._released_jobs(detail))
    theirs = [j for b in view.blocks for j in b.job_ids if j in ids]
    return notices._earlier_released_note(theirs) if theirs else ""


# --- a facility endpoint: named from the record --------------------------------------------------------------------


def _left(b: EarlierBlock) -> LeftBlock:
    """An earlier server's facility-endpoint block as a block a stop left (`app.left_blocks`): named, never billed."""
    end = b.walltime_end
    return LeftBlock(endpoint_id=b.endpoint_id, job=b.job, until=_mono(end) if end is not None else None,
                     left_at=_mono(float(b.up_at or b.confirmed_at)), how="session",
                     kind="block" if b.job else "check", bound="earlier" if end is not None else None,
                     ends_by=_mono(end) if end is not None else None, verified=bool(b.job),
                     partition=b.partition, account=b.account)


def mep_left(app: AppCtx, eid: str | None) -> str:
    """A bind of a facility endpoint: the blocks earlier servers left there, from their records (no scheduler channel to
    check them), named in its answer and kept in `app.left_blocks`, so a stop names them as well. Callers hold
    app.lock."""
    if eid is None:
        return ""
    rows, _problem = _records(app, eid)
    blocks = [_block(r, verified=False) for r in rows if r.get("mep") and _whose(app, r) == "earlier"]
    app.earlier[eid] = EarlierView(at=time.monotonic(), blocks=tuple(blocks))
    if not blocks:
        return ""
    have = {(b.endpoint_id, b.job) for b in app.left_blocks if b.job}
    app.left_blocks += [_left(b) for b in blocks if (b.endpoint_id, b.job) not in have or not b.job]
    return "".join(notices._earlier_mep_note(b, b.scheduler or _scheduler(app)) for b in blocks)


# --- a stop with no endpoint bound ------------------------------------------------------------------------------------


Stopper = Callable[[AppCtx], Awaitable[EndpointStatus]]
Asker = Callable[[AppCtx], Awaitable[EndpointStatus | None]]


@dataclass
class _Target:
    """One recorded endpoint a cold stop goes to: its records, and the scratch context bound to it (None: it cannot be
    reached from here, `part` saying why)."""

    eid: str
    recs: list[dict]
    scratch: AppCtx | None = None
    part: EndpointStatus | None = None
    failed: bool = False   # its stop raised: `part` says so
    ask_error: str | None = None   # its ask failed: its stop still runs, answered unconfirmed (as a bound stop's)
    done: bool = False     # a read closed every record there: nothing to stop


async def stop_cold(app: AppCtx, stop: Stopper, ask: Asker | None = None,
                    login_of: Callable[[AppCtx], LoginRunner] | None = None) -> EndpointStatus | None:
    """stop_endpoint with no endpoint bound (a fresh server's first call, a startup pin before its first block): the
    blocks the record names — this server's, and earlier servers' that are gone — are reached again, each endpoint on a
    scratch context bound to it for this call alone (nothing stays bound here), and get the WHOLE stop a bound endpoint
    gets (`stop`: server._stop_endpoint with the call's own jobs= and handoffs= — its handoff question and scope, its
    Jobs-mode keep/cancel, its busy refusal; review round 1's blocker: the block release alone had cancelled a
    handoff consumer's pool unasked). What any of them would ASK (`ask`, read only) is asked first, for all of them, and
    then nothing is changed anywhere; an ask that fails still lets that endpoint's stop run, answered unconfirmed, as a
    bound stop does (review round 2). A block of another server that still runs, or of another machine, is named and
    left alone — another machine's is first read against the scheduler (`login_of`: the scratch's login runner), and
    closed when gone. The session's lock is held throughout: a connect and a confirmation meanwhile cannot have their
    new block released under them. None: nothing on record, or an endpoint got bound meanwhile (the caller's bound
    stop). `down` with spend_stopped true only when every endpoint's stop said so."""
    rows, problem = _records(app)
    if problem is not None:
        return EndpointStatus(status="draining", block_state="cold", notice=notices._earlier_unread_stop(problem))
    whose = {(r["endpoint_id"], r["request"]): _whose(app, r) for r in rows}
    # this server's own facility-endpoint blocks are its `left_blocks` (a detach keeps them; their walltime prunes them)
    rows = [r for r in rows if not (r.get("mep") and whose[(r["endpoint_id"], r["request"])] == "mine")]
    if not rows:
        return None
    async with app.lock:
        if app.state.endpoint_id is not None:
            return None
        targets: list[_Target] = []
        try:
            for eid in dict.fromkeys(r["endpoint_id"] for r in rows):
                targets.append(await _target(app, eid, [r for r in rows if r["endpoint_id"] == eid], whose, login_of))
            reach = [t for t in targets if t.scratch is not None and t.part is None]
            if ask is not None:
                questions = [q for q in await asyncio.gather(*(_guarded(t, ask, read=True) for t in reach))
                             if q is not None and q.status == "needs_confirmation"]
                if questions:
                    return _questions(questions)
            reach = [t for t in reach if not t.failed]
            done = await asyncio.gather(*(_guarded(t, stop) for t in reach))
            for t, st in zip(reach, done, strict=True):
                t.part = _cold_part(t, st)
            parts = [t.part for t in targets if t.part is not None]
            return _combined(parts) if parts else None   # every record closed by its read: nothing was on record
        finally:
            for t in targets:
                if t.scratch is not None:
                    await _close(t.scratch)


async def _target(app: AppCtx, eid: str, recs: list[dict], whose: dict,
                  login_of: Callable[[AppCtx], LoginRunner] | None) -> _Target:
    """A recorded endpoint: reachable — a scratch context bound to it; held by a server that still runs, or of another
    machine (read first against the scheduler where it can be, and closed when gone) — named, left alone; else why it
    cannot be reached from here (`part`)."""
    t = _Target(eid, recs)
    facility, host = recs[-1].get("facility"), _host(recs)
    try:
        fac, why = await _facility(app, recs)
        if fac is not None and app.login_flow is not None and await asyncio.to_thread(app.login_flow.login_required):
            fac, why = None, "this session has no Globus login to reach it"
    except Exception as exc:  # noqa: BLE001 - said: the endpoint is named, never a tool error
        fac, why = None, f"checking how to reach it failed ({type(exc).__name__}: {str(exc)[:160]})"
    if fac is not None:
        mep = any(r.get("mep") for r in recs)
        t.scratch = _scratch(app, fac, eid, facility, left_blocks=[_left(_block(r, verified=False)) for r in recs
                                                                   if r.get("mep")] if mep else [])
    held = [r for r in recs if whose[(eid, r["request"])] in ("live", "elsewhere")]
    if (held and t.scratch is not None and login_of is not None and _has_login_shape(t.scratch)
            and not any(whose[(eid, r["request"])] == "live" for r in held)):
        # another machine's records: read against the scheduler, so one whose block is gone is closed, not left to
        # keep every cold stop draining (review round 2)
        try:
            await verify(t.scratch, eid, login_of(t.scratch))
        except Exception:  # noqa: BLE001 - unread: they stay named
            pass
        left, _problem = _records(app, eid)
        still = {r["request"] for r in left}
        held = [r for r in held if r["request"] in still]
        t.recs = [r for r in recs if r["request"] in still]
    if held:
        here = all(whose[(eid, r["request"])] == "live" for r in held)
        blocks = [_block(r, verified=False) for r in held]
        notice = notices._earlier_live_stop(blocks, here=here)
        if not here:   # another machine's: the user can still end it by its id
            notice += notices._earlier_user_cancel(blocks, recs[-1].get("scheduler"), host)
        t.part = EndpointStatus(status="draining", block_state="warm", endpoint_id=eid,
                                block_job_id=next((b.job for b in blocks if b.job), None), notice=notice)
        return t
    if not t.recs:   # every record there was closed by the read: nothing of it is left to stop
        t.part = None
        if t.scratch is not None:
            await _close(t.scratch)
            t.scratch = None
        t.done = True
        return t
    if t.scratch is None:
        blocks = [_block(r, verified=False) for r in t.recs]
        t.part = _unreachable(blocks, facility, eid, host, why or "the record does not say how to reach it")
    return t


async def _guarded(t: _Target, call: Callable[[AppCtx], Awaitable[EndpointStatus | None]], *,
                   read: bool = False) -> EndpointStatus | None:
    """One endpoint's ask or stop: what its stop raised is that endpoint's `draining`, with the reason — never a tool
    error that leaves the others unsaid (review round 1). An ask that failed (a read of what can still bill that timed
    out) asks nothing: that endpoint's stop runs all the same and is answered unconfirmed, as a bound stop's read
    timing out is (review round 2: it had been left unreleased while the others were released)."""
    assert t.scratch is not None
    try:
        return await call(t.scratch)
    except Exception as exc:  # noqa: BLE001
        why = f"{type(exc).__name__}: {str(exc)[:160]}" if str(exc) else type(exc).__name__
        if read:
            t.ask_error = why
            return None
        blocks = [_block(r, verified=False) for r in t.recs]
        t.part = _unreachable(blocks, t.recs[-1].get("facility"), t.eid, _host(t.recs),
                              f"the stop there failed ({why})")
        t.failed = True
        return t.part


def _cold_part(t: _Target, st: EndpointStatus | None) -> EndpointStatus | None:
    """A reached endpoint's stop, led by what the record named there; `down` with spend_stopped true only on that stop's
    own — an offline endpoint's orphaned `down` is no confirmation, nor is an unconfirmed release: `draining`, with the
    user's own cancel of the job on record."""
    if st is None or t.failed:
        return t.part
    blocks = [_block(r, verified=False) for r in t.recs]
    if any(b.mep for b in blocks):   # a facility endpoint: no cancel channel; its answer names what may still bill
        st.notice = notices._earlier_cold_lead(blocks, t.recs[-1].get("facility"), t.eid, reached=None) + (
            st.notice or "")
        return st
    out = st.model_copy()
    reached = (st.status == "down" and st.spend_stopped is True) or st.status in ("up", "needs_confirmation")
    if t.ask_error is not None and out.status == "down":   # what of ours can still bill there was not read first
        out.status, out.spend_stopped = "draining", None
        out.notice = (f"{out.notice or ''} The stop could not first read what of ours can still bill there "
                      f"({t.ask_error}): spend is NOT confirmed stopped — call stop_endpoint again.").strip()
    ok = out.status == "down" and out.spend_stopped is True
    out.notice = notices._earlier_cold_lead(blocks, t.recs[-1].get("facility"), t.eid, reached=reached
                                            ) + (out.notice or "")
    if not ok and st.status not in ("up", "needs_confirmation"):
        out.status, out.spend_stopped = "draining", None   # not reached, or not confirmed: never `down`
        if "NOT confirmed" not in out.notice:
            out.notice += " Spend is NOT confirmed stopped for the block on record."
        out.notice += notices._earlier_user_cancel(blocks, t.recs[-1].get("scheduler"), _host(t.recs))
    out.block_job_id = st.block_job_id or next((b.job for b in blocks if b.job), None)
    return out


def _questions(qs: list[EndpointStatus]) -> EndpointStatus:
    """What the cold stop asks before changing anything anywhere: every endpoint's question, its handoffs and jobs."""
    if len(qs) == 1:
        return qs[0]
    jobs: dict[str, Any] = {}
    for q in qs:   # the same Jobs-mode jobs are every endpoint's question: each once
        for j in q.jobs or []:
            jobs.setdefault(str(getattr(j, "id", None) or id(j)), j)
    notices_ = list(dict.fromkeys((q.notice or "").strip() for q in qs))
    return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=None,
                          handoffs=[h for q in qs for h in (q.handoffs or [])], jobs=list(jobs.values()),
                          notice=" Also: ".join(notices_))


def combined(parts: list[EndpointStatus]) -> EndpointStatus:
    """Several endpoints' stops as one answer."""
    return _combined(parts)


async def _facility(app: AppCtx, recs: list[dict]) -> tuple[Any, str | None]:
    """The facility to reach a recorded endpoint through: built from the catalog entry it was bound from (no SSH: an SSH
    facility's build reads ~/.ssh/config and the login-node pin), else the bound one when it is that facility (a
    startup pin). (facility, why not)."""
    raw = next((r["entry"] for r in reversed(recs) if r.get("entry")), None)
    if raw:
        try:
            entry = CatalogEntry.model_validate_json(raw)
            return await asyncio.to_thread(binding._facility_from_entry, entry, account=""), None
        except Exception as exc:  # noqa: BLE001 - said: the endpoint is then named, not reached
            return None, f"its facility could not be built from the record ({type(exc).__name__}: {str(exc)[:160]})"
    facility = recs[-1].get("facility")
    if not _no_facility_bound(app) and facility and (app.machine or config.machine()) == facility:
        return app.facility, None
    return None, "the record does not say how to reach it"


def _scratch(app: AppCtx, fac: Any, eid: str | None, facility: str | None, **kw: Any) -> AppCtx:
    """A context bound to one recorded endpoint for this stop alone (the session stays as it was)."""
    return AppCtx(facility=fac, profile=app.profile, machine=facility,
                  state=EndpointState(endpoint_id=eid, reused=eid is not None),
                  scratch_root=binding._resolve_scratch_root(fac), charge_factor=app.charge_factor,
                  runner_factory=app.runner_factory, login_flow=app.login_flow, ledger=app.ledger,
                  session_token=app.session_token, batch_autoloop=False, **kw)


async def _close(scratch: AppCtx) -> None:
    from . import warmth  # warmth imports this module

    await warmth._flush_all(scratch, timeout=5.0)
    for rt in scratch.shapes.values():
        if rt.runner is not None:
            rt.runner.close()


def _host(recs: list[dict]) -> str | None:
    raw = next((r["entry"] for r in reversed(recs) if r.get("entry")), None)
    try:
        return json.loads(raw).get("ssh_host") if raw else None
    except (ValueError, AttributeError):
        return None


def _unreachable(blocks: list[EarlierBlock], facility: str | None, eid: str, host: str | None,
                 why: str) -> EndpointStatus:
    return EndpointStatus(status="draining", block_state="warm", endpoint_id=eid,
                          block_job_id=next((b.job for b in blocks if b.job), None),
                          notice=notices._earlier_unreachable(blocks, facility, eid, host, why))


def _combined(parts: list[EndpointStatus]) -> EndpointStatus:
    """Several stops as one answer: the worst status, spend_stopped true only when every part's was, and every part's
    Jobs-mode jobs (each once) and handoffs (review round 2: they had been dropped)."""
    if len(parts) == 1:
        return parts[0]
    rank = {"up": 0, "draining": 1, "down": 2}
    worst = min(parts, key=lambda p: rank.get(p.status, 1))
    stopped = [p.spend_stopped for p in parts]
    out = EndpointStatus(
        status=worst.status, block_state=worst.block_state, endpoint_id=None,
        block_job_id=next((p.block_job_id for p in parts if p.block_job_id), None),
        spend_stopped=True if all(s is True for s in stopped) else (False if False in stopped else None),
        notice=" Also: ".join((p.notice or "").strip() for p in parts))
    jobs: dict[str, Any] = {}
    for p in parts:
        for j in getattr(p, "jobs", None) or []:
            jobs.setdefault(str(getattr(j, "id", None) or id(j)), j)
    out.jobs = list(jobs.values())
    out.handoffs = [h for p in parts for h in (getattr(p, "handoffs", None) or [])]
    return out
