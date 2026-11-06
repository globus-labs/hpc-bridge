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
from dataclasses import replace
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
# A record never seen up is closed after this (a facility endpoint's, which nothing here can read, or a queue lost).
NO_JOB_MAX_S = 86400.0
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
    entry = app.bound_entry.model_dump_json() if app.bound_entry is not None else None
    me = _me()
    try:
        _ledger(app).record_block(
            eid, _request(app, uec), facility=app.machine or config.machine() or None, entry=entry,
            mep=not _has_login_shape(app), scheduler=_scheduler(app), uec_hash=wire_hash(app, uec),
            partition=uec.get("partition"), account=uec.get("account"),
            walltime_s=config._parse_hhmmss(uec.get("walltime")) or None, session=app.session_token,
            owner_host=me.host, owner_pid=me.pid, owner_start=me.started)
    except NetworkFilesystemError:
        return   # the ledger cannot live there (batch says so); nothing can be recorded
    except Exception as exc:  # noqa: BLE001 - a record is never a reason a call fails
        _warn("written", exc)


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


def _records(app: AppCtx, eid: str | None = None) -> tuple[list[dict], str | None]:
    """The open records (of `eid`, or every endpoint), those that cannot still bill closed first: past their walltime,
    or never seen up for a day. (rows, problem): `problem` says why the record could not be read (fail closed)."""
    try:
        led = _ledger(app)
        rows = led.open_blocks(eid)
    except NetworkFilesystemError:
        return [], None   # no ledger can live there: nothing was ever recorded
    except Exception as exc:  # noqa: BLE001
        return [], f"{type(exc).__name__}: {exc}"[:300]
    now, keep = time.time(), []
    for r in rows:
        end = (r["up_at"] + r["walltime_s"]) if r["up_at"] and r["walltime_s"] else None
        how = ("walltime" if end is not None and end + WALLTIME_GRACE_S < now else
               "never up" if r["job"] is None and now - float(r["confirmed_at"]) > NO_JOB_MAX_S else None)
        if how is None:
            keep.append(r)
            continue
        try:
            led.end_blocks(r["endpoint_id"], [r["request"]], how=how)
        except Exception as exc:  # noqa: BLE001
            _warn("closed", exc)
    return keep, None


def _whose(app: AppCtx, r: dict) -> str:
    """"mine" (this server's), "live" (another server that still runs: its own session's), or "earlier" (gone)."""
    if r.get("session") == app.session_token:
        return "mine"
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
        owner=f"pid {r.get('owner_pid')} on {r.get('owner_host')}")


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
    rows = [r for r in rows if _whose(app, r) == "earlier"]
    if not rows:
        view = EarlierView(at=now)
    elif not _has_login_shape(app):
        view = EarlierView(at=now, blocks=tuple(_block(r, verified=False) for r in rows))
    else:
        view = await _read_scheduler(app, eid, rows, now, run_login)
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
    if problem is None and not any(_whose(app, r) == "earlier" for r in rows):
        app.earlier[eid] = EarlierView(at=time.monotonic())
        return app.earlier[eid]
    if run_login is None and _has_login_shape(app):
        return view
    return await verify(app, eid, run_login or _no_runner)


async def _no_runner(cmd: str) -> ShellOutcome:   # a facility endpoint's verify reads no scheduler
    raise RuntimeError("no login endpoint")


def _view(app: AppCtx) -> EarlierView | None:
    eid = app.state.endpoint_id
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


async def stop_cold(app: AppCtx, stop_ssh: Stopper, stop_mep: Stopper) -> EndpointStatus | None:
    """stop_endpoint with no endpoint bound (a fresh server's first call, a startup pin before its first block): the
    blocks the record names — this server's, and earlier servers' that are gone — are reached again, one endpoint at a
    time, and released by the stop a bound endpoint gets (`stop_ssh`: server._stop_endpoint_blocks, on a scratch
    context bound to that endpoint, so nothing stays bound here); a facility endpoint's are named (`stop_mep`). A block
    another server that still runs holds is named and left to it. None: nothing on record (the old "no endpoint was
    up"). `down` with spend_stopped true only when every endpoint's stop said so."""
    rows, problem = _records(app)
    if problem is not None:
        return EndpointStatus(status="draining", block_state="cold", notice=notices._earlier_unread_stop(problem))
    whose = {(r["endpoint_id"], r["request"]): _whose(app, r) for r in rows}
    # this server's own facility-endpoint blocks are its `left_blocks` (a detach keeps them; their walltime prunes them)
    rows = [r for r in rows if not (r.get("mep") and whose[(r["endpoint_id"], r["request"])] == "mine")]
    if not rows:
        return None
    held = {r["endpoint_id"] for r in rows if whose[(r["endpoint_id"], r["request"])] == "live"}
    parts: list[EndpointStatus] = []
    for eid in dict.fromkeys(r["endpoint_id"] for r in rows):
        recs = [r for r in rows if r["endpoint_id"] == eid]
        if eid in held:
            live = [_block(r, verified=False) for r in recs if whose[(eid, r["request"])] == "live"]
            parts.append(EndpointStatus(status="draining", block_state="warm", endpoint_id=eid,
                                        block_job_id=next((b.job for b in live if b.job), None),
                                        notice=notices._earlier_live_stop(live)))
        elif any(r.get("mep") for r in recs):
            parts.append(await _stop_mep_cold(app, eid, recs, stop_mep))
        else:
            parts.append(await _stop_ssh_cold(app, eid, recs, stop_ssh))
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


async def _stop_ssh_cold(app: AppCtx, eid: str, recs: list[dict], stop_ssh: Stopper) -> EndpointStatus:
    blocks = [_block(r, verified=False) for r in recs]
    facility = recs[-1].get("facility")
    host = _host(recs)
    fac, why = await _facility(app, recs)
    if fac is None:
        return _unreachable(blocks, facility, eid, host, why or "")
    if app.login_flow is not None and await asyncio.to_thread(app.login_flow.login_required):
        return _unreachable(blocks, facility, eid, host, "this session has no Globus login to reach it")
    scratch = _scratch(app, fac, eid, facility)
    try:
        st = await stop_ssh(scratch)
    finally:
        await _close(scratch)
    ok = st.status == "down" and st.spend_stopped is True
    out = st.model_copy()
    out.notice = notices._earlier_cold_lead(blocks, facility, eid) + (st.notice or "")
    if not ok and st.status != "up":
        out.status, out.spend_stopped = "draining", None   # not reached, or not confirmed: never `down`
        if "NOT confirmed" not in out.notice:
            out.notice += " Spend is NOT confirmed stopped for the block on record."
        out.notice += notices._earlier_user_cancel(blocks, recs[-1].get("scheduler"), host)
    out.block_job_id = st.block_job_id or next((b.job for b in blocks if b.job), None)
    return out


async def _stop_mep_cold(app: AppCtx, eid: str, recs: list[dict], stop_mep: Stopper) -> EndpointStatus:
    blocks = [_block(r, verified=False) for r in recs if r.get("mep")]
    fac, _why = await _facility(app, recs)
    scratch = _scratch(app, fac if fac is not None else app.facility, None, recs[-1].get("facility"),
                       left_blocks=[_left(b) for b in blocks])
    try:
        return await stop_mep(scratch)
    finally:
        await _close(scratch)


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
    if len(parts) == 1:
        return parts[0]
    rank = {"up": 0, "draining": 1, "down": 2}
    worst = min(parts, key=lambda p: rank.get(p.status, 1))
    stopped = [p.spend_stopped for p in parts]
    return EndpointStatus(
        status=worst.status, block_state=worst.block_state, endpoint_id=None,
        block_job_id=next((p.block_job_id for p in parts if p.block_job_id), None),
        spend_stopped=True if all(s is True for s in stopped) else (False if False in stopped else None),
        notice=" Also: ".join((p.notice or "").strip() for p in parts))
