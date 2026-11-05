"""Batch phase 2a: the budget envelope, Jobs mode and D2's gate and adopt (vault `Planned/Batch and swarm execution.md`
§2 "Promotion", §3, §12 D2).

- **The envelope** (`EnvelopeInfo`, the ledger's `envelope` table) is approved ONCE — submit_job's needs_confirmation
  carries the terms, the user's yes comes back as confirm_spend=True — and the server enforces it for everything
  hpc-bridge starts: every job reserves its worst case before it is submitted (`Ledger.reserve_job`, one BEGIN
  IMMEDIATE), and a compute block confirmed while it is open is admitted against it (`block_admission`). It belongs to
  the session that approved it (`AppCtx.session_token`): another session — a restarted server too — asks again, naming
  what remains. Its deadline is required (24 h by default, at most 168) and every Slurm job carries it (`--deadline`,
  in the cluster's own local time: no TZ is forced on sbatch, which would reach the job); on PBS it is soft.
  `HPC_BRIDGE_ENVELOPE_MAX` bounds the SUM of the open envelopes' caps per facility and account — every account an
  envelope admits — checked in the transaction that opens or widens one.
- **Jobs mode** (`submit_job`): SSH facilities only — a facility MEP has no free status or cancel channel. The command
  is promoted into a frozen `<scratch>/jobs/<job id>/job.sh`, written once (noclobber), its sha256 on the ledger: the
  header from the terms (`--deadline` included), the session's cwd and env as they were at submit (copied beside it),
  the command (base64: no line of it can become a directive), its exit code written beside it. `{index}` — an array
  task's index — goes through phase 1's `substitute` (a value is data, never code).
- **The meter** (hpc_bridge.meter) reads one bounded sacct (or qstat) per tick and never understates; the first job of
  an envelope is admitted only after one reading of what else the user runs.
- **D2** (round 3, the user's decision — what the design says): a recognised hand sbatch / qsub while this session's
  envelope is open asks first (needs_confirmation). On the yes the command runs UNMODIFIED inside one remote shell that
  lists the user's jobs immediately before it and — into the session dir, so it is taken when the command ENDS even
  past the sync-wait — immediately after it (`hand_command`). The jobs that appeared between are adopted after the
  fact (`reconcile_hand`): ledgered (source `adopt`), metered, reserved even past the cap (which then closes
  admission, said by how much), named as jobs that appeared while the approved command ran, with no envelope deadline
  applied. hpc-bridge never cancels them on its own: the stop asks about them by name. The `hand` ledger row is written
  before the command is sent, so whatever its outcome the next call (or the stop) adopts what it submitted.
- **A stop** (server._stop_endpoint) with jobs of ours that can still bill — on any facility — or an approved hand
  submit not yet accounted for asks `jobs="cancel"|"keep"` and changes nothing (`stop_question`); proceeding it closes
  admission first, adopts, cancels on "cancel" (`cancel_for_stop`) and reads again before it answers (`after_stop`),
  all under one deadline.
"""
from __future__ import annotations

import asyncio
import base64
import calendar
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import time
from dataclasses import dataclass, field
from typing import Any

from . import batch, meter, resources, session_shell
from .config import _parse_hhmmss
from .context import AppCtx, _has_login_shape
from .ledger import (
    ENVELOPE_ADMITTING,
    JOB_LIVE,
    EnvelopeCeiling,
    EnvelopeClosed,
    EnvelopeError,
    EnvelopeExhausted,
    EnvelopeLimit,
    EnvelopeNotOwned,
    Ledger,
    LedgerError,
    envelope_accounts,
    job_hold,
)
from .models import (
    BatchStatus,
    BlockLeft,
    CancelResult,
    EndpointStatus,
    EnvelopeInfo,
    EnvelopeRequest,
    JobInfo,
    JobStatus,
    JobTerms,
    PartitionShape,
    ShellOutcome,
    Submission,
    UnledgeredJob,
)

JOB_ID = re.compile(r"^job-[0-9a-f]{12}$")
ENV_ID = re.compile(r"^env-[0-9a-f]{12}$")
DEFAULT_DEADLINE_H = 24.0
MAX_DEADLINE_H = 168.0
MAX_TASKS = 10_000
UNIT = "billing units"
_VALID_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_WALLTIME = re.compile(r"^(\d{1,3}-)?\d{1,3}:\d{2}:\d{2}$")
_ARRAY = re.compile(r"^\d{1,6}(-\d{1,6})?(,\d{1,6}(-\d{1,6})?){0,63}(%\d{1,5})?$")
_NATIVE = re.compile(r"^[0-9]{1,12}(\[\])?(\.[A-Za-z0-9][A-Za-z0-9.-]{0,63})?$")
# What the REAL sbatch / qsub prints for a job it took (read only from the submit's own output, never a command's):
# Slurm `Submitted batch job N` or `N[;cluster]` (--parsable); PBS `<seq>[[]].<server>` — a server name starts with a
# letter, so `3.14` or `10.0.0.1` is never a receipt (review A1).
_SLURM_RECEIPT = re.compile(r"Submitted batch job (\d+)|^(\d+)(?:;\S+)?\s*$", re.M)
_PBS_RECEIPT = re.compile(r"^\s*(\d+(?:\[\])?\.[A-Za-z][\w.-]*)\s*$", re.M)
DEADLINE_MARK = "@@HPCB_DEADLINE@@"   # job.sh's --deadline, said in the cluster's local time by the call that writes it
STALE_RESERVATION_S = 600.0           # a reservation no admission finished, that the scheduler never listed
HAND_MAX_OPEN_S = 24 * 3600.0         # a hand submit's reconciliation ends with its envelope, or after this long

BINDS = ("The envelope binds what hpc-bridge starts: submit_job's jobs, the compute blocks confirmed while it is open, "
         "and — counted after the fact, with NO envelope deadline applied to them — the jobs that appear while a hand "
         "sbatch/qsub it gated runs. It cannot bind a shell or code it cannot see — an sbatch inside a script, "
         "Python's subprocess — which the meter counts (and names) once its sweep of the scheduler finds them; nor a "
         "stolen token: on an SSH facility any task can read the Compute refresh token in "
         "~/.globus_compute/storage.db and submit to any endpoint the identity reaches. A hard cap needs the site: an "
         "sbank suballocation, a Slurm GrpTRESMins, or a small dedicated account.")


class JobRefusal(ValueError):
    """submit_job's arguments cannot make a job (the reason is the text)."""


def _now() -> float:
    return time.time()


def _iso(t: float | None) -> str | None:
    return None if t is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def _g(x: float) -> str:
    return f"{x:.2f}".rstrip("0").rstrip(".") if abs(x - round(x)) > 1e-9 else f"{round(x):d}"


def _up(x: float, nd: int = 4) -> float:
    """`x` rounded UP to `nd` decimals: what a job needs, a cap proposed for it — never shown below what it is."""
    q = 10 ** nd
    return math.ceil(float(x) * q - 1e-6) / q


def _down(x: float, nd: int = 4) -> float:
    """`x` rounded DOWN to `nd` decimals: what remains — never shown above what it is."""
    q = 10 ** nd
    return math.floor(float(x) * q + 1e-6) / q


def _gu(x: float) -> str:
    """A need, as said in words: rounded up to 2 decimals (asking for that figure admits the job: live 2026-11-01)."""
    return _g(_up(x, 2))


def _gd(x: float) -> str:
    return _g(_down(x, 2))


def _base(native: Any) -> str:
    return str(native or "").split(".")[0].split("[")[0].split("_")[0]


def ceiling() -> float | None:
    """HPC_BRIDGE_ENVELOPE_MAX: the operator's bound on the sum of open envelopes per facility and account."""
    raw = os.environ.get("HPC_BRIDGE_ENVELOPE_MAX", "").strip()
    try:
        v = float(raw)
    except ValueError:
        return None
    return v if v > 0 else None


def facility_key(app: AppCtx) -> str:
    return str(app.machine or getattr(app.facility, "name", None) or app.state.endpoint_id or "local")


def scheduler_of(app: AppCtx) -> str:
    return str(getattr(getattr(app.facility, "profile", None), "scheduler", "slurm") or "slurm")


def _ledger(app: AppCtx) -> Ledger:
    return batch.ledger_of(app)


# ---- the login channel -----------------------------------------------------------------------------------------------

async def _login(app: AppCtx, cmd: str) -> ShellOutcome:
    if batch.login_runner is None:
        raise RuntimeError("no login-shape channel")
    return await batch.login_runner(app)(cmd)


def _text(out: Any) -> str:
    return f"{getattr(out, 'stdout', '') or ''}\n{getattr(out, 'stderr_snippet', '') or ''}"


def _can_reach(app: AppCtx) -> bool:
    return _has_login_shape(app) and app.state.endpoint_id is not None and batch.login_runner is not None


# A scheduler command of hpc-bridge's own that outlived its sync-wait (a slow sacct under load) holds the internal login
# session until it ends, and the next one fails at once as busy (sweep 2026-11-02: the yes of a first job was refused
# for it). submit_job's admission reads wait for it, at most this long each — two such reads in one call, well within
# the ~300 s a tool call may run — and answer `busy` (retryable: nothing submitted, nothing refused) when it still runs.
LOGIN_BUSY_WAIT_S = 30.0
_LOGIN_BUSY_POLL_S = 0.25


async def _login_free(app: AppCtx, wait_s: float) -> str | None:
    """None once no scheduler command of hpc-bridge's own holds the login channel, waiting for one (off the lock) at
    most `wait_s`; else the task id of the one still running."""
    busy = batch.login_busy
    if busy is None:
        return None
    deadline = time.monotonic() + max(0.0, wait_s)
    while (tid := busy(app)) is not None:
        left = deadline - time.monotonic()
        if left <= 0:
            return tid
        await asyncio.sleep(min(_LOGIN_BUSY_POLL_S, left))
    return None


def _busy_words(tid: str, wait_s: float) -> str:
    return (f"hpc-bridge's login channel is still busy after {wait_s:.0f} s: an earlier scheduler command of its own "
            f"(task_id={tid!r}) is still running in its login session")


_USER = '"${USER:-$(id -un)}"'


# ---- the meter's tick ------------------------------------------------------------------------------------------------

@dataclass
class Tick:
    ok: bool
    readings: dict[str, meter.Reading] = field(default_factory=dict)   # job id -> reading
    error: str = ""
    busy: bool = False   # not read because a scheduler command of ours still holds the login channel: transient


def _state_of(r: meter.Reading, row: dict[str, Any]) -> str:
    if r.source in ("missing", "squeue"):   # not read: the ledger's state stands
        return str(row.get("state"))
    if r.state in ("RUNNING", "COMPLETING"):
        return "running"
    if r.state == "PENDING":
        return "held" if row.get("state") == "held" and "held" in (r.reason or "").lower() else "queued"
    if r.live:   # ended in the accounting, still listed by the scheduler (a cancelled array on its way out): it bills
        return "running"
    if r.state == "COMPLETED":
        return "done" if (r.exit_code or 0) == 0 else "failed"
    if r.state == "CANCELLED":
        return "cancelled"
    if r.state in meter._ENDED:
        return "failed"
    return str(row.get("state"))


def _link_block(app: AppCtx, led: Ledger, jobs: list[dict[str, Any]]) -> None:
    """A confirmed block's reservation stands for ONE block: the block's own scheduler job, once known, is linked to it
    (it is the reservation, not an unledgered job); every other parsl pilot still counts (review B1)."""
    rt = app.shapes.get("compute")
    bj = str(getattr(rt, "block_job", None) or "") if rt is not None else ""
    if not bj or not _NATIVE.match(bj) or any(_base(j.get("native_id")) == _base(bj) for j in jobs
                                               if j.get("native_id")):
        return
    pools = [j for j in jobs if j.get("source") == "pool_block" and j.get("state") in JOB_LIVE
             and not j.get("native_id") and j.get("approved_by") == app.session_token]
    if pools:
        led.update_job(pools[-1]["id"], native_id=bj)
        pools[-1]["native_id"] = bj


async def tick(app: AppCtx, *, envelope_ids: list[str] | None = None, also_known: set[str] | None = None,
               wait_s: float = 0.0) -> Tick:
    """One meter reading of this facility's envelopes (or `envelope_ids`): ONE login-shape call, then every ledger job
    and every envelope's totals updated. Without a login channel nothing is read (the ledger's last view stands); with
    an accounting read that failed, every job is held at its full walltime and the jobs hpc-bridge did not start keep
    their last count (review B3). `also_known`: jobs being adopted right now — counted by their reservation, not again
    as unledgered. `wait_s`: how long to wait for the login channel while an earlier scheduler command of ours still
    holds it (`_login_free`); a read the channel kept from running, or one still running past its own sync-wait, is
    `busy` — transient, never a verdict."""
    led = _ledger(app)
    fac = facility_key(app)
    all_envs = led.envelopes(facility=fac)
    envs = [e for e in all_envs if envelope_ids is None or e["id"] in envelope_ids]
    if not envs or not _can_reach(app):
        return Tick(False, error="no login channel")
    jobs = led.jobs(envelope_ids=[e["id"] for e in all_envs])
    env_ids = {e["id"] for e in envs}
    sched = scheduler_of(app)
    since = min([float(e["created_at"]) for e in envs] + [float(j["created_at"]) for j in jobs
                                                         if j["envelope_id"] in env_ids]) - 120
    cmd = meter.pbs_cmd() if sched == "pbs" else meter.slurm_cmd(time.strftime("%Y-%m-%dT%H:%M:%S",
                                                                                time.gmtime(since)))
    if wait_s > 0 and (held := await _login_free(app, wait_s)) is not None:
        return Tick(False, error=_busy_words(held, wait_s), busy=True)
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001 - a cold or failed channel reads nothing; the ledger's view stands
        return Tick(False, error=f"{type(exc).__name__}: {exc}")
    raw = _text(out)
    if meter.END_MARK not in raw:
        # its own read still running past the sync-wait, or the channel taken again meanwhile: the next call reads it
        busy = getattr(out, "phase", None) == "running" or (batch.login_busy is not None
                                                             and batch.login_busy(app) is not None)
        return Tick(False, error=(getattr(out, "notice", None) or raw.strip()[-200:]
                                  or "the login shape did not answer"), busy=busy)
    view = meter.parse_pbs(raw) if sched == "pbs" else meter.parse_slurm(raw)
    now = _now()
    t = Tick(True)
    _link_block(app, led, jobs)
    if view.ok:
        # a job whose submit's fate is unknown (or a reservation whose admission did not finish): the accounting finds
        # it by its name; one it never lists after a while was never submitted
        by_name = {rows[0].name: base for base, rows in view.rows.items() if rows}
        for j in jobs:
            if j.get("native_id") or j.get("state") not in ("submit_unknown", "reserved"):
                continue
            if j.get("source") == "pool_block":   # a block's reservation waits for its block, however long it queues
                continue
            base = by_name.get(f"hpcb-{j['id']}")
            if base is not None:
                led.update_job(j["id"], native_id=base, state="queued")
                j.update(native_id=base, state="queued")
            elif (j.get("state") == "reserved" and j["id"] not in app.jobs_admitting
                  and float(j["created_at"]) < now - STALE_RESERVATION_S):
                led.update_job(j["id"], state="refused", reason="never reached the scheduler (its admission did not "
                                                                "finish)")
                j["state"] = "refused"
    known = {_base(j["native_id"]) for j in jobs if j.get("native_id")} | {_base(x) for x in (also_known or ())}
    if view.ok and sched != "pbs":
        excluded_all = {x for e in envs for x in json.loads(e.get("excluded") or "[]")}
        await _fill_squeue_only(app, view, known | excluded_all)
    by_env: dict[str, list[dict[str, Any]]] = {}
    for j in jobs:
        by_env.setdefault(j["envelope_id"], []).append(j)
        if j["envelope_id"] not in env_ids or j.get("state") == "refused" or j.get("source") == "pool_block":
            continue   # a block's reservation holds its worst case; its own job is `known` once linked
        r = meter.job_reading(j, view)
        if r.source in ("missing", "squeue") and j.get("state") not in JOB_LIVE:
            continue   # an ended job the accounting does not list now keeps what it was last read at
        t.readings[j["id"]] = r
        state = _state_of(r, j)
        upd = {"charged": r.charged, "committed": r.committed, "meter_source": r.source, "sched_state": r.state,
               "metered_at": now, "array_counts": json.dumps(r.counts), "state": state, "reason": r.reason}
        if r.source not in ("missing", "squeue"):   # read: its exit code only once it ended with one (None until)
            upd["exit_code"] = r.exit_code
        if state not in JOB_LIVE and not j.get("ended_at"):
            upd["ended_at"] = now
        led.update_job(j["id"], **upd)
        j.update(upd)
    for e in envs:
        excluded = set(json.loads(e.get("excluded") or "[]"))
        if view.ok:
            accts = {a for a in envelope_accounts(e) if a}
            un = _mark_session_blocks(app, meter.unledgered(view, known, excluded, accounts=accts or None,
                                                            horizon_s=max(0.0, float(e["deadline"]) - now)))
        else:   # not read: the jobs it did not start keep their last count, never nothing (review B3)
            un = [u for u in json.loads(e.get("unledgered") or "[]")
                  if _base(u.get("native_id")) not in known and u.get("native_id") not in excluded]
        hold_un = sum(float(u["charged"]) + float(u["committed"]) for u in un)
        mine = [j for j in by_env.get(e["id"], []) if j.get("state") != "refused"]
        spent = sum(float(j.get("charged") or 0) for j in mine) + sum(float(u["charged"]) for u in un)
        committed = sum(job_hold(j) - float(j.get("charged") or 0) for j in mine) + sum(float(u["committed"])
                                                                                       for u in un)
        upd2: dict[str, Any] = {"spent": spent, "committed": committed, "unledgered": json.dumps(un),
                                "unledgered_hold": hold_un}
        if view.ok:
            upd2["metered_at"] = now
        live = [j for j in mine if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
        if float(e["deadline"]) <= now and e["state"] in ENVELOPE_ADMITTING:   # its deadline: admit no more
            upd2["state"] = "draining" if live else "closed"
            upd2["close_reason"] = "its deadline passed"
            if e.get("at_deadline") == "abort" and live:
                await _scancel(app, [str(j["native_id"]) for j in live if j.get("native_id") and not j.get("unheld")])
        elif e["state"] == "draining" and not live:
            upd2["state"] = "closed"
            upd2["closed_at"] = now
        led.update_envelope(e["id"], **upd2)
        led.meter_reading(e["id"], source=view.source, spent=spent, committed=committed, unledgered=un,
                          detail={"ok": view.ok, "error": view.error, "jobs": len(mine)})
        meter.journal_tick(envelope_id=e["id"], jobs=len(mine), sacct_calls=view.calls, source=view.source)
    t.ok = view.ok
    t.error = view.error
    return t


# ---- views -----------------------------------------------------------------------------------------------------------

def _hms(secs: float | None) -> str | None:
    if not secs:
        return None
    s = int(secs)
    d, rest = divmod(s, 86400)
    out = f"{rest // 3600:02d}:{rest % 3600 // 60:02d}:{rest % 60:02d}"
    return f"{d}-{out}" if d else out


def job_info(j: dict[str, Any]) -> JobInfo:
    held = job_hold(j)
    charged = float(j.get("charged") or 0.0)
    state = str(j.get("sched_state") or "") or {"reserved": "RESERVED", "submit_unknown": "SUBMIT_UNKNOWN",
                                                 "held": "HELD", "queued": "PENDING"}.get(str(j["state"]),
                                                                                         str(j["state"]).upper())
    if j.get("state") == "held" and state == "PENDING":
        state = "HELD"
    # an exit code only for a job that ended with one (a ledger an earlier build wrote may hold sacct's 0:0 for a live
    # or cancelled job: the demo of 2026-11-03)
    code = j.get("exit_code") if (state.split() or [""])[0] in meter._EXIT_CODED else None
    return JobInfo(job_id=j["id"], native_id=j.get("native_id"), source=j.get("source") or "submit_job",
                   state=state, exit_code=code, reason=j.get("reason"), charged=round(charged, 4),
                   committed=round(max(0.0, held - charged), 4), worst_case=_up(float(j.get("worst_case") or 0)),
                   unit=UNIT, meter_source=j.get("meter_source") or "none", tasks=int(j.get("tasks") or 1),
                   throttle=j.get("throttle"), array_counts=json.loads(j.get("array_counts") or "{}"),
                   partition=j.get("partition"), account=j.get("account"), walltime=_hms(j.get("walltime_s")),
                   deadline=_iso(j.get("deadline")), script_path=j.get("script_path"),
                   script_hash=j.get("script_hash"), session_id=j.get("session_id"), unheld=bool(j.get("unheld")))


def envelope_info(app: AppCtx, e: dict[str, Any], *, state: str | None = None) -> EnvelopeInfo:
    led = _ledger(app)
    jobs = [j for j in led.jobs(envelope_id=e["id"]) if j.get("state") != "refused"]
    held = sum(job_hold(j) for j in jobs) + float(e.get("unledgered_hold") or 0.0)
    spent = sum(float(j.get("charged") or 0) for j in jobs) + sum(
        float(u.get("charged") or 0) for u in json.loads(e.get("unledgered") or "[]"))
    cap = float(e["cap"])
    return EnvelopeInfo(
        envelope_id=e["id"], state=state or e["state"], facility=e.get("facility"), account=e.get("account"),  # type: ignore[arg-type]
        unit=e.get("unit") or UNIT, cap=round(cap, 4), spent=round(spent, 4), committed=round(held - spent, 4),
        remaining=_down(cap - held), max_jobs=e.get("max_jobs"),
        jobs=[job_info(j) for j in jobs if j.get("source") != "pool_block"],
        tasks=sum(int(j.get("tasks") or 1) for j in jobs),
        max_nodes_per_block=e.get("max_nodes_per_block"), accounts=json.loads(e.get("accounts") or "[]"),
        partitions=json.loads(e.get("partitions") or "[]"), deadline=_iso(float(e["deadline"])) or "",
        at_deadline=e.get("at_deadline") or "drain", site_backstop=e.get("site_backstop") or "none",
        owned=e.get("approved_by") == app.session_token, binds=BINDS, job_ids=[j["id"] for j in jobs],
        unledgered=[UnledgeredJob(**u) for u in json.loads(e.get("unledgered") or "[]")],
        excluded=json.loads(e.get("excluded") or "[]"))


# Why a pending job waits, when it is a SITE LIMIT (an association's or QOS's Grp*/Max* limit, a partition's limit, a
# PBS server or queue limit) — it may never run (live 2026-11-01: AssocGrpCPUMinutesLimit under GrpTRESMins + `safe`).
# Priority, Resources, Dependency, BeginTime, JobHeldUser are not.
_SITE_LIMIT = re.compile(r"^(?:Assoc|QOS)|MaxJobs|MaxSubmit|Limit|would exceed|AccountNotAllowed|InvalidAccount"
                         r"|InvalidQOS", re.I)


def site_limit(reason: str | None) -> bool:
    return bool(reason) and _SITE_LIMIT.search(str(reason)) is not None


def _limit_words(reason: str, ident: str) -> str:
    return (f" A site limit holds it ({reason}): it will not start until that limit changes (an administrator lifts "
            "or raises it, or the usage it counts ages out), and it bills nothing while it waits — tell the user; "
            f"cancel('{ident}', 'abort') ends it.")


SESSION_BLOCK = "the session's compute block, confirmed before this envelope opened"


def _ended(state: str | None) -> bool:
    return (str(state or "").split() or [""])[0] in meter._ENDED


def _bills_note(info: EnvelopeInfo) -> str:
    """What still bills: every job that can — an ended one with nothing committed is counted in `spent`, not listed
    here (the demo of 2026-11-03 listed a CANCELLED job "+ 0")."""
    live = [j for j in info.jobs if j.committed > 0 or j.state in ("RUNNING", "PENDING", "HELD", "RESERVED")]
    parts = [f"{j.job_id} ({j.native_id or 'not submitted'}, {j.state}"
             + (", appeared while your approved command ran, no envelope deadline" if j.unheld else "")
             + (f", held by a site limit: {j.reason}" if j.state == "PENDING" and site_limit(j.reason) else "")
             + f", {_g(j.charged)} charged, up to {_gu(j.committed)} more)" for j in live]
    parts += [f"job {u.native_id} ({u.name or '?'}, {u.state}: "
              + (SESSION_BLOCK if u.session_block else "not started by hpc-bridge")
              + f", counted {_g(u.charged)} + {_gu(u.committed)})"
              for u in info.unledgered if u.committed > 0 or not _ended(u.state)]
    return "; ".join(parts) or "nothing"


def _unledgered_note(info: EnvelopeInfo) -> str:
    """The jobs hpc-bridge did not start, which the user may say are not theirs to count (never the session's own
    compute block: it bills the same allocation, and is counted as such)."""
    theirs = [u for u in info.unledgered if not u.session_block]
    if not theirs:
        return ""
    ids = ", ".join(f"{u.native_id} ({u.name or '?'})" for u in theirs)
    return (f" It counts job(s) hpc-bridge did not start: {ids} — if the user says one is not theirs to count here, "
            "pass exclude_jobs=[…] with their yes (confirm_spend=True) and it is never counted again.")


def session_blocks(app: AppCtx) -> set[str]:
    """The scheduler jobs that are this session's own compute blocks on the bound endpoint (their base ids): every job
    a check of ours proved up there (`job_seen_since`), each shape's confirmed or presumed block, and the blocks a stop
    or switch left (`left_blocks`). A block confirmed while an envelope is open is that envelope's reservation
    (`_link_block`); one confirmed before is counted as a job hpc-bridge did not start, and named for what it is."""
    eid = app.state.endpoint_id
    jobs = {j for (e, j) in app.job_seen_since if e == eid}
    for rt in app.shapes.values():
        jobs |= {j for j in (rt.block_job, rt.presumed_block_job) if j}
    jobs |= {lb.job for lb in app.left_blocks if lb.job and lb.endpoint_id == eid}
    return {_base(j) for j in jobs if j}


def _mark_session_blocks(app: AppCtx, un: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mine = session_blocks(app)
    for u in un:
        if _base(u.get("native_id")) in mine:
            u["session_block"] = True
    return un


# ---- the job's spec and terms ----------------------------------------------------------------------------------------

@dataclass
class Spec:
    command: str
    session_id: str
    account: str | None
    partition: str | None
    nodes: int
    whole_node: bool | None
    cores: int | None
    memory_gb: float | None
    gpus: int | None
    walltime_s: float
    tasks: int
    throttle: int | None
    array: str | None
    depends: list[str]
    rate: float
    per_task: float
    worst_case: float
    basis: str
    min_charge_h: float | None
    shape: PartitionShape | None


def _array_count(spec: str) -> tuple[int, int | None]:
    body, _, thr = spec.partition("%")
    n = 0
    for part in body.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            if int(b) < int(a):
                raise JobRefusal(f"array {spec!r}: {part} runs backwards")
            n += int(b) - int(a) + 1
        else:
            n += 1
    return n, (int(thr) if thr else None)


def _default_uec(app: AppCtx) -> dict[str, Any]:
    from . import warmth

    rt = app.shapes.get("compute")
    return dict(rt.user_endpoint_config) if rt is not None else warmth._seed_uec(app, "compute")


def spec_of(app: AppCtx, *, command: Any, session_id: str, account: Any, partition: Any, nodes: Any, cores: Any,
            memory_gb: Any, gpus: Any, whole_node: Any, walltime: Any, array: Any, depends_on: Any) -> Spec:
    """submit_job's arguments, checked (rule 7: nothing here may become shell code), and the job's size and rate."""
    if not isinstance(command, str) or not command.strip():
        raise JobRefusal("command is required: the shell command the job runs")
    if len(command) > batch.MAX_COMMAND:
        raise JobRefusal(f"command is {len(command):,} characters; at most {batch.MAX_COMMAND:,}")
    try:
        session_shell.Session(session_id, app.scratch_root)
    except ValueError as exc:
        raise JobRefusal(str(exc)) from None
    uec = _default_uec(app)
    for name, v in (("partition", partition), ("account", account)):
        if v is not None and (not isinstance(v, str) or not _VALID_NAME.match(v)):
            raise JobRefusal(f"invalid {name} {v!r}: must match [A-Za-z0-9_.:-]{{1,64}}")
    part = partition if partition is not None else (uec.get("partition") or None)
    acct = account if account is not None else (uec.get("account") or None)
    if part is not None and not _VALID_NAME.match(str(part)):
        part = None
    if acct is not None and not _VALID_NAME.match(str(acct)):
        acct = None
    wt = walltime if walltime is not None else (uec.get("walltime") or "00:30:00")
    if not isinstance(wt, str) or not _WALLTIME.match(wt.strip()):
        raise JobRefusal(f"invalid walltime {walltime!r}: HH:MM:SS (or D-HH:MM:SS)")
    wall = float(_parse_hhmmss(wt.strip()))
    if wall <= 0:
        raise JobRefusal("walltime must be more than 0")
    tasks, throttle = 1, None
    if array is not None:
        if not isinstance(array, str) or not _ARRAY.match(array):
            raise JobRefusal(f"invalid array {array!r}: index ranges such as 0-19, 1,3,5 or 0-19%4 (at most 4 at once)")
        tasks, throttle = _array_count(array)
        if tasks > MAX_TASKS:
            raise JobRefusal(f"{tasks:,} array tasks; at most {MAX_TASKS:,}")
    _sizing, bad = resources.requested(cores, memory_gb, gpus, nodes, whole_node)
    if bad:
        raise JobRefusal(bad)
    depends: list[str] = []
    if depends_on is not None:
        if not isinstance(depends_on, list) or not all(isinstance(d, str) and JOB_ID.match(d) for d in depends_on):
            raise JobRefusal("depends_on is a list of job- ids (job-<12 hex>) from submit_job")
        led = _ledger(app)
        for d in depends_on:
            row = led.job(d)
            if row is None or not row.get("native_id"):
                raise JobRefusal(f"depends_on: {d} is not a submitted job in the ledger")
            depends.append(str(row["native_id"]).split(".")[0])
    shape = resources.shape_of(getattr(app.facility, "partitions", None), part)
    n = int(nodes or 1)
    slice_ask = whole_node is False or (whole_node is None and (cores is not None or memory_gb is not None))
    if slice_ask and shape is not None and shape.kind == "whole_node":
        raise JobRefusal(f"partition {part!r} hands out whole nodes only: leave cores/memory_gb unset, or pick a "
                         "per-core partition")
    if slice_ask or (whole_node is None and shape is not None and shape.kind == "per_core"):
        c = int(cores) if cores is not None else resources.default_slice_cores(shape, n)
        size = resources.Size(nodes=n, whole_node=False, cores=c,
                              memory_gb=float(memory_gb) if memory_gb is not None else None,
                              gpus=int(gpus) if gpus else None)
    else:
        size = resources._whole(n, shape, int(gpus) if gpus else None)
    got = resources.charge(size, shape, 3600.0)
    if got is not None:
        rate, per_hour = got
    else:   # no weights known: Slurm bills a job's CPUs by default — never fewer than the job holds
        billed, _why = resources.billed_cores(size, shape)
        if not billed:
            raise JobRefusal("the job's cost cannot be bounded (the partition's cores per node are not known): name "
                             "cores= (a slice) or pick a partition whose node size is known")
        rate = float(billed * size.nodes)
        per_hour = f"{size.nodes} node{'s' if size.nodes > 1 else ''} × {billed} cores (its CPUs, the default billing)"
    min_h = shape.min_charge_h if shape is not None else None
    per_task = rate * max(wall / 3600, min_h or 0.0)
    hours = f"{_g(wall / 3600)} h" + (f" (at least the queue's minimum charge of {_g(min_h)} h)"
                                     if min_h and min_h * 3600 > wall else "")
    basis = (f"{tasks} task{'s' if tasks > 1 else ''} × " if tasks > 1 else "") + f"{rate:g}/h ({per_hour}) × {hours}"
    cmd = command
    if array is not None:
        try:
            cmd = batch.substitute(command, {"index": 0}, 0, {"index"})
        except batch.BatchRefusal as exc:
            raise JobRefusal(str(exc)) from None
    return Spec(command=cmd, session_id=session_id, account=acct, partition=part, nodes=n,
                whole_node=size.whole_node, cores=size.cores, memory_gb=size.memory_gb, gpus=size.gpus,
                walltime_s=wall, tasks=tasks, throttle=throttle, array=array, depends=depends, rate=rate,
                per_task=per_task, worst_case=round(tasks * per_task, 9), basis=basis, min_charge_h=min_h,
                shape=shape)


def terms_of(app: AppCtx, spec: Spec, deadline: float, backstop: str | None,
             script_path: str | None = None) -> JobTerms:
    return JobTerms(facility=facility_key(app), scheduler=scheduler_of(app), account=spec.account,
                    partition=spec.partition, nodes=spec.nodes, whole_node=spec.whole_node, cores_per_node=spec.cores,
                    memory_gb=spec.memory_gb, gpus_per_node=spec.gpus, walltime=_hms(spec.walltime_s) or "",
                    tasks=spec.tasks, throttle=spec.throttle, rate=spec.rate, unit=UNIT,
                    worst_case=_up(spec.worst_case), basis=spec.basis, min_charge_h=spec.min_charge_h,
                    deadline=_iso(deadline) or "", script_path=script_path, binds=BINDS,
                    site_backstop=backstop or "none")


# ---- the frozen job.sh -----------------------------------------------------------------------------------------------

def job_dir(app: AppCtx, job_id: str) -> str:
    return f"{app.scratch_root.rstrip('/')}/jobs/{job_id}"


def job_script(app: AppCtx, *, job_id: str, envelope_id: str, spec: Spec, deadline: float) -> str:
    """The job's frozen script: the header from the terms (every value checked: none can become code), the session's
    cwd and env as they were at submit (copied beside the script by the call that writes it), the command (base64),
    its exit code written beside the script. A Slurm job's `--deadline` is DEADLINE_MARK here: the writing call says
    the deadline in the cluster's own local time (sbatch reads it so, and no TZ is forced on sbatch)."""
    sched = scheduler_of(app)
    lines = ["#!/bin/bash"]
    if sched == "pbs":
        sel = f"{spec.nodes}:ncpus={spec.cores or 1}"
        if spec.memory_gb and spec.whole_node is False:
            sel += f":mem={math.ceil(spec.memory_gb)}gb"
        if spec.gpus:
            sel += f":ngpus={spec.gpus}"
        lines += [f"#PBS -N hpcb-{job_id}", f"#PBS -l select={sel}", f"#PBS -l walltime={_hms(spec.walltime_s)}"]
        if spec.whole_node:
            lines.append("#PBS -l place=excl")
        if spec.partition:
            lines.append(f"#PBS -q {spec.partition}")
        if spec.account:
            lines.append(f"#PBS -A {spec.account}")
        if spec.array:
            lines.append(f"#PBS -J {spec.array}")
        if spec.depends:
            lines.append(f"#PBS -W depend=afterok:{':'.join(spec.depends)}")
    else:
        lines += [f"#SBATCH --job-name=hpcb-{job_id}", f"#SBATCH --nodes={spec.nodes}",
                  f"#SBATCH --time={_hms(spec.walltime_s)}", f"#SBATCH --deadline={DEADLINE_MARK}"]
        if spec.partition:
            lines.append(f"#SBATCH --partition={spec.partition}")
        if spec.account:
            lines.append(f"#SBATCH --account={spec.account}")
        if spec.whole_node:
            lines.append("#SBATCH --exclusive")
        else:
            lines += ["#SBATCH --ntasks-per-node=1", f"#SBATCH --cpus-per-task={spec.cores or 1}"]
            if spec.memory_gb:
                lines.append(f"#SBATCH --mem={math.ceil(spec.memory_gb)}G")
        if spec.gpus:
            lines.append(f"#SBATCH --gres=gpu:{spec.gpus}")
        if spec.array:
            lines.append(f"#SBATCH --array={spec.array}")
        if spec.depends:
            lines.append(f"#SBATCH --dependency=afterok:{':'.join(spec.depends)}")
    b64 = shlex.quote(base64.b64encode(spec.command.encode()).decode())
    lines += [
        f"# hpc-bridge {job_id} (envelope {envelope_id}): frozen when it was written; its sha256 is on the ledger",
        f"__hpcb_dir={batch.quoted_dir(job_dir(app, job_id))}",
        '__hpcb_task="${SLURM_ARRAY_TASK_ID:-${PBS_ARRAY_INDEX:-}}"',
    ]
    if spec.array:
        lines.append(f'{batch.FIELD_VAR}index="$__hpcb_task"; export {batch.FIELD_VAR}index')
    lines += [
        "# the session's cwd and env as they were when the job was submitted (copied beside it then, read-only)",
        'if [ -f "$__hpcb_dir/session.cwd" ]; then cd "$(cat "$__hpcb_dir/session.cwd")" 2>/dev/null '
        '|| cd "$__hpcb_dir"; else cd "$__hpcb_dir"; fi',
        '[ -f "$__hpcb_dir/session.env" ] && . "$__hpcb_dir/session.env"',
        f'( eval "$(printf %s {b64} | base64 --decode)" )',
        "__hpcb_rc=$?",
        'echo "$__hpcb_rc" > "$__hpcb_dir/exit_code${__hpcb_task:+.$__hpcb_task}"',
        'exit "$__hpcb_rc"',
    ]
    return "\n".join(lines) + "\n"


_LOCAL_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")


async def write_job_script(app: AppCtx, job_id: str, text: str, *, session_id: str | None = None,
                           deadline: float | None = None) -> tuple[str, str]:
    """Write `text` as the job's job.sh ONCE (noclobber: an existing one is never replaced), with the session's cwd and
    env copied beside it as they are now (review A7: a queued job runs with what it was submitted with), and its
    DEADLINE_MARK said in the cluster's local time. Returns (its path on the facility, its sha256 as the facility read
    it — checked against what was meant)."""
    if not JOB_ID.match(job_id):
        raise JobRefusal(f"invalid job id {job_id!r}")
    d = batch.quoted_dir(job_dir(app, job_id))
    b64 = shlex.quote(base64.b64encode(text.encode()).decode())
    marked = DEADLINE_MARK in text
    local = ""
    put = f"printf %s {b64} | base64 --decode > job.sh"
    if marked:
        if deadline is None:
            raise JobRefusal("job.sh carries a deadline and none was given")
        ep = math.floor(deadline)
        local = (f"__dl=$(date -d @{ep} +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -r {ep} +%Y-%m-%dT%H:%M:%S "
                 "2>/dev/null); case \"$__dl\" in "
                 "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]) ;; "
                 '*) echo "HPCB_WRITE_FAILED the facility could not say the deadline in its local time"; exit 0;; '
                 "esac; ")
        put = f'printf %s {b64} | base64 --decode | sed "s/{DEADLINE_MARK}/$__dl/" > job.sh'
    copy = ""
    if session_id is not None:
        sd = session_shell.Session(session_id, app.scratch_root).quoted_state_dir()
        copy = (f"__sd={sd}; for __f in cwd env; do if [ -f \"$__sd/.$__f\" ]; then "
                'cp "$__sd/.$__f" "session.$__f" && chmod 400 "session.$__f"; fi; done; ')
    cmd = (f"umask 077; d={d}; mkdir -p \"$d\" && cd \"$d\" "
           '|| { echo "HPCB_WRITE_FAILED cannot create $d"; exit 0; }; '
           'if [ -e job.sh ]; then echo "HPCB_JOB_EXISTS $(pwd)/job.sh"; exit 0; fi; '
           + local + copy +
           f"( set -C; {put} ) 2>/dev/null "
           '|| { echo "HPCB_JOB_EXISTS $(pwd)/job.sh"; exit 0; }; '
           'h=$( (sha256sum job.sh 2>/dev/null || shasum -a 256 job.sh) | cut -c1-64); '
           'echo "HPCB_JOB_SH $(pwd)/job.sh $h ${__dl:-}"')
    out = await _login(app, cmd)
    raw = _text(out)
    if (m := re.search(r"HPCB_JOB_EXISTS (\S+)", raw)) is not None:
        raise JobRefusal(f"{m.group(1)} exists: a job's script is written once and never replaced")
    m = re.search(r"HPCB_JOB_SH (\S+) ([0-9a-f]{64})(?: (\S+))?", raw)
    if m is None:
        why = re.search(r"HPCB_WRITE_FAILED (.*)", raw)
        raise JobRefusal(f"job.sh could not be written: {why.group(1) if why else raw.strip()[-200:] or 'no answer'}")
    meant = text
    if marked:
        if not m.group(3) or not _LOCAL_TIME.match(m.group(3)):
            raise JobRefusal("job.sh's deadline could not be said in the cluster's local time")
        meant = text.replace(DEADLINE_MARK, m.group(3))
    want = hashlib.sha256(meant.encode()).hexdigest()
    if m.group(2) != want:
        raise JobRefusal(f"job.sh on the facility does not hash as written ({m.group(2)[:12]} ≠ {want[:12]})")
    return m.group(1), want


def _receipts_in(sched: str, text: str) -> list[str]:
    """The job ids a REAL sbatch / qsub printed for the jobs it took, in order (`text`: that submit's own output — the
    hand submit's mark file, or the frozen job's sbatch — never a command's stdout)."""
    pat = _PBS_RECEIPT if sched == "pbs" else _SLURM_RECEIPT
    out: list[str] = []
    for m in pat.finditer(text or ""):
        nid = next((g for g in m.groups() if g), None)
        if nid and _NATIVE.match(nid) and nid not in out:
            out.append(nid)
    return out


async def _submit_script(app: AppCtx, job_id: str, path: str, spec: Spec) -> tuple[str | None, str]:
    """sbatch / qsub the frozen script. (native id; "" — the scheduler refused it; None — its fate is unknown: no
    answer, or a 0 exit with no receipt (review A8): the meter finds it by its name)."""
    d = batch.quoted_dir(job_dir(app, job_id))
    q = shlex.quote(path)
    if scheduler_of(app) == "pbs":
        cmd = f"cd {d} && qsub -o {d}/pbs.out -j oe {q} 2>&1; echo \"HPCB_SUBMIT_RC=$?\""
    else:
        pattern = "slurm-%A_%a.out" if spec.array else "slurm-%j.out"
        cmd = f"cd {d} && sbatch --parsable --output={d}/{pattern} {q} 2>&1; echo \"HPCB_SUBMIT_RC=$?\""
    out = await _login(app, cmd)
    raw = _text(out)
    rc = re.search(r"HPCB_SUBMIT_RC=(\d+)", raw)
    if rc is None:
        return None, raw
    if rc.group(1) != "0":
        return "", raw
    got = _receipts_in(scheduler_of(app), raw.split("HPCB_SUBMIT_RC=")[0])
    return (got[0] if got else None), raw


# ---- finding and opening the envelope --------------------------------------------------------------------------------

def _account_ok(e: dict[str, Any], account: str | None) -> bool:
    allowed = json.loads(e.get("accounts") or "[]")
    return (account or "") in allowed if allowed else (e.get("account") or "") == (account or "")


def _owner_alive(app: AppCtx, e: dict[str, Any]) -> bool:
    led = _ledger(app)
    owner = led.envelope_owner(e)
    me = batch.owner_of(app)
    if (owner.host, owner.pid) == (me.host, me.pid) and e.get("approved_by") != app.session_token:
        return True   # another session in this very process (tests; a second AppCtx)
    return led.holder_alive(owner)


def find_envelope(app: AppCtx, account: str | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """(this session's admitting envelope for the account, another session's that is ownerless — offered again)."""
    envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
            if _account_ok(e, account) and float(e["deadline"]) > _now()]
    mine = next((e for e in envs if e.get("approved_by") == app.session_token), None)
    orphan = next((e for e in envs if e.get("approved_by") != app.session_token and not _owner_alive(app, e)), None)
    return mine, orphan


def _ceiling_room(app: AppCtx, accounts: list[str], *, except_id: str | None = None) -> tuple[float | None, float]:
    """(the ceiling, the most that is open on this facility on any of `accounts` — every account an envelope admits
    counts (review B7) —, without `except_id`). The transaction that opens or widens checks it again (the race)."""
    top = ceiling()
    if top is None:
        return None, 0.0
    envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=("open", "exhausted", "draining"))
            if e["id"] != except_id]
    return top, max((sum(float(e["cap"]) for e in envs if a in envelope_accounts(e)) for a in accounts or [""]),
                    default=0.0)


def _ceiling_refusal(top: float, open_caps: float, cap: float) -> JobStatus:
    left = max(0.0, top - open_caps)
    return JobStatus(phase="refused", reason="envelope_ceiling",
                     notice=(f"Refused: the operator's ceiling HPC_BRIDGE_ENVELOPE_MAX is {_g(top)} {UNIT} for the SUM "
                             f"of open envelopes on this facility and account; {_g(open_caps)} is open already, so "
                             f"{_gd(left)} is left under it and an envelope of {_g(cap)} does not fit. Ask the user "
                             f"for a cap of at most {_gd(left)}, or close another envelope first. Nothing was "
                             "submitted."))


def _deadline_hours(req: EnvelopeRequest) -> float:
    h = DEFAULT_DEADLINE_H if req.deadline_hours is None else float(req.deadline_hours)
    if not 0 < h <= MAX_DEADLINE_H:
        raise JobRefusal(f"the envelope's deadline must be within {int(MAX_DEADLINE_H)} hours and in the future "
                         f"(deadline_hours={req.deadline_hours!r}); the default is {int(DEFAULT_DEADLINE_H)}")
    return h


def _proposed(app: AppCtx, req: EnvelopeRequest, spec: Spec, deadline: float, *, cap: float,
               held: list[dict[str, Any]]) -> EnvelopeInfo:
    """The envelope a proposal shows: its cap, and what the jobs hpc-bridge did not start already hold in it."""
    spent = sum(float(u.get("charged") or 0) for u in held)
    committed = sum(float(u.get("committed") or 0) for u in held)
    return EnvelopeInfo(envelope_id="env-" + "0" * 12, state="proposed", facility=facility_key(app),
                        account=spec.account, unit=UNIT, cap=cap, spent=round(spent, 4), committed=round(committed, 4),
                        remaining=_down(cap - spent - committed),
                        max_jobs=req.max_jobs, max_nodes_per_block=req.max_nodes_per_block,
                        accounts=req.accounts or ([spec.account] if spec.account else []),
                        partitions=req.partitions or [], deadline=_iso(deadline) or "", at_deadline=req.at_deadline,
                        site_backstop=req.site_backstop or "none", owned=True, binds=BINDS,
                        unledgered=[UnledgeredJob(**u) for u in held])


def _proposal_key(app: AppCtx, spec: Spec) -> tuple:
    return (facility_key(app), spec.account or "", spec.partition or "", spec.tasks, round(spec.worst_case, 6))


async def _fill_squeue_only(app: AppCtx, view: meter.SchedView, skip: set[str]) -> None:
    """The jobs squeue lists and sacct does not — an array none of whose tasks has started, a job slurmdbd has not
    recorded yet (live r2: envelope_bypass_counted's job was missing) — read for their billing in ONE login call
    (`scontrol show job` ReqTRES; review r3: an unstarted GPU array was counted at its CPUs). Unread: the partition's
    weights, a whole node per task where they bill more than CPUs; bare CPUs only where nothing else is known."""
    bases = [b for b in view.squeue_only() if b not in skip][:READ_BATCH]
    if not bases:
        return
    try:
        reqs = await _read_requests(app, bases)
    except Exception:  # noqa: BLE001 - the partition's weights stand in
        reqs = {}
    parts = getattr(app.facility, "partitions", None)
    for b in bases:
        r = reqs.get(b)
        if r is not None:
            view.extra[b] = {"rate": r.rate, "limit_s": r.walltime_s or None, "name": r.name}
            continue
        shape = resources.shape_of(parts, view.partitions.get(b))
        if shape is not None and shape.billing and any(k != "cpu" for k in shape.billing):
            whole = resources.charge(resources._whole(1, shape, None), shape, 3600.0)
            if whole is not None:
                view.extra[b] = {"rate": whole[0]}


async def _held_now(app: AppCtx, accounts: list[str], exclude: list[str], horizon_s: float, *,
                    wait_s: float = 0.0) -> tuple[bool, list[dict[str, Any]], str]:
    """What the jobs hpc-bridge did not start hold on this facility right now — the reading a NEW envelope's proposal
    stands on (live 2026-11-01: a proposal at exactly the job's worst case, a leftover job of the user's counted by the
    yes, and the yes refused itself). (read, those jobs as the meter counts them, why not read). `wait_s`: as tick's."""
    if not _can_reach(app):
        return False, [], "no login channel"
    if wait_s > 0 and (held := await _login_free(app, wait_s)) is not None:
        return False, [], _busy_words(held, wait_s)
    led = _ledger(app)
    sched = scheduler_of(app)
    known = {_base(j["native_id"]) for j in led.jobs(envelope_ids=[e["id"] for e in
                                                                   led.envelopes(facility=facility_key(app))])
             if j.get("native_id")}
    cmd = meter.pbs_cmd() if sched == "pbs" else meter.slurm_cmd(time.strftime("%Y-%m-%dT%H:%M:%S",
                                                                                time.gmtime(_now() - 120)))
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001 - not read: the proposal says so
        return False, [], f"{type(exc).__name__}: {exc}"
    raw = _text(out)
    if meter.END_MARK not in raw:
        return False, [], raw.strip()[-200:] or "the login shape did not answer"
    view = meter.parse_pbs(raw) if sched == "pbs" else meter.parse_slurm(raw)
    meter.journal_tick(envelope_id=None, jobs=0, sacct_calls=view.calls, source=view.source)
    if not view.ok:
        return False, [], view.error or "the accounting did not answer"
    await _fill_squeue_only(app, view, known | set(exclude))
    accts = {a for a in accounts if a}
    return True, _mark_session_blocks(app, meter.unledgered(view, known, set(exclude), accounts=accts or None,
                                                            horizon_s=horizon_s)), ""


def _held_words(held: list[dict[str, Any]]) -> str:
    return ", ".join(f"job {u['native_id']} ({u.get('name') or '?'}, {u.get('state')}"
                     + (", the session's compute block, confirmed before this envelope" if u.get("session_block")
                        else ", not started by hpc-bridge")
                     + f": {_gu(float(u.get('charged') or 0) + float(u.get('committed') or 0))})" for u in held)


def _limits_refusal(req: EnvelopeRequest | None, e: dict[str, Any] | None, spec: Spec) -> str | None:
    """The job against the envelope's other dimensions (before anything is opened or reserved)."""
    max_jobs = req.max_jobs if req is not None and req.max_jobs is not None else (e or {}).get("max_jobs")
    max_nodes = (req.max_nodes_per_block if req is not None and req.max_nodes_per_block is not None
                 else (e or {}).get("max_nodes_per_block"))
    parts = (req.partitions if req is not None and req.partitions else json.loads((e or {}).get("partitions") or "[]"))
    if max_jobs and spec.tasks > int(max_jobs):
        return (f"the job has {spec.tasks} task(s), over the envelope's max_jobs {int(max_jobs)} (array tasks count): "
                "ask the user for a larger max_jobs, or submit fewer tasks")
    if max_nodes and spec.nodes > int(max_nodes):
        return f"{spec.nodes} nodes is over the envelope's max_nodes_per_block {int(max_nodes)}"
    if parts and spec.partition and spec.partition not in parts:
        return f"partition {spec.partition!r} is not one the envelope allows ({', '.join(parts)})"
    return None


# ---- submit_job ------------------------------------------------------------------------------------------------------

def _refused(why: str, reason: str = "invalid") -> JobStatus:
    return JobStatus(phase="refused", reason=reason, notice=f"Nothing was submitted: {why}")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


async def submit_job(app: AppCtx, *, command: Any = None, session_id: str = "default", account: Any = None,
                     partition: Any = None, nodes: Any = None, cores: Any = None, memory_gb: Any = None,
                     gpus: Any = None, whole_node: Any = None, walltime: Any = None, array: Any = None,
                     depends_on: Any = None, adopt: Any = None, envelope: Any = None, confirm_spend: bool = False,
                     exclude_jobs: Any = None) -> JobStatus:
    if not _has_login_shape(app):
        return _refused("Jobs mode is SSH-only: a facility multi-user endpoint has no free status or cancel channel "
                        "for a scheduler job, so hpc-bridge could neither meter nor cancel it. On ALCF, Jobs mode "
                        "collapses into Campaign (a pilot block with fan-out inside it): use run_batch on the "
                        "confirmed block.", "mep")
    if exclude_jobs is not None and (not isinstance(exclude_jobs, list)
                                     or not all(isinstance(x, str) and _NATIVE.match(x) for x in exclude_jobs)):
        return _refused("exclude_jobs is a list of scheduler job ids (the ones the notice names)")
    try:
        req = EnvelopeRequest.model_validate(envelope or {})
    except ValueError as exc:
        return _refused(f"envelope: {exc}"[:300])
    if req.site_backstop is not None:
        req.site_backstop = " ".join("".join(ch for ch in req.site_backstop if ch.isprintable()).split())[:160] or None
    for name in (req.accounts or []) + (req.partitions or []):
        if not _VALID_NAME.match(name):
            return _refused(f"envelope: invalid name {name!r}")
    await reconcile_pending(app)   # a hand submit still open: its jobs are on the ledger before anything is admitted
    if adopt is not None:
        if not isinstance(adopt, str) or not _NATIVE.match(adopt):
            return _refused(f"invalid adopt {adopt!r}: a scheduler job id")
        return await _adopt_existing(app, adopt, req=req, confirm_spend=confirm_spend)
    try:
        spec = spec_of(app, command=command, session_id=session_id, account=account, partition=partition, nodes=nodes,
                       cores=cores, memory_gb=memory_gb, gpus=gpus, whole_node=whole_node, walltime=walltime,
                       array=array, depends_on=depends_on)
        hours = _deadline_hours(req)
    except JobRefusal as exc:
        return _refused(str(exc))
    if getattr(app.facility, "account_required", False) and not spec.account:
        return JobStatus(phase="needs_account", reason="account",
                         notice="The facility charges every job to an allocation account and none is set: pass "
                                "account=… (a LOGIN NAME is not an account). Nothing was submitted.")
    mine, orphan = find_envelope(app, spec.account)
    led = _ledger(app)
    if mine is None and orphan is not None:
        return await _offer_again(app, orphan, spec, req, confirm_spend=confirm_spend, exclude=exclude_jobs)
    if mine is None:
        return await _open_and_admit(app, spec, req, hours, confirm_spend=confirm_spend, exclude=exclude_jobs)
    # this session's envelope: a job that fits is admitted at once; widening it is the user's yes
    if exclude_jobs and confirm_spend:
        led.exclude_jobs(mine["id"], exclude_jobs)
        for j in led.jobs(envelope_id=mine["id"]):   # an adopted job the user says was not from their command
            if j.get("unheld") and _base(j.get("native_id")) in {_base(x) for x in exclude_jobs}:
                led.update_job(j["id"], state="refused", reason="the user said it was not from the approved command")
    if (why := _limits_refusal(None, mine, spec)) is not None:
        return JobStatus(phase="needs_confirmation", reason="envelope_limit", envelope=envelope_info(app, mine),
                         terms=terms_of(app, spec, float(mine["deadline"]), mine.get("site_backstop")),
                         notice=f"{why}. Nothing was submitted.")
    if req.cap is not None and confirm_spend and float(req.cap) != float(mine["cap"]):
        top, open_caps = _ceiling_room(app, envelope_accounts(mine), except_id=mine["id"])
        if top is not None and open_caps + float(req.cap) > top + 1e-9:
            return _ceiling_refusal(top, open_caps, float(req.cap))
        try:
            led.set_cap(mine["id"], float(req.cap), ceiling=top)
        except EnvelopeCeiling as exc:
            return _ceiling_refusal(exc.ceiling, exc.open_caps, float(req.cap))
    if req.site_backstop and confirm_spend:
        led.update_envelope(mine["id"], site_backstop=req.site_backstop)
    return await _admit(app, led.envelope(mine["id"]) or mine, spec, confirm_spend=confirm_spend)


async def _open_and_admit(app: AppCtx, spec: Spec, req: EnvelopeRequest, hours: float, *, confirm_spend: bool,
                          exclude: list[str] | None) -> JobStatus:
    """No envelope of this session here yet: propose one, after reading what the jobs hpc-bridge did not start already
    hold (its cap, by default, covers them plus this job — rounded up), or open it on the user's yes. A yes with no cap
    of its own opens at the cap this session PROPOSED; when what is held outgrew it (or no proposal of this server
    preceded the yes) it is proposed again — the yes never refuses itself for an amount the user could not see, and
    never opens at one they did not see (round 2). A proposal is STABLE (round 3): it names an absolute deadline,
    reused by the yes and every re-proposal, and a job of the user's with no time limit (or past it) is held to that
    deadline — so what is held does not grow with the user's think-time; the cap also carries a minute of the running
    held jobs' billing, the clock's own jitter between two reads."""
    now = _now()
    key = _proposal_key(app, spec)
    prop = app.jobs_proposals.get(key)
    deadline = (float(prop["deadline"]) if prop is not None and float(prop.get("deadline") or 0) > now
                + spec.walltime_s else now + hours * 3600)
    terms = terms_of(app, spec, deadline, req.site_backstop)
    if spec.walltime_s > deadline - now:
        return _refused(f"the job's walltime ({_hms(spec.walltime_s)}) does not fit before the envelope's deadline "
                        f"({_g(hours)} h): it could never finish in it — ask for a longer deadline or a shorter job")
    accts = req.accounts or ([spec.account] if spec.account else [])
    read, held, unread = await _held_now(app, accts or [spec.account or ""], list(exclude or []), deadline - now,
                                         wait_s=LOGIN_BUSY_WAIT_S)
    held_sum = sum(float(u.get("charged") or 0) + float(u.get("committed") or 0) for u in held)
    jitter = sum(float(u.get("rate") or 0) for u in held if u.get("state") == "RUNNING") * 60 / 3600
    need = _up(held_sum + spec.worst_case + jitter, 2)
    if req.cap is not None:
        cap = float(req.cap)
    elif confirm_spend and prop is not None and held_sum + spec.worst_case <= float(prop["cap"]) + 1e-9:
        cap = float(prop["cap"])
    else:
        cap = need
    proposal = _proposed(app, req, spec, deadline, cap=cap, held=held)
    if (why := _limits_refusal(req, None, spec)) is not None:
        return JobStatus(phase="refused" if confirm_spend else "needs_confirmation", reason="envelope_limit",
                         terms=terms, envelope=proposal, notice=f"{why}. Nothing was submitted.")
    held_note = (f" Already held on this facility and account: {_gu(held_sum)} {UNIT}, which counts against any "
                 f"envelope here — {_held_words(held)}." if held else "")
    if any(not u.get("session_block") for u in held):   # never the session's own block: it bills this allocation
        held_note += (" If a job hpc-bridge did not start is not the user's to count here, pass exclude_jobs=[…] "
                      "with their yes and it is never counted.")
    if not read:
        held_note = (f" What else the user runs could not be read just now ({unread}): the cap covers this job alone, "
                     "and the yes is admitted only once a reading works.")
    if cap + 1e-9 < held_sum + spec.worst_case:
        return JobStatus(phase="needs_confirmation", reason="envelope_exhausted", terms=terms, envelope=proposal,
                         notice=(f"The envelope asked for ({_g(cap)} {UNIT}) cannot hold this job: its worst case is "
                                 f"{_gu(spec.worst_case)} ({spec.basis}).{held_note} Ask the user to approve at least "
                                 f"{_gu(held_sum + spec.worst_case)}, or a smaller job — do not split the work to fit. "
                                 "Nothing was submitted."))
    accounts = proposal.accounts or [spec.account or ""]
    top, open_caps = _ceiling_room(app, accounts)
    if top is not None and open_caps + proposal.cap > top + 1e-9:
        return _ceiling_refusal(top, open_caps, proposal.cap)
    asking_again = confirm_spend and req.cap is None and (prop is None or cap > float(prop["cap"]) + 1e-9)
    if not confirm_spend or asking_again:
        if req.cap is None:
            app.jobs_proposals[key] = {"cap": cap, "held": held_sum, "at": now, "deadline": deadline}
        backstop = (f"site backstop: {req.site_backstop}" if req.site_backstop else "site backstop: none")
        why_again = ""
        if asking_again and prop is not None:
            why_again = (f"What is held changed since the proposal ({_gu(float(prop['held']))} then, "
                         f"{_gu(held_sum)} now), so the yes is not admitted at a figure the user did not see: this is "
                         "the proposal again. ")
        elif asking_again:
            why_again = ("No proposal of this server came before this yes (a restart, or the question was skipped), "
                         "so nothing is opened at a figure the user did not see: this is the proposal. ")
        return JobStatus(phase="needs_confirmation", reason="envelope_needed", terms=terms, envelope=proposal,
                         notice=(f"{why_again}This job needs the user's yes to a budget envelope first — nothing was "
                                 f"submitted. The job: {spec.basis} = at most {_gu(spec.worst_case)} {UNIT} "
                                 f"({spec.partition or 'the default partition'}, account {spec.account or 'none'}). "
                                 f"The envelope: a cap of {_g(proposal.cap)} {UNIT}, deadline {proposal.deadline} "
                                 f"({_g((deadline - now) / 3600)} h; every job carries it), {backstop}.{held_note} "
                                 f"{BINDS} Ask the user; "
                                 "on their yes call submit_job again with the same arguments and confirm_spend=True. "
                                 "Within the envelope, later jobs that fit are admitted without asking."))
    app.jobs_proposals.pop(key, None)
    led = _ledger(app)
    eid = _new_id("env")
    try:
        led.add_envelope(eid, facility=facility_key(app), account=spec.account, unit=UNIT, cap=proposal.cap,
                         deadline=deadline, approved_by=app.session_token, owner=batch.owner_of(app),
                         endpoint_id=app.state.endpoint_id, max_jobs=req.max_jobs,
                         max_nodes_per_block=req.max_nodes_per_block, accounts=proposal.accounts,
                         partitions=req.partitions, at_deadline=req.at_deadline, site_backstop=req.site_backstop,
                         ceiling=top)
    except EnvelopeCeiling as exc:   # another session opened one meanwhile: the transaction's own check
        return _ceiling_refusal(exc.ceiling, exc.open_caps, proposal.cap)
    if exclude:
        led.exclude_jobs(eid, exclude)
    e = led.envelope(eid)
    assert e is not None
    return await _admit(app, e, spec, confirm_spend=True)


async def _offer_again(app: AppCtx, e: dict[str, Any], spec: Spec, req: EnvelopeRequest, *, confirm_spend: bool,
                       exclude: list[str] | None) -> JobStatus:
    """Rule 3: an envelope another session approved (its server is gone) is offered again, naming what remains; the
    user's yes makes it this session's."""
    led = _ledger(app)
    await tick(app, envelope_ids=[e["id"]])
    e = led.envelope(e["id"]) or e
    info = envelope_info(app, e)
    terms = terms_of(app, spec, float(e["deadline"]), e.get("site_backstop"))
    if not confirm_spend:
        return JobStatus(phase="needs_confirmation", reason="envelope_new_session", terms=terms, envelope=info,
                         notice=(f"Envelope {e['id']} was approved in an earlier session (a server that is gone): this "
                                 f"session must ask the user again before it admits anything. It has "
                                 f"{_gd(info.remaining)} of {_g(info.cap)} {UNIT} left (spent {_g(info.spent)}, "
                                 f"committed {_g(info.committed)}), deadline {info.deadline}; what still bills: "
                                 f"{_bills_note(info)}. This job needs {_gu(spec.worst_case)} ({spec.basis}). On the "
                                 "user's yes call submit_job again with confirm_spend=True. Nothing was submitted."))
    cap = float(req.cap) if req.cap is not None else None
    top = None
    if cap is not None:
        top, open_caps = _ceiling_room(app, envelope_accounts(e), except_id=e["id"])
        if top is not None and open_caps + cap > top + 1e-9:
            return _ceiling_refusal(top, open_caps, cap)
    try:
        led.rebind_envelope(e["id"], session=app.session_token, owner=batch.owner_of(app), cap=cap, ceiling=top)
    except EnvelopeCeiling as exc:
        return _ceiling_refusal(exc.ceiling, exc.open_caps, cap or 0.0)
    if exclude:
        led.exclude_jobs(e["id"], exclude)
    return await _admit(app, led.envelope(e["id"]) or e, spec, confirm_spend=True)


async def _admit(app: AppCtx, e: dict[str, Any], spec: Spec, *, confirm_spend: bool) -> JobStatus:
    """Read, reserve, write, submit — in that order (reserve-before-admit). The meter reads first: the first job of an
    envelope is admitted only once what else the user runs has been counted (review B3)."""
    led = _ledger(app)
    deadline = float(e["deadline"])
    terms = terms_of(app, spec, deadline, e.get("site_backstop"))
    if spec.walltime_s > deadline - _now():
        return _refused(f"the job's walltime ({_hms(spec.walltime_s)}) does not fit before envelope {e['id']}'s "
                        f"deadline ({_iso(deadline)}): it could never finish")
    t = await tick(app, envelope_ids=[e["id"]], wait_s=LOGIN_BUSY_WAIT_S)
    e = led.envelope(e["id"]) or e
    if not t.ok and not e.get("metered_at") and t.busy:
        # transient: hpc-bridge's own channel, not the accounting and not the job — retryable, never a refusal
        return JobStatus(phase="busy", reason="login_channel_busy", terms=terms, envelope=envelope_info(app, e),
                         notice=(f"Not submitted yet, and not refused: the scheduler's accounting could not be read in "
                                 f"time ({t.error or 'no answer'}), and envelope {e['id']}'s first job is admitted "
                                 "only once it has been. The envelope stays open with the user's yes on it: call "
                                 f"submit_job again with the same arguments in about {LOGIN_BUSY_WAIT_S:.0f} s — no "
                                 "new yes is needed unless that answer asks for one."))
    if not t.ok and not e.get("metered_at"):
        return JobStatus(phase="refused", reason="meter_unread", terms=terms, envelope=envelope_info(app, e),
                         notice=(f"The scheduler's accounting could not be read just now ({t.error or 'no answer'}), "
                                 f"so the jobs hpc-bridge did not start cannot be counted against envelope {e['id']} "
                                 "before its first job: nothing was submitted. The envelope stays open; try the same "
                                 "call again shortly."))
    job_id = _new_id("job")
    try:
        led.reserve_job(e["id"], job_id, session=app.session_token, worst_case=spec.worst_case, tasks=spec.tasks,
                        nodes=spec.nodes, account=spec.account, partition=spec.partition, source="submit_job",
                        scheduler=scheduler_of(app), session_id=spec.session_id, cores=spec.cores,
                        memory_gb=spec.memory_gb, gpus=spec.gpus, walltime_s=spec.walltime_s, array_spec=spec.array,
                        throttle=spec.throttle, depends_on=json.dumps(spec.depends), rate=spec.rate,
                        min_charge_h=spec.min_charge_h, deadline=deadline)
    except EnvelopeExhausted as exc:
        led.update_envelope(e["id"], state="exhausted")
        info = envelope_info(app, led.envelope(e["id"]) or e)
        return JobStatus(phase="needs_confirmation", reason="envelope_exhausted", terms=terms, envelope=info,
                         notice=(f"Envelope {e['id']} is exhausted for this job: {_gu(exc.held)} of {_g(exc.cap)} "
                                 f"{UNIT} is held and this job needs {_gu(exc.need)} ({spec.basis}); "
                                 f"{_gd(info.remaining)} is left, so the cap would have to be at least "
                                 f"{_gu(exc.held + exc.need)}. What still bills: {_bills_note(info)}. Nothing was "
                                 "submitted. Ask the user: raise the cap (submit_job(…, envelope={cap: …}, "
                                 "confirm_spend=True)) or wait for jobs to end — never split the work to fit."
                                 + _unledgered_note(info)))
    except EnvelopeNotOwned:
        return JobStatus(phase="needs_confirmation", reason="envelope_new_session", terms=terms,
                         envelope=envelope_info(app, e), notice="The envelope belongs to another session: ask the "
                                                               "user again. Nothing was submitted.")
    except (EnvelopeClosed, EnvelopeLimit, EnvelopeError) as exc:
        return JobStatus(phase="needs_confirmation", reason="envelope_limit", terms=terms,
                         envelope=envelope_info(app, e), notice=f"{exc}. Nothing was submitted.")
    app.jobs_admitting.add(job_id)   # a stop meanwhile counts it as one that can still bill, and asks it to give up
    try:
        return await _write_and_submit(app, e, spec, job_id, deadline, terms)
    finally:
        app.jobs_admitting.discard(job_id)


async def _write_and_submit(app: AppCtx, e: dict[str, Any], spec: Spec, job_id: str, deadline: float,
                            terms: JobTerms) -> JobStatus:
    led = _ledger(app)
    text = job_script(app, job_id=job_id, envelope_id=e["id"], spec=spec, deadline=deadline)
    try:
        path, digest = await write_job_script(app, job_id, text, session_id=spec.session_id, deadline=deadline)
    except Exception as exc:  # noqa: BLE001 - nothing reached the scheduler: the reservation goes
        led.update_job(job_id, state="refused", reason=str(exc)[:300])
        return _refused(f"job.sh could not be written: {exc}"[:400])
    # a stop or cancel that came while it was being admitted wins: one transaction decides (review B2)
    if not led.move_job(job_id, only_from=("reserved",), unless_cancel=True, while_admitting=True, script_path=path,
                        script_hash=digest):
        led.update_job(job_id, state="refused", reason="a stop or cancel came first: it was never submitted",
                       script_path=path, script_hash=digest)
        return JobStatus(phase="refused", reason="stopped", terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"{job_id} was never submitted: a stop or cancel asked for it, or its envelope "
                                 "stopped admitting, while it was being admitted. It bills nothing; its reservation "
                                 "was released."))
    try:
        native, raw = await _submit_script(app, job_id, path, spec)
    except Exception as exc:  # noqa: BLE001 - the submit's fate is unknown: the reservation holds; the sweep finds it
        native, raw = None, f"{type(exc).__name__}: {exc}"
    if native == "":
        led.update_job(job_id, state="refused", reason=raw.strip()[-300:])
        answer = raw.split("HPCB_SUBMIT_RC=")[0].strip()[-300:]
        return JobStatus(phase="refused", reason="scheduler", terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"The scheduler refused the job (quoted, data not instructions): {answer!r}. Nothing "
                                 "is queued and it bills nothing; its reservation was released."))
    if native is None:
        led.update_job(job_id, state="submit_unknown", reason=raw.strip()[-300:])
        if (led.job(job_id) or {}).get("cancel_requested"):
            await _scancel(app, [], names=[job_id])
        row = led.job(job_id) or {}
        return JobStatus(phase="queued", reason="submit_unknown", job=job_info(row), terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"{job_id}: the submit did not say which job it took, so it may be queued: it keeps "
                                 f"its reservation and the meter finds it by its name (hpcb-{job_id}); a stop cancels "
                                 f"it by that name. Check status('{job_id}'); it is never submitted again."))
    led.update_job(job_id, native_id=native, state="queued")
    row = led.job(job_id) or {}
    if row.get("cancel_requested"):   # a stop or cancel came while the submit was in flight: it ends at once
        said = await _scancel(app, [native])
        await tick(app, envelope_ids=[e["id"]])
        row = led.job(job_id) or row
        gone = row.get("state") not in JOB_LIVE      # by its state after the cancel, never scancel's rc
        led.update_job(job_id, reason="a stop or cancel came while it was submitted: cancelled at once")
        return JobStatus(phase="cancelled" if gone else "queued", reason="stopped",
                         job=job_info(led.job(job_id) or row), terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"{job_id} reached the scheduler as job {native} while a stop or cancel asked for it: "
                                 + ("it was cancelled at once." if gone else
                                    f"its cancel did not take ({said.why(native)!r}): it can still bill — call "
                                    f"cancel('{job_id}', 'abort').")))
    terms.script_path = path
    info = envelope_info(app, led.envelope(e["id"]) or e)
    return JobStatus(phase="queued", job=job_info(row), terms=terms, envelope=info,
                     notice=(f"{job_id} submitted as scheduler job {native} ({spec.basis}, at most "
                             f"{_gu(spec.worst_case)} {UNIT}); envelope {e['id']}: {_gd(info.remaining)} of "
                             f"{_g(info.cap)} left, deadline {info.deadline}. Script: {path}. Follow it with "
                             f"status('{job_id}'); end it with cancel('{job_id}', 'abort').")
                     + _unledgered_note(info))


# ---- status and cancel of job- and env- ids --------------------------------------------------------------------------

_PHASE = {"reserved": "queued", "submit_unknown": "queued", "held": "queued", "queued": "queued", "running": "running",
          "done": "done", "failed": "failed", "cancelled": "cancelled", "refused": "refused"}


def _status_refused(ident: str, why: str) -> BatchStatus:
    return BatchStatus(kind="job" if ident.startswith("job-") else "envelope", phase="refused", reason=why,
                       notice=f"status refused: {why}")


async def status(app: AppCtx, ident: str) -> BatchStatus:
    led = _ledger(app)
    if not (JOB_ID.match(ident) or ENV_ID.match(ident)):
        return _status_refused(ident, f"{ident!r} is no job or envelope id (job-<12 hex>, env-<12 hex>)")
    hand_notes = await reconcile_pending(app)
    extra = (" " + " ".join(hand_notes)) if hand_notes else ""
    if JOB_ID.match(ident):
        row = led.job(ident)
        if row is None:
            return _status_refused(ident, f"no job {ident} in the ledger")
        e = led.envelope(row["envelope_id"])
        if e is not None and e.get("facility") == facility_key(app):
            t = await tick(app, envelope_ids=[e["id"]])
        else:
            t = Tick(False, error="the job's facility is not the one bound here")
        row = led.job(ident) or row
        e = led.envelope(row["envelope_id"]) or e
        info = job_info(row)
        note = f"{ident} ({row.get('native_id') or 'not submitted'}): {info.state}"
        if info.reason:
            note += f" ({info.reason})"
        note += f"; charged {_g(info.charged)} {UNIT}, up to {_gu(info.committed)} more."
        if info.state == "PENDING" and info.reason and site_limit(info.reason):
            note += _limit_words(info.reason, ident)
        if info.meter_source in ("missing", "squeue"):
            note += (" sacct did not list it" + (" (it could not be read: " + t.error + ")" if t.error and not t.ok
                                                 else "") + ": it is counted at its full walltime until sacct does.")
        if not t.ok and info.meter_source not in ("missing", "squeue"):
            note += f" (Not read just now: {t.error}; this is the ledger's last view.)"
        return BatchStatus(kind="job", phase=_PHASE.get(str(row["state"]), "queued"), job=info,  # type: ignore[arg-type]
                           envelope=envelope_info(app, e) if e else None, notice=note + extra)
    e = led.envelope(ident)
    if e is None:
        return _status_refused(ident, f"no envelope {ident} in the ledger")
    if e.get("facility") == facility_key(app):
        await tick(app, envelope_ids=[ident])
    e = led.envelope(ident) or e
    einfo = envelope_info(app, e)
    note = (f"{ident}: {einfo.state}; {_g(einfo.spent)} spent + {_g(einfo.committed)} committed of "
            f"{_g(einfo.cap)} {UNIT} ({_gd(einfo.remaining)} left), deadline {einfo.deadline}. What still bills: "
            f"{_bills_note(einfo)}."
            + ("" if einfo.owned else " Another session approved it: admitting more needs the user's yes again.")
            + _unledgered_note(einfo))
    phase = einfo.state if einfo.state != "proposed" else "open"
    return BatchStatus(kind="envelope", phase=phase, envelope=einfo, notice=note + extra)


@dataclass
class Cancelled:
    """What the scheduler said to a cancel. What the cancel DID is read from each job's state after it (`_cancel_words`)
    — scancel answers rc 0 for a job that had already ended (probe r1). `failed`: what it said, for each id it was sent,
    when it answered non-zero."""
    ok: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    text: str = ""

    def why(self, native: str) -> str:
        return self.failed.get(native, "no answer")


_BENIGN = ("Invalid job id", "already completing or completed", "Unknown Job Id", "has finished", "Job has finished")


async def _scancel(app: AppCtx, natives: list[str], *, pending_only: bool = False,
                   names: list[str] | None = None) -> Cancelled:
    """Cancel `natives` in ONE scheduler invocation (`scancel a b c` / `qdel a b c`), and by their name hpcb-<job id>
    the jobs of `names` whose scheduler id is unknown (a submit of unknown fate) — one login call, bounded (r3)."""
    ids = [n for n in natives if _NATIVE.match(n)]
    by_name = [n for n in (names or []) if JOB_ID.match(n)]
    if (not ids and not by_name) or not _can_reach(app):
        return Cancelled()
    cmd = f"{meter._bound()}set -f; "
    if scheduler_of(app) == "pbs":
        if ids:
            cmd += f"__r=$(__hb_bound qdel {' '.join(shlex.quote(i) for i in ids)} 2>&1); echo \"HPCB_CAN_RC=$?\"; "
        for n in by_name:
            cmd += (f"for __j in $(__hb_bound qselect -u {_USER} -N hpcb-{n} 2>/dev/null); do __hb_bound qdel "
                    "\"$__j\" >/dev/null 2>&1; done; ")
    else:
        verb = "scancel --state=PENDING" if pending_only else "scancel"
        if ids:
            words = " ".join(shlex.quote(i.split(".")[0]) for i in ids)
            cmd += f"__r=$(__hb_bound {verb} {words} 2>&1); echo \"HPCB_CAN_RC=$?\"; "
        for n in by_name:
            cmd += f"__hb_bound scancel --name=hpcb-{n} --user={_USER} >/dev/null 2>&1; "
    cmd += 'printf "HPCB_CAN_SAID %s\\n" "$(printf %s "${__r:-}" | tr "\\n" " " | cut -c1-300)"; echo HPCB_CANCEL_END'
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001 - nothing is said cancelled
        return Cancelled(failed={i: f"{type(exc).__name__}: {exc}" for i in ids}, text=str(exc))
    raw = _text(out)
    said = Cancelled(text=raw)
    rc = re.search(r"HPCB_CAN_RC=(\d+)", raw)
    m_said = re.search(r"HPCB_CAN_SAID (.*)", raw)
    words_said = m_said.group(1) if m_said else ""
    if rc is not None and rc.group(1) == "0":
        said.ok = list(ids)
    elif ids:
        said.failed = {i: words_said.strip() or f"rc {rc.group(1) if rc else '?'}" for i in ids}
    return said


def _left(rows: list[dict[str, Any]]) -> list[BlockLeft]:
    out = []
    for j in rows:
        info = job_info(j)
        out.append(BlockLeft(job_id=j.get("native_id"), bill_until=_iso(j.get("deadline")),
                             how_to_end=f"cancel('{j['id']}', 'abort') (scancel/qdel {j.get('native_id')}); it can "
                                        f"still bill up to {_gu(info.committed)} {UNIT}"))
    return out


def _cancel_words(said: Cancelled, after: list[dict[str, Any]], *, drain: bool = False) -> str:
    """What a cancel did, by each job's state AFTER it, as the meter read it — never by scancel's exit code (probe r1:
    scancel of a job that had already ended answers rc 0 and nothing)."""
    def nid(j: dict[str, Any]) -> str:
        return str(j.get("native_id"))

    done = [nid(j) for j in after if j.get("state") == "cancelled"]
    ended = [f"{nid(j)} ({j.get('sched_state') or j.get('state')})" for j in after if j.get("state") in ("done",
                                                                                                         "failed")]
    live = [j for j in after if j.get("state") in JOB_LIVE]
    refused = [j for j in live if nid(j) in said.failed and not any(b in said.failed[nid(j)] for b in _BENIGN)]
    going = [nid(j) for j in live if j not in refused]
    parts = []
    if done:
        parts.append(f"cancelled: {' '.join(done)}")
    if ended:
        parts.append(f"had already ended (not cancelled): {', '.join(ended)}")
    if going:
        parts.append(f"pending tasks cancelled, running ones go on: {' '.join(going)}" if drain else
                     f"cancel sent, still listed by the scheduler: {' '.join(going)}")
    if refused:
        parts.append("the scheduler did NOT cancel " + ", ".join(f"{nid(j)} ({said.failed[nid(j)][:160]!r})"
                                                                  for j in refused))
    return "; ".join(parts)


async def cancel(app: AppCtx, ident: str, mode: str = "drain") -> CancelResult:
    if mode not in ("drain", "abort"):
        return CancelResult(id=str(ident), status="refused", notice=f"mode must be drain or abort, not {mode!r}")
    led = _ledger(app)
    if JOB_ID.match(ident):
        row = led.job(ident)
        if row is None:
            return CancelResult(id=ident, status="refused", mode=mode,  # type: ignore[arg-type]
                                notice=f"no job {ident} in the ledger")
        rows = [row]
        eid = row["envelope_id"]
    elif ENV_ID.match(ident):
        e = led.envelope(ident)
        if e is None:
            return CancelResult(id=ident, status="refused", mode=mode,  # type: ignore[arg-type]
                                notice=f"no envelope {ident} in the ledger")
        if e["state"] in ENVELOPE_ADMITTING:
            led.update_envelope(ident, state="draining", close_reason=f"cancel('{ident}', '{mode}')")
        rows = led.jobs(envelope_id=ident)
        eid = ident
    else:
        return CancelResult(id=str(ident), status="refused", mode=mode,  # type: ignore[arg-type]
                            notice=f"{ident!r} is no job or envelope id")
    live = [j for j in rows if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
    # a cancel is the user's choice: it ends an adopted job too (only automatic ends — an at_deadline abort — never do)
    targets = live
    natives = [str(j["native_id"]) for j in targets if j.get("native_id")]
    unsent = [j for j in targets if not j.get("native_id")]
    acting = mode == "abort" or ident.startswith("job-")
    if (natives or unsent) and acting and not _can_reach(app):
        return CancelResult(id=ident, status="draining", mode=mode, in_flight=len(live),  # type: ignore[arg-type]
                            blocks_left=_left(live),
                            notice=(f"No login channel to the facility is bound here: nothing was cancelled. Call "
                                    f"connect_facility first, then cancel('{ident}', '{mode}') again."))
    said = Cancelled()
    for j in unsent:   # an admission in flight gives up (a drain admits nothing more either)
        if acting or j.get("state") == "reserved":
            led.update_job(j["id"], cancel_requested=1)
    if acting:   # a submit of unknown fate is cancelled by its name
        names = [j["id"] for j in unsent if j.get("state") == "submit_unknown"
                 or (j.get("state") == "reserved" and j["id"] not in app.jobs_admitting)]
        if natives or names:
            said = await _scancel(app, natives, pending_only=mode == "drain" and ident.startswith("job-"), names=names)
    await tick(app, envelope_ids=[eid])
    rows = [led.job(j["id"]) or j for j in rows]
    still = [j for j in rows if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
    e = led.envelope(eid)
    if e is not None and e["state"] == "draining" and not still and ident.startswith("env-"):
        led.update_envelope(eid, state="closed", closed_at=_now())
    what = _cancel_words(said, [j for j in rows if j["id"] in {t["id"] for t in targets} and j.get("native_id")],
                         drain=mode == "drain") if acting and natives else ""
    if not what:
        what = "drained: it admits nothing more" if mode == "drain" else "nothing was cancelled"
    status_ = "draining" if still else "down"
    listed = ", ".join(str(j.get("native_id") or f"{j['id']} (not yet submitted)") for j in still)
    again = " (abort ends them)" if mode == "drain" else " until it says down"
    tail = (f" {len(still)} job(s) can still bill: {listed} — call cancel again{again}."
            if still else " Nothing of it can bill now.")
    return CancelResult(id=ident, status=status_, mode=mode, in_flight=len(still),  # type: ignore[arg-type]
                        blocks_left=_left(still), notice=f"{ident}: {what}.{tail}")


# ---- D2: the gate and the adoption after the fact (round 3, the user's decision: what the design says) ---------------

_SUBMITTERS = frozenset({"sbatch", "qsub"})


def _submits(sub: Submission | None) -> bool:
    """Whether the command line submits a batch job by hand ANYWHERE in it (review A4: `srun …; sbatch x` was named by
    its first call, srun, and never gated)."""
    return sub is not None and bool(_SUBMITTERS & {sub.command, *sub.names})


def gate_envelope(app: AppCtx, sub: Submission | None) -> dict[str, Any] | None:
    """The envelope a recognised hand sbatch / qsub must ask about (D2 (c)): this session's admitting envelope on this
    SSH facility. None: no gate (no envelope here, or the command submits no batch job — #176's advisory stands)."""
    if not _submits(sub) or not _has_login_shape(app):
        return None
    try:
        envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
                if e.get("approved_by") == app.session_token and float(e["deadline"]) > _now()]
    except LedgerError:
        return None
    return envs[0] if envs else None


def gated_outcome(app: AppCtx, e: dict[str, Any], sub: Submission) -> ShellOutcome:
    info = envelope_info(app, e)
    which = next((n for n in (*sub.names, sub.command) if n in _SUBMITTERS), sub.command)
    return ShellOutcome(
        phase="needs_confirmation", block_state="cold", envelope=info,
        notice=(f"This command submits a scheduler job by hand (`{which}`) while envelope {e['id']} is open "
                f"({_gd(info.remaining)} of {_g(info.cap)} {UNIT} left, deadline {info.deadline}): hpc-bridge asks "
                "first — NOTHING ran. Ask the user. On their yes, call run_shell again with the same command and "
                "confirm_spend=True: it runs as it is, and the jobs of the user's that appear while it runs are "
                "adopted after the fact — ledgered as job- ids, counted against the envelope at their full request, "
                "named — with NO envelope deadline applied to them; if they put the envelope over its cap, it admits "
                "nothing more. hpc-bridge never cancels them on its own: stop_endpoint asks about them by name, and "
                "cancel('job-…') ends one."))


def _hand_dir(h: dict[str, Any]) -> str:
    """Where a hand submit's snapshots live: <session state dir>/hands/<token> (the row's `mark_path` is its `after`
    file — the column kept its name from the hold design)."""
    return str(h["mark_path"]).rsplit("/", 1)[0]


def open_hand(app: AppCtx, e: dict[str, Any], session: session_shell.Session) -> dict[str, Any]:
    """The user said yes to a gated hand submit: its `hand` ledger row, written BEFORE the command is sent, so whatever
    becomes of the call its jobs are adopted. No login call: the snapshots are taken in the command's own shell."""
    led = _ledger(app)
    token = secrets.token_hex(6)
    hd = f"{session.state_dir}/hands/{token}"
    led.add_hand(token, envelope_id=e["id"], facility=facility_key(app), session=app.session_token,
                 mark_path=f"{hd}/after", shim_dir="", before=[])
    row = next((h for h in led.hands(facility=facility_key(app)) if h["token"] == token), None)
    assert row is not None
    return row


def drop_hand(app: AppCtx, h: dict[str, Any], why: str) -> None:
    """The command never ran (its block was cold, its spend unconfirmed, the call was cancelled first…): the hand is
    closed at once — an open one would read later jobs of the user's as this command's (review r2, P1)."""
    _ledger(app).update_hand(h["token"], state="closed", note=f"never ran: {why}"[:200])


def _snap_fn(sched: str) -> str:
    """`__hpcb_snap FILE`: the user's job ids the scheduler lists now, one a line, into FILE — and FILE.ok when it
    could be read (bounded)."""
    if sched == "pbs":
        read, norm = f"__hb_bound qselect -u {_USER}", "sed -n '/./p'"
    else:
        read, norm = f"__hb_bound squeue -h -u {_USER} -o %i", "sed -n 's/_.*//;/./p'"
    return ('__hpcb_snap() { local __ids; rm -f "$1.ok"; '
            f'if __ids=$({read} 2>/dev/null); then printf \'%s\\n\' "$__ids" | {norm} | sort -u > "$1" && '
            ': > "$1.ok"; fi; }\n')


def hand_command(app: AppCtx, h: dict[str, Any], command: str) -> str:
    """The gated command, UNMODIFIED, inside one remote shell that lists the user's jobs immediately before it and —
    into the session dir, so it is taken when the command ENDS even past the sync-wait — immediately after it (an `exit`
    in the command runs it from the EXIT trap, chained to the session's own). Nothing reaches the command's environment
    but two shell functions, unset after it."""
    b64 = shlex.quote(base64.b64encode(command.encode()).decode())
    hd = batch.quoted_dir(_hand_dir(h))
    return (f"{meter._bound()}\n{_snap_fn(scheduler_of(app))}"
            f"__hpcb_hd={hd}\n"
            'mkdir -p "$__hpcb_hd" 2>/dev/null\n'
            '__hpcb_after() { [ -n "${__hpcb_done:-}" ] && return 0; __hpcb_done=1; __hpcb_snap "$__hpcb_hd/after"; '
            'date +%s > "$__hpcb_hd/ended" 2>/dev/null; }\n'
            '__hpcb_tp=$(trap -p EXIT); __hpcb_t0=""\n'
            '[ -n "$__hpcb_tp" ] && eval "__hpcb_ta=( $__hpcb_tp )" && __hpcb_t0=${__hpcb_ta[2]}\n'
            'trap "__hpcb_x=\\$?; __hpcb_after; (exit \\$__hpcb_x); $__hpcb_t0" EXIT\n'
            '__hpcb_snap "$__hpcb_hd/before"\n'
            f'eval "$(printf %s {b64} | base64 -d)"\n'
            "__hpcb_hrc=$?\n"
            "__hpcb_after\n"
            'if [ -n "$__hpcb_t0" ]; then trap -- "$__hpcb_t0" EXIT; else trap - EXIT; fi\n'
            "unset -f __hpcb_after __hpcb_snap __hb_bound; unset __hpcb_hd __hpcb_tp __hpcb_ta __hpcb_t0 __hpcb_done\n"
            '(exit "$__hpcb_hrc")\n')


@dataclass
class _Request:
    native: str
    rate: float
    walltime_s: float
    tasks: int
    nodes: int
    partition: str | None
    account: str | None
    held: bool
    name: str
    deadline: float | None = None   # Slurm's Deadline, read back (UTC); None: none set (or PBS: soft)
    state: str = "PENDING"


def _slurm_time(s: str | None) -> float | None:
    if not s or not _LOCAL_TIME.match(s):
        return None
    return float(calendar.timegm(time.strptime(s, "%Y-%m-%dT%H:%M:%S")))


def _slurm_request(native: str, text: str) -> _Request | None:
    """A job's request from `scontrol show job -o` (run under TZ=UTC0): its billing — ReqTRES / AllocTRES (Slurm 23)
    or TRES, the most of them, else its CPUs —, its walltime, its tasks (every record's, a stepped array's `0-98:2`
    counted by its step: review A5), its deadline, whether it is held."""
    recs = [ln for ln in (text or "").splitlines() if "JobId=" in ln]
    if not recs:
        return None
    first: dict[str, str] = {}
    tasks = 0
    for ln in recs:
        kv = dict(re.findall(r"(\w+)=(\S*)", ln))
        first = first or kv
        tasks += meter.range_count(kv["ArrayTaskId"]) if kv.get("ArrayTaskId") else 1
    kv = first
    specs = [kv.get(k, "") for k in ("ReqTRES", "AllocTRES", "TRES")]
    bills = [b for b in (meter.tres(s, "billing") for s in specs) if b]
    cpus = [c for c in (meter.tres(s, "cpu") for s in specs) if c]
    bill = max(bills) if bills else (max(cpus) if cpus else None)
    if bill is None:
        try:
            bill = float(kv.get("NumCPUs") or 0) or None
        except ValueError:
            bill = None
    limit = kv.get("TimeLimit")
    try:
        wall = float(_parse_hhmmss(limit)) if limit and limit not in ("UNLIMITED", "Partition_Limit") else 0.0
    except (TypeError, ValueError):
        wall = 0.0
    try:
        nodes = int(str(kv.get("NumNodes") or "1").split("-")[0])
    except ValueError:
        nodes = 1
    if not bill:
        return None
    acct = kv.get("Account")
    return _Request(native, float(bill), wall, max(tasks, 1), nodes, kv.get("Partition") or None,
                    acct if acct not in (None, "", "(null)") else None,
                    kv.get("JobState") == "PENDING" and "held" in (kv.get("Reason") or "").lower(),
                    kv.get("JobName") or "", _slurm_time(kv.get("Deadline")), kv.get("JobState") or "PENDING")


def _pbs_request(native: str, text: str) -> _Request | None:
    start = text.find("{")
    try:
        data = json.loads(text[start:]) if start >= 0 else {}
    except ValueError:
        return None
    for key, j in (data.get("Jobs") or {}).items():
        if _base(key) != _base(native):
            continue
        rl = j.get("Resource_List") or {}
        wall = meter._pbs_secs(rl.get("walltime")) or 0.0   # 0: no walltime — held to the envelope's horizon
        ncpus = rl.get("ncpus")
        if not isinstance(ncpus, int | float):
            return None
        tasks = meter.range_count(str(j["array_indices_submitted"])) if j.get("array_indices_submitted") else 1
        st = str(j.get("job_state") or "Q")
        return _Request(native, float(ncpus), wall, tasks, int(rl.get("nodect") or 1), j.get("queue"),
                        j.get("Account_Name"), st == "H", str(j.get("Job_Name") or ""), None,
                        meter._PBS_STATE.get(st, "PENDING"))
    return None


def _request(sched: str, native: str, text: str) -> _Request | None:
    return _pbs_request(native, text) if sched == "pbs" else _slurm_request(native, text)


def _req_cmd(sched: str) -> str:
    return ('__hb_bound qstat -f -F json "$__j" 2>&1' if sched == "pbs"
            else '__hb_bound env TZ=UTC0 scontrol show job -o "$__j" 2>&1')


def _req_chunks(raw: str) -> dict[str, str]:
    body = raw.split("HPCB_REQ_END")[0]
    parts = re.split(r"^HPCB_REQ (\S+)\s*$", body, flags=re.M)
    return {parts[i]: parts[i + 1] for i in range(1, len(parts) - 1, 2)}


READ_BATCH = 100   # requests one login call reads; what is left is read by the next call (nothing hinges on it)


async def _read_requests(app: AppCtx, natives: list[str]) -> dict[str, _Request]:
    """Up to READ_BATCH jobs' requests in ONE login call (each read bounded)."""
    sched = scheduler_of(app)
    ids = [n for n in natives if _NATIVE.match(n)]
    if not ids:
        return {}
    cmd = (f"{meter._bound()}set -f; for __j in {' '.join(shlex.quote(n) for n in ids[:READ_BATCH])}; do "
           f"echo \"HPCB_REQ $__j\"; {_req_cmd(sched)}; done; echo HPCB_REQ_END")
    out = await _login(app, cmd)
    got: dict[str, _Request] = {}
    for n, text in _req_chunks(_text(out)).items():
        r = _request(sched, n, text)
        if r is not None:
            got[n] = r
    return got


def _worst(app: AppCtx, r: _Request, *, horizon_s: float = 0.0) -> tuple[float, float | None]:
    """(a request's worst case — every task, at its billing, for its walltime and never below the queue's minimum
    charge (review B4); one with no walltime (UNLIMITED, a PBS job without one) for `horizon_s` —, that minimum)."""
    shape = resources.shape_of(getattr(app.facility, "partitions", None), r.partition)
    min_h = shape.min_charge_h if shape is not None else None
    wall = r.walltime_s if r.walltime_s > 0 else horizon_s
    return r.tasks * r.rate * max(wall / 3600, min_h or 0.0), min_h


@dataclass
class HandResult:
    jobs: list[JobInfo] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    found: bool = False     # the command submitted something: a job of the user's appeared while it ran
    closed: bool = False
    pending: list[str] = field(default_factory=list)   # jobs that appeared, not yet adopted (read-only, or unread)
    running: bool = False   # the command has not ended (no `after` snapshot yet)


async def reconcile_hand(app: AppCtx, h: dict[str, Any], *, final: bool, adopt: bool = True) -> HandResult:
    """D2 after the user's yes (round 3, the user's decision): the jobs of the user's that appeared between the
    command's two snapshots — before it, and when it ENDED (in the session dir: never at this later call, so a job the
    user submits afterwards from their own terminal is never read as the command's) — are adopted after the fact:
    ledgered (source `adopt`, `unheld`: no envelope deadline applied), metered, reserved even past the cap (which then
    closes admission, said by how much). What one call cannot read (READ_BATCH) the next call adopts; nothing is ever
    cancelled here. `final`: the call's outcome says the command ended. `adopt=False` reads only (a stop that asks):
    `pending` names what appeared, `running` says the command has not ended."""
    led = _ledger(app)
    sched = scheduler_of(app)
    res = HandResult()
    token = str(h["token"])
    e = led.envelope(str(h["envelope_id"]))
    hd = batch.quoted_dir(_hand_dir(h))
    known = sorted({_base(j["native_id"]) for j in led.jobs() if j.get("native_id")})
    excluded = set(json.loads((e or {}).get("excluded") or "[]"))
    skip = sorted(set(known) | {_base(x) for x in excluded})
    filt = ("grep -vxF " + " ".join(f"-e {shlex.quote(x)}" for x in skip)) if skip else "cat"
    diff = '{ [ -f "$hd/before.ok" ] && [ -f "$hd/after.ok" ] && comm -13 "$hd/before" "$hd/after"; } 2>/dev/null'
    old = [x for x in led.hands(facility=facility_key(app), states=("closed",)) if x["token"] != token][:20]
    rm = "".join(f"rm -rf {batch.quoted_dir(_hand_dir(x))} 2>/dev/null; " for x in old)
    reads = (f'for __j in $( {diff} | {filt} | head -n {READ_BATCH} ); do echo "HPCB_REQ $__j"; {_req_cmd(sched)}; '
             "done; " if adopt else "")
    cmd = (f"{meter._bound()}set -f; {rm if adopt else ''}hd={hd}; "
           '[ -f "$hd/before.ok" ] && echo HPCB_BEFORE_OK; [ -f "$hd/after.ok" ] && echo HPCB_AFTER_OK; '
           '[ -f "$hd/ended" ] && echo HPCB_CMD_ENDED; '
           f"echo HPCB_NEW_BEGIN; {diff}; echo HPCB_NEW_END; {reads}echo HPCB_REQ_END")
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001 - the next call reads it again
        res.notes.append(f"The jobs of an approved hand submit could not be read just now ({type(exc).__name__}); the "
                         "next call reads them again.")
        res.running = True
        return res
    raw = _text(out)
    if "HPCB_REQ_END" not in raw:
        res.notes.append("The jobs of an approved hand submit could not be read just now; the next call reads them "
                         "again.")
        res.running = True
        return res
    if adopt:
        for x in old:
            led.update_hand(x["token"], state="gone")
    return await _adopt_from(app, h, e, raw, res, final=final, adopt=adopt, known=set(known), excluded=excluded)


async def _adopt_from(app: AppCtx, h: dict[str, Any], e: dict[str, Any] | None, raw: str, res: HandResult, *,
                      final: bool, adopt: bool, known: set[str], excluded: set[str]) -> HandResult:
    led = _ledger(app)
    sched = scheduler_of(app)
    token = str(h["token"])
    before_ok, after_ok = "HPCB_BEFORE_OK" in raw, "HPCB_AFTER_OK" in raw
    ended = "HPCB_CMD_ENDED" in raw
    body = raw.split("HPCB_NEW_BEGIN", 1)[-1].split("HPCB_NEW_END", 1)[0]
    new = [x.strip() for x in body.splitlines() if _NATIVE.match(x.strip())]
    fresh = [x for x in new if _base(x) not in known and x not in excluded and _base(x) not in excluded]
    res.found = bool(new)
    now = _now()
    # a command still running keeps its hand open whatever became of its envelope; only a day, or no envelope, ends it
    stale = now - float(h.get("created_at") or now) > HAND_MAX_OPEN_S or e is None
    if not after_ok:
        if final and not ended:
            res.notes.append("@needs_after")   # the outcome says it ended, its own after-snapshot is missing (exec)
            return res
        if ended or stale:
            res.notes.append("The user's jobs could not be listed when the approved command ended, so what it "
                             "submitted cannot be told apart; the meter's sweep counts any such job (as one "
                             "hpc-bridge did not start).")
            if adopt:
                led.update_hand(token, state="closed", note="no after snapshot")
                res.closed = True
            return res
        res.running = True
        return res
    if not before_ok:
        res.notes.append("The user's jobs could not be listed before the approved command ran, so what it submitted "
                         "cannot be told apart; the meter's sweep counts any such job (as one hpc-bridge did not "
                         "start).")
        if adopt:
            led.update_hand(token, state="closed", note="no before snapshot")
            res.closed = True
        return res
    if not adopt or e is None:
        res.pending = fresh
        return res
    chunks = _req_chunks(raw)
    reqs = {_base(k): r for k, t in chunks.items() if (r := _request(sched, k, t)) is not None}
    tried = {_base(k) for k in chunks}
    take = [x for x in fresh if _base(x) in reqs]
    if take:
        await tick(app, envelope_ids=[e["id"]], also_known={_base(x) for x in take})
        e = led.envelope(e["id"]) or e
    horizon = max(0.0, float(e["deadline"]) - now)
    words: list[str] = []
    for n in take:
        if _base(n) in {_base(j["native_id"]) for j in led.jobs() if j.get("native_id")}:
            continue   # another reconciliation took it meanwhile
        r = reqs[_base(n)]
        worst, min_h = _worst(app, r, horizon_s=horizon)
        job_id = _new_id("job")
        led.reserve_job(e["id"], job_id, session=str(e["approved_by"]), worst_case=worst, tasks=r.tasks,
                        nodes=r.nodes, account=r.account, partition=r.partition, source="adopt", native_id=n,
                        state={"RUNNING": "running"}.get(r.state, "held" if r.held else "queued"), scheduler=sched,
                        walltime_s=r.walltime_s or horizon, rate=r.rate, min_charge_h=min_h, deadline=None,
                        hand=token, unheld=1, ignore_owner=True, reason="appeared while your approved command ran")
        words.append(f"{job_id} (scheduler job {n}, at most {_gu(worst)} {UNIT})")
    gave_up = [x for x in fresh if _base(x) in tried and _base(x) not in reqs]
    pending = [x for x in fresh if _base(x) not in tried]
    if words:
        info = envelope_info(app, led.envelope(e["id"]) or e)
        over = info.remaining < 0
        if over:
            led.update_envelope(e["id"], state="exhausted")
        res.notes.append(
            f"Adopted into envelope {e['id']} — jobs that appeared while your approved command ran: "
            f"{'; '.join(words)}. No envelope deadline is applied to them (they run to their own walltime); if one is "
            "not from that command, pass exclude_jobs=[its scheduler id] with the user's yes and it is not counted. "
            "hpc-bridge never cancels them on its own: stop_endpoint asks about them by name, and cancel('job-…', "
            "'abort') ends one."
            + (f" The approved submit put envelope {e['id']} over its cap by {_gu(-info.remaining)} {UNIT}: it admits "
               "nothing more until the user raises the cap or jobs end." if over else ""))
    if gave_up:
        res.notes.append(f"Job(s) {', '.join(gave_up)} appeared while the approved command ran but could not be read "
                         "(ended and purged?): the meter's sweep counts them if the scheduler still lists them.")
    if pending:
        res.notes.append(f"{len(pending)} more job(s) that appeared while it ran are adopted by the next call.")
        res.pending = pending
    elif adopt:
        led.update_hand(token, state="closed")
        res.closed = True
    res.jobs = [job_info(j) for j in led.jobs(envelope_id=e["id"]) if j.get("hand") == token]
    return res


async def _late_after(app: AppCtx, h: dict[str, Any]) -> None:
    """The call's outcome says the command ended, and its own after-snapshot is missing (an `exec` replaced its shell):
    taken now, the moment its outcome came back."""
    hd = batch.quoted_dir(_hand_dir(h))
    try:
        await _login(app, f"{meter._bound()}\n{_snap_fn(scheduler_of(app))}__hpcb_snap {hd}/after; "
                          f"date +%s > {hd}/ended 2>/dev/null; echo HPCB_LATE_AFTER")
    except Exception:  # noqa: BLE001 - the next call reads the hand again
        pass


async def finish_hand(app: AppCtx, h: dict[str, Any], out: ShellOutcome) -> tuple[ShellOutcome, bool]:
    """The gated command's outcome, reconciled whatever it was: the jobs that appeared while it ran, adopted — or, for
    a command still running, adopted by the next call once it ends."""
    final = out.phase == "complete"
    res = await reconcile_hand(app, h, final=final)
    if "@needs_after" in res.notes:
        await _late_after(app, h)
        res = await reconcile_hand(app, h, final=False)
    res.notes = [n for n in res.notes if n != "@needs_after"]
    if res.running and out.phase == "failed":   # the dispatch failed: whether it ran at all is not known
        drop_hand(app, h, "its dispatch failed")
        res.running = False
        res.notes.append("The command's dispatch failed, so whether it submitted anything is not known: any job it "
                         "did submit is counted by the meter's sweep (as one hpc-bridge did not start).")
    out.jobs = res.jobs
    e = _ledger(app).envelope(str(h["envelope_id"]))
    if e is not None:
        out.envelope = envelope_info(app, e)
    if res.notes:
        out.notice = (f"{out.notice} " if out.notice else "") + " ".join(res.notes)
    if res.running:
        out.notice = ((f"{out.notice} " if out.notice else "") + "The jobs it submits are adopted when it ends — by "
                      "the next status, submit_job or stop_endpoint call.")
    return out, res.found or res.running


RECONCILE_BUDGET_S = 60.0   # what one status / submit_job / stop spends on open hand submits, at most (review r2)


async def hand_results(app: AppCtx, *, budget_s: float = RECONCILE_BUDGET_S, adopt: bool = True) -> list[
        tuple[dict[str, Any], HandResult]]:
    """Every hand submit still open on this facility — a command that outlived its sync-wait, a server that died
    before it read the jobs — reconciled now (`adopt`; else only read: a stop that asks changes nothing), within
    `budget_s`; what is left waits for the next call."""
    if not _can_reach(app):
        return []
    try:
        hands = _ledger(app).hands(facility=facility_key(app))
    except LedgerError:
        return []
    out: list[tuple[dict[str, Any], HandResult]] = []
    started = time.monotonic()
    for h in hands:
        if time.monotonic() - started > budget_s:
            res = HandResult(running=True)
            res.notes.append(f"{len(hands)} approved hand submit(s) are open; the rest are read by the next call.")
            out.append((h, res))
            break
        try:
            res = await reconcile_hand(app, h, final=False, adopt=adopt)
        except Exception as exc:  # noqa: BLE001 - the next call tries again
            res = HandResult(running=True, notes=[f"An approved hand submit could not be read just now "
                                                  f"({type(exc).__name__})."])
        res.notes = [n for n in res.notes if n != "@needs_after"]
        out.append((h, res))
    return out


async def reconcile_pending(app: AppCtx, *, budget_s: float = RECONCILE_BUDGET_S) -> list[str]:
    """`hand_results`, adopting: what it said."""
    return [n for _h, res in await hand_results(app, budget_s=budget_s) for n in res.notes]


async def _adopt_existing(app: AppCtx, native: str, *, req: EnvelopeRequest, confirm_spend: bool) -> JobStatus:
    """submit_job(adopt=<id>): a job already submitted by hand (before the envelope, or out of the gate's sight) is
    brought under this session's envelope — ledgered, reserved at its full request (never below the queue's minimum
    charge), metered, ended by a stop's cancel. It is never cancelled for not fitting: it already runs; the user is
    asked."""
    e, _orphan = find_envelope(app, None)
    if e is None:
        envs = [x for x in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
                if x.get("approved_by") == app.session_token]
        e = envs[0] if envs else None
    if e is None:
        return _refused("adopt needs an open envelope of this session: submit_job a job first (its envelope)",
                        "envelope_needed")
    reqs = await _read_requests(app, [native])
    r = reqs.get(native)
    if r is None:
        return _refused(f"job {native}: the scheduler did not describe it (gone, or not the user's)")
    worst, min_h = _worst(app, r)
    if not confirm_spend:
        return JobStatus(phase="needs_confirmation", reason="adopt", envelope=envelope_info(app, e),
                         notice=(f"Job {native} would be counted against envelope {e['id']} at its full request "
                                 f"(at most {_gu(worst)} {UNIT}). Ask the user; on their yes call submit_job(adopt=…, "
                                 "confirm_spend=True)."))
    await tick(app, envelope_ids=[e["id"]], also_known={_base(native)})
    job_id = _new_id("job")
    try:
        _ledger(app).reserve_job(e["id"], job_id, session=app.session_token, worst_case=worst, tasks=r.tasks,
                                 nodes=r.nodes, account=r.account, partition=r.partition, source="adopt",
                                 native_id=native, state="running" if r.state == "RUNNING" else "queued",
                                 scheduler=scheduler_of(app), walltime_s=r.walltime_s, rate=r.rate,
                                 min_charge_h=min_h, deadline=float(e["deadline"]))
    except EnvelopeError as exc:
        return JobStatus(phase="needs_confirmation", reason="envelope_exhausted", envelope=envelope_info(app, e),
                         notice=f"Job {native} does not fit envelope {e['id']}: {exc}. It runs on, uncounted by the "
                                "envelope's admission; ask the user (raise the cap, or cancel it).")
    row = _ledger(app).job(job_id) or {}
    return JobStatus(phase="queued", job=job_info(row), envelope=envelope_info(app, e),
                     notice=f"job {native} adopted as {job_id} into envelope {e['id']} (at most {_gu(worst)} {UNIT}).")


# ---- the compute block, admitted against the envelope ----------------------------------------------------------------

async def block_admission(app: AppCtx, shape: str, *, partition: str | None, account: str | None = None,
                          sizing: dict[str, Any] | None = None) -> EndpointStatus | None:
    """A compute block confirmed while this session's envelope is open is admitted against it: its worst case — the
    size THIS call confirms (its nodes, cores, partition, account: review B1), at the partition's billing, for its
    walltime and never below the queue's minimum charge — is reserved, or the confirmation is answered
    needs_confirmation naming the envelope. A confirmed block resized by a later confirmation is admitted again. None:
    no envelope here (the 0.1.18 floor alone), or it fits."""
    from . import notices, warmth

    if shape != "compute" or not _has_login_shape(app):
        return None
    rt = app.shapes.get(shape)
    named = partition is not None or account is not None or bool(sizing)
    if rt is not None and rt.spend_confirmed and not named:
        return None
    uec = dict(rt.user_endpoint_config) if rt is not None else warmth._seed_uec(app, shape)
    if not uec.get("compute"):
        return None
    if partition:
        uec["partition"] = partition
    if account:
        uec["account"] = account
    for k, v in (sizing or {}).items():
        if v is None:
            uec.pop(k, None)
        else:
            uec[k] = v
    try:
        envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
                if e.get("approved_by") == app.session_token and _account_ok(e, uec.get("account"))]
    except LedgerError:
        return None
    if not envs:
        return None
    e = envs[0]
    try:
        wall = float(_parse_hhmmss(uec.get("walltime"))) if uec.get("walltime") else 0.0
    except (TypeError, ValueError):
        wall = 0.0
    size, sh = notices._block_size(app, uec)
    got = resources.charge(size, sh, 3600.0)
    rate = got[0] if got is not None else None
    if rate is None:
        billed, _why = resources.billed_cores(size, sh)
        rate = float(billed * size.nodes) if billed else None
    min_h = sh.min_charge_h if sh is not None else None
    if rate is None or not wall:
        return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
                              notice=(f"Envelope {e['id']} is open, and this block's cost cannot be bounded (its "
                                      "partition's node size or the walltime is not known), so it cannot be admitted "
                                      "against the envelope: name cores=/nodes= and a walltime. Nothing was started."))
    worst = rate * max(wall / 3600, min_h or 0.0)
    await tick(app, envelope_ids=[e["id"]])
    try:
        _ledger(app).reserve_job(e["id"], _new_id("job"), session=app.session_token, worst_case=worst,
                                 nodes=size.nodes, account=uec.get("account"), partition=uec.get("partition"),
                                 source="pool_block", walltime_s=wall, rate=rate, min_charge_h=min_h,
                                 deadline=float(e["deadline"]))
    except EnvelopeError as exc:
        info = envelope_info(app, _ledger(app).envelope(e["id"]) or e)
        return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
                              notice=(f"This block does not fit envelope {e['id']}, which binds everything hpc-bridge "
                                      f"starts: it needs up to {_gu(worst)} {UNIT} and {_gd(info.remaining)} of "
                                      f"{_g(info.cap)} is left ({exc}). Ask the user to raise the envelope's cap or to "
                                      "wait for its jobs to end. Nothing was started."))
    return None


# ---- the stop --------------------------------------------------------------------------------------------------------

def _facility_of(led: Ledger, j: dict[str, Any], cache: dict[str, str | None]) -> str | None:
    eid = str(j["envelope_id"])
    if eid not in cache:
        cache[eid] = (led.envelope(eid) or {}).get("facility")
    return cache[eid]


async def billing_jobs(app: AppCtx, *, reconcile: bool = True) -> list[dict[str, Any]]:
    """Every Jobs-mode job of ours that can still bill — any session's, a dead server's included, an adopted one
    too — on ANY facility (review B6): this facility's read fresh by the meter when a login channel is bound (its open
    hand submits adopted first, with `reconcile`); every other's, the ledger's last view, bounded by its deadline and
    walltime only where the scheduler enforces the deadline. Compute blocks are not here: the stop releases those."""
    led = _ledger(app)
    fac = facility_key(app)
    here = app.state.endpoint_id is not None and _can_reach(app)
    if here:
        if reconcile:
            await reconcile_pending(app, budget_s=30.0)
        ids = [e["id"] for e in led.envelopes(facility=fac) if any(j.get("state") in JOB_LIVE
                                                                   for j in led.jobs(envelope_id=e["id"]))]
        if ids:
            await tick(app, envelope_ids=ids)
    now = _now()
    cache: dict[str, str | None] = {}
    out = []
    for j in led.jobs(states=JOB_LIVE):
        if j.get("source") == "pool_block":
            continue
        if here and _facility_of(led, j, cache) == fac:
            out.append(j)
            continue
        # aged out by deadline + walltime only where the scheduler enforces the deadline (Slurm): a PBS job carries
        # none, and stays until a read shows it ended (review r2)
        dl = j.get("deadline")
        if (dl is None or j.get("scheduler") == "pbs"
                or now < float(dl) + float(j.get("walltime_s") or 0.0)):
            out.append(j)
    return out


def _jobs_words(app: AppCtx, rows: list[dict[str, Any]]) -> str:
    led = _ledger(app)
    fac = facility_key(app)
    cache: dict[str, str | None] = {}
    out = []
    for j in rows:
        info = job_info(j)
        where = _facility_of(led, j, cache)
        out.append(f"{j.get('native_id') or 'not yet submitted'} ({j['id']}, {info.state}"
                   + (f", {info.reason}" if info.reason else "") + f", up to {_gu(info.committed)} {UNIT} more, "
                   f"envelope {j['envelope_id']}"
                   + (f", on {where}: connect_facility to it to cancel it" if where and where != fac else "")
                   + (", appeared while your approved command ran, no envelope deadline" if j.get("unheld") else "")
                   + ")")
    return "; ".join(out)


def hand_words(results: list[tuple[dict[str, Any], HandResult]]) -> str:
    """What open hand submits leave unaccounted: jobs that appeared and are not adopted yet, commands still running."""
    parts = []
    for _h, res in results:
        if res.pending:
            parts.append("job(s) " + ", ".join(res.pending) + " appeared while your approved command ran (not yet "
                         "adopted: the next call that proceeds adopts them)")
        if res.running:
            parts.append("an approved hand-submit command is still running (the jobs it submits are adopted when it "
                         "ends)")
    return "; ".join(parts)


def stop_question(app: AppCtx, rows: list[dict[str, Any]],
                  hands: list[tuple[dict[str, Any], HandResult]] | None = None) -> EndpointStatus:
    unread = hand_words(hands or [])
    what = (f"{len(rows)} Jobs-mode job(s) of ours can still bill — {_jobs_words(app, rows)}" if rows else "")
    what += ("; " if what and unread else "") + unread
    return EndpointStatus(
        status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
        jobs=[job_info(j) for j in rows],
        notice=(f"stop_endpoint needs the user's choice first: {what}. Ask the user, then call "
                "stop_endpoint(jobs=\"cancel\") to cancel them and close their envelope (down only once nothing of "
                "ours can bill), or stop_endpoint(jobs=\"keep\") to release only the endpoint's blocks and leave them "
                "running under their envelope. Nothing was changed: nothing cancelled, released or adopted."))


async def cancel_for_stop(app: AppCtx, rows: list[dict[str, Any]]) -> str:
    """jobs="cancel" — the user's choice: this facility's jobs of ours are cancelled, an adopted one too (a submit of
    unknown fate by its name; an admission in flight asked to give up) in ONE scheduler call; another facility's cannot
    be reached from here and stays listed, so the stop is not down while it bills."""
    led = _ledger(app)
    fac = facility_key(app)
    cache: dict[str, str | None] = {}
    mine = [j for j in rows if _facility_of(led, j, cache) == fac]
    for eid in {j["envelope_id"] for j in mine}:
        e = led.envelope(eid)
        if e is not None and e["state"] not in ("closed",):
            led.update_envelope(eid, state="draining", close_reason="stop_endpoint(jobs='cancel')")
    targets = mine
    natives = [str(j["native_id"]) for j in targets if j.get("native_id")]
    unsent = [j for j in targets if not j.get("native_id")]
    for j in unsent:
        led.update_job(j["id"], cancel_requested=1)
    names = [j["id"] for j in unsent if j.get("state") == "submit_unknown"
             or (j.get("state") == "reserved" and j["id"] not in app.jobs_admitting)]
    if natives or names:
        await _scancel(app, natives, names=names)
    # the job ids a cancel went to: what became of each is read after it (`after_stop`), never from scancel's rc
    return " ".join(j["id"] for j in targets if j.get("native_id"))


def keep_for_stop(app: AppCtx, rows: list[dict[str, Any]]) -> None:
    """jobs="keep": the jobs run on; nothing more is admitted — an admission in flight gives up."""
    led = _ledger(app)
    for j in rows:
        if not j.get("native_id") and j.get("state") == "reserved":
            led.update_job(j["id"], cancel_requested=1)
    for eid in {j["envelope_id"] for j in rows}:
        e = led.envelope(eid)
        if e is not None and e["state"] in ENVELOPE_ADMITTING:
            led.update_envelope(eid, state="draining", close_reason="stop_endpoint(jobs='keep')")


def close_admission(app: AppCtx) -> None:
    """After any stop nothing more is admitted: this facility's admitting envelopes close (draining while a job of
    theirs can still bill); a confirmed block's reservation ends with the block."""
    led = _ledger(app)
    try:
        for e in led.envelopes(facility=facility_key(app)):
            if e["state"] not in ENVELOPE_ADMITTING and e["state"] != "draining":
                continue
            jobs = led.jobs(envelope_id=e["id"])
            for j in jobs:
                if j.get("source") == "pool_block" and j.get("state") in JOB_LIVE:
                    led.update_job(j["id"], state="done", charged=float(j.get("worst_case") or 0.0), committed=0.0,
                                   meter_source="reservation")
            live = [j for j in jobs if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
            if not live:
                led.update_envelope(e["id"], state="closed", closed_at=_now(), close_reason="stop_endpoint")
            elif e["state"] in ENVELOPE_ADMITTING:
                led.update_envelope(e["id"], state="draining", close_reason="stop_endpoint")
    except LedgerError:
        pass


async def after_stop(app: AppCtx, st: EndpointStatus, choice: str | None, cancelled: str,
                     notes: list[str] | None = None) -> EndpointStatus:
    """The stop's answer, honest about our jobs: `cancel` — draining while any of ours can still bill or an approved
    hand submit is unaccounted for (a command still running, jobs not yet adopted); `keep` — draining, never down,
    naming them and how to end them. `notes`: what the adoptions said — never thrown away."""
    still = await billing_jobs(app)   # always read again: a job admitted while the stop ran is one that can bill
    close_admission(app)
    led = _ledger(app)
    if notes:
        st.notice = f"{st.notice or ''} {' '.join(notes)}".strip()
    try:
        open_hands = led.hands(facility=facility_key(app))
    except LedgerError:
        open_hands = []
    if open_hands:
        st.notice = (f"{st.notice or ''} Not every job of an approved hand submit is accounted for yet: "
                     f"{len(open_hands)} approved command(s) still running or not yet read — the jobs they submit are "
                     "adopted when they end; call stop_endpoint(jobs=\"cancel\") again after that.").strip()
        if st.status == "down":
            st.status = "draining"
            st.spend_stopped = None
    sent = [r for r in (led.job(i) for i in cancelled.split()) if r is not None]
    done = [str(r["native_id"]) for r in sent if r.get("state") == "cancelled"]
    ended = [f"{r['native_id']} ({r.get('sched_state') or r.get('state')})" for r in sent
             if r.get("state") in ("done", "failed")]
    prefix = ((f"Jobs-mode job(s) cancelled: {' '.join(done)}. " if done else "")
              + (f"Already ended, not cancelled: {', '.join(ended)}. " if ended else ""))
    if choice == "keep" and still:
        envs = sorted({j["envelope_id"] for j in still})
        st.jobs = [job_info(j) for j in still]
        st.status = "draining" if st.status in ("down", "draining") else st.status
        st.spend_stopped = False
        st.notice = (f"{st.notice or ''} Kept running, as asked: {_jobs_words(app, still)} — they bill until they end "
                     "or their envelope's deadline; end them with " + ", ".join(f"cancel('{j['id']}', 'abort')"
                                                                                for j in still)
                     + " or " + ", ".join(f"cancel('{x}', 'abort')" for x in envs) + ". Their envelope admits nothing "
                     "more (status reads it).").strip()
        return st
    if still:
        st.jobs = [job_info(j) for j in still]
        if st.status == "down":
            st.status = "draining"
            st.spend_stopped = None
        st.notice = (f"{prefix}{st.notice or ''} Spend is NOT confirmed stopped: these can still bill — "
                     f"{_jobs_words(app, still)} — call stop_endpoint(jobs=\"cancel\") again until it says down."
                     ).strip()
        return st
    if prefix:
        st.notice = f"{prefix}{st.notice or ''}".strip()
    return st


def stop_without_channel(app: AppCtx, rows: list[dict[str, Any]], choice: str) -> EndpointStatus:
    """A stop with no facility bound (nothing to release, no login channel) while ledger jobs can still bill."""
    if choice == "keep":
        keep_for_stop(app, rows)
    return EndpointStatus(
        status="draining", block_state="cold", endpoint_id=None, jobs=[job_info(j) for j in rows], spend_stopped=False,
        notice=(f"No endpoint is bound here, so nothing could be {'cancelled' if choice == 'cancel' else 'released'}: "
                f"{_jobs_words(app, rows)} can still bill. "
                + ("connect_facility to their facility, then stop_endpoint(jobs=\"cancel\") again."
                   if choice == "cancel" else "They keep running under their envelope and deadline; end them with "
                   + ", ".join(f"cancel('{j['id']}', 'abort')" for j in rows) + " once connected.")))


def unconfirmed(st: EndpointStatus, what: str) -> EndpointStatus:
    """The stop's deadline passed (review r3): never `down` — draining, saying what it could not confirm."""
    if st.status == "down":
        st.status = "draining"
    st.spend_stopped = None
    st.notice = (f"{st.notice or ''} The stop could not confirm, within its time budget, that nothing of ours bills: "
                 f"{what}. Spend is NOT confirmed stopped — call stop_endpoint again (status('env-…') reads the "
                 "envelope).").strip()
    return st


def ledger_unreadable(st: EndpointStatus, exc: BaseException) -> EndpointStatus:
    """The stop could not read the Jobs-mode ledger (review B8): the blocks were released as always, but jobs of ours
    may still bill — never `down`."""
    if st.status == "down":
        st.status = "draining"
        st.spend_stopped = None
    st.notice = (f"{st.notice or ''} The Jobs-mode ledger could not be read ({type(exc).__name__}: {exc}), so jobs "
                 "hpc-bridge submitted may still bill: spend is NOT confirmed stopped. Check the scheduler (squeue -u "
                 "$USER / qstat -u $USER) and end any job of ours with scancel / qdel.").strip()
    return st
