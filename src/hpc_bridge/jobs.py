"""Batch phase 2a: the budget envelope, Jobs mode and D2's gate and adopt (vault `Planned/Batch and swarm execution.md`
§2 "Promotion", §3, §12 D2).

- **The envelope** (`EnvelopeInfo`, the ledger's `envelope` table) is approved ONCE — submit_job's needs_confirmation
  carries the terms, the user's yes comes back as confirm_spend=True — and the server enforces it for everything
  hpc-bridge starts: every job reserves its worst case before it is submitted (`Ledger.reserve_job`, one BEGIN
  IMMEDIATE), and a compute block confirmed while it is open is admitted against it (`block_admission`). It belongs to
  the session that approved it (`AppCtx.session_token`): another session — a restarted server too — asks again, naming
  what remains. Its deadline is required (24 h by default, at most 168) and every Slurm job carries it (`--deadline`,
  in UTC); on PBS it is soft. `HPC_BRIDGE_ENVELOPE_MAX` bounds the SUM of the open envelopes' caps per facility and
  account.
- **Jobs mode** (`submit_job`): SSH facilities only — a facility MEP has no free status or cancel channel. The command
  is promoted into a frozen `<scratch>/jobs/<job id>/job.sh`, written once (noclobber), its sha256 on the ledger: the
  header from the terms (`--deadline` included), the session's cwd and env replayed, the command (base64: no line of it
  can become a directive), its exit code written beside it. `{index}` — an array task's index — goes through phase 1's
  `substitute` (a value is data, never code).
- **The meter** (hpc_bridge.meter) reads one sacct (or qstat) per tick and never understates.
- **D2 (c)**: a recognised hand sbatch / qsub while this session's envelope is open asks first (needs_confirmation);
  on the yes it runs HELD (`HOLD_SHIM`), is adopted (scontrol / qstat read its request), reserved, given the envelope's
  deadline and released — or cancelled while still held when it does not fit.
- **A stop** (server._stop_endpoint) with jobs of ours that can still bill asks `jobs="cancel"|"keep"` (`stop_question`,
  `cancel_for_stop`, `after_stop`); nothing is admitted after a stop either way.
"""
from __future__ import annotations

import base64
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
    EnvelopeClosed,
    EnvelopeError,
    EnvelopeExhausted,
    EnvelopeLimit,
    EnvelopeNotOwned,
    Ledger,
    LedgerError,
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
_SLURM_RECEIPT = re.compile(r"Submitted batch job (\d+)|^(\d+)(?:;\S+)?\s*$", re.M)
_PBS_RECEIPT = re.compile(r"^\s*(\d+(?:\[\])?\.[A-Za-z0-9][\w.-]*)\s*$", re.M)
EXHAUSTED_STATE_HOLD_S = 0.0

BINDS = ("The envelope binds what hpc-bridge starts: submit_job's jobs, a hand sbatch/qsub it gated and adopted, the "
         "compute blocks confirmed while it is open. It cannot bind a shell or code it cannot see — an sbatch inside a "
         "script, Python's subprocess — which the meter counts (and names) once its sweep of the scheduler finds them; "
         "nor a stolen token: on an SSH facility any task can read the Compute refresh token in "
         "~/.globus_compute/storage.db and submit to any endpoint the identity reaches. A hard cap needs the site: an "
         "sbank suballocation, a Slurm GrpTRESMins, or a small dedicated account.")

# D2's hold: a hand submit runs with these in its shell (and every bash it starts), so the scheduler takes its jobs HELD
# until hpc-bridge has read them, reserved them against the envelope and given them its deadline.
HOLD_SHIM = ('sbatch() { command sbatch --hold "$@"; }\nqsub() { command qsub -h "$@"; }\n'
             "export -f sbatch qsub >/dev/null 2>&1\n")


class JobRefusal(ValueError):
    """submit_job's arguments cannot make a job (the reason is the text)."""


def _now() -> float:
    return time.time()


def _iso(t: float | None) -> str | None:
    return None if t is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def _g(x: float) -> str:
    return f"{x:.2f}".rstrip("0").rstrip(".") if abs(x - round(x)) > 1e-9 else f"{round(x):d}"


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


# ---- the meter's tick ------------------------------------------------------------------------------------------------

@dataclass
class Tick:
    ok: bool
    readings: dict[str, meter.Reading] = field(default_factory=dict)   # job id -> reading
    error: str = ""


def _state_of(r: meter.Reading, row: dict[str, Any]) -> str:
    if r.source in ("missing", "squeue"):   # not read: the ledger's state stands
        return str(row.get("state"))
    if r.state in ("RUNNING",):
        return "running"
    if r.state == "PENDING":
        return "held" if row.get("state") == "held" and "held" in (r.reason or "").lower() else "queued"
    if r.state == "COMPLETED":
        return "done" if (r.exit_code or 0) == 0 else "failed"
    if r.state == "CANCELLED":
        return "cancelled"
    if r.state in meter._ENDED:
        return "failed"
    return str(row.get("state"))


async def tick(app: AppCtx, *, envelope_ids: list[str] | None = None) -> Tick:
    """One meter reading of this facility's envelopes (or `envelope_ids`): ONE login-shape call, then every ledger job
    and every envelope's totals updated. Without a login channel nothing is read (the ledger's last view stands)."""
    led = _ledger(app)
    fac = facility_key(app)
    envs = [e for e in led.envelopes(facility=fac) if envelope_ids is None or e["id"] in envelope_ids]
    if not envs or not _can_reach(app):
        return Tick(False, error="no login channel")
    all_envs = led.envelopes(facility=fac)
    jobs = led.jobs(envelope_ids=[e["id"] for e in all_envs])
    sched = scheduler_of(app)
    since = min([float(e["created_at"]) for e in envs] + [float(j["created_at"]) for j in jobs
                                                         if j["envelope_id"] in {e["id"] for e in envs}]) - 120
    cmd = meter.pbs_cmd() if sched == "pbs" else meter.slurm_cmd(time.strftime("%Y-%m-%dT%H:%M:%S",
                                                                                time.gmtime(since)))
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001 - a cold or failed channel reads nothing; the ledger's view stands
        return Tick(False, error=f"{type(exc).__name__}: {exc}")
    raw = _text(out)
    if meter.END_MARK not in raw:
        return Tick(False, error=(getattr(out, "notice", None) or raw.strip()[-200:]
                                  or "the login shape did not answer"))
    view = meter.parse_pbs(raw) if sched == "pbs" else meter.parse_slurm(raw)
    now = _now()
    t = Tick(True)
    # a job whose submit's fate is unknown: the sweep finds it by its name
    names = {f"hpcb-{j['id']}": j for j in jobs if not j.get("native_id")}
    for base, rows in view.rows.items():
        j = names.get(rows[0].name)
        if j is not None:
            led.update_job(j["id"], native_id=base, state="queued")
            j["native_id"] = base
    known = {str(j["native_id"]).split(".")[0].split("[")[0] for j in jobs if j.get("native_id")}
    by_env: dict[str, list[dict[str, Any]]] = {}
    for j in jobs:
        by_env.setdefault(j["envelope_id"], []).append(j)
        if j["envelope_id"] not in {e["id"] for e in envs} or j.get("state") == "refused":
            continue
        if j.get("source") == "pool_block" and not j.get("native_id"):
            continue
        r = meter.job_reading(j, view)
        t.readings[j["id"]] = r
        state = _state_of(r, j)
        upd = {"charged": r.charged, "committed": r.committed, "meter_source": r.source, "sched_state": r.state,
               "metered_at": now, "array_counts": json.dumps(r.counts), "state": state}
        if r.exit_code is not None:
            upd["exit_code"] = r.exit_code
        upd["reason"] = r.reason
        if state not in JOB_LIVE and not j.get("ended_at"):
            upd["ended_at"] = now
        led.update_job(j["id"], **upd)
        j.update(upd)
    for e in envs:
        pools = [j for j in by_env.get(e["id"], []) if j.get("source") == "pool_block"]
        un = meter.unledgered(view, known, set(json.loads(e.get("excluded") or "[]")),
                              account=e.get("account") or None)
        if pools:   # a confirmed block of ours is counted at its reservation, not again as its parsl pilot
            un = [u for u in un if not str(u.get("name") or "").startswith("parsl")]
        hold_un = sum(float(u["charged"]) + float(u["committed"]) for u in un)
        mine = [j for j in by_env.get(e["id"], []) if j.get("state") != "refused"]
        spent = sum(float(j.get("charged") or 0) for j in mine) + sum(float(u["charged"]) for u in un)
        committed = sum(job_hold(j) - float(j.get("charged") or 0) for j in mine) + sum(float(u["committed"])
                                                                                       for u in un)
        upd2: dict[str, Any] = {"spent": spent, "committed": committed, "unledgered": json.dumps(un),
                                "unledgered_hold": hold_un, "metered_at": now}
        live = [j for j in mine if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
        if float(e["deadline"]) <= now and e["state"] in ENVELOPE_ADMITTING:   # its deadline: admit no more
            upd2["state"] = "draining" if live else "closed"
            upd2["close_reason"] = "its deadline passed"
            if e.get("at_deadline") == "abort" and live:
                await _scancel(app, [str(j["native_id"]) for j in live if j.get("native_id")])
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
    return JobInfo(job_id=j["id"], native_id=j.get("native_id"), source=j.get("source") or "submit_job",
                   state=state, exit_code=j.get("exit_code"), reason=j.get("reason"), charged=round(charged, 4),
                   committed=round(max(0.0, held - charged), 4), worst_case=round(float(j.get("worst_case") or 0), 4),
                   unit=UNIT, meter_source=j.get("meter_source") or "none", tasks=int(j.get("tasks") or 1),
                   throttle=j.get("throttle"), array_counts=json.loads(j.get("array_counts") or "{}"),
                   partition=j.get("partition"), account=j.get("account"), walltime=_hms(j.get("walltime_s")),
                   deadline=_iso(j.get("deadline")), script_path=j.get("script_path"),
                   script_hash=j.get("script_hash"), session_id=j.get("session_id"))


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
        remaining=round(cap - held, 4), max_jobs=e.get("max_jobs"),
        jobs=[job_info(j) for j in jobs if j.get("source") != "pool_block"],
        tasks=sum(int(j.get("tasks") or 1) for j in jobs),
        max_nodes_per_block=e.get("max_nodes_per_block"), accounts=json.loads(e.get("accounts") or "[]"),
        partitions=json.loads(e.get("partitions") or "[]"), deadline=_iso(float(e["deadline"])) or "",
        at_deadline=e.get("at_deadline") or "drain", site_backstop=e.get("site_backstop") or "none",
        owned=e.get("approved_by") == app.session_token, binds=BINDS, job_ids=[j["id"] for j in jobs],
        unledgered=[UnledgeredJob(**u) for u in json.loads(e.get("unledgered") or "[]")],
        excluded=json.loads(e.get("excluded") or "[]"))


def _bills_note(info: EnvelopeInfo) -> str:
    live = [j for j in info.jobs if j.committed > 0 or j.state in ("RUNNING", "PENDING", "HELD", "RESERVED")]
    parts = [f"{j.job_id} ({j.native_id or 'not submitted'}, {j.state}, {_g(j.charged)} charged, up to "
             f"{_g(j.committed)} more)" for j in live]
    parts += [f"job {u.native_id} ({u.name or '?'}, {u.state}: not started by hpc-bridge, counted {_g(u.charged)} + "
              f"{_g(u.committed)})" for u in info.unledgered]
    return "; ".join(parts) or "nothing"


def _unledgered_note(info: EnvelopeInfo) -> str:
    if not info.unledgered:
        return ""
    ids = ", ".join(f"{u.native_id} ({u.name or '?'})" for u in info.unledgered)
    return (f" It counts job(s) hpc-bridge did not start: {ids} — if the user says one is not theirs to count here, "
            "pass exclude_jobs=[…] with their yes (confirm_spend=True) and it is never counted again.")


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
                per_task=per_task, worst_case=round(tasks * per_task, 6), basis=basis, min_charge_h=min_h,
                shape=shape)


def terms_of(app: AppCtx, spec: Spec, deadline: float, backstop: str | None,
             script_path: str | None = None) -> JobTerms:
    return JobTerms(facility=facility_key(app), scheduler=scheduler_of(app), account=spec.account,
                    partition=spec.partition, nodes=spec.nodes, whole_node=spec.whole_node, cores_per_node=spec.cores,
                    memory_gb=spec.memory_gb, gpus_per_node=spec.gpus, walltime=_hms(spec.walltime_s) or "",
                    tasks=spec.tasks, throttle=spec.throttle, rate=spec.rate, unit=UNIT,
                    worst_case=round(spec.worst_case, 4), basis=spec.basis, min_charge_h=spec.min_charge_h,
                    deadline=_iso(deadline) or "", script_path=script_path, binds=BINDS,
                    site_backstop=backstop or "none")


# ---- the frozen job.sh -----------------------------------------------------------------------------------------------

def job_dir(app: AppCtx, job_id: str) -> str:
    return f"{app.scratch_root.rstrip('/')}/jobs/{job_id}"


def job_script(app: AppCtx, *, job_id: str, envelope_id: str, spec: Spec, deadline: float) -> str:
    """The job's frozen script: the header from the terms (every value checked: none can become code), the session's
    cwd and env replayed (read-only), the command (base64), its exit code written beside the script."""
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
                  f"#SBATCH --time={_hms(spec.walltime_s)}",
                  f"#SBATCH --deadline={time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(deadline))}"]
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
    sd = session_shell.Session(spec.session_id, app.scratch_root).quoted_state_dir()
    b64 = shlex.quote(base64.b64encode(spec.command.encode()).decode())
    lines += [
        f"# hpc-bridge {job_id} (envelope {envelope_id}): frozen when it was written; its sha256 is on the ledger",
        f"__hpcb_dir={batch.quoted_dir(job_dir(app, job_id))}",
        '__hpcb_task="${SLURM_ARRAY_TASK_ID:-${PBS_ARRAY_INDEX:-}}"',
    ]
    if spec.array:
        lines.append(f'{batch.FIELD_VAR}index="$__hpcb_task"; export {batch.FIELD_VAR}index')
    lines += [
        f"__hpcb_sd={sd}",
        'if [ -f "$__hpcb_sd/.cwd" ]; then cd "$(cat "$__hpcb_sd/.cwd")" 2>/dev/null || cd "$__hpcb_dir"; '
        'else cd "$__hpcb_dir"; fi',
        '[ -f "$__hpcb_sd/.env" ] && . "$__hpcb_sd/.env"',
        f'( eval "$(printf %s {b64} | base64 --decode)" )',
        "__hpcb_rc=$?",
        'echo "$__hpcb_rc" > "$__hpcb_dir/exit_code${__hpcb_task:+.$__hpcb_task}"',
        'exit "$__hpcb_rc"',
    ]
    return "\n".join(lines) + "\n"


async def write_job_script(app: AppCtx, job_id: str, text: str) -> tuple[str, str]:
    """Write `text` as the job's job.sh ONCE (noclobber: an existing one is never replaced). Returns (its path on the
    facility, its sha256 as the facility read it)."""
    if not JOB_ID.match(job_id):
        raise JobRefusal(f"invalid job id {job_id!r}")
    d = batch.quoted_dir(job_dir(app, job_id))
    b64 = shlex.quote(base64.b64encode(text.encode()).decode())
    cmd = (f"umask 077; d={d}; mkdir -p \"$d\" && cd \"$d\" "
           '|| { echo "HPCB_WRITE_FAILED cannot create $d"; exit 0; }; '
           'if [ -e job.sh ]; then echo "HPCB_JOB_EXISTS $(pwd)/job.sh"; exit 0; fi; '
           f'( set -C; printf %s {b64} | base64 --decode > job.sh ) 2>/dev/null '
           '|| { echo "HPCB_JOB_EXISTS $(pwd)/job.sh"; exit 0; }; '
           'h=$( (sha256sum job.sh 2>/dev/null || shasum -a 256 job.sh) | cut -c1-64); '
           'echo "HPCB_JOB_SH $(pwd)/job.sh $h"')
    out = await _login(app, cmd)
    raw = _text(out)
    if (m := re.search(r"HPCB_JOB_EXISTS (\S+)", raw)) is not None:
        raise JobRefusal(f"{m.group(1)} exists: a job's script is written once and never replaced")
    m = re.search(r"HPCB_JOB_SH (\S+) ([0-9a-f]{64})", raw)
    if m is None:
        why = re.search(r"HPCB_WRITE_FAILED (.*)", raw)
        raise JobRefusal(f"job.sh could not be written: {why.group(1) if why else raw.strip()[-200:] or 'no answer'}")
    want = hashlib.sha256(text.encode()).hexdigest()
    if m.group(2) != want:
        raise JobRefusal(f"job.sh on the facility does not hash as written ({m.group(2)[:12]} ≠ {want[:12]})")
    return m.group(1), want


async def _submit_script(app: AppCtx, job_id: str, path: str, spec: Spec) -> tuple[str | None, str]:
    """sbatch / qsub the frozen script (Slurm under TZ=UTC0: its --deadline is UTC). (native id or None, output)."""
    d = batch.quoted_dir(job_dir(app, job_id))
    q = shlex.quote(path)
    if scheduler_of(app) == "pbs":
        cmd = f"cd {d} && qsub -o {d}/pbs.out -j oe {q} 2>&1; echo \"HPCB_SUBMIT_RC=$?\""
    else:
        pattern = "slurm-%A_%a.out" if spec.array else "slurm-%j.out"
        cmd = f"cd {d} && TZ=UTC0 sbatch --parsable --output={d}/{pattern} {q} 2>&1; echo \"HPCB_SUBMIT_RC=$?\""
    out = await _login(app, cmd)
    raw = _text(out)
    rc = re.search(r"HPCB_SUBMIT_RC=(\d+)", raw)
    if rc is None:
        return None, raw
    if rc.group(1) != "0":
        return "", raw
    pat = _PBS_RECEIPT if scheduler_of(app) == "pbs" else _SLURM_RECEIPT
    m = pat.search(raw.split("HPCB_SUBMIT_RC=")[0])
    if m is None:
        return "", raw
    return next(g for g in m.groups() if g), raw


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


def _ceiling_room(app: AppCtx, account: str | None, *, except_id: str | None = None) -> tuple[float | None, float]:
    """(the ceiling, what is open under it on this facility and account, without `except_id`)."""
    top = ceiling()
    if top is None:
        return None, 0.0
    open_caps = sum(float(e["cap"]) for e in _ledger(app).envelopes(facility=facility_key(app),
                                                                     states=("open", "exhausted", "draining"))
                    if (e.get("account") or "") == (account or "") and e["id"] != except_id)
    return top, open_caps


def _ceiling_refusal(top: float, open_caps: float, cap: float) -> JobStatus:
    left = max(0.0, top - open_caps)
    return JobStatus(phase="refused", reason="envelope_ceiling",
                     notice=(f"Refused: the operator's ceiling HPC_BRIDGE_ENVELOPE_MAX is {_g(top)} {UNIT} for the SUM "
                             f"of open envelopes on this facility and account; {_g(open_caps)} is open already, so "
                             f"{_g(left)} is left under it and an envelope of {_g(cap)} does not fit. Ask the user for "
                             f"a cap of at most {_g(left)}, or close another envelope first. Nothing was submitted."))


def _deadline_hours(req: EnvelopeRequest) -> float:
    h = DEFAULT_DEADLINE_H if req.deadline_hours is None else float(req.deadline_hours)
    if not 0 < h <= MAX_DEADLINE_H:
        raise JobRefusal(f"the envelope's deadline must be within {int(MAX_DEADLINE_H)} hours and in the future "
                         f"(deadline_hours={req.deadline_hours!r}); the default is {int(DEFAULT_DEADLINE_H)}")
    return h


def _proposed(app: AppCtx, req: EnvelopeRequest, spec: Spec, deadline: float) -> EnvelopeInfo:
    cap = float(req.cap) if req.cap is not None else spec.worst_case
    return EnvelopeInfo(envelope_id="env-" + "0" * 12, state="proposed", facility=facility_key(app),
                        account=spec.account, unit=UNIT, cap=round(cap, 4), remaining=round(cap, 4),
                        max_jobs=req.max_jobs, max_nodes_per_block=req.max_nodes_per_block,
                        accounts=req.accounts or ([spec.account] if spec.account else []),
                        partitions=req.partitions or [], deadline=_iso(deadline) or "", at_deadline=req.at_deadline,
                        site_backstop=req.site_backstop or "none", owned=True, binds=BINDS)


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
    if (why := _limits_refusal(None, mine, spec)) is not None:
        return JobStatus(phase="needs_confirmation", reason="envelope_limit", envelope=envelope_info(app, mine),
                         terms=terms_of(app, spec, float(mine["deadline"]), mine.get("site_backstop")),
                         notice=f"{why}. Nothing was submitted.")
    if req.cap is not None and confirm_spend and float(req.cap) != float(mine["cap"]):
        top, open_caps = _ceiling_room(app, spec.account, except_id=mine["id"])
        if top is not None and open_caps + float(req.cap) > top + 1e-9:
            return _ceiling_refusal(top, open_caps, float(req.cap))
        led.update_envelope(mine["id"], cap=float(req.cap))
    if req.site_backstop and confirm_spend:
        led.update_envelope(mine["id"], site_backstop=req.site_backstop)
    return await _admit(app, led.envelope(mine["id"]) or mine, spec, confirm_spend=confirm_spend)


async def _open_and_admit(app: AppCtx, spec: Spec, req: EnvelopeRequest, hours: float, *, confirm_spend: bool,
                          exclude: list[str] | None) -> JobStatus:
    deadline = _now() + hours * 3600
    proposal = _proposed(app, req, spec, deadline)
    terms = terms_of(app, spec, deadline, req.site_backstop)
    if spec.walltime_s > hours * 3600:
        return _refused(f"the job's walltime ({_hms(spec.walltime_s)}) does not fit before the envelope's deadline "
                        f"({_g(hours)} h): it could never finish in it — ask for a longer deadline or a shorter job")
    if (why := _limits_refusal(req, None, spec)) is not None:
        return JobStatus(phase="refused" if confirm_spend else "needs_confirmation", reason="envelope_limit",
                         terms=terms, envelope=proposal, notice=f"{why}. Nothing was submitted.")
    if proposal.cap + 1e-9 < spec.worst_case:
        return JobStatus(phase="needs_confirmation", reason="envelope_exhausted", terms=terms, envelope=proposal,
                         notice=(f"The envelope asked for ({_g(proposal.cap)} {UNIT}) cannot hold this job: its worst "
                                 f"case is {_g(spec.worst_case)} ({spec.basis}). Ask the user to approve at least "
                                 f"{_g(spec.worst_case)}, or a smaller job — do not split the work to fit. Nothing was "
                                 "submitted."))
    top, open_caps = _ceiling_room(app, spec.account)
    if top is not None and open_caps + proposal.cap > top + 1e-9:
        return _ceiling_refusal(top, open_caps, proposal.cap)
    if not confirm_spend:
        backstop = (f"site backstop: {req.site_backstop}" if req.site_backstop else "site backstop: none")
        return JobStatus(phase="needs_confirmation", reason="envelope_needed", terms=terms, envelope=proposal,
                         notice=(f"This job needs the user's yes to a budget envelope first — nothing was submitted. "
                                 f"The job: {spec.basis} = at most {_g(spec.worst_case)} {UNIT} "
                                 f"({spec.partition or 'the default partition'}, account {spec.account or 'none'}). "
                                 f"The envelope: a cap of {_g(proposal.cap)} {UNIT}, deadline {proposal.deadline} "
                                 f"({_g(hours)} h; every job carries it), {backstop}. {BINDS} Ask the user; on their "
                                 "yes call submit_job again with the same arguments and confirm_spend=True. Within the "
                                 "envelope, later jobs that fit are admitted without asking."))
    led = _ledger(app)
    eid = _new_id("env")
    led.add_envelope(eid, facility=facility_key(app), account=spec.account, unit=UNIT, cap=proposal.cap,
                     deadline=deadline, approved_by=app.session_token, owner=batch.owner_of(app),
                     endpoint_id=app.state.endpoint_id, max_jobs=req.max_jobs,
                     max_nodes_per_block=req.max_nodes_per_block, accounts=proposal.accounts,
                     partitions=req.partitions, at_deadline=req.at_deadline, site_backstop=req.site_backstop)
    if exclude:
        led.exclude_jobs(eid, exclude)
    e = led.envelope(eid)
    assert e is not None
    return await _admit(app, e, spec, confirm_spend=True, opened=True)


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
                                 f"{_g(info.remaining)} of {_g(info.cap)} {UNIT} left (spent {_g(info.spent)}, "
                                 f"committed {_g(info.committed)}), deadline {info.deadline}; what still bills: "
                                 f"{_bills_note(info)}. This job needs {_g(spec.worst_case)} ({spec.basis}). On the "
                                 "user's yes call submit_job again with confirm_spend=True. Nothing was submitted."))
    cap = float(req.cap) if req.cap is not None else None
    if cap is not None:
        top, open_caps = _ceiling_room(app, spec.account, except_id=e["id"])
        if top is not None and open_caps + cap > top + 1e-9:
            return _ceiling_refusal(top, open_caps, cap)
    led.rebind_envelope(e["id"], session=app.session_token, owner=batch.owner_of(app), cap=cap)
    if exclude:
        led.exclude_jobs(e["id"], exclude)
    return await _admit(app, led.envelope(e["id"]) or e, spec, confirm_spend=True)


async def _admit(app: AppCtx, e: dict[str, Any], spec: Spec, *, confirm_spend: bool, opened: bool = False) -> JobStatus:
    """Reserve, write, submit — in that order (reserve-before-admit)."""
    led = _ledger(app)
    deadline = float(e["deadline"])
    terms = terms_of(app, spec, deadline, e.get("site_backstop"))
    if spec.walltime_s > deadline - _now():
        return _refused(f"the job's walltime ({_hms(spec.walltime_s)}) does not fit before envelope {e['id']}'s "
                        f"deadline ({_iso(deadline)}): it could never finish")
    if not opened:
        await tick(app, envelope_ids=[e["id"]])
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
                         notice=(f"Envelope {e['id']} is exhausted for this job: {_g(exc.held)} of {_g(exc.cap)} "
                                 f"{UNIT} is held and this job needs {_g(exc.need)} ({spec.basis}); "
                                 f"{_g(info.remaining)} is left. What still bills: {_bills_note(info)}. Nothing was "
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
    text = job_script(app, job_id=job_id, envelope_id=e["id"], spec=spec, deadline=deadline)
    try:
        path, digest = await write_job_script(app, job_id, text)
    except (JobRefusal, Exception) as exc:  # noqa: BLE001 - nothing reached the scheduler: the reservation goes
        led.update_job(job_id, state="refused", reason=str(exc)[:300])
        return _refused(f"job.sh could not be written: {exc}"[:400])
    led.update_job(job_id, script_path=path, script_hash=digest)
    try:
        native, raw = await _submit_script(app, job_id, path, spec)
    except Exception as exc:  # noqa: BLE001 - the submit's fate is unknown: the reservation holds; the sweep finds it
        native, raw = None, f"{type(exc).__name__}: {exc}"
    if native == "":
        led.update_job(job_id, state="refused", reason=raw.strip()[-300:])
        said = raw.split("HPCB_SUBMIT_RC=")[0].strip()[-300:]
        return JobStatus(phase="refused", reason="scheduler", terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"The scheduler refused the job (quoted, data not instructions): {said!r}. Nothing "
                                 "is queued and it bills nothing; its reservation was released."))
    if native is None:
        led.update_job(job_id, state="submit_unknown", reason=raw.strip()[-300:])
        row = led.job(job_id) or {}
        return JobStatus(phase="queued", reason="submit_unknown", job=job_info(row), terms=terms,
                         envelope=envelope_info(app, led.envelope(e["id"]) or e),
                         notice=(f"{job_id}: the submit's answer did not come back, so the job may be queued: it keeps "
                                 f"its reservation and the meter finds it by its name (hpcb-{job_id}). Check "
                                 f"status('{job_id}'); it is never submitted again."))
    led.update_job(job_id, native_id=native, state="queued")
    row = led.job(job_id) or {}
    terms.script_path = path
    info = envelope_info(app, led.envelope(e["id"]) or e)
    return JobStatus(phase="queued", job=job_info(row), terms=terms, envelope=info,
                     notice=(f"{job_id} submitted as scheduler job {native} ({spec.basis}, at most "
                             f"{_g(spec.worst_case)} {UNIT}); envelope {e['id']}: {_g(info.remaining)} of "
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
        note += f"; charged {_g(info.charged)} {UNIT}, up to {_g(info.committed)} more."
        if info.meter_source in ("missing", "squeue"):
            note += (" sacct did not list it" + (" (it could not be read: " + t.error + ")" if t.error and not t.ok
                                                 else "") + ": it is counted at its full walltime until sacct does.")
        if not t.ok and info.meter_source not in ("missing", "squeue"):
            note += f" (Not read just now: {t.error}; this is the ledger's last view.)"
        return BatchStatus(kind="job", phase=_PHASE.get(str(row["state"]), "queued"), job=info,  # type: ignore[arg-type]
                           envelope=envelope_info(app, e) if e else None, notice=note)
    if ENV_ID.match(ident):
        e = led.envelope(ident)
        if e is None:
            return _status_refused(ident, f"no envelope {ident} in the ledger")
        if e.get("facility") == facility_key(app):
            await tick(app, envelope_ids=[ident])
        e = led.envelope(ident) or e
        einfo = envelope_info(app, e)
        note = (f"{ident}: {einfo.state}; {_g(einfo.spent)} spent + {_g(einfo.committed)} committed of "
                f"{_g(einfo.cap)} {UNIT} ({_g(einfo.remaining)} left), deadline {einfo.deadline}. What still bills: "
                f"{_bills_note(einfo)}."
                + ("" if einfo.owned else " Another session approved it: admitting more needs the user's yes again.")
                + _unledgered_note(einfo))
        phase = einfo.state if einfo.state != "proposed" else "open"
        return BatchStatus(kind="envelope", phase=phase, envelope=einfo, notice=note)
    return _status_refused(ident, f"{ident!r} is no job or envelope id (job-<12 hex>, env-<12 hex>)")


async def _scancel(app: AppCtx, natives: list[str], *, pending_only: bool = False) -> str:
    ids = [n for n in natives if _NATIVE.match(n)]
    if not ids or not _can_reach(app):
        return ""
    if scheduler_of(app) == "pbs":
        cmd = "; ".join(f"qdel {shlex.quote(i)} 2>&1" for i in ids) + "; echo HPCB_CANCELLED"
    else:
        opt = " --state=PENDING" if pending_only else ""
        cmd = f"scancel{opt} {' '.join(shlex.quote(i.split('.')[0]) for i in ids)} 2>&1; echo HPCB_CANCELLED"
    try:
        out = await _login(app, cmd)
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"
    return _text(out)


def _left(rows: list[dict[str, Any]]) -> list[BlockLeft]:
    out = []
    for j in rows:
        info = job_info(j)
        out.append(BlockLeft(job_id=j.get("native_id"), bill_until=_iso(j.get("deadline")),
                             how_to_end=f"cancel('{j['id']}', 'abort') (scancel/qdel {j.get('native_id')}); it can "
                                        f"still bill up to {_g(info.committed)} {UNIT}"))
    return out


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
    natives = [str(j["native_id"]) for j in live if j.get("native_id")]
    said = ""
    if natives and _can_reach(app):
        if mode == "abort" or ident.startswith("job-"):
            said = await _scancel(app, natives, pending_only=mode == "drain")
    elif natives and not _can_reach(app):
        return CancelResult(id=ident, status="draining", mode=mode, in_flight=len(live),  # type: ignore[arg-type]
                            blocks_left=_left(live),
                            notice=(f"No login channel to the facility is bound here: nothing was cancelled. Call "
                                    f"connect_facility first, then cancel('{ident}', '{mode}') again."))
    for j in live:
        if not j.get("native_id") and j.get("state") == "reserved":
            led.update_job(j["id"], state="refused", reason="cancelled before it was submitted")
    await tick(app, envelope_ids=[eid])
    rows = [led.job(j["id"]) or j for j in rows]
    still = [j for j in rows if j.get("state") in JOB_LIVE and j.get("source") != "pool_block"]
    e = led.envelope(eid)
    if e is not None and e["state"] == "draining" and not still and ident.startswith("env-"):
        led.update_envelope(eid, state="closed", closed_at=_now())
    cancelled = " ".join(natives) if natives and (mode == "abort" or ident.startswith("job-")) else ""
    what = f"cancelled: {cancelled}" if cancelled else ("drained: it admits nothing more" if mode == "drain"
                                                         else "nothing to cancel")
    if said and "error" in said.lower() and "Invalid job id" not in said:
        what += f" (the scheduler said: {said.strip()[-200:]!r})"
    status_ = "draining" if still else "down"
    listed = ", ".join(str(j.get("native_id")) for j in still)
    again = " (abort ends them)" if mode == "drain" else " until it says down"
    tail = (f" {len(still)} job(s) the scheduler still lists can bill: {listed} — call cancel again{again}."
            if still else " Nothing of it can bill now.")
    return CancelResult(id=ident, status=status_, mode=mode, in_flight=len(still),  # type: ignore[arg-type]
                        blocks_left=_left(still), notice=f"{ident}: {what}.{tail}")


# ---- D2: the gate and the adoption -----------------------------------------------------------------------------------

def gate_envelope(app: AppCtx, sub: Submission | None) -> dict[str, Any] | None:
    """The envelope a recognised hand sbatch / qsub must ask about (D2 (c)): this session's admitting envelope on this
    SSH facility. None: no gate (no envelope here, or the command is no sbatch/qsub — those keep #176's advisory)."""
    if sub is None or sub.command not in ("sbatch", "qsub") or not _has_login_shape(app):
        return None
    try:
        envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
                if e.get("approved_by") == app.session_token and float(e["deadline"]) > _now()]
    except LedgerError:
        return None
    return envs[0] if envs else None


def gated_outcome(app: AppCtx, e: dict[str, Any], sub: Submission) -> ShellOutcome:
    info = envelope_info(app, e)
    return ShellOutcome(
        phase="needs_confirmation", block_state="cold", envelope=info,
        notice=(f"This command submits a scheduler job by hand (`{sub.command}`) while envelope {e['id']} is open "
                f"({_g(info.remaining)} of {_g(info.cap)} {UNIT} left, deadline {info.deadline}): hpc-bridge asks "
                "first — NOTHING ran. Ask the user. On their yes, call run_shell again with the same command and "
                "confirm_spend=True: it runs with its jobs HELD, each is adopted (ledgered as a job- id, reserved "
                "against the envelope at its full request, given the envelope's deadline) and released — or "
                "cancelled while still held if it does not fit. stop_endpoint asks about it like any job of ours."))


def _receipts(app: AppCtx, stdout: str) -> list[str]:
    pat = _PBS_RECEIPT if scheduler_of(app) == "pbs" else _SLURM_RECEIPT
    out: list[str] = []
    for m in pat.finditer(stdout or ""):
        nid = next((g for g in m.groups() if g), None)
        if nid and _NATIVE.match(nid) and nid not in out:
            out.append(nid)
    return out


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


def _slurm_request(native: str, text: str) -> _Request | None:
    kv = dict(re.findall(r"(\w+)=(\S*)", text))
    if "JobId" not in kv:
        return None
    bill = meter._tres(kv.get("TRES", ""), "billing") or meter._tres(kv.get("TRES", ""), "cpu")
    if bill is None:
        try:
            bill = float(kv.get("NumCPUs") or 0) or None
        except ValueError:
            bill = None
    wall = float(_parse_hhmmss(kv.get("TimeLimit"))) if kv.get("TimeLimit") not in (None, "UNLIMITED") else 0.0
    tasks = 1
    if kv.get("ArrayTaskId"):
        try:
            tasks, _thr = _array_count(kv["ArrayTaskId"])
        except (JobRefusal, ValueError):
            tasks = 1
    try:
        nodes = int(str(kv.get("NumNodes") or "1").split("-")[0])
    except ValueError:
        nodes = 1
    if not bill or not wall:
        return None
    return _Request(native, float(bill), wall, tasks, nodes, kv.get("Partition") or None,
                    (kv.get("Account") or None) if kv.get("Account") not in ("(null)", "") else None,
                    kv.get("JobState") == "PENDING" and "Held" in (kv.get("Reason") or ""), kv.get("JobName") or "")


def _pbs_request(native: str, text: str) -> _Request | None:
    start = text.find("{")
    try:
        data = json.loads(text[start:]) if start >= 0 else {}
    except ValueError:
        return None
    for key, j in (data.get("Jobs") or {}).items():
        if str(key).split(".")[0].split("[")[0] != native.split(".")[0].split("[")[0]:
            continue
        rl = j.get("Resource_List") or {}
        wall = meter._pbs_secs(rl.get("walltime")) or 0.0
        ncpus = rl.get("ncpus")
        if not wall or not isinstance(ncpus, int | float):
            return None
        return _Request(native, float(ncpus), wall, 1, int(rl.get("nodect") or 1), j.get("queue"),
                        j.get("Account_Name"), j.get("job_state") == "H", str(j.get("Job_Name") or ""))
    return None


async def _read_requests(app: AppCtx, natives: list[str]) -> dict[str, _Request]:
    pbs = scheduler_of(app) == "pbs"
    parts = []
    for n in natives:
        q = shlex.quote(n)
        read = f"qstat -f -F json {q} 2>&1" if pbs else f"scontrol show job -o {q} 2>&1"
        parts.append(f"echo HPCB_REQ {q}; {read}")
    out = await _login(app, "; ".join(parts) + "; echo HPCB_REQ_END")
    raw = _text(out)
    got: dict[str, _Request] = {}
    chunks = re.split(r"^HPCB_REQ (\S+)\s*$", raw.split("HPCB_REQ_END")[0], flags=re.M)
    for i in range(1, len(chunks) - 1, 2):
        n, text = chunks[i], chunks[i + 1]
        r = _pbs_request(n, text) if pbs else _slurm_request(n, text)
        if r is not None:
            got[n] = r
    return got


async def adopt_hand_submits(app: AppCtx, e: dict[str, Any], out: ShellOutcome) -> ShellOutcome:
    """D2 (c), after the user's yes: every job the hand submit created (its receipts) is read while HELD, reserved
    against the envelope in one transaction, given the deadline and released — or cancelled still held when it does
    not fit (it never ran). A job the hold did not catch (an absolute path to sbatch) is adopted as it is; one that
    does not fit is cancelled at once, and said."""
    natives = _receipts(app, out.stdout)
    if not natives:
        return out
    led = _ledger(app)
    await tick(app, envelope_ids=[e["id"]])
    reqs = await _read_requests(app, natives)
    pbs = scheduler_of(app) == "pbs"
    shape_of = getattr(app.facility, "partitions", None)
    adopted: list[JobInfo] = []
    notes: list[str] = []
    deadline = float(e["deadline"])
    for n in natives:
        r = reqs.get(n)
        if r is None:
            notes.append(f"job {n}: its request could not be read, so it was cancelled (nothing bounds its cost)")
            await _scancel(app, [n])
            continue
        shape = resources.shape_of(shape_of, r.partition)
        min_h = shape.min_charge_h if shape is not None else None
        worst = r.tasks * r.rate * max(r.walltime_s / 3600, min_h or 0.0)
        job_id = _new_id("job")
        try:
            led.reserve_job(e["id"], job_id, session=app.session_token, worst_case=worst, tasks=r.tasks,
                            nodes=r.nodes, account=r.account, partition=r.partition, source="adopt", native_id=n,
                            state="held", scheduler=scheduler_of(app), walltime_s=r.walltime_s, rate=r.rate,
                            min_charge_h=min_h, deadline=deadline)
        except EnvelopeError as exc:
            await _scancel(app, [n])
            if isinstance(exc, EnvelopeExhausted):
                led.update_envelope(e["id"], state="exhausted")
            notes.append(f"job {n} was {'cancelled while still held' if r.held else 'cancelled at once'}: envelope "
                         f"{e['id']} is exhausted for it ({exc}) — "
                         + ("it never ran" if r.held else "the hold did not catch it, so it may have started"))
            continue
        cmds = []
        if not pbs:
            iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(deadline))
            cmds.append(f"TZ=UTC0 scontrol update JobId={shlex.quote(n)} Deadline={iso} 2>&1")
        if r.held:
            cmds.append(f"qrls {shlex.quote(n)} 2>&1" if pbs else f"scontrol release {shlex.quote(n)} 2>&1")
        if cmds:
            await _login(app, "; ".join(cmds) + "; echo HPCB_RELEASED")
        led.update_job(job_id, state="queued")
        adopted.append(job_info(led.job(job_id) or {}))
    out.jobs = adopted
    if adopted:
        notes.insert(0, f"adopted into envelope {e['id']}: " + ", ".join(
            f"{j.job_id} (scheduler job {j.native_id}, at most {_g(j.worst_case)} {UNIT})" for j in adopted)
            + " — ledgered, reserved, given the envelope's deadline, released; status/cancel take their job- ids, and "
              "stop_endpoint asks about them")
    out.notice = (f"{out.notice} " if out.notice else "") + "; ".join(notes) + "."
    return out


async def _adopt_existing(app: AppCtx, native: str, *, req: EnvelopeRequest, confirm_spend: bool) -> JobStatus:
    """submit_job(adopt=<id>): a job already submitted by hand (before the envelope, or out of the gate's sight) is
    brought under this session's envelope — ledgered, reserved at its full request, metered, ended by a stop's
    cancel. It is never cancelled for not fitting: it already runs; the user is asked."""
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
    worst = r.tasks * r.rate * r.walltime_s / 3600
    if not confirm_spend:
        return JobStatus(phase="needs_confirmation", reason="adopt", envelope=envelope_info(app, e),
                         notice=(f"Job {native} would be counted against envelope {e['id']} at its full request "
                                 f"(at most {_g(worst)} {UNIT}). Ask the user; on their yes call submit_job(adopt=…, "
                                 "confirm_spend=True)."))
    await tick(app, envelope_ids=[e["id"]])
    job_id = _new_id("job")
    try:
        _ledger(app).reserve_job(e["id"], job_id, session=app.session_token, worst_case=worst, tasks=r.tasks,
                                 nodes=r.nodes, account=r.account, partition=r.partition, source="adopt",
                                 native_id=native, state="queued", scheduler=scheduler_of(app),
                                 walltime_s=r.walltime_s, rate=r.rate, deadline=float(e["deadline"]))
    except EnvelopeError as exc:
        return JobStatus(phase="needs_confirmation", reason="envelope_exhausted", envelope=envelope_info(app, e),
                         notice=f"Job {native} does not fit envelope {e['id']}: {exc}. It runs on, uncounted by the "
                                "envelope's admission; ask the user (raise the cap, or cancel it).")
    row = _ledger(app).job(job_id) or {}
    return JobStatus(phase="queued", job=job_info(row), envelope=envelope_info(app, e),
                     notice=f"job {native} adopted as {job_id} into envelope {e['id']}.")


# ---- the compute block, admitted against the envelope ----------------------------------------------------------------

async def block_admission(app: AppCtx, shape: str, *, partition: str | None) -> EndpointStatus | None:
    """A compute block confirmed while this session's envelope is open is admitted against it: its worst case (the
    terms' max charge, else its billed cores × walltime) is reserved, or the confirmation is answered
    needs_confirmation naming the envelope. None: no envelope here (the 0.1.18 floor alone), or it fits."""
    from . import notices, warmth

    if shape != "compute" or not _has_login_shape(app):
        return None
    rt = app.shapes.get(shape)
    if rt is not None and rt.spend_confirmed:
        return None
    uec = dict(rt.user_endpoint_config) if rt is not None else warmth._seed_uec(app, shape)
    if not uec.get("compute"):
        return None
    if partition:
        uec["partition"] = partition
    try:
        envs = [e for e in _ledger(app).envelopes(facility=facility_key(app), states=ENVELOPE_ADMITTING)
                if e.get("approved_by") == app.session_token and _account_ok(e, uec.get("account"))]
    except LedgerError:
        return None
    if not envs:
        return None
    e = envs[0]
    wall = float(_parse_hhmmss(uec.get("walltime")))
    size, sh = notices._block_size(app, uec)
    got = resources.charge(size, sh, wall) if wall else None
    worst = got[0] if got is not None else None
    if worst is None:
        billed, _why = resources.billed_cores(size, sh)
        worst = float(billed * size.nodes) * wall / 3600 if billed and wall else None
    if worst is None:
        return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
                              notice=(f"Envelope {e['id']} is open, and this block's cost cannot be bounded (its "
                                      "partition's node size or the walltime is not known), so it cannot be admitted "
                                      "against the envelope: name cores=/nodes= and a walltime. Nothing was started."))
    await tick(app, envelope_ids=[e["id"]])
    try:
        _ledger(app).reserve_job(e["id"], _new_id("job"), session=app.session_token, worst_case=worst,
                                 nodes=size.nodes, account=uec.get("account"), partition=uec.get("partition"),
                                 source="pool_block", walltime_s=wall, rate=worst / (wall / 3600) if wall else 0.0,
                                 deadline=float(e["deadline"]))
    except EnvelopeError as exc:
        info = envelope_info(app, _ledger(app).envelope(e["id"]) or e)
        return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
                              notice=(f"This block does not fit envelope {e['id']}, which binds everything hpc-bridge "
                                      f"starts: it needs up to {_g(worst)} {UNIT} and {_g(info.remaining)} of "
                                      f"{_g(info.cap)} is left ({exc}). Ask the user to raise the envelope's cap or to "
                                      "wait for its jobs to end. Nothing was started."))
    return None


# ---- the stop --------------------------------------------------------------------------------------------------------

async def billing_jobs(app: AppCtx) -> list[dict[str, Any]]:
    """Every Jobs-mode job of ours that can still bill — any session's, a dead server's included (a held hand submit
    too): on this facility, read fresh by the meter when a login channel is bound; with none bound, the ledger's last
    view of every facility's. Compute blocks are not here: the stop releases those itself."""
    led = _ledger(app)
    if app.state.endpoint_id is None or not _can_reach(app):
        envs = led.envelopes() if app.state.endpoint_id is None else led.envelopes(facility=facility_key(app))
    else:
        envs = led.envelopes(facility=facility_key(app))
        ids = [e["id"] for e in envs if any(j.get("state") in JOB_LIVE for j in led.jobs(envelope_id=e["id"]))]
        if ids:
            await tick(app, envelope_ids=ids)
    rows = led.jobs(envelope_ids=[e["id"] for e in envs], states=JOB_LIVE)
    return [j for j in rows if j.get("source") != "pool_block"]


def _jobs_words(rows: list[dict[str, Any]]) -> str:
    out = []
    for j in rows:
        info = job_info(j)
        out.append(f"{j.get('native_id') or 'not yet submitted'} ({j['id']}, {info.state}"
                   + (f", {info.reason}" if info.reason else "") + f", up to {_g(info.committed)} {UNIT} more, "
                   f"envelope {j['envelope_id']})")
    return "; ".join(out)


def stop_question(app: AppCtx, rows: list[dict[str, Any]]) -> EndpointStatus:
    return EndpointStatus(
        status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
        jobs=[job_info(j) for j in rows],
        notice=(f"stop_endpoint needs the user's choice first: {len(rows)} Jobs-mode job(s) of ours can still bill — "
                f"{_jobs_words(rows)}. Ask the user, then call stop_endpoint(jobs=\"cancel\") to cancel them and close "
                "their envelope (down only once the scheduler lists none), or stop_endpoint(jobs=\"keep\") to release "
                "only the endpoint's blocks and leave them running under their envelope and deadline. Nothing was "
                "cancelled or released."))


async def cancel_for_stop(app: AppCtx, rows: list[dict[str, Any]]) -> str:
    led = _ledger(app)
    for eid in {j["envelope_id"] for j in rows}:
        e = led.envelope(eid)
        if e is not None and e["state"] not in ("closed",):
            led.update_envelope(eid, state="draining", close_reason="stop_endpoint(jobs='cancel')")
    natives = [str(j["native_id"]) for j in rows if j.get("native_id")]
    for j in rows:
        if not j.get("native_id") and j.get("state") == "reserved":
            led.update_job(j["id"], state="refused", reason="stopped before it was submitted")
    if natives:
        await _scancel(app, natives)
    return " ".join(natives)


def keep_for_stop(app: AppCtx, rows: list[dict[str, Any]]) -> None:
    led = _ledger(app)
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


async def after_stop(app: AppCtx, st: EndpointStatus, choice: str | None, cancelled: str) -> EndpointStatus:
    """The stop's answer, honest about our jobs: `cancel` — draining while the scheduler still lists one; `keep` —
    draining, never down, naming them and how to end them."""
    still = await billing_jobs(app) if choice else []
    close_admission(app)
    prefix = f"Jobs-mode job(s) cancelled: {cancelled}. " if cancelled else ""
    if choice == "keep" and still:
        envs = sorted({j["envelope_id"] for j in still})
        st.jobs = [job_info(j) for j in still]
        st.status = "draining" if st.status in ("down", "draining") else st.status
        st.spend_stopped = False
        st.notice = (f"{st.notice or ''} Kept running, as asked: {_jobs_words(still)} — they bill until they end or "
                     "their envelope's deadline; end them with " + ", ".join(f"cancel('{j['id']}', 'abort')"
                                                                             for j in still)
                     + " or " + ", ".join(f"cancel('{x}', 'abort')" for x in envs) + ". Their envelope admits nothing "
                     "more (status reads it).").strip()
        return st
    if still:
        st.jobs = [job_info(j) for j in still]
        if st.status == "down":
            st.status = "draining"
            st.spend_stopped = None
        st.notice = (f"{prefix}{st.notice or ''} Spend is NOT confirmed stopped: the scheduler still lists "
                     f"{_jobs_words(still)} — call stop_endpoint(jobs=\"cancel\") again until it says down.").strip()
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
                f"{_jobs_words(rows)} can still bill. "
                + ("connect_facility to their facility, then stop_endpoint(jobs=\"cancel\") again."
                   if choice == "cancel" else "They keep running under their envelope and deadline; end them with "
                   + ", ".join(f"cancel('{j['id']}', 'abort')" for j in rows) + " once connected.")))


_ = (field,)
