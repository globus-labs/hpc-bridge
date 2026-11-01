"""The billing meter (batch phase 2a; vault `Planned/Batch and swarm execution.md` §3 "The meter never understates").

One login-shape call per tick reads what the scheduler says every job of ours charged and can still charge:

- **Slurm**: `sacct -X -n -P -u "$USER" -S <earliest> -o JobID,JobName,State,ElapsedRaw,TimelimitRaw,AllocTRES,ReqTRES,
  Partition,Account,ExitCode,Reason` (under TZ=UTC0: -S is UTC), then, in the same call, `squeue -h -u "$USER" -o
  "%i|%T|%M|%l|%j"` — the fallback for a job sacct does not list, and whether a cancelled job is still on its way out.
- **PBS**: `qstat -x -f -F json $(qselect -x -u "$USER")`.

The rules (`job_reading`): **charged** = the job's `billing` TRES (AllocTRES, else ReqTRES: it carries the site's
partition weights) × elapsed; where the scheduler reports no billing, the rate the terms reserved at (the slice's billed
cores × the weights, never a whole node for a slice). Never below the queue's minimum charge (`min_charge_h`, a catalog
fact) once the job started. **committed** = a running job's remaining walltime, a queued one's full walltime (and every
pending array task's). **A ledger job sacct does not list** holds its full walltime: squeue says only whether it
is still live. **Unledgered** jobs — every other job of the user in the same output — count, and are named, until
the user says they are not theirs (`exclude_jobs`).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

SACCT_FIELDS = ("JobID", "JobName", "State", "ElapsedRaw", "TimelimitRaw", "AllocTRES", "ReqTRES", "Partition",
                "Account", "ExitCode", "Reason")
SQUEUE_FMT = "%i|%T|%M|%l|%j"
SACCT_MARK, SQUEUE_MARK, END_MARK = "HPCB_SACCT_BEGIN", "HPCB_SQUEUE_BEGIN", "HPCB_METER_END"
QSTAT_MARK = "HPCB_QSTAT_BEGIN"
_LIVE_SQUEUE = frozenset({"PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "SUSPENDED", "RESIZING", "REQUEUED",
                          "REQUEUE_HOLD", "REQUEUE_FED", "SIGNALING", "STAGE_OUT", "STOPPED"})
_ENDED = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY",
                    "PREEMPTED", "BOOT_FAIL", "REVOKED", "SPECIAL_EXIT"})
_FAILED = frozenset({"FAILED", "TIMEOUT", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "BOOT_FAIL",
                     "REVOKED", "SPECIAL_EXIT"})


def slurm_cmd(since_iso: str) -> str:
    """The tick's one login-shape command on Slurm (`since_iso`: UTC, `YYYY-MM-DDTHH:MM:SS`)."""
    fields = ",".join(f"{f}%200" if f in ("JobName", "AllocTRES", "ReqTRES", "Reason") else f for f in SACCT_FIELDS)
    return (f'echo {SACCT_MARK}; TZ=UTC0 sacct -X -n -P -u "$USER" -S {since_iso} -o {fields} 2>&1; '
            f'echo "HPCB_SACCT_RC=$?"; echo {SQUEUE_MARK}; squeue -h -u "$USER" -o \'{SQUEUE_FMT}\' 2>/dev/null; '
            f'echo "HPCB_SQUEUE_RC=$?"; echo {END_MARK}')


def pbs_cmd() -> str:
    return (f'echo {QSTAT_MARK}; ids=$(qselect -x -u "$USER" 2>/dev/null); '
            'if [ -n "$ids" ]; then qstat -x -f -F json $ids 2>/dev/null; else echo "{}"; fi; '
            f'echo "HPCB_QSTAT_RC=$?"; echo {END_MARK}')


def _between(text: str, start: str, end: str | None) -> str | None:
    i = text.find(start)
    if i < 0:
        return None
    rest = text[i + len(start):]
    j = rest.find(end) if end else -1
    return rest if j < 0 else rest[:j]


@dataclass
class Row:
    """One scheduler row: a job, an array task (`task`), or a range of pending array tasks (`pending_tasks`)."""
    base: str
    task: str | None
    name: str
    state: str
    elapsed_s: float
    limit_s: float | None
    billing: float | None
    partition: str | None = None
    account: str | None = None
    exit_code: int | None = None
    reason: str | None = None
    pending_tasks: int = 0
    cpus: float | None = None


@dataclass
class SchedView:
    """What one tick read: `rows` by base id; `live` — ids the scheduler still lists (squeue, or a non-final PBS state);
    `ok` — whether the accounting read worked (sacct / qstat); `source`."""
    rows: dict[str, list[Row]] = field(default_factory=dict)
    live: dict[str, str] = field(default_factory=dict)
    ok: bool = False
    source: str = "sacct"
    calls: int = 0
    error: str = ""


def _tres(spec: str, key: str) -> float | None:
    m = re.search(rf"(?:^|,){key}=([0-9.]+)", spec or "")
    return float(m.group(1)) if m else None


def _range_count(spec: str) -> int:
    """`[4-19%4]` / `[2,5,7-9]` → how many tasks."""
    s = spec.strip("[]").split("%", 1)[0]
    n = 0
    for part in s.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                n += int(b) - int(a) + 1
            except ValueError:
                n += 1
        elif part:
            n += 1
    return max(n, 1)


def parse_slurm(out: str) -> SchedView:
    v = SchedView(source="sacct", calls=1)
    sacct = _between(out, SACCT_MARK, "HPCB_SACCT_RC=") or ""
    m = re.search(r"HPCB_SACCT_RC=(\d+)", out)
    v.ok = m is not None and m.group(1) == "0"
    if not v.ok:
        v.error = sacct.strip()[-200:]
    else:
        for line in sacct.splitlines():
            cols = line.split("|")
            if len(cols) < len(SACCT_FIELDS) or not cols[0][:1].isdigit():
                continue
            d = dict(zip(SACCT_FIELDS, cols, strict=False))
            jid = d["JobID"]
            base, _, task = jid.partition("_")
            pending = 0
            if task.startswith("["):
                pending, task = _range_count(task), ""
            state = (d["State"].split() or ["UNKNOWN"])[0]
            try:
                elapsed = float(d["ElapsedRaw"] or 0)
            except ValueError:
                elapsed = 0.0
            try:
                limit = float(d["TimelimitRaw"]) * 60 if d["TimelimitRaw"] not in ("", "UNLIMITED", "Partition_Limit") \
                    else None
            except ValueError:
                limit = None
            bill = _tres(d["AllocTRES"], "billing")
            if bill is None:
                bill = _tres(d["ReqTRES"], "billing")
            cpus = _tres(d["AllocTRES"], "cpu") or _tres(d["ReqTRES"], "cpu")
            ec = None
            if re.match(r"^\d+", d.get("ExitCode") or ""):
                ec = int(re.match(r"^(\d+)", d["ExitCode"]).group(1))  # type: ignore[union-attr]
            v.rows.setdefault(base.split(".")[0], []).append(Row(
                base=base.split(".")[0], task=task or None, name=d["JobName"], state=state, elapsed_s=elapsed,
                limit_s=limit, billing=bill, partition=d.get("Partition") or None, account=d.get("Account") or None,
                exit_code=ec, reason=(d.get("Reason") or None) if d.get("Reason") not in ("None", "") else None,
                pending_tasks=pending, cpus=cpus))
    sq = _between(out, SQUEUE_MARK, "HPCB_SQUEUE_RC=") or ""
    for line in sq.splitlines():
        cols = line.split("|")
        if len(cols) < 2 or not cols[0][:1].isdigit():
            continue
        base = cols[0].split("_", 1)[0]
        if cols[1] in _LIVE_SQUEUE:
            v.live[base] = cols[1] if base not in v.live or cols[1] == "RUNNING" else v.live[base]
    return v


def _pbs_secs(hms: Any) -> float | None:
    if not isinstance(hms, str) or not re.match(r"^\d+:\d{2}:\d{2}$", hms):
        return None
    h, m, s = (int(x) for x in hms.split(":"))
    return float(h * 3600 + m * 60 + s)


_PBS_STATE = {"Q": "PENDING", "H": "PENDING", "W": "PENDING", "T": "PENDING", "S": "SUSPENDED", "U": "SUSPENDED",
              "R": "RUNNING", "E": "COMPLETING", "B": "RUNNING", "M": "RUNNING"}


def parse_pbs(out: str) -> SchedView:
    v = SchedView(source="qstat", calls=1)
    body = _between(out, QSTAT_MARK, "HPCB_QSTAT_RC=") or ""
    start = body.find("{")
    try:
        data = json.loads(body[start:]) if start >= 0 else {}
        v.ok = isinstance(data, dict)
    except ValueError:
        v.error = body.strip()[-200:]
        return v
    for key, j in (data.get("Jobs") or {}).items():
        if not isinstance(j, dict):
            continue
        base = str(key).split(".", 1)[0].split("[", 1)[0]
        st = str(j.get("job_state") or "")
        rl, used = j.get("Resource_List") or {}, j.get("resources_used") or {}
        if st in ("F", "X"):
            es = j.get("Exit_status")
            state = "COMPLETED" if es == 0 else "CANCELLED" if es in (271, -3) else "FAILED"
        else:
            state = _PBS_STATE.get(st, "PENDING")
            v.live[base] = state
        ncpus = rl.get("ncpus")
        v.rows.setdefault(base, []).append(Row(
            base=base, task=None, name=str(j.get("Job_Name") or ""), state=state,
            elapsed_s=_pbs_secs(used.get("walltime")) or 0.0, limit_s=_pbs_secs(rl.get("walltime")), billing=None,
            partition=j.get("queue"), account=j.get("Account_Name"),
            exit_code=j.get("Exit_status") if isinstance(j.get("Exit_status"), int) else None,
            reason=("held" if st == "H" else (str(j.get("comment") or "") or None)),
            cpus=float(ncpus) if isinstance(ncpus, int | float) else None))
    return v


@dataclass
class Reading:
    charged: float
    committed: float
    state: str
    source: str
    exit_code: int | None = None
    reason: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    live: bool = True


def _phase_state(states: list[str]) -> str:
    if any(s in ("RUNNING", "COMPLETING") for s in states):
        return "RUNNING"
    if any(s in ("PENDING", "SUSPENDED", "CONFIGURING", "REQUEUED") for s in states):
        return "PENDING"
    if states and all(s == "COMPLETED" for s in states):
        return "COMPLETED"
    if any(s in _FAILED for s in states):
        return next(s for s in states if s in _FAILED)
    return states[-1] if states else "UNKNOWN"


def job_reading(job: dict[str, Any], view: SchedView) -> Reading:
    """One ledger job's charged / committed from the tick (see the module doc). Never less than it can bill."""
    rate = float(job.get("rate") or 0.0)
    min_s = float(job.get("min_charge_h") or 0.0) * 3600
    tasks = int(job.get("tasks") or 1)
    limit_s = float(job.get("walltime_s") or 0.0)
    worst = float(job.get("worst_case") or 0.0)
    native = str(job.get("native_id") or "").split(".")[0].split("[")[0]
    rows = view.rows.get(native) if native else None
    listed = view.live.get(native) if native else None
    if not view.ok or not rows:
        # sacct (or qstat) did not list it: its full walltime is held — squeue says only whether it is still live
        prior = float(job.get("charged") or 0.0)
        # still live: the scheduler lists it, or — sacct unreadable — the ledger last saw it live
        live = listed is not None or (not view.ok and job.get("state") in ("reserved", "submit_unknown", "held",
                                                                           "queued", "running"))
        return Reading(prior, max(0.0, worst - prior), listed or (job.get("sched_state") or "UNKNOWN"),
                       "squeue" if listed else "missing", live=live)
    charged = committed = 0.0
    counts: dict[str, int] = {}
    states: list[str] = []
    accounted = 0
    exit_code: int | None = None
    reason: str | None = None
    for r in rows:
        r_rate = r.billing if r.billing is not None else rate
        r_limit = r.limit_s if r.limit_s is not None else limit_s
        if r.pending_tasks:
            n = r.pending_tasks
            counts["PENDING"] = counts.get("PENDING", 0) + n
            committed += n * r_rate * max(r_limit, min_s) / 3600
            accounted += n
            states.append("PENDING")
            reason = reason or r.reason
            continue
        accounted += 1
        state = r.state
        if state in _ENDED and listed and native and r.task is None and len(rows) == 1:
            state = listed   # sacct already says CANCELLED; the scheduler still lists it on its way out
        counts[state] = counts.get(state, 0) + 1
        states.append(state)
        started = r.elapsed_s > 0 or state not in ("PENDING",)
        billed = max(r.elapsed_s, min_s) if started and state != "PENDING" else 0.0
        charged += r_rate * billed / 3600
        if state in _ENDED:
            pass
        elif state == "PENDING":
            committed += r_rate * max(r_limit, min_s) / 3600
        else:
            committed += r_rate * max(0.0, max(r_limit, min_s) - billed) / 3600
        if len(rows) == 1:
            exit_code = r.exit_code
        reason = reason or (r.reason if state == "PENDING" else None)
    if accounted < tasks:   # tasks the scheduler did not list at all: each holds its full walltime
        committed += (tasks - accounted) * rate * max(limit_s, min_s) / 3600
    state = _phase_state(states)
    live = state in ("RUNNING", "PENDING") or bool(listed)
    return Reading(round(charged, 6), round(committed, 6), state, view.source, exit_code, reason, counts, live)


def unledgered(view: SchedView, known: set[str], excluded: set[str], *, account: str | None = None) -> list[dict]:
    """Every job in the tick that is not a ledger job's and not one the user excluded (on the envelope's account when
    the row says one): what it charged and can still charge, at its own billing (else its cpus) per hour."""
    out = []
    for base, rows in view.rows.items():
        if base in known or base in excluded:
            continue
        if account and any(r.account and r.account != account for r in rows):
            continue
        ch = co = 0.0
        states = []
        for r in rows:
            rate = r.billing if r.billing is not None else (r.cpus or 1.0)
            n = r.pending_tasks or 1
            states.append(r.state)
            if r.pending_tasks or r.state == "PENDING":
                co += n * rate * (r.limit_s or 0.0) / 3600
                continue
            ch += rate * r.elapsed_s / 3600
            if r.state not in _ENDED:
                co += rate * max(0.0, (r.limit_s or 0.0) - r.elapsed_s) / 3600
        out.append({"native_id": base, "name": rows[0].name, "state": _phase_state(states), "charged": round(ch, 6),
                    "committed": round(co, 6)})
    return out


def journal_tick(*, envelope_id: str | None, jobs: int, sacct_calls: int, source: str) -> None:
    """The journal's `meter.tick` row (HPC_BRIDGE_JOURNAL): the harness grades one scheduler read per tick from it."""
    from .batch import _journal

    _journal({"event": "meter.tick", "envelope_id": envelope_id, "jobs": jobs, "sacct_calls": sacct_calls,
              "source": source})
