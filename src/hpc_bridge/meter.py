"""The billing meter (batch phase 2a; vault `Planned/Batch and swarm execution.md` §3 "The meter never understates").

One login-shape call per tick reads what the scheduler says every job of ours charged and can still charge:

- **Slurm**: `sacct -X -D -n -P -u "$USER" -S <earliest> -o JobID,JobName,State,ElapsedRaw,TimelimitRaw,AllocTRES,
  ReqTRES,Partition,Account,ExitCode,Reason` (under TZ=UTC0: -S is UTC; `-D`: a requeued job's earlier runs too), then,
  in the same call, `squeue -h -u "$USER" -o "%i|%T|%M|%l|%j"` — what is still live (a cancelled job on its way out),
  the pending tasks of an array (sacct has no row for them: the fake cluster's probe, 2026-11-01), and the fallback for
  a job sacct does not list.
- **PBS**: `qstat -x -f -F json -t $(qselect -x -u "$USER")` (`-t`: an array's subjobs).

Each scheduler command runs under a bash-native timeout (`METER_TIMEOUT_S`, below the sync-wait): with slurmdbd down
sacct HANGS (live 2026-11-01: the tick outlived the sync-wait, read nothing and held the login channel for the next
call); killed, it is an unreadable sacct, and every job is held at its full walltime.

The rules (`job_reading`): **charged** = the job's `billing` TRES (AllocTRES, else ReqTRES: it carries the site's
partition weights) × elapsed; where the scheduler reports no billing, the rate the terms reserved at (the slice's billed
cores × the weights, never a whole node for a slice). Never below the queue's minimum charge (`min_charge_h`, a catalog
fact) once the job started. **committed** = a running job's remaining walltime, a queued one's full walltime (and every
pending array task's). **A ledger job sacct does not list** holds its full walltime: squeue says only whether it is
still live. **Unledgered** jobs — every other job of the user in the same output — count, and are named, until the user
says they are not theirs (`exclude_jobs`); one with no time limit (UNLIMITED) is held to the envelope's deadline.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

SACCT_FIELDS = ("JobID", "JobName", "State", "ElapsedRaw", "TimelimitRaw", "AllocTRES", "ReqTRES", "Partition",
                "Account", "ExitCode", "Reason")
# %r: a pending job's reason — sacct's Reason says `None` for a job a site limit holds (live 2026-11-01:
# AssocGrpCPUMinutesLimit under GrpTRESMins + `safe`); the name last, so a `|` in it cannot shift a column
# %l and %C: an array no task of which has started has NO sacct row (probe 3b): squeue's limit and CPUs are all there
# is to count it by (review r2)
SQUEUE_FMT = "%i|%T|%M|%l|%C|%P|%r|%j"
SACCT_MARK, SQUEUE_MARK, END_MARK = "HPCB_SACCT_BEGIN", "HPCB_SQUEUE_BEGIN", "HPCB_METER_END"
QSTAT_MARK = "HPCB_QSTAT_BEGIN"
_LIVE_SQUEUE = frozenset({"PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "SUSPENDED", "RESIZING", "REQUEUED",
                          "REQUEUE_HOLD", "REQUEUE_FED", "SIGNALING", "STAGE_OUT", "STOPPED"})
_ENDED = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY",
                    "PREEMPTED", "BOOT_FAIL", "REVOKED", "SPECIAL_EXIT", "REQUEUED"})
_FAILED = frozenset({"FAILED", "TIMEOUT", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "BOOT_FAIL",
                     "REVOKED", "SPECIAL_EXIT"})
# The ended states whose exit code is the job's own: sacct says 0:0 for a job still running, and for one CANCELLED or
# TIMEOUT (the demo of 2026-11-03: an adopted job read exit_code 0 while RUNNING and after CANCELLED). PBS's are read
# the same way (`parse_pbs` names an Exit_status of 0 COMPLETED, a qdel CANCELLED, anything else FAILED).
_EXIT_CODED = frozenset({"COMPLETED", "FAILED"})
# A bash-native timeout (no coreutils `timeout` assumed): the command in the background, a watcher that TERMs it. The
# watcher's own output goes nowhere: an orphaned `sleep` must not hold the call's stdout open past the command.
_BOUND = ('__hb_bound() { "$@" & local p=$!; ( sleep @@S@@; kill -TERM "$p" 2>/dev/null ) </dev/null >/dev/null 2>&1 & '
          'local w=$!; wait "$p"; local rc=$?; kill "$w" 2>/dev/null; wait "$w" 2>/dev/null; return "$rc"; }; ')


def timeout_s() -> int:
    """How long one scheduler read may take: HPC_BRIDGE_METER_TIMEOUT_S, else 15 s — and always a few seconds under
    the sync-wait, so the tick's login call answers in it."""
    from .config import SYNC_WAIT_S

    try:
        want = float(os.environ.get("HPC_BRIDGE_METER_TIMEOUT_S", "") or 15.0)
    except ValueError:
        want = 15.0
    return max(1, int(min(want, max(1.0, float(os.environ.get("HPC_BRIDGE_SYNC_WAIT_S", "") or SYNC_WAIT_S) - 4))))


def _bound() -> str:
    return _BOUND.replace("@@S@@", str(timeout_s()))


def slurm_cmd(since_iso: str) -> str:
    """The tick's one login-shape command on Slurm (`since_iso`: UTC, `YYYY-MM-DDTHH:MM:SS`)."""
    fields = ",".join(f"{f}%200" if f in ("JobName", "AllocTRES", "ReqTRES", "Reason") else f for f in SACCT_FIELDS)
    return (f'{_bound()}echo {SACCT_MARK}; __hb_bound env TZ=UTC0 sacct -X -D -n -P -u "$USER" -S {since_iso} '
            f'-o {fields} 2>&1; echo "HPCB_SACCT_RC=$?"; echo {SQUEUE_MARK}; '
            f'__hb_bound squeue -h -u "$USER" -o \'{SQUEUE_FMT}\' 2>/dev/null; echo "HPCB_SQUEUE_RC=$?"; '
            f'echo {END_MARK}')


def pbs_cmd() -> str:
    """The tick's one command on PBS. HPCB_QSTAT_RC is qselect's answer, then qstat's: a server that cannot be reached
    or a read the bound killed is an UNREAD view (review r2: it parsed as "ok, no jobs")."""
    return (f'{_bound()}echo {QSTAT_MARK}; ids=$(__hb_bound qselect -x -u "$USER" 2>/dev/null); __rc=$?; '
            'if [ "$__rc" -eq 0 ]; then if [ -n "$ids" ]; then __hb_bound qstat -x -f -F json -t $ids 2>/dev/null; '
            '__rc=$?; else echo "{}"; fi; fi; '
            f'echo "HPCB_QSTAT_RC=$__rc"; echo {END_MARK}')


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
    """What one tick read: `rows` by base id (sacct / qstat); `live` — base ids the scheduler still lists, with their
    state (squeue, or a non-final PBS state); `pending` — an array's pending tasks squeue lists (sacct has no row for
    them); `reasons` — why a pending job waits, as squeue says it (`%r`; PBS: its comment); `ok` — whether the
    accounting read worked; `source`."""
    rows: dict[str, list[Row]] = field(default_factory=dict)
    live: dict[str, str] = field(default_factory=dict)
    pending: dict[str, int] = field(default_factory=dict)
    names: dict[str, str] = field(default_factory=dict)
    reasons: dict[str, str] = field(default_factory=dict)
    queued: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)   # base -> squeue (limit, cpus)
    units: dict[str, int] = field(default_factory=dict)          # base -> squeue lines that are one job or one task
    partitions: dict[str, str] = field(default_factory=dict)     # base -> squeue %P
    # base -> what a caller read for a job squeue lists and sacct does not (an array none of whose tasks has started;
    # one slurmdbd has not recorded yet): {"rate": billing per task, "limit_s": …, "name": …} — its billing, never
    # bare CPUs where the billing differs (review r3)
    extra: dict[str, dict[str, Any]] = field(default_factory=dict)

    def squeue_only(self) -> list[str]:
        return [b for b in dict.fromkeys([*self.pending, *self.units]) if b not in self.rows]
    ok: bool = False
    source: str = "sacct"
    calls: int = 0
    error: str = ""


def tres(spec: str, key: str) -> float | None:
    m = re.search(rf"(?:^|,){re.escape(key)}=([0-9.]+)", spec or "")
    return float(m.group(1)) if m else None


def range_count(spec: str) -> int:
    """`[4-19%4]` / `[2,5,7-9]` / `0-98:2` → how many tasks."""
    s = spec.strip("[]").split("%", 1)[0]
    n = 0
    for part in s.split(","):
        step = 1
        if ":" in part:
            part, st = part.split(":", 1)
            try:
                step = max(1, int(st))
            except ValueError:
                step = 1
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                n += (int(b) - int(a)) // step + 1
            except ValueError:
                n += 1
        elif part:
            n += 1
    return max(n, 1)


def _sq_secs(raw: str) -> float | None:
    """squeue's %l: `[D-]H:MM:SS`, `M:SS` or `M`; UNLIMITED (or anything else) -> None."""
    m = re.fullmatch(r"(?:(\d+)-)?(\d+)(?::(\d+))?(?::(\d+))?", (raw or "").strip())
    if not m:
        return None
    d, a, b, c = (int(x) if x else None for x in m.groups())
    if c is not None:
        secs = (a or 0) * 3600 + (b or 0) * 60 + c
    elif b is not None:
        secs = (a or 0) * 60 + b if d is None else (a or 0) * 3600 + b * 60
    else:
        secs = (a or 0) * 60 if d is None else (a or 0) * 3600
    return float((d or 0) * 86400 + secs)


def _limit(raw: str) -> float | None:
    try:
        return float(raw) * 60 if raw not in ("", "UNLIMITED", "Partition_Limit") else None
    except ValueError:
        return None


def parse_slurm(out: str) -> SchedView:
    v = SchedView(source="sacct", calls=1)
    sacct = _between(out, SACCT_MARK, "HPCB_SACCT_RC=") or ""
    m = re.search(r"HPCB_SACCT_RC=(\d+)", out)
    v.ok = m is not None and m.group(1) == "0"
    if not v.ok:
        v.error = sacct.strip()[-200:] or "sacct did not answer in time"
    else:
        for line in sacct.splitlines():
            cols = line.split("|")
            if len(cols) < len(SACCT_FIELDS) or not cols[0][:1].isdigit():
                continue
            d = dict(zip(SACCT_FIELDS, cols, strict=False))
            base, _, task = d["JobID"].partition("_")
            pending = 0
            if task.startswith("["):
                pending, task = range_count(task), ""
            state = (d["State"].split() or ["UNKNOWN"])[0]
            try:
                elapsed = float(d["ElapsedRaw"] or 0)
            except ValueError:
                elapsed = 0.0
            bill = tres(d["AllocTRES"], "billing")
            if bill is None:
                bill = tres(d["ReqTRES"], "billing")
            cpus = tres(d["AllocTRES"], "cpu") or tres(d["ReqTRES"], "cpu")
            em = re.match(r"^(\d+)", d.get("ExitCode") or "")
            b = base.split(".")[0]
            v.names.setdefault(b, d["JobName"])
            v.rows.setdefault(b, []).append(Row(
                base=b, task=task or None, name=d["JobName"], state=state, elapsed_s=elapsed,
                limit_s=_limit(d["TimelimitRaw"]), billing=bill, partition=d.get("Partition") or None,
                account=d.get("Account") or None, exit_code=int(em.group(1)) if em else None,
                reason=(d.get("Reason") or None) if d.get("Reason") not in ("None", "") else None,
                pending_tasks=pending, cpus=cpus))
    sq = _between(out, SQUEUE_MARK, "HPCB_SQUEUE_RC=") or ""
    for line in sq.splitlines():
        cols = line.split("|")
        if len(cols) < 2 or not cols[0][:1].isdigit() or cols[1] not in _LIVE_SQUEUE:
            continue
        base, _, task = cols[0].partition("_")
        if task.startswith("[") and cols[1] == "PENDING":
            v.pending[base] = v.pending.get(base, 0) + range_count(task)
        else:
            v.units[base] = v.units.get(base, 0) + 1
        if len(cols) > 4 and base not in v.queued:
            try:
                sq_cpus: float | None = float(cols[4])
            except ValueError:
                sq_cpus = None
            v.queued[base] = (_sq_secs(cols[3]), sq_cpus)
        if len(cols) > 5 and cols[5]:
            v.partitions.setdefault(base, cols[5])
        if cols[1] == "PENDING" and len(cols) > 6 and cols[6] not in ("", "None", "(null)"):
            v.reasons[base] = cols[6]
        if len(cols) > 7:
            v.names.setdefault(base, "|".join(cols[7:]))
        prev = v.live.get(base)
        v.live[base] = cols[1] if prev is None or cols[1] == "RUNNING" else prev
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
    rc = re.search(r"HPCB_QSTAT_RC=(\d+)", out)
    if rc is None or rc.group(1) != "0":
        v.error = body.strip()[-200:] or f"qselect/qstat did not answer (rc {rc.group(1) if rc else '?'})"
        return v
    start = body.find("{")
    try:
        data = json.loads(body[start:body.rfind("}") + 1]) if start >= 0 else None
    except ValueError:
        data = None
    if not isinstance(data, dict):
        v.error = body.strip()[-200:] or "qstat did not answer in time"
        return v
    v.ok = True
    jobs = data.get("Jobs") or {}
    subjobs = {str(k).split("[", 1)[0] for k in jobs if re.search(r"\[\d+\]", str(k))}
    for key, j in jobs.items():
        if not isinstance(j, dict):
            continue
        k = str(key)
        base = k.split(".", 1)[0].split("[", 1)[0]
        is_parent = "[]" in k
        if is_parent and base in subjobs:
            continue   # the subjobs carry the array (qstat -t)
        st = str(j.get("job_state") or "")
        rl, used = j.get("Resource_List") or {}, j.get("resources_used") or {}
        if st in ("F", "X"):
            es = j.get("Exit_status")
            state = "COMPLETED" if es == 0 else "CANCELLED" if es in (271, -3) else "FAILED"
        else:
            state = _PBS_STATE.get(st, "PENDING")
            v.live[base] = state
        ncpus = rl.get("ncpus")
        task = re.search(r"\[(\d+)\]", k)
        v.names.setdefault(base, str(j.get("Job_Name") or ""))
        why = "held" if st == "H" else str(j.get("comment") or "")
        if state == "PENDING" and why:
            v.reasons.setdefault(base, why)
        v.rows.setdefault(base, []).append(Row(
            base=base, task=task.group(1) if task else None, name=str(j.get("Job_Name") or ""), state=state,
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
    if any(s in ("PENDING", "SUSPENDED", "CONFIGURING") for s in states):
        return "PENDING"
    final = [s for s in states if s != "REQUEUED"] or states
    if final and all(s == "COMPLETED" for s in final):
        return "COMPLETED"
    if any(s in _FAILED for s in final):
        return next(s for s in final if s in _FAILED)
    return final[-1] if final else "UNKNOWN"


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
        live = listed is not None or (not view.ok and job.get("state") in ("reserved", "submit_unknown", "held",
                                                                           "queued", "running"))
        return Reading(prior, max(0.0, worst - prior), listed or (job.get("sched_state") or "UNKNOWN"),
                       "squeue" if listed else "missing", reason=view.reasons.get(native) if native else None,
                       live=live)
    charged = committed = 0.0
    counts: dict[str, int] = {}
    states: list[str] = []
    accounted = 0
    exit_code: int | None = None
    reason: str | None = None
    finals = [r for r in rows if r.state != "REQUEUED"]
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
        state = r.state
        if r.state != "REQUEUED":   # a requeued job's earlier run (sacct -D) is billed, but no task of its own
            accounted += 1
            counts[state] = counts.get(state, 0) + 1
        states.append(state)
        started = r.elapsed_s > 0 or state != "PENDING"
        billed = max(r.elapsed_s, min_s) if started and state != "PENDING" else 0.0
        charged += r_rate * billed / 3600
        if state in _ENDED:
            pass
        elif state == "PENDING":
            committed += r_rate * max(r_limit, min_s) / 3600
        else:
            committed += r_rate * max(0.0, max(r_limit, min_s) - billed) / 3600
        if len(finals) == 1 and r.state in _EXIT_CODED:
            exit_code = r.exit_code
        reason = reason or (r.reason if state == "PENDING" else None)
    sq_pending = view.pending.get(native, 0)
    if sq_pending and "PENDING" not in counts:   # an array's pending tasks: in squeue only
        counts["PENDING"] = sq_pending
        states.append("PENDING")
    missing = max(0, tasks - accounted)   # tasks the accounting did not list (pending ones, or not seen at all)
    if missing:
        committed += missing * rate * max(limit_s, min_s) / 3600
    if (rest := tasks - sum(counts.values())) > 0:
        # a task neither sacct -X nor squeue's pending ranges list (one between the two: the demo of 2026-11-03 read 8
        # COMPLETED + 1 RUNNING of 10) is held at its full walltime above, and counted PENDING: the counts sum to tasks
        counts["PENDING"] = counts.get("PENDING", 0) + rest
    state = _phase_state(states)
    if state == "PENDING":   # why it waits, as the scheduler says it now (sacct's Reason may be `None`)
        reason = view.reasons.get(native) or reason
    if listed and state not in ("RUNNING", "PENDING"):
        state = "COMPLETING" if listed in ("COMPLETING", "RUNNING") else listed   # cancelled, still on its way out
    live = bool(listed) or state in ("RUNNING", "PENDING", "COMPLETING")
    return Reading(round(charged, 6), round(committed, 6), state, view.source, exit_code, reason, counts, live)


def unledgered(view: SchedView, known: set[str], excluded: set[str], *, accounts: set[str] | None = None,
               horizon_s: float = 0.0) -> list[dict]:
    """Every job in the tick that is not a ledger job's and not one the user excluded (on one of the envelope's
    accounts when the row says one): what it charged and can still charge, at its own billing (else its cpus) per hour.
    A job with no time limit (UNLIMITED) holds the whole time left to `horizon_s` (the envelope's deadline), never
    nothing; an array's pending tasks — in squeue only — count too, and an array no task of which has started (no sacct
    row at all) is counted from squeue's limit and CPUs (review r2)."""
    out = []
    for base, rows in view.rows.items():
        if base in known or base in excluded:
            continue
        if accounts and any(r.account and r.account not in accounts for r in rows):
            continue
        ch = co = 0.0
        states = []
        for r in rows:
            rate = r.billing if r.billing is not None else (r.cpus or 1.0)
            n = r.pending_tasks or 1
            states.append(r.state)
            # no limit (UNLIMITED, a PBS job with none) — or running past it — holds all the time to `horizon_s`:
            # charged + committed is then the same whenever it is read, so a proposal does not grow with the clock
            no_limit = r.limit_s is None or (r.state not in _ENDED and r.state != "PENDING"
                                             and r.elapsed_s >= r.limit_s)
            limit = r.elapsed_s + horizon_s if no_limit else float(r.limit_s or 0.0)
            if r.pending_tasks or r.state == "PENDING":
                co += n * rate * limit / 3600
                continue
            ch += rate * r.elapsed_s / 3600
            if r.state not in _ENDED:
                co += rate * max(0.0, limit - r.elapsed_s) / 3600
        first = rows[0]
        rate0 = first.billing if first.billing is not None else (first.cpus or 1.0)
        pend = view.pending.get(base, 0) if not any(r.pending_tasks for r in rows) else 0
        if pend:   # sacct has no row for an array's pending tasks (probe 3b): squeue's count
            limit = first.limit_s if first.limit_s is not None else (view.queued.get(base, (None, None))[0]
                                                                     or horizon_s)
            co += pend * rate0 * limit / 3600
            states.append("PENDING")
        out.append({"native_id": base, "name": first.name, "state": _phase_state(states), "charged": round(ch, 6),
                    "committed": round(co, 6), "rate": rate0})
    for base in view.squeue_only():   # squeue lists it, sacct does not: an unstarted array, one not recorded yet
        if base in known or base in excluded:
            continue
        x = view.extra.get(base) or {}
        limit_s, cpus = view.queued.get(base, (None, None))
        raw_limit = x.get("limit_s") or limit_s
        sq_limit = float(raw_limit) if raw_limit else horizon_s
        rate = float(x.get("rate") or cpus or 1.0)
        n = view.pending.get(base, 0) + view.units.get(base, 0)
        state = view.live.get(base, "PENDING")
        out.append({"native_id": base, "name": x.get("name") or view.names.get(base, ""), "state": state,
                    "charged": 0.0, "committed": round(max(n, 1) * rate * sq_limit / 3600, 6), "rate": rate})
    return out


def journal_tick(*, envelope_id: str | None, jobs: int, sacct_calls: int, source: str) -> None:
    """The journal's `meter.tick` row (HPC_BRIDGE_JOURNAL): the harness grades one scheduler read per tick from it."""
    from .batch import _journal

    _journal({"event": "meter.tick", "envelope_id": envelope_id, "jobs": jobs, "sacct_calls": sacct_calls,
              "source": source})
