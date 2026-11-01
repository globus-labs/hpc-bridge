"""Batch phase 1 (vault `Planned/Batch and swarm execution.md` §10): what the seven batch scenarios share — the script
steps that drive a batch, the graders that read a trace, and the world checks that read the cluster and the ledger.

Pure and SDK-free (scenarios import it on the host, as they import invariants): sqlite3 is the standard library's.

The tool surface the graders read (the stage-A checkpoint fixes it):
- `run_batch(command, rows, task_walltime_s, results_dir=None, session_id="default", shape="compute", idempotent=False,
  confirm_spend=False, batch_id=None)` and `status(id, detail="summary"|"failed"|"page", offset=0, limit=50,
  wait_s=0)` answer a BatchStatus: `batch_id`, `phase` (admitting | waiting | needs_confirmation | needs_account | done
  | cancelled | refused), `tasks`, `capacity`, `admitted`, `counts` {backlog, in_flight, done, failed, lost,
  presumed_lost, submit_unknown, cancelled}, `results` [{index, state, attempt, exit_code, tail, stdout_path,
  stdout_bytes, truncated, job_id, host, started_at, settled_by, task_id}], `results_dir`, `blocks_seen`, `owner`,
  `reason`, `block`, `notice`.
- `cancel(id, mode="drain"|"abort")` answers {id, status: down | draining | refused, backlog_dropped, in_flight,
  blocks_left: [{job_id, bill_until, how_to_end}], notice}.
- The journal's event rows (`Trace.events`): {"event": "batch.admit" | "batch.settle" | "batch.pause" | "batch.resume",
  "ts", "pid", "batch_id", "index", "attempt", "task_id", "in_flight", "capacity", "remaining_s", "need_s", "job_id",
  "state", "reason"}.
- The wrapper's files on the facility, in the batch's own dir (`results_dir` in its answers: `<base>/<batch id>`):
  `<dir>/tasks/<index>/attempt-<n>/meta.json` (written when the attempt
  STARTS: index, attempt, job, host, start, remaining_s), `…/rc`, `…/stdout`, `…/stderr`.

MAIN (no batch tools): a script whose `run_batch` meets "Unknown tool" falls back to what an agent could do today —
the rows as concurrent `run_shell` calls (`MainFanout`), each writing the same files — so a cell is red on main on its
own graders (the window, the margin, the re-ask, the rows after a restart), not on the missing tool alone.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from invariants import Result, ToolCall, Trace, _billed_start_idxs, _shape

TERMINAL = ("done", "failed", "lost", "cancelled")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_UNKNOWN_TOOL = re.compile(r"unknown tool|tool .*not found|no such tool", re.I)
STARTED = "HPCB_STARTED"   # the main fallback's own start line: `HPCB_STARTED <index> <epoch> <job> <remaining_s>`


# ---- reading a trace -------------------------------------------------------------------------------------------------

def endpoint_ids(t: Trace) -> list[str]:
    out: list[str] = []
    for c in t.calls:
        eid = str((c.result or {}).get("endpoint_id") or "")
        if _UUID.fullmatch(eid) and eid not in out:
            out.append(eid)
    return out


def unknown_tool(r: dict | None) -> bool:
    """The tool does not exist on this server (main): an answer that is only text naming an unknown tool. FastMCP 1.30
    answers an unknown tool with a plain text result, not an error one (live 2026-10-31: `{"text": "Unknown tool:
    run_batch"}`), so the text decides, on an answer that carries no batch."""
    return bool(r and not r.get("batch_id") and not r.get("phase")
                and _UNKNOWN_TOOL.search(str(r.get("text") or "")))


def batch_answers(t: Trace, bid: str | None = None) -> list[tuple[int, ToolCall]]:
    """Every run_batch / status answer that is a BatchStatus (for `bid`, when given)."""
    out = []
    for i, c in t.named("run_batch", "status"):
        r = c.result or {}
        if r.get("batch_id") and (bid is None or r.get("batch_id") == bid):
            out.append((i, c))
    return out


def batch_ids(t: Trace) -> list[str]:
    out: list[str] = []
    for _i, c in batch_answers(t):
        b = str((c.result or {}).get("batch_id"))
        if b not in out:
            out.append(b)
    return out


def rows_seen(t: Trace, bid: str | None = None) -> dict[int, dict]:
    """index -> the latest row a run_batch / status answer listed for it (later answers win)."""
    rows: dict[int, dict] = {}
    for _i, c in batch_answers(t, bid):
        for r in (c.result or {}).get("results") or []:
            if isinstance(r, dict) and isinstance(r.get("index"), int):
                rows[r["index"]] = r
    return rows


def events(t: Trace, kind: str | None = None, bid: str | None = None) -> list[dict]:
    return [e for e in getattr(t, "events", []) or []
            if (kind is None or e.get("event") == kind) and (bid is None or e.get("batch_id") == bid)]


@dataclass
class Window:
    """The most work hpc-bridge had out at once: batch tasks admitted and not settled (the journal's events), plus
    compute run_shell commands in their sync-wait or behind a poll handle (the trace) — what an agent looping
    run_shell puts on the block."""
    peak: int = 0
    at: str = ""
    capacity: int | None = None
    sources: list[str] = field(default_factory=list)


def _shell_intervals(t: Trace) -> list[tuple[float, float, str]]:
    """(start, end, label) of every compute run_shell's command: from its call to its result, or — handed back as a
    running task — to the poll that resolved it (the end of the run if none did)."""
    end_of_run = max([c.t_result or c.t_call or 0.0 for c in t.calls] + [0.0])
    resolved: dict[str, float] = {}
    for _i, c in t.named("poll_task"):
        r = c.result or {}
        if str(r.get("phase")) in ("complete", "failed") and c.input.get("task_id") and c.t_result is not None:
            resolved.setdefault(str(c.input["task_id"]), c.t_result)
    out = []
    for i, c in t.named("run_shell"):
        if _shape(c) != "compute" or c.t_call is None:
            continue
        r = c.result or {}
        phase = str(r.get("phase"))
        if phase == "running" and r.get("task_id"):
            out.append((c.t_call, resolved.get(str(r["task_id"]), end_of_run), f"run_shell call {i}"))
        elif phase in ("complete", "failed"):
            out.append((c.t_call, c.t_result if c.t_result is not None else c.t_call, f"run_shell call {i}"))
    return out


def window(t: Trace, bid: str | None = None) -> Window:
    """The peak of work out at once (see Window), per the events' own order and the calls' server-side stamps."""
    w = Window()
    marks: list[tuple[float, int, str]] = []
    out: set[tuple[Any, ...]] = set()
    for e in events(t, bid=bid):
        key = (e.get("batch_id"), e.get("index"), e.get("attempt"))
        if e.get("event") == "batch.admit":
            out.add(key)
            if isinstance(e.get("capacity"), int):
                w.capacity = max(w.capacity or 0, e["capacity"])
            if len(out) > w.peak:
                w.peak, w.at = len(out), f"batch.admit of row {e.get('index')} (pid {e.get('pid')})"
        elif e.get("event") == "batch.settle":
            out.discard(key)
    if events(t, bid=bid):
        w.sources.append("journal events")
    for start, end, label in _shell_intervals(t):
        marks += [(start, 1, label), (end, -1, label)]
    if marks:
        w.sources.append("run_shell calls")
        live = 0
        for _ts, step, label in sorted(marks, key=lambda m: (m[0], m[1])):
            live += step
            if live > w.peak:
                w.peak, w.at = live, label
    return w


def said(t: Trace) -> str:
    return "\n".join(t.texts)


def epoch(t: Trace, rel: float | None) -> float | None:
    """A call's stamp (`t_call` / `t_result`, seconds from the run's first hpc-bridge call) on the epoch clock the
    events' `ts` and the results' times use."""
    return None if rel is None or t.t0 is None else t.t0 + rel


# ---- the script's steps ----------------------------------------------------------------------------------------------

LOGIN_UP_WAIT_S = 300.0
UP_WAIT_S = 420.0
POLL_S = 5.0


def _brief(r: dict) -> str:
    return str(r.get("notice") or r.get("reason") or r.get("text") or "")[:240]


def proposal_details(r: dict, **over: Any) -> dict:
    """The probe's proposal as the confirming details=: env_setup and partitions left out (the server keeps them)."""
    d = {k: v for k, v in dict(r.get("proposed_details") or {}).items()
         if v is not None and k not in ("env_setup", "partitions")}
    d.update({k: v for k, v in over.items() if v is not None})
    return d


async def connect_byo(ops, **details: Any) -> dict:
    """connect_facility from the probe's proposal (with `details` over it, e.g. walltime="00:04:00") until the login
    shape is up (needs_account)."""
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {_brief(r)!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=proposal_details(r, **details))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(POLL_S)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    return r


async def reconnect(ops) -> dict:
    """A fresh server's connect to the facility a previous one bound (the cached config, zero SSH reuse)."""
    r = await ops.call("connect_facility", facility=ops.facility, ssh_host=ops.ssh_host)
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") not in ("needs_account",):
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the reconnect did not bind: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(POLL_S)
        r = await ops.call("connect_facility", facility=ops.facility, ssh_host=ops.ssh_host)
    return r


async def block_up(ops, **args: Any) -> dict:
    """ensure_endpoint_up(confirm_spend=True) — the one confirmation — then polls until the block is up."""
    e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True, **args)
    deadline = time.monotonic() + UP_WAIT_S
    while e.get("status") != "up":
        if e.get("is_error") or e.get("status") not in ("provisioning", "up") or time.monotonic() > deadline:
            ops.abort(f"the block did not come up: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute", **args)
    return e


async def stop(ops, tries: int = 4, *, final_draining: bool = False) -> dict:
    """stop_endpoint, again while it refuses (`up`: work still holds the block) or — on an SSH endpoint — drains;
    `final_draining`: a facility MEP, where `draining` is the final answer."""
    s = await ops.call("stop_endpoint")
    for _ in range(tries):
        status = s.get("status")
        if status not in ("draining", "up") or (status == "draining" and final_draining):
            break
        await ops.sleep(POLL_S * 2)
        s = await ops.call("stop_endpoint")
    return s


def results_dir_for(scenario: str) -> str:
    """A results directory of this run on the shared home (the fake cluster has no scratch filesystem): the run's
    endpoint name keeps cells apart."""
    name = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or f"run-{int(time.time())}"
    return f"$HOME/hpcb-batches/{name}/{scenario}"


async def wait_batch(ops, bid: str, *, until, timeout_s: float = 900.0, wait_s: float = 30.0) -> dict:
    """status(bid, wait_s=…) until `until(answer)` (or the timeout: the last answer)."""
    deadline = time.monotonic() + timeout_s
    st = await ops.call("status", id=bid, wait_s=wait_s)
    while not until(st) and time.monotonic() < deadline:
        if st.get("is_error"):
            await ops.sleep(POLL_S)
        st = await ops.call("status", id=bid, wait_s=wait_s)
    return st


async def all_rows(ops, bid: str, n: int) -> dict:
    """status(detail="page") until every row is listed (one page per call)."""
    got: dict[int, dict] = {}
    offset, last = 0, {}
    while offset is not None and len(got) < n:
        last = await ops.call("status", id=bid, detail="page", offset=offset, limit=100)
        for r in last.get("results") or []:
            if isinstance(r, dict) and isinstance(r.get("index"), int):
                got[r["index"]] = r
        offset = last.get("next_offset")
    return {"rows": got, "last": last}


# ---- main's fallback: the rows as concurrent run_shell calls ----------------------------------------------------------

def main_row_command(command: str, row: dict, index: int, results_dir: str, attempt: int = 1) -> str:
    """What one row runs on main (no wrapper): the same start file the wrapper writes (meta.json, with the block's
    remaining walltime from inside it), the command, its rc — so the world checks read main and the batch alike."""
    cmd = re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}",
                 lambda m: shlex.quote(str(row[m.group(1)])) if m.group(1) in row else
                 (str(index) if m.group(1) == "index" else m.group(0)), command)
    d = f"{results_dir}/tasks/{index}/attempt-{attempt}"
    return (
        f'd="{d}"; mkdir -p "$d"; j="${{SLURM_JOB_ID:-${{PBS_JOBID:-}}}}"; now=$(date +%s); '
        'if [ -n "${SLURM_JOB_END_TIME:-}" ]; then rem=$((SLURM_JOB_END_TIME-now)); else rem=null; fi; '
        f'printf \'{{"index": {index}, "attempt": {attempt}, "job": "%s", "host": "%s", "start": %s, "remaining_s": %s}}\\n\' '
        '"$j" "$(hostname)" "$now" "$rem" > "$d/meta.json"; '
        f'echo "{STARTED} {index} $now $j $rem"; '
        f'( {cmd} ) > "$d/stdout" 2> "$d/stderr"; rc=$?; echo $rc > "$d/rc"; cat "$d/stdout"; exit $rc'
    )


@dataclass
class MainFanout:
    """The outcome of main's fallback: the run_shell answers, by row."""
    results: dict[int, dict] = field(default_factory=dict)
    handles: dict[int, str] = field(default_factory=dict)


async def main_fanout(ops, command: str, rows: list[dict], results_dir: str, *, poll_until_s: float = 900.0,
                      after_submit=None, attempt: int = 1) -> MainFanout:
    """MAIN: every row as its own run_shell (session `b<index>`, so the busy guard does not refuse them), sent at once
    as an agent's parallel calls; the running ones are polled to their end. `after_submit(ops, fan)` runs once every row
    was sent (a restart, a stop)."""
    fan = MainFanout()
    ops.say(f"MAIN FALLBACK: run_batch is not a tool here; {len(rows)} rows as concurrent run_shell calls "
            f"(results dir {results_dir})")
    answers = await asyncio.gather(*(ops.call("run_shell", shape="compute", session_id=f"b{i}",
                                               command=main_row_command(command, row, i, results_dir, attempt))
                                     for i, row in enumerate(rows)))
    for i, r in enumerate(answers):
        if r.get("phase") == "running" and r.get("task_id"):
            fan.handles[i] = str(r["task_id"])
        else:
            fan.results[i] = r
    if after_submit is not None:
        await after_submit(ops, fan)
    deadline = time.monotonic() + poll_until_s
    while fan.handles and time.monotonic() < deadline:
        for i, tid in list(fan.handles.items()):
            p = await ops.call("poll_task", task_id=tid, wait=10)
            if p.get("phase") != "running":
                fan.results[i] = p
                fan.handles.pop(i)
    return fan


async def fanout(ops, command: str, rows: list[dict], task_walltime_s: float, *, results_dir: str | None = None,
                 idempotent: bool = False, confirm_spend: bool = False, main_after_submit=None,
                 main_poll_until_s: float = 900.0) -> tuple[dict, MainFanout | None]:
    """run_batch — or, where it is no tool (main), `main_fanout`. Returns (the run_batch answer, the fallback's
    outcome or None)."""
    args: dict[str, Any] = {"command": command, "rows": rows, "task_walltime_s": task_walltime_s}
    if results_dir:
        args["results_dir"] = results_dir
    if idempotent:
        args["idempotent"] = True
    if confirm_spend:
        args["confirm_spend"] = True
    r = await ops.call("run_batch", **args)
    if not unknown_tool(r):
        if r.get("batch_id"):
            ops.say(f"HPCB_RESULTS_DIR {r.get('results_dir') or results_dir}")
        return r, None
    rd = results_dir or results_dir_for("batch")
    ops.say(f"HPCB_RESULTS_DIR {rd}")
    return r, await main_fanout(ops, command, rows, rd, after_submit=main_after_submit,
                                poll_until_s=main_poll_until_s)


def results_dir_of(t: Trace) -> str | None:
    """The results directory the run used: the script's own `HPCB_RESULTS_DIR <path>` line, else a run_batch answer's."""
    for text in t.texts:
        m = re.search(r"HPCB_RESULTS_DIR (\S+)", text)
        if m and m.group(1) != "None":
            return m.group(1)
    for _i, c in batch_answers(t):
        if (c.result or {}).get("results_dir"):
            return str(c.result["results_dir"])
    return None


def started_lines(t: Trace) -> dict[int, tuple[float, str, float | None]]:
    """MAIN's start lines in the results: index -> (epoch, job, remaining_s)."""
    out: dict[int, tuple[float, str, float | None]] = {}
    pat = re.compile(rf"^{STARTED} (\d+) (\d+) (\S*) (\S+)$", re.M)
    for _i, c in t.named("run_shell", "poll_task"):
        for m in pat.finditer(str((c.result or {}).get("stdout") or "")):
            rem = None if m.group(4) == "null" else float(m.group(4))
            out[int(m.group(1))] = (float(m.group(2)), m.group(3), rem)
    return out


def row_starts(t: Trace) -> dict[int, float]:
    """index -> when the row's (latest) attempt started on the block: the batch's own results (`started_at`), else
    main's start lines."""
    out = {i: float(r["started_at"]) for i, r in rows_seen(t).items() if isinstance(r.get("started_at"), int | float)}
    for i, (ts, _job, _rem) in started_lines(t).items():
        out.setdefault(i, ts)
    return out


# ---- graders ---------------------------------------------------------------------------------------------------------

def _accepted(t: Trace) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in t.named("run_batch")
            if (c.result or {}).get("batch_id") and str((c.result or {}).get("phase")) != "refused"]


def batch_used(t: Trace, *, rows: int) -> Result:
    """The fan-out went through run_batch — one accepted batch — not one run_shell per row: at most a handful of
    compute run_shell calls."""
    shells = [i for i, c in t.named("run_shell") if _shape(c) == "compute"]
    got = _accepted(t)
    ok = bool(got) and len(shells) < max(5, rows // 4)
    if not got:
        why = "no run_batch was accepted" + (" (run_batch is not a tool on this server)" if any(
            unknown_tool(c.result) for _i, c in t.named("run_batch")) else "")
    else:
        why = f"run_batch accepted at call {got[0][0]} ({(got[0][1].result or {}).get('batch_id')})"
    return Result("batch_used", ok, f"{'ok: ' if ok else ''}{why}; {len(shells)} compute run_shell call(s) for {rows} rows")


def one_confirmation(t: Trace) -> Result:
    """Exactly ONE block was confirmed (a billed start whose answer carries the confirmation's receipt): the batch rode
    the session's block, it asked for no other."""
    starts = [k for k in _billed_start_idxs(t) if (t.calls[k].result or {}).get("block")]
    ok = len(starts) == 1
    return Result("one_confirmation", ok, f"{len(starts)} confirmation(s) with a receipt (calls {starts})")


def window_within_capacity(t: Trace, *, workers: int) -> Result:
    """The journal never shows more batch work out than the window, and the window never more than the block's workers
    (less the one kept for interactive work, once there are two or more)."""
    w = window(t)
    cap = w.capacity if w.capacity is not None else max(1, workers - 1 if workers >= 2 else 1)
    ok = w.peak <= cap <= workers and bool(w.sources)
    return Result("window_within_capacity", ok,
                  f"peak {w.peak} out at once ({w.at or 'nothing'}; from {', '.join(w.sources) or 'no record'}), "
                  f"window {cap}, workers {workers}")


def failures_have_rc(t: Trace, *, failing: dict[int, int]) -> Result:
    """Every seeded failure is reported as `failed` with its own exit code (status / run_batch results; main: the
    run_shell answers)."""
    rows = rows_seen(t)
    shells: dict[int, dict] = {}
    for _i, c in t.named("run_shell", "poll_task"):
        m = re.search(rf"^{STARTED} (\d+) ", str((c.result or {}).get("stdout") or ""), re.M)
        if m:
            shells[int(m.group(1))] = c.result or {}
    bad = []
    for idx, rc in failing.items():
        r = rows.get(idx)
        if r is not None:
            if r.get("state") != "failed" or r.get("exit_code") != rc:
                bad.append(f"row {idx}: {r.get('state')} rc={r.get('exit_code')}")
        elif idx in shells:
            if shells[idx].get("exit_code") != rc:
                bad.append(f"row {idx}: run_shell rc={shells[idx].get('exit_code')}")
            else:
                bad.append(f"row {idx}: only a run_shell answer, no batch row")
        else:
            bad.append(f"row {idx}: never reported")
    return Result("failures_have_rc", not bad, "ok: " + ", ".join(f"{i}→{rc}" for i, rc in failing.items())
                  if not bad else "; ".join(bad))


def agent_reports_failures(t: Trace, *, failing: dict[int, int], label: str = "n") -> Result:
    """The agent's final words name each failed row with its exit code."""
    text = said(t)
    missing = [i for i, rc in failing.items()
               if not re.search(rf"(?<![0-9]){i}(?![0-9])", text) or not re.search(rf"(?<![0-9]){rc}(?![0-9])", text)]
    return Result("agent_reports_failures", not missing,
                  "ok: every failure named with its rc" if not missing else f"not named with an rc: {label}={missing}")


def stop_down(t: Trace) -> Result:
    stops = t.named("stop_endpoint")
    if not stops:
        return Result("stop_down", False, "no stop_endpoint call")
    r = stops[-1][1].result or {}
    ok = r.get("status") == "down"
    return Result("stop_down", ok, f"the last stop answered {r.get('status')!r} (spend_stopped={r.get('spend_stopped')!r})")


def no_admission_under_margin(t: Trace) -> Result:
    """Every admission came with the block's remaining walltime at least task_walltime_s + the margin (the events'
    own `remaining_s` / `need_s`); `unknown` admits nothing. MAIN: no admission was ever gated — the start lines' own
    remaining walltime is graded the same (a row started with less than its walltime left)."""
    adm = events(t, "batch.admit")
    if adm:
        bad = [e for e in adm if not isinstance(e.get("remaining_s"), int | float)
               or not isinstance(e.get("need_s"), int | float) or e["remaining_s"] < e["need_s"]]
        return Result("no_admission_under_margin", not bad,
                      f"{len(adm)} admission(s), " + ("all with enough walltime left" if not bad else
                                                      "under the margin: " + ", ".join(
                                                          f"row {e.get('index')} at {e.get('remaining_s')}s < "
                                                          f"{e.get('need_s')}s" for e in bad[:5])))
    starts = started_lines(t)
    if not starts:
        return Result("no_admission_under_margin", False, "no admission recorded (no batch.admit event, no start line)")
    walltime = _task_walltime(t)
    late = sorted(i for i, (_ts, _j, rem) in starts.items() if rem is None or (walltime and rem < walltime))
    return Result("no_admission_under_margin", False,
                  f"MAIN: no admission gate at all; rows started with under {walltime}s left: {late or 'none seen'}")


def _task_walltime(t: Trace) -> float | None:
    for _i, c in t.named("run_batch"):
        v = c.input.get("task_walltime_s")
        if isinstance(v, int | float):
            return float(v)
    return None


def reask_named(t: Trace, *, words: Iterable[str], name: str) -> Result:
    """A batch answer paused at needs_confirmation with a reason naming one of `words` (the block's end: walltime, a
    reap, a restart), and nothing was admitted after it until a run_batch(batch_id, confirm_spend=True)."""
    pat = re.compile("|".join(map(re.escape, words)), re.I)
    asks = [(i, c) for i, c in batch_answers(t)
            if str((c.result or {}).get("phase")) == "needs_confirmation"
            and pat.search(str((c.result or {}).get("reason") or "") + " " + str((c.result or {}).get("notice") or ""))]
    if not asks:
        return Result(name, False, f"no batch answer asked again naming {list(words)}")
    i, c = asks[0]
    ask_ts = epoch(t, c.t_result)
    resumes = [(k, x) for k, x in t.named("run_batch")
               if k > i and x.input.get("batch_id") and str(x.input.get("confirm_spend")).lower() == "true"]
    resume_ts = epoch(t, resumes[0][1].t_call) if resumes else None
    early = [e for e in events(t, "batch.admit") if ask_ts is not None and e.get("ts", 0) > ask_ts
             and (resume_ts is None or e.get("ts", 0) < resume_ts)]
    ok = not early
    return Result(name, ok, f"asked at call {i}: {str((c.result or {}).get('reason'))[:160]!r}; "
                  + (f"{len(early)} admission(s) between the question and the yes" if early else
                     "nothing admitted before the yes" + (f" (call {resumes[0][0]})" if resumes else " (no resume)")))


def every_row_returned(t: Trace, *, rows: int, after: int | None = None) -> Result:
    """The status answers (after call `after`, e.g. the restart) list every row, each settled."""
    got: dict[int, dict] = {}
    for i, c in batch_answers(t):
        if after is not None and i < after:
            continue
        for r in (c.result or {}).get("results") or []:
            if isinstance(r, dict) and isinstance(r.get("index"), int):
                got[r["index"]] = r
    missing = sorted(set(range(rows)) - set(got))
    open_ = sorted(i for i, r in got.items() if r.get("state") not in TERMINAL)
    ok = not missing and not open_
    return Result("every_row_returned", ok, f"{len(got)}/{rows} rows listed" + (f"; missing {missing[:10]}" if missing
                  else "") + (f"; not settled {open_[:10]}" if open_ else ""))


def restart_index(t: Trace) -> int | None:
    """The first call after the script's restart (its words say `killed (SIGKILL`)."""
    for text, at in zip(t.texts, t.text_at, strict=False):
        if "killed (SIGKILL" in text:
            return at
    return None


def nothing_admitted_before_resume(t: Trace) -> Result:
    """After the restart, the new server admitted nothing until run_batch(batch_id, confirm_spend=True): a plain
    status never resumes spend (spend_confirmed lived in the old process)."""
    k = restart_index(t)
    if k is None:
        return Result("nothing_admitted_before_resume", False, "premise not met: no restart in the run")
    new_ts = epoch(t, t.calls[k].t_call) if k < len(t.calls) else None
    resume = next((x for j, x in t.named("run_batch") if j >= k and x.input.get("batch_id")
                   and str(x.input.get("confirm_spend")).lower() == "true"), None)
    resume_ts = epoch(t, resume.t_call) if resume is not None else None
    early = [e for e in events(t, "batch.admit") if new_ts is not None and e.get("ts", 0) >= new_ts
             and (resume_ts is None or e.get("ts", 0) < resume_ts)]
    first = next((x for j, x in t.named("status") if j >= k), None)
    asked = first is not None and str((first.result or {}).get("phase")) == "needs_confirmation"
    ok = not early and asked
    return Result("nothing_admitted_before_resume", ok,
                  f"first status after the restart: {(first.result or {}).get('phase') if first else 'none'}; "
                  f"{len(early)} admission(s) before the resume")


def only_holder_admits(t: Trace) -> Result:
    """Two servers on one state directory: every admission came from ONE process (the lease holder), and the other
    server's run_batch on that batch was refused before it started anything."""
    adm = events(t, "batch.admit")
    pids = sorted({e.get("pid") for e in adm})
    b_calls = [(i, c) for i, c in t.named("run_batch") if c.server == "B"]
    b_started = [k for k in _billed_start_idxs(t) if t.calls[k].server == "B"]
    b_refused = all(str((c.result or {}).get("phase")) == "refused" for _i, c in b_calls) and bool(b_calls)
    shells_b = [i for i, c in t.named("run_shell") if c.server == "B" and _shape(c) == "compute"]
    ok = len(pids) == 1 and b_refused and not b_started and not shells_b
    return Result("only_holder_admits", ok,
                  f"admissions from pid(s) {pids or 'none'}; server B: {len(b_calls)} run_batch "
                  f"({'all refused' if b_refused else 'NOT all refused'}), {len(b_started)} billed start(s), "
                  f"{len(shells_b)} compute run_shell call(s)")


def second_server_reads_ledger(t: Trace) -> Result:
    """Server B's status found the batch server A runs (from the ledger), with its rows counted."""
    for i, c in t.named("status"):
        r = c.result or {}
        if c.server == "B" and r.get("batch_id") and isinstance(r.get("counts"), dict) and r.get("tasks"):
            return Result("second_server_reads_ledger", True, f"ok: call {i} saw {r.get('tasks')} rows, "
                          f"owner {r.get('owner')!r}")
    return Result("second_server_reads_ledger", False, "server B's status never read the batch")


def inflight_lost_with_evidence(t: Trace) -> Result:
    """The rows in flight when the block was reaped are settled `lost` on evidence (a result, sacct, the manager's
    log) — never `done`, and never left in flight."""
    rows = rows_seen(t)
    lost = {i: r for i, r in rows.items() if r.get("state") == "lost"}
    if not lost:
        return Result("inflight_lost_with_evidence", False, "no row was settled lost")
    bare = [i for i, r in lost.items() if not str(r.get("settled_by") or "").startswith(("evidence", "result"))]
    stuck = sorted(i for i, r in rows.items() if r.get("state") in ("in_flight", "submit_unknown"))
    ok = not bare and not stuck
    return Result("inflight_lost_with_evidence", ok,
                  f"lost: {sorted(lost)} ({', '.join(sorted({str(r.get('settled_by')) for r in lost.values()}))})"
                  + (f"; settled by no evidence: {bare}" if bare else "") + (f"; still open: {stuck}" if stuck else ""))


def backlog_survives(t: Trace, *, rows: int) -> Result:
    """After the reap the backlog is still there (not dropped or cancelled) — the answer that asked again counted it —
    and every row is accounted for."""
    asks = [c for _i, c in batch_answers(t) if str((c.result or {}).get("phase")) == "needs_confirmation"]
    if not asks:
        return Result("backlog_survives", False, "no batch answer paused after the reap")
    counts = (asks[0].result or {}).get("counts") or {}
    total = sum(int(v) for v in counts.values() if isinstance(v, int))
    ok = counts.get("backlog", 0) > 0 and counts.get("cancelled", 0) == 0 and total == rows
    return Result("backlog_survives", ok, f"at the re-ask: {counts} (total {total} of {rows})")


def nothing_admitted_after_stop(t: Trace) -> Result:
    """Nothing was admitted once stop_endpoint was called, and no row STARTED after the stop's answer unless it was
    admitted before the stop (main: every row was already sent, and the queued ones start after it)."""
    stops = t.named("stop_endpoint")
    if not stops:
        return Result("nothing_admitted_after_stop", False, "no stop_endpoint call")
    first = stops[0][1]
    t_stop = epoch(t, first.t_call)
    late = [e for e in events(t, "batch.admit") if t_stop is not None and e.get("ts", 0) >= t_stop]
    before = {(e.get("index")) for e in events(t, "batch.admit") if t_stop is not None and e.get("ts", 0) < t_stop}
    answer = epoch(t, first.t_result)
    started_after = sorted(i for i, ts in row_starts(t).items() if answer is not None and ts > answer
                           and i not in before)
    ok = not late and not started_after and t_stop is not None
    return Result("nothing_admitted_after_stop", ok, f"{len(late)} admission(s) after the stop; rows started after "
                  f"its answer: {started_after[:10] or 'none'}")


def mep_stop_names_jobs(t: Trace) -> Result:
    """On a MEP the stop is draining and FINAL, spend not claimed stopped, and it names the block job(s) the results
    reported — the rows a page listed, or the `blocks_seen` every batch answer carries (a summary status lists no rows)."""
    stops = t.named("stop_endpoint")
    if not stops:
        return Result("mep_stop_names_jobs", False, "no stop_endpoint call")
    r = stops[-1][1].result or {}
    jobs = {str(x.get("job_id")) for x in rows_seen(t).values() if x.get("job_id")}
    jobs |= {str(j) for _i, c in batch_answers(t) for j in (c.result or {}).get("blocks_seen") or [] if j}
    jobs |= {j for _ts, j, _rem in started_lines(t).values() if j}
    text = f"{r.get('notice') or ''} {r.get('block_job_id') or ''}"
    named = sorted(j for j in jobs if j and j in text)
    ok = r.get("status") == "draining" and r.get("spend_stopped") is not True and bool(named)
    return Result("mep_stop_names_jobs", ok, f"stop: {r.get('status')!r} spend_stopped={r.get('spend_stopped')!r}; "
                  f"block jobs seen {sorted(jobs) or 'none'}, named {named or 'none'}")


def abort_ends_in_flight(t: Trace, *, rows: int, within_s: float = 60.0) -> Result:
    """cancel(abort) ends what is in flight (design §5): the first abort cancels the backlog and answers `draining` while
    a task is out; a later cancel answers `down` within `within_s`; every row that was out settled `cancelled` on the
    CANCELLED marker's evidence — never done (its command was killed, not let run on)."""
    aborts = [(i, c) for i, c in t.named("cancel") if str(c.input.get("mode")) == "abort" and c.result]
    if not aborts:
        why = "no cancel(abort) call"
        if any(unknown_tool(c.result) for _i, c in t.named("cancel")):
            why += " was answered (cancel is not a tool on this server)"
        return Result("abort_ends_in_flight", False, why)
    i0, first = aborts[0]
    r0 = first.result or {}
    downs = [(i, c) for i, c in aborts if (c.result or {}).get("status") == "down"]
    when = None
    if downs and first.t_result is not None and downs[0][1].t_result is not None:
        when = downs[0][1].t_result - first.t_result
    rowsd = rows_seen(t)
    out = [r for r in rowsd.values() if str(r.get("settled_by") or "").find("CANCELLED") >= 0]
    done_after = [r for r in rowsd.values() if r.get("state") == "done" and r.get("index") in {x.get("index") for x in out}]
    ok = (r0.get("status") == "draining" and int(r0.get("in_flight") or 0) > 0 and int(r0.get("backlog_dropped") or 0) > 0
          and bool(downs) and when is not None and when <= within_s and bool(out) and not done_after
          and sum(1 for r in rowsd.values() if r.get("state") == "cancelled") == rows)
    return Result("abort_ends_in_flight", ok,
                  f"first abort (call {i0}): {r0.get('status')!r}, {r0.get('in_flight')} in flight, "
                  f"{r0.get('backlog_dropped')} dropped; down {f'after {when:.0f}s' if when is not None else 'never'}; "
                  f"{len(out)} row(s) settled on the marker; "
                  f"{sum(1 for r in rowsd.values() if r.get('state') == 'cancelled')}/{rows} cancelled")


def big_output_truncated(t: Trace, *, index: int, full: int) -> Result:
    """The big row's stdout (`full` bytes) came back cut — truncated, stored at less than its full size, with a
    pointer to the file and a tail — and nothing anywhere met Globus Compute's result limit."""
    over = [i for i, c in enumerate(t.calls) if "MaxResultSizeExceeded" in json.dumps(c.result or {}, default=str)
            or "over Globus Compute's result limit" in str((c.result or {}).get("notice") or "")]
    r = rows_seen(t).get(index)
    if r is None:
        return Result("big_output_truncated", False, f"row {index} never reported"
                      + (f"; result limit met at call(s) {over}" if over else ""))
    stored = r.get("stdout_bytes")
    ok = (r.get("state") == "done" and r.get("truncated") is True and isinstance(stored, int) and 0 < stored < full
          and str(r.get("stdout_path") or "").endswith(f"tasks/{index}/attempt-{r.get('attempt')}/stdout")
          and bool(r.get("tail")) and not over)
    return Result("big_output_truncated", ok, f"row {index}: state={r.get('state')} truncated={r.get('truncated')} "
                  f"stored={r.get('stdout_bytes')} pointer={r.get('stdout_path')!r}"
                  + (f"; result limit met at call(s) {over}" if over else ""))


# ---- world checks (run.py WORLD_CHECKS: async fn(ctx) after the script, before the teardown) -----------------------

SACCT_FIELDS = "JobID,JobName%60,State,Submit,Start,End,SubmitLine%600"


def _sacct_cmd() -> str:
    return (f'sacct -u "$(whoami)" -X -n -P -S now-1days -E now -s CA,CD,F,PD,R,TO,NF,PR -o {SACCT_FIELDS} 2>&1; '
            'echo "HPCB_RC=$?"; echo "HPCB_NOW $(date +%s) $(date +%Y-%m-%dT%H:%M:%S)"')


@dataclass
class Pilot:
    job: str
    state: str
    submit: float | None
    start: float | None
    end: float | None


def _clock(out: str) -> float:
    """The cluster's offset from UTC, from its own `date` pair (sacct prints local times without a zone)."""
    m = re.search(r"HPCB_NOW (\d+) (\S+)", out)
    if not m:
        return 0.0
    naive = datetime.strptime(m.group(2), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp()
    return float(m.group(1)) - naive


def _ts(text: str, offset: float) -> float | None:
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp() + offset
    except ValueError:
        return None


def pilots_of(out: str, eids: list[str]) -> list[Pilot]:
    """This run's pilots in sacct: parsl jobs whose SubmitLine names one of its endpoints (`uep.<eid>`)."""
    names = re.sub(r"%[0-9]+", "", SACCT_FIELDS).split(",")
    off = _clock(out)
    found = []
    for line in out.splitlines():
        cols = line.split("|")
        if len(cols) < len(names):
            continue
        row = dict(zip(names, cols[: len(names) - 1] + ["|".join(cols[len(names) - 1:])], strict=True))
        if "parsl" in row["JobName"] and any(f"uep.{e}" in row["SubmitLine"] for e in eids):
            found.append(Pilot(row["JobID"], row["State"].split()[0] if row["State"] else "",
                               _ts(row["Submit"], off), _ts(row["Start"], off), _ts(row["End"], off)))
    return sorted(found, key=lambda p: (p.submit or 0, p.job))


def _sacct(ctx) -> tuple[list[Pilot] | None, str]:
    eids = endpoint_ids(ctx.trace)
    if not eids:
        return None, "premise not met: no endpoint id in the run's results"
    rc, out = ctx.ssh(_sacct_cmd(), 60)
    if rc != 0 or "HPCB_RC=0" not in out:
        return None, f"UNVERIFIABLE: sacct over the world channel failed (rc={rc}): {out[-300:]!r}"
    return pilots_of(out, eids), ""


def ledger_path() -> Path:
    """The jail's ledger (the MCP server's host): HPC_BRIDGE_STATE_DIR, else ~/.hpc-bridge."""
    base = os.environ.get("HPC_BRIDGE_STATE_DIR", "").strip() or str(Path.home() / ".hpc-bridge")
    return Path(base).expanduser() / "ledger.sqlite3"


def read_ledger(path: Path, bids: list[str]) -> tuple[dict[str, dict], dict[str, list[dict]]] | None:
    """(batch rows by id, task rows by batch id) — read-only; None when there is no ledger."""
    if not path.exists():
        return None
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        q = ",".join("?" * len(bids)) or "''"
        batches = {r["id"]: dict(r) for r in con.execute(f"select * from batch where id in ({q})", bids)}
        tasks: dict[str, list[dict]] = {}
        for r in con.execute(f"select * from task where batch_id in ({q}) order by idx", bids):
            tasks.setdefault(r["batch_id"], []).append(dict(r))
        return batches, tasks
    finally:
        con.close()


SLACK_S = 20.0


async def ledger_matches_sacct(ctx) -> Result:
    """world: the ledger and the scheduler agree. Every block job a settled row names is a pilot of this run's endpoint
    in sacct, and the row ran within that job's life; and every pilot of the endpoint that RAN while the batch was
    admitting is one the ledger knows (a row names it or it is in the batch's blocks_seen): no block ran batch work
    unledgered."""
    name = "world:ledger_matches_sacct"
    bids = batch_ids(ctx.trace)
    led = read_ledger(ledger_path(), bids)
    if led is None or not bids:
        return Result(name, False, f"no ledger at {ledger_path()}" if led is None else "no batch in the run's results")
    batches, tasks = led
    pilots, why = _sacct(ctx)
    if pilots is None:
        return Result(name, False, why)
    by_job = {p.job: p for p in pilots}
    bad, named = [], set()
    first_admit, last_settle = None, None
    for bid in bids:
        seen = set(json.loads(batches.get(bid, {}).get("blocks_seen") or "[]"))
        named |= seen
        for r in tasks.get(bid, []):
            job = str(r.get("job_id") or "")
            if r.get("admitted_at"):
                first_admit = min(first_admit or r["admitted_at"], r["admitted_at"])
            if r.get("settled_at"):
                last_settle = max(last_settle or r["settled_at"], r["settled_at"])
            if r.get("state") not in ("done", "failed") or not job:
                continue
            named.add(job)
            p = by_job.get(job)
            if p is None:
                bad.append(f"row {r['idx']} names job {job}, no pilot of this endpoint in sacct")
            elif p.start is not None and r.get("started_at") and r["started_at"] + SLACK_S < p.start:
                bad.append(f"row {r['idx']} started before job {job} did")
    ran = [p for p in pilots if p.start is not None and first_admit is not None
           and p.start <= (last_settle or time.time()) + SLACK_S and (p.end is None or p.end + SLACK_S >= first_admit)]
    unknown = [p.job for p in ran if p.job not in named]
    if unknown:
        bad.append(f"pilot(s) {unknown} ran during the batch and the ledger does not know them")
    ok = not bad
    return Result(name, ok, (f"ok: {sum(len(v) for v in tasks.values())} rows, pilots {[p.job for p in pilots]} "
                             f"all known" if ok else "; ".join(bad)[:600]))


async def one_block_job(ctx) -> Result:
    """world: the batch ran on ONE block — sacct lists exactly one pilot of this run's endpoint."""
    pilots, why = _sacct(ctx)
    if pilots is None:
        return Result("world:one_block_job", False, why)
    return Result("world:one_block_job", len(pilots) == 1,
                  f"{len(pilots)} pilot(s): " + ", ".join(f"{p.job} {p.state}" for p in pilots))


def _confirm_times(t: Trace) -> list[float]:
    """When each NEW block was consented to (epoch): the billed starts whose answer carries the receipt
    (ensure_endpoint_up's, a new run_batch's), and every resume — run_batch(batch_id=…, confirm_spend=True), the user's
    yes to the next block, which the batch may ask for only once its tasks on the block in front have finished."""
    out = []
    for k in _billed_start_idxs(t):
        c = t.calls[k]
        at = epoch(t, c.t_call)
        if at is not None and ((c.result or {}).get("block") or (c.name == "run_batch" and c.input.get("batch_id"))):
            out.append(at)
    return sorted(out)


async def no_job_before_reask(ctx) -> Result:
    """world: no pilot of this endpoint was SUBMITTED without its own confirmation before it: the k-th pilot's submit
    follows the k-th confirmed block (a walltime crossing asks again; parsl must not start a block for a task left at
    the interchange, #204)."""
    name = "world:no_job_before_reask"
    pilots, why = _sacct(ctx)
    if pilots is None:
        return Result(name, False, why)
    confirms = _confirm_times(ctx.trace)
    bad = []
    for k, p in enumerate(pilots):
        if k >= len(confirms):
            bad.append(f"pilot {p.job} (#{k + 1}) has no confirmation of its own ({len(confirms)} confirmation(s))")
        elif p.submit is not None and p.submit + 2 < confirms[k]:
            bad.append(f"pilot {p.job} was submitted {confirms[k] - p.submit:.0f}s before confirmation #{k + 1}")
    return Result(name, not bad, ("ok: " if not bad else "") + f"{len(pilots)} pilot(s), {len(confirms)} confirmation(s)"
                  + ("; " + "; ".join(bad) if bad else ""))


def _meta_cmd(rd: str) -> str:
    return (f'for f in {rd}/tasks/*/attempt-*/meta.json; do [ -f "$f" ] && echo "HPCB_META $f $(tr -d "\\n" < "$f")"; '
            'done; echo HPCB_DONE')


def read_metas(ctx) -> tuple[list[dict] | None, str]:
    rd = results_dir_of(ctx.trace)
    if not rd:
        return None, "premise not met: the run named no results directory"
    rc, out = ctx.ssh(_meta_cmd(rd), 60)
    if "HPCB_DONE" not in out:
        return None, f"UNVERIFIABLE: could not list {rd} (rc={rc}): {out[-200:]!r}"
    metas = []
    for m in re.finditer(r"^HPCB_META (\S+) (\{.*\})$", out, re.M):
        try:
            metas.append({**json.loads(m.group(2)), "_path": m.group(1)})
        except ValueError:
            continue
    return metas, ""


async def tasks_started_with_margin(ctx) -> Result:
    """world: every attempt STARTED on the block with at least its task walltime left (the wrapper's own reading at
    its start, from inside the block)."""
    name = "world:tasks_started_with_margin"
    metas, why = read_metas(ctx)
    if metas is None:
        return Result(name, False, why)
    walltime = _task_walltime(ctx.trace) or 0.0
    short = [m for m in metas if not isinstance(m.get("remaining_s"), int | float) or m["remaining_s"] < walltime]
    return Result(name, bool(metas) and not short, f"{len(metas)} attempt(s); started with under {walltime:g}s left: "
                  + (", ".join(f"row {m.get('index')} ({m.get('remaining_s')}s)" for m in short[:8]) or "none"))


async def no_row_ran_twice(ctx) -> Result:
    """world: each row's command started at most once (one attempt with a start file per row): a duplicate attempt
    claim exits before it runs anything."""
    name = "world:no_row_ran_twice"
    metas, why = read_metas(ctx)
    if metas is None:
        return Result(name, False, why)
    per: dict[int, int] = {}
    for m in metas:
        if isinstance(m.get("index"), int):
            per[m["index"]] = per.get(m["index"], 0) + 1
    twice = sorted(i for i, n in per.items() if n > 1)
    return Result(name, bool(per) and not twice, f"{len(per)} row(s) started; more than once: {twice or 'none'}")


async def big_output_on_disk(ctx, *, index: int) -> Result:
    """world: the big row's stdout is on the facility at the pointer its result names, cut at the size the result
    reports (the cap: what was kept, not the whole stream)."""
    name = "world:big_output_on_disk"
    r = rows_seen(ctx.trace).get(index) or {}
    path, stored = r.get("stdout_path"), r.get("stdout_bytes")
    if not path:
        return Result(name, False, f"row {index} carries no pointer")
    _rc, out = ctx.ssh(f"wc -c < {path} 2>&1; echo HPCB_RC=$?", 60)
    m = re.match(r"\s*(\d+)", out)
    size = int(m.group(1)) if m else None
    return Result(name, size is not None and size == stored, f"{path}: {size} bytes on disk, {stored} reported")
