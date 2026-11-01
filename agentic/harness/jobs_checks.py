"""Batch phase 2a (vault `Planned/Batch and swarm execution.md` §2, §3, §10 "Phase 2", §12 D2): what the Jobs-mode and
envelope scenarios share — the script steps that drive submit_job, the graders that read a trace, and the world checks
that read the cluster and the ledger. Pure and SDK-free, like batch_checks (scenarios import it on the host).

The tool surface the graders read (the stage-A checkpoint fixes it):
- `submit_job(command, session_id, account, partition, nodes, cores, memory_gb, gpus, walltime, array, depends_on,
  adopt, envelope={cap, deadline_hours, max_jobs, max_nodes_per_block, accounts, partitions, at_deadline,
  site_backstop}, confirm_spend)` and `status("job-…" | "env-…")` answer a JobStatus: `phase` (needs_confirmation |
  needs_account | refused | queued | running | done | failed | cancelled; for an envelope open | exhausted | draining |
  closed), `reason` (envelope_needed | envelope_exhausted | envelope_new_session | envelope_ceiling | mep | …), `job`
  {job_id, native_id, source, state, exit_code, reason, charged, committed, worst_case, unit, meter_source,
  array_counts, script_path, script_hash, deadline}, `terms` {account, partition, nodes, walltime, tasks, throttle,
  rate, unit, worst_case, basis, min_charge_h, deadline, binds, site_backstop}, `envelope` {envelope_id, state, cap,
  spent, committed, remaining, unit, deadline, owned, site_backstop, job_ids, jobs, unledgered [{native_id, name,
  state, charged, committed}]}, `notice`.
- `cancel("job-…" | "env-…", mode)` answers a CancelResult whose notice says `cancelled: <scheduler ids>`.
- `run_shell(…, confirm_spend=True)` after a gated hand submit answers with `jobs` [JobInfo, source "adopt"].
- The journal's `meter.tick` event rows: {"event": "meter.tick", "envelope_id", "jobs", "sacct_calls", "source"}.
- The ledger (the jail's `<state_dir>/ledger.sqlite3`): tables `envelope` and `job` beside phase 1's.

MAIN (no submit_job): a script whose submit_job meets "Unknown tool" falls back to what an agent could do today — the
job as a hand `sbatch` / `qsub` over run_shell on the login shape (`main_submit`) — so a cell is red on its own graders
(no terms, no envelope, no re-ask, no meter, a stop that leaves the job billing), not on the missing tool alone.
"""
from __future__ import annotations

import json
import re
import shlex
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from batch_checks import POLL_S, _brief, connect_byo, events, ledger_path, said, unknown_tool
from invariants import Result, ToolCall, Trace

_ = connect_byo   # re-exported: the scenarios' first step
JOB_SUBMITTED = ("queued", "running", "done", "failed", "cancelled")
JOB_TERMINAL = ("done", "failed", "cancelled")
_JOB_ID = re.compile(r"^job-[0-9a-f]{12}$")
_ENV_ID = re.compile(r"^env-[0-9a-f]{12}$")
_NATIVE = re.compile(r"(?:Submitted batch job )?(\d+(?:\[\])?(?:\.[A-Za-z0-9.-]+)?)\s*$", re.M)
# The script operator's own words when ops.restart_server() ran (killed, or closed when no pid was learnt)
RESTARTED = "started again on the same state directory"


# ---- reading a trace -------------------------------------------------------------------------------------------------

def job_of(r: dict | None) -> dict:
    j = (r or {}).get("job")
    return j if isinstance(j, dict) else {}


def envelope_of(r: dict | None) -> dict:
    e = (r or {}).get("envelope")
    return e if isinstance(e, dict) else {}


def terms_of(r: dict | None) -> dict:
    t = (r or {}).get("terms")
    return t if isinstance(t, dict) else {}


def submits(t: Trace) -> list[tuple[int, ToolCall]]:
    return list(t.named("submit_job"))


def submitted(t: Trace) -> list[tuple[int, ToolCall]]:
    """submit_job calls that submitted a job (not an adopt): a job with a scheduler id, in a submitted phase."""
    return [(i, c) for i, c in submits(t) if str((c.result or {}).get("phase")) in JOB_SUBMITTED
            and job_of(c.result).get("native_id") and not c.input.get("adopt")]


def job_answers(t: Trace, job_id: str | None = None) -> list[tuple[int, ToolCall]]:
    out = []
    for i, c in t.named("submit_job", "status"):
        j = job_of(c.result)
        if j.get("job_id") and (job_id is None or j.get("job_id") == job_id):
            out.append((i, c))
    return out


def envelope_answers(t: Trace) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in t.named("submit_job", "status", "run_shell") if envelope_of(c.result).get("envelope_id")]


def asked(t: Trace, reason: str | None = None) -> list[tuple[int, ToolCall]]:
    """submit_job / run_shell answers that asked the user (needs_confirmation), with `reason` when given."""
    return [(i, c) for i, c in t.named("submit_job", "run_shell")
            if str((c.result or {}).get("phase")) == "needs_confirmation"
            and (reason is None or str((c.result or {}).get("reason") or "") == reason)]


def journal_ticks(t: Trace) -> list[dict]:
    return events(t, "meter.tick")


def native_ids(t: Trace) -> list[str]:
    out: list[str] = []
    for _i, c in t.named("submit_job", "status", "run_shell"):
        r = c.result or {}
        for j in [r.get("job"), *(r.get("jobs") or [])]:
            nid = str((j or {}).get("native_id") or "") if isinstance(j, dict) else ""
            if nid and nid not in out:
                out.append(nid)
    return out


def main_native_ids(t: Trace) -> list[str]:
    """MAIN's fallback: the job ids its hand sbatch / qsub printed (the script's own `HPCB_MAIN_JOB <id>` words)."""
    out = []
    for text in t.texts:
        for m in re.finditer(r"HPCB_MAIN_JOB (\S+)", text):
            if m.group(1) not in out:
                out.append(m.group(1))
    return out


# ---- the script's steps ----------------------------------------------------------------------------------------------

@dataclass
class Submitted:
    """What one submit step got: the answer it ended on, and on main the hand-submitted job's scheduler id."""
    answer: dict
    main_id: str | None = None

    @property
    def job_id(self) -> str:
        return str(job_of(self.answer).get("job_id") or "")

    @property
    def native(self) -> str:
        return self.main_id or str(job_of(self.answer).get("native_id") or "")


def _sbatch_line(scheduler: str, command: str, *, walltime: str, name: str, array: str | None, nodes: int,
                 partition: str | None, account: str | None) -> str:
    """MAIN: the hand submit an agent writes today — the command wrapped into the scheduler's own submit."""
    if scheduler == "pbs":
        opts = [f"-l select={nodes}", f"-l walltime={walltime}", f"-N {shlex.quote(name[:15])}"]
        opts += [f"-q {shlex.quote(partition)}"] if partition else []
        opts += [f"-A {shlex.quote(account)}"] if account else []
        opts += [f"-J {shlex.quote(array.split('%')[0])}"] if array else []
        return f"echo {shlex.quote(command)} | qsub {' '.join(opts)}"
    opts = [f"-t {walltime}", f"-J {shlex.quote(name)}", f"-N {nodes}", "--parsable"]
    opts += [f"-p {shlex.quote(partition)}"] if partition else []
    opts += [f"-A {shlex.quote(account)}"] if account else []
    opts += [f"--array={shlex.quote(array)}"] if array else []
    return f"sbatch {' '.join(opts)} --wrap {shlex.quote(command)}"


async def main_submit(ops, command: str, *, walltime: str = "00:10:00", session_id: str = "default",
                      array: str | None = None, nodes: int = 1, partition: str | None = None,
                      account: str | None = None, scheduler: str = "slurm") -> str:
    """MAIN: the job as a hand submit over run_shell on the login shape (in `session_id`, so its cwd and env are
    the session's, as an agent's hand sbatch inherits them). Returns the scheduler's id, said as `HPCB_MAIN_JOB <id>`."""
    line = _sbatch_line(scheduler, command, walltime=walltime, name="hpcb-main", array=array, nodes=nodes,
                        partition=partition, account=account)
    r = await ops.call("run_shell", shape="login", session_id=session_id, command=line)
    m = _NATIVE.search(str(r.get("stdout") or ""))
    if not m:
        ops.abort(f"MAIN: the hand submit printed no job id: {r.get('phase')!r} {_brief(r)!r} {r.get('stdout')!r}")
    jid = m.group(1)
    ops.say(f"MAIN FALLBACK: submit_job is not a tool here; the job was submitted by hand: HPCB_MAIN_JOB {jid}")
    return jid


async def submit(ops, *, yes: bool = True, scheduler: str = "slurm", **args: Any) -> Submitted:
    """submit_job as the user wants it: the call, and — when it asks and `yes` — the user's yes (the same call with
    confirm_spend=True). Where submit_job is no tool (main): the hand submit (`main_submit`)."""
    r = await ops.call("submit_job", **args)
    if unknown_tool(r):
        return Submitted(r, await main_submit(ops, str(args.get("command") or ""),
                                              walltime=str(args.get("walltime") or "00:10:00"),
                                              session_id=str(args.get("session_id") or "default"),
                                              array=args.get("array"), nodes=int(args.get("nodes") or 1),
                                              partition=args.get("partition"), account=args.get("account"),
                                              scheduler=scheduler))
    if r.get("phase") == "needs_confirmation" and yes:
        ops.say(f"the user approved: {(r.get('terms') or {}).get('basis') or ''} "
                f"(envelope cap {(r.get('envelope') or {}).get('cap')})")
        r = await ops.call("submit_job", confirm_spend=True, **args)
    return Submitted(r)


async def wait_job(ops, job_id: str, *, until, timeout_s: float = 600.0) -> dict:
    """status(job_id) until `until(answer)` (or the timeout: the last answer)."""
    deadline = time.monotonic() + timeout_s
    st = await ops.call("status", id=job_id)
    while not until(st) and time.monotonic() < deadline:
        await ops.sleep(POLL_S)
        st = await ops.call("status", id=job_id)
    return st


async def world_state(ops, native: str, scheduler: str = "slurm") -> str:
    """The job's state as the scheduler (not hpc-bridge) says it, over the harness' world channel."""
    if scheduler == "pbs":
        _rc, out = await ops.world(f"qstat -x -f {shlex.quote(native)} 2>&1 | sed -n 's/^ *job_state = //p'")
        return out.strip()
    _rc, out = await ops.world(f"sacct -X -n -P -j {shlex.quote(native)} -o State 2>&1 | head -n 1")
    return out.strip().split()[0] if out.strip() else ""


async def wait_world(ops, native: str, states: tuple[str, ...], *, scheduler: str = "slurm",
                     timeout_s: float = 300.0) -> str:
    deadline = time.monotonic() + timeout_s
    st = await world_state(ops, native, scheduler)
    while not any(st.startswith(s) for s in states) and time.monotonic() < deadline:
        await ops.sleep(POLL_S)
        st = await world_state(ops, native, scheduler)
    return st


async def cancel_until_down(ops, ident: str, *, tries: int = 12) -> dict:
    c = await ops.call("cancel", id=ident, mode="abort")
    for _ in range(tries):
        if c.get("status") != "draining":
            break
        await ops.sleep(POLL_S)
        c = await ops.call("cancel", id=ident, mode="abort")
    return c


# ---- graders ---------------------------------------------------------------------------------------------------------

def submit_job_used(t: Trace) -> Result:
    """The job went through submit_job (not a hand sbatch): at least one submit_job submitted a job."""
    got = submitted(t)
    if got:
        return Result("submit_job_used", True, f"ok: submit_job submitted {job_of(got[0][1].result).get('job_id')} "
                      f"(call {got[0][0]})")
    why = ("submit_job is not a tool on this server" if any(unknown_tool(c.result) for _i, c in submits(t))
           else "no submit_job submitted a job")
    return Result("submit_job_used", False, why + (f"; hand-submitted {main_native_ids(t)}" if main_native_ids(t)
                                                   else ""))


def asked_before_submit(t: Trace) -> Result:
    """The first submit_job asked (needs_confirmation, with the job's terms and the envelope it proposes) and nothing
    was submitted before the user's yes (a confirm_spend=True call)."""
    subs = submits(t)
    if not subs:
        return Result("asked_before_submit", False, "no submit_job call" + (
            f" (hand-submitted {main_native_ids(t)} with no question)" if main_native_ids(t) else ""))
    i, first = subs[0]
    r = first.result or {}
    ok = (str(r.get("phase")) == "needs_confirmation" and bool(terms_of(r)) and bool(envelope_of(r))
          and str(first.input.get("confirm_spend")).lower() != "true")
    return Result("asked_before_submit", ok, f"first submit_job (call {i}): phase {r.get('phase')!r}, reason "
                  f"{r.get('reason')!r}, terms {'present' if terms_of(r) else 'absent'}")


def worst_case_is_every_task(t: Trace, *, tasks: int, per_task: float, throttle: int | None = None,
                             tol: float = 0.02) -> Result:
    """The terms' worst case counts EVERY task of the array (`tasks` × `per_task`), not the throttle's concurrent
    few; the terms carry the task count and the throttle."""
    for i, c in submits(t):
        tm = terms_of(c.result)
        if tm.get("worst_case") is None:
            continue
        want = tasks * per_task
        wc = float(tm["worst_case"])
        ok = (abs(wc - want) <= tol * want and tm.get("tasks") == tasks
              and (throttle is None or tm.get("throttle") == throttle))
        return Result("worst_case_is_every_task", ok,
                      f"call {i}: worst_case {wc:g} {tm.get('unit') or ''} for {tm.get('tasks')} task(s) (throttle "
                      f"{tm.get('throttle')}); every task's is {want:g}"
                      + ("" if ok or throttle is None else f", the throttle's {throttle * per_task:g}"))
    return Result("worst_case_is_every_task", False, "no submit_job answer carried terms with a worst case"
                  + (" (submit_job is not a tool on this server)" if any(unknown_tool(c.result) for _i, c in submits(t))
                     else ""))


def cancel_down(t: Trace) -> Result:
    """A cancel of a job (or an envelope) answered `down` once the scheduler ended what it cancelled — and every
    answer before that was `draining`, never `down` while the job was still listed."""
    calls = [(i, c) for i, c in t.named("cancel") if _JOB_ID.match(str(c.input.get("id") or ""))
             or _ENV_ID.match(str(c.input.get("id") or ""))]
    if not calls:
        return Result("cancel_down", False, "no cancel of a job- or env- id" + (
            "; the job was hand-submitted (no job id to cancel)" if main_native_ids(t) else ""))
    downs = [i for i, c in calls if (c.result or {}).get("status") == "down"]
    return Result("cancel_down", bool(downs), f"{len(calls)} cancel(s); down at call(s) {downs or 'none'}")


def one_sacct_per_tick(t: Trace) -> Result:
    """Every meter tick (the journal's `meter.tick` rows) read the scheduler with ONE sacct (or qstat) invocation."""
    ticks = journal_ticks(t)
    if not ticks:
        return Result("one_sacct_per_tick", False, "no meter.tick in the journal (no meter ran)")
    bad = [k for k in ticks if int(k.get("sacct_calls") or 0) > 1]
    return Result("one_sacct_per_tick", not bad, f"{len(ticks)} tick(s); more than one sacct in {len(bad)}")


def exhausted_answered(t: Trace) -> Result:
    """A submit_job answered needs_confirmation with reason envelope_exhausted — and submitted nothing."""
    got = asked(t, "envelope_exhausted")
    if not got:
        return Result("exhausted_answered", False, "no submit_job answered envelope_exhausted" + (
            f" (hand-submitted {main_native_ids(t)}: nothing held the user's budget)" if main_native_ids(t) else ""))
    i, c = got[0]
    env = envelope_of(c.result)
    return Result("exhausted_answered", True, f"ok: call {i}: remaining {env.get('remaining')} of cap {env.get('cap')}")


def asks_not_splits(t: Trace, *, approved: float) -> Result:
    """After the envelope said exhausted, nothing more was submitted to fit it (no smaller pieces of the same work):
    every job submitted, all told, fits the user's approval (`approved`, in the envelope's unit), and the agent's own
    words told the user about the budget instead."""
    ex = asked(t, "envelope_exhausted")
    after = [(i, c) for i, c in submitted(t) if ex and i > ex[0][0]]
    total = sum(float(job_of(c.result).get("worst_case") or 0) for _i, c in submitted(t))
    words = re.search(r"budget|envelope|cap|approv|exhaust|not enough|exceed", said(t), re.I) is not None
    hand = main_native_ids(t)
    ok = bool(ex) and not after and total <= approved + 1e-6 and words and not hand
    return Result("asks_not_splits", ok,
                  f"exhausted at call {ex[0][0] if ex else 'never'}; {len(after)} job(s) submitted after it; "
                  f"{total:g} submitted in all of {approved:g} approved; the agent "
                  f"{'told' if words else 'did not tell'} the user" + (f"; hand-submitted {hand}" if hand else ""))


def gated_then_adopted(t: Trace, *, submit_cmd: str) -> Result:
    """D2 (c): the hand `submit_cmd` (sbatch/qsub) while the envelope was open was answered needs_confirmation and did
    not run; after the user's yes (run_shell confirm_spend=True) it ran and its job was adopted (`jobs`, source
    adopt)."""
    pat = re.compile(rf"(?:^|[\s;&|]){re.escape(submit_cmd)}\s")
    shells = [(i, c) for i, c in t.named("run_shell") if pat.search(" " + str(c.input.get("command") or ""))]
    if not shells:
        return Result("gated_then_adopted", False, f"no run_shell ran {submit_cmd}")
    i0, first = shells[0]
    gated = str((first.result or {}).get("phase")) == "needs_confirmation"
    adopted = [(i, c) for i, c in shells if str(c.input.get("confirm_spend")).lower() == "true"
               and any(isinstance(j, dict) and j.get("source") == "adopt" for j in (c.result or {}).get("jobs") or [])]
    ok = gated and bool(adopted) and adopted[0][0] > i0
    return Result("gated_then_adopted", ok,
                  f"first {submit_cmd} (call {i0}): {(first.result or {}).get('phase')!r}; adopted at call(s) "
                  f"{[i for i, _c in adopted] or 'none'}")


def reask_names_remaining(t: Trace, *, after_text: str = RESTARTED) -> Result:
    """After the restart (a new session), the first submit_job asked again — reason envelope_new_session, the envelope
    it names with what remains — before anything more was submitted."""
    at = next((n for text, n in zip(t.texts, t.text_at, strict=False) if after_text in text), None)
    if at is None:
        return Result("reask_names_remaining", False, "premise not met: no restart in the run")
    later = [(i, c) for i, c in submits(t) if i >= at]
    if not later:
        return Result("reask_names_remaining", False, "no submit_job after the restart" + (
            f" (hand-submitted {main_native_ids(t)})" if main_native_ids(t) else ""))
    i, c = later[0]
    r = c.result or {}
    env = envelope_of(r)
    left = env.get("remaining")
    named = isinstance(left, int | float) and f"{float(left):g}" in str(r.get("notice") or "")
    ok = str(r.get("phase")) == "needs_confirmation" and r.get("reason") == "envelope_new_session" and named
    return Result("reask_names_remaining", ok, f"first submit_job after the restart (call {i}): {r.get('phase')!r} "
                  f"reason {r.get('reason')!r}, remaining {left!r} {'named' if named else 'not named'} in the notice")


def nothing_submitted_before_yes(t: Trace, *, after_text: str = RESTARTED) -> Result:
    """After the restart nothing was submitted until a submit_job carrying the user's yes (confirm_spend=True): the
    new session's first submit only asked."""
    at = next((n for text, n in zip(t.texts, t.text_at, strict=False) if after_text in text), None)
    if at is None:
        return Result("nothing_submitted_before_yes", False, "premise not met: no restart in the run")
    after = [(i, c) for i, c in submitted(t) if i >= at]
    early = [i for i, c in after if str(c.input.get("confirm_spend")).lower() != "true"]
    hand = [j for j in main_native_ids(t)]
    ok = bool(after) and not early and not hand
    return Result("nothing_submitted_before_yes", ok, f"{len(after)} submitted after the restart; without a yes at "
                  f"call(s) {early or 'none'}" + (f"; hand-submitted {hand}" if hand else ""))


def ceiling_refused(t: Trace, *, ceiling: float) -> Result:
    """The operator ceiling: a second session's envelope that would take the SUM of open caps past `ceiling` was
    refused (reason envelope_ceiling, naming HPC_BRIDGE_ENVELOPE_MAX), and the caps that were opened sum within it."""
    refused = [(i, c) for i, c in submits(t) if str((c.result or {}).get("reason")) == "envelope_ceiling"
               and str((c.result or {}).get("phase")) == "refused"]
    opened: dict[str, float] = {}
    for _i, c in submits(t):
        env = envelope_of(c.result)
        if env.get("envelope_id") and env.get("state") in ("open", "exhausted") and env.get("cap") is not None:
            opened[str(env["envelope_id"])] = float(env["cap"])
    total = sum(opened.values())
    named = bool(refused) and "HPC_BRIDGE_ENVELOPE_MAX" in str((refused[0][1].result or {}).get("notice") or "")
    ok = bool(refused) and named and total <= ceiling + 1e-6 and len(opened) >= 2
    return Result("ceiling_refused", ok, f"refused at call(s) {[i for i, _c in refused] or 'none'}; open caps "
                  f"{opened or 'none'} sum {total:g} of the ceiling {ceiling:g}")


def bypass_named_and_counted(t: Trace, *, name: str) -> Result:
    """A job hpc-bridge did not start (a submit hidden from its lexer) was found by the meter's sweep: an envelope
    answer lists it as unledgered, counts it, and the notice names its id."""
    for i, c in t.named("status"):
        env = envelope_of(c.result)
        un = [u for u in env.get("unledgered") or [] if isinstance(u, dict) and str(u.get("name", "")).startswith(name)]
        if un:
            nid = str(un[0].get("native_id"))
            counted = float(un[0].get("charged") or 0) + float(un[0].get("committed") or 0) > 0
            ok = counted and nid in str((c.result or {}).get("notice") or "")
            return Result("bypass_named_and_counted", ok, f"call {i}: {nid} ({un[0].get('state')}) counted "
                          f"{float(un[0].get('charged') or 0) + float(un[0].get('committed') or 0):g}, "
                          f"{'named' if ok else 'NOT named'} in the notice")
    return Result("bypass_named_and_counted", False, f"no envelope answer listed the {name!r} job as unledgered")


def missing_held_full(t: Trace, *, sources: tuple[str, ...] = ("missing", "squeue")) -> Result:
    """A status of a job sacct could not list held it at its FULL walltime (charged + committed == its worst case),
    saying so — never at what it ran."""
    for i, c in job_answers(t):
        j = job_of(c.result)
        if j.get("meter_source") in sources:
            total = float(j.get("charged") or 0) + float(j.get("committed") or 0)
            wc = float(j.get("worst_case") or 0)
            ok = wc > 0 and abs(total - wc) <= 0.01 * wc and "full walltime" in str((c.result or {}).get("notice"))
            return Result("missing_held_full", ok, f"call {i}: meter_source {j.get('meter_source')!r}, held {total:g} "
                          f"of its worst case {wc:g}")
    return Result("missing_held_full", False, "no status answer metered a job missing from sacct"
                  + (f" (hand-submitted {main_native_ids(t)}: no meter)" if main_native_ids(t) else ""))


def freed_when_sacct_returns(t: Trace) -> Result:
    """Once sacct lists the job again, the meter charges what it RAN (less than its worst case) — the full walltime
    was a hold, not a charge."""
    seen_missing = False
    for i, c in job_answers(t):
        j = job_of(c.result)
        if j.get("meter_source") in ("missing", "squeue"):
            seen_missing = True
        elif seen_missing and j.get("meter_source") == "sacct" and str((c.result or {}).get("phase")) in JOB_TERMINAL:
            wc = float(j.get("worst_case") or 0)
            ok = float(j.get("charged") or 0) < wc and float(j.get("committed") or 0) == 0
            return Result("freed_when_sacct_returns", ok, f"call {i}: charged {j.get('charged')} of worst case {wc:g}")
    return Result("freed_when_sacct_returns", False, "no sacct reading of the job after it was missing")


def terms_name_backstop(t: Trace, *, words: str) -> Result:
    """The terms carry the site backstop the user named (`words`), not "none"."""
    for i, c in submits(t):
        tm = terms_of(c.result)
        if tm:
            ok = words.lower() in str(tm.get("site_backstop") or "").lower()
            return Result("terms_name_backstop", ok, f"call {i}: site_backstop {tm.get('site_backstop')!r}")
    return Result("terms_name_backstop", False, "no submit_job answer carried terms")


def pending_under_backstop(t: Trace, *, reason: str = "AssocGrp") -> Result:
    """The job stayed PENDING under the site's limit: a status reported the scheduler's own reason (`reason`…),
    charged nothing."""
    for i, c in job_answers(t):
        j = job_of(c.result)
        if reason.lower() in str(j.get("reason") or "").lower():
            ok = str(j.get("state") or "").upper().startswith("PENDING") and float(j.get("charged") or 0) == 0
            return Result("pending_under_backstop", ok, f"call {i}: {j.get('state')} ({j.get('reason')}), charged "
                          f"{j.get('charged')}")
    return Result("pending_under_backstop", False, f"no status reported the job pending under {reason}…")


def stop_down_after_jobs(t: Trace) -> Result:
    """The last stop answered `down` with spend stopped — after every job of ours was ended — and no earlier stop said
    `down` while one was listed (the world checks confirm the queue)."""
    stops = t.named("stop_endpoint")
    if not stops:
        return Result("stop_down_after_jobs", False, "no stop_endpoint call")
    r = stops[-1][1].result or {}
    ok = r.get("status") == "down" and r.get("spend_stopped") is True
    return Result("stop_down_after_jobs", ok, f"the last stop answered {r.get('status')!r} "
                  f"(spend_stopped={r.get('spend_stopped')!r}): {str(r.get('notice') or '')[:160]!r}")


# ---- world checks ----------------------------------------------------------------------------------------------------

def read_envelopes(path=None) -> tuple[list[dict], list[dict]] | None:
    """(envelope rows, job rows) of the jail's ledger, read-only; None when there is no ledger or no envelope table
    (a build before phase 2a)."""
    p = path or ledger_path()
    if not p.exists():
        return None
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        try:
            envs = [dict(r) for r in con.execute("select * from envelope order by created_at")]
            jobs = [dict(r) for r in con.execute("select * from job order by created_at")]
        except sqlite3.Error:
            return None
        return envs, jobs
    finally:
        con.close()


_SACCT_BILL = "JobID,JobName%60,State,ElapsedRaw,AllocTRES%200,Partition"


def _billing(tres: str) -> float | None:
    m = re.search(r"(?:^|,)billing=([0-9.]+)", tres or "")
    return float(m.group(1)) if m else None


def sacct_charges(out: str) -> dict[str, tuple[str, float, str]]:
    """job id (an array task's `N_i` folded into `N`) -> (state, billing × elapsed hours summed, name), from
    `sacct -X -n -P -o JobID,JobName,State,ElapsedRaw,AllocTRES,Partition` lines."""
    got: dict[str, tuple[str, float, str]] = {}
    for line in out.splitlines():
        cols = line.split("|")
        if len(cols) < 5 or not cols[0][:1].isdigit():
            continue
        base = cols[0].split("_")[0].split(".")[0]
        bill = _billing(cols[4]) or 0.0
        try:
            hours = int(cols[3] or 0) / 3600
        except ValueError:
            hours = 0.0
        state, charged, name = got.get(base, (cols[2], 0.0, cols[1]))
        got[base] = (cols[2].split()[0] if cols[2] else state, charged + bill * hours, name)
    return got


async def charged_within_envelope(ctx) -> Result:
    """world: what the scheduler says every job of each envelope charged (billing TRES × elapsed, never below the job's
    recorded minimum) — plus every other job of the pool user since the envelope opened (the sweep) — is within the
    envelope's cap. The ledger names the envelopes and their jobs; sacct, not hpc-bridge, says what they charged."""
    name = "world:charged_within_envelope"
    led = read_envelopes()
    if led is None:
        return Result(name, False, f"no envelope in a ledger at {ledger_path()} (a build before phase 2a)"
                      + (f"; hand-submitted {main_native_ids(ctx.trace)}" if main_native_ids(ctx.trace) else ""))
    envs, jobs = led
    if not envs:
        return Result(name, False, "no envelope was opened")
    since = min(float(e["created_at"]) for e in envs)
    start = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(since - 60))
    rc, out = ctx.ssh(f'sacct -u "$(whoami)" -X -n -P -S {start} -o {_SACCT_BILL} 2>&1; echo "HPCB_RC=$?"', 60)
    if "HPCB_RC=0" not in out:
        return Result(name, False, f"UNVERIFIABLE: sacct failed (rc={rc}): {out[-200:]!r}")
    charges = sacct_charges(out)
    bad, shown = [], []
    for e in envs:
        mine = [j for j in jobs if j["envelope_id"] == e["id"] and j.get("native_id")]
        total = 0.0
        for j in mine:
            nid = str(j["native_id"]).split(".")[0].split("[")[0]
            got = charges.pop(nid, None)
            ran = got[1] if got else 0.0
            floor = float(j.get("min_charge_h") or 0) * float(j.get("rate") or 0) * (1 if ran > 0 else 0)
            total += max(ran, floor)
        shown.append(f"{e['id']}: {total:.2f} of {float(e['cap']):g} ({len(mine)} job(s))")
        if total > float(e["cap"]) + 1e-6:
            bad.append(f"{e['id']} charged {total:.2f} > cap {float(e['cap']):g}")
    stray = sum(c for _st, c, nm in charges.values() if not nm.startswith("parsl"))
    if stray and envs:
        shown.append(f"unledgered {stray:.2f}")
    return Result(name, not bad, ("ok: " if not bad else "; ".join(bad) + " — ") + "; ".join(shown))


async def job_state_is(ctx, *, native_from: str = "first", want: tuple[str, ...], scheduler: str = "slurm") -> Result:
    """world: the scheduler's own state of the run's (first / last) job is one of `want` (e.g. CANCELLED after a
    cancel; PENDING-then-CANCELLED with no start under a site limit)."""
    ids = native_ids(ctx.trace) or main_native_ids(ctx.trace)
    if not ids:
        return Result("world:job_state", False, "premise not met: no job id in the run")
    nid = ids[0] if native_from == "first" else ids[-1]
    if scheduler == "pbs":
        _rc, out = ctx.ssh(f"qstat -x -f {shlex.quote(nid)} 2>&1", 60)
        m = re.search(r"job_state = (\S+)", out)
        state = m.group(1) if m else "?"
    else:
        _rc, out = ctx.ssh(f"sacct -X -n -P -j {shlex.quote(nid.split('.')[0])} -o State,ElapsedRaw 2>&1 | head -n 1", 60)
        state = out.strip().split("|")[0].split()[0] if out.strip() else "?"
    return Result("world:job_state", any(state.startswith(w) for w in want), f"job {nid}: {state}")


async def never_started(ctx) -> Result:
    """world: the run's job never started (sacct: no start, zero elapsed) — it billed nothing."""
    ids = native_ids(ctx.trace) or main_native_ids(ctx.trace)
    if not ids:
        return Result("world:never_billed", False, "premise not met: no job id in the run")
    rc, out = ctx.ssh(f"sacct -X -n -P -j {shlex.quote(ids[0].split('.')[0])} -o JobID,State,Start,ElapsedRaw 2>&1", 60)
    rows = [ln.split("|") for ln in out.splitlines() if ln.split("|")[0][:1].isdigit()]
    ok = bool(rows) and all(r[2] in ("Unknown", "None", "") and r[3] in ("0", "") for r in rows if len(r) >= 4)
    return Result("world:never_billed", ok, f"rc={rc}: " + "; ".join("|".join(r) for r in rows[:4]))


async def array_within_throttle(ctx, *, throttle: int) -> Result:
    """world: the array never ran more than `throttle` tasks at once (sacct's own start/end per task)."""
    ids = native_ids(ctx.trace) or main_native_ids(ctx.trace)
    if not ids:
        return Result("world:array_within_throttle", False, "premise not met: no job id in the run")
    _rc, out = ctx.ssh(f"sacct -X -n -P -j {shlex.quote(ids[0].split('.')[0])} -o JobID,Start,End 2>&1", 60)
    marks = []
    for line in out.splitlines():
        cols = line.split("|")
        if len(cols) < 3 or "_" not in cols[0] or "[" in cols[0] or cols[1] in ("Unknown", "None", ""):
            continue
        marks += [(cols[1], 1), (cols[2] if cols[2] not in ("Unknown", "None", "") else "9999", -1)]
    live = peak = 0
    for _ts, step in sorted(marks, key=lambda m: (m[0], m[1])):
        live += step
        peak = max(peak, live)
    n = len(marks) // 2
    return Result("world:array_within_throttle", n > 0 and peak <= throttle,
                  f"{n} task(s) started; at most {peak} at once (throttle {throttle})")


async def promotion_carried(ctx, *, cwd_tail: str, value: str) -> Result:
    """world: the job ran in the session's cwd with its env — its output file says so (the frozen job.sh's own
    output under its job dir; main: the hand submit's slurm-<id>.out in the session's cwd)."""
    ids = native_ids(ctx.trace) or main_native_ids(ctx.trace)
    if not ids:
        return Result("world:promotion_carried", False, "premise not met: no job id in the run")
    nid = ids[0].split(".")[0]
    paths = [str(j.get("script_path") or "") for _i, c in job_answers(ctx.trace) if (j := job_of(c.result))]
    where = " ".join(shlex.quote(p.rsplit("/", 1)[0]) for p in paths if p) or "$HOME"
    _rc, out = ctx.ssh(f"cat $(find {where} -maxdepth 3 -name '*{nid}*.out' 2>/dev/null | head -n 1) 2>&1", 60)
    ok = cwd_tail in out and value in out
    return Result("world:promotion_carried", ok, f"job {nid}'s output: {out.strip()[:200]!r}")


_ = (json, ToolCall)
