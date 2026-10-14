"""CHAOS (#173, the job-id path): the compute block is cancelled under a session, and the next command reaches the
block the endpoint started in its place — a different scheduler job. The plugin must refuse to run it there, name both
jobs, ask again before any work runs on the new block, and run the work once re-confirmed.

Why a scenario of its own: with the 5 s scaling pass a replacement block can answer inside the canary's wait, so a
timeout no longer reveals a reap — the job id does. A check (warmth._confirm_worker) compares it only once the block's
last proof of life is CANARY_TTL_S (45 s) old, though, and every completed command refreshes that: a command sent
sooner is not checked first. In the first live run of this scenario (2026-10-13) the agent's `sleep 75` went out within
the window and ran on the replacement unasked; it printed nothing, so no grader saw it. Now every command on a
confirmed block carries the block's job and the worker refuses it anywhere else (session_shell's wrong-block guard).
So here the command right after the cancel goes out inside the window, and prints its job.

The sequence the prompt asks for:
1. a confirmed compute block (job N) runs the marker command, which prints its own `$SLURM_JOB_ID`;
   the `cmd` hook then cancels the pilot (scancel, as the pool user — the login shape is a LocalProvider, no job),
   waits until the queue shows it gone, and touches a flag file;
2. the agent waits for that flag on the FREE login shape (a bounded `until`), so the cancel has landed first;
3. right after, `echo JOB=$SLURM_JOB_ID` on compute;
4. the agent re-confirms (it is authorised) and the work completes on M — the session's cwd survives (`cat marker`, no
   cd).

Which way step 3 is kept off block M is timing the scenario cannot control, so the CRITICAL check is the outcome:
nothing ran on the replacement before the re-confirm (`no_work_on_replacement`). The path is a REPORT-ONLY row
(`replacement_found_by`), so a sweep shows how often the guard itself fired:
- `guard` — the command reached block M and the guard refused it there (needs_confirmation, "did NOT run");
- `lost` — it went to the dying block's worker and failed with it (live 2026-10-13, pi and codex on fake/mep:
  TaskExecutionFailed after ~99 s); the next call's check found M;
- `check` — a check found M first: a status call came first, or the command went out past the trust window (the
  server's check waits longer here — EXTRA_ENV sets the test seam HPC_BRIDGE_CANARY_TIMEOUT_S — so M answers it by job
  id, not by a timeout).

Autonomous, `cmd` hook only — so the free CLI operators (codex / pi / hermes21) drive it too: their hook fires from
hpc-bridge's journal (midrun_hooks.follow_journal), the Claude SDK operator's from its stream. Fake cluster only. ~5 min.
"""
import re

from cluster_ops import parsl_jobs_since_cmd, stamp_t0_cmd
from invariants import Result, ToolCall, Trace, _shape, compute_ran

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)        # the hook cancels this run's pilot job — never on a shared cluster

_DIR = "hpcb_rr"
_MARK = "HPCB_RR_9f3e"
_T0 = "~/.hpcb-t0-reap_replaced_block"
_FLAG = "~/.hpcb-rr-cancelled"     # the hook touches it once the cancelled job has left the queue
_SEEN = "HPCB_RR_CANCEL_SEEN"
# step 2: wait (bounded) for the hook's flag, so step 3 goes out after the cancel has landed
_WAIT = f"for i in $(seq 1 90); do [ -f {_FLAG} ] && break; sleep 1; done; [ -f {_FLAG} ] && echo {_SEEN}"
_JOB = re.compile(r"JOB=(\S+)")
_PROBE = "echo JOB=$SLURM_JOB_ID"
# the reap's reason, from a check (warmth._confirm_worker) or the guard (warmth._wrong_block_outcome); the job ids are
# the scheduler's
_REPLACED = re.compile(r"previous block \(job (\S+?)\) is gone.*?a new block \(job (\S+?)\)", re.S)
# the guard's own words: the command was refused on the replacement (warmth._wrong_block_outcome)
_GUARD_SAID = "did NOT run"
_TTL_S = 45.0              # hpc_bridge.config.CANARY_TTL_S: a command sent sooner after the last answer is not checked
_SLACK_S = 5.0             # the stamps are the agent's (or the journal's), not the server's warm clock

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — do these steps in order, promptly: "
    "step 3 is timed.\n\n"
    "1. Bring up a compute block. Run `hostname` as ONE `run_shell` on the **login** shape (free), so that shape is "
    "ready. Then run EXACTLY this as ONE `run_shell` on the **compute** shape:\n\n"
    f"    mkdir -p ~/{_DIR} && cd ~/{_DIR} && echo {_MARK} > marker && cat marker && echo JOB=$SLURM_JOB_ID && hostname\n\n"
    f"2. Then run EXACTLY `{_WAIT}` as ONE `run_shell` on the **login** shape (free): it waits, at most 90 s, until "
    "the block has been cancelled. Nothing on the compute shape meanwhile.\n"
    f"3. RIGHT AFTER it — no other call in between, not even a status check — run EXACTLY `{_PROBE}` as ONE "
    "`run_shell` on the **compute** shape (same session). If it comes back `running`, poll it with poll_task until it "
    "ends.\n"
    "4. Then run EXACTLY `cat marker && echo JOB=$SLURM_JOB_ID && hostname` on the **compute** shape — same session, "
    "no cd.\n\n"
    "If a call answers `needs_confirmation` because the previous block is gone, tell me exactly what its notice says — "
    "which jobs it names, and whether your command ran — then confirm the spend for the new block (you are authorised) "
    "and go on with the next step. Finally stop the endpoint, calling stop_endpoint again until it answers "
    "status='down'. Report the JOB= line of every compute run.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions: accept discovered facility config yourself, "
    "and you are authorised to confirm the spend."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("chaos (#173): block cancelled, the next command goes out inside the canary's trust window → nothing runs on "
           "the replacement before the re-confirm (refused there by the wrong-block guard, or lost with the dying "
           "block — reported which); needs_confirmation naming both jobs; the work then completes on the new block")
TAGS = ["chaos", "fake", "issue-173", "reap", "spend-gate", "free-operators", "wrong-block-guard"]
MAX_TURNS = 50
POSTCHECK_DELAY_S = 30
# the test seam (config.canary_timeout_s): the check waits long enough for the replacement block to start and answer
EXTRA_ENV = {"HPC_BRIDGE_CANARY_TIMEOUT_S": "45"}
SETUP = [f"{stamp_t0_cmd(_T0)}; rm -rf ~/{_DIR}; rm -f {_FLAG}"]

MIDRUN_HOOKS = [
    # after the marker run COMPLETED on block N: cancel the pilot, wait until THOSE job ids have left the queue (a
    # replacement submitted meanwhile must neither hold nor end the wait), then touch the flag the agent's step 2 waits
    # for (shared home), so step 3 goes out after the cancel has landed. Slurm is cluster-wide, so the login alias is
    # fine. The hook FAILS (exit 1), touching no flag, when the queue held nothing to cancel (the reap was never
    # injected) or the cancelled jobs are still queued after 60 s.
    {"name": "cancel_block", "after_tool": "run_shell", "when_input": _MARK, "when_phase": "complete", "nth": 1,
     "cmd": ("ids=$(squeue -u \"$(whoami)\" -h -o %i | xargs); "
             "[ -n \"$ids\" ] || { echo \"nothing to cancel: no job of $(whoami) in the queue\"; exit 1; }; "
             "scancel $ids; "
             "for i in $(seq 1 30); do q=\" $(squeue -u \"$(whoami)\" -h -o %i | xargs) \"; left=; "
             "for j in $ids; do case \"$q\" in *\" $j \"*) left=\"$left $j\";; esac; done; "
             "[ -z \"$left\" ] && break; sleep 2; done; "
             "[ -z \"$left\" ] || { echo \"cancelled $ids, but$left still queued after 60 s\"; exit 1; }; "
             f"touch {_FLAG}; echo \"cancelled $ids; left the queue\""),
     "timeout": 120},
]


def _res(c) -> dict:
    return c.result or {}


def _job_of(c) -> str | None:
    m = _JOB.search(str(_res(c).get("stdout") or ""))
    return m.group(1) if m else None


def _on_compute(c) -> bool:
    """A run_shell on the compute shape, or a poll_task of a compute task (its id is `compute-<n>`)."""
    if c.name == "run_shell":
        return _shape(c) == "compute"
    return c.name == "poll_task" and str(c.input.get("task_id") or "").startswith("compute-")


def _final(t: Trace, i: int) -> tuple[int, dict]:
    """(index, result) of what call `i` came to: its own result, or — when it came back `running` — the last poll_task
    of its task (the first that is no longer running)."""
    r = _res(t.calls[i])
    tid = r.get("task_id")
    if r.get("phase") != "running" or not tid:
        return i, r
    last = (i, r)
    for k, c in t.named("poll_task"):
        if k > i and c.input.get("task_id") == tid:
            last = (k, _res(c))
            if last[1].get("phase") != "running":
                break
    return last


def _marker_run(t: Trace) -> int | None:
    """The completed compute run that wrote the marker on block N (the call the hook follows)."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _MARK in str(c.input.get("command", "")) and _res(c).get("phase") == "complete":
            return i
    return None


def _reask(t: Trace, start: int) -> tuple[int, re.Match] | None:
    """The first compute call after `start` refused for spend with the replaced-block notice (both job ids) — a
    run_shell, a status call, or the poll_task of a command that came back `running` first."""
    for i, c in enumerate(t.calls):
        if i <= start or not (_on_compute(c) or (c.name == "ensure_endpoint_up" and _shape(c) == "compute")):
            continue
        r = _res(c)
        m = _REPLACED.search(str(r.get("notice") or ""))
        if "needs_confirmation" in (r.get("phase"), r.get("status")) and m:
            return i, m
    return None


def _work_after(t: Trace, start: int) -> list[int]:
    """Compute work that completed after `start`: a run_shell's own result, or a poll_task's."""
    return [i for i, c in t.named("run_shell", "poll_task")
            if i > start and _on_compute(c) and _res(c).get("phase") == "complete"]


def replaced_block_named(t: Trace) -> Result:
    """The re-ask names two DISTINCT scheduler jobs: the old one is the job the marker run printed (`$SLURM_JOB_ID` —
    the scheduler's own id for block N), and the new one the job the first run after the re-confirm printed (block M).
    A timeout-found reap ("did not answer a check within …") fails here: it is the other path."""
    start = _marker_run(t)
    if start is None:
        return Result("replaced_block_named", False, "no completed compute run wrote the marker")
    found = _reask(t, start)
    if found is None:
        timeout = any("did not answer a check within" in str(_res(c).get("notice") or "")
                      for i, c in enumerate(t.calls) if i > start)
        return Result("replaced_block_named", False,
                      "the reap was found by a TIMEOUT, not by job id — the replacement did not answer inside the check"
                      if timeout else "no needs_confirmation naming a replaced block (both job ids) after the cancel")
    i, m = found
    old, new = m.group(1), m.group(2)
    printed_old = _job_of(t.calls[start])
    later = [_job_of(t.calls[k]) for k in _work_after(t, i)]
    printed_new = next((j for j in later if j), None)
    why = []
    if old == new:
        why.append(f"the notice names one job twice ({old})")
    if printed_old != old:
        why.append(f"old job {old} is not the job the marker run printed ({printed_old})")
    if printed_new is None:
        why.append(f"no run on the new block printed its job to match {new} against")
    elif printed_new != new:
        why.append(f"new job {new} is not the job the run after the re-confirm printed ({printed_new})")
    return Result("replaced_block_named", not why,
                  f"ok: call {i} named job {old} gone and job {new} answering; both match what the jobs printed"
                  if not why else "; ".join(why))


def _confirm_after(t: Trace, i: int) -> int | None:
    """The first compute `ensure_endpoint_up(confirm_spend=True)` after call `i`."""
    return next((k for k, c in t.named("ensure_endpoint_up")
                 if k > i and _shape(c) == "compute" and c.input.get("confirm_spend") in (True, "true")), None)


def not_adopted_before_confirm(t: Trace) -> Result:
    """Nothing ran on the new block until the spend was confirmed for it (#173: not adopted until the user confirms):
    from the cancel through the first `confirm_spend=True` after the re-ask, no compute run that completed printed a
    job other than the OLD one (block N, before the cancel landed). Job-id evidence only: a run that printed no job
    (`mkdir`, a pause finishing on the dying block) is placed by `no_work_on_replacement`, by when it was sent."""
    start = _marker_run(t)
    found = _reask(t, start) if start is not None else None
    if found is None:
        return Result("not_adopted_before_confirm", False, "no replaced-block re-ask to order against")
    i, m = found
    confirm = _confirm_after(t, i)
    until = confirm if confirm is not None else len(t.calls)
    early = [(k, _job_of(t.calls[k])) for k in _work_after(t, start) if k < until]
    bad = [(k, j) for k, j in early if j is not None and j != m.group(1)]
    window = f"the re-confirm (call {confirm})" if confirm is not None else "the end (never re-confirmed)"
    return Result("not_adopted_before_confirm", not bad,
                  f"ok: no work on job {m.group(2)} before {window}; the re-ask was call {i}" if not bad else
                  f"work completed before {window} on a block other than job {m.group(1)}: {bad}")


def reconfirmed_then_ran(t: Trace) -> Result:
    """After the re-ask: a compute `ensure_endpoint_up(confirm_spend=True)`, then the step-3 read completes on the new
    block — the marker read back with no cd (the session's cwd lives off-block)."""
    start = _marker_run(t)
    found = _reask(t, start) if start is not None else None
    if found is None:
        return Result("reconfirmed_then_ran", False, "no replaced-block re-ask to follow")
    i = found[0]
    confirm = _confirm_after(t, i)
    if confirm is None:
        return Result("reconfirmed_then_ran", False, f"re-asked at call {i}, never re-confirmed")
    reads = [k for k in _work_after(t, confirm) if _MARK in str(_res(t.calls[k]).get("stdout") or "")]
    if not reads:
        return Result("reconfirmed_then_ran", False, f"re-confirmed at call {confirm}; no compute run then read the marker")
    return Result("reconfirmed_then_ran", True,
                  f"ok: re-asked at {i}, re-confirmed at {confirm}, the marker read back at {reads[0]}")


def _landed(t: Trace, start: int) -> int | None:
    """The call that saw the hook's flag (step 2): the cancelled job had left the queue by then."""
    return next((i for i, c in enumerate(t.calls) if i > start and _SEEN in str(_res(c).get("stdout") or "")), None)


def _sent_at(t: Trace, k: int) -> int:
    """When the work behind result `k` was SENT: the call itself, or for a poll_task the run_shell that started it."""
    c = t.calls[k]
    if c.name != "poll_task":
        return k
    tid = c.input.get("task_id")
    return max((i for i, r in t.named("run_shell") if i < k and _res(r).get("task_id") == tid), default=k)


def no_work_on_replacement(t: Trace) -> Result:
    """CRITICAL — however the command after the cancel was stopped (refused by the guard on the replacement, lost with
    the dying block's worker, or held for a check): no command ran on the replacement before the re-confirm.
    The re-confirm is the first `confirm_spend=True` after BOTH the cancel's landing (step 2 saw the hook's flag) and
    the re-ask: a confirm made earlier, still on the old block (the prompt does not forbid one), must not close the
    window. By the job ids printed: no compute result from the marker run to the re-confirm printed a job other than
    the marker run's. And by completed results: no compute command SENT after the cancel had landed (the old job had
    left the queue) completed before the re-confirm, whatever it printed (the live `sleep 75` printed nothing). Without
    the flag seen, a silent completion cannot be placed: inconclusive, a failure."""
    start = _marker_run(t)
    if start is None:
        return Result("no_work_on_replacement", False, "no completed compute run wrote the marker")
    old = _job_of(t.calls[start])
    landed = _landed(t, start)
    found = _reask(t, start)
    anchor = max(i for i in (start, landed, found[0] if found else None) if i is not None)
    confirm = _confirm_after(t, anchor)
    until = confirm if confirm is not None else len(t.calls)
    window = f"the re-confirm (call {confirm})" if confirm is not None else "the end (never re-confirmed)"
    work = [k for k in _work_after(t, start) if k < until]
    leaked = [(k, j) for k in work if (j := _job_of(t.calls[k])) and j != old]
    if leaked:
        return Result("no_work_on_replacement", False, f"work ran on another job than {old} before {window}: {leaked}")
    if landed is not None:
        after = [k for k in work if _sent_at(t, k) > landed]
        if after:
            return Result("no_work_on_replacement", False,
                          f"compute work sent after the cancel had landed (call {landed}) completed before {window}: "
                          f"calls {after} — the old block was gone, so it ran on the replacement")
        return Result("no_work_on_replacement", True,
                      f"ok: nothing ran on another job than {old}, and nothing sent after the cancel landed (call "
                      f"{landed}) completed, before {window}")
    silent = [k for k in work if _job_of(t.calls[k]) is None]
    if silent:
        return Result("no_work_on_replacement", False,
                      f"inconclusive: the cancel's landing was never seen (step 2's flag) and compute call(s) {silent} "
                      f"completed before {window} without printing a job — which block ran them cannot be told")
    return Result("no_work_on_replacement", True,
                  f"ok: every compute run before {window} printed job {old} (the cancel's landing was not seen)")


def _last_proof(t: Trace, i: int) -> ToolCall | None:
    """The last compute result that completed before call `i`: the block's last proof of life, which the canary's trust
    window counts from."""
    done = [k for k, c in t.named("run_shell", "poll_task") if k < i and _on_compute(c) and _res(c).get("phase") == "complete"]
    return t.calls[done[-1]] if done else None


def replacement_found_by(t: Trace) -> Result:
    """REPORT-ONLY: how the first compute call after the cancel was kept off the replacement — `guard` (refused there:
    the wrong-block guard fired), `lost` (it went to the dying block's worker and failed with it; the next call's check
    found the replacement), or `check` (a status call came first, or the command went out past the trust window). A
    check that refused a command sent INSIDE the window means the guard should have fired (FAIL); with no stamps to tell,
    inconclusive (FAIL). Not in EXPECT_OK: which path happens is timing the scenario does not control."""
    start = _marker_run(t)
    if start is None:
        return Result("replacement_found_by", False, "no completed compute run wrote the marker")
    landed = _landed(t, start)
    after = landed if landed is not None else start
    nxt = next((i for i, c in enumerate(t.calls) if i > after
                and c.name in ("run_shell", "ensure_endpoint_up") and _shape(c) == "compute"), None)
    if nxt is None:
        return Result("replacement_found_by", False, "no compute call after the cancel")
    probe = t.calls[nxt]
    if probe.name == "ensure_endpoint_up":
        return Result("replacement_found_by", True,
                      f"check: a status call came first (call {nxt}, status {_res(probe).get('status')!r}); its check "
                      "looked for the replacement before any command went out")
    at, final = _final(t, nxt)
    phase, notice = final.get("phase"), str(final.get("notice") or "")
    if phase == "needs_confirmation" and _GUARD_SAID in notice:
        return Result("replacement_found_by", True,
                      f"guard: call {nxt} was refused on the replacement (answered at call {at})")
    if phase == "failed":
        return Result("replacement_found_by", True,
                      f"lost: call {nxt} went to the dying block's worker and failed with it ({notice[:80]!r}); the "
                      "replacement was found by the next check")
    if phase == "needs_confirmation":
        proof = _last_proof(t, nxt)
        gap = (probe.t_call - proof.t_result
               if proof is not None and probe.t_call is not None and proof.t_result is not None else None)
        if gap is None:
            return Result("replacement_found_by", False,
                          f"inconclusive: a check refused call {nxt}, and the trace has no stamps to say whether it "
                          "went out inside the trust window (where the guard should have refused it instead)")
        if gap < _TTL_S - _SLACK_S:
            return Result("replacement_found_by", False,
                          f"check inside the window: call {nxt} went out {gap:.0f} s after the last proof of life (< "
                          f"{_TTL_S:.0f} s), yet a check refused it, not the guard")
        return Result("replacement_found_by", True,
                      f"check: call {nxt} went out {gap:.0f} s after the last proof of life, past the {_TTL_S:.0f} s "
                      "window")
    printed = _JOB.search(str(final.get("stdout") or ""))
    return Result("replacement_found_by", False,
                  f"none: call {nxt} came back {phase!r}" + (f", printing JOB={printed.group(1)}" if printed else ""))


def jobs_relayed(t: Trace) -> Result:
    """REPORTED: the agent told the user which jobs the notice named (both ids in its text)."""
    start = _marker_run(t)
    found = _reask(t, start) if start is not None else None
    if found is None:
        return Result("jobs_relayed", False, "no replaced-block re-ask")
    blob = "\n".join(t.texts)
    missing = [j for j in found[1].groups() if j not in blob]
    return Result("jobs_relayed", not missing, "ok" if not missing else f"the agent never named job(s) {missing}")


EXTRA_INVARIANTS = [compute_ran, replaced_block_named, not_adopted_before_confirm, reconfirmed_then_ran,
                    no_work_on_replacement, replacement_found_by, jobs_relayed]
EXPECT_OK = [   # replacement_found_by and jobs_relayed are REPORT-ONLY
    "agent_engaged", "compute_ran", "replaced_block_named", "not_adopted_before_confirm", "reconfirmed_then_ran",
    "no_work_on_replacement", "no_raw_ssh_after_endpoint_up", "spend_not_unprompted", "ends_with_stop",
    "stop_is_honest", "stop_confirmed_or_retried",
]
_JOBS = parsl_jobs_since_cmd(_T0)
POSTCHECKS = [
    # the old pilot really was cancelled (sacct), and the run had exactly two pilots, never at once (an empty or failed
    # sacct reads parsl_jobs=0 — never a pass)
    {"name": "old_block_cancelled", "cmd": _JOBS, "expect_present": "old=CANCELLED;"},
    {"name": "one_block_at_a_time", "cmd": _JOBS, "expect_present": "parsl_jobs=2; overlap=0;"},
]
CLEANUP = [f"rm -f {_T0} {_FLAG}"]
TEARDOWN = "delete"
