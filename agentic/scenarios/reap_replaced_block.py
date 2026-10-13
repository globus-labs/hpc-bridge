"""CHAOS (#173, the job-id path): the compute block is cancelled under a session, and the next check is ANSWERED — by
the block that check had the endpoint start, a different scheduler job. The plugin must name both jobs, ask again
before any work runs on the new block, and run the work once re-confirmed.

Why a scenario of its own: with the 5 s scaling pass a replacement block can answer inside the canary's wait, so a
timeout no longer reveals a reap — the job id does (warmth._confirm_worker). But on the fake cluster the 8 s default
wait is shorter than a block's start, so `block_reaped_resume` (10-13) took the TIMEOUT path ("did not answer a check
within 8 s") and the job-id path never ran live. Here the server's check waits longer (EXTRA_ENV sets the test seam
HPC_BRIDGE_CANARY_TIMEOUT_S), so the replacement answers inside it.

The sequence the prompt asks for:
1. a confirmed compute block (job N) runs the marker command, which prints its own `$SLURM_JOB_ID`;
   the `cmd` hook then cancels the pilot (scancel, as the pool user — the login shape is a LocalProvider, no job);
2. the agent waits ~75 s on the FREE login shape — past CANARY_TTL_S (45 s), so the next compute call must check
   rather than trust its last proof of life;
3. the next compute call's check is answered by the new block (job M): `needs_confirmation`, naming N and M; the agent
   re-confirms (it is authorised) and the work completes on M — the session's cwd survives (`cat marker`, no cd).

Autonomous, `cmd` hook only — so the free CLI operators (codex / pi / hermes21) drive it too: their hook fires from
hpc-bridge's journal (midrun_hooks.follow_journal), the Claude SDK operator's from its stream. Fake cluster only. ~5 min.
"""
import re

from cluster_ops import parsl_jobs_since_cmd, stamp_t0_cmd
from invariants import Result, Trace, _shape, compute_ran

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)        # the hook cancels this run's pilot job — never on a shared cluster

_DIR = "hpcb_rr"
_MARK = "HPCB_RR_9f3e"
_T0 = "~/.hpcb-t0-reap_replaced_block"
_JOB = re.compile(r"JOB=(\S+)")
# warmth._confirm_worker's notice for this path (the job ids are the scheduler's)
_REPLACED = re.compile(r"previous block \(job (\S+?)\) is gone.*?a new block \(job (\S+?)\)", re.S)

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — do these steps in order.\n\n"
    "1. Bring up a compute block, then run EXACTLY this as ONE `run_shell` on the **compute** shape:\n\n"
    f"    mkdir -p ~/{_DIR} && cd ~/{_DIR} && echo {_MARK} > marker && cat marker && echo JOB=$SLURM_JOB_ID && hostname\n\n"
    "2. Then WAIT: run `sleep 75` as ONE `run_shell` on the **login** shape (free). Do not touch the compute shape "
    "meanwhile.\n"
    "3. Then run EXACTLY `cat marker && echo JOB=$SLURM_JOB_ID && hostname` on the **compute** shape — same session, "
    "no cd.\n\n"
    "If a call answers `needs_confirmation` because the previous block is gone, tell me exactly what its notice says — "
    "which jobs it names — then confirm the spend for the new block (you are authorised) and run step 3 again. "
    "Finally stop the endpoint, calling stop_endpoint again until it answers status='down'. Report the JOB= line of "
    "every compute run.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions: accept discovered facility config yourself, "
    "and you are authorised to confirm the spend."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("chaos (#173): block cancelled, the next check answered by its replacement → needs_confirmation naming both "
           "jobs; the new block is not used before the re-confirm; the work then completes on it")
TAGS = ["chaos", "fake", "issue-173", "reap", "spend-gate", "free-operators"]
MAX_TURNS = 50
POSTCHECK_DELAY_S = 30
# the test seam (config.canary_timeout_s): the check waits long enough for the replacement block to start and answer
EXTRA_ENV = {"HPC_BRIDGE_CANARY_TIMEOUT_S": "45"}
SETUP = [f"{stamp_t0_cmd(_T0)}; rm -rf ~/{_DIR}"]

MIDRUN_HOOKS = [
    # after the marker run COMPLETED on block N: cancel the pilot and wait until the queue shows it gone. No sleep: the
    # agent's own `sleep 75` on the login shape (step 2) carries the session past the canary's trust window. Slurm is
    # cluster-wide, so the login alias is fine; the hook FAILS (exit 1) when the queue held nothing to cancel — the
    # reap was never injected.
    {"name": "cancel_block", "after_tool": "run_shell", "when_input": _MARK, "when_phase": "complete", "nth": 1,
     "cmd": ("ids=$(squeue -u \"$(whoami)\" -h -o %i | xargs); "
             "[ -n \"$ids\" ] || { echo \"nothing to cancel: no job of $(whoami) in the queue\"; exit 1; }; "
             "scancel $ids; for i in $(seq 1 30); do squeue -u \"$(whoami)\" -h | grep -q . || break; sleep 2; done; "
             "echo \"cancelled $ids; queue-left=$(squeue -u \"$(whoami)\" -h | wc -l)\""),
     "timeout": 120},
]


def _res(c) -> dict:
    return c.result or {}


def _job_of(c) -> str | None:
    m = _JOB.search(str(_res(c).get("stdout") or ""))
    return m.group(1) if m else None


def _marker_run(t: Trace) -> int | None:
    """The completed compute run that wrote the marker on block N (the call the hook follows)."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _MARK in str(c.input.get("command", "")) and _res(c).get("phase") == "complete":
            return i
    return None


def _reask(t: Trace, start: int) -> tuple[int, re.Match] | None:
    """The first compute call after `start` refused for spend with the replaced-block notice (both job ids)."""
    for i, c in enumerate(t.calls):
        if i <= start or c.name not in ("run_shell", "ensure_endpoint_up") or _shape(c) != "compute":
            continue
        r = _res(c)
        m = _REPLACED.search(str(r.get("notice") or ""))
        if "needs_confirmation" in (r.get("phase"), r.get("status")) and m:
            return i, m
    return None


def _work_after(t: Trace, start: int) -> list[int]:
    return [i for i, c in t.named("run_shell") if i > start and _shape(c) == "compute" and _res(c).get("phase") == "complete"]


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
    from the cancel through the first `confirm_spend=True` after the re-ask, every compute run that completed printed
    the OLD job (block N, before the cancel landed) — none ran on another job."""
    start = _marker_run(t)
    found = _reask(t, start) if start is not None else None
    if found is None:
        return Result("not_adopted_before_confirm", False, "no replaced-block re-ask to order against")
    i, m = found
    confirm = _confirm_after(t, i)
    until = confirm if confirm is not None else len(t.calls)
    early = [(k, _job_of(t.calls[k])) for k in _work_after(t, start) if k < until]
    bad = [(k, j) for k, j in early if j != m.group(1)]
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


def jobs_relayed(t: Trace) -> Result:
    """REPORTED: the agent told the user which jobs the notice named (both ids in its text)."""
    start = _marker_run(t)
    found = _reask(t, start) if start is not None else None
    if found is None:
        return Result("jobs_relayed", False, "no replaced-block re-ask")
    blob = "\n".join(t.texts)
    missing = [j for j in found[1].groups() if j not in blob]
    return Result("jobs_relayed", not missing, "ok" if not missing else f"the agent never named job(s) {missing}")


EXTRA_INVARIANTS = [compute_ran, replaced_block_named, not_adopted_before_confirm, reconfirmed_then_ran, jobs_relayed]
EXPECT_OK = [
    "agent_engaged", "compute_ran", "replaced_block_named", "not_adopted_before_confirm", "reconfirmed_then_ran",
    "no_raw_ssh_after_endpoint_up", "spend_not_unprompted", "ends_with_stop", "stop_is_honest",
    "stop_confirmed_or_retried",
]
_JOBS = parsl_jobs_since_cmd(_T0)
POSTCHECKS = [
    # the old pilot really was cancelled (sacct), and the run had exactly two pilots, never at once (an empty or failed
    # sacct reads parsl_jobs=0 — never a pass)
    {"name": "old_block_cancelled", "cmd": _JOBS, "expect_present": "old=CANCELLED;"},
    {"name": "one_block_at_a_time", "cmd": _JOBS, "expect_present": "parsl_jobs=2; overlap=0;"},
]
CLEANUP = [f"rm -f {_T0}"]
TEARDOWN = "delete"
