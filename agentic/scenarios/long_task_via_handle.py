"""LIVE regression for #21 item 2: a long compute task completes via a poll handle (~5 min).

Before item 2, a single blocking run_shell task on the compute shape was CUT at ~110s (it never
returned `complete`). Now the client sync-wait is decoupled from the task ceiling: a task that outlives
the sync-wait comes back `phase="running"` with a `task_id`, keeps running on the block (which stays
warm because a running task holds it), and `poll_task` retrieves the full result. This scenario runs a
180s task and asserts it COMPLETES via that handle path — the inverse of the old `task_cap_probe`,
which observed the cut.

Deterministic on purpose (the prompt dictates the exact command + shape). Run with **--no-skill**
(`HPCB_NO_SKILL=1`) so the agent follows the literal instruction rather than skill-driven choices; the
`run_shell` "running" notice itself points at poll_task, so the handle path completes even without the
skill. One short billed block. See issue #21 (https://github.com/globus-labs/hpc-bridge/issues/21).
"""
from invariants import Result, Trace

_MARK = "HPCB_LONGTASK"

PROMPT = (
    "I'm testing that a LONG single task on the compute block runs to completion (not cut short) on the "
    "HPC facility with login host `{ssh_host}` — facility id `{facility}`. Bring up a "
    "compute node, then run EXACTLY this as ONE `run_shell` on the **compute** shape — do NOT split it, "
    "do NOT use sbatch, do NOT background/detach it, do NOT use the login shape:\n\n"
    "    python3 -c \"import time; print('" + _MARK + " start', flush=True); time.sleep(180); "
    "print('" + _MARK + " DONE')\"\n\n"
    "It runs longer than the sync-wait, so run_shell will return phase='running' with a task_id — that "
    "is expected, it was NOT cut. Retrieve the result with poll_task(task_id) until it reports "
    "phase='complete', then report the final result (phase, exit_code, stdout) and stop the endpoint to "
    "release the block.\n\n"
    "This is an AUTOMATED run — accept discovered facility config yourself and you are authorised to "
    "confirm the spend."
)

USER_GOAL = ""
PERSONA = None
KIND = "diagnostic"
SUMMARY = "#21 item 2 live: a >sync-wait run_shell on compute returns a poll handle and COMPLETES (not cut)"
TAGS = ["issue-21", "bounds", "diagnostic", "ablation", "submit-poll"]
MAX_TURNS = 55


def completed_via_handle(t: Trace) -> Result:
    """#21 item 2: the long compute task was NOT cut — it returned phase="running" with a task_id, and
    poll_task retrieved the full result (the DONE marker), so the block held it to completion."""
    runs = [
        (i, c) for i, c in t.named("run_shell")
        if c.input.get("shape") in (None, "compute") and _MARK in str(c.input.get("command", ""))
    ]
    if not runs:
        return Result("completed_via_handle", False,
                      "agent never ran the probe on the compute shape (rerouted — needs --no-skill / a firmer prompt)")
    got_handle = any(
        str((c.result or {}).get("phase")) == "running" and (c.result or {}).get("task_id")
        for _, c in runs
    )
    done_polls = [
        i for i, c in t.named("poll_task")
        if (_MARK + " DONE") in str((c.result or {}).get("stdout", ""))
    ]
    done_in_run = any((_MARK + " DONE") in str((c.result or {}).get("stdout", "")) for _, c in runs)
    ok = got_handle and (bool(done_polls) or done_in_run)
    return Result(
        "completed_via_handle", ok,
        f"ok: long compute task returned a poll handle and completed (DONE via poll {done_polls}) — #21 item 2 works"
        if ok else
        f"handle={got_handle} done_polls={done_polls} done_in_run={done_in_run}: "
        "expected running -> poll_task -> complete with the DONE marker",
    )


# A probe the server never started is not a cut: the block wasn't up yet, or the spend / account gate held it.
_NOT_STARTED = ("cold_start", "needs_confirmation", "needs_account")


def not_cut_at_cap(t: Trace) -> Result:
    """Regression guard: no compute probe result is a bare cut — a probe that STARTED and then ended in neither
    complete nor running, or an exit 124 (the old ~110s per-task guillotine). A run_shell sent while the block was
    still allocating honestly answers `cold_start` without running anything (Codex did, 2026-10-09)."""
    runs = [
        c for _, c in t.named("run_shell")
        if c.input.get("shape") in (None, "compute") and _MARK in str(c.input.get("command", ""))
    ]
    cut = [
        str(c.input.get("command", ""))[:40] for c in runs
        if str((c.result or {}).get("phase")) not in ("complete", "running", *_NOT_STARTED)
        or (c.result or {}).get("exit_code") == 124
    ]
    ok = not cut
    return Result("not_cut_at_cap", ok,
                  "ok: no compute probe was cut at a per-task cap" if ok else
                  f"a compute probe looks CUT (it started, then neither completed nor kept running; or exit 124): {cut}")


EXTRA_INVARIANTS = [completed_via_handle, not_cut_at_cap]

EXPECT_OK = [
    "completed_via_handle",  # the point: the long task finished via the handle path, not cut
    "not_cut_at_cap",        # and was never guillotined at ~110s
    "ends_with_stop",        # released the block after
    "stop_is_honest",            # #24: the release must not claim down while unconfirmed
    "stop_confirmed_or_retried", # and must be re-stopped on draining until it reads down
]

POSTCHECK_DELAY_S = 10
NEEDS_COMPUTE_NODE = True   # brings up a billed block (no compute_ran grader to derive it from) — run_suite gates it
TEARDOWN = "delete"
