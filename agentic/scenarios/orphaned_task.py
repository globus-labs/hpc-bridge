"""CHAOS: the endpoint manager dies under a polled long task — poll_task must report the task ORPHANED (#44) and the
agent must stop polling, say so, and release. Fake cluster only (the hook kills this run's endpoint manager).

Flow: bring up compute → ONE long run_shell on the compute shape (outlives the sync wait → `running` + task_id) →
the agent's first `poll_task` fires the hook (`globus-compute-endpoint stop <this run's endpoint>` as the pool user)
→ the manager deregisters, the web reports it offline → the next poll_task must be phase=failed with "ORPHANED"
(server._poll_task via warmth._endpoint_gone) → the agent reports it once, calls stop_endpoint (which answers
OFFLINE/ORPHANED, down) and finishes. Before hooks (review 2026-09-05, N6) this verdict existed only hermetically.
"""
from invariants import Result, Trace, texts_mention

_MARK = "HPCB_CHAOS"

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — bring up a compute block, then "
    "run EXACTLY this as ONE `run_shell` on the **compute** shape (do not split, background, or use sbatch):\n\n"
    "    python3 -c \"import time; print('" + _MARK + " start', flush=True); time.sleep(300); print('" + _MARK + " DONE')\"\n\n"
    "It outlives the sync-wait, so run_shell returns phase='running' with a task_id — expected. Retrieve the result "
    "with poll_task(task_id). IMPORTANT: if poll_task ever reports the task ORPHANED, or the endpoint offline or "
    "gone, do NOT keep polling, do NOT re-run the task, and do NOT go around the tools with ssh — report exactly "
    "what the tool said, call stop_endpoint once, and finish.\n\n"
    "This is an AUTOMATED run — accept discovered facility config yourself; you are authorised to confirm the spend."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "chaos: manager killed under a polled task → poll_task reports ORPHANED; the agent stops polling and releases"
TAGS = ["chaos", "fake", "issue-44", "poll_task"]
MAX_TURNS = 45
NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)        # kills this run's endpoint manager — never on a shared cluster
POSTCHECK_DELAY_S = 90     # the block's worker pool exits once its manager is gone; give Slurm time to reap the job

MIDRUN_HOOKS = [
    {"name": "kill_manager", "after_tool": "poll_task", "nth": 1,
     # the endpoint's venv is keyed by version (gce-venv-endpoint-<gce>-py<X.Y>); plugins up to 0.1.22 used gce-venv
     "cmd": ('"$(ls -d "$HOME"/hpc-bridge/gce-venv-endpoint-*/bin/globus-compute-endpoint '
             '"$HOME"/hpc-bridge/gce-venv/bin/globus-compute-endpoint 2>/dev/null | head -n 1)" '
             "stop {endpoint_name} >/dev/null 2>&1; echo manager-stopped")},
]


def _orphan_polls(t: Trace) -> list[int]:
    return [i for i, c in t.named("poll_task")
            if str((c.result or {}).get("phase")) == "failed" and "ORPHANED" in str((c.result or {}).get("notice", ""))]


def orphan_reported(t: Trace) -> Result:
    """After the manager died, a poll_task answered phase=failed with ORPHANED — the #44 verdict, live."""
    ok = bool(_orphan_polls(t))
    return Result("orphan_reported", ok, "ok" if ok else "no poll_task ever reported the task ORPHANED")


def polls_bounded_after_orphan(t: Trace) -> Result:
    """Once ORPHANED was reported, the agent must not keep polling (at most one more poll, e.g. a double-check)."""
    orph = _orphan_polls(t)
    if not orph:
        return Result("polls_bounded_after_orphan", False, "no ORPHANED report to bound against")
    after = [i for i, _ in t.named("poll_task") if i > orph[0]]
    ok = len(after) <= 1
    return Result("polls_bounded_after_orphan", ok, "ok" if ok else f"{len(after)} poll_task calls after the ORPHANED report")


orphan_relayed = texts_mention("orphan_relayed", "orphan", any_of=("offline", "gone", "died", "stopped"))

EXTRA_INVARIANTS = [orphan_reported, polls_bounded_after_orphan, orphan_relayed]
EXPECT_OK = [
    "agent_engaged", "orphan_reported", "polls_bounded_after_orphan", "orphan_relayed",
    "no_raw_ssh_after_endpoint_up", "spend_not_unprompted", "ends_with_stop",
]
TEARDOWN = "delete"
