"""CHAOS: the endpoint manager dies under a polled long task — poll_task must report the task ORPHANED (#44) and the
agent must stop polling, say so, and release. Fake cluster only (the hook kills this run's endpoint manager).

Flow: bring up compute → ONE long run_shell on the compute shape (outlives the sync wait → `running` + task_id) →
the agent's first `poll_task` that finds it running fires the hook (stop this run's endpoint manager where it runs, as
the pool user)
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
    # Stop THIS run's manager on the login node where it runs (run.py _run_hook → cluster_ops.stop_here_cmd on every
    # login node, each acting only if the manager is in its own ps). It FAILS harness:midrun_hooks unless some node
    # stopped it. The old hook — `gce stop {endpoint_name} >/dev/null 2>&1; echo manager-stopped` to the round-robin
    # alias — landed on the node without the manager and still said it worked; the task then completed normally
    # (pi, live 2026-10-13).
    # After the run_shell of THIS task (its marker) that answered `running`: the task is in flight, whatever the agent's
    # polling. It fired on the first RUNNING poll before, and an agent whose one poll waited the task out (qwen under pi
    # and codex: poll_task(wait=200) → complete, live 2026-10-22) never met an orphan — the cell failed on
    # orphan_reported for a kill that never happened. `when_phase: running` still skips a blind opening batch (hermes21,
    # live 2026-10-15: its run_shell failed before any endpoint, and a hook there found no manager anywhere).
    {"name": "kill_manager", "after_tool": "run_shell", "when_input": _MARK, "when_phase": "running", "nth": 1,
     "world": "stop_manager"},
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
