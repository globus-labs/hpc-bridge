"""job_array_terms — an array's terms count EVERY task (batch phase 2a, design §10 "Phase 2": "the worst case is 20 ×
size, not the throttle's 4"). A throttle (`%4`) bounds how many tasks run at once, not what the array can bill: the
spend question must name 20 tasks' worth. Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh job_array_terms

THE SCRIPT: connect; submit_job(command="sleep 20; echo task {index}", array="0-19%4", cores=1, walltime="00:10:00")
— the first call asks; the user's yes; status until every task ended; stop.

GRADED: the terms' worst case is 20 × (1 core × 10 min) — not 4 × (`worst_case_is_every_task`); the first call asked
(`asked_before_submit`); the scheduler never ran more than 4 tasks at once (`world:array_within_throttle`); what the
array charged is within the envelope (`world:charged_within_envelope`); stop is `down`.

ON MAIN the fallback hand-submits `sbatch --array=0-19%4`: no terms were stated at all (`worst_case_is_every_task`
fails: no answer carried a worst case), and there is no envelope to be within.
"""
from __future__ import annotations

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "jobs mode: an array's spend question counts all 20 tasks, not the throttle's 4; the throttle holds"
TAGS = ["fake", "jobs", "envelope", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

TASKS, THROTTLE = 20, 4
PER_TASK = 1 * 10 / 60        # 1 core × 10 min, in billing units (the default profile bills CPUs)
COMMAND = "sleep 20; echo task {index}"

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` "
          "— submit a 20-task array, at most 4 at once, read its terms, run it, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    sub = await jc.submit(ops, command=COMMAND, array=f"0-{TASKS - 1}%{THROTTLE}", cores=1, walltime="00:10:00")
    if sub.main_id:
        await jc.wait_world(ops, sub.main_id, ("COMPLETED", "FAILED", "CANCELLED"), timeout_s=420)
        s = await bc.stop(ops)
        return f"main fallback: hand-submitted {sub.main_id}; stop said {s.get('status')!r}"
    if not sub.job_id:
        ops.abort(f"submit_job did not submit: {sub.answer.get('phase')!r} {bc._brief(sub.answer)!r}")
    st = await jc.wait_job(ops, sub.job_id, until=lambda s: s.get("phase") in jc.JOB_TERMINAL, timeout_s=600)
    ops.say(f"{sub.job_id}: {st.get('phase')!r}, tasks {jc.job_of(st).get('array_counts')}, charged "
            f"{jc.job_of(st).get('charged')}")
    s = await bc.stop(ops)
    return f"{sub.job_id} {st.get('phase')!r}; stop said {s.get('status')!r}"


def worst_case_is_every_task(t: Trace) -> Result:
    return jc.worst_case_is_every_task(t, tasks=TASKS, per_task=PER_TASK, throttle=THROTTLE)


async def array_within_throttle(ctx) -> Result:
    return await jc.array_within_throttle(ctx, throttle=THROTTLE)


EXTRA_INVARIANTS = [worst_case_is_every_task, jc.asked_before_submit, jc.stop_down_after_jobs]
WORLD_CHECKS = [array_within_throttle, jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "worst_case_is_every_task", "asked_before_submit", "stop_down_after_jobs"]
