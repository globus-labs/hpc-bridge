"""envelope_stop_races_submit — a submit_job issued while a stop runs (batch phase 2a, fix round 2; review P5: a stop
that found nothing live released the blocks for up to 180 s with the envelope still admitting, then answered `down`
without reading the ledger again — a job admitted meanwhile was PENDING). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh envelope_stop_races_submit

THE SCRIPT: connect; submit_job a small job under its envelope (the yes); cancel it until `down` (nothing of ours bills
now); then, CONCURRENTLY, stop_endpoint and — a moment later — submit_job of a job that would fit the old envelope; if
that job got in anyway, stop again (jobs="cancel") until `down`.

GRADED: the submit during the stop was not admitted from the envelope the stop closed — proposed afresh — or the stop
did not say `down` (`no_admission_during_stop`); the last stop is `down` with nothing of ours left
(`stop_down_after_jobs`, and the universal check).

The window is as wide as the stop's own work: on the fake cluster with no compute block it is short, so a pass here
says the ordering holds in the live stack; tests/test_batch_phase2a_r2_review.py holds the stop open to pin the race.
"""
from __future__ import annotations

import asyncio

import batch_checks as bc
import jobs_checks as jc

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "stop: a submit_job issued while a stop runs is not admitted from the envelope the stop closes"
TAGS = ["fake", "jobs", "envelope", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}
JOB = {"command": "sleep 300", "cores": 1, "walltime": "00:10:00"}
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — open an envelope, end its job, then stop while a new job is submitted.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    a = await jc.submit(ops, envelope={"cap": 1}, **JOB)
    if a.job_id:
        c = await jc.cancel_until_down(ops, a.job_id)
        ops.say(f"{a.job_id}: {c.get('status')!r} — nothing of ours bills now")

    async def late_submit() -> dict:
        await ops.sleep(0.2)
        return await ops.call("submit_job", **JOB)

    stop, late = await asyncio.gather(ops.call("stop_endpoint"), late_submit())
    ops.say(f"the stop said {stop.get('status')!r}; the submit during it said {late.get('phase')!r} "
            f"({late.get('reason')!r})")
    if str(late.get("phase")) in jc.JOB_SUBMITTED or stop.get("status") != "down":
        s = await jc.stop(ops, tries=8)
        return f"stop said {s.get('status')!r}"
    return f"stop said {stop.get('status')!r}"


EXTRA_INVARIANTS = [jc.no_admission_during_stop, jc.stop_down_after_jobs]
EXPECT_OK = ["agent_engaged", "no_admission_during_stop", "stop_down_after_jobs"]
