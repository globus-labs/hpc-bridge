"""meter_missing_from_sacct_full_walltime — a ledger job sacct cannot list counts at its FULL walltime (batch phase 2a,
design §3 "The meter never understates"): the accounting daemon goes away while the job runs and ends, so neither sacct
(down) nor squeue (it ended) can say what it charged — the meter holds its whole walltime, says so, and frees it only
when sacct lists it again. Fake cluster, `default` profile; scripted, NODE_ACTIONS (`dbd_off` / `dbd_on`: the host
stops and starts the slurmdbd container).

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh meter_missing_from_sacct_full_walltime

THE SCRIPT: connect; submit_job(command="sleep 20", cores=1, walltime="00:10:00") under an envelope of 1 (the yes);
wait until it RUNS; `dbd_off`; wait until squeue no longer lists it (it ended); status(job) — held at its full 10
minutes, "full walltime" said; `dbd_on`; status(job) until sacct reads it — charged what it ran; stop.

GRADED: while sacct could not list it the job was held at its worst case (`missing_held_full`); once sacct listed it
again it was charged what it ran (`freed_when_sacct_returns`).

ON MAIN there is no meter: the fallback hand-submits the job and nothing ever holds or charges it
(`missing_held_full` fails: no status answer metered a job).
"""
from __future__ import annotations

import asyncio
import time

import batch_checks as bc
import jobs_checks as jc
from node_actions import NodeActions

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "meter: a job sacct cannot list (slurmdbd down) is held at its full walltime, freed when sacct returns"
TAGS = ["fake", "jobs", "envelope", "meter", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
NODE_ACTIONS = True
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — submit a short job, take the accounting daemon away while it ends, and read the meter.")


async def SCRIPT(ops) -> str:
    acts = NodeActions.from_env()
    await bc.connect_byo(ops)
    sub = await jc.submit(ops, command="sleep 20", cores=1, walltime="00:10:00", envelope={"cap": 1})
    native = sub.native
    if not native:
        ops.abort(f"nothing was submitted: {sub.answer.get('phase')!r} {bc._brief(sub.answer)!r}")
    await jc.wait_world(ops, native, ("RUNNING", "COMPLETED"))
    rc, out = await asyncio.to_thread(acts.run, "dbd_off", "slurmdbd")
    if rc != 0:
        ops.abort(f"dbd_off failed (rc={rc}): {out!r}")
    ops.say(f"slurmdbd is down: {out.strip()[-120:]}")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        _rc, q = await ops.world(f"squeue -h -j {native} -o %T 2>&1")
        if not q.strip() or "Invalid job id" in q:
            break
        await ops.sleep(bc.POLL_S)
    if sub.job_id:
        held = await ops.call("status", id=sub.job_id)
        ops.say(f"with sacct down: {jc.job_of(held).get('meter_source')!r} held "
                f"{jc.job_of(held).get('charged')}+{jc.job_of(held).get('committed')}")
    rc, out = await asyncio.to_thread(acts.run, "dbd_on", "slurmdbd")
    ops.say(f"slurmdbd is back (rc={rc})")
    if sub.job_id:
        await jc.wait_job(ops, sub.job_id, until=lambda s: jc.job_of(s).get("meter_source") == "sacct"
                          and s.get("phase") in jc.JOB_TERMINAL, timeout_s=180)
    s = await bc.stop(ops, tries=8)
    return f"stop said {s.get('status')!r}"


EXTRA_INVARIANTS = [jc.missing_held_full, jc.freed_when_sacct_returns]
WORLD_CHECKS = [jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "missing_held_full", "freed_when_sacct_returns"]
