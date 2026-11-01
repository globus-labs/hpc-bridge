"""envelope_new_session_reasks — the envelope belongs to the session that approved it (batch phase 2a, design §3
"Approval", rule 3): a restarted server — a new session — asks again before admitting anything, naming what remains.
Fake cluster, `default` profile; scripted (`ops.restart_server()`: a SIGKILL, then a new server on the same state).

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh envelope_new_session_reasks

THE SCRIPT: connect; submit_job A (1 core × 30 min = 0.5) under an envelope of 2 — the yes; KILL the server; a new
server reconnects; submit_job B (the same size) without a yes — it must ask, naming the 1.5 left; then the yes; stop.

GRADED: the new session's first submit asked again, naming what remains (`reask_names_remaining`); nothing was
submitted after the restart without the yes (`nothing_submitted_before_yes`); within the envelope
(`world:charged_within_envelope`); the stop ended both (`stop_down_after_jobs`).

ON MAIN the fallback hand-submits A and B: nothing asks after the restart (`reask_names_remaining` fails: no
submit_job), and B went in without a yes (`nothing_submitted_before_yes` fails).
"""
from __future__ import annotations

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "envelope: a restarted server (a new session) asks again, naming what remains, before it submits anything"
TAGS = ["fake", "jobs", "envelope", "restart", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

JOB = {"command": "sleep 300", "cores": 1, "walltime": "00:30:00"}
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — open an envelope with job A, restart the server, submit job B and see it ask again.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    a = await jc.submit(ops, envelope={"cap": 2}, **JOB)
    await ops.restart_server()
    await bc.reconnect(ops)
    if a.main_id:
        b = await jc.submit(ops, **JOB)
        s = await jc.stop(ops)
        return f"main fallback: hand-submitted {a.main_id}, {b.main_id}; stop said {s.get('status')!r}"
    r = await ops.call("submit_job", **JOB)
    ops.say(f"the new session's first submit_job: {r.get('phase')!r} ({r.get('reason')!r}): {bc._brief(r)}")
    if r.get("phase") == "needs_confirmation":
        ops.say("the user says yes")
        r = await ops.call("submit_job", confirm_spend=True, **JOB)
    s = await jc.stop(ops, tries=8)
    return f"B: {r.get('phase')!r}; stop said {s.get('status')!r}"


EXTRA_INVARIANTS = [jc.reask_names_remaining, jc.nothing_submitted_before_yes, jc.stop_asked_about_jobs,
                    jc.stop_down_after_jobs]
WORLD_CHECKS = [jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "reask_names_remaining", "nothing_submitted_before_yes", "stop_asked_about_jobs",
             "stop_down_after_jobs"]
_ = (Result, Trace)
