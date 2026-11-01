"""envelope_ceiling_sums — the operator ceiling `HPC_BRIDGE_ENVELOPE_MAX` bounds the SUM of the open envelopes on one
facility and account (batch phase 2a, design §3 "Approval", rule 3): two live sessions cannot each open a cap that
together passes it. Fake cluster, `default` profile; scripted, two servers on one state directory
(`ops.open_server("B")`).

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh envelope_ceiling_sums

THE SCRIPT (the ceiling is 1.0): server A opens an envelope of 0.6 with a 0.25 job; server B asks for 0.6 — refused,
naming the ceiling and the 0.4 left under it; B asks for 0.4 — opened, a 0.25 job; both stop.

GRADED: B's 0.6 was refused naming HPC_BRIDGE_ENVELOPE_MAX, and the caps opened sum within the ceiling
(`ceiling_refused`); within each envelope (`world:charged_within_envelope`).

ON MAIN there are no envelopes: both servers hand-submit; nothing refuses, nothing sums (`ceiling_refused` fails).
"""
from __future__ import annotations

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "envelope: two live sessions' caps on one facility+account are summed against HPC_BRIDGE_ENVELOPE_MAX"
TAGS = ["fake", "jobs", "envelope", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
CEILING = 1.0
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10", "HPC_BRIDGE_ENVELOPE_MAX": str(CEILING)}

JOB = {"command": "sleep 120", "cores": 1, "walltime": "00:15:00"}
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — two servers each open an envelope; the second must stay under the operator's ceiling.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    a = await jc.submit(ops, envelope={"cap": 0.6}, **JOB)
    b_ops = await ops.open_server("B")
    await bc.reconnect(b_ops)
    if a.main_id:
        b = await jc.submit(b_ops, **JOB)
        s = await bc.stop(ops)
        return f"main fallback: hand-submitted {a.main_id}, {b.main_id}; stop said {s.get('status')!r}"
    over = await b_ops.call("submit_job", confirm_spend=True, envelope={"cap": 0.6}, **JOB)
    b_ops.say(f"B asked for 0.6: {over.get('phase')!r} ({over.get('reason')!r}): {bc._brief(over)}")
    fits = await b_ops.call("submit_job", confirm_spend=True, envelope={"cap": 0.4}, **JOB)
    b_ops.say(f"B asked for 0.4: {fits.get('phase')!r}")
    sb = await bc.stop(b_ops, tries=8)
    sa = await bc.stop(ops, tries=8)
    return f"B over: {over.get('phase')!r}; B fits: {fits.get('phase')!r}; stops {sb.get('status')!r}/{sa.get('status')!r}"


def ceiling_refused(t: Trace) -> Result:
    return jc.ceiling_refused(t, ceiling=CEILING)


EXTRA_INVARIANTS = [ceiling_refused, jc.stop_down_after_jobs]
WORLD_CHECKS = [jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "ceiling_refused", "stop_down_after_jobs"]
