"""job_promote_track_cancel — Jobs mode, promotion (batch phase 2a, design §2 "Promotion", §10 "Phase 2"): work set up
in a session is promoted to a scheduler job that outlives any endpoint; it carries the session's cwd and env, is
tracked through the ledger with ONE sacct call per look, and `cancel` ends it, answering `down` only once the
scheduler has. Fake cluster, `default` profile; an agent cell (Pi on the free model) or the script operator:

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh job_promote_track_cancel
    HPCB_OPERATOR=pi     HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh job_promote_track_cancel

THE STEPS: connect (BYO, the probe's proposal); in session `dev` on the free login shape, `mkdir -p ~/promo && cd
~/promo && export HPCB_PROMO=carried-<n>`; submit_job(command="pwd; echo $HPCB_PROMO; sleep 300", session_id="dev",
cores=1, walltime="00:10:00") — the first call asks (the job's terms and the envelope it opens), the user's yes is the
same call with confirm_spend=True; status(job) until RUNNING; cancel(job, "abort") until `down`; stop.

GRADED: the first submit asked (`asked_before_submit`); submit_job carried it (`submit_job_used`); one sacct per meter
tick (`one_sacct_per_tick`); cancel answered `down` (`cancel_down`); the job's own output shows the session's cwd and
env (`world:promotion_carried`); sacct says CANCELLED (`world:job_state`); what it charged is within the envelope
(`world:charged_within_envelope`); the universal check finds no `hpcb-job-*` left.

ON MAIN there is no submit_job: the script's fallback hand-submits `sbatch --wrap` from the session (main_submit) —
the job runs (sbatch inherits the cwd and env), but no question was asked, there is no job id to track or cancel, no
meter, no envelope, and the stop leaves it running (the universal check names it as ours).
"""
from __future__ import annotations

import os
import time

import batch_checks as bc
import jobs_checks as jc
from invariants import Result

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "jobs mode: a session's work promoted to a job (cwd + env carried), tracked by the meter, cancelled down"
TAGS = ["fake", "jobs", "envelope", "spend", "stop"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 40
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

MARK = f"carried-{int(time.time()) % 100000}"
CWD = "promo"
COMMAND = "pwd; echo $HPCB_PROMO; sleep 300"

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — connect (it is not catalogued: "
    "probe the login node and accept the discovered configuration yourself) and wait until the login node is up.\n\n"
    f"In run_shell session `dev` on the LOGIN shape, run: mkdir -p ~/{CWD} && cd ~/{CWD} && export HPCB_PROMO={MARK}\n\n"
    "Then run this as a SCHEDULER JOB (not on an interactive block) that carries that session's directory and "
    f"environment: `{COMMAND}` — 1 core, 10 minutes of walltime. The user has ALREADY approved that one job and a "
    "budget envelope just big enough for it; you may confirm exactly that, and nothing else.\n\n"
    "Once the job is RUNNING, the user changes their mind: cancel it, and report what the cancel said and what the job "
    "had been charged. Finally stop the endpoint and report what stop_endpoint said.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions."
)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    r = await ops.call("run_shell", shape="login", session_id="dev",
                       command=f"mkdir -p ~/{CWD} && cd ~/{CWD} && export HPCB_PROMO={MARK} && pwd")
    if r.get("phase") != "complete":
        ops.abort(f"the session could not be set up: {r.get('phase')!r} {bc._brief(r)!r}")
    sub = await jc.submit(ops, command=COMMAND, session_id="dev", cores=1, walltime="00:10:00")
    if sub.main_id:
        await jc.wait_world(ops, sub.main_id, ("RUNNING",))
        s = await jc.stop(ops)
        return f"main fallback: hand-submitted {sub.main_id}; stop said {s.get('status')!r}"
    if not sub.job_id:
        ops.abort(f"submit_job did not submit: {sub.answer.get('phase')!r} {bc._brief(sub.answer)!r}")
    st = await jc.wait_job(ops, sub.job_id, until=lambda s: s.get("phase") in ("running", *jc.JOB_TERMINAL))
    ops.say(f"{sub.job_id} ({sub.native}) is {st.get('phase')!r}, charged {jc.job_of(st).get('charged')}")
    c = await jc.cancel_until_down(ops, sub.job_id)
    ops.say(f"cancel said {c.get('status')!r}: {str(c.get('notice') or '')[:200]}")
    s = await jc.stop(ops)
    return f"{sub.job_id} cancelled ({c.get('status')!r}); stop said {s.get('status')!r}"


async def promotion_carried(ctx) -> Result:
    return await jc.promotion_carried(ctx, cwd_tail=f"/{CWD}", value=MARK)


async def job_cancelled(ctx) -> Result:
    return await jc.job_state_is(ctx, want=("CANCELLED",))


EXTRA_INVARIANTS = [jc.asked_before_submit, jc.submit_job_used, jc.one_sacct_per_tick, jc.cancel_down]
WORLD_CHECKS = [promotion_carried, job_cancelled, jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "asked_before_submit", "submit_job_used", "one_sacct_per_tick", "cancel_down",
             "ends_with_stop", "stop_is_honest"]
_ = os
