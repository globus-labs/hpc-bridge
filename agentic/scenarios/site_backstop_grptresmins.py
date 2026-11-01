"""site_backstop_grptresmins — the honest limit and the site's backstop (batch phase 2a, design §3 "The honest limit",
§10): the envelope binds only what hpc-bridge starts, so the terms say "site backstop: none" unless the user names
one. Here the user names a Slurm `GrpTRESMins` on their association; the site's limit holds the job PENDING
(`AssocGrpCPUMinutesLimit`) and it never bills — and hpc-bridge reports that in the scheduler's own words. Fake
cluster, `site` profile (enforced accounting: limits on); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=site ./agentic/run_smoke.sh site_backstop_grptresmins

ADMIN_SETUP gives the pool user's `hpcb` association GrpTRESMins=cpu=1 (one CPU-minute: any 1-core 10-minute job is
over it); ADMIN_CLEANUP lifts it and cancels what is left.

THE SCRIPT: connect; submit_job(account="hpcb", partition="compute", cores=1, walltime="00:10:00",
envelope={cap: 1, site_backstop: "GrpTRESMins cpu=1 on hpcb"}) — asked, then the yes; status(job) until the scheduler
says why it waits; cancel(job) until `down`; stop.

GRADED: the terms name the backstop the user gave (`terms_name_backstop`); a status reported the job PENDING under
AssocGrp…Minutes, charged 0 (`pending_under_backstop`); cancel answered `down` (`cancel_down`); sacct says it never
started (`world:never_billed`).

ON MAIN the fallback hand-submits it: it pends the same way, but no terms named a backstop and nothing reported the
scheduler's reason — `terms_name_backstop` and `pending_under_backstop` fail on their own terms.
"""
from __future__ import annotations

import time

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none", "accounting": "enforce"}
KIND = "regression"
SUMMARY = "envelope: the user names a GrpTRESMins backstop; the terms say so; the job pends under it and never bills"
TAGS = ["fake", "site", "jobs", "envelope", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

BACKSTOP = "GrpTRESMins cpu=1 on hpcb"
ADMIN_SETUP = ["sacctmgr -i modify user where name={user} account=hpcb set GrpTRESMins=cpu=1 >/dev/null 2>&1; "
               "sacctmgr -nP show assoc where user={user} account=hpcb format=GrpTRESMins | grep -q 'cpu=1' "
               "&& echo 'backstop: GrpTRESMins=cpu=1 on {user}/hpcb'"]
ADMIN_CLEANUP = ["scancel -u {user} 2>/dev/null || true",
                 "sacctmgr -i modify user where name={user} account=hpcb set GrpTRESMins=cpu=-1 >/dev/null 2>&1 || true"]
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — submit a job under an envelope whose site backstop the user names; read why it waits; cancel.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    sub = await jc.submit(ops, command="sleep 60", account="hpcb", partition="compute", cores=1, walltime="00:10:00",
                          envelope={"cap": 1, "site_backstop": BACKSTOP})
    if sub.main_id:
        await ops.sleep(20)
        s = await bc.stop(ops)
        return f"main fallback: hand-submitted {sub.main_id}; stop said {s.get('status')!r}"
    if not sub.job_id:
        ops.abort(f"submit_job did not submit: {sub.answer.get('phase')!r} {bc._brief(sub.answer)!r}")
    deadline = time.monotonic() + 120
    st = await ops.call("status", id=sub.job_id)
    while "AssocGrp" not in str(jc.job_of(st).get("reason") or "") and time.monotonic() < deadline:
        await ops.sleep(bc.POLL_S)
        st = await ops.call("status", id=sub.job_id)
    ops.say(f"{sub.job_id}: {jc.job_of(st).get('state')} ({jc.job_of(st).get('reason')}), charged "
            f"{jc.job_of(st).get('charged')}")
    c = await jc.cancel_until_down(ops, sub.job_id)
    s = await bc.stop(ops)
    return f"cancel {c.get('status')!r}; stop said {s.get('status')!r}"


def terms_name_backstop(t: Trace) -> Result:
    return jc.terms_name_backstop(t, words="GrpTRESMins")


EXTRA_INVARIANTS = [terms_name_backstop, jc.pending_under_backstop, jc.cancel_down]
WORLD_CHECKS = [jc.never_started]
EXPECT_OK = ["agent_engaged", "terms_name_backstop", "pending_under_backstop", "cancel_down"]
