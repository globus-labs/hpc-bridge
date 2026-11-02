"""hand_submit_gated_adopted — decision D2, gate and adopt (batch phase 2a, design §12 D2, §10; round 3: as the design
says): while an envelope is open, a recognised hand `qsub` answers needs_confirmation and runs nothing; on the user's
yes it runs UNMODIFIED, and the job that appeared while it ran is adopted after the fact (ledgered, reserved against the
envelope, named, no envelope deadline applied) — and stop_endpoint asks about it, then cancels it with everything else
of ours on the user's "cancel". Fake cluster, `pbs` profile (OpenPBS `qselect`, `qstat -f -F json`); scripted. Its
Slurm twin: hand_sbatch_adopted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=pbs ./agentic/run_smoke.sh hand_submit_gated_adopted

THE SCRIPT: connect; submit_job a small job (1 cpu × 10 min) under an envelope of 2 units — the envelope opens; write
`~/hand.pbs` (1 cpu, 10 min, `sleep 300`); run_shell(login, "qsub ~/hand.pbs") — asked, nothing runs; the user's yes:
the same with confirm_spend=True — adopted; status(env) lists it; stop_endpoint until `down`.

GRADED: the hand qsub was gated, then adopted (`gated_then_adopted`) and said so — appeared while the approved command
ran, no envelope deadline (`adopted_named_without_deadline`); the stop ended down (`stop_down_after_jobs`); PBS
says the hand job FINISHED (`world:hand_job_ended` — qdel'd by the stop) and the universal check finds no job of ours
left (the adopted id is ours); within the envelope (`world:charged_within_envelope`).

ON MAIN qsub runs at once (only #176's advisory), nothing adopts it, and the stop leaves it running:
`gated_then_adopted` and `world:hand_job_ended` fail on their own terms.
"""
from __future__ import annotations

import re
import shlex

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "pbs", "mfa": "none"}
SCHEDULER = "pbs"
KIND = "regression"
SUMMARY = "D2: a hand qsub under an open envelope asks first, then runs as is; its job is adopted, and the stop ends it"
TAGS = ["fake", "pbs", "jobs", "envelope", "spend", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

HAND = "~/hand.pbs"
HAND_SCRIPT = "#!/bin/bash\n#PBS -l select=1:ncpus=1\n#PBS -l walltime=00:10:00\n#PBS -N handjob\nsleep 300\n"
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — open an envelope with a small job, hand-submit a qsub script (asked, then adopted), stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    first = await jc.submit(ops, scheduler="pbs", command="sleep 300", cores=1, walltime="00:10:00",
                            envelope={"cap": 2})
    await ops.call("run_shell", shape="login", command=f"printf %s {shlex.quote(HAND_SCRIPT)} > {HAND} && echo written")
    asked = await ops.call("run_shell", shape="login", command=f"qsub {HAND}")
    ops.say(f"the hand qsub answered {asked.get('phase')!r}: {bc._brief(asked)}")
    if asked.get("phase") == "needs_confirmation":
        ops.say("the user says yes to the hand job")
        yes = await ops.call("run_shell", shape="login", command=f"qsub {HAND}", confirm_spend=True)
        ops.say(f"after the yes: {yes.get('phase')!r}, jobs {[j.get('job_id') for j in yes.get('jobs') or []]}")
    env_id = str(jc.envelope_of(first.answer).get("envelope_id") or "")
    if env_id:
        st = await ops.call("status", id=env_id)
        ops.say(f"envelope {env_id}: jobs {(st.get('envelope') or {}).get('job_ids')}")
    s = await jc.stop(ops, tries=8)
    return f"stop said {s.get('status')!r}"


def gated_then_adopted(t: Trace) -> Result:
    return jc.gated_then_adopted(t, submit_cmd="qsub")


def _hand_id(t: Trace) -> str | None:
    for _i, c in t.named("run_shell"):
        if "qsub" not in str(c.input.get("command") or ""):
            continue
        r = c.result or {}
        jobs = [j for j in r.get("jobs") or [] if isinstance(j, dict) and j.get("native_id")]
        if jobs:
            return str(jobs[0]["native_id"])
        m = re.search(r"^(\d+(?:\.[\w.-]+)?)\s*$", str(r.get("stdout") or ""), re.M)
        if m:
            return m.group(1)
    return None


async def hand_job_ended(ctx) -> Result:
    """world: PBS says the hand job is finished (F) — never still queued or running after the stop."""
    hid = _hand_id(ctx.trace)
    if hid is None:
        return Result("world:hand_job_ended", False, "premise not met: no hand qsub ran (no job id)")
    _rc, out = ctx.ssh(f"qstat -x -f {shlex.quote(hid)} 2>&1 | sed -n 's/^ *job_state = //p'", 60)
    state = out.strip()
    return Result("world:hand_job_ended", state == "F", f"hand job {hid}: job_state {state or '?'}")


EXTRA_INVARIANTS = [gated_then_adopted, jc.adopted_named_without_deadline, jc.stop_asked_about_jobs,
                    jc.stop_down_after_jobs]
WORLD_CHECKS = [hand_job_ended, jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "gated_then_adopted", "adopted_named_without_deadline", "stop_asked_about_jobs",
             "stop_down_after_jobs"]
