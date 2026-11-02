"""hand_sbatch_adopted — decision D2 on Slurm, as the design says (batch phase 2a, fix round 3, the user's decision): while
an envelope is open, a recognised hand `sbatch` answers needs_confirmation and runs nothing; on the user's yes it runs
UNMODIFIED, and the job that appeared while it ran is adopted after the fact — ledgered, reserved against the envelope,
named as a job that appeared while the approved command ran, no envelope deadline applied — then the stop asks about it
by name and cancels it on the user's "cancel". Fake cluster, `default` profile; scripted. Its PBS twin:
hand_submit_gated_adopted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh hand_sbatch_adopted

THE SCRIPT: connect; submit_job a small job (1 core × 10 min) under an envelope of 2 units; write `~/hand.sh` (1 core,
10 min, `sleep 300`); run_shell(login, "sbatch ~/hand.sh") — asked, nothing runs; the user's yes: the same with
confirm_spend=True — adopted; status(env) lists it; stop (asks, naming both; "cancel") until `down`.

GRADED: gated, then adopted (`gated_then_adopted`) and said so (`adopted_named_without_deadline`); the stop asked about
both jobs (`stop_asked_about_jobs`) and ended down (`stop_down_after_jobs`); Slurm says the hand job never got an
envelope deadline (`world:hand_job_no_deadline`) and ended CANCELLED (`world:hand_job_cancelled`); the universal check
finds nothing of ours left.
"""
from __future__ import annotations

import re
import shlex

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "D2 on Slurm: a hand sbatch under an open envelope asks first, then runs as is; its job is adopted, no deadline"
TAGS = ["fake", "jobs", "envelope", "spend", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

HAND = "~/hand.sh"
HAND_SCRIPT = "#!/bin/bash\n#SBATCH -t 00:10:00\n#SBATCH -c 1\n#SBATCH -J handjob\nsleep 300\n"
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — open an envelope with a small job, hand-submit an sbatch script (asked, then adopted), stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    first = await jc.submit(ops, command="sleep 300", cores=1, walltime="00:10:00", envelope={"cap": 2})
    await ops.call("run_shell", shape="login", command=f"printf %s {shlex.quote(HAND_SCRIPT)} > {HAND} && echo written")
    asked = await ops.call("run_shell", shape="login", command=f"sbatch {HAND}")
    ops.say(f"the hand sbatch answered {asked.get('phase')!r}: {bc._brief(asked)}")
    if asked.get("phase") == "needs_confirmation":
        ops.say("the user says yes to the hand job")
        yes = await ops.call("run_shell", shape="login", command=f"sbatch {HAND}", confirm_spend=True)
        ops.say(f"after the yes: {yes.get('phase')!r}, jobs {[j.get('job_id') for j in yes.get('jobs') or []]}")
    env_id = str(jc.envelope_of(first.answer).get("envelope_id") or "")
    if env_id:
        st = await ops.call("status", id=env_id)
        ops.say(f"envelope {env_id}: jobs {(st.get('envelope') or {}).get('job_ids')}")
    s = await jc.stop(ops, tries=8)
    return f"stop said {s.get('status')!r}"


def gated_then_adopted(t: Trace) -> Result:
    return jc.gated_then_adopted(t, submit_cmd="sbatch")


def _hand_id(t: Trace) -> str | None:
    for _i, c in t.named("run_shell"):
        if "sbatch" not in str(c.input.get("command") or ""):
            continue
        jobs = [j for j in (c.result or {}).get("jobs") or [] if isinstance(j, dict) and j.get("native_id")]
        if jobs:
            return str(jobs[0]["native_id"])
        m = re.search(r"Submitted batch job (\d+)", str((c.result or {}).get("stdout") or ""))
        if m:
            return m.group(1)
    return None


async def hand_job_no_deadline(ctx) -> Result:
    """world: Slurm shows no deadline on the adopted hand job — the yes ran the command unmodified."""
    hid = _hand_id(ctx.trace)
    if hid is None:
        return Result("world:hand_job_no_deadline", False, "premise not met: no hand sbatch ran (no job id)")
    _rc, out = ctx.ssh(f"scontrol show job -o {shlex.quote(hid)} 2>&1 | tr ' ' '\\n' | sed -n 's/^Deadline=//p'", 60)
    dl = out.strip()
    ok = dl in ("N/A", "", "Unknown", "None") or "Invalid job id" in out
    return Result("world:hand_job_no_deadline", ok, f"hand job {hid}: Deadline={dl or '?'}")


async def hand_job_cancelled(ctx) -> Result:
    """world: sacct says the adopted hand job ended CANCELLED — the stop's "cancel" ended it."""
    hid = _hand_id(ctx.trace)
    if hid is None:
        return Result("world:hand_job_cancelled", False, "premise not met: no hand sbatch ran (no job id)")
    _rc, out = ctx.ssh(f"sacct -X -n -P -j {shlex.quote(hid)} -o State 2>&1 | head -n 1", 60)
    state = (out.strip().split() or ["?"])[0]
    return Result("world:hand_job_cancelled", state.startswith("CANCELLED"), f"hand job {hid}: {state}")


EXTRA_INVARIANTS = [gated_then_adopted, jc.adopted_named_without_deadline, jc.stop_asked_about_jobs,
                    jc.stop_down_after_jobs]
WORLD_CHECKS = [hand_job_no_deadline, hand_job_cancelled, jc.charged_within_envelope]
EXPECT_OK = ["agent_engaged", "gated_then_adopted", "adopted_named_without_deadline", "stop_asked_about_jobs",
             "stop_down_after_jobs"]
