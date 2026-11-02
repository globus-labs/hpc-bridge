"""envelope_leftover_held — a job of the user's already in the queue BEFORE the envelope is proposed (batch phase 2a, fix
round 2; live 2026-11-01: job_promote_track_cancel's proposal offered exactly the job's worst case, the meter then
counted a leftover job of the user's, and the yes refused itself — "exhausted … this job needs 0.17"). Fake cluster,
`default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh envelope_leftover_held

THE SCRIPT: connect; run_shell(login) `python3 -c "… subprocess.run(['sbatch', …, '-J', 'leftover', …])"` — a job of
the user's hpc-bridge did not start, queued first (the lexer cannot see it; no envelope is open anyway); submit_job a
small job with NO cap of its own — the proposal must be made after a meter reading, cover the leftover's hold plus the
job's worst case, and name the leftover; the user's yes (once more if the proposal comes again) — admitted;
status(env); stop (ours ended; the leftover is not ledgered, so it is left — named — KEEP_RUNNING tells the universal
check; ADMIN_CLEANUP cancels it).

GRADED: the first proposal covers what was held and names the leftover (`proposal_covers_held`); the yes was admitted,
never refused itself (`proposal_yes_admits`); the stop ended ours (`stop_down_after_jobs`).

AT 3d0b272 the proposal was the job's worst case alone: `proposal_covers_held` fails, and when the leftover counts by
the yes, `proposal_yes_admits` fails too.
"""
from __future__ import annotations

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "envelope: a leftover job of the user's queued before the proposal — the proposal covers and names it"
TAGS = ["fake", "jobs", "envelope", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

LEFTOVER = "leftover"
KEEP_RUNNING = (LEFTOVER,)   # not hpc-bridge's: the stop must leave it alone (the universal check's allowlist)
ADMIN_CLEANUP = [f"scancel -u {{user}} -n {LEFTOVER} 2>/dev/null || true"]
HIDDEN = ("python3 -c \"import subprocess; subprocess.run(['sbatch', '--parsable', '-J', '" + LEFTOVER + "', '-t', "
          "'10', '-c', '1', '--wrap', 'sleep 300'])\"")
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — queue a job of the user's first, then propose and open an envelope for a small job.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    hidden = await ops.call("run_shell", shape="login", command=HIDDEN)
    ops.say(f"the leftover job: {hidden.get('phase')!r} stdout {str(hidden.get('stdout') or '').strip()!r}")
    a = await jc.submit(ops, command="sleep 300", cores=1, walltime="00:10:00")
    ops.say(f"the yes: {a.answer.get('phase')!r} {str(a.answer.get('notice') or '')[:200]!r}")
    env_id = str(jc.envelope_of(a.answer).get("envelope_id") or "")
    if env_id:
        st = await ops.call("status", id=env_id)
        e = st.get("envelope") or {}
        ops.say(f"{env_id}: cap {e.get('cap')}, spent {e.get('spent')}, committed {e.get('committed')}, unledgered "
                f"{[u.get('native_id') for u in e.get('unledgered') or []]}")
    s = await jc.stop(ops, tries=8)
    return f"stop said {s.get('status')!r}"


def proposal_covers_held(t: Trace) -> Result:
    return jc.proposal_covers_held(t, name=LEFTOVER)


EXTRA_INVARIANTS = [proposal_covers_held, jc.proposal_yes_admits, jc.stop_down_after_jobs]
EXPECT_OK = ["agent_engaged", "proposal_covers_held", "proposal_yes_admits", "stop_down_after_jobs"]
