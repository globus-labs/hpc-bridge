"""envelope_bypass_counted — the honest limit, counted (batch phase 2a, design §3 "The meter never understates",
"The honest limit"): a submit hidden from hpc-bridge's lexer — Python's subprocess running `sbatch` — passes the D2
gate, so the envelope cannot BIND it; but the meter's sweep (`sacct -u $USER -S <opened>`) finds it, COUNTS it against
the envelope and NAMES it, so the user can say whether it is theirs. Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh envelope_bypass_counted

THE SCRIPT: connect; submit_job a small job under an envelope of 2 (the yes); run_shell(login) `python3 -c "…
subprocess.run(['sbatch', …, '-J', 'bypass', …])"` — no gate (the lexer cannot see it); status(env) — it lists and
counts the `bypass` job; stop (ours ended; the bypass job is not ledgered, so it is left — named — and KEEP_RUNNING
tells the universal check so; ADMIN_CLEANUP cancels it).

GRADED: an envelope answer lists the bypass job as unledgered, counts it, names its id (`bypass_named_and_counted`);
the stop ended ours (`stop_down_after_jobs`).

ON MAIN there is no meter: nothing names or counts it (`bypass_named_and_counted` fails).
"""
from __future__ import annotations

import batch_checks as bc
import jobs_checks as jc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "envelope: a submit hidden in Python's subprocess passes the gate — the meter's sweep counts and names it"
TAGS = ["fake", "jobs", "envelope", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

BYPASS = "bypass"
KEEP_RUNNING = (BYPASS,)   # not hpc-bridge's: the stop must leave it alone (the universal check's allowlist)
ADMIN_CLEANUP = [f"scancel -u {{user}} -n {BYPASS} 2>/dev/null || true"]
HIDDEN = ("python3 -c \"import subprocess; subprocess.run(['sbatch', '--parsable', '-J', '" + BYPASS + "', '-t', "
          "'10', '-c', '1', '--wrap', 'sleep 300'])\"")
PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — open an envelope, submit a job the lexer cannot see, and read the envelope back.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    a = await jc.submit(ops, command="sleep 300", cores=1, walltime="00:10:00", envelope={"cap": 2})
    hidden = await ops.call("run_shell", shape="login", command=HIDDEN)
    ops.say(f"the hidden submit: {hidden.get('phase')!r} stdout {str(hidden.get('stdout') or '').strip()!r}")
    env_id = str(jc.envelope_of(a.answer).get("envelope_id") or "")
    if env_id:
        st = await ops.call("status", id=env_id)
        e = st.get("envelope") or {}
        ops.say(f"{env_id}: spent {e.get('spent')}, committed {e.get('committed')}, unledgered "
                f"{[u.get('native_id') for u in e.get('unledgered') or []]}")
    s = await bc.stop(ops, tries=8)
    return f"stop said {s.get('status')!r}"


def bypass_named_and_counted(t: Trace) -> Result:
    return jc.bypass_named_and_counted(t, name=BYPASS)


EXTRA_INVARIANTS = [bypass_named_and_counted, jc.stop_down_after_jobs]
EXPECT_OK = ["agent_engaged", "bypass_named_and_counted", "stop_down_after_jobs"]
