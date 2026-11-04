"""batch_pilot_proposal — a user who does not know the size runs a pilot: one row alone with the whole block, then a
proposal in sbatch terms (seff's pattern), then the rest at that size (batch row sizing). Fake cluster, `default`
profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_pilot_proposal

THE SCRIPT: connect, confirm one block; run_batch 3 rows with pilot=1 — each row keeps two CPUs busy for 6 s (two
`yes`) and holds 200 MB; wait for `needs_sizing`; resume with the proposal (run_batch(batch_id=…, cores=…,
memory_gb=…), no new block); collect; stop.

GRADED: the pilot's answer proposes cores 2 (⌈CPU-s ÷ wall⌉, with its tolerance) and memory 1.25 × the peak RSS,
between 0.25 and 0.75 GB (`pilot_proposed`); nothing but the pilot ran before the proposal (`pilot_ran_alone`); every
row done; one confirmation; stop answers down.

ON THE BASE (906e303): run_batch refuses `pilot`, so the script aborts.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: pilot=1 runs one row alone, proposes cores 2 and ~0.25-0.5 GB, the rest run at that size"
TAGS = ["fake", "batch", "sizing", "pilot", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 3
TASK_WALLTIME_S = 120
WORK = ("timeout 6 yes > /dev/null & timeout 6 yes > /dev/null & "
        "python3 -c 'import time; b = b\"x\" * (200 << 20); time.sleep(6)' & wait")
COMMAND = WORK + "; echo row {n} done"
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, pilot one row of a batch, resume at the proposed size, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r = await sc.accept(ops, command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, pilot=1)
    bid = str(r["batch_id"])
    st = await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in sc.ENDED, timeout_s=600)
    p = dict(st.get("proposal") or {})
    if st.get("phase") != "needs_sizing" or not p:
        ops.abort(f"the pilot ended without a proposal: phase={st.get('phase')!r} {bc._brief(st)!r}")
    ops.say(f"the proposal: {p}")
    res = await ops.call("run_batch", batch_id=bid, cores=p.get("cores"), memory_gb=p.get("memory_gb"))
    if res.get("phase") not in ("admitting", "waiting", "done"):
        ops.abort(f"the resize was not taken: phase={res.get('phase')!r} {bc._brief(res)!r}")
    st, _rows = await sc.finish(ops, bid, N_ROWS)
    s = await bc.stop(ops)
    return f"proposal {p}; {st.get('phase')!r}; stop said {s.get('status')!r}"


def pilot_proposed(t: Trace) -> Result:
    return sc.pilot_proposed(t, cores=2, memory_gb=(0.25, 0.75))


def pilot_ran_alone(t: Trace) -> Result:
    """Until the proposal, ONE row of the batch was ever out (the journal's admissions)."""
    asked = [i for i, c in t.named("status", "run_batch") if (c.result or {}).get("phase") == "needs_sizing"]
    if not asked:
        return Result("pilot_ran_alone", False, "no needs_sizing answer")
    cut = bc.epoch(t, t.calls[asked[0]].t_result)
    admits = [e for e in bc.events(t, "batch.admit") if cut is None or float(e.get("ts") or 0) <= cut]
    return Result("pilot_ran_alone", len(admits) == 1, f"{len(admits)} admission(s) before the proposal")


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


EXTRA_INVARIANTS = [pilot_proposed, pilot_ran_alone, batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [bc.one_block_job]
EXPECT_OK = ["agent_engaged", "pilot_proposed", "pilot_ran_alone", "batch_done", "one_confirmation", "stop_down",
             "ends_with_stop", "stop_is_honest"]
