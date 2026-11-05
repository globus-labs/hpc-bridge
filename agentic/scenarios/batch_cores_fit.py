"""batch_cores_fit — rows of 2 CPUs on a 4-CPU block: never more out than fits, each on its own 2 CPUs (batch row
sizing). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_cores_fit

THE SCRIPT: connect, confirm one block, run_batch 6 rows of `sleep 12` with cores=2 (no concurrency: the block
decides), collect, stop.

GRADED: the batch announced 2 slots, bound by the block's CPUs (`sizing_announced`); every row done; two rows really
ran together and the rows running at any instant never held more than the block's 4 CPUs, on disjoint masks
(`world:rows_overlap` with at least 2, `world:cpus_never_oversubscribed`); each row saw exactly 2 CPUs
(`world:nproc_matches_cores`); one block job; stop answers down.

ON THE BASE (906e303): run_batch refuses `cores`, so the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: 2-CPU rows on a 4-CPU block — 2 at once, never more, each pinned to its own 2 CPUs"
TAGS = ["fake", "batch", "sizing", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 6
CORES = 2
BLOCK_CPUS = 4
TASK_WALLTIME_S = 120
COMMAND = sc.probe(CORES, "sleep 12")
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, run 6 two-CPU rows of `sleep 12` through run_batch, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r = await sc.accept(ops, command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, cores=CORES,
                        memory_gb=0.25)
    st, _rows = await sc.finish(ops, str(r["batch_id"]), N_ROWS)
    s = await bc.stop(ops)
    return f"{st.get('phase')!r}, sizing {sc.sizing_of(r)}; stop said {s.get('status')!r}"


def sizing_announced(t: Trace) -> Result:
    return sc.sizing_announced(t, cores=CORES, slots=BLOCK_CPUS // CORES, bound_by="cpus")


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


async def rows_overlap(ctx) -> Result:
    return await sc.rows_overlap(ctx, at_least=BLOCK_CPUS // CORES)


async def cpus_never_oversubscribed(ctx) -> Result:
    return await sc.cpus_never_oversubscribed(ctx, block_cpus=BLOCK_CPUS)


EXTRA_INVARIANTS = [sizing_announced, batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [rows_overlap, cpus_never_oversubscribed, sc.nproc_matches_cores, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "sizing_announced", "batch_done", "one_confirmation", "stop_down", "ends_with_stop",
             "stop_is_honest"]
