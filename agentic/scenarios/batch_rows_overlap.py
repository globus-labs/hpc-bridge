"""batch_rows_overlap — 1-CPU rows really run together on one block (batch row sizing; vault `Planned/Batch and swarm
execution.md`, "Batch row sizing"). Fake cluster, `default` profile (two 4-CPU nodes, task/none: Slurm confines
nothing, so the pin is hpc-bridge's own); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_rows_overlap

The demo of 2026-11-03: every SSH batch ran ONE row at a time (the template's 2 workers less one kept back). Now the
block's workers follow its cores and each row holds one CPU: on a 4-CPU block at least 3 rows run at once.

THE SCRIPT: connect, confirm one block, run_batch 8 rows of `sleep 20` with cores=1 (each row prints its own start and
end, nproc and CPU mask: sizing_checks.probe), collect, stop.

GRADED: every row done (`batch_done`); at least 3 rows ran at one instant, from each row's OWN start and end in its
stdout file (`world:rows_overlap`: the lower bound — a window that admits 3 and runs them one after another fails),
and never more than the batch announced; no two rows that ran together shared a CPU and their cores never summed past
4 (`world:cpus_never_oversubscribed`); each row saw exactly 1 CPU, by nproc and by its mask
(`world:nproc_matches_cores`); one block job; stop answers down.

ON THE BASE (906e303): run_batch refuses `cores` (an unexpected argument), so the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: 8 one-CPU rows on a 4-CPU block — at least 3 really run at once, each pinned to its own CPU"
TAGS = ["fake", "batch", "sizing", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 8
TASK_WALLTIME_S = 120
BLOCK_CPUS = 4                # the default profile's nodes (slurm.conf CPUs=4)
AT_LEAST = 3                  # 4 CPUs at 1 per row, the interactive worker beside (it may take the 4th CPU's worker)
COMMAND = sc.probe(1, "sleep 20")
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, run 8 one-CPU rows of `sleep 20` through run_batch, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r = await sc.accept(ops, command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, cores=1, memory_gb=0.25)
    st, _rows = await sc.finish(ops, str(r["batch_id"]), N_ROWS)
    s = await bc.stop(ops)
    return f"{st.get('phase')!r}, slots {sc.sizing_of(r).get('slots')!r}; stop said {s.get('status')!r}"


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


async def rows_overlap(ctx) -> Result:
    return await sc.rows_overlap(ctx, at_least=AT_LEAST)


async def cpus_never_oversubscribed(ctx) -> Result:
    return await sc.cpus_never_oversubscribed(ctx, block_cpus=BLOCK_CPUS)


EXTRA_INVARIANTS = [batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [rows_overlap, cpus_never_oversubscribed, sc.nproc_matches_cores, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "batch_done", "one_confirmation", "stop_down", "ends_with_stop", "stop_is_honest"]
