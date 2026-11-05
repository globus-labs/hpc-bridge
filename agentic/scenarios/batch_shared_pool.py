"""batch_shared_pool — two batches of different row sizes on ONE block share its CPUs, each row seeing exactly its own
cores (batch row sizing: one slot pool per block, extending PR 1's shared window). Fake cluster, `default` profile;
scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_shared_pool

THE SCRIPT: connect, confirm one block; run_batch A (4 rows of `sleep 15`, cores=1) and, right after, B (3 rows of
`sleep 15`, cores=2); collect both; stop.

GRADED: both batches done; the cores of the rows running at any instant, A's and B's together, never summed past the
block's 4 CPUs, and rows that ran together had disjoint masks (`world:cpus_never_oversubscribed`); every row saw its
OWN batch's cores — 1 for A's, 2 for B's — by nproc and by its mask (`world:nproc_matches_cores`); one block job;
stop answers down.

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
SUMMARY = "batch row sizing: a 1-CPU and a 2-CPU batch share one 4-CPU block's pool — never past 4 CPUs, each row on its own"
TAGS = ["fake", "batch", "sizing", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

BLOCK_CPUS = 4
TASK_WALLTIME_S = 120
A_ROWS, B_ROWS = 4, 3
A = {"command": sc.probe(1, "sleep 15"), "rows": [{"n": i} for i in range(A_ROWS)], "cores": 1}
B = {"command": sc.probe(2, "sleep 15"), "rows": [{"n": i} for i in range(B_ROWS)], "cores": 2}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, run a 1-CPU batch and a 2-CPU batch on it together, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    a = await sc.accept(ops, task_walltime_s=TASK_WALLTIME_S, memory_gb=0.25, **A)
    b = await sc.accept(ops, task_walltime_s=TASK_WALLTIME_S, memory_gb=0.25, **B)
    sa, _ = await sc.finish(ops, str(a["batch_id"]), A_ROWS)
    sb, _ = await sc.finish(ops, str(b["batch_id"]), B_ROWS)
    s = await bc.stop(ops)
    return f"A {sa.get('phase')!r}, B {sb.get('phase')!r}; stop said {s.get('status')!r}"


def both_done(t: Trace) -> Result:
    finals: dict[str, dict] = {}
    for r in sc.statuses(t):
        finals[str(r["batch_id"])] = r
    done = {bid: (r.get("phase"), (r.get("counts") or {}).get("done")) for bid, r in finals.items()}
    ok = len(finals) == 2 and sorted(n for _p, n in done.values()) == sorted([A_ROWS, B_ROWS]) \
        and all(p == "done" for p, _n in done.values())
    return Result("both_done", ok, f"batches: {done}")


async def cpus_never_oversubscribed(ctx) -> Result:
    return await sc.cpus_never_oversubscribed(ctx, block_cpus=BLOCK_CPUS)


EXTRA_INVARIANTS = [both_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [cpus_never_oversubscribed, sc.nproc_matches_cores, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "both_done", "one_confirmation", "stop_down", "ends_with_stop", "stop_is_honest"]
