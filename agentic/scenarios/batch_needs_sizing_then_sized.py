"""batch_needs_sizing_then_sized — with no size and no block, run_batch asks for the size BEFORE any spend question,
with the facts to answer it in sbatch terms; the sized call then asks for the block derived from it (batch row sizing:
"size first, the sbatch way"). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_needs_sizing_then_sized

THE SCRIPT (no ensure_endpoint_up: the batch brings its own block):
1. connect; run_batch(command, 4 rows) with no size → `needs_sizing` (nothing recorded, no block, no spend question);
2. run_batch(…, cores=1, memory_gb=0.25, concurrency=2) → `needs_confirmation` naming the DERIVED block (2 × 1 + 1 =
   3 CPUs, at least 0.5 GB, one node) and the concurrency;
3. the same with confirm_spend=True (the user's yes) → the batch runs; collect; stop.

GRADED: the first answer is needs_sizing with the facts — the node's CPUs and memory and a candidate table — and no
billed start before it (`needs_sizing_first`); the terms are the derived block and the receipt matches
(`derived_terms`); every row done; ONE confirmation; sacct gave the pilot 3 CPUs on one node and at least 0.5 GB
(`world:derived_block_allocated`) and lists one block job; stop answers down.

ON THE BASE (906e303): the first run_batch asks the spend question (needs_confirmation), never for a size.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: no size and no block → needs_sizing with the facts; the sized call asks for the derived block"
TAGS = ["fake", "batch", "sizing", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 4
CORES, MEMORY_GB, CONCURRENCY = 1, 0.25, 2
TASK_WALLTIME_S = 120
COMMAND = sc.probe(CORES, "sleep 5")
ROWS = [{"n": i} for i in range(N_ROWS)]
SIZE = {"cores": CORES, "memory_gb": MEMORY_GB, "concurrency": CONCURRENCY}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — run a batch with no size (it asks), then sized (it asks for the derived block), confirm, "
          "collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    base = {"command": COMMAND, "rows": ROWS, "task_walltime_s": TASK_WALLTIME_S}
    first = await ops.call("run_batch", **base)
    if first.get("phase") != "needs_sizing":
        ops.abort(f"run_batch with no size and no block did not ask for one: phase={first.get('phase')!r} "
                  f"{bc._brief(first)!r}")
    asked = await ops.call("run_batch", **base, **SIZE)
    if asked.get("phase") != "needs_confirmation":
        ops.abort(f"the sized call did not ask for its block: phase={asked.get('phase')!r} {bc._brief(asked)!r}")
    ops.say(f"the derived block: {asked.get('block')}")
    r = await sc.accept(ops, **base, **SIZE, confirm_spend=True)
    st, _rows = await sc.finish(ops, str(r["batch_id"]), N_ROWS, timeout_s=900)
    s = await bc.stop(ops)
    return f"needs_sizing, then the derived block; {st.get('phase')!r}; stop said {s.get('status')!r}"


def derived_terms(t: Trace) -> Result:
    return sc.derived_terms(t, cores=CORES, memory_gb=MEMORY_GB, concurrency=CONCURRENCY)


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


async def derived_block_allocated(ctx) -> Result:
    return await sc.derived_block_allocated(ctx, cores=CORES, memory_gb=MEMORY_GB, concurrency=CONCURRENCY)


EXTRA_INVARIANTS = [sc.needs_sizing_first, derived_terms, batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [derived_block_allocated, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "needs_sizing_first", "derived_terms", "batch_done", "one_confirmation", "stop_down",
             "ends_with_stop", "stop_is_honest"]
