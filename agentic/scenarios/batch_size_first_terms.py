"""batch_size_first_terms — the spend question for a sized batch states the block DERIVED from its rows, as a real slice
of a per-core partition, and the scheduler gives exactly that (batch row sizing). Fake cluster, `slices` profile (its
`shared` partition: cons_tres, CR_Core_Memory, DefMemPerCPU, billed per core); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=slices ./agentic/run_smoke.sh batch_size_first_terms

THE SCRIPT: connect; ensure_endpoint_up(partition="shared") with no confirmation (the session's partition; it asks,
nothing starts); run_batch 6 rows with cores=1, memory_gb=0.5, concurrency=2 → `needs_confirmation`: the terms are a
SLICE of `shared` — 2 × 1 + 1 = 3 CPUs, at least 1 GB, one node — at 2 rows at once, costed per core; the same call
with confirm_spend=True; collect; stop.

GRADED: the terms are the derived block, a slice (whole_node false) on `shared`, and the receipt matches
(`derived_terms`, `slice_on_shared`); every row done; one confirmation; sacct gave the pilot AllocCPUS 3, ReqMem at
least 1 GB, one node (`world:derived_block_allocated`); one block job; stop answers down.

ON THE BASE (906e303): run_batch refuses `cores`, so the script aborts at the sized call.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"slices": "per_core+whole_node", "scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: size first — the terms are the derived slice (3 CPUs, 2 rows at once) and sacct agrees"
TAGS = ["fake", "slices", "batch", "sizing", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

PARTITION = "shared"
N_ROWS = 6
CORES, MEMORY_GB, CONCURRENCY = 1, 0.5, 2
TASK_WALLTIME_S = 120
COMMAND = sc.probe(CORES, "sleep 5")
ROWS = [{"n": i} for i in range(N_ROWS)]
SIZE = {"cores": CORES, "memory_gb": MEMORY_GB, "concurrency": CONCURRENCY}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — pick the per-core `shared` partition, run a sized batch (it asks for the derived slice), "
          "confirm, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    pick = await ops.call("ensure_endpoint_up", shape="compute", partition=PARTITION)
    if pick.get("status") != "needs_confirmation":
        ops.abort(f"choosing {PARTITION!r} did not stay a question: {pick.get('status')!r} {bc._brief(pick)!r}")
    base = {"command": COMMAND, "rows": ROWS, "task_walltime_s": TASK_WALLTIME_S, **SIZE}
    asked = await ops.call("run_batch", **base)
    if asked.get("phase") != "needs_confirmation":
        ops.abort(f"the sized call did not ask for its block: phase={asked.get('phase')!r} {bc._brief(asked)!r}")
    ops.say(f"the derived block: {asked.get('block')}; sizing {asked.get('sizing')}")
    r = await sc.accept(ops, **base, confirm_spend=True)
    st, _rows = await sc.finish(ops, str(r["batch_id"]), N_ROWS, timeout_s=900)
    s = await bc.stop(ops)
    return f"the derived slice; {st.get('phase')!r}; stop said {s.get('status')!r}"


def derived_terms(t: Trace) -> Result:
    return sc.derived_terms(t, cores=CORES, memory_gb=MEMORY_GB, concurrency=CONCURRENCY)


def slice_on_shared(t: Trace) -> Result:
    """The derived block is a SLICE of `shared` (the terms can show it: a per-core partition), costed per core."""
    asked = [r for _i, r in sc.run_batch_answers(t) if r.get("phase") == "needs_confirmation" and r.get("block")]
    if not asked:
        return Result("slice_on_shared", False, "no needs_confirmation with a block")
    b = asked[-1]["block"]
    ok = b.get("queue") == PARTITION and b.get("whole_node") is False and b.get("max_charge") is not None
    return Result("slice_on_shared", ok, f"queue={b.get('queue')!r} whole_node={b.get('whole_node')!r} "
                  f"max_charge={b.get('max_charge')!r} ({b.get('charge_basis')!r})")


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


async def derived_block_allocated(ctx) -> Result:
    return await sc.derived_block_allocated(ctx, cores=CORES, memory_gb=MEMORY_GB, concurrency=CONCURRENCY)


EXTRA_INVARIANTS = [derived_terms, slice_on_shared, batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [derived_block_allocated, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "derived_terms", "slice_on_shared", "batch_done", "one_confirmation", "stop_down",
             "ends_with_stop", "stop_is_honest"]
