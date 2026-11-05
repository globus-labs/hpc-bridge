"""batch_gpu_rows — one GPU per row: the derived block asks for N GPUs (none for the interactive reserve), and each row
sees only its own (batch row sizing). Fake cluster, `site` profile: c3 carries two fake GPUs (gres.conf File=
/dev/nvidia[0-1], device files only — nothing drives them), on the `gpu` partition that only the `hpcb-gpu` account may
use and whose job_submit rule rejects a job with no GPU request; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=site ./agentic/run_smoke.sh batch_gpu_rows

Review finding 5: Delta's and Anvil's templates render no `available_accelerators`, so every row of a batch saw every
GPU. The wrapper claims a GPU per row (node-local) and sets CUDA_VISIBLE_DEVICES (ROCR_VISIBLE_DEVICES,
ZE_AFFINITY_MASK) from the block's measured list.

THE SCRIPT: connect; ensure_endpoint_up(partition="gpu", account="hpcb-gpu") with no confirmation (the session's
choice); run_batch 4 rows of `sleep 10` with cores=1, gpus=1, memory_gb=0.25, concurrency=2 → the derived block (3
CPUs, 2 GPUs); confirm; collect; stop.

GRADED: the terms are the derived block with 2 GPUs (`derived_terms`); every row done; every row saw ONE of the block's
GPUs and rows that ran together saw different ones (`world:gpus_one_per_row`); the rows' cores never summed past the
block's 3 CPUs (`world:cpus_never_oversubscribed`); one block job; stop answers down.

NEEDS LIVE VERIFICATION FIRST: that slurmd sets CUDA_VISIBLE_DEVICES (or SLURM_JOB_GPUS) for the fake GPUs under
task/none — the block's measure reads it; with neither set, it falls back to counting /dev/nvidia*.

ON THE BASE (906e303): run_batch refuses `gpus`, so the script aborts at the sized call.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"gres": "gpu", "accounting": "enforce", "scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: one GPU per row — the derived block asks 2 GPUs for 2 rows, each row sees only its own"
TAGS = ["fake", "site", "gpu", "batch", "sizing", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

PARTITION, ACCOUNT = "gpu", "hpcb-gpu"
N_ROWS = 4
CORES, GPUS, MEMORY_GB, CONCURRENCY = 1, 1, 0.25, 2
TASK_WALLTIME_S = 120
COMMAND = sc.probe(CORES, "sleep 10")
ROWS = [{"n": i} for i in range(N_ROWS)]
SIZE = {"cores": CORES, "gpus": GPUS, "memory_gb": MEMORY_GB, "concurrency": CONCURRENCY}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — on the gpu partition and account, run 4 one-GPU rows two at a time, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    pick = await ops.call("ensure_endpoint_up", shape="compute", partition=PARTITION, account=ACCOUNT)
    if pick.get("status") != "needs_confirmation":
        ops.abort(f"choosing {PARTITION!r} did not stay a question: {pick.get('status')!r} {bc._brief(pick)!r}")
    base = {"command": COMMAND, "rows": ROWS, "task_walltime_s": TASK_WALLTIME_S, **SIZE}
    asked = await ops.call("run_batch", **base)
    if asked.get("phase") != "needs_confirmation":
        ops.abort(f"the sized call did not ask for its block: phase={asked.get('phase')!r} {bc._brief(asked)!r}")
    r = await sc.accept(ops, **base, confirm_spend=True)
    st, _rows = await sc.finish(ops, str(r["batch_id"]), N_ROWS, timeout_s=900)
    s = await bc.stop(ops)
    return f"{st.get('phase')!r}; stop said {s.get('status')!r}"


def derived_terms(t: Trace) -> Result:
    return sc.derived_terms(t, cores=CORES, memory_gb=MEMORY_GB, gpus=GPUS, concurrency=CONCURRENCY)


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=N_ROWS)


async def gpus_one_per_row(ctx) -> Result:
    return await sc.gpus_one_per_row(ctx, gpus=("0", "1"))


async def cpus_never_oversubscribed(ctx) -> Result:
    return await sc.cpus_never_oversubscribed(ctx, block_cpus=CONCURRENCY * CORES + 1)


EXTRA_INVARIANTS = [derived_terms, batch_done, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [gpus_one_per_row, cpus_never_oversubscribed, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "derived_terms", "batch_done", "one_confirmation", "stop_down", "ends_with_stop",
             "stop_is_honest"]
