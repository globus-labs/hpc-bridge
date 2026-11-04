"""batch_memory_kill — a row over its memory is killed by hpc-bridge at its limit, and the row beside it lives (batch
row sizing). Fake cluster, `default` profile: Slurm confines no memory there (task/none, cgroup/v1 unused), so the
kernel would let the row grow — what ends it is the wrapper's own kill, which the scenario isolates; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_memory_kill

Why (review finding 6): on a site the overrun's blast radius is the site's — under Slurm cgroups the OOM killer may
take a NEIGHBOURING row; under OverMemoryKill the whole step (the worker pool, so the block); under PBS's `$enforce
mem` the job. The wrapper sums the row's process group's RSS each second and kills the row at its limit first.

THE SCRIPT: connect, confirm one block; run_batch 2 rows at once (concurrency=2, memory_gb=0.25): row 0 holds 400 MB
for 20 s, row 1 holds 50 MB for 8 s and prints "fine"; collect; stop.

GRADED: row 0 failed with why=memory and the memory rc (`memory_killed_with_why`); row 1 done, rc 0, "fine" in its
tail (`neighbour_survived`); the two started together and row 1 ran past row 0's kill (`world:neighbour_ran_beside_the
_kill`, from the rows' own lines); one block job; stop answers down.

ON THE BASE (906e303): run_batch refuses `memory_gb`, so the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: a row over memory_gb is killed at its limit (why=memory), its neighbour on the block lives"
TAGS = ["fake", "batch", "sizing", "memory", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

TASK_WALLTIME_S = 120
HOG = ("python3 -c 'import sys, time; b = b\"x\" * (int(sys.argv[1]) << 20); time.sleep(int(sys.argv[2])); "
       "print(\"fine\")' {mb} {secs}")
COMMAND = sc.probe(1, HOG)
ROWS = [{"n": 0, "mb": 400, "secs": 20}, {"n": 1, "mb": 50, "secs": 8}]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, run a row over its memory beside one within it, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r = await sc.accept(ops, command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, cores=1, memory_gb=0.25,
                        concurrency=2)
    st, got = await sc.finish(ops, str(r["batch_id"]), len(ROWS))
    s = await bc.stop(ops)
    return (f"{st.get('phase')!r}; row 0 {got.get(0, {}).get('why')!r} rc {got.get(0, {}).get('exit_code')!r}, row 1 "
            f"{got.get(1, {}).get('state')!r}; stop said {s.get('status')!r}")


def memory_killed_with_why(t: Trace) -> Result:
    return sc.memory_killed_with_why(t, index=0)


def neighbour_survived(t: Trace) -> Result:
    return sc.neighbour_survived(t, index=1, word="fine")


async def neighbour_ran_beside_the_kill(ctx) -> Result:
    return await sc.neighbour_ran_beside_the_kill(ctx, killed=0, neighbour=1)


EXTRA_INVARIANTS = [memory_killed_with_why, neighbour_survived, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [neighbour_ran_beside_the_kill, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "memory_killed_with_why", "neighbour_survived", "one_confirmation", "stop_down",
             "ends_with_stop", "stop_is_honest"]
