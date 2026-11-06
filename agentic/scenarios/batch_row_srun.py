"""batch_row_srun — a batch row's own `srun` runs (the Anvil live test, F3), and its result says it ran a step. Fake
cluster, `default` profile (Slurm 23.11, as Anvil runs); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_row_srun

Why: on Anvil a 1-CPU row's `srun -n1 bash -c …` on a 7-CPU block died — `srun: fatal: cpus-per-task set by two
different environment variables SLURM_CPUS_PER_TASK=1 != SLURM_TRES_PER_TASK=cpu=7` — and, once that agreed, on the
worker launcher step's binding (`CPU binding outside of job step allocation`). Slurm 23.11 sets SLURM_TRES_PER_TASK in
the block's job (its --cpus-per-task: the slice hpc-bridge asks for), so the first failure shows here too; the fake's
task/none sets no binding, so the second is the node test's (tests/test_anvil_live_findings_node.py, a fake srun).

THE SCRIPT: connect, confirm one block, run_batch 2 rows at once (cores=1, memory_gb=0.25), each running `srun -n1
bash -c 'sleep 3; echo HPCB_STEP_OK …'` and printing srun's rc; collect; stop.

GRADED: each row done, rc 0, with the step's line and `HPCB_SRUN_RC=0` in its tail (`srun_ran_in_each_row`); each
result says the row ran srun steps, and status says where such a step runs (`srun_steps_said`: on CPUs Slurm picks
within the block, not the row's slot); one confirmation; stop answers down.

ON THE BASE (822347c): the step dies on the two cpus-per-task variables (rc 1, `HPCB_SRUN_RC=1`), and no result says
a step ran — both graders fail.
"""
from __future__ import annotations

import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch row sizing: a row's own `srun` runs on Slurm 23.11 (its env agrees), and its result says it ran a step"
TAGS = ["fake", "batch", "sizing", "srun", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 2
TASK_WALLTIME_S = 120
STEP = "srun -n1 bash -c 'sleep 3; echo HPCB_STEP_OK $(hostname)'; echo HPCB_SRUN_RC=$?"
COMMAND = sc.probe(1, STEP)
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, run 2 one-CPU rows that each run an srun step, collect, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r = await sc.accept(ops, command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, cores=1, memory_gb=0.25,
                        concurrency=N_ROWS)
    st, got = await sc.finish(ops, str(r["batch_id"]), N_ROWS)
    s = await bc.stop(ops)
    return (f"{st.get('phase')!r}; rows " + ", ".join(f"{i}: {g.get('state')!r} rc {g.get('exit_code')!r} srun_steps "
                                                      f"{g.get('srun_steps')!r}" for i, g in sorted(got.items()))
            + f"; stop said {s.get('status')!r}")


def srun_ran_in_each_row(t: Trace) -> Result:
    return sc.srun_ran(t, rows=N_ROWS)


def srun_steps_said(t: Trace) -> Result:
    return sc.srun_steps_said(t, rows=N_ROWS)


EXTRA_INVARIANTS = [srun_ran_in_each_row, srun_steps_said, bc.one_confirmation, bc.stop_down]
WORLD_CHECKS = [bc.one_block_job]
EXPECT_OK = ["agent_engaged", "srun_ran_in_each_row", "srun_steps_said", "one_confirmation", "stop_down",
             "ends_with_stop", "stop_is_honest"]
