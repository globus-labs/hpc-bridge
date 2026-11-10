"""env_ssh_switch — switching envs on an endpoint hpc-bridge stood up over SSH needs no new block and leaves nothing of
the old env behind; a batch given its own env runs every row in it while the session keeps its own (block
environments, PR 3). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh env_ssh_switch

THE SCRIPT: connect, confirm one block; a built env (six) and the probe; switch to an existing env (`activate`: a line
that sets HPCB_TAG=b) and the probe; run_batch 3 probe rows with the built env as the BATCH's own; the probe (still the
session's `activate` env); switch to no env ({}) and the probe; stop.

GRADED: every switch answered `up` — never a spend question — and the run confirmed one block
(`env_switch_no_new_block`); after the switch to the existing env a command saw HPCB_TAG=b and nothing of the built env
(no VIRTUAL_ENV, its bin not first on PATH), and after the switch to none, neither (`env_switch_clean`); the batch's
answer stated its env and every row ran in it (`batch_rows_in_env`); stop answers down. World: one block job.

NEEDS the fake cluster's login node to reach PyPI. ON THE BASE (bcc97a4): ensure_endpoint_up takes no `env`: the cell
aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import envs_checks as ec
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "block envs: switching env (built, existing, none) needs no new block and leaves nothing behind; a batch's own env"
TAGS = ["fake", "env", "ssh", "batch", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

TAG = "b"
N_ROWS = 3
TASK_WALLTIME_S = 120
COMMAND = "echo row {n}; " + ec.PROBE
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, switch between a built env, an existing one and none, run a batch in the "
          "built env, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    built = {"requirements": ec.REQUIREMENTS, "root": ec.root_for(ops)}
    await ec.ask_env(ops, built)
    await ec.probe(ops)
    await ec.ask_env(ops, {"activate": f"export HPCB_TAG={TAG}", "name": "tagged"})
    await ec.probe(ops)
    r = await ops.call("run_batch", command=COMMAND, rows=ROWS, task_walltime_s=TASK_WALLTIME_S, env=built)
    bid = str(r.get("batch_id") or "")
    if not bid or bc.unknown_tool(r):
        ops.abort(f"run_batch did not accept the batch with its env: {bc._brief(r)!r}")
    await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused"), timeout_s=600)
    await bc.all_rows(ops, bid, N_ROWS)
    await ec.probe(ops)
    await ec.ask_env(ops, {})
    await ec.probe(ops)
    s = await bc.stop(ops)
    return f"batch {bid} in env {ec.env_of(r).get('key')}; stop said {s.get('status')!r}"


def env_switch_clean(t: Trace) -> Result:
    return ec.env_switch_clean(t, tag=TAG)


def batch_rows_in_env(t: Trace) -> Result:
    return ec.batch_rows_in_env(t, rows=N_ROWS)


EXTRA_INVARIANTS = [ec.env_switch_no_new_block, env_switch_clean, batch_rows_in_env, bc.stop_down]
WORLD_CHECKS = [bc.one_block_job]
EXPECT_OK = ["agent_engaged", "env_switch_no_new_block", "env_switch_clean", "batch_rows_in_env", "stop_down",
             "ends_with_stop", "stop_is_honest"]
