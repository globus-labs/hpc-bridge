"""batch_rows_in_user_env — an AGENT cell (block environments × batches): independent tasks that each import a package
the cluster lacks; the agent must run them as a batch whose rows run in an env hpc-bridge built from it — the session's
env or the batch's own. Fake cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_rows_in_user_env

THE USER: 4 checks (alpha … delta), each `<the tomli import line>; echo checked <config>`, needing tomli 2.0.1; one
block on the default partition approved. The scripted user gives a size when asked (1 CPU core, half a GB, 3 at once),
says yes to a spend question, answers a failed build with "don't install by hand, stop", gives the go-ahead once. NEEDS
the fake login node to reach PyPI. SETUP removes any tomli an earlier run left outside an hpc-bridge env.

GRADED (critical): `agent_turn_engaged`; an env argument naming tomli==2.0.1 (`env_requested`); the work went through
run_batch, not a run_shell per row (`batch_used`); no installer, no raw ssh (`no_manual_install`); stop answers down.
World: every check's stdout file in the run's batch directories shows `checked <config>` and tomli 2.0.1 imported from a
built env hpc-bridge stated ready (`world:rows_ran_in_env`); no tomli outside hpc-bridge's envs
(`world:no_stray_install`); one block job.
"""
from __future__ import annotations

import agent_cells as ac
import agent_checks as ak
import batch_checks as bc
from invariants import Result, Trace

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
OPERATORS = ("claude", "pi", "codex", "hermes21")
KIND = "regression"
SUMMARY = "agent + envs + batch: every row imports a package the cluster lacks — the rows run in a built env"
TAGS = ["fake", "env", "batch", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

CONFIGS = ["alpha", "bravo", "charlie", "delta"]
SPEC = f"{ac.PKG}=={ac.PKG_VERSION}"
SETUP = [ak.stray_scan_cmd(ac.PKG, remove=True)]

TASK = (ac.CONNECTED + "\n\n"
        "I have 4 independent checks to run on a compute node, one per config: alpha, bravo, charlie and delta. Each "
        f"needs the Python package `{ac.PKG}`, exactly version {ac.PKG_VERSION}, which the cluster doesn't have. For "
        "each config, run\n\n"
        f"    {ac.PKG_LINE}; echo checked <config>\n\n"
        "Each takes a few seconds. You may start one compute block for this on the default partition — that block is "
        "approved. When they are done, tell me what each printed, then stop the endpoint.")
SIM = ac.UserSim(sizes="Each check is tiny: 1 CPU core and half a GB of memory, and 3 can run at the same time.",
                 yes="Yes — that's the block I approved; go ahead.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def env_requested(t: Trace) -> Result:
    return ak.env_requested(t, spec=SPEC)


def batch_used(t: Trace) -> Result:
    return bc.batch_used(t, rows=len(CONFIGS))


async def rows_ran_in_env(ctx) -> Result:
    return await ak.rows_ran_in_env(ctx, pkg=ac.PKG, version=ac.PKG_VERSION, inputs=CONFIGS)


async def no_stray_install(ctx) -> Result:
    return await ak.no_stray_install(ctx, pkg=ac.PKG)


EXTRA_INVARIANTS = [agent_turn_engaged, env_requested, batch_used, ak.no_manual_install, bc.stop_down]
WORLD_CHECKS = [rows_ran_in_env, no_stray_install, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "env_requested", "batch_used", "no_manual_install", "stop_down",
             "ends_with_stop", "stop_is_honest"]
