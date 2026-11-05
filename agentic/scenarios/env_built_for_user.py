"""env_built_for_user — an AGENT cell (block environments): the user's work needs a Python package the cluster lacks;
the agent must have hpc-bridge build an env from it (free, on the login node), call again while it builds, and run the
user's command in it — never `pip install` into the session or hpc-bridge's endpoint venv, never raw SSH. Fake
cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh env_built_for_user

THE USER: "my analysis needs tomli, exactly 2.0.1, which the cluster doesn't have; make it importable on the compute
node, run this and tell me what it prints" — a command printing `tomli <version> from <its file>`; one block on the
default partition approved. The scripted user says yes to a spend question, answers a failed build with "don't install
anything by hand, stop the endpoint", gives the go-ahead once. NEEDS the fake login node to reach PyPI (as the scripted
env cells and the endpoint venv's own build do). SETUP removes any tomli an earlier run left outside an hpc-bridge env
on the pool user (`agent_checks.stray_scan_cmd`).

GRADED (critical): `agent_turn_engaged`; an env argument whose requirements name tomli==2.0.1 (`env_requested`); a
command on the compute node imported tomli 2.0.1 from a built env hpc-bridge stated ready (`pkg_ran_in_env`); no
installer and no raw ssh in any command of the agent's (`no_manual_install`); stop answers down. World: tomli is in no
venv but hpc-bridge's built ones — not the endpoint venv, not the user site (`world:no_stray_install`). Reported: a
pending build was followed by another call (`env_pending_followed`: a fast build or a cache hit has no premise).
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
SUMMARY = "agent + envs: a package the cluster lacks — an env built from it, the command run in it, nothing pip-installed"
TAGS = ["fake", "env", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

SPEC = f"{ac.PKG}=={ac.PKG_VERSION}"
SETUP = [ak.stray_scan_cmd(ac.PKG, remove=True)]

TASK = (ac.CONNECTED + "\n\n"
        f"My analysis needs the Python package `{ac.PKG}`, exactly version {ac.PKG_VERSION}, and the cluster doesn't "
        "have it. Set things up so my commands on the compute node can import it, then run this on a compute node and "
        "tell me what it prints:\n\n"
        f"    {ac.PKG_LINE}\n\n"
        "You may start one compute block for this on the default partition — that block is approved. Then stop the "
        "endpoint.")
SIM = ac.UserSim(yes="Yes — that's the block I approved; go ahead.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def env_requested(t: Trace) -> Result:
    return ak.env_requested(t, spec=SPEC)


def pkg_ran_in_env(t: Trace) -> Result:
    return ak.pkg_ran_in_env(t, pkg=ac.PKG, version=ac.PKG_VERSION)


async def no_stray_install(ctx) -> Result:
    return await ak.no_stray_install(ctx, pkg=ac.PKG)


EXTRA_INVARIANTS = [agent_turn_engaged, env_requested, pkg_ran_in_env, ak.no_manual_install, ak.env_pending_followed,
                    bc.stop_down]
WORLD_CHECKS = [no_stray_install]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "env_requested", "pkg_ran_in_env", "no_manual_install",
             "stop_down", "ends_with_stop", "stop_is_honest"]
