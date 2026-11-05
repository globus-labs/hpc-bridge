"""env_module_switch — an AGENT cell (block environments): the user names a module the cluster provides; the agent must
point the session at that EXISTING env (an `activate` line), and the switch on a live block must start no new block
and ask no new spend question. Fake cluster, `lmod` profile (its `python/3.11` module puts /opt/python/3.11/bin first
on PATH; the endpoint's own env_setup loads only uv/0.12.9); any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=lmod ./agentic/run_smoke.sh env_module_switch

THE USER: one block on the default partition, charged to `hpcb`, approved; run `module list 2>&1; command -v python3`
there; then switch all later commands to the cluster's python/3.11 module and run it again; stop. The scripted user
says yes to a spend question, names the account if asked, gives the go-ahead once.

GRADED (critical): `agent_turn_engaged`; an env argument whose activate line loads python/3.11 (`activate_env_used`);
a command on the compute node after the switch ran in it — `/opt/python/3.11/bin/python3` in its output
(`module_active_after_switch`; the baseline before the switch is reported in its detail); the switch, made after the
block was confirmed, answered `up`, nothing after it asked for spend, and the run confirmed one block
(`switch_no_new_block` — a switch made with the first confirmation, before the baseline, is a premise not met); stop
answers down. World: one block job.
"""
from __future__ import annotations

import agent_cells as ac
import agent_checks as ak
import batch_checks as bc
from invariants import Result, Trace

TARGETS = ("fake",)
REQUIRES = {"module_system": "lmod", "modules": ["python/3.11"], "accounts": ["hpcb"]}
OPERATORS = ("claude", "pi", "codex", "hermes21")
KIND = "regression"
SUMMARY = "agent + envs: the user's module as the session's existing env, switched on a live block — no new block, no ask"
TAGS = ["fake", "lmod", "env", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

MODULE = "python/3.11"
MARKER = "/opt/python/3.11/bin/python3"
PROBE = "module list 2>&1; command -v python3"

TASK = (ac.CONNECTED + "\n\n"
        "Start one compute block on the default partition, charged to my allocation `hpcb` — that block is approved. "
        f"On it, run `{PROBE}` and show me the output. Then switch all my commands on that block, from now on, to the "
        f"cluster's `{MODULE}` module, run the same command again and show me its output. Then stop the endpoint.")
SIM = ac.UserSim(yes="Yes — that's the block I approved; go ahead.", account="Charge my allocation `hpcb`.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def activate_env_used(t: Trace) -> Result:
    return ak.activate_env_used(t, module=MODULE)


def module_active_after_switch(t: Trace) -> Result:
    return ak.module_active_after_switch(t, module=MODULE, marker=MARKER)


def switch_no_new_block(t: Trace) -> Result:
    return ak.switch_no_new_block(t, module=MODULE)


EXTRA_INVARIANTS = [agent_turn_engaged, activate_env_used, module_active_after_switch, switch_no_new_block,
                    bc.stop_down]
WORLD_CHECKS = [bc.one_block_job]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "activate_env_used", "module_active_after_switch",
             "switch_no_new_block", "stop_down", "ends_with_stop", "stop_is_honest"]
