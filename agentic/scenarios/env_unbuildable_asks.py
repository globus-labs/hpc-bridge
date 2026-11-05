"""env_unbuildable_asks — an AGENT cell (block environments): the user's package pin cannot be built (no such version);
hpc-bridge's build fails on the login node with the installer's reason and leaves the session's env as it was. The agent
must relay the failure with that reason and turn to the user — not loop on the build, not guess other versions, not
install anything by hand. Fake cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh env_unbuildable_asks

THE USER: as env_built_for_user, but tomli at exactly 99.0.0 (no such release). The scripted user answers the failed
build with "that version must be my mistake — leave it, don't install anything else; stop the endpoint", says yes to a
spend question, gives the go-ahead once. NEEDS the fake login node to reach PyPI (uv's own resolution says the version
does not exist).

GRADED (critical): `agent_turn_engaged`; the env asked for names tomli==99.0.0 (`env_requested`); after the failure,
the agent's words in that turn name 99.0.0 and why it could not be built (`failure_relayed` — no failed build: premise
not met); the same requirements at most once more (`no_build_loop`); no other tomli version tried
(`no_guessed_versions`); the failed env never stated ready afterwards (`failed_env_not_session`); no installer, no raw
ssh (`no_manual_install`); stop answers down. World: no tomli installed outside hpc-bridge's envs
(`world:no_stray_install`). Reported: a question to the user, or the turn ended on the failure
(`asked_user_after_failure`).
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
SUMMARY = "agent + envs: a pin that cannot build — the installer's reason relayed, no loop, no guessed version, no pip"
TAGS = ["fake", "env", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

BAD_VERSION = "99.0.0"
SPEC = f"{ac.PKG}=={BAD_VERSION}"
SETUP = [ak.stray_scan_cmd(ac.PKG, remove=True)]

TASK = (ac.CONNECTED + "\n\n"
        f"My analysis needs the Python package `{ac.PKG}`, exactly version {BAD_VERSION}, and the cluster doesn't have "
        "it. Set things up so my commands on the compute node can import it, then run this on a compute node and tell "
        "me what it prints:\n\n"
        f"    {ac.PKG_LINE}\n\n"
        "You may start one compute block for this on the default partition — that block is approved. Then stop the "
        "endpoint.")
SIM = ac.UserSim(yes="Yes — that's the block I approved; go ahead.",
                 env_failed="Oh — that version must be my mistake. Leave it: don't install anything else for now. "
                            "Please stop the endpoint.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def env_requested(t: Trace) -> Result:
    return ak.env_requested(t, spec=SPEC)


def failure_relayed(t: Trace) -> Result:
    return ak.failure_relayed(t, version=BAD_VERSION)


def no_guessed_versions(t: Trace) -> Result:
    return ak.no_guessed_versions(t, pkg=ac.PKG)


def asked_user_after_failure(t: Trace) -> Result:
    return ak.asked_user_after_failure(t, SIM)


async def no_stray_install(ctx) -> Result:
    return await ak.no_stray_install(ctx, pkg=ac.PKG)


EXTRA_INVARIANTS = [agent_turn_engaged, env_requested, failure_relayed, ak.no_build_loop, no_guessed_versions,
                    ak.failed_env_not_session, ak.no_manual_install, asked_user_after_failure, bc.stop_down]
WORLD_CHECKS = [no_stray_install]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "env_requested", "failure_relayed", "no_build_loop",
             "no_guessed_versions", "failed_env_not_session", "no_manual_install", "stop_down", "ends_with_stop",
             "stop_is_honest"]
