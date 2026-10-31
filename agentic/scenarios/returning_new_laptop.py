"""A RETURNING USER with an AGENT in session 2, on a NEW LAPTOP: session 1 is scripted on the previous release
(prev_release.py) and leaves its endpoint running; between the sessions ~/.hpc-bridge is gone (no facilities.json, no
login-node pin). The agent must REATTACH to the endpoint still online — the same endpoint id, reused over the web —
not stand up a second one beside it, and run "hello world" on a compute node (returning.py: one live endpoint, checked
on every login node and in the cloud).

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh returning_new_laptop
"""
from returning import UV_CUTOFF_CLEAR, agent_cell, expect_ok, graders, interphase, phases, world_check

CELL = agent_cell("new_laptop")
TARGETS = ("fake",)
REQUIRES = {"submit_policy": "nersc", "login_nodes": 2, "mfa": "password_otp"}
NODE_ACTIONS = True
NEEDS_COMPUTE_NODE = True
KIND = "regression"
SUMMARY = f"a returning user on a new laptop, an agent in session 2: reattach, no second endpoint ({CELL.describe()})"
TAGS = ["fake", "nersc", "returning-user", "chain", "agent", "reuse", "new-laptop"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

PHASES = phases(CELL)
INTERPHASE_SETUP = [interphase(CELL)]
SETUP = ['rm -rf "$HOME"/hpc-bridge/gce-venv*; '
         "if [ -e ~/.ssh/authorized_keys.hpcb-revoked ]; then mv ~/.ssh/authorized_keys.hpcb-revoked "
         "~/.ssh/authorized_keys; fi; [ -e ~/.ssh/authorized_keys ]", UV_CUTOFF_CLEAR]
CLEANUP = [UV_CUTOFF_CLEAR]   # session 1's uv cutoff (returning.uv_cutoff_cmd), should the cell end inside it
ADMIN_SETUP = ["rm -f /etc/slurm/retired_qos.conf"]
ADMIN_CLEANUP = ["rm -f /etc/slurm/retired_qos.conf"]

EXTRA_INVARIANTS = graders(CELL)
EXPECT_OK = expect_ok(CELL)
WORLD_CHECKS = [world_check(CELL)]
