"""A RETURNING USER with an AGENT in session 2, nothing changed: session 1 is scripted on the previous release
(prev_release.py) and leaves its endpoint running; nothing happens between the sessions. The agent must REUSE it — the
same endpoint, reattached over the web (reused=True), no probe, no venv rebuilt — and run "hello world" on a compute
node. The control for the other returning-user cells (returning.py), and the upgrade case every user meets: an
endpoint an older plugin stood up, driven by this one.

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh returning_all_well
"""
from returning import agent_cell, expect_ok, graders, interphase, phases, world_check

CELL = agent_cell("all_well")
TARGETS = ("fake",)
REQUIRES = {"submit_policy": "nersc", "login_nodes": 2, "mfa": "password_otp"}
NODE_ACTIONS = True
NEEDS_COMPUTE_NODE = True
KIND = "regression"
SUMMARY = f"a returning user with an agent in session 2, nothing changed: reuse, rebuild nothing ({CELL.describe()})"
TAGS = ["fake", "nersc", "returning-user", "chain", "agent", "reuse"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

PHASES = phases(CELL)
INTERPHASE_SETUP = [interphase(CELL)]
SETUP = ['rm -rf "$HOME"/hpc-bridge/gce-venv*; '
         "if [ -e ~/.ssh/authorized_keys.hpcb-revoked ]; then mv ~/.ssh/authorized_keys.hpcb-revoked "
         "~/.ssh/authorized_keys; fi; [ -e ~/.ssh/authorized_keys ]"]
ADMIN_SETUP = ["rm -f /etc/slurm/retired_qos.conf"]
ADMIN_CLEANUP = ["rm -f /etc/slurm/retired_qos.conf"]

EXTRA_INVARIANTS = graders(CELL)
EXPECT_OK = expect_ok(CELL)
WORLD_CHECKS = [world_check(CELL)]
