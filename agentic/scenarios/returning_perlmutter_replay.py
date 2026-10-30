"""A RETURNING USER with an AGENT in session 2: the Perlmutter report replayed (plugin 0.1.20, 2026-10-29). Session 1
is scripted on the previous release (prev_release.py) and leaves an endpoint ONLINE; between the sessions the login
node it is pinned to closes to new logins (its processes run on) and the user's key stops being accepted (password +
one-time code only), the local files untouched — the stack of leftovers the reporter met. Session 2 is the cell's agent
(any operator; the free CLI ones too: the chain opens with a scripted phase, midrun_hooks.seeded_chain).

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh returning_perlmutter_replay

The agent must end with "hello world" on a compute node — the endpoint is still online, so a zero-SSH reattach needs
neither the dead node nor the key — or, where hpc-bridge needs SSH, relay the terminal handoff (never NO SSH ACCESS);
then the harness does the user's part (the key back, as a re-run sshproxy would issue one) and a follow-up turn
carries on. It never uses the dead pin twice, never hand-edits ~/.hpc-bridge or ~/.globus_compute, leaves one live
endpoint and today's file format (returning.py: the invariants every cell holds).
"""
from returning import agent_cell, expect_ok, graders, interphase, phases, world_check

CELL = agent_cell("perlmutter_replay")
TARGETS = ("fake",)
REQUIRES = {"submit_policy": "nersc", "login_nodes": 2, "mfa": "password_otp"}
NODE_ACTIONS = True
NEEDS_COMPUTE_NODE = True
KIND = "regression"
SUMMARY = f"a returning user, the Perlmutter report replayed with an agent in session 2 ({CELL.describe()})"
TAGS = ["fake", "nersc", "returning-user", "chain", "agent", "perlmutter-report"]
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
CLEANUP = ["if [ -e ~/.ssh/authorized_keys.hpcb-revoked ]; then mv ~/.ssh/authorized_keys.hpcb-revoked "
           "~/.ssh/authorized_keys; fi; true"]
ADMIN_SETUP = ["rm -f /etc/slurm/retired_qos.conf"]
ADMIN_CLEANUP = ["rm -f /etc/slurm/retired_qos.conf"]

EXTRA_INVARIANTS = graders(CELL)
EXPECT_OK = expect_ok(CELL)
WORLD_CHECKS = [world_check(CELL)]
