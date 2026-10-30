"""A RETURNING USER, scripted: one cell of the pairwise table (agentic/harness/returning_cells.tsv) — a user comes back
to a facility with leftover state from an earlier session, written by this plugin or the release before it, after the
world changed under it. The combinatorial follow-up to the Perlmutter report (plugin 0.1.20, 2026-10-29), whose
leftovers came stacked: a dead login-node pin, a proven record whose login became password + one-time code, a venv
path under /home, an endpoint left online and reused under a renamed id, hand-edits to ~/.hpc-bridge.

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_RETURNING_CELL=c07 HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc \
        ./agentic/run_smoke.sh returning_user

A two-phase chain (run.py `_phase_spec`), no model in it: session 1 runs the cell's `origin` plugin (this checkout or
the previous release built into the image, prev_release.py), brings up a working endpoint and ends as `ended` says; the
INTERPHASE world change applies the other factors (the endpoint stopped or killed, a login node closed, the key
revoked, files deleted, the QOS retired, the host keys changed, the token store gone); session 2 runs the current code
toward "hello world" on a compute node, doing the user's part when hpc-bridge asks for it. returning.py has the story,
the factors, the invariants every cell holds and what each factor level expects; the world check runs before the
harness' teardown and puts the world back for the next cell. HPCB_RETURNING_CELL picks the cell (default c01, nothing
changed); `python3 agentic/harness/returning.py --list` names them.
"""
from returning import (
    ACCOUNT,
    UV_CUTOFF_CLEAR,
    cell_from_env,
    expect_ok,
    graders,
    interphase,
    phases,
    world_check,
)

CELL = cell_from_env()
OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"submit_policy": "nersc", "login_nodes": 2, "mfa": "password_otp"}
NODE_ACTIONS = True            # run_smoke.sh serves the interphase's node actions (a login node closed, a QOS retired)
NEEDS_COMPUTE_NODE = True
KIND = "regression"
SUMMARY = f"a returning user, cell {CELL.describe()} (pairwise; the Perlmutter report, 2026-10-29)"
TAGS = ["fake", "nersc", "returning-user", "chain", "scripted", "perlmutter-report", CELL.id]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10        # short: a killed endpoint must still be listed online when session 2 starts

PROMPT = ("(Scripted — no agent reads this.) A returning user: session 1 leaves `{facility}` at `{ssh_host}` behind, "
          f"the world changes, session 2 drives the current code to hello world on account {ACCOUNT}.")
PHASES = phases(CELL)
INTERPHASE_SETUP = [interphase(CELL)]

# A user new to the facility before session 1: no hpc-bridge venv on the login node (each origin builds its own), the
# key accepted (a cell killed before its world check put it back), no QOS retired (ADMIN_*, through the admin channel)
SETUP = ['rm -rf "$HOME"/hpc-bridge/gce-venv*; '
         "if [ -e ~/.ssh/authorized_keys.hpcb-revoked ]; then mv ~/.ssh/authorized_keys.hpcb-revoked "
         "~/.ssh/authorized_keys; fi; [ -e ~/.ssh/authorized_keys ]", UV_CUTOFF_CLEAR]
CLEANUP = ["if [ -e ~/.ssh/authorized_keys.hpcb-revoked ]; then mv ~/.ssh/authorized_keys.hpcb-revoked "
           "~/.ssh/authorized_keys; fi; true", UV_CUTOFF_CLEAR]
ADMIN_SETUP = ["rm -f /etc/slurm/retired_qos.conf"]
ADMIN_CLEANUP = ["rm -f /etc/slurm/retired_qos.conf"]

EXTRA_INVARIANTS = graders(CELL)
EXPECT_OK = expect_ok(CELL)
WORLD_CHECKS = [world_check(CELL)]
