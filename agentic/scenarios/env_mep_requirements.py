"""env_mep_requirements — a built env on a facility endpoint whose template builds `requirements` itself (Delta's and
Anvil's kind): passed through, so it is the WORKERS' env; a new env is a new user endpoint and a new block, and the
spend question says so (block environments, PR 3). Fake cluster, `pbs_mep` profile — its second manager,
`hpcb-mep-pbs-req` (the Polaris-shaped template that also builds `requirements` when a block starts), our identity mapped
to `hpcbmep`; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=pbs_mep ./agentic/run_smoke.sh env_mep_requirements

THE SCRIPT: attach (zero SSH); confirm one block on `debug` charged to `hpcb`; ask for the env (six==1.17.0) — the
answer is a spend question; the user's yes; wait for the new block; the probe; stop (final `draining` on a MEP).

GRADED: the env's spend question states `block.env` with how `facility`, a new block, and the words "new user
endpoint", "new block" and "billed" (`env_terms`); the yes carries the env as a receipt with the block it started
(`env_receipt`); from inside the block python is the venv the FACILITY built under the mapped user's
~/.globus_compute/venvs/, and six imports (`worker_runs_facility_env`); the stop is honest.

NEEDS the fake PBS compute nodes to reach PyPI (the template's build runs there, in the block). ON THE BASE (bcc97a4):
ensure_endpoint_up takes no `env`: the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import envs_checks as ec
from invariants import Result, Trace

FACILITY_ID = "fake-pbs-mep-req"      # the pbs_mep profile's second local catalog entry (hpcb-pbs-mep-catalog)
MAPPED_USER = "hpcbmep"
OPERATORS = ("script",)
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # the fake-MEP cells' group: one mapped account, one queue
TARGETS = ("fake",)
REQUIRES = {"scheduler": "pbs", "mep": "consent-free", "site_rule": "filesystems", "mep_schemas": ["pbs-req"]}
KIND = "regression"
SUMMARY = "block envs on a facility endpoint that builds requirements: passed through, a new block, the terms say so"
TAGS = ["fake", "mep", "pbs", "env", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
ADMIN_CLEANUP = [f"ids=$(qselect -u {MAPPED_USER} 2>/dev/null); [ -z \"$ids\" ] || qdel $ids 2>/dev/null; true"]

PROMPT = ("(Scripted — no agent reads this.) Attach to the facility `{facility}` (a multi-user endpoint whose template "
          "builds requirements), confirm one block charged to `hpcb`, ask for an env from six, say yes, run in it, stop.")


async def SCRIPT(ops) -> str:
    r = await ops.call("connect_facility", facility=ops.facility)
    if r.get("phase") in ("failed", "unsupported") or r.get("is_error"):
        ops.abort(f"the MEP attach failed: {r.get('phase')!r} {r.get('notice') or r.get('text')!r}")
    await bc.block_up(ops, partition="debug", account="hpcb")
    env = {"requirements": ec.REQUIREMENTS}
    ask = await ec.ask_env(ops, env)
    if ask.get("status") != "needs_confirmation":
        ops.abort(f"a new env on this facility must ask (a new block): {bc._brief(ask)!r}")
    yes = await ec.ask_env(ops, env, confirm_spend=True)
    await ec.wait_up(ops)
    await ec.probe(ops)
    s = await bc.stop(ops, final_draining=True)
    return f"env how={ec.env_of(yes).get('how')!r}; the stop said {s.get('status')!r}"


def env_terms(t: Trace) -> Result:
    return ec.env_terms(t, how="facility", words=("new user endpoint", "new block", "billed"))


def env_receipt(t: Trace) -> Result:
    return ec.env_receipt(t, how="facility")


EXTRA_INVARIANTS = [env_terms, env_receipt, ec.worker_runs_facility_env]
EXPECT_OK = ["agent_engaged", "env_terms", "env_receipt", "worker_runs_facility_env", "stop_is_honest"]
