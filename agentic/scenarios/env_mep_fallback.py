"""env_mep_fallback — a built env on a facility endpoint whose template does NOT build `requirements` (Polaris's kind):
the block's own worker_init builds it on the shared filesystem (under flock where the node has it, never changing the
worker's env), and each command activates it; the first build is billed, and the spend question says so (block
environments, PR 3). Fake cluster, `pbs_mep` profile — its first manager, `hpcb-mep-pbs` (Polaris-shaped: a user
worker_init PREPENDED to the facility's own), our identity mapped to `hpcbmep`; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=pbs_mep ./agentic/run_smoke.sh env_mep_fallback

THE SCRIPT: attach (zero SSH); confirm one block on `debug` charged to `hpcb`; ask for the env (six==1.17.0) — the answer
is a spend question; the user's yes; wait for the new block (its worker_init builds the env first); the probe; stop
(final `draining` on a MEP).

GRADED: the env's spend question states `block.env` with how `worker_init`, a new block, and the words "first build" and
"billed" (`env_terms`); the yes carries the env as a receipt (`env_receipt`); from inside the block the command ran in
the env worker_init built — `<key>-<arch>` under the env root, VIRTUAL_ENV its own, six imports — while the worker
kept the facility's (`env_built_by_worker_init`); the stop is honest.

NEEDS the fake PBS compute nodes to reach PyPI (the build runs in the block; its probe of the index comes first). ON THE
BASE (bcc97a4): ensure_endpoint_up takes no `env`: the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import envs_checks as ec
from invariants import Result, Trace

FACILITY_ID = "fake-pbs-mep"          # the pbs_mep profile's local catalog entry (hpcb-pbs-mep-catalog)
MAPPED_USER = "hpcbmep"
OPERATORS = ("script",)
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # the fake-MEP cells' group: one mapped account, one queue
TARGETS = ("fake",)
REQUIRES = {"scheduler": "pbs", "mep": "consent-free", "site_rule": "filesystems"}
KIND = "regression"
SUMMARY = "block envs on a facility endpoint that cannot build requirements: built by the block's worker_init, first build billed"
TAGS = ["fake", "mep", "pbs", "env", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
ADMIN_CLEANUP = [f"ids=$(qselect -u {MAPPED_USER} 2>/dev/null); [ -z \"$ids\" ] || qdel $ids 2>/dev/null; true"]

PROMPT = ("(Scripted — no agent reads this.) Attach to the facility `{facility}` (a Polaris-shaped multi-user endpoint), "
          "confirm one block charged to `hpcb`, ask for an env from six, say yes, run in it, stop.")


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
    return f"env how={ec.env_of(yes).get('how')!r} key={ec.env_of(yes).get('key')!r}; the stop said {s.get('status')!r}"


def env_terms(t: Trace) -> Result:
    return ec.env_terms(t, how="worker_init", words=("first build", "billed"))


def env_receipt(t: Trace) -> Result:
    return ec.env_receipt(t, how="worker_init")


EXTRA_INVARIANTS = [env_terms, env_receipt, ec.env_built_by_worker_init]
EXPECT_OK = ["agent_engaged", "env_terms", "env_receipt", "env_built_by_worker_init", "stop_is_honest"]
