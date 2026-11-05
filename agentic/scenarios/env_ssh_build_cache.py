"""env_ssh_build_cache — a built env on an endpoint hpc-bridge stood up over SSH: built on the LOGIN node (free, no new
block), work runs in it, and the same env asked again is a cache hit (block environments, PR 3). Fake cluster, `default`
profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh env_ssh_build_cache

THE SCRIPT: connect, confirm one block; ensure_endpoint_up(env={requirements: six==1.17.0, root: a root of this run's
own}) until the env is ready; the probe on the compute block; the same env again in other words (trailing blanks, a
comment); the probe again; stop.

GRADED: the env was built on the login node, stated ready, nothing billed and no spend question after it
(`env_built_on_login`); the second ask was a cache hit — the same key and path, `cached` (`env_cache_hit`); a command
on the block ran in it: python's prefix is the env's path, six imports (`env_active_in_command`); one block confirmed;
stop answers down. World: the env is on the shared filesystem where the answer said, its marker naming its key and the
LOGIN node's architecture, built before the second ask (the cache hit rebuilt nothing), nothing of a build left beside
it (`world:env_on_login_disk`); one block job.

NEEDS the fake cluster's login node to reach PyPI (as the endpoint venv's own build does). ON THE BASE (bcc97a4):
ensure_endpoint_up takes no `env`: the cell aborts at its premise.
"""
from __future__ import annotations

import batch_checks as bc
import envs_checks as ec

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "block envs: a requirements env built on the login node (free, no new block), used on the block, then a cache hit"
TAGS = ["fake", "env", "ssh", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

SAME_ENV_OTHER_WORDS = "# the same env\nsix==1.17.0   \n\n"

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — confirm one block, build an env from `six==1.17.0`, run in it, ask for it again, stop.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    root = ec.root_for(ops)
    first = await ec.ask_env(ops, {"requirements": ec.REQUIREMENTS, "root": root})
    await ec.probe(ops)
    again = await ec.ask_env(ops, {"requirements": SAME_ENV_OTHER_WORDS, "root": root})
    await ec.probe(ops)
    s = await bc.stop(ops)
    return (f"env {ec.env_of(first).get('key')} {ec.env_of(first).get('state')!r} at {ec.env_of(first).get('path')!r}; "
            f"again cached={ec.env_of(again).get('cached')!r}; stop said {s.get('status')!r}")


EXTRA_INVARIANTS = [ec.env_built_on_login, ec.env_cache_hit, ec.env_active_in_command, bc.one_confirmation,
                    bc.stop_down]
WORLD_CHECKS = [ec.env_on_login_disk, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "env_built_on_login", "env_cache_hit", "env_active_in_command", "one_confirmation",
             "stop_down", "ends_with_stop", "stop_is_honest"]
