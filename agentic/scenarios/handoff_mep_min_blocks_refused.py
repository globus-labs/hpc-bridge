"""handoff_mep_min_blocks_refused — D6 (design §12, taken (a)): a loam handoff with `min_blocks >= 1` on a facility
multi-user endpoint is refused until spike S3 proves the user endpoint exits when idle. Parsl replaces a cancelled block
to keep its minimum, so `qdel` ends nothing, and no client can stop a facility's user endpoint: terms the user cannot
bound are not consent to a number. The refusal comes before any terms, and a yes does not lift it. Fake cluster,
`pbs_mep` profile (whose schema allows `min_blocks` up to 1, as Polaris lets users set it); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=pbs_mep ./agentic/run_smoke.sh handoff_mep_min_blocks_refused

THE SCRIPT: attach (zero SSH); export_handoff(loam's Polaris-shaped config with min_blocks 1) — then the same call with
confirm_spend=True, as a user's yes would come.

GRADED: both answers are refused, reason mep_min_blocks, with no terms, no record and no canary, naming min_blocks
(`handoff_d6_refused`). World: no job of the mapped account was created after the run began
(`world:no_job_since_start`). Free: nothing is submitted, so it needs no compute node.

ON MAIN `export_handoff` is not a tool: the grader fails, naming the unknown tool.
"""
from __future__ import annotations

import handoff_checks as hc
from invariants import Result

FACILITY_ID = "fake-pbs-mep"
MAPPED_USER = "hpcbmep"
OPERATORS = ("script",)
NEEDS_COMPUTE_NODE = False
SERIAL = "hpcbmep"
TARGETS = ("fake",)
REQUIRES = {"scheduler": "pbs", "mep": "consent-free", "site_rule": "filesystems"}
KIND = "regression"
SUMMARY = "batch phase 2b, D6: a MEP handoff with min_blocks >= 1 is refused before any terms; a yes does not lift it"
TAGS = ["fake", "mep", "pbs", "polaris", "handoff", "loam", "spend", "scripted", "d6"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
ADMIN_CLEANUP = [f"ids=$(qselect -u {MAPPED_USER} 2>/dev/null); [ -z \"$ids\" ] || qdel $ids 2>/dev/null; true"]

UEC = {"queue": "debug", "account": "hpcb", "walltime": "00:15:00", "nodes_per_block": 1, "init_blocks": 0,
       "min_blocks": 1, "max_blocks": 1, "max_workers_per_node": 2,
       "scheduler_options": "#PBS -l filesystems=home"}

PROMPT = ("(Scripted — no agent reads this.) Attach to the facility `{facility}` and try to export a loam handoff "
          "whose pool keeps one block warm (min_blocks 1): it must be refused, before and after a yes.")


async def SCRIPT(ops) -> str:
    r = await ops.call("connect_facility", facility=ops.facility)
    if r.get("phase") in ("failed", "unsupported") or r.get("is_error"):
        ops.abort(f"the MEP attach failed: {r.get('phase')!r} {r.get('notice') or r.get('text')!r}")
    ask = await ops.call(hc.TOOL, user_endpoint_config=UEC)
    yes = await ops.call(hc.TOOL, user_endpoint_config=UEC, confirm_spend=True)
    return f"ask: {ask.get('phase')!r}/{ask.get('reason')!r}; yes: {yes.get('phase')!r}/{yes.get('reason')!r}"


async def no_job_since_start(ctx) -> Result:
    return await hc.no_job_since_start(ctx, user=MAPPED_USER)


EXTRA_INVARIANTS = [hc.d6_refused]
WORLD_CHECKS = [no_job_since_start]
EXPECT_OK = ["agent_engaged", "handoff_d6_refused"]
