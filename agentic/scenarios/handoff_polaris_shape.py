"""handoff_polaris_shape — the loam handoff on a Polaris-shaped facility endpoint (batch phase 2b, design §8 and §10
"Phase 2"): `export_handoff` states its terms first, sends ONE billed canary with loam's exact user endpoint config
once the user says yes, and exports a v1 record that is wire-ready (the template's own key names: `queue`, not
`partition`), says `enforced_by: uec-bounds-only`, and names the end that really works. Fake cluster, `pbs_mep` profile
(PBS, a strict Polaris-shaped template that pins one GPU per worker, 240 s idle release, our identity mapped to
`hpcbmep`); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=pbs_mep ./agentic/run_smoke.sh handoff_polaris_shape

THE SCRIPT: attach (zero SSH); export_handoff(loam's config: queue debug, account hpcb, 15 min, 1 node, min_blocks 0,
max_blocks 2, 2 workers pinned to 2 accelerators, `#PBS -l filesystems=home`) — it asks; the user's yes; the same call
until the record is exported. No stop: once loam holds the record hpc-bridge does not police the endpoint.

GRADED: the first answer asked, in node-hours, `uec-bounds-only`, and nothing was sent before the yes
(`handoff_asked_first`); one canary (`handoff_one_canary`); the record's shape (`handoff_record_shape`: the config as
sent, every key rendered, the canary's PBS job, `hpcbmep`, a CUDA_VISIBLE_DEVICES pin, the user endpoint seen); the
release (`handoff_release_honest`: stop submitting, then idle release, or `qdel <job>` because min_blocks is 0).
World: the canary's job ran in queue debug, charged to hpcb, for 15 min (`world:canary_job_as_recorded`); after the
idle window nothing of the mapped account is left in the queue — the release's "ends by itself" held (postcheck).

ON MAIN `export_handoff` is not a tool: every grader fails, naming the unknown tool.
"""
from __future__ import annotations

import handoff_checks as hc
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
SUMMARY = ("batch phase 2b: export_handoff on a Polaris-shaped PBS facility endpoint — terms first, one canary with "
           "loam's exact config, a wire-ready v1 record (uec-bounds-only) whose release ends the pool honestly")
TAGS = ["fake", "mep", "pbs", "polaris", "handoff", "loam", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
ADMIN_CLEANUP = [f"ids=$(qselect -u {MAPPED_USER} 2>/dev/null); [ -z \"$ids\" ] || qdel $ids 2>/dev/null; true"]

# loam's Polaris-shaped config, in the template's own key names (as loam's HPC_ENDPOINT_CONFIG_JSON is)
LOAM_UEC = {"queue": "debug", "account": "hpcb", "walltime": "00:15:00", "nodes_per_block": 1, "init_blocks": 0,
            "min_blocks": 0, "max_blocks": 2, "max_workers_per_node": 2, "available_accelerators": 2,
            "scheduler_options": "#PBS -l filesystems=home"}

PROMPT = ("(Scripted — no agent reads this.) Attach to the facility `{facility}` (a Polaris-shaped multi-user "
          "endpoint) and export a loam handoff for a Polaris-shaped pool: read its terms, say yes, collect the record.")


async def SCRIPT(ops) -> str:
    r = await ops.call("connect_facility", facility=ops.facility)
    if r.get("phase") in ("failed", "unsupported") or r.get("is_error"):
        ops.abort(f"the MEP attach failed: {r.get('phase')!r} {r.get('notice') or r.get('text')!r}")
    out = await hc.export(ops, LOAM_UEC)
    rec = out.get("record") or {}
    return (f"export_handoff: {out.get('phase')!r} {out.get('reason') or ''} — job "
            f"{(rec.get('validation') or {}).get('job_id')!r}, path {out.get('path')!r}")


def record_shape(t: Trace) -> Result:
    return hc.record_shape(t, uec=LOAM_UEC, scheduler="pbs", access="mep", identity=MAPPED_USER, pinned=True)


def release_honest(t: Trace) -> Result:
    return hc.release_honest(t, cancel="qdel")


async def canary_job_as_recorded(ctx) -> Result:
    return await hc.canary_job_as_recorded(ctx, scheduler="pbs", queue="debug", account="hpcb", walltime="00:15:00")


EXTRA_INVARIANTS = [hc.asked_first, hc.one_canary, record_shape, release_honest]
WORLD_CHECKS = [canary_job_as_recorded]
EXPECT_OK = ["agent_engaged", "handoff_asked_first", "handoff_one_canary", "handoff_record_shape",
             "handoff_release_honest"]
# After the template's 240 s idle release (plus the canary's own run and a margin), nothing of the mapped account is
# left in the queue: min_blocks 0, nothing submitted — the pool ended by itself, as the record's release says.
POSTCHECK_DELAY_S = 330
POSTCHECKS = [{"name": "handoff_block_idle_released",
               "cmd": f"command -v qstat >/dev/null || {{ echo NO-QSTAT; exit 3; }}; qstat -u {MAPPED_USER} -w 2>&1; true",
               "expect_absent": "parsl"}]
