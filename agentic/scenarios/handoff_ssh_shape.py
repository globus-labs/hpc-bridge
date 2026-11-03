"""handoff_ssh_shape — the loam handoff on an endpoint hpc-bridge stood up over SSH (batch phase 2b, design §8): the
same contract as on a facility endpoint — terms first, one canary with the exact config, a v1 record — on hpc-bridge's
own template, where the handoff gets a user endpoint of its own (a block-request number in its config), min_blocks is
fixed at 0 by the template, and the release names `scancel` and what hpc-bridge's own stop and teardown do to it. Fake
cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh handoff_ssh_shape

THE SCRIPT: connect (the probe's proposal, the login shape up); export_handoff({walltime 5 min, max_blocks 1}) — the
session's partition — it asks; the user's yes; the same call until the record is exported. No stop: the release says
hpc-bridge's stop_endpoint would cancel this pool's blocks too.

GRADED: `handoff_asked_first`, `handoff_one_canary`, `handoff_record_shape` (access ssh, a Slurm job id, the user
endpoint seen), `handoff_release_honest` (`scancel <job>` after "stop submitting"), and `ssh_record_specifics` (its own
`hpcb_block`, no `compute` marker, the idle windows of hpc-bridge's template, the stop/teardown scope). World: the
canary's job ran on the partition the record names (`world:canary_job_as_recorded`); after the template's idle window
nothing of the pool user is left in the queue (postcheck).

ON MAIN `export_handoff` is not a tool: every grader fails, naming the unknown tool.
"""
from __future__ import annotations

import batch_checks as bc
import handoff_checks as hc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = ("batch phase 2b: export_handoff on hpc-bridge's own SSH endpoint — its own user endpoint, min_blocks fixed "
           "0, a release that names scancel and what hpc-bridge's stop and teardown do to it")
TAGS = ["fake", "handoff", "loam", "spend", "scripted", "ssh"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

UEC = {"walltime": "00:05:00", "max_blocks": 1}

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id "
          "`{facility}` — export a loam handoff for a one-block pool: read its terms, say yes, collect the record.")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    out = await hc.export(ops, UEC)
    rec = out.get("record") or {}
    return (f"export_handoff: {out.get('phase')!r} {out.get('reason') or ''} — job "
            f"{(rec.get('validation') or {}).get('job_id')!r}, path {out.get('path')!r}")


def record_shape(t: Trace) -> Result:
    return hc.record_shape(t, uec=UEC, scheduler="slurm", access="ssh")


def release_honest(t: Trace) -> Result:
    return hc.release_honest(t, cancel="scancel")


def ssh_record_specifics(t: Trace) -> Result:
    """On hpc-bridge's own endpoint: the record's config carries a block-request number of its own (`hpcb_block`, a
    positive whole number — its own user endpoint) and no `compute` marker; the release states the template's idle
    windows (block and user endpoint) and that hpc-bridge's stop_endpoint and teardown_endpoint reach this pool."""
    rec = hc.record_of(t)
    if rec is None:
        return Result("ssh_record_specifics", False, hc._main(t) or "no export_handoff answer exported a record")
    wire, rel = rec.get("user_endpoint_config") or {}, rec.get("release") or {}
    why = []
    blk = wire.get("hpcb_block")
    if not (isinstance(blk, int) and not isinstance(blk, bool) and blk > 0) or "compute" in wire:
        why.append(f"hpcb_block={blk!r}, compute in config: {'compute' in wire}")
    if rec.get("endpoint", {}).get("kind") != "hpc_bridge_ssh":
        why.append(f"endpoint kind {rec.get('endpoint', {}).get('kind')!r}")
    for k in ("block_idle_release_s", "uep_idle_exit_s"):
        if not (isinstance(rel.get(k), int) and rel[k] > 0):
            why.append(f"release.{k}={rel.get(k)!r}")
    scope = str(rel.get("hpc_bridge_scope") or "")
    if "stop_endpoint" not in scope or "teardown_endpoint" not in scope:
        why.append(f"hpc_bridge_scope does not name stop_endpoint and teardown_endpoint: {scope[:120]!r}")
    return Result("ssh_record_specifics", not why, f"ok: hpcb_block {blk}, idle {rel.get('block_idle_release_s')} s / "
                  f"{rel.get('uep_idle_exit_s')} s" if not why else "; ".join(why))


async def canary_job_as_recorded(ctx) -> Result:
    rec = hc.record_of(ctx.trace) or {}
    part = (rec.get("user_endpoint_config") or {}).get("partition")
    return await hc.canary_job_as_recorded(ctx, scheduler="slurm", queue=part or "?")


EXTRA_INVARIANTS = [hc.asked_first, hc.one_canary, record_shape, release_honest, ssh_record_specifics]
WORLD_CHECKS = [canary_job_as_recorded]
EXPECT_OK = ["agent_engaged", "handoff_asked_first", "handoff_one_canary", "handoff_record_shape",
             "handoff_release_honest", "ssh_record_specifics"]
# hpc-bridge's template idle-releases a block after max_idletime (600 s) and its compute user endpoint exits after its
# idle heartbeats (20 × 30 s): past both, nothing of the pool user is left queued or running.
POSTCHECK_DELAY_S = 720
POSTCHECKS = [{"name": "handoff_block_idle_released",
               "cmd": "command -v squeue >/dev/null || { echo NO-SQUEUE; exit 3; }; squeue -u \"$USER\" -h -o '%j %T' 2>&1; true",
               "expect_absent": "parsl"}]
