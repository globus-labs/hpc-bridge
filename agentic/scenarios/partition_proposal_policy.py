"""The probe's proposed partition must be one the site's scheduler ACCEPTS — the Perlmutter report, item 7a (plugin
0.1.20, 2026-10-29). Fake cluster, `nersc` profile; scripted (no model). A RED proof: it fails on main, by design, until
the fix.

On Perlmutter the probe proposed `partition: system` — sinfo's starred default — and every block the user submitted
on it was refused at submit: "sbatch: error: Job request does not match any supported policy." NERSC routes jobs by
QOS and node type: a job names `-C cpu` or `-C gpu` and EITHER a QOS or a "partition" name (debug, regular, shared),
and the site's submit filter picks the real partition (regular_milan_ss11, …). The plugin's picker matches exact
partition names (debug, regular, …), misses the suffixed ones, and falls back to the starred default, which no job may
name. The `nersc` profile carries that partition list and that submit filter (job_submit.lua).

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh partition_proposal_policy

The SCRIPT connects once (the probe's proposal; nothing is confirmed, nothing started). The world check
`world:proposed_terms_accepted` writes the proposal's block terms as a batch script — `#SBATCH --partition=<proposed>`,
a short walltime, and the proposal's own scheduler_options lines — and asks the scheduler, as the pool user, whether it
would take it (`sbatch --test-only`: the submit filter runs, nothing is queued). Main proposes `system`, which the
filter refuses. `proposed_partition` (reported) names what was proposed.
"""
from __future__ import annotations

import re
import shlex

from byo_script import PROPOSED, brief
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"submit_policy": "nersc"}
KIND = "regression"
SUMMARY = ("the probe's proposed partition and terms pass the site's submit filter (sbatch --test-only) — "
           "not the starred `system` that NERSC's policy refuses (Perlmutter report 7a)")
TAGS = ["fake", "nersc", "discovery", "partition", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False     # nothing runs: --test-only queues nothing
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` at login host `{ssh_host}` and read the "
          "probe's proposal; the world check asks the scheduler whether it would accept a block on those terms.")

TEST_WALLTIME = "00:10:00"
_ACCEPTED = re.compile(r"sbatch: Job \d+ to start at \S+ using \d+ processors on nodes \S+ in partition (\S+)")


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    r = await ops.call("connect_facility", facility=ops.facility, ssh_host=ops.ssh_host)
    if r.get("phase") != PROPOSED:
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {brief(r)!r}")
    d = r.get("proposed_details") or {}
    ops.say(f"the probe proposed partition {d.get('partition')!r}, scheduler_options {d.get('scheduler_options')!r}")
    return f"Proposed partition {d.get('partition')!r} (scheduler_options {d.get('scheduler_options')!r}); not confirmed."


def _proposal(t: Trace) -> dict | None:
    for _i, c in t.named("connect_facility"):
        if (c.result or {}).get("phase") == PROPOSED:
            return dict((c.result or {}).get("proposed_details") or {})
    return None


def proposed_partition(t: Trace) -> Result:
    """Reported: what the probe proposed — the partition (and any scheduler_options) a confirmed block would carry."""
    d = _proposal(t)
    if d is None:
        return Result("proposed_partition", False, "premise not met: no proposal in the trace")
    return Result("proposed_partition", True,
                  f"partition {d.get('partition')!r}, scheduler_options {d.get('scheduler_options')!r}")


def test_only_cmd(partition: str, scheduler_options: str | None) -> str:
    """The proposal's terms as a batch script, offered to `sbatch --test-only` (the submit filter runs; nothing is
    queued). Its words come back prefixed `HPCB_SB `, then `HPCB_RC=<sbatch's rc>` — the login's startup noise is
    neither."""
    lines = ["#!/bin/bash", f"#SBATCH --partition={partition}" if partition else "", f"#SBATCH --time={TEST_WALLTIME}",
             "#SBATCH --nodes=1", *(scheduler_options or "").splitlines(), "true"]
    script = "\n".join(ln for ln in lines if ln) + "\n"
    return (f"d=$(mktemp -d) && printf %s {shlex.quote(script)} > \"$d/terms.sh\" && "
            "sbatch --test-only \"$d/terms.sh\" > \"$d/out\" 2>&1; rc=$?; sed 's/^/HPCB_SB /' \"$d/out\"; "
            "echo \"HPCB_RC=$rc\"; rm -rf \"$d\"")


def read_test_only(out: str) -> tuple[bool | None, str]:
    """(accepted?, sbatch's own words). None when the command never reported its rc (an ssh failure)."""
    words = " | ".join(ln[len("HPCB_SB "):].strip() for ln in out.splitlines() if ln.startswith("HPCB_SB "))
    m = re.search(r"^HPCB_RC=(\d+)", out, re.M)
    if m is None:
        return None, out.strip()[-300:]
    return (m.group(1) == "0" and _ACCEPTED.search(words) is not None), words


async def proposed_terms_accepted(ctx) -> Result:
    """world: the scheduler would take a block on the proposal's terms (sbatch --test-only, as the pool user)."""
    name = "world:proposed_terms_accepted"
    d = _proposal(ctx.trace)
    if d is None:
        return Result(name, False, "premise not met: no proposal in the trace")
    part, opts = str(d.get("partition") or ""), d.get("scheduler_options")
    rc, out = ctx.ssh(test_only_cmd(part, opts), 60)
    ok, words = read_test_only(out)
    terms = f"partition {part!r}" + (f" + {opts!r}" if opts else "")
    if ok is None:
        return Result(name, False, f"UNVERIFIABLE: sbatch --test-only did not report (ssh rc={rc}): {words!r}")
    return Result(name, ok, (f"ok: the scheduler would take {terms}: {words[:200]}" if ok else
                             f"the scheduler refuses the proposed {terms}: {words[:300]}"))


WORLD_CHECKS = [proposed_terms_accepted]
EXTRA_INVARIANTS = [proposed_partition]
EXPECT_OK = ["agent_engaged"]
