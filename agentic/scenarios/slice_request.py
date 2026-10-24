"""A block that asks for a SLICE of a node gets exactly that slice from the scheduler — resource requests (2026-10-24;
vault `Planned/Resource requests.md`). Fake cluster, `slices` profile; scripted (no model).

The live case: on Anvil a bring-your-own block on the per-core `shared` partition asked parsl's default exclusive node,
queued for more than a day and would have billed all 128 cores. Here the `slices` profile puts a per-core partition
(`shared`: select/cons_tres, CR_Core_Memory, DefMemPerCPU) beside a whole-node one (`whole`: OverSubscribe=EXCLUSIVE),
on the default cluster's two 4-CPU nodes.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=slices ./agentic/run_smoke.sh slice_request

The SCRIPT
1. connects from the probe's proposal and checks the proposal CLASSIFIED the partitions (`shared` per-core, `whole`
   whole-node), then that the connect's terms PROPOSE a slice (D3) on the per-core default;
2. asks for 2 cores on `whole` and expects the refusal — a slice of a whole-node partition is refused before any submit,
   saying why — with nothing changed;
3. asks for 2 cores and 2 GB on `shared`, confirming in the same call: the receipt names that slice;
4. waits for the block, runs a command reading what Slurm gave the job (SLURM_CPUS_PER_TASK, SLURM_MEM_PER_NODE,
   SLURM_JOB_NUM_NODES), and stops the endpoint.
The world check `slice_allocated` reads the pilot's own sacct row (AllocCPUS / ReqMem / NNodes / Partition), found by
this endpoint's `uep.<eid>` marker in its SubmitLine: 2 CPUs, 2 GB, 1 node on `shared` — not a whole node.
"""
from __future__ import annotations

import re
import time

from invariants import Result, ToolCall, Trace

OPERATORS = ("script",)        # a scripted proof cell: an agent operator is refused (run.py: SKIPPED)
TARGETS = ("fake",)
REQUIRES = {"slices": "per_core+whole_node", "scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "a block asks for a slice (2 cores, 2 GB) on a per-core partition and sacct shows that slice; a slice of a whole-node partition is refused"
TAGS = ["fake", "slices", "resources", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1                  # no agent: the script runs once
NEEDS_COMPUTE_NODE = True      # the slice runs on one node (2 of its 4 CPUs)
TEARDOWN = "delete"

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "bring up the login shape from the probe's proposal, ask for a 2-core slice of the whole-node partition (refused), "
    "then start a block of 2 cores and 2 GB on the per-core `shared` partition, read what Slurm gave it, and stop."
)

WANT_CORES, WANT_GB = 2, 2
PER_CORE, WHOLE = "shared", "whole"
LOGIN_UP_WAIT_S = 300.0
UP_WAIT_S = 300.0
POLL_S = 10.0
STOP_RETRIES = 3
_ALLOC = re.compile(r"HPCB_ALLOC cpus=(\S*) mem=(\S*) nodes=(\S*) job=(\S*)")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

ALLOC_CMD = ('echo "HPCB_ALLOC cpus=${SLURM_CPUS_PER_TASK:-} mem=${SLURM_MEM_PER_NODE:-} '
             'nodes=${SLURM_JOB_NUM_NODES:-} job=${SLURM_JOB_ID:-}"')


def _brief(r: dict) -> str:
    return str(r.get("notice") or r.get("text") or "")[:240]


def proposal_details(r: dict) -> dict:
    """The proposal as the confirming details=: env_setup and partitions left out (the server keeps the proposed ones —
    the tool's own instruction), unset fields dropped."""
    return {k: v for k, v in dict(r.get("proposed_details") or {}).items()
            if v is not None and k not in ("env_setup", "partitions")}


def proposal_kinds(r: dict) -> dict[str, str | None]:
    """partition -> kind, as the probe's proposal classified them."""
    return {str(p.get("name")): p.get("kind") for p in (r.get("proposed_details") or {}).get("partitions") or []
            if isinstance(p, dict)}


async def connect_slices(ops) -> dict:
    """connect_facility from the probe's proposal until the login shape is up (needs_account); aborts unless the
    proposal classified `shared` per-core and `whole` whole-node. Returns the bound result."""
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {_brief(r)!r}")
    kinds = proposal_kinds(r)
    if kinds.get(PER_CORE) != "per_core" or kinds.get(WHOLE) != "whole_node":
        ops.abort(f"the proposal did not classify the partitions ({PER_CORE} per-core, {WHOLE} whole-node): {kinds}")
    ops.say(f"the probe classified the partitions: {kinds}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=proposal_details(r))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(5)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    return r


async def wait_up(ops, e: dict) -> dict:
    deadline = time.monotonic() + UP_WAIT_S
    while e.get("status") != "up":
        if e.get("is_error") or e.get("status") not in ("provisioning", "up") or time.monotonic() > deadline:
            ops.abort(f"the block did not come up: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute")
    return e


async def stop(ops) -> dict:
    s = await ops.call("stop_endpoint")
    for _ in range(STOP_RETRIES):
        if s.get("status") != "draining":
            break
        await ops.sleep(POLL_S)
        s = await ops.call("stop_endpoint")
    return s


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    bound = await connect_slices(ops)
    block = bound.get("block") or {}
    if block.get("size_source") != "proposed" or block.get("whole_node") is not False:
        ops.abort(f"the connect's terms did not propose a slice on the per-core default: {block}")
    ops.say(f"the connect proposed a slice: {block.get('cores_per_node')} cores, {block.get('memory_gb')} GB on "
            f"{block.get('queue')!r}")
    # a slice of the whole-node partition: refused before any submit, nothing changed
    refused = await ops.call("ensure_endpoint_up", shape="compute", partition=WHOLE, cores=WANT_CORES)
    if "hands out whole nodes only" not in str(refused.get("notice") or "") or refused.get("status") == "up":
        ops.abort(f"a slice of {WHOLE!r} was not refused: status={refused.get('status')!r} {_brief(refused)!r}")
    ops.say(f"a {WANT_CORES}-core slice of {WHOLE!r} was refused: {_brief(refused)}")
    # the slice, confirmed in one call: the receipt names it
    e = await ops.call("ensure_endpoint_up", shape="compute", partition=PER_CORE, cores=WANT_CORES,
                       memory_gb=WANT_GB, confirm_spend=True)
    receipt = e.get("block") or {}
    if (receipt.get("cores_per_node"), receipt.get("memory_gb"), receipt.get("whole_node")) != (WANT_CORES,
                                                                                                float(WANT_GB), False):
        ops.abort(f"the confirming call's receipt does not name the slice asked for: {receipt}")
    e = await wait_up(ops, e)
    out = await ops.call("run_shell", command=ALLOC_CMD, shape="compute")
    m = _ALLOC.search(str(out.get("stdout") or ""))
    ops.say(f"Slurm gave the block: {m.group(0) if m else out.get('stdout')!r}")
    s = await stop(ops)
    return (f"A {WANT_CORES}-core, {WANT_GB} GB slice of {PER_CORE!r} ran ({m.group(0) if m else 'no allocation line'}); "
            f"stop_endpoint answered {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}).")


# --- graders -----------------------------------------------------------------------------------------------------------

def _ensures(t: Trace) -> list[tuple[int, ToolCall]]:
    return t.named("ensure_endpoint_up")


def whole_partition_slice_refused(t: Trace) -> Result:
    """A slice of the whole-node partition was refused, saying why, and nothing started: the answer is the standing
    spend question (never `up`), and no runner ran on it."""
    for _i, c in _ensures(t):
        a = c.input or {}
        if a.get("partition") == WHOLE and a.get("cores") is not None:
            notice = str((c.result or {}).get("notice") or "")
            ok = "hands out whole nodes only" in notice and (c.result or {}).get("status") != "up"
            return Result("whole_partition_slice_refused", ok,
                          "ok: refused, naming why" if ok else f"not refused as a whole-node partition: {notice[:200]!r}")
    return Result("whole_partition_slice_refused", False, "premise not met: no slice of the whole-node partition asked")


def slice_receipt(t: Trace) -> Result:
    """The call that confirmed the slice carries its receipt: cores 2, 2 GB, a slice (not whole), on `shared`."""
    for _i, c in _ensures(t):
        a = c.input or {}
        if a.get("confirm_spend") and a.get("cores") == WANT_CORES:
            b = (c.result or {}).get("block") or {}
            got = (b.get("queue"), b.get("cores_per_node"), b.get("memory_gb"), b.get("whole_node"), b.get("size_source"))
            ok = got == (PER_CORE, WANT_CORES, float(WANT_GB), False, "this_call")
            return Result("slice_receipt", ok, f"ok: {got}" if ok else f"the receipt is {got}")
    return Result("slice_receipt", False, "premise not met: no confirming call asked for the slice")


def slice_seen_by_the_job(t: Trace) -> Result:
    """The command on the block saw the slice in its own environment (Slurm's per-task CPUs and per-node memory).
    Report-only: an srun step may set its own SLURM_CPUS_PER_TASK; the scheduler's accounting (`world:slice_allocated`)
    is what gates."""
    for _i, c in t.named("run_shell"):
        m = _ALLOC.search(str((c.result or {}).get("stdout") or ""))
        if m:
            ok = m.group(1) == str(WANT_CORES) and m.group(2) == str(WANT_GB * 1024) and m.group(3) == "1"
            return Result("slice_seen_by_the_job", ok, f"{'ok: ' if ok else ''}{m.group(0)}")
    return Result("slice_seen_by_the_job", False, "no run_shell result carries the allocation line")


def endpoint_ids(t: Trace) -> list[str]:
    out: list[str] = []
    for c in t.calls:
        eid = str((c.result or {}).get("endpoint_id") or "")
        if _UUID.fullmatch(eid) and eid not in out:
            out.append(eid)
    return out


SACCT_FIELDS = "JobID,JobName%60,Partition,AllocCPUS,ReqMem,NNodes,NodeList,State,SubmitLine%600"


def sacct_cmd() -> str:
    """This pool user's jobs of the last day, one parsable row each (the pilot's SubmitLine carries `uep.<eid>`)."""
    return f'sacct -u "$(whoami)" -X -n -P -S now-1days -o {SACCT_FIELDS} 2>&1; echo "HPCB_RC=$?"'


def _mem_mb(req: str) -> float | None:
    m = re.match(r"^\s*(\d+(?:\.\d+)?)([KMGT]?)", req or "", re.I)
    if not m:
        return None
    return float(m.group(1)) * {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}[m.group(2).upper()]


def pilot_rows(out: str, eids: list[str]) -> list[dict[str, str]]:
    """The sacct rows of this run's pilots: a parsl job whose SubmitLine names one of its endpoints (`uep.<eid>`)."""
    names = re.sub(r"%[0-9]+", "", SACCT_FIELDS).split(",")
    rows = []
    for line in out.splitlines():
        cols = line.split("|")
        if len(cols) < len(names):
            continue
        row = dict(zip(names, cols[: len(names) - 1] + ["|".join(cols[len(names) - 1:])], strict=True))
        if "parsl" in row["JobName"] and any(f"uep.{e}" in row["SubmitLine"] for e in eids):
            rows.append(row)
    return rows


async def slice_allocated(ctx) -> Result:
    """world: the pilot of this run that RAN got exactly the slice — AllocCPUS 2, ReqMem 2 GB, 1 node, on `shared` —
    from the scheduler's own accounting, read as the pool user before the teardown."""
    eids = endpoint_ids(ctx.trace)
    if not eids:
        return Result("world:slice_allocated", False, "premise not met: no endpoint id in the run's results")
    rc, out = ctx.ssh(sacct_cmd(), 60)
    if rc != 0 or "HPCB_RC=0" not in out:
        return Result("world:slice_allocated", False, f"UNVERIFIABLE: sacct over the world channel failed (rc={rc}): "
                      f"{out[-300:]!r}")
    rows = [r for r in pilot_rows(out, eids) if r["Partition"] == PER_CORE]
    if not rows:
        return Result("world:slice_allocated", False, f"no pilot of uep.{eids[-1]} on {PER_CORE!r} in sacct: {out[-300:]!r}")
    ran = [r for r in rows if r["State"].split()[0] in ("RUNNING", "COMPLETED", "CANCELLED", "TIMEOUT")]
    r = (ran or rows)[-1]
    got = (r["AllocCPUS"], _mem_mb(r["ReqMem"]), r["NNodes"])
    ok = got == (str(WANT_CORES), float(WANT_GB * 1024), "1")
    detail = (f"job {r['JobID']} on {r['Partition']} ({r['State']}): AllocCPUS={r['AllocCPUS']} ReqMem={r['ReqMem']} "
              f"NNodes={r['NNodes']} NodeList={r['NodeList']}")
    return Result("world:slice_allocated", ok, ("ok: " if ok else "not the slice asked for: ") + detail)


WORLD_CHECKS = [slice_allocated]
EXTRA_INVARIANTS = [whole_partition_slice_refused, slice_receipt, slice_seen_by_the_job]
# world:slice_allocated gates as every world row does; agent_engaged, run_completed (the script finished — an abort is
# a premise unmet) and the safety floor always gate.
EXPECT_OK = ["agent_engaged", "whole_partition_slice_refused", "slice_receipt", "ends_with_stop", "stop_is_honest"]
