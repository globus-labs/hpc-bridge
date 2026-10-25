"""A block of TWO nodes is one shell over the whole allocation, like `salloc` (decision D2 of resource requests,
2026-10-24; vault `Planned/Resource requests.md`): hpc-bridge's worker runs on the first node only, and the user's own
`srun -N2` reaches both. Fake cluster, `slices` profile (two 4-CPU nodes; the per-core `shared` partition); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=slices ./agentic/run_smoke.sh multinode_allocation

Why it matters: parsl's SrunLauncher (hpc-bridge's one-node launcher) would start a worker pool on EVERY node of the
block, as a job step holding the CPUs it was given — so a user's `srun` from the shell waits for them, and the shell
lands on any node. The template takes SimpleLauncher for more than one node: one worker, in the batch script on the
first node. The SCRIPT connects from the probe's proposal, asks for `nodes=2` on `shared` (the spend question must say
what two nodes are; the confirmation that follows must carry a 2-node receipt), waits for the block, and runs three commands through
run_shell: where the shell is (its host, the job's first node), `srun -N2 hostname` (two distinct nodes), and per node
how many worker pools of this user run there (one on the first, none on the second). Then it stops the endpoint. The
world check reads the pilot's sacct row: 2 nodes.
"""
from __future__ import annotations

import re

from invariants import Result, Trace
from slice_request import (
    PER_CORE,
    connect_slices,
    endpoint_ids,
    pilot_rows,
    sacct_cmd,
    stop,
    wait_up,
)

OPERATORS = ("script",)        # a scripted proof cell: an agent operator is refused (run.py: SKIPPED)
TARGETS = ("fake",)
REQUIRES = {"slices": "per_core+whole_node", "scheduler": "slurm", "mfa": "none", "min_nodes": 2}
KIND = "regression"
SUMMARY = "a 2-node block is one shell on the first node: srun -N2 reaches both, hpc-bridge's worker runs on the first only"
TAGS = ["fake", "slices", "resources", "multinode", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = 2         # both nodes (a slice of each)
TEARDOWN = "delete"

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "bring up the login shape from the probe's proposal, start a block of 2 nodes on the per-core `shared` partition, "
    "run `srun -N2 hostname` from its shell, see where hpc-bridge's worker runs, and stop."
)

NODES = 2
WHERE_CMD = ('echo "HPCB_SHELL host=$(hostname -s) first=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1) '
             'nodes=${SLURM_JOB_NUM_NODES:-}"')
SRUN_CMD = "srun -N2 --ntasks-per-node=1 hostname -s | sort"
# per node: how many processes of this user run hpc-bridge's worker pool ([p]: the pattern never matches this command)
POOLS_CMD = ("srun -N2 --ntasks-per-node=1 bash -c "
             "'echo \"HPCB_POOLS $(hostname -s) $(pgrep -u \"$USER\" -f \"[p]rocess_worker_pool\" | wc -l)\"' | sort")
_SHELL = re.compile(r"HPCB_SHELL host=(\S+) first=(\S+) nodes=(\S*)")
_POOLS = re.compile(r"^HPCB_POOLS (\S+) (\d+)$", re.M)


def _stdout(t: Trace, cmd: str) -> str | None:
    for _i, c in t.named("run_shell"):
        if (c.input or {}).get("command") == cmd:
            return str((c.result or {}).get("stdout") or "")
    return None


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    await connect_slices(ops)
    # the spend question first: its terms say what two nodes are (D2), then the confirmation
    q = await ops.call("ensure_endpoint_up", shape="compute", partition=PER_CORE, nodes=NODES)
    notice = str(q.get("notice") or "")
    if q.get("status") != "needs_confirmation" or "one shell, on the first node" not in notice:
        ops.abort(f"the spend question does not say what {NODES} nodes are: status={q.get('status')!r} {notice[:300]!r}")
    e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True)
    if (e.get("block") or {}).get("nodes") != NODES:
        ops.abort(f"the confirming call's receipt is not a {NODES}-node block: {e.get('block')}")
    e = await wait_up(ops, e)
    where = await ops.call("run_shell", command=WHERE_CMD, shape="compute")
    hosts = await ops.call("run_shell", command=SRUN_CMD, shape="compute")
    pools = await ops.call("run_shell", command=POOLS_CMD, shape="compute")
    ops.say(f"the shell: {str(where.get('stdout') or '').strip()!r}; srun -N2: "
            f"{str(hosts.get('stdout') or '').split()!r}; worker pools per node: "
            f"{_POOLS.findall(str(pools.get('stdout') or ''))!r}")
    s = await stop(ops)
    return (f"A {NODES}-node block ran: srun -N2 reached {str(hosts.get('stdout') or '').split()}; stop_endpoint "
            f"answered {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}).")


# --- graders -----------------------------------------------------------------------------------------------------------

def multinode_terms(t: Trace) -> Result:
    """The spend question for 2 nodes says what they are (D2) — one shell on the first node, srun / mpiexec across them,
    unused nodes still bill — and the confirming call that follows carries a 2-node receipt."""
    calls = t.named("ensure_endpoint_up")
    for n, (_i, c) in enumerate(calls):
        if (c.input or {}).get("nodes") == NODES and (c.result or {}).get("status") == "needs_confirmation":
            notice = str((c.result or {}).get("notice") or "")
            said = "one shell, on the first node" in notice and "srun" in notice and "still bill" in notice
            receipt = next(((d.result or {}).get("block") or {} for _j, d in calls[n + 1:]
                            if (d.input or {}).get("confirm_spend")), {})
            ok = said and ((c.result or {}).get("block") or {}).get("nodes") == NODES and receipt.get("nodes") == NODES
            return Result("multinode_terms", ok, "ok: 2 nodes, one shell on the first, and its receipt" if ok
                          else f"D2 unsaid or no 2-node receipt: said={said} receipt={receipt} {notice[:160]!r}")
    return Result("multinode_terms", False, "premise not met: no spend question for 2 nodes")


def srun_reaches_both_nodes(t: Trace) -> Result:
    """`srun -N2 hostname` from the block's shell named two distinct nodes."""
    out = _stdout(t, SRUN_CMD)
    if out is None:
        return Result("srun_reaches_both_nodes", False, "premise not met: srun -N2 was not run")
    names = sorted(set(out.split()))
    ok = len(names) == NODES
    return Result("srun_reaches_both_nodes", ok, f"{'ok: ' if ok else 'not two distinct nodes: '}{names}")


def worker_on_the_first_node_only(t: Trace) -> Result:
    """The shell runs on the job's first node, and hpc-bridge's worker pool runs there and nowhere else."""
    where = _SHELL.search(_stdout(t, WHERE_CMD) or "")
    pools = dict(_POOLS.findall(_stdout(t, POOLS_CMD) or ""))
    if where is None or len(pools) != NODES:
        return Result("worker_on_the_first_node_only", False, f"premise not met: shell {where and where.group(0)!r}, "
                      f"pools {pools}")
    host, first = where.group(1), where.group(2)
    others = {h: int(n) for h, n in pools.items() if h != first}
    ok = host == first and int(pools.get(first, "0")) >= 1 and not any(others.values())
    return Result("worker_on_the_first_node_only", ok,
                  f"{'ok: ' if ok else ''}the shell on {host} (first node {first}); worker pools per node {pools}")


async def two_nodes_allocated(ctx) -> Result:
    """world: the pilot of this run that ran held 2 nodes, by the scheduler's own accounting."""
    eids = endpoint_ids(ctx.trace)
    if not eids:
        return Result("world:two_nodes_allocated", False, "premise not met: no endpoint id in the run's results")
    rc, out = ctx.ssh(sacct_cmd(), 60)
    if rc != 0 or "HPCB_RC=0" not in out:
        return Result("world:two_nodes_allocated", False, f"UNVERIFIABLE: sacct over the world channel failed "
                      f"(rc={rc}): {out[-300:]!r}")
    rows = pilot_rows(out, eids)
    ran = [r for r in rows if r["State"].split()[0] in ("RUNNING", "COMPLETED", "CANCELLED", "TIMEOUT")]
    if not ran:
        return Result("world:two_nodes_allocated", False, f"no pilot of this run ran: {out[-300:]!r}")
    r = ran[-1]
    ok = r["NNodes"] == str(NODES)
    return Result("world:two_nodes_allocated", ok, f"{'ok: ' if ok else ''}job {r['JobID']} ({r['State']}): "
                  f"NNodes={r['NNodes']} NodeList={r['NodeList']} AllocCPUS={r['AllocCPUS']}")


WORLD_CHECKS = [two_nodes_allocated]
EXTRA_INVARIANTS = [multinode_terms, srun_reaches_both_nodes, worker_on_the_first_node_only]
EXPECT_OK = ["agent_engaged", "multinode_terms", "srun_reaches_both_nodes", "worker_on_the_first_node_only",
             "ends_with_stop", "stop_is_honest"]
