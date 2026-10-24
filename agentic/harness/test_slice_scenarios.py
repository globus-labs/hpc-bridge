"""Hermetic: the `slices` profile and its two scripted scenarios (resource requests, 2026-10-24) — slice_request (a 2-core,
2 GB slice that sacct must show; a slice of the whole-node partition refused) and multinode_allocation (2 nodes: one
shell on the first node, srun -N2 reaching both, hpc-bridge's worker on the first only). The profile's slurm.conf read
through the product's own probe parser; each SCRIPT against a stand-in server; the graders and world checks on what a
correct and a broken build would show. No agent, no cluster."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import multinode_allocation as mn  # noqa: E402
import script_runner  # noqa: E402
import slice_request as sr  # noqa: E402
import targets  # noqa: E402
from invariants import Result, ToolCall, Trace, check_all  # noqa: E402

FC = HERE.parent / "fakecluster"
EID = "54736c9f-06eb-4183-96e3-65a23b75a770"


# ---- the profile ------------------------------------------------------------------------------------------------------

def test_the_slices_profile_puts_a_per_core_partition_beside_a_whole_node_one():
    m = targets.load_profile("slices")
    caps = m["capabilities"]
    assert m["layers"] == ["default", "slices"] and caps["partitions"] == ["main", "shared", "whole"]
    assert caps["nodes"] == 2 and caps["slices"] == "per_core+whole_node"
    conf = (FC / "profiles" / "slices" / "slurm.conf").read_text()
    assert "SelectType=select/cons_tres" in conf and "SelectTypeParameters=CR_Core_Memory" in conf
    assert "DefMemPerCPU=500" in conf
    assert "PartitionName=main   Nodes=c[1-2] Default=YES" in conf            # the harness's default partition stays
    assert "PartitionName=shared" in conf and "OverSubscribe=NO DefMemPerCPU=500 TRESBillingWeights=CPU=1.0" in conf
    assert "PartitionName=whole" in conf and "OverSubscribe=EXCLUSIVE" in conf
    for sc in (sr, mn):
        assert sc.TARGETS == ("fake",) and sc.OPERATORS == ("script",)
        assert targets.meets(sc.REQUIRES, caps)[0], sc.__name__
        for other in ("default", "site", "pbs", "mep"):
            assert not targets.meets(sc.REQUIRES, targets.load_profile(other)["capabilities"])[0], (sc.__name__, other)


def _probe_text() -> str:
    """What the product's login-node probe prints on the `slices` cluster (slurm 23.11's formats)."""
    return "\n".join([
        "HPCB_PROBE_BEGIN", "USER=hpcbridge-test-00", "HOME=/home/hpcbridge-test-00", "SCHED=slurm",
        "PART=main*|up", "PART=shared|up", "PART=whole|up",
        "PSHAPE=main*|4|4000|(null)", "PSHAPE=shared|4|4000|(null)", "PSHAPE=whole|4|4000|(null)",
        "SELECT_TYPE=select/cons_tres", "SELECT_PARAMS=CR_CORE_MEMORY",
        "PDETAIL=PartitionName=main AllowGroups=ALL Default=YES QoS=N/A OverSubscribe=NO DefMemPerCPU=500 State=UP",
        "PDETAIL=PartitionName=shared Default=NO QoS=N/A OverSubscribe=NO DefMemPerCPU=500 State=UP "
        "TRESBillingWeights=CPU=1.0",
        "PDETAIL=PartitionName=whole Default=NO QoS=N/A OverSubscribe=EXCLUSIVE DefMemPerCPU=500 State=UP",
        "QOS=normal||", "NIC=eth0|172.18.0.5/16", "UV=/usr/local/bin/uv", "HPCB_PROBE_END"])


def _proposal() -> dict:
    from hpc_bridge import discovery
    draft, notes = discovery.parse_probe(_probe_text(), ssh_host="login")
    return {"phase": "proposed_facility_details", "proposed_details": draft.model_dump(), "notice": " | ".join(notes)}


def test_the_product_probe_classifies_the_slices_partitions_and_proposes_shared():
    r = _proposal()
    kinds = sr.proposal_kinds(r)   # `main` depends on whether scontrol shows the global DefMemPerCPU for it: not read
    assert (kinds["shared"], kinds["whole"]) == ("per_core", "whole_node")
    assert r["proposed_details"]["partition"] == "shared"            # the probe's own pick: a cheap per-core one
    details = sr.proposal_details(r)
    assert "partitions" not in details and "env_setup" not in details and details["partition"] == "shared"


# ---- slice_request's SCRIPT against a stand-in server -------------------------------------------------------------------

class _Ops:
    """script_runner.ScriptOps' surface, answering from `answers` (tool -> list, consumed in order)."""

    def __init__(self, answers):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls, self.texts = [], []
        self.facility, self.ssh_host = "fake-1", "login"

    async def call(self, tool, **args):
        res = self.answers[tool].pop(0)
        self.calls.append((tool, args, res))
        return res

    def say(self, text):
        self.texts.append(text)

    def abort(self, reason):
        raise script_runner.ScriptAbort(reason)

    async def sleep(self, s):
        return None

    def trace(self) -> Trace:
        return Trace([ToolCall.of(f"mcp__hpc_bridge__{t}", a, r) for t, a, r in self.calls], self.texts)


_PROPOSED_BLOCK = {"queue": "shared", "whole_node": False, "cores_per_node": 2, "memory_gb": 1.0,
                   "memory_default": True, "size_source": "proposed", "nodes": 1}
_BOUND = {"phase": "needs_account", "block": _PROPOSED_BLOCK, "endpoint_id": EID}
_REFUSED = {"status": "needs_confirmation", "endpoint_id": EID, "notice": (
    "partition 'whole' hands out whole nodes only (OverSubscribe=EXCLUSIVE): cores=2 asks for a slice, which would "
    "hold and bill the whole node there. Ask for whole_node=True on it, or a per-core partition (main, shared).")}
_RECEIPT = {"status": "provisioning", "endpoint_id": EID, "block": {
    "queue": "shared", "cores_per_node": 2, "memory_gb": 2.0, "whole_node": False, "size_source": "this_call",
    "nodes": 1}}
_UP = {"status": "up", "endpoint_id": EID, "notice": "worker live on c1"}
_DOWN = {"status": "down", "spend_stopped": True, "endpoint_id": EID, "notice": "compute block released (released 3)"}


def _gated_failures(sc, t: Trace) -> dict[str, str]:
    results = {r.name: r for r in check_all(t)} | {r.name: r for r in (fn(t) for fn in sc.EXTRA_INVARIANTS)}
    return {n: results[n].detail for n in sc.EXPECT_OK if not results[n].ok}


def _slice_ops(**over) -> _Ops:
    answers = {"connect_facility": [_proposal(), {"phase": "provisioning"}, _BOUND],
               "ensure_endpoint_up": [_REFUSED, _RECEIPT, _UP],
               "run_shell": [{"phase": "complete", "exit_code": 0,
                              "stdout": "HPCB_ALLOC cpus=2 mem=2048 nodes=1 job=3\n"}],
               "stop_endpoint": [{"status": "draining", "notice": "Spend is NOT confirmed stopped"}, _DOWN]}
    answers.update(over)
    return _Ops(answers)


def test_the_slice_script_refuses_the_whole_partition_then_runs_the_slice_and_stops():
    ops = _slice_ops()
    final = asyncio.run(sr.SCRIPT(ops))
    tools = [t for t, _a, _r in ops.calls]
    assert tools == ["connect_facility"] * 3 + ["ensure_endpoint_up"] * 3 + ["run_shell", "stop_endpoint", "stop_endpoint"]
    ensures = [a for t, a, _r in ops.calls if t == "ensure_endpoint_up"]
    assert ensures[0] == {"shape": "compute", "partition": "whole", "cores": 2}
    assert ensures[1] == {"shape": "compute", "partition": "shared", "cores": 2, "memory_gb": 2, "confirm_spend": True}
    assert ensures[2] == {"shape": "compute"}                       # a poll never confirms again
    assert "'down'" in final and "HPCB_ALLOC cpus=2 mem=2048" in final
    assert _gated_failures(sr, ops.trace()) == {}
    assert sr.slice_seen_by_the_job(ops.trace()).ok


@pytest.mark.parametrize("over, why", [
    ({"connect_facility": [{**_proposal(), "proposed_details": {**_proposal()["proposed_details"], "partitions": None}}]},
     "did not classify the partitions"),
    ({"connect_facility": [_proposal(), _BOUND | {"block": {**_PROPOSED_BLOCK, "size_source": "facility",
                                                             "whole_node": None}}]},
     "did not propose a slice"),
    ({"ensure_endpoint_up": [{"status": "needs_confirmation", "notice": "Block terms: partition 'whole' …"}]},
     "was not refused"),
    ({"ensure_endpoint_up": [_REFUSED, {"status": "provisioning", "block": {"cores_per_node": None}}]},
     "receipt does not name the slice"),
])
def test_the_slice_script_aborts_on_a_build_that_does_not_size_the_block(over, why):
    with pytest.raises(script_runner.ScriptAbort, match=why):
        asyncio.run(sr.SCRIPT(_slice_ops(**over)))


def test_the_slice_graders_fail_a_whole_node_receipt_and_an_unrefused_slice():
    ops = _slice_ops(ensure_endpoint_up=[_REFUSED, {**_RECEIPT, "block": {**_RECEIPT["block"], "whole_node": True,
                                                                          "cores_per_node": 4}}, _UP])
    with pytest.raises(script_runner.ScriptAbort):
        asyncio.run(sr.SCRIPT(ops))
    assert not sr.slice_receipt(ops.trace()).ok
    no_refusal = Trace([ToolCall.of("mcp__hpc_bridge__ensure_endpoint_up", {"partition": "whole", "cores": 2},
                                    {"status": "up", "notice": "worker live"})])
    assert not sr.whole_partition_slice_refused(no_refusal).ok
    assert "premise not met" in sr.whole_partition_slice_refused(Trace([])).detail


# ---- slice_request's world check: the pilot's own sacct row -------------------------------------------------------------

def _sacct(*rows: tuple[str, ...]) -> str:
    """sacct -X -n -P -o JobID,JobName,Partition,AllocCPUS,ReqMem,NNodes,NodeList,State,SubmitLine (slurm 23.11)."""
    return "\n".join("|".join(r) for r in rows) + "\nHPCB_RC=0\n"


_SUBMIT = (f"sbatch /home/hpcbridge-test-00/.globus_compute/uep.{EID}.34aaadd8-3a58-d9ea-c9b0-49257afec4d2/"
           "submit_scripts/parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1792105266.submit")
_PILOT = "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1792105266"


class _Ctx:
    def __init__(self, trace: Trace, out: str):
        self.trace, self._out, self.cmds = trace, out, []

    def ssh(self, cmd, timeout=120):
        self.cmds.append(cmd)
        return 0, self._out


def _run_trace() -> Trace:
    ops = _slice_ops()
    asyncio.run(sr.SCRIPT(ops))
    return ops.trace()


def test_the_world_check_reads_the_slice_from_sacct():
    good = _sacct(("3", _PILOT, "shared", "2", "2G", "1", "c1", "CANCELLED by 1000", _SUBMIT),
                  ("2", "other-job", "shared", "4", "2000M", "1", "c2", "COMPLETED", "sbatch other.sh"))
    ctx = _Ctx(_run_trace(), good)
    r = asyncio.run(sr.slice_allocated(ctx))
    assert r.name == "world:slice_allocated" and r.ok, r.detail
    assert "AllocCPUS=2 ReqMem=2G NNodes=1" in r.detail
    assert "SubmitLine" in ctx.cmds[0] and "-u \"$(whoami)\"" in ctx.cmds[0]
    # a whole node (parsl's old default) is the regression this cell exists for
    whole = _sacct(("3", _PILOT, "shared", "4", "2000M", "1", "c1", "CANCELLED by 1000", _SUBMIT))
    bad = asyncio.run(sr.slice_allocated(_Ctx(_run_trace(), whole)))
    assert not bad.ok and "not the slice asked for" in bad.detail and "AllocCPUS=4" in bad.detail
    # a pilot of another endpoint (another cell's) is never read as this one's
    other = _sacct(("3", _PILOT, "shared", "2", "2G", "1", "c1", "COMPLETED", _SUBMIT.replace(EID, "0" * 8 + EID[8:])))
    assert not asyncio.run(sr.slice_allocated(_Ctx(_run_trace(), other))).ok
    assert "premise not met" in asyncio.run(sr.slice_allocated(_Ctx(Trace([]), good))).detail

    class _Down(_Ctx):
        def ssh(self, cmd, timeout=120):
            return 255, "ssh: connect to host login port 22: Connection refused"

    for check in (sr.slice_allocated, mn.two_nodes_allocated):   # a world channel that is down proves nothing
        r = asyncio.run(check(_Down(_run_trace(), "")))
        assert not r.ok and r.detail.startswith("UNVERIFIABLE")


# ---- multinode_allocation ---------------------------------------------------------------------------------------------

_Q2 = {"status": "needs_confirmation", "endpoint_id": EID, "block": {"nodes": 2, "cores_per_node": 2}, "notice": (
    "Block terms: partition 'shared' (proposed by the login-node probe); no account set …; 2 nodes × 2 cores, 1 GB (the "
    "partition's default per core) for up to 00:30:00 (at most 1 node-hour; at most 2 SU: 2 nodes × 2 cores × 0.5 h). "
    "2 nodes: one shell, on the first node — run work across them with srun (or mpiexec) from it, as from an salloc "
    "shell; nodes you do not use still bill. Spend not yet confirmed: nothing was started.")}
_R2 = {"status": "provisioning", "endpoint_id": EID, "block": {"nodes": 2, "cores_per_node": 2}}


def _mn_ops(*, where="HPCB_SHELL host=c1 first=c1 nodes=2\n", hosts="c1\nc2\n",
            pools="HPCB_POOLS c1 3\nHPCB_POOLS c2 0\n", ensure=None) -> _Ops:
    return _Ops({"connect_facility": [_proposal(), _BOUND],
                 "ensure_endpoint_up": ensure or [_Q2, _R2, _UP],
                 "run_shell": [{"phase": "complete", "exit_code": 0, "stdout": s} for s in (where, hosts, pools)],
                 "stop_endpoint": [_DOWN]})


def test_the_multinode_script_asks_then_confirms_and_reads_where_the_worker_runs():
    ops = _mn_ops()
    final = asyncio.run(mn.SCRIPT(ops))
    ensures = [a for t, a, _r in ops.calls if t == "ensure_endpoint_up"]
    assert ensures == [{"shape": "compute", "partition": "shared", "nodes": 2}, {"shape": "compute", "confirm_spend": True},
                       {"shape": "compute"}]
    shells = [a["command"] for t, a, _r in ops.calls if t == "run_shell"]
    assert shells == [mn.WHERE_CMD, mn.SRUN_CMD, mn.POOLS_CMD] and "srun -N2" in mn.SRUN_CMD
    assert "[p]rocess_worker_pool" in mn.POOLS_CMD                  # the pattern never matches its own command line
    assert "['c1', 'c2']" in final
    assert _gated_failures(mn, ops.trace()) == {}


def test_the_multinode_graders_fail_a_worker_on_every_node_one_node_or_a_shell_elsewhere():
    every = _mn_ops(pools="HPCB_POOLS c1 3\nHPCB_POOLS c2 3\n")      # SrunLauncher: a pool on every node
    asyncio.run(mn.SCRIPT(every))
    r = mn.worker_on_the_first_node_only(every.trace())
    assert not r.ok and "'c2': '3'" in r.detail
    one = _mn_ops(hosts="c1\nc1\n")
    asyncio.run(mn.SCRIPT(one))
    assert not mn.srun_reaches_both_nodes(one.trace()).ok
    elsewhere = _mn_ops(where="HPCB_SHELL host=c2 first=c1 nodes=2\n")
    asyncio.run(mn.SCRIPT(elsewhere))
    assert not mn.worker_on_the_first_node_only(elsewhere.trace()).ok
    unsaid = {**_Q2, "notice": "Block terms: … 2 nodes for up to 00:30:00. Spend not yet confirmed."}
    with pytest.raises(script_runner.ScriptAbort, match="does not say what 2 nodes are"):
        asyncio.run(mn.SCRIPT(_mn_ops(ensure=[unsaid])))
    assert not mn.multinode_terms(Trace([ToolCall.of("mcp__hpc_bridge__ensure_endpoint_up", {"nodes": 2},
                                                     unsaid)])).ok


def test_the_multinode_world_check_wants_two_nodes_in_sacct():
    trace = _mn_ops()
    asyncio.run(mn.SCRIPT(trace))
    two = _sacct(("4", _PILOT, "shared", "4", "1000M", "2", "c[1-2]", "CANCELLED by 1000", _SUBMIT))
    r = asyncio.run(mn.two_nodes_allocated(_Ctx(trace.trace(), two)))
    assert r.ok and "NNodes=2 NodeList=c[1-2]" in r.detail
    one = _sacct(("4", _PILOT, "shared", "2", "1000M", "1", "c1", "CANCELLED by 1000", _SUBMIT))
    assert not asyncio.run(mn.two_nodes_allocated(_Ctx(trace.trace(), one))).ok
    assert isinstance(asyncio.run(mn.two_nodes_allocated(_Ctx(Trace([]), two))), Result)


def test_the_scenarios_gate_only_graders_they_provide():
    universal = {r.name for r in check_all(Trace([]))}
    for sc in (sr, mn):
        provided = universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
        assert set(sc.EXPECT_OK) <= provided, (sc.__name__, set(sc.EXPECT_OK) - provided)
        assert [fn.__name__ for fn in sc.WORLD_CHECKS]
