"""Resource requests, the pure half (hpc_bridge.resources): what a partition hands out — classified from the login-node
probe's lines (the live Anvil probe of 2026-10-24, Expanse's 127-CPU QOS cap, a select/linear cluster, PBS node sharing)
— what a block of a config gets on it (the default slice, D3), what is refused before any submit, what the wire says on
each scheduler, and the words and the cost of the terms. Vault: `Planned/Resource requests.md`."""
from pathlib import Path

import pytest

from hpc_bridge import discovery, resources
from hpc_bridge.models import PartitionShape
from hpc_bridge.resources import CORES, GPUS, MEM, NODES, WHOLE, Size

FIX = Path(__file__).resolve().parent / "resource_fixtures"


def _probe(name: str):
    draft, notes = discovery.parse_probe((FIX / name).read_text(), ssh_host="login.example.edu")
    return draft, notes, {p.name: p for p in draft.partitions or []}


# ---- the request ----------------------------------------------------------------------------------------------------

def test_the_arguments_become_config_updates_only_for_what_the_call_named():
    assert resources.requested() == ({}, None)
    assert resources.requested(cores=8, memory_gb=16) == ({CORES: 8, MEM: 16, WHOLE: None}, None)  # a slice
    assert resources.requested(nodes=2) == ({NODES: 2}, None)
    assert resources.requested(gpus=1) == ({GPUS: 1}, None)
    assert resources.requested(gpus=0) == ({GPUS: None}, None)                  # 0 clears a GPU request
    # whole_node=True clears a slice's cores and memory, unless cores comes with it (validated against the node)
    assert resources.requested(whole_node=True) == ({WHOLE: True, CORES: None, MEM: None}, None)
    assert resources.requested(whole_node=True, cores=128) == ({WHOLE: True, CORES: 128}, None)
    assert resources.requested(whole_node=False) == ({WHOLE: False}, None)


@pytest.mark.parametrize("kw, why", [
    ({"cores": 0}, "invalid cores 0"), ({"nodes": -1}, "invalid nodes -1"), ({"memory_gb": True}, "a whole number"),
    ({"cores": "8"}, "a whole number"), ({"gpus": -1}, "from 0"), ({"whole_node": "yes"}, "true or false"),
    ({"cores": 10**6}, "from 1 to 100000"),
])
def test_a_malformed_value_is_refused_and_nothing_is_applied(kw, why):
    updates, bad = resources.requested(**kw)
    assert updates == {} and bad is not None and why in bad


# ---- discovery: what a partition hands out --------------------------------------------------------------------------

def test_the_live_anvil_probe_classifies_its_partitions():
    draft, notes, p = _probe("anvil-probe.txt")
    # per-core where the scheduler shares nodes (cons_tres, OverSubscribe=NO), with Anvil's per-core memory default
    for name in ("debug", "shared"):
        assert p[name].kind == "per_core" and "cons_tres" in p[name].why
        assert (p[name].cores, p[name].memory_gb, p[name].mem_per_core_mb) == (128, 251.4, 1896)
        assert p[name].billing == {"cpu": 1.0}
    assert p["shared"].max_cores_per_job == 2048            # part-shared's MaxTRESPU cpu=2048 bounds one job too
    assert p["standard"].kind == "per_core"                  # nothing in its scontrol line says whole nodes
    # `wholenode` says OverSubscribe=NO too (a submit rule makes it whole): only its name tells, and the why says so
    assert p["wholenode"].kind == "whole_node" and "name" in p["wholenode"].why
    # no scontrol line was captured for these: their kind is not known; that sinfo had no GRES column: GPUs unknown
    assert p["wide"].kind is None and p["gpu"].kind is None and p["gpu"].gpus is None
    assert p["gpu"].memory_gb == 503.3 and p["ai"].cores == 96
    assert draft.partition == "debug"                        # the proposal's default partition is unchanged
    note = next(n for n in notes if n.startswith("partitions:"))
    assert "per-core (a block is a slice): standard" in note and "whole-node: wholenode" in note
    assert "not known: wide" in note


def test_expanse_shared_carries_its_127_cpu_qos_cap_and_compute_is_whole_node():
    _draft, _notes, p = _probe("expanse-probe.txt")
    assert p["shared"].kind == "per_core" and p["shared"].max_cores_per_job == 127
    assert "CR_Core_Memory" in p["shared"].why
    assert p["compute"].kind == "whole_node" and p["compute"].why == "OverSubscribe=EXCLUSIVE"


def test_select_linear_makes_every_partition_whole_node_and_exclusive_user_one():
    parts = resources.classify_slurm(select_type="select/linear", select_params=None,
                                     shapes=["main*|4|4000|(null)", "gpu|4|4000|gpu:2"],
                                     details=["PartitionName=main OverSubscribe=NO", "PartitionName=gpu"], qos=[])
    assert [(x.name, x.kind) for x in parts] == [("main", "whole_node"), ("gpu", "whole_node")]
    assert "select/linear" in parts[0].why and parts[1].gpus == 2 and parts[0].gpus == 0
    one = resources.classify_slurm(select_type="select/cons_tres", select_params="CR_Core", shapes=[],
                                   details=["PartitionName=p ExclusiveUser=YES OverSubscribe=NO"], qos=[])
    assert one[0].kind == "whole_node" and one[0].why == "ExclusiveUser=YES"


def test_billing_weights_and_qos_caps_are_read_in_slurms_units():
    assert resources.billing_weights("CPU=1.0,Mem=0.25G,GRES/gpu=16") == {"cpu": 1.0, "mem_gb": 0.25, "gpu": 16.0}
    assert resources.billing_weights("CPU=2,Mem=0.001") == {"cpu": 2.0, "mem_gb": pytest.approx(1.024)}  # per MB
    assert resources.billing_weights("GRES/gpu:a100=8,GRES/gpu=4") == {"gpu": 4.0}  # the untyped weight wins
    assert resources.billing_weights(None) is None and resources.billing_weights("junk") is None
    parts = resources.classify_slurm(
        select_type="select/cons_tres", select_params=None, shapes=["g|64|512000|gpu:a100:4(S:0-1)"],
        details=["PartitionName=g QoS=gq OverSubscribe=NO TRESBillingWeights=CPU=1.0,GRES/gpu=16"],
        qos=["gq|cpu=256,node=4,gres/gpu=8|cpu=128"])
    g = parts[0]
    assert (g.gpus, g.max_cores_per_job, g.max_nodes_per_job, g.max_gpus_per_job) == (4, 128, 4, 8)
    assert g.billing == {"cpu": 1.0, "gpu": 16.0}


def test_pbs_queues_are_classified_by_node_sharing_and_default_place():
    parts = resources.classify_pbs(
        queues=["workq", "exclq", "big"],
        nodes=["n1|64|256gb|4|default_shared|", "n2|64|262144mb|4|default_shared|", "x1|32|128gb|0|force_excl|exclq"],
        queue_attrs=["workq|resources_default.place|excl", "workq|resources_max.ncpus|128",
                     "workq|resources_max.nodect|2", "big|resources_max.ngpus|8"],
        server_attrs=["resources_default.place|scatter"])
    p = {x.name: x for x in parts}
    # workq: nodes shared by default, but the queue's default place is excl: a slice asks to share
    assert p["workq"].kind == "per_core" and p["workq"].place == "free:shared"
    assert (p["workq"].cores, p["workq"].memory_gb, p["workq"].gpus) == (64, 256.0, 4)
    assert (p["workq"].max_cores_per_job, p["workq"].max_nodes_per_job) == (128, 2)
    # exclq: its only node is force_excl — whole nodes
    assert p["exclq"].kind == "whole_node" and "force_excl" in p["exclq"].why and p["exclq"].cores == 32
    # big: the server's default place shares: no place needed
    assert p["big"].kind == "per_core" and p["big"].place is None and p["big"].max_gpus_per_job == 8
    assert resources.classify_pbs(queues=["q"], nodes=[], queue_attrs=[], server_attrs=[])[0].kind is None


def test_a_pbs_probe_reaches_the_proposal():
    text = "\n".join([
        "HPCB_PROBE_BEGIN", "USER=u", "HOME=/home/u", "SCHED=pbs", "QUEUE=debug", "QUEUE=prod",
        "PNODE=n1|32|512gb|4|default_excl|", "PQUEUE=prod|resources_max.nodect|10", "NIC=hsn0|10.1.0.1/16",
        "HPCB_PROBE_END"])
    draft, notes = discovery.parse_probe(text, ssh_host="polaris.example.edu")
    p = {x.name: x for x in draft.partitions}
    assert p["debug"].kind == "per_core" and p["debug"].place == "free:shared"   # default_excl nodes: ask to share
    assert p["prod"].max_nodes_per_job == 10 and p["prod"].cores == 32
    assert any(n.startswith("partitions:") for n in notes)


def test_a_partition_shape_is_read_leniently():
    s = PartitionShape.model_validate({"name": "shared*", "kind": "sliced", "cores": "128", "memory_gb": -1,
                                       "gpus": 0, "billing": {"cpu": "1.0", "bogus": 3, "gpu": "x"}, "why": 5})
    assert (s.name, s.kind, s.cores, s.memory_gb, s.gpus, s.billing, s.why) == ("shared", None, 128, None, 0,
                                                                                  {"cpu": 1.0}, None)
    with pytest.raises(ValueError):
        PartitionShape.model_validate({"name": "bad name; rm"})


# ---- what a block gets ----------------------------------------------------------------------------------------------

SHARED = PartitionShape(name="shared", kind="per_core", cores=128, memory_gb=251.4, mem_per_core_mb=1896,
                        billing={"cpu": 1.0})
WHOLE_P = PartitionShape(name="wholenode", kind="whole_node", why="OverSubscribe=EXCLUSIVE", cores=128,
                         memory_gb=251.4, billing={"cpu": 1.0})


def _uec(**kw):
    return {"compute": True, "partition": "shared", NODES: 1, **kw}


def test_d3_a_per_core_partition_with_nothing_asked_gets_a_proposed_slice():
    size = resources.effective(_uec(), SHARED, channel="ssh", takes=True)
    assert size == Size(nodes=1, whole_node=False, cores=8, memory_gb=14.8, memory_default=True, proposed=True)
    # never more than half a node, nor than a per-job cap allows for the nodes asked
    small = PartitionShape(name="s", kind="per_core", cores=4, memory_gb=4.0)
    assert resources.effective(_uec(), small, channel="ssh").cores == 2
    capped = SHARED.model_copy(update={"max_cores_per_job": 12})
    assert resources.effective(_uec(**{NODES: 4}), capped, channel="ssh").cores == 3
    # no DefMemPerCPU: the node's memory per core, SENT (a partition with no default may give the whole node's)
    nodef = SHARED.model_copy(update={"mem_per_core_mb": None})
    s = resources.effective(_uec(), nodef, channel="ssh")
    assert (s.memory_gb, s.memory_default, s.send_mem) == (15.0, False, 15)


def test_no_proposal_where_the_template_cannot_take_it_or_nothing_is_known():
    for takes in (False, None):   # an endpoint configured before this change, or one whose template was not read
        size = resources.effective(_uec(), SHARED, channel="ssh", takes=takes)
        assert size.whole_node is None and not size.proposed
    # nothing known of the partition: the facility's own default, as before (the registry's exclusive: False)
    assert resources.effective(_uec(), None, channel="ssh").whole_node is None
    expanse_like = resources.effective(_uec(**{WHOLE: False}), None, channel="ssh")
    assert (expanse_like.whole_node, expanse_like.cores, expanse_like.proposed) == (False, None, False)


def test_whole_nodes_when_asked_or_when_the_partition_gives_only_those():
    asked = resources.effective(_uec(**{WHOLE: True}), SHARED, channel="ssh")
    assert (asked.whole_node, asked.cores, asked.by_partition) == (True, 128, False)
    by_part = resources.effective(_uec(partition="wholenode"), WHOLE_P, channel="ssh", takes=False)
    assert (by_part.whole_node, by_part.cores, by_part.by_partition) == (True, 128, True)


def test_an_explicit_slice_is_what_was_asked():
    s = resources.effective(_uec(**{CORES: 2, MEM: 2, GPUS: 1}), SHARED, channel="ssh")
    assert (s.whole_node, s.cores, s.memory_gb, s.memory_default, s.send_mem, s.gpus, s.proposed) == (
        False, 2, 2.0, False, 2, 1, False)
    # cores without memory: the partition's default per core (not sent)
    s = resources.effective(_uec(**{CORES: 16}), SHARED, channel="ssh")
    assert (s.memory_gb, s.memory_default, s.send_mem) == (29.6, True, None)


def test_a_facility_endpoint_gets_exactly_the_keys_it_receives():
    s = resources.effective({CORES: 1, NODES: 1}, SHARED, channel="mep")
    assert (s.whole_node, s.cores, s.memory_gb, s.memory_default, s.proposed) == (False, 1, 1.9, True, False)
    assert resources.effective({NODES: 1}, SHARED, channel="mep").whole_node is None   # no D3 there
    assert resources.effective({WHOLE: True, NODES: 1}, SHARED, channel="mep").cores == 128


# ---- the validation -------------------------------------------------------------------------------------------------

def test_a_slice_of_a_whole_node_partition_is_refused_naming_the_per_core_ones():
    why = resources.refusal(_uec(partition="wholenode", **{CORES: 8}), WHOLE_P, named={CORES},
                            partitions=[SHARED, WHOLE_P])
    assert "hands out whole nodes only (OverSubscribe=EXCLUSIVE)" in why and "cores=8" in why
    assert "whole_node=True" in why and "a per-core partition (shared)" in why
    assert "would hold and bill the whole node" in why
    assert "whole_node=False" in resources.refusal(_uec(partition="wholenode", **{WHOLE: False}), WHOLE_P,
                                                   named={WHOLE})
    # the whole node itself is fine there, and a facility endpoint's template decides its own
    assert resources.refusal(_uec(partition="wholenode", **{WHOLE: True}), WHOLE_P, named={WHOLE}) is None
    assert resources.refusal(_uec(**{CORES: 8}), WHOLE_P, named={CORES}, channel="mep") is None


def test_whole_node_with_fewer_cores_than_a_node_is_refused():
    why = resources.refusal(_uec(**{WHOLE: True, CORES: 8}), SHARED, named={WHOLE, CORES})
    assert "every core of a node (128" in why and "cores=8" in why
    assert "not known" in resources.refusal(_uec(**{WHOLE: True, CORES: 8}), None, named={WHOLE, CORES})
    assert resources.refusal(_uec(**{WHOLE: True, CORES: 128}), SHARED, named={WHOLE, CORES}) is None


def test_more_than_a_node_has_is_refused_and_an_earlier_choice_is_named_as_one():
    assert "more than a node of partition 'shared' has (128 cores)" in resources.refusal(
        _uec(**{CORES: 200}), SHARED, named={CORES})
    assert "(251.4 GB)" in resources.refusal(_uec(**{MEM: 300}), SHARED, named={MEM})
    no_gpu = SHARED.model_copy(update={"gpus": 0})
    assert "have no GPUs" in resources.refusal(_uec(**{GPUS: 1}), no_gpu, named={GPUS})
    four = SHARED.model_copy(update={"gpus": 4})
    assert "(4 GPUs)" in resources.refusal(_uec(**{GPUS: 8}), four, named={GPUS})
    # the cores came from an earlier call; this call switched the partition
    why = resources.refusal(_uec(partition="wholenode", **{CORES: 8}), WHOLE_P, named={"partition"})
    assert "cores=8 (chosen earlier this session)" in why
    assert resources.refusal(_uec(**{CORES: 200}), None, named={CORES}) is None   # nothing known: not refused


def test_a_request_over_a_qos_per_job_cap_is_refused_naming_the_cap():
    # Expanse `shared`: 127 CPUs per job on 128-core nodes — such a block pends forever (QOSMaxCpuPerJobLimit)
    _d, _n, p = _probe("expanse-probe.txt")
    why = resources.refusal(_uec(**{CORES: 128}), p["shared"], named={CORES})
    assert "over the QOS cap of partition 'shared': at most 127 cores per job" in why and "pends" in why
    assert "2 nodes × 64 cores" in resources.refusal(_uec(**{CORES: 64, NODES: 2}), p["shared"], named={CORES, NODES})
    assert resources.refusal(_uec(**{CORES: 127}), p["shared"], named={CORES}) is None
    capped = SHARED.model_copy(update={"max_nodes_per_job": 2, "max_gpus_per_job": 4, "gpus": 4})
    assert "at most 2 nodes per job" in resources.refusal(_uec(**{NODES: 3}), capped, named={NODES})
    assert "at most 4 GPUs per job" in resources.refusal(_uec(**{NODES: 2, GPUS: 4}), capped, named={GPUS})


# ---- the wire -------------------------------------------------------------------------------------------------------

def test_slurm_wire_a_slice_whole_nodes_and_gpus():
    proposed = resources.effective(_uec(), SHARED, channel="ssh")
    assert resources.slurm_wire(_uec(), proposed) == {**_uec(), WHOLE: False, CORES: 8}   # no --mem: DefMemPerCPU
    asked = _uec(**{CORES: 2, MEM: 2, GPUS: 1, "scheduler_options": "#SBATCH --constraint=a100"})
    out = resources.slurm_wire(asked, resources.effective(asked, SHARED, channel="ssh"))
    assert (out[WHOLE], out[CORES], out[MEM]) == (False, 2, 2) and GPUS not in out
    assert out["scheduler_options"] == "#SBATCH --constraint=a100\n#SBATCH --gpus-per-node=1"
    whole = _uec(**{WHOLE: True})
    assert resources.slurm_wire(whole, resources.effective(whole, SHARED, channel="ssh")) == whole
    # nothing known: what the config had (the registry's exclusive, or none: parsl's whole node)
    assert resources.slurm_wire(_uec(), resources.effective(_uec(), None, channel="ssh")) == _uec()


def test_pbs_wire_select_ncpus_and_place():
    q = PartitionShape(name="workq", kind="per_core", cores=64, memory_gb=256.0, gpus=4, place="free:shared")
    base = {"compute": True, "partition": "workq", NODES: 1, "cpus_per_node": 64, "select_options": "ngpus=4",
            "scheduler_options": "#PBS -l filesystems=home:eagle"}
    asked = {**base, CORES: 4, MEM: 16, GPUS: 1}
    out = resources.pbs_wire(asked, resources.effective(asked, q, channel="ssh", profile_cpus=64), q)
    assert out["cpus_per_node"] == 4 and out["select_options"] == "mem=16gb:ngpus=1"   # the profile's ngpus replaced
    assert out["scheduler_options"].endswith("\n#PBS -l place=free:shared") and not set(out) & {CORES, MEM, GPUS}
    two = {**base, NODES: 2, CORES: 4}
    out = resources.pbs_wire(two, resources.effective(two, q, channel="ssh", profile_cpus=64), q)
    assert out["scheduler_options"].endswith("place=scatter:shared")            # N chunks on N distinct nodes
    whole = {**base, WHOLE: True}
    out = resources.pbs_wire(whole, resources.effective(whole, q, channel="ssh", profile_cpus=64), q)
    assert out["cpus_per_node"] == 64 and "place" not in out["scheduler_options"]   # the node's ncpus fills it
    unknown = {**base, WHOLE: True}
    out = resources.pbs_wire(unknown, resources.effective(unknown, None, channel="ssh", profile_cpus=64), None)
    assert out["scheduler_options"].endswith("#PBS -l place=excl")              # a whole node of unknown size
    # nothing asked and nothing known: the config as it was
    plain = resources.effective(base, None, channel="ssh", profile_cpus=64)
    assert resources.pbs_wire(base, plain, None) == base


# ---- the terms and the cost -----------------------------------------------------------------------------------------

def test_the_cost_estimate_of_a_slice_a_whole_node_and_several_nodes():
    slice_ = resources.effective(_uec(), SHARED, channel="ssh")
    assert resources.charge(slice_, SHARED, 1800) == (4.0, "8 cores × 0.5 h")        # the spec's own example
    whole = resources.effective(_uec(**{WHOLE: True}), SHARED, channel="ssh")
    assert resources.charge(whole, SHARED, 1800) == (64.0, "128 cores × 0.5 h")
    two = resources.effective(_uec(**{NODES: 2, CORES: 4}), SHARED, channel="ssh")
    assert resources.charge(two, SHARED, 3600) == (8.0, "2 nodes × 4 cores × 1 h")
    w = SHARED.model_copy(update={"billing": {"cpu": 1.0, "mem_gb": 0.25, "gpu": 16.0}})
    g = resources.effective(_uec(**{CORES: 4, MEM: 16, GPUS: 1}), w, channel="ssh")
    assert resources.charge(g, w, 900) == (6.0, "(4 cores + 16 GB × 0.25 + 1 GPU × 16) × 0.25 h")
    assert resources.charge(slice_, SHARED.model_copy(update={"billing": None}), 1800) is None   # weights unknown
    assert resources.charge(resources.Size(nodes=1), SHARED, 1800) is None                     # cores unknown


def test_the_size_words():
    w = resources.size_words
    assert w(nodes=1, whole_node=False, cores=8, memory_gb=14.8, memory_default=True, gpus=None) == (
        "1 node × 8 cores, 15 GB (the partition's default per core)")
    assert w(nodes=1, whole_node=True, cores=128, memory_gb=251.4, memory_default=False, gpus=None) == (
        "1 whole node (128 cores)")
    assert w(nodes=2, whole_node=False, cores=2, memory_gb=2.0, memory_default=False, gpus=1) == (
        "2 nodes × 2 cores, 2 GB, 1 GPU each")
    assert w(nodes=1, whole_node=None, cores=None, memory_gb=None, memory_default=False, gpus=None) == "1 node"
    assert w(nodes=None, whole_node=None, cores=None, memory_gb=None, memory_default=False, gpus=None) == (
        "the facility's default size")
    note = resources.multinode_note(2, False)
    assert "one shell, on the first node" in note and "srun" in note and "still bill" in note
    assert resources.multinode_note(1, False) == "" and "one of the nodes" in resources.multinode_note(3, True)


def test_a_registry_exclusive_false_is_no_slice_request_on_a_whole_node_partition():
    # the Expanse entry's `exclusive: false` default (for `shared`) rides every block's config: on its whole-node
    # `compute` it is not the user asking for a slice — the partition decides, and the block asks for the whole node
    uec = _uec(partition="wholenode", **{WHOLE: False})
    assert resources.refusal(uec, WHOLE_P, named={"partition"}) is None
    assert "whole_node=False (chosen earlier" in resources.refusal(uec, WHOLE_P, named={"partition"}, chosen={WHOLE})
    size = resources.effective(uec, WHOLE_P, channel="ssh")
    assert size.whole_node is True and size.by_partition and resources.slurm_wire(uec, size)[WHOLE] is True
