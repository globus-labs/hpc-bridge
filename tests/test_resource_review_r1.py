"""Resource requests, review round 1 (2026-10-24): the terms must never UNDERSTATE what a block can bill. Each test is a
reviewer's probe (scratchpad rv-resources/probes) turned into an assertion — red on 6ea6130, green after:

1. a facility endpoint's whole-node partition is a whole node (Anvil's `wholenode` read "1 core … 0.25 SU", billing 128
   cores), a slice there is refused, and a slice is claimed only when `exclusive: false` reaches the template;
2. size directives in scheduler_options are refused, pointing at cores=/memory_gb=/gpus=/whole_node= — and the ones a
   facility's own configuration carries (Delta's `--gpus-per-node=1`, an `--exclusive`) are read into the terms;
3. terms read before the endpoint's template was known are asked again when the block would hold more;
4. memory bills as cores past the per-core share (Anvil: cores or memory share, whichever is larger);
5. per-core only on evidence (DefMemPerCPU, OverSubscribe=YES/FORCE): not CR_Socket, not a whole-node partition's QoS
   twin; the name rule anchored (`nonexclusive` is not whole-node);
6. a request equal to what the block already holds is no term switch;
7./8. an unknown-kind partition (or an endpoint older than block sizes) says parsl's whole node, with its cost;
9. partition limits (MaxNodes, MaxCPUsPerNode, TotalNodes; GPUs by the largest node);
11. several nodes are not offered on a facility endpoint; 12. the MPI words; and the nits."""
import dataclasses

import pytest

from hpc_bridge import binding, resources
from hpc_bridge.catalog.entry import CatalogEntry, defaults_extra_raw_problems, scheduler_terms_kind
from hpc_bridge.facility import remote
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.models import FacilityDetails, PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx, _ensure_endpoint_up
from tests.fakes import fake_entry, fake_mep_entry
from tests.test_mep_server import ANVIL_SCHEMA, ANVIL_TEMPLATE, DELTA_SCHEMA, _Meta, _Templated
from tests.test_resource_requests import _app, _Sized
from tests.test_spend_terms import _mep_app, _runners

SHARED = PartitionShape(name="shared", kind="per_core", why="x", cores=128, memory_gb=251.4, mem_per_core_mb=1896,
                        billing={"cpu": 1.0})
WHOLE = PartitionShape(name="wholenode", kind="whole_node", why="Anvil's whole-node partition", cores=128,
                       memory_gb=251.4, billing={"cpu": 1.0})
GPU = PartitionShape(name="gpu", kind="per_core", why="x", cores=128, memory_gb=512, gpus=4, mem_per_core_mb=3800,
                     billing={"cpu": 1.0, "gpu": 32.0})


def _ssh(takes=True, partitions=(SHARED, WHOLE), **kw):
    prof = remote.MachineProfile(name="x", endpoint_name="hpc-bridge-x", env_setup="true", interface="ib0",
                                 partition=kw.pop("partition", "shared"), account="acct", worker_init="true",
                                 partitions=tuple(partitions))
    real = SlurmFacility(dataclasses.replace(prof, **kw), cli=None)
    real.takes_resources = takes
    app = AppCtx(facility=_Sized(real), profile=Profile())
    _runners(app)
    return app, real


def _anvil_mep_entry(**over):
    return fake_mep_entry(id="anvil", account_required=True, defaults={
        "partition": "shared", "walltime": "00:15:00", "init_blocks": 1, "extra": {"cores_per_node": 1},
        "partitions": [SHARED.model_dump(), WHOLE.model_dump()]}, **over)


_ANVIL = {**ANVIL_SCHEMA, "properties": {**ANVIL_SCHEMA["properties"], "mem_per_node": {"type": "integer"},
                                         "exclusive": {"type": "boolean"}}}


# ---- 1. a facility endpoint's whole-node partition ----------------------------------------------------------------------

async def test_1_a_facility_endpoints_whole_node_partition_is_a_whole_node_and_a_slice_there_is_refused(monkeypatch):
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta(_ANVIL))
    app, _res = await _mep_app(monkeypatch, _anvil_mep_entry())
    r = await _ensure_endpoint_up(app, shape="compute", account="cis250223", partition="wholenode")
    b = r.block
    assert (b.whole_node, b.cores_per_node, b.size_source, b.max_charge) == (True, 128, "partition", 32.0)
    assert "1 whole node (128 cores)" in r.notice and "1 core" not in r.notice     # it read "1 core … 0.25 SU"
    r2 = await _ensure_endpoint_up(app, shape="compute", cores=8)
    assert "hands out whole nodes only" in r2.notice and "2 SU" not in r2.notice   # it was accepted at "2 SU"
    assert "cores=8 asks for a slice" in r2.notice                                  # in the user's own words
    assert "cores_per_node" not in app.compute_choice


async def test_1_a_facility_endpoint_claims_a_slice_only_when_exclusive_false_reaches_it(monkeypatch):
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta(_ANVIL))
    _app0, res = await _mep_app(monkeypatch, _anvil_mep_entry())
    assert res.block.whole_node is None and res.block.max_charge == 32.0           # costed as a whole node
    assert "parsl's is the whole node" in res.block.size_why
    entry = fake_mep_entry(id="anvil", account_required=True, defaults={
        "partition": "shared", "walltime": "00:15:00", "init_blocks": 1,
        "extra": {"cores_per_node": 1, "exclusive": False}, "partitions": [SHARED.model_dump(), WHOLE.model_dump()]})
    _app2, res2 = await _mep_app(monkeypatch, entry)                                # the entry sends exclusive: false
    # …to a template not read: nothing shows it renders it, so still the whole node (review round 2)
    assert (res2.block.whole_node, res2.block.cores_per_node, res2.block.max_charge) == (None, 1, 32.0)
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Templated(_ANVIL, ANVIL_TEMPLATE))
    _app3, res3 = await _mep_app(monkeypatch, entry)                                # …to one that renders it
    assert (res3.block.whole_node, res3.block.cores_per_node, res3.block.max_charge) == (False, 1, 0.25)


# ---- 2. size directives in scheduler_options ----------------------------------------------------------------------------

@pytest.mark.parametrize("line", [
    "#SBATCH --exclusive", "#SBATCH --mem=0", "#SBATCH --mem-per-cpu=4G", "#SBATCH -c 4", "#SBATCH --cpus-per-task=4",
    "#SBATCH -n 8", "#SBATCH --ntasks-per-node=4", "#SBATCH --gres=gpu:1", "#SBATCH --gpus-per-node=2",
    "#SBATCH -G 1", "#SBATCH --oversubscribe", "#SBATCH -s",
])
def test_2_a_slurm_size_directive_in_details_is_refused_pointing_at_the_size_arguments(line):
    assert scheduler_terms_kind(line) == "size"
    d = FacilityDetails(ssh_host="h", interface="ib0", env_setup="true", scratch_root="/s", partition="p",
                        scheduler_options=line)
    with pytest.raises(binding.TermsDirectiveError, match=r"ensure_endpoint_up\(cores=…, memory_gb=…, gpus=…"):
        binding._entry_from_details("byo", d)


@pytest.mark.parametrize("line", ["#PBS -l place=excl", "#PBS -l ncpus=8", "#PBS -l mem=32gb", "#PBS -l ngpus=1"])
def test_2_a_pbs_size_directive_is_refused(line):
    d = FacilityDetails(ssh_host="h", interface="hsn0", env_setup="true", scratch_root="/s", partition="q",
                        scheduler="pbs", scheduler_options=line)
    with pytest.raises(binding.TermsDirectiveError, match="the block's size"):
        binding._entry_from_details("byo", d)


def test_2_other_directives_stay_and_a_cached_size_line_is_offered_back_without_it():
    assert scheduler_terms_kind("#SBATCH --constraint=a100") is None
    assert scheduler_terms_kind("#SBATCH --gres=tmpfs:10") is None
    assert scheduler_terms_kind("#PBS -l filesystems=home:eagle") is None
    d = FacilityDetails(ssh_host="h", interface="ib0", env_setup="true", scratch_root="/s", partition="p",
                        scheduler_options="#SBATCH --gres=gpu:4\n#SBATCH --constraint=a100")
    kept, dropped = binding._accepted_details(d)
    assert kept.scheduler_options == "#SBATCH --constraint=a100"
    assert any("gpus=" in why for why in dropped)
    # the old advice pointed a GPU at `#SBATCH --gres=gpu:N`; it points at gpus= now
    assert "ensure_endpoint_up(gpus=4)" in binding._select_words("#PBS -l select=1:ngpus=4", "slurm")


def test_2_a_registry_ssh_entry_loses_a_size_line_and_a_facility_endpoint_keeps_its_own():
    e = fake_entry(id="ssh1", facility_key="k")
    ssh = CatalogEntry.model_validate({**e.model_dump(mode="json"), "compute": {
        **e.compute.model_dump(), "scheduler_options": "#SBATCH --exclusive\n#SBATCH --constraint=x"}})
    assert ssh.compute.scheduler_options == "#SBATCH --constraint=x"
    delta = fake_mep_entry(id="delta", defaults={"partition": "gpuA40x4", "extra": {
        "cores_per_node": 1, "scheduler_options": "#SBATCH --gpus-per-node=1"}})
    assert delta.defaults.extra["scheduler_options"] == "#SBATCH --gpus-per-node=1"
    assert defaults_extra_raw_problems(delta.model_dump(mode="json")) == []
    assert any("block's size" in p for p in defaults_extra_raw_problems(
        {**e.model_dump(mode="json"), "compute": {**e.compute.model_dump(), "scheduler_options": "#SBATCH -c 4"}}))


async def test_2_probe_a_an_exclusive_in_the_facilitys_options_is_a_whole_node():
    app, _real = _ssh(scheduler_options="#SBATCH --exclusive\n#SBATCH --mem=0")
    r = await _ensure_endpoint_up(app, shape="compute")
    assert r.block.whole_node is True and r.block.max_charge == 64.0                # it read "8 cores … 4 SU"
    assert "the facility's own scheduler_options ask for it" in r.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1].get("exclusive") is not False and "cores_per_node" not in app.built[-1]


async def test_2_probe_b_a_gpu_in_the_facilitys_options_is_counted_and_gpus_is_not_asked_twice():
    app, _real = _ssh(partitions=(GPU,), partition="gpu", scheduler_options="#SBATCH --gres=gpu:1")
    r = await _ensure_endpoint_up(app, shape="compute")
    assert r.block.gpus_per_node == 1 and r.block.max_charge == 20.0                 # it read 4 SU
    g = await _ensure_endpoint_up(app, shape="compute", gpus=2, confirm_spend=True)
    assert "already ask for GPUs ('#SBATCH --gres=gpu:1')" in g.notice and app.built == []


async def test_2_probe_delta_its_own_gpu_line_is_named_in_the_terms(monkeypatch):
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta(DELTA_SCHEMA))
    e = fake_mep_entry(id="delta", account_required=True, defaults={
        "partition": "gpuA40x4", "walltime": "00:15:00", "init_blocks": 1, "nodes_per_block": 1,
        "extra": {"cores_per_node": 1, "scheduler_options": "#SBATCH --gpus-per-node=1"}})
    app, _res = await _mep_app(monkeypatch, e)
    r = await _ensure_endpoint_up(app, shape="compute", account="bgta-delta-gpu")
    assert r.block.gpus_per_node == 1 and "1 GPU" in r.notice
    # 11: several nodes are not offered on a facility endpoint (its launcher places its own workers)
    n = await _ensure_endpoint_up(app, shape="compute", nodes=2)
    assert "places its own workers across a block's nodes" in n.notice and app.compute_choice.get("nodes_per_block") is None


def test_2_options_size_reads_the_directives():
    o = resources.options_size("#SBATCH --gres=gpu:a100:2\n#SBATCH --constraint=x")
    assert (o.gpus, o.whole, o.line) == (2, False, "#SBATCH --gres=gpu:a100:2")
    assert resources.options_size("#SBATCH --gres=tmpfs:10").gpus is None
    assert resources.options_size("#PBS -l place=scatter:excl").whole
    assert resources.options_size(None, "system=polaris:ngpus=4").gpus == 4


# ---- 3. terms read before the template was known ------------------------------------------------------------------------

@pytest.mark.parametrize("excl", [None, False])
async def test_3_a_size_read_again_after_the_template_is_known_is_asked_again(excl):
    app, real = _ssh(takes=None, exclusive=excl)
    q = await _ensure_endpoint_up(app, shape="compute")
    assert q.block.cores_per_node != 8                          # the question named parsl's whole node / one core

    async def adopt(_eid):
        real.takes_resources = True                             # what the bring-up learns

    app.facility.adopt_endpoint = adopt
    r = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert r.status == "needs_confirmation" and app.built == []             # the proposed 8 cores were sent unasked
    assert "the block's size was read again once this endpoint's template was known" in r.notice
    assert r.block.cores_per_node == 8 and r.block.size_source == "proposed"
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert up.status == "up" and app.built[-1]["cores_per_node"] == 8


@pytest.mark.parametrize("excl", [None, False])
async def test_3_the_size_the_question_named_is_the_one_a_later_confirm_covers(excl):
    # the reviewer's probe as written: the template becomes known BETWEEN the question and the confirm — the confirm
    # compares with what the question named (ShapeRuntime.question_size), not only with the size before its own adopt
    app, real = _ssh(takes=None, exclusive=excl)
    await _ensure_endpoint_up(app, shape="compute")
    real.takes_resources = True
    r = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert r.status == "needs_confirmation" and app.built == [] and r.block.cores_per_node == 8
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)   # a yes to the re-ask's size
    assert up.status == "up" and app.built[-1]["cores_per_node"] == 8
    # a confirming call that names its own terms confirms those (its receipt shows them): no re-ask
    app2, real2 = _ssh(takes=None, exclusive=excl)
    await _ensure_endpoint_up(app2, shape="compute")
    real2.takes_resources = True
    own = await _ensure_endpoint_up(app2, shape="compute", cores=4, confirm_spend=True)
    assert own.status == "up" and app2.built[-1]["cores_per_node"] == 4


# ---- 4. memory bills cores ----------------------------------------------------------------------------------------------

async def test_4_memory_past_the_per_core_share_bills_as_cores():
    app, _real = _ssh()
    r = await _ensure_endpoint_up(app, shape="compute", cores=1, memory_gb=200)
    assert r.block.max_charge == 54.5                                                  # it read 0.5 SU
    assert "200 GB at 1.85 GB per core counts as 109 cores" in r.notice
    capped = SHARED.model_copy(update={"max_mem_per_core_mb": 4000})                   # MaxMemPerCPU is the share
    s = resources.effective({"cores_per_node": 1, "mem_per_node": 200, "nodes_per_block": 1}, capped, channel="ssh")
    assert resources.billed_cores(s, capped)[0] == 52
    noshare = SHARED.model_copy(update={"mem_per_core_mb": None})
    s2 = resources.effective({"cores_per_node": 1, "mem_per_node": 200, "nodes_per_block": 1}, noshare,
                             channel="ssh")
    assert "memory beyond the cores' share may bill too" in resources.charge(s2, noshare, 1800)[1]
    # a memory request that counts as more cores than a node has never starts (MaxMemPerCPU raises the CPUs)
    big = SHARED.model_copy(update={"max_mem_per_core_mb": 1000})
    assert "counts as 205 cores" in resources.refusal({"cores_per_node": 1, "mem_per_node": 200}, big,
                                                      named={"mem_per_node"})


# ---- 5. classification on weak evidence ---------------------------------------------------------------------------------

def test_5_per_core_only_on_evidence():
    def one(params, detail, name="batch"):
        return resources.classify_slurm(select_type="select/cons_tres", select_params=params,
                                        shapes=[f"{name}|128|256000|(null)"], details=[detail], qos=[])[0]
    assert one("CR_Core", "PartitionName=batch OverSubscribe=NO").kind is None
    assert one("CR_Core", "PartitionName=batch OverSubscribe=YES:4").kind == "per_core"
    assert one("CR_Core_Memory", "PartitionName=batch DefMemPerCPU=1000").kind == "per_core"
    sock = one("CR_Socket_Memory", "PartitionName=batch DefMemPerCPU=1000")
    assert sock.kind is None and "whole sockets" in sock.why
    nx = one("CR_Core", "PartitionName=nonexclusive DefMemPerCPU=1000", name="nonexclusive")
    assert nx.kind == "per_core"                                                      # not whole by its name
    assert one("CR_Core", "PartitionName=gpu-exclusive", name="gpu-exclusive").kind == "whole_node"
    twins = resources.classify_slurm(select_type="select/cons_tres", select_params="CR_Core", shapes=[], qos=[],
                                     details=["PartitionName=wholenode QoS=part-standard OverSubscribe=EXCLUSIVE",
                                              "PartitionName=standard QoS=part-standard DefMemPerCPU=1896"])
    assert twins[1].kind is None and "shares QoS 'part-standard' with whole-node partition 'wholenode'" in twins[1].why


# ---- 6. the same block in other words is no switch ----------------------------------------------------------------------

async def test_6_naming_what_the_block_already_holds_is_no_term_switch():
    app = _app()
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert up.status == "up" and up.block.cores_per_node == 8 and len(app.built) == 1
    same = await _ensure_endpoint_up(app, shape="compute", cores=8)
    assert same.status == "up" and app.shapes["compute"].spend_confirmed and len(app.built) == 1
    assert app.compute_choice["cores_per_node"] == 8                 # the session's choice now
    w = _app()
    await _ensure_endpoint_up(w, shape="compute", partition="wholenode", confirm_spend=True)
    again = await _ensure_endpoint_up(w, shape="compute", whole_node=True)
    assert again.status == "up" and w.shapes["compute"].spend_confirmed and len(w.built) == 1


# ---- 7./8. weak evidence reads as a whole node ---------------------------------------------------------------------------

async def test_7_an_unknown_kind_partition_is_parsls_whole_node_with_its_cost():
    unknown = PartitionShape(name="shared", cores=128, memory_gb=251.4, billing={"cpu": 1.0})
    app, _real = _ssh(partitions=(unknown,))
    r = await _ensure_endpoint_up(app, shape="compute")
    assert r.block.whole_node is True and r.block.max_charge == 64.0
    assert "1 whole node (parsl's default: exclusive; 128 cores)" in r.notice


# ---- 9. partition limits ------------------------------------------------------------------------------------------------

def test_9_partition_limits_are_parsed_and_refused():
    mx = resources.classify_slurm(select_type="select/cons_tres", select_params="CR_Core_Memory",
                                  shapes=["debug|128|256000|(null)", "mixed|64|256000|(null)", "mixed|64|256000|gpu:4"],
                                  details=["PartitionName=debug MaxNodes=2 MaxCPUsPerNode=64 MaxMemPerCPU=1896 "
                                           "DefMemPerCPU=1896 TotalNodes=10 OverSubscribe=NO",
                                           "PartitionName=mixed DefMemPerCPU=1000"], qos=[])
    d, m = mx
    assert (d.max_nodes_per_job, d.max_cores_per_node, d.max_mem_per_core_mb, d.nodes) == (2, 64, 1896, 10)
    assert "at most 2 nodes per job" in resources.refusal({"nodes_per_block": 4}, d, named={"nodes_per_block"})
    assert "MaxCPUsPerNode=64" in resources.refusal({"cores_per_node": 100}, d, named={"cores_per_node"})
    assert resources.default_slice_cores(d.model_copy(update={"max_cores_per_node": 4})) == 4
    # a mixed CPU/GPU partition: its smallest node has no GPU, its largest has 4 — a GPU request is fine
    assert (m.gpus, m.max_gpus_per_node) == (0, 4)
    assert resources.refusal({"gpus_per_node": 1}, m, named={"gpus_per_node"}) is None
    assert "(4 GPUs)" in resources.refusal({"gpus_per_node": 8}, m, named={"gpus_per_node"})


# ---- nits ---------------------------------------------------------------------------------------------------------------

async def test_nit_back_after_a_nodes_only_change_says_size_and_the_stop_keeps_the_size():
    app = _app()
    await _ensure_endpoint_up(app, shape="compute", nodes=2, confirm_spend=True)
    s = await _ensure_endpoint_up(app, shape="compute", nodes=1)
    assert "keeps the new partition, account and size for the next one" in s.notice
    b = await _ensure_endpoint_up(app, shape="compute", nodes=2)
    assert "a partition, account or size switch dropped the confirmation" in b.notice


async def test_nit_a_whole_gpu_node_counts_its_gpus_and_the_unit_is_the_sites():
    app, _real = _ssh(partitions=(GPU,), partition="gpu")
    w = await _ensure_endpoint_up(app, shape="compute", whole_node=True)
    assert w.block.max_charge == 128.0 and "4 GPUs × 32" in w.block.charge_basis   # (128 + 4 × 32) × 0.5 h
    assert w.block.charge_unit == "billing units"
    app.charge_unit = "SU"
    w2 = await _ensure_endpoint_up(app, shape="compute")
    assert w2.block.charge_unit == "SU" and " SU: " in w2.notice
