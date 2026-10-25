"""Resource requests, review round 2 (2026-10-24): the terms still never understate what a block can bill. Each test is a
reviewer's probe (scratchpad rv-resources-r2/probes) turned into an assertion — red on 7fda1f7, green after:

N1. `exclusive: false` is a slice only on a partition known to hand one out (per core): elsewhere — Anvil's `wide`,
    `gpu`, `highmem`, `standard`, Expanse's `gpu`, `debug`, `large-shared` — the partition's own default, costed as a
    whole node where its shape is known; so is a slice asked there (cores=), which the wire still asks for;
N2. a facility endpoint claims a slice only where its published template renders `exclusive` (a schema listing the key
    is not enough; with no template read, the terms keep the whole node), and Anvil's seed sends no `exclusive: false`;
N3. `--exclude=<nodes>` and `--mem-bind=` are no size directives (and every seed still loads as written);
N4. --core-spec/-S, --thread-spec, --mincpus and --tres-per-task are; a facility's own --core-spec is a whole node;
N5. no cost where the node's cores, memory or GPUs a billing weight needs are not known;
N6. a re-ask's "read again" note goes once the size (or the partition, or the account) changes;
and the nits: a facility's whole-node partition is sent exclusive: true and no slice's cores; "counted as a whole node"
only beside a count; -c / ncpus= anchored; a bare --gres=gpu and --gpus-per-task counted; a select line's place= no bar
to its advice."""
import json
from pathlib import Path

import pytest

from hpc_bridge import binding, discovery, resources
from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.catalog.entry import CatalogEntry, scheduler_terms_kind, select_advice
from hpc_bridge.catalog.ingest import ingest
from hpc_bridge.facility import remote
from hpc_bridge.facility.remote import SlurmFacility, profile_from_catalog_entry
from hpc_bridge.models import FacilityDetails, PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx, _ensure_endpoint_up
from tests.fakes import fake_mep_entry
from tests.test_mep_server import ANVIL_TEMPLATE, DELTA_SCHEMA, _Meta, _Templated
from tests.test_resource_requests import _Sized
from tests.test_resource_review_r1 import _ssh
from tests.test_spend_terms import _mep_app, _runners

SEEDS = Path(remote.__file__).resolve().parents[1] / "catalog" / "seed"
FIXTURES = Path(__file__).resolve().parent / "resource_fixtures"
# Anvil's published schema (strict), as live on 2026-10-24
ANVIL = {"type": "object", "additionalProperties": False, "properties": {k: {"type": "string"} for k in (
    "account", "partition", "qos", "walltime", "cores_per_node", "mem_per_node", "constraint", "exclusive",
    "min_blocks", "max_blocks", "init_blocks", "nodes_per_block", "scheduler_options", "resource_specification",
    "worker_init", "endpoint_setup", "venvs_dir", "requirements")}}
CORES, NODES = resources.CORES, resources.NODES


def _seed(fname: str, eid: str) -> CatalogEntry:
    return next(e for e in BundledCatalog(SEEDS / fname).entries() if e.id == eid)


def _anvil(monkeypatch, template: str | None = ANVIL_TEMPLATE):
    meta = (lambda: _Templated(ANVIL, template, version="4.16.0")) if template is not None else (
        lambda: _Meta(ANVIL, version="4.16.0"))
    monkeypatch.setattr("tests.test_spend_terms._Status", meta)
    return _mep_app(monkeypatch, _seed("anvil.yaml", "anvil"))


def _expanse():
    prof = profile_from_catalog_entry(_seed("sdsc-expanse.yaml", "expanse"), user="u", account="chi152")
    real = SlurmFacility(prof, cli=None)
    real.takes_resources = True
    app = AppCtx(facility=_Sized(real), profile=Profile())
    _runners(app)
    return app


# ---- N1. exclusive: false is a slice only on a per-core partition ------------------------------------------------------

@pytest.mark.parametrize("part", ["wide", "standard", "gpu", "highmem"])
async def test_n1_anvils_seed_claims_no_slice_on_a_partition_it_does_not_describe(monkeypatch, part):
    app, _res = await _anvil(monkeypatch)
    r = await _ensure_endpoint_up(app, shape="compute", account="cis250223", partition=part)
    assert r.block.whole_node is None and "1 node × 1 core" not in r.notice      # it read "1 node × 1 core"
    assert "parsl's is the whole node" in r.block.size_why
    # a slice asked there: sent (the template renders it), not claimed — the partition's own sharing decides
    c = await _ensure_endpoint_up(app, shape="compute", cores=8)
    assert (c.block.whole_node, c.block.cores_per_node) == (None, 8)
    assert "1 node, 8 cores asked (not known to share its nodes" in c.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert (app.built[-1]["cores_per_node"], app.built[-1]["exclusive"]) == (8, False)


async def test_n1_a_slice_of_anvils_per_core_partition_is_claimed_where_the_template_renders_it(monkeypatch):
    app, res = await _anvil(monkeypatch)
    # nothing asked: no exclusive is sent, and the template's own default may be parsl's whole node — costed as one
    assert (res.block.queue, res.block.whole_node, res.block.max_charge) == ("shared", None, 32.0)
    ok = await _ensure_endpoint_up(app, shape="compute", account="cis250223", cores=8, memory_gb=16)
    assert (ok.block.whole_node, ok.block.cores_per_node, ok.block.max_charge) == (False, 8, 2.25)
    assert "16 GB at 1.85 GB per core counts as 9 cores" in ok.notice


async def test_n1_expanses_exclusive_false_is_no_slice_where_the_seed_does_not_describe_the_partition():
    app = _expanse()
    for part in ("gpu", "debug", "large-shared"):
        r = await _ensure_endpoint_up(app, shape="compute", partition=part)
        assert r.block.whole_node is None, part                          # it read "a slice of the scheduler's default"
        assert "a slice of the scheduler's default size" not in r.notice
        assert ("1 node (not known to share its nodes: the partition decides, and it may hold — and bill — the whole "
                "node)") in r.notice
    await _ensure_endpoint_up(app, shape="compute", partition="gpu", confirm_spend=True)
    assert app.built[-1]["exclusive"] is False      # the registry's default still reaches the template, as before
    shared = await _ensure_endpoint_up(app, shape="compute", partition="shared")
    assert (shared.block.whole_node, shared.block.size_source) == (False, "proposed")   # per core: the slice stands


async def test_n1_a_slice_asked_on_a_partition_not_known_to_share_is_costed_as_the_whole_node():
    # Anvil's `standard` (bring-your-own, from the live probe): OverSubscribe=NO with no DefMemPerCPU, the QoS of the
    # whole-node `wholenode` — kind not known. cores=8 there bills 128 cores if the site makes it whole: never "8 × 0.5"
    draft, _notes = discovery.parse_probe((FIXTURES / "anvil-probe.txt").read_text(), ssh_host="anvil.example.edu")
    prof = remote.MachineProfile(name="x", endpoint_name="hpc-bridge-x", env_setup="true", interface="ib0",
                                 partition="standard", account="acct", worker_init="true",
                                 partitions=tuple(draft.partitions))
    real = SlurmFacility(prof, cli=None)
    real.takes_resources = True
    app = AppCtx(facility=_Sized(real), profile=Profile())
    _runners(app)
    r = await _ensure_endpoint_up(app, shape="compute", cores=8)
    assert (r.block.whole_node, r.block.cores_per_node, r.block.max_charge) == (None, 8, 64.0)   # it said 4.0
    assert "a whole node's worth, the most it can be: 128 cores × 0.5 h" in r.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert (app.built[-1]["cores_per_node"], app.built[-1]["exclusive"]) == (8, False)   # the wire still asks it


# ---- N2. a facility template's own evidence --------------------------------------------------------------------------

def test_n2_anvils_seed_sends_no_exclusive():
    assert "exclusive" not in (_seed("anvil.yaml", "anvil").defaults.extra or {})


async def test_n2_a_schema_listing_exclusive_is_no_slice_without_its_template(monkeypatch):
    app, _res = await _anvil(monkeypatch, template=None)
    ok = await _ensure_endpoint_up(app, shape="compute", account="cis250223", cores=8)
    assert (ok.block.whole_node, ok.block.max_charge) == (None, 32.0)          # it read a slice at 2 SU
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert (app.built[-1]["cores_per_node"], app.built[-1]["exclusive"]) == (8, False)   # asked all the same


async def test_n2_a_template_that_renders_no_exclusive_is_never_sent_one(monkeypatch):
    tmpl = "provider:\n  cores_per_node: {{ cores_per_node|default(1) }}\n  exclusive: true\n"
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Templated(ANVIL, tmpl, version="4.16.0"))
    app, _res = await _mep_app(monkeypatch, _seed("anvil.yaml", "anvil"))
    c = await _ensure_endpoint_up(app, shape="compute", account="cis250223", cores=8)
    assert c.block.whole_node is None and c.block.max_charge == 32.0
    assert "no `exclusive` setting" in (await _ensure_endpoint_up(app, shape="compute", whole_node=True)).notice
    assert "no `mem_per_node` setting" in (await _ensure_endpoint_up(app, shape="compute", memory_gb=8)).notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert "exclusive" not in app.built[-1]


def test_n2_takes_size_key_reads_the_template_over_the_schema_and_the_entry():
    from hpc_bridge.facility.mep import MEPFacility

    fac = MEPFacility.from_entry(_seed("anvil.yaml", "anvil"))
    fac.schema = ANVIL
    assert fac.takes_size_key("exclusive") and fac.takes_size_key("cores_per_node")          # no template read
    assert not fac.takes_size_key("gpus_per_node")                                           # not in the schema
    fac.template_text = "cores_per_node: {{ cores_per_node }}\nexclusive_x: {{ exclusive_x }}\n"
    assert fac.takes_size_key("cores_per_node") and not fac.takes_size_key("exclusive")
    assert not fac.takes_size_key("mem_per_node")
    fac.schema = DELTA_SCHEMA            # a permissive schema: the entry sends cores_per_node, the template decides
    fac.template_text = "mem_per_node: {{ mem_per_node }}\n"
    assert not fac.takes_size_key("cores_per_node") and fac.takes_size_key("mem_per_node")


# ---- N3. --exclude and --mem-bind are no sizes --------------------------------------------------------------------------

@pytest.mark.parametrize("line", ["#SBATCH --exclude=gpu007", "#SBATCH -x gpu007", "#SBATCH --mem-bind=local",
                                  "#SBATCH --constraint=a100", "#SBATCH --gpu-bind=closest", "#SBATCH --qos=standby"])
def test_n3_an_exclude_or_a_mem_bind_is_no_size(line):
    assert scheduler_terms_kind(line) is None
    assert resources.options_size(line) == resources.OptionsSize()
    d = FacilityDetails(ssh_host="h", interface="ib0", env_setup="true", scratch_root="/s", partition="p",
                        scheduler_options=line)
    assert binding._entry_from_details("byo", d).compute.scheduler_options == line     # it was refused


@pytest.mark.parametrize("line", ["#SBATCH --exclusive", "#SBATCH --exclus", "#SBATCH --exclusive=user",
                                  "#SBATCH --mem=4G", "#SBATCH --mem 4G", "#SBATCH --mem-per-cpu=2G",
                                  "#SBATCH --mem-per-gpu=8G"])
def test_n3_the_exclusive_and_memory_sizes_still_are(line):
    assert scheduler_terms_kind(line) == "size"


async def test_n3_a_facilitys_exclude_line_is_no_whole_node(monkeypatch):
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta({"type": "object",
                                                                           "additionalProperties": True,
                                                                           "properties": {}}))
    e = fake_mep_entry(id="delta", account_required=True, defaults={
        "partition": "gpuA40x4", "walltime": "00:15:00", "init_blocks": 1, "nodes_per_block": 1,
        "extra": {"cores_per_node": 1, "scheduler_options": "#SBATCH --exclude=gpub001\n#SBATCH --gpus-per-node=1"}})
    _app, res = await _mep_app(monkeypatch, e)
    assert res.block.whole_node is None and res.block.gpus_per_node == 1    # it read "the facility's options ask"
    assert "#SBATCH --exclude=gpub001" in _app.facility._user_opts["scheduler_options"]


@pytest.mark.parametrize("fname", sorted(p.name for p in SEEDS.glob("*.yaml")))
def test_n3_every_seed_loads_and_ingests_as_written(fname):
    import yaml

    raw = {row["id"]: row for row in yaml.safe_load((SEEDS / fname).read_text())}
    for entry in BundledCatalog(SEEDS / fname).entries():
        row = raw[entry.id]
        for opts, got in ((row.get("compute", {}).get("scheduler_options"), entry.compute.scheduler_options),
                          (((row.get("defaults") or {}).get("extra") or {}).get("scheduler_options"),
                           (entry.defaults.extra or {}).get("scheduler_options"))):
            if isinstance(opts, str):    # nothing stripped: no seed line reads as a size or a term
                assert [ln.strip() for ln in got.splitlines()] == [ln.strip() for ln in opts.splitlines()]
        # the cached-entry path (catalog.search's cache: the entry's JSON) reads it back the same
        assert CatalogEntry.model_validate(json.loads(entry.model_dump_json())) == entry

    class _Client:
        def ingest(self, _index, _doc):
            return None

    assert ingest("idx", SEEDS / fname, _Client()) >= 1


# ---- N4. core / thread specialisation, --mincpus, --tres-per-task ----------------------------------------------------

@pytest.mark.parametrize("line", ["#SBATCH --core-spec=2", "#SBATCH -S 2", "#SBATCH -S2", "#SBATCH --thread-spec=1",
                                  "#SBATCH --mincpus=64", "#SBATCH --tres-per-task=cpu:16"])
def test_n4_core_and_thread_specialisation_mincpus_and_tres_per_task_are_sizes(line):
    assert scheduler_terms_kind(line) == "size"
    d = FacilityDetails(ssh_host="h", interface="ib0", env_setup="true", scratch_root="/s", partition="p",
                        scheduler_options=line)
    with pytest.raises(binding.TermsDirectiveError, match="sets the block's size"):
        binding._entry_from_details("byo", d)


@pytest.mark.parametrize("line", ["#SBATCH --core-spec=2", "#SBATCH -S 2", "#SBATCH --thread-spec=1"])
def test_n4_a_specialisation_holds_the_whole_node(line):
    assert resources.options_size(line).whole is True   # Slurm: a specialized core count implies --exclusive


async def test_n4_a_facilitys_own_core_spec_is_a_whole_node():
    app, _real = _ssh(scheduler_options="#SBATCH --core-spec=2")
    r = await _ensure_endpoint_up(app, shape="compute")
    assert (r.block.whole_node, r.block.cores_per_node) == (True, 128)                # it read "1 node × 8 cores"
    assert "the facility's own scheduler_options ask for it" in r.notice


# ---- N5. no cost on a node of unknown size ----------------------------------------------------------------------------

def test_n5_no_cost_where_the_node_a_weight_needs_is_not_known():
    unknown = PartitionShape(name="p", billing={"cpu": 1.0})
    mep = resources.effective({CORES: 1, NODES: 1}, unknown, channel="mep")
    assert (mep.whole_node, resources.charge(mep, unknown, 1800)) == (None, None)     # it said 1 core × 0.5 h
    whole = PartitionShape(name="w", kind="whole_node", billing={"cpu": 1.0})
    ws = resources.effective({NODES: 1, "scheduler_options": "#SBATCH -c 4"}, whole, channel="mep")
    assert (ws.whole_node, ws.cores, resources.charge(ws, whole, 1800)) == (True, None, None)   # it said 4 cores
    gpu = PartitionShape(name="g", kind="whole_node", cores=64, billing={"cpu": 1.0, "gpu": 16.0})
    gs = resources.effective({NODES: 1}, gpu, channel="ssh")
    assert resources.charge(gs, gpu, 1800) is None                                    # it dropped the GPU weight
    mem = PartitionShape(name="m", kind="whole_node", cores=64, billing={"cpu": 1.0, "mem_gb": 0.25})
    assert resources.charge(resources.effective({NODES: 1}, mem, channel="ssh"), mem, 1800) is None
    known = PartitionShape(name="k", kind="whole_node", cores=64, gpus=4, memory_gb=500,
                           billing={"cpu": 1.0, "gpu": 16.0})
    assert resources.charge(resources.effective({NODES: 1}, known, channel="ssh"), known, 1800) == (
        64.0, "(64 cores + 4 GPUs × 16) × 0.5 h")
    cpu_only = PartitionShape(name="c", kind="whole_node", cores=64, gpus=0, billing={"cpu": 1.0, "gpu": 16.0})
    assert resources.charge(resources.effective({NODES: 1}, cpu_only, channel="ssh"), cpu_only, 1800)[0] == 32.0


# ---- N6. the re-ask's note goes with a change ---------------------------------------------------------------------------

async def test_n6_a_change_after_a_re_ask_drops_its_read_again_note():
    app, real = _ssh(takes=None)
    await _ensure_endpoint_up(app, shape="compute")

    async def adopt(_eid):
        real.takes_resources = True

    app.facility.adopt_endpoint = adopt
    again = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert again.status == "needs_confirmation" and "read again once this endpoint's template" in again.notice
    c = await _ensure_endpoint_up(app, shape="compute", cores=4)
    assert c.block.cores_per_node == 4 and "read again once this endpoint's template" not in c.notice   # it stayed
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert up.status == "up" and app.built[-1]["cores_per_node"] == 4


# ---- the nits --------------------------------------------------------------------------------------------------------

async def test_a_facilitys_whole_node_partition_is_sent_exclusive_and_no_slices_cores(monkeypatch):
    app, _res = await _anvil(monkeypatch)
    w = await _ensure_endpoint_up(app, shape="compute", account="cis250223", partition="wholenode",
                                  confirm_spend=True)
    assert w.block.whole_node is True
    sent = app.built[-1]
    assert sent["exclusive"] is True and "cores_per_node" not in sent and "mem_per_node" not in sent


async def test_counted_as_a_whole_node_only_beside_a_count(monkeypatch):
    app, res = await _anvil(monkeypatch)
    assert res.block.max_charge is not None and "counted as a whole node" in res.notice
    wide = await _ensure_endpoint_up(app, shape="compute", account="cis250223", partition="wide")
    assert wide.block.max_charge is None and "counted as a whole node" not in wide.notice
    assert "it may hold — and bill — the whole node" in wide.notice


@pytest.mark.parametrize(("line", "cores"), [("#SBATCH --mincpus=16", None), ("#SBATCH --job-name=x-c2", None),
                                             ("#SBATCH -c 4", 4), ("#SBATCH -c4", 4), ("#SBATCH --cpus-per-task=6", 6),
                                             ("#PBS -l select=1:ncpus=8", 8), ("#PBS -l ncpus=4,mem=8gb", 4),
                                             ("#PBS -lncpus=2", 2), ("#PBS -l mincpus=3", None)])
def test_cores_in_a_facilitys_options_are_read_anchored(line, cores):
    assert resources.options_size(line).cores == cores


@pytest.mark.parametrize(("line", "gpus"), [("#SBATCH --gres=gpu", 1), ("#SBATCH --gres=gpu:a100", 1),
                                            ("#SBATCH --gres=gpu:a100:2", 2), ("#SBATCH --gres=gpu:4", 4),
                                            ("#SBATCH --gpus-per-task=1", 1), ("#SBATCH --gpus-per-node=a100:3", 3),
                                            ("#SBATCH --gres=tmpfs:10", None), ("#SBATCH --gpu-bind=closest", None),
                                            ("#SBATCH -G 2", 2)])
def test_gpus_in_a_facilitys_options_are_counted(line, gpus):
    assert resources.options_size(line).gpus == gpus


def test_a_select_line_with_a_place_keeps_its_advice():
    line = "#PBS -l select=1:ncpus=4:ngpus=1,place=scatter"
    assert select_advice(line) == ("ngpus=1", 4, None)                                # it gave no advice
    assert select_advice("#PBS -l select=1:ngpus=4,place=excl,filesystems=home") == (
        "ngpus=4", None, "#PBS -l filesystems=home")
    words = binding._select_words(line, "pbs")
    assert "select_options='ngpus=1' and cpus_per_node=4" in words and "leaving its place= out" in words

