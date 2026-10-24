"""Resource requests through ensure_endpoint_up (D1–D3, vault `Planned/Resource requests.md`): the spend question
proposes a slice on a per-core partition, a whole node on a whole-node one; cores=/memory_gb=/gpus=/nodes=/whole_node=
persist for the session and a change is a term switch (spend is asked again, the old block handled as a partition
switch handles it); a request the partition cannot give, or the facility's template does not take, is refused before
anything is submitted, saying why; the config a runner is built with carries the size as the scheduler's template
renders it. On a facility endpoint only its own template's keys size a block."""
import dataclasses

from hpc_bridge import connect, warmth
from hpc_bridge.catalog.entry import CatalogEntry
from hpc_bridge.facility import remote
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.models import FacilityDetails, PartitionShape, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.resources import CORES, MEM, NODES, WHOLE, size_key
from hpc_bridge.server import AppCtx, _ensure_endpoint_up
from tests.fakes import FakeFacility, fake_mep_entry
from tests.test_mep_server import ANVIL_SCHEMA, DELTA_SCHEMA, _Meta
from tests.test_spend_terms import _mep_app, _runners

SHARED = PartitionShape(name="shared", kind="per_core", why="SelectType=select/cons_tres, OverSubscribe=NO",
                        cores=128, memory_gb=251.4, mem_per_core_mb=1896, billing={"cpu": 1.0})
WHOLE_P = PartitionShape(name="wholenode", kind="whole_node", why="OverSubscribe=EXCLUSIVE", cores=128,
                         memory_gb=251.4, billing={"cpu": 1.0})
CAPPED = PartitionShape(name="capped", kind="per_core", cores=128, max_cores_per_job=127)


class _Sized(FakeFacility):
    """An SSH facility as the server sees one: it comes up warm (a fake), with a REAL SlurmFacility's config, its size
    mapping (dispatch_uec, block_size) and what discovery saw of its partitions."""

    def __init__(self, real: SlurmFacility) -> None:
        super().__init__()
        self.workers = 1
        self._real = real
        self.profile = real.profile
        self.account_required = real.account_required
        self.partitions = real.partitions
        self.scheduler = real.profile.scheduler

    @property
    def takes_resources(self):
        return self._real.takes_resources

    def config_template(self, profile):
        return self._real.config_template(profile)

    def dispatch_uec(self, uec):
        return self._real.dispatch_uec(uec)

    def block_size(self, uec):
        return self._real.block_size(uec)


def _app(*, takes: bool | None = True, partitions=(SHARED, WHOLE_P, CAPPED), **kw) -> AppCtx:
    prof = remote.MachineProfile(name="anvil-byo", endpoint_name="hpc-bridge-anvil", env_setup="true",
                                 interface="ib0", partition="shared", account="", worker_init="true",
                                 partitions=tuple(partitions))
    real = SlurmFacility(dataclasses.replace(prof, **kw), cli=None)
    real.takes_resources = takes
    app = AppCtx(facility=_Sized(real), profile=Profile())
    _runners(app)
    return app


def _compute(app: AppCtx) -> dict:
    return app.shapes["compute"].user_endpoint_config


# ---- D3: the spend question proposes a slice ------------------------------------------------------------------------

async def test_the_spend_question_proposes_a_slice_on_a_per_core_partition():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute")
    b = res.block
    assert res.status == "needs_confirmation" and app.built == []
    assert (b.whole_node, b.cores_per_node, b.memory_gb, b.memory_default, b.size_source) == (
        False, 8, 14.8, True, "proposed")
    assert (b.max_charge, b.charge_basis) == (4.0, "8 cores × 0.5 h")
    assert ("1 node × 8 cores, 15 GB (the partition's default per core) for up to 00:30:00 (at most 0.5 node-hours; "
            "at most 4 SU: 8 cores × 0.5 h)") in res.notice
    assert "hpc-bridge's proposal for this per-core partition, a slice and not the whole node" in res.notice
    assert "with cores=/memory_gb=/gpus=/nodes= in the same call" in res.notice
    # confirmed as proposed: the block asks for that slice — exclusive off, 8 cores, the partition's memory default
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert up.status == "up" and up.block.size_source == "proposed"
    sent = app.built[-1]
    assert (sent[WHOLE], sent[CORES]) == (False, 8) and MEM not in sent and sent[NODES] == 1


async def test_a_whole_node_partition_is_a_whole_node_and_a_slice_there_is_refused():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute", partition="wholenode")
    b = res.block
    assert res.status == "needs_confirmation"
    assert (b.whole_node, b.cores_per_node, b.size_source, b.max_charge) == (True, 128, "partition", 64.0)
    assert "1 whole node (128 cores)" in res.notice
    assert "Whole nodes: partition 'wholenode' hands out only whole nodes (OverSubscribe=EXCLUSIVE)" in res.notice
    # a slice there: refused, nothing set (the partition, the choice and the runner as they were)
    before = (dict(_compute(app)), dict(app.compute_choice))
    refused = await _ensure_endpoint_up(app, shape="compute", partition="wholenode", cores=8)
    assert "hands out whole nodes only" in refused.notice and "a per-core partition (shared, capped)" in refused.notice
    assert refused.status == "needs_confirmation"            # the standing state, never a `down`
    assert (dict(_compute(app)), dict(app.compute_choice)) == before and app.built == []


async def test_a_request_is_what_the_block_asks_for_and_it_persists_for_the_session():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute", cores=2, memory_gb=2, confirm_spend=True)
    b = res.block
    assert res.status == "up" and (b.cores_per_node, b.memory_gb, b.memory_default, b.size_source) == (
        2, 2.0, False, "this_call")
    sent = app.built[-1]
    assert (sent[WHOLE], sent[CORES], sent[MEM]) == (False, 2, 2)
    # a stop drops the shape; the next block is seeded with the session's size (as its partition and account)
    warmth._drop_compute_shape_locked(app)
    again = await _ensure_endpoint_up(app, shape="compute")
    assert again.status == "needs_confirmation" and again.block.size_source == "earlier_call"
    assert (_compute(app)[CORES], _compute(app)[MEM]) == (2, 2) and again.block.cores_per_node == 2


async def test_a_size_change_is_a_term_switch_and_the_terms_can_come_back():
    app = _app()
    await _ensure_endpoint_up(app, shape="compute", cores=2, confirm_spend=True)
    switched = await _ensure_endpoint_up(app, shape="compute", cores=4)
    assert switched.status == "needs_confirmation" and not app.shapes["compute"].spend_confirmed
    assert "the partition, account or size has changed since spend was confirmed" in switched.notice
    assert "the one on partition 'shared', nodes=1, cores=2" in switched.notice
    assert app.shapes["compute"].switched_terms == ("shared", "", size_key({NODES: 1, CORES: 2}))
    back = await _ensure_endpoint_up(app, shape="compute", cores=2)
    assert "the terms are back to the ones it covered (partition 'shared', nodes=1, cores=2)" in back.notice
    # changing and confirming in one call: its answer says what the confirmation is for (a receipt)
    moved = await _ensure_endpoint_up(app, shape="compute", cores=6, confirm_spend=True)
    assert moved.status == "up"
    assert moved.notice.startswith("This confirmation is for a new block: the partition, account or size changed "
                                   "from partition 'shared', nodes=1, cores=2.")
    # a partition switch alone keeps the old wording
    app2 = _app()
    await _ensure_endpoint_up(app2, shape="compute", confirm_spend=True)
    p = await _ensure_endpoint_up(app2, shape="compute", partition="capped")
    assert "the partition or account has changed since spend was confirmed" in p.notice


async def test_whole_node_true_clears_a_slice_and_conflicts_with_fewer_cores():
    app = _app()
    await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=16)
    whole = await _ensure_endpoint_up(app, shape="compute", whole_node=True)
    assert whole.block.whole_node is True and whole.block.cores_per_node == 128
    assert CORES not in _compute(app) and MEM not in _compute(app) and _compute(app)[WHOLE] is True
    assert app.compute_choice[CORES] is None                 # cleared for the session too: a stop does not bring it back
    clash = await _ensure_endpoint_up(app, shape="compute", whole_node=True, cores=8)
    assert "every core of a node (128" in clash.notice and _compute(app)[WHOLE] is True


async def test_a_request_over_a_qos_cap_or_a_node_is_refused_before_anything_is_submitted():
    app = _app()
    capped = await _ensure_endpoint_up(app, shape="compute", partition="capped", cores=128, confirm_spend=True)
    assert "over the QOS cap of partition 'capped': at most 127 cores per job" in capped.notice
    assert capped.status == "needs_confirmation" and app.built == [] and app.compute_choice == {}
    big = await _ensure_endpoint_up(app, shape="compute", memory_gb=512)
    assert "more than a node of partition 'shared' has (251.4 GB)" in big.notice
    bad = await _ensure_endpoint_up(app, shape="compute", cores=0)
    assert "invalid cores 0" in bad.notice and bad.status == "needs_confirmation"


async def test_an_endpoint_whose_template_predates_resource_requests_refuses_them_and_proposes_nothing():
    app = _app(takes=False)
    app.state.endpoint_id = "e-old"
    plain = await _ensure_endpoint_up(app, shape="compute")
    assert plain.block.whole_node is None and plain.block.size_source == "facility"   # its old size, said as before
    assert "1 node for up to 00:30:00 (at most 0.5 node-hours)." in plain.notice
    refused = await _ensure_endpoint_up(app, shape="compute", cores=4)
    assert "configured before hpc-bridge wrote a block's size into its template" in refused.notice
    assert "teardown_endpoint, then connect_facility" in refused.notice and CORES not in _compute(app)


async def test_a_size_asked_before_the_bring_up_met_an_older_endpoint_is_refused_before_any_check():
    # a startup pin: nothing is known of the template until the bring-up adopts the endpoint it reaches — an older one
    app = _app(takes=None)
    fac = app.facility

    async def adopt(eid):
        fac._real.takes_resources = False

    fac.adopt_endpoint = adopt
    assert (await _ensure_endpoint_up(app, shape="compute", cores=4)).status == "needs_confirmation"   # not yet known
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "down" and "configured before hpc-bridge wrote a block's size" in res.notice
    assert "(cores)" in res.notice and app.built == [] and not app.shapes["compute"].spend_confirmed
    # whole nodes are what an older template asks for anyway: not refused
    assert (await _ensure_endpoint_up(app, shape="compute", whole_node=True)).status == "needs_confirmation"


async def test_several_nodes_are_one_shell_on_the_first_node():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute", nodes=2, cores=4)
    assert "2 nodes × 4 cores, 7.4 GB (the partition's default per core)" in res.notice
    assert "2 nodes: one shell, on the first node — run work across them with srun (or mpiexec)" in res.notice
    assert "nodes you do not use still bill" in res.notice and res.block.max_charge == 4.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1][NODES] == 2 and app.built[-1][CORES] == 4


async def test_the_login_shape_has_no_size_and_a_live_command_blocks_a_size_change():
    app = _app()
    login = await _ensure_endpoint_up(app, shape="login", cores=4)
    assert "with no block to size; ignored cores" in login.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    app.shapes["compute"].inflight = 1
    busy = await _ensure_endpoint_up(app, shape="compute", cores=4)
    assert "can't change the block's size (cores=4)" in busy.notice and CORES not in _compute(app)


async def test_a_rejected_block_then_a_new_size_is_not_the_same_terms_again():
    app = _app()
    await _ensure_endpoint_up(app, shape="compute", cores=2)
    rt = app.shapes["compute"]
    rt.rejected, rt.rejected_size = ("sbatch: error: QOSMaxCpuPerJobLimit", "shared", ""), size_key(_compute(app))
    same = await _ensure_endpoint_up(app, shape="compute")
    assert "Confirming the same terms again repeats a request the scheduler refuses" in same.notice
    changed = await _ensure_endpoint_up(app, shape="compute", cores=1)
    assert "The partition, account or size has changed since" in changed.notice


async def test_pbs_a_slice_of_a_queue_and_its_select():
    workq = PartitionShape(name="workq", kind="per_core", cores=64, memory_gb=256.0, gpus=4, place="free:shared")
    app = _app(partitions=(workq,), scheduler="pbs", partition="workq", cpus_per_node=64)
    res = await _ensure_endpoint_up(app, shape="compute", gpus=1)
    assert "queue 'workq'" in res.notice and "1 node × 8 cores, 32 GB, 1 GPU" in res.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    sent = app.built[-1]
    assert sent["cpus_per_node"] == 8 and sent["select_options"] == "mem=32gb:ngpus=1"
    assert sent["scheduler_options"].endswith("#PBS -l place=free:shared")


# ---- connect: the next block's terms ask about the slice --------------------------------------------------------------

async def test_connect_asks_whether_the_proposed_slice_is_enough(monkeypatch):
    app = _app()
    app.machine = "anvil-byo"
    terms = connect._next_block_terms(app)
    assert terms.size_source == "proposed" and terms.cores_per_node == 8
    note = connect._connect_terms_note(app, terms)
    assert "whether that slice is enough (or the cores, memory and GPUs the work needs)" in note
    assert "ensure_endpoint_up(account=…, partition=…, cores=…, confirm_spend=True)" in note


async def test_a_bring_your_own_proposal_carries_its_partitions_into_the_bound_facility(monkeypatch):
    # the probe's draft keeps what it saw of each partition server-side, as its env_setup: a details= that leaves it
    # out (as the proposal asks) still sizes the block by it
    from pathlib import Path

    from hpc_bridge import binding, discovery

    text = (Path(__file__).resolve().parent / "resource_fixtures" / "anvil-probe.txt").read_text()
    draft, _notes = discovery.parse_probe(text, ssh_host="anvil.example.edu")
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.proposed_drafts["anvil-byo"] = draft
    seen = {}

    def build(entry, *, account):
        seen["entry"] = entry
        return FakeFacility()

    monkeypatch.setattr(binding, "_facility_from_entry", build)

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    confirmed = FacilityDetails(**{k: v for k, v in draft.model_dump().items() if k not in ("env_setup", "partitions")})
    await connect._connect_facility(app, "anvil-byo", details=confirmed, run_login=run_login)
    parts = {p.name: p for p in seen["entry"].defaults.partitions}
    assert parts["shared"].kind == "per_core" and parts["wholenode"].kind == "whole_node"


# ---- a facility endpoint: only its own template's keys size a block ----------------------------------------------------

def _anvil_like(**over) -> CatalogEntry:
    return fake_mep_entry(id="anvil", account_required=True, defaults={
        "partition": "shared", "walltime": "00:15:00", "init_blocks": 1, "extra": {"cores_per_node": 1},
        "partitions": [SHARED.model_dump()]}, **over)


async def test_a_facility_endpoint_takes_the_size_its_strict_schema_lists(monkeypatch):
    schema = {**ANVIL_SCHEMA, "properties": {**ANVIL_SCHEMA["properties"], "mem_per_node": {"type": "integer"},
                                             "exclusive": {"type": "boolean"}}}
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta(schema))
    app, res = await _mep_app(monkeypatch, _anvil_like())
    assert res.block.cores_per_node == 1 and res.block.memory_gb == 1.9 and res.block.size_source == "facility"
    ok = await _ensure_endpoint_up(app, shape="compute", account="cis250223", cores=8, memory_gb=16)
    assert ok.block.cores_per_node == 8 and ok.block.size_source == "this_call" and ok.block.max_charge == 2.0
    assert "8 cores × 0.25 h" in ok.notice
    gpus = await _ensure_endpoint_up(app, shape="compute", gpus=1)
    assert "this facility's endpoint sets the block size: its published template has no `gpus_per_node` setting" in (
        gpus.notice)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    sent = app.built[-1]
    assert (sent["cores_per_node"], sent["mem_per_node"]) == (8, 16) and "gpus_per_node" not in sent
    whole = await _ensure_endpoint_up(app, shape="compute", whole_node=True)
    assert whole.block.whole_node is True and "cores_per_node" not in _compute(app)


async def test_a_permissive_schema_takes_what_its_template_renders_or_the_entry_sends(monkeypatch):
    class _Rendered(_Meta):
        def get_endpoint_metadata(self, eid):
            return {**super().get_endpoint_metadata(eid),
                    "user_config_template": "provider:\n  mem_per_node: {{ mem_per_node | default(none) }}\n"}

    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Rendered(DELTA_SCHEMA))
    app, _ = await _mep_app(monkeypatch, _anvil_like())
    assert (await _ensure_endpoint_up(app, shape="compute", account="x", memory_gb=8)).block.memory_gb == 8.0
    assert (await _ensure_endpoint_up(app, shape="compute", cores=2)).block.cores_per_node == 2   # the entry sends it
    refused = await _ensure_endpoint_up(app, shape="compute", whole_node=True)
    assert "no `exclusive` setting" in refused.notice


async def test_a_facility_key_map_renames_a_size_key_at_the_wire(monkeypatch):
    schema = {"type": "object", "additionalProperties": False,
              "properties": {k: {} for k in ("ACCOUNT_ID", "partition", "walltime", "GPUS_PER_NODE", "worker_init",
                                             "nodes_per_block", "init_blocks", "max_blocks")}}
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Meta(schema))
    entry = fake_mep_entry(id="nesi", account_required=True,
                           compute={"scheduler": "slurm", "interface": "ib0", "env_setup": "true",
                                    "scratch_root": "$HOME/.hpc-bridge",
                                    "key_map": {"account": "ACCOUNT_ID", "gpus_per_node": "GPUS_PER_NODE"}})
    app, _ = await _mep_app(monkeypatch, entry)
    res = await _ensure_endpoint_up(app, shape="compute", account="nesi00001", gpus=2, confirm_spend=True)
    assert res.status == "up" and res.block.gpus_per_node == 2
    assert app.built[-1]["GPUS_PER_NODE"] == 2 and "gpus_per_node" not in app.built[-1]
    assert "cores=" not in str(await _ensure_endpoint_up(app, shape="compute"))   # untouched by an unasked key
    refused = await _ensure_endpoint_up(app, shape="compute", cores=4)
    assert "no `cores_per_node` setting" in refused.notice


def test_a_facility_endpoint_reads_its_partitions_from_the_registry_entry():
    fac = MEPFacility.from_entry(_anvil_like())
    assert [p.name for p in fac.partitions] == ["shared"] and fac.partitions[0].mem_per_core_mb == 1896


async def test_the_expanse_entry_proposes_a_slice_on_shared_and_a_whole_node_on_compute():
    # the registry carries the classification (no probe for a curated facility): `shared` per core with its QOS's
    # 127-CPU cap, `compute` whole nodes — and the entry's `exclusive: false` for shared is no slice request on compute
    from pathlib import Path

    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.facility.remote import profile_from_catalog_entry

    seeds = Path(remote.__file__).resolve().parents[1] / "catalog" / "seed"
    entry = next(e for e in BundledCatalog(seeds / "sdsc-expanse.yaml").entries() if e.id == "expanse")
    prof = profile_from_catalog_entry(entry, user="u", account="chi152")
    real = SlurmFacility(prof, cli=None)
    real.takes_resources = True
    app = AppCtx(facility=_Sized(real), profile=Profile())
    _runners(app)
    shared = await _ensure_endpoint_up(app, shape="compute")
    assert (shared.block.cores_per_node, shared.block.size_source, shared.block.whole_node) == (8, "proposed", False)
    over = await _ensure_endpoint_up(app, shape="compute", cores=128)
    assert "at most 127 cores per job" in over.notice and CORES not in _compute(app)
    compute = await _ensure_endpoint_up(app, shape="compute", partition="compute")
    assert compute.status == "needs_confirmation" and compute.block.whole_node is True
    assert compute.block.size_source == "partition" and "whole-node charging" in compute.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1][WHOLE] is True and CORES not in app.built[-1]
