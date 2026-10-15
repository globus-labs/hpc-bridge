"""The block's terms after review (spend terms, round 2): they show only what is sent, the question is asked once, and its
answer is the confirmation.

- `block` names only what the facility receives: a facility endpoint whose schema drops a key at the wire (NeSI's
  shape) reads as the facility's own default, and a partition or account it would drop is refused, not set. The notes
  fitting the config to its schema makes are said at connect.
- The receipt comes once: on the call that newly confirmed spend, not on a poll that confirms again.
- A bring-your-own `scheduler_options` may not set the account or the queue, and a registry entry's `defaults.extra`
  may not either (dropped on read, refused at ingest).
- A command refused on a replacement and polled after a switch names the new block's terms as the new block's.
- The account hint is plain text, quoted as the registry's words; control, format and bidi characters never reach the
  agent.
- The notices ask one question, only for what is unsaid (presets count as said), and say the user's yes is the spend
  confirmation; with no account where one is needed the spend question asks for it in the same breath.
"""
import pytest
import yaml

from hpc_bridge import binding, connect, notices, server
from hpc_bridge.catalog.entry import CatalogEntry
from hpc_bridge.catalog.ingest import ingest
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _poll_task, _run_shell
from tests.fakes import FakeCatalog, FakeFacility, fake_mep_entry
from tests.test_spend_terms import _mep_app, _ok, _runners, _ssh_connect, _ssh_entry

# NeSI's published template: strict, the account under its own name, no partition and no node count.
NESI_SCHEMA = {"additionalProperties": False, "required": ["ACCOUNT_ID"],
               "properties": {"ACCOUNT_ID": {}, "WALL_TIME": {}, "MEM_PER_CPU": {}, "GPUS_PER_NODE": {}}}


class _Meta:
    def __init__(self, schema):
        self.schema = schema

    def get_endpoint_status(self, eid):
        return {"status": "online"}

    def get_endpoint_metadata(self, eid):
        return {"user_config_schema": self.schema, "endpoint_version": "4.15.0", "display_name": "NeSI"}


def _nesi_entry():
    return fake_mep_entry(id="nesi", account_required=True, account_hint="your NeSI project code", compute={
        "scheduler": "slurm", "interface": "enP7s7", "env_setup": "uv pip install -q globus-compute-endpoint==4.15.0",
        "scratch_root": "$HOME/.hpc-bridge", "key_map": {"account": "ACCOUNT_ID", "walltime": "WALL_TIME"},
    }, defaults={"partition": "main", "walltime": "02:00:00", "init_blocks": 1, "nodes_per_block": 4})


async def _nesi(monkeypatch, schema=NESI_SCHEMA):
    entry = _nesi_entry()
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry",
                        lambda e, *, account: MEPFacility.from_entry(e, account=account or None,
                                                                     client_factory=lambda: _Meta(schema)))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app, _ok())
    res = await server._connect_facility(app, entry.id)
    assert res.phase == "needs_account", res
    return app, res


# --- P1: only what is sent ------------------------------------------------------------------------------------------


async def test_a_strict_schema_names_only_what_it_receives(monkeypatch):
    app, res = await _nesi(monkeypatch)
    b = res.block
    # no partition and no node count reach NeSI: its template decides; the walltime goes as WALL_TIME
    assert (b.queue, b.queue_source, b.nodes, b.walltime, b.max_node_hours) == (
        None, "facility_template", None, "02:00:00", None)
    assert "the facility's default partition (hpc-bridge sends none)" in res.notice
    assert "the facility's default size for up to 02:00:00" in res.notice
    # the keys fitting the config dropped are said at connect, before any spend
    assert "keys not in the facility's template schema were dropped" in res.notice
    up = await _ensure_endpoint_up(app, shape="compute", account="nesi99999", confirm_spend=True)
    assert up.status == "up" and (up.block.account, up.block.queue, up.block.nodes) == ("nesi99999", None, None)
    assert app.built[-1].get("ACCOUNT_ID") == "nesi99999" and "partition" not in app.built[-1]   # what was sent


async def test_a_partition_the_facility_would_drop_is_refused_not_set(monkeypatch):
    app, _ = await _nesi(monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute", partition="gpu", account="nesi99999", confirm_spend=True)
    assert res.status == "down" and res.block_state == "cold"
    assert "this facility's endpoint takes no partition" in res.notice and "leave partition= out" in res.notice
    assert app.compute_choice == {} and app.built == [] and not app.shapes.get("compute", None)


async def test_a_value_the_wire_drops_reads_as_the_facilitys_default():
    # the schema learned after a runtime was seeded with a partition: the block still says only what is sent
    fac = MEPFacility.from_entry(_nesi_entry(), client_factory=lambda: _Meta(NESI_SCHEMA))
    fac.schema = NESI_SCHEMA
    fac.key_map = {"account": "ACCOUNT_ID", "walltime": "WALL_TIME"}
    app = AppCtx(facility=fac, profile=Profile())
    from hpc_bridge.context import ShapeRuntime
    rt = ShapeRuntime(user_endpoint_config={"compute": True, "partition": "gpu", "account": "p1", "walltime": "01:00:00",
                                            "nodes_per_block": 4})
    app.compute_choice["partition"] = "gpu"
    t = notices._block_terms(app, rt, named={"partition"})
    assert (t.queue, t.queue_source, t.account, t.walltime, t.nodes) == (None, "facility_template", "p1", "01:00:00",
                                                                         None)


# --- P2: one receipt ------------------------------------------------------------------------------------------------


async def test_the_receipt_comes_once_per_confirmation(monkeypatch):
    app, _ = await _mep_app(monkeypatch, canary=_ok())
    first = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    poll = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert first.block is not None and poll.block is None


# --- P3: no account or queue behind the terms ------------------------------------------------------------------------


_DRAFT = dict(ssh_host="login.example.edu", interface="ib0", env_setup="module load x", scratch_root="/scratch/{user}",
              partition="batch")


@pytest.mark.parametrize("opts", ["#SBATCH --account=secretproj", "#SBATCH -A secretproj", "#SBATCH -Asecretproj",
                                  "#SBATCH --partition=gpu", "#SBATCH -p gpu", "#PBS -A proj", "#PBS -q prod",
                                  "#SBATCH --gpus-per-node=1\n#SBATCH --account secretproj"])
def test_byo_scheduler_options_may_not_set_the_account_or_queue(opts):
    with pytest.raises(ValueError, match="sets the account or the partition/queue"):
        binding._entry_from_details("mine", FacilityDetails(**_DRAFT, scheduler_options=opts))


@pytest.mark.parametrize("opts", ["#SBATCH -q normal", "#PBS -l filesystems=home:eagle", "#SBATCH --gpus-per-node=1",
                                  "#SBATCH --parsable"])
def test_other_directives_pass(opts):
    assert binding._entry_from_details("mine", FacilityDetails(**_DRAFT, scheduler_options=opts))


async def test_a_details_connect_with_an_account_directive_fails_and_says_why(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    app = AppCtx(facility=FakeFacility(), profile=Profile())

    async def run_login(cmd):
        raise AssertionError("nothing runs")

    res = await connect._connect_facility(
        app, "mine", details=FacilityDetails(**_DRAFT, scheduler_options="#SBATCH --account=secretproj"),
        run_login=run_login)
    assert res.phase == "failed" and "ensure_endpoint_up(account=…)" in res.notice


def test_defaults_extra_never_carries_the_account_or_partition():
    raw = fake_mep_entry().model_dump(mode="json")
    raw["compute"]["key_map"] = {"partition": "queue"}
    raw["defaults"]["extra"] = {"account": "x", "partition": "y", "queue": "z", "cores_per_node": 1}
    assert CatalogEntry.model_validate(raw).defaults.extra == {"cores_per_node": 1}   # read leniently: dropped


def test_ingest_refuses_terms_in_defaults_extra(tmp_path):
    row = fake_mep_entry().model_dump(mode="json")
    row["defaults"]["extra"] = {"account": "x"}
    seed = tmp_path / "seed.yaml"
    seed.write_text(yaml.safe_dump([row]))

    class _Client:
        def ingest(self, *a):
            raise AssertionError("must not reach the index")

    with pytest.raises(ValueError, match=r"defaults\.extra sets"):
        ingest("idx", seed, _Client())


# --- P4: a refusal polled after a switch -----------------------------------------------------------------------------


async def test_a_refusal_polled_after_a_switch_names_the_new_blocks_terms(tmp_path):
    from tests.test_wrong_block_guard import _mep, _probe, _warm

    app, _rt, runner = await _warm(tmp_path, hold=True, timeout=0.05, facility=_mep())
    runner.job = "113"
    out = await _run_shell(app, _probe(tmp_path))
    assert out.phase == "running"
    runner.land()
    await _ensure_endpoint_up(app, partition="other")
    polled = await _poll_task(app, out.task_id)
    assert polled.phase == "needs_confirmation" and polled.notice.startswith("Your command did NOT run")
    assert "before anything runs on it" not in polled.notice
    assert "switched since: a confirmation now starts a block on the new terms" in polled.notice
    assert "The new block's terms: partition 'other' (chosen earlier this session)" in polled.notice


# --- P5: an instruction even without terms ---------------------------------------------------------------------------


async def test_a_connect_whose_terms_cannot_be_read_still_says_what_to_ask(monkeypatch):
    monkeypatch.setattr(connect, "_next_block_terms", lambda app: None)
    _app, res = await _ssh_connect(monkeypatch, _ssh_entry())
    assert res.block is None
    assert "ask the user in one question for what they have not said" in res.notice
    assert "their yes is the spend confirmation" in res.notice


# --- P6: the hint is the registry's words, plain text -----------------------------------------------------------------


@pytest.mark.parametrize(("raw", "read"), [("proj \x1b[2K erased", "proj [2K erased"), ("proj \u202eevil\u202c", "proj evil"),
                                           ("proj\u200b\u200bx", "projx"), ("proj\x00x", "projx"),
                                           ("a\u2028b", "a b")])
def test_hidden_characters_never_reach_the_agent(raw, read):
    entry = CatalogEntry.model_validate({**fake_mep_entry().model_dump(mode="json"), "account_hint": raw})
    assert entry.account_hint == read


@pytest.mark.parametrize("raw", ["proj \x1b[2K", "proj \u202eevil", "proj\u200bx", "a\nb"])
def test_ingest_refuses_a_hint_with_hidden_characters(tmp_path, raw):
    row = fake_mep_entry().model_dump(mode="json")
    row["account_hint"] = raw
    seed = tmp_path / "seed.yaml"
    seed.write_text(yaml.safe_dump([row]))

    class _Client:
        def ingest(self, *a):
            raise AssertionError("must not reach the index")

    with pytest.raises(ValueError, match="account_hint has a control/format character"):
        ingest("idx", seed, _Client())


async def test_the_hint_is_quoted_as_the_registrys_words(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True, account_hint="ask; then x"))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    # its own sentence, quoted: a ";" in it never reads as part of the terms list
    assert "account: none set (required here); 1 node" in res.notice
    assert "Where the user finds the account (the facility registry's words): \"ask; then x\"." in res.notice


# --- the notices: one question, its yes is the confirmation ----------------------------------------------------------


async def test_no_account_where_one_is_needed_asks_for_it_in_the_spend_question(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True))
    for res in (await _ensure_endpoint_up(app, shape="compute"), await _run_shell(app, "hostname")):
        assert ("Ask the user which account to charge and whether these terms are OK, then call "
                "ensure_endpoint_up(account=…, confirm_spend=True)") in res.notice
        assert "then call ensure_endpoint_up(confirm_spend=True)" not in res.notice   # it would only meet the floor


async def test_a_kicked_reap_does_not_say_nothing_was_started(tmp_path):
    from tests.test_wrong_block_guard import _probe, _warm

    app, rt, runner = await _warm(tmp_path)
    runner.job = "113"
    await _run_shell(app, _probe(tmp_path))
    res = await _ensure_endpoint_up(app)
    assert rt.reap_kicked and res.status == "needs_confirmation" and res.block_state == "provisioning"
    assert "Spend not yet confirmed." in res.notice and "nothing was started" not in res.notice


async def test_an_account_preset_is_said_not_asked(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "Diaspora")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "globus1")
    _app, res = await _mep_app(monkeypatch, fake_mep_entry(account_required=True))
    assert "The HPC_BRIDGE_ACCOUNT preset counts as the user's own answer." in res.notice
    assert "ask the user in one question whether that partition is OK" in res.notice
    assert "which account" not in res.notice


async def test_a_cluster_you_bring_asks_whether_it_charges_an_account(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    from tests.test_spend_terms import _REAL_FROM_ENTRY, _SshLike
    monkeypatch.setattr(binding, "_facility_from_entry",
                        lambda e, *, account: _SshLike(_REAL_FROM_ENTRY(e, account=account)))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app)

    async def run_login(cmd):
        return None

    res = await connect._connect_facility(app, "mine", details=FacilityDetails(**_DRAFT), run_login=run_login)
    assert res.block.account_source == "none_set"
    assert "which account to charge, if their cluster charges one, and whether that partition is OK" in res.notice


async def test_compute_only_is_said_once(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True))
    res = await _ensure_endpoint_up(app, shape="compute", account="p1")
    assert res.notice.lower().count("compute-only") == 1 and res.notice.count("Ask the user") == 1


async def test_a_pbs_switch_says_queue_throughout(monkeypatch):
    entry = fake_mep_entry(id="polaris", account_required=True,
                           compute={"scheduler": "pbs", "interface": "hsn0", "env_setup": "",
                                    "scratch_root": "$HOME/.hpc-bridge", "key_map": {"partition": "queue"}},
                           defaults={"partition": "debug", "walltime": "00:15:00", "init_blocks": 1})
    app, _ = await _mep_app(monkeypatch, entry)
    assert (await _ensure_endpoint_up(app, shape="compute", account="p1", confirm_spend=True)).status == "up"
    res = await _ensure_endpoint_up(app, shape="compute", partition="prod")
    assert "queue 'prod'" in res.notice and "the one on queue 'debug', account 'p1'" in res.notice
    assert "partition 'debug'" not in res.notice


async def test_the_needs_account_notice_says_access_only_without_a_hint(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True, account_hint="your project code"))
    hinted = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert "ACCESS" not in hinted.notice and "ensure_endpoint_up(account=…, confirm_spend=True)" in hinted.notice
    app2, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True))
    bare = await _ensure_endpoint_up(app2, shape="compute", confirm_spend=True)
    assert "(on an ACCESS machine, usually the ACCESS project id)" in bare.notice
    assert "partition=…" not in bare.notice


async def test_an_empty_listing_is_not_called_listed(monkeypatch):
    _app, res = await _ssh_connect(monkeypatch, _ssh_entry(allocation=True), balance="")
    assert res.allocations == [] and "the allocation listing returned none" in res.notice
    assert "your allocations are listed" not in res.notice


def test_the_outcome_factories_need_their_terms():
    with pytest.raises(TypeError):
        notices._needs_confirmation_outcome()   # type: ignore[call-arg]
    with pytest.raises(TypeError):
        notices._needs_account_outcome()   # type: ignore[call-arg]
