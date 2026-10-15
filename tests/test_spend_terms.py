"""The spend question names the block: its queue and account, where each came from, its size and walltime (BlockTerms).

- One `block` on ensure_endpoint_up's and run_shell's needs_confirmation and needs_account, and on the confirming call's
  up / provisioning (a receipt: a one-call start still shows what it started). Their notices lead with the same terms
  line: queue and account first, each with its source.
- Sources: a default is the registry's, an env preset's (HPC_BRIDGE_PARTITION / HPC_BRIDGE_ACCOUNT, for the machine
  HPC_BRIDGE_MACHINE names), a facility you bring yourself's details (or the probe's proposal those details kept), or the
  facility template's (nothing sent); a choice is this call's or an earlier call's (a stop keeps it); an account with
  none set is "none_needed" only where the registry says so.
- A registry entry may carry `account_hint` (where a user finds the account): seed → entry → the block's terms → the
  needs_account notice and connect_facility's result.
- connect_facility states the next block's terms before any spend, and the one question to ask: the confirming call
  comes only after it (the user's yes is the confirmation).
"""
import time
from pathlib import Path

import pytest
import yaml

from hpc_bridge import binding, connect, notices, server, warmth
from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.catalog.entry import ACCOUNT_HINT_MAX, CatalogEntry
from hpc_bridge.catalog.ingest import ingest
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _run_shell, _stop_endpoint
from tests.fakes import FakeCatalog, FakeFacility, fake_entry, fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_server import _FakeRunner, _Res

SEEDS = Path(__file__).resolve().parents[1] / "src" / "hpc_bridge" / "catalog" / "seed"


def _ok(job: str | None = "41") -> CanaryResult:
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9", worker_job=job,
                        worker_user="hpcbmep", answered_at=time.monotonic())


def _runners(app: AppCtx, canary: CanaryResult | None = None) -> None:
    app.built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "ok\n", ""), canary_result=canary or _ok())

    app.runner_factory = factory


async def _mep_app(monkeypatch, entry: CatalogEntry | None = None, *, canary: CanaryResult | None = None) -> tuple:
    """connect_facility to a facility endpoint through the real connect path (a REAL MEPFacility built from the same
    entry): returns (app, the connect result)."""
    entry = entry or fake_mep_entry()
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry",
                        lambda e, *, account: MEPFacility.from_entry(e, account=account or None, client_factory=_Status))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app, canary)
    res = await server._connect_facility(app, entry.id)
    assert res.phase == "needs_account", res
    return app, res


class _SshLike(FakeFacility):
    """An SSH facility as connect sees one: it comes up warm (a fake), with a REAL SlurmFacility's config — its defaults
    (partition, walltime, nodes, account) and its account flag."""

    def __init__(self, real) -> None:
        super().__init__()
        self.workers = 1
        self._real = real
        self.profile = real.profile
        self.account_required = real.account_required

    def config_template(self, profile):
        return self._real.config_template(profile)


def _ssh_entry(*, account_required: bool = True, allocation: bool = False, hint: str | None = None,
               scheduler: str = "slurm", id: str = "expanse") -> CatalogEntry:  # noqa: A002
    e = fake_entry(id=id, facility_key="sdsc")
    return e.model_copy(update={
        "account_required": account_required, "account_hint": hint,
        "allocation": e.allocation if allocation else None,
        "compute": e.compute.model_copy(update={"scheduler": scheduler}),
        "defaults": e.defaults.model_copy(update={"partition": "shared", "walltime": "00:30:00", "nodes_per_block": 2}),
    })


_REAL_FROM_ENTRY = binding._facility_from_entry


async def _ssh_connect(monkeypatch, entry: CatalogEntry, *, balance: str = "") -> tuple:
    """connect_facility to an SSH entry through the real connect path (the facility built by the real
    binding._facility_from_entry, presets included, then made to come up as a fake): returns (app, result)."""
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry",
                        lambda e, *, account: _SshLike(_REAL_FROM_ENTRY(e, account=account)))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app)

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout=balance)

    res = await connect._connect_facility(app, entry.id, run_login=run_login)
    return app, res


# --- BlockTerms on every spend answer, on both tools -----------------------------------------------------------------


async def test_ensure_endpoint_up_needs_confirmation_names_the_block(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True, account_hint="your project (e.g. hpcb)"))
    res = await _ensure_endpoint_up(app, shape="compute", partition="main", account="hpcb")
    b = res.block
    assert res.status == "needs_confirmation" and b is not None
    assert (b.facility, b.queue, b.queue_source, b.account, b.account_source) == (
        "globus1", "main", "this_call", "hpcb", "this_call")
    assert (b.nodes, b.walltime, b.max_node_hours, b.compute_only, b.account_required) == (
        1, "02:00:00", 2.0, True, True)
    # the notice leads with the queue and the account, each with where it came from
    assert res.notice.startswith("Block terms: partition 'main' (set by this call); account 'hpcb' (set by this "
                                 "call); 1 node for up to 02:00:00 (at most 2 node-hours). Compute-only")
    assert "Spend not yet confirmed: nothing was started." in res.notice and app.built == []


async def test_needs_account_names_the_block_and_the_hint_on_both_tools(monkeypatch):
    hint = "your ALCF project name, as listed in the ALCF portal (e.g. Diaspora)"
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True, account_hint=hint))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "needs_account" and res.block is not None
    assert (res.block.account, res.block.account_source, res.block.account_hint) == (None, "none_set", hint)
    assert res.notice.startswith("Block terms: partition 'main' (the registry's default); account: none set (required "
                                 "here); 1 node for up to 02:00:00 (at most 2 node-hours). Where the user finds the "
                                 f"account (the facility registry's words): \"{hint}\". ")
    assert "LOGIN NAME is not an account" in res.notice
    # run_shell meets the spend floor first (it never confirms), so its needs_account is the same factory's answer
    out = notices._needs_account_outcome(app, app.shapes["compute"])
    assert out.phase == "needs_account" and out.block == res.block
    assert out.notice == res.notice                       # one text for both tools
    assert app.built == []


async def test_run_shell_needs_account_carries_the_same_terms(monkeypatch):
    # run_shell meets the spend floor before the account floor today; if its provision ever answers needs_account,
    # the outcome still names the block, in ensure_endpoint_up's words
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True, account_hint="your project"))
    ensure = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)

    async def floor(app_, shape, **_kw):
        return "needs_account"

    monkeypatch.setattr(warmth, "_provision", floor)
    out = await _run_shell(app, "hostname")
    assert out.phase == "needs_account" and out.block == ensure.block and out.notice == ensure.notice


async def test_run_shell_needs_confirmation_carries_the_same_terms(monkeypatch):
    app, _ = await _mep_app(monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute")
    out = await _run_shell(app, "hostname")
    assert out.phase == "needs_confirmation" and out.block == res.block
    head = notices._terms_line(app, res.block)
    assert res.notice.startswith(head) and out.notice.startswith(head)   # the same words, at the head of both
    assert "Spend not confirmed, so nothing ran." in out.notice


async def test_a_one_call_start_carries_a_receipt(monkeypatch):
    app, _ = await _mep_app(monkeypatch, fake_mep_entry(account_required=True))
    res = await _ensure_endpoint_up(app, shape="compute", account="hpcb", confirm_spend=True)
    assert res.status == "up" and res.block is not None
    assert (res.block.queue, res.block.queue_source, res.block.account, res.block.account_source) == (
        "main", "registry", "hpcb", "this_call")
    # a status call that confirms nothing carries none, and neither does a poll that re-passes confirm_spend=True on
    # the block already confirmed: it confirmed nothing new
    assert (await _ensure_endpoint_up(app, shape="compute")).block is None
    again = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert again.status == "up" and again.block is None
    # a switch confirmed in the same call is a new block: its receipt names the new terms
    switched = await _ensure_endpoint_up(app, shape="compute", partition="gpu", confirm_spend=True)
    assert switched.status == "up" and (switched.block.queue, switched.block.queue_source) == ("gpu", "this_call")


async def test_a_provisioning_confirm_carries_a_receipt(monkeypatch):
    app, _ = await _mep_app(monkeypatch, canary=CanaryResult(ok=False, error="timeout"))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning" and res.block is not None and res.block.account_source == "none_needed"


async def test_the_free_login_shape_has_no_terms():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    _runners(app)
    res = await _ensure_endpoint_up(app, shape="login", confirm_spend=True)
    assert res.status == "up" and res.block is None


async def test_a_refused_replacement_names_the_block_it_asks_for(tmp_path):
    from tests.test_wrong_block_guard import _probe, _warm

    app, _rt, runner = await _warm(tmp_path)
    runner.job = "113"
    out = await _run_shell(app, _probe(tmp_path))
    assert out.phase == "needs_confirmation" and out.notice.startswith("Your command did NOT run")
    assert out.block is not None and out.block.queue_source == "facility_template"
    assert notices._terms_line(app, out.block) in out.notice


# --- the sources ------------------------------------------------------------------------------------------------------


async def test_a_switch_is_this_call_then_an_earlier_call_and_a_stop_keeps_it(monkeypatch):
    app, _ = await _mep_app(monkeypatch)
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).block.queue_source == "registry"
    res = await _ensure_endpoint_up(app, shape="compute", partition="other")       # a switch, not confirmed
    assert res.status == "needs_confirmation" and (res.block.queue, res.block.queue_source) == ("other", "this_call")
    out = await _run_shell(app, "hostname")
    assert out.phase == "needs_confirmation" and out.block.queue_source == "earlier_call"
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).block.queue_source == "earlier_call"
    await _stop_endpoint(app)
    assert "compute" not in app.shapes
    # the next block is seeded with the session's choice: an earlier call's, not the registry's
    res = await _ensure_endpoint_up(app, shape="compute")
    assert (res.block.queue, res.block.queue_source) == ("other", "earlier_call")


async def test_naming_the_default_again_is_a_choice(monkeypatch):
    app, _ = await _mep_app(monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute", partition="main")       # the registry default, named
    assert (res.block.queue, res.block.queue_source) == ("main", "this_call")
    assert (await _ensure_endpoint_up(app, shape="compute")).block.queue_source == "earlier_call"


async def test_a_new_connect_forgets_the_choice(monkeypatch):
    app, _ = await _mep_app(monkeypatch)
    await _ensure_endpoint_up(app, shape="compute", partition="other")
    res = await server._connect_facility(app, "globus1")
    assert (res.block.queue, res.block.queue_source) == ("main", "registry")


async def test_no_account_needed_only_where_the_registry_says_so(monkeypatch):
    app, res = await _mep_app(monkeypatch, fake_mep_entry(account_required=False))
    assert (res.block.account, res.block.account_source, res.block.account_required) == (None, "none_needed", False)
    assert "no allocation account is needed — do not look for one" in notices._terms_line(app, res.block)
    # a facility you bring yourself says nothing either way: none set, the scheduler's default applies
    byo = binding._entry_from_details("mine", FacilityDetails(
        ssh_host="login.example.edu", interface="ib0", env_setup="module load x", scratch_root="/scratch/{user}",
        partition="batch"))
    origin = binding._terms_origin(byo)
    assert origin.account_needed is None and origin.partition == "byo_details"


async def test_env_presets_are_named_as_presets(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "expanse")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "debug")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "chi152")
    app, res = await _ssh_connect(monkeypatch, _ssh_entry())
    assert res.phase == "needs_account" and res.block is not None
    assert (res.block.queue, res.block.queue_source, res.block.account, res.block.account_source) == (
        "debug", "env", "chi152", "env")
    assert ("partition 'debug' (the HPC_BRIDGE_PARTITION preset); account 'chi152' (the HPC_BRIDGE_ACCOUNT preset)"
            in res.notice)
    # both presets count as the user's answer: no question, the terms are stated and the block started
    assert "The HPC_BRIDGE_PARTITION and the HPC_BRIDGE_ACCOUNT presets count as the user's own answer." in res.notice
    assert "State these terms to the user in one line and start: ensure_endpoint_up(confirm_spend=True)." in res.notice
    assert "ask the user" not in res.notice
    # ...and once this session names one, it is this call's
    up = await _ensure_endpoint_up(app, shape="compute", account="chi999", confirm_spend=True)
    assert up.status == "up" and (up.block.account, up.block.account_source) == ("chi999", "this_call")


async def test_a_template_default_queue_is_named_as_the_facilitys():
    f = FakeFacility()                                    # sends no partition: the facility's template decides
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    _runners(app)
    res = await _ensure_endpoint_up(app, shape="compute")
    assert (res.block.queue, res.block.queue_source, res.block.nodes, res.block.walltime, res.block.max_node_hours) == (
        None, "facility_template", None, None, None)
    assert res.notice.startswith("Block terms: the facility's default partition (hpc-bridge sends none); no account "
                                 "set (the scheduler's default account applies, if it has one); the facility's "
                                 "default size for the facility's default walltime.")


def test_bring_your_own_details_say_whether_the_probe_proposed_the_partition():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    draft = FacilityDetails(ssh_host="login.example.edu", interface="ib0", env_setup="module load x",
                            scratch_root="/scratch/{user}", partition="batch")
    app.proposed_drafts["mine"] = draft
    kept = binding._entry_from_details("mine", draft)
    changed = binding._entry_from_details("mine", draft.model_copy(update={"partition": "gpu"}))
    assert connect._discovered_partition(app, "mine", kept) is True
    assert connect._discovered_partition(app, "mine", changed) is False
    assert binding._terms_origin(kept, discovered=True).partition == "discovered"
    assert binding._terms_origin(changed).partition == "byo_details"


async def test_a_details_connect_records_the_discovered_partition(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    draft = FacilityDetails(ssh_host="login.example.edu", interface="ib0", env_setup="module load x",
                            scratch_root="/scratch/{user}", partition="batch")
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: f)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app)
    app.proposed_drafts["mine"] = draft

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    await connect._connect_facility(app, "mine", details=draft, run_login=run_login)
    assert app.terms_origin.partition == "discovered" and app.terms_origin.account_needed is None
    await connect._connect_facility(app, "mine", details=draft.model_copy(update={"partition": "gpu"}),
                                    run_login=run_login)
    assert app.terms_origin.partition == "byo_details"


async def test_the_startup_pin_records_its_terms_origin(monkeypatch):
    entry = _ssh_entry(hint="your Expanse project id")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "expanse")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "chi152")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "debug")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    await binding.make_facility()
    origin = binding._PINNED_ORIGIN["expanse"]
    assert (origin.partition, origin.account_needed, origin.account_hint) == ("env", True, "your Expanse project id")
    async with server.lifespan(server.mcp) as app:       # the pin's origin is bound with the facility
        assert app.terms_origin == origin


def test_a_failed_pin_records_no_origin(monkeypatch):
    entry = _ssh_entry()
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, **kw: (_ for _ in ()).throw(RuntimeError("bad")))
    with pytest.raises(RuntimeError):
        binding._pinned_facility("expanse", entry, account="x")
    assert binding._PINNED_ORIGIN == {} and binding._PINNED == {}


# --- PBS and facility-endpoint wording --------------------------------------------------------------------------------


async def test_a_pbs_facility_endpoint_says_queue(monkeypatch):
    entry = fake_mep_entry(id="polaris", account_required=True,
                           compute={"scheduler": "pbs", "interface": "hsn0", "env_setup": "",
                                    "scratch_root": "$HOME/.hpc-bridge", "key_map": {"partition": "queue"}},
                           defaults={"partition": "debug", "walltime": "00:15:00", "init_blocks": 1})
    app, res = await _mep_app(monkeypatch, entry)
    assert res.block.queue == "debug" and "queue 'debug' (the registry's default)" in res.notice
    need = await _ensure_endpoint_up(app, shape="compute")
    assert need.notice.startswith("Block terms: queue 'debug' (the registry's default); account: none set (required "
                                  "here); 1 node for up to 00:15:00 (at most 0.25 node-hours). Compute-only through "
                                  "this channel: every command bills, and the block stays warm between commands")


# --- the account hint, from the seed to the result --------------------------------------------------------------------


def test_the_seeds_carry_their_account_hints():
    entries = {e.id: e for e in BundledCatalog(SEEDS).entries()}
    assert entries["polaris"].account_hint == "your ALCF project name, as listed in the ALCF portal (e.g. Diaspora)"
    assert "lowercased (e.g. cis250223)" in entries["anvil"].account_hint
    assert "expanse-client user -r expanse" in entries["expanse"].account_hint
    assert "'accounts'" in entries["delta"].account_hint
    assert entries["globus-labs"].account_hint is None and entries["globus-labs"].account_required is False
    assert not any("sbank" in (e.account_hint or "") for e in entries.values())   # never point a user at sbank
    for e in entries.values():                           # every account-required entry says where to look
        assert e.account_hint or not e.account_required, e.id


async def test_the_hint_reaches_connect_and_the_account_floor(monkeypatch):
    polaris = {e.id: e for e in BundledCatalog(SEEDS).entries()}["polaris"]
    app, res = await _mep_app(monkeypatch, polaris)
    assert res.block.account_hint == polaris.account_hint and polaris.account_hint in res.notice
    floor = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert floor.status == "needs_account" and polaris.account_hint in floor.notice


async def test_an_ssh_hint_reaches_connect_with_the_defaults(monkeypatch):
    expanse = {e.id: e for e in BundledCatalog(SEEDS).entries()}["expanse"]
    _app, res = await _ssh_connect(monkeypatch, expanse)
    assert res.phase == "needs_account" and res.block is not None
    assert (res.block.queue, res.block.queue_source, res.block.account_required, res.block.nodes,
            res.block.walltime) == ("shared", "registry", True, 1, "00:30:00")
    assert ("The next block's terms: partition 'shared' (the registry's default); account: none set (required here); "
            "1 node for up to 00:30:00 (at most 0.5 node-hours). Where the user finds the account (the facility "
            f"registry's words): \"{expanse.account_hint}\".") in res.notice
    assert res.notice.startswith("login node is up; hpc-bridge reads no allocation listing here.")


def test_a_malformed_hint_costs_the_hint_not_the_entry():
    raw = fake_mep_entry().model_dump(mode="json")
    assert CatalogEntry.model_validate({**raw, "account_hint": 42}).account_hint is None
    assert CatalogEntry.model_validate({**raw, "account_hint": "  two\nlines  "}).account_hint == "two lines"
    assert len(CatalogEntry.model_validate({**raw, "account_hint": "x" * 999}).account_hint) == ACCOUNT_HINT_MAX
    assert "account_hint" not in fake_mep_entry().model_dump(exclude_none=True)   # absent: nothing for old clients


@pytest.mark.parametrize("hint", [42, "", "x" * (ACCOUNT_HINT_MAX + 1)])
def test_ingest_refuses_a_hint_the_client_would_drop(tmp_path, hint):
    row = fake_mep_entry().model_dump(mode="json")
    row["account_hint"] = hint
    seed = tmp_path / "seed.yaml"
    seed.write_text(yaml.safe_dump([row]))

    class _Client:
        def ingest(self, *a):
            raise AssertionError("must not reach the index")

    with pytest.raises(ValueError, match="account_hint"):
        ingest("idx", seed, _Client())


# --- connect states the defaults and asks before any confirm ---------------------------------------------------------


@pytest.mark.parametrize("required", [True, False])
async def test_a_facility_endpoint_connect_states_the_defaults(monkeypatch, required):
    app, res = await _mep_app(monkeypatch, fake_mep_entry(account_required=required))
    assert res.block is not None and (res.block.queue, res.block.walltime, res.block.nodes) == ("main", "02:00:00", 1)
    assert "Nothing is started or billed yet. The next block's terms: partition 'main' (the registry's default); " \
           in res.notice
    assert ("account: none set (required here)" if required
            else "no allocation account is needed — do not look for one") in res.notice
    assert res.notice.lower().count("no allocation account is needed") == (0 if required else 1)   # said once
    assert ("ask the user in one question which account to charge, and whether that partition is OK" if required
            else "ask the user in one question whether that partition is OK") in res.notice
    # their yes is the spend confirmation, sent with their answer in one call — after the question, never before it
    call = ("ensure_endpoint_up(account=…, partition=…, confirm_spend=True)" if required
            else "ensure_endpoint_up(partition=…, confirm_spend=True)")
    assert res.notice.index("their yes is the spend confirmation") < res.notice.index(call)
    assert "compute" not in app.shapes and app.built == []      # stating the terms built and started nothing


@pytest.mark.parametrize("allocation", [False, True])
async def test_an_ssh_connect_states_the_defaults(monkeypatch, allocation):
    balance = ("Allocation Type SU Limit SU Usage SU Usage SU Balance\n=====  ====  ===  ===  ===  ===\n"
               "chi152 CPU 10.0 1.0 1.0 9.0\n")
    app, res = await _ssh_connect(monkeypatch, _ssh_entry(allocation=allocation), balance=balance)
    assert res.phase == "needs_account" and res.block is not None
    assert (res.block.queue, res.block.nodes, res.block.walltime, res.block.max_node_hours) == (
        "shared", 2, "00:30:00", 1.0)
    assert "2 nodes for up to 00:30:00 (at most 1 node-hour)" in res.notice          # singular
    assert ("which listed allocation to charge" if allocation else "which account to charge") in res.notice
    assert res.notice.index("ask the user in one question") < res.notice.index("confirm_spend=True")
    assert "compute" not in app.shapes                          # only the free login shape came up


async def test_a_warming_ssh_connect_states_the_defaults_and_promises_no_listing(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    entry = _ssh_entry()
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry",
                        lambda e, *, account: _SshLike(_REAL_FROM_ENTRY(e, account=account)))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    _runners(app, CanaryResult(ok=False, error="timeout"))   # the login worker is still warming

    async def run_login(cmd):
        raise AssertionError("no listing while the login node warms")

    res = await connect._connect_facility(app, entry.id, run_login=run_login)
    assert res.phase == "provisioning" and res.block is not None and res.block.queue == "shared"
    assert "read your allocations" not in res.notice                 # this entry lists none
    assert "confirm_spend" not in res.notice                         # nothing to ask yet: the login node warms


async def test_no_connect_notice_suggests_a_confirm_before_the_question(monkeypatch):
    # every connect answer that names the confirming call puts the user's question (or, with every term said, the
    # one-line statement) first: their yes is the confirmation, never one the agent gives itself
    results = [(await _mep_app(monkeypatch, fake_mep_entry(account_required=r)))[1] for r in (True, False)]
    results += [(await _ssh_connect(monkeypatch, _ssh_entry(allocation=a)))[1] for a in (True, False)]
    for res in results:
        n = res.notice
        assert "confirm_spend=True" in n, n
        first = min(i for i in (n.find("ask the user in one question"), n.find("State these terms")) if i >= 0)
        assert first < n.index("confirm_spend=True"), n
