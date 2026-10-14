"""The spend gate's terms: who may start or charge a billed block, and on which partition and account.

- The account floor holds on every channel: a facility built from a registry entry carries the entry's
  `account_required`, an SSH one as a facility endpoint, and the floor reads the account the block would be charged to
  (this session's choice, or the facility's configured default). A bring-your-own facility has none.
- The env presets HPC_BRIDGE_ACCOUNT / HPC_BRIDGE_PARTITION apply only to the facility HPC_BRIDGE_MACHINE names (by id,
  subject or the alias the startup pin resolved); a connect where one goes unused says so in one clause, never its value.
- A partition or account switch on a confirmed block is a different block: the call answers `needs_confirmation` unless
  it confirms itself, and either way an answer says what became of the old block. Nothing cancels that block; it is
  billed once to the latest it can bill, and a switch back that the same job answers keeps its age and its clock. A task
  that finished on the old block never vouches for the new one.
- A stop keeps the session's partition and account; a connect_facility or a teardown forgets them.
"""
import time
from pathlib import Path

import pytest

from hpc_bridge import binding, connect, notices, scheduler_ops, server, warmth
from hpc_bridge.context import LeftBlock
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _poll_task, _run_shell, _shape_runtime, _stop_endpoint
from tests.fakes import FakeCatalog, FakeFacility, fake_entry, fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_server import _FakeRunner, _Res


def _ok(job: str | None = None) -> CanaryResult:
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9", worker_job=job,
                        worker_user="hpcbmep", answered_at=time.monotonic())


class _Web:
    """The Globus web service as an SSH facility asks it: the manager is online; its metadata is not readable."""

    def get_endpoint_status(self, endpoint_id):
        return {"status": "online"}

    def get_endpoint_metadata(self, endpoint_id):
        raise RuntimeError("no metadata in this test")


def _ssh_entry(*, scheduler: str = "slurm", account_required: bool = True, id: str = "expanse"):  # noqa: A002
    """A registry SSH entry like Expanse's: account_required, an SSH host, no facility endpoint."""
    e = fake_entry(id=id, facility_key="sdsc")
    return e.model_copy(update={"account_required": account_required,
                                "compute": e.compute.model_copy(update={"scheduler": scheduler})})


def _ssh_app(monkeypatch, entry=None, *, account: str = "") -> AppCtx:
    """A REAL SlurmFacility built from a catalog entry (binding._facility_from_entry), its endpoint already up (no
    bootstrap), with a fake runner that records every config it is built with."""
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    fac = binding._facility_from_entry(entry or _ssh_entry(), account=account)
    assert isinstance(fac, SlurmFacility)
    fac._client_factory = _Web
    app = AppCtx(facility=fac, profile=Profile())
    app.state = EndpointState(endpoint_id="eid-1", reused=True)
    app.built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "ok", ""), canary_result=_ok())

    app.runner_factory = factory
    return app


# --- the account floor on an SSH facility ---------------------------------------------------------------------------


@pytest.mark.parametrize("scheduler", ["slurm", "pbs"])
async def test_an_account_required_ssh_entry_starts_nothing_without_an_account(monkeypatch, scheduler):
    app = _ssh_app(monkeypatch, _ssh_entry(scheduler=scheduler))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "needs_account" and res.block_state == "cold" and res.account is None
    assert app.built == []                                       # no runner, nothing submitted
    assert app.shapes["compute"].spend_confirmed is False          # the re-call with account= re-gates cleanly
    # run_shell takes the same floor before any dispatch
    out = await _run_shell(app, "hostname")
    assert out.phase in ("needs_confirmation", "needs_account") and app.built == []
    res = await _ensure_endpoint_up(app, shape="compute", account="chi152", confirm_spend=True)
    assert res.status == "up" and app.built[-1]["account"] == "chi152"


async def test_the_floor_takes_the_facilitys_own_configured_account(monkeypatch):
    # the startup pin's HPC_BRIDGE_ACCOUNT (or a preset for the machine it names) is the facility's default account:
    # it is what the block is charged to, so the floor must not ask for another
    app = _ssh_app(monkeypatch, account="chi152")
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "up" and app.built[-1]["account"] == "chi152"


async def test_an_ssh_entry_that_needs_no_account_is_unchanged(monkeypatch):
    app = _ssh_app(monkeypatch, _ssh_entry(account_required=False))
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"


async def test_a_bring_your_own_facility_keeps_todays_behaviour(monkeypatch):
    # a session entry has no curated account_required: hpc-bridge does not know, so it never floors the start
    entry = binding._entry_from_details("mine", FacilityDetails(
        ssh_host="login.example.edu", interface="ib0", env_setup="module load x", scratch_root="/scratch/{user}",
        partition="batch"))
    app = _ssh_app(monkeypatch, entry)
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"


async def test_the_needs_account_notice_points_an_ssh_user_at_the_login_shape(monkeypatch):
    app = _ssh_app(monkeypatch)
    notice = (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).notice
    assert "LOGIN NAME is not an account" in notice and "ensure_endpoint_up(account=" in notice
    assert "shape='login'" in notice and "connect_facility" in notice   # where an SSH user finds the account
    assert "multi-user endpoint" not in notice
    # ...and a facility endpoint's keeps pointing at the facility's own tool or portal (no login shape there)
    mep = AppCtx(facility=MEPFacility.from_entry(fake_mep_entry(account_required=True), client_factory=_Status),
                 profile=Profile())
    assert "shape='login'" not in server._needs_account_notice(mep)
    assert "allocation tool or portal" in server._needs_account_notice(mep)


# --- the env presets apply only to the facility HPC_BRIDGE_MACHINE names --------------------------------------------


async def _connect_mep_capturing(monkeypatch, entry=None):
    """connect_facility to a MEP entry through the real connect path; returns (result, the account it was built with)."""
    entry = entry or fake_mep_entry()
    seen = []

    def build(e, *, account):
        seen.append(account)
        return MEPFacility.from_entry(e, account=account or None, client_factory=_Status)

    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry", build)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, entry.id)
    assert res.phase == "needs_account"
    return res, seen[0], app


@pytest.mark.parametrize("machine", ["anvil", None])
async def test_the_account_preset_does_not_reach_another_facility(monkeypatch, machine):
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "hpcb-gpu")
    monkeypatch.delenv("HPC_BRIDGE_PARTITION", raising=False)
    if machine:
        monkeypatch.setenv("HPC_BRIDGE_MACHINE", machine)
    else:
        monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    res, account, _app = await _connect_mep_capturing(monkeypatch)
    assert account == ""
    # one clause says so — naming the variable and the machine it is for, never the account itself
    assert "HPC_BRIDGE_ACCOUNT applies only to" in res.notice and "hpcb-gpu" not in res.notice
    assert (f"HPC_BRIDGE_MACHINE={machine}" if machine else "HPC_BRIDGE_MACHINE is unset") in res.notice


@pytest.mark.parametrize("machine", ["globus1", "globus:globus1"], ids=["id", "subject"])
async def test_the_account_preset_reaches_the_facility_it_names(monkeypatch, machine):
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "hpcb-gpu")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", machine)
    res, account, _app = await _connect_mep_capturing(monkeypatch)
    assert account == "hpcb-gpu" and "applies only to" not in res.notice


async def test_no_preset_means_no_preset_clause(monkeypatch):
    for v in ("HPC_BRIDGE_ACCOUNT", "HPC_BRIDGE_PARTITION", "HPC_BRIDGE_MACHINE"):
        monkeypatch.delenv(v, raising=False)
    res, account, _app = await _connect_mep_capturing(monkeypatch)
    assert account == "" and "applies only to" not in res.notice


async def test_an_ssh_connect_says_the_partition_preset_was_not_used(monkeypatch):
    monkeypatch.delenv("HPC_BRIDGE_ACCOUNT", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "other")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([_ssh_entry()]))
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: f)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    res = await connect._connect_facility(app, "expanse", run_login=run_login)
    assert res.phase == "needs_account"
    assert "(HPC_BRIDGE_PARTITION applies only to HPC_BRIDGE_MACHINE=other, so it was not used here.)" in res.notice


def test_both_presets_unused_read_as_one_clause(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "secret-project")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    note = notices._unapplied_presets_note(named=False, facility_endpoint=True, own_partition=False)
    assert note == ("(HPC_BRIDGE_ACCOUNT and HPC_BRIDGE_PARTITION apply only to the facility HPC_BRIDGE_MACHINE names, "
                    "and HPC_BRIDGE_MACHINE is unset, so they were not used here.) ")


async def test_the_partition_preset_says_it_never_applies_to_a_facility_endpoint(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "hpcb-gpu")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "globus1")
    res, account, _app = await _connect_mep_capturing(monkeypatch)
    assert account == "hpcb-gpu"                                       # the account is the named machine's
    assert ("(HPC_BRIDGE_PARTITION never applies to a facility endpoint, whose entry sets the partition, so it was not "
            "used here.)") in res.notice and "gpu'" not in res.notice


def test_details_win_over_the_partition_preset_of_the_machine_they_define(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "mine")
    entry = binding._entry_from_details("mine", FacilityDetails(
        ssh_host="login.example.edu", interface="ib0", env_setup="module load x", scratch_root="/scratch/{user}",
        partition="batch"))
    assert binding._facility_from_entry(entry, account="").profile.partition == "batch"
    assert notices._unapplied_presets_note(named=True, facility_endpoint=False, own_partition=True) == (
        "(HPC_BRIDGE_PARTITION gives way to the partition given in details=, so it was not used here.) ")


async def test_an_alias_the_pin_resolved_names_its_machine_for_the_presets(monkeypatch):
    from hpc_bridge.catalog.bundled import BundledCatalog

    monkeypatch.setattr(binding, "_PINNED", {}, raising=False)
    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "x-u")
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_PARTITION", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "anvil.rcac.purdue.edu")
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "ACC")
    await binding.make_facility()                                      # the startup pin resolves the alias
    seen = []
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: seen.append(account) or f)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    res = await connect._connect_facility(app, "anvil", run_login=run_login)
    assert seen == ["ACC"] and "applies only to" not in (res.notice or "")


@pytest.mark.parametrize(("machine", "want"), [(None, "debug"), ("other", "debug"), ("expanse", "gpu"),
                                               ("sdsc:expanse", "gpu")])
def test_the_partition_preset_reaches_only_the_ssh_entry_it_names(monkeypatch, machine, want):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    if machine:
        monkeypatch.setenv("HPC_BRIDGE_MACHINE", machine)
    else:
        monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    fac = binding._facility_from_entry(_ssh_entry(), account="")
    assert fac.profile.partition == want


def test_the_partition_preset_does_not_override_a_bring_your_own_partition(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "gpu")
    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    entry = binding._entry_from_details("mine", FacilityDetails(
        ssh_host="login.example.edu", interface="ib0", env_setup="module load x", scratch_root="/scratch/{user}",
        partition="batch"))
    assert binding._facility_from_entry(entry, account="").profile.partition == "batch"


_ANVIL_SSH_SEED = Path(__file__).parent / "catalog_fixtures" / "anvil-ssh.yaml"


@pytest.mark.parametrize("machine", ["anvil", "purdue:anvil", "anvil.rcac.purdue.edu"], ids=["id", "subject", "alias"])
async def test_the_startup_pin_still_applies_both_presets(monkeypatch, machine):
    # the startup pin IS the machine HPC_BRIDGE_MACHINE names, however it was named (an alias resolves too)
    from hpc_bridge.catalog.bundled import BundledCatalog

    monkeypatch.setattr(binding, "make_catalog", lambda: BundledCatalog(_ANVIL_SSH_SEED))
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "x-u")
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", machine)
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "ACC")
    monkeypatch.setenv("HPC_BRIDGE_PARTITION", "wholenode")
    fac = await binding.make_facility()
    assert fac.profile.account == "ACC" and fac.profile.partition == "wholenode"
    monkeypatch.delenv("HPC_BRIDGE_ACCOUNT")
    with pytest.raises(RuntimeError, match="HPC_BRIDGE_ACCOUNT"):     # ...and the pin still demands the account
        await binding.make_facility()


# --- a partition or account switch asks for spend again -------------------------------------------------------------


async def _warm(facility=None, job: str | None = "112"):
    """A billed compute shape on an SSH-like facility (it has a login shape): confirmed, warm, one command run."""
    f = facility or FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "ok", ""), canary_result=_ok(job))

    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, partition="debug", account="hpcb", confirm_spend=True)
    assert res.status == "up"
    assert (await _run_shell(app, "true")).phase == "complete"
    return app


@pytest.mark.parametrize("switch", [{"partition": "compute"}, {"account": "hpcb-gpu"}])
async def test_a_switch_without_a_confirmation_starts_nothing(switch):
    app = await _warm()
    rt = app.shapes["compute"]
    runner, built = rt.runner, len(app.built)
    canaries, commands = runner.canaries, len(runner.commands)
    res = await _ensure_endpoint_up(app, **switch)
    assert res.status == "needs_confirmation" and res.block_state == "cold"
    assert len(app.built) == built and runner.canaries == canaries and len(runner.commands) == commands
    assert rt.spend_confirmed is False
    # the agent's next command is refused the same way, with the same reason
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation" and len(app.built) == built
    assert "different block" in out.notice
    # the user's yes on the new terms starts it
    res = await _ensure_endpoint_up(app, confirm_spend=True)
    assert res.status == "up" and len(app.built) == built + 1
    assert all(app.built[-1][k] == v for k, v in switch.items())
    assert rt.switched_terms is None


async def test_a_switch_of_both_in_one_call_asks_once_and_names_the_old_terms():
    app = await _warm()
    res = await _ensure_endpoint_up(app, partition="compute", account="hpcb-gpu")
    assert res.status == "needs_confirmation" and res.partition == "compute" and res.account == "hpcb-gpu"
    assert res.notice.count("Confirm spend again") == 1 and "partition 'debug', account 'hpcb'" in res.notice
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert app.built[-1]["partition"] == "compute" and app.built[-1]["account"] == "hpcb-gpu"


async def test_a_switch_with_its_own_confirmation_proceeds():
    app = await _warm()
    res = await _ensure_endpoint_up(app, partition="compute", account="hpcb-gpu", confirm_spend=True)
    assert res.status == "up" and app.built[-1]["partition"] == "compute" and app.built[-1]["account"] == "hpcb-gpu"


async def test_the_same_partition_and_account_are_not_a_switch():
    app = await _warm()
    rt = app.shapes["compute"]
    runner = rt.runner
    res = await _ensure_endpoint_up(app, partition="debug", account="hpcb")
    assert res.status == "up" and rt.spend_confirmed is True and rt.runner is runner


async def test_an_unconfirmed_shape_has_nothing_to_drop_and_claims_no_previous_block():
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, partition="compute")
    assert res.status == "needs_confirmation" and "previous block" not in res.notice


async def test_the_ssh_switch_notice_says_the_old_block_idles_out_and_stop_cancels_it():
    app = await _warm()
    notice = (await _ensure_endpoint_up(app, partition="compute")).notice
    assert "Confirm spend again" in notice and "partition 'debug'" in notice and "account 'hpcb'" in notice
    assert "Nothing cancels the previous block on a switch" in notice and "min_blocks=0" in notice
    assert "idle-releases ~600s after its last task" in notice
    assert "call stop_endpoint before confirming the new block" in notice
    assert "on 'compute'" in notice                                    # the head names the new block's partition


async def test_after_an_ssh_switch_stop_cancels_the_old_block_and_keeps_the_new_choice(monkeypatch):
    # what the notice promises: stop_endpoint releases every block of this endpoint (the old partition's too), and the
    # new choice is what the next block uses
    app = await _warm()
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["cmd"] = scheduler_ops._release_cmd_for(a, eid)
        seen["expect_block"] = expect_block
        return True, "released 112"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    stop = await _stop_endpoint(app)
    assert stop.status == "down" and "uep.fake-eid" in seen["cmd"]   # every block of the endpoint, by its marker
    assert seen["expect_block"] is False                              # the old block was seen up: nothing in flight
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and res.partition == "compute"


async def test_a_switch_away_from_a_block_still_being_requested_keeps_the_stop_looking_for_its_pilot(monkeypatch):
    # spend confirmed, the block never seen up (its sbatch may not have landed), then an unconfirmed switch: the stop
    # must still poll for that pilot (the 0.1.14 stop-during-provisioning race), not take "released none" as done
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "provisioning"
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["expect_block"] = expect_block
        return True, "released 7"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    await _stop_endpoint(app)
    assert seen["expect_block"] is True


async def test_an_ssh_switch_banks_the_old_block_to_the_latest_it_can_bill():
    # nothing cancels it, so it bills until its idle window after its last task (+ the release bound): an upper bound
    app = await _warm()
    app.charge_factor = 1.0
    rt = app.shapes["compute"]
    rt.warm_since -= 3600                                              # the old block has billed an hour
    await _ensure_endpoint_up(app, partition="compute")
    assert rt.warm_since is None                                       # no clock runs while the user is asked
    expected_s = 3600 + app.profile.max_idletime_s + warmth._RELEASE_BOUND_S
    assert server._total_session_spend(app) == pytest.approx(expected_s / 3600, abs=0.01)


def _mep_app(job: str = "100") -> AppCtx:
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=_Status)
    fac.max_idletime_s = 600
    app = AppCtx(facility=fac, profile=Profile())
    app.state = EndpointState(endpoint_id=fac.endpoint_id, reused=True)
    app.built = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "ok", ""), canary_result=_ok(job))

    app.runner_factory = factory
    return app


async def test_a_switch_on_a_facility_endpoint_asks_again_and_names_the_block_it_leaves():
    app = _mep_app()
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert (await _run_shell(app, "true")).phase == "complete"
    built = len(app.built)
    res = await _ensure_endpoint_up(app, partition="other")
    assert res.status == "needs_confirmation" and len(app.built) == built
    # #178's words for a block a facility endpoint lets go of, and the block it remembered for a later stop
    assert "scheduler job 100, was not cancelled — no cancel channel on a facility endpoint" in res.notice
    assert "`scancel 100` on that facility ends it" in res.notice and "stop_endpoint names it" in res.notice
    assert "partition 'main'" in res.notice
    assert [(b.job, b.partition, b.how) for b in app.left_blocks] == [("100", "main", "switch")]
    stop = await _stop_endpoint(app)
    assert stop.block_job_id == "100" and "by an earlier switch" in stop.notice


# --- a stop keeps the session's partition and account ---------------------------------------------------------------


async def _ssh_stop(app, monkeypatch):
    async def release(a, eid, *rest, expect_block=False, **kw):
        return True, "released 112"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    return await _stop_endpoint(app)


async def test_a_stop_keeps_the_chosen_partition_and_account(monkeypatch):
    app = await _warm()
    assert (await _ssh_stop(app, monkeypatch)).status == "down"
    assert "compute" not in app.shapes
    res = await _ensure_endpoint_up(app)
    # the spend question is asked again (a fresh shape), for the terms the session chose
    assert res.status == "needs_confirmation" and res.partition == "debug" and res.account == "hpcb"
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert app.built[-1]["partition"] == "debug" and app.built[-1]["account"] == "hpcb"
    rt = app.shapes["compute"]
    assert rt.block_job == "112" and warmth._guard_job(rt) == "112"   # #186's guard covers the new block


async def test_a_facility_endpoint_stop_keeps_them_too():
    app = _mep_app()
    assert (await _ensure_endpoint_up(app, partition="gpu", account="proj", confirm_spend=True)).status == "up"
    assert (await _stop_endpoint(app)).status == "draining"
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and res.partition == "gpu" and res.account == "proj"
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert app.built[-1]["partition"] == "gpu" and app.built[-1]["account"] == "proj"
    assert app.left_blocks == []                                       # #178: the same terms' block took it over


async def test_a_new_connect_forgets_them(monkeypatch):
    app = await _warm()
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    f2 = FakeFacility()
    f2.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: f2)

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout="")

    await connect._connect_facility(app, "anvil", run_login=run_login)
    assert _shape_runtime(app, "compute").user_endpoint_config.get("partition") is None


async def test_dropping_the_binding_forgets_them():
    app = await _warm()
    warmth._drop_all_shapes(app, bank=True)
    assert _shape_runtime(app, "compute").user_endpoint_config.get("account") is None


async def test_a_new_globus_login_keeps_them(monkeypatch):
    # chosen: a new login resets per-identity VERDICTS (no-account, the mapped user), not the session's terms — the live
    # shape keeps its account across a login, so a stopped one does too; a start still asks for spend again
    app = await _warm()
    await _ssh_stop(app, monkeypatch)
    warmth._forget_identity_verdicts(app)
    res = await _ensure_endpoint_up(app)
    assert res.status == "needs_confirmation" and res.partition == "debug" and res.account == "hpcb"


# --- the old block after a switch: its tasks, a switch back, its fate ----------------------------------------------


@pytest.fixture
def clock(monkeypatch):
    """A fake time.monotonic the test moves by hand."""
    c = [100000.0]
    monkeypatch.setattr(time, "monotonic", lambda: c[0])
    return c


async def test_a_task_that_finished_on_the_old_block_never_vouches_for_the_new_one():
    # finished unpolled on job 112, polled after the switch and its confirmation: it used to mark the still-queued new
    # block warm, so the next command ran no check, adopted a block with no job, and the wrong-block guard was off
    app = await _warm()
    rt = app.shapes["compute"]
    old_runner = rt.runner
    old_runner.pending = True
    out = await _run_shell(app, "sleep 200")
    assert out.phase == "running"
    old_runner.futures[-1].finish(_Res(0, "done", ""))                # it finished on job 112; nobody polled it yet
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "ok", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    new_runner = rt.runner
    polled = await _poll_task(app, out.task_id)
    assert polled.phase == "complete" and polled.block_state == "cold"   # its result, but no news about the new block
    assert rt.warm_confirmed_at is None and warmth._last_activity(app, "compute", rt) is None
    canaries = new_runner.canaries
    nxt = await _run_shell(app, "hostname")
    assert nxt.phase == "cold_start" and new_runner.canaries == canaries + 1 and rt.block_since is None


async def test_a_switch_back_to_the_confirmed_terms_says_so():
    app = await _warm()
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    res = await _ensure_endpoint_up(app, partition="debug")
    assert res.status == "needs_confirmation" and res.partition == "debug"
    assert "back to the ones it covered (partition 'debug', account 'hpcb')" in res.notice
    assert "different block" not in res.notice and "Nothing cancels" not in res.notice


async def test_a_switch_back_answered_by_the_same_job_keeps_its_age_bills_fresh_and_never_under(clock):
    t0 = clock[0]
    app = await _warm()                                                # job 112, confirmed warm at t0
    app.charge_factor = 1.0
    rt = app.shapes["compute"]
    clock[0] = t0 + 100
    await _ensure_endpoint_up(app, partition="compute")                # banks 112 once, to its latest end
    clock[0] = t0 + 200
    await _ensure_endpoint_up(app, partition="debug")
    clock[0] = t0 + 300
    res = await _ensure_endpoint_up(app, confirm_spend=True)           # job 112 answers again
    assert res.status == "up" and rt.block_job == "112" and rt.block_since == t0   # its walltime counts from t0
    assert rt.warm_since == t0 + 300                                   # it bills fresh from this confirmation
    assert server._total_session_spend(app) * 3600 >= 300              # nothing since t0 went unbilled
    clock[0] = t0 + 900
    assert server._total_session_spend(app) * 3600 >= 900


async def test_a_new_block_with_no_job_id_on_other_terms_is_not_dated_as_the_one_left(clock):
    # with no job id to compare, a block answering before the left block's latest end would inherit its age — but
    # only a block of the same terms can be that block
    t0 = clock[0]
    app = await _warm()
    rt = app.shapes["compute"]
    clock[0] = t0 + 100
    await _ensure_endpoint_up(app, partition="compute")
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "ok", ""),
                                                                                   canary_result=_ok(None))
    clock[0] = t0 + 200
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.block_since == t0 + 200


async def test_a_switch_from_a_block_seen_up_leaves_no_pilot_in_flight(monkeypatch):
    # a voided confirmation (a failed dispatch, a rebuild, a new login) is not a block that was never seen up: a stop
    # must not poll the long budget and say it is "still being submitted"
    app = await _warm()
    rt = app.shapes["compute"]
    warmth._void_warm(rt)
    await _ensure_endpoint_up(app, partition="compute")
    assert rt.switch_unseen is False
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["expect_block"] = expect_block
        return True, "released 112"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    await _stop_endpoint(app)
    assert seen["expect_block"] is False


async def test_a_voided_confirmation_of_a_block_seen_up_does_not_make_the_stop_wait(monkeypatch):
    app = await _warm()
    warmth._void_warm(app.shapes["compute"])
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["expect_block"] = expect_block
        return True, "released 112"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    await _stop_endpoint(app)
    assert seen["expect_block"] is False


async def test_a_confirmation_ends_the_wait_for_a_switched_away_pilot(monkeypatch):
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    runner = _FakeRunner("fake-eid", _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "provisioning"
    assert (await _ensure_endpoint_up(app, partition="compute")).status == "needs_confirmation"
    runner._canary = _ok("113")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"   # the new block is up and confirmed
    seen = {}

    async def release(a, eid, *rest, expect_block=False, **kw):
        seen["expect_block"] = expect_block
        return True, "released 113"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    await _stop_endpoint(app)
    assert seen["expect_block"] is False


async def test_a_facility_switch_after_a_kicked_reap_names_the_replacement_it_leaves():
    app = _mep_app("100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.runner._canary = _ok("101")                                     # job 100 was replaced: job 101 answers the check
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "needs_confirmation" and rt.reap_kicked
    res = await _ensure_endpoint_up(app, partition="other")
    assert res.status == "needs_confirmation"
    assert "That block, scheduler job 101, was not cancelled" in res.notice and "`scancel 101`" in res.notice
    assert [(b.job, b.how) for b in app.left_blocks] == [("101", "switch")]


async def test_a_switch_confirmed_in_the_same_call_says_once_what_became_of_the_old_block():
    app = await _warm()
    res = await _ensure_endpoint_up(app, partition="compute", confirm_spend=True)
    assert res.status == "up"
    assert res.notice.startswith("This confirmation is for a new block: the partition or account changed from "
                                 "partition 'debug', account 'hpcb'. Nothing cancelled the previous block")
    assert "stop_endpoint cancels every block of this endpoint, this new one included" in res.notice
    assert "This confirmation is for a new block" not in (await _ensure_endpoint_up(app)).notice   # once
    mep = _mep_app("100")
    assert (await _ensure_endpoint_up(mep, confirm_spend=True)).status == "up"
    res = await _ensure_endpoint_up(mep, partition="other", confirm_spend=True)
    assert "That block, scheduler job 100, was not cancelled" in res.notice and "`scancel 100`" in res.notice


@pytest.mark.parametrize("kind", ["block", "check"])
def test_a_reconnect_and_a_switch_name_a_left_block_in_the_same_words(kind):
    left = LeftBlock(endpoint_id="e", job="7.pbs01" if kind == "block" else None, until=None, left_at=0.0, how="switch",
                     kind=kind, partition="debug")
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=_Status)
    fac.scheduler = "pbs"
    app = AppCtx(facility=fac, profile=Profile())
    clause = notices._left_block_clause(left, "pbs")
    assert clause in connect._prior_note(0.0, left, "pbs", same=True, old_mep=True)
    switch = notices._left_block_clause(left, "pbs", subject="that block")
    assert switch[1:] in notices._switch_fate(app, left, confirmed=False)       # (capitalised there)
    assert clause.split("was not cancelled")[1] == switch.split("was not cancelled")[1]
    assert ("`qdel 7.pbs01` on that facility ends it" in clause) is (kind == "block")
    assert ("a block the facility starts for it bills" in clause) is (kind == "check")


async def test_the_choice_never_reaches_the_login_shape():
    app = await _warm()
    await _ensure_endpoint_up(app, partition="compute", account="hpcb-gpu", confirm_spend=True)
    login = warmth._shape_runtime(app, "login").user_endpoint_config
    assert "partition" not in login and "account" not in login


# --- a registry facility's scheduler directives cannot be set per session ---------------------------------------------


@pytest.mark.parametrize("rows", ["H 7.pbs01 - held: needs -l filesystems\n", "P 7 - AssocGrpSubmitJobsLimit\n"],
                         ids=["held", "pending-for-good"])
async def test_a_held_pilot_offers_details_only_for_a_facility_you_bring(rows):
    def app_for(machine, byo):
        app = AppCtx(facility=FakeFacility(), profile=Profile())
        app.machine = machine
        if byo:
            app.session_facilities[machine] = binding._entry_from_details(machine, FacilityDetails(
                ssh_host="login.example.edu", interface="ib0", env_setup="module load x",
                scratch_root="/scratch/{user}", partition="batch"))
        return app

    async def run_login(cmd):
        return ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout=rows)

    mine = await scheduler_ops._pilot_status_over_login(app_for("mine", True), "eid", 30.0, run_login)
    registry = await scheduler_ops._pilot_status_over_login(app_for("expanse", False), "eid", 30.0, run_login)
    assert mine is not None and registry is not None and mine[0] == registry[0] == "held"
    assert "connect_facility with details=" in mine[1]
    assert "connect_facility with details=" not in registry[1] and "registry entry" in registry[1]
    assert "cannot change per session yet" in registry[1]


def test_the_agent_facing_texts_promise_no_directive_a_registry_facility_cannot_take():
    late = notices._allocating_notice("gpuA40x4", 480, facility_mep=True)
    assert "scheduler_options" not in late and "cannot add to a facility endpoint's block yet" in late
    desc = FacilityDetails.model_fields["scheduler_options"].description or ""
    assert "Polaris" not in desc and "filesystems directive" in desc


# --- a job keeps its age on any number of hops; whatever block a switch knows up is billed -------------------------


def _answer(app, job):
    """Every runner built from now on answers its check from scheduler job `job`."""
    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _FakeRunner(eid, _Res(0, "ok", ""), canary_result=_ok(job))
    app.runner_factory = factory


async def test_a_job_met_again_after_two_switches_keeps_its_age_and_its_walltime_is_presumed(clock):
    # debug (job 112, up from t0) -> compute confirmed (job 200) -> back to debug confirmed, answered by 112 again: a
    # slot that dated a block by its terms held 200's block by then, so 112 got a fresh age and its real walltime end
    # passed unnoticed
    t0 = clock[0]
    app = await _warm()
    rt = app.shapes["compute"]
    rt.user_endpoint_config["walltime"] = "00:30:00"
    _answer(app, "200")
    clock[0] = t0 + 100
    assert (await _ensure_endpoint_up(app, partition="compute", confirm_spend=True)).status == "up"
    assert rt.block_job == "200"
    _answer(app, "112")
    clock[0] = t0 + 200
    assert (await _ensure_endpoint_up(app, partition="debug", confirm_spend=True)).status == "up"
    assert rt.block_job == "112" and rt.block_since == t0
    for k in range(1, 16):                                             # a command every 100 s keeps it busy
        clock[0] = t0 + 200 + 100 * k
        assert (await _run_shell(app, "true")).phase == "complete"
    clock[0] = t0 + 1810                                               # past job 112's real walltime end (t0 + 1800)
    runner = rt.runner
    sent = len(runner.commands)
    out = await _run_shell(app, "hostname")
    assert out.phase == "needs_confirmation" and "reached its walltime" in out.notice and len(runner.commands) == sent


async def test_the_same_on_a_facility_endpoint(clock):
    t0 = clock[0]
    app = _mep_app("100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    _answer(app, "200")
    clock[0] = t0 + 100
    assert (await _ensure_endpoint_up(app, partition="other", confirm_spend=True)).status == "up"
    _answer(app, "100")
    clock[0] = t0 + 200
    assert (await _ensure_endpoint_up(app, partition="main", confirm_spend=True)).status == "up"
    assert rt.block_job == "100" and rt.block_since == t0


async def test_a_job_met_again_never_resumes_a_clock_inside_what_a_switch_banked(clock):
    # facility endpoint: the idle presumption fires at the switch, which banks job 100 to its latest end; met again
    # after the switch back, it bills fresh from that confirmation (never from before that mark)
    t0 = clock[0]
    app = _mep_app("100")
    app.charge_factor = 1.0
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    clock[0] = t0 + 600 + 20                                           # "has been idle-released, or is about to be"
    await _ensure_endpoint_up(app, partition="other")
    clock[0] = t0 + 625
    await _ensure_endpoint_up(app, partition="main")
    clock[0] = t0 + 630
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.block_job == "100" and rt.block_since == t0 and rt.warm_since == t0 + 630


@pytest.mark.parametrize("kind", ["ssh", "mep"])
async def test_a_switch_bills_a_block_a_late_check_answer_proved_up(clock, kind):
    # spend confirmed, the check timed out, then answered late: that block is up and billing. A switch bills it from
    # its answer to the latest it can bill — on an SSH endpoint as on a facility endpoint
    t0 = clock[0]
    if kind == "ssh":
        f = FakeFacility()
        f.workers = 1
        app = AppCtx(facility=f, profile=Profile())
    else:
        app = _mep_app("100")
    app.charge_factor = 1.0
    late = CanaryResult(ok=True, worker_job="100", worker_user="u", answered_at=t0 + 30)

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "ok", ""), canary_result=CanaryResult(ok=False, error="timeout"))
        r.canary_answer = lambda: late
        return r

    app.runner_factory = factory
    assert (await _ensure_endpoint_up(app, partition="main", confirm_spend=True)).status == "provisioning"
    clock[0] = t0 + 60
    await _ensure_endpoint_up(app, partition="other")
    bound_s = 600 + warmth._RELEASE_BOUND_S                            # its idle window after its answer, + the bound
    assert server._total_session_spend(app) * 3600 == pytest.approx(bound_s, abs=0.01)


async def test_an_ssh_switch_bills_the_replacement_a_check_found(clock):
    # job 112 was replaced: job 113 answered a check (a kicked reap: not adopted, but up and billing). A switch away
    # bills it to the latest it can bill, as a facility endpoint's switch does
    t0 = clock[0]
    app = await _warm()
    app.charge_factor = 1.0
    rt = app.shapes["compute"]
    rt.runner._canary = _ok("113")
    clock[0] = t0 + 100
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "needs_confirmation" and rt.reap_kicked
    before = server._total_session_spend(app) * 3600
    clock[0] = t0 + 120
    await _ensure_endpoint_up(app, partition="compute")
    added = server._total_session_spend(app) * 3600 - before
    assert added == pytest.approx(600 + warmth._RELEASE_BOUND_S, abs=0.01)   # 113, from its answer at t0 + 100


async def test_flipping_partitions_over_counts_and_never_under_counts(clock):
    # accepted: a job met again bills fresh, so back-and-forth switching counts some time twice — never less than ran
    t0 = clock[0]
    app = await _warm()
    app.charge_factor = 1.0
    t = t0
    for _ in range(5):
        t += 60
        clock[0] = t
        await _ensure_endpoint_up(app, partition="compute")
        t += 10
        clock[0] = t
        await _ensure_endpoint_up(app, partition="debug")
        t += 10
        clock[0] = t
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    await _ensure_endpoint_up(app, partition="compute")
    assert server._total_session_spend(app) * 3600 >= (t - t0) + app.profile.max_idletime_s


async def test_a_switch_names_a_left_block_only_while_a_stop_would(clock):
    # past the left block's walltime end it is no longer named on a stop, nor in the re-ask
    t0 = clock[0]
    app = _mep_app("100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    clock[0] = t0 + 60
    first = await _ensure_endpoint_up(app, partition="other")
    assert "scheduler job 100, was not cancelled" in first.notice
    clock[0] = t0 + 3 * 3600                                           # past its walltime's end (02:00:00)
    out = await _run_shell(app, "true")
    assert out.phase == "needs_confirmation"
    assert "job 100" not in out.notice and "stop_endpoint names it" not in out.notice


async def test_a_refused_command_from_before_a_switch_is_taken_in_and_its_replacement_named(clock):
    # facility endpoint: a guarded command for job 100 ran into a replacement (job 101) and was refused; unpolled, the
    # agent switches partition. The switch takes the refusal in first, so it names 101 (billing), not 100 (gone)
    from hpc_bridge import session_shell

    t0 = clock[0]
    app = _mep_app("100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.runner.pending = True
    out = await _run_shell(app, "sleep 500")
    assert out.phase == "running"
    h = app.tasks[out.task_id]
    rt.runner.futures[-1].finish(_Res(session_shell.WRONG_BLOCK_EXIT, "",
                                      f"{session_shell.WRONG_BLOCK_MARKER} {h.guard.nonce} 101"))
    clock[0] = t0 + 30
    sw = await _ensure_endpoint_up(app, partition="other")
    assert "scheduler job 101, was not cancelled" in sw.notice and [b.job for b in app.left_blocks] == ["101"]
    polled = await _poll_task(app, out.task_id)
    assert polled.phase == "needs_confirmation" and "did NOT run" in polled.notice
    stop = await _stop_endpoint(app)
    assert stop.block_job_id == "101"


async def test_a_switch_after_a_reap_says_it_switched(clock):
    app = _mep_app("100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.runner._canary = _ok("101")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "needs_confirmation"
    res = await _ensure_endpoint_up(app, partition="other")
    assert "The partition or account was switched since, so a confirmation now starts a block on the new terms." in (
        res.notice) and "scheduler job 101, was not cancelled" in res.notice


async def test_a_pin_that_fails_names_no_machine(monkeypatch):
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry(account_required=True)]))
    monkeypatch.delenv("HPC_BRIDGE_ACCOUNT", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_ID", raising=False)
    with pytest.raises(RuntimeError, match="HPC_BRIDGE_ACCOUNT"):
        await binding._catalog_facility("globus:globus1")
    assert binding._PINNED == {}


def test_the_jobs_remembered_are_bounded_and_never_moved_later():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    last = str(warmth._JOBS_SEEN_MAX + 9)
    for i in range(warmth._JOBS_SEEN_MAX + 10):
        warmth._saw_job(app, str(i), float(i))
    assert len(app.job_seen_since) == warmth._JOBS_SEEN_MAX and (None, "0") not in app.job_seen_since
    warmth._saw_job(app, last, 1e9)                                    # never moved later
    assert app.job_seen_since[(None, last)] == warmth._JOBS_SEEN_MAX + 9


# --- a requeued job, a refusal nobody polled, a reconnect, the job map's bounds --------------------------------------


class _ClockRunner(_FakeRunner):
    """A fake runner whose check is answered now (the fake clock), by `self.job` — or, with `self.answered` set, handed
    back as a cached answer from then; with no job, not answered within its wait."""

    answered: float | None = None

    def __init__(self, eid, clock, job, **kw):
        super().__init__(eid, _Res(0, "ok", ""), **kw)
        self.clock, self.job = clock, job

    async def canary(self, timeout=8.0):
        self.canaries += 1
        if self.job is None:
            return CanaryResult(ok=False, error="timeout")
        return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9",
                            worker_job=self.job, worker_user="u",
                            answered_at=self.answered if self.answered is not None else self.clock[0])


def _answer_now(app, clock, job):
    """The current runner and every one built from now on answer their checks now, from scheduler job `job`."""
    def factory(eid, user_endpoint_config=None, **_kw):
        app.built.append(dict(user_endpoint_config or {}))
        return _ClockRunner(eid, clock, job)
    app.runner_factory = factory
    rt = app.shapes.get("compute")
    if rt is not None and rt.runner is not None:
        rt.runner.__class__ = _ClockRunner
        rt.runner.clock, rt.runner.job = clock, job


def _refuse(app, out, job):
    """Finish the pending poll handle `out` as the wrong-block guard's refusal on scheduler job `job`."""
    from hpc_bridge import session_shell

    h = app.tasks[out.task_id]
    app.shapes["compute"].runner.futures[-1].finish(
        _Res(session_shell.WRONG_BLOCK_EXIT, "", f"{session_shell.WRONG_BLOCK_MARKER} {h.guard.nonce} {job}"))
    return h


async def _refused_unpolled(app, clock, t0, job="113"):
    """A long command handed back as a poll handle, then refused on scheduler job `job` (the block was replaced) at
    t0 + 20; nobody polls it."""
    rt = app.shapes["compute"]
    rt.runner.pending = True
    clock[0] = t0 + 10
    out = await _run_shell(app, "sleep 500")
    assert out.phase == "running"
    clock[0] = t0 + 20
    return out, _refuse(app, out, job)


@pytest.mark.parametrize("kind", ["ssh", "mep"])
async def test_a_job_requeued_under_its_id_runs_a_fresh_walltime_and_bills_on(clock, kind):
    # Slurm PreemptMode=REQUEUE, a node-failure requeue, a PBS rerun: the same job id answers past its walltime counted
    # from its first proof. It used to stay dated t0: every confirmation came back "up", the next command re-asked
    # "reached its walltime", and the spend clock froze (banked to block_since + wall, already behind it)
    t0 = clock[0]
    app = await _warm() if kind == "ssh" else _mep_app("100")
    job = "112" if kind == "ssh" else "100"
    app.charge_factor = 1.0
    _answer_now(app, clock, job)
    if kind == "mep":
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.user_endpoint_config["walltime"] = "00:30:00"
    for t in range(300, 1501, 300):                                    # busy all along: no idle presumption
        clock[0] = t0 + t
        assert (await _run_shell(app, "true")).phase == "complete"
    clock[0] = t0 + 1810
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"   # its walltime, presumed
    clock[0] = t0 + 1840                                               # past its walltime + the kill grace
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"   # the requeued job answers
    assert rt.block_job == job and rt.block_since == t0 + 1840
    clock[0] = t0 + 1850
    assert (await _run_shell(app, "true")).phase == "complete"
    before = server._total_session_spend(app) * 3600
    clock[0] = t0 + 2450
    assert (await _run_shell(app, "true")).phase == "complete"
    assert server._total_session_spend(app) * 3600 - before == pytest.approx(600, abs=0.01)   # it bills on


@pytest.mark.parametrize("kind", ["ssh", "mep"])
async def test_a_switch_that_confirms_after_an_unpolled_refusal_answers_with_the_reap(clock, kind):
    # the refusal found job 113 replacing the confirmed block; the call that switches and confirms is the call that
    # finds it, so it answers needs_confirmation naming 113, as any call that finds a reap does
    t0 = clock[0]
    app = await _warm() if kind == "ssh" else _mep_app("100")
    _answer_now(app, clock, "112" if kind == "ssh" else "100")
    if kind == "mep":
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    out, _h = await _refused_unpolled(app, clock, t0)
    clock[0] = t0 + 60
    _answer_now(app, clock, "200")
    res = await _ensure_endpoint_up(app, partition="compute" if kind == "ssh" else "other", confirm_spend=True)
    assert res.status == "needs_confirmation" and "job 113" in res.notice and "is gone" in res.notice
    assert "The partition or account was switched since" in res.notice
    if kind == "mep":
        assert [b.job for b in app.left_blocks] == ["113"]
    polled = await _poll_task(app, out.task_id)
    assert "did NOT run" in polled.notice


async def test_a_stop_after_an_unpolled_refusal_names_the_replacement_and_bills_once(clock):
    t0 = clock[0]
    app = _mep_app("100")
    app.charge_factor = 1.0
    _answer_now(app, clock, "100")
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    out, _h = await _refused_unpolled(app, clock, t0, job="101")
    clock[0] = t0 + 60
    stop = await _stop_endpoint(app)
    assert stop.block_job_id == "101" and "`scancel 101`" in stop.notice and "scancel 100" not in stop.notice
    billed = server._total_session_spend(app)
    polled = await _poll_task(app, out.task_id)
    assert "did NOT run" in polled.notice
    assert server._total_session_spend(app) == pytest.approx(billed) and [b.job for b in app.left_blocks] == ["101"]


async def test_a_reconnect_to_the_same_endpoint_keeps_the_jobs_ages(monkeypatch, clock):
    from tests.test_mep_stop_names_block import _attached, _reconnect

    t0 = clock[0]
    app, _runner = await _attached(monkeypatch)
    app.built = []
    _answer_now(app, clock, "100")
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    clock[0] = t0 + 3000
    await _reconnect(app)
    clock[0] = t0 + 3010
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    assert app.shapes["compute"].block_since == t0                    # job 100 keeps its age across the reconnect
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: FakeFacility())
    monkeypatch.setattr(warmth, "_provision", _no_provision)
    await server._connect_facility(app, "globus1")                     # a bind elsewhere drops them
    assert app.job_seen_since == {}


async def _no_provision(app, shape, **_kw):
    return "warm"


async def test_a_refusal_dates_its_replacement_when_it_refused(clock):
    t0 = clock[0]
    app = await _warm()
    _answer_now(app, clock, "112")
    _out, h = await _refused_unpolled(app, clock, t0)
    clock[0] = t0 + 400
    await _ensure_endpoint_up(app, partition="compute")                # the switch takes the refusal in
    assert app.job_seen_since[(app.state.endpoint_id, "113")] == h.done_at == t0 + 20


async def test_an_ssh_switch_after_a_reap_says_it_switched(clock):
    t0 = clock[0]
    app = await _warm()
    _answer_now(app, clock, "113")
    clock[0] = t0 + 100
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "needs_confirmation"
    clock[0] = t0 + 120
    res = await _ensure_endpoint_up(app, partition="compute")
    assert "The partition or account was switched since, so a confirmation now starts a block on the new terms." in (
        res.notice)


async def test_the_bound_on_remembered_jobs_never_drops_the_confirmed_blocks(clock):
    t0 = clock[0]
    app = await _warm()
    key = (app.state.endpoint_id, "112")
    assert key in app.job_seen_since and app.shapes["compute"].block_job == "112"
    for i in range(warmth._JOBS_SEEN_MAX + 5):
        warmth._saw_job(app, str(5000 + i), t0 + 1 + i)
    assert key in app.job_seen_since and len(app.job_seen_since) == warmth._JOBS_SEEN_MAX


async def test_a_pin_with_a_bad_ssh_host_names_no_machine(monkeypatch):
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([_ssh_entry()]))
    monkeypatch.setenv("HPC_BRIDGE_ACCOUNT", "chi152")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "-oProxyCommand=evil")
    with pytest.raises(ValueError):
        await binding._catalog_facility("expanse")
    assert binding._PINNED == {}


@pytest.mark.parametrize("kind", ["ssh", "mep"])
@pytest.mark.parametrize("how", ["cached_answer", "kill_slack"])
async def test_a_job_at_its_hard_end_is_not_taken_for_a_requeued_one(clock, kind, how):
    # job 112 (30 min walltime, first proven at t0) is NOT requeued; a check at t0+1790 times out (a kicked reap). 112
    # answers the confirm inside the scheduler's kill grace: at t0+1810 itself, or that answer handed back cached at
    # t0+1835 (25 s old, inside the runner's freshness window; the call's own clock is past walltime + grace). It must
    # not be re-dated as a fresh walltime: the next command re-asks and is not sent to a block at its hard end
    t0 = clock[0]
    app = await _warm() if kind == "ssh" else _mep_app("112")
    _answer_now(app, clock, "112")
    if kind == "mep":
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    rt.user_endpoint_config["walltime"] = "00:30:00"
    for t in range(300, 1501, 300):
        clock[0] = t0 + t
        assert (await _run_shell(app, "true")).phase == "complete"
    clock[0] = t0 + 1790
    rt.runner.job = None                                               # the check is not answered within its wait
    assert (await _ensure_endpoint_up(app)).status == "needs_confirmation" and rt.reaped
    clock[0] = t0 + 1810
    rt.runner.job = "112"
    if how == "cached_answer":
        rt.runner.answered, clock[0] = t0 + 1810, t0 + 1835
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "up"
    assert rt.block_job == "112" and rt.block_since == t0                # its first proof: not a fresh walltime
    clock[0] += 10
    sent = len(rt.runner.commands)
    out = await _run_shell(app, "hostname")
    assert out.phase == "needs_confirmation" and "reached its walltime" in out.notice and len(rt.runner.commands) == sent


async def test_a_switch_back_to_the_terms_of_a_reap_does_not_claim_new_terms(clock):
    # a reap on debug (113 replaced 112 there) -> a switch to compute says it switched -> a switch back to debug: these
    # are 113's terms again, so the re-ask no longer says a confirmation starts a block on new terms
    t0 = clock[0]
    app = await _warm()
    _answer_now(app, clock, "113")
    clock[0] = t0 + 100
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "needs_confirmation"
    away = await _ensure_endpoint_up(app, partition="compute")
    assert "The partition or account was switched since" in away.notice
    back = await _ensure_endpoint_up(app, partition="debug")
    assert back.status == "needs_confirmation" and "job 113" in back.notice
    assert "switched since" not in back.notice and "new terms" not in back.notice
