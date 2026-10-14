"""Five holes in the spend gate (2026-10-14), each RED on main before its fix.

1. The account floor held only on a facility multi-user endpoint: a registry SSH entry with `account_required` (Expanse)
   built a Slurm or PBS facility without it, so a confirmed start with no account submitted a block the scheduler could
   only reject. Now a facility built from a catalog entry carries the entry's `account_required` on every channel; the
   floor reads the account that would be sent (this session's choice, or the facility's configured default).
2. The env presets leaked to every facility: HPC_BRIDGE_ACCOUNT reached every catalogued facility on the agentic
   connect path, HPC_BRIDGE_PARTITION every SSH entry (bring-your-own too). Now each applies only to the facility
   HPC_BRIDGE_MACHINE names, and a connect elsewhere says in one clause that a set preset was not used.
3. A partition or account switch started a new billed block under the old confirmation. Now a real change on a
   confirmed block drops the confirmation: the switching call answers `needs_confirmation` unless it carries
   confirm_spend=True itself, and says what became of the previous block.
5. A stop forgot the session's partition and account (the compute shape was dropped whole), so the next block silently
   used the facility defaults. Now they survive a stop; a connect_facility or a teardown forgets them.

(4 is documentation only: docs/user/facilities.md no longer offers a per-session `scheduler_options`.)
"""
import time
from pathlib import Path

import pytest

from hpc_bridge import binding, connect, notices, scheduler_ops, server, warmth
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _run_shell, _shape_runtime, _stop_endpoint
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


# --- 1. the account floor on an SSH facility ------------------------------------------------------------------------


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


async def test_the_needs_account_notice_reads_right_on_an_ssh_facility(monkeypatch):
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


# --- 2. the env presets apply only to the facility HPC_BRIDGE_MACHINE names -----------------------------------------


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
    assert (f"HPC_BRIDGE_MACHINE={machine}" if machine else "HPC_BRIDGE_MACHINE names, and that is unset") in res.notice


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
    note = notices._unapplied_presets_note("delta", "ncsa:delta")
    assert note == ("(HPC_BRIDGE_ACCOUNT and HPC_BRIDGE_PARTITION apply only to the facility HPC_BRIDGE_MACHINE names, "
                    "and that is unset, so they were not used here.) ")


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


# --- 3. a partition or account switch asks for spend again ----------------------------------------------------------


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
    assert rt.switched is None


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
    assert "stop_endpoint first" in notice
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


async def test_an_ssh_switch_stops_the_old_blocks_clock_at_the_switch():
    app = await _warm()
    app.charge_factor = 1.0
    rt = app.shapes["compute"]
    rt.warm_since -= 3600                                              # the old block has billed an hour
    await _ensure_endpoint_up(app, partition="compute")
    assert rt.warm_since is None
    at_switch = server._total_session_spend(app)
    assert at_switch == pytest.approx(1.0, abs=0.01)                  # an hour, one node, banked at the switch


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
    # #188's words for a block a facility endpoint lets go of, and the block it remembered for a later stop
    assert "scheduler job 100, was not cancelled — no cancel channel on a facility endpoint" in res.notice
    assert "`scancel 100` on that facility ends it" in res.notice and "stop_endpoint names it" in res.notice
    assert "partition 'main'" in res.notice
    assert [(b.job, b.partition, b.how) for b in app.left_blocks] == [("100", "main", "switch")]
    stop = await _stop_endpoint(app)
    assert stop.block_job_id == "100" and "by an earlier switch" in stop.notice


# --- 5. a stop keeps the session's partition and account ------------------------------------------------------------


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
    assert app.left_blocks == []                                       # #188: the same terms' block took it over


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


async def test_a_teardown_forgets_them():
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
