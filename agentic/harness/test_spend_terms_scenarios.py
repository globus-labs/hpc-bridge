"""Hermetic: the spend-terms scenarios (term_switch_reasks, preset_account_scoped, ssh_account_floor) — their graders on
synthetic traces (a run the plugin gets right passes, one an unfixed plugin would produce fails, a broken premise is
labelled as one), their REQUIRES against the profile manifests, and the local catalog ssh_account_floor writes."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def _connect(notice="attached", phase="needs_account"):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "x"},
                       {"phase": phase, "reused": True, "allocations": [], "notice": notice})


def _ensure(status, notice="", reports=None, **inp):
    """An ensure_endpoint_up: `inp` is what the agent passed, `reports` the account the result names as the block's
    (default: the agent's own, when it passed one)."""
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "compute", **inp},
                       {"status": status, "notice": notice, "account": reports or inp.get("account")})


def _run(stdout, phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "echo …"},
                       {"phase": phase, "stdout": stdout})


def _stop(status="draining", notice="NO CANCEL WAS ISSUED: …"):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "notice": notice})


# --- term_switch_reasks ---------------------------------------------------------------------------------------------

_REASK = ("Confirm spend again with the user: the partition or account has changed since spend was confirmed, and that "
          "is a different block (the earlier confirmation covered only the one on partition 'debug', account 'hpcb'). "
          "That block, scheduler job 41, was not cancelled — no cancel channel on a facility endpoint: it bills until "
          "that facility's idle-release (`scancel 41` on that facility ends it); stop_endpoint names it. …")
_REAPED = ("Confirm spend again with the user: the previous block has been idle-released, or is about to be (no task for "
           "~200 s, past the 180 s idle window). The earlier confirmation covered that block only; a new block bills "
           "again. That block, scheduler job 41, was not cancelled — … (`scancel 41` on that facility ends it) …")


def _switch_trace(*, switch_status="needs_confirmation", notice=_REASK, ran_between=False, new_part="compute",
                  new_job="42"):
    calls = [_connect(),
             _ensure("up", partition="debug", account="hpcb", confirm_spend=True),
             _run("JOB=41 PART=debug\nc1\n"),
             _ensure(switch_status, notice, partition="compute")]
    if ran_between:
        calls.append(_run("JOB=42 PART=compute\nc2\n"))
    calls += [_ensure("up", partition="compute", confirm_spend=True), _run(f"JOB={new_job} PART={new_part}\nc2\n"),
              _stop()]
    return Trace(calls, ["The switch asked for spend again; job 41 was not cancelled (`scancel 41`)."])


def test_term_switch_reasks_fails_on_the_unfixed_plugin():
    import term_switch_reasks as sc
    good = _switch_trace()
    for g in (sc.switch_reasked, sc.switch_names_old_block, sc.nothing_ran_before_the_confirm,
              sc.ran_on_the_new_partition, sc.mep_stop_is_draining_only):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: the unconfirmed switch started the compute block under the debug block's confirmation
    main = _switch_trace(switch_status="up", notice="worker live on c2", ran_between=True)
    assert not sc.switch_reasked(main).ok and not sc.switch_names_old_block(main).ok
    assert not sc.nothing_ran_before_the_confirm(main).ok
    # a re-ask that does not name the block it leaves, or a "new" block that is the old one, fails its grader
    assert not sc.switch_names_old_block(_switch_trace(notice="Confirm spend again … different block.")).ok
    assert not sc.ran_on_the_new_partition(_switch_trace(new_job="41")).ok
    assert not sc.ran_on_the_new_partition(_switch_trace(new_part="debug")).ok


def test_term_switch_reasks_calls_a_block_gone_before_the_switch_inconclusive():
    import term_switch_reasks as sc
    late = _switch_trace(notice=_REAPED)
    for g in (sc.switch_reasked, sc.switch_names_old_block):
        r = g(late)
        assert not r.ok and r.detail.startswith("inconclusive: the old block idle-released before the switch"), r


def test_term_switch_reasks_reads_its_marker_line_however_spaced():
    import term_switch_reasks as sc
    t = Trace([_run("JOB=41    PART=debug\nc1\n")])
    assert sc._runs(t) == [(0, "41", "debug")] and sc.NEEDS_COMPUTE_NODE == 2


def test_term_switch_reasks_fails_when_its_premise_never_happened():
    import term_switch_reasks as sc
    confirmed_only = Trace([_connect(), _ensure("up", partition="debug", account="hpcb", confirm_spend=True),
                            _run("JOB=41 PART=debug\n"), _ensure("up", partition="compute", confirm_spend=True),
                            _run("JOB=42 PART=compute\n"), _stop()])
    r = sc.switch_reasked(confirmed_only)
    assert not r.ok and "premise" in r.detail
    assert not sc.switch_reasked(Trace([_connect(), _stop()])).ok


# --- preset_account_scoped ------------------------------------------------------------------------------------------

_CLAUSE = "(HPC_BRIDGE_ACCOUNT applies only to the facility HPC_BRIDGE_MACHINE names, and that is unset, so it was not used here.) attached …"


def _preset_trace(*, clause=_CLAUSE, bare_status="needs_account", bare_account=None, printed="hpcb"):
    calls = [_connect(clause), _ensure(bare_status, "this facility requires an allocation account …",
                                       reports=bare_account, partition="compute", confirm_spend=True)]
    if bare_status == "needs_account":
        calls.append(_ensure("up", partition="compute", account="hpcb", confirm_spend=True))
    calls += [_run(f"JOB=7 ACCOUNT={printed}\nc2\n"), _stop()]
    return Trace(calls, [f"The block was charged to {printed}."])


def test_preset_account_scoped_fails_on_the_unfixed_plugin():
    import preset_account_scoped as sc
    good = _preset_trace()
    for g in (sc.preset_clause_shown, sc.floor_asked_without_the_preset, sc.preset_not_charged,
              sc.charged_the_named_account):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: no clause, the preset satisfied the floor, and the block was charged to it
    main = _preset_trace(clause="attached …", bare_status="up", bare_account="hpcb-gpu", printed="hpcb-gpu")
    for g in (sc.preset_clause_shown, sc.floor_asked_without_the_preset, sc.preset_not_charged,
              sc.charged_the_named_account):
        assert not g(main).ok, g.__name__
    # the clause must never print the preset's value
    leaky = _preset_trace(clause="(HPC_BRIDGE_ACCOUNT=hpcb-gpu applies only to … ) attached")
    assert not sc.preset_clause_shown(leaky).ok


def test_preset_account_scoped_tells_the_agents_own_choice_from_a_leak():
    # a CLI operator's agent can read EXTRA_ENV and pass the preset itself: reported apart, not the product's leak
    import preset_account_scoped as sc
    calls = [_connect(_CLAUSE),
             _ensure("needs_account", "this facility requires an allocation account …", partition="compute",
                     confirm_spend=True),
             _ensure("up", partition="compute", account="hpcb-gpu", confirm_spend=True),
             _run("JOB=7 ACCOUNT=hpcb-gpu\nc2\n"), _stop()]
    t = Trace(calls, [])
    assert sc.preset_not_charged(t).ok and "passed it itself at call 2" in sc.preset_not_charged(t).detail
    assert not sc.agent_chose_the_preset(t).ok and "agent_chose_the_preset" not in sc.EXPECT_OK


def test_preset_account_scoped_sets_the_preset_for_the_server_only_and_leaves_the_machine_unset():
    import preset_account_scoped as sc
    assert sc.EXTRA_ENV == {"HPC_BRIDGE_ACCOUNT": "hpcb-gpu"}
    (grant,) = sc.ADMIN_SETUP
    assert grant.endswith(sc._HAS_GRANT) and "|| true" not in grant      # a grant that did not take fails the setup
    assert any("delete user" in c and "hpcb-gpu" in c and "sleep" in c for c in sc.ADMIN_CLEANUP)   # retried
    world = {p["name"]: p for p in sc.POSTCHECKS}
    assert world["no_job_charged_to_the_preset"]["expect_absent"] == "hpcb-gpu"
    assert sc._START in sc.SETUP[0] and sc._START in world["no_job_charged_to_the_preset"]["cmd"]
    assert "Submit" in world["no_job_charged_to_the_preset"]["cmd"]      # jobs submitted since the cell started


# --- ssh_account_floor ----------------------------------------------------------------------------------------------

_FLOOR = ("this facility requires an allocation account for every block and none is set — nothing was started. A LOGIN "
          "NAME is not an account: … the facility's allocation tool — it runs free on the login shape, run_shell(…, "
          "shape='login') — …")


def test_ssh_account_floor_fails_on_the_unfixed_plugin():
    import ssh_account_floor as sc
    connect = _connect("login node is up; this facility has no allocation listing …")
    good = Trace([connect, _ensure("needs_account", _FLOOR, partition="compute", confirm_spend=True),
                  _stop("down", "compute block released over AMQP (released none) …")], ["It needs an account."])
    for g in (sc.ssh_floor_answered, sc.no_start_without_an_account, sc.no_account_guessed):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: the start went through with no account and work ran on the block
    main = Trace([connect, _ensure("provisioning", "allocating nodes…", partition="compute", confirm_spend=True),
                  _ensure("up", "worker live", partition="compute"), _run("JOB=9 ACCOUNT=hpcb\n"),
                  _stop("down")], [])
    assert not sc.ssh_floor_answered(main).ok and not sc.no_start_without_an_account(main).ok
    guessed = Trace([connect, _ensure("needs_account", _FLOOR, confirm_spend=True),
                     _ensure("up", account="hpcb", confirm_spend=True), _stop("down")])
    assert not sc.no_account_guessed(guessed).ok
    assert not sc.ssh_floor_answered(Trace([connect, _stop("down")])).ok   # premise


def test_ssh_account_floor_calls_a_details_connect_a_broken_premise():
    # details= defines a bring-your-own facility in place of the entry: no curated account_required, so a start that
    # goes through is not the product's failure
    import ssh_account_floor as sc
    byo = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake-ssh-site", "details": {"ssh_host": "login"}},
                      {"phase": "needs_account", "notice": "login node is up …"})
    t = Trace([byo, _ensure("up", "worker live", partition="compute", confirm_spend=True), _stop("down")])
    for g in (sc.ssh_floor_answered, sc.no_start_without_an_account):
        r = g(t)
        assert not r.ok and r.detail.startswith("premise: agent bypassed the catalog entry (details="), r


def test_ssh_account_floor_points_the_server_at_the_catalog_its_cell_writes():
    import ssh_account_floor as sc
    assert sc.EXTRA_ENV["HPC_BRIDGE_CATALOG_FILE"] == sc._CATALOG
    (cmd,) = sc.LOCAL_SETUP
    assert "agentic/harness/fake_ssh_catalog.py" in cmd and sc._CATALOG in cmd and f"--id {sc.FACILITY_ID}" in cmd
    assert sc.NEEDS_COMPUTE_NODE is False and sc.POSTCHECKS[0]["expect_absent"] == "parsl"
    assert "Submit" in sc.POSTCHECKS[0]["cmd"]                            # jobs submitted since the cell started


def test_the_written_catalog_is_an_account_required_ssh_entry_the_plugin_floors(tmp_path, monkeypatch):
    import fake_ssh_catalog

    from hpc_bridge import binding
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.facility.endpoint_venv import is_line

    out = tmp_path / "catalog.yaml"
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_NAME", raising=False)
    assert fake_ssh_catalog.main([str(out)]) == 2 and not out.exists()   # no run endpoint name: refuse, write nothing
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-run123")
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "login")
    assert fake_ssh_catalog.main([str(out), "--id", "fake-ssh-site"]) == 0
    import asyncio
    entry = asyncio.run(BundledCatalog(str(out)).get("fake-ssh-site"))
    assert entry is not None and entry.compute_mep_uuid is None and entry.ssh_host == "login"
    assert entry.account_required is True and entry.allocation is None and "hpcb" not in entry.description
    assert entry.compute.endpoint_name == "hpc-bridge-fake-run123"   # the harness tears it down by this name
    assert is_line(entry.compute.env_setup)                          # hpc-bridge's own line: built at the bootstrap
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-00")
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(tmp_path / "state"))
    fac = binding._facility_from_entry(entry, account="")
    assert fac.account_required is True and fac.profile.endpoint_name == "hpc-bridge-fake-run123"


# --- coupling -------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "profile", "not_on"), [
    ("term_switch_reasks", "mep", ("site", "default")),
    ("preset_account_scoped", "mep", ("site", "default")),
    ("ssh_account_floor", "site", ("default", "pbs", "totp")),
])
def test_each_runs_where_it_can_and_gates_only_checks_it_provides(name, profile, not_on):
    import importlib
    sc = importlib.import_module(name)
    assert sc.TARGETS == ("fake",)
    assert targets.meets(sc.REQUIRES, targets.load_profile(profile)["capabilities"])[0], targets.meets(
        sc.REQUIRES, targets.load_profile(profile)["capabilities"])
    for other in not_on:
        assert not targets.meets(sc.REQUIRES, targets.load_profile(other)["capabilities"])[0], (name, other)
    assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0]
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided
    assert sc.PERSONA is None and not getattr(sc, "MIDRUN_HOOKS", None)   # autonomous: the free operators drive it
