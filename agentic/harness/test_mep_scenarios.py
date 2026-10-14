"""Hermetic: the fake-MEP scenarios (mep profile) — graders on synthetic traces and their REQUIRES vs the manifests."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def _connect(phase, reused=True, notice=""):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake-mep-strict"},
                       {"phase": phase, "reused": reused, "allocations": [], "notice": notice})


def _ensure(status, notice="", **inp):
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", inp, {"status": status, "notice": notice})


def _run(shape, stdout, phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": shape, "command": "hostname; whoami"}, {"phase": phase, "stdout": stdout})


def _stop(status):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "notice": "x"})


def test_fake_mep_compute_graders():
    import fake_mep_compute as fm
    good = Trace([_connect("needs_account", notice="attached to the facility's multi-user endpoint … keys not in the facility's template schema were dropped: compute, interface, worker_init"),
                  _ensure("provisioning", "allocating", shape="compute", partition="compute", account="hpcb", confirm_spend=True),
                  _ensure("up", shape="compute"), _run("compute", "c2\nhpcbmep\n"), _stop("draining")], ["Ran on c2 as hpcbmep; stop: draining (final)."])
    assert fm.mep_identity_mapped(good).ok and fm.strict_keys_dropped(good).ok
    assert fm.mep_zero_ssh(good).ok and fm.mep_stop_is_draining_only(good).ok and fm.mep_no_login_shape_submit(good).ok
    wrong_user = Trace([_ensure("up", shape="compute"), _run("compute", "c2\nhpcbridge-test-00\n")])
    assert not fm.mep_identity_mapped(wrong_user).ok
    assert not fm.strict_keys_dropped(Trace([_connect("needs_account", notice="attached")])).ok
    assert fm.MAPPED_USER == "hpcbmep" and fm.WARM_BLOCK_USER == "hpcbmep" and fm.SERIAL is True
    assert fm.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"] and fm.POSTCHECK_DELAY_S == 240
    assert fm.FACILITY_ID == "fake-mep-strict"


def _stop_result(job="1226", *, notice=None, spend_stopped=False, status="draining"):
    n = notice if notice is not None else (
        f"NO CANCEL WAS ISSUED: hpc-bridge has no cancel channel on a facility endpoint. Scheduler job {job} may still "
        f"be running and billing until 21:35 (in ~11 min) at the latest, possibly sooner (not verified). The user ends it "
        f"now with `scancel {job}`, or checks it with `squeue -j {job}`, on the facility; relay both. …")
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "block_job_id": job,
                                                           "spend_stopped": spend_stopped, "notice": n})


def test_fake_mep_stop_names_block_graders():
    import fake_mep_stop_names_block as sc
    run = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "echo JOB=$SLURM_JOB_ID; hostname; whoami"},
                      {"phase": "complete", "stdout": "JOB=1226\nc2\nhpcbmep\n"})
    said = ["Job 1226 is still running and billing until 14:32. End it with `scancel 1226` on the facility."]
    good = Trace([_connect("needs_account"), _ensure("up", shape="compute", confirm_spend=True), run, _stop_result()], said)
    for g in (sc.mep_stop_names_block, sc.no_cancel_claimed, sc.cancel_command_given, sc.no_probe_after_stop):
        assert g(good).ok, (g.__name__, g(good).detail)
    # the wrong job, or none: not named
    assert not sc.mep_stop_names_block(Trace([run, _stop_result("1225")])).ok
    assert not sc.mep_stop_names_block(Trace([run, _stop_result(None, notice="NO CANCEL WAS ISSUED (…). A block …")])).ok
    assert not sc.mep_stop_names_block(Trace([run])).ok   # no stop
    unnamed = _stop_result(None, notice="NO CANCEL WAS ISSUED (…). Scheduler job 1226 … `scancel 1226` …")
    assert not sc.mep_stop_names_block(Trace([run, unnamed])).ok   # in the text only: block_job_id must carry it
    # the old stop: no lead, or a claim that spend stopped
    old = _stop_result(notice="stopped submitting; the block is DRAINING. …", spend_stopped=None)
    assert not sc.no_cancel_claimed(Trace([run, old])).ok
    assert not sc.no_cancel_claimed(Trace([run, _stop_result(spend_stopped=True)])).ok
    # the agent never relayed the command
    assert not sc.cancel_command_given(Trace(good.calls, ["Stopped; it is draining."])).ok
    # what could reach the block after the stop (a run, a reset, a confirmed ensure) is flagged; an unconfirmed ensure
    # and a reconnect send nothing, so they are not; a refused stop is not "the" stop
    reset = ToolCall.of("mcp__endpoint__reset_session", {"shape": "compute"}, {"phase": "needs_confirmation"})
    for probe in (_run("compute", "c2"), reset, _ensure("up", shape="compute", confirm_spend=True),
                  _ensure("up", shape="compute", confirm_spend="True")):
        assert not sc.no_probe_after_stop(Trace(good.calls + [probe], said)).ok, probe.name
    for free in (_ensure("needs_confirmation", shape="compute"), _connect("needs_account")):
        assert sc.no_probe_after_stop(Trace(good.calls + [free], said)).ok, free.name
    refused = _stop_result(status="up", notice="can't stop yet: task(s) compute-1 are still running")
    late = Trace([run, refused, ToolCall.of("mcp__endpoint__poll_task", {"task_id": "compute-1"}, {"phase": "complete"}),
                  _stop_result()], said)
    assert sc.no_probe_after_stop(late).ok and sc.mep_stop_names_block(late).ok
    # the world hook: the block is still in the mapped account's queue right after the stop
    h = sc.MIDRUN_HOOKS[0]
    assert h["after_tool"] == "stop_endpoint" and "squeue -u hpcbmep" in h["cmd"] and h["expect_any_host"] == "parsl"
    assert sc.FACILITY_ID == "fake-mep-strict" and sc.MAPPED_USER == "hpcbmep" and sc.SERIAL is True
    assert sc.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"]


def test_fake_mep_scenarios_need_the_mep_profile_and_gate_only_provided_checks():
    import fake_mep_compute
    import fake_mep_no_account
    import fake_mep_stop_names_block
    mep = targets.load_profile("mep")["capabilities"]
    site = targets.load_profile("site")["capabilities"]
    universal = {r.name for r in check_all(Trace([]))}
    for sc in (fake_mep_compute, fake_mep_no_account, fake_mep_stop_names_block):
        assert sc.TARGETS == ("fake",), sc.__name__
        assert targets.meets(sc.REQUIRES, mep)[0], (sc.__name__, targets.meets(sc.REQUIRES, mep))
        assert not targets.meets(sc.REQUIRES, site)[0], sc.__name__          # site has no MEP
        assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0], sc.__name__
        provided = universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
        assert set(sc.EXPECT_OK) <= provided, (sc.__name__, set(sc.EXPECT_OK) - provided)
    assert fake_mep_no_account.GLOBUS_DB_SECRET == "HPCB_TEST_GLOBUS_DB_NOACCOUNT" and fake_mep_no_account.FACILITY_ID == "fake-mep-open"
