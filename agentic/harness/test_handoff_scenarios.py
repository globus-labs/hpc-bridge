"""Hermetic: batch phase 2b's harness work (design §10 "Phase 2": `handoff_polaris_shape`,
`handoff_mep_min_blocks_refused`, plus the SSH cell `handoff_ssh_shape`) — the graders and world checks
(agentic/harness/handoff_checks.py) on synthetic traces, and each scenario's SCRIPT driven against a stand-in server
answering as main does (no such tool: every gated grader fails, naming it) and as the build should (every gated grader
passes). No server, no cluster."""
from __future__ import annotations

import copy
import importlib
import shlex
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import handoff_checks as hc  # noqa: E402
import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402
from test_batch_scenarios import _connect_answers, _drive, _Server, _stub_sdk  # noqa: E402

SCENARIOS = ["handoff_polaris_shape", "handoff_mep_min_blocks_refused", "handoff_ssh_shape"]
MEP = "da3df250-4013-4d69-942c-eef1568f860c"
UEP = "4b1c0a52-8d3e-4f6a-9b7c-2e5d8f0a1c33"
SSH_EID = "7e0f6c1a-2b3d-4e5f-8a9b-0c1d2e3f4a5b"
MAIN = {"text": "Unknown tool: export_handoff"}   # FastMCP 1.30's answer to an unknown tool: plain text


def _c(name, inp=None, res=None):
    return ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)


def _terms(**over):
    t = {"facility": "fake-pbs-mep", "scheduler": "pbs", "account": "hpcb", "queue": "debug", "walltime": "00:15:00",
         "nodes_per_block": 1, "init_blocks": 0, "min_blocks": 0, "max_blocks": 2, "unit": "node-hours",
         "canary_worst_case": 0.25, "worst_case": 48.5, "enforced_by": "uec-bounds-only", "approved_at": None,
         "expires_at": None, "binds": "the config bounds the consumer's pool; hpc-bridge does not police it"}
    t.update(over)
    return t


def _record(uec, *, scheduler="pbs", access="mep", job="12.pbs", user="hpcbmep", manager=MEP, cancel="qdel", **over):
    rec = {
        "schema": "hpc-bridge.handoff", "version": 1, "handoff_id": "hoff-0123456789ab",
        "created_at": "2026-11-02T10:01:00Z", "expires_at": "2026-11-03T10:00:00Z", "hpc_bridge_version": "0.1.26",
        "facility": {"id": "fake-pbs-mep", "system": "fake-pbs-mep", "scheduler": scheduler, "access": access},
        "endpoint": {"manager_id": manager, "kind": "facility_mep" if access == "mep" else "hpc_bridge_ssh",
                     "uep_id_seen": UEP},
        "user_endpoint_config": dict(uec),
        "template": {"sha256": "a" * 64, "renders": {k: True for k in uec}},
        "worker": {"python": "3.13.11", "dill": "0.3.9", "parsl": "2026.2.23", "host": "node1"},
        "client_requirements": {"python": "3.13"},
        "terms": _terms(approved_at="2026-11-02T10:00:00Z", expires_at="2026-11-03T10:00:00Z"),
        "validation": {"ok": True, "job_id": job, "host": "node1", "device_pin": {"CUDA_VISIBLE_DEVICES": "0"},
                       "identity": user, "task_id": "t-1", "queue_checks": []},
        "release": {"min_blocks": 0, "ends_by_itself": True, "block_idle_release_s": 240, "uep_idle_exit_s": None,
                    "cancel_block": f"{cancel} {shlex.quote(job)}",
                    "steps": ["Stop submitting tasks with this user_endpoint_config (any submit restarts it).",
                              "Its blocks then idle-release after 240 s; nothing replaces them.",
                              f"To end a block sooner once nothing is queued: `{cancel} {job}` — min_blocks is 0, so "
                              "nothing replaces it."],
                    "hpc_bridge_scope": "stop_endpoint cancels every block of this endpoint; teardown_endpoint ends it"},
    }
    for k, v in over.items():
        rec[k] = v
    return rec


def _polaris_trace(rec=None, *, ask=None, yes=None, more=()):
    import handoff_polaris_shape as sc
    rec = rec or _record(sc.LOAM_UEC)
    return Trace([
        _c("connect_facility", {"facility": "fake-pbs-mep"}, {"phase": "needs_account", "endpoint_id": MEP}),
        _c("export_handoff", {"user_endpoint_config": sc.LOAM_UEC},
           ask or {"phase": "needs_confirmation", "terms": _terms(), "user_endpoint_config": sc.LOAM_UEC}),
        _c("export_handoff", {"user_endpoint_config": sc.LOAM_UEC, "confirm_spend": True},
           yes or {"phase": "validating", "task_id": "t-1"}),
        *more,
        _c("export_handoff", {"user_endpoint_config": sc.LOAM_UEC},
           {"phase": "exported", "record": rec, "path": "/s/handoffs/hoff-0123456789ab.json"}),
    ])


def _gated(mod, t: Trace) -> dict[str, bool]:
    rows = {r.name: r for r in check_all(t)} | {r.name: r for r in (fn(t) for fn in mod.EXTRA_INVARIANTS)}
    return {n: rows[n].ok for n in mod.EXPECT_OK if n != "agent_engaged"}


# ---- the graders, on synthetic traces -----------------------------------------------------------------------------

def test_a_handoff_the_plugin_gets_right_passes_every_gated_grader():
    import handoff_polaris_shape as sc
    t = _polaris_trace()
    assert all(_gated(sc, t).values()), {fn(t).name: fn(t).detail for fn in sc.EXTRA_INVARIANTS}


@pytest.mark.parametrize("break_it,grader", [
    (lambda rec: rec["user_endpoint_config"].update(partition="debug"), "handoff_record_shape"),
    (lambda rec: rec["user_endpoint_config"].update(compute=True), "handoff_record_shape"),
    (lambda rec: rec["terms"].update(enforced_by="hpc-bridge"), "handoff_record_shape"),
    (lambda rec: rec["template"]["renders"].update(queue=False), "handoff_record_shape"),
    (lambda rec: rec["validation"].update(device_pin={}), "handoff_record_shape"),
    (lambda rec: rec["endpoint"].update(uep_id_seen=MEP), "handoff_record_shape"),
    (lambda rec: rec.update(loam={"env": {"HPC_GLOBUS_ENDPOINT_ID": MEP}}), "handoff_record_shape"),  # consumer-named
    (lambda rec: rec.update(set_env_sh="export HPC_NNODES=1\n"), "handoff_record_shape"),   # shell in the record
    (lambda rec: rec["endpoint"].update(manager_id="not-an-endpoint"), "handoff_record_shape"),
    (lambda rec: rec["validation"].update(identity="someone"), "handoff_record_shape"),
    (lambda rec: rec["release"].update(steps=["qdel 12.pbs"]), "handoff_release_honest"),
    (lambda rec: rec["release"]["steps"].__setitem__(2, "Or `qdel 12.pbs`."), "handoff_release_honest"),
    (lambda rec: rec["release"].update(cancel_block="qdel 99.pbs"), "handoff_release_honest"),
    (lambda rec: rec["release"].update(min_blocks=1), "handoff_release_honest"),
    (lambda rec: rec["release"]["steps"].append("or rm ~/.globus_compute/*/daemon.pid"), "handoff_release_honest"),
], ids=lambda v: v if isinstance(v, str) else "")
def test_each_grader_fails_what_it_guards(break_it, grader):
    import handoff_polaris_shape as sc
    rec = copy.deepcopy(_record(sc.LOAM_UEC))
    break_it(rec)
    got = _gated(sc, _polaris_trace(rec))
    assert got[grader] is False, got
    assert all(ok for n, ok in got.items() if n != grader), got


def test_asked_first_and_one_canary_fail_what_they_guard():
    import handoff_polaris_shape as sc
    rec = _record(sc.LOAM_UEC)
    # the first call already said yes: nothing was asked
    t = Trace([_c("export_handoff", {"user_endpoint_config": {}, "confirm_spend": True},
                  {"phase": "exported", "record": rec})])
    assert not hc.asked_first(t).ok
    # an ask whose terms name no unit, or another enforcement
    for terms in (_terms(unit=None), _terms(worst_case=0), _terms(enforced_by="server")):
        assert not hc.asked_first(_polaris_trace(ask={"phase": "needs_confirmation", "terms": terms})).ok
    # a second canary
    t2 = _polaris_trace(more=[_c("export_handoff", {"user_endpoint_config": {}, "confirm_spend": True},
                                 {"phase": "validating", "task_id": "t-2"})])
    assert not hc.one_canary(t2).ok and hc.one_canary(_polaris_trace()).ok


def test_d6_grader_wants_both_answers_refused_with_nothing_asked_or_sent():
    import handoff_mep_min_blocks_refused as sc
    refused = {"phase": "refused", "reason": "mep_min_blocks",
               "notice": "Refused: min_blocks must be 0 on a facility endpoint (D6)."}
    good = Trace([_c("connect_facility", {}, {"phase": "needs_account"}),
                  _c("export_handoff", {"user_endpoint_config": sc.UEC}, refused),
                  _c("export_handoff", {"user_endpoint_config": sc.UEC, "confirm_spend": True}, refused)])
    assert hc.d6_refused(good).ok, hc.d6_refused(good).detail
    for bad in ({"phase": "needs_confirmation", "terms": _terms()}, {**refused, "reason": "invalid"},
                {**refused, "terms": _terms()}, {**refused, "notice": "no."}, {"phase": "validating", "task_id": "t"}):
        t = Trace([good.calls[0], good.calls[1], _c("export_handoff", {"confirm_spend": True}, bad)])
        assert not hc.d6_refused(t).ok, bad
    assert not hc.d6_refused(Trace(good.calls[:2])).ok          # the yes was never tried


def test_the_ssh_specifics_grader():
    import handoff_ssh_shape as sc
    uec = {**sc.UEC, "partition": "debug", "hpcb_block": 123456}
    rec = _record(uec, scheduler="slurm", access="ssh", job="4242", user=None, manager=SSH_EID, cancel="scancel")
    rec["release"].update(uep_idle_exit_s=600, block_idle_release_s=600)
    t = Trace([_c("export_handoff", {}, {"phase": "exported", "record": rec})])
    assert sc.ssh_record_specifics(t).ok, sc.ssh_record_specifics(t).detail
    for change in ({"hpcb_block": True}, {"compute": True}):
        bad = copy.deepcopy(rec)
        bad["user_endpoint_config"].update(change)
        assert not sc.ssh_record_specifics(Trace([_c("export_handoff", {}, {"phase": "exported", "record": bad})])).ok
    bad = copy.deepcopy(rec)
    bad["release"]["hpc_bridge_scope"] = "nothing"
    assert not sc.ssh_record_specifics(Trace([_c("export_handoff", {}, {"phase": "exported", "record": bad})])).ok


# ---- the world checks -------------------------------------------------------------------------------------------------

class _Ctx:
    def __init__(self, trace, out, rc=0):
        self.trace, self._out, self._rc, self.cmds = trace, out, rc, []

    def ssh(self, cmd, timeout=120, host=None):
        self.cmds.append(cmd)
        return self._rc, self._out


def test_the_canary_job_is_read_back_from_the_scheduler():
    import asyncio

    import handoff_polaris_shape as sc
    t = _polaris_trace()
    good = "Job Id: 12.pbs\n    queue = debug\n    Account_Name = hpcb\n    Resource_List.walltime = 00:15:00\n"
    ctx = _Ctx(t, good)
    assert asyncio.run(sc.canary_job_as_recorded(ctx)).ok and "qstat -x -f 12.pbs" in ctx.cmds[0]
    assert not asyncio.run(sc.canary_job_as_recorded(_Ctx(t, good.replace("hpcb", "other")))).ok
    assert not asyncio.run(sc.canary_job_as_recorded(_Ctx(Trace([]), good))).ok          # no record: premise


def test_no_job_since_start_reads_ctimes_against_the_runs_start():
    import asyncio

    import handoff_mep_min_blocks_refused as sc
    t = Trace([_c("export_handoff", {}, {"phase": "refused"})])
    t.t0 = 1_000_000.0
    ok = asyncio.run(sc.no_job_since_start(_Ctx(t, "JOB 3.pbs 999000\nJOB 4.pbs 999990\nEND\n")))
    assert ok.ok, ok.detail
    late = asyncio.run(sc.no_job_since_start(_Ctx(t, "JOB 3.pbs 999000\nJOB 5.pbs 1000100\nEND\n")))
    assert not late.ok and "5.pbs" in late.detail
    assert not asyncio.run(sc.no_job_since_start(_Ctx(t, "NO-QSELECT\n", rc=3))).ok
    assert not asyncio.run(sc.no_job_since_start(_Ctx(t, "JOB 6.pbs -1\nEND\n"))).ok   # unreadable: never a pass


# ---- the scripts: red on main for the right reason, green against the build's answers ---------------------------------

def _build_answers(mod, *, ssh=False):
    """A stand-in for the build: the ask, then validating once, then the record."""
    seen = {"n": 0}
    uec = getattr(mod, "LOAM_UEC", None) or mod.UEC
    if ssh:
        wire = {**uec, "partition": "debug", "hpcb_block": 99}
        rec = _record(wire, scheduler="slurm", access="ssh", job="4242", user="u", manager=SSH_EID, cancel="scancel")
        rec["release"].update(uep_idle_exit_s=600, block_idle_release_s=600)
        rec["validation"]["device_pin"] = {}
    else:
        rec = _record(uec)

    def answer(a):
        if a.get("confirm_spend") is True:
            seen["n"] += 1
            return {"phase": "validating", "task_id": "t-1"}
        if seen["n"] == 0:
            return {"phase": "needs_confirmation", "terms": _terms(), "user_endpoint_config": uec}
        return {"phase": "exported", "record": rec, "path": "/s/handoffs/hoff-0123456789ab.json"}
    return answer


@pytest.mark.parametrize("name", SCENARIOS)
def test_on_main_every_gated_grader_fails_naming_the_missing_tool(name, monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    monkeypatch.setattr(hc, "POLL_S", 0.01)
    mod = importlib.import_module(name)
    server = _Server({"connect_facility": _connect_answers() if name == "handoff_ssh_shape"
                      else (lambda a: {"phase": "needs_account", "endpoint_id": MEP}),
                      "export_handoff": lambda a: MAIN})
    res = _drive(mod.SCRIPT, server)
    assert not res.final.is_error, res.final.result
    rows = {r.name: r for r in (fn(res.trace) for fn in mod.EXTRA_INVARIANTS)}
    gated = [n for n in mod.EXPECT_OK if n in rows]
    assert gated and all(not rows[n].ok for n in gated), {n: rows[n].detail for n in gated}
    assert all("not a tool" in rows[n].detail or "no export_handoff" in rows[n].detail for n in gated), \
        {n: rows[n].detail for n in gated}


@pytest.mark.parametrize("name", ["handoff_polaris_shape", "handoff_ssh_shape"])
def test_against_the_builds_answers_the_script_asks_says_yes_once_and_collects(name, monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    monkeypatch.setattr(hc, "POLL_S", 0.01)
    mod = importlib.import_module(name)
    ssh = name == "handoff_ssh_shape"
    server = _Server({"connect_facility": _connect_answers() if ssh
                      else (lambda a: {"phase": "needs_account", "endpoint_id": MEP}),
                      "export_handoff": _build_answers(mod, ssh=ssh)})
    res = _drive(mod.SCRIPT, server)
    calls = [(tool, a.get("confirm_spend")) for _n, tool, a in server.calls if tool == "export_handoff"]
    assert calls == [("export_handoff", None), ("export_handoff", True), ("export_handoff", None)], calls
    assert all(_gated(mod, res.trace).values()), {fn(res.trace).name: fn(res.trace).detail
                                                  for fn in mod.EXTRA_INVARIANTS}


def test_against_the_builds_answers_the_d6_script_passes():
    import handoff_mep_min_blocks_refused as sc
    refused = {"phase": "refused", "reason": "mep_min_blocks", "notice": "min_blocks must be 0 here (D6)"}
    t = Trace([_c("connect_facility", {}, {"phase": "needs_account"}),
               _c("export_handoff", {"user_endpoint_config": sc.UEC}, refused),
               _c("export_handoff", {"user_endpoint_config": sc.UEC, "confirm_spend": True}, refused)])
    assert all(_gated(sc, t).values())
    assert sc.UEC["min_blocks"] == 1


# ---- wiring -------------------------------------------------------------------------------------------------------------

def test_the_cells_run_where_their_facility_is_and_gate_only_graders_they_have():
    caps = {p: targets.load_profile(p)["capabilities"] for p in ("pbs_mep", "mep", "default", "pbs")}
    for name, where in (("handoff_polaris_shape", "pbs_mep"), ("handoff_mep_min_blocks_refused", "pbs_mep"),
                        ("handoff_ssh_shape", "default")):
        mod = importlib.import_module(name)
        assert mod.TARGETS == ("fake",) and mod.OPERATORS == ("script",), name
        assert targets.meets(mod.REQUIRES, caps[where])[0], (name, where)
        provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
        assert set(mod.EXPECT_OK) <= provided, (name, set(mod.EXPECT_OK) - provided)
    for name in ("handoff_polaris_shape", "handoff_mep_min_blocks_refused"):
        mod = importlib.import_module(name)
        assert mod.FACILITY_ID == "fake-pbs-mep" and mod.SERIAL == "hpcbmep", name
        assert not targets.meets(mod.REQUIRES, caps["mep"])[0] and not targets.meets(mod.REQUIRES, caps["pbs"])[0]
