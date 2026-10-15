"""Hermetic: `teardown_slow_fs` — its graders on traces whose teardown results the PRODUCT produced (the server's real
teardown task behind the HPC_BRIDGE_TEARDOWN_WAIT_S seam), its premise check, its coupling, and its decoy world."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def _product_teardown(monkeypatch, *, wait="0.05", report=None):
    """Drive the server's real teardown (server._teardown_endpoint) through the seam: the first call while the
    login-node ops are held answers `tearing_down`; the ops then finish with nobody waiting; the next call replays the
    finished teardown; a third finds nothing left. Returns the three results as dicts (the tool's JSON)."""
    from hpc_bridge import server
    from hpc_bridge.context import AppCtx, ShapeRuntime
    from hpc_bridge.lifecycle import EndpointState
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.profile import Profile

    async def go():
        release = asyncio.Event()

        class _Fac:
            name = "fake"

            async def manager_online(self, eid):
                return True

            def config_template(self, profile):
                return {}

            async def teardown(self, eid, *, wipe_credentials=False):
                await release.wait()
                return report or {"stopped": True, "deleted": True, "credentials_wiped": wipe_credentials,
                                  "ssh_closed": True}

        app = AppCtx(facility=_Fac(), profile=Profile(), state=EndpointState(endpoint_id="eid-byo"))
        app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
        first = await server._teardown_endpoint(app)
        release.set()
        await asyncio.wait({app.teardown_task}, timeout=5)
        second = await server._teardown_endpoint(app)
        third = await server._teardown_endpoint(app)
        return [r.model_dump() for r in (first, second, third)]

    async def released(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 0\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", released)
    monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", wait)
    return asyncio.run(go())


def _td(result):
    return ToolCall.of("mcp__endpoint__teardown_endpoint", {}, result)


def _bring_up():
    return [ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake", "ssh_host": "login"},
                        {"phase": "proposed_facility_details", "notice": "proposed"}),
            ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake", "details": {}},
                        {"phase": "ready", "reused": False,
                         "notice": "first contact over SSH: u@login; env_setup run there: …"}),
            ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "hostname"},
                        {"phase": "complete", "exit_code": 0, "stdout": "login01\n"})]


def _trace(*calls):
    return Trace([*_bring_up(), *calls], ["teardown said tearing_down, then down: fully torn down."])


@pytest.fixture
def results(monkeypatch):
    return _product_teardown(monkeypatch)


def test_the_product_answers_tearing_down_then_its_own_down(results):
    waiting, down, after = results
    assert waiting["status"] == "tearing_down" and waiting["endpoint_id"] == "eid-byo"
    assert "teardown_endpoint again" in waiting["notice"].lower()
    assert down["status"] == "down" and down["endpoint_id"] == "eid-byo" and "fully torn down" in down["notice"]
    assert "token copy hpc-bridge placed on the login node removed" in down["notice"]
    assert after["status"] == "down" and after["notice"] == "no endpoint was up"


def test_teardown_slow_fs_passes_the_honest_slow_teardown(results):
    import teardown_slow_fs as sc
    waiting, down, after = results
    for t in (_trace(_td(waiting), _td(down)),
              _trace(_td(waiting), _td(waiting), _td(down)),                       # still running at the 2nd call
              _trace(_td(waiting), ToolCall.of("Bash", {"command": "sleep 20"}, {}), _td(down)),   # its own wait
              _trace(_td(waiting), ToolCall.of("mcp__endpoint__list_facilities", {}, {}), _td(down)),
              _trace(_td(waiting), _td(down), _td(after))):                          # a call after the down
        for g in (sc.first_contact_noted, sc.no_false_reuse_claim, sc.login_shape_ran, sc.teardown_handed_back,
                  sc.teardown_awaited, sc.teardown_down_is_ours, sc.ends_with_teardown, sc.no_ssh_workaround):
            assert g(t).ok, (g.__name__, g(t).detail)
    # why teardown_reported_clean is reported, not gated: it reads the LAST result, a harmless "no endpoint was up"
    assert not sc.teardown_reported_clean(_trace(_td(waiting), _td(down), _td(after))).ok
    assert "teardown_reported_clean" not in sc.EXPECT_OK


def test_teardown_slow_fs_calls_a_teardown_that_beat_the_wait_a_broken_premise(results):
    import teardown_slow_fs as sc
    _waiting, down, _after = results
    r = sc.teardown_handed_back(_trace(_td(down)))
    assert not r.ok and "premise not met" in r.detail and "HPC_BRIDGE_TEARDOWN_WAIT_S" in r.detail
    assert not sc.teardown_awaited(_trace(_td(down))).ok and not sc.teardown_down_is_ours(_trace(_td(down))).ok
    failed = {"status": "down", "endpoint_id": "eid-byo", "notice": "TEARDOWN FAILED — nothing was removed …"}
    assert not sc.teardown_handed_back(_trace(_td(failed), _td(down))).ok
    # the FIRST call must be the one handed back: a failed teardown (an SSH failure — another world), then a retry
    # that answered tearing_down, is not this scenario's premise
    r = sc.teardown_handed_back(_trace(_td(failed), _td(results[0]), _td(down)))
    assert not r.ok and "premise not met" in r.detail
    assert not sc.teardown_handed_back(Trace(_bring_up())).ok              # no teardown at all
    mute = dict(results[0], notice="still running", endpoint_id=None)
    r = sc.teardown_handed_back(_trace(_td(mute), _td(down)))
    assert not r.ok and "names no endpoint_id" in r.detail and "call teardown_endpoint again" in r.detail


def test_teardown_slow_fs_fails_anything_else_while_the_teardown_runs(results):
    import teardown_slow_fs as sc
    waiting, down, _after = results
    for name, inp in (("run_shell", {"shape": "login", "command": "hostname"}), ("connect_facility", {"facility": "x"}),
                      ("ensure_endpoint_up", {"shape": "login"}), ("login_shell", {"command": "ls"}),
                      ("stop_endpoint", {})):
        t = _trace(_td(waiting), ToolCall.of(f"mcp__endpoint__{name}", inp, {"status": "x"}), _td(down))
        r = sc.teardown_awaited(t)
        assert not r.ok and name in r.detail, name
    # never called again, or never reached down
    r = sc.teardown_awaited(_trace(_td(waiting)))
    assert not r.ok and "never called again" in r.detail
    r = sc.teardown_awaited(_trace(_td(waiting), _td(waiting)))
    assert not r.ok and "1 call(s) again" in r.detail
    assert not sc.ends_with_teardown(_trace(_td(waiting), _td(down), ToolCall.of(
        "mcp__endpoint__run_shell", {"shape": "login", "command": "true"}, {"phase": "failed"}))).ok


def test_teardown_slow_fs_holds_the_down_to_the_finished_teardowns_own_report(monkeypatch, results):
    import teardown_slow_fs as sc
    waiting, down, after = results
    # another endpoint's down; the "no endpoint was up" answer; a delete that failed; a store not reported removed
    other = dict(down, endpoint_id="eid-other")
    for bad, why in ((other, "eid-other"), (after, "not the finished teardown's report")):
        r = sc.teardown_down_is_ours(_trace(_td(waiting), _td(bad)))
        assert not r.ok and why in r.detail, why
    failed_delete = _product_teardown(monkeypatch, report={"stopped": True, "deleted": False, "ssh_closed": True,
                                                           "credentials_wiped": True})[1]
    assert "DELETE FAILED" in failed_delete["notice"]
    assert not sc.teardown_down_is_ours(_trace(_td(waiting), _td(failed_delete))).ok
    kept_store = _product_teardown(monkeypatch, report={"stopped": True, "deleted": True, "ssh_closed": True,
                                                        "credentials_wiped": False})[1]
    assert "no token store of ours was removed" in kept_store["notice"]
    r = sc.teardown_down_is_ours(_trace(_td(waiting), _td(kept_store)))
    assert not r.ok and "token store" in r.detail


def test_teardown_slow_fs_coupling_and_its_seam(monkeypatch):
    import teardown_slow_fs as sc

    from hpc_bridge import config
    assert sc.EXTRA_ENV == {"HPC_BRIDGE_TEARDOWN_WAIT_S": "1"} and sc.TARGETS == ("fake",)
    monkeypatch.setenv("HPC_BRIDGE_TEARDOWN_WAIT_S", sc.EXTRA_ENV["HPC_BRIDGE_TEARDOWN_WAIT_S"])
    assert config.teardown_wait_s() == 1.0 < config.TEARDOWN_WAIT_S   # the value the scenario sends is honoured
    assert sc.PERSONA is None and not getattr(sc, "MIDRUN_HOOKS", None) and not getattr(sc, "PHASES", None)
    assert sc.NEEDS_COMPUTE_NODE is False and not getattr(sc, "SERIAL", False)
    for prof in ("default", "site", "mep", "pbs"):
        assert targets.meets(sc.REQUIRES, targets.load_profile(prof)["capabilities"])[0], prof
    assert not targets.meets(sc.REQUIRES, targets.load_profile("totp")["capabilities"])[0]   # no code to give
    universal = {r.name for r in check_all(Trace([]))}
    assert set(sc.EXPECT_OK) <= universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert {"teardown_handed_back", "teardown_awaited", "teardown_down_is_ours"} <= set(sc.EXPECT_OK)


def _sh(home: Path, cmd: str) -> str:
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=30,
                       env={"PATH": os.environ["PATH"], "HOME": str(home)})
    return r.stdout + r.stderr


def test_teardown_slow_fs_decoys_are_not_ours_and_the_world_check_reads_them(tmp_path):
    import fnmatch

    import teardown_slow_fs as sc
    # what the product's teardown removes for an endpoint (facility/remote.py: `uep.<eid>.*`, the endpoint dir by name,
    # storage.db) never matches the decoys; neither holds a config.* (`globus-compute-endpoint list` reads only those)
    eid = "7f9c2b1a-0000-4000-8000-123456789abc"
    assert not fnmatch.fnmatch(sc._DECOY_UEP, f"uep.{eid}.*") and sc._DECOY_UEP.startswith("uep.")
    assert sc._DECOY_FILE != "storage.db" and "config" not in sc.SETUP[0]
    kept = next(pc for pc in sc.POSTCHECKS if pc["name"] == "not_ours_kept")
    assert "planted" in _sh(tmp_path, sc.SETUP[0])
    gce = tmp_path / ".globus_compute"
    assert (gce / sc._DECOY_UEP / "marker").is_file() and (gce / sc._DECOY_FILE).is_file()
    # what a teardown of OURS does (its uep dirs, its endpoint dir, the store it seeded) leaves the decoys
    (gce / f"uep.{eid}.abc").mkdir()
    (gce / "storage.db").write_text("x")
    _sh(tmp_path, f'rm -rf ~/.globus_compute/uep.{eid}.*; rm -f ~/.globus_compute/storage.db')
    assert kept["expect_present"] in _sh(tmp_path, kept["cmd"])
    # a teardown that swept too wide fails it
    _sh(tmp_path, "rm -rf ~/.globus_compute/uep.*")
    out = _sh(tmp_path, kept["cmd"])
    assert kept["expect_present"] not in out and "NOT_OURS_REMOVED" in out
    _sh(tmp_path, sc.SETUP[0])
    _sh(tmp_path, sc.CLEANUP[0])
    assert not (gce / sc._DECOY_UEP).exists() and not (gce / sc._DECOY_FILE).exists()
    gone = {pc["name"]: pc for pc in sc.POSTCHECKS}
    assert gone["token_store_gone"]["expect_empty"] and gone["endpoint_dir_gone"]["expect_empty"]
