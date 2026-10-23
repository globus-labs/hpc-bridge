"""Hermetic: the `internal` profile and internal_hostnames' graders."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def test_internal_profile_layers_on_site():
    m = targets.load_profile("internal")
    assert m["layers"] == ["site", "internal"] and m["capabilities"]["hostnames"] == "internal"
    assert m["capabilities"]["login_hosts"] == ["login01.hpcb.test", "login02.hpcb.test"]      # the PUBLIC names stay
    assert m["capabilities"]["login_internal_hosts"] == ["login01.int.hpcb.test", "login02.int.hpcb.test"]
    import internal_hostnames as sc
    assert targets.meets(sc.REQUIRES, m["capabilities"])[0]
    assert not targets.meets(sc.REQUIRES, targets.load_profile("site")["capabilities"])[0]
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided


def test_internal_hostnames_graders():
    import internal_hostnames as sc
    login = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "hostname -f"}, {"phase": "complete", "stdout": "login02.int.hpcb.test\n"})
    assert sc.internal_name_seen(Trace([login])).ok
    public = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "hostname -f"}, {"phase": "complete", "stdout": "login02.hpcb.test\n"})
    assert not sc.internal_name_seen(Trace([public])).ok
    by_addr = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"}, {"phase": "provisioning", "notice": "first contact over SSH: hpcbridge-test-00@172.20.0.6 (host pinned by HPC_BRIDGE_SSH_HOST); env_setup run there"})
    by_name = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"}, {"phase": "provisioning", "notice": "first contact over SSH: hpcbridge-test-00@login02.hpcb.test; env_setup run there"})
    assert sc.pinned_by_address(Trace([by_addr])).ok and "172.20.0.6" in sc.pinned_by_address(Trace([by_addr])).detail
    assert not sc.pinned_by_address(Trace([by_name])).ok


def _first_contact(host):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"},
                       {"phase": "provisioning", "notice": f"first contact over SSH: hpcbridge-test-00@{host}; env_setup run there: 'x'"})


def _pin_run(host, *ensures):
    login = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "hostname -f"},
                        {"phase": "complete", "exit_code": 0, "stdout": "login01.int.hpcb.test\n"})
    teardown = ToolCall.of("mcp__endpoint__teardown_endpoint", {}, {"status": "down", "notice": "fully torn down; removed"})
    return Trace([_first_contact(host), *ensures, login, teardown], ["The endpoint ran on login01; removed everything."])


def test_the_pin_scenarios_run_without_ssh_multiplexing_and_gate_the_pin():
    # the review of the 2026-10-22 sweep: the alias's ControlMaster (ControlPersist 60 s) carried a teardown sent to the
    # alias to the node it first reached, masking a pin regression — and neither cell gated the pin itself
    import internal_hostnames as ih
    import login_pin_teardown as lp
    for sc in (ih, lp):
        assert sc.EXTRA_ENV == {"HPC_BRIDGE_SSH_CONTROL_PERSIST": "0"}, sc.__name__   # config._control_settings: off
    assert "pinned_by_address" in ih.EXPECT_OK and "pinned_to_a_node" in lp.EXPECT_OK
    assert ih.pinned_by_address(_pin_run("172.20.0.6")).ok and not ih.pinned_by_address(_pin_run("login")).ok
    for host, ok in (("login01.hpcb.test", True), ("login02", True), ("172.20.0.7", True), ("login", False),
                     ("login.hpcb.test", False)):
        assert lp.pinned_to_a_node(_pin_run(host)).ok is ok, host
    assert not lp.pinned_to_a_node(Trace([])).ok


def test_the_pin_scenarios_allow_the_free_login_shape_ensure():
    # calls_bounded("ensure_endpoint_up", 0) failed an agent that waited for the login node with a login-shape ensure
    import internal_hostnames as ih
    import login_pin_teardown as lp
    login_ensure = ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "login"}, {"status": "up"})
    t = _pin_run("172.20.0.6", login_ensure)
    for sc in (ih, lp):
        rows = {fn(t).name: fn(t) for fn in sc.EXTRA_INVARIANTS}
        assert rows["no_compute_ensure"].ok and "ensure_endpoint_up_calls_le_0" not in sc.EXPECT_OK, sc.__name__
        compute = _pin_run("172.20.0.6", ToolCall.of("mcp__endpoint__ensure_endpoint_up", {}, {"status": "provisioning"}))
        assert not {fn(compute).name: fn(compute) for fn in sc.EXTRA_INVARIANTS}["no_compute_ensure"].ok
