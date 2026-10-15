"""Hermetic: the `f2b` profile, the LOCAL_SETUP / each_login knobs, and the fail2ban scenarios' graders."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import ClassVar

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def test_f2b_profile_layers_on_site_with_the_jail_and_the_harness_port():
    m = targets.load_profile("f2b")
    assert m["layers"] == ["site", "f2b"] and m["capabilities"]["fail2ban"] == "sshd"
    assert m["capabilities"]["fail2ban_maxretry"] == 3 and m["capabilities"]["harness_ssh_port"] == 2200 and m["capabilities"]["nodes"] == 3
    import f2b_banned
    import f2b_stranger
    for sc in (f2b_stranger, f2b_banned):
        assert sc.TARGETS == ("fake",) and sc.SERIAL == "fail2ban"   # one fail2ban state and log: one group
        assert targets.meets(sc.REQUIRES, m["capabilities"])[0]
        assert not targets.meets(sc.REQUIRES, targets.load_profile("site")["capabilities"])[0]
        provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
        assert set(sc.EXPECT_OK) <= provided, (sc.__name__, set(sc.EXPECT_OK) - provided)
    assert f2b_stranger.EXTRA_ENV["HPC_BRIDGE_SSH_USER"] == "hpcbridge-stranger"
    assert f2b_banned.LOCAL_SETUP and "login01.hpcb.test login02.hpcb.test" in f2b_banned.LOCAL_SETUP[0]


def test_world_cmds_fan_out_to_every_login_node(monkeypatch, harness_run):
    monkeypatch.setenv("HPCB_TARGET_CAPS", json.dumps({"login_hosts": ["l1", "l2"]}))
    cmds = harness_run._world_cmds(["plain", {"cmd": "per-node", "on": "each_login"}, {"cmd": "default-host"}])
    assert cmds == [("plain", None), ("per-node", "l1"), ("per-node", "l2"), ("default-host", None)]
    monkeypatch.setenv("HPCB_TARGET_CAPS", json.dumps({}))
    assert harness_run._world_cmds([{"cmd": "x", "on": "each_login"}]) == [("x", None)]
    assert harness_run._world_cmds(None) == []


def test_local_setup_runs_in_the_jail_and_a_failure_aborts(harness_run):
    class R:
        def __init__(self, rc, out=""):
            self.returncode, self.stdout, self.stderr = rc, out, ""

    class Scen:
        LOCAL_SETUP: ClassVar[list[str]] = ["burn budget", "second"]

    seen = []
    assert harness_run._local_setup(Scen(), runner=lambda c: (seen.append(c), R(0))[1]) is True
    assert seen == ["burn budget", "second"]
    assert harness_run._local_setup(Scen(), runner=lambda c: R(1, "boom")) is False
    assert harness_run._local_setup(Scen(), runner=lambda c: (_ for _ in ()).throw(TimeoutError("hung"))) is False
    assert harness_run._local_setup(object()) is True   # no LOCAL_SETUP: nothing to do


def test_f2b_banned_graders():
    import f2b_banned as fb
    refused = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "ssh_host": "login"},
                          {"phase": "failed", "notice": "CANNOT REACH login: ssh: connect to host login port 22: Connection timed out. Check the login host name and your network/VPN"})
    assert fb.cannot_reach_surfaced(Trace([refused])).ok
    assert fb.unreachable_relayed(Trace([refused], ["I cannot reach `login` — the connection timed out; it may be blocked."])).ok
    other = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"}, {"phase": "failed", "notice": "NO SSH ACCESS to login as x"})
    assert not fb.cannot_reach_surfaced(Trace([other])).ok
    assert not fb.unreachable_relayed(Trace([refused], ["Done."])).ok
