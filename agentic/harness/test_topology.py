"""Hermetic: the fake cluster's network topologies (harness/topology.py) — the plan per topology, the ssh configs the
jail writes, the world channel's `ssh -F`, the compose overlay and project, and topology_roundtrip's graders."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
FC = HERE.parent / "fakecluster"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
import topology  # noqa: E402
from invariants import ToolCall, Trace  # noqa: E402

SITE = ["login01.hpcb.test", "login02.hpcb.test"]


def _plan(name, hosts=None, **kw):
    return topology.plan(name, project="hpcb-x", login_hosts=hosts or ["login"], ports=[2242, 2243], **kw)


# ---- the plan ---------------------------------------------------------------------------------------------------------

def test_in_network_is_todays_behaviour():
    p = _plan("in_network", SITE)
    assert (p.ssh_host, p.network, p.docker_args, p.user_hosts, p.harness_hosts, p.service, p.isolate) == (
        "login", "hpcb-x_default", (), {}, {}, None, False)
    # every other topology isolates the client: Docker Desktop routes between its bridge networks (topology.py doc)
    assert all(_plan(t).isolate for t in topology.TOPOLOGIES if t != "in_network")
    assert _plan("in_network", network="other_net").network == "other_net"   # HPCB_FAKE_NETWORK still wins


def test_host_forward_joins_no_cluster_network_and_reaches_each_node_by_its_published_port():
    p = _plan("host_forward", SITE)
    assert p.ssh_host == topology.ALIAS and p.network is None and p.service is None and p.overlay is None
    assert p.docker_args == ("--add-host=host.docker.internal:host-gateway",)
    assert p.user_hosts == {"hpcb-fake": {"HostName": "host.docker.internal", "Port": "2242"}}   # all the product is told
    assert p.harness_hosts == {"hpcb-fake": {"HostName": "host.docker.internal", "Port": "2242"},
                               "login01.hpcb.test": {"HostName": "host.docker.internal", "Port": "2242"},
                               "login02.hpcb.test": {"HostName": "host.docker.internal", "Port": "2243"}}


def test_bastion_and_vip_join_only_the_edge_network():
    b = _plan("bastion", SITE)
    assert (b.ssh_host, b.network, b.service, b.overlay) == ("hpcb-fake", "hpcb-x_edge", "bastion", "topologies/bastion.yml")
    assert b.user_hosts == {"bastion": {"HostName": "bastion"}, "hpcb-fake": {"HostName": "login", "ProxyJump": "bastion"}}
    assert b.harness_hosts["login02.hpcb.test"] == {"HostName": "login02.hpcb.test", "ProxyJump": "bastion"}
    v = _plan("vip", SITE)
    assert (v.ssh_host, v.network, v.service, v.overlay) == ("hpcb-fake", "hpcb-x_edge", "vip", "topologies/vip.yml")
    assert v.user_hosts == {"hpcb-fake": {"HostName": "vip", "Port": "22"}}
    assert [v.harness_hosts[h]["Port"] for h in SITE] == ["2201", "2202"]   # the vip's per-node ports, the harness' only


def test_unknown_topologies_and_mfa_profiles_are_refused():
    with pytest.raises(SystemExit):
        _plan("carrier_pigeon")
    with pytest.raises(SystemExit):   # an MFA profile publishes its key-only harness sshd: a forward would skip the code
        _plan("host_forward", harness_ssh_port=2200)
    assert topology.selected({}) == "in_network" and topology.selected({"HPCB_FAKE_TOPOLOGY": " VIP "}) == "vip"


def test_the_harness_config_never_prompts_and_the_users_does_not_relax_host_keys():
    p = _plan("bastion")
    user = topology.ssh_config(p.user_hosts, user="hpcbridge-test-07", key="/k")
    harness = topology.ssh_config(p.harness_hosts, user="hpcbridge-test-07", key="/k", harness=True)
    assert "Host bastion\n    HostName bastion\n    User hpcbridge-test-07\n    IdentityFile /k\n" in user
    assert "StrictHostKeyChecking" not in user and "BatchMode" not in user
    # the ProxyJump hop reads only the file (ssh passes it -F, never the command line's -o): its stanza carries them
    hop = harness.split("Host hpcb-fake")[0]
    assert "BatchMode yes" in hop and "StrictHostKeyChecking accept-new" in hop and "IdentityFile /k" in hop


def test_install_writes_the_users_alias_and_the_harness_config(tmp_path):
    (tmp_path / ".ssh").mkdir()
    (tmp_path / ".ssh" / "config").write_text("Host mine\n    HostName example.org\n")
    spec = json.loads(_plan("host_forward").to_json())
    path = topology.install(spec, home=tmp_path, user="u", key="/k")
    cfg = (tmp_path / ".ssh" / "config").read_text()
    assert cfg.startswith("Host mine\n") and "Host hpcb-fake\n    HostName host.docker.internal\n    Port 2242" in cfg
    assert "Host login\n" not in cfg                               # the per-node ways are the harness' alone
    assert path == tmp_path / topology.HARNESS_CONFIG and "Host login\n" in path.read_text()
    assert oct(path.stat().st_mode)[-3:] == "600" and oct((tmp_path / ".ssh" / "config").stat().st_mode)[-3:] == "600"
    assert topology.install(json.loads(_plan("in_network").to_json()), home=tmp_path / "b", user="u", key="/k") is None
    assert not (tmp_path / "b").exists()


def test_internal_only_reads_the_subnets_the_jail_is_not_on():
    nets = ["172.22.0.0/16", "10.9.0.0/24"]
    assert topology.internal_only("172.22.0.7", nets) and topology.internal_only("10.9.0.3", nets)
    assert not topology.internal_only("172.21.0.4", nets) and not topology.internal_only(None, nets)
    assert not topology.internal_only("no-such-host.invalid", nets)   # unresolvable: pin_reachable's to judge
    calls = []

    def run(argv):
        calls.append(argv)
        if argv[:3] == ["docker", "network", "ls"]:
            return "p_default\np_edge\np_data\n"
        return {"p_default": "172.22.0.0/16 ", "p_data": "10.9.0.0/24 "}[argv[-1]]
    assert topology.internal_subnets("p", "p_edge", run=run) == nets
    assert "label=com.docker.compose.project=p" in calls[0]


def test_a_preauth_command_is_run_with_batchmode_and_a_scratch_socket():
    cmd = ("ssh -fN -o ControlMaster=yes -o 'ControlPath=/home/a/.hpc-bridge/cm/%C' -o ControlPersist=1h "
           "-o HostKeyAlias=login -i /k -- u@172.22.0.7")
    argv = topology.preauth_check_argv(cmd, "/scratch/x/m")
    assert argv[:5] == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]   # first: they win
    assert "ControlPath=/scratch/x/m" in argv and not any("hpc-bridge/cm" in a for a in argv)
    assert argv[5:] == ["-fN", "-o", "ControlMaster=yes", "-o", "ControlPath=/scratch/x/m", "-o", "ControlPersist=1h",
                        "-o", "HostKeyAlias=login", "-i", "/k", "--", "u@172.22.0.7"]
    with pytest.raises(ValueError):
        topology.preauth_check_argv("rm -rf /", "/scratch/x/m")


# ---- the target, run_smoke's assignments, run.py's world channel ------------------------------------------------------

def test_the_fake_target_carries_the_topology(monkeypatch):
    monkeypatch.setenv("HPCB_FAKE_PROJECT", "hpcb-topo")
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "site")
    monkeypatch.setenv("HPCB_FAKE_TOPOLOGY", "vip")
    monkeypatch.delenv("HPCB_FAKE_NETWORK", raising=False)
    t = targets.get("fake")
    assert (t.ssh_host, t.docker_network, t.topology) == ("hpcb-fake", "hpcb-topo_edge", "vip")
    assert t.admin_argv[2] == "hpcb-topo-slurmctld-1"                       # the admin channel follows the project
    monkeypatch.setenv("HPCB_FAKE_TOPOLOGY", "in_network")
    assert targets.get("fake").docker_network == "hpcb-topo_default"
    env = {**os.environ, "HPCB_FAKE_TOPOLOGY": "host_forward", "HPCB_FAKE_PROFILE": "default", "HPCB_FAKE_SSH_PORT": "2242"}
    r = subprocess.run([sys.executable, str(HERE / "targets.py"), "fake"], capture_output=True, text=True, env=env,
                       timeout=30)
    kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
    assert r.returncode == 0 and kv["HPCB_T_TOPOLOGY"] == "host_forward" and kv["HPCB_T_NETWORK"] == ""
    assert kv["HPCB_T_SSH_HOST"] == "hpcb-fake" and kv["HPCB_T_TOPOLOGY_SERVICE"] == ""
    assert kv["HPCB_T_TOPOLOGY_ISOLATE"] == "1"
    out = subprocess.run(["bash", "-c", r.stdout + '\neval "A=($HPCB_T_DOCKER_ARGS)"; printf "%s\\n" "${A[@]}"; '
                          'printf "%s" "$HPCB_T_TOPOLOGY_PLAN"'], capture_output=True, text=True, timeout=30).stdout
    args, plan = out.split("\n", 1)
    assert args == "--add-host=host.docker.internal:host-gateway"
    assert json.loads(plan)["user_hosts"]["hpcb-fake"]["Port"] == "2242"
    g = subprocess.run([sys.executable, str(HERE / "targets.py"), "globus1"], capture_output=True, text=True, timeout=30)
    assert "HPCB_T_TOPOLOGY=\n" in g.stdout   # no topology off the fake cluster


def test_the_world_channel_takes_the_harness_config(harness_run, monkeypatch, tmp_path):
    import run
    monkeypatch.setattr(run, "_HARNESS_SSH_CONFIG", None)
    seen = []
    monkeypatch.setattr(run.subprocess, "run", lambda argv, **kw: (seen.append(argv),
                                                                   subprocess.CompletedProcess(argv, 0, "ok", ""))[1])
    run._ssh_run("true", host="login")
    assert "-F" not in seen[-1]                                        # in_network: as it always was
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-07")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/home/agent/.ssh/test_key")
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "hpcb-fake")
    monkeypatch.setenv(topology.PLAN_ENV, _plan("bastion").to_json())
    path = run._install_topology(home=tmp_path)
    run._ssh_run("true", host="login")
    assert seen[-1][:3] == ["ssh", "-F", path] and seen[-1][-2] == "hpcbridge-test-07@login"
    assert "User hpcbridge-test-07\n    IdentityFile /home/agent/.ssh/test_key" in (tmp_path / ".ssh" / "config").read_text()


def test_run_smoke_removes_the_client_netns_on_every_exit_path():
    # the isolated jail's holder container (hpcb-netns-<runid>) must go with the cell, whichever EXIT trap is last set
    import re
    text = (HERE.parent / "run_smoke.sh").read_text()
    traps = [ln for ln in text.splitlines() if re.match(r"\s*trap '.*' EXIT\b", ln)]
    assert len(traps) >= 5 and all("stop_netns" in ln for ln in traps), traps
    # `unreachable`: "No route to host", which hpc-bridge reads as a network failure (a blackhole's EINVAL it does not)
    assert '--network "container:$NETNS"' in text and "ip route add unreachable" in text and "blackhole" not in text
    assert "--add-host=*) NET_ARGS+=" in text   # an --add-host moves to the holder: docker refuses it beside container:


def test_compose_files_add_the_topology_overlay_and_the_project(tmp_path):
    script = ('source "$1" --profile site --topology vip; printf "%s\\n" "${COMPOSE[@]}"; echo "VIP=$HPCB_VIP_BACKENDS"; '
              'echo "SVC=$TOPOLOGY_SERVICE TOPO=$HPCB_FAKE_TOPOLOGY"')
    env = {k: v for k, v in os.environ.items() if not k.startswith("HPCB_")}
    r = subprocess.run(["bash", "-c", script, "x", str(FC / "bin" / "compose_files.sh")], capture_output=True, text=True,
                       env={**env, "HPCB_FAKE_PROJECT": "hpcb-topo"}, timeout=60)
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[:4] == ["docker", "compose", "-p", "hpcb-topo"]
    assert lines[lines.index("VIP=login01.hpcb.test login02.hpcb.test") - 1].endswith("topologies/vip.yml")   # last
    assert "SVC=vip TOPO=vip" in lines
    plain = subprocess.run(["bash", "-c", 'source "$1" --profile default; printf "%s\\n" "${COMPOSE[@]}"', "x",
                            str(FC / "bin" / "compose_files.sh")], capture_output=True, text=True, env=env, timeout=60)
    assert "-p" not in plain.stdout.splitlines() and "topologies" not in plain.stdout   # unchanged without them
    bad = subprocess.run(["bash", "-c", 'source "$1" --topology moon', "x", str(FC / "bin" / "compose_files.sh")],
                         capture_output=True, text=True, env=env, timeout=60)
    assert bad.returncode != 0 and "unknown topology" in bad.stderr
    for name in ("bastion", "vip"):   # each overlay names the files it mounts
        text = (FC / "topologies" / f"{name}.yml").read_text()
        assert "edge" in text and all((FC / m).exists() for m in [
            ln.split(":")[0].strip(" -") for ln in text.splitlines() if "./topologies/" in ln])


def test_the_vip_fronts_every_login_node_and_gives_the_harness_one_port_each(tmp_path):
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "haproxy").write_text('#!/bin/sh\ncat "$3"\n')
    (bin_ / "haproxy").chmod(0o755)
    r = subprocess.run(["sh", str(FC / "topologies" / "vip-haproxy.sh")], capture_output=True, text=True, timeout=30,
                       env={"PATH": f"{bin_}:{os.environ['PATH']}", "HPCB_VIP_BACKENDS": " ".join(SITE),
                            "HPCB_VIP_CFG": str(tmp_path / "h.cfg")})
    cfg = r.stdout
    assert "bind :22\n" in cfg and "balance roundrobin" in cfg
    assert "server node1 login01.hpcb.test:22 " in cfg and "server node2 login02.hpcb.test:22 " in cfg
    assert "listen node1\n  bind :2201\n  server node1 login01.hpcb.test:22" in cfg
    assert "listen node2\n  bind :2202\n  server node2 login02.hpcb.test:22" in cfg
    assert topology.VIP_NODE_PORT0 == 2201


# ---- topology_roundtrip's graders ---------------------------------------------------------------------------------------

def _trace(*, pins=("172.22.0.7",), internal="172.22.0.0/16", restart=True, ok=True, masters=None, ssh_host="hpcb-fake"):
    c = lambda n, r, i=None: ToolCall.of(f"mcp__hpc-bridge__{n}", i or {}, r)   # noqa: E731
    calls = [c("connect_facility", {"phase": "provisioning",
                                    "notice": f"first contact over SSH: u@{pins[0] if pins else ssh_host} (…)"}),
             c("connect_facility", {"phase": "needs_account"}),
             c("ensure_endpoint_up", {"status": "up"}),
             c("run_shell", {"phase": "complete", "exit_code": 0, "stdout": "c1\nHPCB_TOPO_OK\n"}),
             c("stop_endpoint", {"status": "down"})]
    texts = [f"TOPO setup topology=host_forward ssh_host={ssh_host} internal={internal}"]
    text_at = [0]
    after = [c("connect_facility", {"phase": "needs_account"}),
             c("login_shell", {"exit_code": 0, "stdout": "login\n"} if ok else
               {"exit_code": 255, "notice": "CANNOT REACH 172.22.0.7"}),
             c("teardown_endpoint", {"status": "down"})]
    if restart:
        texts.append("server A killed (SIGKILL, pid 51) and started again on the same state directory")
        text_at.append(len(calls))
    calls += after
    for p in pins:
        texts.append(f"TOPO pin connect {p}")
        text_at.append(2)
    texts.append(f"TOPO masters {json.dumps(masters if masters is not None else {'60': '/h/cm/a', '129': '/h/cm/b'})}")
    text_at.append(len(calls))
    return Trace(calls, texts, text_at)


def test_every_step_is_read_off_the_tool_results():
    import topology_roundtrip as tr
    r = tr.every_step_succeeded(_trace())
    assert r.ok and r.detail.startswith("ok: connect → compute_block → run_shell → stop → restart → reconnect")
    bad = tr.every_step_succeeded(_trace(ok=False))
    assert not bad.ok and "login_shell FAILED" in bad.detail and "connect FAILED" not in bad.detail
    no_restart = tr.every_step_succeeded(_trace(restart=False))
    assert not no_restart.ok and "restart FAILED" in no_restart.detail


def test_an_internal_pin_is_caught_and_a_reachable_one_passes():
    import topology_roundtrip as tr
    assert tr.pins(_trace()) == ["172.22.0.7"]
    bad = tr.no_internal_pin(_trace())
    assert not bad.ok and "172.22.0.7" in bad.detail
    assert tr.no_internal_pin(_trace(internal="172.21.0.0/16")).ok              # in_network: the jail is on 172.22
    assert tr.no_internal_pin(_trace(pins=(), internal="172.22.0.0/16")).ok     # no pin at all: the alias throughout
    assert tr.pins(_trace(pins=())) == []                                        # the first contact named the alias


def test_a_destination_authenticated_twice_is_a_reauth():
    import topology_roundtrip as tr
    assert tr.no_reauth(_trace()).ok and "2 authentication(s) over 2 destination(s)" in tr.no_reauth(_trace()).detail
    again = tr.no_reauth(_trace(masters={"60": "/h/cm/a", "200": "/h/cm/a"}))
    assert not again.ok and "a ×2" in again.detail
    assert not tr.no_reauth(Trace([], ["nothing"], [0])).ok   # no watcher record: never a vacuous pass


def test_the_master_watcher_reads_mux_titles(tmp_path, monkeypatch):
    import topology_roundtrip as tr
    if not Path("/proc/self/cmdline").exists():
        pytest.skip("no /proc here (the jail has one)")
    assert tr._masters("/nowhere/cm") == {}


def test_topology_roundtrip_is_scripted_fake_only_and_gates_its_graders():
    import topology_roundtrip as tr
    assert tr.OPERATORS == ("script",) and tr.TARGETS == ("fake",) and tr.TEARDOWN == "delete"
    assert {"every_step_succeeded", "no_internal_pin", "no_reauth"} <= set(tr.EXPECT_OK)
    assert tr.EXTRA_ENV["HPC_BRIDGE_SSH_CONTROL_PERSIST"] == "900"
    assert [f.__name__ for f in tr.WORLD_CHECKS] == ["pin_and_preauth", "no_manager_after_product_teardown"]
