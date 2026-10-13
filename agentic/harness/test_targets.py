"""Hermetic: the target presets (globus1 | fake) and the prompt token substitution."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import targets  # noqa: E402


def test_presets_carry_every_target_fact_together(monkeypatch):
    monkeypatch.delenv("HPCB_TARGET", raising=False)
    g = targets.get()
    assert g.name == "globus1" and g.ssh_host == "globus1.cs.uchicago.edu" and g.nodes == 4   # spark1-4 since 2026-10
    assert g.capabilities["nodes"] == 4 and g.capabilities["catalog"] == "registry"
    assert g.endpoint_prefix == "hpc-bridge-globus1" and g.docker_network is None
    assert g.probe_argv[-1] == "globus1" and g.cleanup_argv("hpcbridge-test-03", "/k")[-1] == "hpcbridge-test-03@globus1.cs.uchicago.edu"
    monkeypatch.setenv("HPCB_FAKE_SSH_PORT", "2299")
    f = targets.get("fake")
    assert f.ssh_host == "login" and f.nodes == 2 and f.endpoint_prefix == "hpc-bridge-fake"
    assert f.docker_network == "hpcb-fake_default" and f.default_key.endswith("/.ssh/hpcb-fake")
    assert "2299" in f.probe_argv and f.probe_argv[-1] == "hpcbridge-test-00@localhost"
    argv = f.cleanup_argv("hpcbridge-test-01", "/k")
    assert argv[-1] == "hpcbridge-test-01@localhost" and "-p" in argv and "UserKnownHostsFile=/dev/null" in argv
    monkeypatch.setenv("HPCB_TARGET", "fake")
    assert targets.get().name == "fake"  # the env selects it
    with pytest.raises(SystemExit):
        targets.get("moon")


def test_fill_prompt_substitutes_both_tokens_literally():
    p = "Connect to `{ssh_host}` as `{facility}`; a code block with {other} braces stays: f'{x}'"
    out = targets.fill_prompt(p, facility="fake-1-2", ssh_host="login")
    assert out == "Connect to `login` as `fake-1-2`; a code block with {other} braces stays: f'{x}'"


def test_cli_prints_shell_assignments_for_run_smoke():
    r = subprocess.run([sys.executable, str(HERE / "targets.py"), "fake"], capture_output=True, text=True, timeout=30)
    kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
    assert r.returncode == 0 and kv["HPCB_T_SSH_HOST"] == "login" and kv["HPCB_T_NETWORK"] == "hpcb-fake_default"
    assert kv["HPCB_T_EP_PREFIX"] == "hpc-bridge-fake" and kv["HPCB_T_NODES"] == "2"
    bad = subprocess.run([sys.executable, str(HERE / "targets.py"), "moon"], capture_output=True, text=True, timeout=30)
    assert bad.returncode != 0


def test_every_ssh_scenario_names_the_login_host_by_token():
    # a literal globus1 host in a PROMPT/PHASES/USER_GOAL would silently send a fake-target run to the lab cluster
    import importlib
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    for p in sorted((HERE.parent / "scenarios").glob("*.py")):
        if p.stem.startswith("_"):
            continue
        mod = importlib.import_module(p.stem)
        texts = [getattr(mod, "PROMPT", ""), getattr(mod, "USER_GOAL", ""), *(getattr(mod, "PHASES", []) or [])]
        assert not any("globus1.cs.uchicago.edu" in t for t in texts), p.stem


def test_fake_profiles_load_and_requires_match(monkeypatch):
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "site")
    t = targets.get("fake")
    assert t.profile == "site" and t.nodes == 3 and t.capabilities["login_nodes"] == 2
    assert t.capabilities["accounting"] == "enforce" and "gpu" in t.capabilities["partitions"]
    assert t.capabilities["login_hosts"] == ["login01.hpcb.test", "login02.hpcb.test"]
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "default")
    d = targets.get("fake")
    assert d.nodes == 2 and d.capabilities["login_nodes"] == 1 and d.capabilities["balance_tool"] == "none"
    # REQUIRES vocabulary against capabilities
    assert targets.meets({"login_nodes": 2}, t.capabilities) == (True, "")
    ok, why = targets.meets({"login_nodes": 2}, d.capabilities)
    assert not ok and "login_nodes" in why
    assert targets.meets({"scheduler": "slurm", "min_nodes": 3, "accounting": "enforce", "min_partitions": 3}, t.capabilities)[0]
    assert not targets.meets({"min_nodes": 4}, t.capabilities)[0]
    assert not targets.meets({"scheduler": "pbs"}, targets.GLOBUS1_CAPABILITIES)[0]
    assert targets.meets(None, d.capabilities)[0] and targets.meets({}, d.capabilities)[0]
    with pytest.raises(SystemExit):
        targets.load_profile("no-such-profile")
    r = subprocess.run([sys.executable, str(HERE / "targets.py"), "fake"], capture_output=True, text=True, timeout=30,
                       env={**__import__("os").environ, "HPCB_FAKE_PROFILE": "site"})
    assert "HPCB_T_PROFILE=site" in r.stdout and '"login_nodes":2' in r.stdout


def test_login_pin_scenario_requires_two_login_nodes():
    r = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), "login_pin_teardown"], capture_output=True, text=True, timeout=60)
    kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
    assert kv.get("HPCB_KNOB_TARGETS") == "fake" and '"login_nodes": 2' in kv.get("HPCB_KNOB_REQUIRES", "")


def test_only_the_fake_target_has_an_admin_channel(monkeypatch):
    monkeypatch.delenv("HPCB_FAKE_CTLD", raising=False)
    assert targets.get("globus1").admin_argv is None
    f = targets.get("fake")
    assert f.admin_argv == ("docker", "exec", "hpcb-fake-slurmctld-1", "bash", "-lc")
    monkeypatch.setenv("HPCB_FAKE_CTLD", "other-ctld")
    assert targets.get("fake").admin_argv[2] == "other-ctld"
    # the CLI prints it shell-quoted for run_smoke.sh; empty = no channel
    out = subprocess.run([sys.executable, str(HERE / "targets.py"), "fake"], capture_output=True, text=True, timeout=60,
                         env={**__import__("os").environ, "HPCB_FAKE_CTLD": "hpcb-fake-slurmctld-1"}).stdout
    assert "HPCB_T_ADMIN_ARGV='docker exec hpcb-fake-slurmctld-1 bash -lc'" in out
    out = subprocess.run([sys.executable, str(HERE / "targets.py"), "globus1"], capture_output=True, text=True, timeout=60).stdout
    assert "HPCB_T_ADMIN_ARGV=\n" in out


def test_catalog_capability_says_whether_the_public_registry_is_the_catalog():
    # a profile with a [catalog] cmd mounts its own catalog (HPC_BRIDGE_CATALOG_FILE), which REPLACES the registry: the
    # registry scenarios (zero_config_list, needs_login_paste, registry_over_cache, mep_no_account) cannot run there
    import importlib
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    mep, default, site = (targets.load_profile(p)["capabilities"] for p in ("mep", "default", "site"))
    assert mep["catalog"] == "local" and default["catalog"] == "registry" and site["catalog"] == "registry"
    for name in ("zero_config_list", "needs_login_paste", "registry_over_cache", "mep_no_account"):
        req = importlib.import_module(name).REQUIRES
        assert not targets.meets(req, mep)[0], name
        assert targets.meets(req, default)[0] and targets.meets(req, targets.GLOBUS1_CAPABILITIES)[0], name


def test_list_valued_capabilities_are_membership_tests():
    site, default, pbs = (targets.load_profile(p)["capabilities"] for p in ("site", "default", "pbs"))
    assert targets.meets({"partitions": ["debug"], "accounts": ["hpcb"]}, site) == (True, "")
    assert targets.meets({"partitions": "debug"}, site)[0]                        # a bare name works too
    ok, why = targets.meets({"partitions": ["debug"]}, default)
    assert not ok and "partitions" in why and "main" in why
    assert not targets.meets({"partitions": ["debug", "nope"]}, site)[0]          # every named one must be on offer
    import importlib
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    req = importlib.import_module("hand_batch_job").REQUIRES     # `#SBATCH -p debug -A hpcb`: the site family only
    assert targets.meets(req, site)[0] and targets.meets(req, targets.load_profile("mep")["capabilities"])[0]
    assert not targets.meets(req, default)[0] and not targets.meets(req, pbs)[0]
