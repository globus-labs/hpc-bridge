"""Hermetic: fake-cluster profile INHERITANCE (bin/profile.py) and what targets.py derives from a layered profile."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import targets  # noqa: E402

FC = HERE.parent / "fakecluster"
spec = importlib.util.spec_from_file_location("hpcb_profile", FC / "bin" / "profile.py")
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def test_mep_layers_on_site_and_merges_capabilities():
    m = profile.manifest("mep")
    assert m["layers"] == ["site", "mep"] and m["name"] == "mep"
    caps = m["capabilities"]
    assert caps["login_hosts"] == ["login01.hpcb.test", "login02.hpcb.test"] and caps["nodes"] == 3   # from site
    assert caps["mep"] == "consent-free" and caps["mep_schemas"] == ["strict", "open"]                    # from mep
    assert m["catalog"]["cmd"].startswith("docker exec hpcb-fake-login-1 ")
    assert "base" not in m
    assert profile.manifest("site")["layers"] == ["site"] and "catalog" not in profile.manifest("site")


def test_build_materialises_base_files_plus_overlay_and_a_flat_manifest(tmp_path):
    out = tmp_path / "mep"
    m = profile.build("mep", out)
    assert (out / "slurm.conf").is_file() and (out / "job_submit.lua").is_file()          # site's
    assert (out / "setup.d" / "login.sh").is_file() and (out / "setup.d" / "login-mep.sh").is_file()   # both layers
    assert (out / "mep" / "schema-strict.json").is_file() and (out / "mep" / "hpcb-mep-catalog").is_file()
    assert (out / "deregister.sh").is_file() and (out / "mep" / "tools.sh").is_file()   # down.sh --wipe runs the former
    import os
    assert os.access(out / "deregister.sh", os.X_OK)
    assert not (out / "compose.override.yml").exists()   # overlays go to compose -f, never into the mounted dir
    import tomllib
    flat = tomllib.loads((out / "profile.toml").read_text())
    assert flat["capabilities"]["mep"] == "consent-free" and flat["capabilities"]["nodes"] == 3 and "base" not in flat
    assert flat["catalog"]["cmd"] == m["catalog"]["cmd"]
    # rebuilding replaces (no stale files)
    (out / "stale").write_text("x")
    profile.build("mep", out)
    assert not (out / "stale").exists()


def test_unknown_profile_and_cycles_are_refused():
    with pytest.raises(SystemExit):
        profile.manifest("moon")


def test_targets_reads_the_layered_profile_and_the_catalog_command(monkeypatch):
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "mep")
    t = targets.get("fake")
    assert t.profile == "mep" and t.nodes == 3 and t.capabilities["mep"] == "consent-free"
    assert t.catalog_cmd == "docker exec hpcb-fake-login-1 /usr/local/bin/hpcb-mep-catalog"
    assert targets.meets({"mep": "consent-free", "accounting": "enforce", "login_nodes": 2}, t.capabilities)[0]
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "site")
    assert targets.get("fake").catalog_cmd is None
    assert targets.get("globus1").catalog_cmd is None
    import subprocess
    out = subprocess.run([sys.executable, str(HERE / "targets.py"), "fake"], capture_output=True, text=True, timeout=60,
                         env={**__import__("os").environ, "HPCB_FAKE_PROFILE": "mep"}).stdout
    assert "HPCB_T_CATALOG_CMD='docker exec hpcb-fake-login-1 /usr/local/bin/hpcb-mep-catalog'" in out


def test_pbs_profile_selects_its_own_stack_and_declares_the_scheduler(monkeypatch):
    m = profile.manifest("pbs")
    assert m["compose"] == "docker-compose.pbs.yml" and m["capabilities"]["scheduler"] == "pbs" and m["capabilities"]["nodes"] == 2
    assert m["capabilities"]["partitions"] == ["workq", "debug"] and m["layers"] == ["pbs"]
    import subprocess as sp
    out = sp.run([sys.executable, str(FC / "bin" / "profile.py"), "build", "pbs", str(FC / ".merged" / "pbs-test")], capture_output=True, text=True, timeout=60).stdout
    assert "PROFILE_COMPOSE=docker-compose.pbs.yml" in out and "PROFILE_SCHEDULER=pbs" in out and "PROFILE_NODES=2" in out
    out = sp.run([sys.executable, str(FC / "bin" / "profile.py"), "build", "site", str(FC / ".merged" / "site-test")], capture_output=True, text=True, timeout=60).stdout
    assert "PROFILE_COMPOSE=docker-compose.yml" in out and "PROFILE_SCHEDULER=slurm" in out
    import shutil
    shutil.rmtree(FC / ".merged" / "pbs-test", ignore_errors=True)
    shutil.rmtree(FC / ".merged" / "site-test", ignore_errors=True)
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "pbs")
    t = targets.get("fake")
    assert t.capabilities["scheduler"] == "pbs" and t.nodes == 2
    assert targets.meets({"scheduler": "pbs"}, t.capabilities)[0] and not targets.meets({"scheduler": "pbs"}, targets.load_profile("site")["capabilities"])[0]


def test_every_boot_gives_each_pool_user_its_own_writable_home():
    # f2b, re-run 2026-10-23: the two login nodes raced to create the shared homes; the loser skipped a half-made,
    # root-owned home, and every SETUP write to ~ was refused — six cells SETUP FAILED "Permission denied"
    fc = Path(__file__).resolve().parents[1] / "fakecluster"
    ep = (fc / "entrypoint.sh").read_text()
    loop = ep[ep.index('for u in "${POOL_USERS[@]}"; do'):ep.index("run_setup login")]
    fi_end = loop.index("    fi\n")
    assert 'chown "$u:hpcb" "$h" && chmod 700 "$h"' in loop[fi_end:], "ownership must be set outside the create branch"
    assert "cp -rTn /etc/skel" in loop                                   # a second node never overwrites the first's
    wait = (fc / "bin" / "wait-for-cluster.sh").read_text()
    assert '.hpcb-write-check' in wait and "cannot write its home" in wait   # ready means the pool user can write ~
