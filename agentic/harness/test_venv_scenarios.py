"""Hermetic: the endpoint-venv scenarios (0.1.24) — their graders, their target coupling, and their world setup run for
real against a stub venv in a temp home (the same stubs the product's tests use, `tests/venv_stubs.py`), together with
the product's own env_setup line, so what a live cell plants is what the line then meets."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))
sys.path.insert(0, str(ROOT))

from invariants import ToolCall, Trace  # noqa: E402


def _c(name: str, inp: dict | None = None, res: dict | None = None) -> ToolCall:
    return ToolCall.of(f"mcp__endpoint__{name}", inp or {}, res)


# ---- graders ---------------------------------------------------------------------------------------------------------

def test_venv_broken_repair_graders():
    import venv_broken_repair as sc

    connect = _c("connect_facility", {"facility": "f"}, {"phase": "needs_account", "notice":
                 "built hpc-bridge's endpoint venv /home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11 (its "
                 "globus-compute-endpoint fails: ImportError: planted); the broken one was moved to "
                 "/home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11.broken.20261013-120000. "})
    login = _c("run_shell", {"command": "hostname", "shape": "login"}, {"phase": "complete", "stdout": "login"})
    shapeless = _c("run_shell", {"command": "hostname"}, {"phase": "complete", "stdout": "c1"})  # the compute shape
    assert not sc.bring_up_reached_up(Trace([shapeless])).ok
    assert "leave env_setup out of details=" in sc.PROMPT
    good = Trace([connect, login, _c("stop_endpoint", {}, {"status": "down"})])
    assert sc.bring_up_reached_up(good).ok and sc.repair_reported(good).ok and sc.no_hand_repair(good).ok
    failed = Trace([_c("connect_facility", {}, {"phase": "failed", "notice": "remote configure failed: Traceback"})])
    assert not sc.bring_up_reached_up(failed).ok and not sc.repair_reported(failed).ok
    hand = Trace([connect, _c("login_shell", {"command": "~/hpc-bridge/x/bin/python -m pip install globus-sdk"}, {})])
    assert not sc.no_hand_repair(hand).ok
    assert sc.TARGETS == ("fake",) and not sc.NEEDS_COMPUTE_NODE and sc.PERSONA is None
    assert not getattr(sc, "MIDRUN_HOOKS", None) and not getattr(sc, "PHASES", None)  # free operators can run it
    names = {p["name"] for p in sc.POSTCHECKS}
    assert names == {"keyed_venv_at_the_pin", "broken_venv_kept_aside", "no_lock_left"}
    assert all(n in sc.EXPECT_OK for n in ("bring_up_reached_up", "repair_reported"))


def test_venv_arch_mismatch_graders():
    import venv_arch_mismatch as sc

    said = ("allocating nodes… — pilot 1108 already FINISHED (exit status 1): its job said "
            "'hpc-bridge: endpoint venv /home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11 (built on x86_64) does not "
            "run on c1 (aarch64)'.")
    up = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", "notice": said})
    stop = _c("stop_endpoint", {}, {"status": "down", "notice": "released 1"})
    good = Trace([up, stop], ["The block never ran: the endpoint's environment was built on x86_64 and does not run on "
                              "the aarch64 compute node c1. I stopped the endpoint."])
    assert sc.arch_mismatch_surfaced(good).ok and sc.no_endless_wait_on_a_dead_block(good).ok
    assert sc.no_false_success(good).ok
    forever = Trace([up] + [_c("ensure_endpoint_up", {"shape": "compute"}, {"status": "provisioning", "notice": said})] * 4)
    assert not sc.no_endless_wait_on_a_dead_block(forever).ok
    liar = Trace([up, _c("run_shell", {"command": "hostname", "shape": "compute"}, {"phase": "complete", "stdout": "c1"})],
                 ["hostname ran on c1."])
    assert not sc.no_false_success(liar).ok
    assert not sc.arch_mismatch_surfaced(Trace([_c("ensure_endpoint_up", {}, {"status": "provisioning",
                                                                              "notice": "allocating nodes…"})])).ok
    same = said.replace("(built on x86_64)", "(built on aarch64)")  # not an architecture mismatch
    assert not sc.arch_mismatch_surfaced(Trace([_c("ensure_endpoint_up", {}, {"status": "provisioning", "notice": same})])).ok
    # a shape-less call is the compute shape (the server's default): it counts as a poll of the dead block
    shapeless = _c("ensure_endpoint_up", {}, {"status": "provisioning", "notice": said})
    assert not sc.no_endless_wait_on_a_dead_block(Trace([up] + [shapeless] * 4)).ok
    assert sc.TARGETS == ("fake",) and sc.NEEDS_COMPUTE_NODE and sc.PERSONA is None and sc.CLEANUP


def test_venv_arch_mismatch_the_line_the_agent_read_itself_counts():
    # the review of the 2026-10-22 sweep: an agent that cat'ed the pilot's stderr on the login shape saw the very line
    # the probe quotes — _said_idxs read only notices, and arch_mismatch_surfaced / no_false_success failed it
    import venv_arch_mismatch as sc
    start = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", "notice": "allocating"})
    err = _c("run_shell", {"shape": "login", "command": "cat ~/.hpc-bridge/*/submit_scripts/*.stderr"},
             {"phase": "complete", "stdout": "hpc-bridge: endpoint venv /x (built on aarch64) does not run on c1 (x86_64)"})
    stop = _c("stop_endpoint", {}, {"status": "down", "notice": "released 5"})
    t = Trace([start, err, stop], ["The venv was built for aarch64 and does not run on c1 (x86_64)."], [2])
    assert sc.arch_mismatch_surfaced(t).ok and sc.no_false_success(t).ok
    # the same words as a COMPUTE command's output are not a read of the login node's record
    on_compute = _c("run_shell", {"shape": "compute", "command": "x"}, {"phase": "failed", "stdout": err.result["stdout"]})
    assert not sc.arch_mismatch_surfaced(Trace([start, on_compute])).ok


def test_venv_arch_mismatch_a_success_claim_fails_and_the_cause_must_follow_the_line():
    # the review's probe: "Login node architecture: x86_64." (before anything failed) + "hostname ran successfully on
    # c1." passed no_false_success — the docstring promised a false claim fails it
    import venv_arch_mismatch as sc
    start = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", "notice": "allocating"})
    said = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "provisioning", "notice":
              "pilot 5: hpc-bridge: endpoint venv /x (built on aarch64) does not run on c1 (x86_64)"})
    stop = _c("stop_endpoint", {}, {"status": "down", "notice": "released 5"})
    liar = Trace([start, said, stop], ["Login node architecture: x86_64.", "hostname ran successfully on c1."], [1, 3])
    r = sc.no_false_success(liar)
    assert not r.ok and "claimed the command ran" in r.detail, r.detail
    early = Trace([start, said, stop], ["Login node architecture: x86_64.", "Stopped the endpoint."], [1, 3])
    assert not sc.no_false_success(early).ok          # the architecture named only BEFORE the line
    honest = ["The block never ran: the venv was built on aarch64 and does not run on c1 (x86_64); hostname never ran.",
              "The pilot ran on c1 and died there. The login node's own hostname command ran fine."]
    assert sc.no_false_success(Trace([start, said, stop], honest, [3, 3])).ok



def test_endpoint_upgrade_reuse_graders():
    import endpoint_upgrade_reuse as sc

    eid = "da3df250-4013-4d69-942c-eef1568f860c"
    connect = _c("connect_facility", {"facility": "f", "ssh_host": "login"},
                 {"phase": "needs_account", "reused": True, "notice": "reused the already-online endpoint"})
    up = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": eid, "notice": "warm"})
    run = _c("run_shell", {"command": "hostname", "shape": "compute"},
             {"phase": "complete", "stdout": "c1", "endpoint_id": eid})
    good = Trace([connect, up, run, _c("stop_endpoint", {}, {"status": "down"})])
    assert sc.reused_old_endpoint(good).ok and sc.one_endpoint_throughout(good).ok and sc.no_env_differs_note(good).ok
    noted = Trace([connect, _c("ensure_endpoint_up", {}, {"status": "provisioning", "notice":
                  "this endpoint was started with a different env_setup than this facility's current one"})])
    assert not sc.no_env_differs_note(noted).ok
    fresh = Trace([_c("connect_facility", {}, {"phase": "needs_account", "reused": False}), up])
    assert not sc.reused_old_endpoint(fresh).ok
    two = Trace([up, _c("ensure_endpoint_up", {}, {"endpoint_id": "11111111-2222-3333-4444-555555555555"})])
    assert not sc.one_endpoint_throughout(two).ok
    # phase 1 is simulated: 0.1.23's cached config, and its endpoint stood up by the harness inside the jail
    assert sc.SEED_FACILITY_CACHE["login"]["env_setup"].startswith("[ -d {venv} ] || uv venv {venv} --python ")
    import shlex
    words = shlex.split(sc.LOCAL_SETUP[0])
    assert words[-2].endswith("agentic/harness/old_endpoint.py") and json.loads(words[-1]) == sc._DETAILS
    assert sc.TARGETS == ("fake",) and sc.PERSONA is None and not getattr(sc, "PHASES", None)


def test_the_upgrade_scenarios_cached_line_is_what_0_1_23_generated_and_reads_as_the_short_line():
    import endpoint_upgrade_reuse as sc
    import old_endpoint

    from hpc_bridge.facility import endpoint_venv as ev

    assert old_endpoint.old_discovery_line(sc.PY_V, sc.GCE_V) == sc._OLD_LINE
    new = ev.migrate_env_setup(sc._OLD_LINE, python=sc.PY_V, gce=sc.GCE_V)
    assert new == ev.short_env_setup("{venv}", python=sc.PY_V, gce=sc.GCE_V)
    # the endpoint's own line (resolved) upgrades to exactly the profile's: no env-differs note
    own = sc._OLD_LINE.replace("{venv}", "/home/hpcbridge-test-03/hpc-bridge/gce-venv")
    assert ev.migrate_env_setup(own, python=sc.PY_V, gce=sc.GCE_V) == new.replace(
        "{venv}", "/home/hpcbridge-test-03/hpc-bridge/gce-venv")


def test_the_sweep_runs_them_on_the_default_profile():
    import re
    text = (HERE.parent / "sweep_profiles.sh").read_text()
    default = re.search(r'^\s+default\)\s+echo "([\w,]+) \d+" ;;', text, re.M).group(1).split(",")
    assert {"venv_broken_repair", "venv_arch_mismatch", "endpoint_upgrade_reuse"} <= set(default)


# ---- the world setup, run for real ------------------------------------------------------------------------------------

@pytest.fixture
def world(tmp_path, monkeypatch):
    """A home with the keyed venv the scenarios plant into (a stub gce at the plugin's pin)."""
    import cluster_ops

    from tests import venv_stubs as vs

    (tmp_path / "stubs").mkdir()
    (tmp_path / "box").mkdir()
    stubs = vs.make_stubs(tmp_path / "stubs")
    monkeypatch.setenv("HPCB_TEST_TEMPLATE", str(stubs["template"]))
    box = vs.make_box(tmp_path / "box", stubs)
    gce, _py = cluster_ops.plugin_versions()
    key = Path(cluster_ops.keyed_endpoint_venv(gce, vs.PY).replace("$HOME", str(box["home"])))
    vs.venv(box, key, gce)
    return {"box": box, "key": key, "gce": gce, "vs": vs}


def test_the_upgrade_scenarios_setup_leaves_0_1_23s_line_a_venv_whose_list_works(tmp_path, monkeypatch):
    # 0.1.23's line installs the endpoint unbounded, so since texttable 1.7.2 (2026-10-18) its venv's `list` raised and
    # the old bootstrap failed after its `start`, leaving a manager on the pool user. SETUP now builds the unkeyed venv
    # first, with texttable pinned; the line's guards leave it, and its own install never runs.
    import endpoint_upgrade_reuse as sc

    from tests import venv_stubs as vs

    (tmp_path / "stubs").mkdir()
    (tmp_path / "box").mkdir()
    stubs = vs.make_stubs(tmp_path / "stubs")
    monkeypatch.setenv("HPCB_TEST_TEMPLATE", str(stubs["template"]))
    box = vs.make_box(tmp_path / "box", stubs)

    def sh(cmd: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", "-c", cmd], env=vs.env(box), capture_output=True, text=True, timeout=60,
                              check=False)

    for cmd in sc.SETUP:  # as the cell runs them, in order
        r = sh(cmd)
        assert r.returncode == 0, (cmd, r.stderr)
    (box["home"] / ".globus_compute" / "hpc-bridge-fake-local").mkdir(parents=True)
    old = sc._OLD_LINE.replace("{venv}", str(box["venv"]))  # resolved, as old_endpoint.py resolves it
    r = sh(f"{old} && globus-compute-endpoint list")
    assert r.returncode == 0 and "hpc-bridge-fake-local" in r.stdout, r.stderr
    installs = [c for c in vs.calls(box) if c[:3] == ["uv", "pip", "install"]]
    assert len(installs) == 1 and "texttable<1.7.2" in installs[0]  # SETUP's: the line's own install was skipped
    assert f"globus-compute-endpoint=={sc.GCE_V}" in installs[0]


def _sh(cmd: str, home: Path, extra_path: str = "") -> subprocess.CompletedProcess:
    return subprocess.run(["bash", "-c", cmd], env={"HOME": str(home), "PATH": f"{extra_path}/usr/bin:/bin", "LANG": "C"},
                          capture_output=True, text=True, timeout=60, check=False)


def _retarget(cmd: str, sc, world) -> str:
    """The scenario's command, for the stub venv (keyed by this interpreter's Python, not the jail's)."""
    return cmd.replace(sc.KEY, str(world["key"]))


def test_the_repair_scenarios_break_is_a_replaced_file(world):
    import venv_broken_repair as sc

    key, vs = world["key"], world["vs"]
    cli = next(key.glob("lib/python*/site-packages/globus_compute_endpoint/cli.py"))
    other = cli.with_name("cli-hardlink.py")
    os.link(cli, other)  # a hardlink, as uv's cache would hold one
    r = _sh(_retarget(sc.SETUP[2], sc, world), world["box"]["home"])
    assert r.returncode == 0 and "planted" in r.stdout, r.stderr
    assert vs.version_of(key) is None and "planted by venv_broken_repair" in cli.read_text()
    assert "planted" not in other.read_text()  # the linked copy (the "cache") is untouched: mv, never an edit
    # the world checks then read the repaired venv / the moved-aside break
    check = next(p for p in sc.POSTCHECKS if p["name"] == "broken_venv_kept_aside")["cmd"]
    shutil.move(str(key), f"{key}.broken.20261013-120000")
    r = _sh(_retarget(check, sc, world), world["box"]["home"])
    assert "cli.py" in r.stdout, r


def test_the_arch_scenarios_plant_fails_only_off_the_login_node_and_the_line_names_it(world, tmp_path):
    import venv_arch_mismatch as sc

    from hpc_bridge.facility import endpoint_venv as ev

    key, vs, home = world["key"], world["vs"], world["box"]["home"]
    fake = tmp_path / "node"
    fake.mkdir()

    def node(name: str) -> str:
        (fake / "hostname").write_text(f"#!/bin/sh\necho {name}\n")
        (fake / "hostname").chmod(0o755)
        (fake / "uname").write_text('#!/bin/sh\nif [ "$1" = -m ]; then echo aarch64; else exec /usr/bin/uname "$@"; fi\n')
        (fake / "uname").chmod(0o755)
        return f"{fake}:"

    r = _sh(_retarget(sc.SETUP[1], sc, world), home, node("login"))
    assert r.returncode == 0 and f"version: {world['gce']}" in r.stdout, r.stderr  # the login node's check passes
    assert (key / ".hpcb-arch").read_text().strip() == "x86_64"  # the other architecture than this "cluster's" aarch64
    r = _sh(f'"{key}/bin/globus-compute-endpoint" version', home, node("c1"))
    assert r.returncode == 2 and "Syntax error" in r.stderr  # what a wrong-arch python gives uv's trampoline
    # the product's line, as a block's worker_init on c1
    line = ev.short_env_setup(str(home / "hpc-bridge" / "gce-venv"), python=vs.PY, gce=world["gce"])
    r = subprocess.run(["bash", "-c", line + '\necho "HPCB_DONE"\n'], capture_output=True, text=True, check=False,
                       env={"HOME": str(home), "PATH": f"{node('c1')}/usr/bin:/bin", "SLURM_JOB_ID": "1108"})
    assert r.returncode == 1 and "HPCB_DONE" not in r.stdout
    assert f"hpc-bridge: endpoint venv {key} (built on x86_64) does not run on c1 (aarch64)" in r.stderr, r.stderr
    # cleanup puts it back
    r = _sh(_retarget(sc.CLEANUP[0], sc, world), home, node("c1"))
    assert vs.version_of(key) == world["gce"] and not (key / "bin" / ".gce-real").exists()
