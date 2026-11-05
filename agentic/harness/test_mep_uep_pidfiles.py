"""Hermetic: the fake MEP profiles (`mep`, `pbs_mep`) clear the pid files their user endpoints left when the login
container last went down — and nothing else.

Why (free sweep 2026-10-16): every fake-MEP cell maps to `hpcbmep` with the same user-endpoint config, so they share ONE
user endpoint dir on the persistent `/home` (`~hpcbmep/.globus_compute/uep.<manager uuid>.<config hash>/`). The next
operator's run_suite ran bin/up.sh, whose `docker compose up -d --build` recreated the stack; that killed the previous
cell's user endpoint and left its `daemon.pid` behind. When the next cell's first task came, the manager forked the user
endpoint and globus-compute-endpoint refused to start it ("Another instance of this endpoint is running", rc 73), since
the file was younger than 3 heartbeats. codex got its endpoint 45 s late; hermes21 never got one; both cells failed.

`mep_clear_stale_uep_pids` (the profiles' shared `mep/tools.sh`, kept byte-for-byte identical by test_pbs_mep.py) is run
by each profile's login setup at container start, BEFORE any manager starts. Here it runs in a real bash against a tmp
tree, under the entrypoint's `set -euo pipefail`."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
FC = HERE.parent / "fakecluster"
TOOLS = {p: FC / "profiles" / p / "mep" / "tools.sh" for p in ("mep", "pbs_mep")}
EID = "2b3bdf70-f46d-44ee-8f13-4c83e4c3e1dc"        # this manager's uuid (the sweep's pbs_mep manager)
OTHER = "93a1a7d7-66a2-43f7-b74a-360d5be0244d"      # a manager this node does not run
HASH_A, HASH_B = "beaeb44d-18c0-57b1-9356-c8bd1214674c", "6749391c-68cd-05d9-66fe-2af86343891a"
HASH_C, HASH_D = "75ff860e-1941-035c-b9a1-630cf7f34aaf", "0a1b2c3d-4e5f-5a6b-8c7d-9e0f1a2b3c4d"


def _tree(tmp_path: Path) -> tuple[Path, Path]:
    """A mapped user's home holding user endpoint dirs, and a manager's endpoint dir naming EID (as globus-compute-endpoint
    writes them: `daemon.pid` holding the pid, an endpoint.log beside it)."""
    home, mgr = tmp_path / "home" / "hpcbmep", tmp_path / "state" / "hpcb-mep-pbs"
    mgr.mkdir(parents=True)
    (mgr / "endpoint.json").write_text(f'{{"endpoint_id": "{EID}"}}')
    for name in (f"uep.{EID}.{HASH_A}", f"uep.{EID}.{HASH_B}", f"uep.{OTHER}.{HASH_A}"):
        d = home / ".globus_compute" / name
        d.mkdir(parents=True)
        (d / "daemon.pid").write_text("459\n")
        (d / "endpoint.log").write_text("log\n")
        (d / "endpoint.json").write_text("{}")
    return home, mgr


def _clear(tools: Path, *args: str, path: str | None = None) -> subprocess.CompletedProcess:
    """Source tools.sh as the setup scripts do (under the entrypoint's `set -euo pipefail` + the script's `set -u`), run
    the cleanup, then prove the shell carried on."""
    env = {**os.environ, "PATH": path} if path is not None else None
    script = 'set -euo pipefail; set -u; source "$0"; mep_clear_stale_uep_pids "$@"; echo "ENTRYPOINT CONTINUED"'
    return subprocess.run([shutil.which("bash") or "/bin/bash", "-c", script, str(tools), *args], capture_output=True,
                          text=True, timeout=60, env=env)


@pytest.mark.parametrize("prof", sorted(TOOLS))
def test_a_dead_user_endpoints_pid_file_is_removed_and_nothing_else(tmp_path, prof):
    home, mgr = _tree(tmp_path)
    r = _clear(TOOLS[prof], str(home), str(mgr))
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    gc = home / ".globus_compute"
    for h in (HASH_A, HASH_B):   # both of this manager's user endpoints: gone with the container's last run
        assert not (gc / f"uep.{EID}.{h}" / "daemon.pid").exists(), h
        assert (gc / f"uep.{EID}.{h}" / "endpoint.log").read_text() == "log\n"   # only the pid file
        assert (gc / f"uep.{EID}.{h}" / "endpoint.json").is_file()
    assert (gc / f"uep.{OTHER}.{HASH_A}" / "daemon.pid").read_text() == "459\n"   # not this node's manager: untouched
    assert (mgr / "endpoint.json").is_file()
    assert "stale user-endpoint pid files removed: 2" in r.stdout


def _wait_for_pgrep(pattern: str) -> None:
    deadline = time.monotonic() + 10   # until pgrep sees it (the exec may lag the Popen)
    while subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode != 0:
        assert time.monotonic() < deadline, f"the stand-in user endpoint {pattern} never showed in pgrep"
        time.sleep(0.1)


def test_a_live_user_endpoint_keeps_its_pid_file(tmp_path):
    # A live user endpoint shuts itself down when its pid file disappears (it re-touches it every 15 s): one running
    # here must keep it, whatever path ran the cleanup. Two stand-ins: one still showing its entry-point command line,
    # one showing the process title 4.18 sets once it runs (`Globus Compute Endpoint (<id>, <name>) [<state>]`).
    if shutil.which("pgrep") is None:
        pytest.skip("no pgrep on this host")
    home, mgr = _tree(tmp_path)
    gc = home / ".globus_compute"
    (gc / f"uep.{EID}.{HASH_C}").mkdir()
    (gc / f"uep.{EID}.{HASH_C}" / "daemon.pid").write_text("460\n")   # a third, dead one
    argv = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)",
                             "globus-compute-endpoint", "_start-user-endpoint", f"uep.{EID}.{HASH_B}"])
    title = subprocess.Popen([shutil.which("bash") or "/bin/bash", "-c",
                              f"exec -a 'Globus Compute Endpoint ({HASH_A}, uep.{EID}.{HASH_A}) [running]' sleep 60"])
    try:
        _wait_for_pgrep(f"uep.{EID}.{HASH_B}")
        _wait_for_pgrep(f"Globus Compute Endpoint .*uep.{EID}.{HASH_A}")
        r = _clear(TOOLS["pbs_mep"], str(home), str(mgr))
    finally:
        for proc in (argv, title):
            proc.kill()
            proc.wait()
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    assert (gc / f"uep.{EID}.{HASH_B}" / "daemon.pid").read_text() == "459\n"      # live (argv): kept
    assert (gc / f"uep.{EID}.{HASH_A}" / "daemon.pid").read_text() == "459\n"      # live (process title): kept
    assert not (gc / f"uep.{EID}.{HASH_C}" / "daemon.pid").exists()               # dead: removed
    assert f"uep.{EID}.{HASH_B} is running here" in r.stdout and f"uep.{EID}.{HASH_A} is running here" in r.stdout
    assert "removed: 1" in r.stdout


def _bin(tmp_path: Path, pgrep: str | None = None) -> str:
    """A PATH holding only what the helper needs, and `pgrep` only as given (a script body), else none."""
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    for tool in ("sed", "head", "rm"):
        (bin_ / tool).symlink_to(shutil.which(tool))
    if pgrep is not None:
        (bin_ / "pgrep").write_text(f"#!/bin/sh\n{pgrep}\n")
        (bin_ / "pgrep").chmod(0o755)
    return str(bin_)


def test_without_pgrep_nothing_is_removed(tmp_path):
    # no way to rule out a live user endpoint -> keep every pid file (the precondition alone is not trusted)
    home, mgr = _tree(tmp_path)
    r = _clear(TOOLS["pbs_mep"], str(home), str(mgr), path=_bin(tmp_path))
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    gc = home / ".globus_compute"
    for h in (HASH_A, HASH_B):
        assert (gc / f"uep.{EID}.{h}" / "daemon.pid").read_text() == "459\n", h
    assert "no pgrep" in r.stdout and "removed: 0" in r.stdout


@pytest.mark.parametrize("rc", [2, 3])
def test_a_pgrep_error_keeps_the_file(tmp_path, rc):
    # only pgrep's "no process matched" (exit 1) lets a file go; a syntax error (2) or a fatal one (3, /proc trouble)
    # says nothing about whether the user endpoint runs
    home, mgr = _tree(tmp_path)
    r = _clear(TOOLS["mep"], str(home), str(mgr), path=_bin(tmp_path, pgrep=f"exit {rc}"))
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    gc = home / ".globus_compute"
    for h in (HASH_A, HASH_B):
        assert (gc / f"uep.{EID}.{h}" / "daemon.pid").read_text() == "459\n", h
    assert f"pgrep failed (rc {rc})" in r.stdout and "removed: 0" in r.stdout
    # and the same PATH with a "no match" pgrep does remove them: the error, not the PATH, is what kept them
    (tmp_path / "again").mkdir()
    r = _clear(TOOLS["mep"], str(home), str(mgr), path=_bin(tmp_path / "again", pgrep="exit 1"))
    assert r.returncode == 0 and "removed: 2" in r.stdout, r.stdout + r.stderr


def test_the_pgrep_pattern_is_the_name_with_its_dots_literal(tmp_path):
    # the uep name reaches pgrep as an ERE: its dots escaped, nothing else to escape (a manager writes only hex, dashes
    # and dots, and no other name is taken)
    home, mgr = _tree(tmp_path)
    r = _clear(TOOLS["pbs_mep"], str(home), str(mgr),
               path=_bin(tmp_path, pgrep='printf "%s\\n" "$*" >> "${0%/bin/pgrep}/pgrep.args"; exit 1'))
    assert r.returncode == 0 and "removed: 2" in r.stdout, r.stdout + r.stderr
    want = sorted("-f " + f"uep.{EID}.{h}".replace(".", "\\.") for h in (HASH_A, HASH_B))
    assert sorted((tmp_path / "pgrep.args").read_text().splitlines()) == want


def test_symlinks_and_foreign_names_are_never_followed_or_taken(tmp_path):
    # Root runs this over ~hpcbmep/.globus_compute, which the agent (running as hpcbmep) can write: a planted symlink
    # must never make root unlink a daemon.pid somewhere else, and a name a manager never writes is not taken.
    home, mgr = _tree(tmp_path)
    gc = home / ".globus_compute"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "daemon.pid").write_text("victim\n")
    (outside / "victim").write_text("victim\n")
    (gc / f"uep.{EID}.{HASH_C}").symlink_to(outside, target_is_directory=True)       # a symlinked uep dir
    real = gc / f"uep.{EID}.{HASH_D}"
    real.mkdir()
    (real / "daemon.pid").symlink_to(outside / "victim")                              # a symlinked daemon.pid
    for odd in ("x[", "ABCDEF", "a b", ""):                                           # not a manager's names
        (gc / f"uep.{EID}.{odd}").mkdir(exist_ok=True)
        (gc / f"uep.{EID}.{odd}" / "daemon.pid").write_text("459\n")
    r = _clear(TOOLS["pbs_mep"], str(home), str(mgr))
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    assert (outside / "daemon.pid").read_text() == "victim\n" and (outside / "victim").read_text() == "victim\n"
    assert (gc / f"uep.{EID}.{HASH_C}").is_symlink() and (real / "daemon.pid").is_symlink()
    for odd in ("x[", "ABCDEF", "a b", ""):
        assert (gc / f"uep.{EID}.{odd}" / "daemon.pid").read_text() == "459\n", odd
    assert r.stdout.count("a symlink, never followed") == 2
    assert "removed: 2" in r.stdout                                                   # HASH_A, HASH_B still go
    # a symlinked ~/.globus_compute: nothing under it is touched
    home2 = tmp_path / "home2"
    home2.mkdir()
    (home2 / ".globus_compute").symlink_to(gc, target_is_directory=True)
    (gc / f"uep.{EID}.{HASH_A}" / "daemon.pid").write_text("459\n")
    r = _clear(TOOLS["mep"], str(home2), str(mgr))
    assert r.returncode == 0 and "a symlink, never followed" in r.stdout and "removed: 0" in r.stdout, r.stdout
    assert (gc / f"uep.{EID}.{HASH_A}" / "daemon.pid").is_file()


def test_an_unregistered_manager_clears_nothing_and_never_stops_the_entrypoint(tmp_path):
    home, mgr = _tree(tmp_path)
    (mgr / "endpoint.json").unlink()   # first boot: the manager has not minted its uuid, so it forked nothing
    for args in ((str(home), str(mgr)), (str(home), str(tmp_path / "no-such-manager")),
                 (str(tmp_path / "no-such-home"), str(mgr)), (str(home),)):
        r = _clear(TOOLS["mep"], *args)
        assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout and "removed: 0" in r.stdout, (args, r.stderr)
    assert all((d / "daemon.pid").is_file() for d in (home / ".globus_compute").iterdir())


# --- the wiring: both login setups clear before any manager starts, on the node that runs the managers -------------

def _code(path: Path) -> str:
    return re.sub(r"#.*", "", path.read_text())


def test_pbs_mep_clears_its_managers_user_endpoints_before_it_starts_the_manager():
    login = _code(FC / "profiles" / "pbs_mep" / "setup.d" / "login-pbsmep.sh")
    call = 'mep_clear_stale_uep_pids "$h" "$d"'
    assert login.count(call) == 1, "one call, with the mapped user's home and the manager's dir"
    assert login.index('source "$PROFILE_DIR/mep/tools.sh"') < login.index(call)
    assert login.index('h="/home/$MEPUSER"') < login.index(call) and login.index("d=\"/root/.globus_compute/$name\"") < login.index(call)
    assert login.index(call) < login.index('globus-compute-endpoint" start "$name"')   # before the manager can fork one
    assert re.search(re.escape(call) + r" *\\?\n? *\|\| *echo", login), "guarded: a failure must not stop the entrypoint"


def test_mep_clears_both_managers_user_endpoints_on_login01_before_either_starts():
    login = _code(FC / "profiles" / "mep" / "setup.d" / "login-mep.sh")
    call = ('mep_clear_stale_uep_pids "$h" /root/.globus_compute/hpcb-mep-strict '
            "/root/.globus_compute/hpcb-mep-open")
    assert login.count(call) == 1
    # on login01 only (the managers' node — login02 shares /home, and a pid file removed under a LIVE user endpoint on
    # login01 would stop it), after tools.sh is sourced, before the loop that starts the managers
    assert login.index('if [ "$(hostname -s)" = login01 ]; then') < login.index('source "$PROFILE_DIR/mep/tools.sh"') \
        < login.index(call) < login.index("for kind in strict open; do") < login.index('globus-compute-endpoint" start')
    assert re.search(re.escape(call) + r" *\\?\n? *\|\| *echo", login)
    assert 'name="hpcb-mep-$kind"; d="/root/.globus_compute/$name"' in login   # the dirs named are the managers'
