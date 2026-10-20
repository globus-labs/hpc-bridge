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


def test_a_live_user_endpoint_keeps_its_pid_file(tmp_path):
    # A live user endpoint shuts itself down when its pid file disappears (it re-touches it every 15 s): one running
    # here must keep it, whatever path ran the cleanup.
    home, mgr = _tree(tmp_path)
    if shutil.which("pgrep") is None:
        pytest.skip("no pgrep on this host")
    live = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)",
                             "globus-compute-endpoint", "_start-user-endpoint", f"uep.{EID}.{HASH_B}"])
    try:
        deadline = time.monotonic() + 10   # until pgrep sees it (the exec may lag the Popen)
        while subprocess.run(["pgrep", "-f", f"uep.{EID}.{HASH_B}"], capture_output=True).returncode != 0:
            assert time.monotonic() < deadline, "the stand-in user endpoint never showed in pgrep"
            time.sleep(0.1)
        r = _clear(TOOLS["pbs_mep"], str(home), str(mgr))
    finally:
        live.kill()
        live.wait()
    gc = home / ".globus_compute"
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    assert (gc / f"uep.{EID}.{HASH_B}" / "daemon.pid").read_text() == "459\n"      # live: kept
    assert not (gc / f"uep.{EID}.{HASH_A}" / "daemon.pid").exists()               # dead: removed
    assert f"uep.{EID}.{HASH_B} is running here" in r.stdout and "removed: 1" in r.stdout


def test_without_pgrep_nothing_is_removed(tmp_path):
    # no way to rule out a live user endpoint -> keep every pid file (the precondition alone is not trusted)
    home, mgr = _tree(tmp_path)
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    for tool in ("sed", "head", "basename", "dirname", "rm"):
        (bin_ / tool).symlink_to(shutil.which(tool))
    r = _clear(TOOLS["pbs_mep"], str(home), str(mgr), path=str(bin_))
    assert r.returncode == 0 and "ENTRYPOINT CONTINUED" in r.stdout, r.stdout + r.stderr
    gc = home / ".globus_compute"
    for h in (HASH_A, HASH_B):
        assert (gc / f"uep.{EID}.{h}" / "daemon.pid").read_text() == "459\n", h
    assert "no pgrep" in r.stdout and "removed: 0" in r.stdout


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
