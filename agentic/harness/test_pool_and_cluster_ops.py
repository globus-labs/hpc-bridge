"""Hermetic tests for pool.py (cross-process claims) and cluster_ops.py (run-scoped commands).

    python -m pytest agentic/harness/test_pool_and_cluster_ops.py -q
"""
import shlex

from cluster_ops import (
    capture_logs_cmd,
    delete_endpoint_cmd,
    endpoint_uuid_cmd,
    scoped_cancel_cmd,
    uep_dirs_cleanup_cmd,
)
from pool import PoolClaims

POOL = [f"hpcbridge-test-{i:02d}" for i in range(4)]
EID = "da3df250-4013-4d69-942c-eef1568f860c"


def test_two_claimants_never_get_the_same_user(tmp_path):
    # two PoolClaims = two run_suite processes (flock is per open file description, so two
    # independent opens conflict exactly like two processes do)
    a, b = PoolClaims(tmp_path), PoolClaims(tmp_path)
    assert a.claim_any(POOL) == "hpcbridge-test-00"
    assert b.claim_any(POOL) == "hpcbridge-test-01"  # NOT -00: the 2026-08-19 collision, prevented
    assert a.claim_any(POOL) == "hpcbridge-test-02"
    assert sorted(b.busy(POOL)) == ["hpcbridge-test-00", "hpcbridge-test-01", "hpcbridge-test-02"]


def test_release_frees_the_user_for_another_process(tmp_path):
    a, b = PoolClaims(tmp_path), PoolClaims(tmp_path)
    assert a.claim_any(POOL) == "hpcbridge-test-00"
    assert b.try_claim("hpcbridge-test-00") is False
    a.release("hpcbridge-test-00")
    assert b.try_claim("hpcbridge-test-00") is True
    b.release_all()
    assert a.try_claim("hpcbridge-test-00") is True


def test_pool_exhausted_returns_none(tmp_path):
    a = PoolClaims(tmp_path)
    for _ in POOL:
        assert a.claim_any(POOL) is not None
    assert a.claim_any(POOL) is None
    assert PoolClaims(tmp_path).claim_any(POOL) is None  # another process sees the same exhaustion


def test_scoped_cancel_never_user_wide():
    cmd = scoped_cancel_cmd("slurm", [EID])
    assert "scancel -u" not in cmd and "-u \"$(whoami)\"" in cmd  # -u only to LIST, never to cancel
    assert f"uep.{EID}" in cmd and "scancel $ids" in cmd
    two = scoped_cancel_cmd("slurm", [EID, "11111111-2222-3333-4444-555555555555"])
    assert "uep.11111111-2222" in two and f"uep.{EID}" in two
    pbs = scoped_cancel_cmd("pbs", [EID])
    assert "qdel $ids" in pbs and "qselect -u" not in pbs and f"uep.{EID}" in pbs


def test_no_eid_means_cancel_nothing():
    cmd = scoped_cancel_cmd("slurm", [])
    assert "scancel" not in cmd and "qdel" not in cmd and "NOTHING" in cmd


def test_delete_targets_only_this_runs_endpoint():
    cmd = delete_endpoint_cmd("hpc-bridge-globus1-123-4")
    assert "hpc-bridge-globus1-123-4" in cmd
    assert 'grep "^hpc-bridge-"' not in cmd and "for ep in" not in cmd  # no enumerate-everything loop


def test_uuid_and_log_capture_commands_are_bounded_and_scoped():
    assert "endpoint.json" in endpoint_uuid_cmd("ep-x") and "ep-x" in endpoint_uuid_cmd("ep-x")
    cap = capture_logs_cmd("ep-x", [EID], tail_lines=500)
    flat = cap.replace('"', "").replace("'", "")  # bash concatenates the quoted pieces
    assert "tail -n 500" in cap and f"uep.{EID}" in flat and "submit_scripts" in cap
    assert "ep-x/endpoint.log" in flat


def test_uep_dir_cleanup_is_scoped_to_this_runs_uuids():
    cmd = uep_dirs_cleanup_cmd([EID])
    flat = cmd.replace('"', "")
    assert f".globus_compute/uep.{EID}.*" in flat and 'rm -rf "$d"' in cmd  # only that uuid's UEP dirs
    assert "rm -rf $HOME" not in flat and 'rm -rf "$HOME' not in cmd  # never a broad delete
    two = uep_dirs_cleanup_cmd([EID, "11111111-2222-3333-4444-555555555555"]).replace('"', "")
    assert "uep.11111111-2222" in two and f"uep.{EID}" in two
    none = uep_dirs_cleanup_cmd([])
    assert "rm" not in none and "removing nothing" in none


def test_harness_cancel_matches_the_products_marker_scoping():
    # The harness' scoped_cancel_cmd is a DELIBERATE copy of server._release_cmd (no import: the harness
    # must survive a broken product). Pin the one thing that matters: both cancel ONLY jobs carrying the
    # endpoint's `uep.<eid>` marker, never a whole user (code-quality review 2026-09-03).
    import re
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from cluster_ops import scoped_cancel_cmd

    from hpc_bridge.server import _release_cmd
    eid = "abcd1234-0000-4000-8000-000000000001"
    for scheduler in ("slurm", "pbs"):
        prod, harn = _release_cmd(scheduler, eid), scoped_cancel_cmd(scheduler, [eid])
        for cmd in (prod, harn):
            assert f"uep.{eid}" in cmd
            assert not re.search(r"(scancel|qdel)\s+-u\b", cmd)   # never user-wide
            assert ("scancel $ids" in cmd) if scheduler == "slurm" else ("qdel $ids" in cmd)


def test_run_scoped_cancel_never_matches_the_saturation_sleepers():
    # saturation's SETUP submits `hpcb-sat` sleepers; the run-scoped cancel keys on `uep.<eid>` markers only, so it
    # must not (and does not) reclaim them — the scenario declares its own CLEANUP for that (review 2026-09-05, 2.1)
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scenarios"))
    import saturation

    cmd = scoped_cancel_cmd("slurm", [EID])
    assert "hpcb-sat" not in cmd and "-n hpcb-sat" not in cmd  # (the `[ -n "$ids" ]` test is a shell -n, fine)
    assert any("hpcb-sat" in c and "scancel" in c for c in saturation.CLEANUP)
    assert saturation.SERIAL is True and saturation.NEEDS_COMPUTE_NODE == 4   # every globus1 compute node (spark1-4)


def test_token_store_cleanup_removes_only_the_pool_users_seeded_store():
    from cluster_ops import token_store_cleanup_cmd
    cmd = token_store_cleanup_cmd()
    assert '"$HOME/.globus_compute/storage.db"' in cmd and "rm -f" in cmd and "token store removed" in cmd
    assert "uep." not in cmd and "rm -rf" not in cmd   # the store only — never a directory sweep


def test_every_cell_starts_without_a_token_store(monkeypatch, harness_run):
    # a cell that died before its teardown leaves the pool user's store; the plugin cannot prove it its own and (since
    # 0.1.18) refuses to replace it — so the harness clears it BEFORE the agent too, not only after (live 2026-10-06)
    from cluster_ops import token_store_cleanup_cmd
    seen = []
    monkeypatch.setattr(harness_run, "_ssh_run", lambda cmd, timeout=60, host=None: (seen.append(cmd), (0, "token store removed"))[1])
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-03")
    harness_run._preclean_token_store(object())
    assert seen == [token_store_cleanup_cmd()]


def test_the_preclean_spares_an_untrusted_host_key_and_a_real_user(monkeypatch, harness_run):
    # unknown_host_key needs the jail's known_hosts EMPTY in phase 1 (the harness ssh's accept-new would fill it), and
    # a recipe run as a real user (aurora_pbs_bringup) must never lose that user's own Globus login
    seen = []
    monkeypatch.setattr(harness_run, "_ssh_run", lambda cmd, timeout=60, host=None: (seen.append(cmd), (0, ""))[1])
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-00")
    harness_run._preclean_token_store(type("S", (), {"TRUST_HOST_KEY": False})())
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "jdoe")
    harness_run._preclean_token_store(object())
    assert seen == []
    assert harness_run._is_pool_user("hpcbridge-test") and harness_run._is_pool_user("hpcbridge-test-12")
    assert not harness_run._is_pool_user("hpcbridge-tester") and not harness_run._is_pool_user("jdoe")


# ---- after the harness' own teardown: no manager of this run's endpoint may be left (2026-10-11: 14 were) ----------

_NAME = "hpc-bridge-fake-1791565281-68624-123"


def _leftovers(listing_lines: list[str], name: str = _NAME) -> list[str]:
    """Run leftover_manager_cmd in a real bash with `ps` replaced by a fixed listing (no sleeping between tries)."""
    import shlex
    import subprocess

    from cluster_ops import leftover_manager_cmd
    listing = "printf '%s\\n' " + " ".join(shlex.quote(ln) for ln in listing_lines)
    cmd = leftover_manager_cmd(name, tries=2, pause_s=0, listing=listing)
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=30, check=False)
    assert r.returncode == 0, r.stderr
    return [ln for ln in r.stdout.splitlines() if ln.strip()]


def test_leftover_manager_cmd_finds_this_runs_manager_in_either_form():
    title = f"4242 Globus Compute Endpoint *({EID}, {_NAME}) [idle]"
    argv = f"4243 /home/u/hpc-bridge/gce-venv/bin/python /home/u/hpc-bridge/gce-venv/bin/globus-compute-endpoint start {_NAME} --detach"
    assert _leftovers([title, argv]) == [title, argv]


def test_leftover_manager_cmd_finds_managers_started_from_a_keyed_venv_or_the_planted_wrapper():
    # 0.1.24: the endpoint runs from a keyed venv (uv's relocatable trampoline execs `<venv>/bin/python <entry> …`), and
    # venv_arch_mismatch moves the real entry point to bin/.gce-real behind a wrapper that execs it on a login node
    k = "/home/u/hpc-bridge/gce-venv-endpoint-4.16.0-py3.12"
    keyed = f"4250 {k}/bin/python {k}/bin/globus-compute-endpoint start {_NAME} --detach"
    planted = f"4251 {k}/bin/python {k}/bin/.gce-real start {_NAME} --detach"                 # before its title is set
    titled = f"4252 Globus Compute Endpoint *({EID}, {_NAME}) [{k}/bin/python {k}/bin/.gce-real start {_NAME}]"
    assert _leftovers([keyed, planted, titled]) == [keyed, planted, titled]
    restore = f"4253 mv -f {k}/bin/.gce-real {k}/bin/globus-compute-endpoint"                  # names no endpoint
    wrapper_sh = f"4254 /bin/sh {k}/bin/.gce-real start {_NAME}"                                # a shell: never
    assert _leftovers([restore, wrapper_sh]) == []


def test_leftover_manager_cmd_ignores_other_runs_and_unrelated_processes():
    other = f"4244 Globus Compute Endpoint *({EID}, {_NAME}4) [idle]"            # a longer name: another run's
    prefix = f"4245 globus-compute-endpoint start {_NAME[:-1]} --detach"          # a shorter one
    mep = f"4246 Globus Compute Endpoint *({EID}, hpcb-mep-strict) [running]"     # the fake MEP's managers
    tail = f"4247 tail -f /home/u/.globus_compute/{_NAME}/endpoint.log"           # names it, but is not an endpoint
    assert _leftovers([other, prefix, mep, tail]) == []
    assert _leftovers([]) == []


def test_leftover_manager_cmd_never_matches_its_own_shell():
    import subprocess

    from cluster_ops import leftover_manager_cmd
    # the real `ps` of this machine: the bash running the check carries the patterns in its own argv
    cmd = leftover_manager_cmd(_NAME, tries=1, pause_s=0, listing="ps -ww -o pid=,args=")
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=30, check=False)
    assert r.returncode == 0 and r.stdout.strip() == "", r.stdout


def test_leftover_manager_cmd_refuses_a_name_it_cannot_quote_safely():
    import pytest
    from cluster_ops import leftover_manager_cmd
    with pytest.raises(ValueError):
        leftover_manager_cmd("bad name; rm -rf ~")


# ---- the stop goes where the manager RUNS; nothing waits on a prompt (2026-10-13 root cause of the leaks) -----------

def _fake_gce(tmp_path):
    """A stand-in `globus-compute-endpoint`: records each call, and READS STDIN (as `delete` does at its authorization-
    code prompt) — so a command that left stdin open would hang on it."""
    gce = tmp_path / "gce"
    gce.write_text('#!/bin/bash\necho "$@" >> "$(dirname "$0")/calls"\nread -r code\necho "fake gce: $1 done (code=${code:-none})"\n')
    gce.chmod(0o755)
    return str(gce)


def _bash(cmd: str, env: dict | None = None, timeout: float = 20) -> str:
    """Run `cmd` with stdin an OPEN pipe that is never written or closed (like an ssh session's): only the command's
    own `</dev/null` lets a prompt-reader inside it return. Raises TimeoutExpired if something waits on stdin."""
    import os
    import subprocess
    import tempfile
    with tempfile.TemporaryFile("w+") as out:
        p = subprocess.Popen(["bash", "-c", cmd], stdin=subprocess.PIPE, stdout=out, stderr=subprocess.STDOUT,
                             text=True, env={**os.environ, **(env or {})})
        try:
            p.wait(timeout=timeout)
        finally:
            p.kill()
            p.stdin.close()
            p.wait()
        out.seek(0)
        return out.read()


def test_stop_runs_gce_only_on_the_node_running_the_manager(tmp_path):
    from cluster_ops import stop_here_cmd
    gce = _fake_gce(tmp_path)
    here = f"printf '%s\\n' {shlex.quote(f'4242 Globus Compute Endpoint *({EID}, {_NAME}) [idle]')}"
    away = "printf '%s\\n' '1 /sbin/init' '7 sshd: hpcbridge-test-03@notty'"
    on_manager_node = _bash(stop_here_cmd(_NAME, listing=here, gce=gce))
    assert on_manager_node.startswith("HPCB_STOP ") and " rc=0 fake gce: stop done" in on_manager_node
    on_other_node = _bash(stop_here_cmd(_NAME, listing=away, gce=gce))
    assert on_other_node.strip().endswith(" none")       # the shared daemon.pid is never acted on from there
    assert (tmp_path / "calls").read_text().splitlines() == [f"stop {_NAME}"]   # one stop, on the manager's node


def test_delete_cannot_wait_on_a_prompt_and_says_what_happened(tmp_path):
    from cluster_ops import delete_registration_cmd
    gce = _fake_gce(tmp_path)
    home = tmp_path / "home"
    (home / ".globus_compute" / _NAME).mkdir(parents=True)
    out = _bash(delete_registration_cmd(_NAME, gce=gce), env={"HOME": str(home)})   # returns: stdin is /dev/null
    assert out.startswith("HPCB_DELETE rc=0 ") and "delete done (code=none)" in out
    (home / ".globus_compute" / _NAME).rmdir()
    assert _bash(delete_registration_cmd(_NAME, gce=gce), env={"HOME": str(home)}).strip() == "HPCB_DELETE none"
    # the guard is real: the same delete with stdin left open waits on the prompt
    import subprocess

    import pytest
    (home / ".globus_compute" / _NAME).mkdir()
    with pytest.raises(subprocess.TimeoutExpired):
        _bash(delete_registration_cmd(_NAME, gce=gce).replace("</dev/null", ""), env={"HOME": str(home)}, timeout=2)


# ---- the REAL command text, a REAL shell and the REAL `ps`: the guard must not see itself (live check 2026-10-13) --

def _spawn(argv: list[str], executable: str | None = None):
    import subprocess
    return subprocess.Popen(argv, executable=executable, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def _manager(name: str):
    """A process whose args are a manager's TITLE (`Globus Compute Endpoint *(<uuid>, <name>) [idle]`), as the daemon
    sets it — argv[0] given, `sleep` underneath."""
    import shutil
    return _spawn([f"Globus Compute Endpoint *({EID}, {name}) [idle]", "60"], executable=shutil.which("sleep"))


def _uep_child(tmp_path, eid: str):
    """A user endpoint the manager forked, as an entry-point command line: `<python> …/globus-compute-endpoint
    _start-user-endpoint uep.<eid>.<hash>` (the user endpoint's name carries the MANAGER's uuid, not its name)."""
    import sys
    gce = tmp_path / "uepbin" / "globus-compute-endpoint"
    gce.parent.mkdir(exist_ok=True)
    gce.write_text("import time\ntime.sleep(60)\n")
    return _spawn([sys.executable, str(gce), "_start-user-endpoint", f"uep.{eid}.f6187c3b-7cab-ce94"])


def _real_gce_home(tmp_path):
    """HOME with a stand-in where the guard's real text finds a gce (cluster_ops.GCE: the first hpc-bridge venv whose
    `version` runs) — the finder and `stop <name>` in its own argv, as on the cluster — recording every call."""
    home = tmp_path / "home"
    gce = home / "hpc-bridge" / "gce-venv" / "bin" / "globus-compute-endpoint"
    gce.parent.mkdir(parents=True)
    gce.write_text('#!/bin/bash\necho "$@" >> "$HOME/gce-calls"\necho "Endpoint <$2> is now stopped"\n')
    gce.chmod(0o755)
    return home


def _wait_listed(proc, timeout: float = 5.0) -> None:
    import subprocess
    import time
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if str(proc.pid) in subprocess.run(["ps", "-ww", "-o", "pid=", "-p", str(proc.pid)], capture_output=True,
                                           text=True, check=False).stdout:
            return
        time.sleep(0.05)


def test_the_real_stop_guard_ignores_its_own_shell_and_finds_a_real_manager(tmp_path):
    from cluster_ops import stop_here_cmd
    name = "hpc-bridge-fake-1791900000-4242-77"
    home = _real_gce_home(tmp_path)
    cmd = stop_here_cmd(name)                     # the real text: the gce finder, default `ps` listing
    assert "globus-compute-endpoint" in cmd and '" stop hpc-bridge-fake-1791900000-4242-77' in cmd  # in its own argv
    # a node WITHOUT the manager — the guard's own `bash -c` (and its `$(…)` subshell) carry the name and the gce
    # path in their args; they must not count
    out = _bash(cmd, env={"HOME": str(home)})
    assert out.strip().endswith(" none"), out
    assert not (home / "gce-calls").exists()
    # a node WITH it: the manager's title is in `ps`
    mgr = _manager(name)
    try:
        _wait_listed(mgr)
        out = _bash(cmd, env={"HOME": str(home)})
        assert " rc=0 Endpoint <" in out and out.startswith("HPCB_STOP "), out
        assert (home / "gce-calls").read_text().split() == ["version", "stop", name]   # the finder's check, the stop
    finally:
        mgr.kill()
        mgr.wait()


def test_the_leak_check_finds_a_real_manager_title_and_a_uep_child_but_no_lookalike(tmp_path):
    from cluster_ops import leftover_manager_cmd
    name = "hpc-bridge-fake-1791900000-4242-78"
    eid = "269d2e35-5301-41ab-ba6a-c1978b1a586d"
    cmd = leftover_manager_cmd(name, [eid], tries=1, pause_s=0)
    lookalikes = [   # each names the endpoint in its args, none IS one
        _spawn(["bash", "-c", f"sleep 60; globus-compute-endpoint stop {name}"]),
        _spawn(["sh", "-c", f"sleep 60 # Globus Compute Endpoint *({eid}, {name})"]),
        _manager(name + "9"),                              # another run's manager (a longer name)
    ]
    try:
        for p in lookalikes:
            _wait_listed(p)
        assert _bash(cmd).strip() == ""                    # the check's own shell, the lookalikes: nothing
        mgr, uep = _manager(name), _uep_child(tmp_path, eid)
        try:
            _wait_listed(mgr)
            _wait_listed(uep)
            found = [ln for ln in _bash(cmd).splitlines() if ln.strip()]
            assert len(found) == 2, found
            assert any(ln.split(None, 1)[1].startswith("Globus Compute Endpoint *(") for ln in found)
            assert any("_start-user-endpoint uep." + eid in ln for ln in found)
        finally:
            for p in (mgr, uep):
                p.kill()
                p.wait()
    finally:
        for p in lookalikes:
            p.kill()
            p.wait()


def test_a_failing_ps_is_unverifiable_never_clean_and_the_stop_still_runs(tmp_path):
    # BusyBox `ps` has no `-o`: it fails, and an empty listing must not read as "no manager left" / "not on this node"
    from cluster_ops import leftover_manager_cmd, stop_here_cmd
    failing = "ps_that_fails() { echo 'ps: unrecognized option: o' >&2; return 1; }; ps_that_fails"
    assert _bash(leftover_manager_cmd(_NAME, tries=1, pause_s=0, listing=failing)).startswith("HPCB_UNVERIFIABLE: ps failed")
    gce = _fake_gce(tmp_path)
    out = _bash(stop_here_cmd(_NAME, listing=failing, gce=gce))
    assert " rc=0 fake gce: stop done" in out              # unknown ⇒ the stop runs, as before the guard existed


def test_pool_claims_are_host_wide_and_honour_the_old_per_checkout_dir(tmp_path, monkeypatch):
    import importlib

    import pool
    monkeypatch.delenv("HPCB_POOL_CLAIMS_DIR", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    fresh = importlib.reload(pool)
    try:
        assert tmp_path / "state" / "hpc-bridge" / "harness-claims" == fresh.DEFAULT_CLAIMS_DIR   # not in a checkout
        assert "agentic" not in fresh.DEFAULT_CLAIMS_DIR.parts
    finally:
        monkeypatch.undo()
        importlib.reload(pool)
    # an older suite in this checkout claims in the OLD directory: a new claimant must see it, and hold it too
    new_dir, old_dir = tmp_path / "new", tmp_path / "old"
    old_dir.mkdir()
    older = pool.PoolClaims(old_dir)                          # older code: the per-checkout dir is its only dir
    assert older.try_claim("hpcbridge-test-00")
    newer = pool.PoolClaims(new_dir, legacy_dir=old_dir)
    assert newer.claim_any(POOL) == "hpcbridge-test-01"       # -00 is held in the old dir
    assert "hpcbridge-test-00" in newer.busy(POOL)
    assert older.try_claim("hpcbridge-test-01") is False      # newer took the old lock as well
    newer.release_all()
    assert older.try_claim("hpcbridge-test-01") is True
    assert pool.PoolClaims(tmp_path / "x").legacy is None     # an explicit dir is a deliberate choice: no legacy


# ---- the endpoint's venv: found like the plugin's teardown finds it ---------------------------------------------------

def _gce_stub(venv, version: str, works: bool = True) -> None:
    (venv / "bin").mkdir(parents=True)
    ep = venv / "bin" / "globus-compute-endpoint"
    ep.write_text(f'#!/bin/sh\n[ "$1" = version ] && {{ echo "Globus Compute endpoint version: {version}"; exit '
                  f'{0 if works else 1}; }}\necho "$@" >> "{venv}/calls"\n')
    ep.chmod(0o755)


def test_the_harness_finds_a_working_endpoint_venv(tmp_path):
    import subprocess

    from cluster_ops import GCE, GCE_FIND, delete_registration_cmd, stop_here_cmd

    home = tmp_path / "home"
    hb = home / "hpc-bridge"

    def found() -> str:
        r = subprocess.run(["bash", "-c", GCE_FIND], env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                           capture_output=True, text=True, check=False)
        return r.stdout.strip()

    assert found() == ""  # nothing there: an empty path, never an error
    _gce_stub(hb / "gce-venv", "4.13.0")  # plugins up to 0.1.23
    assert found() == f"{hb}/gce-venv/bin/globus-compute-endpoint"
    _gce_stub(hb / "gce-venv-endpoint-4.18.0-py3.11", "4.18.0", works=False)  # keyed, broken
    _gce_stub(hb / "gce-venv-endpoint-4.19.0-py3.11.broken.20261013-1", "4.19.0")  # moved aside: skipped
    _gce_stub(hb / "gce-venv-endpoint-4.20.0-py3.11.build.abc", "4.20.0")  # a build in progress: skipped
    _gce_stub(hb / "gce-venv-endpoint-4.9.0-py3.11", "4.9.0")  # older than 4.16 (`sort -V`, not by text)
    _gce_stub(hb / "gce-venv-endpoint-4.16.0-py3.11", "4.16.0")
    assert found() == f"{hb}/gce-venv-endpoint-4.16.0-py3.11/bin/globus-compute-endpoint"
    # the stop (its manager runs here) and the delete (its directory exists) the teardown runs go through that one
    (home / ".globus_compute" / "hpc-bridge-fake-1").mkdir(parents=True)
    here = f"printf '%s\\n' {shlex.quote(f'4242 Globus Compute Endpoint *({EID}, hpc-bridge-fake-1) [idle]')}"
    env = {"HOME": str(home), "PATH": "/usr/bin:/bin"}
    out = _bash(f"{stop_here_cmd('hpc-bridge-fake-1', listing=here)}; {delete_registration_cmd('hpc-bridge-fake-1')}",
                env=env)
    assert " rc=0 " in out and "HPCB_DELETE rc=0" in out, out
    assert (hb / "gce-venv-endpoint-4.16.0-py3.11" / "calls").read_text().splitlines() == [
        "stop hpc-bridge-fake-1", "delete hpc-bridge-fake-1 --yes"]
    assert GCE.startswith('"$(') and GCE_FIND in GCE


def test_the_sweep_script_uses_the_same_finder():
    import re
    from pathlib import Path

    from cluster_ops import GCE_FIND

    text = (Path(__file__).resolve().parents[1] / "sweep_pool_user.sh").read_text()
    m = re.search(r"^GCE='\"\$\((.*)\)\"'$", text, re.M)
    assert m and m.group(1) == GCE_FIND


def test_a_planted_entry_point_is_restored_before_every_cell(tmp_path, monkeypatch, harness_run):
    # venv_arch_mismatch moves the real entry point to bin/.gce-real behind a failing wrapper; a cell killed before its
    # CLEANUP must not leave it for the next cell on that pool user
    import subprocess

    from cluster_ops import restore_planted_entry_points_cmd

    home = tmp_path / "home"
    k = home / "hpc-bridge" / "gce-venv-endpoint-4.18.0-py3.11"
    _gce_stub(k, "4.18.0")
    (k / "bin" / "globus-compute-endpoint").rename(k / "bin" / ".gce-real")
    (k / "bin" / "globus-compute-endpoint").write_text("#!/bin/sh\nexit 2\n")
    (k / ".hpcb-arch").write_text("x86_64\n")
    r = subprocess.run(["bash", "-c", restore_planted_entry_points_cmd()], env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                       capture_output=True, text=True, check=False)
    assert r.returncode == 0 and "restored: 1" in r.stdout
    assert not (k / "bin" / ".gce-real").exists() and "4.18.0" in (k / "bin" / "globus-compute-endpoint").read_text()
    seen = []
    monkeypatch.setattr(harness_run, "_ssh_run", lambda cmd, timeout=60, host=None: (seen.append(cmd), (0, "restored: 0"))[1])
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-03")
    harness_run._restore_planted_venvs()
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "jdoe")  # never a real user's venvs
    harness_run._restore_planted_venvs()
    assert seen == [restore_planted_entry_points_cmd()]


def test_the_sweep_script_restores_planted_entry_points_too():
    from pathlib import Path

    from cluster_ops import restore_planted_entry_points_cmd

    text = (Path(__file__).resolve().parents[1] / "sweep_pool_user.sh").read_text()
    assert f"RESTORE='{restore_planted_entry_points_cmd()}'" in text and 'REMOTE="$RESTORE; ' in text


# ---- world checks over the run's pilot jobs (cluster_ops.parsl_jobs_since_cmd), against a fake `sacct` ------------------

def _jobs_world(tmp_path, rows: list[str] | None, *, t0: str | None = "2026-10-13T10:00:00") -> tuple[int, str]:
    """Run the postcheck command in bash with `sacct`/`whoami` stubbed and HOME holding the t0 stamp."""
    import subprocess
    import sys

    from cluster_ops import parsl_jobs_since_cmd
    bin_ = tmp_path / "bin"
    bin_.mkdir(exist_ok=True)
    (tmp_path / "sacct.out").write_text("".join(r + "\n" for r in rows or []))
    (bin_ / "sacct").write_text(f'#!/bin/sh\necho "$@" > {tmp_path}/sacct.args\ncat {tmp_path}/sacct.out\n')
    (bin_ / "whoami").write_text("#!/bin/sh\necho hpcbridge-test-03\n")
    for f in ("sacct", "whoami"):
        (bin_ / f).chmod(0o755)
    if t0 is not None:
        (tmp_path / ".t0").write_text(t0 + "\n")
    py = bin_ / "python3"   # the login node's python3: this interpreter
    py.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
    py.chmod(0o755)
    env = {"HOME": str(tmp_path), "PATH": f"{bin_}:/usr/bin:/bin"}
    r = subprocess.run(["bash", "-c", parsl_jobs_since_cmd("~/.t0")], capture_output=True, text=True, env=env, timeout=30)
    return r.returncode, r.stdout + r.stderr


def test_the_pilot_jobs_check_counts_cancels_and_overlaps(tmp_path):
    old = "101|parsl.htex.block-0.1|CANCELLED by 1003|2026-10-13T10:01:00|2026-10-13T10:01:02|2026-10-13T10:03:00"
    new = "102|parsl.htex.block-1.2|COMPLETED|2026-10-13T10:04:10|2026-10-13T10:04:12|2026-10-13T10:06:00"
    other = "100|hpcb-handjob|COMPLETED|2026-10-13T10:00:30|2026-10-13T10:00:31|2026-10-13T10:00:40"
    earlier = "99|parsl.htex.block-0.0|COMPLETED|2026-10-13T09:00:00|2026-10-13T09:00:01|2026-10-13T09:30:00"
    rc, out = _jobs_world(tmp_path, [earlier, other, new, old])
    assert rc == 0 and "parsl_jobs=2; overlap=0; old=CANCELLED; jobs=101:CANCELLED,102:COMPLETED" in out, out
    args = (tmp_path / "sacct.args").read_text()
    assert "-S 2026-10-13T10:00:00" in args and "-u hpcbridge-test-03" in args   # this user, since the stamp
    # a replacement that started while the old block was still running is an overlap
    racing = "102|parsl.htex.block-1.2|RUNNING|2026-10-13T10:02:00|2026-10-13T10:02:05|Unknown"
    rc, out = _jobs_world(tmp_path, [old, racing])
    assert "overlap=1;" in out and "parsl_jobs=2;" in out, out
    # a block still running when the check runs, and a later one that never started, never overlap by themselves
    pending = "103|parsl.htex.block-2.3|PENDING|2026-10-13T10:05:00|Unknown|Unknown"
    rc, out = _jobs_world(tmp_path, ["101|parsl.b|RUNNING|2026-10-13T10:01:00|2026-10-13T10:01:02|Unknown", pending])
    assert "parsl_jobs=2; overlap=0; old=RUNNING;" in out, out
    rc, out = _jobs_world(tmp_path, [old])
    assert "parsl_jobs=1;" in out and "parsl_jobs=11" not in out
    rc, out = _jobs_world(tmp_path, [])
    assert rc == 0 and "parsl_jobs=0; overlap=0; old=none; jobs=none" in out
    # an empty (or failed) sacct never passes a check that wants the run's pilots (review P4)
    assert "parsl_jobs=2; overlap=0;" not in out and "parsl_jobs=1;" not in out


def test_the_pilot_jobs_check_fails_without_its_stamp(tmp_path):
    rc, out = _jobs_world(tmp_path, ["101|parsl.b|COMPLETED|2026-10-13T10:01:00|2026-10-13T10:01:02|2026-10-13T10:02:00"], t0=None)
    assert rc == 1 and "no t0 stamp" in out and "parsl_jobs" not in out   # never a vacuous pass


def test_the_t0_stamp_is_in_sacct_format():
    from cluster_ops import stamp_t0_cmd
    assert stamp_t0_cmd("~/.t0") == "date +%Y-%m-%dT%H:%M:%S > ~/.t0"


def test_a_stop_line_reads_as_stopped_only_where_gce_stop_succeeded():
    from cluster_ops import stop_outcome
    assert stop_outcome("HPCB_STOP login01 none\n") == ("none", "")
    assert stop_outcome("x\nHPCB_STOP login02 rc=0 Endpoint <e> is now stopped\n") == ("rc=0", "Endpoint <e> is now stopped")
    assert stop_outcome("HPCB_STOP login02 rc=1 no daemon.pid\n")[0] == "rc=1"
    assert stop_outcome("ssh: connect to host login01 port 22: Connection refused") == (
        "missing", "ssh: connect to host login01 port 22: Connection refused")
