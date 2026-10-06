"""Hermetic tests for pool.py (cross-process claims) and cluster_ops.py (run-scoped commands).

    python -m pytest agentic/harness/test_pool_and_cluster_ops.py -q
"""
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
    assert saturation.SERIAL is True and saturation.NEEDS_COMPUTE_NODE == 3


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
    harness_run._preclean_token_store()
    assert seen == [token_store_cleanup_cmd()]
