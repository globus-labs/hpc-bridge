"""Host-side knobs: scenario_knobs.py output and run_suite's idle-node gate (hermetic)."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
KNOBS = HERE / "scenario_knobs.py"


def _knobs(scenario: str) -> tuple[int, dict[str, str]]:
    r = subprocess.run([sys.executable, str(KNOBS), scenario], capture_output=True, text=True, timeout=60, check=False)
    return r.returncode, dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)


def test_compute_scenarios_declare_needs_node():
    for s in ("happy_path", "gated_provision"):
        rc, k = _knobs(s)
        assert rc == 0 and k.get("HPCB_KNOB_NEEDS_NODE") == "1", (s, k)
        assert "HPCB_KNOB_SERIAL" not in k


def test_ssh_failure_scenario_is_serial_not_node_gated():
    rc, k = _knobs("no_ssh_access")
    assert rc == 0 and k["HPCB_KNOB_SERIAL"] == "1" and k["HPCB_KNOB_COOLDOWN_S"] == "660"
    assert "HPCB_KNOB_NEEDS_NODE" not in k


def test_unknown_scenario_exits_nonzero():
    rc, _ = _knobs("no_such_scenario")
    assert rc == 2


def _run_suite():
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- node need is DERIVED, and the gate does in-flight accounting (review 2026-09-05, 2.2) ----------------------

def test_node_need_is_derived_for_every_block_bringing_scenario():
    # explicit True -> 1; derived from a `compute_ran` gate -> 1; explicit int -> that many; MEP pair name a warm user
    for s, need in (("happy_path", "1"), ("gated_provision", "1"), ("spend_gate_enforced", "1"),
                    ("mep_compute_only", "1"), ("stranger_mep_walk", "1"), ("long_task_via_handle", "1"),
                    ("idle_release_kill", "1"), ("long_job_30m", "1"), ("saturation", "4")):
        rc, k = _knobs(s)
        assert rc == 0 and k.get("HPCB_KNOB_NEEDS_NODE") == need, (s, k)
    for s in ("mep_compute_only", "stranger_mep_walk"):
        assert _knobs(s)[1].get("HPCB_KNOB_WARM_BLOCK_USER") == "glabs-gc", s
    for s in ("zero_config_list", "needs_login_paste", "registry_over_cache", "spend_refusal", "session_persistence"):
        assert "HPCB_KNOB_NEEDS_NODE" not in _knobs(s)[1], s  # no block, no gate
    rc, k = _knobs("saturation")
    assert k.get("HPCB_KNOB_SERIAL") == "1"  # holds every node: never alongside another cell


def _gate(mod, idle_seq, monkeypatch, warm_seq=None, *, claims_dir, clock=None):
    """A NodeGate over a fake probe and clock, its cross-process launch record in `claims_dir` (never the repo's)."""
    clock = clock if clock is not None else {"t": 1000.0}
    seq = iter(idle_seq)
    monkeypatch.setattr(mod, "_idle_nodes", lambda: next(seq))
    wseq = iter(warm_seq or [])
    monkeypatch.setattr(mod, "_warm_block_running", lambda user: next(wseq, False))
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["t"])

    async def fake_sleep(s):
        clock["t"] += s

    monkeypatch.setattr(mod.asyncio, "sleep", fake_sleep)  # restored after the test (a bare assignment leaked into others)
    launches = mod.NodeLaunches("fake", claims_dir, window_s=mod.NODE_CLAIM_S, clock=lambda: clock["t"])
    return mod.NodeGate(launches=launches), clock


def test_gate_counts_in_flight_launches_against_the_idle_nodes(monkeypatch, tmp_path):
    # two idle nodes, three cells arriving within the claim window: the third must WAIT (it used to launch —
    # every cell read "2 idle" and one starved PENDING for its whole run, the 2026-09-03 starvation)
    mod = _run_suite()
    gate, clock = _gate(mod, [2, 2, 2, 2, 3], monkeypatch, claims_dir=tmp_path)

    async def go():
        halt = asyncio.Event()
        a = await gate.admit("a", 1, None, halt, 3600)
        b = await gate.admit("b", 1, None, halt, 3600)
        assert (a, b) == (2, 2)
        # third cell: 2 idle - 2 recent launches = 0 free -> waits (fake sleep advances the clock) until the
        # window expires or the probe shows more; the 5th probe says 3 idle -> admitted
        c = await gate.admit("c", 1, None, halt, 3600)
        assert c == 3 and clock["t"] > 1000.0
    asyncio.run(go())


def test_gate_need_of_three_admits_only_when_all_are_free(monkeypatch, tmp_path):
    mod = _run_suite()
    gate, _clock = _gate(mod, [2, 2, 3], monkeypatch, claims_dir=tmp_path)

    async def go():
        assert await gate.admit("sat", 3, None, asyncio.Event(), 3600) == 3
    asyncio.run(go())


def test_gate_warm_block_satisfies_a_mep_cell_without_an_idle_node(monkeypatch, tmp_path):
    mod = _run_suite()
    gate, _clock = _gate(mod, [0], monkeypatch, warm_seq=[True], claims_dir=tmp_path)

    async def go():
        assert await gate.admit("mep", 1, "glabs-gc", asyncio.Event(), 3600) == -1  # unguarded: reusing the warm block
    asyncio.run(go())


def test_gate_gives_up_after_max_wait_and_launches_unguarded_on_probe_failure(monkeypatch, tmp_path):
    mod = _run_suite()
    gate, _clock = _gate(mod, [0] * 10, monkeypatch, claims_dir=tmp_path)

    async def go():
        assert await gate.admit("x", 1, None, asyncio.Event(), 120) is None
    asyncio.run(go())
    gate2, _c2 = _gate(mod, [None], monkeypatch, claims_dir=tmp_path / "b")

    async def go2():
        assert await gate2.admit("y", 1, None, asyncio.Event(), 3600) == -1
    asyncio.run(go2())


def test_gate_counts_another_suites_launches(monkeypatch, tmp_path):
    # three concurrent run_suite invocations (one per operator) each saw "3 idle" and oversubscribed the fake cluster
    # (itinerary review 2026-10-13): two gates over ONE claims dir are two suites on one host — the second must count
    # the first's launches
    mod = _run_suite()
    clock = {"t": 1000.0}
    a, _ = _gate(mod, [2, 2], monkeypatch, claims_dir=tmp_path, clock=clock)
    b, _ = _gate(mod, [2, 2, 2, 3], monkeypatch, claims_dir=tmp_path, clock=clock)
    seq = iter([2, 2, 2, 2, 3])
    monkeypatch.setattr(mod, "_idle_nodes", lambda: next(seq))   # one cluster, seen by both suites

    async def go():
        halt = asyncio.Event()
        assert await a.admit("suite-a/1", 1, None, halt, 3600) == 2
        assert await a.admit("suite-a/2", 1, None, halt, 3600) == 2
        t_before = clock["t"]
        assert await b.admit("suite-b/1", 1, None, halt, 3600) == 3   # waited: 2 idle - 2 launched elsewhere = 0
        assert clock["t"] > t_before
    asyncio.run(go())
    rec = json.loads((tmp_path / "nodes-fake.json").read_text())
    assert [e["label"] for e in rec] == ["suite-a/1", "suite-a/2", "suite-b/1"]


def test_node_launches_expire_and_are_seen_by_another_process(tmp_path):
    # a REAL second process records a launch; this one counts it, and the entry stops counting after the window (a
    # crashed suite's launches never pin the gate shut)
    import os

    import pool
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import pool; "
            "L = pool.NodeLaunches('fake', sys.argv[2], window_s=300, clock=lambda: 1000.0)\n"
            "with L.locked():\n    L.record('other-suite')")
    r = subprocess.run([sys.executable, "-c", code, str(HERE), str(tmp_path)], capture_output=True, text=True,
                       timeout=60, check=False)
    assert r.returncode == 0, r.stderr
    now = {"t": 1100.0}
    mine = pool.NodeLaunches("fake", tmp_path, window_s=300, clock=lambda: now["t"])
    with mine.locked():
        assert mine.recent() == 1
        mine.record("this-suite")
        assert mine.recent() == 2
    now["t"] = 1350.0   # the other suite's launch is 350 s old: expired; ours (250 s) still counts
    with mine.locked():
        assert mine.recent() == 1
    assert pool.NodeLaunches("globus1", tmp_path).path.name == "nodes-globus1.json"   # one record per target
    first = json.loads(mine.path.read_text())[0]
    assert first["label"] == "other-suite" and first["pid"] != os.getpid()   # written by the other process


def test_node_launch_lock_is_exclusive_across_processes(tmp_path):
    # the probe + decision + claim must be ONE step across suites: while ANOTHER PROCESS holds the lock, this one
    # cannot take it; once that process exits (the kernel drops its flock), it can
    import fcntl
    import os

    import pool
    holder = subprocess.Popen(
        [sys.executable, "-c", "import sys, time; sys.path.insert(0, sys.argv[1]); import pool; "
                               "L = pool.NodeLaunches('fake', sys.argv[2])\n"
                               "with L.locked():\n    print('held', flush=True); time.sleep(30)",
         str(HERE), str(tmp_path)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        lock_path = pool.NodeLaunches("fake", tmp_path)._lock_path
        fd = os.open(lock_path, os.O_RDWR)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                got_while_held = True
            except OSError:
                got_while_held = False
        finally:
            os.close(fd)
    finally:
        holder.kill()
        holder.wait()
    assert got_while_held is False
    with pool.NodeLaunches("fake", tmp_path).locked():   # the holder is gone: free again (would block forever if not)
        pass


def test_idle_probe_counts_only_exactly_idle_nodes(monkeypatch):
    mod = _run_suite()
    # the live 2026-09-05 cluster: globus1 allocated, globus2 DRAINED ("Duplicate jobid"), globus3 mixed -> ZERO
    # usable nodes; `sinfo -t idle -o %D` said 1 (drain = idle+drained) and a block cell launched onto nothing
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: "alloc\ndrain\nmix\n")
    assert mod._idle_nodes() == 0
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: "idle\nidle*\ndrng\nidle\n")  # idle* = not responding
    assert mod._idle_nodes() == 2
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: "sinfo: error: Invalid partition name")
    assert mod._idle_nodes() is None  # unknown, never 0
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: "")
    assert mod._idle_nodes() == 0
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: None)
    assert mod._idle_nodes() is None


def test_only_the_unknown_host_key_scenario_starts_with_a_cold_known_hosts():
    # every other SSH scenario models a RETURNING user whose own ssh already trusts the cluster (run.py pre-trusts
    # the key through the harness channel); the first block-tier run after #75 failed 5/5 on UNKNOWN HOST KEY
    cold = {p.stem for p in (HERE.parent / "scenarios").glob("*.py")
            if not p.stem.startswith("_") and _knobs(p.stem)[1].get("HPCB_KNOB_COLD_HOST_KEY") == "1"}
    assert cold == {"unknown_host_key"}


def test_cell_env_strips_stray_knobs_but_keeps_credentials(monkeypatch):
    mod = _run_suite()
    monkeypatch.setenv("HPCB_NO_SKILL", "1")        # a persisted ablation must not leak into a baseline cell
    monkeypatch.setenv("HPCB_EFFORT", "max")
    monkeypatch.setenv("HPCB_TEST_GLOBUS_DB", "/x/storage.db")
    monkeypatch.setenv("HPCB_NODE_PARTITION", "main")
    env = mod._cell_env("hpcbridge-test-03", "123-4")
    assert "HPCB_NO_SKILL" not in env and "HPCB_EFFORT" not in env
    assert env["HPCB_TEST_GLOBUS_DB"] == "/x/storage.db" and env["HPCB_NODE_PARTITION"] == "main"
    assert env["HPCB_TEST_SSH_USER"] == "hpcbridge-test-03" and env["HPCB_RUNID"] == "123-4"


def test_cancelled_cell_is_terminated_and_leaves_the_list_only_once_it_exited(monkeypatch, tmp_path):
    # live 2026-09-05: Ctrl-C emptied `inflight` via the cell's finally BEFORE _emergency_cleanup ran, and the jail
    # container ran on for 3 minutes; nothing was cleaned up. Now the cell is SIGTERMed (docker proxies it in) and
    # waited for; only a cell that does not finish stays listed for the suite's run-scoped cleanup.
    mod = _run_suite()
    events = []

    class _Proc:
        def __init__(self, exits_in_time):
            self._exits = exits_in_time

        async def communicate(self):
            await asyncio.sleep(3600)  # the cell "runs" until cancelled

        def terminate(self):
            events.append("terminate")

        async def wait(self):
            if self._exits:
                return 0
            await asyncio.sleep(3600)
            return 1

    async def drive(exits_in_time):
        inflight = {}
        proc = _Proc(exits_in_time)

        async def fake_exec(*a, **k):
            return proc

        monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
        monkeypatch.setattr(mod, "CELL_STOP_GRACE_S", 0.05)
        claims = type("C", (), {"claim_any": lambda self, pool: "hpcbridge-test-07", "release": lambda self, u: None,
                                "busy": lambda self, pool: []})()
        sem = asyncio.Semaphore(1)
        stagger = mod.Stagger(0)
        task = asyncio.create_task(mod._run_job("happy_path", "m", None, None, None, claims, sem, stagger,
                                                asyncio.Event(), inflight))
        await asyncio.sleep(0.01)
        assert len(inflight) == 1  # listed while running
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return inflight

    left = asyncio.run(drive(exits_in_time=True))
    assert events == ["terminate"] and left == {}  # tore itself down in time: nothing for the suite to clean
    events.clear()
    left = asyncio.run(drive(exits_in_time=False))
    assert events == ["terminate"] and len(left) == 1  # did not finish: stays listed for _emergency_cleanup
    (user, name) = next(iter(left.values()))
    assert user == "hpcbridge-test-07" and name.startswith("hpc-bridge-globus1-")


def test_saturation_node_need_follows_the_target(monkeypatch):
    monkeypatch.setenv("HPCB_TARGET_NODES", "2")  # the fake cluster has two compute nodes
    rc, k = _knobs("saturation")
    assert rc == 0 and k.get("HPCB_KNOB_NEEDS_NODE") == "2"


def test_cell_env_keeps_the_target_selection(monkeypatch):
    mod = _run_suite()
    monkeypatch.setenv("HPCB_TARGET", "fake")
    monkeypatch.setenv("HPCB_FAKE_SSH_PORT", "2222")
    env = mod._cell_env("hpcbridge-test-00", "1-2")
    assert env["HPCB_TARGET"] == "fake" and env["HPCB_FAKE_SSH_PORT"] == "2222"


def test_idle_probe_uses_the_targets_default_partition(monkeypatch):
    mod = _run_suite()
    seen = []

    def fake_probe(remote):
        seen.append(remote)
        return "idle\n"

    monkeypatch.setattr(mod, "_probe_ssh", fake_probe)
    monkeypatch.delenv("HPCB_NODE_PARTITION", raising=False)
    monkeypatch.setattr(mod, "_TARGET", mod.targets.Target(name="fake", ssh_host="login", nodes=3, endpoint_prefix="x", default_key="k",
                                                            docker_network=None, probe_argv=("ssh",), cleanup_host="h", cleanup_ssh_opts=(),
                                                            profile="site", capabilities={"default_partition": "compute"}))
    assert mod._idle_nodes() == 1 and seen == ["sinfo -h -p compute -N -o %t"]  # site has no `main`


def test_suite_reads_shell_quoted_knob_values_as_data():
    # scenario_knobs.py shell-quotes JSON/CSV values for run_smoke.sh's eval; run_suite must unquote them (the first
    # site-profile suite crashed on json.loads("'{...}'") — 2026-09-05)
    mod = _run_suite()
    assert mod._requires("login_pin_teardown") == {"login_nodes": 2}
    assert mod._allowed_targets("login_pin_teardown") == {"fake"}
    assert mod._requires("happy_path") == {}


def test_admin_knobs_are_printed_and_gate_the_target():
    import json
    import shlex
    rc, k = _knobs("submit_policy_rejected")
    assert rc == 0 and "HPCB_KNOB_NEEDS_NODE" not in k and k["HPCB_KNOB_TARGETS"] == "fake"
    setup = json.loads(shlex.split(k["HPCB_KNOB_ADMIN_SETUP"])[0])
    cleanup = json.loads(shlex.split(k["HPCB_KNOB_ADMIN_CLEANUP"])[0])
    assert setup == ["sacctmgr -i modify user where name={user} set MaxSubmitJobs=0"]
    assert cleanup == ["sacctmgr -i modify user where name={user} set MaxSubmitJobs=-1"]
    rc, k2 = _knobs("happy_path")
    assert "HPCB_KNOB_ADMIN_SETUP" not in k2
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod._needs_admin("submit_policy_rejected") and not mod._needs_admin("happy_path")


def test_idle_probe_counts_free_pbs_nodes_on_a_pbs_target(monkeypatch):
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    import targets as tg
    monkeypatch.setattr(mod, "_TARGET", tg.Target(name="fake", ssh_host="login", nodes=2, endpoint_prefix="x", default_key="k",
                                                  docker_network=None, probe_argv=("true",), cleanup_host="localhost", cleanup_ssh_opts=(),
                                                  profile="pbs", capabilities={"scheduler": "pbs", "default_partition": "workq"}))
    seen = []
    monkeypatch.setattr(mod, "_probe_ssh", lambda cmd: (seen.append(cmd), "free\njob-busy\nfree\noffline\n")[1])
    assert mod._idle_nodes() == 2 and "pbsnodes" in seen[0] and "sinfo" not in seen[0]
    monkeypatch.setattr(mod, "_probe_ssh", lambda cmd: "pbsnodes: Server has no node list\n")
    assert mod._idle_nodes() is None   # an error message is not a state column
