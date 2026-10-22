"""Hermetic: slurm_worker_died's graders and its coupling to a Slurm profile with the login-only balance tool."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402

DEAD = "allocating nodes on 'debug'… — pilot 12 already FINISHED (exit status 42): the block started and its worker exited"


def _ensure(status, notice="", **inp):
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", inp, {"status": status, "notice": notice})


def test_scenario_couples_to_slurm_profiles_with_mybalance():
    import slurm_worker_died as sc
    for prof in ("site", "internal", "f2b", "totp", "lmod"):
        assert targets.meets(sc.REQUIRES, targets.load_profile(prof)["capabilities"])[0], prof
    assert not targets.meets(sc.REQUIRES, targets.load_profile("default")["capabilities"])[0]   # no mybalance
    assert not targets.meets(sc.REQUIRES, targets.load_profile("pbs")["capabilities"])[0]       # PBS
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided


def test_slurm_worker_died_graders():
    import slurm_worker_died as sc
    start = _ensure("provisioning", "allocating", shape="compute", partition="debug", account="hpcb", confirm_spend=True)
    explained = Trace([start, _ensure("provisioning", DEAD, shape="compute")],
                      ["The pilot job FINISHED with exit status 42: the env_setup's `command -v mybalance` check fails on the compute nodes."])
    assert sc.finished_surfaced(explained).ok and sc.exit_status_correct(explained).ok and sc.cause_relayed(explained).ok
    assert sc.died_pilot_handled(explained).ok and sc.no_endless_dead_wait(explained).ok
    vague = Trace(explained.calls, ["The block did not come up."])
    assert not sc.cause_relayed(vague).ok and not sc.died_pilot_handled(vague).ok
    forever = Trace([start] + [_ensure("provisioning", DEAD, shape="compute")] * 6, ["waiting"])
    assert not sc.no_endless_dead_wait(forever).ok
    other = Trace([start, _ensure("provisioning", DEAD.replace("42", "127"), shape="compute")], ["exit 127 …mybalance"])
    assert sc.finished_surfaced(other).ok and not sc.exit_status_correct(other).ok
    assert not sc.finished_surfaced(Trace([start, _ensure("provisioning", "— but NO pilot job is in the scheduler after ~60s", shape="compute")])).ok


def _shell(stdout, **inp):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", **inp}, {"phase": "complete", "stdout": stdout})


def test_a_dead_pilot_the_agent_read_itself_counts_once_it_is_explained():
    # codex, sweep 2026-10-22 (1792695120-15080-26483): one ensure_endpoint_up saw the pilot queued; the agent then read
    # `scontrol show job 7` itself (FAILED, ExitCode=42:0), explained it and stopped, never polling the tool again
    import slurm_worker_died as sc
    start = _ensure("provisioning", "allocating nodes on 'debug'… — pilot 7 is queued (PENDING (reason Resources))",
                    shape="compute", partition="debug", account="hpcb", confirm_spend=True)
    scontrol = _shell("JobId=7 JobName=parsl.GlobusComputeEngine-HighThroughputExecutor.block-0\n"
                      "   JobState=FAILED Reason=NonZeroExitCode Dependency=(null)\n   ExitCode=42:0\n",
                      command="scontrol show job 7")
    why = ["Pilot 7 FAILED with ExitCode=42: your guard `command -v mybalance || exit 42` fails on the compute node."]
    t = Trace([start, scontrol], why)
    assert not sc.finished_surfaced(t).ok              # the tool never said it …
    r = sc.died_pilot_handled(t)
    assert r.ok and "read the dead pilot itself (call 1)" in r.detail, r.detail
    sacct = _shell("JobID  State ExitCode\n10     FAILED     42:0\n", command="sacct -j 10")
    assert sc.died_pilot_handled(Trace([start, sacct], why)).ok
    # … but reading it is not explaining it, and a queued pilot is not a dead one
    assert not sc.died_pilot_handled(Trace([start, scontrol], ["The block did not come up."])).ok
    assert not sc.died_pilot_handled(Trace([start, _shell("7|debug|PENDING|Resources")], why)).ok
