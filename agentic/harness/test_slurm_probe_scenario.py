"""Hermetic: slurm_worker_died's graders and its coupling to a Slurm profile with the login-only balance tool."""
from __future__ import annotations

import json
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


def test_the_cause_must_be_explained_after_the_dead_pilot_not_echoed_from_the_plan():
    # the review's probe of the 2026-10-22 sweep: the agent echoed its env_setup plan — "prepending `command -v mybalance
    # … || exit 42; `" — named 42 and mybalance before anything died, said nothing about the cause after, and passed
    import slurm_worker_died as sc
    start = _ensure("provisioning", "allocating", shape="compute", partition="debug", account="hpcb", confirm_spend=True)
    dead = _ensure("provisioning", DEAD, shape="compute")
    stop = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down", "notice": "released 12"})
    plan = "Accepting the config and prepending `command -v mybalance >/dev/null 2>&1 || exit 42; ` to env_setup."
    echo = Trace([start, dead, stop], [plan, "The block did not come up. Stopping."], [0, 2])
    assert not sc.cause_relayed(echo).ok and not sc.died_pilot_handled(echo).ok
    told = Trace(echo.calls, [plan, "Pilot 12 FINISHED with exit status 42: mybalance is missing on the compute nodes."], [0, 2])
    assert sc.cause_relayed(told).ok and sc.died_pilot_handled(told).ok


def test_compute_that_ran_without_the_users_policy_check_is_no_fix():
    # the "fixed" branch passed any run that reached compute — including one whose env_setup had dropped the user's
    # `command -v mybalance` check (or never added it): the policy went, and the cell read "fixed"
    import slurm_worker_died as sc
    def connect(env):
        return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": {"env_setup": env}},
                           {"phase": "needs_account"})
    start = _ensure("up", "", shape="compute", partition="debug", account="hpcb", confirm_spend=True)
    ran = _shell("c1\n", command="hostname", shape="compute")
    kept = "export PATH=$HOME/bin:$PATH; command -v mybalance >/dev/null 2>&1 || exit 42; source venv"
    r = sc.died_pilot_handled(Trace([connect("command -v mybalance >/dev/null 2>&1 || exit 42; x"), connect(kept), start, ran]))
    assert r.ok and r.detail.startswith("fixed"), r.detail
    for env in ("source venv", None):
        calls = ([connect(env)] if env else []) + [start, ran]
        r = sc.died_pilot_handled(Trace(calls))
        assert not r.ok and r.detail.startswith("policy dropped"), r.detail
    # details sent as a JSON string (a CLI harness's coercion) are read too
    as_json = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": json.dumps({"env_setup": kept})},
                          {"phase": "needs_account"})
    assert sc.died_pilot_handled(Trace([as_json, start, ran])).ok


def test_run_shell_polls_of_a_dead_pilot_count_toward_the_bound():
    import slurm_worker_died as sc
    start = _ensure("provisioning", "allocating", shape="compute", partition="debug", account="hpcb", confirm_spend=True)
    polls = [ToolCall.of("mcp__endpoint__run_shell", {"command": "hostname"}, {"phase": "cold_start", "notice": DEAD})] * 5
    r = sc.no_endless_dead_wait(Trace([start, _ensure("provisioning", DEAD, shape="compute"), *polls]))
    assert not r.ok and "the dead pilot" in r.detail, r.detail


def test_a_later_details_without_the_check_is_the_policy_dropped():
    # the review of re-run #1: the last details= decides; one that leaves env_setup out drops the check even if an
    # earlier one carried it (the server falls back to the proposed env_setup)
    import slurm_worker_died as sc

    def connect(details):
        return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": details}, {"phase": "needs_account"})
    ran = ToolCall.of("mcp__endpoint__run_shell", {"command": "hostname", "shape": "compute"},
                      {"phase": "complete", "stdout": "c1\n", "exit_code": 0})
    kept = {"ssh_host": "login", "env_setup": "command -v mybalance >/dev/null 2>&1 || exit 42; true"}
    r = sc.died_pilot_handled(Trace([connect(kept), connect({"ssh_host": "login", "partition": "debug"}), ran], ["ok"]))
    assert not r.ok and "the last details= set no env_setup" in r.detail, r.detail
    assert sc.died_pilot_handled(Trace([connect(kept), ran], ["ok"])).ok
