"""CHAOS: force the honest `draining` stop (#24) and make the agent re-stop until `down`. Fake cluster only.

stop_endpoint releases the block by dispatching the scheduler cancel through the LOGIN shape (AMQP). When that
channel is cold it cannot confirm, and the product answers status=draining (never a false `down`); the skill then
says: call stop_endpoint again until `down`. Live that happens in ~5% of stops by luck — `stop_confirmed_or_retried`
has only ever passed on its vacuous branch. The hook kills the login-shape worker pool on the login node right after
the agent's second compute run_shell, so the stop that follows meets a cold channel.
"""
from invariants import Result, Trace, compute_ran

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — bring up a compute block and run "
    "`hostname` on the **compute** shape TWICE, as two separate run_shell calls. Then stop the endpoint to release "
    "the block and confirm it is released. stop_endpoint may answer status='draining' — that means the release could "
    "not yet be confirmed: call stop_endpoint again until it answers status='down', and report the final status.\n\n"
    "This is an AUTOMATED run — accept discovered facility config yourself; you are authorised to confirm the spend."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "chaos: login worker killed before stop → `draining`, then the agent re-stops to `down` (#24's positive branch)"
TAGS = ["chaos", "fake", "issue-24", "stop"]
MAX_TURNS = 40
NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
POSTCHECK_DELAY_S = 30

MIDRUN_HOOKS = [
    # the login shape's worker pool runs on the login node under the pool user; the compute block's workers run on a
    # compute container and are untouched. FREEZE it (SIGSTOP), don't kill it: a killed worker is relaunched by the
    # manager within its strategy period, and on the 2026-09-06 sweep the stop landed after the relaunch — a warm
    # channel, an honest `down`, and a scenario that graded the wrong world. A frozen worker stays frozen until the
    # second hook resumes it after the FIRST stop (draining), so the re-stop finds the channel warm again (down).
    # The bracket trick: a plain `-f process_worker_pool` matched the ssh shell's own command line (first run, 09-05).
    # "on": "each_login": the endpoint's manager (and its login worker pool) sits on ONE node of a round-robin pool;
    # a hook sent to the alias may land on the other one (the sweep rerun, 2026-09-06). pkill's own rc is echoed, and
    # `expect_any_host` makes each hook FAIL (harness:midrun_hooks) unless some node actually froze / thawed a worker.
    {"name": "freeze_login_worker", "after_tool": "run_shell", "when_input": {"shape": "compute"}, "nth": 2, "on": "each_login",
     "cmd": "pkill -STOP -u $(whoami) -f 'process_worker_[p]ool'; echo \"frozen pkill-rc=$?\"", "expect_any_host": "pkill-rc=0"},
    {"name": "thaw_login_worker", "after_tool": "stop_endpoint", "nth": 1, "on": "each_login",
     "cmd": "pkill -CONT -u $(whoami) -f 'process_worker_[p]ool'; echo \"thawed pkill-rc=$?\"", "expect_any_host": "pkill-rc=0"},
]
CLEANUP = [{"on": "each_login", "cmd": "pkill -CONT -u $(whoami) -f 'process_worker_[p]ool' 2>/dev/null; echo thawed"}]   # never leave a frozen worker


def draining_seen(t: Trace) -> Result:
    """The chaos worked: at least one stop_endpoint answered status=draining (the honest unconfirmed release)."""
    ok = any(str((c.result or {}).get("status")) == "draining" for _, c in t.named("stop_endpoint"))
    return Result("draining_seen", ok, "ok" if ok else "no stop_endpoint answered draining — the cold-channel case was not provoked")


EXTRA_INVARIANTS = [compute_ran, draining_seen]
EXPECT_OK = [
    "agent_engaged", "compute_ran", "draining_seen", "stop_confirmed_or_retried", "stop_is_honest", "ends_with_stop",
    "spend_not_unprompted", "no_raw_ssh_after_endpoint_up",
]
TEARDOWN = "delete"
