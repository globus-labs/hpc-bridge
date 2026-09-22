"""CHAOS: the compute block is REAPED while the session sits idle, then the user comes back and asks for more work.

The shape nobody else covers: a session left open (the MCP server process alive, its in-memory state still saying
"warm"), the pilot block gone underneath it (idle-release after `max_idletime`, a walltime expiry, or an operator's
scancel), and then a new `run_shell`. `idle_release_kill` proves the reap kills a detached job; `orphaned_task` kills the
MANAGER; neither returns and launches. Here the harness plays the reap (an `scancel` of the pilot job on the login node,
as the pool user, then a wait past CANARY_TTL_S so the plugin cannot trust its last canary) and the user (an `interject`
hook: "I'm back — run it again and tell me what happened").

What must hold, from the code (warmth._confirm_worker / server._run_shell):
- the first call after the reap is honest: `phase="cold_start"` / `block_state="provisioning"` — the command is NOT
  dispatched into the void, and the canary's submit re-kicks a block;
- the session's persisted state survives the block: cwd/env live under `<scratch>/sessions/<id>` on the shared
  filesystem, so `cat marker` WITHOUT a cd reads the marker back on the NEW block. The first live run (2026-09-22)
  passed a weaker check: the agent cd'd defensively AND told the user the cwd "lived in the dead worker's session
  state" — false, and exactly the narration slip this scenario now catches (the returning user forbids the cd);
- the work then completes on the new block (`phase="complete"` with the marker in stdout);
- the agent TELLS the user a new block was needed rather than narrating continuity;
- the spend acknowledgement persists for the session, so the re-provision happens without a re-ask — by design
  (`ShapeRuntime.spend_confirmed`); this scenario records the fact, it does not grade it.
Fake cluster only (the hook cancels this run's pilot job). One block twice, ~6-8 min.
"""
from invariants import Result, Trace, _shape, texts_mention

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)        # the hook scancels the run's pilot job — never on a shared cluster

_DIR = "hpcb_reap"
_MARK = "HPCB_REAP_7c1d"

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — bring up a compute block, then "
    "run EXACTLY this as ONE `run_shell` on the **compute** shape (default session):\n\n"
    f"    mkdir -p ~/{_DIR} && cd ~/{_DIR} && echo {_MARK} > marker && cat marker && hostname && pwd\n\n"
    "Report its output to me and then STOP — do not run anything else, do not release the block, do not poll; "
    "I will come back with the next step.\n\n"
    "Before starting anything billed, tell me what it will charge and ask me."
)
RETURN = (
    "Sorry — I got pulled away for a while, I'm back now. The cluster may have cleaned things up in the meantime. "
    f"Please run EXACTLY `cat marker && hostname && pwd` on the compute node again — same session, do NOT cd first and "
    f"do not use an absolute path: I want to see whether the session is still in ~/{_DIR} on its own. Tell me exactly "
    "what happened: was the block still there, or did a new one have to start? Was anything billed again? Then "
    "release the block."
)
USER_GOAL = (
    "You asked an assistant to bring up one compute block on your cluster (facility id {facility}) and run one marker "
    "command. Answer discovery/configuration questions with the recommended option, and approve the compute spend "
    "when asked. LATER YOU COME BACK after being away: you want the marker re-read on the compute node and an honest "
    "account of whether the block survived or a new one had to start — if the assistant asks to confirm spend again, "
    "approve it. You want the check run exactly as you said it, WITHOUT a cd or an absolute path (if offered a choice, "
    "pick the one that runs the command as-is); then you want the block released."
)
PERSONA = "cooperative"
KIND = "regression"
SUMMARY = "chaos: block reaped under an idle session → honest cold_start, session state survives, work completes on a new block, agent says so"
TAGS = ["chaos", "fake", "idle-release", "session", "cold_start"]
MAX_TURNS = 60
POSTCHECK_DELAY_S = 45

MIDRUN_HOOKS = [
    # 1. Reap the pilot block after the marker was written: cancel every job of this run's user on the login node
    #    (the login shape is a LocalProvider there — no Slurm job — so only the compute pilot goes), wait for the queue
    #    to drain, then sit past CANARY_TTL_S (45 s) so the next call must re-canary instead of trusting warmth.
    {"name": "reap_block", "after_tool": "run_shell", "when_input": {"shape": "compute"}, "nth": 1,
     "cmd": ("scancel -u \"$(whoami)\"; for i in $(seq 1 30); do squeue -u \"$(whoami)\" -h | grep -q . || break; sleep 2; done; "
             "echo \"reaped; queue-left=$(squeue -u \"$(whoami)\" -h | wc -l)\"; sleep 55"),
     "timeout": 180},
    # 2. The user returns. Same trigger; hooks fire in list order, and the cmd above is awaited first.
    {"name": "user_returns", "after_tool": "run_shell", "when_input": {"shape": "compute"}, "nth": 1,
     "interject": RETURN},
]


def _res(c) -> dict:
    return c.result or {}


def _first_marker_write(t: Trace) -> int | None:
    """Index of the compute run_shell that wrote the marker (the call the reap follows)."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _MARK in str(_res(c).get("stdout", "")) and _res(c).get("phase") == "complete":
            return i
    return None


def _cold_after(t: Trace, start: int) -> int | None:
    """First call after `start` whose result shows the block gone: cold_start / provisioning."""
    for i, c in enumerate(t.calls):
        if i <= start or c.name not in ("run_shell", "ensure_endpoint_up", "poll_task"):
            continue
        r = _res(c)
        if r.get("phase") == "cold_start" or r.get("block_state") == "provisioning" or r.get("status") == "provisioning":
            return i
    return None


def marker_written_before_reap(t: Trace) -> Result:
    ok = _first_marker_write(t) is not None
    return Result("marker_written_before_reap", ok, "ok" if ok else "no completed compute run_shell carried the marker")


def resume_is_honest_cold_start(t: Trace) -> Result:
    """After the reap, the plugin must NOT pretend the old block is there: some call reports cold_start/provisioning."""
    start = _first_marker_write(t)
    if start is None:
        return Result("resume_is_honest_cold_start", False, "no marker write to anchor on")
    cold = _cold_after(t, start)
    ok = cold is not None
    return Result("resume_is_honest_cold_start", ok,
                  "ok" if ok else "no run_shell/ensure_endpoint_up/poll_task after the reap reported cold_start or provisioning")


def session_state_survived_on_new_block(t: Trace) -> Result:
    """After the cold report, a complete run_shell reads the marker back WITHOUT a cd or an absolute path — the only
    way the read proves the session's cwd (in `<scratch>/sessions/<id>/.cwd`, off-block) survived the reap. A
    defensive `cd ~/hpcb_reap && cat marker` reads the file too, but proves nothing and usually comes with the false
    story that the session was lost with the block (first live run)."""
    start = _first_marker_write(t)
    cold = _cold_after(t, start) if start is not None else None
    if cold is None:
        return Result("session_state_survived_on_new_block", False, "no cold report to follow")
    reads = [(i, c) for i, c in enumerate(t.calls) if i > cold and c.name in ("run_shell", "poll_task")
             and _res(c).get("phase") == "complete" and _MARK in str(_res(c).get("stdout", ""))]
    if not reads:
        return Result("session_state_survived_on_new_block", False,
                      "no completed call after the cold report returned the marker")
    cmds = [str((c.input or {}).get("command", "")) for _, c in reads if c.name == "run_shell"]
    trusting = [cmd for cmd in cmds if "cd " not in cmd and _DIR not in cmd]
    if trusting:
        return Result("session_state_survived_on_new_block", True, "ok: marker read back with no cd (session cwd survived the reap)")
    return Result("session_state_survived_on_new_block", False,
                  f"marker read only via an explicit cd/path — the session's cwd was not trusted: {cmds[0][:80]!r}")


reprovision_relayed = texts_mention(
    "reprovision_relayed",
    any_of=("new block", "new compute", "re-provision", "reprovision", "provisioned again", "provisioned a new",
            "cold start", "cold-start", "had to start", "had to be started", "was gone", "no longer there",
            "was reaped", "was released", "spun up", "started a new", "started again", "brought up a new"),
)

EXTRA_INVARIANTS = [marker_written_before_reap, resume_is_honest_cold_start, session_state_survived_on_new_block,
                    reprovision_relayed]
EXPECT_OK = [
    "agent_engaged", "marker_written_before_reap", "resume_is_honest_cold_start",
    "session_state_survived_on_new_block", "reprovision_relayed",
    "no_raw_ssh_after_endpoint_up", "spend_not_unprompted", "ends_with_stop",
]
TEARDOWN = "delete"
