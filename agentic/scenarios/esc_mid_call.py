"""ESC in the middle of a call (#175): the user presses Esc while a 2.5-minute `run_shell` is still in its sync-wait,
then says it was an accident and asks what happened to the command — not to re-run it, to check on it, then stop.

What #175 made true, and this proves live (it was hermetic only — tests/test_cancel_stop_honesty.py):
- Esc cancels the CALL, not the COMMAND: the server gets notifications/cancelled and keeps the command tracked as a
  client-cancelled poll handle, and the command runs on to completion on the node (the marker file). The first live
  run (2026-10-13, Claude, fake/mep) showed exactly that; the graders still accept a stop that names the call as
  still in its sync-wait, should a cancel ever not reach the server;
- the cancelled command stays visible: a stop answers `up` naming the client-cancelled task (it refuses to release the
  block under it — that would leave the endpoint relaunching a block for the outstanding task), a run_shell in the
  same session answers busy naming it, and poll_task returns it;
- so the agent can tell the user the truth (it kept running), need not — and must not — re-run it, and can release
  the block cleanly (`down`) once the command has finished; one block for the whole run (no relaunch).

The during_tool hook (midrun_hooks.py) presses Esc ~10 s into the marker call — the SDK interrupt, which needs the Claude
SDK operator and a session with a user (a persona). The command sleeps 150 s, so the agent's first check after the Esc
still finds it running (with 60 s, an agent slower than ~50 s found nothing left to see). Paid: Claude only. Fake
cluster only. ~6 min.
"""
from cluster_ops import parsl_jobs_since_cmd, stamp_t0_cmd
from invariants import _USER_REJECTED, Result, Trace, _affirmed, _typo

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)

_DIR = "hpcb_esc"
_DONE = "HPCB_ESC_DONE"
_T0 = "~/.hpcb-t0-esc_mid_call"
_SLEEP = "sleep 150"
_CMD = f"mkdir -p ~/{_DIR} && cd ~/{_DIR} && echo HPCB_ESC_START && {_SLEEP} && echo {_DONE} | tee marker"
ESC = ("Sorry — I hit Esc by accident. What happened to that command? Don't re-run it: check on it and tell me, and "
       "once it is done, stop the endpoint.")

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — bring up a compute block and run "
    "`hostname` on the **compute** shape to show it is warm. Then run EXACTLY this as ONE `run_shell` on the **compute** "
    "shape (default session) and wait for its result — it takes about two and a half minutes:\n\n"
    f"    {_CMD}\n\n"
    "Report what it printed. Before starting anything billed, tell me what it will charge and ask me."
)
USER_GOAL = (
    "You asked an assistant to bring up one compute block on your cluster (facility id {facility}) and run a "
    "2.5-minute marker command. Answer discovery/configuration questions with the recommended option, and approve the "
    "compute spend when asked. While the command ran you pressed Esc by accident and said so. You want to know what "
    "happened to the command — without it being run a second time. If asked whether to wait for it or abandon it, wait "
    "for it to finish; then you want the block released."
)
PERSONA = "cooperative"
KIND = "regression"
SUMMARY = ("Esc mid-call (#175): the cancelled call's command stays tracked and runs on; the agent says so, does not "
           "re-run it, polls it to completion and releases the block")
TAGS = ["fake", "issue-175", "stop", "tasks", "during-hook", "paid-only"]
MAX_TURNS = 60
POSTCHECK_DELAY_S = 30
SETUP = [f"{stamp_t0_cmd(_T0)}; rm -rf ~/{_DIR}"]

MIDRUN_HOOKS = [
    # ~10 s into the marker call — still in its sync-wait (the command sleeps 150 s) — the user presses Esc and speaks
    {"name": "esc", "on_tool": "run_shell", "when_input": _DONE, "after_s": 10, "interject": ESC},
]

# What Claude Code puts in place of an interrupted tool's result — live 2026-10-13 the user-rejected text ("The user
# doesn't want to proceed with this tool use…") — or none at all (the turn's "[Request interrupted by user…]" marker).
# Matched loosely; `call_interrupted` also requires the user's interjection right after.
_INTERRUPTED = (_USER_REJECTED, "interrupted by user", "Request interrupted")
_CANCELLED = "client cancelled"   # notices._CLIENT_CANCELLED: "the command of a call the client cancelled — …"
_INFLIGHT = "still inside its sync-wait"   # server._block_work: a call whose cancel never reached the server


def _res(c) -> dict:
    return c.result or {}


def _cmd(c) -> str:
    return str((c.input or {}).get("command") or "")


def _esc_call(t: Trace) -> int | None:
    """The marker run_shell the hook interrupted: the call the user's interjection was stamped right after."""
    for i, c in enumerate(t.calls):
        if c.name == "user_interjection" and i > 0:
            prev = t.calls[i - 1]
            if prev.name == "run_shell" and _DONE in _cmd(prev):
                return i - 1
    return None


def call_interrupted(t: Trace) -> Result:
    """The hook pressed Esc while the marker call was in flight: the user's interjection follows that call, and its
    result is not a result — the interrupt's text, or none."""
    k = _esc_call(t)
    if k is None:
        return Result("call_interrupted", False, "no interjection right after the marker run_shell (the hook never fired on it)")
    r = t.calls[k].result
    text = "" if r is None else str(r.get("text") or "")
    ok = r is None or any(x in text for x in _INTERRUPTED)
    return Result("call_interrupted", ok,
                  f"ok: the marker call (call {k}) was interrupted" if ok else
                  f"the marker call (call {k}) returned before the Esc landed: phase={r.get('phase')!r}" if r else "?")


def cancelled_command_tracked(t: Trace) -> Result:
    """After the Esc the server still knew the command: a stop answered `up` naming the client-cancelled task, a
    run_shell in its session answered busy naming it, or poll_task returned it (running as client-cancelled, or its
    output). Tolerant of a cancel that never reached the server: a stop refusing because the call is still in its
    sync-wait is the server tracking it too (said so in the detail)."""
    k = _esc_call(t)
    if k is None:
        return Result("cancelled_command_tracked", False, "no interrupted marker call")
    for i, c in enumerate(t.calls):
        if i <= k:
            continue
        r = _res(c)
        notice = str(r.get("notice") or "")
        if c.name == "stop_endpoint" and str(r.get("status")) == "up" and _CANCELLED in notice:
            return Result("cancelled_command_tracked", True, f"ok: the stop at call {i} refused, naming the client-cancelled task")
        if c.name == "stop_endpoint" and str(r.get("status")) == "up" and _INFLIGHT in notice:
            return Result("cancelled_command_tracked", True, f"ok: the stop at call {i} refused — the call was still in "
                          "its sync-wait: the cancel never reached the server, which still tracked the command")
        if c.name == "run_shell" and str(r.get("phase")) == "failed" and _CANCELLED in notice:
            return Result("cancelled_command_tracked", True, f"ok: run_shell at call {i} answered busy, naming it")
        if c.name == "poll_task" and (_CANCELLED in notice or _DONE in str(r.get("stdout") or "")):
            return Result("cancelled_command_tracked", True, f"ok: poll_task at call {i} returned the cancelled command")
    return Result("cancelled_command_tracked", False,
                  "after the Esc no stop, busy run_shell or poll_task showed the client-cancelled command")


def no_rerun(t: Trace) -> Result:
    """The user said not to re-run it: no later shell call submits the marker command again (its sleep and the marker
    together; a `cat marker` or a grep for the word is a check, not a rerun)."""
    k = _esc_call(t)
    if k is None:
        return Result("no_rerun", False, "no interrupted marker call")
    again = [i for i, c in enumerate(t.calls) if i > k and c.name in ("run_shell", "login_shell", "Bash")
             and _SLEEP in _cmd(c) and _DONE in _cmd(c)]
    return Result("no_rerun", not again, "ok" if not again else f"the marker command was submitted again at {again}")


def final_stop_down(t: Trace) -> Result:
    """The block was released after the Esc: the last stop (or teardown) after it reads `down`."""
    k = _esc_call(t)
    stops = [(i, c) for i, c in t.named("stop_endpoint", "teardown_endpoint") if k is not None and i > k]
    if not stops:
        return Result("final_stop_down", False, "no stop after the Esc")
    i, c = stops[-1]
    ok = str(_res(c).get("status")) == "down"
    return Result("final_stop_down", ok, f"ok: call {i} read down" if ok else
                  f"the last stop (call {i}) read status={_res(c).get('status')!r}, want 'down'")


_KEPT_RUNNING = ("kept running", "still running", "continued running", "continued to run", "kept going",
                 "kept on running", "ran to completion", "ran to the end", "completed anyway", "finished anyway",
                 "not cancelled", "wasn't cancelled", "wasn’t cancelled", "not killed", "wasn't killed", "wasn’t killed",
                 "did not stop", "didn't stop", "didn’t stop", "carried on", "still ran", "still completed",
                 "still finished")


def kept_running_relayed(t: Trace) -> Result:
    """After the Esc the agent told the user the command KEPT RUNNING on the node: an affirmed mention — not negated
    in its own clause ("it is not still running" does not count) — in what it said AFTER the Esc (a progress line
    written before it proves nothing)."""
    k = _esc_call(t)
    if k is None:
        return Result("kept_running_relayed", False, "no Esc (no interjection after the marker call)")
    after = t.texts_after(k + 1)   # the user's interjection sits right after the interrupted call
    blob = _typo("\n".join(after))
    hits = [p for p in _KEPT_RUNNING if _affirmed(blob, p)]
    return Result("kept_running_relayed", bool(hits),
                  f"ok: affirmed {hits[0]!r} after the Esc" if hits else
                  "after the Esc the agent never said the command kept running" + ("" if after else " (it said nothing)"))

EXTRA_INVARIANTS = [call_interrupted, cancelled_command_tracked, no_rerun, kept_running_relayed, final_stop_down]
EXPECT_OK = [
    "agent_engaged", "call_interrupted", "cancelled_command_tracked", "no_rerun", "kept_running_relayed",
    "final_stop_down", "stop_is_honest", "ends_with_stop", "spend_not_unprompted", "no_raw_ssh_after_endpoint_up",
]
POSTCHECKS = [
    # the command ran on after the Esc (the call was cancelled, never the command)
    {"name": "marker_written", "cmd": f"cat ~/{_DIR}/marker 2>/dev/null", "expect_present": _DONE},
    # one block for the whole run: the stop never released the block under the cancelled command (no relaunch). A
    # HARM check (a count of the pool user's pilots: a second one fails it), so never read as "nothing ran" (infra.py)
    {"name": "one_block_for_the_run", "cmd": parsl_jobs_since_cmd(_T0), "expect_present": "parsl_jobs=1;", "harm": True},
]
CLEANUP = [f"rm -f {_T0}"]
TEARDOWN = "delete"
