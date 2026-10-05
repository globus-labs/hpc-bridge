"""LIVE reproduction of the #21 idle-release SILENT KILL (the headline half, ~18 min).

The INVERSE of `long_job_30m`. There the agent AVOIDS the trap (sbatch-on-login) and the job SURVIVES.
Here the prompt FORCES the trap: launch a long **detached** job ON the compute pilot block and poll it
from the **login** shape — so no Compute task keeps the block busy. After ~600s (`max_idletime`,
`min_blocks=0`) the block idle-releases (`scancel`) and the detached job dies with it, silently, mid-run.

Run with **--no-skill** (`HPCB_NO_SKILL=1`): the skill exists precisely to steer off this footgun.
`POSTCHECK_DELAY_S=720` waits PAST the 600s window, then proves the job DIED. One billed block, ~18 min.

Item 2 (submit/poll) makes detaching UNNECESSARY — a long *foreground* task keeps the block busy and is
retrieved via poll_task (see `long_task_via_handle`) — but it does NOT make a *detached* process safe:
a backgrounded job is still not a Compute task, so this scenario still reproduces the kill.
See issue #21 (https://github.com/globus-labs/hpc-bridge/issues/21).
"""
from invariants import Result, Trace

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` (facility id `{facility}`), bring "
    "up a compute node and start a ~20-minute CPU job DIRECTLY ON THE COMPUTE BLOCK as a detached "
    "background process — I want it decoupled from any single run_shell call. Concretely:\n\n"
    "1. On the **compute** shape, make `~/hpcb_idlekill/` and write `job.py` (brace-free on purpose):\n"
    "   ```\n"
    "   import time, os\n"
    "   p = os.path.expanduser('~/hpcb_idlekill/progress.log')\n"
    "   for i in range(40):\n"
    "       with open(p, 'a') as f:\n"
    "           f.write(str(time.time()) + ' iter ' + str(i) + chr(10))\n"
    "       time.sleep(30)\n"
    "   open(p, 'a').write('DONE' + chr(10))\n"
    "   ```\n"
    "2. Launch it on the **compute** shape, detached: "
    "`setsid nohup python3 ~/hpcb_idlekill/job.py >/dev/null 2>&1 &` — NOT sbatch, NOT the login shape.\n"
    "3. Confirm it wrote a first progress line, then poll its progress ONLY from the **login** shape "
    "(`run_shell(shape=login)`, e.g. `tail ~/hpcb_idlekill/progress.log`). Do NOT run anything else on "
    "the compute shape, and do NOT stop the endpoint — I'm heading offline and need it to keep going.\n\n"
    "Tell me how you set it up and that it's producing output. This is an AUTOMATED run — accept "
    "discovered config yourself and you are authorised to confirm the spend."
)

USER_GOAL = ""
PERSONA = None
KIND = "diagnostic"
SUMMARY = "#21 live: a detached long job on the compute pilot block is silently scancel'd by idle-release (~600s)"
TAGS = ["issue-21", "idle-release", "silent-kill", "diagnostic", "ablation"]

# Wait PAST the 600s idle-release window before the world checks — the inverse of long_job_30m.
POSTCHECK_DELAY_S = 720

POSTCHECKS = [
    {
        # The job's PROGRESS freezes at ~600s (idle-release yanks its block) without ever reaching
        # DONE — the #21 silent kill, read against the real mechanism. Detect FROZEN progress, NOT
        # process-absence: globus1's `setsid` escape leaves a STUCK ORPHAN (the process lingers on the
        # login node, hung after its compute node is reclaimed), so `pgrep` gives a false "still
        # running" — the honest signal is the log stopped growing short of DONE. (Observed live 2026-07.)
        "name": "job_killed_by_idle_release",
        "cmd": (
            'n1=$(wc -l < ~/hpcb_idlekill/progress.log 2>/dev/null || echo 0); sleep 35; '
            'n2=$(wc -l < ~/hpcb_idlekill/progress.log 2>/dev/null || echo 0); '
            'if grep -q DONE ~/hpcb_idlekill/progress.log 2>/dev/null; then echo SURVIVED_TO_DONE; '
            'elif [ "$n1" = "$n2" ]; then echo KILLED; else echo STILL_RUNNING; fi'
        ),
        "expect_present": "KILLED",
    },
    {
        # It DID start (proves the job ran on the block) — so KILLED means killed, not never-launched.
        "name": "progress_started_then_stopped",
        "cmd": '[ -s ~/hpcb_idlekill/progress.log ] && echo STARTED || echo NEVER_STARTED',
        "expect_present": "STARTED",
    },
]


def launched_detached_on_compute(t: Trace) -> Result:
    """Confirm the agent actually took the #21 trap: a detached launch on the COMPUTE shape (not
    sbatch / not the login shape). If it rerouted, the kill won't reproduce and this flags it."""
    for i, c in t.named("run_shell"):
        cmd = str(c.input.get("command", ""))
        if c.input.get("shape") in (None, "compute") and (
            "setsid" in cmd or "nohup" in cmd or cmd.rstrip().endswith("&")
        ):
            return Result("launched_detached_on_compute", True,
                          f"ok: detached launch on the compute shape at call {i}")
    return Result("launched_detached_on_compute", False,
                  "agent did NOT launch a detached job on the compute shape (rerouted — the kill won't reproduce)")


EXTRA_INVARIANTS = [launched_detached_on_compute]

# The trace confirms the trap was taken; the world postchecks (which always gate) confirm the kill.
# NOT ends_with_stop: the agent is told to leave the job running — the idle timer does the killing.
EXPECT_OK = ["launched_detached_on_compute"]

NEEDS_COMPUTE_NODE = True   # brings up a billed block (no compute_ran grader to derive it from) — run_suite gates it
TEARDOWN = "delete"
MAX_TURNS = 60
