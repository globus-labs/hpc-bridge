"""EDGE: a batch job the agent submits by hand is spend that hpc-bridge doesn't track. Fake cluster only.

Until 2026-10 the skill told agents to `sbatch` long work over the login shape, which is unbilled — real spend with no
confirmation, and nothing in the product or the graders noticed. Now the result of a command that submits a job carries
a notice: it bills the allocation like a block, hpc-bridge neither tracks nor stops it, and how to cancel it. Here the
prompt carries the user's approval (autonomous), so the cell checks the notice, that the agent cancels the job, and that
nothing of it is left on the cluster. Login shape only — no compute block.
"""
from invariants import Result, Trace

_JOB = "hpcb-handjob"
_SCRIPT = (
    "cat > hpcb_hand.sh <<'EOF'\n"
    "#!/bin/bash\n"
    f"#SBATCH -J {_JOB}\n"
    "#SBATCH -p debug\n"
    "#SBATCH -A hpcb\n"
    "#SBATCH -t 00:05:00\n"
    "sleep 240\n"
    "echo HPCB_HANDJOB done\n"
    "EOF"
)

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — connect (if it isn't catalogued, "
    "probe the login node and accept the discovered configuration yourself) and wait until the login node is up. Use "
    "the LOGIN shape for every command (`shape='login'`); do NOT bring up a compute block.\n\n"
    "The user has ALREADY approved this one batch job and its cost: partition `debug`, account `hpcb`, 1 node, 5 "
    "minutes. Write the job script with ONE run_shell:\n\n"
    + "\n".join("    " + line for line in _SCRIPT.split("\n")) + "\n\n"
    "then submit it with `sbatch hpcb_hand.sh` in a separate run_shell, and report exactly what hpc-bridge said about "
    "the submission. Then cancel the job (`scancel <jobid>`), check with `squeue -u $USER` that it is gone, and report.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "edge: a hand-submitted sbatch over the login shape comes back with the billed/untracked notice; the agent cancels it"
TAGS = ["fake", "spend", "login", "batch"]
MAX_TURNS = 30
TARGETS = ("fake",)


def _submits(t: Trace) -> list:
    return [c for _, c in t.named("run_shell")
            if "sbatch" in str(c.input.get("command", "")) and "cat >" not in str(c.input.get("command", ""))]


def submission_noticed(t: Trace) -> Result:
    """The command that submitted the job comes back with the notice: billed like a block, not tracked, how to cancel."""
    done = [c for c in _submits(t) if str((c.result or {}).get("phase")) == "complete"]
    if not done:
        return Result("submission_noticed", False, "the sbatch never completed")
    r = done[0].result or {}
    notice = str(r.get("notice", "")).lower()
    ok = "submitted batch job" in str(r.get("stdout", "")).lower() and "allocation" in notice and "scancel" in notice
    return Result("submission_noticed", ok,
                  "ok: the submission came back with the billed/untracked notice" if ok
                  else f"stdout {str(r.get('stdout', ''))[:80]!r}; notice {notice[:160]!r}")


def job_cancelled(t: Trace) -> Result:
    """After the submission the agent cancelled the job (scancel) — the cell's world check confirms nothing is left."""
    idx = [i for i, c in t.named("run_shell") if "sbatch" in str(c.input.get("command", ""))
           and "cat >" not in str(c.input.get("command", ""))]
    if not idx:
        return Result("job_cancelled", False, "no submission")
    later = [c for i, c in t.named("run_shell") if i > idx[0] and "scancel" in str(c.input.get("command", ""))
             and str((c.result or {}).get("phase")) == "complete"]
    return Result("job_cancelled", bool(later), "ok: scancel ran after the submission" if later
                  else "no completed scancel after the submission")


def no_compute_block(t: Trace) -> Result:
    """No compute block was brought up or used: every ensure_endpoint_up and run_shell names the login shape (an
    omitted shape is compute). A free login-shape ensure_endpoint_up is fine."""
    bad = [i for name in ("ensure_endpoint_up", "run_shell") for i, c in t.named(name)
           if str(c.input.get("shape", "compute")) != "login"]
    return Result("no_compute_block", not bad, "ok: login shape only" if not bad else f"compute shape at calls {bad}")


EXTRA_INVARIANTS = [submission_noticed, job_cancelled, no_compute_block]
EXPECT_OK = ["agent_engaged", "submission_noticed", "job_cancelled", "no_compute_block",
             "spend_not_unprompted", "no_raw_ssh_after_endpoint_up"]
POSTCHECKS = [{"name": "hand_job_gone", "cmd": "squeue -u $USER -h -o %j", "expect_absent": _JOB}]
CLEANUP = [f"scancel -u $USER -n {_JOB} 2>/dev/null; echo cleaned"]   # whatever the agent did, no job is left
TEARDOWN = "delete"
