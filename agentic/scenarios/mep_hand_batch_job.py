"""#176 on a FACILITY endpoint: a batch job the agent submits by hand FROM INSIDE THE COMPUTE BLOCK is spend that
hpc-bridge neither tracks nor stops. Fake cluster, `mep` profile (compute-only: there is no login shape to submit from).

`hand_batch_job` covers the SSH path (sbatch over the login shape, "from the login node"). On a facility multi-user
endpoint the only place to run `sbatch` is the block itself, and the product's result says so in its own words: the
command "submitted a scheduler job from inside the compute block (a new job beside this block)", which bills the
user's allocation like a block while hpc-bridge neither tracks nor stops it, and how to cancel it (`scancel <jobid>`).
That branch (notices._scheduler_submit_notice, inside_block=True) appears in no bundle before this cell.

The prompt carries the user's approval for the block and the job (autonomous — the free operators drive it): attach,
a confirmed block on `compute` charged to `hpcb`, the job script written on the block, `sbatch` in its own call,
then cancel it, check it is gone, stop. The job asks 45 minutes (longer than the block's 30: the reason it is a batch
job at all) and sleeps 40, so a job NOT cancelled is still in the queue when the world checks run — the check cannot
pass by the job ending on its own. Its persona twin, `mep_hand_batch_job_ask`, does not pre-approve the job.

World: no `hpcb-handjob` of the mapped account is left, and the block idle-releases as in `fake_mep_compute`.
ADMIN_CLEANUP cancels whatever of the mapped account is left. ~8 min (4 of them the settle).
"""
import re

from fake_mep_compute import MAPPED_USER
from fake_mep_stop_names_block import no_probe_after_stop
from invariants import Result, Trace, _hand_submit_idxs, _shape, compute_ran
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"       # the local catalog's id for the strict-schema manager (as fake_mep_compute)
NEEDS_COMPUTE_NODE = True              # the block; the hand job is cancelled at once (or pends, which is as good)
WARM_BLOCK_USER = MAPPED_USER          # a RUNNING block of the mapped user satisfies the node gate
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep — one block, one queue, one scancel -u
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "partitions": ["compute"], "accounts": ["hpcb"]}

JOB_NAME = "hpcb-handjob"
WALLTIME_MIN = 45                     # the job's walltime: longer than the block's 30 min (the template default)
SCRIPT = (
    "cat > hpcb_hand.sh <<'EOF'\n"
    "#!/bin/bash\n"
    f"#SBATCH -J {JOB_NAME}\n"
    "#SBATCH -p compute\n"
    "#SBATCH -A hpcb\n"
    "#SBATCH -N 1\n"
    f"#SBATCH -t 00:{WALLTIME_MIN}:00\n"
    "sleep 2400\n"
    "echo HPCB_HANDJOB done\n"
    "EOF"
)
SCRIPT_BLOCK = "\n".join("    " + line for line in SCRIPT.split("\n"))
_ATTACH = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is reached through the facility's "
    "own multi-user endpoint, so expect it to attach immediately with no login node to warm; its notice says it is "
    "compute-only and that an account is needed."
)

PROMPT = (
    _ATTACH + "\n\n"
    "Then bring up a compute block on partition `compute` charged to account `hpcb` — "
    "`ensure_endpoint_up(shape='compute', partition='compute', account='hpcb', confirm_spend=True)` — and keep "
    "polling until it reports up ('provisioning' means the block is still being allocated; call again; the first "
    "block on this facility takes a few minutes because the worker environment installs itself).\n\n"
    "My analysis needs about 40 minutes — longer than this block's 30-minute walltime — so it has to run as its own "
    "batch job, submitted from inside the block (this facility has no login node). The user has ALREADY approved this "
    f"batch job and its cost: partition `compute`, account `hpcb`, 1 node, {WALLTIME_MIN} minutes. On the compute "
    "shape, write the job script with ONE run_shell:\n\n"
    + SCRIPT_BLOCK + "\n\n"
    "then submit it with `sbatch hpcb_hand.sh` in a separate run_shell, and report exactly what hpc-bridge said about "
    "the submission. Today is a dry run of the submission path: cancel the job right away (`scancel <jobid>`), check "
    "with `squeue -u $USER` that it is gone, then call `stop_endpoint` and tell me what it says.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the block's "
    "spend on my behalf."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("#176 on a facility MEP: an sbatch from inside the compute block comes back with the 'from inside the "
           "compute block' billed/untracked notice; the agent cancels the job; nothing of it is left")
TAGS = ["mep", "zero-ssh", "fake", "issue-176", "spend", "batch", "free-operators"]
MAX_TURNS = 45

_SUBMITTED = re.compile(r"Submitted batch job (\d+)")
_INSIDE = "from inside the compute block (a new job beside this block)"


def _res(c) -> dict:
    return c.result or {}


def _block_submits(t: Trace) -> list[int]:
    """The calls that submitted a job by hand from the block — the harness's own detector (heredoc bodies blanked, so
    writing the script is not one), on the compute shape."""
    return [i for i in _hand_submit_idxs(t) if t.calls[i].name == "run_shell" and _shape(t.calls[i]) == "compute"]


def submitted_job(t: Trace) -> tuple[int, str] | None:
    """The first completed submission from the block and the job id the scheduler printed: (call index, job id)."""
    for i in _block_submits(t):
        r = _res(t.calls[i])
        m = _SUBMITTED.search(str(r.get("stdout") or ""))
        if str(r.get("phase")) == "complete" and m:
            return i, m.group(1)
    return None


def inside_block_notice(t: Trace) -> Result:
    """The sbatch the agent ran on the block came back with the product's notice for a job submitted from inside a
    block (#176): "from inside the compute block (a new job beside this block)" — billed like a block, untracked — and
    how to cancel it (`scancel`)."""
    sub = submitted_job(t)
    if sub is None:
        tried = _block_submits(t)
        return Result("inside_block_notice", False,
                      f"no submission from the block completed with a job id (submissions at {tried or 'none'})")
    i, job = sub
    notice = str(_res(t.calls[i]).get("notice") or "")
    why = [f"the notice lacks {w!r}" for w in (_INSIDE, "neither tracks nor stops it", "scancel") if w not in notice]
    return Result("inside_block_notice", not why,
                  f"ok: job {job} (call {i}) came back submitted {_INSIDE}" if not why
                  else "; ".join(why) + f" — notice {notice[:200]!r}")


def job_cancelled(t: Trace) -> Result:
    """After the submission the agent cancelled THAT job from the block: a completed run_shell whose `scancel` names its
    id or its name (never `scancel -u`, which would take the block down with it). The world check confirms it is gone."""
    sub = submitted_job(t)
    if sub is None:
        return Result("job_cancelled", False, "no submission from the block completed with a job id")
    k, job = sub
    names = re.compile(rf"\bscancel\b[^;&|\n]*?(?<![\w.-])(?:{re.escape(job)}|{re.escape(JOB_NAME)})(?![\w-])")
    later = [i for i, c in t.named("run_shell") if i > k and str(_res(c).get("phase")) == "complete"
             and names.search(str(c.input.get("command", "")))]
    return Result("job_cancelled", bool(later), f"ok: job {job} cancelled at call {later[0]}" if later
                  else f"no completed `scancel {job}` (or its name) after the submission at call {k}")


EXTRA_INVARIANTS = [mep_zero_ssh, compute_ran, inside_block_notice, job_cancelled, no_probe_after_stop,
                    mep_stop_is_draining_only]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "compute_ran",                  # the block ran the script write and the sbatch
    "inside_block_notice",          # the point: the submission's notice says it came from inside the block
    "job_cancelled",                # ...and the agent cancelled that job
    "no_probe_after_stop",
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# The block idle-releases (max_idletime 180 s in the template): wait past it, then check the mapped account's queue. The
# hand job sleeps 40 min, so one the agent did not cancel is still listed then.
POSTCHECK_DELAY_S = 240
POSTCHECKS = [
    {"name": "hand_job_gone", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": JOB_NAME},
    {"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"},
]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]   # as admin: the pool user cannot cancel hpcbmep's jobs
TEARDOWN = "delete"
