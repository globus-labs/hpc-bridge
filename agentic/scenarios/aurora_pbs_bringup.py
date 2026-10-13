"""MANUAL ONLY — retired from every sweep (2026-10-13): the Aurora allocation expired unused (2026-06-03), the recipe
cannot run in the Docker jail (the two-hop MFA below), and its one bundle (2026-07-16) FAILED. What it was for — the
plugin's PBS path end to end — is covered by the fake `pbs` / `polaris` profiles and the Polaris MEP registry entry
(`agentic/registry_reprove.py`). Kept runnable by hand (PRECONDITIONS below) for when an ALCF allocation returns.

LIVE end-to-end bring-up of ALCF **Aurora** — the first **PBS** + **bastion/MFA** facility in the
harness. Exercises the scheduler-neutral compute path (#28) against a real PBS Pro system and, above
all, **validates the `interface=hsn0` guess**: if the worker registers (the canary goes warm), hsn0 is
the right address_by_interface for the login-node interchange; if it stays `provisioning`, it isn't.

Aurora is un-indexed, so the prompt hands the agent the discovered `FacilityDetails` (see the vault's
Aurora bring-up note) to connect with — the discovery probe's NIC heuristic would mis-guess the
interface, so we supply the known values and let the **login canary validate** them.

PRECONDITIONS (this scenario CANNOT run in the Docker jail — the container can't do the two-hop MFA):
  1. ~/.ssh/config has an `aurora` alias: ProxyJump bastion.alcf.anl.gov, User <you>, and
     `ControlPath ~/.hpc-bridge/cm/%C` (the SERVER's control dir — so its BatchMode calls reuse your
     master) + `ControlMaster auto` + `ControlPersist 1h`. `mkdir -p ~/.hpc-bridge/cm`.
  2. Pre-open the master ONCE (enter both passcodes — bastion + login): `ssh -fN aurora`.
  3. Run LOCALLY (not run_smoke.sh):
        export HPC_BRIDGE_SSH_HOST=aurora HPC_BRIDGE_SSH_USER=<you> \
               HPC_BRIDGE_SSH_KEY=~/.ssh/id_ed25519 \
               HPC_BRIDGE_USER_DIR=~/.globus_compute CLAUDE_CODE_OAUTH_TOKEN=...
        python agentic/harness/run.py aurora_pbs_bringup
     (HPC_BRIDGE_SSH_KEY is required by the harness but UNUSED for Aurora — auth rides the master;
     it's harmless because ControlMaster reuse never re-authenticates.)

See issue #28 (PBS support) and the vault note `Planned/Aurora (PBS + bastion) bring-up.md`.
"""
from invariants import Result, Trace

_MARK = "HPCB_AURORA"

# The discovered Aurora config (live probe, 2026-07): hsn0 on the Slingshot fabric; the debug queue
# (1-2 nodes, 1 hr); the Diaspora project (writable /lus/flare); python/3.12.12 module + an idempotent
# venv (the login node reaches PyPI, the compute node reuses the shared /home venv); and the MANDATORY
# `-l filesystems=home:flare` (home for the venv, flare for scratch — omit either and PBS holds the job).
PROMPT = (
    "Bring up a compute node on ALCF **Aurora** (login host alias `aurora`, facility id `{facility}`) "
    "and run one command on it. Aurora is NOT in the catalog, so connect with these discovered details "
    "via `connect_facility(facility='{facility}', details=…)`:\n"
    "  - ssh_host: aurora\n"
    "  - scheduler: pbs\n"
    "  - interface: hsn0\n"
    "  - partition: debug\n"
    "  - scratch_root: /lus/flare/projects/Diaspora/{user}/.hpc-bridge\n"
    "  - cpus_per_node: 104\n"
    "  - walltime: 00:15:00\n"
    "  - scheduler_options: #PBS -l filesystems=home:flare\n"
    "  - env_setup: module load python/3.12.12 && (test -d {venv} || python3 -m venv {venv}) && "
    "source {venv}/bin/activate && (command -v globus-compute-endpoint >/dev/null 2>&1 || "
    "pip install -q globus-compute-endpoint)\n\n"
    "Then: provision the billed compute block on the `debug` queue with account `Diaspora` "
    "(`ensure_endpoint_up(shape='compute', account='Diaspora', partition='debug', confirm_spend=True)`); "
    "this is PBS, so watch the pilot with `run_shell('qstat -u $USER', shape='login')` (NOT squeue) "
    "until it runs and the worker warms. Once warm, run EXACTLY this one command on the **compute** "
    "shape:\n\n"
    "    echo " + _MARK + " on $(hostname)\n\n"
    "Report the result (phase, exit_code, stdout), then `stop_endpoint` to release the block.\n\n"
    "This is an AUTOMATED run with no human: accept the details above yourself and you are authorised "
    "to confirm the spend on the debug queue."
)

USER_GOAL = ""
PERSONA = None
KIND = "integration"
SUMMARY = "#28 live: ALCF Aurora (PBS + bastion) — connect → debug block → run on compute → stop; validates interface=hsn0"
TAGS = ["aurora", "pbs", "bastion", "mfa", "integration", "issue-28"]
FACILITY_ID = "aurora"   # stable id: reuses the local facilities.json cache; {facility} -> "aurora"
SCHEDULER = "pbs"        # tells the harness world channel to use qstat/qdel, not squeue/scancel
MAX_TURNS = 80           # two-hop bootstrap + module/pip + PBS queue wait is heavier than globus1


def compute_block_warmed(t: Trace) -> Result:
    """THE key assertion: a PBS `debug` block came up and a worker REGISTERED (the canary warmed) —
    which is exactly what validates `interface=hsn0`. A wrong interface leaves it stuck `provisioning`."""
    warm = [
        i for i, c in t.named("ensure_endpoint_up")
        if str((c.result or {}).get("status")) == "up"
        and str((c.result or {}).get("block_state")) == "warm"
    ]
    return Result(
        "compute_block_warmed", bool(warm),
        f"ok: a PBS compute block warmed — worker registered, so interface=hsn0 is valid (calls {warm})"
        if warm else
        "no ensure_endpoint_up returned status=up/block=warm — the PBS block never warmed "
        "(interface guess wrong? job held on filesystems? long debug-queue wait past MAX_TURNS?)",
    )


def ran_on_compute(t: Trace) -> Result:
    """A command actually executed on the compute block and returned the marker — end-to-end PBS."""
    runs = [
        (i, c) for i, c in t.named("run_shell")
        if c.input.get("shape") in (None, "compute") and _MARK in str(c.input.get("command", ""))
    ]
    done = [
        i for i, c in runs
        if str((c.result or {}).get("phase")) == "complete" and _MARK in str((c.result or {}).get("stdout", ""))
    ]
    return Result(
        "ran_on_compute", bool(done),
        f"ok: the marker command completed on the Aurora compute block (calls {done})" if done else
        "no completed compute run carrying the marker — work never ran on the PBS block",
    )


EXTRA_INVARIANTS = [compute_block_warmed, ran_on_compute]

EXPECT_OK = [
    "compute_block_warmed",           # the point: PBS block warmed → interface=hsn0 validated
    "ran_on_compute",                 # work executed end-to-end on Aurora
    "no_raw_ssh_after_endpoint_up",   # stayed on the AMQP hot path (no fresh SSH mid-run)
    "ends_with_stop",                 # released the billed block after
    "stop_is_honest",                 # #24: qdel over the login shape — no false 'down'
    "stop_confirmed_or_retried",      # re-stop on draining until down (Aurora has a login shape,
                                      # so draining is NOT terminal here — unlike a facility MEP)
]

POSTCHECK_DELAY_S = 15
TEARDOWN = "delete"
