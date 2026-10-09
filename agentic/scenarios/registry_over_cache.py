"""The registry wins over a stale local cache (decision 2026-09-03). The jail's `facilities.json` is
pre-seeded with an SSH-era BYO config for `globus-labs` — exactly what the maintainer's own cache held —
and the registry serves `globus-labs` as a facility MEP. `connect_facility("globus-labs")` must ATTACH to the
MEP (reused=True, zero SSH, no probe) and never take the cached SSH path. Attach only: no block, no
spend. Cheap.

Knob: SEED_FACILITY_CACHE — run.py writes it into the server's ~/.hpc-bridge/facilities.json before
the agent starts.
"""
from invariants import calls_bounded, no_ssh_workaround, texts_mention
from mep_compute_only import mep_zero_ssh

FACILITY_ID = "globus-labs"
SEED_FACILITY_CACHE = {
    "globus-labs": {   # a valid, STALE SSH-era config for the same id the registry now serves as a MEP
        "ssh_host": "globus1.cs.uchicago.edu",
        "interface": "enP7s7",
        "env_setup": "uv pip install -q globus-compute-endpoint==4.15.0",
        "scratch_root": "/home/{user}/.hpc-bridge",
        "partition": "main",
        "scheduler": "slurm",
    }
}
PROMPT = (
    "Connect me to the HPC facility `globus-labs` (catalogued: connect_facility(facility='globus-labs') — no "
    "ssh_host, no details) and tell me exactly how it was reached: attached to a facility-run "
    "multi-user endpoint, or bootstrapped over SSH? Do NOT provision or run anything. This is an "
    "automated run with no human present."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "a stale cached SSH config for a catalogued id must NOT shadow the registry's MEP entry: attach, zero SSH"
TAGS = ["registry", "cache", "mep", "zero-ssh", "cheap"]
MAX_TURNS = 10

reach_reported = texts_mention("reach_reported", any_of=("multi-user", "multi user", "zero ssh", "zero-ssh", "attached",
                                                                   "facility-run"))
EXTRA_INVARIANTS = [mep_zero_ssh, calls_bounded("ensure_endpoint_up", 0), no_ssh_workaround, reach_reported]
EXPECT_OK = ["agent_engaged", "mep_zero_ssh", "ensure_endpoint_up_calls_le_0", "no_ssh_workaround", "reach_reported"]
TEARDOWN = "delete"
