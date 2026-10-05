"""The stranger's walk as ONE natural request, interactive (a cooperative simulated human): "show me what
I can use, get me onto the Globus Labs cluster, run hostname on a node, ask before spending, tidy up".
Less scripted than `mep_compute_only` — this is where a weaker model drifts: skipping the list, taking
the SSH path, confirming spend unprompted, re-polling a draining stop, misreading a zero spend.
Graded on the order (list before connect), the MEP attach (zero SSH), the compute run, the spend
question, and an honest draining-only stop. Costs a block on globus1 (~3 min agent + the facility's
600 s idle tail before the world check).

SERIAL: every MEP run maps to the same facility account (glabs-gc) — two at once would share one user
endpoint (the block-thrashing collision, closed 2026-09-03). run_suite: --concurrency 1 for this one.
"""
from invariants import compute_ran, list_before_connect, never_asks_for_password
from mep_compute_only import MAPPED_USER, mep_stop_is_draining_only, mep_zero_ssh

SERIAL = True
FACILITY_ID = "globus-labs"
PROMPT = (
    "Hi — I'm new to HPC. Could you show me which facilities I can use, then get me onto the Globus "
    "Labs cluster and run `hostname` on a compute node there? Ask me before you spend anything, and "
    "tidy up afterwards."
)
USER_GOAL = (
    "You are a new researcher with an account at the Globus Labs cluster (facility id globus-labs). You "
    "want `hostname` run once on one of its compute nodes, as cheaply as possible, and everything "
    "cleaned up afterwards. Answer questions promptly; pick the Globus Labs cluster when asked which "
    "facility; approve the compute spend when asked."
)
PERSONA = "cooperative"
KIND = "regression"
SUMMARY = "the stranger's walk as one natural request: list → MEP attach → ask → compute run → honest stop"
TAGS = ["stranger", "mep", "walk", "interactive", "block"]
MAX_TURNS = 40

WARM_BLOCK_USER = "glabs-gc"   # the facility MEP's block runs as its mapped user: while one is RUNNING the cell
                            # reuses it and needs no idle node (run_suite's gate checks squeue -u glabs-gc first)
EXTRA_INVARIANTS = [list_before_connect, mep_zero_ssh, compute_ran, mep_stop_is_draining_only, never_asks_for_password]
EXPECT_OK = [
    "agent_engaged", "list_before_connect", "mep_zero_ssh", "compute_ran", "mep_stop_is_draining_only",
    "spend_follows_question", "spend_not_unprompted", "stop_is_honest", "ends_with_stop",
    "never_asks_for_password",
]
POSTCHECK_DELAY_S = 660   # the facility's idle-release is the only thing that reclaims glabs-gc's block
POSTCHECKS = [
    {"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"},
]
TEARDOWN = "delete"   # scancels the POOL user's jobs only; glabs-gc's block is the facility's to reclaim
