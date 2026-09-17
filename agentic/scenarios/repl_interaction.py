"""REPL-like interaction on hpc-bridge's COMPUTE shape — Tier A of vault `Planned/REPL-like interaction benchmark.md`.

The talk's claim is "a batch supercomputer as an agent's REPL". This scenario tests it against a definition that does
not mention hpc-bridge: a scripted ten-step protocol (repl_protocol.py) run as separate `run_shell` calls in one
session on a warm compute block, graded for direct results (P1), state carried across calls — files, working
directory AND environment (P3), fail-soft errors (P4) and no per-turn ceremony (P5). Turn latency (P2) is measured from
the runner's arrival stamps and reported, not gated, until there is data; compare it with `repl_baseline_local`, the
same protocol on the agent's own Bash tool, via `agentic/repl_report.py`.

Warm-up is deliberately outside the protocol: the prompt has the agent wait for `up` first, so the protocol measures
the loop, not provisioning. Autonomous (no persona): the spend is pre-authorised in the prompt.
"""
from invariants import compute_ran
from repl_protocol import GATED, graders, protocol_prompt

NEEDS_COMPUTE_NODE = True

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — I want to measure whether a compute "
    "node behaves like an interactive shell. This is an AUTOMATED run with no human present: accept the discovered "
    "facility config yourself, and you are authorised to confirm the spend on my behalf.\n\n"
    "First, bring up ONE compute node (the cheapest sensible partition) and wait until "
    "`ensure_endpoint_up(shape='compute')` reports `up`. Do not start the protocol below until it does.\n\n"
    + protocol_prompt("bridge")
    + "\n\nWhen the last step has returned, stop the endpoint, then report what each step printed."
)

USER_GOAL = ""
PERSONA = None
KIND = "diagnostic"
SUMMARY = "REPL-like interaction on a warm compute block: direct results, state, fail-soft, no ceremony; turn latency reported"
TAGS = ["repl", "benchmark", "compute", "session"]
MAX_TURNS = 60

EXTRA_INVARIANTS = [*graders("bridge"), compute_ran]

EXPECT_OK = [
    *GATED["bridge"],                 # P1, P3 (files · cwd · env), P4, P5 — repl_latency (P2) is report-only for now
    "compute_ran",
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
]

TEARDOWN = "delete"
