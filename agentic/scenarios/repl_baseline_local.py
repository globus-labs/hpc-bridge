"""The local REFERENCE for the REPL benchmark: the same ten-step protocol on the agent's own Bash tool, no HPC at all.

"Low latency" and "state carries" need a reference, and the fair one is what the same agent, model and harness get
locally. This run anchors `repl_interaction`'s turn latency as a ratio, and records the local channel's own REPL
profile: Claude Code's Bash tool keeps the working directory between calls but NOT environment variables, so
`repl_env_carries` is reported here, not gated. `LOCAL_BASELINE` drops the `agent_engaged` liveness gate (it demands an
hpc-bridge call); `repl_local_only` gates instead — some Bash, and no hpc-bridge call at all. No cluster work.
"""
from repl_protocol import GATED, graders, protocol_prompt

LOCAL_BASELINE = True

PROMPT = (
    "This is a calibration run on THIS machine — no HPC facility is involved. Do NOT call any hpc-bridge tool "
    "(nothing named connect_facility, run_shell, list_facilities, ensure_endpoint_up, …). Use only your own Bash tool.\n\n"
    + protocol_prompt("local")
    + "\n\nWhen the last step has returned, report what each step printed. This is an AUTOMATED run — no one is "
    "available to answer questions."
)

USER_GOAL = ""
PERSONA = None
KIND = "diagnostic"
SUMMARY = "REPL benchmark reference: the same protocol on the agent's local Bash tool (no HPC)"
TAGS = ["repl", "benchmark", "baseline", "cheap"]
MAX_TURNS = 30

EXTRA_INVARIANTS = graders("local")

EXPECT_OK = [*GATED["local"]]   # repl_env_carries + repl_latency reported: the local Bash tool drops env by design

TEARDOWN = "delete"
