"""batch_asks_user_for_size — an AGENT cell (batch row sizing): the user gives no size; run_batch with no size and no
block answers `needs_sizing` with the node's facts; the agent must ASK the user rather than guess, then run the batch
at the size the user gives, on the user's yes. Fake cluster, `default` profile; any agent operator (a seeded chain —
agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_asks_user_for_size

THE USER: the same 6 tasks as batch_sized_by_user, no size; "ask me if you need anything; tell me what it would start
and wait for my yes". The scripted user gives the size when asked (1 CPU core, half a GB, 3 at once — also in answer
to an offer to measure one first), says yes to a spend question, and gives the go-ahead once.

GRADED (critical): `agent_turn_engaged`; in the turn hpc-bridge answered needs_sizing the agent named no size of its
own, started nothing, sent the same unsized call at most twice, and the user's next answer was the sizes
(`asked_for_sizes` — no needs_sizing at all is a premise not met, never a pass); the recorded batch carried the user's
size (`user_sizes_used`); no block before a yes (`yes_before_billed_block`); all rows done (`batch_done`); stop answers
down. World: the derived block in sacct (`world:derived_block_allocated`), one block job. Reported: the block's CPUs
named before the yes (`terms_shown_before_yes`), the digests relayed (`results_relayed`).
"""
from __future__ import annotations

import agent_cells as ac
import agent_checks as ak
import batch_checks as bc
import sizing_checks as sc
from invariants import Result, Trace

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
OPERATORS = ("claude", "pi", "codex", "hermes21")
KIND = "regression"
SUMMARY = "agent + sizing: run_batch answers needs_sizing, the agent asks the user, then runs at the user's size"
TAGS = ["fake", "batch", "sizing", "spend", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

WORDS, TOKENS = ac.WORDS, ac.word_digests(ac.WORDS)
CORES, MEMORY_GB, MAX_CONCURRENCY = 1, 0.5, 3
SIZES = "Each task needs 1 CPU core and about half a GB of memory, and it's fine to run 3 at the same time."

TASK = (ac.CONNECTED + "\n\n"
        f"I have 6 small, independent tasks to run on a compute node: for each word in {ac.WORDS_SAID}, run\n\n"
        f"    {ac.WORD_TASK}\n\n"
        "Each takes about 5 seconds. Ask me if you need anything from me. Before you start anything that bills my "
        "allocation, tell me exactly what it would start and wait for my yes. When they are done, tell me each word's "
        "output, then stop the endpoint.")
SIM = ac.UserSim(sizes=SIZES, pilot_reply="No need to measure one first: " + SIZES[0].lower() + SIZES[1:])
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def asked_for_sizes(t: Trace) -> Result:
    return ak.asked_for_sizes(t, SIM)


def user_sizes_used(t: Trace) -> Result:
    return ak.sizes_as_given(t, cores=CORES, memory_gb=MEMORY_GB, max_concurrency=MAX_CONCURRENCY,
                             name="user_sizes_used")


def yes_before_billed_block(t: Trace) -> Result:
    return ak.yes_before_billed_block(t, SIM)


def terms_shown_before_yes(t: Trace) -> Result:
    return ak.terms_shown_before_yes(t, SIM)


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=len(WORDS))


def results_relayed(t: Trace) -> Result:
    return ak.results_relayed(t, tokens=TOKENS)


EXTRA_INVARIANTS = [agent_turn_engaged, asked_for_sizes, user_sizes_used, yes_before_billed_block, batch_done,
                    terms_shown_before_yes, results_relayed, bc.stop_down]
WORLD_CHECKS = [ak.derived_block_as_confirmed, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "asked_for_sizes", "user_sizes_used", "yes_before_billed_block",
             "batch_done", "stop_down", "ends_with_stop", "stop_is_honest"]
