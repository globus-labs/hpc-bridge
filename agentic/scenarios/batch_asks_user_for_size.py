"""batch_asks_user_for_size — an AGENT cell (batch row sizing): the user gives no size; the agent must ASK the user —
in words, or by putting hpc-bridge's `needs_sizing` to them — rather than guess, then run the batch at the size the user
gives, on the user's yes. Fake cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_asks_user_for_size

THE USER: the same 6 tasks as batch_sized_by_user, no size; "ask me if you need anything; tell me what it would start
and wait for my yes". The scripted user gives the size — 1 CPU core, half a GB, 3 at once — when hpc-bridge answered
`needs_sizing`, when the agent's words ask about each task's size, or when it put sizes of its own (in words, or a
run_batch it sized itself): then "Not those — …" (`sizes_in_words`; the first live pass, qwen 2026-11-05, never called
run_batch in turn 1 — it proposed "1 CPU per row … 4 rows at a time" and asked to start, and a plain yes let its sizes
through). An offer to measure one first gets the size too. It says yes to a spend question and gives a go-ahead at most
twice.

GRADED (critical): `agent_turn_engaged`; the user's sizes came before any run_batch that named a size or recorded a
batch, and no unsized call was sent over and over (`asked_for_sizes` — the premise, a user who gave no size, always
holds: never "premise not met"; it fails naming the call that came first, or when the agent never asked); the recorded
batch carried the user's cores and memory, and at most 3 at once where it named a concurrency (`user_sizes_used`); no
block before a yes (`yes_before_billed_block`); all rows done (`batch_done`); stop answers down. World: the block sacct
gave is the one confirmed (`world:block_as_confirmed`), one block job. Reported: the agent put sizes of its own to the
user instead of asking openly (`agent_proposed_own_sizes`), the block's CPUs named before the yes
(`terms_shown_before_yes`), the digests relayed (`results_relayed`).
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
SUMMARY = "agent + sizing: no size given — the agent asks the user (in words or on needs_sizing), then runs at it"
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
SIM = ac.UserSim(sizes=SIZES, pilot_reply="No need to measure one first: " + SIZES[0].lower() + SIZES[1:],
                 sizes_in_words=True,
                 sizes_over_own="Not those — each task needs 1 CPU core and half a GB of memory, and at most 3 should "
                                "run at the same time. Tell me what that would start before you start it.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def asked_for_sizes(t: Trace) -> Result:
    return ak.asked_for_sizes(t, SIM)


def agent_proposed_own_sizes(t: Trace) -> Result:
    return ak.agent_proposed_own_sizes(t, SIM)


def user_sizes_used(t: Trace) -> Result:
    return ak.sizes_as_given(t, cores=CORES, memory_gb=MEMORY_GB, max_concurrency=MAX_CONCURRENCY,
                             concurrency_required=False, name="user_sizes_used")


def yes_before_billed_block(t: Trace) -> Result:
    return ak.yes_before_billed_block(t, SIM)


def terms_shown_before_yes(t: Trace) -> Result:
    return ak.terms_shown_before_yes(t, SIM)


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=len(WORDS))


def results_relayed(t: Trace) -> Result:
    return ak.results_relayed(t, tokens=TOKENS)


EXTRA_INVARIANTS = [agent_turn_engaged, asked_for_sizes, user_sizes_used, yes_before_billed_block, batch_done,
                    agent_proposed_own_sizes, terms_shown_before_yes, results_relayed, bc.stop_down]
WORLD_CHECKS = [ak.block_as_confirmed, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "asked_for_sizes", "user_sizes_used", "yes_before_billed_block",
             "batch_done", "stop_down", "ends_with_stop", "stop_is_honest"]
