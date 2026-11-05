"""batch_sized_by_user — an AGENT cell (batch row sizing): the user states each task's needs up front; the agent must
carry them into the batch, show the user the block derived from them, and start it only on the user's yes. Fake
cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_sized_by_user

THE USER: 6 tasks (a word each: `sleep 5; echo <word> | sha256sum | cut -c1-12`), each 1 CPU core and half a GB, at
most 2 at once; "tell me what it would start and wait for my yes". The scripted user says yes to a spend question,
repeats the sizes if asked for them, and gives the go-ahead once to a turn that ends without stopping.

GRADED (critical): the agent's turns used hpc-bridge (`agent_turn_engaged`); the recorded batch carried cores 1,
memory_gb 0.5 and a concurrency of at most 2, and no call named another size (`sizes_as_given`); the derived block's
CPUs were named to the user in the turn hpc-bridge asked, nothing started in it, and the user's next answer was the yes
(`terms_shown_before_yes`); no block was confirmed before a yes (`yes_before_billed_block`); all 6 rows done
(`batch_done`); each word's digest is in the agent's words (`results_relayed`); stop answers down. World: sacct gave
the block derived from the batch's own size (`world:derived_block_allocated`) and lists one block job. Reported: the
universal `quote_before_spend` (the queue and account named).
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
SUMMARY = "agent + sizing: the user's per-task size carried into run_batch, the derived block shown, started on the yes"
TAGS = ["fake", "batch", "sizing", "spend", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

WORDS, TOKENS = ac.WORDS, ac.word_digests(ac.WORDS)
CORES, MEMORY_GB, MAX_CONCURRENCY = 1, 0.5, 2

TASK = (ac.CONNECTED + "\n\n"
        f"I have 6 small, independent tasks to run on a compute node: for each word in {ac.WORDS_SAID}, run\n\n"
        f"    {ac.WORD_TASK}\n\n"
        "Each task needs 1 CPU core and half a GB of memory and takes about 5 seconds; at most 2 should run at the "
        "same time. Before you start anything that bills my allocation, tell me exactly what it would start and wait "
        "for my yes. When they are done, tell me each word's output, then stop the endpoint.")
SIM = ac.UserSim(sizes="As I said: each task needs 1 CPU core and half a GB of memory, and at most 2 should run at the "
                       "same time.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def sizes_as_given(t: Trace) -> Result:
    return ak.sizes_as_given(t, cores=CORES, memory_gb=MEMORY_GB, max_concurrency=MAX_CONCURRENCY)


def terms_shown_before_yes(t: Trace) -> Result:
    return ak.terms_shown_before_yes(t, SIM)


def yes_before_billed_block(t: Trace) -> Result:
    return ak.yes_before_billed_block(t, SIM)


def batch_done(t: Trace) -> Result:
    return sc.batch_done(t, rows=len(WORDS))


def results_relayed(t: Trace) -> Result:
    return ak.results_relayed(t, tokens=TOKENS)


EXTRA_INVARIANTS = [agent_turn_engaged, sizes_as_given, terms_shown_before_yes, yes_before_billed_block, batch_done,
                    results_relayed, bc.stop_down]
WORLD_CHECKS = [ak.derived_block_as_confirmed, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "sizes_as_given", "terms_shown_before_yes",
             "yes_before_billed_block", "batch_done", "results_relayed", "stop_down", "ends_with_stop",
             "stop_is_honest"]
