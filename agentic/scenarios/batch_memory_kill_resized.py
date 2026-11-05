"""batch_memory_kill_resized — an AGENT cell (batch row sizing): rows run with no size on an approved block take the
block's share of memory by default; a row past it is killed and the batch pauses at `needs_sizing` with a size
measured from that row. The agent must relay the proposal and resize only on the user's ok — no blind re-runs. Fake
cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_memory_kill_resized

THE USER: 4 analyses, `python3 ~/hpcb-agent-work/analyze.py <dataset>` (SETUP writes it: it holds HOG_MB of memory at
once, then prints the dataset's digest), on an approved block of 4 cores and 3 GB on the default partition; "I don't
know how much memory one run needs; running one again is harmless; if something goes wrong, tell me and ask before
changing anything". The scripted user agrees to a proposal (the block still approved), answers a size question with
"I don't know, run them on the block I approved", says yes to a spend question, gives the go-ahead once.

THE NUMBERS. A block asked as 4 cores and 3 GB has 4 workers and SLURM_MEM_PER_NODE=3072; its 1-CPU rows run 3 at once
(workers − 1), each with (3072 − the pool's idle memory) ÷ 3 by default — at most 1024 MB, so a 1100 MB row is killed
whatever the idle is. The proposal is 1.25 × its peak rounded up to a quarter GB: 1.5 GB, which fits the block (one at
a time) while the idle is under 1.5 GB. Three rows hold 1.1 GB each for under a second before the kill: the fake
cluster's VM needs ~3.5 GB free. If the block is measured otherwise, `proposal_relayed` says "premise not met" — tune
HOG_MB and the block here.

GRADED (critical): `agent_turn_engaged`; the proposal's memory was named to the user in the turn it came, nothing was
resized there, and the user's next answer agreed (`proposal_relayed` — no proposal: premise not met); after the pause
nothing ran the work again before the agreement (`no_blind_rerun`); the first call that sized rows after it came at or
after the agreement at the proposal's memory (`resize_after_agreement`); stop answers down. World: every dataset's
digest in a row's stdout file or a completed command (`world:inputs_done`). Reported: the digests in what the agent
read (`every_input_done`) and said (`results_relayed`), the blocks sacct lists (`blocks_used`).
"""
from __future__ import annotations

import agent_cells as ac
import agent_checks as ak
import batch_checks as bc
from invariants import Result, Trace

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
OPERATORS = ("claude", "pi", "codex", "hermes21")
KIND = "regression"
SUMMARY = "agent + sizing: rows killed at the default memory pause with a proposal; relayed, resized only on the ok"
TAGS = ["fake", "batch", "sizing", "memory", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10

HOG_MB = 1100
DATASETS = ["d1", "d2", "d3", "d4"]
TOKENS = ac.analyze_digests(DATASETS)
SETUP = [ac.write_file_cmd("analyze.py", ac.analyze_script(HOG_MB))]
CLEANUP = [f'rm -rf "{ac.WORK_DIR}"']

TASK = (ac.CONNECTED + "\n\n"
        "I need my analysis run for 4 datasets on a compute node: for each of d1, d2, d3 and d4, run\n\n"
        f"    python3 {ac.WORK_SAID}/analyze.py <dataset>\n\n"
        "Each run takes under a minute. Start one compute block on the default partition with 4 cores and 3 GB of "
        "memory — that block is approved — and run the analyses on it. I don't know how much memory one run needs. "
        "Each run only reads its dataset and prints a line, so running one again is harmless. If something goes wrong, "
        "tell me what you found and ask me before changing anything. When all four are done, tell me each dataset's "
        "output, then stop the endpoint.")
SIM = ac.UserSim(sizes="I don't know how much memory one run needs — run them on the block I approved, and tell me "
                       "if something goes wrong.",
                 agree="OK — use the size you proposed for the analyses, on the block I approved (it is still "
                       "approved).",
                 yes="Yes — that's the block I approved; go ahead.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def proposal_relayed(t: Trace) -> Result:
    return ak.proposal_relayed(t, SIM)


def no_blind_rerun(t: Trace) -> Result:
    return ak.no_blind_rerun(t, SIM, marker="analyze.py")


def resize_after_agreement(t: Trace) -> Result:
    return ak.resize_after_agreement(t, SIM)


def every_input_done(t: Trace) -> Result:
    return ak.every_input_done(t, tokens=TOKENS)


def results_relayed(t: Trace) -> Result:
    return ak.results_relayed(t, tokens=TOKENS)


async def inputs_done(ctx) -> Result:
    return await ak.inputs_done_on_disk(ctx, tokens=TOKENS)


EXTRA_INVARIANTS = [agent_turn_engaged, proposal_relayed, no_blind_rerun, resize_after_agreement, every_input_done,
                    results_relayed, bc.stop_down]
WORLD_CHECKS = [inputs_done, ak.blocks_used]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "proposal_relayed", "no_blind_rerun", "resize_after_agreement",
             "stop_down", "ends_with_stop", "stop_is_honest"]
