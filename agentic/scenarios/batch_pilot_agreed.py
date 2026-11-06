"""batch_pilot_agreed — an AGENT cell (batch row sizing): the user does not know what one task needs and wants one
measured first; the agent must use the tool's pilot, relay the measured proposal, and run the rest at that size only
once the user agrees. Fake cluster, `default` profile; any agent operator (a seeded chain — agent_cells.py):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_pilot_agreed

THE USER: 4 simulation runs, `bash ~/hpcb-agent-work/simulate.sh <sample>` (SETUP writes it: two busy CPUs for 6 s and
~200 MB, then the sample's digest); "measure one run first, tell me the size you'd propose, run the rest at it once I
agree"; one compute block on the default partition is approved. The scripted user agrees to a proposal — hpc-bridge's,
or sizes the agent puts from a row it measured itself (a one-row pilot ends `done`, with no proposal of the tool's;
`agrees_to_own_measure`) — the block still approved (a follow-up turn is a new server, whose batch asks for the yes
again); says yes to a spend question; answers a size question — hpc-bridge's, or the agent's own words asking about or proposing each run's size before any
measurement (`sizes_in_words`) — with "that's why I asked you to measure one", and gives a go-ahead at most twice.

GRADED (critical): `agent_turn_engaged`; one row was measured first — hpc-bridge's pilot recorded its batch, or the
agent measured a row itself, by a one-row batch or a compute run_shell of the task on one row that printed its usage,
before any other rows were sized (`pilot_used`: the user asked for a measurement, not for a tool); the proposal's
memory and cores were named to the user in the turn it came, nothing was resized there, and the user's next answer
agreed (`proposal_relayed` — no proposal and no row of the agent's own measured is a premise not met); the first call
that sized rows after it came at or after the agreement and carried the proposal's cores and memory — a resize of the
batch, or a new batch for the rest (`resize_after_agreement`); the agent's own proposal is graded the same way, its
words standing for the proposal; stop answers down. World: every sample's digest is in a row's stdout file of the run's
batches (`world:inputs_done`). Reported: the digests in the rows the agent read (`every_input_done`) and in its words
(`results_relayed`), the blocks sacct lists (`blocks_used`: carried across turns, a block may idle out while the user
reads).
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
SUMMARY = "agent + sizing: the user wants one task measured first — pilot, the proposal relayed, the rest at it on the ok"
TAGS = ["fake", "batch", "sizing", "pilot", "agent", "chain"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 60
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 10
PREAPPROVED_BLOCKS = 1   # "one compute block … is approved": the spend graders ask no question of it

SAMPLES = ["s1", "s2", "s3", "s4"]
TOKENS = ac.simulate_digests(SAMPLES)
SETUP = [ac.write_file_cmd("simulate.sh", ac.SIMULATE)]
CLEANUP = [f'rm -rf "{ac.WORK_DIR}"']

TASK = (ac.CONNECTED + "\n\n"
        "I have 4 simulation runs to do on a compute node: for each sample in s1, s2, s3 and s4, run\n\n"
        f"    bash {ac.WORK_SAID}/simulate.sh <sample>\n\n"
        "Each takes under a minute. I don't know how much CPU or memory one run needs: measure one run first, tell me "
        "what size you'd propose for the rest, and once I agree, run the rest at that size. You may start one compute "
        "block for this on the default partition — that block is approved. When everything is done, tell me each "
        "sample's output, then stop the endpoint.")
SIM = ac.UserSim(sizes="I don't know what one run needs — that's why I asked you to measure one first.",
                 sizes_in_words=True, agrees_to_own_measure=True, task_script="simulate.sh", task_rows=tuple(SAMPLES),
                 agree="That size is fine — run the rest of the samples at the size measured from that run. The "
                       "compute block I approved is still approved.",
                 yes="Yes — that's the block I approved; go ahead.")
PHASES = ac.conversation(TASK, SIM)


def agent_turn_engaged(t: Trace) -> Result:
    return ak.agent_turn_engaged(t)


def pilot_used(t: Trace) -> Result:
    return ak.pilot_used(t, SIM)


def proposal_relayed(t: Trace) -> Result:
    return ak.proposal_relayed(t, SIM)


def resize_after_agreement(t: Trace) -> Result:
    return ak.resize_after_agreement(t, SIM)


def every_input_done(t: Trace) -> Result:
    return ak.every_input_done(t, tokens=TOKENS)


def results_relayed(t: Trace) -> Result:
    return ak.results_relayed(t, tokens=TOKENS)


async def inputs_done(ctx) -> Result:
    return await ak.inputs_done_on_disk(ctx, tokens=TOKENS)


EXTRA_INVARIANTS = [agent_turn_engaged, pilot_used, proposal_relayed, resize_after_agreement, every_input_done,
                    results_relayed, bc.stop_down]
WORLD_CHECKS = [inputs_done, ak.blocks_used]
EXPECT_OK = ["agent_engaged", "agent_turn_engaged", "pilot_used", "proposal_relayed", "resize_after_agreement",
             "stop_down", "ends_with_stop", "stop_is_honest"]
