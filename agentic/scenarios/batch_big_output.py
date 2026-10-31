"""batch_big_output — a row whose stdout is far over Globus Compute's 10 MB result limit: the wrapper caps each stream
at the source and the result carries a tail, the stored size and a pointer, so the row completes with no
MaxResultSizeExceeded (batch phase 1, design §4 "The wrapper", review finding 10). Fake cluster, `default` profile;
scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_big_output

THE SCRIPT: connect, confirm one block, run_batch 3 rows; row 1 prints 12 MB of `x` then a last line, the others one
line; collect and stop.

GRADED: row 1 is `done`, truncated, stored at less than its full size, with a pointer and a tail, and no answer anywhere
met the result limit (`big_output_truncated`); the file at the pointer holds what the result says
(`world:big_output_on_disk`); every row is settled.

ON MAIN the fallback's row 1 is a run_shell whose result is over the limit: the command ran, and none of its output or
its exit code came back (`dispatch.failure_outcome`'s MaxResultSizeExceeded notice) — `big_output_truncated` fails.
"""
from __future__ import annotations

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: a row printing 12 MB comes back capped with a tail and a pointer, never MaxResultSizeExceeded"
TAGS = ["fake", "batch", "output", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

TASK_WALLTIME_S = 120
BIG_ROW = 1
FULL_BYTES = 12_000_000 + 1 + len("last line\n")
COMMAND = ("if [ {n} = 1 ]; then head -c 12000000 /dev/zero | tr '\\0' x; echo; echo last line; "
           "else echo row {n}; fi")
ROWS = [{"n": i} for i in range(3)]

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "confirm one block, run 3 rows through run_batch (one prints 12 MB), collect them and stop."
)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, main_poll_until_s=300)
    if main is not None:
        big = main.results.get(BIG_ROW) or {}
        ops.say(f"main: row {BIG_ROW} answered {big.get('phase')!r}: {str(big.get('notice'))[:200]!r}")
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused",
                                                                    "needs_confirmation"), timeout_s=600)
    got = (await bc.all_rows(ops, bid, len(ROWS)))["rows"]
    big = got.get(BIG_ROW) or {}
    ops.say(f"row {BIG_ROW}: {big.get('state')!r}, truncated {big.get('truncated')!r}, stored "
            f"{big.get('stdout_bytes')!r} bytes at {big.get('stdout_path')!r}; tail {str(big.get('tail'))[-40:]!r}")
    s = await bc.stop(ops)
    return f"row {BIG_ROW} {big.get('state')!r} (truncated={big.get('truncated')!r}); stop said {s.get('status')!r}"


def big_output_truncated(t: Trace) -> Result:
    return bc.big_output_truncated(t, index=BIG_ROW, full=FULL_BYTES)


def every_row_returned(t: Trace) -> Result:
    return bc.every_row_returned(t, rows=len(ROWS))


async def big_output_on_disk(ctx) -> Result:
    return await bc.big_output_on_disk(ctx, index=BIG_ROW)


EXTRA_INVARIANTS = [big_output_truncated, every_row_returned]
WORLD_CHECKS = [big_output_on_disk]
EXPECT_OK = ["agent_engaged", "big_output_truncated", "every_row_returned", "ends_with_stop"]
