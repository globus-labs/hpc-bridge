from __future__ import annotations

import re
from typing import Protocol, TypeVar

from .cost import cut_streams
from .lifecycle import BlockState
from .models import LoginShellResult, ShellOutcome
from .runner import MAX_OUTPUT_CHARS, SNIPPET_LINES


class ShellLike(Protocol):
    returncode: int
    stdout: str
    stderr: str


class Runner(Protocol):
    async def run(self, command: str) -> ShellLike: ...


async def execute(command: str, runner: Runner, *, block_state: BlockState = "warm") -> ShellOutcome:
    """Dispatch a shell command through a Runner and shape the structured result.

    Any dispatch failure (timeout, oversized result, remote task failure, transport
    error) is translated into a structured `failed` ShellOutcome rather than raised,
    so a hung/broken endpoint never crashes the MCP tool or hangs the agent silently.
    """
    try:
        res = await runner.run(command)
    except Exception as exc:  # noqa: BLE001 - deliberately translate ALL failures to an outcome
        return failure_outcome(exc, block_state)
    return complete_outcome(res, block_state)


def complete_outcome(res: ShellLike, block_state: BlockState) -> ShellOutcome:
    """Shape a successful runner result into a `complete` outcome. Shared by execute() and the
    submit/poll path (server._run_shell / poll_task) so the completion mapping lives in one place.
    The streams are the worker's, uncut: hpc-bridge's own consumers (the pilot probe, the allocation
    parsers) read them whole, and `for_agent` cuts what a tool hands the agent."""
    return ShellOutcome(
        phase="complete",
        exit_code=res.returncode,
        stdout=res.stdout,
        stderr_snippet=res.stderr,
        block_state=block_state,
    )


# A result over Globus Compute's 10 MiB limit (the endpoint's engines/helper.py `_RESULT_SIZE_LIMIT`, applied to the
# SERIALIZED result) is reported as `repr(MaxResultSizeExceeded(size, limit))` and reaches the client wrapped in a
# TaskExecutionFailed — the SDK never raises MaxResultSizeExceeded itself, so matching that class alone never fired.
# The error's own message (globus_compute_sdk errors/error_types.py) is matched too, for a rendering without the name.
_RESULT_LIMIT = re.compile(
    r"MaxResultSizeExceeded\((\d+),\s*(\d+)\)|Task result of (\d+)B exceeded current limit of (\d+)B"
)


def _result_over_limit(exc: Exception) -> str | None:
    """How big an oversized result was ("12,000,000 bytes serialized; the limit is 10,485,760"), or None when `exc` is
    not one. Matched by name and text, keeping the SDK out of this pure layer."""
    name, text = type(exc).__name__, str(exc)
    m = _RESULT_LIMIT.search(text)
    named = "MaxResultSizeExceeded" in text or m is not None
    if name != "MaxResultSizeExceeded" and not (name == "TaskExecutionFailed" and named):
        return None
    if not m:
        return "size unknown"
    size, limit = (int(g) for g in (m.group(1, 2) if m.group(1) else m.group(3, 4)))
    return f"{size:,} bytes serialized; the limit is {limit:,}"  # bytes: just over must not read "10.0 vs 10.0 MiB"


# An exception's text is a diagnosis, not command output, so it is bounded here at the source by keeping BOTH ends:
# its head says what failed (the SDK's "Malformed or unexpected data structure. Data: <the serialized result>" puts the
# one useful line first), and a TaskExecutionFailed ends with the remote traceback's final exception line followed by
# the SDK's own help (~600 chars on a serialization error, ~230 more on a lost worker). What is cut is the middle.
_ERROR_HEAD_CHARS = 2_500  # the first line(s) and the outermost frames of a traceback
_ERROR_TAIL_CHARS = 1_500  # the SDK's appended help (up to ~840) plus ~650 for the final exception line before it


def _error_text(exc: Exception) -> str:
    text = str(exc)
    if len(text) <= _ERROR_HEAD_CHARS + _ERROR_TAIL_CHARS:
        return text
    cut = len(text) - _ERROR_HEAD_CHARS - _ERROR_TAIL_CHARS
    return (f"{text[:_ERROR_HEAD_CHARS]}\n[hpc-bridge: error text cut — {cut:,} chars from its middle]\n"
            f"{text[-_ERROR_TAIL_CHARS:]}")


def failure_outcome(exc: Exception, block_state: BlockState) -> ShellOutcome:
    name = type(exc).__name__
    if isinstance(exc, TimeoutError):
        return ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=124,
            notice=(
                "Command or endpoint timed out. Run ensure_endpoint_up and retry, "
                "or move long-running work into a batch job."
            ),
        )
    if (over := _result_over_limit(exc)) is not None:
        out = ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=None,  # unknown: it came back with the rest of the result, and none of it did
            stderr_snippet=_error_text(exc).strip(),
            notice=(
                f"The command RAN, but its result ({over}) was over Globus Compute's result limit, so none of it "
                "came back — not its output, not its exit code: it may have succeeded or failed. To see it, have it "
                "write to a file on the facility (`cmd > out.log 2>&1; echo $? > out.rc`) and read that in ranges "
                "(`wc -l out.log`, `head -n 100 out.log`, `sed -n '101,200p' out.log`, `tail -n 100 out.log`). "
                "Before running it again, check whether it already did its work (files written, jobs submitted, "
                "data changed): a command with side effects may not be safe to run twice."
            ),
        )
        out._worker_answered = True  # the worker ran it and answered: a liveness proof, not a lost worker
        return out
    if name == "TaskExecutionFailed":
        return ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=1,
            stderr_snippet=_error_text(exc),
            notice="The remote task failed to execute.",
        )
    return ShellOutcome(
        phase="failed",
        block_state=block_state,
        exit_code=1,
        stderr_snippet=_error_text(exc),
        notice=f"Dispatch error: {name}",
    )


_Result = TypeVar("_Result", ShellOutcome, LoginShellResult)


def for_agent(res: _Result, max_chars: int = MAX_OUTPUT_CHARS) -> _Result:
    """What a tool hands the AGENT: each stream of COMMAND output cut to its marked end (`cost.cut_output`), plus a
    notice saying how to read the whole output. Applied only where a tool returns (run_shell, poll_task,
    reset_session, login_shell) — hpc-bridge's own consumers of a result read it uncut. Command output comes only in
    a completed task (the SDK's snippet: text `SNIPPET_LINES` long may already be its cut, counted "at least") or a
    login_shell result; any other outcome's text is an exception's, already bounded by `failure_outcome`."""
    if isinstance(res, ShellOutcome):
        if res.phase != "complete":
            return res
        sdk_lines: int | None = SNIPPET_LINES
    else:
        sdk_lines = None  # login_shell: SSH output, no SDK line limit before ours
    out, err, cut = cut_streams(res.stdout, res.stderr_snippet, max_chars, sdk_lines=sdk_lines)
    if cut is None:
        return res
    notice = f"{res.notice} {cut[:1].upper()}{cut[1:]}" if res.notice else cut
    return res.model_copy(update={"stdout": out, "stderr_snippet": err, "notice": notice})
