from __future__ import annotations

from typing import Protocol

from .cost import cap_output
from .lifecycle import BlockState
from .models import ShellOutcome


class ShellLike(Protocol):
    returncode: int
    stdout: str
    stderr: str


class Runner(Protocol):
    async def run(self, command: str) -> ShellLike: ...


async def execute(
    command: str,
    runner: Runner,
    *,
    block_state: BlockState = "warm",
    max_output_chars: int = 1_000_000,
) -> ShellOutcome:
    """Dispatch a shell command through a Runner and shape the structured result.

    Any dispatch failure (timeout, oversized result, remote task failure, transport
    error) is translated into a structured `failed` ShellOutcome rather than raised,
    so a hung/broken endpoint never crashes the MCP tool or hangs the agent silently.
    """
    try:
        res = await runner.run(command)
    except Exception as exc:  # noqa: BLE001 - deliberately translate ALL failures to an outcome
        return failure_outcome(exc, block_state, max_output_chars)
    return complete_outcome(res, block_state, max_output_chars)


def complete_outcome(res: ShellLike, block_state: BlockState, max_output_chars: int) -> ShellOutcome:
    """Shape a successful runner result into a `complete` outcome. Shared by execute() and the
    submit/poll path (server._run_shell / poll_task) so the completion mapping lives in one place."""
    return ShellOutcome(
        phase="complete",
        exit_code=res.returncode,
        stdout=cap_output(res.stdout, max_output_chars),
        stderr_snippet=cap_output(res.stderr, max_output_chars),
        block_state=block_state,
    )


def failure_outcome(exc: Exception, block_state: BlockState, max_output_chars: int) -> ShellOutcome:
    name = type(exc).__name__
    if isinstance(exc, TimeoutError):
        return ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=124,
            notice=(
                "Command or endpoint timed out. Run ensure_endpoint_up and retry, or move long-running work "
                "into a batch job (billed to the allocation like a block, so only with the user's spend "
                "confirmation)."
            ),
        )
    # Match by class name to avoid importing globus_compute_sdk into the pure layer.
    if name == "MaxResultSizeExceeded":
        return ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=1,
            notice=(
                "Output exceeded the 10 MB result limit. Redirect verbose output to a "
                "file and read it back in bounded chunks."
            ),
        )
    if name == "TaskExecutionFailed":
        return ShellOutcome(
            phase="failed",
            block_state=block_state,
            exit_code=1,
            stderr_snippet=cap_output(str(exc), max_output_chars),
            notice="The remote task failed to execute.",
        )
    return ShellOutcome(
        phase="failed",
        block_state=block_state,
        exit_code=1,
        stderr_snippet=cap_output(str(exc), max_output_chars),
        notice=f"Dispatch error: {name}",
    )
