from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CanaryResult:
    """Outcome of a worker-registration canary: did a real worker answer, and (best-effort)
    its Python/Dill versions + host, so the caller can flag serialization skew."""

    ok: bool
    worker_python: str | None = None
    worker_dill: str | None = None
    worker_host: str | None = None
    error: str | None = None


# A worker that runs this proves the compute block is live (manager_online cannot). A result
# returning at all = liveness; the version line is parsed best-effort (`|| true` keeps a
# missing python/dill from failing the probe — the worker still answered).
_CANARY_SENTINEL = "HPCB_CANARY"
# How recent a canary answer that arrived while nobody was waiting must be to count as "a worker is live now".
_CANARY_FRESH_S = 30.0
_CANARY_CMD = (
    f"echo {_CANARY_SENTINEL}; "
    'python -c "import platform,dill,socket;'
    'print(platform.python_version(),dill.__version__,socket.gethostname())" '
    "2>/dev/null || true"
)


def _parse_canary(stdout: str) -> tuple[str | None, str | None, str | None]:
    """Pull (python, dill, host) out of canary stdout; all None when the version line is absent."""
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0][:1].isdigit():  # the "3.11.7 0.3.9 <host>" line
            return parts[0], parts[1], parts[2]
    return None, None, None


def _escape_for_shellfunction(command: str) -> str:
    """ShellFunction runs cmd.format(**kwargs); double literal braces so arbitrary
    shell (brace groups, ${VAR}) survives the format pass as single braces."""
    return command.replace("{", "{{").replace("}", "}}")


async def _probe_executor(executor, *, timeout: float, command: str = _CANARY_CMD) -> tuple[bool, str]:
    """Submit `command` to `executor` and await a result within `timeout`, NEVER raising.

    Returns (True, stdout) if a worker answered, else (False, "<reason>"). The submit itself is
    INSIDE the guard: a shut-down or broken Executor raises `RuntimeError: Executor is shutdown`
    at `.submit()`, which we map to (False, …) instead of letting it propagate — every caller
    (the warmth canary; the reuse liveness gate) treats any failure as 'no live worker' (#37)."""
    from globus_compute_sdk import ShellFunction

    fn = ShellFunction(_escape_for_shellfunction(command), walltime=max(timeout - 1.0, 2.0))
    try:
        fut = executor.submit(fn)
        res = await asyncio.to_thread(fut.result, timeout)
    except TimeoutError:
        return False, "timeout"
    except Exception as exc:  # noqa: BLE001 - shutdown Executor / broken dispatch path, not a crash
        return False, dispatch_error_text(exc)
    return True, getattr(res, "stdout", "") or ""


def dispatch_error_text(exc: BaseException, limit: int = 600) -> str:
    """The diagnosis a caller can act on. A Globus API error's repr opens with the method, the full
    submit URL, the scheme and the status — ~190 chars before the message starts — so a short slice
    of `str(exc)` lost the part that matters (found while wiring the no-account failure: the 422
    'Identity failed to map to a local user name … Globus username: alice@example.edu' begins past
    char 190). Prefer the API error's `.message`; fall back to str(exc)."""
    msg = getattr(exc, "message", None)
    text = msg if isinstance(msg, str) and msg.strip() else str(exc)
    code = getattr(exc, "code", None)
    head = f"{type(exc).__name__}" + (f"[{code}]" if isinstance(code, str) and code else "")
    return f"{head}: {' '.join(text.split())}"[:limit]


class GlobusRunner:
    """Dispatches shell commands to a Globus Compute endpoint via a long-lived Executor.

    The executor is created lazily and reused across calls so the AMQP result
    subscription outlives individual dispatches. `executor_factory` is injectable
    for tests; the default builds a real `globus_compute_sdk.Executor`.
    """

    def __init__(
        self,
        endpoint_id: str,
        executor_factory=None,
        timeout: float = 120.0,
        walltime: float | None = None,
        user_endpoint_config: dict | None = None,
    ) -> None:
        self.endpoint_id = endpoint_id
        self.timeout = timeout
        # Server-side per-task ceiling: the worker kills the process and returns 124 at `walltime`.
        # DECOUPLED from the client sync-wait `timeout` — a task that outlives `timeout` is NOT
        # killed; the caller gets a poll handle and the task runs on until `walltime` (the block
        # walltime). The default (when unset) keeps the old timeout-linked value for back-compat.
        self.walltime = walltime if walltime is not None else max(timeout - 10.0, 5.0)
        self.user_endpoint_config = user_endpoint_config
        self._ex = None
        self._factory = executor_factory or self._default_factory
        # The ONE canary task still in flight (and when it resolved). Each canary is a real task; one submitted
        # per probe while a block cold-starts piled up dozens (24 in one 15-minute wait, Delta 2026-10-06), and a
        # facility multi-user endpoint keeps relaunching billed blocks for as long as ANY task is queued — for up
        # to its hard idle limit (48 h there), with no cancel channel from the client. So a pending canary is
        # waited on again, never resubmitted.
        self._canary_fut = None
        self._canary_done_at: float | None = None

    def _default_factory(self):
        from globus_compute_sdk import Executor

        return Executor(
            endpoint_id=self.endpoint_id,
            user_endpoint_config=self.user_endpoint_config,
        )

    def executor(self):
        if self._ex is None:
            self._ex = self._factory()
        return self._ex

    def submit(self, command: str):
        """Submit a shell command as a task and return its future WITHOUT waiting. The task runs on
        the worker up to `walltime` regardless of when the caller stops waiting; the long-lived
        Executor's result subscription resolves the future whenever it finishes, so a later poll can
        retrieve it. This is what lets a long task outlive the client sync-wait as a poll handle."""
        from globus_compute_sdk import ShellFunction

        fn = ShellFunction(_escape_for_shellfunction(command), walltime=self.walltime)
        return self.executor().submit(fn)

    async def run(self, command: str):
        fut = self.submit(command)
        # .result() blocks on the AMQP round-trip; run it off the event loop.
        return await asyncio.to_thread(fut.result, self.timeout)

    async def canary(self, timeout: float = 8.0) -> CanaryResult:
        """Confirm a WORKER answers a trivial task within `timeout`.

        A returned result (any) proves a worker is live — the worker-registration signal
        `manager_online()` can't give. Not-ok (never raised) => no worker yet (block still
        cold-starting) OR the dispatch path is broken (e.g. a shut-down Executor). Reuses the
        long-lived Executor, so the same AMQP path real dispatches use is what gets proven (and
        the submit kicks a cold block). At most ONE canary is in flight: a still-pending one is waited on
        again rather than resubmitted (see `_canary_fut`); one that answered long ago proves nothing about now
        and is replaced."""
        from globus_compute_sdk import ShellFunction

        fut = self._canary_fut
        if fut is not None and fut.done():
            fresh = self._canary_done_at is not None and time.monotonic() - self._canary_done_at <= _CANARY_FRESH_S
            if fut.cancelled() or not fresh:
                fut = self._canary_fut = None  # cancelled with its Executor, or a stale answer: ask again
        if fut is None:
            fn = ShellFunction(_escape_for_shellfunction(_CANARY_CMD), walltime=max(timeout - 1.0, 2.0))
            try:
                fut = self.executor().submit(fn)
            except Exception as exc:  # noqa: BLE001 - shutdown Executor / broken dispatch path, not a crash
                return CanaryResult(ok=False, error=dispatch_error_text(exc))
            self._canary_fut, self._canary_done_at = fut, None

            def _stamp(f, runner=self):  # runs on the SDK's thread; only for the canary still current
                if runner._canary_fut is f:
                    runner._canary_done_at = time.monotonic()

            fut.add_done_callback(_stamp)
        try:
            res = await asyncio.to_thread(fut.result, timeout)
        except TimeoutError:
            return CanaryResult(ok=False, error="timeout")  # still queued: the NEXT probe waits on the same task
        except Exception as exc:  # noqa: BLE001 - the task failed or the dispatch path broke
            if self._canary_fut is fut:
                self._canary_fut = None
            return CanaryResult(ok=False, error=dispatch_error_text(exc))
        if self._canary_fut is fut:
            self._canary_fut = None
        py, dill_v, host = _parse_canary(getattr(res, "stdout", "") or "")
        return CanaryResult(ok=True, worker_python=py, worker_dill=dill_v, worker_host=host)

    def close(self) -> None:
        if self._ex is not None:
            # shutdown() DEFAULTS to wait=True, which blocks until every pending future resolves
            # (the AMQP drain) — that was the multi-minute "stop hang": the scancel ran fast, then
            # close() blocked here. We never need pending results at teardown/runner-swap, so don't
            # wait, and cancel anything not yet registered with the web service.
            self._ex.shutdown(wait=False, cancel_futures=True)
            self._ex = None
        self._canary_fut, self._canary_done_at = None, None
