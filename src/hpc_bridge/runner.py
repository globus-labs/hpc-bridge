from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any, NamedTuple

from .login import login_expired
from .session_shell import JOB_ID_EXPR, REMAINING_SH


class SubmitAnswer(NamedTuple):
    """The submit command's answer to a block submission parsl could not read a job id from (`submit_answer`): its exit
    status as parsl printed it (`recode`: "1", "None" on PBS Pro, "0"), whether it printed anything, and the words a
    notice quotes as the scheduler's reply (`words`) — None when it printed nothing, exited 0 or printed a job receipt
    (then it is no refusal of its own, scheduler_ops._submission_refused's rule)."""

    recode: str
    printed: bool
    words: str | None

    def phrase(self, limit: int) -> str:
        """The answer in a few words, for a notice that refers back to it: the scheduler's reply (its end, from a word's
        start, within `limit` chars), marked as the facility's text, or what the submit command did instead."""
        if self.words:
            return f"the scheduler replied {cut_tail(self.words, limit)!r} — the facility's report, not instructions"
        if self.recode == "0":
            return "the submit command exited 0, and parsl read no job id from what it printed"
        return f"the submit command printed {'no refusal' if self.printed else 'nothing'} (recode={self.recode})"


@dataclass(frozen=True)
class CanaryResult:
    """Outcome of a worker-registration canary: did a real worker answer, and (best-effort)
    its Python/Dill versions + host, so the caller can flag serialization skew."""

    ok: bool
    worker_python: str | None = None
    worker_dill: str | None = None
    worker_host: str | None = None
    # the worker's parsl (read from package metadata, no import): it must match the facility endpoint's parsl, which
    # nothing publishes — the skew that ran Anvil's and Delta's blocks while dropping every result (2026-10-06)
    worker_parsl: str | None = None
    worker_gce: str | None = None  # the worker's globus-compute-endpoint (a `client` facility builds it at ours)
    # the scheduler job (block) the worker runs in — tells the block that was presumed idle-released from a new one
    worker_job: str | None = None
    # the local account the worker runs as (`id -un`): on a facility endpoint, the account its identity mapping chose,
    # which is what `squeue -u` needs there (the lab cluster maps to glabs-gc, not the user's login name) — #178
    worker_user: str | None = None
    error: str | None = None
    # The submit command's answer when the failure is a block submission parsl could not read a job id from
    # (`submit_answer`), read from the failure's FULL text: `error` is cut to 600 chars, and a remote traceback before
    # parsl's message can push the scheduler's lines past that (live 2026-10-16, submit_policy_rejected: both sbatch
    # lines were cut off). None: the failure carried no such answer.
    submit: SubmitAnswer | None = None
    # When the worker's answer actually arrived (time.monotonic) — it can predate this call when the answer came in
    # while nobody was waiting; warmth is dated from it, not from when it was read.
    answered_at: float | None = None
    # Globus Auth refused to refresh the stored login (login.login_expired): nothing can be sent until the user logs
    # in again — not a queue wait, not the config.
    login_expired: bool = False
    # The block's remaining walltime as the worker read it from inside the block (session_shell.REMAINING_SH: Slurm's
    # SLURM_JOB_END_TIME, `squeue %L`, PBS stime + walltime), in seconds at `answered_at`; None: unknown (no job id —
    # the login shape — or nothing answered). A batch admits nothing on unknown (batch phase 1, the admission gate).
    worker_remaining_s: float | None = None
    # PARSL_WORKER_COUNT on the worker: how many workers its block runs (a batch's window); None when not set.
    worker_count: int | None = None


# A worker that runs this proves the compute block is live (manager_online cannot). A result
# returning at all = liveness; the version line is parsed best-effort (`|| true` keeps a
# missing python/dill from failing the probe — the worker still answered).
_CANARY_SENTINEL = "HPCB_CANARY"
# How recent a canary answer that arrived while nobody was waiting must be to count as "a worker is live now".
_CANARY_FRESH_S = 30.0
# How long a pending canary is waited on before it is abandoned for a fresh one: an endpoint that restarts can drop
# a task it had accepted, and a canary that will never resolve must not wedge the shape. Long enough that a slow
# queue costs a handful of canaries, not one per probe.
_CANARY_MAX_WAIT_S = 300.0
_CANARY_PARSL = "HPCB_PARSL"
_CANARY_GCE = "HPCB_GCE"
_CANARY_JOB = "HPCB_JOB"
_CANARY_USER = "HPCB_USER"
_CANARY_REMAINING = "HPCB_REMAINING"
_CANARY_WORKERS = "HPCB_WORKERS"
_CANARY_CMD = (
    f"echo {_CANARY_SENTINEL}; "
    'python -c "import platform,dill,socket;'
    'print(platform.python_version(),dill.__version__,socket.gethostname())" '
    "2>/dev/null || true; "
    f"python -c \"import importlib.metadata as m;print('{_CANARY_PARSL}',m.version('parsl'))\" 2>/dev/null || true; "
    f"python -c \"import importlib.metadata as m;print('{_CANARY_GCE}',m.version('globus-compute-endpoint'))\" "
    "2>/dev/null || true; "
    # The local account the worker runs as: on a facility endpoint, the identity mapping's — a stop names it for
    # `squeue -u` (server._stop_mep, #178).
    f'echo "{_CANARY_USER} $(id -un 2>/dev/null)"; '
    # The scheduler job the worker runs in (Slurm's or PBS's job id; empty on a LocalProvider block): which BLOCK
    # answered. After an idle presumption, the block that answers next is dated by it: the old block keeps its age, a
    # new one starts its own (warmth._provision). The wrong-block guard compares the SAME expression with the confirmed
    # block's job before every command it wraps (session_shell). ShellFunction runs this through str.format, then
    # /bin/bash.
    f'echo "{_CANARY_JOB} {JOB_ID_EXPR}"; '
    # The block's remaining walltime and worker count, from inside it (batch phase 1: the admission gate, the window)
    + REMAINING_SH + "; "
    f'echo "{_CANARY_REMAINING} ${{__hb_rem:-}}"; echo "{_CANARY_WORKERS} ${{PARSL_WORKER_COUNT:-}}"'
)


def _parse_canary_tagged(stdout: str, tag: str) -> str | None:
    """A value from canary stdout's `<TAG> <value>` line — a version, or a job id (Slurm's and PBS's start with a
    digit) — or None, also when the value is empty."""
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == tag and parts[1][:1].isdigit():
            return parts[1]
    return None


# A POSIX-portable login name (what `id -un` prints): letters, digits, '.', '_', '-', at most 64 — and never an option.
_USER_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9._-]{0,63}$")


def _parse_canary_user(stdout: str) -> str | None:
    """The worker's local account from canary stdout's `HPCB_USER <name>` line (the `HPCB_JOB` line's style), or None
    when it is absent, empty or not a plain login name."""
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == _CANARY_USER and _USER_NAME.match(parts[1]):
            return parts[1]
    return None


def _parse_canary_parsl(stdout: str) -> str | None:
    """The worker's parsl version from canary stdout (the `HPCB_PARSL <version>` line), or None."""
    return _parse_canary_tagged(stdout, _CANARY_PARSL)


def _parse_canary(stdout: str) -> tuple[str | None, str | None, str | None]:
    """Pull (python, dill, host) out of canary stdout; all None when the version line is absent."""
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0][:1].isdigit():  # the "3.11.7 0.3.9 <host>" line
            return parts[0], parts[1], parts[2]
    return None, None, None


# How much of each output stream a tool result hands the AGENT, in characters: its end, cut by `dispatch.for_agent`
# with a marker saying what was dropped (hpc-bridge's own consumers — the pilot probe, the allocation parsers — read
# the uncut text). Sized for what hosts put in front of the model (2026-10): Claude Code moves an MCP result over
# 50,000 chars to a file (its own Bash keeps ~30,000 inline) and Hermes spills one over 50,000; Codex middle-cuts
# past ~10,000 tokens by default and Pi past 20 KiB. One full stream plus the JSON around it (~17,500) fits all
# four; both streams full (~34,500) fit all but Pi, which marks its own cut and saves the rest.
MAX_OUTPUT_CHARS = 16_000
# The SDK's ShellFunction keeps only the LAST `snippet_lines` lines of each stream (default 1000) and says nothing, so
# `cat` of a longer file came back as its tail, passing for the whole file. The worker's whole result must also stay
# under Globus Compute's 10 MiB limit, measured AFTER serialization (~1.35x the text): over it the task fails and
# nothing comes back, exit code included. At 2,001 lines a result fails only once its lines average ~3,880 bytes on
# one stream, ~1,940 with both full (16,001 lines would fail at ~485 / ~243: JSONL, wide CSV, VCF, build logs).
# The cost: output averaging under 8 chars a line comes back as its last 2,001 lines, under 16,000 chars. Text that
# reaches the limit may be the SDK's cut, so it is marked ("at least"); the +1 keeps a `head -n 2000` unmarked.
SNIPPET_LINES = MAX_OUTPUT_CHARS // 8 + 1


def _escape_for_shellfunction(command: str) -> str:
    """ShellFunction runs cmd.format(**kwargs); double literal braces so arbitrary
    shell (brace groups, ${VAR}) survives the format pass as single braces."""
    return command.replace("{", "{{").replace("}", "}}")


def _error_message(exc: BaseException) -> str:
    """An exception's own text, whole: a Globus API error's `.message`, else str(exc)."""
    msg = getattr(exc, "message", None)
    return msg if isinstance(msg, str) and msg.strip() else str(exc)


def dispatch_error_text(exc: BaseException, limit: int = 600) -> str:
    """The diagnosis a caller can act on. A Globus API error's repr opens with the method, the full
    submit URL, the scheme and the status — ~190 chars before the message starts — so a short slice
    of `str(exc)` lost the part that matters (found while wiring the no-account failure: the 422
    'Identity failed to map to a local user name … Globus username: alice@example.edu' begins past
    char 190). Prefer the API error's `.message`; fall back to str(exc). Cut to `limit`: a refused submission's
    answer is read from the whole text before this (`submit_answer`, `_failed_canary`)."""
    text = _error_message(exc)
    code = getattr(exc, "code", None)
    head = f"{type(exc).__name__}" + (f"[{code}]" if isinstance(code, str) and code else "")
    return f"{head}: {' '.join(text.split())}"[:limit]


# The submit command's answer in a block submission parsl could not read a job id from is what parsl's SubmitException
# carries, the same on Slurm and PBS Pro (parsl/providers/errors.py, 2026.01–2026.10): `Cannot launch job <name>:
# <message>; recode=<rc>, stdout=<out>, stderr=<err>` — Slurm's message is "Could not read job ID from submit command
# standard output", PBS Pro's "Submit command 'qsub …' failed" (recode=None). A task gets it as parsl's block error,
# "Failed to start block <n>: <that>" under "Error <n>:", once per failed block, inside the endpoint's traceback: each
# attempt between a rule of 68 '+' and one of 68 '-' (a retry history adds "Traceback from attempt: <n>"), the SDK's
# help text after the last. The streams are the submit command's own, whole: sbatch's or qsub's lines, often two
# ("sbatch: error: AssocMaxSubmitJobLimit" / "sbatch: error: Batch job submission failed: Job violates accounting/QOS
# policy (…)"). Where they end is read from that structure, never from what they say: a site's submit filter may print
# blank lines, rules or an "Error 1: …" of its own. The next block's error ("Error <n>:" alone on its line: parsl's
# `_get_error`) ends them too — a non-submit one (a MISSING job) is not the scheduler's reply.
_SUBMIT_STREAMS = re.compile(r"; recode=([^,\n]*), stdout=")
_NEXT_ERROR = re.compile(r"(?m)^[ \t]*Error \d+:[ \t]*$")
_NEXT_ATTEMPT = re.compile(r"(?:^|\s)Traceback from attempt")
_RULE = re.compile(r"(?:^|\s)-{68}(?=\s|$)")  # the endpoint's closing rule (on text already joined, too)
# A scheduler's receipt for an accepted job: Slurm's "Submitted batch job N" (a bare id with --parsable), a PBS job id.
_RECEIPT = re.compile(r"Submitted batch job \d+|^\s*\d+(?:;\S+)?\s*$|^\s*\d+(?:\[\d*\])?\.[\w.-]+\s*$", re.M)
# How much of the scheduler's reply a notice quotes. Real refusals are one to three lines: the live two-line sbatch
# refusal above is 165 chars, a Slurm or PBS line runs 40–130 (a site's job_submit plugin or a PBS hook adds a sentence
# or two), so 400 keeps them whole; of a longer text — a site's banner before its reason — the END is kept (sbatch's
# decisive "Batch job submission failed: …" comes last), marked, which also bounds the facility text one notice carries.
# (The cut it replaced, 220 chars of parsl's wrapper, ended at "stderr=sbatch".)
SCHEDULER_WORDS_MAX = 400


def cut_tail(text: str, limit: int) -> str:
    """`text`, or as much of its end as fits in `limit` chars behind a leading "…", from a word's start (unless one word
    is all there is)."""
    if len(text) <= limit:
        return text
    if limit < 2:
        return "…"[:max(limit, 0)]
    tail = text[-(limit - 1):]
    if not text[-limit].isspace() and " " in tail:  # the cut fell inside a word: start at the next one
        tail = tail.split(" ", 1)[1]
    return "…" + tail.lstrip()


def submit_answer(text: str | None, *, framed: bool = True) -> SubmitAnswer | None:
    """The submit command's answer in a block submission parsl could not read a job id from (`; recode=…, stdout=…,
    stderr=…`; the last one, the newest block's, when several blocks failed), its streams whitespace-normalised; the
    words quoted are its stderr, else its stdout, at most `SCHEDULER_WORDS_MAX` chars (their end). They end at the next
    block's error; and `framed` — the text is a task's failure — at the endpoint's LAST closing rule (a rule in stderr
    comes before it) or the next attempt; else (a user endpoint log's error, `scheduler_ops._refused_answer`) at its
    end. None when the text carries no such answer (another failure, or a text cut before its stderr)."""
    t = str(text or "")
    found = list(_SUBMIT_STREAMS.finditer(t))
    if not found:
        return None
    rest = t[found[-1].end():]
    if m := _NEXT_ERROR.search(rest):
        rest = rest[:m.start()]
    if framed:
        if m := _NEXT_ATTEMPT.search(rest):
            rest = rest[:m.start()]
        if rules := list(_RULE.finditer(rest)):
            rest = rest[:rules[-1].start()]
    out, sep, err = rest.rpartition(", stderr=")
    if not sep:
        return None
    recode = found[-1].group(1).strip()
    words = " ".join(err.split()) or " ".join(out.split())
    printed = bool(words)
    if not words or recode == "0" or _RECEIPT.search(out) or _RECEIPT.search(err):
        return SubmitAnswer(recode, printed, None)
    return SubmitAnswer(recode, printed, cut_tail(words, SCHEDULER_WORDS_MAX))



def _answered(fut) -> bool:
    """A canary future a worker answered: done with a result (a failure or a cancel may be the client's alone)."""
    return bool(fut.done() and not fut.cancelled() and fut.exception() is None)


def never_sent(fut) -> bool:
    """A future whose task the web service certainly never took: it ended with no `task_id` because a close cancelled
    it before the submit, or the service REFUSED the submission (an HTTP 4xx: auth, a bad request). A submit that
    failed any other way (a timeout, a 5xx, a malformed answer, "only partially initiated") may have been taken — the
    caller treats that as unknown for a while (warmth._check_state)."""
    if not fut.done() or getattr(fut, "task_id", None) is not None:
        return False
    if fut.cancelled():
        return True
    status = getattr(fut.exception(), "http_status", None)
    return isinstance(status, int) and 400 <= status < 500


def _may_be_queued(fut) -> bool:
    """A canary future that may still be a task queued on the endpoint: no worker answered it, and the web service
    may have taken it (`never_sent` says when it certainly did not)."""
    return not _answered(fut) and not never_sent(fut)


def _task_client(executor) -> Any:
    """The SDK Client behind an Executor (`get_task`, `get_batch_result`), or None."""
    return getattr(executor, "client", None) if executor is not None else None


def _failed_canary(exc: BaseException) -> CanaryResult:
    """A canary that failed with `exc`: its diagnosis, what the scheduler said if it refused a block submission (read
    from the whole text, before the diagnosis is cut), and whether it was the stored Globus login that failed."""
    return CanaryResult(ok=False, error=dispatch_error_text(exc), submit=submit_answer(_error_message(exc)),
                        login_expired=login_expired(exc))


def _canary_result(res: object, answered_at: float) -> CanaryResult:
    """A worker's canary answer, parsed."""
    stdout = getattr(res, "stdout", "") or ""
    py, dill_v, host = _parse_canary(stdout)
    return CanaryResult(ok=True, worker_python=py, worker_dill=dill_v, worker_host=host,
                        worker_parsl=_parse_canary_parsl(stdout),
                        worker_gce=_parse_canary_tagged(stdout, _CANARY_GCE),
                        worker_job=_parse_canary_tagged(stdout, _CANARY_JOB),
                        worker_user=_parse_canary_user(stdout), answered_at=answered_at,
                        worker_remaining_s=_float_or_none(_parse_canary_tagged(stdout, _CANARY_REMAINING)),
                        worker_count=_int_or_none(_parse_canary_tagged(stdout, _CANARY_WORKERS)))


def _float_or_none(v: str | None) -> float | None:
    try:
        return float(v) if v is not None else None
    except ValueError:
        return None


def _int_or_none(v: str | None) -> int | None:
    try:
        return int(v) if v is not None else None
    except ValueError:
        return None


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
        self._canary_submitted_at: float | None = None
        # Every canary this runner SENT that may still be queued, with when (wall clock), and the SDK Client that
        # looks a task up (`sent_checks`): a stop answers `down` only once each has resolved, since a check still
        # queued can make the endpoint start a block for it.
        self._sent: list[tuple[Any, float]] = []
        # The block request each of those was sent for (the `hpcb_block` of this runner's config AT ITS SEND, by future
        # id): a stop matches a check to its own compute user endpoint by it (server._settle_by_evidence). Read at the
        # send, never later — the shape's config can be the same dict and gets a new request's number after it.
        self._sent_blocks: dict[int, Any] = {}
        # When this process first SAW each one's task id (wall clock and monotonic, by future id): the service took the
        # task before that — an upper bound on when it did. `Executor.submit` only queues a task; the SDK sends it later
        # (batched, rate-limited, retried), so the submit's time says nothing of when the service had it.
        self._taken: dict[int, tuple[float, float]] = {}
        self._client: Any = None
        # Called right after a canary is submitted, before its wait (warmth._confirm_worker records it on disk).
        self.on_sent: Any = None

    def _default_factory(self):
        from globus_compute_sdk import Executor

        ex = Executor(
            endpoint_id=self.endpoint_id,
            user_endpoint_config=self.user_endpoint_config,
        )
        # Its task group is the SERVICE's, minted at its first submit (a client-made id 404s and shuts the Executor
        # down: spike S8) — never one an earlier Executor of this process adopted: the SDK's setter writes that into
        # the class-wide default (`Executor._default_task_group_id`), and SDK 4.18's result watcher acks every message
        # on its group's queue, so two Executors on one group swallow each other's results (S1). Set past the setter,
        # which would write the class default again.
        ex._task_group_id = None
        return ex

    @property
    def task_group_id(self) -> Any:
        """The service-minted task group of this runner's Executor (None before its first submit)."""
        return getattr(self._ex, "task_group_id", None) if self._ex is not None else None

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

        fn = ShellFunction(_escape_for_shellfunction(command), walltime=self.walltime, snippet_lines=SNIPPET_LINES)
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
        # Not re-entrant: the only caller (warmth._confirm_worker) runs under app.lock, so one coroutine at a time
        # owns this runner's canary state.
        from concurrent.futures import CancelledError

        from globus_compute_sdk import ShellFunction

        now = time.monotonic()
        fut = self._canary_fut
        if fut is not None:
            if fut.done():
                fresh = self._canary_done_at is not None and now - self._canary_done_at <= _CANARY_FRESH_S
                # a FAILURE is handed back once whatever its age (its text is the diagnosis — a no-account verdict
                # is terminal); a stale SUCCESS proves nothing about now and is replaced
                if fut.cancelled() or (not fresh and fut.exception() is None):
                    fut = self._canary_fut = None
            elif self._canary_submitted_at is not None and now - self._canary_submitted_at > _CANARY_MAX_WAIT_S:
                fut = self._canary_fut = None  # waited long enough: the endpoint may have lost it; ask once more
        if fut is None:
            fn = ShellFunction(_escape_for_shellfunction(_CANARY_CMD), walltime=max(timeout - 1.0, 2.0))
            try:
                fut = self.executor().submit(fn)
            except Exception as exc:  # noqa: BLE001 - shutdown Executor / broken dispatch path, not a crash
                return _failed_canary(exc)
            self._canary_fut, self._canary_done_at, self._canary_submitted_at = fut, None, now
            self._sent = [(f, at) for f, at in self._sent if _may_be_queued(f)] + [(fut, time.time())]
            self._sent_blocks = {id(f): self._sent_blocks.get(id(f)) for f, _ in self._sent[:-1]}
            self._taken = {id(f): self._taken[id(f)] for f, _ in self._sent[:-1] if id(f) in self._taken}
            self._sent_blocks[id(fut)] = (self.user_endpoint_config or {}).get("hpcb_block")
            self._client = _task_client(self._ex) or self._client
            if self.on_sent is not None:  # the caller records it NOW, before the wait (a restart must still see it)
                try:
                    self.on_sent()
                except Exception:  # noqa: BLE001 - a record that cannot be written never fails the check
                    pass

            def _stamp(f, runner=self):  # runs on the SDK's thread; only for the canary still current
                if runner._canary_fut is f:
                    runner._canary_done_at = time.monotonic()

            fut.add_done_callback(_stamp)
        try:
            res = await asyncio.to_thread(fut.result, timeout)
        except TimeoutError:
            task_exc = fut.exception() if fut.done() and not fut.cancelled() else None
            if isinstance(task_exc, TimeoutError):
                # the TASK failed with a TimeoutError — a result, not "still queued": hand it back and ask afresh.
                # (By type, not identity: 3.11's asyncio re-creates a concurrent TimeoutError on the way out.)
                if self._canary_fut is fut:
                    self._canary_fut = None
                return _failed_canary(task_exc)
            return CanaryResult(ok=False, error="timeout")  # still queued: the NEXT probe waits on the same task
        except (CancelledError, asyncio.CancelledError):  # asyncio.to_thread raises asyncio's for a cancelled Future
            if not fut.cancelled():
                raise  # this coroutine was cancelled, not the task
            if self._canary_fut is fut:
                self._canary_fut = None
            return CanaryResult(ok=False, error="canary task was cancelled")
        except Exception as exc:  # noqa: BLE001 - the task failed or the dispatch path broke
            if self._canary_fut is fut:
                self._canary_fut = None
            return _failed_canary(exc)
        answered_at = self._canary_done_at or time.monotonic()
        if self._canary_fut is fut:
            self._canary_fut = None
        return _canary_result(res, answered_at)

    def canary_answer(self) -> CanaryResult | None:
        """The answer of a canary that resolved after its caller stopped waiting (a timeout) and that nobody has read
        yet — the block it ran on (its job, its account) — or None. Never waits and never submits: a facility stop
        names that block instead of calling it a check still queued (#178)."""
        fut = self._canary_fut
        if fut is None or not fut.done() or fut.cancelled() or fut.exception() is not None:
            return None
        return _canary_result(fut.result(), self._canary_done_at or time.monotonic())

    def task_client(self) -> Any:
        """The SDK Client that looks this runner's tasks up by id (`get_task`, `get_batch_result`) — kept from the last
        send, so it works after `close()` — or None before anything was sent."""
        return _task_client(self._ex) or self._client

    def sent_checks(self) -> list[tuple[Any, Any, float]]:
        """(future, client, sent_at) for each canary this runner sent that may still be queued (no worker answered it):
        its SDK future — `task_id` once the web service registered it —, the SDK Client that looks it up (kept from the
        send, so it works after this runner's close) and when it was sent (wall clock). `close()` cancels only tasks
        never handed to the web service; one it took stays queued on the endpoint whatever happens here, and its
        future can also fail on the client's side alone (the result watcher quitting), so the caller asks the service
        by task id (warmth._check_states)."""
        client = self.task_client()
        return [(f, client, at) for f, at in self._sent if _may_be_queued(f)]

    def _seen(self, fut: Any) -> tuple[float, float] | None:
        key = id(fut)
        if key not in self._taken and getattr(fut, "task_id", None) and any(f is fut for f, _ in self._sent):
            self._taken[key] = (time.time(), time.monotonic())
        return self._taken.get(key)

    def taken_at(self, fut: Any) -> float | None:
        """When this process first saw the task id of a canary in `sent_checks` (wall clock, for the record): the
        service had taken it by then. Stamped by the first caller that sees the id — the server's id watcher looks every
        50 ms to 2 s after a send (warmth._watch_ids) —, so it is never earlier than the service took it. None while it
        has no id."""
        seen = self._seen(fut)
        return seen[0] if seen is not None else None

    def taken_mono(self, fut: Any) -> float | None:
        """`taken_at` on this process's monotonic clock: what an age is measured from here, so a step of the wall clock
        cannot make a check look older than it is."""
        seen = self._seen(fut)
        return seen[1] if seen is not None else None

    def sent_block(self, fut: Any) -> Any:
        """The block request (`hpcb_block`) a canary in `sent_checks` was sent for, as this runner's config read at its
        send; None when it had none (the login shape, a facility endpoint, an endpoint of an earlier plugin)."""
        return self._sent_blocks.get(id(fut))

    @property
    def canary_pending(self) -> bool:
        """A canary of ours was submitted and has not resolved: a task still queued on the endpoint, for which a
        facility endpoint starts (or keeps) a billed block — whatever happens to this runner (#178)."""
        return self._canary_fut is not None and not self._canary_fut.done()

    def close(self) -> None:
        if self._ex is not None:
            # shutdown() DEFAULTS to wait=True, which blocks until every pending future resolves
            # (the AMQP drain) — that was the multi-minute "stop hang": the scancel ran fast, then
            # close() blocked here. We never need pending results at teardown/runner-swap, so don't
            # wait, and cancel anything not yet registered with the web service.
            self._ex.shutdown(wait=False, cancel_futures=True)
            self._ex = None
        self._canary_fut, self._canary_done_at, self._canary_submitted_at = None, None, None
