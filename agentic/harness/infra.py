"""INFRA: a cell whose operator produced nothing to grade, because the model endpoint (not hpc-bridge) failed.

Seen live 2026-10-14/15: ALCF's Sophia streaming path answered HTTP 200 carrying `StreamingTimeoutError`; Pi ended with
an empty final answer and no tool call (its record: `Stream ended without finish_reason`), and the cell graded FAILED on
agent_engaged, compute_ran, … — at a glance, a product regression. `infra_reason` names such a run, so run.py records
`RESULT: INFRA: <reason>` (exit code INFRA_RC) instead of FAILED, and regrade.py derives the same from the bundle.

Conservative by construction:
- a run with one or more hpc-bridge calls is never INFRA: a partial run is real evidence, and it is graded;
- INFRA never hides a failed safety-floor grader or a failed world check;
- the Claude operators (`claude`, `claude-acp`) are INFRA only when the API itself ended the run with a server error
  (5xx, overloaded, unreachable). A 429 is RATE_LIMITED's, which run.py decides first;
- the other operators (codex, pi, hermes21, hermes) are INFRA when the operator's own record ends in a model or
  transport error, or its final answer is itself such an error; and, when it made no tool call of any kind, also when
  it exited non-zero, errored, or gave an empty final answer. An agent that used its own tools and then ended silently
  was acting: its ending is graded. A known signature the operator recovered from (a retry that then succeeded)
  decides nothing.

Every input is read from what the bundle stores (messages.jsonl, record.json), so the live verdict and the replay agree.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from invariants import _HPC_TOOLS, FLOOR_NAMES, Trace

INFRA_RC = 4                      # run.py / run_smoke.sh: 0 OK · 1 FAILED · 2 SETUP FAILED/SKIPPED · 3 RATE_LIMITED · 4 INFRA
CLAUDE_OPERATORS = frozenset({"claude", "claude-acp"})
PREFLIGHT_ROW = "harness:alcf_stream"   # the grading row of a cell run_smoke.sh never launched (alcf_probe.py)

# What a model endpoint's failure reads like. Only an operator's TERMINAL error and its final answer DECIDE INFRA; the raw
# stdout/stderr tails, where a retry the operator recovered from leaves the same words, only label a reason.
_SIGNATURES = (
    re.compile(r"Stream ended without finish_reason"),
    re.compile(r"StreamingTimeoutError"),
    re.compile(r"No data received from (?:the )?compute endpoint"),
    re.compile(r"\b(?:5\d\d|429) status code\b"),
    re.compile(r"\b(?:5\d\d|429)\s+(?:Internal Server Error|Bad Gateway|Service Unavailable|Gateway Time-?out|"
               r"Too Many Requests)\b", re.I),
    re.compile(r"\b(?:HTTP(?:/[\d.]+)?|status(?:[ _]code)?|error code|code)\W{0,3}(?:5\d\d|429)\b", re.I),
)


def signature(text: str) -> str | None:
    """The first model-endpoint failure signature in `text`, as it appears there, or None."""
    for rx in _SIGNATURES:
        m = rx.search(text or "")
        if m:
            return m.group(0)
    return None


@dataclass(frozen=True)
class Outcome:
    """What the operator left behind, as far as INFRA is concerned."""
    operator: str
    hpcb_calls: int               # calls to hpc-bridge tools, refused or failed ones included
    tool_calls: int = 0           # calls of any kind (the operator's own shell / read / search tools too)
    final_text: str = ""          # the agent's last word to the user
    rc: int | None = None         # the operator process's exit code (the CLI operators); None = no such process
    is_error: bool | None = None  # the runner's own "the run did not complete" (hermes: non-zero exit, timeout, ACP death)
    terminal_error: str = ""      # the operator's own record of the error its run ENDED on (Pi, Codex)
    api_error: str = ""           # the Claude operators: the API server error the run ended on
    tails: str = ""               # the operator's stdout/stderr tails: they label a reason, never decide one


def _get(obj: Any, key: str) -> Any:
    return obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)


def _jsonl(text: str) -> list[dict]:
    out = []
    for line in (text or "").split("\n"):
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
            except ValueError:
                continue   # the first line of a tail is usually cut
            if isinstance(obj, dict):
                out.append(obj)
    return out


def pi_terminal_error(stdout: str) -> str:
    """`pi --mode json`: the error the run ended on — `auto_retry_end` with success false (its retries spent), or a last
    assistant `message_end` with stopReason "error" — else "" (a later success clears an earlier error)."""
    last = ""
    for ev in _jsonl(stdout):
        kind = ev.get("type")
        if kind == "auto_retry_end":
            last = "" if ev.get("success") else str(ev.get("finalError") or "retries exhausted")
        elif kind == "message_end":
            msg = ev.get("message") or {}
            if isinstance(msg, dict) and msg.get("role") == "assistant":
                last = str(msg.get("errorMessage") or "model error") if msg.get("stopReason") == "error" else ""
    return last


def codex_terminal_error(stdout: str) -> str:
    """`codex exec --json`: a `turn.failed` event's message, else "" (its `error` events are retries in progress)."""
    last = ""
    for ev in _jsonl(stdout):
        if ev.get("type") == "turn.failed":
            err = ev.get("error")
            last = str((err.get("message") if isinstance(err, dict) else err) or "turn failed")
        elif ev.get("type") == "turn.completed":
            last = ""
    return last


def _server_error(err: Any, status: Any) -> bool:
    return err == "server_error" or "overloaded" in str(err or "").lower() or (isinstance(status, int) and status >= 500)


def _assistant_text(m: Any) -> str:
    content = _get(m, "content")
    if content is None and isinstance(_get(m, "message"), dict):
        content = m["message"].get("content")   # a Claude Code transcript line
    if isinstance(content, str):
        return content
    return " ".join(str(_get(b, "text") or "") for b in content or []).strip()


def claude_api_error(messages: Iterable[Any], final: Any) -> str:
    """The API server error a Claude operator's run ended on, or "". The SDK's ResultMessage `api_error_status` (5xx),
    or the run's LAST assistant message being an API error of the server kind: the SDK's AssistantMessage `error`
    ("server_error"), or a Claude Code transcript line with `isApiErrorMessage` (claude-acp). A later healthy assistant
    message clears an earlier error: the run went on."""
    last = ""
    for m in messages:
        sdk = (type(m).__name__ == "AssistantMessage") or (isinstance(m, dict) and m.get("__type__") == "AssistantMessage")
        transcript = isinstance(m, dict) and m.get("type") == "assistant"
        if not (sdk or transcript):
            continue
        err = _get(m, "error")
        status = _get(m, "apiErrorStatus") if transcript else None
        if (sdk or _get(m, "isApiErrorMessage")) and _server_error(err, status):
            last = (f"Claude API error ({err or status}"
                    + (f", {status}" if err and isinstance(status, int) else "") + f"): {_assistant_text(m)[:160]}")
        else:
            last = ""
    status = _get(final, "api_error_status")
    if isinstance(status, int) and status >= 500:
        return last or f"Claude API error {status}: {str(_get(final, 'result') or '')[:160]}"
    return last


def _cli_head(messages: Iterable[Any]) -> dict | None:
    """The CLI operators' (cli_runner) first bundle message: harness, rc, stdout/stderr tails."""
    for m in messages:
        if isinstance(m, dict) and "harness" in m and "rc" in m:
            return m
        break
    return None


def outcome_of(operator: str, trace: Trace, messages: list[Any], final: Any) -> Outcome:
    """Read an Outcome from a run: live (run.py: the RunResult's trace, messages and final) or from a bundle (regrade.py:
    the rebuilt trace, messages.jsonl's rows and `bundle_final`). The same fields in both."""
    calls, every = sum(1 for c in trace.calls if c.name in _HPC_TOOLS), len(trace.calls)
    head = _cli_head(messages)
    if head is not None:
        stdout, stderr = str(head.get("stdout_tail") or ""), str(head.get("stderr_tail") or "")
        harness = str(head.get("harness"))
        final_text = next((str(m["final"] or "") for m in messages if isinstance(m, dict) and "final" in m), "")
        rc = head.get("rc") if isinstance(head.get("rc"), int) else None
        term = pi_terminal_error(stdout) if harness == "pi" else codex_terminal_error(stdout) if harness == "codex" else ""
        return Outcome(operator, calls, every, final_text, rc, bool(rc), term, "", stdout[-2000:] + "\n" + stderr[-2000:])
    final_text = trace.texts[-1] if trace.texts else str(_get(final, "result") or "")
    api = claude_api_error(messages, final) if operator in CLAUDE_OPERATORS else ""
    is_error = _get(final, "is_error")
    return Outcome(operator, calls, every, final_text, None, is_error if isinstance(is_error, bool) else None, "", api, "")


def bundle_final(messages: list[Any], record_final: dict | None) -> Any:
    """A bundle's `final`, as run.py held it live: the Claude SDK's ResultMessage (its last one in messages.jsonl, which
    carries `api_error_status` and `result`), else record.json's `final` (is_error)."""
    last = next((m for m in reversed(messages) if isinstance(m, dict) and m.get("__type__") == "ResultMessage"), None)
    return last if last is not None else (record_final or {})


def _short(text: str, n: int = 200) -> str:
    s = " ".join(str(text).split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _last_line(text: str) -> str:
    return next((ln.strip() for ln in reversed((text or "").splitlines()) if ln.strip()), "")


def infra_reason(o: Outcome, failed: Iterable[str] = ()) -> str | None:
    """Why this run is INFRA (the operator produced nothing usable because its model endpoint failed), or None — it is
    graded as it stands. `failed`: the gating checks that broke; a broken safety floor or world check is never hidden."""
    if o.hpcb_calls > 0:
        return None
    if any(n in FLOOR_NAMES or n.startswith("world:") for n in failed):
        return None
    none = "0 hpc-bridge calls"
    if o.operator in CLAUDE_OPERATORS:
        return f"{o.operator}: {_short(o.api_error)} ({none})" if o.api_error else None
    if o.terminal_error:
        return f"{o.operator}: {_short(o.terminal_error)} ({none})"
    sig = signature(o.final_text)
    if sig:
        return f"{o.operator}: its final answer is a model-endpoint error, {sig!r} ({none})"
    if o.tool_calls:
        return None   # it acted with its own tools: an ending with no endpoint error on record is the agent's, graded
    seen = signature(o.tails)
    if o.rc not in (None, 0):
        why = repr(seen) if seen else _short(_last_line(o.tails), 160)
        return f"{o.operator} exited rc={o.rc}" + (f": {why}" if why else "") + f" ({none})"
    if o.is_error:
        return f"{o.operator}: the run errored before any tool call" + (f": {seen!r}" if seen else "") + f" ({none})"
    if not o.final_text.strip():
        return f"{o.operator}: an empty final answer" + (f" ({seen!r})" if seen else "") + f" ({none})"
    return None
