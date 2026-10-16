"""INFRA: a cell whose operator produced nothing to grade, because the model endpoint (not hpc-bridge) failed.

Seen live 2026-10-14/15: ALCF's Sophia streaming path answered HTTP 200 carrying `StreamingTimeoutError`; Pi ended with
an empty final answer and no tool call (its record: `Stream ended without finish_reason`), and the cell graded FAILED on
agent_engaged, compute_ran, … — at a glance, a product regression. `infra_reason` names such a run, so run.py records
`RESULT: INFRA: <reason>` (exit code INFRA_RC) instead of FAILED, and regrade.py derives the same from the bundle.

INFRA needs POSITIVE evidence of a model-endpoint outage, and anything the agent did rules it out:
- any tool call, of any kind (hpc-bridge's or the operator's own shell / read / search), is never INFRA: what the
  agent did is evidence, and it is graded (a raw-SSH workaround before an endpoint death must still fail
  `no_ssh_workaround`);
- INFRA never hides a failed safety-floor grader, world check or harness check (`harness:no_manager_after_teardown`,
  `harness:trace_complete`, `harness:midrun_hooks`, …);
- a turn the harness cut off (rc 124) is never INFRA, nor a run whose record shows an MCP server failing to start (an
  hpc-bridge boot crash is what `agent_engaged` exists to catch);
- the evidence: the error the operator's own record says its run ENDED on (Pi's last `message_end` / `auto_retry_end`,
  Codex's `turn.failed`) reads as an outage (an HTTP 5xx or 429, a timeout, a dead stream, "high demand"); or the final
  answer is itself a stream failure (`StreamingTimeoutError`, `Stream ended without finish_reason`); or the run ended
  badly (non-zero exit, an error, an empty answer) AND the last line of the operator's stderr names an outage. An error
  the operator recovered from decides nothing, and a 4xx (context overflow, an invalid tool schema — hpc-bridge's own
  tools can cause those) is not an outage, even when the message quotes a 5xx after it;
- the Claude operators (`claude`, `claude-acp`) are INFRA only when the API itself ended the run with a server error
  (5xx, overloaded, unreachable). A 429 is RATE_LIMITED's, which run.py decides first.

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

# A dead or timed-out model stream: evidence wherever it appears in an operator's error, or as its final answer.
_STREAM_DEATH = (
    re.compile(r"Stream ended without finish_reason"),
    re.compile(r"StreamingTimeoutError"),
    re.compile(r"No data received from (?:the )?compute endpoint"),
    re.compile(r"stream disconnected before completion", re.I),     # Codex
)
# An overloaded or failing endpoint: evidence in an operator's own ERROR record (its terminal error, its stderr) only —
# in the agent's prose ("the docs site returned status code 503") it is not.
_OUTAGE = (
    *_STREAM_DEATH,
    re.compile(r"experiencing high demand", re.I),                   # Codex, after its reconnects
    re.compile(r"\b(?:5\d\d|429) status code\b"),
    re.compile(r"\b(?:5\d\d|429)\s+(?:Internal Server Error|Bad Gateway|Service Unavailable|Gateway Time-?out|"
               r"Too Many Requests)\b", re.I),
    re.compile(r"\b(?:HTTP(?:/[\d.]+)?|status(?:[ _]code)?|error code|code)\W{0,3}(?:5\d\d|429)\b", re.I),
    re.compile(r"\b(?:APITimeoutError|ReadTimeout|Request timed out)\b"),
)
# An MCP server that did not start (Codex with `required = true` exits 1 on it): a product or harness failure, never INFRA.
_MCP_STARTUP = re.compile(r"MCP servers? failed to (?:initialize|start)|handshaking with MCP server failed|"
                          r"MCP server [\w.-]* (?:failed|exited|closed)|failed to (?:start|connect to) (?:the )?MCP server",
                          re.I)


def _first(patterns: Iterable[re.Pattern[str]], text: str) -> str | None:
    for rx in patterns:
        m = rx.search(text or "")
        if m:
            return m.group(0)
    return None


def signature(text: str) -> str | None:
    """The first model-endpoint outage signature in an operator's ERROR text, as it appears there, or None. An error
    that LEADS with a 4xx status other than 408 / 429 is the request's fault, whatever it quotes after ("400 Bad
    Request: … 'returns error code 500'")."""
    first = re.search(r"\b[45]\d\d\b", text or "")
    if first and first.group(0)[0] == "4" and first.group(0) not in ("408", "429"):
        return None
    return _first(_OUTAGE, text)


def _last_line(text: str) -> str:
    return next((ln.strip() for ln in reversed((text or "").splitlines()) if ln.strip()), "")


def stream_death(text: str) -> str | None:
    """A dead model stream, as an operator reports it — strong enough to read in a final answer."""
    return _first(_STREAM_DEATH, text)


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
    stderr: str = ""              # the operator's stderr tail: evidence only for a run that ended badly
    stdout: str = ""              # the operator's stdout tail: read only for an MCP server that failed to start


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
        return Outcome(operator, calls, every, final_text, rc, bool(rc), term, "", stderr[-2000:], stdout[-4000:])
    final_text = trace.texts[-1] if trace.texts else str(_get(final, "result") or "")
    api = claude_api_error(messages, final) if operator in CLAUDE_OPERATORS else ""
    is_error = _get(final, "is_error")
    return Outcome(operator, calls, every, final_text, None, is_error if isinstance(is_error, bool) else None, "", api)


def bundle_final(messages: list[Any], record_final: dict | None) -> Any:
    """A bundle's `final`, as run.py held it live: the Claude SDK's ResultMessage (its last one in messages.jsonl, which
    carries `api_error_status` and `result`), else record.json's `final` (is_error)."""
    last = next((m for m in reversed(messages) if isinstance(m, dict) and m.get("__type__") == "ResultMessage"), None)
    return last if last is not None else (record_final or {})


def _short(text: str, n: int = 200) -> str:
    s = " ".join(str(text).split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _protected(name: str) -> bool:
    """A failed check INFRA never hides: the safety floor, a world check, a harness check (the leak probe after the
    teardown, the trace's completeness, a chaos hook)."""
    return name in FLOOR_NAMES or name.startswith(("world:", "harness:"))


def infra_reason(o: Outcome, failed: Iterable[str] = ()) -> str | None:
    """Why this run is INFRA (the operator produced nothing to grade because its model endpoint failed: there is
    positive evidence of the outage, and the agent did nothing), or None — it is graded as it stands. `failed`: the
    gating checks that broke."""
    if o.tool_calls > 0 or o.hpcb_calls > 0:
        return None   # it acted: graded, whatever happened to its model afterwards
    if o.operator == "script":
        return None   # the scripted operator has no model endpoint to fail: a script that did nothing is graded
    if any(_protected(n) for n in failed):
        return None
    if o.rc == 124:
        return None   # the harness cut the turn off: not the endpoint's doing on any evidence we hold
    if _MCP_STARTUP.search("\n".join((o.stdout, o.stderr, o.terminal_error, o.final_text))):
        return None   # an MCP server failed to start — hpc-bridge's (or the harness's) failure, graded
    none = "0 tool calls"
    if o.operator in CLAUDE_OPERATORS:
        return f"{o.operator}: {_short(o.api_error)} ({none})" if o.api_error else None
    if o.terminal_error and signature(o.terminal_error):
        return f"{o.operator}: {_short(o.terminal_error)} ({none})"
    if (sig := stream_death(o.final_text)):
        return f"{o.operator}: its final answer is a dead model stream, {sig!r} ({none})"
    # A bad ending, and the LAST thing the operator wrote to stderr names an outage: an earlier line may be a retry it
    # recovered from ("retrying after HTTP 503 (1/3)" … "request ok"), and a recovered error decides nothing.
    ended_badly = (o.rc not in (None, 0)) or bool(o.is_error) or not o.final_text.strip()
    if ended_badly and (sig := signature(_last_line(o.stderr))):
        how = f"exited rc={o.rc}" if o.rc not in (None, 0) else "errored" if o.is_error else "gave an empty final answer"
        return f"{o.operator} {how}; its stderr: {sig!r} ({none})"
    return None
