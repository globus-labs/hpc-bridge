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
  `harness:trace_complete`, `harness:midrun_hooks`, …) — except a world check that failed only because nothing ran
  (`absences`): a scenario postcheck that only expects something the agent's work would have left (`expect_present`,
  no `expect_absent` / `expect_empty`) and found it missing. In a run with no tool call that says what INFRA says;
  a check that guards against harm names what must not be there, and still decides;
- the model endpoint refusing the request Codex's relay sent (`relay_rejection`: a 4xx, or vLLM's 400 inside ALCF's
  gateway 500, which Codex reports as "high demand") is the harness's failure, or a tool schema's — never INFRA;
- a turn the harness cut off (rc 124) is never INFRA, nor a run whose record shows an MCP server failing to start (an
  hpc-bridge boot crash is what `agent_engaged` exists to catch);
- the evidence: the error the operator's own record says its run ENDED on (Pi's last `message_end` / `auto_retry_end`,
  Codex's `turn.failed`) reads as an outage (an HTTP 5xx or 429, a timeout, a dead stream, "high demand"); or the final
  answer is itself a stream failure (`StreamingTimeoutError`, `Stream ended without finish_reason`); or, in a run that
  otherwise ended cleanly, the final answer is a tool call that never became one (`printed_tool_call`: raw harmony
  tool-call tokens the model server failed to parse, or a call written out as JSON instead of made — the maintainer's
  call: INFRA too, reported apart); or the run ended badly (non-zero exit, an error, an empty answer) AND the last line
  of the operator's stderr names an outage. An error the operator recovered from decides nothing, and a 4xx (context
  overflow, an invalid tool schema — hpc-bridge's own tools can cause those) is not an outage, even when the message
  quotes a 5xx after it;
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
# A final answer that is a tool call which never became one (live 2026-10-15, Codex on ALCF's gpt-oss: six cells ended
# after one model response, with no call of any kind and this as the final message; the relay's log shows vLLM's
# output as `["reasoning", "message"]`, no function_call item). Two forms, told apart in the reason:
# - raw HARMONY tokens: `<|start|>assistant<|channel|>commentary to=functions.mcp__hpc_bridge__ensure_endpoint_up<|call|>`
#   — a call in gpt-oss's own format that the server's parser left unparsed: an unambiguous leak. Needs the recipient
#   `to=functions.…` together with `<|call|>` or `<|channel|>`;
# - the call written as JSON: `{"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-ssh-site"}`, once
#   `{"cmd":["mcp__hpc_bridge__connect_facility","{"facility":…}"]}` with its inner quotes unescaped (not even JSON, so
#   a shape test, not json.loads). This may be the model writing the call on its FINAL channel (a format error of the
#   model's) rather than a parser leak; either way nothing reached a tool. INFRA by the maintainer's decision, with its
#   own reason. Tight: the WHOLE answer is one object whose FIRST key is call-ish (`cmd`, `name`, `tool`, `function`,
#   `recipient`) and holds an `mcp__…` / `functions.…` name (for `cmd`, its list's first item too). Not a sentence
#   naming a tool, a call quoted in prose or a fence, `{"next_step": "mcp__…", "reason": …}`, or a report led by
#   anything else (review of e126f41).
_PRINTED_CALL_JSON = re.compile(r'\A\{\s*(?:"(?:cmd|name|tool|function|recipient)"\s*:\s*|"cmd"\s*:\s*\[\s*)'
                                r'"((?:mcp__|functions\.)[\w.-]+)"')
_HARMONY_CALL = re.compile(r"<\|(?:call|channel)\|>")
_HARMONY_RECIPIENT = re.compile(r"\bto=(functions\.[\w.-]+)")
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


def printed_tool_call(text: str) -> tuple[str, str] | None:
    """A final answer that IS a tool call never made: ("harmony", the recipient) for raw harmony tool-call tokens,
    ("json", the tool) for a call written out as JSON, or None for anything else."""
    s = (text or "").strip()
    recipient = _HARMONY_RECIPIENT.search(s)
    if recipient and _HARMONY_CALL.search(s):
        return "harmony", recipient.group(1)
    if s.endswith("}") and (m := _PRINTED_CALL_JSON.match(s)):
        return "json", m.group(1)
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
    stderr: str = ""              # the operator's stderr tail: evidence only for a run that ended badly
    stdout: str = ""              # the operator's stdout tail: read only for an MCP server that failed to start
    relay_error: str = ""         # Codex: the model endpoint refused the relay's last request (`relay_rejection`)


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


def terminal_error(harness: str, stdout: str) -> str:
    """The error a CLI operator's own event stream says its run ENDED on (Pi, Codex), else ""."""
    return pi_terminal_error(stdout) if harness == "pi" else codex_terminal_error(stdout) if harness == "codex" else ""


_UPSTREAM_STATUS = re.compile(r"\bstatus code:?\s*(\d{3})\b", re.I)


def _json_string_at(text: str, key: str) -> str | None:
    """The first `"<key>": "…"` string value in `text`, unescaped — also when the JSON around it was cut off (the relay
    keeps an error body's first 1500 bytes)."""
    m = re.search(rf'"{re.escape(key)}"\s*:\s*"', text or "")
    if m is None:
        return None
    i, out = m.end(), []
    while i < len(text):
        c = text[i]
        if c == '"':
            break
        step = 6 if text.startswith("\\u", i) else 2 if c == "\\" else 1
        if i + step > len(text):
            break   # an escape the cut split
        out.append(text[i:i + step])
        i += step
    try:
        return json.loads('"' + "".join(out) + '"')
    except ValueError:
        return None


def _upstream_words(body: str) -> tuple[int | None, str]:
    """(the status a gateway's error says the model server answered, the model server's own message) of an error body."""
    msg = _json_string_at(body, "message") or body
    code = _UPSTREAM_STATUS.search(msg)
    inner = re.search(r"Response text:\s*(.+)", msg)
    words = (_json_string_at(inner.group(1), "message") or inner.group(1)) if inner else msg
    return (int(code.group(1)) if code else None), " ".join(words.split())


def relay_rejection(hops: Iterable[Any]) -> str:
    """Codex's relay log (responses_relay: one record per request it forwarded): how the model endpoint REFUSED the
    LAST request as malformed — "HTTP 500 wrapping 400: 1 validation error for Message …" — or "". A 4xx other than
    408 / 429, as the endpoint answered it, or quoted by ALCF's gateway inside its own 5xx (`internal_endpoint_error` …
    "API request failed with status code: 400": vLLM's validation of what the relay sent). 16 stored Codex bundles end
    so (to 2026-10-16): vLLM refused Codex's list-form tool outputs (fixed by #209), and Codex reported the gateway's
    500s as "We're currently experiencing high demand". The relay is the harness: a request it sent wrong is its failure,
    or a tool schema's, never an outage. A request that succeeded, a 5xx with no 4xx inside it, or the relay's own 502
    (it could not reach the endpoint) is ""."""
    last = next((h for h in reversed(list(hops or [])) if isinstance(h, dict)
                 and str(h.get("path") or "").rstrip("/").endswith("/responses")), None)
    try:
        status = int(last.get("status")) if last is not None else 0
    except (TypeError, ValueError):
        return ""
    if last is None or status < 400:
        return ""
    code, words = _upstream_words(str(last.get("error") or ""))
    refused = status if status < 500 else code
    if refused is None or not 400 <= refused < 500 or refused in (408, 429):
        return ""
    how = f"HTTP {status}" + (f" wrapping {refused}" if refused != status else "")
    problem = f"; the relay could not adapt it: {last['problem']}" if last.get("problem") else ""
    return f"{how}: {_short(words, 140)}{problem}"


def ended_on(harness: str, stdout: str, hops: Iterable[Any] = ()) -> str:
    """What a CLI operator's run ENDED on, in words — `run_completed`'s reason when the run wrote no final answer or
    errored (cli_runner): the request the model endpoint refused (Codex's relay log), then the error the operator's own
    event stream ended on (Codex `turn.failed`, Pi's last error); "" when neither says — the caller then falls back to
    the stderr tail, which in every Codex cell is only "Reading additional input from stdin..." (Codex reading stdin)."""
    relay = relay_rejection(hops) if harness == "codex" else ""
    term = terminal_error(harness, stdout)
    parts = [f"the model endpoint refused the relay's request ({relay})"] if relay else []
    if term:
        parts.append(f"{harness} {'turn.failed' if harness == 'codex' else 'ended on'}: {_short(term, 160)}")
    return "; ".join(parts)


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
        relay = (relay_rejection(m["relay"] for m in messages if isinstance(m, dict) and "relay" in m)
                 if harness == "codex" else "")
        return Outcome(operator, calls, every, final_text, rc, bool(rc), terminal_error(harness, stdout), "",
                       stderr[-2000:], stdout[-4000:], relay)
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


_HARM_KEYS = frozenset({"expect_absent", "expect_empty", "pilots"})


def absence_checks(postchecks: Iterable[Any]) -> frozenset[str]:
    """The `world:` rows of a scenario's POSTCHECKS that only expect something PRESENT — `expect_present`, and neither
    `expect_absent` nor `expect_empty`: what the agent's work should have left (a marker, a log line, a cancelled block).
    A check that guards against harm names what must not be there (a pilot, a manager, an endpoint dir, egress, a
    removed file) and is never one of these; nor is the universal pilot check, nor a scenario's WORLD_CHECKS row."""
    return frozenset(f"world:{pc['name']}" for pc in postchecks or []
                     if isinstance(pc, dict) and pc.get("name") and "expect_present" in pc and not _HARM_KEYS & pc.keys())


def absent_only(detail: str, expected: str) -> bool:
    """A postcheck failure (run.py `_postchecks`: "<why>; <why> — output: …") whose ONE reason is the lack of its
    `expect_present` text: "missing '<expected>'" — not a failed or unverifiable command (`rc=…`, `UNVERIFIABLE — ssh
    rc=255`) beside it, nor something found that must not be there."""
    return str(detail or "").split(" — output: ", 1)[0].strip() == f"missing {expected!r}"


def absences(failed_rows: Iterable[tuple[str, str]], postchecks: Iterable[Any]) -> frozenset[str]:
    """Of the failed world rows (name, detail), those that failed only because something the agent's work would have
    left is missing (`absence_checks`, `absent_only`). In a run with no tool call they say only that nothing ran: that
    does not keep a cell from INFRA (codex submit_policy_rejected 1792181630, 2026-10-16: the call written as JSON, no
    call made, and `world:policy_in_endpoint_log` "missing 'endpoint.log'" kept it FAILED)."""
    names = absence_checks(postchecks)
    expected = {f"world:{pc['name']}": str(pc["expect_present"]) for pc in postchecks or []
                if isinstance(pc, dict) and f"world:{pc.get('name')}" in names}
    return frozenset(n for n, detail in failed_rows if n in expected and absent_only(detail, expected[n]))


def infra_reason(o: Outcome, failed: Iterable[str] = (), absent: Iterable[str] = ()) -> str | None:
    """Why this run is INFRA (the operator produced nothing to grade because its model endpoint failed: there is
    positive evidence of the outage, and the agent did nothing), or None — it is graded as it stands. `failed`: the
    gating checks that broke. `absent`: those of them that are world checks which failed only because nothing ran
    (`absences`) — read only once the run is known to have made no tool call, so they never excuse a run that acted."""
    if o.tool_calls > 0 or o.hpcb_calls > 0:
        return None   # it acted: graded, whatever happened to its model afterwards
    if o.operator == "script":
        return None   # the scripted operator has no model endpoint to fail: a script that did nothing is graded
    excused = frozenset(n for n in absent if n.startswith("world:"))   # never the floor, never a harness check
    if any(_protected(n) and n not in excused for n in failed):
        return None
    if o.rc == 124:
        return None   # the harness cut the turn off: not the endpoint's doing on any evidence we hold
    if _MCP_STARTUP.search("\n".join((o.stdout, o.stderr, o.terminal_error, o.final_text))):
        return None   # an MCP server failed to start — hpc-bridge's (or the harness's) failure, graded
    if o.relay_error:
        return None   # the endpoint refused the request the relay sent: the harness's (or a tool schema's) doing
    none = "0 tool calls"
    if o.operator in CLAUDE_OPERATORS:
        return f"{o.operator}: {_short(o.api_error)} ({none})" if o.api_error else None
    if o.terminal_error and signature(o.terminal_error):
        return f"{o.operator}: {_short(o.terminal_error)} ({none})"
    if (sig := stream_death(o.final_text)):
        return f"{o.operator}: its final answer is a dead model stream, {sig!r} ({none})"
    # A call that never became one: nothing reached a tool, so there is nothing of the agent's to grade. Read only
    # here, after `tool_calls == 0` (a printed call after a real one is graded), and only for a run that otherwise
    # ended cleanly: a terminal error the outage signatures did not take above (a 4xx — an invalid tool schema can be
    # hpc-bridge's own fault), a non-zero exit or an error is never excused by what the answer looks like.
    clean_end = not o.terminal_error and o.rc in (None, 0) and not o.is_error
    if clean_end and (call := printed_tool_call(o.final_text)):
        kind, name = call
        if kind == "harmony":
            return f"{o.operator}: the model server returned raw harmony tool-call tokens (unparsed): {name!r} ({none})"
        return (f"{o.operator}: the model answered with a tool call written as JSON instead of calling it: {name!r} "
                f"({none})")
    # A bad ending, and the LAST thing the operator wrote to stderr names an outage: an earlier line may be a retry it
    # recovered from ("retrying after HTTP 503 (1/3)" … "request ok"), and a recovered error decides nothing.
    ended_badly = (o.rc not in (None, 0)) or bool(o.is_error) or not o.final_text.strip()
    if ended_badly and (sig := signature(_last_line(o.stderr))):
        how = f"exited rc={o.rc}" if o.rc not in (None, 0) else "errored" if o.is_error else "gave an empty final answer"
        return f"{o.operator} {how}; its stderr: {sig!r} ({none})"
    return None
