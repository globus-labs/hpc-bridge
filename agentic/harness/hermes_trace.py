"""Adapter: a hermes-agent run's state.db (SQLite) -> normalised Trace (invariants.py).

hermes records the whole conversation in ``$HERMES_HOME/state.db`` (``~/.hermes/state.db`` by default):
each ``messages`` row is one turn — an ``assistant`` row carries ``tool_calls`` (a JSON array of
OpenAI-style function calls: ``id`` + ``function.name`` + ``function.arguments``) and/or prose in
``content``; a ``tool`` row carries the result (``tool_call_id`` pairs back to the call, ``content`` is
the tool output — MCP results wrapped in an ``<untrusted_tool_result source="…">…</untrusted_tool_result>``
envelope). We pair them into ToolCalls and collect the assistant prose into ``Trace.texts`` — the SAME
shape the Claude SDK adapter (``trace_adapter``) produces, so the SAME invariants grade both operators.

hermes' own built-in tools (``tool_search`` for deferred-tool discovery, ``todo_list``, …) appear as
ToolCalls too; graders key on the hpc-bridge logical names (``list_facilities``, ``run_shell``, …) and
simply don't match those, exactly as the Claude trace's ``Bash``/``Read`` calls don't match.

This couples grading to hermes' on-disk schema; the harness-agnostic alternative (a tap at the MCP
stdio boundary) is a later refinement. Pure stdlib, so the hermetic tier can test it.
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from invariants import ToolCall, Trace

# The envelope hermes wraps every MCP tool result in (prompt-injection framing). We grade the payload,
# so we unwrap it; a tool-loop warning hermes appends lands OUTSIDE the closing tag and is dropped.
_UNTRUSTED = re.compile(r"<untrusted_tool_result\b[^>]*>(.*?)</untrusted_tool_result>", re.S)
_DECODER = json.JSONDecoder()


def _first_json(text: str) -> Any | None:
    """The first JSON value in ``text`` (a result may be several JSON objects — e.g. list_facilities'
    per-facility blocks — or JSON followed by a trailing hermes warning). Returns None if none parses."""
    for i, ch in enumerate(text):
        if ch in "{[":
            try:
                obj, _ = _DECODER.raw_decode(text, i)
                return obj
            except json.JSONDecodeError:
                continue
    return None


def _unwrap(name: str, args: dict) -> tuple[str, dict]:
    """hpc-bridge tools are DEFERRED in hermes (discovered via ``tool_search``), so the model may invoke one
    directly (``function.name = mcp__hpc_bridge__list_facilities``) OR through hermes' generic dispatcher
    (``function.name = tool_call`` with ``arguments = {"name": "mcp__hpc_bridge__…", "arguments": {…}}``) —
    gpt-oss-120b does both across runs. Unwrap the dispatcher so the REAL tool + input reach the Trace; a
    bare tool_call without a nested name (shouldn't happen) passes through unchanged."""
    if name == "tool_call" and isinstance(args, dict) and args.get("name"):
        # the nested args key varies by model: gpt-oss uses "arguments", Devstral/Mistral uses "parameters"
        inner = args.get("arguments")
        if inner is None:
            inner = args.get("parameters")
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except (json.JSONDecodeError, TypeError):
                inner = {}
        return str(args["name"]), (inner if isinstance(inner, dict) else {})
    return name, args


def _unwrap_dispatcher_result(obj: dict) -> dict:
    """hermes' generic ``tool_call`` dispatcher returns the tool's output wrapped as
    ``{"result": "<the MCP result, re-encoded as a JSON string>"}`` — so a tool invoked through it
    (which gpt-oss does) arrives DOUBLE-encoded, and a grader keyed on ``result["phase"]`` / ``["status"]``
    sees nothing. Unwrap ONLY when the ``result`` value is a JSON string that parses to a dict (the
    dispatcher envelope); a legitimate string field like run_shell's command output (not JSON) is left
    alone, so a directly-invoked tool's result passes through untouched."""
    inner = obj.get("result")
    if isinstance(inner, str):
        parsed = _first_json(inner)
        if isinstance(parsed, dict):
            return parsed
    return obj


def result_to_dict(content: str | None) -> dict | None:
    """A hermes ``tool`` row's ``content`` -> parsed dict, mirroring ``trace_adapter._result_to_dict``.
    Strips the untrusted-result envelope, takes the first JSON value, and unwraps the tool_call
    dispatcher's ``{"result": …}`` envelope; non-JSON output is wrapped as ``{"text": …}`` (bounded)
    so a grader can still read what was said."""
    if not content:
        return None
    m = _UNTRUSTED.search(content)
    text = (m.group(1) if m else content).strip()
    if not text:
        return None
    obj = _first_json(text)
    if obj is None:
        return {"text": text[:2000]}
    if not isinstance(obj, dict):
        return {"value": obj}
    return _unwrap_dispatcher_result(obj)


def load_messages(db_path: str | Path) -> list[dict]:
    """The run's messages as plain dicts, in order — the trace source AND the bundle transcript.
    Ordered by ``id`` (insertion order); a single jail container holds exactly one hermes run."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT id, session_id, role, content, tool_calls, tool_name, tool_call_id "
            "FROM messages ORDER BY id"
        ).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]


def trace_from_messages(rows: list[dict]) -> Trace:
    """Pair hermes ``assistant`` tool_calls with their ``tool`` results into a Trace of ToolCalls,
    collecting assistant prose into ``texts``."""
    calls: list[ToolCall] = []
    by_id: dict[str, ToolCall] = {}
    texts: list[str] = []
    for r in rows:
        role = r.get("role")
        if role == "assistant":
            raw = r.get("tool_calls")
            if raw:
                try:
                    tcs = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    tcs = []
                for tc in tcs if isinstance(tcs, list) else []:
                    fn = tc.get("function") or {}
                    name = fn.get("name") or ""
                    try:
                        args = json.loads(fn.get("arguments") or "{}")
                    except (json.JSONDecodeError, TypeError):
                        args = {}
                    if not isinstance(args, dict):
                        args = {"value": args}
                    name, args = _unwrap(name, args)
                    call = ToolCall.of(name, args)
                    calls.append(call)
                    cid = tc.get("id") or tc.get("call_id")
                    if cid:
                        by_id[cid] = call
            txt = (r.get("content") or "").strip()
            if txt:
                texts.append(txt)
        elif role == "tool":
            call = by_id.get(r.get("tool_call_id"))
            if call is not None and call.result is None:
                call.result = result_to_dict(r.get("content"))
    return Trace(calls, texts)


def trace_from_hermes_db(db_path: str | Path) -> Trace:
    return trace_from_messages(load_messages(db_path))


def stamp_exchanges(trace: Trace, exchanges: list[dict] | None) -> Trace:
    """Represent the human-sim's PROSE Q&A as synthetic ``AskUserQuestion`` ToolCalls in the trace, so the
    interactive graders (spend_follows_question / choice_respected / refusal_exercised / no_spend_after_decline
    — all keyed on ``t.named("AskUserQuestion")`` with the answer in ``ToolCall.answers``) work for the hermes
    operator exactly as for Claude. hermes has no AskUserQuestion tool: the operator asks in prose and the
    human-sim replies in prose. Each exchange = ``{call_index, question, answer, kind}``; the synthetic call is
    inserted right AFTER ``call_index`` (the trace position where the operator asked). Highest index first so
    earlier inserts don't shift later ones. No structured options are attached (prose has none), which is safe:
    choice_respected only flags a provisioned partition that matches a NON-chosen OPTION label, so with no
    options it cannot false-positive.

    A NUDGE (kind "nudge" — the ACP turn policy told a paused operator to carry on) is stamped as a ``user_nudge``
    marker (like the runner's ``user_interjection``), NOT as an AskUserQuestion: the operator put nothing to the
    user, so it must not count as a spend question answered — "Login node is up, next I'll provision" + "carry on"
    would otherwise satisfy `spend_follows_question` through the broad spend-ish regex (`node`, `provision`)."""
    todo = [e for e in (exchanges or []) if e.get("question") or e.get("kind") == "nudge"]
    for e in sorted(todo, key=lambda e: -int(e.get("call_index") or 0)):
        k = int(e.get("call_index") or 0)
        at = min(k + 1, len(trace.calls))
        q = str(e.get("question") or "")
        a = str(e.get("answer") or "")
        if e.get("kind") == "nudge":
            tc = ToolCall(name="user_nudge", input={"text": a}, result={"after_call": k, "paused_on": q[-300:]},
                          raw_name="user_nudge")
        else:
            tc = ToolCall.of("AskUserQuestion", {"questions": [{"question": q}]}, answers={q: a})
        trace.calls.insert(at, tc)
    return trace


def exchanges_from_messages(rows: list[dict], replies: list[dict]) -> list[dict]:
    """Build ``stamp_exchanges`` records by correlating the human-sim's replies to the operator's prose questions
    using the FLUSHED state.db message order — the robust path for the ACP operator (no mid-run state.db reads,
    no ACP-capture-vs-trace count skew, no streamed-chunk merge).

    Each human reply was sent as the next prompt, so it lands as a ``user`` row AFTER the first (the original
    task); the Nth such row pairs with ``replies[N]`` (order-preserving; ``replies`` = ``{answer, kind}`` in the
    order the human-sim produced them). The QUESTION is the assistant prose right before that reply — one clean
    message, not merged chunks — so ``_is_spend_question`` classifies the real ask. The trace position is the
    number of tool-calls emitted before the reply, counted EXACTLY as ``trace_from_messages`` counts them (each
    entry of every assistant ``tool_calls`` array), so ``call_index = count - 1`` places the synthetic
    AskUserQuestion right where the operator asked — before any later billed ``ensure_endpoint_up``."""
    exchanges: list[dict] = []
    call_count = 0
    last_prose = ""
    user_seen = 0
    reply_i = 0
    for r in rows:
        role = r.get("role")
        if role == "assistant":
            raw = r.get("tool_calls")
            if raw:
                try:
                    tcs = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    tcs = []
                call_count += len(tcs) if isinstance(tcs, list) else 0
            txt = (r.get("content") or "").strip()
            if txt:
                last_prose = txt
        elif role == "user":
            user_seen += 1
            if user_seen == 1:
                continue                      # the original task prompt, not a human-sim reply
            if reply_i < len(replies):
                rep = replies[reply_i]
                exchanges.append({
                    "call_index": max(0, call_count - 1),
                    "question": last_prose[-1000:],
                    "answer": rep.get("answer", ""),
                    "kind": rep.get("kind"),
                })
                reply_i += 1
    return exchanges
