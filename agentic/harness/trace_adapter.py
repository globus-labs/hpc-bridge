"""Adapter: Claude Agent SDK message stream -> normalised Trace (invariants.py).

Tool calls come from ``AssistantMessage`` content blocks (a ToolUseBlock carries
``name`` + ``input`` + ``id``); tool outputs come from ``UserMessage`` content
blocks (a ToolResultBlock carries ``tool_use_id`` + ``content``) and are paired
back into ``ToolCall.result``. Block types are **duck-typed** (by attribute), so
this survives SDK version/class-name drift. The runner feeds the raw message list
here; ``invariants.py`` then asserts over the returned ``Trace``.

Named ``trace_adapter`` (not ``trace``) to avoid shadowing the stdlib ``trace``.
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from invariants import ToolCall, Trace


def _is_tool_use(b: Any) -> bool:
    return hasattr(b, "name") and hasattr(b, "input") and hasattr(b, "id")


def _is_tool_result(b: Any) -> bool:
    return hasattr(b, "tool_use_id")


def _result_to_dict(content: Any) -> dict | None:
    """MCP tool results arrive as a text string (usually JSON) or a list of blocks
    (live SDK objects, or dict-form when re-read from a bundle). Parse to a dict when we
    can; otherwise wrap the raw text."""
    text: str | None = None
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts = []
        for item in content:
            t = getattr(item, "text", None)
            if t is None and isinstance(item, dict):
                t = item.get("text")
            if t:
                parts.append(t)
        text = "\n".join(parts) if parts else None
    if not text:
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {"value": obj}
    except (json.JSONDecodeError, TypeError):
        return {"text": text}


def build_trace(
    messages: Iterable[Any],
    injected_answers: dict[str, dict[str, str]] | None = None,
    arrivals: list[float | None] | None = None,
) -> Trace:
    """Normalise an SDK message stream into a Trace of ToolCalls (with results paired).

    ``injected_answers`` (tool_use_id -> answers) is the harness' structural record of what
    the human-sim answered — attached to the matching ToolCall so grading is independent of
    the CLI's answer-rendering. ``arrivals`` (parallel to ``messages``: seconds since the session
    started) stamps each call's ``t_call`` / ``t_result``; absent => the stamps stay None."""
    injected_answers = injected_answers or {}
    arrivals = arrivals or []
    calls: list[ToolCall] = []
    by_id: dict[str, ToolCall] = {}
    texts: list[str] = []
    for n, msg in enumerate(messages):
        t_at = arrivals[n] if n < len(arrivals) else None
        content = getattr(msg, "content", None)
        if not isinstance(content, list):
            continue
        for b in content:
            if type(msg).__name__ == "AssistantMessage" and not _is_tool_use(b) and not _is_tool_result(b):
                txt = getattr(b, "text", None)
                if isinstance(txt, str) and txt.strip():
                    texts.append(txt)
                continue
            if _is_tool_use(b):
                bid = getattr(b, "id", None)
                tc = ToolCall.of(
                    getattr(b, "name", "") or "",
                    dict(getattr(b, "input", {}) or {}),
                    answers=injected_answers.get(bid) if bid else None,
                )
                tc.t_call = t_at
                calls.append(tc)
                if bid:
                    by_id[bid] = tc
            elif _is_tool_result(b):
                tc = by_id.get(getattr(b, "tool_use_id", None))
                if tc is not None and tc.result is None:
                    tc.result = _result_to_dict(getattr(b, "content", None))
                    tc.t_result = t_at
    return Trace(calls, texts)


def trace_from_bundle(bundle_dir) -> Trace:
    """Rebuild a Trace from a stored provenance bundle's messages.jsonl (dict-form blocks,
    as serialized by provenance._jsonable) — the offline re-grading path.

    Chain phases are recovered from the stream itself: each agent session opens with a
    `SystemMessage(subtype="init")` carrying its `session_id`, and a PHASES run concatenates one
    session per phase (run.py `_combine`), so the k-th DISTINCT session id marks phase k. Keyed
    on the id, not a bare init count — a single session can re-emit `init` (same id) mid-stream,
    which must not split a one-phase run in two. Mirrors the live stamp in `_combine`."""
    from pathlib import Path

    calls: list[ToolCall] = []
    by_id: dict[str, ToolCall] = {}
    texts: list[str] = []
    sessions: list[str] = []   # distinct session ids, in order of first appearance
    phase = 0
    with (Path(bundle_dir) / "messages.jsonl").open() as fh:
        for line in fh:
            m = json.loads(line)
            t_at = m.get("__t__")   # arrival stamp (provenance writes it; older bundles have none)
            t_at = float(t_at) if isinstance(t_at, int | float) else None
            if m.get("__type__") == "SystemMessage" and m.get("subtype") == "init":
                sid = str((m.get("data") or {}).get("session_id") or "")
                if sid not in sessions:
                    sessions.append(sid)
                    phase = len(sessions) - 1
                continue
            content = m.get("content")
            if not isinstance(content, list):
                continue
            for b in content:
                if not isinstance(b, dict):
                    continue
                if m.get("__type__") == "AssistantMessage" and b.get("__type__") == "TextBlock":
                    if str(b.get("text") or "").strip():
                        texts.append(str(b.get("text")))
                    continue
                if b.get("__type__") == "ToolUseBlock":
                    tc = ToolCall.of(b.get("name", "") or "", dict(b.get("input") or {}), phase=phase)
                    tc.t_call = t_at
                    calls.append(tc)
                    if b.get("id"):
                        by_id[b["id"]] = tc
                elif b.get("__type__") == "ToolResultBlock":
                    tc = by_id.get(b.get("tool_use_id"))
                    if tc is not None and tc.result is None:
                        tc.result = _result_to_dict(b.get("content"))
                        tc.t_result = t_at
    return Trace(calls, texts)


def insert_interjections(trace: Trace, events: list[dict] | None) -> Trace:
    """Stamp the user's mid-run INTERJECTIONS into the trace as synthetic `user_interjection` calls, each placed right
    after the call whose result triggered it, so graders can order the agent's actions against what the user said
    (`revocation_honoured`). Events come from the runner (`interject` hooks: {text, call_index}) live, or from a
    bundle's `events` (the same hooks, carrying `interject`) on regrade. No-op on an empty list."""
    todo = [e for e in (events or []) if e.get("text") or e.get("interject")]
    for e in sorted(todo, key=lambda e: -int(e.get("call_index") or 0)):   # highest first: earlier inserts don't shift later ones
        k = int(e.get("call_index") or 0)
        at = min(k + 1, len(trace.calls))
        phase = trace.calls[k].phase if 0 <= k < len(trace.calls) else 0
        trace.calls.insert(at, ToolCall(name="user_interjection", input={"text": str(e.get("text") or e.get("interject"))},
                                        result={"after_call": k}, raw_name="user_interjection", phase=phase))
    return trace
