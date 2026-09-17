"""Claude Code's NATIVE session transcript → the normalised Trace — the Claude-side twin of `hermes_trace`.

Why this exists: the cross-harness benchmark drives every operator over ACP (`acp_client`), and the ACP update
stream is NOT a grading source — hermes' adapter drops structured results (`raw_output=None` for any JSON result,
a truncated rendering goes into `content`), and Zed's `claude-agent-acp` sets neither `rawInput` nor `rawOutput`
and gives an MCP call only its name (both verified in source, 2026-09-08). Each agent has a full-fidelity POST-RUN
source instead: hermes' `state.db` (hermes_trace) and, for Claude Code, the CLI's own session transcript under
`$CLAUDE_CONFIG_DIR/projects/<cwd-slug>/<session>.jsonl` — which the jail's entrypoint already harvests into the
bundle as `claude-session/`.

Transcript shape (CLI 2.1.x): one JSON object per line; the lines that matter have `type` "assistant" or "user" and
`message.content` = API-shaped blocks (`text`, `thinking`, `tool_use{id,name,input}`, `tool_result{tool_use_id,
content}`). An AskUserQuestion's tool_result line also carries `toolUseResult` (the structured questions/answers)
and the rendered `"question"="answer"` text the graders' `_answered_pairs` already parses as a fallback. Other line
types (ai-title, attachment, queue-operation, …) are metadata. A harvest holds the HUMAN-SIM's sessions too (the sim
is an SDK query from the same cwd): its first user message is the role-play prompt, so `select_operator_session`
excludes those and prefers the session whose first user message is the task.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from invariants import ToolCall, Trace
from trace_adapter import _result_to_dict

# The human-sim's prompts all open with this (human_sim.answer / reply / reply_hermes / move).
SIM_PROMPT_PREFIX = "You are role-playing a HUMAN USER"


def load_lines(path: Path | str) -> list[dict]:
    """The transcript's JSON lines, in file order (malformed lines skipped)."""
    out: list[dict] = []
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(d, dict):
                out.append(d)
    return out


def find_session_files(root: Path | str) -> list[Path]:
    """Every session transcript under a `.claude/projects` dir or a bundle's `claude-session/` copy."""
    return sorted(p for p in Path(root).rglob("*.jsonl") if p.is_file())


def _blocks(line: dict) -> list[dict]:
    content = (line.get("message") or {}).get("content")
    return [b for b in content if isinstance(b, dict)] if isinstance(content, list) else []


# The CLI echoes local slash commands into the transcript as user messages (`claude-agent-acp` issues `/model` at
# session start: "<local-command-caveat>…", "<command-name>/model…", "<local-command-stdout>Set model to …"). Not
# prompts — skipped when looking for what the user said.
_LOCAL_COMMAND_PREFIXES = ("<local-command", "<command-name>", "<command-message>", "<command-args>")


def user_prompt_text(line: dict) -> str | None:
    """The text of a user PROMPT line (a string, or text blocks) — None for a tool_result line or a local-command
    echo. This is what the operator was told, in order: the task first, then each human-sim reply."""
    if line.get("type") != "user":
        return None
    content = (line.get("message") or {}).get("content")
    if isinstance(content, str):
        text = content
    else:
        blocks = _blocks(line)
        if any(b.get("type") == "tool_result" for b in blocks):
            return None
        text = "\n".join(str(b.get("text") or "") for b in blocks if b.get("type") == "text")
    text = text.strip()
    if not text or text.startswith(_LOCAL_COMMAND_PREFIXES):
        return None
    return text


def first_user_text(lines: list[dict]) -> str:
    """The first real user prompt — the task for the operator, the role-play prompt for the sim."""
    for line in lines:
        t = user_prompt_text(line)
        if t is not None:
            return t
    return ""


def exchanges_from_transcript(lines: list[dict], replies: list[dict]) -> list[dict]:
    """`stamp_exchanges` records from a native transcript — the Claude-side twin of `hermes_trace.exchanges_from_messages`.
    Each human-sim reply was sent as the next prompt, so it is a user PROMPT line after the first (the task); the
    Nth pairs with `replies[N]` in order. The question is the assistant prose before it; the trace position is the
    number of tool_use blocks before it (counted exactly as `trace_from_transcript` counts calls)."""
    exchanges: list[dict] = []
    call_count = 0
    last_prose = ""
    prompts_seen = 0
    reply_i = 0
    for line in lines:
        t = line.get("type")
        if t == "assistant":
            for b in _blocks(line):
                if b.get("type") == "tool_use":
                    call_count += 1
                elif b.get("type") == "text" and str(b.get("text") or "").strip():
                    last_prose = str(b["text"])
        elif t == "user":
            if user_prompt_text(line) is None:
                continue
            prompts_seen += 1
            if prompts_seen == 1:
                continue                      # the task prompt, not a reply
            if reply_i < len(replies):
                rep = replies[reply_i]
                exchanges.append({"call_index": max(0, call_count - 1), "question": last_prose[-1000:],
                                  "answer": rep.get("answer", ""), "kind": rep.get("kind")})
                reply_i += 1
    return exchanges


def _n_tool_uses(lines: list[dict]) -> int:
    return sum(1 for line in lines if line.get("type") == "assistant"
               for b in _blocks(line) if b.get("type") == "tool_use")


def select_operator_session(files: list[Path], *, task: str | None = None) -> Path | None:
    """Pick the OPERATOR's session out of a harvest: never a human-sim session (first message = the role-play
    prompt); prefer the session whose first user message opens with the task; then the one with the most tool
    calls (a sidechain/summary session has few)."""
    best: tuple[tuple[int, int], Path] | None = None
    head = (task or "").strip()[:80]
    for f in files:
        lines = load_lines(f)
        fu = first_user_text(lines).strip()
        if fu.startswith(SIM_PROMPT_PREFIX):
            continue
        key = (1 if head and fu.startswith(head) else 0, _n_tool_uses(lines))
        if best is None or key > best[0]:
            best = (key, f)
    return best[1] if best else None


def _answers_from_tool_use_result(line: dict) -> dict[str, str] | None:
    """The structural answers on an AskUserQuestion result line (`toolUseResult.answers`), when present."""
    tur = line.get("toolUseResult")
    if isinstance(tur, str):
        try:
            tur = json.loads(tur)
        except json.JSONDecodeError:
            return None
    if isinstance(tur, dict) and isinstance(tur.get("answers"), dict) and tur["answers"]:
        return {str(k): str(v) for k, v in tur["answers"].items()}
    return None


def trace_from_transcript(lines: list[dict], *, include_sidechains: bool = True) -> Trace:
    """Normalise a native transcript into a Trace: assistant `text` blocks → texts, `tool_use` → ToolCalls,
    `tool_result` paired by id → results (via the SDK adapter's `_result_to_dict`, so a JSON result reads the same
    as from the SDK stream). AskUserQuestion answers come from `toolUseResult` when the CLI recorded them
    structurally; otherwise the graders fall back to the rendered result text. Sidechains (subagents) are included
    by default — a Task subagent's hpc-bridge calls are the operator's calls for grading purposes."""
    calls: list[ToolCall] = []
    by_id: dict[str, ToolCall] = {}
    texts: list[str] = []
    for line in lines:
        t = line.get("type")
        if t not in ("assistant", "user"):
            continue
        if not include_sidechains and line.get("isSidechain") in (True, "True", "true"):
            continue
        for b in _blocks(line):
            bt = b.get("type")
            if t == "assistant" and bt == "text":
                if str(b.get("text") or "").strip():
                    texts.append(str(b["text"]))
            elif t == "assistant" and bt == "tool_use":
                tc = ToolCall.of(str(b.get("name") or ""), dict(b.get("input") or {}))
                calls.append(tc)
                if b.get("id"):
                    by_id[str(b["id"])] = tc
            elif bt == "tool_result":
                tc = by_id.get(str(b.get("tool_use_id") or ""))
                if tc is None or tc.result is not None:
                    continue
                tc.result = _result_to_dict(b.get("content"))
                if tc.name == "AskUserQuestion":
                    answers = _answers_from_tool_use_result(line)
                    if answers:
                        tc.answers = answers
    return Trace(calls, texts)


def trace_from_harvest(root: Path | str, *, task: str | None = None) -> tuple[Trace, Path | None]:
    """The operator's Trace from a harvested `claude-session/` dir (or a live `.claude/projects`), plus which file
    it came from. An empty Trace when no operator session is found — never a sim session graded as the operator."""
    f = select_operator_session(find_session_files(root), task=task)
    if f is None:
        return Trace([], []), None
    return trace_from_transcript(load_lines(f)), f


def looks_like_transcript(first_line: dict[str, Any]) -> bool:
    """Format sniff for regrade: a native CLI transcript line (vs the SDK dict-form or hermes' state.db rows)."""
    return isinstance(first_line, dict) and "sessionId" in first_line and "type" in first_line and "__type__" not in first_line
