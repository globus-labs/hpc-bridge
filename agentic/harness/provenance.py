"""Per-run provenance bundle — the durable evidence a graded run leaves behind.

Every run (pass, fail, or crash) writes `agentic/runs/<runid>-<scenario>/`:
- record.json    — resolved config (prompt/persona/goal/model/effort/ablation/git SHA/pool
                   user/endpoint), grading verdicts, cost/usage, redacted env, dialogue.
- messages.jsonl — the COMPLETE SDK message stream, one message per line: assistant text,
                   thinking blocks (as the API returns them — summarized on Opus 4.7+),
                   tool_use inputs, tool_results. The raw material for re-grading and the
                   future LLM-judge — grading can be re-run WITHOUT re-running the agent.
- transcript.md  — human-readable rendering of the same.
- claude-session/ — harvested by entrypoint.sh after the run: the CLI's own native session
                   JSONLs from inside the jail, including the human-sim's separate sessions.

Design: prefer lossy-but-never-failing serialization (a provenance writer that can crash the
run it documents is worse than useless).
"""
from __future__ import annotations

import dataclasses
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Values of these env vars are secrets; names are still recorded (presence is provenance).
_REDACT = {"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ALCF_INFERENCE_TOKEN", "OPENAI_API_KEY"}


def _jsonable(obj: Any, depth: int = 0) -> Any:
    if depth > 10:
        return repr(obj)[:500]
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_jsonable(x, depth + 1) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _jsonable(v, depth + 1) for k, v in obj.items()}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        d = {"__type__": type(obj).__name__}
        for f in dataclasses.fields(obj):
            d[f.name] = _jsonable(getattr(obj, f.name), depth + 1)
        return d
    return {"__type__": type(obj).__name__, "repr": repr(obj)[:2000]}


def _safe_env() -> dict[str, str]:
    out = {}
    for k, v in os.environ.items():
        if k.startswith(("HPCB_", "HPC_BRIDGE_")) or k in _REDACT or k == "GLOBUS_COMPUTE_USER_DIR":
            out[k] = "<redacted>" if k in _REDACT else v
    return out


def _block_lines(block: Any) -> list[str]:
    t = getattr(block, "text", None)
    if t is not None:
        return [t.strip()] if t.strip() else []
    th = getattr(block, "thinking", None)
    if th is not None:
        quoted = "\n".join(f"> {ln}" for ln in th.strip().splitlines())
        return [f"> 🧠 **thinking**\n{quoted}"] if th.strip() else []
    if hasattr(block, "name") and hasattr(block, "input"):
        inp = json.dumps(_jsonable(getattr(block, "input", {})), default=str)
        return [f"`🔧 {getattr(block, 'name', '?')}({inp[:600]}{'…' if len(inp) > 600 else ''})`"]
    if hasattr(block, "tool_use_id"):
        content = getattr(block, "content", "")
        if isinstance(content, list):
            content = " ".join(str(getattr(c, "text", c)) for c in content)
        s = str(content).strip()
        return [f"```\n↩ {s[:1500]}{'…' if len(s) > 1500 else ''}\n```"] if s else []
    return []


def _transcript_md(config: dict, messages: list[Any], dialogue: list[Any],
                   grading: list[Any], rc: int | None, gating: list[str] | None = None,
                   result: str | None = None, failed: list[str] | None = None) -> str:
    gates = set(gating or [])
    lines = [
        f"# {config.get('scenario', 'run')} — {config.get('runid', '')}",
        "",
        f"*{datetime.now(UTC).isoformat(timespec='seconds')} · model "
        f"{config.get('model')} · effort {config.get('effort') or 'default'} · persona "
        f"{config.get('persona') or 'autonomous'}"
        f"{' · ABLATED: skill' if config.get('ablate_skill') else ''} · rc {rc}"
        f" · build {config.get('build', 'unknown')}*",
        "",
        f"RESULT: {result or '?'}" + (f" — gating checks that failed: {failed}" if failed else ""),
        "",
        "## Conversation",
        "",
    ]
    for m in messages:
        role = type(m).__name__.replace("Message", "").lower()
        content = getattr(m, "content", None)
        if not isinstance(content, list):
            continue
        chunk: list[str] = []
        for b in content:
            chunk += _block_lines(b)
        if chunk:
            lines.append(f"**{role}:**")
            lines += chunk
            lines.append("")
    if dialogue:
        lines += ["## Human-sim dialogue", ""]
        for x in dialogue:
            for q in getattr(x, "questions", []):
                lines.append(f"- **asked:** {q.get('question', '?')}")
            for _k, v in getattr(x, "answers", {}).items():
                lines.append(f"  - **answered:** {v}")
            if getattr(x, "note", ""):
                lines.append(f"  - *user note:* {x.note}")
        lines.append("")
    lines += ["## Grading", "", "*`*gate*` marks the checks that decide the verdict; the rest are reported only.*", ""]
    for r in grading:
        gate = " *gate*" if r.name in gates or r.name.startswith("world:") else ""
        lines.append(f"- [{'PASS' if r.ok else 'FAIL'}{gate}] `{r.name}` — {r.detail}")
    return "\n".join(lines) + "\n"


def write_run_record(
    runs_dir: Path,
    *,
    config: dict,
    messages: list[Any],
    dialogue: list[Any],
    grading: list[Any],
    final: Any,
    rc: int | None,
    gating: list[str] | None = None,
    failed: list[str] | None = None,
    result: str | None = None,
    events: list[dict] | None = None,
    extra_jsonl: dict[str, list[Any]] | None = None,
) -> Path | None:
    """Write the bundle; never raises (best-effort provenance must not fail the run).

    `gating` (the check names that decided the verdict), `failed` and `result` (OK | FAILED | RATE_LIMITED |
    SETUP FAILED | CRASHED) are persisted so a bundle explains its own verdict — regrade.py and watch.sh read them
    (review 2026-09-05, 2.5: `run_completed` used to exist only on stdout)."""
    try:
        d = runs_dir / f"{config.get('runid', 'local')}-{config.get('scenario', 'run')}"
        d.mkdir(parents=True, exist_ok=True)
        with (d / "messages.jsonl").open("w") as fh:
            for m in messages:
                fh.write(json.dumps(_jsonable(m), default=str) + "\n")
        # Operator-specific side streams (e.g. the ACP client's event log, `acp-updates.jsonl`): one file each,
        # one JSON object per line, next to messages.jsonl.
        for name, rows in (extra_jsonl or {}).items():
            if rows is None:
                continue
            with (d / f"{name}.jsonl").open("w") as fh:
                for r in rows:
                    fh.write(json.dumps(_jsonable(r), default=str) + "\n")
        gates = set(gating or [])
        record = {
            "schema": 2,
            "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "config": config,
            "env": _safe_env(),
            "result": result,
            "failed": list(failed or []),
            "events": _jsonable(events or []),   # chaos hooks that fired (tool, nth, call index, rc, output)
            "gating": sorted(gates),
            "grading": [{"name": r.name, "ok": r.ok, "detail": r.detail,
                         "gating": r.name in gates or r.name.startswith("world:")} for r in grading],
            "rc": rc,
            "final": {
                "is_error": getattr(final, "is_error", None),
                "total_cost_usd": getattr(final, "total_cost_usd", None),
                "num_turns": getattr(final, "num_turns", None),
                "duration_ms": getattr(final, "duration_ms", None),
                "usage": _jsonable(getattr(final, "usage", None)),
                "session_id": getattr(final, "session_id", None),
            },
            "dialogue": _jsonable(dialogue),
            "n_messages": len(messages),
        }
        (d / "record.json").write_text(json.dumps(record, indent=2, default=str))
        md = _transcript_md(config, messages, dialogue, grading, rc, gating=sorted(gates), result=result, failed=failed)
        if events:
            md += "\n## Harness events (chaos hooks)\n\n" + "\n".join(
                f"- **{e.get('name')}** after `{e.get('after_tool')}` #{e.get('nth')} at call {e.get('call_index')}: "
                f"rc={e.get('rc')} — {str(e.get('out', ''))[:200]}" for e in events) + "\n"
        (d / "transcript.md").write_text(md)
        return d
    except Exception as exc:  # noqa: BLE001 - provenance must never break the run
        print(f"provenance: write failed (ignored) — {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
        return None
