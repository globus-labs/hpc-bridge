"""Offline re-grader: replay stored provenance bundles through the CURRENT invariants.

The bundles' messages.jsonl is the grading substrate, so invariant changes can be validated
against every past run without re-running any agent:

    uv run python agentic/harness/regrade.py [runs_dir]

For each bundle: rebuild the Trace, run the universal registry + the scenario's
EXTRA_INVARIANTS, and diff per-invariant verdicts against what record.json stored (world:
postchecks are skipped — they measured the live cluster, which is gone). Reports verdict
flips and fresh critical failures, then a summary.
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[0] / "scenarios"))

from invariants import FLOOR_NAMES, OPERATOR_PREFERENCE_GRADERS, Trace, check_all, floor_graders  # noqa: E402
from trace_adapter import insert_interjections, trace_from_bundle  # noqa: E402


def _first_line(d: Path) -> dict:
    with (d / "messages.jsonl").open() as fh:
        for line in fh:
            if line.strip():
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    return {}
    return {}


def bundle_trace(d: Path, rec: dict) -> Trace:
    """Rebuild the graded Trace from a bundle, whichever operator wrote it — sniffed from messages.jsonl's shape:
    the SDK dict-form (`__type__`, Claude-SDK operator), hermes' state.db rows (`role` + `tool_calls`), or Claude
    Code's native CLI transcript (`sessionId` + `type`, the Claude-over-ACP operator). A hermes ACP bundle also
    re-stamps the human-sim's prose exchanges from the record's dialogue (message-order correlation, as live), so
    the interactive gates replay; a hermes transcript-replay (`-z`) bundle cannot (each turn was its own session
    carrying the whole conversation) and replays trace-only."""
    first = _first_line(d)
    if "__type__" in first:
        return trace_from_bundle(d)
    from claude_transcript import exchanges_from_transcript, load_lines, looks_like_transcript, trace_from_transcript
    if looks_like_transcript(first):
        lines = load_lines(d / "messages.jsonl")
        t = trace_from_transcript(lines)
        replies = [{"answer": (x.get("answers") or {}).get("reply", ""), "kind": x.get("kind")}
                   for x in rec.get("dialogue") or [] if x.get("kind") not in (None, "", "conclude")]
        if replies:   # the claude-acp operator: prose asks, stamped by prompt order (as live)
            from hermes_trace import stamp_exchanges
            t = stamp_exchanges(t, exchanges_from_transcript(lines, replies))
        return t
    if "role" in first and "tool_calls" in first:
        from hermes_trace import exchanges_from_messages, stamp_exchanges, trace_from_messages
        rows = [json.loads(line) for line in (d / "messages.jsonl").read_text().splitlines() if line.strip()]
        t = trace_from_messages(rows)
        if (rec.get("env") or {}).get("HPCB_HERMES_ACP"):
            replies = [{"answer": (x.get("answers") or {}).get("reply", ""), "kind": x.get("kind")}
                       for x in rec.get("dialogue") or [] if x.get("kind") not in (None, "", "conclude")]
            t = stamp_exchanges(t, exchanges_from_messages(rows, replies))
        return t
    return trace_from_bundle(d)


def regrade(runs_dir: Path, *, strict: bool = False) -> int:
    """Replay every bundle. `strict`: exit 1 when any bundle would now grade FAIL (a CI guard over a golden
    corpus). The completion gate is re-derived from the persisted `rc`/`final` — a bundle with rc=1 and every
    trace row PASS used to re-grade as OK (review 2026-09-05, 2.5)."""
    bundles = flips = now_failing = 0
    for d in sorted(runs_dir.iterdir()):
        if not (d / "record.json").exists() or not (d / "messages.jsonl").exists():
            continue
        bundles += 1
        rec = json.loads((d / "record.json").read_text())
        cfg = rec.get("config", {})
        scen_name = cfg.get("scenario", "?")
        old = {g["name"]: g["ok"] for g in rec.get("grading", [])
               if not g["name"].startswith("world:")}

        t = insert_interjections(bundle_trace(d, rec), [e for e in rec.get("events") or [] if e.get("interject")])
        # the floor replays too — without the jail's secret material (no_secret_material says so, vacuously)
        results = check_all(t) + [fn(t) for fn in floor_graders(own_user=cfg.get("pool_user"))]
        critical = {"agent_engaged", *FLOOR_NAMES}
        try:
            scen = importlib.import_module(scen_name)
            results += [fn(t) for fn in getattr(scen, "EXTRA_INVARIANTS", [])]
            critical |= set(getattr(scen, "EXPECT_OK", []))
        except ModuleNotFoundError:
            pass
        # the completion gate, from what the bundle recorded (not from the trace)
        fin = rec.get("final") or {}
        completed = rec.get("rc") != 3 and fin.get("is_error") is False
        from invariants import Result
        results.append(Result("run_completed", completed, "ok" if completed else f"rc={rec.get('rc')} is_error={fin.get('is_error')}"))
        critical.add("run_completed")
        # Benchmark mode (recorded in the config): the operator-preference graders were REPORT-ONLY live, so they
        # must not decide the replayed verdict either — else every cross-harness bundle "would now grade FAIL".
        if cfg.get("benchmark_mode"):
            critical -= OPERATOR_PREFERENCE_GRADERS
        new = {r.name: r for r in results}

        changed = [(n, old[n], new[n].ok) for n in old if n in new and old[n] != new[n].ok]
        fresh_crit_fails = [r for r in results
                            if not r.ok and r.name in critical and r.name not in old]
        crit_fail_names = [r.name for r in results if not r.ok and r.name in critical]
        label = (f"{cfg.get('persona') or 'auto'}"
                 f"{', ~skill' if cfg.get('ablate_skill') else ''}")
        tag = "FAIL" if crit_fail_names else "OK  "
        if crit_fail_names:
            now_failing += 1
        mark = "  <-- verdict change" if (changed or fresh_crit_fails) else ""
        print(f"[{tag}] {d.name}  ({label}){mark}")
        for n, o, w in changed:
            flips += 1
            print(f"        flip {n}: {'PASS' if o else 'FAIL'} -> {'PASS' if w else 'FAIL'}"
                  f" — {new[n].detail[:110]}")
        for r in fresh_crit_fails:
            print(f"        new critical FAIL {r.name} — {r.detail[:110]}")
    print(f"\n{bundles} bundles re-graded · {flips} per-invariant verdict flips · "
          f"{now_failing} would now grade FAIL")
    return 1 if (strict and now_failing) else 0


if __name__ == "__main__":
    argv = [a for a in sys.argv[1:] if a != "--strict"]
    sys.exit(regrade(Path(argv[0]) if argv else _HERE.parents[0] / "runs", strict="--strict" in sys.argv))
