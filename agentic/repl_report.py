#!/usr/bin/env python3
"""Side-by-side REPL benchmark report: hpc-bridge (`repl_interaction`) against the local reference (`repl_baseline_local`).

Reads stored run bundles offline (no agent, no cluster), re-derives the REPL properties and turn latency from each
bundle's messages.jsonl with the CURRENT graders, and prints one row per bundle plus the bridge / local latency ratio.
Definition and rationale: vault `Planned/REPL-like interaction benchmark.md`.

    python agentic/repl_report.py agentic/runs
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "harness"))

from repl_protocol import graders, metrics  # noqa: E402
from trace_adapter import trace_from_bundle  # noqa: E402

SCENARIOS = {"repl_interaction": "bridge", "repl_baseline_local": "local"}
_PROPS = [("repl_protocol_complete", "steps"), ("repl_direct_results", "P1"), ("repl_state_carries", "P3"),
          ("repl_env_carries", "env"), ("repl_fail_soft", "P4"), ("repl_no_ceremony", "P5")]


def rows(runs_dir: Path) -> list[dict]:
    out = []
    for d in sorted(p for p in runs_dir.iterdir() if p.is_dir()):
        rec_path, msgs = d / "record.json", d / "messages.jsonl"
        if not rec_path.exists() or not msgs.exists():
            continue
        cfg = json.loads(rec_path.read_text()).get("config", {})
        channel = SCENARIOS.get(str(cfg.get("scenario")))
        if channel is None:
            continue
        t = trace_from_bundle(d)
        results = {r.name: r for r in (g(t) for g in graders(channel))}
        target = cfg.get("target") or "?"
        out.append({"bundle": d.name, "channel": channel, "target": target,
                    "series": "local" if channel == "local" else f"bridge@{target}",
                    "model": cfg.get("model") or "?", "props": {n: results[n].ok for n, _ in _PROPS},
                    **metrics(t, channel)})
    return out


def _fmt(v: object) -> str:
    return "—" if v is None else str(v)


def main(argv: list[str]) -> int:
    runs_dir = Path(argv[1]) if len(argv) > 1 else HERE / "runs"
    if not runs_dir.is_dir():
        print(f"no runs directory at {runs_dir}", file=sys.stderr)
        return 2
    rs = rows(runs_dir)
    if not rs:
        print(f"no repl_interaction / repl_baseline_local bundles under {runs_dir}")
        return 1
    head = (["bundle", "channel", "target"] + [label for _, label in _PROPS]
            + ["setup s", "tool p50", "p95", "max", "drift", "turn p50", "agent p50"])
    print(" | ".join(head))
    print(" | ".join("---" for _ in head))
    for r in rs:
        props = ["✓" if r["props"][n] else "✗" for n, _ in _PROPS]
        lat = (["unmeasured", "", "", "", "", "", ""] if r.get("unmeasured") else
               [_fmt(r.get("setup_s")), _fmt(r.get("warm_p50_s")), _fmt(r.get("warm_p95_s")), _fmt(r.get("warm_max_s")),
                _fmt(r.get("drift")), _fmt(r.get("turn_p50_s")), _fmt(r.get("agent_p50_s"))])
        print(" | ".join([r["bundle"], r["channel"], str(r["target"]), *props, *lat]))

    def med(key: str, series: str) -> float | None:
        xs = [r[key] for r in rs if r["series"] == series and r.get(key) is not None]
        return statistics.median(xs) if xs else None

    present = sorted({r["series"] for r in rs}, key=lambda x: (x != "local", x != "bridge@fake", x))
    print()
    for ch in present:
        n = sum(1 for r in rs if r["series"] == ch and not r.get("unmeasured"))
        turn, tool = med("turn_p50_s", ch), med("warm_p50_s", ch)
        print(f"{ch:>15}: " + (f"whole turn {turn:.2f} s · tool {tool:.2f} s · over {n} run(s)"
                               if turn is not None and tool is not None else "no measured runs"))
    lt, lw = med("turn_p50_s", "local"), med("warm_p50_s", "local")
    for ch in (c for c in present if c != "local"):
        target = ch.split("@", 1)[1]
        bt, bw = med("turn_p50_s", ch), med("warm_p50_s", ch)
        if bt is not None and lt is not None:
            print(f"  {target}: a REPL turn takes {bt / max(lt, 1e-3):.1f}× a local one ({bt:.2f} s vs {lt:.2f} s)")
            if bw is not None and lw is not None:
                print(f"  {target}: the tool alone is {bw / max(lw, 1e-3):.0f}× the local Bash tool's "
                      f"({bw:.2f} s vs {lw:.2f} s) — read the turn ratio first")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
