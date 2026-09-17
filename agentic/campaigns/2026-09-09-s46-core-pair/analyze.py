#!/usr/bin/env python3
"""Tabulate the sonnet-4.6 core-pair campaign: per-cell verdicts from the bundles + the per-grader failure taxonomy
per (harness, scenario). Primary result = the taxonomy; rates with Wilson intervals as secondary."""
import glob
import json
import math
import os
import sys
from collections import Counter, defaultdict

RUNS = "/Users/gusellerm/Projects/hpc-bridge/agentic/runs"
LABELS = {"s46hermes": "hermes/Argo", "s46claude": "Claude Code"}


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def main():
    cells = []
    for rec_path in sorted(glob.glob(f"{RUNS}/s46*/record.json"), key=os.path.getmtime):
        d = os.path.basename(os.path.dirname(rec_path))
        label = d.split("-")[0]
        with open(rec_path) as fh:
            rec = json.load(fh)
        cfg = rec.get("config", {})
        grading = rec.get("grading", [])
        gating = set(rec.get("gating") or [])
        failed = [g["name"] for g in grading if not g["ok"] and (g["name"] in gating or g["name"].startswith("world:"))]
        report_only_fail = [g["name"] for g in grading if not g["ok"] and g["name"] not in gating and not g["name"].startswith("world:")]
        inter = next((g["detail"] for g in grading if g["name"] == "harness:interaction"), "")
        guid = next((g["detail"][:12] for g in grading if g["name"] == "harness:guidance_fetched"), "")
        xcheck = next((("ok" if g["ok"] else "MISMATCH") for g in grading if g["name"] == "harness:acp_capture"), "-")
        cells.append({"dir": d, "harness": LABELS.get(label, label), "scenario": cfg.get("scenario"), "result": rec.get("result"),
                      "failed": failed, "report_only": report_only_fail, "interaction": inter.replace("exchanges by kind: ", ""),
                      "guidance": guid, "xcheck": xcheck, "n_graders": len(grading)})
    if not cells:
        print("no s46 bundles yet")
        return
    print(f"{'cell':52} {'result':11} {'failed (gating)':38} {'interaction':28} {'guid':6} xcheck")
    for c in cells:
        print(f"{c['dir'][:52]:52} {c['result']!s:11} {','.join(c['failed'])[:38]:38} {c['interaction'][:28]:28} {c['guidance']:6} {c['xcheck']}")
    print("\n=== pass counts (primary: the taxonomy below) ===")
    by = defaultdict(list)
    for c in cells:
        by[(c["harness"], c["scenario"])].append(c)
    for (h, s), cs in sorted(by.items()):
        k = sum(1 for c in cs if c["result"] == "OK")
        lo, hi = wilson(k, len(cs))
        print(f"  {h:12} {s:16} {k}/{len(cs)}   wilson95 [{lo:.2f}, {hi:.2f}]")
    print("\n=== failure taxonomy: gating graders that failed, per harness ===")
    tax = defaultdict(Counter)
    for c in cells:
        for f in c["failed"]:
            tax[c["harness"]][f] += 1
    for h, cnt in sorted(tax.items()):
        print(f"  {h}: " + (", ".join(f"{k}×{v}" for k, v in cnt.most_common()) or "none"))
    print("\n=== report-only graders that failed (operator preferences), per harness ===")
    ro = defaultdict(Counter)
    for c in cells:
        for f in c["report_only"]:
            ro[c["harness"]][f] += 1
    for h, cnt in sorted(ro.items()):
        print(f"  {h}: " + (", ".join(f"{k}×{v}" for k, v in cnt.most_common()) or "none"))
    print("\n=== interaction kinds, per harness ===")
    ik = defaultdict(Counter)
    for c in cells:
        for part in c["interaction"].split(", "):
            if "×" in part:
                k, v = part.split("×")
                ik[c["harness"]][k] += int(v)
    for h, cnt in sorted(ik.items()):
        print(f"  {h}: " + ", ".join(f"{k}×{v}" for k, v in cnt.most_common()))
    print("\n=== cross-check (harness:acp_capture) ===")
    xc = Counter((c["harness"], c["xcheck"]) for c in cells)
    for (h, x), v in sorted(xc.items()):
        print(f"  {h}: {x} ×{v}")


if __name__ == "__main__":
    sys.exit(main())
