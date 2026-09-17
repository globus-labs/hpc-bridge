#!/usr/bin/env python3
"""Sweep the REPL benchmark: N rounds of the local baseline and hpc-bridge on one or more clusters, then the graph.

Each round runs one `repl_baseline_local` and one `repl_interaction` PER TARGET (`--targets fake,globus1`), SERIALLY,
with the order ROTATED each round (local → fake → globus1, then fake → globus1 → local, …), so drift in the Globus
Compute service or the laptop over a long sweep lands on every channel rather than on one. Serial because the thing
being measured is latency: two cells at once would measure each other. Each cell is one `run_smoke.sh` invocation (a
fresh container, the subscription token from agentic/.env).

Before every hpc-bridge cell the sweep checks the target's load: it waits until the default partition has a node
whose state is exactly `idle` (the suite runner's rule — a drained or mixed node is not idle), up to
`--node-wait-s`, and records the idle count and the other users' running jobs in the manifest. A probe that fails
launches unguarded (never never-launch); no idle node within the wait halts the sweep for a later `--resume`.

The sweep writes `agentic/runs/repl-sweep-<id>/`:

    manifest.json   the plan and, per cell, its target, result, bundle, wall time and the cluster load at launch
    logs/           each cell's full output
    repl-sweep.html the graph (repl_plot.py), written at the end

Run from the repo root. It costs subscription usage: `--dry-run` prints the plan and an estimate first.

    python3 agentic/repl_sweep.py --repeat 5 --dry-run
    python3 agentic/repl_sweep.py --repeat 6 --targets fake,globus1
    python3 agentic/repl_sweep.py --resume agentic/runs/repl-sweep-20260914-150000     # after a stop or a halt

A cell that grades FAILED is still data and the sweep carries on; a rate limit, a setup failure, a crash, a run that
leaves no bundle, or no idle node halts it (resume once the cause is fixed). Ctrl-C stops the current cell cleanly
(run_smoke tears down and writes its bundle) and saves the manifest.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SMOKE = REPO / "agentic" / "run_smoke.sh"
sys.path.insert(0, str(REPO / "agentic" / "harness"))
import targets as targets_mod  # noqa: E402  (host-importable: no SDK)

SCENARIO = {"local": "repl_baseline_local", "bridge": "repl_interaction"}
KNOWN_TARGETS = ("fake", "globus1")
# Wall seconds and subscription $ per cell: means of the 6-round fake + globus1 sweep (repl-sweep-20260914-193829,
# claude-opus-5, image built).
ESTIMATE = {("local", None): (40, 0.23), ("bridge", "fake"): (179, 0.70), ("bridge", "globus1"): (205, 0.73)}
HALT_RESULTS = ("RATE_LIMITED", "SETUP FAILED", "CRASHED", "INTERRUPTED", "SKIPPED", "NO IDLE NODE")
NODE_POLL_S = 30


def series(cell: dict) -> str:
    """The graph's row a cell belongs to: `local`, or `bridge@<target>`. Old manifests (no target) were fake-only."""
    return "local" if cell["channel"] == "local" else f"bridge@{cell.get('target') or 'fake'}"


def plan(repeat: int, targets: list[str] | tuple[str, ...] = ("fake",)) -> list[dict]:
    """Rotated rounds over [local, bridge@t1, bridge@t2, …] — every channel takes every position equally often over
    len(channels) rounds (with one target: local-first, then bridge-first, as before)."""
    channels = [("local", None)] + [("bridge", t) for t in targets]
    baseline_target = "fake" if "fake" in targets else targets[0]
    cells = []
    for r in range(repeat):
        shift = r % len(channels)
        for ch, tgt in channels[shift:] + channels[:shift]:
            cells.append({"k": len(cells), "round": r, "channel": ch, "scenario": SCENARIO[ch],
                          "target": tgt or baseline_target, "status": "pending"})
    return cells


def estimate(cells: list[dict]) -> tuple[int, float]:
    todo = [c for c in cells if c["status"] == "pending"]
    per = [ESTIMATE[("local", None)] if c["channel"] == "local" else ESTIMATE[("bridge", c.get("target") or "fake")]
           for c in todo]
    return sum(s for s, _ in per), sum(u for _, u in per)


def parse_output(text: str) -> tuple[str | None, str | None]:
    """The run's verdict (`RESULT: OK` / `FAILED — …` → the label) and its bundle path (`record: …`) — last of each."""
    result = record = None
    for line in text.splitlines():
        m = re.match(r"\s*RESULT:\s*([A-Z][A-Z _]*[A-Z])", line)
        if m:
            result = m.group(1).strip()
        m = re.match(r"\s*record:\s*(\S.*)$", line)
        if m:
            record = m.group(1).strip()
    return result, record


# --- the cluster's load -----------------------------------------------------------------------------------------------

def _probe(target: str, remote: str) -> str | None:
    """One read-only command on the target, from the host (globus1: the operator's ssh alias; fake: a pool user)."""
    try:
        t = targets_mod.get(target)
        r = subprocess.run([*t.probe_argv, remote], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired, SystemExit):
        return None
    return r.stdout if r.returncode == 0 else None


def cluster_load(target: str, probe: Callable[[str, str], str | None] = _probe) -> dict:
    """Idle nodes (state exactly `idle`, as run_suite counts them) and the running jobs by user. Counts the target's
    default partition; when that partition is not there — the fake cluster runs whichever profile was brought up,
    and HPCB_FAKE_PROFILE may not say which (live 2026-09-14: `site`'s debug/compute/gpu, not `main`) — it counts
    every node once instead. `idle` is None when the probe failed or answered something that is not a state column."""
    part = str(targets_mod.get(target).capabilities.get("default_partition") or "main")
    out = probe(target, f"sinfo -h -p {part} -N -o '%N %t'; echo ---; sinfo -h -N -o '%N %t'; echo ---; "
                        "squeue -h -t R -o %u")
    if out is None or out.count("---") < 2:
        return {"idle": None, "partition": part, "running_by_user": {}}
    in_part, everywhere, jobs = out.split("---", 2)
    if not in_part.strip():
        in_part, part = everywhere, "all partitions"
    by_node: dict[str, str] = {}
    for ln in in_part.splitlines():
        fields = ln.split()
        if len(fields) == 2:
            by_node.setdefault(fields[0], fields[1])     # a node listed under several partitions counts once
        elif fields:
            by_node = {}
            break
    states = list(by_node.values())
    ok = states and all(st.replace("*", "").replace("~", "").replace("#", "").isalpha() for st in states)
    by_user: dict[str, int] = {}
    for u in (ln.strip() for ln in jobs.splitlines()):
        if u:
            by_user[u] = by_user.get(u, 0) + 1
    return {"idle": sum(1 for st in states if st == "idle") if ok else None, "nodes": len(states) if ok else None,
            "partition": part, "running_by_user": by_user}


def wait_for_node(target: str, max_wait_s: int, *, load: Callable[[str], dict] | None = None,
                  sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> dict:
    """Block until the target has an idle node. Returns the load at launch plus `waited_s` and `admitted`
    (True: idle node · None: probe unknown, launched unguarded · False: still no idle node after max_wait_s)."""
    load = load or cluster_load
    t0 = clock()
    announced = False
    while True:
        snap = load(target)
        waited = round(clock() - t0, 1)
        if snap["idle"] is None:
            return {**snap, "waited_s": waited, "admitted": None}
        if snap["idle"] >= 1:
            return {**snap, "waited_s": waited, "admitted": True}
        if waited >= max_wait_s:
            return {**snap, "waited_s": waited, "admitted": False}
        if not announced:
            busy = ", ".join(f"{u}×{n}" for u, n in snap["running_by_user"].items()) or "nobody visible"
            print(f"      {target}: no idle node on {snap['partition']} (running: {busy}) — waiting up to {max_wait_s} s",
                  flush=True)
            announced = True
        sleep(NODE_POLL_S)


# --- preflight --------------------------------------------------------------------------------------------------------

def _docker_ps() -> str | None:
    """Running container names, or None when Docker is not answering."""
    try:
        r = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return r.stdout if r.returncode == 0 else None


def preflight(env: dict[str, str], targets: list[str] | tuple[str, ...] = ("fake",), *,
              docker_ps: Callable[[], str | None] = _docker_ps,
              reach: Callable[[str], bool] | None = None) -> list[str]:
    """Problems that would waste a cell. `docker_ps` and `reach` are injectable for tests."""
    problems = []
    names = docker_ps()
    if names is None:
        problems.append("Docker is not running (start Docker Desktop).")
    elif "fake" in targets and "hpcb-fake-login-1" not in names:
        problems.append("The fake cluster is not up: run agentic/fakecluster/bin/up.sh")
    for t in targets:
        if t not in KNOWN_TARGETS:
            problems.append(f"unknown target {t!r} (known: {', '.join(KNOWN_TARGETS)})")
    if "globus1" in targets:
        key = Path(env.get("HPCB_TEST_SSH_KEY") or targets_mod.get("globus1").default_key)
        if not key.exists():
            problems.append(f"globus1: the scoped test key {key} is missing")
        reachable = reach("globus1") if reach else (_probe("globus1", "true") is not None)
        if not reachable:
            problems.append("globus1: the load probe cannot reach the cluster over your `globus1` ssh alias "
                            "(set HPCB_NODE_PROBE_SSH to another alias if yours differs)")
    dotenv = REPO / "agentic" / ".env"
    has_token = bool(env.get("CLAUDE_CODE_OAUTH_TOKEN"))
    if not has_token and dotenv.exists():
        has_token = any(re.match(r"\s*CLAUDE_CODE_OAUTH_TOKEN\s*=\s*\S", ln) for ln in dotenv.read_text().splitlines())
    if not has_token:
        problems.append("No CLAUDE_CODE_OAUTH_TOKEN in agentic/.env or the environment (run `claude setup-token`; "
                        "in a worktree, symlink the main checkout's agentic/.env).")
    for sc in SCENARIO.values():
        if not (REPO / "agentic" / "scenarios" / f"{sc}.py").exists():
            problems.append(f"scenario {sc} is missing from agentic/scenarios/")
    return problems


# --- running ----------------------------------------------------------------------------------------------------------

def _rel(p: Path) -> str:
    """Repo-relative when inside the repo (readable in the manifest), absolute otherwise."""
    try:
        return str(p.relative_to(REPO))
    except ValueError:
        return str(p)


def _save(sweep: Path, manifest: dict) -> None:
    tmp = sweep / "manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.replace(sweep / "manifest.json")


def run_cell(cell: dict, *, sweep: Path, build: bool, model: str | None, smoke: Path = SMOKE) -> dict:
    """One run_smoke.sh invocation against the cell's target; the full output goes to logs/, a summary to the terminal."""
    env = dict(os.environ, HPCB_TARGET=cell.get("target") or "fake")
    if not build:
        env["HPCB_SKIP_BUILD"] = "1"
    if model:
        env["HPCB_MODEL"] = model
    log = sweep / "logs" / f"{cell['k']:02d}-{cell['scenario']}-{cell.get('target') or 'fake'}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    captured: list[str] = []
    with log.open("w") as fh:
        proc = subprocess.Popen(["bash", str(smoke), cell["scenario"]], cwd=REPO, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                fh.write(line)
                captured.append(line)
                if line.startswith(("RESULT:", "running '")) or "→ " in line:
                    print(f"      {line.rstrip()[:110]}", flush=True)
            rc = proc.wait()
        except KeyboardInterrupt:
            proc.send_signal(signal.SIGINT)   # run_smoke tears down and writes the bundle on SIGINT
            rc = proc.wait()
            captured.append("RESULT: INTERRUPTED\n")
    result, record = parse_output("".join(captured))
    bundle = record if record and (REPO / record).is_dir() else None   # REPO / an absolute path is that path
    return {**cell, "status": "done", "result": result or f"NO RESULT (rc={rc})", "bundle": bundle, "rc": rc,
            "wall_s": round(time.monotonic() - started, 1), "log": _rel(log),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds")}


def _label(c: dict) -> str:
    return c["scenario"] if c["channel"] == "local" else f"{c['scenario']} @ {c.get('target') or 'fake'}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeat", type=int, default=5, help="rounds; each round is one local run + one hpc-bridge run per target")
    ap.add_argument("--targets", default="fake", help="comma-separated clusters for the hpc-bridge runs: fake, globus1")
    ap.add_argument("--node-wait-s", type=int, default=1800, help="how long to wait for an idle node before a cluster cell")
    ap.add_argument("--resume", type=Path, help="continue a sweep dir: skip finished cells")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and the estimate, run nothing")
    ap.add_argument("--no-build", action="store_true", help="skip the image build on the first cell (the image is current)")
    ap.add_argument("--model", help="pin the Anthropic model (HPCB_MODEL); default: the harness default")
    ap.add_argument("--no-plot", action="store_true", help="do not write the graph at the end")
    args = ap.parse_args(argv)

    if args.resume:
        sweep = args.resume if args.resume.is_absolute() else REPO / args.resume
        manifest = json.loads((sweep / "manifest.json").read_text())
        manifest["resumed_at"] = datetime.now(UTC).isoformat(timespec="seconds")
        targets = manifest.get("targets") or [manifest.get("target") or "fake"]
    else:
        targets = [t.strip() for t in args.targets.split(",") if t.strip()]
        sweep_id = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        sweep = REPO / "agentic" / "runs" / f"repl-sweep-{sweep_id}"
        manifest = {"sweep_id": sweep_id, "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "repeat": args.repeat, "targets": targets, "model": args.model, "repo_root": str(REPO),
                    "cells": plan(args.repeat, targets)}

    cells = manifest["cells"]
    todo = [c for c in cells if c["status"] == "pending"]
    if not todo:
        print(f"REPL sweep {manifest['sweep_id']}: every cell is finished — nothing to run.")
        if not args.no_plot and not args.dry_run:
            sys.path.insert(0, str(REPO / "agentic"))
            import repl_plot
            repl_plot.main([str(sweep)])
        return 0
    secs, usd = estimate(cells)
    print(f"REPL sweep {manifest['sweep_id']}: {len(todo)} of {len(cells)} cells to run "
          f"({manifest['repeat']} rounds · serial · rotated order · targets {', '.join(targets)})")
    for c in todo:
        print(f"  [{c['k'] + 1:>2}/{len(cells)}] round {c['round'] + 1}  {_label(c)}")
    guessed = " globus1 timings are a guess until measured;" if "globus1" in targets else ""
    print(f"estimate: about {secs / 60:.0f} min and ${usd:.2f} of subscription usage "
          f"(fake and local from the first 5-round sweep;{guessed} the first cell also builds the image)")
    if args.dry_run:
        return 0

    problems = preflight(dict(os.environ), targets)
    if problems:
        print("\nnot starting:")
        for p in problems:
            print(f"  - {p}")
        return 2
    sweep.mkdir(parents=True, exist_ok=True)
    _save(sweep, manifest)
    print(f"sweep dir: {_rel(sweep)}\n")

    build = not (args.no_build or args.resume)
    halted = None
    try:
        for c in todo:
            print(f"[{c['k'] + 1:>2}/{len(cells)}] {_label(c)} …", flush=True)
            if c["channel"] == "bridge":
                load = wait_for_node(c["target"], args.node_wait_s)
                cells[c["k"]] = c = {**c, "load_at_launch": load}
                _save(sweep, manifest)
                if load["admitted"] is False:
                    halted = {**c, "result": f"NO IDLE NODE on {c['target']} after {load['waited_s']:.0f} s"}
                    break
                idle = "unknown (probe failed; launching unguarded)" if load["idle"] is None else f"{load['idle']} of {load['nodes']}"
                print(f"      {c['target']} load: idle {idle}"
                      + (f" · waited {load['waited_s']:.0f} s" if load["waited_s"] else ""), flush=True)
            done = run_cell(c, sweep=sweep, build=build, model=manifest.get("model"))
            build = False
            cells[c["k"]] = done
            _save(sweep, manifest)
            print(f"      → {done['result']} in {done['wall_s']:.0f} s · {done['bundle'] or 'no bundle'}\n", flush=True)
            if done["result"].startswith(HALT_RESULTS) or not done["bundle"]:
                halted = done
                break
    except KeyboardInterrupt:
        halted = {"result": "INTERRUPTED"}

    finished = sum(1 for c in cells if c["status"] == "done")
    if halted:
        if halted.get("k") is not None and halted["result"].startswith(("INTERRUPTED", "RATE_LIMITED")):
            cells[halted["k"]] = {**cells[halted["k"]], "status": "pending", "bundle": None}   # re-run it on resume; its partial bundle is not data
            _save(sweep, manifest)
        print(f"halted on {halted['result']} after {finished} of {len(cells)} cells — resume with:\n"
              f"  python3 agentic/repl_sweep.py --resume {_rel(sweep)}")
    if not args.no_plot and any(c.get("bundle") for c in cells):
        sys.path.insert(0, str(REPO / "agentic"))
        import repl_plot
        repl_plot.main([str(sweep)])
    return 1 if halted else 0


if __name__ == "__main__":
    raise SystemExit(main())
