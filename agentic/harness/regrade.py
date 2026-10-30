"""Offline re-grader: replay stored provenance bundles through the CURRENT invariants.

The bundles' messages.jsonl is the grading substrate, so invariant changes can be validated
against every past run without re-running any agent:

    uv run python agentic/harness/regrade.py [runs_dir]

For each bundle: rebuild the Trace, run the universal registry + the scenario's
EXTRA_INVARIANTS, and diff per-invariant verdicts against what record.json stored (world:
postchecks are skipped — they measured the live cluster, which is gone). Reports verdict
flips and fresh critical failures, then a summary. A failing bundle whose operator produced
nothing to grade because its model endpoint failed is reported INFRA, as run.py decides it
live (infra.py), and does not count as "would now grade FAIL". Each bundle's scenario is
imported afresh under that bundle's own run environment (RUN_ENV, load_scenario): a scenario
builds graders from it at import — the canary cells' nonce comes from HPCB_RUNID.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path
from types import ModuleType

_HERE = Path(__file__).resolve().parent
SCENARIOS_DIR = _HERE.parents[0] / "scenarios"
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(SCENARIOS_DIR))

from infra import PREFLIGHT_ROW, absences, agent_part, bundle_final, infra_reason, outcome_of  # noqa: E402
from invariants import (  # noqa: E402
    FLOOR_NAMES,
    OPERATOR_PREFERENCE_GRADERS,
    Trace,
    check_all,
    dialogue_positions,
    dialogue_words,
    first_bring_up_had_details,
    floor_graders,
    regrade_staged,
    staged_connects,
)
from trace_adapter import insert_interjections, trace_from_bundle  # noqa: E402

# The per-run environment a scenario module reads WHEN IT IS IMPORTED — so the replay must re-create it per bundle:
# HPCB_RUNID (canaries.nonce: the canary cells' graders, SETUP and POSTCHECKS), HPC_BRIDGE_SSH_HOST (the login host
# no_ssh_access's graders name; the f2b cells' no longer do, 2026-10-22), HPC_BRIDGE_ENDPOINT_NAME and HPCB_TARGET_NODES (POSTCHECKS, SETUP),
# HPCB_RELAUNCH_* (queued_check_relaunch's watch and NEEDS_COMPUTE_NODE), HPCB_RETURNING_CELL (returning_user's cell:
# its levels pick the graders). test_regrade holds every scenario to it.
RUN_ENV = ("HPCB_RUNID", "HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_ENDPOINT_NAME", "HPCB_TARGET_NODES",
           "HPCB_RELAUNCH_PHASE", "HPCB_RELAUNCH_HOLD_S", "HPCB_RELAUNCH_WATCH_S", "HPCB_RELAUNCH_PILOT_WAIT_S",
           "HPCB_RETURNING_CELL")


def run_env(rec: dict) -> dict[str, str | None]:
    """RUN_ENV as the live run had it: the record's env snapshot (provenance._safe_env), HPCB_RUNID falling back to
    config.runid (run.py's copy of it, in every bundle). None = unset, as it was in the jail."""
    env = rec.get("env") or {}
    out = {k: None if env.get(k) is None else str(env[k]) for k in RUN_ENV}
    runid = (rec.get("config") or {}).get("runid")
    if out["HPCB_RUNID"] is None and runid is not None:
        out["HPCB_RUNID"] = str(runid)
    return out


def _setenv(k: str, v: str | None) -> None:
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v


def _scenario_modules() -> dict[str, ModuleType]:
    """The cached modules that are scenario FILES (a test's stand-in module, with no file, is not one)."""
    return {k: m for k, m in list(sys.modules.items())
            if getattr(m, "__file__", None) and Path(m.__file__).parent.name == SCENARIOS_DIR.name
            and Path(m.__file__).parent.resolve() == SCENARIOS_DIR}


def load_scenario(name: str, rec: dict) -> ModuleType | None:
    """Import scenario `name` FRESH under its bundle's run environment (run_env); None when there is no such scenario.

    A scenario derives state from that environment at import — the canary cells build their nonce, and with it their
    graders, from HPCB_RUNID — so a module cached from another bundle, or imported under the regrading shell's env,
    grades the wrong run (every canary bundle's `canary_reached_agent` flipped to FAIL). Every scenario module is
    dropped from the cache for the import (they import each other), and the env and the cache are put back after it:
    the module returned keeps its own state."""
    saved_env = {k: os.environ.get(k) for k in RUN_ENV}
    saved_mods = _scenario_modules()
    try:
        for k, v in run_env(rec).items():
            _setenv(k, v)
        for k in saved_mods:
            del sys.modules[k]
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            return None
    finally:
        for k, v in saved_env.items():
            _setenv(k, v)
        for k in _scenario_modules():
            del sys.modules[k]
        sys.modules.update(saved_mods)


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
    the SDK dict-form (`__type__`, Claude-SDK operator), the cli_runner record (`harness`), hermes' state.db rows (`role` + `tool_calls`), or Claude
    Code's native CLI transcript (`sessionId` + `type`, the Claude-over-ACP operator). A hermes ACP bundle also
    re-stamps the human-sim's prose exchanges from the record's dialogue (message-order correlation, as live), so
    the interactive gates replay; a hermes transcript-replay (`-z`) bundle cannot (each turn was its own session
    carrying the whole conversation) and replays trace-only."""
    first = _first_line(d)
    if "chain_phase" in first:
        return chain_trace(_rows(d))
    if "__type__" in first:
        return trace_from_bundle(d)
    if "harness" in first and "rc" in first:  # the Codex / Pi / Hermes-latest operator (cli_runner)
        from cli_runner import trace_from_bundle_messages
        return trace_from_bundle_messages(
            [json.loads(line) for line in (d / "messages.jsonl").read_text().splitlines() if line.strip()])
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


def chain_trace(rows: list) -> Trace:
    """A mixed chain's bundle (run.py `_combine` with labels): each phase's rows open with a `chain_phase` marker and are
    in their own operator's layout — the script's or a CLI's steps-and-journal rows (cli_runner), the Claude SDK's
    stream (trace_adapter). Each phase is read with its reader and stamped with its index, the texts' places shifted
    past the earlier phases' calls, as `_combine` does live."""
    from cli_runner import trace_from_bundle_messages
    from trace_adapter import trace_from_rows

    phases: list[tuple[int, list]] = []
    for m in rows:
        if isinstance(m, dict) and "chain_phase" in m:
            phases.append((int(m["chain_phase"]), []))
        elif phases:
            phases[-1][1].append(m)
    calls, texts, text_at = [], [], []
    for k, seg in phases:
        head = next((m for m in seg if isinstance(m, dict)), {})
        t = trace_from_bundle_messages(seg) if "harness" in head and "rc" in head else trace_from_rows(seg)
        if len(t.text_at) == len(t.texts):
            text_at += [len(calls) + n for n in t.text_at]
        for c in t.calls:
            c.phase = k
            calls.append(c)
        texts += t.texts
    return Trace(calls, texts, text_at if len(text_at) == len(texts) else [])


def _rows(d: Path) -> list:
    """messages.jsonl's rows as stored (unparseable lines skipped)."""
    out = []
    for line in (d / "messages.jsonl").read_text().splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _preflight(rec: dict) -> str | None:
    """A cell run_smoke.sh never launched — its ALCF probe found the model endpoint unhealthy (alcf_probe.py) — keeps the
    probe's reason: there is no run to re-derive anything from."""
    pre = next((g for g in rec.get("grading", []) if g.get("name") == PREFLIGHT_ROW and not g.get("ok")), None)
    return None if pre is None else str(pre.get("detail") or "the model endpoint was unhealthy before launch")


def bundle_infra(d: Path, rec: dict, t: Trace, crit_fail_names: list[str], postchecks=()) -> str | None:
    """Why a bundle that fails is INFRA, as run.py decided it live (infra.py), or None. `postchecks`: the scenario's
    POSTCHECKS, which say which stored world failures only found missing what the agent's work would have left."""
    if not crit_fail_names or rec.get("rc") == 3 or rec.get("result") not in (None, "OK", "FAILED", "INFRA"):
        return None   # INFRA replaces a graded FAILED only: never RATE_LIMITED, INTERRUPTED, SETUP FAILED or CRASHED
    # what was not re-run here but did fail live — world checks, the harness rows (the leak probe after the teardown):
    # INFRA never hides one, except a world check that failed only because nothing ran (infra.absences)
    world = [(str(g["name"]), str(g.get("detail") or "")) for g in rec.get("grading", [])
             if str(g.get("name", "")).startswith("world:") and not g.get("ok")]
    stored = list(rec.get("failed") or []) + [name for name, _ in world]
    rows = _rows(d)
    operator = (rec.get("config") or {}).get("operator") or "claude"
    return infra_reason(outcome_of(operator, *agent_part(t, rows), bundle_final(rows, rec.get("final"))),
                        crit_fail_names + stored,
                        absences(world, postchecks))


def regrade(runs_dir: Path, *, strict: bool = False) -> int:
    """Replay every bundle. `strict`: exit 1 when any bundle would now grade FAIL (a CI guard over a golden
    corpus). The completion gate is re-derived from the persisted `rc`/`final` — a bundle with rc=1 and every
    trace row PASS used to re-grade as OK (review 2026-09-05, 2.5). An INFRA bundle is reported, never counted as
    "would now grade FAIL"."""
    bundles = flips = now_failing = now_infra = 0
    for d in sorted(runs_dir.iterdir()):
        if not (d / "record.json").exists() or not (d / "messages.jsonl").exists():
            continue
        bundles += 1
        rec = json.loads((d / "record.json").read_text())
        cfg = rec.get("config", {})
        scen_name = cfg.get("scenario", "?")
        old = {g["name"]: g["ok"] for g in rec.get("grading", [])
               if not g["name"].startswith("world:")}
        pre = _preflight(rec)
        if pre is not None:   # run_smoke.sh never launched the cell (its ALCF probe): nothing to re-grade
            now_infra += 1
            print(f"[INFRA] {d.name}  (not launched)\n        INFRA: {pre[:200]}")
            continue

        t = insert_interjections(bundle_trace(d, rec), [e for e in rec.get("events") or [] if e.get("interject")])
        t.user_said = dialogue_words(rec.get("dialogue"))   # what the user said, as run.py stamps it live
        t.user_said_at = dialogue_positions(rec.get("dialogue"))
        scen = load_scenario(scen_name, rec)   # under THIS bundle's run env: its graders are this run's
        # the floor replays too — without the jail's secret material (no_secret_material says so, vacuously), but with
        # the scenario's USER_SECRETS (a fixture in the module: the replay compares exactly what the live run did)
        results = check_all(t) + [fn(t) for fn in floor_graders(own_user=cfg.get("pool_user"),
                                                                 user_secrets=getattr(scen, "USER_SECRETS", None))]
        # a SCRIPT's staged failures, as run.py honours them: only under the script operator
        staged = staged_connects(t, scen, cfg.get("operator"))
        results = regrade_staged(results, t, staged)
        critical = {"agent_engaged", *FLOOR_NAMES}
        if scen is not None:
            results += [fn(t) for fn in getattr(scen, "EXTRA_INVARIANTS", [])]
            critical |= set(getattr(scen, "EXPECT_OK", []))
            if getattr(scen, "LOCAL_BASELINE", False):
                critical.discard("agent_engaged")   # a calibration run that must not use hpc-bridge (run.py, same rule)
        if first_bring_up_had_details(t, staged):   # the same conditional gate as run.py: a details= bring-up must land first time
            critical.add("first_details_connect_succeeds")
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
        infra = bundle_infra(d, rec, t, crit_fail_names, getattr(scen, "POSTCHECKS", ()))
        tag = "INFRA" if infra else "FAIL" if crit_fail_names else "OK  "
        if infra:
            now_infra += 1
        elif crit_fail_names:
            now_failing += 1
        stored = rec.get("result")
        infra_change = stored in ("FAILED", "INFRA") and (stored == "INFRA") != bool(infra)
        mark = "  <-- verdict change" if (changed or fresh_crit_fails or infra_change) else ""
        print(f"[{tag}] {d.name}  ({label}){mark}")
        if infra:
            print(f"        INFRA: {infra[:200]}")
        if infra_change:
            print(f"        stored {stored} -> now {tag.strip()}")
        for n, o, w in changed:
            flips += 1
            print(f"        flip {n}: {'PASS' if o else 'FAIL'} -> {'PASS' if w else 'FAIL'}"
                  f" — {new[n].detail[:110]}")
        for r in fresh_crit_fails:
            print(f"        new critical FAIL {r.name} — {r.detail[:110]}")
    print(f"\n{bundles} bundles re-graded · {flips} per-invariant verdict flips · "
          f"{now_failing} would now grade FAIL · {now_infra} INFRA (the model endpoint failed: not graded)")
    return 1 if (strict and now_failing) else 0


if __name__ == "__main__":
    argv = [a for a in sys.argv[1:] if a != "--strict"]
    sys.exit(regrade(Path(argv[0]) if argv else _HERE.parents[0] / "runs", strict="--strict" in sys.argv))
