"""Hermetic: the profile sweep names only real scenarios, every named scenario is runnable on its profile, and every
scenario that CAN run on the fake target is either swept or excluded with a reason — none drops out silently."""
from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import targets  # noqa: E402

SWEEP = HERE.parent / "sweep_profiles.sh"
SCENARIOS = {p.stem for p in (HERE.parent / "scenarios").glob("*.py") if not p.stem.startswith("_")}


def _cells() -> dict[str, list[str]]:
    out = {}
    for m in re.finditer(r'^\s+(\w+)\)\s+echo "([\w,]+) (\d+)" ;;', SWEEP.read_text(), re.M):
        out[m.group(1)] = m.group(2).split(",")
    return out


def _excluded() -> dict[str, str]:
    m = re.search(r"^excluded\(\) \{.*?\n  cat <<'EXCLUDED'\n(.*?)\nEXCLUDED\n", SWEEP.read_text(), re.S | re.M)
    assert m, "sweep_profiles.sh lost its excluded() list"
    out = {}
    for line in m.group(1).splitlines():
        name, _, why = line.strip().partition(" ")
        out[name] = why.strip()
    return out


def _swept() -> set[str]:
    """Every scenario the sweep runs: the cells() map plus the cells run_smoke.sh'd on their own (the no-skill
    spend_gate_enforced ablation on `default`)."""
    names = {n for ns in _cells().values() for n in ns}
    return names | set(re.findall(r'run_smoke\.sh" (\w+)', SWEEP.read_text()))


def test_every_fake_runnable_scenario_is_swept_or_excluded_with_a_reason():
    import importlib
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    swept, excluded = _swept(), _excluded()
    assert not swept & set(excluded), sorted(swept & set(excluded))       # excluded means not swept
    assert set(excluded) <= SCENARIOS, sorted(set(excluded) - SCENARIOS)   # no stale names
    assert all(excluded.values()), [n for n, why in excluded.items() if not why]
    fake_runnable = {n for n in SCENARIOS
                     if getattr(importlib.import_module(n), "TARGETS", None) is None
                     or "fake" in importlib.import_module(n).TARGETS}
    dropped = fake_runnable - swept - set(excluded)
    assert not dropped, f"in neither the sweep nor excluded(): {sorted(dropped)} — add each to cells() or excluded()"


def test_sweep_names_existing_scenarios_that_their_profile_can_run():
    import importlib
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    cells = _cells()
    assert set(cells) == {"default", "site", "totp", "pbs", "lmod", "f2b", "totp_f2b", "polaris", "internal", "hostile",
                          "mep", "pbs_mep", "nersc"}
    for prof, names in cells.items():
        caps = targets.load_profile(prof)["capabilities"]
        for n in names:
            assert n in SCENARIOS, (prof, n)
            mod = importlib.import_module(n)
            tg = getattr(mod, "TARGETS", None)
            assert tg is None or "fake" in tg, (prof, n)
            ok, why = targets.meets(getattr(mod, "REQUIRES", None), caps)
            assert ok, (prof, n, why)


def test_sweep_never_defaults_to_fable():
    text = SWEEP.read_text()
    assert 'MODELS="claude-opus-5"' in text and "fable" not in text.lower().replace("never fable", "")


def test_the_docs_list_every_scenario_and_count_them_right():
    # doc drift (itinerary review 2026-10-13): README's list named 18 of 48 scenarios, HANDOFF said 46
    readme = (HERE.parent / "README.md").read_text()
    block = re.search(r"^  scenarios/ +← (.*?)^  runs/", readme, re.S | re.M)
    assert block, "agentic/README.md lost its scenarios/ block"
    listed = set()
    for line in block.group(1).splitlines():   # "<label>: a · b · c" (a label is optional on continuation lines)
        items = line.split(": ", 1)[-1] if ": " in line else line
        listed |= {x.strip() for x in items.split("·") if re.fullmatch(r"[a-z][a-z0-9_]*", x.strip())}
    assert listed == SCENARIOS, {"not in the README": sorted(SCENARIOS - listed), "not a scenario": sorted(listed - SCENARIOS)}
    assert f"{len(SCENARIOS)} scenarios" in block.group(1)
    handoff = (HERE.parents[1] / "HANDOFF.md").read_text()
    counts = {int(n) for n in re.findall(r"(\d+) scenarios\b", handoff)}
    assert counts == {len(SCENARIOS)}, (counts, len(SCENARIOS))


def _paid_only() -> list[str]:
    m = re.search(r"^paid_only\(\) \{.*?\n  cat <<'PAID'\n(.*?)\nPAID\n", SWEEP.read_text(), re.S | re.M)
    assert m, "sweep_profiles.sh lost its paid_only() list"
    return [ln.strip() for ln in m.group(1).splitlines() if ln.strip()]


def test_the_free_wave_leaves_out_exactly_what_the_free_operators_cannot_drive():
    # HPCB_OPERATOR=codex|pi|hermes21 drops paid_only() from every profile: it must be exactly the swept scenarios
    # run.py would report SKIPPED for those operators (a persona, PHASES, a hook that needs the SDK interrupt) — a
    # stale entry would hide a free cell, a missing one would launch a container just to skip it
    import importlib

    from midrun_hooks import CLI_OPERATORS, operator_refusal
    sys.path.insert(0, str(HERE.parent / "scenarios"))
    paid = _paid_only()
    assert len(paid) == len(set(paid))
    for op in CLI_OPERATORS:
        refused = set()
        for n in _swept():
            mod = importlib.import_module(n)
            if operator_refusal(op, persona=getattr(mod, "PERSONA", None), phases=getattr(mod, "PHASES", None),
                                hooks=getattr(mod, "MIDRUN_HOOKS", None)):
                refused.add(n)
        assert set(paid) == refused, {"paid_only but drivable": sorted(set(paid) - refused),
                                      "not drivable, missing from paid_only": sorted(refused - set(paid))}
    assert "esc_mid_call" in paid and "reap_replaced_block" not in paid   # the Esc needs the SDK; the reap is free
    assert {"reap_replaced_block", "esc_mid_call"} <= set(_cells()["default"])


def test_the_free_wave_filter_keeps_order_and_drops_paid_cells(tmp_path):
    import subprocess
    fn = re.search(r"^paid_only\(\) \{.*?\n\}\n", SWEEP.read_text(), re.S | re.M).group(0)
    script = tmp_path / "f.sh"
    script.write_text(fn + 'scen="$1"\ntr \',\' \'\\n\' <<< "$scen" | grep -vxF -f <(paid_only) | paste -sd, -\n')
    out = subprocess.run(["bash", str(script), "happy_path,gated_provision,reap_replaced_block,esc_mid_call,long_output"],
                         capture_output=True, text=True, timeout=30)
    assert out.stdout.strip() == "happy_path,reap_replaced_block,long_output", out
    assert 'grep -vxF -f <(paid_only)' in SWEEP.read_text()   # the same filter the sweep runs


# ---- the sweep's own loop, driven with stub run_suite.py / run_smoke.sh (no docker, no cells) ------------------------

_STUB_SUITE = r'''
import json, os, sys
argv = sys.argv[1:]
scen = argv[argv.index("--scenarios") + 1].split(",")
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(json.dumps({"tool": "run_suite", "argv": argv, "operator": os.environ.get("HPCB_OPERATOR"),
                         "wait": os.environ.get("HPCB_ALCF_WAIT_S")}) + "\n")
infra = set(filter(None, os.environ.get("STUB_INFRA", "").split(",")))
setup = set(filter(None, os.environ.get("STUB_SETUP", "").split(",")))
crash = set(filter(None, os.environ.get("STUB_CRASH", "").split(",")))
for s in scen:
    if s in infra:
        print(f"⚠ done   {s} · opus/default · u — RESULT: INFRA: ALCF streaming unhealthy: HTTP 504")
    elif s in setup:
        print(f"✗ done   {s} · opus/default · u — RESULT: SETUP FAILED — ALCF refused the request")
    elif s in crash:
        print(f"✗ done   {s} · opus/default · u — (no RESULT line; rc=1)")
    else:
        print(f"✓ done   {s} · opus/default · u — RESULT: OK")
n_infra = sum(s in infra for s in scen)
n_ok = sum(s not in infra | setup | crash for s in scen)
print(f"==== SUITE: {n_ok}/{len(scen) - n_infra} passed" + (f" · {n_infra} infra" if n_infra else "") + " ====")
'''
_STUB_SMOKE = r'''#!/usr/bin/env bash
printf '{"tool": "run_smoke", "argv": "%s", "operator": "%s", "phase": "%s", "profile": "%s", "cell": "%s"}\n' "$*" \
  "${HPCB_OPERATOR:-}" "${HPCB_RELAUNCH_PHASE:-}" "${HPCB_FAKE_PROFILE:-}" "${HPCB_RETURNING_CELL:-}" >> "$STUB_LOG"
echo "RESULT: OK"
'''

_STUB_CLUSTER = r'''#!/usr/bin/env bash
printf '{{"tool": "{name}", "argv": "%s"}}\n' "$*" >> "$STUB_LOG"
[ -z "${{STUB_BOOT_FAIL:-}}" ] || [ "{name}" != up.sh ]
'''


def _sweep(tmp_path, *args, **env_extra):
    """Run a copy of sweep_profiles.sh whose run_suite.py / run_smoke.sh are stubs that log each call."""
    import json
    import os
    import subprocess

    root = tmp_path / "repo"
    (root / "agentic").mkdir(parents=True)   # tmp_path may be a fresh subdirectory (parents=True makes it)
    (root / "agentic" / "sweep_profiles.sh").write_text(SWEEP.read_text())
    (root / "agentic" / "run_suite.py").write_text(_STUB_SUITE)
    (root / "agentic" / "run_smoke.sh").write_text(_STUB_SMOKE)
    (root / "agentic" / "run_smoke.sh").chmod(0o755)
    bin_ = root / "agentic" / "fakecluster" / "bin"   # the script-only profiles boot and wipe their own cluster
    (root / "agentic" / "harness").mkdir(parents=True)  # the returning-user cells' table (the `returning` profile)
    (root / "agentic" / "harness" / "returning_cells.tsv").write_text((HERE / "returning_cells.tsv").read_text())
    bin_.mkdir(parents=True)
    for name in ("up.sh", "down.sh"):
        (bin_ / name).write_text(_STUB_CLUSTER.format(name=name))
        (bin_ / name).chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {k: v for k, v in os.environ.items() if not k.startswith("HPCB_")}
    env.update(STUB_LOG=str(log), HPCB_MEP_EMAIL="ops@example.org", **env_extra)
    r = subprocess.run(["bash", str(root / "agentic" / "sweep_profiles.sh"), *args], env=env, capture_output=True,
                       text=True, timeout=120)
    calls = [json.loads(ln) for ln in log.read_text().splitlines()] if log.exists() else []
    summaries = sorted((root / "agentic" / "runs").glob("sweep-*.md")) if (root / "agentic" / "runs").exists() else []
    return r, calls, (summaries[0].read_text() if summaries else ""), root


def _arg(call, flag):
    return call["argv"][call["argv"].index(flag) + 1]


def test_operators_boot_each_profile_once_and_run_its_free_cells_per_operator(tmp_path):
    r, calls, summary, root = _sweep(tmp_path, "--operators", "pi,codex,hermes21", "--profiles", "default,lmod,mep",
                                     HPCB_ALCF_WAIT_S="120", STUB_INFRA="happy_path")
    assert r.returncode == 0, r.stdout + r.stderr
    suites = [c for c in calls if c["tool"] == "run_suite"]
    assert [(_arg(c, "--profile"), c["operator"]) for c in suites] == [
        (p, op) for p in ("default", "lmod", "mep") for op in ("pi", "codex", "hermes21")]
    resets = [_arg(c, "--profile") for c in suites if "--reset-cluster" in c["argv"]]
    assert resets == ["default", "lmod", "mep"]                      # 3 resets for 3 profiles × 3 operators, not 9
    assert all(("--reset-cluster" in c["argv"]) == (c["operator"] == "pi") for c in suites)   # the first operator's
    paid = set(_paid_only())
    for c in suites:   # the free wave's cells: the profile's, minus paid_only(), order kept
        want = [s for s in _cells()[_arg(c, "--profile")] if s not in paid]
        assert _arg(c, "--scenarios").split(",") == want
    assert {c["wait"] for c in suites} == {"120"}                    # HPCB_ALCF_WAIT_S reaches every cell's suite
    smokes = [c for c in calls if c["tool"] == "run_smoke"]
    # spend_gate_enforced per operator, then queued_check_relaunch ONCE (both phases) under the script operator, and
    # lmod_bootstrap ONCE under it on lmod (the review of the 2026-10-22 sweep: the failed-details path every sweep)
    assert [(c["argv"], c["operator"], c["phase"]) for c in smokes] == [
        ("spend_gate_enforced", op, "") for op in ("pi", "codex", "hermes21")] + [
        ("queued_check_relaunch", "script", "second"), ("queued_check_relaunch", "script", "first"),
        ("lmod_bootstrap", "script", "")]
    assert [c["profile"] for c in smokes if c["argv"] == "lmod_bootstrap"] == ["lmod"]
    # every summary row names its operator; INFRA counted apart, per operator
    rows = [ln for ln in summary.splitlines() if "RESULT:" in ln]
    assert rows and all(ln.lstrip().startswith(("[pi] ", "[codex] ", "[hermes21] ", "[script] ")) for ln in rows)
    assert "    [script] lmod_bootstrap (script) — RESULT: OK" in summary
    assert "## totals [script] — OK 3 · FAILED 0 · INFRA 0 " in summary
    assert {p.name.split("-", 3)[-1] for p in (root / "agentic" / "runs").glob("sweep-*-default-qcr-*.log")} == {
        "default-qcr-second.log", "default-qcr-first.log"}
    for op in ("pi", "codex", "hermes21"):
        n_ok = sum(1 for ln in rows if ln.lstrip().startswith(f"[{op}] ") and "RESULT: OK" in ln)
        assert f"## totals [{op}] — OK {n_ok} · FAILED 0 · INFRA 1 " in summary
        assert any((root / "agentic" / "runs").glob(f"sweep-*-default-{op}.log"))   # one log per profile+operator
        assert any((root / "agentic" / "runs").glob(f"sweep-*-default-spend_gate-{op}.log"))
    assert "## default [codex] — free wave leaves out (paid_only): gated_provision," in summary
    assert "free wave: HPCB_ALCF_WAIT_S=120" in summary


def test_paid_only_runs_the_claude_operator_on_paid_cells_and_never_boots_a_profile_without_one(tmp_path):
    r, calls, summary, _ = _sweep(tmp_path, "--paid-only", "--profiles", "default,lmod,pbs")
    assert r.returncode == 0, r.stdout + r.stderr
    suites = [c for c in calls if c["tool"] == "run_suite"]
    assert [_arg(c, "--profile") for c in suites] == ["default", "pbs"]          # lmod has no paid cell: not booted
    assert all(c["operator"] == "claude" and "--reset-cluster" in c["argv"] for c in suites)
    paid = set(_paid_only())
    assert _arg(suites[0], "--scenarios").split(",") == [s for s in _cells()["default"] if s in paid]
    assert _arg(suites[1], "--scenarios") == "gated_provision"
    assert not [c for c in calls if c["tool"] == "run_smoke"]   # spend_gate_enforced / the script cells: not paid-only
    assert "## lmod — SKIPPED (--paid-only: no paid_only() cell here; not booted)" in summary


def test_paid_only_refuses_a_free_operator(tmp_path):
    r, calls, _, _ = _sweep(tmp_path / "a", "--paid-only", "--operators", "pi", "--profiles", "default")
    assert r.returncode == 2 and "only the claude operator" in r.stdout and not calls
    r, calls, _, _ = _sweep(tmp_path / "b", "--operators", "nope", "--profiles", "default")
    assert r.returncode == 2 and "unknown operator nope" in r.stdout and not calls
    r, calls, _, _ = _sweep(tmp_path / "c", "--operators", "pi,codex,pi", "--profiles", "default")
    assert r.returncode == 2 and "operator pi is listed twice" in r.stdout and not calls


def test_without_the_flags_the_sweep_runs_as_before_and_counts_infra_apart(tmp_path):
    r, calls, summary, root = _sweep(tmp_path, "--profiles", "default,lmod", STUB_INFRA="lmod_bootstrap")
    assert r.returncode == 0, r.stdout + r.stderr
    suites = [c for c in calls if c["tool"] == "run_suite"]
    assert [(_arg(c, "--profile"), c["operator"], "--reset-cluster" in c["argv"]) for c in suites] == [
        ("default", "claude", True), ("lmod", "claude", True)]
    assert _arg(suites[0], "--scenarios").split(",") == _cells()["default"]     # every cell: the paid wave
    rows = [ln for ln in summary.splitlines() if "RESULT:" in ln]
    assert rows and not any("[claude]" in ln for ln in rows)                    # untagged, as before
    assert "## default — " in summary and "free wave" not in summary
    n_ok = sum("RESULT: OK" in ln for ln in rows)
    assert f"## totals — OK {n_ok} · FAILED 0 · INFRA 1 " in summary
    assert sorted(p.name.split("-", 3)[-1] for p in (root / "agentic" / "runs").glob("sweep-*.log")) == [
        "default-qcr-first.log", "default-qcr-second.log", "default-spend_gate.log", "default.log",
        "lmod-script-lmod_bootstrap.log", "lmod.log"]


def test_the_cells_env_keeps_the_alcf_wait(monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite_sweep", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("HPCB_ALCF_WAIT_S", "300")
    monkeypatch.setenv("HPCB_PERSONA", "stray")
    env = mod._cell_env("hpcbridge-test-01", "r1")
    assert env["HPCB_ALCF_WAIT_S"] == "300" and "HPCB_PERSONA" not in env   # run_smoke.sh reads it per cell


def test_the_totals_count_setup_failed_and_crashed_rows(tmp_path):
    r, _, summary, _ = _sweep(tmp_path, "--profiles", "default", STUB_INFRA="happy_path",
                              STUB_SETUP="gated_provision,spend_refusal", STUB_CRASH="endpoint_reuse")
    assert r.returncode == 0, r.stdout + r.stderr
    n = len(_cells()["default"]) + 3   # + spend_gate_enforced and queued_check_relaunch's two phases (stub: OK)
    assert (f"## totals — OK {n - 4} · FAILED 0 · INFRA 1 (the model endpoint failed: not graded) · SETUP FAILED 2 · "
            f"CRASHED 1 (no RESULT line) · other 0") in summary


def test_the_held_and_failed_details_paths_run_once_per_sweep_under_the_script_operator(tmp_path):
    # the review of the 2026-10-22 sweep: polaris's HELD path and lmod's failed-details path were exercised only when a
    # model happened to take them — now ONCE per sweep with no model, on their profiles, whatever --operators names
    r, calls, summary, root = _sweep(tmp_path, "--operators", "pi,codex", "--profiles", "polaris,lmod,site")
    assert r.returncode == 0, r.stdout + r.stderr
    scripted = [(c["argv"], c["profile"]) for c in calls if c["tool"] == "run_smoke" and c["operator"] == "script"]
    assert scripted == [("polaris_filesystems", "polaris"), ("lmod_bootstrap", "lmod"),
                        ("stale_login_pin", "site"), ("kept_entry_after_failure", "site")]
    assert "    [script] polaris_filesystems (script) — RESULT: OK" in summary
    assert any((root / "agentic" / "runs").glob("sweep-*-polaris-script-polaris_filesystems.log"))
    # --paid-only: none (no paid cell on either profile, so neither is booted)
    _, calls, _, _ = _sweep(tmp_path / "paid", "--paid-only", "--profiles", "polaris,lmod")
    assert not [c for c in calls if c["tool"] == "run_smoke"]


def test_the_summary_names_the_model_each_operator_really_drives(tmp_path):
    # the qwen sweep's summary said "models claude-opus-5" for cells that ran HPCB_ALCF_MODEL
    _, _, free, _ = _sweep(tmp_path / "a", "--operators", "pi,codex", "--profiles", "lmod", HPCB_ALCF_MODEL="qwen3.8-flash-next")
    assert free.splitlines()[0].endswith("target fake, free operators: model qwen3.8-flash-next"), free.splitlines()[0]
    _, _, paid, _ = _sweep(tmp_path / "b", "--profiles", "lmod")
    assert paid.splitlines()[0].endswith("target fake, models claude-opus-5"), paid.splitlines()[0]
    _, _, both, _ = _sweep(tmp_path / "c", "--operators", "claude,pi", "--profiles", "lmod")
    assert both.splitlines()[0].endswith("models claude-opus-5; free operators: model openai/gpt-oss-120b")


def test_a_free_operators_cell_labels_name_its_real_model(monkeypatch, tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite_labels", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("HPCB_OPERATOR", "pi")
    monkeypatch.setenv("HPCB_ALCF_MODEL", "qwen3.8-flash-next")
    assert mod._short_model("claude-opus-5") == "qwen3.8-flash-next"
    assert mod._cell("claude-opus-5", None) == "qwen3.8-flash-next @ default"
    monkeypatch.setenv("HPCB_ALCF_MODEL", "openai/gpt-oss-120b")
    assert mod._short_model("claude-opus-5") == "gpt-oss-120b"
    monkeypatch.delenv("HPCB_ALCF_MODEL")
    monkeypatch.setattr(mod, "REPO", tmp_path)                     # no agentic/.env: run_smoke.sh's default
    assert mod._free_model() == "openai/gpt-oss-120b"
    (tmp_path / "agentic").mkdir()
    (tmp_path / "agentic" / ".env").write_text("HPCB_ALCF_MODEL=qwen-from-env\n")
    assert mod._short_model("claude-opus-5") == "qwen-from-env"
    monkeypatch.setenv("HPCB_OPERATOR", "claude")
    assert mod._short_model("claude-opus-5") == "opus" and mod._cell("claude-opus-5", "high") == "claude-opus-5 @ high"


def test_a_build_that_fails_is_retried_and_names_its_last_error(monkeypatch):
    # hostile[hermes21], sweep 2026-10-23: Docker Hub's token endpoint answered 502, the suite stopped, and its two
    # cells were left out of the sweep's totals altogether
    import asyncio
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite_build", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    err = b"#5 ERROR: failed to fetch oauth token\nERROR: failed to build: unexpected status: 502 Bad Gateway\n"
    rcs = []

    class Proc:
        def __init__(self, rc):
            self.returncode = rc

        async def communicate(self):
            return b"", (b"" if self.returncode == 0 else err)

    async def fake_exec(*a, **k):
        return Proc(rcs.pop(0))

    async def no_sleep(_s):
        return None
    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(mod.asyncio, "sleep", no_sleep)
    rcs[:] = [1, 0]                                    # a hiccup, then it builds
    assert asyncio.run(mod._build_once(waits=(1, 1))) is None and rcs == []
    rcs[:] = [1, 1, 1]                                 # down throughout: the last error line, for the INFRA rows
    why = asyncio.run(mod._build_once(waits=(1, 1)))
    assert why == "ERROR: failed to build: unexpected status: 502 Bad Gateway" and rcs == []


def test_a_suite_whose_image_did_not_build_lists_every_cell_as_infra():
    # the INFRA row's shape is the one sweep_profiles.sh tallies and sweep_dash.py reads ("⚠ done <scen> · … — RESULT:")
    src = (HERE.parent / "run_suite.py").read_text()
    assert 'unrun(f"the jail image did not build ({why})")' in src and "for s, m, e, pe, ab in jobs:" in src
    assert 'unrun("the fake cluster did not come up")' in src
    sys.path.insert(0, str(HERE.parent))
    import sweep_dash
    line = ("⚠ done   canary_login_discovery · qwen3.8-flash-next/default · - — RESULT: INFRA: the jail image did not "
            "build (ERROR: 502) — the cell was not run")
    m = sweep_dash.DONE.match(line)
    assert m and m.group(2) == "canary_login_discovery" and sweep_dash._kind(m.group(3)) == "INFRA"


def test_the_script_only_profiles_boot_their_own_cluster_and_run_only_their_scripted_cells(tmp_path):
    # `slices` and `returning` carry no agent wave: the sweep boots each one's cluster (`returning` boots `nersc`), runs
    # its scripted cells — returning_user once per cell of the checked-in table — and wipes it
    import returning
    cells = list(returning.load_cells())
    r, calls, summary, _ = _sweep(tmp_path, "--operators", "pi,codex", "--profiles", "returning,slices")
    assert r.returncode == 0, r.stdout + r.stderr
    assert not [c for c in calls if c["tool"] == "run_suite"]
    scripted = [(c["argv"], c["profile"], c["cell"]) for c in calls if c["tool"] == "run_smoke"]
    assert scripted == [("returning_user", "nersc", cell) for cell in cells] + [
        ("slice_request", "slices", ""), ("multinode_allocation", "slices", "")]
    assert all(c["operator"] == "script" for c in calls if c["tool"] == "run_smoke")
    assert [(c["tool"], c["argv"]) for c in calls if c["tool"] in ("up.sh", "down.sh")] == [
        ("up.sh", "--profile nersc"), ("down.sh", "--profile nersc --wipe"),
        ("up.sh", "--profile slices"), ("down.sh", "--profile slices --wipe")]
    assert f"    [script] returning_user {cells[0]} — RESULT: OK" in summary
    assert "    [script] slice_request (script) — RESULT: OK" in summary
    # a profile that does not boot is one INFRA row, its cells not run; --paid-only boots neither
    _, calls, summary, _ = _sweep(tmp_path / "f", "--operators", "pi", "--profiles", "returning", STUB_BOOT_FAIL="1")
    assert not [c for c in calls if c["tool"] == "run_smoke"] and "returning (script) — RESULT: INFRA" in summary
    _, calls, _, _ = _sweep(tmp_path / "p", "--paid-only", "--profiles", "returning,slices")
    assert not [c for c in calls if c["tool"] in ("run_smoke", "up.sh")]


def test_nersc_runs_the_returning_agent_cells_in_every_wave_then_the_perlmutter_proofs(tmp_path):
    # the three returning-user agent cells are chains that open with a scripted session: the free operators drive them
    # (midrun_hooks.seeded_chain), so they stay out of paid_only(); the report's scripted proofs follow, once
    r, calls, _, _ = _sweep(tmp_path, "--operators", "pi,codex,hermes21", "--profiles", "nersc")
    assert r.returncode == 0, r.stdout + r.stderr
    suites = [(_arg(c, "--profile"), c["operator"], _arg(c, "--scenarios")) for c in calls if c["tool"] == "run_suite"]
    agent = "returning_perlmutter_replay,returning_all_well,returning_new_laptop"
    assert suites == [("nersc", op, agent) for op in ("pi", "codex", "hermes21")]
    scripted = [(c["argv"], c["profile"]) for c in calls if c["tool"] == "run_smoke"]
    assert scripted == [(n, "nersc") for n in ("cached_password_otp", "home_outside_home", "noisy_failure_tail",
                                               "partition_proposal_policy")]
    assert not set(agent.split(",")) & set(_paid_only())
