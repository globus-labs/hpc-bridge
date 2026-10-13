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
    assert set(cells) == {"default", "site", "totp", "pbs", "lmod", "f2b", "polaris", "internal", "hostile", "mep"}
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
            if operator_refusal(op, persona=getattr(mod, "PERSONA", None), phases=bool(getattr(mod, "PHASES", None)),
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
