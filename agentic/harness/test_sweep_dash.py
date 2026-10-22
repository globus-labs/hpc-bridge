"""agentic/sweep_dash.py reads a sweep's own summary and per-operator logs into the dashboard's cells."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import sweep_dash


def _sweep(runs: Path, stamp: str, prof: str, op: str, plan: str, log: str, summary_tail: str = "") -> None:
    with (runs / f"sweep-{stamp}.md").open("a") as f:
        f.write(f"# profile sweep {stamp}\n## {prof} [{op}] — free wave leaves out (paid_only): x\n"
                f"## {prof} [{op}] — 09:45 — {plan}\n{summary_tail}")
    (runs / f"sweep-{stamp}-{prof}-{op}.log").write_text(log)


def _repo(tmp_path: Path) -> Path:
    runs = tmp_path / "agentic" / "runs"
    runs.mkdir(parents=True)
    (tmp_path / "agentic" / "sweep_profiles.sh").write_text('MODELS="m"; PROFILES="default,pbs"\n')
    _sweep(runs, "20261016-1337", "default", "pi", "a,b,c",
           "", "    [pi] ✓ done   a · opus/default · u0 — RESULT: OK\n"
               "    [pi] ✗ done   b · opus/default · u1 — RESULT: FAILED — critical checks broke: ['x']\n")
    _sweep(runs, "20261021-0945", "default", "pi", "a,b,c",
           "▶ start  a · opus/default · u0\n▶ start  b · opus/default · u1\n"
           "✓ done   a · opus/default · u0 — RESULT: OK  (first result 41 s)\n"
           "⚠ done   b · opus/default · u1 — RESULT: INFRA: codex: the model answered with a tool call as JSON\n",
           "    [pi] spend_gate_enforced (no-skill ablation):\n    [pi] spend_gate_enforced — RESULT: OK  (first result 47 s)\n")
    _sweep(runs, "20261021-1010", "pbs", "pi", "d",
           "▶ start  d · opus/default · u0\n✗ done   d · opus/default · u0 — RESULT: FAILED — critical checks broke: "
           "['spend_follows_question', 'no_raw_ssh']\n")
    return tmp_path


def test_cells_take_their_state_from_the_live_log_and_the_ablation_from_the_summary(tmp_path):
    repo = _repo(tmp_path)
    base = sweep_dash.baseline(repo / "agentic" / "runs", "20261016-1337")
    assert base == {"default|pi|a": "OK", "default|pi|b": "FAILED"}
    st = sweep_dash.status(repo, "20261021-0945", base, ["default", "pbs"])
    assert st["stamps"] == ["20261021-0945", "20261021-1010"] and st["ops"] == ["pi"]
    cells = {c["scen"]: c for c in st["rows"][0]["ops"]["pi"]}
    assert cells["a"]["state"] == "OK" and cells["a"]["base"] == "OK"
    assert cells["b"]["state"] == "INFRA" and cells["b"]["base"] == "FAILED"     # ⚠ done is a result too
    assert cells["c"]["state"] == "PENDING" and cells["c"]["base"] is None
    assert cells["spend_gate_enforced (ablation)"]["state"] == "OK"
    assert cells["spend_gate_enforced (ablation)"]["base"] is None               # never the normal cell's result
    pbs = st["rows"][1]["ops"]["pi"][0]
    assert pbs["state"] == "FAILED" and pbs["detail"] == "spend_follows_question, no_raw_ssh"


def test_a_started_cell_is_running_until_its_result_and_a_bound_keeps_one_sweep(tmp_path):
    repo = _repo(tmp_path)
    runs = repo / "agentic" / "runs"
    (runs / "sweep-20261021-0945-default-pi.log").write_text("▶ start  a · opus/default · u0\n")
    st = sweep_dash.status(repo, "20261021-0945", {}, ["default"], until="20261021-0945")
    assert st["stamps"] == ["20261021-0945"]
    assert {c["scen"]: c["state"] for c in st["rows"][0]["ops"]["pi"]}["a"] == "RUNNING"
    assert [r["profile"] for r in st["rows"]] == ["default"]   # the bound left pbs's sweep out


def test_the_grid_rows_follow_sweep_profiles_sh(tmp_path):
    assert sweep_dash.default_profiles(_repo(tmp_path)) == "default,pbs"
    assert sweep_dash.default_profiles(tmp_path / "nowhere") == "default"
