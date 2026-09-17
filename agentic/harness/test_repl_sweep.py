"""Hermetic tests for the REPL sweep driver and its graph: the plan, parsing, preflight, one cell against a stand-in
run_smoke.sh, resume, and the HTML graph from synthetic bundles. No agent, no Docker, no cluster."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import repl_protocol as rp  # noqa: E402
from test_repl_protocol import _outputs, _write_bundle  # noqa: E402


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, HERE.parent / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod   # dataclasses look their module up here
    spec.loader.exec_module(mod)
    return mod


sweep = _load("repl_sweep")
plot = _load("repl_plot")


# ---- the plan ------------------------------------------------------------------------------------------------------

def test_rounds_alternate_which_channel_goes_first():
    cells = sweep.plan(4)
    assert [c["channel"] for c in cells] == ["local", "bridge", "bridge", "local", "local", "bridge", "bridge", "local"]
    first = [cells[2 * r]["channel"] for r in range(4)]
    assert first.count("local") == first.count("bridge") == 2   # each channel leads equally often
    assert {c["scenario"] for c in cells} == {"repl_baseline_local", "repl_interaction"}
    assert all(c["status"] == "pending" for c in cells) and [c["k"] for c in cells] == list(range(8))


def test_estimate_counts_only_what_is_left():
    cells = sweep.plan(2)
    (ls, lu), (bs, bu) = sweep.ESTIMATE[("local", None)], sweep.ESTIMATE[("bridge", "fake")]
    secs, usd = sweep.estimate(cells)
    assert secs == 2 * (ls + bs) and round(usd, 2) == round(2 * (lu + bu), 2)
    cells[0]["status"] = "done"                     # round 1 starts with the local baseline
    assert sweep.estimate(cells)[0] == secs - ls


def test_parse_output_takes_the_label_and_the_bundle():
    out = ("running 'repl_interaction'…\n  → run_shell({})\nRESULT: FAILED — critical checks broke: ['x']\n"
           "record: agentic/runs/17-repl_interaction\n")
    assert sweep.parse_output(out) == ("FAILED", "agentic/runs/17-repl_interaction")
    assert sweep.parse_output("RESULT: SETUP FAILED — scenario not run\n")[0] == "SETUP FAILED"
    assert sweep.parse_output("RESULT: RATE_LIMITED — session/rate limit\n")[0] == "RATE_LIMITED"
    assert sweep.parse_output("RESULT: OK\n") == ("OK", None)
    assert sweep.parse_output("nothing here") == (None, None)


def test_preflight_names_what_would_waste_a_cell():
    token = {"CLAUDE_CODE_OAUTH_TOKEN": "x"}
    assert sweep.preflight(token, docker_ps=lambda: "hpcb-fake-login-1\nhpcb-fake-c1-1\n") == []
    assert any("fake cluster is not up" in p for p in sweep.preflight(token, docker_ps=lambda: "other\n"))
    assert any("Docker is not running" in p for p in sweep.preflight(token, docker_ps=lambda: None))


# ---- one cell, against a stand-in run_smoke.sh ---------------------------------------------------------------------

def test_run_cell_logs_the_output_and_reads_the_verdict(tmp_path, capsys):
    bundle = tmp_path / "bundle-1"
    bundle.mkdir()
    smoke = tmp_path / "run_smoke.sh"
    smoke.write_text(f'#!/usr/bin/env bash\necho "running \'$1\'…"\necho "  → run_shell(x)"\n'
                     f'echo "RESULT: OK"\necho "record: {bundle}"\n')
    cell = sweep.plan(1)[0]
    done = sweep.run_cell(cell, sweep=tmp_path, build=False, model=None, smoke=smoke)
    assert done["result"] == "OK" and done["bundle"] == str(bundle) and done["status"] == "done" and done["rc"] == 0
    log = (sweep.REPO / done["log"])
    assert "RESULT: OK" in log.read_text() and "repl_baseline_local" in log.name
    assert "RESULT: OK" in capsys.readouterr().out


def test_a_cell_without_a_bundle_is_recorded_as_such(tmp_path):
    smoke = tmp_path / "run_smoke.sh"
    smoke.write_text('#!/usr/bin/env bash\necho "ERROR: set CLAUDE_CODE_OAUTH_TOKEN"\nexit 1\n')
    done = sweep.run_cell(sweep.plan(1)[1], sweep=tmp_path, build=True, model="claude-opus-5", smoke=smoke)
    assert done["bundle"] is None and done["result"] == "NO RESULT (rc=1)"


def test_dry_run_prints_the_plan_and_runs_nothing(capsys):
    assert sweep.main(["--repeat", "2", "--dry-run"]) == 0
    out = capsys.readouterr().out
    secs, usd = sweep.estimate(sweep.plan(2))
    assert "4 of 4 cells to run" in out and f"estimate: about {secs / 60:.0f} min and ${usd:.2f}" in out


def test_resume_of_a_finished_sweep_needs_no_docker(tmp_path, capsys):
    cells = [{**c, "status": "done", "bundle": None} for c in sweep.plan(1)]
    (tmp_path / "manifest.json").write_text(json.dumps({"sweep_id": "t", "repeat": 1, "cells": cells}))
    assert sweep.main(["--resume", str(tmp_path), "--no-plot"]) == 0
    assert "nothing to run" in capsys.readouterr().out


# ---- the graph -----------------------------------------------------------------------------------------------------

def _pair_bundles(runs: Path, n: int) -> list[Path]:
    dirs = []
    for i in range(n):
        bridge_rows, local_rows = [], []
        for cmd, out in _outputs(env=True):
            err = cmd == rp.ERROR.command
            res = {"phase": "complete", "exit_code": 1 if err else 0, "stdout": "" if err else out}
            bridge_rows.append(("mcp__endpoint__run_shell", {"command": cmd, "session_id": rp.SESSION, "shape": "compute"},
                                json.dumps(res)))
        for cmd, out in _outputs(env=False):
            local_rows.append(("Bash", {"command": cmd}, out))
        dirs.append(_write_bundle(runs, f"b{i}", "repl_interaction", bridge_rows, latency=1.5 + i, spacing=2.0))
        dirs.append(_write_bundle(runs, f"l{i}", "repl_baseline_local", local_rows, latency=0.25, spacing=1.5))
    return dirs


def test_graph_decomposes_the_mean_turn_and_counts_every_step(tmp_path):
    runs = plot.collect(_pair_bundles(tmp_path, 2))
    assert sorted(r.channel for r in runs) == ["bridge", "bridge", "local", "local"] and all(r.timed for r in runs)
    _, stats = plot.turn_view(runs)
    assert round(stats["bridge@fake"]["tool"], 3) == 2.0 and round(stats["bridge@fake"]["agent"], 3) == 2.0   # tools 1.5 and 2.5
    assert round(stats["local"]["tool"], 3) == 0.25 and round(stats["local"]["agent"], 3) == 1.5
    page = plot.render(runs, {"sweep_id": "20260914-150000"})
    assert page.count('class="dot"') == 4 * (len(rp.PROTOCOL) - 1)          # every warm step of every run
    assert page.count('class="tool mark"') == 2 and page.count('class="agent mark"') == 2
    assert "took 1.75 s on the local shell. On hpc-bridge it took 4.00 s on the fake cluster (2.3×" in page
    assert "sweep 20260914-150000" in page and "prefers-color-scheme:dark" in page and "textContent" in page
    assert "✗</span> 0 of 2" in page      # local fails env carry-over, as expected
    assert page.count("<tr>") == len(plot.PROPS) + 1 + 4 + 1                 # props table + every-run table rows


def test_graph_escapes_names_and_reads_a_sweep_manifest(tmp_path):
    dirs = _pair_bundles(tmp_path / "runs", 1)
    manifest = {"sweep_id": "s", "repo_root": "/", "cells": [{"bundle": str(d)} for d in dirs] + [{"bundle": None}]}
    sw = tmp_path / "sweep"
    sw.mkdir()
    (sw / "manifest.json").write_text(json.dumps(manifest))
    found, meta = plot.bundles_from(sw)
    assert found == dirs and meta["sweep_id"] == "s"
    runs = plot.collect(found)
    runs[0].bundle = '<script>alert("x")</script>'
    page = plot.render(runs, meta)
    assert "<script>alert" not in page and "&lt;script&gt;" in page
    assert plot.main([str(sw)]) == 0 and (sw / "repl-sweep.html").exists()


def test_graph_with_one_channel_still_renders(tmp_path):
    dirs = [d for d in _pair_bundles(tmp_path, 1) if d.name.endswith("repl_baseline_local")]
    page = plot.render(plot.collect(dirs))
    assert "On average" not in page and "hpc-bridge ·" not in page   # no headline, and no empty hpc-bridge row


def test_slide_svg_is_one_scoped_light_mode_svg(tmp_path):
    runs = plot.collect(_pair_bundles(tmp_path, 2))
    svg = plot.slide_svg(runs)
    assert svg.startswith("<svg ") and svg.count("<svg ") == 3                 # the outer one, holding both charts
    assert "var(--" not in svg and "<style>" in svg
    assert all(rule.startswith(".repl-graph ") for rule in svg.split("<style>")[1].split("</style>")[0].split("}") if rule)
    assert "\n\n" not in svg                                                  # a blank line would break inlining into Markdown
    assert svg.count('class="tool mark"') == 2 and svg.count('class="dot"') == 4 * (len(rp.PROTOCOL) - 1)


def test_slide_svg_puts_both_charts_on_one_axis(tmp_path):
    import re
    runs = plot.collect(_pair_bundles(tmp_path, 2))       # mean turns 4.0 s; slowest tool step 2.5 s
    svg = plot.slide_svg(runs)
    ticks = re.findall(r'class="tick"[^>]*>([^<]+)<', svg)
    half = len(ticks) // 2
    assert ticks[:half] == ticks[half:] and ticks[-1] == "5 s", ticks


# ---- targets: the rotated plan, the load probe, the idle-node wait ---------------------------------------------------

def test_two_targets_rotate_every_channel_through_every_position():
    cells = sweep.plan(3, ["fake", "globus1"])
    rounds = [[sweep.series(c) for c in cells if c["round"] == r] for r in range(3)]
    assert rounds == [["local", "bridge@fake", "bridge@globus1"],
                      ["bridge@fake", "bridge@globus1", "local"],
                      ["bridge@globus1", "local", "bridge@fake"]]
    assert all(c["target"] == "fake" for c in cells if c["channel"] == "local")   # the baseline never needs globus1
    assert sweep.plan(1, ["globus1"])[0]["target"] == "globus1"                  # …unless globus1 is all there is
    assert sweep.estimate(sweep.plan(1, ["globus1"]))[0] == sweep.ESTIMATE[("local", None)][0] + sweep.ESTIMATE[("bridge", "globus1")][0]


def test_cluster_load_counts_only_exactly_idle_nodes():
    part = "n1 idle\nn2 alloc\nn3 drain\nn4 idle*\nn5 idle\n"
    out = part + "---\n" + part + "---\nsvc-inference\nsvc-inference\nalice\n"
    load = sweep.cluster_load("globus1", probe=lambda t, cmd: out)
    assert load["idle"] == 2 and load["nodes"] == 5 and load["partition"] == "main"
    assert load["running_by_user"] == {"svc-inference": 2, "alice": 1}
    assert sweep.cluster_load("globus1", probe=lambda t, cmd: None)["idle"] is None
    garbled = "sinfo: error: something broke\n---\n---\n"
    assert sweep.cluster_load("globus1", probe=lambda t, cmd: garbled)["idle"] is None


def test_cluster_load_falls_back_to_every_node_once_when_the_partition_is_absent():
    # the fake cluster running the `site` profile: no `main` partition, nodes listed under several partitions
    everywhere = "c1 idle\nc1 idle\nc2 idle\nc2 alloc\nc3 mix\n"    # c1 in two partitions; c2's first listing wins
    out = "---\n" + everywhere + "---\n"
    load = sweep.cluster_load("globus1", probe=lambda t, cmd: out)
    assert load["partition"] == "all partitions" and load["nodes"] == 3 and load["idle"] == 2


def test_wait_for_node_admits_waits_or_gives_up():
    clock = {"t": 0.0}
    def tick(s):
        clock["t"] += s
    snaps = iter([{"idle": 0, "partition": "main", "running_by_user": {"x": 1}},
                  {"idle": 0, "partition": "main", "running_by_user": {"x": 1}},
                  {"idle": 1, "nodes": 3, "partition": "main", "running_by_user": {}}])
    got = sweep.wait_for_node("globus1", 600, load=lambda t: next(snaps), sleep=tick, clock=lambda: clock["t"])
    assert got["admitted"] is True and got["waited_s"] == 2 * sweep.NODE_POLL_S

    clock["t"] = 0.0
    busy = {"idle": 0, "partition": "main", "running_by_user": {}}
    got = sweep.wait_for_node("globus1", 90, load=lambda t: busy, sleep=tick, clock=lambda: clock["t"])
    assert got["admitted"] is False and got["waited_s"] >= 90

    got = sweep.wait_for_node("globus1", 90, load=lambda t: {"idle": None, "partition": "main", "running_by_user": {}},
                              sleep=tick, clock=lambda: 0.0)
    assert got["admitted"] is None          # an unknown probe launches unguarded, never never-launch


def test_preflight_for_globus1_needs_the_key_and_the_alias(tmp_path):
    env = {"CLAUDE_CODE_OAUTH_TOKEN": "x", "HPCB_TEST_SSH_KEY": str(tmp_path / "missing")}
    probs = sweep.preflight(env, ["globus1"], docker_ps=lambda: "", reach=lambda t: False)
    assert any("scoped test key" in p for p in probs) and any("cannot reach" in p for p in probs)
    key = tmp_path / "key"
    key.write_text("k")
    assert sweep.preflight({**env, "HPCB_TEST_SSH_KEY": str(key)}, ["globus1"], docker_ps=lambda: "",
                           reach=lambda t: True) == []
    assert any("unknown target" in p for p in sweep.preflight(env, ["nersc"], docker_ps=lambda: ""))


def test_run_cell_points_run_smoke_at_the_cells_target(tmp_path):
    bundle = tmp_path / "b"
    bundle.mkdir()
    smoke = tmp_path / "run_smoke.sh"
    smoke.write_text(f'#!/usr/bin/env bash\necho "target=$HPCB_TARGET"\necho "RESULT: OK"\necho "record: {bundle}"\n')
    cell = sweep.plan(1, ["fake", "globus1"])[2]
    done = sweep.run_cell(cell, sweep=tmp_path, build=False, model=None, smoke=smoke)
    log = (sweep.REPO / done["log"]).read_text()
    assert "target=globus1" in log and done["log"].endswith("-globus1.log")


def test_graph_gives_each_target_its_own_row(tmp_path):
    dirs = _pair_bundles(tmp_path, 1)
    g1 = [d for d in _pair_bundles(tmp_path / "g1", 1) if d.name.endswith("repl_interaction")]
    for d in g1:
        rec = json.loads((d / "record.json").read_text())
        rec["config"]["target"] = "globus1"
        (d / "record.json").write_text(json.dumps(rec))
    runs = plot.collect(dirs + g1)
    assert plot.series_order(runs) == ["local", "bridge@fake", "bridge@globus1"]
    page = plot.render(runs)
    assert "hpc-bridge · fake cluster" in page and "hpc-bridge · Globus Cluster" in page
    assert "on the fake cluster (" in page and "on the Globus Cluster (" in page
    assert page.count('class="median"') == 3 and page.count('class="tool mark"') == 3
    svg = plot.slide_svg(runs)
    assert svg.count('class="rowlabel"') == 6            # three rows in each chart


def test_slide_svg_is_drawn_at_the_slides_content_width(tmp_path):
    import re
    runs = plot.collect(_pair_bundles(tmp_path, 2))
    svg = plot.slide_svg(runs)
    assert re.match(r'<svg [^>]*viewBox="0 0 1136 (\d+)"', svg)
    height = int(re.match(r'<svg [^>]*viewBox="0 0 1136 (\d+)"', svg).group(1))
    assert height <= 500                               # fits between the slide title and its footer band at 1:1
    assert 'font-size:18px' in svg and 'x="0" y="18">Where a REPL turn' in svg   # projector type; headings on the left edge
