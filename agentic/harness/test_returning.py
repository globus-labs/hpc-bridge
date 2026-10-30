"""Hermetic: the returning-user cells (returning.py) — the pairwise generator and the checked-in cell table, the previous
release's tag, the mixed-operator chain (run.py `_phase_spec`, `_combine`'s markers, regrade's reader, the free
operators' seeded chains), the scripted session that runs on a server with no journal, the node actions the
interphase uses, and every grader on what a good and a bad session 2 would answer. No agent, no cluster."""
from __future__ import annotations

import asyncio
import json
import sys
import types
from itertools import combinations
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import node_actions  # noqa: E402
import pairwise  # noqa: E402
import prev_release  # noqa: E402
import returning as ru  # noqa: E402
from invariants import ToolCall, Trace  # noqa: E402
from midrun_hooks import operator_refusal, seeded_chain  # noqa: E402


def _run_module(monkeypatch):
    """run.py, importable without the Claude Agent SDK (only the jail has it): a stub, as test_server_stderr does."""
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher",
                 "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    for m in ("runner", "run"):
        sys.modules.pop(m, None)
    import run
    return run


# ---- the generator and the table ----------------------------------------------------------------------------------------

def test_the_checked_in_table_is_what_the_generator_makes():
    # a stale table would run cells nobody reviewed under ids that moved: regenerate with --write-cells
    assert ru.CELLS_FILE.read_text() == ru.render_table(ru.generated_cells())


def test_every_valid_pair_is_covered_and_no_cell_breaks_a_constraint():
    cells = list(ru.load_cells().values())
    rows = [c.levels for c in cells]
    assert pairwise.uncovered_pairs(rows, ru.FACTORS, ru.CONSTRAINTS) == []
    for c in cells:
        assert set(c.levels) == set(ru.FACTORS), c.id
        assert all(c.levels[f] in ru.FACTORS[f] for f in ru.FACTORS), c.id
        assert pairwise.violations(c.levels, ru.FACTORS, ru.CONSTRAINTS) == [], c.id
    assert [c.id for c in cells] == [f"c{n:02d}" for n in range(1, len(cells) + 1)]
    assert cells[0].levels == {f: levels[0] for f, levels in ru.FACTORS.items()}   # the control: nothing changed
    # far fewer than the full product (7680), never fewer than the two largest factors' pairs (ended × local: 20)
    assert 20 <= len(cells) <= 28
    assert {c.ended for c in cells} >= {"killed_earlier", "killed_recent"}
    # the constraints are real: neither forbidden pair is in any cell, both are otherwise reachable
    for pair, _why in ru.CONSTRAINTS:
        assert not any(all(c.levels[f] == v for f, v in pair.items()) for c in cells)
    # every cell's every changed level BITES (the review of 2026-10-30 found no-op cells: a changed host key behind a
    # reattach that makes no SSH, a dead pin a teardown had removed)
    assert all(ru.valid_row(c.levels) for c in cells)
    assert "shared_host" not in ru.FACTORS   # dropped: it did nothing under the run's one endpoint name (returning.py)


def test_a_whole_row_rule_is_kept_and_its_rows_mended():
    factors = {"a": ("0", "1"), "b": ("0", "1"), "c": ("0", "1")}

    def valid(row):   # a=1 only where b=1 or c=1 (the shape of `bites`: a change only where something lets it act)
        return row["a"] == "0" or row["b"] == "1" or row["c"] == "1"
    rows = pairwise.generate(factors, [], valid=valid)
    assert all(valid(r) for r in rows) and pairwise.uncovered_pairs(rows, factors, []) == []
    assert rows == pairwise.generate(factors, [], valid=valid)


def test_the_generator_is_deterministic_and_pairwise_on_a_small_space():
    factors = {"a": ("0", "1", "2"), "b": ("0", "1"), "c": ("0", "1"), "d": ("x", "y", "z")}
    forbidden = [({"a": "2", "d": "z"}, "cannot")]
    rows = pairwise.generate(factors, forbidden)
    assert rows == pairwise.generate(factors, forbidden)
    assert pairwise.uncovered_pairs(rows, factors, forbidden) == []
    assert not any(r["a"] == "2" and r["d"] == "z" for r in rows)
    want = {((fa, la), (fb, lb)) for fa, fb in combinations(factors, 2) for la in factors[fa] for lb in factors[fb]}
    assert pairwise.all_pairs(factors, forbidden) == want - {(("a", "2"), ("d", "z"))}
    with pytest.raises(ValueError):
        pairwise.generate(factors, [({"a": "9", "b": "0"}, "no such level")])
    with pytest.raises(ValueError):
        pairwise.check_constraints(factors, [({"a": "0"}, "a constraint names two factors")])
    # a factor whose every level is forbidden beside a seed's: no row can be built for what is left
    with pytest.raises(ValueError):
        pairwise.generate({"a": ("0", "1"), "b": ("0",), "c": ("0",)},
                          [({"a": "1", "c": "0"}, "x"), ({"b": "0", "c": "0"}, "y")])


def test_a_cell_is_picked_by_its_id_and_an_unknown_one_is_refused(monkeypatch):
    monkeypatch.delenv(ru.LEVELS_ENV, raising=False)
    monkeypatch.setenv(ru.CELL_ENV, "c03")
    assert ru.cell_from_env().id == "c03" and ru.cell_from_env().origin == ru.load_cells()["c03"].levels["origin"]
    monkeypatch.setenv(ru.CELL_ENV, "c99")
    with pytest.raises(ValueError):
        ru.cell_from_env()
    monkeypatch.delenv(ru.CELL_ENV)
    assert ru.cell_from_env().id == "c01"


def test_a_cell_is_graded_under_the_levels_it_ran(monkeypatch):
    # the bundle's env keeps the levels (regrade replays HPCB_RETURNING_LEVELS): a regrade after the table was
    # regenerated grades the cell that RAN, not whatever the id names now (the review of 2026-10-30)
    import regrade
    assert "HPCB_RETURNING_LEVELS" in regrade.RUN_ENV
    monkeypatch.delenv(ru.LEVELS_ENV, raising=False)
    monkeypatch.setenv(ru.CELL_ENV, "c02")
    picked = ru.cell_from_env()
    carried = json.loads(__import__("os").environ[ru.LEVELS_ENV])
    assert carried == {"id": "c02", "levels": picked.levels}
    ran = {**{f: v[0] for f, v in ru.FACTORS.items()}, "ended": "stopped", "tokens": "expired"}
    monkeypatch.setenv(ru.LEVELS_ENV, json.dumps({"id": "c02", "levels": ran}))
    assert ru.cell_from_env().levels == ran                      # its own levels, whatever the table says now
    monkeypatch.setenv(ru.CELL_ENV, "c03")
    assert ru.cell_from_env().levels == ru.load_cells()["c03"].levels   # another cell's levels are not carried over
    monkeypatch.setenv(ru.CELL_ENV, "c02")
    monkeypatch.setenv(ru.LEVELS_ENV, json.dumps({"id": "c02", "levels": {"origin": "current"}}))
    with pytest.raises(ValueError):
        ru.cell_from_env()


def test_the_agent_cells_are_the_three_hand_picked_stories():
    replay, well, laptop = (ru.agent_cell(n) for n in ("perlmutter_replay", "all_well", "new_laptop"))
    assert (replay.origin, replay.ended, replay.pin, replay.auth, replay.local) == (
        "previous", "running", "dead", "revoked", "untouched")
    assert (well.origin, well.ended, well.pin, well.auth, well.local) == ("previous", "running", "alive", "key",
                                                                          "untouched")
    assert (laptop.origin, laptop.ended, laptop.local) == ("previous", "running", "wiped")
    for c in (replay, well, laptop):
        assert c.agent and "ru_no_hand_edits" in ru.expect_ok(c)
    assert "ru_reused" in ru.expect_ok(well) and "ru_reattached" in ru.expect_ok(laptop)
    assert {"ru_dead_pin", "ru_revoked_handoff"} <= set(ru.expect_ok(replay))
    # the replay runs the report's own plugin (0.1.20) whatever `previous` is; the others the release before this one
    assert ru.release_slot(replay) == "0.1.20" in prev_release.PINNED
    assert ru.release_slot(well) == ru.release_slot(laptop) == prev_release.PREVIOUS
    first = ru.phases(replay)[0]
    assert first["server"] == "0.1.20" and first["premise"] and first["env"] == {}
    assert ru.phases(well)[0]["env"] == {"HPC_BRIDGE_REMOTE_VENV": ru.OLD_VENV}   # its /home default fails on nersc


def test_each_factor_level_gates_its_own_expectation():
    cells = ru.load_cells()
    for c in cells.values():
        gated = set(ru.expect_ok(c))
        assert {"ru_session_1", "ru_hello_world", "ru_bounded", "ru_explicit_args", "harness:phase_server"} <= gated
        # each factor with a grader of its own: gated in EVERY cell where its level is not the default, and it bites
        for factor, grader in ru.FACTOR_GRADERS.items():
            changed = c.levels[factor] != ru.FACTORS[factor][0]
            assert (grader in gated) == changed, (c.id, factor)
            assert not changed or ru.BITES.get(factor, lambda _lv: True)(c.levels), (c.id, factor)
        assert ("ru_killed_not_up" in gated) == c.ended.startswith("killed_")
        assert ("ru_reused" in gated) == ru.reuse_expected(c.levels)
        assert ("ru_same_endpoint" in gated) == ru.same_endpoint_expected(c.levels)
        assert ("ru_no_rebuild" in gated) == ru.no_rebuild_expected(c.levels)
    assert ru.reuse_expected(cells["c01"].levels)   # nothing changed: the endpoint must be reused


def test_only_a_deletion_forces_another_endpoint_and_only_a_venv_change_a_rebuild():
    # the review of 2026-10-30: the old `reuse_expected` gated "same endpoint" in one cell; a stopped or killed manager,
    # a new laptop, a renamed id, a dead pin, a revoked key, expired tokens, a changed host key or policy, an older
    # origin force neither a second registration nor a rebuild
    base = {f: v[0] for f, v in ru.FACTORS.items()}
    harmless = [{"ended": "stopped"}, {"ended": "killed_earlier"}, {"ended": "killed_recent"}, {"local": "wiped"},
                {"local": "renamed_id"}, {"local": "record_deleted"}, {"pin": "dead"}, {"auth": "revoked"},
                {"tokens": "expired"}, {"host_key": "changed", "local": "wiped"}, {"site": "policy_changed"},
                {"origin": "previous"}, {"origin": "previous", "ended": "stopped"}]
    for change in harmless:
        lv = {**base, **change}
        assert ru.same_endpoint_expected(lv) and ru.no_rebuild_expected(lv), change
    assert not ru.same_endpoint_expected({**base, "ended": "torn_down"})
    assert not ru.same_endpoint_expected({**base, "remote": "endpoint_dir_deleted"})
    assert not ru.no_rebuild_expected({**base, "remote": "venv_deleted"})
    assert not ru.no_rebuild_expected({**base, "gce": "changed"})
    killed = ru.Cell("x", {**base, "ended": "killed_earlier"})
    assert {"ru_same_endpoint", "ru_no_rebuild"} <= set(ru.expect_ok(killed))


# ---- the previous release -----------------------------------------------------------------------------------------------

# This repo's first-parent history of pyproject.toml, as `prev_release.history` reads it (newest first), cut short:
# the release commits are where the version changed; the commits between carry it on toward the next release
HISTORY = [("fbe2eb7", "2026-10-20T10:00:00-05:00", "0.1.25"), ("7880898", "2026-10-20T09:00:00-05:00", "0.1.24"),
           ("85e0c22", "2026-10-16T09:00:00-05:00", "0.1.24"), ("9c75742", "2026-10-13T18:38:54-05:00", "0.1.24"),
           ("5ce62f6", "2026-10-13T09:00:00-05:00", "0.1.23"), ("b19581d", "2026-10-06T12:30:39-05:00", "0.1.20"),
           ("743de8c", "2026-10-06T10:00:00-05:00", "0.1.20-beta.1"), ("0000000", "2026-01-01T00:00:00Z", "garbage")]


def test_the_previous_release_is_the_newest_release_commit_older_than_this_version():
    # the review of 2026-10-30: choosing by TAG made `previous` v0.1.20-beta.1 under 0.1.25 (only three tags exist) —
    # four releases back; the version history names 0.1.24, at the commit that made it (9c75742)
    rels = prev_release.releases(HISTORY)
    assert [(r.version, r.commit) for r in rels] == [("0.1.25", "fbe2eb7"), ("0.1.24", "9c75742"),
                                                    ("0.1.23", "5ce62f6"), ("0.1.20", "b19581d"),
                                                    ("0.1.20-beta.1", "743de8c")]
    prev = prev_release.previous_release(rels, "0.1.25")
    assert (prev.version, prev.commit, prev.date) == ("0.1.24", "9c75742", "2026-10-13T18:38:54-05:00")
    assert prev_release.previous_release(rels, "0.1.26").version == "0.1.25"      # a release moves it forward
    assert prev_release.previous_release(rels, "0.1.24").version == "0.1.23"      # never this version's own
    assert prev_release.previous_release(rels, "0.1.20").version == "0.1.20-beta.1"   # a pre-release is older
    assert prev_release.previous_release(rels, "0.1.1") is None
    assert prev_release.previous_release(rels, "garbage") is None
    assert prev_release.pinned_release(rels, "0.1.20").commit == "b19581d"
    assert prev_release.pinned_release(rels, "0.1.19") is None


def test_versions_order_numerically_pre_releases_included():
    k = prev_release.version_key
    assert k("0.1.20-beta.10") > k("0.1.20-beta.9") > k("0.1.20-beta.1")   # numerically, not as text
    assert k("0.1.20") > k("0.1.20-beta.10") and k("v0.1.21-beta.1") > k("0.1.20")
    assert k("0.1.10") > k("0.1.9") and k("0.1.20-rc.1") > k("0.1.20-beta.2")
    assert k("0.1.20-beta.2") > k("0.1.20-beta")                          # more parts: later (semver)
    assert k("not-a-version") is None and k("0.1") is None


def test_the_image_carries_each_release_slot_or_says_it_does_not(tmp_path):
    assert prev_release.server_argv("previous", tmp_path) is None and prev_release.installed("previous", tmp_path) is None
    slot = tmp_path / "previous"
    (slot / "venv" / "bin").mkdir(parents=True)
    (slot / "venv" / "bin" / "hpc-bridge").write_text("")
    assert prev_release.server_argv("previous", tmp_path) is None   # no VERSION/COMMIT/DATE: the export never finished
    for name, value in (("VERSION", "0.1.24"), ("COMMIT", "9c75742"), ("DATE", "2026-10-13T23:38:54Z")):
        (slot / name).write_text(value + "\n")
    assert prev_release.server_argv("previous", tmp_path) == [str(slot / "venv" / "bin" / "hpc-bridge")]
    assert prev_release.installed("previous", tmp_path) == {"version": "0.1.24", "commit": "9c75742",
                                                            "date": "2026-10-13T23:38:54Z"}
    meta = slot / "venv" / "lib" / "python3.11" / "site-packages" / "hpc_bridge-0.1.24.dist-info"
    meta.mkdir(parents=True)
    (meta / "METADATA").write_text("Metadata-Version: 2.4\nName: hpc-bridge\nVersion: 0.1.24\n")
    assert prev_release.installed_version(slot / "venv") == "0.1.24"
    with pytest.raises(ValueError):
        prev_release.slot_dir("../etc", tmp_path)
    dockerfile = (HERE.parent / "Dockerfile").read_text()
    assert "COPY agentic/.prev-release /opt/hpcb-prev" in dockerfile and "--frozen --no-editable" in dockerfile
    assert 'for d in /opt/hpcb-prev/*/' in dockerfile and "|| exit 1" in dockerfile   # every slot, or the build fails
    assert "prev_release.py\" export" in (HERE.parent / "run_smoke.sh").read_text()
    assert "_export_previous_release()" in (HERE.parent / "run_suite.py").read_text()


def test_a_failed_export_is_said_loudly(monkeypatch, capsys):
    # the review of 2026-10-30: run_suite swallowed the export's failure (capture_output, check=False)
    import subprocess as sp
    sys.path.insert(0, str(HERE.parent))
    import run_suite

    def failed(*a, **k):
        return sp.CompletedProcess(a, 1, "", "prev_release: `git log --first-parent` failed: not a git repository")
    monkeypatch.setattr(run_suite.subprocess, "run", failed)
    why = run_suite._export_previous_release()
    out = capsys.readouterr()
    assert why and "not a git repository" in why
    assert "ERROR: the older hpc-bridge releases could not be exported" in out.out and "SETUP FAILED" in out.out


# ---- the chain ----------------------------------------------------------------------------------------------------------

def test_a_phase_names_its_operator_and_server(monkeypatch):
    run = _run_module(monkeypatch)
    assert run._phase_spec("hi {x}", lambda s: s.replace("{x}", "there")) == {"prompt": "hi there"}

    async def s1(ops):
        return ""
    spec = run._phase_spec({"script": s1, "server": "previous"})
    assert spec["operator"] == "script" and run._phase_label(spec, "pi") == "script@previous"
    assert run._phase_label(run._phase_spec("go"), "pi") == "pi"
    assert run._phase_label(run._phase_spec({"script": s1, "server": "0.1.20"}), "pi") == "script@0.1.20"
    with pytest.raises(ValueError):
        run._phase_spec({"script": s1, "operator": "pi"})
    with pytest.raises(ValueError):
        run._phase_spec({"script": s1, "server": "nightly"})
    with pytest.raises(ValueError):
        run._phase_spec({"script": s1, "server": "0.1.19"})   # a release the image does not pin


def test_each_scripted_phase_says_which_server_it_ran_and_the_gate_reads_it(monkeypatch, tmp_path):
    # the review of 2026-10-30: nothing in a bundle said which version session 1 ran (the head row cut the server's
    # argv to 8 words); `harness:phase_server` gates that it ran the release it declared, older than this checkout
    run = _run_module(monkeypatch)
    slot = tmp_path / "previous"
    for name, value in (("VERSION", "0.1.24"), ("COMMIT", "9c75742"), ("DATE", "2026-10-13T23:38:54Z")):
        slot.mkdir(exist_ok=True)
        (slot / name).write_text(value + "\n")
    meta = slot / "venv" / "lib" / "python3.11" / "site-packages" / "hpc_bridge-0.1.24.dist-info"
    meta.mkdir(parents=True)
    (meta / "METADATA").write_text("Version: 0.1.24\n")
    rel = run.server_release("previous", tmp_path)
    assert rel["ok"] and rel["version"] == rel["want"] == "0.1.24" and rel["commit"] == "9c75742"
    (meta / "METADATA").write_text(f"Version: {rel['current']}\n")       # the current code under the old name
    assert not run.server_release("previous", tmp_path)["ok"]
    assert not run.server_release("0.1.20", tmp_path)["ok"]               # not in the image
    row = run.phase_server_row([{"chain_phase": 0}, {"harness": "script", "server_release": rel}, {"final": ""}])
    assert row.name == "harness:phase_server" and row.ok and "previous: ran 0.1.24, want 0.1.24" in row.detail
    bad = run.phase_server_row([{"harness": "script", "server_release": {**rel, "version": "0.1.26", "ok": False}}])
    assert not bad.ok
    assert run.phase_server_row([{"harness": "pi"}]) is None
    import script_runner
    assert '"server": list(cmd)' in Path(script_runner.__file__).read_text()   # the whole argv, never cut


def _agent_phase(final_error: bool, said: str = ""):
    from runner import RunResult

    class Final:
        is_error = final_error
        result = said
        total_cost_usd = 0
    return RunResult(trace=Trace([]), final=Final(), messages=[{"harness": "x", "stderr_tail": said}])


def test_a_chain_stops_on_a_failed_premise_and_names_a_failed_world_change(monkeypatch):
    # the review of 2026-10-30: a session 1 that did not finish was graded on session 2's rows (FAILED), and an
    # interphase that raised printed to stderr only; both now stop the chain, SETUP FAILED, with the reason in a row
    run = _run_module(monkeypatch)
    ran = []

    async def agent(prompt, **k):
        ran.append(prompt)
        return _agent_phase(prompt == "s1", "script aborted: session 1: the compute block did not come up")

    Scen = types.SimpleNamespace(INTERPHASE_DELAY_S=0, INTERPHASE_SETUP=[])
    phases = [run._phase_spec({"prompt": "s1", "premise": True}), run._phase_spec("s2")]
    out = asyncio.run(run._run_chain(phases, Scen, model="m", effort=None, persona=None, user_goal="", no_skill=False,
                                     run_agent=agent))
    assert ran == ["s1"] and "premise" in out.chain_failure and "compute block did not come up" in out.chain_failure

    def world(ctx):
        raise RuntimeError("the endpoint's directory was not deleted")
    world.__name__ = "returning_world"
    Scen.INTERPHASE_SETUP = [world]
    ran.clear()
    out = asyncio.run(run._run_chain([run._phase_spec("ok"), run._phase_spec("s2")], Scen, model="m", effort=None,
                                     persona=None, user_goal="", no_skill=False, run_agent=agent))
    assert ran == ["ok"] and "returning_world: RuntimeError: the endpoint's directory was not deleted" in out.chain_failure
    ran.clear()
    Scen.INTERPHASE_SETUP = []
    out = asyncio.run(run._run_chain([run._phase_spec("ok"), run._phase_spec("s2")], Scen, model="m", effort=None,
                                     persona=None, user_goal="", no_skill=False, run_agent=agent))
    assert ran == ["ok", "s2"] and out.chain_failure is None
    src = Path(run.__file__).read_text()
    assert 'Result("harness:chain", False' in src and 'result_label = "SETUP FAILED"' in src


def test_a_mixed_chain_marks_each_phase_and_regrade_reads_each_with_its_own_reader(monkeypatch):
    run = _run_module(monkeypatch)
    import regrade
    from runner import RunResult

    script_rows = [{"harness": "script", "rc": 0, "trace_stats": {"steps_as_record": 1, "journal_unplaced": 0}},
                   {"native": {"name": "connect_facility", "hpcb": True, "args": {"facility": "f"},
                               "result": {"phase": "needs_account"}, "ran": None, "call_id": "step-0"}},
                   {"text": ru._FACTS + json.dumps({"eid": "e", "pin": "login01.hpcb.test"}), "at": 1}, {"final": "s1"}]
    sdk_rows = [{"__type__": "SystemMessage", "subtype": "init", "data": {"session_id": "s2"}},
                {"__type__": "AssistantMessage", "content": [
                    {"__type__": "ToolUseBlock", "id": "t1", "name": "mcp__hpc-bridge__connect_facility",
                     "input": {"facility": "f"}}]},
                {"__type__": "UserMessage", "content": [
                    {"__type__": "ToolResultBlock", "tool_use_id": "t1",
                     "content": [{"type": "text", "text": json.dumps({"phase": "needs_account", "reused": True})}]}]},
                {"__type__": "AssistantMessage", "content": [{"__type__": "TextBlock", "text": "reused it"}]}]
    t1 = Trace([ToolCall.of("mcp__hpc-bridge__connect_facility", {"facility": "f"}, {"phase": "needs_account"})],
               [script_rows[2]["text"]], [1])
    t2 = Trace([ToolCall.of("mcp__hpc-bridge__connect_facility", {"facility": "f"},
                            {"phase": "needs_account", "reused": True})], ["reused it"], [1])
    res = run._combine([RunResult(trace=t1, final=None, messages=script_rows),
                        RunResult(trace=t2, final=None, messages=sdk_rows)], ["script@previous", "claude"])
    assert res.messages[0] == {"chain_phase": 0, "operator": "script@previous"}
    assert {"chain_phase": 1, "operator": "claude"} in res.messages
    assert [c.phase for c in res.trace.calls] == [0, 1] and res.trace.text_at == [1, 2]
    back = regrade.chain_trace(res.messages)
    assert [(c.name, c.phase, (c.result or {}).get("reused")) for c in back.calls] == [
        ("connect_facility", 0, None), ("connect_facility", 1, True)]
    assert back.texts == res.trace.texts and back.text_at == res.trace.text_at
    assert ru.facts_of(back)["pin"] == "login01.hpcb.test"
    plain = run._combine([RunResult(trace=t2, final=None, messages=sdk_rows)])
    assert "chain_phase" not in json.dumps(plain.messages)            # a plain chain's bundle is as before


def test_the_free_operators_drive_a_chain_that_opens_with_a_script_and_no_other():
    async def s1(ops):
        return ""
    seeded = [{"script": s1, "server": "previous"}, "the agent's turn"]
    assert seeded_chain(seeded) and not seeded_chain(["a", "b"]) and not seeded_chain(True)
    for op in ("pi", "codex", "hermes21"):
        assert operator_refusal(op, persona=None, phases=seeded, hooks=None) is None
        assert operator_refusal(op, persona=None, phases=["a", "b"], hooks=None) == "cross-restart chains (PHASES)"
        assert operator_refusal(op, persona=None, phases=True, hooks=None) == "cross-restart chains (PHASES)"
    assert operator_refusal("hermes", persona=None, phases=seeded, hooks=None) == "cross-restart chains (PHASES)"
    assert operator_refusal("claude", persona=None, phases=seeded, hooks=None) is None
    import returning_all_well
    import returning_user
    import script_runner
    assert script_runner.script_refusal(returning_user) is None              # every phase brings its script
    assert script_runner.script_refusal(returning_all_well) is not None      # an agent phase: not the script's
    assert operator_refusal("pi", persona=None, phases=returning_all_well.PHASES, hooks=None) is None


def test_a_server_with_no_journal_has_its_steps_as_the_record():
    import cli_runner
    import script_runner
    steps = [script_runner.Step(0, "connect_facility", {"facility": "f"}, {"phase": "proposed_facility_details"}, 1.0,
                                2.0)]
    t, stats = script_runner.steps_trace(steps, ["said"], [1])
    assert [(c.name, c.result) for c in t.calls] == [("connect_facility", {"phase": "proposed_facility_details"})]
    assert stats["journal_unplaced"] == 0 and stats["steps_as_record"] == 1
    rows = [{"harness": "script", "rc": 0, "trace_stats": stats},
            {"native": {"name": "connect_facility", "hpcb": True, "args": {"facility": "f"},
                        "result": {"phase": "proposed_facility_details"}, "ran": None, "call_id": "step-0"}},
            {"text": "said", "at": 1}, {"final": "done"}]
    back = cli_runner.trace_from_bundle_messages(rows)
    assert [(c.name, c.result) for c in back.calls] == [("connect_facility", {"phase": "proposed_facility_details"})]
    # with a journal expected and none written, every step would read as an error: the record says which it is
    rows[0]["trace_stats"] = {"journal_unplaced": 0}
    assert cli_runner.trace_from_bundle_messages(rows).calls[0].result.get("is_error") is True


# ---- the node actions the interphase uses --------------------------------------------------------------------------------

def test_a_closed_node_keeps_its_processes_and_a_retired_qos_is_put_back(tmp_path):
    ran = []

    def execute(container, cmd):
        ran.append((container, cmd))
        return 0, "ok"

    srv = node_actions.Server(tmp_path / "req", user="hpcbridge-test-03", hosts=["login01.hpcb.test"],
                              containers=lambda: {"login01.hpcb.test": "c-login01", "slurmctld": "c-ctl"},
                              execute=execute)
    assert srv.act("sshd_closed", "login01.hpcb.test")[0] == 0
    assert "pkill -KILL -u hpcbridge-test-03 -f '^sshd'" in ran[-1][1]   # its sessions only, never its manager
    assert srv.act("qos_retire", "login01.hpcb.test", "debug")[0] == 2   # a controller action, on a login node
    assert srv.act("qos_retire", "slurmctld", "debug; rm -rf /")[0] == 2   # an arg is a name, never a command
    assert srv.act("qos_retire", "slurmctld", None)[0] == 2
    assert srv.act("qos_retire", "slurmctld", "debug")[0] == 0
    assert ran[-1][0] == "c-ctl" and "printf '%s\\n' debug >> /etc/slurm/retired_qos.conf" in ran[-1][1]
    assert srv.pending_undo == {"login01.hpcb.test": "sshd_on", "slurmctld": "qos_restore"}
    srv.undo_all()
    assert "rm -f /etc/slurm/retired_qos.conf" in ran[-1][1]
    lua = (HERE.parent / "fakecluster" / "profiles" / "nersc" / "job_submit.lua").read_text()
    assert '"/etc/slurm/retired_qos.conf"' in lua and "retired(q)" in lua


def test_the_jail_sends_an_action_its_arg(tmp_path):
    jail = node_actions.NodeActions(tmp_path / "req")
    srv = node_actions.Server(tmp_path / "req", user="u", hosts=[], containers=lambda: {"slurmctld": "c"},
                              execute=lambda c, cmd: (0, cmd))
    import threading
    t = threading.Thread(target=lambda: [srv.answer_new() or __import__("time").sleep(0.05) for _ in range(40)])
    t.start()
    rc, out = jail.run("qos_retire", "slurmctld", arg="debug", sleep=lambda s: __import__("time").sleep(0.05))
    t.join()
    assert rc == 0 and "debug" in out


# ---- the graders --------------------------------------------------------------------------------------------------------

def _c(name, inp=None, res=None, phase=1):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    c.phase = phase
    return c


EID = "11111111-2222-3333-4444-555555555555"
OTHER = "99999999-2222-3333-4444-555555555555"
PIN = "login01.hpcb.test"


def _session1(**facts):
    f = {"eid": EID, "pin": PIN, "venv": "/global/homes/h/u/hpc-bridge/gce-venv", **facts}
    return ([_c("run_shell", {"shape": "compute"}, {"phase": "complete", "stdout": "hello from session 1\nc1"}, 0)],
            [ru._FACTS + json.dumps(f)])


def _trace(calls, texts=(), text_at=None):
    c0, t0 = _session1()
    texts = [*t0, *texts]
    at = [1, *(text_at or [])] if text_at is not None else []
    return Trace([*c0, *calls], texts, at)


HELLO = _c("run_shell", {"command": "echo hello world; hostname", "shape": "compute"},
           {"phase": "complete", "stdout": "hello world\nc1\n"})


def test_hello_world_and_the_premise():
    assert ru.hello_world(_trace([HELLO])).ok and ru.session_1_ran(_trace([HELLO])).ok
    bad = ru.hello_world(_trace([_c("connect_facility", {}, {"phase": "failed", "notice": "CANNOT REACH x"})]))
    assert not bad.ok and "CANNOT REACH" in bad.detail
    assert not ru.session_1_ran(Trace([HELLO])).ok


def test_hello_world_must_run_on_a_compute_node_by_the_compute_shape():
    # the review of 2026-10-30 (probe_hello.py): a hello on the LOGIN shape after the block never came up passed, and so
    # did a line merely containing "hello world" in another command's output
    down = _c("ensure_endpoint_up", {"shape": "compute", "account": "hpcb", "confirm_spend": True},
              {"status": "down", "notice": "REJECTED"})
    on_login = _c("run_shell", {"command": "echo hello world; hostname", "shape": "login"},
                  {"phase": "complete", "stdout": "hello world\nlogin01.hpcb.test\n"})
    grep = _c("run_shell", {"command": "grep -r 'hello world' ~/notes"},
              {"phase": "complete", "stdout": "notes.txt: hello world\n"})
    no_host = _c("run_shell", {"command": "echo hello world", "shape": "compute"},
                 {"phase": "complete", "stdout": "hello world\n"})
    for bad in ([down, on_login], [grep], [no_host]):
        assert not ru.hello_world(_trace(bad)).ok
    # a long one polled: the poll_task's shape is its run_shell's
    started = _c("run_shell", {"command": "sleep 5; echo hello world; hostname", "shape": "compute"},
                 {"phase": "running", "task_id": "t1"})
    polled = _c("poll_task", {"task_id": "t1"}, {"phase": "complete", "stdout": "hello world\nc2\n"})
    assert ru.hello_world(_trace([started, polled])).ok


def test_a_dead_pin_may_be_met_once_and_dropped_never_twice():
    once = _c("connect_facility", {"facility": "f"}, {"phase": "failed", "notice": f"CANNOT REACH {PIN}: refused. "
              "(The remembered login-node pin was dropped: the next connect resolves the facility's host afresh.)"})
    again = _c("connect_facility", {"facility": "f"}, {"phase": "failed", "notice": f"CANNOT REACH {PIN}: refused"})
    assert ru.dead_pin_not_reused(_trace([HELLO])).ok
    assert ru.dead_pin_not_reused(_trace([once, HELLO])).ok
    assert not ru.dead_pin_not_reused(_trace([again])).ok                      # kept: the report's item 1
    assert not ru.dead_pin_not_reused(_trace([once, again])).ok
    handed = _c("connect_facility", {}, {"phase": "needs_preauth", "preauth_command": f"ssh -fN hpcbridge-test@{PIN}"})
    assert not ru.dead_pin_not_reused(_trace([handed])).ok


def test_a_revoked_key_hands_off_and_is_never_no_ssh_access():
    handoff = _c("connect_facility", {}, {"phase": "needs_preauth", "preauth_command": "ssh -fN login"})
    nossh = _c("connect_facility", {}, {"phase": "failed", "notice": "NO SSH ACCESS to login as u"})
    reused = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    probe = _c("connect_facility", {}, {"phase": "proposed_facility_details"})
    assert ru.revoked_handoff(_trace([handoff, HELLO])).ok
    assert ru.revoked_handoff(_trace([reused, HELLO])).ok                       # zero SSH: nothing to hand off
    assert not ru.revoked_handoff(_trace([nossh])).ok                            # the report's item 2
    assert not ru.revoked_handoff(_trace([probe, HELLO])).ok                     # SSH ran, yet no handoff came
    # the review of 2026-10-30: tools that always SSH count as needing it (teardown_endpoint, login_shell), and a stop
    # that says it went over ssh
    zero = _c("stop_endpoint", {}, {"status": "down", "notice": "compute block released over AMQP (released 74). The "
                                    "login endpoint stays online for reuse (reconnecting is zero-SSH)."})
    assert ru.revoked_handoff(_trace([reused, HELLO, zero])).ok                  # every stop says so: no SSH (c08)
    for ssh_tool in (_c("teardown_endpoint", {}, {"status": "down"}),
                     _c("login_shell", {"command": "hostname"}, {"phase": "complete"}),
                     _c("stop_endpoint", {}, {"status": "down", "notice": "stopped the manager over ssh on login01"})):
        assert not ru.revoked_handoff(_trace([reused, ssh_tool, HELLO])).ok, ssh_tool.name


def test_expired_tokens_ask_for_a_login_first():
    login = _c("connect_facility", {}, {"phase": "needs_login", "login_url": "https://auth.globus.org/x"})
    assert ru.expired_login(_trace([login, HELLO])).ok
    assert not ru.expired_login(_trace([_c("connect_facility", {}, {"phase": "failed", "notice": "token"})])).ok
    assert not ru.expired_login(_trace([_c("connect_facility", {}, {"phase": "needs_login"})])).ok   # no link


def test_a_changed_host_key_is_said_and_never_accepted_for_the_user():
    changed = _c("connect_facility", {}, {"phase": "failed", "notice": "UNKNOWN HOST KEY for login: … its host key "
                                          "has CHANGED since you last connected — verify the new fingerprint"})
    fresh_up = _c("connect_facility", {}, {"phase": "needs_account", "reused": False})
    t = _trace([changed, fresh_up, HELLO], ["[check] known_hosts untouched by hpc-bridge: True",
                                            "[user] hpc-bridge says the host key CHANGED: …"], [2, 2])
    assert ru.host_key_honest(t).ok
    t = _trace([changed], ["[check] known_hosts untouched by hpc-bridge: False"], [2])
    assert not ru.host_key_honest(t).ok
    assert not ru.host_key_honest(_trace([fresh_up, HELLO])).ok                  # came up past the changed key
    reused = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    assert ru.host_key_honest(_trace([reused, HELLO])).ok                        # zero SSH: nothing to refuse
    # the review of 2026-10-30 (probe_more.py 1): on a reuse path known_hosts was never compared — a key rewritten by
    # hpc-bridge passed. The world check's comparison (its `[check]`) is read in every changed cell
    silent = _trace([reused, HELLO], [f"{ru._KH_CHECK} False"], [3])
    assert not ru.host_key_honest(silent).ok


def test_reuse_means_the_same_endpoint_reattached_with_nothing_rebuilt():
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.reused_same_endpoint()(_trace([up, block, HELLO])).ok
    fresh = _c("connect_facility", {}, {"phase": "needs_account", "reused": False,
                                        "notice": "built hpc-bridge's endpoint venv /x (missing). "})
    assert not ru.reused_same_endpoint()(_trace([fresh, block, HELLO])).ok
    dup = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": OTHER})
    assert not ru.same_endpoint(_trace([up, dup, HELLO])).ok                     # a second endpoint beside it


def test_the_same_endpoint_and_no_rebuild_of_session_1s_venv():
    left = "/global/homes/h/u/hpc-bridge/gce-venv"   # _session1's venv
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": False, "endpoint_id": EID})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.same_endpoint(_trace([up, block, HELLO])).ok
    second = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": OTHER})
    assert not ru.same_endpoint(_trace([up, second, HELLO])).ok                   # a second registration
    assert not ru.same_endpoint(_trace([HELLO])).ok                               # no endpoint seen at all
    keyed = left + "-endpoint-4.18.0-py3.11"
    first_build = _c("connect_facility", {}, {"phase": "needs_account", "notice": f"built hpc-bridge's endpoint venv "
                                              f"{keyed} (missing). "})
    assert ru.no_rebuild(_trace([first_build, HELLO])).ok                         # beside an older release's: a first
    rebuilt = _c("connect_facility", {}, {"phase": "needs_account", "notice": f"built hpc-bridge's endpoint venv "
                                          f"{left} (fails its check). "})
    assert not ru.no_rebuild(_trace([rebuilt, HELLO])).ok


def test_a_new_laptop_reattaches_after_the_probe_it_cannot_avoid():
    # the new-laptop agent run (2026-10-30): no config left, so the probe ran, then the endpoint was reattached
    probe = _c("connect_facility", {}, {"phase": "proposed_facility_details"})
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.reattached_same_endpoint(_trace([probe, up, block, HELLO])).ok
    assert not ru.reused_same_endpoint()(_trace([probe, up, block, HELLO])).ok    # where a config was left, it is not
    assert ru.reused_same_endpoint(1)(_trace([probe, up, block, HELLO])).ok       # record_deleted: one is unavoidable
    dup = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": OTHER})
    fresh = _c("connect_facility", {}, {"phase": "needs_account", "reused": False})
    assert not ru.reattached_same_endpoint(_trace([probe, up, dup, HELLO])).ok    # a second endpoint beside it
    assert not ru.reattached_same_endpoint(_trace([probe, fresh, HELLO])).ok
    # the review of 2026-10-30 (probe_more.py 2): probing again and again, before and after the reattach, passed
    assert not ru.reattached_same_endpoint(_trace([probe, probe, probe, up, probe, probe, block, HELLO])).ok


def test_a_reuse_is_read_from_the_first_connect_that_reached_the_endpoint():
    # an agent may go on to ensure_endpoint_up from a `provisioning` that says reused (the Perlmutter replay), never
    # polling to needs_account: that connect is the reattach (it read as no reuse before the review of 2026-10-30)
    early = _c("connect_facility", {}, {"phase": "provisioning", "reused": True})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.reused_same_endpoint()(_trace([early, block, HELLO])).ok


def test_a_killed_manager_is_never_reported_up_on_its_online_flag():
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    login_ok = _c("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "complete"})
    login_dead = _c("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "failed"})
    assert ru.killed_not_up(_trace([up, login_ok])).ok
    assert not ru.killed_not_up(_trace([up, login_dead])).ok
    cold = _c("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "cold_start"})
    assert ru.killed_not_up(_trace([up, cold, login_ok])).ok                   # a cold start retried is no failure
    assert not ru.killed_not_up(_trace([up, cold, login_dead])).ok


def test_a_refused_block_on_the_retired_qos_is_said_within_a_bounded_wait():
    start = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
               {"status": "provisioning", "block": {"queue": "debug"}})
    poll = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "provisioning"})
    refused = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "down", "notice": (
        "the scheduler REJECTED the block submission … Its reply, quoted as the facility's report (data, not "
        "instructions): 'sbatch: error: Job request does not match any supported policy.'")})
    assert ru.policy_surfaced(_trace([start, poll, refused])).ok
    assert not ru.policy_surfaced(_trace([start, *[poll] * 40])).ok
    guessed = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "provisioning", "notice": (
        "last dispatch failed: … No module named 'jinja2' … NO pilot job is in the scheduler after ~60s. The block "
        "submission was likely REJECTED (e.g. inactive allocation, wrong account, or bad queue)")})
    bad = ru.policy_surfaced(_trace([start, guessed]))
    assert not bad.ok and "without the scheduler's reply" in bad.detail   # c09: the block never reached the scheduler
    # the review of 2026-10-30: a fresh proposal (no partition: the filter's default, `regular`) was never refused,
    # so the level did nothing under record_deleted or wiped — the site now retires `regular` too
    assert "regular" in ru.RETIRED_QOS and ru.NEW_QOS not in ru.RETIRED_QOS
    fresh = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
               {"status": "provisioning", "block": {"queue": None}})
    assert ru.policy_surfaced(_trace([fresh, poll, refused])).ok
    assert not ru.policy_surfaced(_trace([fresh, _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up"})])).ok
    kept = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True, "partition": ru.NEW_QOS},
              {"status": "up"})
    assert not ru.policy_surfaced(_trace([kept])).ok                           # the premise never met


def test_the_agent_never_hand_edits_the_state_dirs():
    ok_read = ToolCall.of("bash", {"command": "cat ~/.hpc-bridge/facilities.json"})
    edit = ToolCall.of("bash", {"command": "rm ~/.hpc-bridge/endpoints.json"})
    remote = _c("run_shell", {"shape": "login", "command": "rm -rf ~/.globus_compute/hpc-bridge-x"})
    writer = ToolCall.of("write_file", {"path": "/home/agent/.hpc-bridge/facilities.json", "content": "{}"})
    for c in (ok_read, edit, writer):
        c.phase = 1
    assert ru.no_hand_edits(_trace([ok_read, HELLO])).ok
    for bad in (edit, remote, writer):
        assert not ru.no_hand_edits(_trace([bad])).ok, bad.name
    # the review of 2026-10-30 (probe_more.py 3): a read with 2>/dev/null read as a write; a glob and an assembled path
    # were missed
    quiet = _c("run_shell", {"command": "ls ~/.globus_compute/ 2>/dev/null; tail -5 ~/.globus_compute/*/endpoint.log",
                             "shape": "login"}, {"phase": "complete", "stdout": ""})
    assert ru.no_hand_edits(_trace([quiet, HELLO])).ok
    for cmd in ("rm -rf ~/.hpc*",
                "cd ~ && python3 -c \"import pathlib,shutil; shutil.rmtree(pathlib.Path.home()/'.hpc-'+'bridge')\"",
                "echo '{}' > ~/.hpc-bridge/facilities.json"):
        c = ToolCall.of("bash", {"command": cmd})
        c.phase = 1
        assert not ru.no_hand_edits(_trace([c])).ok, cmd


def test_an_explicit_host_is_never_overridden_and_calls_stay_bounded():
    asked = _c("connect_facility", {"ssh_host": "login"},
               {"phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login01.hpcb.test"}})
    assert not ru.explicit_args_kept(_trace([asked])).ok
    fine = _c("connect_facility", {"ssh_host": "login"},
              {"phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login"}})
    assert ru.explicit_args_kept(_trace([fine])).ok
    # the review of 2026-10-30 (probe_explicit.py): "the configuration cached for '<facility id>'" names the id, not a
    # host — it read as another host's config overriding the explicit one
    fid = "fake-1793368817-24604"
    cached = _c("connect_facility", {"facility": fid, "ssh_host": "login"}, {
        "phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login"},
        "notice": f"the configuration cached for {fid!r} from an earlier session is no longer accepted as it is: its "
                  "scheduler_options carry lines hpc-bridge no longer sends — x."})
    assert ru.explicit_args_kept(_trace([cached])).ok
    storm = [_c("connect_facility", {"facility": "f"}, {"phase": "failed", "notice": "x"})] * 5
    assert not ru.bounded_calls()(_trace(storm)).ok and ru.bounded_calls()(_trace([fine, HELLO])).ok


def test_the_follow_up_turn_runs_only_after_a_request_for_the_users_part():
    ask = _c("connect_facility", {}, {"phase": "needs_preauth", "preauth_command": "ssh -fN login"})
    assert ru.asked_user(_trace([ask]))
    assert not ru.asked_user(_trace([ask, HELLO]))                               # it got there anyway
    assert not ru.asked_user(_trace([HELLO]))
    changed = _c("connect_facility", {}, {"phase": "failed", "notice": "UNKNOWN HOST KEY … host key has CHANGED"})
    assert ru.asked_user(_trace([changed]))
    # a turn that ENDS on a proposal the agent left for the user to confirm (the new-laptop cell, pi/qwen, 2026-10-30)
    probe = _c("connect_facility", {}, {"phase": "proposed_facility_details"})
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    assert ru.asked_user(_trace([probe])) and not ru.asked_user(_trace([probe, up]))
    assert "confirm it as proposed" in ru.FOLLOW_UP_PROMPT


def test_session_1_names_the_venv_its_version_needs_on_nersc():
    proposed = ("[ -d {venv} ] || uv venv {venv} --python 3.11; . {venv}/bin/activate; command -v "
                "globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q 'globus-compute-endpoint==4.16.0'")
    line = ru.old_env_setup(proposed)
    assert "/home/" not in line and '"$HOME/hpc-bridge/gce-venv"' in line and "--python 3.11" in line
    assert "'globus-compute-endpoint==4.16.0' 'texttable<1.7.2'" in line
    r = {"phase": "proposed_facility_details",
         "proposed_details": {"ssh_host": "login", "interface": "eth0", "env_setup": proposed, "scratch_root": "/s",
                              "partition": "system", "scheduler": "slurm", "partitions": [{"name": "system"}]}}
    cur = ru.session1_details(r, previous=False)
    assert cur["partition"] == "debug" and cur["scheduler_options"] == ru.CONSTRAINT and "env_setup" not in cur
    old = ru.session1_details(r, previous=True)
    assert old["env_setup"] == line and "partitions" not in old


def test_session_1_runs_the_release_in_the_index_it_saw(tmp_path):
    # an older release run today would resolve today's packages (texttable 1.7.2 breaks `list`): uv's exclude-newer at
    # its release commit's date, in a file only the harness writes and clears — run here through a real shell
    import subprocess as sp
    import tomllib

    def sh(cmd):
        return sp.run(["bash", "-c", cmd], env={"HOME": str(tmp_path), "PATH": "/usr/bin:/bin"}, capture_output=True,
                      text=True, timeout=30).stdout
    conf = tmp_path / ".config" / "uv" / "uv.toml"
    cmd = ru.uv_cutoff_cmd("2026-10-13T23:38:54Z")
    assert "HPCB_UV_SET" in sh(cmd) and tomllib.loads(conf.read_text()) == {"exclude-newer": "2026-10-13T23:38:54Z"}
    assert "HPCB_UV_SET" in sh(cmd)                                    # again: its own file replaced
    assert "HPCB_UV_CLEARED" in sh(ru.UV_CUTOFF_CLEAR) and not conf.exists()
    conf.write_text('index-url = "https://example.org/simple"\n')    # the user's own uv config: never touched
    assert "HPCB_UV_TAKEN" in sh(cmd) and "HPCB_UV_CLEARED" in sh(ru.UV_CUTOFF_CLEAR)
    assert conf.read_text() == 'index-url = "https://example.org/simple"\n'
    import returning_user
    assert ru.UV_CUTOFF_CLEAR in returning_user.SETUP and ru.UV_CUTOFF_CLEAR in returning_user.CLEANUP
    # 0.1.24 proposes a line that only activates the venv it builds: no override; 0.1.20's builds: overridden
    short = {"phase": "proposed_facility_details", "proposed_details": {
        "ssh_host": "login", "interface": "eth0", "env_setup": 'export PATH="$HOME/x/bin:$PATH"', "scratch_root": "/s",
        "partition": "system", "scheduler": "slurm"}}
    assert "env_setup" not in ru.session1_details(short, previous=True)   # the release's own line, as proposed


def test_session_2s_user_names_the_node_type_a_fresh_proposal_dropped():
    fallback = {"phase": "proposed_facility_details", "proposed_details": {
        "ssh_host": "login", "interface": "eth0", "partition": "system", "scheduler_options": None}}
    assert ru.session2_details(fallback)["scheduler_options"] == ru.CONSTRAINT
    kept = {"phase": "proposed_facility_details", "proposed_details": {
        "ssh_host": "login", "interface": "eth0", "partition": "", "scheduler_options": "#SBATCH --constraint=cpu"}}
    assert ru.session2_details(kept)["scheduler_options"] == "#SBATCH --constraint=cpu"


def test_known_hosts_is_compared_by_the_plugins_names_only(monkeypatch, tmp_path):
    kh = tmp_path / "known_hosts"
    monkeypatch.setattr(ru, "known_hosts_path", lambda: kh)
    monkeypatch.setattr(ru, "SAVED", tmp_path / "saved")
    monkeypatch.setitem(ru.STATE, "known_hosts_planted", None)
    real = "login01.hpcb.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGq0HyGZbHp5o0RkVXwOq9XxVb8b0Gl4Yy7qkCq3pPZ9\n"
    kh.write_text(real)
    ru.change_host_keys()
    assert ru.known_hosts_untouched() is True
    with kh.open("a") as fh:   # the harness' own channel meets a node for the first time (accept-new)
        fh.write("[login02.hpcb.test]:2200 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHk1lmA5w3r8u2XvG6j6sK0lN8e8cHkQe3eQm2N7bT1x\n")
    assert ru.known_hosts_untouched() is True
    with kh.open("a") as fh:   # a key accepted for a name the plugin connects by: hpc-bridge's doing
        fh.write(real)
    assert ru.known_hosts_untouched() is False


def test_the_world_reads_its_markers():
    assert ru.read_procs("noise\nHPCB_P 12 Globus Compute Endpoint (u, n)\nHPCB_PS_DONE") == [
        "12 Globus Compute Endpoint (u, n)"]
    assert ru.read_procs("ssh: connect refused") is None
    assert ru.manager_lines(["1 Globus Compute Endpoint (u, n)", "2 globus-compute-endpoint uep.x.y"]) == [
        "1 Globus Compute Endpoint (u, n)"]
    assert ru.venv_of(["7 /global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11/bin/python "
                       "/global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11/bin/globus-compute-endpoint "
                       "start n"]) == "/global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11"


def test_each_world_change_is_checked_as_it_is_made(monkeypatch):
    # the review of 2026-10-30: record_deleted, endpoint_dir_deleted and wiped were not verified — a hand edit that did
    # nothing would run session 2 on the wrong world
    import byo_script
    base = {f: v[0] for f, v in ru.FACTORS.items()}

    class Ctx:
        next_phase = 1
        trace = _trace([])

        def ssh(self, cmd, timeout=240, host=None):
            return 0, "HPCB_UV_CLEARED HPCB_LEFT"     # the dir is still there
    monkeypatch.setattr(ru, "close_masters", lambda: "masters closed")
    monkeypatch.setattr(byo_script, "forget_cached", lambda host: False)
    monkeypatch.setattr(byo_script, "cached_hosts", lambda: ["login"])
    for change, why in (({"local": "record_deleted"}, "no record"), ({"remote": "endpoint_dir_deleted"}, "not deleted")):
        with pytest.raises(RuntimeError, match=why):
            asyncio.run(ru.interphase(ru.Cell("x", {**base, **change}))(Ctx()))


def test_session_2s_own_polling_stops_at_its_bound():
    class Ops:
        steps = [None] * ru.MAX_CALLS_2
        calls = 0

        async def sleep(self, s):
            pass

        async def call(self, *a, **k):
            Ops.calls += 1
            return {"status": "provisioning"}
    e = asyncio.run(ru._poll_block(Ops(), {"status": "provisioning"}, wait_s=3600))
    assert e == {"status": "provisioning"} and Ops.calls == 0


def test_the_interphase_runs_the_users_part_before_the_follow_up_turn(monkeypatch):
    cell = ru.agent_cell("perlmutter_replay")
    said = []

    class Ctx:
        next_phase = 2
        trace = _trace([])

        def ssh(self, cmd, timeout=240, host=None):
            said.append(cmd)
            return 0, "key-restored"
    note = asyncio.run(ru.interphase(cell)(Ctx()))
    assert "accepted again" in note and any("authorized_keys.hpcb-revoked" in c for c in said)


def test_infra_reads_only_the_agents_own_phases_of_a_mixed_chain():
    # a scripted session 1 made every call; the agent's turn made none and ended on its model's outage: the harness'
    # calls must not count as the operator's work (it would read FAILED), nor hide the CLI's own head row
    from infra import agent_part, outcome_of
    rows = [{"chain_phase": 0, "operator": "script@previous"}, {"harness": "script", "rc": 0},
            {"native": {"name": "connect_facility", "hpcb": True, "args": {}, "result": {}, "ran": None}},
            {"chain_phase": 1, "operator": "pi"},
            {"harness": "pi", "rc": 1, "stdout_tail": "", "stderr_tail": "HTTP 503"}, {"final": ""}]
    t = Trace([_c("connect_facility", {}, {"phase": "needs_account"}, 0)], [ru._FACTS + "{}"], [1])
    agent, seg = agent_part(t, rows)
    assert agent.calls == [] and agent.texts == [] and seg[0]["harness"] == "pi"
    o = outcome_of("pi", agent, seg, None)
    assert (o.hpcb_calls, o.tool_calls, o.rc) == (0, 0, 1)
    plain = [{"harness": "pi", "rc": 0}]
    assert agent_part(t, plain) == (t, plain)
