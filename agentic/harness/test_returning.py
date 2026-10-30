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
    # far fewer than the full product (6144), never fewer than the two largest factors' pairs
    assert 16 <= len(cells) <= 24
    # the constraints are real: neither forbidden pair is in any cell, both are otherwise reachable
    for pair, _why in ru.CONSTRAINTS:
        assert not any(all(c.levels[f] == v for f, v in pair.items()) for c in cells)


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
    monkeypatch.setenv(ru.CELL_ENV, "c03")
    assert ru.cell_from_env().id == "c03" and ru.cell_from_env().origin == ru.load_cells()["c03"].levels["origin"]
    monkeypatch.setenv(ru.CELL_ENV, "c99")
    with pytest.raises(ValueError):
        ru.cell_from_env()
    monkeypatch.delenv(ru.CELL_ENV)
    assert ru.cell_from_env().id == "c01"


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


def test_each_factor_level_gates_its_own_expectation():
    cells = ru.load_cells()
    for c in cells.values():
        gated = set(ru.expect_ok(c))
        assert {"ru_session_1", "ru_hello_world", "ru_bounded", "ru_explicit_args"} <= gated
        assert ("ru_dead_pin" in gated) == (c.pin == "dead")
        assert ("ru_revoked_handoff" in gated) == (c.auth == "revoked")
        assert ("ru_expired_login" in gated) == (c.tokens == "expired")
        assert ("ru_host_key_honest" in gated) == (c.host_key == "changed")
        assert ("ru_killed_not_up" in gated) == (c.ended == "killed")
        assert ("ru_policy_surfaced" in gated) == (c.site == "policy_changed")
        assert ("ru_reused" in gated) == ru.reuse_expected(c)
    assert ru.reuse_expected(cells["c01"])   # nothing changed: the endpoint must be reused


# ---- the previous release -----------------------------------------------------------------------------------------------

def test_the_previous_release_is_the_newest_tag_older_than_this_version():
    tags = ["v0.1.1-beta.1", "v0.1.13-beta.1", "v0.1.20-beta.1", "not-a-tag", "v0.1.25"]
    assert prev_release.previous_release(tags, "0.1.25") == "v0.1.20-beta.1"
    assert prev_release.previous_release(tags, "0.1.26") == "v0.1.25"            # a release moves it forward
    assert prev_release.previous_release(tags, "0.1.20") == "v0.1.13-beta.1"     # never this version's own tag
    assert prev_release.previous_release(["v0.1.20-beta.1", "v0.1.20", "v0.1.20-beta.2"], "0.1.21") == "v0.1.20"
    assert prev_release.previous_release(["v0.1.20-beta.1", "v0.1.20-beta.2"], "0.1.21") == "v0.1.20-beta.2"
    assert prev_release.previous_release(tags, "0.1.1") is None
    assert prev_release.previous_release(tags, "garbage") is None


def test_the_image_carries_the_previous_release_or_says_it_does_not(tmp_path):
    assert prev_release.server_argv(tmp_path) is None and prev_release.installed_tag(tmp_path) is None
    (tmp_path / "venv" / "bin").mkdir(parents=True)
    (tmp_path / "venv" / "bin" / "hpc-bridge").write_text("")
    assert prev_release.server_argv(tmp_path) is None              # no TAG: not an install the build finished
    (tmp_path / "TAG").write_text("v0.1.20-beta.1\n")
    assert prev_release.server_argv(tmp_path) == [str(tmp_path / "venv" / "bin" / "hpc-bridge")]
    dockerfile = (HERE.parent / "Dockerfile").read_text()
    assert "COPY agentic/.prev-release /opt/hpcb-prev" in dockerfile and "--frozen --no-editable" in dockerfile
    assert "prev_release.py\" export" in (HERE.parent / "run_smoke.sh").read_text()
    assert "_export_previous_release()" in (HERE.parent / "run_suite.py").read_text()


# ---- the chain ----------------------------------------------------------------------------------------------------------

def test_a_phase_names_its_operator_and_server(monkeypatch):
    run = _run_module(monkeypatch)
    assert run._phase_spec("hi {x}", lambda s: s.replace("{x}", "there")) == {"prompt": "hi there"}

    async def s1(ops):
        return ""
    spec = run._phase_spec({"script": s1, "server": "previous"})
    assert spec["operator"] == "script" and run._phase_label(spec, "pi") == "script@previous"
    assert run._phase_label(run._phase_spec("go"), "pi") == "pi"
    with pytest.raises(ValueError):
        run._phase_spec({"script": s1, "operator": "pi"})
    with pytest.raises(ValueError):
        run._phase_spec({"script": s1, "server": "nightly"})


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


def test_reuse_means_the_same_endpoint_reattached_with_nothing_rebuilt():
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.reused_same_endpoint(_trace([up, block, HELLO])).ok
    fresh = _c("connect_facility", {}, {"phase": "needs_account", "reused": False,
                                        "notice": "built hpc-bridge's endpoint venv /x (missing). "})
    assert not ru.reused_same_endpoint(_trace([fresh, block, HELLO])).ok
    dup = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": OTHER})
    assert not ru.reused_same_endpoint(_trace([up, dup, HELLO])).ok              # a second endpoint beside it


def test_a_new_laptop_reattaches_after_the_probe_it_cannot_avoid():
    # the new-laptop agent run (2026-10-30): no config left, so the probe ran, then the endpoint was reattached
    probe = _c("connect_facility", {}, {"phase": "proposed_facility_details"})
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    block = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": EID})
    assert ru.reattached_same_endpoint(_trace([probe, up, block, HELLO])).ok
    assert not ru.reused_same_endpoint(_trace([probe, up, block, HELLO])).ok      # where a config was left, it is not
    dup = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "endpoint_id": OTHER})
    fresh = _c("connect_facility", {}, {"phase": "needs_account", "reused": False})
    assert not ru.reattached_same_endpoint(_trace([probe, up, dup, HELLO])).ok    # a second endpoint beside it
    assert not ru.reattached_same_endpoint(_trace([probe, fresh, HELLO])).ok


def test_a_killed_manager_is_never_reported_up_on_its_online_flag():
    up = _c("connect_facility", {}, {"phase": "needs_account", "reused": True})
    login_ok = _c("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "complete"})
    login_dead = _c("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "failed"})
    assert ru.killed_not_up(_trace([up, login_ok])).ok
    assert not ru.killed_not_up(_trace([up, login_dead])).ok


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
    regular = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
                 {"status": "up", "block": {"queue": None}})
    assert ru.policy_surfaced(_trace([regular])).ok                             # a fresh proposal: nothing refused


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


def test_an_explicit_host_is_never_overridden_and_calls_stay_bounded():
    asked = _c("connect_facility", {"ssh_host": "login"},
               {"phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login01.hpcb.test"}})
    assert not ru.explicit_args_kept(_trace([asked])).ok
    fine = _c("connect_facility", {"ssh_host": "login"},
              {"phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login"}})
    assert ru.explicit_args_kept(_trace([fine])).ok
    storm = [_c("connect_facility", {"facility": "f"}, {"phase": "failed", "notice": "x"})] * 5
    assert not ru.bounded_calls()(_trace(storm)).ok and ru.bounded_calls()(_trace([fine, HELLO])).ok


def test_the_follow_up_turn_runs_only_after_a_request_for_the_users_part():
    ask = _c("connect_facility", {}, {"phase": "needs_preauth", "preauth_command": "ssh -fN login"})
    assert ru.asked_user(_trace([ask]))
    assert not ru.asked_user(_trace([ask, HELLO]))                               # it got there anyway
    assert not ru.asked_user(_trace([HELLO]))
    changed = _c("connect_facility", {}, {"phase": "failed", "notice": "UNKNOWN HOST KEY … host key has CHANGED"})
    assert ru.asked_user(_trace([changed]))


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


def test_the_world_reads_its_markers():
    assert ru.read_procs("noise\nHPCB_P 12 Globus Compute Endpoint (u, n)\nHPCB_PS_DONE") == [
        "12 Globus Compute Endpoint (u, n)"]
    assert ru.read_procs("ssh: connect refused") is None
    assert ru.manager_lines(["1 Globus Compute Endpoint (u, n)", "2 globus-compute-endpoint uep.x.y"]) == [
        "1 Globus Compute Endpoint (u, n)"]
    assert ru.venv_of(["7 /global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11/bin/python "
                       "/global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11/bin/globus-compute-endpoint "
                       "start n"]) == "/global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11"


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
