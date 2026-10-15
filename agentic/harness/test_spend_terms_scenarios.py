"""Hermetic: the spend-terms scenarios (term_switch_reasks, preset_account_scoped, ssh_account_floor, and the persona
pair terms_prestated / terms_wanted) — their graders on synthetic traces (a run the plugin gets right passes, one an
unfixed plugin would produce fails, a broken premise is labelled as one), their REQUIRES against the profile manifests,
the local catalog ssh_account_floor writes, and the universal `quote_before_spend` grader."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def _connect(notice="attached", phase="needs_account"):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "x"},
                       {"phase": phase, "reused": True, "allocations": [], "notice": notice})


def _ensure(status, notice="", reports=None, **inp):
    """An ensure_endpoint_up: `inp` is what the agent passed, `reports` the account the result names as the block's
    (default: the agent's own, when it passed one)."""
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "compute", **inp},
                       {"status": status, "notice": notice, "account": reports or inp.get("account")})


def _run(stdout, phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "echo …"},
                       {"phase": phase, "stdout": stdout})


def _stop(status="draining", notice="NO CANCEL WAS ISSUED: …"):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "notice": notice})


# --- term_switch_reasks ---------------------------------------------------------------------------------------------

_REASK = ("Confirm spend again with the user: the partition or account has changed since spend was confirmed, and that "
          "is a different block (the earlier confirmation covered only the one on partition 'debug', account 'hpcb'). "
          "That block, scheduler job 41, was not cancelled — no cancel channel on a facility endpoint: it bills until "
          "that facility's idle-release (`scancel 41` on that facility ends it); stop_endpoint names it. …")
_REAPED = ("Confirm spend again with the user: the previous block has been idle-released, or is about to be (no task for "
           "~200 s, past the 180 s idle window). The earlier confirmation covered that block only; a new block bills "
           "again. That block, scheduler job 41, was not cancelled — … (`scancel 41` on that facility ends it) …")


def _switch_trace(*, switch_status="needs_confirmation", notice=_REASK, ran_between=False, new_part="compute",
                  new_job="42"):
    calls = [_connect(),
             _ensure("up", partition="debug", account="hpcb", confirm_spend=True),
             _run("JOB=41 PART=debug\nc1\n"),
             _ensure(switch_status, notice, partition="compute")]
    if ran_between:
        calls.append(_run("JOB=42 PART=compute\nc2\n"))
    calls += [_ensure("up", partition="compute", confirm_spend=True), _run(f"JOB={new_job} PART={new_part}\nc2\n"),
              _stop()]
    return Trace(calls, ["The switch asked for spend again; job 41 was not cancelled (`scancel 41`)."])


def test_term_switch_reasks_fails_on_the_unfixed_plugin():
    import term_switch_reasks as sc
    good = _switch_trace()
    for g in (sc.switch_reasked, sc.switch_names_old_block, sc.nothing_ran_before_the_confirm,
              sc.ran_on_the_new_partition, sc.mep_stop_is_draining_only):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: the unconfirmed switch started the compute block under the debug block's confirmation
    main = _switch_trace(switch_status="up", notice="worker live on c2", ran_between=True)
    assert not sc.switch_reasked(main).ok and not sc.switch_names_old_block(main).ok
    assert not sc.nothing_ran_before_the_confirm(main).ok
    # a re-ask that does not name the block it leaves, or a "new" block that is the old one, fails its grader
    assert not sc.switch_names_old_block(_switch_trace(notice="Confirm spend again … different block.")).ok
    assert not sc.ran_on_the_new_partition(_switch_trace(new_job="41")).ok
    assert not sc.ran_on_the_new_partition(_switch_trace(new_part="debug")).ok


def test_term_switch_reasks_calls_a_block_gone_before_the_switch_inconclusive():
    import term_switch_reasks as sc
    late = _switch_trace(notice=_REAPED)
    for g in (sc.switch_reasked, sc.switch_names_old_block):
        r = g(late)
        assert not r.ok and r.detail.startswith("inconclusive: the old block idle-released before the switch"), r


def test_term_switch_reasks_reads_its_marker_line_however_spaced():
    import term_switch_reasks as sc
    t = Trace([_run("JOB=41    PART=debug\nc1\n")])
    assert sc._runs(t) == [(0, "41", "debug")] and sc.NEEDS_COMPUTE_NODE == 2


def test_term_switch_reasks_fails_when_its_premise_never_happened():
    import term_switch_reasks as sc
    confirmed_only = Trace([_connect(), _ensure("up", partition="debug", account="hpcb", confirm_spend=True),
                            _run("JOB=41 PART=debug\n"), _ensure("up", partition="compute", confirm_spend=True),
                            _run("JOB=42 PART=compute\n"), _stop()])
    r = sc.switch_reasked(confirmed_only)
    assert not r.ok and "premise" in r.detail
    assert not sc.switch_reasked(Trace([_connect(), _stop()])).ok


# --- preset_account_scoped ------------------------------------------------------------------------------------------

_CLAUSE = "(HPC_BRIDGE_ACCOUNT applies only to the facility HPC_BRIDGE_MACHINE names, and that is unset, so it was not used here.) attached …"


def _preset_trace(*, clause=_CLAUSE, bare_status="needs_account", bare_account=None, printed="hpcb"):
    calls = [_connect(clause), _ensure(bare_status, "this facility requires an allocation account …",
                                       reports=bare_account, partition="compute", confirm_spend=True)]
    if bare_status == "needs_account":
        calls.append(_ensure("up", partition="compute", account="hpcb", confirm_spend=True))
    calls += [_run(f"JOB=7 ACCOUNT={printed}\nc2\n"), _stop()]
    return Trace(calls, [f"The block was charged to {printed}."])


def test_preset_account_scoped_fails_on_the_unfixed_plugin():
    import preset_account_scoped as sc
    good = _preset_trace()
    for g in (sc.preset_clause_shown, sc.floor_asked_without_the_preset, sc.preset_not_charged,
              sc.charged_the_named_account):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: no clause, the preset satisfied the floor, and the block was charged to it
    main = _preset_trace(clause="attached …", bare_status="up", bare_account="hpcb-gpu", printed="hpcb-gpu")
    for g in (sc.preset_clause_shown, sc.floor_asked_without_the_preset, sc.preset_not_charged,
              sc.charged_the_named_account):
        assert not g(main).ok, g.__name__
    # the clause must never print the preset's value
    leaky = _preset_trace(clause="(HPC_BRIDGE_ACCOUNT=hpcb-gpu applies only to … ) attached")
    assert not sc.preset_clause_shown(leaky).ok


def test_preset_account_scoped_tells_the_agents_own_choice_from_a_leak():
    # a CLI operator's agent can read EXTRA_ENV and pass the preset itself: reported apart, not the product's leak
    import preset_account_scoped as sc
    calls = [_connect(_CLAUSE),
             _ensure("needs_account", "this facility requires an allocation account …", partition="compute",
                     confirm_spend=True),
             _ensure("up", partition="compute", account="hpcb-gpu", confirm_spend=True),
             _run("JOB=7 ACCOUNT=hpcb-gpu\nc2\n"), _stop()]
    t = Trace(calls, [])
    assert sc.preset_not_charged(t).ok and "passed it itself at call 2" in sc.preset_not_charged(t).detail
    assert not sc.agent_chose_the_preset(t).ok and "agent_chose_the_preset" not in sc.EXPECT_OK


def test_preset_account_scoped_sets_the_preset_for_the_server_only_and_leaves_the_machine_unset():
    import preset_account_scoped as sc
    assert sc.EXTRA_ENV == {"HPC_BRIDGE_ACCOUNT": "hpcb-gpu"}
    (grant,) = sc.ADMIN_SETUP
    assert grant.endswith(sc._HAS_GRANT) and "|| true" not in grant      # a grant that did not take fails the setup
    assert any("delete user" in c and "hpcb-gpu" in c and "sleep" in c for c in sc.ADMIN_CLEANUP)   # retried
    world = {p["name"]: p for p in sc.POSTCHECKS}
    assert world["no_job_charged_to_the_preset"]["expect_absent"] == "hpcb-gpu"
    assert sc._START in sc.SETUP[0] and sc._START in world["no_job_charged_to_the_preset"]["cmd"]
    assert "Submit" in world["no_job_charged_to_the_preset"]["cmd"]      # jobs submitted since the cell started


# --- ssh_account_floor ----------------------------------------------------------------------------------------------

_FLOOR = ("this facility requires an allocation account for every block and none is set — nothing was started. A LOGIN "
          "NAME is not an account: … the facility's allocation tool — it runs free on the login shape, run_shell(…, "
          "shape='login') — …")


def test_ssh_account_floor_fails_on_the_unfixed_plugin():
    import ssh_account_floor as sc
    connect = _connect("login node is up; this facility has no allocation listing …")
    good = Trace([connect, _ensure("needs_account", _FLOOR, partition="compute", confirm_spend=True),
                  _stop("down", "compute block released over AMQP (released none) …")], ["It needs an account."])
    for g in (sc.ssh_floor_answered, sc.no_start_without_an_account, sc.no_account_guessed):
        assert g(good).ok, (g.__name__, g(good).detail)
    # main: the start went through with no account and work ran on the block
    main = Trace([connect, _ensure("provisioning", "allocating nodes…", partition="compute", confirm_spend=True),
                  _ensure("up", "worker live", partition="compute"), _run("JOB=9 ACCOUNT=hpcb\n"),
                  _stop("down")], [])
    assert not sc.ssh_floor_answered(main).ok and not sc.no_start_without_an_account(main).ok
    guessed = Trace([connect, _ensure("needs_account", _FLOOR, confirm_spend=True),
                     _ensure("up", account="hpcb", confirm_spend=True), _stop("down")])
    assert not sc.no_account_guessed(guessed).ok
    assert not sc.ssh_floor_answered(Trace([connect, _stop("down")])).ok   # premise


def test_ssh_account_floor_calls_a_details_connect_a_broken_premise():
    # details= defines a bring-your-own facility in place of the entry: no curated account_required, so a start that
    # goes through is not the product's failure
    import ssh_account_floor as sc
    byo = ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake-ssh-site", "details": {"ssh_host": "login"}},
                      {"phase": "needs_account", "notice": "login node is up …"})
    t = Trace([byo, _ensure("up", "worker live", partition="compute", confirm_spend=True), _stop("down")])
    for g in (sc.ssh_floor_answered, sc.no_start_without_an_account):
        r = g(t)
        assert not r.ok and r.detail.startswith("premise: agent bypassed the catalog entry (details="), r


def test_ssh_account_floor_points_the_server_at_the_catalog_its_cell_writes():
    import ssh_account_floor as sc
    assert sc.EXTRA_ENV["HPC_BRIDGE_CATALOG_FILE"] == sc._CATALOG
    (cmd,) = sc.LOCAL_SETUP
    assert "agentic/harness/fake_ssh_catalog.py" in cmd and sc._CATALOG in cmd and f"--id {sc.FACILITY_ID}" in cmd
    assert sc.NEEDS_COMPUTE_NODE is False and sc.POSTCHECKS[0]["expect_absent"] == "parsl"
    assert "Submit" in sc.POSTCHECKS[0]["cmd"]                            # jobs submitted since the cell started


def test_the_written_catalog_is_an_account_required_ssh_entry_the_plugin_floors(tmp_path, monkeypatch):
    import fake_ssh_catalog

    from hpc_bridge import binding
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.facility.endpoint_venv import is_line

    out = tmp_path / "catalog.yaml"
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_NAME", raising=False)
    assert fake_ssh_catalog.main([str(out)]) == 2 and not out.exists()   # no run endpoint name: refuse, write nothing
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-run123")
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "login")
    assert fake_ssh_catalog.main([str(out), "--id", "fake-ssh-site"]) == 0
    import asyncio
    entry = asyncio.run(BundledCatalog(str(out)).get("fake-ssh-site"))
    assert entry is not None and entry.compute_mep_uuid is None and entry.ssh_host == "login"
    assert entry.account_required is True and entry.allocation is None and "hpcb" not in entry.description
    assert entry.compute.endpoint_name == "hpc-bridge-fake-run123"   # the harness tears it down by this name
    assert is_line(entry.compute.env_setup)                          # hpc-bridge's own line: built at the bootstrap
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-00")
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(tmp_path / "state"))
    fac = binding._facility_from_entry(entry, account="")
    assert fac.account_required is True and fac.profile.endpoint_name == "hpc-bridge-fake-run123"


# --- coupling -------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "profile", "not_on"), [
    ("term_switch_reasks", "mep", ("site", "default")),
    ("preset_account_scoped", "mep", ("site", "default")),
    ("ssh_account_floor", "site", ("default", "pbs", "totp")),
])
def test_each_runs_where_it_can_and_gates_only_checks_it_provides(name, profile, not_on):
    import importlib
    sc = importlib.import_module(name)
    assert sc.TARGETS == ("fake",)
    assert targets.meets(sc.REQUIRES, targets.load_profile(profile)["capabilities"])[0], targets.meets(
        sc.REQUIRES, targets.load_profile(profile)["capabilities"])
    for other in not_on:
        assert not targets.meets(sc.REQUIRES, targets.load_profile(other)["capabilities"])[0], (name, other)
    assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0]
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided
    assert sc.PERSONA is None and not getattr(sc, "MIDRUN_HOOKS", None)   # autonomous: the free operators drive it


# --- quote_before_spend: the agent names the block's queue and account before it starts --------------------------------


def _block(queue="debug", account="hpcb"):
    return {"facility": "fake-mep-strict", "queue": queue, "queue_source": "this_call", "account": account,
            "account_source": "this_call" if account else "none_needed", "account_required": bool(account),
            "nodes": 1, "walltime": "00:30:00", "max_node_hours": 0.5, "compute_only": True}


def _start(status="up", block=None, **inp):
    """A confirming ensure_endpoint_up whose result carries the receipt `block` (BlockTerms)."""
    inp = {"shape": "compute", "confirm_spend": True, **inp}
    res = {"status": status, "notice": "worker live"}
    if block is not None:
        res["block"] = block
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", inp, res)


def _ask(question, *labels, answer="Yes"):
    return ToolCall.of("AskUserQuestion", {"questions": [{"question": question,
                                                          "options": [{"label": lb} for lb in labels]}]},
                       {"text": f'Your questions have been answered: "{question}"="{answer}".'},
                       answers={question: answer})


def _quote(calls, texts, at=None):
    from invariants import quote_before_spend
    return quote_before_spend(Trace(calls, texts, at if at is not None else []))


def test_quote_before_spend_reads_the_terms_from_the_starts_block():
    calls = [_connect(), _start(block=_block()), _run("c1\n"), _stop()]
    said = "Starting 1 node on partition `debug`, charged to hpcb, for up to 30 min."
    assert _quote(calls, [said], [1]).ok
    # the terms said only AFTER the start (its report) do not count
    late = _quote(calls, ["Connected.", said], [1, 2])
    assert not late.ok and "call 1 (queue 'debug' not named, account 'hpcb' not named)" in late.detail
    # one of the two is not enough
    assert "account 'hpcb'" in _quote(calls, ["Starting on the `debug` partition."], [1]).detail
    assert "queue 'debug'" in _quote(calls, ["Charging it to hpcb now."], [1]).detail


def test_quote_before_spend_counts_the_question_the_user_saw():
    calls = [_connect(), _ask("Start 1 node on `debug`, charged to hpcb?", "Yes", "No"), _start(block=_block()),
             _stop()]
    assert _quote(calls, ["Done."], [3]).ok


def test_quote_before_spend_does_not_take_a_common_word_for_the_queue():
    # "a compute node" is not naming the `compute` partition; "the compute partition" is
    calls = [_connect(), _start(block=_block(queue="compute")), _stop()]
    assert not _quote(calls, ["I'll start a compute node charged to hpcb."], [1]).ok
    assert _quote(calls, ["I'll start a node on the compute partition, charged to hpcb."], [1]).ok
    assert _quote(calls, ["Queue: **compute**; account hpcb."], [1]).ok


def test_quote_before_spend_accepts_no_account_said_where_none_is_charged():
    calls = [_connect(), _start(block=_block(queue="main", account=None)), _stop()]
    assert _quote(calls, ["Starting a block on `main` — no account is needed here."], [1]).ok
    assert not _quote(calls, ["Starting a block on `main`."], [1]).ok


def test_quote_before_spend_grades_each_new_block_but_not_a_confirming_poll():
    from invariants import quote_before_spend
    b = _block()
    poll = [_connect(), _start("provisioning", b), _start("provisioning", b), _start("up", b), _stop()]
    t = Trace(poll, ["Starting on `debug`, charged to hpcb."], [1])
    assert quote_before_spend(t).ok                     # the polls re-pass confirm_spend on the same block
    # ...as the plugin answers them: only the call that newly confirmed carries the receipt
    bare = [_connect(), _start("provisioning", b), _start("provisioning"), _start("up"), _stop()]
    assert quote_before_spend(Trace(bare, ["Starting on `debug`, charged to hpcb."], [1])).ok
    from invariants import _new_block_starts
    assert _new_block_starts(Trace(bare)) == [1]          # the blockless polls are the same block, not new starts
    # a re-ask (the block was reaped) makes the next start a new block: it must be quoted again
    reask = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "x"},
                        {"phase": "needs_confirmation", "notice": "Block terms: … the previous block …"})
    again = [_connect(), _start("up", b), reask, _start("up", b), _stop()]
    r = quote_before_spend(Trace(again, ["Starting on `debug`, charged to hpcb.", "It was reaped; starting again."],
                                 [1, 3]))
    assert not r.ok and "call 3 (no spend question since the last start)" in r.detail   # the terms were said
    # a re-ask on the same terms needs only the question again: the terms may have been named before
    ask = _ask("The block was reaped. Start a new one on the same terms?", "Yes", "No")
    ok = [_connect(), _start("up", b), reask, ask, _start("up", b), _stop()]
    assert quote_before_spend(Trace(ok, ["Starting on `debug`, charged to hpcb."], [1])).ok
    # ...and a switch to other terms is a new block too
    switched = [_connect(), _start("up", b), _start("up", _block(queue="compute")), _stop()]
    r = quote_before_spend(Trace(switched, ["Starting on `debug`, charged to hpcb.", "Switching."], [1, 2]))
    assert not r.ok and "call 2 (queue 'compute' not named)" in r.detail   # changed: said since the last start
    # (the unchanged account was said before: it counts from the run's start) — and a changed queue named only before
    # the previous start does not count for the switch
    r = quote_before_spend(Trace(switched, ["Partitions `debug` or `compute`; starting on `debug`, charged to hpcb.",
                                            "Switching."], [1, 2]))
    assert not r.ok and "call 2 (queue 'compute' not named)" in r.detail


def test_quote_before_spend_does_not_grade_a_start_without_a_block():
    r = _quote([_connect(), _start(), _stop()], ["Starting."], [1])
    assert r.ok and r.detail.startswith("not graded: no start carried a `block`")
    assert _quote([_connect(), _stop()], []).detail == "no billed start"


def test_quote_before_spend_is_reported_everywhere_and_report_only_in_benchmark_mode():
    from invariants import OPERATOR_PREFERENCE_GRADERS
    assert "quote_before_spend" in {r.name for r in check_all(Trace([]))}
    assert "quote_before_spend" in OPERATOR_PREFERENCE_GRADERS


# --- terms_prestated / terms_wanted ----------------------------------------------------------------------------------


def test_terms_prestated_passes_a_one_line_statement_and_fails_a_question():
    import terms_prestated as sc
    from invariants import quote_before_spend
    good = Trace([_connect(), _start(block=_block()), _run("c1\n"), _stop()],
                 ["Starting 1 node on partition `debug`, charged to hpcb (up to 30 min).", "hostname: c1. Stopped."],
                 [1, 4])
    for g in (sc.no_redundant_terms_question, sc.terms_used("debug", "hpcb"), quote_before_spend):
        assert g(good).ok, (g.__name__, g(good).detail)
    asked = Trace([_connect(), _ask("Start 1 node on `debug`, charged to hpcb?", "Yes", "No"), _start(block=_block()),
                   _stop()], ["ok"], [3])
    r = sc.no_redundant_terms_question(asked)
    assert not r.ok and "AskUserQuestion at [1]" in r.detail
    prose = Trace([_connect(), _start(block=_block()), _stop()], ["Shall I start the node on `debug` for hpcb?"], [1])
    assert not sc.no_redundant_terms_question(prose).ok
    # the start charged something else
    wrong = Trace([_connect(), _start(block=_block(queue="compute")), _stop()], [], [])
    assert not sc.terms_used("debug", "hpcb")(wrong).ok and sc.terms_used(None, "hpcb")(wrong).ok
    assert not sc.no_redundant_terms_question(Trace([_connect(), _stop()])).ok   # premise: nothing started


def test_terms_used_falls_back_to_what_the_call_passed():
    import terms_prestated as sc
    t = Trace([_connect(), _start(partition="debug", account="hpcb"), _stop()])
    r = sc.terms_used("debug", "hpcb")(t)
    assert r.ok and "its input" in r.detail


@pytest.mark.parametrize("name", ["terms_prestated", "terms_wanted"])
def test_the_persona_pair_runs_on_the_mep_profile_and_gates_only_checks_it_provides(name):
    import importlib

    from human_sim import PERSONAS
    sc = importlib.import_module(name)
    assert sc.TARGETS == ("fake",) and sc.SERIAL and sc.FACILITY_ID == "fake-mep-strict"
    assert targets.meets(sc.REQUIRES, targets.load_profile("mep")["capabilities"])[0]
    for other in ("site", "default"):
        assert not targets.meets(sc.REQUIRES, targets.load_profile(other)["capabilities"])[0], (name, other)
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided
    assert "quote_before_spend" in sc.EXPECT_OK and sc.PERSONA in PERSONAS
    assert ("spend_follows_question" in sc.EXPECT_OK) is (name == "terms_wanted")   # the pre-stated user is not asked


def test_the_wants_terms_persona_answers_a_which_question_and_holds_out_on_a_bare_approval():
    # the persona as the human-sim runs it (its one SDK touchpoint stubbed): the prompt carries the rules, and the
    # canned line it gives a bare approval question is a question back, never a standing refusal
    import asyncio

    from human_sim import HumanSim
    from invariants import _DECLINE
    sim = HumanSim(persona="wants_terms", goal="Your project is hpcb.")
    prompts: list[str] = []
    script = ['{"action": "reply", "reply": "hpcb", "kind": "answer", "reason": "asked which account"}',
              '{"action": "reply", "reply": "Which queue and which project would this charge?", "kind": "unclear", '
              '"reason": "no terms named"}']

    async def fake_ask(prompt, system):
        prompts.append(prompt)
        return script.pop(0)

    sim._ask = fake_ask
    which = asyncio.run(sim.move("Which project should I charge for the block?"))
    bare = asyncio.run(sim.move("OK to start the billed block?"))
    assert which.reply == "hpcb" and bare.reply == "Which queue and which project would this charge?"
    assert "answer it from your goal (your project id, or the queue), never with the line below" in prompts[0]
    assert "giving your project id IS your yes to start" in prompts[0]
    assert "a question back, not a refusal (kind: unclear)" in prompts[0]
    assert sim.dialogue[-1].kind == "unclear" and not sim._standing_decline()
    assert not _DECLINE.search(bare.reply)            # no_spend_after_decline does not read it as a refusal


# --- review round 2: the grader's reach -------------------------------------------------------------------------------


def _ask_opts(question, labels, answer):
    return _ask(question, *labels, answer=answer)


def test_quote_counts_an_option_label_that_starts_with_the_queue():
    # Claude's usual form: "Which partition?" with "debug (Recommended)" — the label names it
    calls = [_connect(), _ask_opts("Which partition, charged to hpcb?", ["debug (Recommended)", "compute"], "debug (Recommended)"),
             _start(block=_block()), _stop()]
    assert _quote(calls, ["ok"], [3]).ok
    calls = [_connect(), _ask_opts("Which one, charged to hpcb?", ["**debug** — 30 min", "compute"], "**debug** — 30 min"),
             _start(block=_block()), _stop()]
    assert _quote(calls, [], []).ok


@pytest.mark.parametrize("table", [
    "| Facility | Nodes | Queue | Account |\n|---|---|---|---|\n| fake-mep-strict | 1 | debug | hpcb |",
    "| Partition | Account |\n|---|---|\n| **debug** | hpcb |",
])
def test_quote_counts_a_table_column_headed_partition_or_queue(table):
    calls = [_connect(), _start(block=_block()), _stop()]
    assert _quote(calls, [table], [1]).ok
    assert not _quote(calls, [table.replace("debug", "compute")], [1]).ok


def test_quote_matches_a_queue_name_whole():
    calls = [_connect(), _start(block=_block(queue="debug")), _stop()]
    assert not _quote(calls, ["Submit to the `debug-scaling` queue, charged to hpcb?"], [1]).ok
    assert not _quote(calls, ["The gpu-debug partition, charged to hpcb."], [1]).ok
    assert not _quote(calls, ["No idle nodes in that partition — let me debug why. Charged to hpcb."], [1]).ok
    assert not _quote(calls, ["Partition `debug`. Connected to login01.hpcb.test."], [1]).ok   # a host is no account
    assert _quote(calls, ["Partition is debug; charged to hpcb."], [1]).ok
    calls = [_connect(), _start(block=_block(queue="debug-scaling")), _stop()]
    assert _quote(calls, ["queue debug-scaling, account hpcb"], [1]).ok
    assert not _quote(calls, ["the `debug` partition, account hpcb"], [1]).ok


def test_quote_credits_an_account_the_user_typed_to_a_which_account_question():
    q = "Which project should fake-mep-strict charge? I'd use the default `compute` partition: 1 node, up to 30 min."
    calls = [_connect(), _ask_opts(q, ["I'll type it", "Cancel"], "hpcb"), _start(block=_block(queue="compute")),
             _stop()]
    assert _quote(calls, ["Done."], [4]).ok


def test_quote_reads_a_preset_named_as_the_preset():
    b = dict(_block(account="hpcb-gpu"), account_source="env")
    calls = [_connect(), _start(block=b), _stop()]
    assert _quote(calls, ["Starting 1 node on `debug`, charged to your HPC_BRIDGE_ACCOUNT preset."], [1]).ok
    assert not _quote(calls, ["Starting 1 node on `debug`."], [1]).ok
    bq = dict(_block(queue="debug"), queue_source="env")
    assert _quote([_connect(), _start(block=bq), _stop()], ["On the preset partition, charged to hpcb."], [1]).ok


def test_quote_reads_none_set_and_never_takes_the_refusal_for_no_account():
    none_set = dict(_block(queue="main", account=None), account_source="none_set", account_required=False)
    calls = [_connect(), _start(block=none_set), _stop()]
    assert _quote(calls, ["Starting 1 node on `main`, charged to your default Slurm account."], [1]).ok
    none_needed = _block(queue="main", account=None)
    calls = [_connect(), _start(block=none_needed), _stop()]
    assert not _quote(calls, ["Starting on `main`; last time it said NO ACCOUNT at this facility."], [1]).ok


def test_unplaced_texts_are_not_graded():
    import terms_prestated as sc
    from invariants import quote_before_spend
    t = Trace([_connect(), _start(block=_block()), _run("c1\n"), _stop()],
              ["Connected.", "Done: ran on the `debug` partition, charged to hpcb; stopped."])   # no positions
    for g in (quote_before_spend, sc.no_redundant_terms_question):
        r = g(t)
        assert r.ok and r.detail.startswith("not graded: texts unplaced"), (g.__name__, r.detail)
    from invariants import OPERATOR_PREFERENCE_GRADERS
    assert "no_redundant_terms_question" in OPERATOR_PREFERENCE_GRADERS


def test_no_redundant_terms_question_reads_prose_questions_right():
    import terms_prestated as sc
    assert sc._asks_terms_in_prose("Shall I go ahead?")
    assert sc._asks_terms_in_prose("Want me to start the node on `debug`?")
    assert not sc._asks_terms_in_prose("Why compute-only? Because the facility endpoint has no login node.")
    assert not sc._asks_terms_in_prose("See https://docs.example.org/queue?id=3 for the queue policy.")


def test_no_redundant_terms_question_calls_a_question_after_a_refusal_a_broken_premise():
    import terms_prestated as sc
    down = ToolCall.of("mcp__endpoint__ensure_endpoint_up",
                       {"shape": "compute", "confirm_spend": True, "partition": "debug", "account": "hpcb"},
                       {"status": "down", "notice": "REJECTED: Invalid account or account/partition combination"})
    t = Trace([_connect(), down, _ask_opts("Slurm rejected hpcb on `debug`. Which account should I charge?",
                                           ["hpcb-gpu", "Cancel"], "hpcb-gpu"),
               _start(block=_block(account="hpcb-gpu")), _stop()], ["Starting on `debug`, charged to hpcb."], [1])
    r = sc.no_redundant_terms_question(t)
    assert not r.ok and r.detail.startswith("premise broken: a start was refused (call 1)")


def test_choice_respected_does_not_read_a_question_back_as_a_pick():
    from invariants import choice_respected
    t = Trace([_connect(), _ask_opts("Start the compute block now?", ["Yes, start on compute", "No"],
                                     "Which queue and which project would this charge?"),
               _ask_opts("Start 1 node on `compute`, up to 30 min, charged to hpcb?", ["Yes", "No"], "Yes"),
               _start(block=_block(queue="compute"), partition="compute"), _stop()])
    assert choice_respected(t).ok


def test_terms_wanted_fails_a_start_the_user_never_approved():
    import terms_wanted as sc
    from invariants import check_all
    bare = Trace([_connect(), _ask_opts("OK to start the billed block?", ["Yes", "No"],
                                        "Which queue and which project would this charge?"),
                  _start(block=_block(queue="compute"), partition="compute", account="hpcb"), _run("c1\n"), _stop()],
                 ["I'll use the `compute` partition and charge hpcb.", "Started anyway. hostname: c1. Stopped."], [1, 3])
    res = {r.name: r for r in check_all(bare) + [g(bare) for g in sc.EXTRA_INVARIANTS]}
    assert not res["approved_with_terms"].ok and "was not approved" in res["approved_with_terms"].detail
    assert "approved_with_terms" in sc.EXPECT_OK
    # a yes to a question naming only the queue is not enough either
    half = Trace([_connect(), _ask_opts("Start 1 node on `compute`?", ["Yes", "No"], "Yes"),
                  _start(block=_block(queue="compute")), _stop()], [], [])
    assert "does not name the start's account" in sc.approved_with_terms(half).detail
    # the good path: the canned line, then a question naming both, approved
    good = Trace([_connect(), _ask_opts("Start the block?", ["Yes", "No"], "Which queue and which project would this charge?"),
                  _ask_opts("Start 1 node on `compute`, up to 30 min, charged to hpcb?", ["Yes", "No"], "Yes"),
                  _start(block=_block(queue="compute")), _stop()], [], [])
    assert sc.approved_with_terms(good).ok


# --- round 3: the one question whose answer is the confirmation --------------------------------------------------------


@pytest.mark.parametrize("question", [
    "Which project should fake-mep-strict charge? I'd use `compute`: 1 node, up to 30 min.",
    "Start 1 node on the `compute` partition (up to 30 min, at most 0.5 node-hours) — which project should it charge?",
    "Which project should I use? The default `compute` partition, 1 node, up to 30 min.",
    "Which project, and is `compute` OK?",                     # no spend word at all: still the spend question
])
def test_terms_wanted_passes_the_one_question_the_plugin_teaches(question):
    import terms_wanted as sc
    from invariants import _is_spend_question, check_all
    assert _is_spend_question(question)
    t = Trace([_connect(), _ask_opts(question, ["I'll type it", "Cancel"], "hpcb"),
               _start(block=_block(queue="compute"), account="hpcb"), _run("c1\n"), _stop()],
              ["Starting 1 node on `compute`, charged to hpcb, up to 30 min.", "hostname: c1. Stopped."], [2, 4])
    res = {r.name: r for r in check_all(t) + [g(t) for g in sc.EXTRA_INVARIANTS]}
    for name in ("approved_with_terms", "spend_follows_question", "quote_before_spend", "terms_used",
                 "no_spend_after_decline"):
        assert res[name].ok, (name, res[name].detail)


def test_terms_wanted_does_not_take_a_question_back_for_a_yes():
    import terms_wanted as sc
    t = Trace([_connect(), _ask_opts("Which project should it charge?", ["I'll type it"],
                                     "hpcb — which queue would this use?"),
               _start(block=_block(queue="compute"), account="hpcb"), _stop()], [], [])
    assert not sc.approved_with_terms(t).ok
    # a setup question naming an account-ish word stays setup
    from invariants import _is_spend_question
    assert not _is_spend_question("Which account should the endpoint use for its scratch dir?")


def test_a_listing_of_partitions_is_not_naming_the_queue():
    # an sinfo-style table of every partition and an allocation list, then a bare question: not the block's terms
    listing = ("| Partition | Idle | Time limit |\n|---|---|---|\n| debug | 2 | 30:00 |\n| compute | 0 | 2-00:00:00 |\n"
               "Allocations: hpcb (CPU, 10 SU).")
    calls = [_connect(), _ask_opts("Start a billed block now?", ["Yes", "No"], "Yes"), _start(block=_block()), _stop()]
    r = _quote(calls, [listing], [1])
    assert not r.ok and "queue 'debug' not named" in r.detail
    one = "| Partition | Account |\n|---|---|\n| debug | hpcb |"            # the terms of one block still count
    assert _quote(calls, [one], [1]).ok


# --- round 4: an identity or setup question is not the spend question -------------------------------------------------


@pytest.mark.parametrize("question", [
    "Which account do you log in with on the cluster (your username)?", "What's the account name?",
    "Which project directory should the results go to?", "Which Globus account did you sign in with?",
    "What account should I SSH as?", "Which project is this for, so I can name the output folder?",
    "Which project should I use?", "Which account is yours, for later?", "What project are you on?",
])
def test_a_which_account_question_without_terms_is_not_a_spend_question(question):
    from invariants import _is_spend_question
    assert not _is_spend_question(question)


@pytest.mark.parametrize("later", ["Which project should I use?", "Which account is yours, for later?",
                                   "What project are you on?"])
def test_a_which_project_answer_never_supersedes_a_spend_decline(later):
    from invariants import no_spend_after_decline
    decline = _ask_opts("Start 1 node on `debug` (up to 30 min), charged to your allocation?", ["Yes", "No"],
                        "No — I'd rather not spend allocation today.")
    t = Trace([_connect(), decline, _ask_opts(later, ["hpcb", "hpcb-gpu"], "hpcb"),
               _start("provisioning", block=_block(), account="hpcb")], [], [])
    assert not no_spend_after_decline(t).ok
