"""Hermetic: the `lmod` profile's manifest and the lmod_bootstrap scenario's graders."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402

MODULE_SETUP = ("type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.12.9; [ -d {venv} ] || uv venv {venv} "
                "--python 3.11; . {venv}/bin/activate; command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q x")
CURL_SETUP = 'export PATH="$HOME/.local/bin:$PATH"; command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh; uv venv {venv}'


def _proposal(env_setup):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "ssh_host": "login"},
                       {"phase": "proposed_facility_details", "proposed_details": {"env_setup": env_setup, "partition": "debug"}})


def _accept(env_setup):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": {"env_setup": env_setup, "ssh_host": "login"}},
                       {"phase": "provisioning"})


def test_lmod_layers_on_site_and_declares_the_module_system(monkeypatch):
    m = targets.load_profile("lmod")
    assert m["layers"] == ["site", "lmod"] and m["capabilities"]["module_system"] == "lmod" and m["capabilities"]["nodes"] == 3
    assert "uv/0.12.9" in m["capabilities"]["modules"] and "python/3.11" in m["capabilities"]["modules"]
    import lmod_bootstrap as sc
    assert targets.meets(sc.REQUIRES, m["capabilities"])[0]
    assert not targets.meets(sc.REQUIRES, targets.load_profile("site")["capabilities"])[0]
    assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0]
    assert sc.TARGETS == ("fake",) and sc.NEEDS_COMPUTE_NODE is True
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided


def test_lmod_bootstrap_graders_read_the_plugins_proposal_not_the_agents_fix():
    import lmod_bootstrap as sc
    good = Trace([_proposal(MODULE_SETUP), _accept(MODULE_SETUP)])
    assert sc.proposal_uses_modules(good).ok and sc.no_curl_bootstrap(good).ok and sc.module_reinit_in_env_setup(good).ok
    # the plugin proposed the curl bootstrap; the agent fixed it by hand — the PROPOSAL still fails
    fixed_by_agent = Trace([_proposal(CURL_SETUP), _accept(MODULE_SETUP)])
    assert not sc.proposal_uses_modules(fixed_by_agent).ok and not sc.no_curl_bootstrap(fixed_by_agent).ok
    # a module proposal that assumes `module` exists (breaks in a batch script)
    naive = Trace([_proposal("module load uv/0.12.9; uv venv {venv}"), _accept("module load uv/0.12.9; uv venv {venv}")])
    assert sc.proposal_uses_modules(naive).ok and not sc.module_reinit_in_env_setup(naive).ok
    # no proposal at all (the facility was catalogued): the module graders fail loudly rather than pass vacuously
    assert not sc.proposal_uses_modules(Trace([_accept(MODULE_SETUP)])).ok


# ---- a details= that failed is not run again (live 2026-10-16) + the SCRIPT that stages it --------------------------

_LMOD = ('remote configure failed: Lmod has detected the following error: The following module(s) are unknown:\n'
         '"gcc/10.2.0"\n\nPlease check the spelling or version number.')
LMOD_ERR = "hpc-bridge error: RuntimeError: " + _LMOD       # as 7012984 and before answered it
NEW_ERR = "hpc-bridge error: ConfigStepFailed: " + _LMOD    # as the fixed product answers it (a typed step failure)
_STAGED_FAILED = {"phase": "failed", "notice": NEW_ERR + "\nThis configuration (details=) is not kept: "
                  "connect_facility('fake-1', ssh_host='login') without details resolves the facility afresh — …"}
_LEAD = ("The details= given earlier for 'fake-1' failed to come up (" + " ".join(NEW_ERR.split()) + ") and is not "
         "kept: this connect did not run it. ")
_FRESH = {"phase": "proposed_facility_details", "notice": _LEAD + "probed the login node and proposed this config — …",
          "proposed_details": {"ssh_host": "login", "interface": "eth0", "env_setup": MODULE_SETUP,
                               "scratch_root": "/home/{user}/.hpc-bridge", "partition": "debug", "scheduler": "slurm",
                               "allocation_command": "mybalance", "allocation_parser": "mybalance", "walltime": None}}
_UP = {"phase": "needs_account", "allocations": [{"account": "hpcb", "balance": 9587.5, "units": "SU", "type": "CPU"},
                                                 {"account": "hpcb-gpu", "balance": 488.0, "units": "SU", "type": "GPU"}]}
_HOSTNAME = {"phase": "complete", "exit_code": 0, "stdout": "c1\nCurrently Loaded Modules:\n  1) uv/0.12.9\n"}


class _ScriptOps:
    """script_runner.ScriptOps' surface, answering from `answers` (tool -> list, consumed in order); keeps each call."""

    def __init__(self, answers):
        import script_runner
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls, self.texts = [], []
        self.facility, self.ssh_host = "fake-1", "login"
        self._abort = script_runner.ScriptAbort

    async def call(self, tool, **args):
        res = self.answers[tool].pop(0)
        self.calls.append((tool, args, res))
        return res

    def say(self, text):
        self.texts.append(text)

    def abort(self, reason):
        raise self._abort(reason)

    async def sleep(self, s):
        return None

    def trace(self):
        return Trace([ToolCall.of(f"mcp__hpc_bridge__{t}", a, r) for t, a, r in self.calls], self.texts)


def _gated_failures(sc, t, operator="script"):
    """Every grader that gates this scenario and a trace decides (the floors and world rows are run.py's), as run.py
    builds them: the #39 row judged past a scripted run's staged failures, and gated by its rule
    (`first_bring_up_had_details`) — the failing ones."""
    from invariants import first_bring_up_had_details, regrade_staged, staged_connects
    staged = staged_connects(t, sc, operator)
    rows = regrade_staged(check_all(t), t, staged) + [fn(t) for fn in sc.EXTRA_INVARIANTS]
    results = {r.name: r for r in rows}
    gated = set(sc.EXPECT_OK) | ({"first_details_connect_succeeds"} if first_bring_up_had_details(t, staged) else set())
    return {n: results[n].detail for n in gated if not results[n].ok}


def test_the_lmod_script_stages_the_failure_then_brings_the_fresh_proposal_up():
    import asyncio

    import lmod_bootstrap as sc
    ops = _ScriptOps({"connect_facility": [_STAGED_FAILED, _FRESH, {"phase": "provisioning"}, _UP],
                      "ensure_endpoint_up": [{"status": "provisioning"}, {"status": "up"}],
                      "run_shell": [_HOSTNAME],
                      "stop_endpoint": [{"status": "draining", "notice": "spend NOT confirmed stopped"},
                                        {"status": "down", "spend_stopped": True, "notice": "released 1"}]})
    final = asyncio.run(sc.SCRIPT(ops))
    tools = [t for t, _a, _r in ops.calls]
    assert tools == ["connect_facility"] * 4 + ["ensure_endpoint_up"] * 2 + ["run_shell"] + ["stop_endpoint"] * 2
    staged = ops.calls[0][1]["details"]
    assert staged["env_setup"] == "module load gcc/10.2.0" and "display_name" not in staged   # no product field marks it
    assert "details" not in ops.calls[1][1]                                     # the connect without details
    confirm = ops.calls[2][1]["details"]
    assert "env_setup" not in confirm and confirm["partition"] == "debug" and "walltime" not in confirm
    assert ops.calls[4][1] == {"shape": "compute", "confirm_spend": True, "account": "hpcb"}   # not the GPU account
    assert ops.calls[5][1] == {"shape": "compute", "account": "hpcb"}                 # polls never confirm again
    assert ops.calls[6][1] == {"shape": "compute", "command": "hostname; module list 2>&1 | tail -3"}
    assert "'down'" in final and "uv/0.12.9" in ops.texts[-1]
    t = ops.trace()
    assert _gated_failures(sc, t) == {}        # every gate, the #39 rule's included (judged at the confirm, call 2)
    # under any other operator the staged call is an ordinary first bring-up: the gate judges it, and it failed
    assert "first_details_connect_succeeds" in _gated_failures(sc, t, operator="hermes21")


def test_the_staged_failure_is_the_scenarios_and_the_script_operators_alone():
    from types import SimpleNamespace

    from invariants import first_bring_up_had_details, first_details_connect_succeeds, staged_connects

    def conn(result, details):
        return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": details}, result)
    d = {"ssh_host": "login", "env_setup": "module load gcc/10.2.0"}
    failed = {"phase": "failed", "notice": NEW_ERR}
    scen = SimpleNamespace(STAGED_FAILED_CONNECTS=(0,))
    t = Trace([conn(failed, d), conn({"phase": "provisioning"}, d)])
    assert staged_connects(t, scen, "script") == frozenset({0})
    # nothing an agent sends earns it: other operators, or a scenario that declares none, pass over nothing
    for op in ("claude", "hermes21", "codex", "pi", "claude-acp", None):
        assert staged_connects(t, scen, op) == frozenset()
    assert staged_connects(t, SimpleNamespace(), "script") == frozenset()
    skip = staged_connects(t, scen, "script")
    assert first_bring_up_had_details(t, skip) and first_details_connect_succeeds(t, skip).ok
    assert "staged failure at call 0 passed over" in first_details_connect_succeeds(t, skip).detail
    # passed over, the next bring-up is judged — its failure still fails the gate
    t2 = Trace([conn(failed, d), conn(failed, d)])
    assert not first_details_connect_succeeds(t2, staged_connects(t2, scen, "script")).ok
    # a staged call that did NOT fail is an ordinary bring-up, judged as one
    t3 = Trace([conn({"phase": "provisioning"}, d)])
    assert staged_connects(t3, scen, "script") == frozenset()
    # and an agent's own failed first details= (2026-10-16) is judged as before
    assert not first_details_connect_succeeds(t).ok


def test_the_lmod_script_aborts_on_a_build_that_runs_the_failed_details_again():
    import asyncio

    import lmod_bootstrap as sc
    import script_runner
    rerun = {"phase": "failed", "notice": LMOD_ERR}            # 7012984: the connect without details ran it again
    ops = _ScriptOps({"connect_facility": [{"phase": "failed", "notice": LMOD_ERR}, rerun]})
    with pytest.raises(script_runner.ScriptAbort, match="it ran the failed details= again"):
        asyncio.run(sc.SCRIPT(ops))
    assert not sc.failed_details_not_rerun(ops.trace()).ok   # and an agent run of that build fails the cell too


@pytest.mark.parametrize(("answers", "match"), [
    ([{"phase": "provisioning"}], "did not fail on the login node"),                    # the site HAS the module
    ([_STAGED_FAILED, {**_FRESH, "proposed_details": {**_FRESH["proposed_details"], "env_setup": CURL_SETUP}}],
     "not a module-based one"),
    ([_STAGED_FAILED, {**_FRESH, "notice": "probed the login node and proposed this config — …"}],
     "does not say what failed"),
    ([_STAGED_FAILED, {"phase": "needs_facility_details", "notice": "give me its SSH host"}], "did not propose afresh"),
])
def test_the_lmod_script_aborts_when_a_premise_fails(answers, match):
    import asyncio

    import lmod_bootstrap as sc
    import script_runner
    with pytest.raises(script_runner.ScriptAbort, match=match):
        asyncio.run(sc.SCRIPT(_ScriptOps({"connect_facility": answers})))


def _conn(result, details=None, facility="f"):
    return ToolCall.of("mcp__endpoint__connect_facility",
                       {"facility": facility, "ssh_host": "login", **({"details": details} if details else {})}, result)


_D = {"ssh_host": "login", "env_setup": "module load gcc/10.2.0"}


def test_failed_details_not_rerun_reads_the_live_bug():
    import lmod_bootstrap as sc
    old = {"phase": "failed", "notice": LMOD_ERR}
    # the live run (hermes21, 1792183016): call 0's failure answered again by the connect without details — with, in
    # between, a details= the harness rejected (never reached the server) and one refused unread (no env_setup)
    assert not sc.failed_details_not_rerun(Trace([_conn(old, _D), _conn(old)])).ok
    rejected = _conn({"error": "failed argument validation: 'partition' is a required property"}, {"ssh_host": "login"})
    refused = _conn({"phase": "failed", "notice": "details= has no env_setup and there is no proposal for this "
                     "facility to take it from. …"}, {"ssh_host": "login", "partition": "debug"})
    r = sc.failed_details_not_rerun(Trace([_conn(old, _D), rejected, refused, _conn(old)]))
    assert not r.ok and "(call 3)" in r.detail and "at call 0" in r.detail
    # a fixed build that claimed the drop and ran it all the same is caught by its own claim
    assert not sc.failed_details_not_rerun(Trace([_conn(_STAGED_FAILED, _D),
                                                  _conn({"phase": "failed", "notice": NEW_ERR})])).ok
    # fixed: the connect without details proposes afresh, or opens any answer with what failed
    assert sc.failed_details_not_rerun(Trace([_conn(_STAGED_FAILED, _D), _conn(_FRESH)])).ok
    assert sc.failed_details_not_rerun(Trace([_conn(_STAGED_FAILED, _D),
                                              _conn({"phase": "failed", "notice": _LEAD + "x"})])).ok
    # the agent's own retry of the same details= is its call, not a rerun; a details= in between starts over
    assert sc.failed_details_not_rerun(Trace([_conn(old, _D), _conn(old, _D)])).ok
    assert sc.failed_details_not_rerun(Trace([_conn(old, _D), _conn({"phase": "provisioning"}, _D), _conn(old)])).ok
    assert sc.failed_details_not_rerun(Trace([])).ok


@pytest.mark.parametrize("first", [
    # (a) a config that CAME UP but whose allocation listing failed: proven, kept on purpose
    {"phase": "failed", "notice": "allocation discovery ('mybalance') failed: mybalance: command timed out"},
    # (b) the teardown-still-running refusal (it asks for connect_facility again)
    {"phase": "failed", "notice": "a teardown of the current endpoint is still running on its login node. Call "
                                  "teardown_endpoint to wait for it (it reports 'down' when done), then connect_facility "
                                  "again."},
    # (c) the Globus login (the product keeps the config: login_expired)
    {"phase": "failed", "notice": "hpc-bridge error: LoginExpiredError: the stored Globus login has expired — Globus "
                                  "refused to refresh it. Nothing is wrong with the facility or its config: …"},
    # failures outside the config, kept for the retry their notices ask for
    {"phase": "failed", "notice": "CANNOT REACH login: ssh: connect to host login port 22: Connection refused."},
    {"phase": "failed", "notice": "hpc-bridge error: RuntimeError: remote configure failed: Connection to login closed "
                                  "by remote host."},
    {"phase": "failed", "notice": "hpc-bridge error: RuntimeError: remote configure failed: pip: Read timed out."},
    # a typed failure the product KEPT (a config equal to the one proven for the host): no claim, so no drop
    {"phase": "failed", "notice": NEW_ERR},
])
def test_failed_details_not_rerun_spares_every_retry_the_product_keeps_on_purpose(first):
    import lmod_bootstrap as sc
    assert sc.failed_details_not_rerun(Trace([_conn(first, _D), _conn(first)])).ok


def test_failed_details_not_rerun_forgets_a_drop_once_the_facility_came_up_another_way():
    # (d) a failed details=, then a connect without details that comes up on the registry's or the cache's config (the
    # product forgets the drop then), then a later failure of THAT config — every Lmod error shares a long opening
    import lmod_bootstrap as sc
    other = {"phase": "failed", "notice": "hpc-bridge error: ConfigStepFailed: remote configure failed: Lmod has "
                                          "detected the following error: The following module(s) are unknown: \"x\""}
    t = Trace([_conn(_STAGED_FAILED, _D), _conn({"phase": "needs_account"}), _conn(other)])
    assert sc.failed_details_not_rerun(t).ok
    # and the match runs past the shared opening: a different module is a different failure
    t2 = Trace([_conn({"phase": "failed", "notice": LMOD_ERR}, _D),
                _conn({"phase": "failed", "notice": LMOD_ERR.replace("gcc/10.2.0", "python/3.13")})])
    assert sc.failed_details_not_rerun(t2).ok


def test_the_graders_words_are_the_products():
    # the scenario keeps a copy of notices._NOT_THE_CONFIG_WORDS (it does not import the product): read the product's
    # from THIS tree's source (a bare pytest in a worktree imports main's installed copy), so the two cannot drift
    import ast
    import re

    import lmod_bootstrap as sc
    src = (HERE.parents[1] / "src" / "hpc_bridge" / "notices.py").read_text()
    node = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Assign)
                and any(isinstance(tg, ast.Name) and tg.id == "_NOT_THE_CONFIG_WORDS" for tg in n.targets))
    call = node.value
    assert isinstance(call, ast.Call) and isinstance(call.args[0], ast.Constant)
    assert sc.NOT_THE_CONFIG_WORDS.pattern == call.args[0].value
    assert sc.NOT_THE_CONFIG_WORDS.flags & re.IGNORECASE and ast.unparse(call.args[1]) == "re.IGNORECASE"
