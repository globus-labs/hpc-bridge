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

LMOD_ERR = ('hpc-bridge error: RuntimeError: remote configure failed: Lmod has detected the following error: The '
            'following module(s) are unknown:\n"gcc/10.2.0"\n\nPlease check the spelling or version number.')
_STAGED_FAILED = {"phase": "failed", "notice": LMOD_ERR + "\nThis configuration (details=) is not kept: "
                  "connect_facility('fake-1', ssh_host='login') without details resolves the facility afresh — …"}
_LEAD = ("The details= given earlier for 'fake-1' failed to come up (" + " ".join(LMOD_ERR.split()) + ") and is not "
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


def _gated_failures(sc, t):
    """Every grader that gates this scenario and a trace decides (the floors and world rows are run.py's), with
    `first_details_connect_succeeds` where run.py's rule adds it (`first_bring_up_had_details`): the failing ones."""
    from invariants import first_bring_up_had_details
    results = {r.name: r for r in check_all(t)} | {r.name: r for r in (fn(t) for fn in sc.EXTRA_INVARIANTS)}
    gated = set(sc.EXPECT_OK) | ({"first_details_connect_succeeds"} if first_bring_up_had_details(t) else set())
    return {n: results[n].detail for n in gated if not results[n].ok}


def test_the_lmod_script_stages_the_failure_then_brings_the_fresh_proposal_up():
    import asyncio

    import lmod_bootstrap as sc
    from invariants import STAGED_FAILURE, first_bring_up_had_details, first_details_connect_succeeds
    ops = _ScriptOps({"connect_facility": [_STAGED_FAILED, _FRESH, {"phase": "provisioning"}, _UP],
                      "ensure_endpoint_up": [{"status": "provisioning"}, {"status": "up"}],
                      "run_shell": [_HOSTNAME],
                      "stop_endpoint": [{"status": "draining", "notice": "spend NOT confirmed stopped"},
                                        {"status": "down", "spend_stopped": True, "notice": "released 1"}]})
    final = asyncio.run(sc.SCRIPT(ops))
    tools = [t for t, _a, _r in ops.calls]
    assert tools == ["connect_facility"] * 4 + ["ensure_endpoint_up"] * 2 + ["run_shell"] + ["stop_endpoint"] * 2
    staged = ops.calls[0][1]["details"]
    assert staged["env_setup"] == "module load gcc/10.2.0" and staged["display_name"] == STAGED_FAILURE
    assert "details" not in ops.calls[1][1]                                     # the connect without details
    confirm = ops.calls[2][1]["details"]
    assert "env_setup" not in confirm and confirm["partition"] == "debug" and "walltime" not in confirm
    assert ops.calls[4][1] == {"shape": "compute", "confirm_spend": True, "account": "hpcb"}   # not the GPU account
    assert ops.calls[5][1] == {"shape": "compute", "account": "hpcb"}                 # polls never confirm again
    assert ops.calls[6][1] == {"shape": "compute", "command": "hostname; module list 2>&1 | tail -3"}
    assert "'down'" in final and "uv/0.12.9" in ops.texts[-1]
    t = ops.trace()
    # the #39 gate judges the confirmed proposal's bring-up (call 2), not the staged failure — and still gates
    assert first_bring_up_had_details(t) and "call 2" in first_details_connect_succeeds(t).detail
    assert _gated_failures(sc, t) == {}


def test_the_lmod_script_aborts_on_a_build_that_runs_the_failed_details_again():
    import asyncio

    import lmod_bootstrap as sc
    import pytest
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


def test_failed_details_not_rerun_reads_the_live_bug_and_spares_a_retry_by_design():
    import lmod_bootstrap as sc

    def conn(result, details=None):
        return ToolCall.of("mcp__endpoint__connect_facility",
                           {"facility": "f", "ssh_host": "login", **({"details": details} if details else {})}, result)
    d = {"ssh_host": "login", "env_setup": "module load gcc/10.2.0"}
    failed = {"phase": "failed", "notice": LMOD_ERR}
    # the live run (hermes21, 1792183016): call 0's failure answered again by the connect without details — with, in
    # between, a details= the harness rejected (never reached the server) and one refused unread (no env_setup)
    assert not sc.failed_details_not_rerun(Trace([conn(failed, d), conn(failed)])).ok
    rejected = conn({"error": "failed argument validation: 'partition' is a required property"}, {"ssh_host": "login"})
    refused = conn({"phase": "failed", "notice": "details= has no env_setup and there is no proposal for this "
                    "facility to take it from. …"}, {"ssh_host": "login", "partition": "debug"})
    r = sc.failed_details_not_rerun(Trace([conn(failed, d), rejected, refused, conn(failed)]))
    assert not r.ok and "(call 3)" in r.detail and "at call 0" in r.detail
    # fixed: the connect without details proposes afresh, or opens any answer with what failed
    assert sc.failed_details_not_rerun(Trace([conn(failed, d), conn(_FRESH)])).ok
    assert sc.failed_details_not_rerun(Trace([conn(failed, d), conn({"phase": "failed", "notice": _LEAD + "x"})])).ok
    # a details= in between starts over; the agent's own retry of the same details= is its call, not a rerun
    assert sc.failed_details_not_rerun(Trace([conn(failed, d), conn(failed, d)])).ok
    assert sc.failed_details_not_rerun(Trace([conn(failed, d), conn({"phase": "provisioning"}, d), conn(failed)])).ok
    # a failure outside the config is kept for the retry its notice asks for: the same answer again is by design
    reach = {"phase": "failed", "notice": "CANNOT REACH login: ssh: connect to host login port 22: Connection refused."}
    assert sc.failed_details_not_rerun(Trace([conn(reach, d), conn(reach)])).ok
    assert sc.failed_details_not_rerun(Trace([])).ok


def test_the_staged_failure_marker_hides_only_a_failure_the_script_staged():
    from invariants import STAGED_FAILURE, first_bring_up_had_details, first_details_connect_succeeds

    def conn(result, details):
        return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f", "details": details}, result)
    staged = {"ssh_host": "login", "env_setup": "module load gcc/10.2.0", "display_name": STAGED_FAILURE}
    plain = {"ssh_host": "login", "env_setup": "module load gcc/10.2.0"}
    failed = {"phase": "failed", "notice": LMOD_ERR}
    # passed over: the first bring-up judged is the one after it, and a failure there still fails the gate
    t = Trace([conn(failed, staged), conn({"phase": "provisioning"}, plain)])
    assert first_bring_up_had_details(t) and first_details_connect_succeeds(t).ok
    assert not first_details_connect_succeeds(Trace([conn(failed, staged), conn(failed, plain)])).ok
    # an unmarked failure (an agent's made-up line, as on 2026-10-16) still fails it: nothing is weakened
    assert not first_details_connect_succeeds(Trace([conn(failed, plain), conn({"phase": "provisioning"}, plain)])).ok
    # a marked call that did NOT fail is an ordinary bring-up: judged as one
    t = Trace([conn({"phase": "provisioning"}, staged)])
    assert first_bring_up_had_details(t) and "call 0" in first_details_connect_succeeds(t).detail
