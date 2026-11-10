"""Hermetic: block environments' scenarios (agentic/scenarios/env_*: envs_checks.py) — each scenario is wired, its batch
command passes run_batch's template reader as it stands, and the graders and world checks read synthetic traces and a
synthetic facility the way they will read a live cell's: each passes a run that did it and fails one that did not. No
server, no cluster."""
from __future__ import annotations

import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import envs_checks as ec  # noqa: E402
import targets  # noqa: E402
from invariants import Result, ToolCall, Trace  # noqa: E402

ENV_SCENARIOS = ["env_ssh_build_cache", "env_ssh_switch"]   # PR 4's facility cells: branch feat/block-envs-mep
_BUILTIN = {"agent_engaged", "ends_with_stop", "stop_is_honest"}
KEY = "0123456789abcdef"
PATH = f"/home/u/.hpc-bridge/envs-fake-1/{KEY}-x86_64"
BID = "batch-0123456789ab"


def _c(name, inp=None, res=None, *, t=None):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    if t is not None:
        c.t_call, c.t_result = t
    return c


def _env(**over):
    e = {"kind": "built", "how": "login", "key": KEY, "state": "ready", "path": PATH, "cached": False,
         "new_block": False, "billed": None, "python": "3.11.9"}
    e.update(over)
    return e


def _confirm():
    return _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
              {"status": "up", "block": {"queue": "debug"}})


def _ask(env_in, *, env=None, status="up", t=None, **res):
    return _c("ensure_endpoint_up", {"shape": "compute", "env": env_in},
              {"status": status, "env": env if env is not None else _env(), **res}, t=t)


def _probe(*, tag="none", venv="none", first="/usr/bin", prefix=None, six=ec.SIX):
    py = f"HPCB_ENV_PY {prefix} {six}" if prefix else "HPCB_ENV_PY none"
    return _c("run_shell", {"command": ec.PROBE},
              {"phase": "complete", "stdout": f"HPCB_ENV_PROBE tag={tag} venv={venv} first={first}\n{py}\n"})


def _ctx(trace, out=""):
    return types.SimpleNamespace(trace=trace, messages=[], ssh=lambda _cmd, _timeout=60: (0, out), tap=None,
                                 endpoint_name="ep")


# ---- wiring ----------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name", ENV_SCENARIOS)
def test_each_env_scenario_is_wired(name):
    mod = importlib.import_module(name)
    graders = {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
    assert set(mod.EXPECT_OK) - _BUILTIN <= graders, set(mod.EXPECT_OK) - _BUILTIN - graders
    assert callable(mod.SCRIPT) and mod.TARGETS == ("fake",) and mod.OPERATORS == ("script",)
    for fn in mod.EXTRA_INVARIANTS:
        r = fn(Trace([]))
        assert isinstance(r, Result) and not r.ok, (fn, r)  # an empty trace is a failed premise, never a crash
    for fn in getattr(mod, "WORLD_CHECKS", []):
        r = asyncio.run(fn(_ctx(Trace([]))))
        assert isinstance(r, Result) and not r.ok         # nothing to read is never a pass


def test_each_env_scenario_runs_on_an_ssh_slurm_profile_and_never_a_facility_endpoint():
    caps = {p: targets.load_profile(p)["capabilities"] for p in ("default", "pbs_mep", "mep")}
    for name in ENV_SCENARIOS:
        mod = importlib.import_module(name)
        assert targets.meets(mod.REQUIRES, caps["default"])[0], name
        assert not getattr(mod, "FACILITY_ID", None), name           # a BYO SSH facility: the probe's proposal
        assert "mep" not in mod.REQUIRES, name


def test_the_switch_cells_batch_command_passes_the_template_reader():
    from hpc_bridge import batch

    mod = importlib.import_module("env_ssh_switch")
    batch.validate(mod.COMMAND, mod.ROWS, 60)


def test_the_probe_reads_back_what_the_command_saw():
    t = Trace([_probe(tag="b", venv=PATH, first=f"{PATH}/bin", prefix=PATH), _probe()])
    got = ec.probes(t)
    assert got[0][1] == {"tag": "b", "venv": PATH, "first": f"{PATH}/bin", "prefix": PATH, "six": ec.SIX}
    assert got[1][1]["prefix"] is None and got[1][1]["tag"] == "none"
    assert "{" not in ec.PROBE.replace("${", "")                # no run_batch placeholder hides in it


# ---- the SSH cells ---------------------------------------------------------------------------------------------------

def test_built_on_login_wants_the_login_free_and_no_spend_question():
    req = {"requirements": ec.REQUIREMENTS}
    assert ec.env_built_on_login(Trace([_confirm(), _ask(req), _probe(prefix=PATH)])).ok
    building = _ask(req, env=_env(state="building", path=None))
    ready = _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "env": _env()})
    assert ec.env_built_on_login(Trace([_confirm(), building, ready])).ok   # polled until ready
    assert not ec.env_built_on_login(Trace([_confirm(), _ask(req, env=_env(how="block", billed="billed"))])).ok
    assert not ec.env_built_on_login(Trace([_confirm(), _ask(req, status="needs_confirmation")])).ok
    assert not ec.env_built_on_login(Trace([_confirm(), _ask(req, env=_env(state="failed"))])).ok


def test_cache_hit_wants_the_same_env_cached():
    req = {"requirements": ec.REQUIREMENTS}
    assert ec.env_cache_hit(Trace([_ask(req), _ask(req, env=_env(cached=True))])).ok
    assert not ec.env_cache_hit(Trace([_ask(req), _ask(req, env=_env(cached=False))])).ok        # rebuilt
    assert not ec.env_cache_hit(Trace([_ask(req), _ask(req, env=_env(cached=True, path="/elsewhere"))])).ok
    assert not ec.env_cache_hit(Trace([_ask(req)])).ok


def test_active_in_command_wants_the_envs_own_prefix_and_six():
    req = {"requirements": ec.REQUIREMENTS}
    assert ec.env_active_in_command(Trace([_ask(req), _probe(prefix=PATH, venv=PATH)])).ok
    assert not ec.env_active_in_command(Trace([_ask(req), _probe(prefix="/usr")])).ok
    assert not ec.env_active_in_command(Trace([_ask(req), _probe(prefix=PATH, six="1.16.0")])).ok
    assert not ec.env_active_in_command(Trace([_ask(req), _probe()])).ok


ENDPOINT_VENV = "/home/u/.hpc-bridge/gce-venv-endpoint-4.18.0-py3.11"   # hpc-bridge's own: every live command's


def _switch_run(*, act_status="up", tag="b", leak=False, base=("none", "/usr/bin")):
    """`base`: what a command sees with no env of the user's — (VIRTUAL_ENV, PATH's first entry). On a live stack that
    is hpc-bridge's own endpoint venv (L1)."""
    req = {"requirements": ec.REQUIREMENTS}
    act = {"activate": "export HPCB_TAG=b"}
    act_env = _env(kind="activate", how="activate", key="fedcba9876543210", path=None)
    venv, first = base
    return Trace([
        _confirm(), _ask(req), _probe(prefix=PATH, venv=PATH, first=f"{PATH}/bin"),
        _ask(act, env=act_env, status=act_status),
        _probe(tag=tag, venv=PATH if leak else venv, first=f"{PATH}/bin" if leak else first),
        _c("ensure_endpoint_up", {"shape": "compute", "env": {}}, {"status": "up", "env": None}),
        _probe(venv=venv, first=first),
    ])


def test_switch_no_new_block_wants_every_switch_up_and_one_confirmation():
    assert ec.env_switch_no_new_block(_switch_run()).ok
    r = ec.env_switch_no_new_block(_switch_run(act_status="needs_confirmation"))
    assert not r.ok and "needs_confirmation" in r.detail
    two = Trace([*_switch_run().calls, _confirm()])
    assert not ec.env_switch_no_new_block(two).ok


def test_switch_clean_wants_nothing_of_the_old_env():
    assert ec.env_switch_clean(_switch_run(), tag="b").ok
    assert not ec.env_switch_clean(_switch_run(leak=True), tag="b").ok
    assert not ec.env_switch_clean(_switch_run(tag="none"), tag="b").ok


def test_switch_clean_judges_against_the_built_env_never_against_no_venv_at_all():
    """L1 (live, env_ssh_switch on c7fda3d): every command on the real stack runs inside hpc-bridge's own endpoint venv
    — VIRTUAL_ENV set, its bin first. That baseline is no leak; the built env's path or bin is."""
    live = (ENDPOINT_VENV, f"{ENDPOINT_VENV}/bin")
    assert ec.env_switch_clean(_switch_run(base=live), tag="b").ok
    assert not ec.env_switch_clean(_switch_run(base=live, leak=True), tag="b").ok
    leaked_after_none = Trace([*_switch_run(base=live).calls[:-1], _probe(venv=PATH, first=f"{PATH}/bin")])
    assert not ec.env_switch_clean(leaked_after_none, tag="b").ok


def test_batch_rows_in_env_wants_every_row_in_the_batchs_env():
    req = {"requirements": ec.REQUIREMENTS}
    rows = [{"index": i, "state": "done", "tail": f"row {i}\nHPCB_ENV_PY {PATH} {ec.SIX}\n"} for i in range(3)]
    run = _c("run_batch", {"command": "x", "rows": [1, 2, 3], "env": req},
             {"batch_id": BID, "phase": "admitting", "env": _env()})
    page = _c("status", {"id": BID, "detail": "page"}, {"batch_id": BID, "phase": "done", "results": rows})
    assert ec.batch_rows_in_env(Trace([run, page]), rows=3).ok
    wrong = [*rows[:2], {"index": 2, "state": "done", "tail": "row 2\nHPCB_ENV_PY none\n"}]
    page2 = _c("status", {"id": BID, "detail": "page"}, {"batch_id": BID, "phase": "done", "results": wrong})
    assert not ec.batch_rows_in_env(Trace([run, page2]), rows=3).ok


def test_an_env_building_answer_is_owed_a_retry_as_a_cold_start_is():
    from invariants import cold_start_is_retried

    building = _c("run_shell", {"command": "echo hi"}, {"phase": "env_building", "block_state": "warm"})
    again = _c("run_shell", {"command": "echo hi"}, {"phase": "complete", "block_state": "warm", "stdout": "hi\n"})
    assert cold_start_is_retried(Trace([building, again])).ok
    assert not cold_start_is_retried(Trace([building])).ok


# ---- the world check -------------------------------------------------------------------------------------------------

def _disk(*, arch="x86_64", built=1000.0, extra=""):
    marker = f"key={KEY}{'0' * 48}\nshort={KEY}\narch=x86_64\npython=3.11.9\nbuilt={built}\n"
    return f"{marker}HPCB_ARCH={arch}\n.\n..\n{KEY}-x86_64\n{extra}HPCB_DONE\n"


def test_env_on_login_disk_wants_the_marker_the_login_arch_one_build_and_no_leftovers():
    req = {"requirements": ec.REQUIREMENTS}
    t = Trace([_ask(req, t=(1.0, 2.0)), _ask(req, env=_env(cached=True), t=(10.0, 11.0))])
    t.t0 = 1000.0                                                        # the second ask at 1010
    assert asyncio.run(ec.env_on_login_disk(_ctx(t, _disk(built=1001.5)))).ok
    assert not asyncio.run(ec.env_on_login_disk(_ctx(t, _disk(arch="aarch64")))).ok          # not the login node's
    assert not asyncio.run(ec.env_on_login_disk(_ctx(t, _disk(built=1012.0)))).ok           # rebuilt by the hit
    assert not asyncio.run(ec.env_on_login_disk(_ctx(t, _disk(extra=f".{KEY}-x86_64.build.t0\n")))).ok
    assert not asyncio.run(ec.env_on_login_disk(_ctx(t, "Permission denied\n"))).ok          # unreadable
