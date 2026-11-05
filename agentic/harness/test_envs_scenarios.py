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

ENV_SCENARIOS = ["env_ssh_build_cache", "env_ssh_switch", "env_mep_requirements", "env_mep_fallback"]
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


def test_each_env_scenario_runs_only_on_its_profile():
    caps = {p: targets.load_profile(p)["capabilities"] for p in ("default", "pbs_mep", "mep", "polaris", "site")}
    want = {"env_ssh_build_cache": "default", "env_ssh_switch": "default", "env_mep_requirements": "pbs_mep",
            "env_mep_fallback": "pbs_mep"}
    for name, prof in want.items():
        mod = importlib.import_module(name)
        assert targets.meets(mod.REQUIRES, caps[prof])[0], name
    # the facility cells need the PBS facility endpoint (the requirements one its second manager)
    for name in ("env_mep_requirements", "env_mep_fallback"):
        mod = importlib.import_module(name)
        for prof in ("mep", "polaris", "default"):
            assert not targets.meets(mod.REQUIRES, caps[prof])[0], (name, prof)
    assert importlib.import_module("env_mep_requirements").FACILITY_ID == "fake-pbs-mep-req"
    assert importlib.import_module("env_mep_fallback").FACILITY_ID == "fake-pbs-mep"


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


def _switch_run(*, act_status="up", tag="b", leak=False):
    req = {"requirements": ec.REQUIREMENTS}
    act = {"activate": "export HPCB_TAG=b"}
    act_env = _env(kind="activate", how="activate", key="fedcba9876543210", path=None)
    return Trace([
        _confirm(), _ask(req), _probe(prefix=PATH, venv=PATH, first=f"{PATH}/bin"),
        _ask(act, env=act_env, status=act_status),
        _probe(tag=tag, venv=PATH if leak else "none", first=f"{PATH}/bin" if leak else "/usr/bin"),
        _c("ensure_endpoint_up", {"shape": "compute", "env": {}}, {"status": "up", "env": None}),
        _probe(),
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


# ---- the facility cells ----------------------------------------------------------------------------------------------

def _mep_run(how, *, words, new_block=True, receipt_how=None, prefix=None, venv="none"):
    req = {"requirements": ec.REQUIREMENTS}
    e = _env(how=how, new_block=new_block, billed=f"{words}", path=None if how == "facility" else PATH)
    ask = _c("ensure_endpoint_up", {"shape": "compute", "env": req},
             {"status": "needs_confirmation", "block": {"queue": "debug", "env": e},
              "notice": "A new env is a new user endpoint and a new block."})
    yes = _c("ensure_endpoint_up", {"shape": "compute", "env": req, "confirm_spend": True},
             {"status": "provisioning", "block": {"queue": "debug", "env": e}, "env": {**e, "how": receipt_how or how}})
    return Trace([_confirm(), ask, yes, _probe(prefix=prefix, venv=venv)])


def test_env_terms_want_the_how_a_new_block_and_the_words():
    t = _mep_run("facility", words="the first build is billed; the compute node must reach the package index")
    assert ec.env_terms(t, how="facility", words=("new user endpoint", "new block", "billed")).ok
    assert not ec.env_terms(t, how="worker_init", words=("billed",)).ok
    assert not ec.env_terms(t, how="facility", words=("first build of the moon",)).ok
    nb = _mep_run("facility", words="billed", new_block=False)
    assert not ec.env_terms(nb, how="facility", words=("billed",)).ok


def test_env_receipt_wants_the_confirming_answer_to_state_the_env():
    assert ec.env_receipt(_mep_run("worker_init", words="first build billed"), how="worker_init").ok
    assert not ec.env_receipt(_mep_run("worker_init", words="x", receipt_how="activate"), how="worker_init").ok


def test_the_worker_runs_the_env_the_facility_built():
    venv = "/home/hpcbmep/.globus_compute/venvs/py3.11-gce4.16.0-0a1b2c3d4e5f"
    assert ec.worker_runs_facility_env(_mep_run("facility", words="billed", prefix=venv, venv=venv)).ok
    assert not ec.worker_runs_facility_env(_mep_run("facility", words="billed", prefix="/opt/hpcb-mep/gce")).ok


def test_the_command_runs_the_env_worker_init_built():
    t = _mep_run("worker_init", words="first build billed", prefix=PATH, venv=PATH)
    assert ec.env_built_by_worker_init(t).ok
    assert not ec.env_built_by_worker_init(_mep_run("worker_init", words="x", prefix=PATH, venv="none")).ok
    assert not ec.env_built_by_worker_init(_mep_run("worker_init", words="x", prefix="/opt/hpcb-mep/gce")).ok


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
