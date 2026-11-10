"""Block environments, PR 3 review round 2 (on 8e63a6d): the two reviewers' reproductions — R2-S1..S11 (the server)
and R2-N1..N8 (the node), moved here from tests/review_pr3_r2_server and tests/review_pr3_r2_node, each failing on
8e63a6d — and a test for each ruling on what was suspected (SS2..SS7, NS2..NS9) and for R2-N9 (bash 4+ ignoring errexit
inside an `if`). Every node test runs the generated bash for real, and passes under bash 3.2 (macOS) and 5.2 (Linux).

Where a reproduction was adapted, it says so: R2-S7's build count (four builds are sent, each needed: the reproduction
counted three), R2-N8's node without flock (made deterministic on Linux, where flock exists, by a `flock` that fails).
The `needs_uv` tests run the REAL uv, offline (UV_OFFLINE=1, UV_PYTHON_DOWNLOADS=never: nothing is fetched), on wheels
made here.
"""
from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest

from hpc_bridge import server
from tests import test_block_envs as tbe
from tests.test_block_envs import (
    _BASH,
    ARCH,
    NONCE,
    PY,
    REQS,
    _bash,
    _beside,
    _box,
    _builds,
    _built,
    _key,
    _purge,
    _ssh,
    _wrap_run,
    _writable,
)
from tests.test_server import _PendingFuture, _Res

pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]

_UV_REAL = shutil.which("uv")
needs_uv = pytest.mark.skipif(_UV_REAL is None, reason="needs a real uv (run offline here)")


@pytest.fixture(autouse=True)
def _quick_gone_recheck(monkeypatch):
    """A missing env is looked at again after GONE_RECHECK_S (review NS4): shortened here, but for the test of it."""
    from hpc_bridge import envs

    monkeypatch.setattr(envs, "GONE_RECHECK_S", 0.05, raising=False)


# =====================================================================================================================
# The server (R2-S1..S11, SS2..SS7)
# =====================================================================================================================

# ---- R2-S1: a refused block call never sets the env — whatever refused it --------------------------------------------

async def _ssh_with_partitions(tmp: Path, partitions):
    """tbe._ssh, with the partition shapes discovery (or the registry) saw: `main` is the session's partition."""
    from tests.test_handoff_phase2b import SSH_EID, _ssh_app

    block = tbe._EnvBlock(tmp, SSH_EID, scheduler="slurm", job="4242", pin=None, workers=2)
    app = _ssh_app(tmp, block=block, partitions=partitions)
    app.runners = tbe._EnvRunners(block)
    app.runner_factory = app.runners
    app.batch_autoloop = False
    st = await server._ensure_endpoint_up(app, "compute", confirm_spend=True)
    if st.status == "needs_confirmation":   # the slice proposed once the template was read: the user's yes again
        st = await server._ensure_endpoint_up(app, "compute", confirm_spend=True)
    assert st.status == "up", st
    return app


async def test_r2_s1_a_size_the_partition_refuses_never_sets_the_env(tmp_path):
    """A size refused by what discovery saw of the partition: its notice lacked the phrase S15's fix grepped for, so
    the env was built and set under a refused call. The block part now says so itself (`_nothing_set`)."""
    from hpc_bridge import resources
    from hpc_bridge.models import PartitionShape

    app = await _ssh_with_partitions(tmp_path, (PartitionShape(name="main", kind="per_core", cores=32),))
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS}, cores=64)
    assert "more than a node" in (st.notice or ""), st.notice                        # the size was refused…
    assert app.shapes["compute"].user_endpoint_config.get(resources.CORES) != 64      # …and not set
    changed = (app.env_spec, app.env_pending, len(_builds(app)))
    assert changed == (None, None, 0), (changed, st.notice)                          # …so neither is the env


async def test_r2_s1_a_partition_change_a_running_task_holds_never_sets_the_env(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    slow = base64.b64encode(b"echo SLOWMARK").decode()
    monkeypatch.setattr(tbe, "_is_build", lambda c: envs.BUILD_TAG in c or slow in c)
    app.runners.hold_builds = True
    out = await server._run_shell(app, "echo SLOWMARK", agent_call=True)
    assert out.phase == "running", out                                               # a task holds the block
    app.runners.hold_builds = False
    st = await server._ensure_endpoint_up(app, "compute", partition="other", env={"activate": "export HPCB_TAG=x"})
    assert "can't change partition" in (st.notice or ""), st.notice
    assert app.env_spec is None and "env was not looked at" in (st.notice or ""), (app.env_spec, st.notice)


async def test_r2_s1_the_envelopes_block_refusal_never_sets_the_env(tmp_path, monkeypatch):
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _confirmed

    sched = FakeSched(tmp_path / "sched", user="u")
    app = _app(tmp_path, sched)
    await _confirmed(app, command="sleep 60", envelope={"cap": 5})
    rt = server._shape_runtime(app, "compute")
    rt.user_endpoint_config["walltime"] = "01:00:00"
    st = await server._ensure_endpoint_up(app, "compute", confirm_spend=True, partition="main", nodes=4,
                                          env={"activate": "export HPCB_TAG=x"})
    assert st.status == "needs_confirmation" and "envelope" in (st.notice or "").lower(), st.notice
    assert app.env_spec is None and "env was not looked at" in (st.notice or ""), (app.env_spec, st.notice)


# ---- R2-S2 / R2-S7: a batch's env refusals, its yes and its one retry -------------------------------------------------

async def _batch_on_a_ready_env(tmp_path):
    """A batch of two rows on the session's built env, built and ready. Returns (app, runner, batch id,
    finish_builds)."""
    from hpc_bridge import envs
    from tests.test_batch_sizing import _app_with

    app, _rt, runner = _app_with(None)
    app.scratch_root = str(tmp_path / "scratch")

    def finish_builds(**extra):
        env = tbe._box(tmp_path, **extra)
        for fut, cmd in zip(runner.futures, runner.commands, strict=True):
            if envs.BUILD_TAG in cmd and not fut.done():
                p = tbe._bash(cmd, env, cwd=tmp_path)
                fut.finish(_Res(p.returncode, p.stdout, p.stderr))

    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    finish_builds()
    assert (await server._ensure_endpoint_up(app, "compute")).env.state == "ready"
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 0}, {"x": 1}], task_walltime_s=60)
    assert out.batch_id, out
    return app, runner, out.batch_id, finish_builds


def _refuse_row(runner, cmd: str, index: int, why: str, *, attempt: int = 1) -> None:
    """The row sent as `cmd` comes back refused by its env on its node (`why`), with its own attempt's nonce."""
    from hpc_bridge import envs
    from tests.test_batch_sizing import _report

    nonce = re.search(envs.REFUSED + r" ([0-9a-f]{16})", cmd).group(1)
    line = f"{envs.REFUSED} {nonce} {why} {_key()} {ARCH}\n"
    runner.futures[runner.commands.index(cmd)].finish(
        _report(index, attempt=attempt, rc=envs.ENV_RC, stderr_tail_b64=base64.b64encode(line.encode()).decode()))


async def test_r2_s2_the_yes_a_failed_mark_pause_asks_for_sends_the_build(tmp_path):
    """A batch paused by its rows' `.failed` refusals: the yes it asks for sent no build (the server still held the env
    as ready), and the rows bounced forever. A row's refusal is believed as run_shell's is (one path)."""
    from hpc_bridge import batch, envs

    app, runner, bid, _finish = await _batch_on_a_ready_env(tmp_path)
    env_dir = tmp_path / "scratch" / "envs" / f"{_key()}-{ARCH}"
    _purge(env_dir)                                                         # scratch purged…
    Path(str(env_dir) + ".failed").write_text("error: network unreachable\n")  # …and a rebuild failed there
    rows = [c for c in runner.commands if batch.RESULT_MARK in c]
    _refuse_row(runner, rows[0], 0, "failed")
    _refuse_row(runner, rows[1], 1, "failed")
    await batch.tick(app, bid)
    st = await server._status(app, bid)
    assert st.phase == "needs_confirmation" and "tries the build once more" in (st.notice or ""), st.notice
    builds = sum(envs.BUILD_TAG in c for c in runner.commands)
    await server._run_batch(app, batch_id=bid, confirm_spend=True)          # the user's yes, as the pause asks
    await batch.tick(app, bid)
    sent = sum(envs.BUILD_TAG in c for c in runner.commands) - builds
    resent_rows = len([c for c in runner.commands if batch.RESULT_MARK in c]) - len(rows)
    assert sent == 1, (f"the yes sent {sent} builds and re-sent {resent_rows} rows to the block, where the same mark "
                       "refuses them")


async def test_r2_s7_a_retry_that_worked_does_not_make_the_next_failure_final(tmp_path):
    """`retried` outlived the retry that worked: the next first failure was said final at once. Cleared once the env is
    ready again. (Adapted: four builds are sent — the first, the purge's rebuild, the yes's retry, the second purge's
    rebuild — where the reproduction counted three.)"""
    from hpc_bridge import batch, envs
    from tests.test_block_envs_r1 import _batch_with_a_purged_env

    app, runner, bid, finish_builds = await _batch_with_a_purged_env(tmp_path)
    finish_builds(HPCB_TEST_UV_FAIL="venv")                      # the rebuild fails, once
    await batch.tick(app, bid)
    await server._run_batch(app, batch_id=bid, confirm_spend=True)   # the user's yes: tried once more…
    finish_builds()                                              # …and it works
    await batch.tick(app, bid)
    again = [c for c in runner.commands if batch.RESULT_MARK in c][-1]
    assert "HPCB_ENV_REFUSED" in again                          # row 0 sent again, in the rebuilt env
    assert "retried" not in batch._env_rec(batch.ledger_of(app).batch(bid))   # the retry is spent
    _purge(tmp_path / "scratch" / "envs" / f"{_key()}-{ARCH}")   # a later purge…
    _refuse_row(runner, again, 0, "gone", attempt=2)
    await batch.tick(app, bid)
    finish_builds(HPCB_TEST_UV_FAIL="venv")                      # …whose rebuild fails for the first time
    await batch.tick(app, bid)
    st = await server._status(app, bid)
    rec = batch._env_rec(batch.ledger_of(app).batch(bid))
    assert not rec.get("unbuildable"), rec.get("unbuildable")
    assert "confirm_spend=True" in (st.notice or ""), st.notice  # the one retry this failure is owed
    assert sum(envs.BUILD_TAG in c for c in runner.commands) == 4


async def test_ss3_the_rows_sent_once_the_env_is_built_carry_the_arch_it_was_built_for(tmp_path):
    """`_admit_locked` wrote the record as it stands once the env was built (its path, its arch), then sent the rows
    from the dict it had read before: the activation never knew the arch (review N7's `gone` vs `arch`)."""
    from hpc_bridge import batch, envs
    from tests.test_batch_sizing import _app_with

    app, _rt, runner = _app_with(None)
    app.scratch_root = str(tmp_path / "scratch")
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 0}], task_walltime_s=60,
                                  env={"requirements": REQS})
    assert [c for c in runner.commands if batch.RESULT_MARK in c] == []            # nothing sent while it builds
    env = _box(tmp_path)
    for fut, cmd in zip(runner.futures, runner.commands, strict=True):
        if envs.BUILD_TAG in cmd and not fut.done():
            p = _bash(cmd, env, cwd=tmp_path)
            fut.finish(_Res(p.returncode, p.stdout, p.stderr))
    await batch.tick(app, out.batch_id)
    rows = [c for c in runner.commands if batch.RESULT_MARK in c]
    assert rows and f"__hb_eb={ARCH}" in rows[0], rows[:1]


# ---- R2-S3 / R2-S5 / R2-S6 / R2-S11: the pending env ------------------------------------------------------------------

async def test_r2_s3_a_pending_env_whose_build_is_lost_is_sent_again_when_the_agent_asks_as_told(tmp_path,
                                                                                                  monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=old"})
    app.runners.hold_builds = True                               # the build's task is never answered
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert st.env.state == "building" and app.env_pending is not None, st.env
    app.runners.hold_builds = False                              # a build sent from here on answers at once
    real = envs.time.monotonic

    class _Later:   # envs' own clock, three build walltimes on: the build is lost
        @staticmethod
        def monotonic() -> float:
            return real() + 3 * envs.BUILD_S

    monkeypatch.setattr(envs, "time", _Later)
    for _ in range(3):                                           # the agent asks, as the pending answer says to
        st = await server._ensure_endpoint_up(app, "compute")
    stuck = (len(_builds(app)), app.env_pending is not None, st.env.state if st.env else None)
    assert len(_builds(app)) == 2, f"(builds sent, still pending, state) = {stuck}: the lost build was never sent again"
    assert app.env_pending is None and st.env.state == "ready", stuck              # and it took over


async def test_r2_s3_a_pending_env_whose_future_a_dropped_runner_cancelled_is_sent_again(tmp_path):
    from hpc_bridge import warmth

    app = await _ssh(tmp_path)
    app.runners.hold_builds = True
    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    app.runners.held[0][1].cancel()                              # a new login dropped the runner: the future cancelled
    warmth._forget_identity_verdicts(app)
    app.runners.hold_builds = False
    st = await server._ensure_endpoint_up(app, "compute")
    assert len(_builds(app)) == 2 and st.env is not None and st.env.state == "ready", (len(_builds(app)), st.env)


async def test_r2_s5_a_failed_request_after_a_pending_one_leaves_the_sessions_env_as_it_was(tmp_path):
    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=old"})
    app.runners.hold_builds = True
    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS, "name": "first"})
    app.runners.hold_builds = False
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": "nosuchpkg==1.0", "name": "second"})
    assert "was not changed" in (st.notice or ""), st.notice     # the newest ask failed: the session's env stands
    assert "first" in (st.notice or "") and "dropped" in (st.notice or ""), st.notice   # and the answer says so
    app.runners.release()                                        # the superseded build answers later
    out = await server._run_shell(app, 'echo "T=${HPCB_TAG:-none}"', agent_call=True)
    assert out.phase == "complete" and "T=old" in out.stdout, (out.stdout, out.notice)


async def test_r2_s6_an_env_call_still_waiting_on_its_build_never_overrides_a_later_call(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    entered, go = asyncio.Event(), asyncio.Event()
    real_wait = envs._wait

    async def slow_wait(fut, wait_s):   # the build takes a few seconds to answer (well inside WAIT_S)
        entered.set()
        await go.wait()
        await real_wait(fut, wait_s)

    monkeypatch.setattr(envs, "_wait", slow_wait)
    first = asyncio.create_task(server._ensure_endpoint_up(app, "compute", env={"requirements": REQS}))
    await asyncio.wait_for(entered.wait(), 30)
    monkeypatch.setattr(envs, "_wait", real_wait)
    later = await server._ensure_endpoint_up(app, "compute", env={})
    assert "No env" in (later.notice or ""), later.notice
    go.set()
    first_st = await first
    out = await server._run_shell(app, 'echo "V=${VIRTUAL_ENV:-none}"', agent_call=True)
    assert app.env_spec is None and "V=none" in out.stdout, (app.env_spec, out.stdout)
    assert "not applied" in (first_st.notice or ""), first_st.notice


async def test_r2_s11_a_batch_started_after_the_pending_env_is_ready_runs_in_it(tmp_path):
    from hpc_bridge import batch

    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=old"})
    app.runners.hold_builds = True
    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS, "name": "new"})
    app.runners.release()                                        # the new env's build answers: it is ready
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 1}], task_walltime_s=60)
    assert out.batch_id, out
    rec = batch._env_rec(batch.ledger_of(app).batch(out.batch_id))
    assert rec is not None and rec.get("key") == _key(), (rec, app.env_spec)


async def test_r2_s11_a_job_submitted_after_the_pending_env_is_ready_runs_in_it(tmp_path):
    from hpc_bridge import envs
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _submit

    sched = FakeSched(tmp_path / "sched", user="u")
    tbe._install_uv(sched.bin)
    app = _app(tmp_path, sched)
    spec = envs.parse({"requirements": REQS})
    k = await envs.ensure_built(app, spec)                       # built: ready…
    app.env_pending = spec                                       # …and pending until something looks
    assert k.info.state == "ready" and app.env_spec is None
    out = await _submit(app, command="true")
    assert out.terms is not None and out.terms.env is not None and out.terms.env.key == _key(), out


# ---- R2-S4 / R2-S10 / R2-S8 / R2-S9: what the answers say --------------------------------------------------------------

class _FailingBuilder:
    """The build user endpoint's Executor as the SDK leaves it after the web service refused a submit: `how` is
    "future" (the submit returns a future that fails) or "raise" (the submit itself raises)."""

    def __init__(self, how: str) -> None:
        self.how, self.endpoint_id = how, "x"

    def submit(self, command):
        if self.how == "raise":
            raise RuntimeError("Executor is shutdown")
        fut = _PendingFuture()
        fut.task_id = "check-broken"
        fut.fail(RuntimeError("the web service refused the submit (HTTP 503)"))
        return fut

    def close(self):
        pass


@pytest.mark.parametrize("how", ["future", "raise"])
async def test_r2_s4_a_runner_failure_is_never_said_as_the_existing_envs_line_failing(tmp_path, how):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    real = app.runner_factory
    app.runner_factory = lambda eid, user_endpoint_config=None, **kw: (
        _FailingBuilder(how) if (user_endpoint_config or {}).get(envs.BUILD_UEC_KEY)
        else real(eid, user_endpoint_config=user_endpoint_config, **kw))
    st = await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=fine"})
    notice = st.notice or ""
    assert "did not work" not in notice, notice
    assert st.env is not None and st.env.state != "failed", st.env   # the line was never run, so never failed
    assert "could not be checked" in notice, notice


async def test_r2_s10_the_install_warning_is_said_once_not_spent_on_an_answer_that_never_says_it(tmp_path):
    app = await _ssh(tmp_path)
    first = await server._run_batch(app, command="echo {x}", rows=[{"x": 1}], task_walltime_s=100000,
                                    env={"requirements": REQS})
    assert first.phase == "refused" and "does not fit" in f"{first.reason} {first.notice}", first
    ok = await server._run_batch(app, command="echo {x}", rows=[{"x": 1}], task_walltime_s=60,
                                 env={"requirements": REQS})
    assert ok.batch_id, ok
    said = f"{first.reason or ''} {first.notice or ''} {ok.notice or ''}"
    assert "build code" in said, said
    later = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert "build code" not in (later.notice or "")                               # and then never again


@pytest.mark.parametrize("tool", ["ensure_endpoint_up", "run_batch", "submit_job", "export_handoff"])
async def test_r2_s8_the_call_journal_never_stores_an_index_urls_credentials(tmp_path, monkeypatch, tool):
    journal = tmp_path / "journal.jsonl"
    monkeypatch.setenv("HPC_BRIDGE_JOURNAL", str(journal))
    env = {"requirements": "--index-url https://alice:s3cr3t-pa55@pypi.example.org/simple\nsix==1.17.0\n"}
    args: dict = {"env": env}
    if tool == "export_handoff":
        args["user_endpoint_config"] = {"partition": "main"}
    try:
        await server.mcp.call_tool(tool, args)
    except Exception:  # noqa: BLE001 - no request context here; the point is what was recorded
        pass
    text = journal.read_text()
    assert tool in text, text[:300]
    assert "s3cr3t-pa55" not in text, text[:600]


def test_r2_s8_an_activate_lines_credentials_are_redacted_in_the_journal_too():
    args = server._journal_args("ensure_endpoint_up", {"env": {"activate": "pip config set https://u:tok@h/x"},
                                                       "shape": "compute"})
    assert "tok@" not in json.dumps(args) and args["shape"] == "compute"


async def test_r2_s9_a_restarted_server_collects_a_handoff_with_the_same_built_env(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    uec = {"partition": "main", "walltime": "00:10:00", "max_blocks": 1}
    env = {"requirements": REQS}
    ask = await server._export_handoff(app, user_endpoint_config=dict(uec), env=env)
    assert ask.phase == "needs_confirmation", ask
    app.runners.pending = True                                   # the canary's block is still queued
    v = await server._export_handoff(app, user_endpoint_config=dict(uec), confirm_spend=True, env=env)
    assert v.phase == "validating", v
    app.handoffs.clear()                                         # the server restarts (the canary is on disk)…
    app.env_known.clear()
    monkeypatch.setattr(envs, "default_python", lambda: "3.9")   # …under a client on another Python
    same = await server._export_handoff(app, user_endpoint_config=dict(uec), confirm_spend=True, env=env)
    assert same.phase != "refused", same.notice


# ---- SS2: a job its env refused ---------------------------------------------------------------------------------------

async def test_ss2_a_job_its_env_refused_says_so_and_its_resubmit_checks_the_env_first(tmp_path):
    from hpc_bridge import batch, envs
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _confirmed

    sched = FakeSched(tmp_path / "sched", user="u")
    tbe._install_uv(sched.bin)
    app = _app(tmp_path, sched)
    out = await _confirmed(app, command="echo RAN", env={"requirements": REQS})
    job = out.job
    rec = batch._env_rec(batch.ledger_of(app).job(job.job_id))
    assert rec.get("nonce_key"), rec                                               # the job's own secret, kept
    _purge(Path(job.env.path))                                                     # purged before the job ran
    p = subprocess.run([_BASH, job.script_path], capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home")})
    assert p.returncode == envs.ENV_RC and "RAN" not in p.stdout
    mark = next(Path(job.script_path).parent.glob("env_refused*"))
    assert envs.refusal_mark(mark.read_text(), envs.nonce_for(rec, job.job_id)).why == "gone"
    sched.set(job.native_id, state="FAILED", exit="88:0")
    st = await server._status(app, job.job_id)
    said = f"{st.job.reason or ''} {st.notice or ''}"
    assert "its env refused it" in said and "gone" in said and "ran nothing" in said, said
    builds = len([c for c in app.test_runner.commands if envs.BUILD_TAG in c])
    await _confirmed(app, command="echo RAN", env={"requirements": REQS})          # the resubmit
    assert len([c for c in app.test_runner.commands if envs.BUILD_TAG in c]) > builds   # checked (rebuilt) first


async def test_ss2_a_jobs_mark_without_its_nonce_is_never_believed(tmp_path):
    from hpc_bridge import batch, envs
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _confirmed

    sched = FakeSched(tmp_path / "sched", user="u")
    tbe._install_uv(sched.bin)
    app = _app(tmp_path, sched)
    out = await _confirmed(app, command="echo RAN", env={"requirements": REQS})
    job = out.job
    Path(job.script_path).parent.joinpath("env_refused").write_text(
        f"{envs.REFUSED} {'0' * 16} gone {_key()} {ARCH}\n")                       # written by the job's own command
    sched.set(job.native_id, state="FAILED", exit="88:0")
    st = await server._status(app, job.job_id)
    assert "its env refused it" not in f"{st.job.reason or ''} {st.notice or ''}", st.notice
    assert batch._env_rec(batch.ledger_of(app).job(job.job_id)) is not None


# ---- SS4 / SS5 / SS6 / SS7 ---------------------------------------------------------------------------------------------

async def test_ss4_a_refusal_for_an_env_the_session_has_left_says_so_and_rebuilds_nothing(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    e = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    monkeypatch.setattr(tbe, "_is_build", lambda c: envs.BUILD_TAG in c or envs.REFUSED in c)
    app.runners.hold_builds = True
    out = await server._run_shell(app, "echo SLOWMARK", agent_call=True)
    assert out.phase == "running", out
    app.runners.hold_builds = False
    monkeypatch.setattr(tbe, "_is_build", lambda c: envs.BUILD_TAG in c)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=other"})   # the session moves on
    _purge(e.path)
    builds = len(_builds(app))
    app.runners.release()
    got = await server._poll_task(app, out.task_id)
    assert got.phase == "failed" and "changed since" in (got.notice or ""), (got.phase, got.notice)
    assert len(_builds(app)) == builds                                             # nothing rebuilt for an env left


async def test_ss5_a_failure_of_an_older_runner_never_closes_the_newer_one(tmp_path):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    k = next(iter(app.env_known.values()))
    old, newer = object(), app.env_runner
    closed: list = []
    newer.close = lambda: closed.append(True)
    fut = _PendingFuture()
    fut.fail(RuntimeError("the old Executor shut down"))
    k.future, k.kind, k.sent_runner, k.sent_at = fut, "build", old, time.monotonic()
    envs.refresh(app)
    assert closed == [] and app.env_runner is newer


class _CancellingBuilder:
    """A build runner whose futures its own close cancels at once (a new login dropped it while the call waited)."""

    endpoint_id = "x"

    def submit(self, command):
        fut = _PendingFuture()
        fut.task_id = "cancelled"
        fut.cancel()
        return fut

    def close(self):
        pass


async def test_ss6_a_job_never_runs_against_an_env_left_unchecked(tmp_path):
    from hpc_bridge import envs
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _submit

    sched = FakeSched(tmp_path / "sched", user="u")
    app = _app(tmp_path, sched)
    real = app.runner_factory
    app.runner_factory = lambda eid, user_endpoint_config=None, **kw: (
        _CancellingBuilder() if (user_endpoint_config or {}).get(envs.BUILD_UEC_KEY)
        else real(eid, user_endpoint_config=user_endpoint_config, **kw))
    out = await _submit(app, command="true", env={"requirements": REQS}, confirm_spend=True)
    assert out.phase == "busy" and out.reason == "env_building", out
    assert not sched.submitted()


async def test_ss6_a_handoff_never_sends_its_canary_against_an_env_left_unchecked(tmp_path):
    from hpc_bridge import envs
    from tests.test_handoff_phase2b import _sent

    app = await _ssh(tmp_path)
    real = app.runner_factory
    app.runner_factory = lambda eid, user_endpoint_config=None, **kw: (
        _CancellingBuilder() if (user_endpoint_config or {}).get(envs.BUILD_UEC_KEY)
        else real(eid, user_endpoint_config=user_endpoint_config, **kw))
    n = len(_sent(app))
    out = await server._export_handoff(app, user_endpoint_config={"partition": "main", "walltime": "00:10:00",
                                                                  "max_blocks": 1},
                                       env={"requirements": REQS}, confirm_spend=True)
    assert out.phase == "env_building" and len(_sent(app)) == n, out


def test_ss7_the_canary_never_passes_an_env_every_command_refuses(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, _b, _p = _built(tmp_path)
    rec = {"kind": "built", "how": "login", "key": key, "root": str(root), "arch": ARCH}
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home"), "LANG": "C"}
    assert "HPCB_ENV_OK 1" in _bash(envs.canary_check(rec), env).stdout
    root.chmod(0o777)                                                               # not the user's alone
    try:
        assert "HPCB_ENV_OK 0" in _bash(envs.canary_check(rec), env).stdout
    finally:
        root.chmod(0o755)


# =====================================================================================================================
# The node (R2-N1..N9, NS2..NS9)
# =====================================================================================================================

def _wheel(where: Path, name: str, ver: str, *, extra: tuple[str, ...] = ()) -> Path:
    """A minimal pure-Python wheel; its RECORD written by the csv module, as every installer writes it."""
    where.mkdir(parents=True, exist_ok=True)
    path = where / f"{name}-{ver}-py3-none-any.whl"
    files = {f"{name}/__init__.py": f"__version__ = {ver!r}\n",
             f"{name}-{ver}.dist-info/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {ver}\n",
             f"{name}-{ver}.dist-info/WHEEL": "Wheel-Version: 1.0\nGenerator: review\nRoot-Is-Purelib: true\n"
                                              "Tag: py3-none-any\n"}
    for e in extra:
        files[f"{name}/{e}"] = "x = 1\n"
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    with zipfile.ZipFile(path, "w") as z:
        for p, c in files.items():
            z.writestr(p, c)
            digest = base64.urlsafe_b64encode(hashlib.sha256(c.encode()).digest()).rstrip(b"=").decode()
            w.writerow([p, f"sha256={digest}", len(c)])
        w.writerow([f"{name}-{ver}.dist-info/RECORD", "", ""])
        z.writestr(f"{name}-{ver}.dist-info/RECORD", buf.getvalue())
    return path


def _real_box(tmp: Path, **extra: str) -> dict[str, str]:
    """A login node's shell with the REAL uv, offline, and this Python's own interpreter directory on PATH."""
    home = tmp / "home"
    home.mkdir(exist_ok=True)
    pydir = os.path.dirname(os.path.realpath(sys.executable))
    return {"PATH": f"{os.path.dirname(str(_UV_REAL))}:{pydir}:/usr/bin:/bin", "HOME": str(home), "LANG": "C",
            "XDG_CONFIG_HOME": str(home / ".config"), "UV_OFFLINE": "1", "UV_PYTHON_DOWNLOADS": "never", **extra}


def _real_build(tmp: Path, reqs: str, root: Path, **extra: str):
    from hpc_bridge import envs

    spec = envs.parse({"requirements": reqs})
    key = envs.env_key(spec)
    p = _bash(envs.build_script(spec, key=key, root=str(root), python=PY, token="t0"), _real_box(tmp, **extra),
              cwd=tmp)
    return key, envs.parse_build(p.stdout), p


# ---- R2-N9: errexit inside the user's line, on bash 4+ ------------------------------------------------------------------

def _sourced(tmp: Path) -> str:
    setup = tmp / "setup.sh"
    setup.write_text("set -e\nfalse\nexport HALF=1\n")
    return f". {setup}"


@pytest.mark.parametrize("form", ["inline", "sourced"])
def test_r2n9_a_lines_own_errexit_stops_it_and_is_a_refusal_in_the_session(tmp_path, form):
    """bash 4+ ignores errexit for everything inside an `if` condition: `if eval "set -e; false; export HALF=1"` ran on
    half-activated (rc 0) on the nodes' bash 5, where bash 3.2 refused it. The line runs outside any condition now."""
    from hpc_bridge import envs

    line = "set -e; false; export HALF=1" if form == "inline" else _sourced(tmp_path)
    act = envs.Activation(how="activate", key="f" * 16, line=line)
    p, _sd = _wrap_run(tmp_path, 'echo "RAN HALF=${HALF:-none}"', act)
    assert "RAN" not in p.stdout, p.stdout
    r = envs.refusal(p.returncode, p.stderr, NONCE)
    assert r is not None and r.why == "activate", (p.returncode, p.stderr[-300:])


@pytest.mark.parametrize("form", ["inline", "sourced"])
def test_r2n9_in_a_batch_row(tmp_path, form):
    from hpc_bridge import batch, envs
    from tests.test_batch_sizing import _result, _spawn, _wrapper_env

    line = "set -e; false; export HALF=1" if form == "inline" else _sourced(tmp_path)
    act = envs.Activation(how="activate", key="f" * 16, line=line)
    rep = _result(_spawn(batch.task_script("echo RAN", results_dir=str(tmp_path / "rd"), index=0, attempt=1,
                                           task_walltime_s=60, env_sh=envs.activation_sh(act, NONCE)),
                         _wrapper_env(tmp_path)))
    assert "RAN" not in rep.tail and rep.rc == envs.ENV_RC and rep.ran is False, (rep.rc, rep.ran, rep.stderr_tail)


@pytest.mark.parametrize("form", ["inline", "sourced"])
def test_r2n9_in_a_job_script(tmp_path, form):
    from hpc_bridge import envs

    line = "set -e; false; export HALF=1" if form == "inline" else _sourced(tmp_path)
    act = envs.Activation(how="activate", key="e" * 16, line=line)
    script = "#!/bin/bash\n" + envs.activation_sh(act, NONCE) + '( eval "echo RAN" )\n'
    p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, timeout=30)
    assert p.returncode == envs.ENV_RC and "RAN" not in p.stdout, (p.returncode, p.stdout, p.stderr[-300:])


# ---- R2-N1: a csv-quoted RECORD entry ---------------------------------------------------------------------------------

def test_r2n1_a_record_entry_quoted_for_a_comma_makes_a_whole_env_partial(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, b, p = _built(tmp_path)
    assert b is not None and b.ok, p.stderr[-300:]
    d = root / f"{key}-{ARCH}"
    _writable(d)
    sp = next(d.glob("lib/python*/site-packages"))
    (sp / "six" / "textwrap.py,cover").write_text("x = 1\n")                    # wcwidth 0.8.3's own layout
    rec = next(sp.glob("six-*.dist-info/RECORD"))
    rec.write_text(rec.read_text() + '"six/textwrap.py,cover",sha256=nia_NpkRxFwkPGhBR7I_yeHc_PJX0pmhxjIBam_NM_Q,6\n')
    assert (sp / "six" / "textwrap.py,cover").is_file()                         # every file the RECORD lists is there
    check = envs.parse_check(_bash(envs.check_script(key=key, root=str(root)), _box(tmp_path)).stdout)
    assert check is not None and check.state == "ready", check
    (sp / "six" / "textwrap.py,cover").unlink()                                 # and one that is not there is missed
    check = envs.parse_check(_bash(envs.check_script(key=key, root=str(root)), _box(tmp_path)).stdout)
    assert check is not None and check.state == "partial" and check.missing == 1, check


@needs_uv
def test_r2n1_with_the_real_uv_a_package_whose_record_quotes_a_path_builds(tmp_path):
    wh = tmp_path / "wheelhouse"
    _wheel(wh, "commapkg", "1.0", extra=("textwrap.py,cover",))
    _key2, b, p = _real_build(tmp_path, "commapkg==1.0", tmp_path / "scratch" / "envs", UV_FIND_LINKS=str(wh))
    assert b is not None and b.ok, (b, p.stderr[-400:])


# ---- R2-N2: a local path that still passed `parse` ----------------------------------------------------------------------

@pytest.mark.parametrize("line", [
    "--no-index --find-links /home/u/wheels",          # the first option was checked; the second never looked at
    "--no-index -f /home/u/wheels",
    "--no-index -r /home/u/more-requirements.txt",     # another file, which the key cannot see either
    "planted-1.0-py3-none-any.whl",                    # uv reads a bare archive name as a path, under its cwd
    "planted-1.0.tar.gz",
    "--no-index stray-word",
])
def test_r2n2_a_local_path_still_passes_the_requirements_check(line):
    from hpc_bridge import envs

    with pytest.raises(envs.EnvRefusal):
        envs.parse({"requirements": f"six==1.17.0\n{line}\n"})


def test_r2n2_every_option_on_a_line_that_is_fine_is_still_accepted():
    from hpc_bridge import envs

    spec = envs.parse({"requirements": "--no-index --find-links https://example.org/w --only-binary :all:\nsix"})
    assert spec.kind == "requirements" and envs.index_lines(spec.requirements)[0].startswith("--no-index")


@needs_uv
@pytest.mark.parametrize("how", ["no-index-then-find-links", "bare-archive-name"])
def test_r2n2_with_the_real_uv_such_a_line_never_installs_from_the_login_nodes_disk(tmp_path, how):
    from hpc_bridge import envs

    wh = tmp_path / "wheelhouse"
    whl = _wheel(wh, "planted", "1.0")
    root = tmp_path / "scratch" / "envs"
    if how == "no-index-then-find-links":
        reqs = f"--no-index --find-links {wh}\nplanted==1.0\n"
    else:
        root.mkdir(parents=True)
        shutil.copy(whl, root / whl.name)
        reqs = f"{whl.name}\n"
    try:
        envs.parse({"requirements": reqs})
    except envs.EnvRefusal:
        return                                       # refused by name, as N9 ruled
    key, b, _p = _real_build(tmp_path, reqs, root)
    got = sorted(x.name for x in (root / f"{key}-{ARCH}").glob("lib/python*/site-packages/planted*"))
    assert not (b is not None and b.ok and got), f"built from the login node's disk: {got} ({b})"


# ---- R2-N3: the requirements text, credentials and all, the user's alone ------------------------------------------------

@pytest.mark.parametrize("node_umask", ["077", "022", "002"])
def test_r2n3_index_credentials_in_the_text_are_never_left_readable_by_other_users(tmp_path, node_umask):
    from hpc_bridge import envs

    spec = envs.parse({"requirements": "--index-url https://me:s3cr3t-t0k@private.example.org/simple\nsix==1.17.0\n"})
    key = envs.env_key(spec)
    root = tmp_path / "scratch" / "envs"
    p = _bash(f"umask {node_umask}; " + envs.build_script(spec, key=key, root=str(root), python=PY, token="t0"),
              _box(tmp_path), cwd=tmp_path)
    b = envs.parse_build(p.stdout)
    assert b is not None and b.ok, (p.stdout, p.stderr[-300:])
    d = root / f"{key}-{ARCH}"
    req = d / "requirements.in"
    assert "s3cr3t-t0k" in req.read_text()
    assert req.stat().st_mode & 0o077 == 0, oct(req.stat().st_mode)
    assert (d / "requirements.lock").stat().st_mode & 0o077 == 0
    assert root.stat().st_mode & 0o022 == 0                                         # never group- or world-writable
    if node_umask == "077":
        assert d.stat().st_mode & 0o077 == 0, oct(d.stat().st_mode)                 # the node's 077 never loosened


# ---- R2-N4 / NS5: where the build's packages come from, all of it named --------------------------------------------------

def test_r2n4_uv_find_links_set_on_the_login_node_is_named_with_the_answer(tmp_path):
    _spec, _key2, _root, b, p = _built(tmp_path, UV_FIND_LINKS="/apps/site-wheelhouse",
                                       UV_INDEX_URL="https://mirror.example.org/simple",
                                       PIP_REQUIRE_VIRTUALENV="1")
    assert b is not None and b.ok, p.stderr[-300:]
    said = " ".join(b.ambient or [])
    assert "UV_INDEX_URL" in said and "UV_FIND_LINKS" in said and "PIP_REQUIRE_VIRTUALENV" in said, said
    assert "site-wheelhouse" not in said and "mirror.example.org" not in said           # names only, never values


def test_ns5_the_uv_config_files_that_apply_are_named(tmp_path):
    cfg = tmp_path / "site-uv.toml"
    cfg.write_text("native-tls = true\n")
    (tmp_path / "home").mkdir(exist_ok=True)
    (tmp_path / "home" / "pyproject.toml").write_text("[tool.uv]\nnative-tls = true\n")   # above uv's own cwd
    (tmp_path / "home" / ".config" / "uv").mkdir(parents=True)
    (tmp_path / "home" / ".config" / "uv" / "uv.toml").write_text("native-tls = true\n")
    _spec, _key2, _root, b, p = _built(tmp_path, UV_CONFIG_FILE=str(cfg))
    assert b is not None and b.ok, p.stderr[-300:]
    said = " ".join(b.ambient or [])
    for f in (cfg, tmp_path / "home" / "pyproject.toml", tmp_path / "home" / ".config" / "uv" / "uv.toml"):
        assert str(f) in said, (f, said)


def test_r2n4_uv_never_runs_in_the_env_root_nor_under_it(tmp_path):
    """uv reads a project's uv.toml from its cwd and every parent: it runs from a fresh, empty directory of the user's
    own (the reproduction with the real uv is the next test)."""
    from hpc_bridge import envs

    shim = tmp_path / "shim"
    shim.mkdir()
    (shim / "uv").write_text(f'#!/bin/sh\npwd >> {tmp_path / "uv-cwd.log"}\nexec {tmp_path / "uvbin" / "uv"} "$@"\n')
    (shim / "uv").chmod(0o755)
    spec = envs.parse({"requirements": REQS})
    root = tmp_path / "proj" / "envs"
    env = _box(tmp_path)
    env["PATH"] = f"{shim}:{env['PATH']}"
    p = _bash(envs.build_script(spec, key=envs.env_key(spec), root=str(root), python=PY, token="t0"), env,
              cwd=tmp_path)
    assert envs.parse_build(p.stdout).ok, p.stderr[-300:]
    cwds = (tmp_path / "uv-cwd.log").read_text().split()
    assert cwds and all(".hpcb-uv." in c and not c.startswith(str(tmp_path / "proj")) for c in cwds), cwds
    assert not any(".hpcb-uv." in x.name for x in (tmp_path / "home").iterdir())       # removed after


@needs_uv
def test_r2n4_a_uv_toml_above_the_env_root_never_changes_the_build_unsaid(tmp_path):
    wh = tmp_path / "wheelhouse"
    _wheel(wh, "planted", "1.0")
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "uv.toml").write_text(f'find-links = ["{wh}"]\n')
    key, b, _p = _real_build(tmp_path, "planted==1.0", proj / "envs")
    said = " ".join((b.ambient or []) if b else [])
    got = sorted(x.name for x in (proj / "envs" / f"{key}-{ARCH}").glob("lib/python*/site-packages/planted*"))
    assert not (b is not None and b.ok and got) or str(proj / "uv.toml") in said, (got, b)


# ---- R2-N5 / NS8: the shell state the wrapper's own loops depend on --------------------------------------------------------

def test_r2n5_allexport_left_on_by_an_activate_line_never_persists_the_wrappers_own_variables(tmp_path):
    from hpc_bridge import envs

    act = envs.Activation(how="activate", key="c" * 16, line="set -a; HPCB_PROJ=demo")
    for _ in range(3):
        p, sd = _wrap_run(tmp_path, "true", act)
        assert p.returncode == 0, p.stderr[-300:]
    saved = (sd / ".env").read_text()
    assert "__hb_" not in saved, f"{saved.count(chr(10))} lines saved, e.g. {saved[:300]!r}"
    p, _sd = _wrap_run(tmp_path, 'env | grep -c "^__hb_" || true', None)
    assert p.stdout.strip() == "0", p.stdout


def test_r2n5_an_activate_line_that_leaves_ifs_changed_never_wipes_the_sessions_variables(tmp_path):
    from hpc_bridge import envs

    _wrap_run(tmp_path, "export MINE=1", None)
    act = envs.Activation(how="activate", key="c" * 16, line="IFS=:; export HPCB_TAG=x")
    p, _sd = _wrap_run(tmp_path, 'echo "IN MINE=${MINE:-unset}"', act)
    assert "IN MINE=1" in p.stdout, (p.stdout, p.stderr[-300:])
    p, sd = _wrap_run(tmp_path, 'echo "M=${MINE:-unset}"', None)
    saved = (sd / ".env").read_text() if (sd / ".env").exists() else ""
    assert "M=1" in p.stdout, (p.stdout, saved[:300])


def test_r2n5_xtrace_left_on_by_an_activate_line_never_dumps_every_exported_value(tmp_path):
    from hpc_bridge import envs

    _wrap_run(tmp_path, "export HPCB_REVIEW_SECRET=hunter2-value", None)
    act = envs.Activation(how="activate", key="c" * 16, line="set -x; export HPCB_TAG=x")
    p, _sd = _wrap_run(tmp_path, "echo done", act)
    assert "done" in p.stdout and "HPCB_TAG=x" in p.stderr, (p.stdout, p.stderr[-300:])   # the user's own trace
    assert "hunter2-value" not in p.stderr, p.stderr[-600:]


def test_ns8_a_commands_own_xtrace_never_dumps_the_values_the_wrapper_persists(tmp_path):
    _wrap_run(tmp_path, "export HPCB_REVIEW_SECRET=hunter2-value", None)
    p, _sd = _wrap_run(tmp_path, "export MORE=1; set -x", None)
    assert "hunter2-value" not in p.stderr, p.stderr[-600:]


def test_ns8_a_commands_own_ifs_and_nocasematch_never_break_what_is_persisted(tmp_path):
    p, sd = _wrap_run(tmp_path, "export MINE=1; export hostname=mine; shopt -s nocasematch; IFS=:", None)
    assert p.returncode == 0, p.stderr[-300:]
    saved = (sd / ".env").read_text()
    assert "MINE=1" in saved and "hostname=mine" in saved, saved                    # not "HOSTNAME" (volatile)


def test_ns6_nothing_of_the_activation_is_left_for_the_command(tmp_path):
    from hpc_bridge import envs

    act = envs.Activation(how="activate", key="c" * 16, line="export HPCB_TAG=x")
    p, _sd = _wrap_run(tmp_path, 'declare -F __hb_erefuse >/dev/null && echo FN; echo "EL=${__hb_el-unset} '
                                 'EK=${__hb_ek-unset} ET=${__hb_etrap-unset}"', act)
    assert "FN" not in p.stdout and "EL=unset EK=unset ET=unset" in p.stdout, p.stdout


def test_ns7_a_lines_own_exit_trap_is_kept(tmp_path):
    from hpc_bridge import envs

    act = envs.Activation(how="activate", key="c" * 16, line="trap 'echo LINE_TRAP >&2' EXIT; export HPCB_TAG=x")
    p, _sd = _wrap_run(tmp_path, 'echo "RAN T=$HPCB_TAG"', act)
    assert "RAN T=x" in p.stdout and "LINE_TRAP" in p.stderr, (p.stdout, p.stderr[-300:])


# ---- R2-N6: the kept rc of a command refused by the EXIT trap ----------------------------------------------------------------

@pytest.mark.parametrize("line", ["exit 0", "set -u; : $HPCB_REVIEW_NOT_SET", "set -e; false; export HALF=1"])
def test_r2n6_a_refusal_by_the_exit_trap_still_leaves_the_commands_kept_rc(tmp_path, line):
    from hpc_bridge import envs, session_shell

    (tmp_path / "home").mkdir()
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home"), "LANG": "C"}

    def run(act_line: str, key: str):
        s = session_shell.Session("default", str(tmp_path / "scratch"))
        act = envs.Activation(how="activate", key="f" * 16, line=act_line)
        script = session_shell.wrap("echo RAN", s, result_key=key, env_sh=envs.activation_sh(act, NONCE))
        p = _bash(script, env, cwd=tmp_path)
        return p, Path(s.state_dir) / "results" / key / "rc"

    p, rc = run("false", "control")
    assert envs.refusal(p.returncode, p.stderr, NONCE) is not None and rc.read_text().strip() == "88"
    p, rc = run(line, "trapped")
    assert envs.refusal(p.returncode, p.stderr, NONCE) is not None, p.stderr[-300:]
    assert rc.is_file() and rc.read_text().strip() == str(envs.ENV_RC), (rc.is_file(), p.stderr[-300:])


# ---- R2-N7: the up-front checks and the per-command rule agree ----------------------------------------------------------

@pytest.mark.parametrize("line", ["export HPCB_TAG=x; exit 0", "set -e; false; export HALF=1"])
def test_r2n7_the_login_nodes_check_never_passes_a_line_every_command_refuses(tmp_path, line):
    from hpc_bridge import envs

    p, _sd = _wrap_run(tmp_path, "echo RAN", envs.Activation(how="activate", key="e" * 16, line=line))
    assert "RAN" not in p.stdout and envs.refusal(p.returncode, p.stderr, NONCE) is not None   # every command
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home"), "LANG": "C"}
    q = _bash(envs.activate_check_script(line), env)
    m = re.search(r"HPCB_ENV_ACTIVATE rc=(\d+)", q.stdout)
    assert m is not None and m.group(1) != "0", q.stdout
    c = _bash(envs.canary_check({"kind": "activate", "key": "e" * 16, "line": line}), env)
    assert "HPCB_ENV_OK 1" not in c.stdout, c.stdout


def test_r2n7_a_line_that_works_passes_the_check_and_says_what_it_printed(tmp_path):
    from hpc_bridge import envs

    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home"), "LANG": "C"}
    q = _bash(envs.activate_check_script("echo loaded >&2; export HPCB_TAG=x"), env)
    m = re.search(r"HPCB_ENV_ACTIVATE rc=(\d+) out_b64=(\S*)", q.stdout)
    assert m is not None and m.group(1) == "0" and base64.b64decode(m.group(2)).decode().strip() == "loaded", q.stdout


# ---- R2-N8: of two builds at once, the later one never displaces the earlier's READY env -----------------------------------

def test_r2n8_a_second_build_never_moves_aside_the_ready_env_the_first_just_claimed(tmp_path):
    """(Adapted: the reproduction relied on a node with no `flock`; on Linux flock exists. A `flock` that fails —
    a node without it, or its wait ran out — makes it deterministic on both.)"""
    from hpc_bridge import envs

    spec = envs.parse({"requirements": REQS})
    key = envs.env_key(spec)
    root = tmp_path / "envs"
    tbe._tool(tmp_path / "noflock", "flock", "exit 1")
    procs = []
    for tok, slow in (("t1", "0.2"), ("t2", "1.5")):          # both see no env at their start; t1 finishes first
        env = _box(tmp_path, HPCB_TEST_UV_SLOW=slow)
        env["PATH"] = f"{tmp_path / 'noflock'}:{env['PATH']}"
        procs.append(subprocess.Popen([_BASH, "-c", envs.build_script(spec, key=key, root=str(root), python=PY,
                                                                       token=tok)],
                                      env=env, cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        time.sleep(0.05)
    first = envs.parse_build(procs[0].communicate(timeout=60)[0])
    d = root / f"{key}-{ARCH}"
    assert first is not None and first.ok and first.path == str(d)
    claimed = d.stat().st_ino                                  # the env rows may start in from here on
    second = envs.parse_build(procs[1].communicate(timeout=60)[0])
    assert second is not None and second.ok and second.cached, second
    assert d.stat().st_ino == claimed and _beside(root) == [d.name], _beside(root)


# ---- NS2/NS3 / NS4 / NS9 -------------------------------------------------------------------------------------------------

def test_ns2_an_unsafe_refusal_names_the_owner_and_mode_it_saw_and_what_can_cause_it(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, _b, _p = _built(tmp_path)
    root.chmod(0o775)
    try:
        p, _sd = _wrap_run(tmp_path, "echo RAN", envs.Activation(how="built", key=key, root=str(root)))
        r = envs.refusal(p.returncode, p.stderr, NONCE)
        assert r is not None and r.why == "unsafe" and str(root) in r.detail, r
        assert f"owner uid {os.getuid()}" in r.detail and "mode drwxrwxr-x" in r.detail, r.detail
        _phase, notice = envs.refused(_Bare(), r, envs.Activation(how="built", key=key, root=str(root)), p.stderr)
        assert "default ACL" in notice and "id-mapping" in notice and "own alone" in notice, notice
        again = envs.parse_build(_bash(envs.build_script(_spec, key=key, root=str(root), python=PY, token="t1"),
                                       _box(tmp_path), cwd=tmp_path).stdout)
        assert not again.ok and "owner uid" in again.error and "default ACL" in again.error, again
    finally:
        root.chmod(0o755)


class _Bare:
    """The least of an AppCtx `refused()` reads."""

    def __init__(self) -> None:
        self.env_known: dict = {}
        self.env_spec = None


def test_ns4_an_env_absent_for_an_instant_is_looked_at_again_before_it_is_refused(tmp_path, monkeypatch):
    from hpc_bridge import envs, session_shell

    monkeypatch.setattr(envs, "GONE_RECHECK_S", 1.5)
    _spec, key, root, _b, _p = _built(tmp_path)
    d = root / f"{key}-{ARCH}"
    aside = root / "aside"
    d.rename(aside)                                                                 # a rebuild's swap, mid-way
    act = envs.Activation(how="built", key=key, root=str(root), arch=ARCH)
    s = session_shell.Session("default", str(tmp_path / "scratch"))
    (tmp_path / "home").mkdir(exist_ok=True)
    proc = subprocess.Popen([_BASH, "-c", session_shell.wrap("echo RAN", s, env_sh=envs.activation_sh(act, NONCE))],
                            env={"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home"), "LANG": "C"},
                            cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(0.4)
    aside.rename(d)                                                                 # …and done
    out, err = proc.communicate(timeout=30)
    assert proc.returncode == 0 and "RAN" in out, (proc.returncode, err[-300:])
    monkeypatch.setattr(envs, "GONE_RECHECK_S", 0.05)
    _purge(d)                                                                       # really gone: refused, after it
    p, _sd = _wrap_run(tmp_path, "echo RAN", act)
    assert envs.refusal(p.returncode, p.stderr, NONCE).why == "gone"


def test_ns9_uv_never_holds_the_builds_lock(tmp_path):
    """uv (and the installer that fetches it) runs with the lock's descriptor closed: a child left running never holds
    the lock. (A `flock` that takes it, on both platforms.)"""
    from hpc_bridge import envs

    log = tmp_path / "fd9.log"
    shim = tmp_path / "shim"
    shim.mkdir()
    (shim / "uv").write_text(f'#!/bin/bash\nif {{ true >&9; }} 2>/dev/null; then echo OPEN; else echo CLOSED; fi '
                             f'>> {log}\nexec {tmp_path / "uvbin" / "uv"} "$@"\n')
    (shim / "uv").chmod(0o755)
    tbe._tool(shim, "flock", "exit 0")
    spec = envs.parse({"requirements": REQS})
    env = _box(tmp_path)
    env["PATH"] = f"{shim}:{env['PATH']}"
    p = _bash(envs.build_script(spec, key=envs.env_key(spec), root=str(tmp_path / "envs"), python=PY, token="t0"), env,
              cwd=tmp_path)
    assert envs.parse_build(p.stdout).ok, p.stderr[-300:]
    seen = log.read_text().split()
    assert seen and set(seen) == {"CLOSED"}, seen
