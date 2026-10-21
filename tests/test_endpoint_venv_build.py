"""The endpoint venv's build (`facility.endpoint_venv.ensure`), the teardown's search (`find_working`), and their place
in the bring-up and the teardown.

There is no lock: a build happens in a directory of its own (`<keyed>.build.<token>`), is verified there, and claims the
keyed path with one atomic rename; of two bring-ups building at once, the one whose rename loses discards its build and
uses the winner's. Two tiers: a FAKE login node (a small model answering each named step) covers every branch of the
Python side — build fails, verify fails or times out, the claim lost, an ssh blip, a cancel mid-build, ssh itself
failing — and a REAL shell runs the same scripts against a temp home with the stub tools (`venv_stubs.py`), the way
they run on a login node (`bash -lc`), so the builds, the renames and the cleanups are exercised as written.
"""
from __future__ import annotations

import asyncio
import errno
import os
import re
import shlex
import signal
import subprocess
import time
from pathlib import Path

import pytest

from hpc_bridge.facility import endpoint_venv as ev
from hpc_bridge.facility.base import ConfigStepFailed
from hpc_bridge.facility.endpoint_venv import EndpointVenvError
from tests import venv_stubs as vs

PIN, PY, SDK = "4.18.0", vs.PY, "4.9.1"
HOST = "login.example.edu"


def _mv(base: str = "/home/u/hpc-bridge/gce-venv", prefix: str = "") -> ev.ManagedVenv:
    mv = ev.managed_venv(prefix + ev.short_env_setup(base, python=PY, gce=PIN))
    assert mv is not None
    return mv


# ---- a fake login node ----------------------------------------------------------------------------------------------

_STATES = {  # (version rc, version line, python line)
    "ok": (0, f"Globus Compute endpoint version: {PIN}", f"({PY.replace('.', ', ')})"),
    "stale": (0, "Globus Compute endpoint version: 4.13.0", f"({PY.replace('.', ', ')})"),
    "otherpy": (0, f"Globus Compute endpoint version: {PIN}", "(3, 9)"),
    "broken": (1, "ImportError: cannot import name 'X' from 'globus_sdk'", f"({PY.replace('.', ', ')})"),
    "slow": (124, "", ""),
}
# what the check's python reports about texttable (`ev._PY_PROBE`); a state not named here reports nothing about it
_TEXTTABLE = {
    "ok": ["TEXTTABLE=ok", "TEXTTABLE_VERSION=1.7.1", "TEXTTABLE_ERROR="],
    "oldtable": ["TEXTTABLE=broken", "TEXTTABLE_VERSION=1.7.2",
                 "TEXTTABLE_ERROR=AttributeError: 'Texttable' object has no attribute '_width'"],
    "notable": ["TEXTTABLE=absent", "TEXTTABLE_VERSION=", "TEXTTABLE_ERROR=ModuleNotFoundError: No module named"],
    # a 1.7.1 venv whose python imports 1.7.2 from a site module's PYTHONPATH
    "shadowed": ["TEXTTABLE=broken", "TEXTTABLE_VERSION=1.7.2",
                 "TEXTTABLE_ERROR=AttributeError: 'Texttable' object has no attribute '_width'",
                 "TEXTTABLE_FILE=/apps/python/3.13/lib/site-packages/texttable.py", "TEXTTABLE_OUTSIDE=1"],
}
_STATES["oldtable"] = _STATES["notable"] = _STATES["shadowed"] = _STATES["ok"]  # only texttable differs


class Remote:
    """What the steps see — the keyed venv, the unkeyed one, the build directory, the tools — and what they did."""

    def __init__(self, venv: str = "missing", *, unkeyed: str = "missing", tools: str = "uv", build_rc: int = 0,
                 build_err: str = "", built: str = "ok", claim: str = "ok", winner: str = "ok",
                 ssh_fail: str | None = None, arch: str | None = None, hang_build: bool = False) -> None:
        self.venv, self.unkeyed, self.build = venv, unkeyed, "missing"  # missing|ok|broken|stale|otherpy|notvenv|...
        self.tools = tools              # uv | curl | python3 | none
        self.build_rc, self.build_err, self.built = build_rc, build_err, built
        self.claim, self.winner = claim, winner  # the rename: ok | lost (another bring-up put `winner` there first)
        self.ssh_fail, self.hang_build = ssh_fail, hang_build
        self.arch = arch if arch is not None else ("x86_64" if venv == "ok" else None)
        self.steps: list[str] = []
        self.scripts: list[tuple[str, str]] = []

    async def __call__(self, step: str, script: str, timeout: float) -> tuple[int, str, str]:
        self.steps.append(step)
        self.scripts.append((step, script))
        if self.ssh_fail == step:
            return 255, "", "u@login: Permission denied (publickey,keyboard-interactive)."
        if step in ("check", "re-check"):
            return 0, self._check(self.venv, self.arch, inode="4242"), ""
        if step == "check unkeyed":
            return 0, self._check(self.unkeyed, "x86_64"), ""
        if step == "verify":
            return 0, self._check(self.build, "x86_64"), ""
        if step == "build":
            if self.hang_build:
                await asyncio.sleep(3600)
            self.build = self.built if self.build_rc == 0 else "broken"
            return self.build_rc, "", self.build_err
        if step == "claim":
            if self.claim == "error":
                return 0, "HPCB_CLAIM=error Permission denied\n", ""
            if self.claim == "lost":
                self.venv, self.arch = self.winner, "x86_64"
                return 0, "HPCB_CLAIM=lost Directory not empty\n", ""
            moved = (f"HPCB_MOVED={_mv().key}.broken.20261013-120000\n"
                     if self.venv != "missing" and "4242" in script else "")
            self.venv, self.build, self.arch = self.build, "missing", "x86_64"
            return 0, moved + "HPCB_CLAIM=ok\n", ""
        if step == "marker":
            self.arch = "x86_64"
            return 0, "", ""
        if step == "cleanup":
            out = ""
            if "HPCB_FAILED" in script and self.build != "missing":
                out = f"HPCB_FAILED={_mv().key}.failed.20261013-120000\n"
            if 'rm -rf "$b"' in script or "HPCB_FAILED" in script:
                self.build = "missing"
            return 0, out, ""
        raise AssertionError(step)

    def _check(self, state: str, arch: str | None, inode: str = "77") -> str:
        lines = ["HPCB_VENV_BEGIN"]
        if state != "missing":
            lines += ["EXISTS=1", f"INODE={inode}"]
            if state not in ("notvenv", "empty"):
                lines.append("PYVENV=1")
            if state == "empty":
                lines.append("EMPTY=1")
        if state in _STATES:
            rc, ver, py = _STATES[state]
            lines += ["ENTRY=1", f"VERSION_RC={rc}", f"VERSION={ver}", f"PY_RC={rc if rc == 124 else 0}", f"PY={py}"]
            lines += _TEXTTABLE.get(state, [])
        if arch and state != "missing":
            lines.append(f"ARCH={arch}")
        lines.append("MACHINE=x86_64")
        lines.append("UV=/usr/bin/uv" if self.tools in ("uv", "olduv") else "UV=")
        if self.tools in ("uv", "olduv"):
            lines.append("UV_VERSION=uv " + ("0.2.0" if self.tools == "olduv" else "0.12.9"))
            lines += ["UV_RELOC=1"] if self.tools == "uv" else []
        lines.append("PY3=3.6" if self.tools in ("uv", "python3") else "PY3=")
        lines.append("CURL=/usr/bin/curl" if self.tools in ("uv", "curl") else "CURL=")
        return "banner from the site\n" + "\n".join(lines + ["HPCB_VENV_END"]) + "\n"

    def script(self, step: str) -> str:
        return [s for st, s in self.scripts if st == step][-1]


async def _ensure(remote: Remote, mv: ev.ManagedVenv | None = None) -> ev.EnsureResult:
    return await ev.ensure(remote, mv or _mv(), host=HOST, gce=PIN, python=PY, sdk=SDK)


# ---- the check -------------------------------------------------------------------------------------------------------

async def test_a_working_venv_costs_one_check():
    r = Remote("ok")
    res = await _ensure(r)
    assert r.steps == ["check"] and not res.built and res.path == _mv().key


async def test_a_working_venv_without_its_marker_gets_one():
    r = Remote("ok", arch="")  # no .hpcb-arch: a venv an older build made
    await _ensure(r)
    assert r.steps == ["check", "marker"] and r.arch == "x86_64"


async def test_an_existing_marker_is_left_as_it_is():
    r = Remote("ok", arch="aarch64")  # another architecture than this login node's: still left alone
    await _ensure(r)
    assert r.steps == ["check"]


async def test_a_check_that_times_out_is_reported_not_rebuilt():
    r = Remote("slow")
    with pytest.raises(EndpointVenvError, match="timed out"):
        await _ensure(r)
    assert r.steps == ["check"]  # a slow venv is not a broken one


async def test_a_missing_keyed_venv_uses_a_prepared_unkeyed_one_that_passes():
    # the line falls back to {venv} when the keyed venv is missing: a venv the user prepared there (a login node with no
    # internet) is used as it is
    r = Remote("missing", unkeyed="ok")
    res = await _ensure(r)
    assert r.steps == ["check", "check unkeyed"] and res.path == _mv().base and not res.built


@pytest.mark.parametrize("unkeyed", ["missing", "stale", "otherpy", "broken"])
async def test_an_unkeyed_venv_that_does_not_pass_is_not_used(unkeyed):
    r = Remote("missing", unkeyed=unkeyed)
    res = await _ensure(r)
    assert res.built and res.path == _mv().key and "build" in r.steps


async def test_a_broken_keyed_venv_never_falls_back_to_the_unkeyed_one():
    r = Remote("broken", unkeyed="ok")  # the line would use the keyed one (it exists): it has to be repaired
    res = await _ensure(r)
    assert res.built and "check unkeyed" not in r.steps


def test_the_check_reads_versions_through_the_entry_point():
    script = ev.check_script(_mv())
    assert '"$k/bin/globus-compute-endpoint" version' in script
    assert '"$k/bin/python" -c ' in script and "print(sys.version_info[:2])\\n" in script  # its last line, as before
    assert "import globus_compute_endpoint" not in script  # fails under -c on a real install
    assert "PYTHONDONTWRITEBYTECODE=1" in script and "timeout 60" in script and "INODE=" in script
    s = ev.parse_check("x\nHPCB_VENV_BEGIN\nEXISTS=1\nPYVENV=1\nENTRY=1\nVERSION_RC=0\n"
                       "VERSION=Globus Compute endpoint version: 4.18.0\nPY_RC=0\nPY=(3, 13)\nHPCB_VENV_END\n")
    assert ev.problem(s, gce="4.18.0", python="3.13") is None
    assert ev.problem(s, gce="4.18.1", python="3.13") == "it reports 4.18.0, not 4.18.1"
    # the version line is found wherever it is (a warning may follow it), not only on the last line
    assert ev.reported_version("Globus Compute endpoint version: 4.18.0\nUserWarning: x") == "4.18.0"
    assert ev.reported_version("ImportError: nope") is None


# ---- the build -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize(("venv", "why"), [
    ("missing", "does not exist yet"), ("broken", "fails: ImportError: cannot import name 'X'"),
    ("stale", "reports 4.13.0, not 4.18.0"), ("otherpy", f"python is 3.9, not {PY}"), ("empty", "does not exist yet"),
])
async def test_a_venv_that_fails_its_check_is_built_beside_it_and_renamed_in(venv, why):
    r = Remote(venv)
    if venv == "empty":
        r = Remote("empty")
        why = "has no bin/"
    res = await _ensure(r)
    unkeyed = ["check unkeyed"] if venv == "missing" else []
    assert r.steps == ["check", *unkeyed, "build", "verify", "claim"]  # no cleanup: the build became the venv
    assert res.built and why in res.reason and (res.moved_aside is not None) is (venv not in ("missing",))
    assert r.venv == "ok"


async def test_the_build_is_relocatable_uncached_copied_and_marked():
    r = Remote("missing")
    await _ensure(r)
    wrapper = r.script("build")
    assert f"timeout -k 10 {ev._BUILD_CAP_S}" in wrapper and ev._TOKEN_PREFIX in wrapper  # capped, named
    tokens = shlex.split(wrapper)
    build = tokens[tokens.index("-lc") + 1]  # the build itself, as the login node's bash runs it
    token = tokens[tokens.index("-lc") + 2].removeprefix(ev._TOKEN_PREFIX).rstrip(";")
    assert f'b="{_mv().key}.build.{token}"' in build  # its own directory, never the keyed path
    assert 'u=$(command -v uv); "$u" venv -q --relocatable --no-cache --python ' + PY in build  # uv by its path
    assert '"$u" pip install -q --no-cache --link-mode=copy' in build
    assert f"'globus-compute-endpoint=={PIN}' 'globus-sdk=={SDK}'" in build  # co-pinned
    assert f'uname -m > "$b/{ev.ARCH_MARKER}"' in build and "astral.sh" not in build
    assert f"{_mv().key}.build.{token}" in r.script("verify")  # verified where it was built
    assert "os.rename(b,k)" in ev._CLAIM_PY and "4242" not in r.script("claim")  # nothing to move aside


@pytest.mark.parametrize("tools", ["curl", "python3"])
async def test_a_node_without_uv_gets_uv_first(tools):
    r = Remote("missing", tools=tools)
    await _ensure(r)
    build = r.script("build")
    assert "astral.sh/uv/install.sh" in build and "pip install -q uv" in build  # astral.sh, else pip
    assert 'UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh' in build  # never edits the user's dotfiles


async def test_no_tool_to_build_with_moves_nothing():
    r = Remote("broken", tools="none")
    with pytest.raises(EndpointVenvError, match="no uv, no curl and no python3"):
        await _ensure(r)
    assert r.steps == ["check"] and r.venv == "broken"  # left where it was


async def test_a_directory_that_is_not_a_venv_is_never_moved():
    r = Remote("notvenv")
    with pytest.raises(EndpointVenvError, match="exists and is not a venv"):
        await _ensure(r)
    assert r.steps == ["check"]


async def test_a_broken_venv_is_moved_aside_only_at_the_claim_and_only_if_unchanged():
    r = Remote("broken")
    res = await _ensure(r)
    claim = r.script("claim")
    assert "4242" in claim and ".broken." in claim and res.moved_aside.endswith(".broken.20261013-120000")
    assert "if st is not None and ino and str(st.st_ino)==ino" in ev._CLAIM_PY  # the same directory the check saw


async def test_a_failed_build_is_kept_as_failed_and_the_broken_venv_stays():
    r = Remote("broken", build_rc=1, build_err="Resolved 40 packages\nerror: Failed to fetch: `https://pypi.org/x`\n")
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    msg = str(e.value)
    assert "error: Failed to fetch: `https://pypi.org/x`" in msg and "Resolved 40" not in msg  # the last line
    assert ".failed." in msg and r.steps == ["check", "build", "cleanup"]
    cleanup = r.script("cleanup")
    assert "HPCB_FAILED" in cleanup and "pgrep" not in cleanup and "rm -rf" not in cleanup  # kept as evidence
    assert r.venv == "broken"  # never touched: nothing claimed the keyed path


async def test_a_build_past_its_cap_says_so():
    r = Remote("missing", build_rc=124)
    with pytest.raises(EndpointVenvError, match=f"did not finish within {ev._BUILD_CAP_S} s"):
        await _ensure(r)


async def test_a_build_that_fails_its_verify_is_kept_as_failed():
    r = Remote("missing", built="otherpy")
    with pytest.raises(EndpointVenvError, match=f"fails its check: its python is 3.9, not {PY}"):
        await _ensure(r)
    assert r.steps[-1] == "cleanup" and "HPCB_FAILED" in r.script("cleanup") and "claim" not in r.steps


async def test_a_verify_that_times_out_leaves_the_build_where_it_is():
    r = Remote("missing", built="slow")
    with pytest.raises(EndpointVenvError, match="verifying the newly built"):
        await _ensure(r)
    cleanup = r.script("cleanup")
    assert "HPCB_FAILED" not in cleanup and "rm -rf" not in cleanup  # not failed, not discarded: left for later


async def test_an_ssh_blip_on_the_claim_never_moves_a_verified_build_to_failed():
    r = Remote("missing", ssh_fail="claim")
    with pytest.raises(RuntimeError, match="Permission denied"):
        await _ensure(r)
    cleanup = r.script("cleanup")
    assert "HPCB_FAILED" not in cleanup and "rm -rf" not in cleanup


async def test_a_lost_claim_discards_ours_and_uses_the_winners():
    r = Remote("missing", claim="lost")
    res = await _ensure(r)
    assert r.steps == ["check", "check unkeyed", "build", "verify", "claim", "re-check", "cleanup"]
    assert not res.built and res.path == _mv().key
    assert 'rm -rf "$b"' in r.script("cleanup") and "HPCB_FAILED" not in r.script("cleanup")  # our duplicate goes


async def test_a_lost_claim_whose_winner_times_out_is_reported_not_rebuilt():
    r = Remote("missing", claim="lost", winner="slow")
    with pytest.raises(EndpointVenvError, match="timed out"):
        await _ensure(r)
    assert r.steps.count("build") == 1 and 'rm -rf "$b"' in r.script("cleanup")


async def test_a_lost_claim_whose_winner_is_broken_is_reported():
    r = Remote("missing", claim="lost", winner="broken")
    with pytest.raises(EndpointVenvError, match=r"put a venv at .* first, and it fails its check"):
        await _ensure(r)
    assert r.steps.count("build") == 1


async def test_a_cancel_mid_build_stops_the_build_and_discards_it():
    r = Remote("broken", hang_build=True)
    task = asyncio.ensure_future(_ensure(r))
    for _ in range(100):
        await asyncio.sleep(0)
        if "build" in r.steps:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    cleanup = r.script("cleanup")
    assert "pgrep -f" in cleanup and "buil[d]-" in cleanup and 'kill -TERM -- "-$g"' in cleanup  # the build stops
    assert 'rm -rf "$b"' in cleanup and r.venv == "broken"  # a partial build leaves nothing; the keyed path untouched


@pytest.mark.parametrize("step", ["check", "check unkeyed", "build", "verify", "claim"])
async def test_ssh_itself_failing_is_a_transport_error_not_a_venv_one(step):
    r = Remote("missing", ssh_fail=step)
    with pytest.raises(RuntimeError) as e:
        await _ensure(r)
    assert not isinstance(e.value, EndpointVenvError)
    assert "Permission denied (publickey,keyboard-interactive)" in str(e.value)  # the explainer/handoff reads it


async def test_an_old_uv_is_named_not_blamed_on_pypi():
    # uv before 0.2.31 has no `venv --relocatable`: say so, and claim no failed copy that was never made
    r = Remote("broken", tools="olduv")
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    msg = str(e.value)
    assert "this node's uv (uv 0.2.0, /usr/bin/uv) is too old" in msg and "uv self update" in msg
    assert "remove it from PATH so hpc-bridge installs its own" in msg and ".failed" not in msg and "PyPI" not in msg
    assert r.steps == ["check"] and r.venv == "broken"


async def test_a_failed_build_names_only_a_copy_that_exists():
    r = Remote("missing", build_rc=1, build_err="error: nope")

    async def run(step, script, timeout):  # the failed build left nothing behind (it died before creating its dir)
        if step == "build":
            r.steps.append(step)
            r.scripts.append((step, script))
            return 1, "", "error: nope"
        return await Remote.__call__(r, step, script, timeout)

    with pytest.raises(EndpointVenvError) as e:
        await ev.ensure(run, _mv(), host=HOST, gce=PIN, python=PY, sdk=SDK)
    assert "Nothing of it was kept." in str(e.value) and ".failed" not in str(e.value)
    assert r.steps.count("cleanup") == 1  # moved (or not) once, before the message


async def test_a_rename_error_other_than_a_lost_race_keeps_the_build_and_says_why():
    r = Remote("missing", claim="error")
    with pytest.raises(EndpointVenvError, match=r"could not put the newly built venv in place .*: Permission denied"):
        await _ensure(r)
    cleanup = r.script("cleanup")
    assert 'rm -rf "$b"' not in cleanup and "HPCB_FAILED" not in cleanup and "re-check" not in r.steps


def _claim_in_process(tmp_path: Path, monkeypatch, *, k: Path, b: Path, ino: str, aside: Path, between=None,
                      fail: dict[int, int] | None = None) -> str:
    """Run `_CLAIM_PY` in this process (argv as the login node passes it), optionally doing `between()` just before its
    first rename — another bring-up acting at the worst moment — or failing rename call N with errno `fail[N]`."""
    import contextlib
    import io
    import sys as _sys

    real = os.rename
    state = {"first": True, "n": 0}

    def rename(src, dst):
        n, state["n"] = state["n"], state["n"] + 1
        if fail and n in fail:
            raise OSError(fail[n], os.strerror(fail[n]))
        if state["first"] and between is not None:
            state["first"] = False
            between()
        return real(src, dst)

    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.setattr(_sys, "argv", ["claim", str(b), str(k), ino, str(aside)])
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.suppress(SystemExit):
        exec(compile(ev._CLAIM_PY, "claim", "exec"), {"__name__": "claim"})  # noqa: S102 - the claim program itself
    return out.getvalue()


def test_the_claim_never_moves_a_venv_another_bring_up_just_put_in_place(tmp_path, monkeypatch):
    # 1500 two-claim races (review 2026-10-13): another bring-up moved the broken venv aside and renamed ITS build in
    # between our inode check and our move — our move took THEIR working venv aside. Checked again after the move: put
    # back, and ours has lost.
    k, b, theirs = tmp_path / "k", tmp_path / "b", tmp_path / "theirs"
    for d, mark in ((k, "broken"), (b, "ours"), (theirs, "theirs")):
        d.mkdir()
        (d / mark).write_text("x")
    ino = str(os.lstat(k).st_ino)

    def other_bring_up():
        os.rename(k, tmp_path / "k.broken.someone-else")
        os.rename(theirs, k)

    out = _claim_in_process(tmp_path, monkeypatch, k=k, b=b, ino=ino, aside=tmp_path / "k.broken.T.tok", between=other_bring_up)
    assert "HPCB_CLAIM=lost" in out and (k / "theirs").exists() and (b / "ours").exists()  # theirs stays, ours unclaimed


def test_the_claim_takes_a_fresh_aside_name_when_one_is_taken(tmp_path, monkeypatch):
    k, b = tmp_path / "k", tmp_path / "b"
    for d in (k, b):
        d.mkdir()
        (d / d.name).write_text("x")
    taken = tmp_path / "k.broken.T.tok"
    taken.mkdir()
    (taken / "older").write_text("x")
    out = _claim_in_process(tmp_path, monkeypatch, k=k, b=b, ino=str(os.lstat(k).st_ino), aside=taken)
    assert f"HPCB_MOVED={taken}x" in out and "HPCB_CLAIM=ok" in out
    assert (Path(f"{taken}x") / "k").exists() and (taken / "older").exists() and (k / "b").exists()


@pytest.mark.parametrize(("which", "err", "verdict"), [
    ("move-aside", errno.EACCES, "error"), ("move-aside", errno.ENOENT, "lost"),
    ("claim", errno.EACCES, "error"), ("claim", errno.EXDEV, "error"),
    ("claim", errno.ENOTEMPTY, "lost"), ("claim", errno.EEXIST, "lost")])
def test_the_claim_reads_only_a_taken_path_as_a_lost_race(tmp_path, monkeypatch, which, err, verdict):
    # Only ENOTEMPTY/EEXIST on the claim (another bring-up's venv is there) and ENOENT on the move-aside (another
    # bring-up moved it first) are a lost race; any other error is reported, and the caller keeps the verified build.
    k, b = tmp_path / "k", tmp_path / "b"
    for d in (k, b):
        d.mkdir()
        (d / d.name).write_text("x")
    out = _claim_in_process(tmp_path, monkeypatch, k=k, b=b, ino=str(os.lstat(k).st_ino), aside=tmp_path / "k.broken",
                            fail={0 if which == "move-aside" else 1: err})
    assert f"HPCB_CLAIM={verdict} {os.strerror(err)}" in out, out
    assert (b / "b").exists()  # the build is never touched by a failed claim


def test_the_claim_aside_name_is_unique_per_bring_up():
    a, b = ev.claim_script(_mv(), "aaaaaaaa1111", "1"), ev.claim_script(_mv(), "bbbbbbbb2222", "1")
    assert shlex.split(a)[-1] != shlex.split(b)[-1] and shlex.split(a)[-1].endswith(".aaaaaaaa")


def test_the_claim_script_runs_with_the_builds_own_python():
    s = ev.claim_script(_mv(), "tok", "4242")
    assert s.startswith(f'b="{_mv().key}.build.tok"; k="{_mv().key}"; "$b/bin/python" -c ')
    assert shlex.split(s)[-2] == "4242" and ".broken." in shlex.split(s)[-1]


# ---- the teardown's search -------------------------------------------------------------------------------------------

def test_the_search_tries_each_candidate_once_within_a_deadline_and_skips_builds():
    s = ev.find_script(_mv())
    assert s.index('"$k"') < s.index('"$b"') < s.index("-name")
    assert "(*.build.*|*.broken.*|*.failed.*|*.lock|*.old.*) continue" in s and "sort -rV" in s
    assert '__hb_seen="$__hb_seen$d "' in s and "__hb_t0=$SECONDS" in s  # the deadline counts from the loop
    assert f'[ "$((SECONDS - __hb_t0))" -lt {ev._FIND_DEADLINE_S} ] || {{ echo HPCB_UNTRIED=1; break; }}' in s
    assert f"timeout {ev._FIND_CAP_S}" in s and "uv " not in s and "pip" not in s  # never a build
    assert ev._FIND_DEADLINE_S + ev._FIND_CAP_S < ev._FIND_S  # the last candidate still fits the ssh budget


# ---- in the bring-up and the teardown --------------------------------------------------------------------------------

def _cli(env_setup: str):
    from hpc_bridge.facility.remote import RemoteEndpointCLI, SshTarget

    return RemoteEndpointCLI(SshTarget("login.example.edu", "u"), env_setup)


def _line(prefix: str = "") -> str:
    return prefix + ev.short_env_setup("/home/u/hpc-bridge/gce-venv")


async def test_a_hand_written_env_setup_is_never_built(monkeypatch):
    called = []

    async def fake_ensure(*a, **k):
        called.append(1)

    async def nothing_of_ours(run, mv, extra=()):
        return ev.Found(0, None, False, "")

    monkeypatch.setattr(ev, "ensure", fake_ensure)
    monkeypatch.setattr(ev, "find_working", nothing_of_ours)
    cli = _cli("module load gce && source /x/bin/activate")
    assert cli.venv is None
    await cli.ensure_venv()
    assert called == [] and await cli.use_working_venv() == (True, "")
    assert cli._env_prefix() == "module load gce && source /x/bin/activate && "  # nothing of ours runs: as given


async def test_the_bring_up_checks_once_per_process_and_drops_a_teardown_override(monkeypatch):
    seen = []

    async def fake_ensure(run, mv, **kw):
        seen.append((mv.key, kw["gce"], kw["python"]))
        return ev.EnsureResult(path=mv.key, built=True, reason="it does not exist yet")

    monkeypatch.setattr(ev, "ensure", fake_ensure)
    cli = _cli(_line())
    cli._prefix_override = 'export PATH="/home/u/hpc-bridge/gce-venv/bin:$PATH" && '  # a teardown found the old one
    await cli.ensure_venv()
    await cli.ensure_venv()
    assert len(seen) == 1 and cli.venv_ready == cli.venv.key and cli.venv_built.built
    assert cli._env_prefix() == _line() + " && "  # the line again, not the old venv


async def test_a_teardown_uses_the_first_working_venv(monkeypatch):
    async def fake_find(run, mv, extra=()):
        return ev.Found(0, "/home/u/hpc-bridge/gce-venv", False, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    cli = _cli(_line("module load uv; "))
    assert await cli.use_working_venv() == (True, "")
    assert cli._env_prefix() == ('module load uv; unset PYTHONHOME; export VIRTUAL_ENV="/home/u/hpc-bridge/gce-venv" '
                                 'PATH="/home/u/hpc-bridge/gce-venv/bin:$PATH" && ')


async def test_a_teardown_after_this_process_built_the_venv_does_not_search(monkeypatch):
    async def fake_find(run, mv, extra=()):
        raise AssertionError("searched")

    monkeypatch.setattr(ev, "find_working", fake_find)
    cli = _cli(_line())
    cli.venv_ready = cli.venv.key
    assert await cli.use_working_venv() == (True, "")


async def test_a_teardown_whose_search_times_out_still_tries_the_stop(monkeypatch):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility

    async def slow_find(run, mv, extra=()):
        raise TimeoutError()

    monkeypatch.setattr(ev, "find_working", slow_find)
    line = _line()
    cli = _cli(line)
    did = []

    async def stop(name):
        did.append("stop")
        return 0, ""

    async def other(*a, **k):
        did.append("other")
        return []

    async def deleted(name):
        return True

    async def closed():
        return True

    cli.stop, cli.cancel_blocks, cli.delete, cli.remove_uep_dirs, cli.close = stop, other, deleted, other, closed
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                          account="", worker_init=line)
    res = await SlurmFacility(prof, cli).teardown("eid-1")
    assert did[0] == "stop" and res["stopped"] and cli._env_prefix() == line + " && "  # through the line itself


async def test_a_teardown_with_no_working_venv_releases_blocks_and_says_so(monkeypatch):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility

    async def fake_find(run, mv, extra=()):
        return ev.Found(0, None, False, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    line = _line()
    cli = _cli(line)
    did = []

    async def cancel_blocks(eid, sched="slurm"):
        did.append(("cancel", eid))
        return ["7"]

    async def stop(name):
        did.append("stop")
        return 0, ""

    cli.cancel_blocks, cli.stop = cancel_blocks, stop
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                          account="", worker_init=line)
    res = await SlurmFacility(prof, cli).teardown("eid-1")
    assert did == [("cancel", "eid-1")]  # blocks released; no stop through a CLI that does not exist
    assert res["stopped"] is False and not res["ssh_failed"]
    assert "no working globus-compute-endpoint venv" in res["error"] and "a teardown never builds one" in res["error"]


async def test_a_teardown_whose_search_cannot_ssh_reports_ssh_failed(monkeypatch):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility

    async def fake_find(run, mv, extra=()):
        return ev.Found(255, None, False, "Connection timed out")

    monkeypatch.setattr(ev, "find_working", fake_find)
    line = _line()
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                          account="", worker_init=line)
    res = await SlurmFacility(prof, _cli(line)).teardown("eid-1")
    assert res["ssh_failed"] and "Connection timed out" in res["error"]


def _order_cli(order: list[str]):
    class _CLI:
        target = type("T", (), {"user": "u", "key_path": None, "host": "h"})()

        async def ensure_venv(self):
            order.append("ensure")

        async def whoami(self):
            order.append("whoami")
            return True

        async def status(self, name):
            order.append("status")
            return "running"

        async def endpoint_id(self, name):
            return "e"

    return _CLI()


async def test_the_bring_up_builds_before_whoami_and_a_reuse_never_builds():
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility
    from hpc_bridge.profile import Profile

    order: list[str] = []
    line = _line()
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                          account="", worker_init=line)
    fac = SlurmFacility(prof, _order_cli(order))

    async def provision(hpc):
        order.append("provision")
        from hpc_bridge.facility.base import EndpointHandle
        return EndpointHandle(endpoint_id="e", name="hpc-bridge-m")

    async def none_online(name):
        return None

    fac.provision, fac.find_online_endpoint = provision, none_online
    await fac.bootstrap(Profile())
    assert order == ["ensure", "whoami", "provision"]

    async def online(name):
        return "e"

    order.clear()
    fac.find_online_endpoint = online
    h = await fac.bootstrap(Profile())
    assert h.reused and order == []  # zero SSH: nothing checked, nothing built


async def test_provision_on_its_own_checks_the_venv_before_any_gce_call():
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility
    from hpc_bridge.profile import Profile

    order: list[str] = []
    line = _line()
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                          account="", worker_init=line)
    await SlurmFacility(prof, _order_cli(order)).provision(Profile())
    assert order[:2] == ["ensure", "status"]


def test_a_build_error_is_shown_as_it_is():
    from hpc_bridge.notices import _explain_provision_error

    e = EndpointVenvError("could not build hpc-bridge's endpoint venv /v on h (…): error: Failed to fetch: "
                          "connection timed out. Nothing was started or billed.")
    assert _explain_provision_error(e) == str(e)  # not mistaken for CANNOT REACH / REMOTE FILESYSTEM


# ---- the same scripts in a real shell ------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def stubs(tmp_path_factory) -> dict:
    return vs.make_stubs(tmp_path_factory.mktemp("venv_build_stubs"))


@pytest.fixture
def box(tmp_path: Path, stubs: dict, monkeypatch) -> dict:
    monkeypatch.setenv("HPCB_TEST_TEMPLATE", str(stubs["template"]))
    b = vs.make_box(tmp_path, stubs)
    # `bash -l` reads /etc/profile (macOS's path_helper, Debian's PATH line); the profile puts the stubs back first,
    # as a user's own ~/.bash_profile would
    (b["home"] / ".bash_profile").write_text(f'export PATH="{b["stub"]}:/usr/bin:/bin"\n')
    b["mv"] = _mv(str(b["venv"]))
    b["key"] = Path(b["mv"].key)
    b["procs"] = []
    b["env"] = {}
    yield b
    for p in b["procs"]:  # a build a test abandoned (its "ssh" went away): never leave it running
        if p.returncode is None:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def _shell(box: dict, before: dict | None = None, **more: str):
    """The production runner's shape: `bash -lc <script>` in a session of its own (as sshd starts it). `before` maps a
    step to a callable run just before it (another bring-up acting on the node)."""
    async def run(step: str, script: str, timeout: float) -> tuple[int, str, str]:
        if before and step in before:
            before.pop(step)()
        env = {**vs.env(box), **box["env"], **more}
        proc = await asyncio.create_subprocess_exec(
            "bash", "-lc", script, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True)
        box["procs"].append(proc)
        out, err = await asyncio.wait_for(proc.communicate(), timeout)  # a cancel abandons it, as ssh does
        return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")
    return run


async def _real(box: dict, before: dict | None = None, **more: str) -> ev.EnsureResult:
    return await ev.ensure(_shell(box, before, **more), box["mv"], host="login", gce=PIN, python=PY, sdk=SDK)


def _leftovers(box: dict) -> list[str]:
    parent = box["venv"].parent
    return sorted(p.name for p in parent.iterdir() if re.search(r"\.(build|broken|failed)\.", p.name)) \
        if parent.exists() else []


def _relocatable(v: Path) -> bool:
    return "relocatable = true" in (v / "pyvenv.cfg").read_text()


async def test_real_a_first_build(box):
    res = await _real(box)
    assert res.built and vs.version_of(box["key"]) == PIN and _relocatable(box["key"])
    installs = [c for c in vs.calls(box) if "install" in c]
    assert len(installs) == 1
    assert {"--no-cache", "--link-mode=copy", f"globus-compute-endpoint=={PIN}", f"globus-sdk=={SDK}"} <= set(installs[0])
    venvs = [c for c in vs.calls(box) if c[:2] == ["uv", "venv"]]
    assert len(venvs) == 1 and "--relocatable" in venvs[0] and ".build." in venvs[0][-1]  # built beside, renamed in
    assert (box["key"] / ".hpcb-arch").read_text().strip() == os.uname().machine
    assert _leftovers(box) == [] and (box["home"] / "keep.txt").read_text() == "not yours"


async def test_real_a_healthy_venv_costs_nothing(box):
    v = vs.venv(box, box["key"], PIN)
    res = await _real(box)
    assert not res.built and vs.calls(box) == [] and (v / "sentinel").exists()
    assert (v / ".hpcb-arch").read_text().strip() == os.uname().machine  # the marker it lacked


async def test_real_a_prepared_unkeyed_venv_is_used(box):
    u = vs.venv(box, box["venv"], PIN)
    res = await _real(box)
    assert res.path == str(u) and not res.built and vs.calls(box) == [] and not box["key"].exists()


@pytest.mark.parametrize("old", [{"broken": True}, {"version": "4.13.0"}], ids=["broken", "stale"])
async def test_real_a_repair_builds_beside_and_moves_the_old_one_aside(box, old):
    v = vs.venv(box, box["key"], old.get("version", PIN), broken=old.get("broken", False))
    res = await _real(box)
    assert res.built and vs.version_of(v) == PIN and not (v / "sentinel").exists()
    aside = [p for p in box["venv"].parent.iterdir() if ".broken." in p.name]
    assert len(aside) == 1 and (aside[0] / "sentinel").read_text() == "old" and str(aside[0]) == res.moved_aside
    assert not [n for n in _leftovers(box) if ".build." in n or ".failed." in n]


async def test_real_a_node_without_uv_installs_it_with_curl(box):
    (box["stub"] / "uv").unlink()
    res = await _real(box)
    assert res.built and vs.version_of(box["key"]) == PIN
    assert len(box["curl"].read_text().splitlines()) == 1 and (box["home"] / ".local/bin/uv").exists()


async def test_real_a_node_where_the_astral_installer_fails_installs_uv_with_pip(box):
    (box["stub"] / "uv").unlink()
    # a curl that cannot connect (a site proxying PyPI but not GitHub) — a stub, so no test ever reaches the network
    (box["stub"] / "curl").write_text("#!/bin/sh\necho 'curl: (7) Failed to connect' >&2\nexit 7\n")
    res = await _real(box)
    assert res.built and vs.version_of(box["key"]) == PIN and (box["home"] / ".local/bin/uv").exists()
    assert any(c[0] == "pip" and "uv" in c for c in vs.calls(box))


def _not_root() -> None:
    if os.geteuid() == 0:
        pytest.skip("root ignores the permission this test takes away")


async def test_real_a_parent_that_cannot_be_created_fails_at_once(box):
    _not_root()
    box["home"].chmod(0o500)
    try:
        with pytest.raises(EndpointVenvError, match=r"could not build .*Permission denied") as got:
            await _real(box)
        assert vs.calls(box) == []
        # the login node's filesystem refused the write: not the configuration's (connect keeps it, review round 3)
        assert not isinstance(got.value, ConfigStepFailed)
    finally:
        box["home"].chmod(0o700)


# a uv that fails the way the login node made it fail (a stub: no test ever reaches the network)
_UV_FAILS = {
    "dns": ("error: Failed to fetch: `https://pypi.org/simple/globus-compute-endpoint/`\n  Caused by: Request failed "
            "after 3 retries\n  Caused by: error sending request for url (https://pypi.org/simple/)\n  Caused by: "
            "dns error: failed to lookup address information: Temporary failure in name resolution", False),
    "refused": ("error: Failed to fetch: `https://pypi.org/simple/parsl/`\n  Caused by: tcp connect error: Connection "
                "refused (os error 111)", False),
    "unreachable": ("error: Request failed after 3 retries\n  Caused by: Network is unreachable (os error 101)", False),
    "no python": ("error: No interpreter found for Python 3.13 in managed installations or search path", True),
}


@pytest.mark.parametrize("why", list(_UV_FAILS))
async def test_real_a_build_the_network_failed_is_not_the_configs_and_one_the_config_failed_is(box, why):
    # review round 3, item 3: a venv build that could not reach PyPI failed for the node's DNS or route — the same
    # env_setup may build next time — so it is never EndpointVenvConfigError (which drops a details= config)
    said, config = _UV_FAILS[why]
    body = "".join(f"echo {shlex.quote(ln)} >&2; " for ln in said.splitlines())
    (box["stub"] / "uv").rename(box["stub"] / "uv.real")   # the stub answers every probe; only its install fails
    (box["stub"] / "uv").write_text(f'#!/bin/sh\ncase " $* " in *" pip "*) {body}exit 2;; esac\n'
                                    'exec "$(dirname "$0")/uv.real" "$@"\n')
    (box["stub"] / "uv").chmod(0o755)
    with pytest.raises(EndpointVenvError, match="could not build hpc-bridge's endpoint venv") as got:
        await _real(box)
    assert isinstance(got.value, ConfigStepFailed) is config, str(got.value)


@pytest.mark.parametrize("line", ["curl: (6) Could not resolve host: astral.sh",
                                  "curl: (7) Failed to connect to astral.sh port 443: Network is unreachable"])
def test_curl_that_cannot_reach_astral_is_the_networks(line):
    assert ev._BUILD_NOT_THE_CONFIG.search(ev._tail("a login banner\n", line))
    assert not ev._BUILD_NOT_THE_CONFIG.search(ev._tail("a login banner\n", "error: No interpreter found for 3.13"))


async def test_real_a_directory_that_is_not_a_venv_is_never_replaced(box):
    box["key"].mkdir(parents=True)
    (box["key"] / "precious").write_text("mine")
    with pytest.raises(EndpointVenvError, match="exists and is not a venv"):
        await _real(box)
    assert (box["key"] / "precious").read_text() == "mine" and vs.calls(box) == [] and _leftovers(box) == []


async def test_real_a_failed_build_is_kept_and_the_old_venv_stays(box):
    v = vs.venv(box, box["key"], PIN, broken=True)
    with pytest.raises(EndpointVenvError) as e:
        await _real(box, HPCB_TEST_UV_FAIL="install")
    assert "error: Failed to fetch" in str(e.value)
    assert (v / "sentinel").read_text() == "old"  # nothing claimed the keyed path: the old venv is where it was
    left = _leftovers(box)
    assert len(left) == 1 and ".failed." in left[0]


async def test_real_two_concurrent_builds_one_claims_the_other_discards(box):
    box["env"]["HPCB_TEST_SLOW"] = "1"
    a, b = await asyncio.gather(_real(box), _real(box))
    assert sum(c[:2] == ["uv", "venv"] for c in vs.calls(box)) == 2  # both built, in their own directories
    assert sorted([a.built, b.built]) == [False, True] and a.path == b.path == str(box["key"])
    assert vs.version_of(box["key"]) == PIN and _leftovers(box) == []  # the loser removed its own build


async def test_real_a_venv_put_there_by_another_bring_up_wins_the_claim(box):
    # between our check and our claim, another bring-up renames its build into place: ours is discarded
    other = box["tmp"] / "other"

    def other_bring_up():
        vs.venv(box, other, PIN, relocatable=True)
        os.rename(other, box["key"])

    res = await _real(box, before={"claim": other_bring_up})
    assert not res.built and (box["key"] / "sentinel").read_text() == "old"  # theirs, untouched
    assert _leftovers(box) == []


async def test_real_a_broken_venv_replaced_meanwhile_is_not_moved_aside(box):
    # we saw a broken venv; before our claim another bring-up repaired it: the inode differs, so ours never moves theirs
    vs.venv(box, box["key"], PIN, broken=True)
    other = box["tmp"] / "other"

    def other_repair():
        os.rename(box["key"], f"{box['key']}.broken.someone-else")
        vs.venv(box, other, PIN, relocatable=True)
        os.rename(other, box["key"])

    res = await _real(box, before={"claim": other_repair})
    assert not res.built and vs.version_of(box["key"]) == PIN and (box["key"] / "sentinel").exists()
    assert [n for n in _leftovers(box)] == [f"{box['key'].name}.broken.someone-else"]


async def test_real_a_cancel_mid_build_stops_it_and_a_retry_builds(box):
    box["env"]["HPCB_TEST_SLOW"] = "30"
    task = asyncio.ensure_future(_real(box))
    for _ in range(200):
        await asyncio.sleep(0.1)
        if any(c[:2] == ["uv", "venv"] for c in vs.calls(box)):
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    time.sleep(0.5)
    assert not box["key"].exists() and _leftovers(box) == []  # the stopped build made nothing, left nothing
    r = subprocess.run(["pgrep", "-f", "hpcb-venv-buil[d]-"], capture_output=True, text=True, check=False)
    stray = [p for p in r.stdout.split() if p]
    assert not [p for p in stray if str(box["tmp"]) in subprocess.run(
        ["ps", "-o", "command=", "-p", p], capture_output=True, text=True, check=False).stdout]
    box["env"].pop("HPCB_TEST_SLOW")
    res = await _real(box)
    assert res.built and vs.version_of(box["key"]) == PIN


async def test_real_the_teardown_search(box):
    run = _shell(box)
    assert await ev.find_working(run, box["mv"]) == (0, None, False, "")
    vs.venv(box, box["venv"], "4.12.0", broken=True)  # the unkeyed one, broken (globus1)
    for suffix, ver in ((".broken.20261013-1", "4.17.0"), (".failed.20261013-1", "4.17.1"), (".build.abc", "4.17.2")):
        vs.venv(box, Path(ev.keyed_venv(str(box["venv"]), python=PY, gce=ver) + suffix), ver)
    vs.venv(box, Path(ev.keyed_venv(str(box["venv"]), python=PY, gce="4.9.0")), "4.9.0")
    other = vs.venv(box, Path(ev.keyed_venv(str(box["venv"]), python=PY, gce="4.13.0")), "4.13.0")
    rc, found, _, _ = await ev.find_working(run, box["mv"])
    assert rc == 0 and found == str(other)  # newest version first (-V: 4.13 > 4.9), moved-aside/builds skipped
    vs.venv(box, box["key"], PIN)
    assert (await ev.find_working(run, box["mv"])).cli == str(box["key"])  # the keyed one first
    assert vs.calls(box) == []  # never a build


async def test_real_a_slow_login_profile_does_not_eat_the_search(box, monkeypatch):
    # SECONDS counts from the shell's start: a login profile slower than the deadline used to skip every candidate
    monkeypatch.setattr(ev, "_FIND_DEADLINE_S", 1)
    vs.venv(box, box["key"], PIN)
    (box["home"] / ".bash_profile").write_text(f'sleep 2; export PATH="{box["stub"]}:/usr/bin:/bin"\n')
    assert (await ev.find_working(_shell(box), box["mv"])).cli == str(box["key"])


async def test_real_candidates_the_deadline_left_untried_are_unknown_not_broken(box, monkeypatch, tmp_path):
    monkeypatch.setattr(ev, "_FIND_DEADLINE_S", 1)
    vs.venv(box, box["key"], PIN)
    (box["key"] / "bin" / "globus-compute-endpoint").write_text("#!/bin/sh\nsleep 2\nexit 1\n")  # slow, then fails
    vs.venv(box, box["venv"], "4.13.0")  # would work, but the deadline has passed by then
    found = await ev.find_working(_shell(box), box["mv"])
    assert found.cli is None and found.unknown  # unknown: the stop is tried, never "no working venv"


async def test_real_an_old_uv_is_named(box):
    with pytest.raises(EndpointVenvError, match=r"this node's uv \(uv 0\.2\.0 \(stub\), .*\) is too old"):
        await _real(box, HPCB_TEST_UV_OLD="0.2.0")
    assert vs.calls(box) == [] and _leftovers(box) == []


async def test_real_old_builds_of_ours_are_swept_and_nothing_else(box):
    parent = box["venv"].parent
    parent.mkdir(parents=True)
    old = Path(f"{box['key']}.build.deadbeef")
    fresh = Path(f"{box['key']}.build.cafef00d")
    other = Path(ev.keyed_venv(str(box["venv"]), python=PY, gce="4.1.0") + ".build.0ld")
    for d in (old, fresh, other):
        d.mkdir()
    stale = time.time() - 5 * 3600
    for d in (old, other):
        os.utime(d, (stale, stale))
    res = await _real(box)
    assert res.built and not old.exists() and fresh.exists() and other.exists()  # only ours, only hours-old ones


async def test_real_the_line_runs_with_a_foreign_pythonhome(box):
    # a site profile exporting PYTHONHOME for another Python: the venv's python would fail ("No module named
    # 'encodings'") — the line unsets it, before the worker's check too
    vs.venv(box, box["key"], PIN)
    line = ev.short_env_setup(str(box["venv"]), python=PY, gce=PIN)
    r = subprocess.run(["bash", "-c", line + '\npython -c "import encodings; print(1)"'], capture_output=True, text=True,
                       env={**vs.env(box), "SLURM_JOB_ID": "1", "PYTHONHOME": "/nonexistent/python"}, check=False)
    assert r.returncode == 0 and r.stdout.strip() == "1" and "does not run on" not in r.stderr, r.stderr


async def test_real_the_teardown_search_tries_the_keyed_venv_once(box, tmp_path):
    log = tmp_path / "tried"
    vs.venv(box, box["key"], PIN)
    ep = box["key"] / "bin" / "globus-compute-endpoint"
    ep.write_text(f'#!/bin/sh\necho "$0" >> {log}\nexit 1\n')  # slow NFS or broken: either way not the answer
    found = await ev.find_working(_shell(box), box["mv"])
    assert found.cli is None and not found.unknown and log.read_text().split().count(str(ep)) == 1


async def test_real_a_candidate_too_slow_to_answer_is_unknown_not_broken(box, tmp_path):
    vs.venv(box, box["key"], PIN)
    fake = tmp_path / "slowbin"
    fake.mkdir()
    (fake / "timeout").write_text("#!/bin/sh\nexit 124\n")  # every capped call ran out of time
    (fake / "timeout").chmod(0o755)
    (box["home"] / ".bash_profile").write_text(f'export PATH="{fake}:{box["stub"]}:/usr/bin:/bin"\n')
    found = await ev.find_working(_shell(box), box["mv"])
    assert found.cli is None and found.unknown  # HPCB_SLOW: the stop is tried rather than "no working venv"


_REAL_GCE = os.environ.get("HPCB_TEST_REAL_GCE_VENV")


@pytest.mark.skipif(not _REAL_GCE, reason="set HPCB_TEST_REAL_GCE_VENV to a real globus-compute-endpoint venv")
async def test_real_install_checks_through_its_entry_point(box):
    real = Path(_REAL_GCE).resolve()
    gce = subprocess.run([str(real / "bin" / "globus-compute-endpoint"), "version"], capture_output=True, text=True,
                         check=True).stdout.strip().rsplit(": ", 1)[-1]
    py = subprocess.run([str(real / "bin" / "python"), "-c", "import sys;print('%d.%d' % sys.version_info[:2])"],
                        capture_output=True, text=True, check=True).stdout.strip()
    mv = ev.managed_venv(ev.short_env_setup(str(box["venv"]), python=py, gce=gce))
    Path(mv.key).parent.mkdir(parents=True)
    Path(mv.key).symlink_to(real)
    marker = real / ".hpcb-arch"
    had = marker.exists()
    try:
        res = await ev.ensure(_shell(box), mv, host="login", gce=gce, python=py, sdk=None)
        assert not res.built and vs.calls(box) == []  # the real `version` passed the check
        assert (await ev.find_working(_shell(box), mv)).cli == mv.key
    finally:
        if not had and marker.exists():
            marker.unlink()


# ---- a teardown after a reconnect changed env_setup (live on globus1, 2026-10-13) ------------------------------------
# An agent "fixed" an architecture mismatch by reconnecting with env_setup "". The endpoint kept running from
# hpc-bridge's venv, and teardown ran a bare `globus-compute-endpoint`: "command not found", manager still running.

def _logging_gce(bin_dir: Path) -> None:
    """A stand-in entry point: records every call in $HOME/gce-calls and answers `version` as the real one does."""
    bin_dir.mkdir(parents=True)
    gce = bin_dir / "globus-compute-endpoint"
    gce.write_text('#!/bin/sh\necho "$*" >> "$HOME/gce-calls"\n'
                   '[ "$1" = version ] && echo "Globus Compute endpoint version: 4.18.0"\nexit 0\n')
    gce.chmod(0o755)


def _reconnected(tmp_path: Path, monkeypatch, *, current: str, started: str | None):
    """A SlurmFacility whose env_setup is now `current`, for an endpoint started with `started` (its published template;
    None: unreadable). Its ssh runs `bash -c` here with HOME = a login node's home and no gce on PATH."""
    from hpc_bridge.facility import remote
    from hpc_bridge.facility.remote import MachineProfile, RemoteEndpointCLI, SlurmFacility, SshTarget
    from hpc_bridge.profile import Profile

    home = tmp_path / "home"
    home.mkdir(exist_ok=True)

    async def local_ssh(target, cmd, timeout=None, **kw):
        p = await asyncio.create_subprocess_exec("bash", "-c", cmd, env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                                                 stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(p.communicate(), 60)
        return p.returncode or 0, out.decode(), err.decode()

    monkeypatch.setattr(remote, "ssh_exec", local_ssh)

    def prof(line: str) -> MachineProfile:
        return MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=line, interface="ib0", partition="p",
                              account="", worker_init=line)

    class _Client:
        def get_endpoint_metadata(self, eid):
            if started is None:
                raise RuntimeError("503")
            return {"user_config_template": SlurmFacility(prof(started), cli=None).config_template(Profile())[0]}

    cli = RemoteEndpointCLI(SshTarget("login.example.edu", "u"), current)

    async def none(*a, **k):
        return []

    async def closed():
        return True

    cli.cancel_blocks, cli.remove_uep_dirs, cli.close = none, none, closed
    return SlurmFacility(prof(current), cli, client_factory=_Client), home


@pytest.mark.parametrize("current", ["", "export SITE_THING=1"], ids=["emptied", "hand-written"])
async def test_a_teardown_after_env_setup_was_changed_stops_through_the_venv_it_was_started_with(
        tmp_path, monkeypatch, current):
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv" / "bin")  # globus1: the unkeyed venv passed the bring-up's check
    started = ev.short_env_setup(f"{home}/hpc-bridge/gce-venv", python="3.12", gce="4.18.0")
    fac, home = _reconnected(tmp_path, monkeypatch, current=current, started=started)
    await fac.adopt_endpoint("eid-1")  # the reconnect's _provision read it
    assert fac.endpoint_worker_init == started and "WARNING: the current env_setup is not hpc-bridge's own line" \
        in fac.worker_init_note
    rc, _out, err = await fac.cli._gce("version")
    assert rc != 0 and "command not found" in err  # what the teardown ran before: the current env_setup has no CLI
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True and res["deleted"] is True, res
    calls = (home / "gce-calls").read_text().splitlines()
    assert "stop hpc-bridge-m" in calls and "delete --yes hpc-bridge-m" in calls


async def test_a_teardown_that_cannot_read_the_endpoints_template_searches_hpc_bridges_default_venv(
        tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv-endpoint-4.18.0-py3.12" / "bin")  # keyed, another client's key
    fac, home = _reconnected(tmp_path, monkeypatch, current="", started=None)
    await fac.adopt_endpoint("eid-1")
    assert fac.endpoint_worker_init is None  # unknown: nothing says which venv it was started with
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res
    assert "stop hpc-bridge-m" in (home / "gce-calls").read_text().splitlines()


async def test_a_teardown_of_an_endpoint_started_with_a_hand_written_line_runs_that_line(tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "site-gce" / "bin")
    started = 'export PATH="$HOME/site-gce/bin:$PATH"'
    fac, home = _reconnected(tmp_path, monkeypatch, current="", started=started)
    await fac.adopt_endpoint("eid-1")
    assert "WARNING" not in fac.worker_init_note  # it was never hpc-bridge's venv
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res
    assert (home / "gce-calls").read_text().splitlines()[0] == "stop hpc-bridge-m"  # no search: the line, as given


async def test_a_teardown_with_nothing_of_hpc_bridges_on_the_node_uses_env_setup_as_given(tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "module-gce" / "bin")
    current = 'export PATH="$HOME/module-gce/bin:$PATH"'  # a site's own CLI, hand-written; no venv of hpc-bridge's
    fac, home = _reconnected(tmp_path, monkeypatch, current=current, started=None)
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res
    assert "stop hpc-bridge-m" in (home / "gce-calls").read_text().splitlines()


def test_the_finder_also_searches_the_default_base_and_its_keyed_venvs(tmp_path):
    import subprocess

    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv-endpoint-4.16.0-py3.12" / "bin")
    mv = _mv(f"{home}/elsewhere/gce-venv")  # the line's own base holds nothing
    r = subprocess.run(["bash", "-c", ev.find_script(mv, (ev.HOME_VENV,))], env={"HOME": str(home),
                       "PATH": "/usr/bin:/bin"}, capture_output=True, text=True, check=False)
    assert f"HPCB_CLI={home}/hpc-bridge/gce-venv-endpoint-4.16.0-py3.12" in r.stdout, r.stdout + r.stderr
    r = subprocess.run(["bash", "-c", ev.find_script(mv)], env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                       capture_output=True, text=True, check=False)
    assert "HPCB_CLI" not in r.stdout  # without it, only the line's own base


async def test_a_teardown_uses_the_started_lines_own_base_not_only_the_default(tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "custom" / "gce-venv" / "bin")  # HPC_BRIDGE_REMOTE_VENV elsewhere when it was started
    started = ev.short_env_setup(f"{home}/custom/gce-venv", python="3.12", gce="4.18.0")
    fac, home = _reconnected(tmp_path, monkeypatch, current="", started=started)
    await fac.adopt_endpoint("eid-1")
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res
    assert "stop hpc-bridge-m" in (home / "gce-calls").read_text().splitlines()


async def test_a_teardown_whose_started_base_is_gone_finds_the_default_venv(tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv" / "bin")
    started = ev.short_env_setup(f"{home}/custom/gce-venv", python="3.12", gce="4.18.0")  # nothing left there
    fac, home = _reconnected(tmp_path, monkeypatch, current="", started=started)
    await fac.adopt_endpoint("eid-1")
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res


async def test_a_line_an_earlier_plugin_generated_is_searched_never_run(monkeypatch):
    # its guarded uv/pip lines BUILD when the CLI is missing: a teardown must read them as the short line
    from tests.test_env_setup import _OLD_UV

    seen = []

    async def fake_find(run, mv, extra=()):
        seen.append((mv.base, extra))
        return ev.Found(0, "/home/u/hpc-bridge/gce-venv", False, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    cli = _cli("")
    old = _OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv")
    assert ev.managed_venv(old) is None  # not the short line as written
    assert await cli.use_working_venv(old) == (True, "")
    assert seen == [("/home/u/hpc-bridge/gce-venv", (ev.HOME_VENV,))]
    assert "pip install" not in cli._env_prefix() and "uv venv" not in cli._env_prefix()


async def test_a_keyed_or_untried_result_goes_through_the_started_line_itself(monkeypatch):
    # the keyed venv (or, when the deadline left the search unfinished, "unknown": the keyed one) is run through the
    # line, which falls back to the unkeyed venv when the keyed directory is missing — never a bare activation of it
    async def fake_find(run, mv, extra=()):
        return ev.Found(0, mv.key, False, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    started = _line()
    cli = _cli("")
    assert await cli.use_working_venv(started) == (True, "")
    assert cli._env_prefix() == started + " && "
    cli = _cli(started)
    assert await cli.use_working_venv(started) == (True, "") and cli._env_prefix() == started + " && "


# ---- the teardown's choice serves that teardown only (delta review 2026-10-13) ----------------------------------------

def _site_gce(home: Path) -> str:
    """A site's own CLI (a hand-written env_setup puts it on PATH), recording its calls as `site …`."""
    bin_dir = home / "site-gce" / "bin"
    bin_dir.mkdir(parents=True)
    gce = bin_dir / "globus-compute-endpoint"
    gce.write_text('#!/bin/sh\necho "site $*" >> "$HOME/gce-calls"\n'
                   '[ "$1" = version ] && echo "Globus Compute endpoint version: 4.18.0"\nexit 0\n')
    gce.chmod(0o755)
    return 'export PATH="$HOME/site-gce/bin:$PATH"'


async def test_a_teardowns_environment_never_reaches_the_next_bring_up(tmp_path, monkeypatch):
    # a site CLI by hand now; the endpoint was started with hpc-bridge's line: the teardown stops it through
    # hpc-bridge's venv, and the next bring-up on the same binding runs the site CLI again
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv" / "bin")
    site = _site_gce(home)
    started = ev.short_env_setup(f"{home}/hpc-bridge/gce-venv", python="3.12", gce="4.18.0")
    fac, home = _reconnected(tmp_path, monkeypatch, current=site, started=started)
    await fac.adopt_endpoint("eid-1")
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True and "stop hpc-bridge-m" in (home / "gce-calls").read_text().splitlines()
    assert fac.cli._env_prefix() == site + " && "  # cleared by teardown's finally
    await fac.cli.ensure_venv()  # a hand-written env_setup: returns early, and still in env_setup's environment
    (home / "gce-calls").unlink()
    rc, _out, _err = await fac.cli._gce("status-probe")
    assert rc == 0 and (home / "gce-calls").read_text().splitlines() == ["site status-probe"]


async def test_a_teardown_that_fails_midway_still_drops_its_environment(tmp_path, monkeypatch):
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv" / "bin")
    started = ev.short_env_setup(f"{home}/hpc-bridge/gce-venv", python="3.12", gce="4.18.0")
    fac, home = _reconnected(tmp_path, monkeypatch, current="", started=started)
    await fac.adopt_endpoint("eid-1")

    async def boom(eid, sched="slurm"):
        assert fac.cli._env_prefix() != ""  # the teardown's own choice is in force here
        raise RuntimeError("scancel exploded")

    fac.cli.cancel_blocks = boom
    with pytest.raises(RuntimeError, match="scancel exploded"):
        await fac.teardown("eid-1")
    assert fac.cli._env_prefix() == ""


@pytest.mark.parametrize("ready", [False, True], ids=["hand-written", "checked"])
async def test_every_bring_up_starts_from_env_setups_environment(ready):
    # ensure_venv returns early for a hand-written env_setup and for a venv this process checked: both still reset
    cli = _cli(_line()) if ready else _cli("module load gce")
    if ready:
        cli.venv_ready = cli.venv.key
    cli._prefix_override = 'export PATH="/old/venv/bin:$PATH" && '  # what a teardown chose
    await cli.ensure_venv()
    assert cli._env_prefix() == (_line() if ready else "module load gce") + " && "


async def test_an_endpoint_started_this_session_is_stopped_through_what_it_was_started_with(tmp_path, monkeypatch):
    # provision records the profile's line as the endpoint's own: a hand-written one is used as given, with no search
    # (a leftover hpc-bridge venv on the node is not preferred over it)
    home = tmp_path / "home"
    _logging_gce(home / "hpc-bridge" / "gce-venv" / "bin")  # a leftover from an earlier bring-up
    site = _site_gce(home)
    fac, home = _reconnected(tmp_path, monkeypatch, current=site, started=None)
    fac._adopted, fac.endpoint_worker_init = "eid-1", site  # as provision leaves it after its own start

    async def no_search(*a, **k):
        raise AssertionError("searched")

    monkeypatch.setattr(ev, "find_working", no_search)
    res = await fac.teardown("eid-1")
    assert res["stopped"] is True, res
    assert [c for c in (home / "gce-calls").read_text().splitlines() if not c.startswith("site ")] == []


async def test_started_with_no_env_setup_runs_the_bare_cli_with_no_search(monkeypatch):
    async def no_search(*a, **k):
        raise AssertionError("searched")

    monkeypatch.setattr(ev, "find_working", no_search)
    cli = _cli("module load gce")
    assert await cli.use_working_venv("") == (True, "") and cli._env_prefix() == ""
    cli = _cli("")
    assert await cli.use_working_venv("") == (True, "") and cli._env_prefix() == ""


@pytest.mark.parametrize("how", ["unknown", "timeout"])
async def test_an_inconclusive_search_uses_the_started_line_else_env_setup(monkeypatch, how):
    async def inconclusive(run, mv, extra=()):
        if how == "timeout":
            raise TimeoutError()
        return ev.Found(0, None, True, "")

    monkeypatch.setattr(ev, "find_working", inconclusive)
    started = ev.short_env_setup("/home/u/hpc-bridge/gce-venv", python="3.12", gce="4.18.0")
    cli = _cli("module load gce")
    assert await cli.use_working_venv(started) == (True, "") and cli._env_prefix() == started + " && "
    cli = _cli("module load gce")  # the start unknown, env_setup not ours: env_setup, never hpc-bridge's default line
    assert await cli.use_working_venv(None) == (True, "") and cli._env_prefix() == "module load gce && "
    cli = _cli(_line())  # the start unknown, env_setup ours: env_setup
    assert await cli.use_working_venv(None) == (True, "") and cli._env_prefix() == _line() + " && "


@pytest.mark.parametrize("started", ["older key", "site prefix"])
async def test_a_checked_venv_is_not_assumed_for_another_started_line(monkeypatch, started):
    # venv_ready covers env_setup's venv only: a start under another key, or behind a site prefix, is searched
    seen = []

    async def fake_find(run, mv, extra=()):
        seen.append(mv)
        return ev.Found(0, mv.key, False, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    cli = _cli(_line())
    cli.venv_ready = cli.venv.key
    line = (ev.short_env_setup("/home/u/hpc-bridge/gce-venv", gce="4.0.0") if started == "older key"
            else "module load python; " + _line())
    assert await cli.use_working_venv(line) == (True, "")
    assert len(seen) == 1 and cli._env_prefix() == line + " && "
    seen.clear()
    assert await cli.use_working_venv(_line()) == (True, "") and seen == []  # exactly env_setup's: no search


@pytest.mark.parametrize("answer", ["keyed", "unknown"])
async def test_a_line_an_earlier_plugin_generated_is_never_run_even_through_the_line(monkeypatch, answer):
    from tests.test_env_setup import _OLD_UV

    async def fake_find(run, mv, extra=()):
        return ev.Found(0, mv.key, False, "") if answer == "keyed" else ev.Found(0, None, True, "")

    monkeypatch.setattr(ev, "find_working", fake_find)
    cli = _cli("")
    assert await cli.use_working_venv(_OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv")) == (True, "")
    prefix = cli._env_prefix()
    assert ev.is_line(prefix.removesuffix(" && ")) and "pip install" not in prefix and "uv venv" not in prefix


# ---- texttable 1.7.2 broke `globus-compute-endpoint list` (released 2026-10-18; live on the fake cluster 2026-10-20) ---
# gce 4.18's `Endpoint.print_endpoint_table` calls `_compute_cols_width()` and then reads `Texttable._width`, which 1.7.2
# no longer sets: `list` raises AttributeError whenever an endpoint is configured, and hpc-bridge finds its endpoint's id
# with `list`. The endpoint leaves texttable unbounded, so every venv built since then broke every SSH bring-up, and the
# check (`version` + the python) passed it. Now every build pins `texttable<1.7.2`, the check runs what `list` runs on
# texttable, and a venv that fails it is rebuilt beside and renamed in, as any broken venv is.

_ATTR = "AttributeError: 'Texttable' object has no attribute '_width'"


@pytest.mark.parametrize("method", ["uv", "bootstrap"])
@pytest.mark.parametrize(("gce", "sdk"), [(PIN, SDK), (PIN, None), (None, None)], ids=["pinned", "no-sdk", "unpinned"])
def test_every_build_pins_texttable_in_the_endpoints_own_install(method, gce, sdk):
    s = ev.build_script(_mv(), token="t0", method=method, gce=gce, python=PY, sdk=sdk)
    installs = re.findall(r'"\$u" pip install [^&]*', s)
    assert len(installs) == 1, s  # one install: the endpoint and the pin resolve together
    words = shlex.split(installs[0])
    assert "texttable<1.7.2" in words
    assert (f"globus-compute-endpoint=={gce}" if gce else "globus-compute-endpoint") in words
    assert "'texttable<1.7.2'" in installs[0]  # quoted: a bare `<` is a redirect to the shell
    assert (method == "bootstrap") == ("astral.sh" in s)  # the bootstrap path installs uv first, then the same install


async def test_a_venv_whose_texttable_breaks_list_is_rebuilt_beside_and_renamed_in():
    r = Remote("oldtable", arch="x86_64")
    res = await _ensure(r)
    assert r.steps == ["check", "build", "verify", "claim"]  # the module's repair: built beside, verified, renamed in
    assert res.built and res.moved_aside is not None and r.venv == "ok"
    assert res.reason == f"its texttable 1.7.2 breaks `globus-compute-endpoint list` ({_ATTR})"
    assert "'texttable<1.7.2'" in r.script("build")


@pytest.mark.parametrize("state", ["ok", "notable"], ids=["texttable-1.7.1", "no-texttable"])
async def test_a_venv_whose_texttable_serves_list_or_that_has_none_costs_one_check(state):
    r = Remote(state, arch="x86_64")
    res = await _ensure(r)
    assert r.steps == ["check"] and not res.built


async def test_an_unkeyed_venv_whose_texttable_breaks_list_is_never_changed_and_says_why():
    # the unkeyed {venv} may be the user's own (a login node without internet): it is never repaired, the keyed venv is
    # built instead, and the bring-up says why the unkeyed one was passed over
    r = Remote("missing", unkeyed="oldtable")
    res = await _ensure(r)
    assert r.steps == ["check", "check unkeyed", "build", "verify", "claim"] and res.path == _mv().key
    assert f"{_mv().base} was not used: its texttable 1.7.2 breaks" in res.reason
    assert all(f'"{_mv().base}"' not in s for st, s in r.scripts if st != "check unkeyed")  # only ever read
    r = Remote("missing", unkeyed="oldtable", build_rc=1, build_err="error: Failed to fetch: `https://pypi.org/x`")
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    assert f"{_mv().base} was not used: its texttable 1.7.2 breaks `globus-compute-endpoint list` ({_ATTR})." \
        in str(e.value) and str(e.value).endswith("Nothing was started or billed.")


def test_the_check_runs_what_list_runs_on_texttable_in_one_line():
    script = ev.check_script(_mv())
    assert "\n" not in script  # still one line, as the check always was
    for call in ("tb._compute_cols_width()", "tb._width[2] = max(10, tb._width[2])", "tb.set_max_width(68)"):
        assert call in ev._PY_PROBE
    assert "grep '^TEXTTABLE'" in script
    assert re.search(r"\bf['\"]", ev._PY_PROBE) is None  # no f-strings: an old python still reports its version
    s = ev.parse_check("HPCB_VENV_BEGIN\nEXISTS=1\nPYVENV=1\nENTRY=1\nVERSION_RC=0\n"
                       "VERSION=Globus Compute endpoint version: 4.18.0\nPY_RC=0\nTEXTTABLE=broken\n"
                       f"TEXTTABLE_VERSION=1.7.2\nTEXTTABLE_ERROR={_ATTR}\nPY=(3, 13)\nHPCB_VENV_END\n")
    want = f"its texttable 1.7.2 breaks `globus-compute-endpoint list` ({_ATTR})"
    assert ev.problem(s, gce="4.18.0", python="3.13") == want
    assert ev.problem(s, gce="4.18.0", python="3.12") == "its python is 3.13, not 3.12"  # the python is named first


@pytest.mark.parametrize(("texttable", "want"), [("1.7.1", "ok"), ("1.7.2", "broken"), (None, "absent")])
def test_the_probe_itself_against_a_stub_texttable_on_pythonpath(tmp_path, texttable, want):
    import sys

    lib: dict = {}
    exec(compile(vs.LIB, "hpcb_stub", "exec"), lib)  # noqa: S102 - the tests' own stub library
    site = tmp_path / "site"
    site.mkdir()
    if texttable:
        body = "self._width = widths" if texttable == "1.7.1" else "return widths"
        (site / "texttable.py").write_text(lib["TEXTTABLE"] % (texttable, body))
    # -S: no site-packages, so this interpreter's own texttable stays out; the stub comes in on PYTHONPATH. A
    # texttable.py in the cwd (the login shell's: $HOME) is never what it reads, as an entry point never reads it.
    (tmp_path / "texttable.py").write_text("raise SystemExit('the cwd was imported')\n")
    r = subprocess.run([sys.executable, "-S", "-c", f"exec({ev._PY_PROBE!r})"], capture_output=True, text=True,
                       cwd=tmp_path, env={"PYTHONPATH": str(site), "PATH": "/usr/bin:/bin"}, check=False)
    assert r.returncode == 0, r.stderr
    s = ev.parse_check("HPCB_VENV_BEGIN\n" + r.stdout + "HPCB_VENV_END\n")
    assert s.texttable == want and r.stdout.splitlines()[-1] == str(sys.version_info[:2])
    assert (s.texttable_error == _ATTR) is (want == "broken") and s.texttable_version == (texttable or "")
    # it came in on PYTHONPATH, outside this interpreter's prefix: named, and marked as from outside
    assert s.texttable_outside is bool(texttable) and s.texttable_file == (str((site / "texttable.py").resolve())
                                                                           if texttable else "")


# -- the same in a real shell, with the stub texttable in the venvs (its `list` draws the table as gce 4.18 does)

def _list(v: Path, home: Path, endpoint: str = "hpc-bridge-login") -> subprocess.CompletedProcess:
    (home / ".globus_compute" / endpoint).mkdir(parents=True, exist_ok=True)
    return subprocess.run([str(v / "bin" / "globus-compute-endpoint"), "list"], capture_output=True, text=True,
                          env={"HOME": str(home), "PATH": "/usr/bin:/bin"}, check=False)


async def test_real_a_venv_built_now_runs_list(box):
    res = await _real(box)
    assert res.built
    install = next(c for c in vs.calls(box) if c[:3] == ["uv", "pip", "install"])
    assert "texttable<1.7.2" in install  # the `<` reached uv: no redirect, no stray file named 1.7.2
    assert not list(box["home"].rglob("1.7.2")) and not list(box["tmp"].glob("1.7.2"))
    r = _list(box["key"], box["home"])
    assert r.returncode == 0 and "hpc-bridge-login" in r.stdout, r.stderr


async def test_real_a_venv_whose_texttable_breaks_list_is_rebuilt(box):
    v = vs.venv(box, box["key"], PIN, texttable="1.7.2")  # built 2026-10-18 on: `version` runs, `list` does not
    assert _ATTR in _list(v, box["home"]).stderr
    res = await _real(box)
    assert res.built and "texttable 1.7.2 breaks" in res.reason and not (v / "sentinel").exists()
    assert (Path(res.moved_aside) / "sentinel").read_text() == "old"  # kept, as every broken venv is
    assert _list(box["key"], box["home"]).returncode == 0 and _relocatable(box["key"])
    assert (box["key"] / ".hpcb-arch").read_text().strip() == os.uname().machine


@pytest.mark.parametrize("texttable", ["1.7.1", None], ids=["texttable-1.7.1", "no-texttable"])
async def test_real_a_venv_whose_texttable_serves_list_or_that_has_none_is_left_alone(box, texttable):
    v = vs.venv(box, box["key"], PIN, texttable=texttable)
    res = await _real(box)
    assert not res.built and vs.calls(box) == [] and (v / "sentinel").exists()


async def test_real_an_unkeyed_venv_whose_texttable_breaks_list_is_never_changed(box):
    u = vs.venv(box, box["venv"], PIN, texttable="1.7.2")  # the user's own, perhaps
    theirs = next((u / "lib").rglob("texttable.py"))
    before = theirs.read_text()
    res = await _real(box)
    assert res.built and res.path == str(box["key"]) and "was not used: its texttable 1.7.2" in res.reason
    assert (u / "sentinel").exists() and theirs.read_text() == before
    assert _list(box["key"], box["home"]).returncode == 0


# -- the list failure says what it is

_LIST_TRACEBACK = """Traceback (most recent call last):
  File "/home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13/bin/globus-compute-endpoint", line 12, in <module>
    sys.exit(cli_run())
             ~~~~~~~^^
  File "/home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13/lib/python3.13/site-packages/click/core.py", line 1485, \
in __call__
    return self.main(*args, **kwargs)
  File "/home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13/lib/python3.13/site-packages/globus_compute_endpoint/\
cli.py", line 1095, in list_endpoints
    Endpoint.print_endpoint_table(compute_dir)
  File "/home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13/lib/python3.13/site-packages/globus_compute_endpoint/\
endpoint/endpoint.py", line 906, in print_endpoint_table
    if table._width[idx_id] < width_id:
       ^^^^^^^^^^^^
AttributeError: 'Texttable' object has no attribute '_width'
"""


def _gce_answers(monkeypatch, answers: dict[str, tuple[int, str, str]]) -> None:
    from hpc_bridge.facility import remote

    async def fake_ssh(target, cmd, **kw):
        for word, answer in answers.items():
            if f"globus-compute-endpoint {word}" in cmd:
                return answer
        raise AssertionError(cmd)

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)


async def test_a_list_that_raises_says_its_exception_and_the_texttable_fix(monkeypatch):
    from hpc_bridge.notices import _explain_provision_error

    _gce_answers(monkeypatch, {"list": (1, "", _LIST_TRACEBACK)})
    with pytest.raises(RuntimeError) as e:
        await _cli(_line()).endpoint_id("hpc-bridge-login")
    msg = str(e.value)
    assert msg.startswith(f"remote list failed: {_ATTR} (at endpoint/endpoint.py:906, in print_endpoint_table). "), msg
    assert "texttable 1.7.2 breaks globus-compute-endpoint's `list`" in msg and "pip install 'texttable<1.7.2'" in msg
    notice = _explain_provision_error(e.value)  # what the agent reads: the notice's 500-character cut keeps all of it
    assert _ATTR in notice and notice.endswith("run `pip install 'texttable<1.7.2'` there."), notice
    assert "Traceback" not in notice


async def test_a_list_failure_that_is_not_this_one_is_quoted_as_before(monkeypatch):
    from hpc_bridge.notices import _explain_provision_error

    _gce_answers(monkeypatch, {"list": (255, "", "Host key verification failed.")})
    with pytest.raises(RuntimeError) as e:
        await _cli(_line()).endpoint_id("hpc-bridge-login")
    assert str(e.value) == "remote list failed: Host key verification failed."  # what the notices classify
    assert _explain_provision_error(e.value, host="h", user="u").startswith("UNKNOWN HOST KEY for h")
    other = _LIST_TRACEBACK.replace(_ATTR, "ImportError: cannot import name 'X' from 'globus_sdk'")
    _gce_answers(monkeypatch, {"list": (1, "", other)})
    with pytest.raises(RuntimeError) as e:
        await _cli(_line()).endpoint_id("hpc-bridge-login")
    assert "ImportError: cannot import name 'X' from 'globus_sdk'" in str(e.value) and "texttable" not in str(e.value)


async def test_a_refused_start_whose_list_fails_too_says_both(monkeypatch):
    # gce 4.13 refuses a second start with exit 73 and nothing on either stream: before, that was the whole error
    _gce_answers(monkeypatch, {"start": (73, "", ""), "list": (1, "", _LIST_TRACEBACK)})
    cli = _cli(_line())
    with pytest.raises(RuntimeError) as e:
        await cli.start("hpc-bridge-login")
    msg = str(e.value)
    assert msg.startswith(f"remote start failed (rc=73): no output; `list` failed too: {_ATTR} (at "), msg
    assert msg.endswith("run `pip install 'texttable<1.7.2'` there.")
    from hpc_bridge.notices import _explain_provision_error

    assert _explain_provision_error(e.value).endswith("there."), "the notice's 500-character cut took the fix"
    _gce_answers(monkeypatch, {"list": (0, "No endpoints configured!", "")})
    assert await cli.status("hpc-bridge-login") is None and cli.last_list_error is None  # a list that works clears it


async def test_real_the_list_failure_of_a_venv_hpc_bridge_does_not_manage(box, monkeypatch):
    # the stub's own Python traceback, through `endpoint_id` as a bring-up runs it, in a venv behind a hand-written
    # env_setup: hpc-bridge never checks or repairs that one, so the error is all the user gets
    from hpc_bridge.facility import remote

    v = vs.venv(box, box["tmp"] / "theirs", PIN, texttable="1.7.2")
    (box["home"] / ".globus_compute" / "hpc-bridge-login").mkdir(parents=True)

    async def local_ssh(target, cmd, timeout=None, **kw):
        p = await asyncio.create_subprocess_exec("bash", "-c", cmd, env={**vs.env(box), "HOME": str(box["home"])},
                                                 stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(p.communicate(), 60)
        return p.returncode or 0, out.decode(), err.decode()

    monkeypatch.setattr(remote, "ssh_exec", local_ssh)
    cli = _cli(f'export PATH="{v}/bin:$PATH"')
    assert cli.venv is None
    with pytest.raises(RuntimeError) as e:
        await cli.endpoint_id("hpc-bridge-login")
    msg = str(e.value)
    assert msg.startswith(f"remote list failed: {_ATTR} (at globus_compute_endpoint/cli.py:"), msg
    assert "pip install 'texttable<1.7.2'" in msg


# -- review round 1 (2026-10-20)

def test_the_pin_and_the_probe_go_together():
    # The probe tests texttable alone, never the endpoint: a build without the pin would take texttable 1.7.2 and fail
    # its own verify on every bring-up (a .failed.<ts> each). Drop both together, once globus-compute-endpoint stops
    # reading `_width`, or neither.
    probes = "tb._width" in ev._PY_PROBE
    pins = "'texttable<1.7.2'" in ev.build_script(_mv(), token="t0", method="uv", gce=PIN, python=PY, sdk=SDK)
    assert probes == pins, "the probe's texttable calls and TEXTTABLE_PIN come out together, or neither does"


def test_the_probe_draws_the_table_as_list_does(tmp_path):
    # gce's `list` ends in `draw()`: a texttable that has `_width` but cannot draw with gce's widths breaks it too
    import sys

    adjust = ev._PY_PROBE.index("tb._width[2] = max(10, tb._width[2])")
    assert ev._PY_PROBE.index("tb.draw()") > adjust
    site = tmp_path / "site"
    site.mkdir()
    (site / "texttable.py").write_text(
        "__version__ = '9.9'\nclass Texttable:\n    def header(self, r): self._width = [1] * len(r)\n"
        "    def add_row(self, r): pass\n    def set_max_width(self, w): pass\n"
        "    def _compute_cols_width(self): pass\n"
        "    def draw(self): raise ValueError('cannot draw with these widths')\n")
    r = subprocess.run([sys.executable, "-S", "-c", f"exec({ev._PY_PROBE!r})"], capture_output=True, text=True,
                       env={"PYTHONPATH": str(site), "PATH": "/usr/bin:/bin"}, check=False)
    s = ev.parse_check("HPCB_VENV_BEGIN\n" + r.stdout + "HPCB_VENV_END\n")
    assert s.texttable == "broken" and s.texttable_error == "ValueError: cannot draw with these widths", r.stdout


async def test_a_texttable_shadowed_from_outside_the_venv_is_named_and_never_rebuilt():
    # a site module's PYTHONPATH puts texttable 1.7.2 ahead of the venv's own 1.7.1: `list` breaks with the same
    # environment, and no build can change that — so nothing is built, every connect, and the file is named
    r = Remote("shadowed", arch="x86_64")
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    msg = str(e.value)
    assert r.steps == ["check"], r.steps  # no build, no .failed copy, no move aside
    named = "texttable 1.7.2 from /apps/python/3.13/lib/site-packages/texttable.py, outside hpc-bridge's endpoint venv "
    assert named + _mv().key in msg
    assert "shadowed from outside the venv (PYTHONPATH" in msg and "a rebuild cannot help" in msg
    assert msg.endswith("Nothing was started or billed.")


async def test_a_first_build_that_is_shadowed_is_claimed_once_and_then_named_without_rebuilding():
    r = Remote("missing", built="shadowed")
    with pytest.raises(EndpointVenvError, match="shadowed from outside the venv"):
        await _ensure(r)
    assert r.steps == ["check", "check unkeyed", "build", "verify", "claim"] and r.venv == "shadowed"
    r.steps.clear()
    with pytest.raises(EndpointVenvError, match="shadowed from outside the venv"):
        await _ensure(r)  # the next connect: said at its check, no second build
    assert r.steps == ["check"]


async def test_a_prepared_unkeyed_venv_that_is_shadowed_is_named_not_passed_over():
    r = Remote("missing", unkeyed="shadowed")
    with pytest.raises(EndpointVenvError, match=f"outside hpc-bridge's endpoint venv {_mv().base} "):
        await _ensure(r)
    assert r.steps == ["check", "check unkeyed"]


async def test_a_build_that_got_the_broken_texttable_despite_the_pin_says_the_pin_was_overridden():
    r = Remote("missing", built="oldtable")  # a uv override on the login node: the build has 1.7.2 inside
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    assert "fails its check: its texttable 1.7.2 breaks" in str(e.value)
    assert "asked for 'texttable<1.7.2' and got 1.7.2 all the same: a uv override or constraint" in str(e.value)


@pytest.mark.parametrize(("tools", "refusal"), [("none", "no uv, no curl and no python3"), ("olduv", "is too old")])
async def test_a_refusal_to_build_says_why_the_prepared_venv_was_passed_over(tools, refusal):
    r = Remote("missing", unkeyed="oldtable", tools=tools)
    with pytest.raises(EndpointVenvError) as e:
        await _ensure(r)
    msg = str(e.value)
    assert refusal in msg and f"{_mv().base} was not used: its texttable 1.7.2 breaks" in msg, msg
    assert msg.endswith("Nothing was started or billed.") and r.steps == ["check", "check unkeyed"]


async def test_real_a_texttable_on_pythonpath_shadowing_the_venvs_is_named_and_nothing_is_built(box):
    v = vs.venv(box, box["key"], PIN, texttable="1.7.1")  # the venv's own texttable serves `list`
    site = box["tmp"] / "site-module"
    site.mkdir()
    (site / "texttable.py").write_text(box["lib"]["TEXTTABLE"] % ("1.7.2", "return widths"))
    with pytest.raises(EndpointVenvError) as e:
        await _real(box, PYTHONPATH=str(site))
    msg = str(e.value)
    assert f"texttable 1.7.2 from {(site / 'texttable.py').resolve()}, outside" in msg and "PYTHONPATH" in msg, msg
    assert vs.calls(box) == [] and (v / "sentinel").exists() and _leftovers(box) == []  # nothing built or moved
    assert _list(v, box["home"]).returncode == 0  # without that PYTHONPATH, the venv's own texttable serves `list`
