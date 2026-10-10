"""The self-provisioning env_setup that discovery proposes, run in a REAL shell (bash, and POSIX sh/dash where
installed) against a temp home.

Found live on globus1 (2026-10-10): the old guards (`[ -d {venv} ] ||`, `command -v globus-compute-endpoint ||`)
kept a venv whose globus-sdk had drifted, so the gce CLI failed at import on every first bring-up; on a heterogeneous
cluster (x86_64 login, aarch64 compute, shared /home) the replayed worker_init could not run the venv's python and the
block died with a bare exit 2. The stubs behave like the real install: `bin/globus-compute-endpoint` is uv's /bin/sh
trampoline to the venv's python and prints the real `version` line, and the stub `cli` reads `sys.argv[0]` at import
as gce 4.12-4.18 do (so `python -c "import …cli"` fails, as it does for real). `uv`, `pip` and `curl` are stubs on
PATH that record every call; `python -m venv` is real. `HPCB_TEST_REAL_GCE_VENV=<a real gce venv>` adds a run against
a real install.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from hpc_bridge import discovery
from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.facility.remote import RemoteEndpointCLI, SshTarget

PY = f"{sys.version_info.major}.{sys.version_info.minor}"
PIN, OLD, SDK = "4.16.0", "4.13.0", "4.9.1"
SEED = Path(__file__).resolve().parents[1] / "src/hpc_bridge/catalog/seed/sdsc-expanse.yaml"
_seen: dict[str, str] = {}
for _s in ("bash", "sh", "dash"):  # bash (`bash -lc`, Slurm/PBS job scripts); sh and dash once per real binary
    if (_p := shutil.which(_s)):
        _seen.setdefault(os.path.realpath(_p), _s)
SHELLS = list(_seen.values())

# Shared by the stubs and the test. A venv is a copy of one real `python -m venv` made per module (its `activate`
# re-pointed at the new path): a real interpreter in a real venv, without paying venv creation in every test.
_LIB = r"""
import json, os, shutil, sys, time

def purelib(venv):
    return os.path.join(venv, "lib", "python%d.%d" % sys.version_info[:2], "site-packages")

def _write(path, text, mode=None):
    with open(path, "w") as fh:
        fh.write(text)
    if mode:
        os.chmod(path, mode)

def install_gce(venv, version, broken=False):
    site = purelib(venv)
    pkg = os.path.join(site, "globus_compute_endpoint")
    os.makedirs(pkg, exist_ok=True)
    _write(os.path.join(pkg, "__init__.py"), f"__version__ = {version!r}\n")
    cli = 'import sys\n_PROG = sys.argv[0].rsplit("/", 1)[1]  # gce 4.12-4.18 read argv[0] at import\n'
    if broken:
        cli += "raise ImportError(\"cannot import name 'X' from 'globus_sdk' (stub)\")\n"
    cli += ('def main():\n    if sys.argv[1:] == ["version"]:\n        import globus_compute_endpoint as g\n'
            '        print(f"Globus Compute endpoint version: {g.__version__}")\n        return 0\n    return 2\n')
    _write(os.path.join(pkg, "cli.py"), cli)
    for d in os.listdir(site):
        if d.startswith("globus_compute_endpoint-") and d.endswith(".dist-info"):
            shutil.rmtree(os.path.join(site, d))
    os.makedirs(os.path.join(site, f"globus_compute_endpoint-{version}.dist-info"))
    _write(os.path.join(site, f"globus_compute_endpoint-{version}.dist-info", "METADATA"),
           f"Metadata-Version: 2.1\nName: globus-compute-endpoint\nVersion: {version}\n")
    py = os.path.join(venv, "bin", "python")  # the console script exactly as uv writes it: a /bin/sh trampoline
    _write(os.path.join(venv, "bin", "globus-compute-endpoint"),
           "#!/bin/sh\n'''exec' '%s' \"$0\" \"$@\"\n' '''\nimport sys\n"
           "from globus_compute_endpoint.cli import main\nsys.exit(main())\n" % py, 0o755)

def log(*argv):
    with open(os.environ["HPCB_TEST_LOG"], "a") as fh:
        fh.write(json.dumps(list(argv)) + "\n")

def do_install(py, reqs):
    for r in reqs:
        if r.startswith("globus-compute-endpoint=="):
            install_gce(os.path.dirname(os.path.dirname(py)), r.split("==", 1)[1],
                        broken=os.environ.get("HPCB_TEST_INSTALL") == "broken")

def make_venv(path):
    template = os.environ["HPCB_TEST_TEMPLATE"]
    shutil.copytree(template, path, symlinks=True)
    for name in os.listdir(os.path.join(path, "bin")):
        f = os.path.join(path, "bin", name)
        if name.startswith("activate") and not os.path.islink(f):
            with open(f) as fh:
                text = fh.read()
            _write(f, text.replace(template, path))

def make_template(path, stubdir):
    import subprocess
    subprocess.check_call([sys.executable, "-m", "venv", "--without-pip", path])
    pipdir = os.path.join(purelib(path), "pip")  # `python -m pip` runs this stub
    os.makedirs(pipdir)
    _write(os.path.join(pipdir, "__init__.py"), "")
    _write(os.path.join(pipdir, "__main__.py"),
           f"import sys; sys.path.insert(0, {stubdir!r}); import hpcb_stub; hpcb_stub.pip_main(sys.argv[1:])\n")

def uv_main(args):
    log("uv", *args)
    if args[:1] == ["venv"]:
        path, skip = None, False
        for a in args[1:]:
            if skip:
                skip = False
            elif a == "--python":
                skip = True
            elif not a.startswith("-"):
                path = a
        if os.environ.get("HPCB_TEST_UV_FAIL") == "venv":
            sys.exit("uv stub: error: no interpreter found (as told)")
        if os.environ.get("HPCB_TEST_SLOW"):
            time.sleep(1.5)  # widen the window two concurrent builds race in
        make_venv(path)
    elif args[:2] == ["pip", "install"]:
        rest = args[2:]
        py = rest[rest.index("--python") + 1]
        do_install(py, [a for a in rest if not a.startswith("-") and a != py])
    else:
        sys.exit(f"uv stub: unexpected {args}")

def pip_main(args):
    log("pip", sys.executable, *args)
    do_install(sys.executable, [a for a in args[1:] if not a.startswith("-")])

def python3_main(args):  # the pip variant's `python3 -m venv "$__hb_venv"`
    if args[:2] == ["-m", "venv"]:
        log("python3", *args)
        make_venv(args[2])
    else:
        os.execv(sys.executable, [sys.executable, *args])
"""


@pytest.fixture(scope="module")
def stubs(tmp_path_factory) -> dict:
    """The stub tools (`uv`, `python3`, `curl`) and the template venv, once per module."""
    root = tmp_path_factory.mktemp("env_setup_stubs")
    stub = root / "stub"
    stub.mkdir()
    (stub / "hpcb_stub.py").write_text(_LIB)
    for name, fn in (("uv", "uv_main"), ("python3", "python3_main")):
        p = stub / name
        p.write_text(f"#!{sys.executable}\nimport sys; sys.path.insert(0, {str(stub)!r}); import hpcb_stub; "
                     f"hpcb_stub.{fn}(sys.argv[1:])\n")
        p.chmod(0o755)
    shutil.copy(stub / "uv", root / "uv-dist")  # what the astral installer "installs" into ~/.local/bin
    curl = stub / "curl"
    curl.write_text(f"#!/bin/sh\necho curl \"$@\" >> \"$HPCB_TEST_CURL\"\n"
                    f"echo 'mkdir -p \"$HOME/.local/bin\" && cp {root}/uv-dist \"$HOME/.local/bin/uv\"'\n")
    curl.chmod(0o755)
    lib: dict = {}
    exec(compile(_LIB, "hpcb_stub", "exec"), lib)  # noqa: S102 - the test's own stub library
    lib["make_template"](str(root / "template"), str(stub))
    return {"stub": stub, "template": root / "template", "lib": lib}


@pytest.fixture
def box(tmp_path: Path, stubs: dict, monkeypatch) -> dict:
    """A home (no ~/hpc-bridge yet) and a PATH of the stub tools only. `venv` is the {venv} base; `key` the keyed
    venv this client's line uses."""
    stub = tmp_path / "stub"  # per test: a test may remove a tool
    shutil.copytree(stubs["stub"], stub)
    home = tmp_path / "home"
    home.mkdir()
    (home / "keep.txt").write_text("not yours")
    monkeypatch.setenv("HPCB_TEST_TEMPLATE", str(stubs["template"]))  # for _venv() in this process
    base = home / "hpc-bridge" / "gce-venv"
    return {"tmp": tmp_path, "stub": stub, "home": home, "venv": base, "key": _key(base),
            "log": tmp_path / "calls.jsonl", "curl": tmp_path / "curl.log", "lib": stubs["lib"],
            "template": stubs["template"]}


def _setup(variant: str, python: str = PY) -> str:
    if variant == "uv":
        return discovery.uv_env_setup(python=python, gce=PIN, sdk=SDK)
    if variant == "bootstrap":
        return discovery.uv_env_setup(python=python, gce=PIN, sdk=SDK, bootstrap=True)
    return discovery.pip_env_setup(python=python, gce=PIN, sdk=SDK)


def _key(base: Path, gce: str = PIN, py: str = PY) -> Path:
    """The keyed venv beside `base` (the `{venv}`) for one endpoint version and Python."""
    return Path(discovery.keyed_venv(str(base), python=py, gce=gce))


def _venv(box: dict, path: Path | None = None, version: str = PIN, broken: bool = False) -> Path:
    """An existing venv (what an earlier run left), with a sentinel a rebuild would remove."""
    v = path or box["key"]
    v.parent.mkdir(parents=True, exist_ok=True)
    box["lib"]["make_venv"](str(v))
    box["lib"]["install_gce"](str(v), version, broken=broken)
    (v / "sentinel").write_text("old")
    return v


def _cmd(box: dict, variant: str, *, flag: str | None, worker: str | None, python: str = PY,
         setup: str | None = None) -> str:
    es = (setup or _setup(variant, python)).replace("{venv}", str(box["venv"]))
    if worker:  # parsl's job script: worker_init on its own lines, then the worker launch runs regardless
        return f'{es}\necho "HPCB_DONE $VIRTUAL_ENV"\n'
    if flag is None:  # a plugin that sets no flag (one released before this line existed)
        return f'{es} && echo "HPCB_DONE $VIRTUAL_ENV"'
    # the plugin's own `bash -lc "<prefix><env_setup> && globus-compute-endpoint …"`
    return RemoteEndpointCLI(SshTarget("h", "u"), es)._env_prefix(repair=flag == "1") + 'echo "HPCB_DONE $VIRTUAL_ENV"'


def _env(box: dict, *, worker: str | None = None, install: str = "ok", path_extra: str = "", **more: str) -> dict:
    env = {"PATH": f"{path_extra}{box['stub']}:/usr/bin:/bin", "HOME": str(box["home"]), "LANG": "C",
           "HPCB_TEST_LOG": str(box["log"]), "HPCB_TEST_CURL": str(box["curl"]), "HPCB_TEST_INSTALL": install,
           "HPCB_TEST_TEMPLATE": str(box["template"]), **more}
    if worker:
        env[worker] = "1108"
        env["HPCB_ENV_REPAIR"] = "1"  # even if the flag leaked into the job's environment (a `set -a` profile)
    return env


def _run(box: dict, variant: str, shell: str, *, flag: str | None = "1", worker: str | None = None, python: str = PY,
         setup: str | None = None, **env: str) -> subprocess.CompletedProcess:
    cmd = _cmd(box, variant, flag=flag, worker=worker, python=python, setup=setup)
    return subprocess.run([shell, "-c", cmd], env=_env(box, worker=worker, **env), capture_output=True, text=True,
                          timeout=120, check=False)


def _activated(r: subprocess.CompletedProcess) -> str | None:
    """The venv the line left activated (the HPCB_DONE line's $VIRTUAL_ENV), compared exactly: the unkeyed {venv}'s
    path is a prefix of every keyed one."""
    m = re.search(r"^HPCB_DONE (.*)$", r.stdout, re.M)
    return m.group(1) if m else None


def _calls(box: dict) -> list[list[str]]:
    return [json.loads(ln) for ln in box["log"].read_text().splitlines()] if box["log"].exists() else []


def _built(box: dict) -> bool:
    return any(c[:2] in (["uv", "venv"], ["python3", "-m"]) for c in _calls(box))


def _version(v: Path) -> str | None:
    r = subprocess.run([str(v / "bin" / "globus-compute-endpoint"), "version"], capture_output=True, text=True,
                       check=False)
    return r.stdout.strip().rsplit(": ", 1)[-1] if r.returncode == 0 else None


def _leftovers(box: dict) -> list[str]:
    parent = box["venv"].parent
    return sorted(p.name for p in parent.iterdir() if re.search(r"\.(old|failed)\.\d+x*$|\.lock$", p.name)) \
        if parent.exists() else []


def _untouched(box: dict) -> None:
    assert (box["home"] / "keep.txt").read_text() == "not yours"
    for call in _calls(box):  # every path a build was handed lies inside the keyed venv
        for a in call:
            if a.startswith("/") and str(box["tmp"]) in a and "/stub/" not in a:
                assert a == str(box["key"]) or a.startswith(str(box["key"]) + "/"), call


VARIANTS = ["uv", "pip", "bootstrap"]
# Every shell runs every path of the text; the variants differ only in the build commands, so they run under bash.
SHELL_VARIANT = [(s, v) for s in SHELLS for v in VARIANTS if s == "bash" or v == "uv"]


# ---- why the check runs the entry point ---------------------------------------------------------------------------

def test_the_stub_cli_fails_under_python_c_like_the_real_one(box):
    # why the line checks through the entry point: gce's cli reads argv[0] at import, which is `-c` under `python -c`
    v = _venv(box)
    r = subprocess.run([str(v / "bin" / "python"), "-c", "import globus_compute_endpoint.cli"], capture_output=True,
                       text=True, check=False)
    assert r.returncode != 0 and "IndexError" in r.stderr
    assert _version(v) == PIN  # the entry point the manager runs works


# ---- a bring-up on the login node (HPCB_ENV_REPAIR=1, or a plugin that sets no flag) -----------------------------

@pytest.mark.parametrize(("shell", "variant"), SHELL_VARIANT)
def test_a_first_bring_up_builds_the_keyed_venv(box, shell, variant):
    r = _run(box, variant, shell)  # a fresh account: not even ~/hpc-bridge exists
    assert r.returncode == 0, r.stderr
    assert _activated(r) == str(box['key']) and _version(box["key"]) == PIN
    installs = [c for c in _calls(box) if "install" in c]
    assert len(installs) == 1 and f"globus-compute-endpoint=={PIN}" in installs[0] and f"globus-sdk=={SDK}" in installs[0]
    assert (box["key"] / ".hpcb-arch").read_text().strip() == os.uname().machine
    assert "hpc-bridge: building" in r.stderr  # why it built is said, not hidden
    assert _leftovers(box) == []
    _untouched(box)


@pytest.mark.parametrize(("shell", "variant"), SHELL_VARIANT)
def test_a_healthy_keyed_venv_costs_no_install(box, shell, variant):
    v = _venv(box)
    r = _run(box, variant, shell)
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr
    assert _calls(box) == [] and not box["curl"].exists()  # no uv, no pip, no network
    assert (v / "sentinel").read_text() == "old" and (v / ".hpcb-arch").exists()


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_plugin_that_sets_no_flag_gets_a_bring_up(box, shell):
    r = _run(box, "uv", shell, flag=None)
    assert r.returncode == 0 and _version(box["key"]) == PIN, r.stderr


@pytest.mark.parametrize(("shell", "variant", "old"),
                         [(s, v, {"broken": True}) for s, v in SHELL_VARIANT] + [("bash", "uv", {"version": OLD})])
def test_a_keyed_venv_that_fails_the_check_is_rebuilt(box, shell, variant, old):
    v = _venv(box, **old)
    r = _run(box, variant, shell)
    assert r.returncode == 0, r.stderr
    assert _built(box) and _version(v) == PIN and not (v / "sentinel").exists()
    assert _leftovers(box) == []  # the old venv moved aside, then removed
    reason = "cannot import name 'X'" if old.get("broken") else f"it reports {OLD}, not {PIN}"
    assert reason in r.stderr  # the failing check's reason is kept


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_pin_or_python_move_builds_beside_the_old_venv(box, shell):
    # a manager may still run from the old one: it is never touched
    legacy = _venv(box, path=box["venv"])                                    # the unkeyed venv older plugins used
    other = _venv(box, path=_key(box["venv"], OLD), version=OLD)             # another version's
    r = _run(box, "uv", shell)
    assert r.returncode == 0 and _version(box["key"]) == PIN, r.stderr
    assert (legacy / "sentinel").exists() and (other / "sentinel").exists() and _version(other) == OLD


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_failed_build_puts_the_old_venv_back(box, shell):
    # offline, quota, an unpublished pin: the account must not end up worse off
    v = _venv(box, version=OLD)
    r = _run(box, "uv", shell, HPCB_TEST_INSTALL="broken")
    assert r.returncode != 0 and "HPCB_DONE" not in r.stdout  # gce never runs on a broken venv
    assert f"could not build globus-compute-endpoint {PIN} (Python {PY}) in {v}: ImportError: cannot import" in r.stderr
    assert (v / "sentinel").read_text() == "old" and _version(v) == OLD  # restored
    assert _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_wrong_python_is_rebuilt_and_an_unbuildable_one_fails_loudly(box, shell):
    # the line asks for another minor; the stub uv can only make this interpreter, so the post-build check fails
    v = _venv(box, path=_key(box["venv"], PIN, "3.9"))
    r = _run(box, "uv", shell, python="3.9")
    assert r.returncode != 0 and "HPCB_DONE" not in r.stdout and _built(box)
    assert f"could not build globus-compute-endpoint {PIN} (Python 3.9) in {v}: its python is not 3.9" in r.stderr
    assert (v / "sentinel").exists()  # put back


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_cleanup_that_fails_on_nfs_does_not_fail_the_build(box, shell):
    # NFS keeps silly-renamed .nfsXXXX files of open .so files: rm -rf of the old venv fails, mv did not
    if os.geteuid() == 0:
        pytest.skip("root removes a read-only directory anyway: the failing cleanup cannot be simulated")
    v = _venv(box, broken=True)
    stuck = v / "lib" / "stuck"
    stuck.mkdir()
    (stuck / ".nfs0001").write_text("open elsewhere")
    stuck.chmod(0o500)
    try:
        r = _run(box, "uv", shell)
        assert r.returncode == 0 and _version(v) == PIN, r.stderr
        assert any(".old." in n for n in _leftovers(box))  # left behind, harmlessly
    finally:
        for p in box["venv"].parent.glob("*.old.*/lib/stuck"):
            p.chmod(0o700)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_directory_that_is_not_a_venv_is_never_replaced(box, shell):
    box["key"].mkdir(parents=True)
    (box["key"] / "precious").write_text("mine")
    r = _run(box, "uv", shell)
    assert r.returncode != 0 and "HPCB_DONE" not in r.stdout
    assert f"{box['key']} exists and is not a venv: move it away" in r.stderr
    assert (box["key"] / "precious").read_text() == "mine" and _calls(box) == [] and _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS)
def test_the_bootstrap_variant_installs_uv_only_to_build(box, shell):
    (box["stub"] / "uv").unlink()  # no uv anywhere: the astral installer runs, inside the build
    if shutil.which("uv", path="/usr/bin:/bin"):
        pytest.skip("a system uv on /usr/bin")
    r = _run(box, "bootstrap", shell)
    assert r.returncode == 0 and len(box["curl"].read_text().splitlines()) == 1, r.stderr
    box["curl"].unlink()
    r = _run(box, "bootstrap", shell)  # healthy now: no installer, no build
    assert r.returncode == 0 and not box["curl"].exists()


# ---- concurrent builds: a mkdir lock ------------------------------------------------------------------------------

@pytest.mark.parametrize("shell", SHELLS[:1])
def test_two_concurrent_first_builds_build_once(box, shell):
    cmd = _cmd(box, "uv", flag="1", worker=None)
    env = _env(box, HPCB_TEST_SLOW="1")
    procs = [subprocess.Popen([shell, "-c", cmd], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)]
    time.sleep(0.5)  # the first holds the lock while its (slow) build runs
    procs.append(subprocess.Popen([shell, "-c", cmd], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True))
    outs = [p.communicate(timeout=120) for p in procs]
    assert all(p.returncode == 0 for p in procs), outs
    assert sum(c[:2] == ["uv", "venv"] for c in _calls(box)) == 1  # the second waited, re-checked, built nothing
    assert _version(box["key"]) == PIN and _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_lock_left_by_a_dead_build_is_taken_over(box, shell):
    lock = Path(f"{box['key']}.lock")
    lock.mkdir(parents=True)
    stale = time.time() - 20 * 60  # older than any bring-up's ssh timeout
    os.utime(lock, (stale, stale))
    r = _run(box, "uv", shell)
    assert r.returncode == 0 and _version(box["key"]) == PIN and not lock.exists(), r.stderr


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_live_lock_is_waited_for_then_reported(box, shell):
    lock = Path(f"{box['key']}.lock")
    lock.mkdir(parents=True)  # fresh: another build is running
    fast = box["tmp"] / "fast"
    fast.mkdir()
    (fast / "sleep").write_text("#!/bin/sh\nexit 0\n")  # the bounded wait, without the wait
    (fast / "sleep").chmod(0o755)
    r = _run(box, "uv", shell, path_extra=f"{fast}:")
    assert r.returncode != 0 and f"{lock} is held by another build" in r.stderr
    assert _calls(box) == [] and lock.exists()  # not ours: left alone


# ---- other plugin calls (HPCB_ENV_REPAIR=0: list, stop, delete) never build ---------------------------------------

@pytest.mark.parametrize("shell", SHELLS)
def test_other_calls_use_the_keyed_venv_unchecked(box, shell):
    v = _venv(box, broken=True)  # even a broken one: a 30 s call never starts a build
    r = _run(box, "uv", shell, flag="0")
    assert r.returncode == 0 and _activated(r) == str(v)
    assert _calls(box) == [] and (v / "sentinel").exists() and not (v / ".hpcb-arch").exists()


@pytest.mark.parametrize("shell", SHELLS)
def test_a_teardown_of_an_endpoint_another_version_started_finds_its_venv(box, shell):
    # a new plugin adopts an old endpoint over the web (no bootstrap), then tears it down: its keyed venv was never
    # built, so stop/delete use the first working hpc-bridge venv — and never start a build that would be killed
    _venv(box, path=box["venv"], broken=True)  # the old unkeyed venv, broken (globus1)
    _venv(box, path=Path(f"{_key(box['venv'], '4.12.0')}.old.77"), version="4.12.0")  # moved aside: skipped
    _venv(box, path=Path(f"{_key(box['venv'], '4.12.1')}.failed.77x"), version="4.12.1")  # a failed build: skipped
    other = _venv(box, path=_key(box["venv"], OLD), version=OLD)
    r = _run(box, "uv", shell, flag="0")
    assert r.returncode == 0 and _activated(r) == str(other), r.stderr
    assert _calls(box) == [] and not box["key"].exists()


@pytest.mark.parametrize("shell", SHELLS)
def test_other_calls_with_no_working_venv_fail_without_building(box, shell):
    _venv(box, path=box["venv"], broken=True)
    r = _run(box, "uv", shell, flag="0")
    assert r.returncode != 0 and "HPCB_DONE" not in r.stdout
    assert f"no working hpc-bridge venv beside {box['venv']}" in r.stderr and _calls(box) == []


# ---- a compute worker (worker_init replays env_setup inside the batch job) ---------------------------------------

@pytest.mark.parametrize(("shell", "variant", "job"),
                         [(s, v, "SLURM_JOB_ID") for s, v in SHELL_VARIANT] + [("bash", "uv", "PBS_JOBID")])
def test_a_worker_uses_a_healthy_venv_without_building(box, shell, variant, job):
    v = _venv(box)
    r = _run(box, variant, shell, worker=job)
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr
    assert _calls(box) == [] and (v / "sentinel").exists()


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_never_checks_the_pin(box, shell):
    # the keyed venv its manager was configured with, whatever is inside: versions are the login node's business
    v = _venv(box, version=OLD)
    r = _run(box, "uv", shell, worker="SLURM_JOB_ID")
    assert r.returncode == 0 and _activated(r) == str(v) and _calls(box) == []


@pytest.mark.parametrize(("shell", "variant"), SHELL_VARIANT)
def test_a_worker_with_a_broken_venv_ends_the_job_clearly(box, shell, variant):
    v = _venv(box, broken=True)
    (v / ".hpcb-arch").write_text(os.uname().machine + "\n")
    r = _run(box, variant, shell, worker="SLURM_JOB_ID")
    assert r.returncode == 1 and "HPCB_DONE" not in r.stdout  # `exit 1`: the job ends here, not in a traceback later
    assert f"hpc-bridge: the endpoint venv {v} (built on {os.uname().machine}) does not run on" in r.stderr
    assert "cannot import name 'X'" in r.stderr  # the cause, just above
    assert _calls(box) == [] and (v / "sentinel").exists()  # never rebuilt from a worker


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_of_another_architecture_names_the_mismatch(box, shell):
    # globus1, 2026-10-10: built on the x86_64 login node, replayed on an aarch64 DGX Spark over the shared home
    v = _venv(box)
    (v / ".hpcb-arch").write_text("x86_64\n")
    py = v / "bin" / "python"
    py.unlink()
    py.write_bytes(b"\x00\x01\x02\x03 not an executable for this machine \x00\n")  # execve -> Exec format error
    py.chmod(0o755)
    fake = box["tmp"] / "arch"
    fake.mkdir()
    (fake / "uname").write_text('#!/bin/sh\nif [ "$1" = -m ]; then echo aarch64; else exec /usr/bin/uname "$@"; fi\n')
    (fake / "uname").chmod(0o755)
    r = _run(box, "uv", shell, worker="SLURM_JOB_ID", path_extra=f"{fake}:")
    assert r.returncode == 1 and "HPCB_DONE" not in r.stdout
    assert re.search(rf"the endpoint venv {re.escape(str(v))} \(built on x86_64\) does not run on \S+ \(aarch64\)",
                     r.stderr), r.stderr
    assert _calls(box) == [] and (v / "sentinel").exists()


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_whose_keyed_venv_is_missing_uses_the_unkeyed_one(box, shell):
    # a manager started by a plugin up to 0.1.22 runs from the unkeyed venv; that plugin, reading a re-ingested seed,
    # sends the new line as the block's worker_init — whose keyed venv was never built
    legacy = _venv(box, path=box["venv"])
    r = _run(box, "bootstrap", shell, worker="PBS_JOBID")
    assert r.returncode == 0 and _activated(r) == str(legacy), r.stderr
    assert _calls(box) == [] and not box["key"].exists() and not box["curl"].exists()


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_worker_whose_keyed_venv_is_broken_does_not_fall_back(box, shell):
    # broken is not missing: its manager runs from the keyed one, and a different venv would mix versions
    _venv(box, path=box["venv"])
    v = _venv(box, broken=True)
    r = _run(box, "uv", shell, worker="SLURM_JOB_ID")
    assert r.returncode == 1 and f"the endpoint venv {v}" in r.stderr and "HPCB_DONE" not in r.stdout


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_worker_with_no_venv_at_all_fails_clearly(box, shell):
    r = _run(box, "bootstrap", shell, worker="PBS_JOBID")
    assert r.returncode == 1 and "does not run on" in r.stderr and "built on unknown" in r.stderr
    assert _calls(box) == [] and not box["key"].exists() and not box["curl"].exists()


# ---- the discovery probe reports what is there (run for real, then parsed) ---------------------------------------

def _probe(box: dict, monkeypatch, path_extra: str = "") -> tuple:
    monkeypatch.setenv("HPC_BRIDGE_REMOTE_VENV", str(box["venv"]))
    monkeypatch.setattr(discovery, "_gce_version", lambda: PIN)  # this client's pin: the keyed path and the note
    env = {"PATH": f"{path_extra}/usr/bin:/bin", "HOME": str(box["home"]), "LANG": "C"}
    r = subprocess.run(["bash", "-c", discovery._probe_script()], env=env, capture_output=True, text=True,
                       timeout=60, check=False)
    assert "HPCB_PROBE_END" in r.stdout, r.stderr
    return discovery.parse_probe(r.stdout, ssh_host="h"), r.stdout


@pytest.mark.parametrize(("old", "says"), [
    ({}, "is reused"),
    ({"version": OLD}, f"has globus-compute-endpoint {OLD} on Python {PY}"),
    ({"broken": True}, "is BROKEN (ImportError: cannot import name 'X'"),
], ids=["healthy", "stale", "broken"])
def test_the_probe_reads_the_keyed_venv(box, monkeypatch, old, says):
    v = _venv(box, **old)
    (_draft, notes), out = _probe(box, monkeypatch)
    assert f"VENV={v}" in out
    env = next(n for n in notes if n.startswith("env_setup"))
    assert says in env, env
    assert (v / "sentinel").read_text() == "old" and not (v / ".hpcb-arch").exists()  # read-only
    assert not list(v.rglob("__pycache__"))  # bytecode writes off


def test_the_probe_reads_an_entry_points_interpreter_from_uvs_trampoline(box, monkeypatch):
    v = _venv(box, path=box["home"] / "tools" / "gce")
    (draft, _notes), out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out
    assert draft.env_setup.startswith("source ") and draft.env_setup.endswith("/tools/gce/bin/activate")  # used


def test_the_probe_does_not_guess_the_interpreter_from_the_directory(box, monkeypatch):
    # a `pip install --user` entry point in ~/.local/bin, next to a python3 shim of another version (uv's, say):
    # the entry point's own trampoline names the interpreter it really runs
    v = _venv(box, path=box["home"] / "tools" / "gce")
    local = box["home"] / ".local" / "bin"
    local.mkdir(parents=True)
    shutil.copy(v / "bin" / "globus-compute-endpoint", local / "globus-compute-endpoint")
    (local / "python3").write_text("#!/bin/sh\necho 3.9\n")
    (local / "python3").chmod(0o755)
    (draft, _notes), out = _probe(box, monkeypatch, path_extra=f"{local}:")
    assert f"GCE={local}/globus-compute-endpoint" in out and f"GCE_STATE={PY} {PIN}" in out
    assert "GCE_ACTIVATE=no" in out and draft.env_setup == "true"  # used from PATH: no `activate` to source there


def test_the_probe_reads_a_plain_shebang(box, monkeypatch):
    v = _venv(box, path=box["home"] / "tools" / "gce")
    ep = v / "bin" / "globus-compute-endpoint"
    ep.write_text(f"#!{v}/bin/python\nimport sys\nfrom globus_compute_endpoint.cli import main\nsys.exit(main())\n")
    _draft, out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out


def test_the_probe_passes_over_a_broken_endpoint_on_path(box, monkeypatch):
    v = _venv(box, path=box["home"] / "tools" / "gce", broken=True)
    (draft, notes), out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE={v}/bin/globus-compute-endpoint" in out and "GCE_STATE=ImportError" in out
    assert "source " not in draft.env_setup  # hpc-bridge's own venv instead
    assert any("BROKEN" in n and "not used" in n for n in notes)


# ---- the plugin side ------------------------------------------------------------------------------------------------

async def test_the_bring_up_calls_carry_repair_and_the_rest_carry_no_build(monkeypatch):
    from hpc_bridge.facility import remote

    cmds: dict[str, str] = {}

    async def fake(target, cmd, **kw):
        op = next(o for o in ("whoami", "configure", "start", "list", "stop", "delete") if f"endpoint {o}" in cmd)
        cmds[op] = cmd
        return (0, "HPCB_HOST=h\n" if op == "start" else "No endpoints configured!\n", "")

    async def _done(name):
        return "eid"

    monkeypatch.setattr(remote, "ssh_exec", fake)
    monkeypatch.setattr(RemoteEndpointCLI, "_await_endpoint_id", lambda self, name: _done(name))
    cli = RemoteEndpointCLI(SshTarget("h", "u"), "SETUP")
    await cli.whoami()
    await cli.configure("ep")
    await cli.start("ep")
    await cli.status("ep")
    await cli.stop("ep")
    await cli.delete("ep")
    for op in ("whoami", "configure", "start"):  # the long-timeout bring-up calls
        assert "HPCB_ENV_REPAIR=1; SETUP && " in cmds[op], op
    for op in ("list", "stop", "delete"):  # status polls and teardown: never a build
        assert "HPCB_ENV_REPAIR=0; SETUP && " in cmds[op], op
    assert "export HPCB_ENV_REPAIR" not in cmds["start"]  # the manager (and its blocks) must not inherit it
    assert RemoteEndpointCLI(SshTarget("h", "u"), "  ")._env_prefix(repair=True) == ""  # no env_setup, no flag


# ---- the worker_init a task carries: the endpoint's own --------------------------------------------------------

def _sanitize(opts: dict) -> dict:
    """The endpoint manager's `_sanitize_user_json`: the real one where globus-compute-endpoint is installed (Linux CI),
    else this copy of gce 4.18's (globus-compute, Apache-2.0)."""
    try:
        from globus_compute_endpoint.endpoint.config.utils import _sanitize_user_json
    except ImportError:
        def _sanitize_user_json(user_json: dict) -> dict:
            def inner(data):
                if isinstance(data, dict):
                    return {k: inner(v) for k, v in data.items()}
                if isinstance(data, list):
                    return [inner(v) for v in data]
                if isinstance(data, str):
                    return json.dumps(data)
                if isinstance(data, (int, float)):
                    return data
                if data is None:
                    return "null"
                raise ValueError(f"{type(data).__name__} is not a valid user config option type")
            return inner(user_json)
    return _sanitize_user_json(opts)


def _render(tmpl: str, uec: dict) -> dict:
    """The UEP config as gce 4.18's manager renders it: sanitized UEC, sandboxed Jinja, StrictUndefined, safe_load."""
    import jinja2
    import yaml
    from jinja2.sandbox import SandboxedEnvironment

    return yaml.safe_load(SandboxedEnvironment(undefined=jinja2.StrictUndefined).from_string(tmpl)
                          .render(**_sanitize(uec)))["engine"]["provider"]


def _facility(wi: str, sched: str = "slurm", client=None):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility

    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=wi, interface="ib0", partition="p",
                          account="a", worker_init=wi, scheduler=sched)
    return SlurmFacility(prof, cli=None, client_factory=client)


_OLD_UV = ("[ -d {venv} ] || uv venv {venv} --python 3.13; . {venv}/bin/activate; "
           "command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q 'globus-compute-endpoint==4.18.0'")
_WORKER_INITS = {
    "the new line": discovery.uv_env_setup(python="3.13", gce=PIN, sdk=SDK, bootstrap=True),
    "the old discovery line": _OLD_UV,
    "true": "true",
    "colon-space": 'module load gce; export PS1="h: "',
    "space-hash": "module load gce # site default",
    "quotes, backslash, $": "echo 'a' \"b\" \\x \\\\ $HOME ${X:-y} $(date) `id`",
    "newline": "module load gce\nsource /x/bin/activate",
    "jinja braces": "echo {{ 7*7 }} {% raw %}x{% endraw %} {# c #} }} %} #}",
    "leading dash": "- module load gce",
    "yes": "yes",
    "123": "123",
    "non-ascii": "export NAME='Zoë 中文'; module load gce",
}


@pytest.mark.parametrize("sched", ["slurm", "pbs"])
@pytest.mark.parametrize("wi", list(_WORKER_INITS.values()), ids=list(_WORKER_INITS))
def test_every_worker_init_reaches_the_block_intact(sched, wi):
    from hpc_bridge.profile import Profile
    from hpc_bridge.shapes import shape_config

    wi = wi.replace("{venv}", "/home/u/hpc-bridge/gce-venv")
    fac = _facility(wi, sched)
    tmpl, defaults = fac.config_template(Profile())
    uec = {**defaults, **shape_config("compute")}
    assert uec["worker_init"] == wi  # every task carries one
    assert _render(tmpl, uec)["worker_init"] == wi
    # an endpoint configured by a plugin up to 0.1.22: its template's default was encoded once, and was never what a
    # block used (the task's config always carried worker_init) — it still is not
    old_tmpl = tmpl.replace(json.dumps(json.dumps(wi)), json.dumps(wi))
    assert old_tmpl != tmpl
    assert _render(old_tmpl, uec)["worker_init"] == wi
    # and with no worker_init in the task's config, the template's own default reaches the block intact
    assert _render(tmpl, {k: v for k, v in uec.items() if k != "worker_init"})["worker_init"] == wi


@pytest.mark.parametrize("encode", [1, 2], ids=["single (up to 0.1.22)", "double"])
def test_the_configured_worker_init_is_read_back_from_the_template(encode):
    from hpc_bridge.facility.remote import configured_worker_init
    from hpc_bridge.profile import Profile

    for wi in _WORKER_INITS.values():
        tmpl, _ = _facility(wi).config_template(Profile())
        if encode == 1:
            tmpl = tmpl.replace(json.dumps(json.dumps(wi)), json.dumps(wi))
        assert configured_worker_init(tmpl) == wi
    assert configured_worker_init("engine: {}\n") is None and configured_worker_init(None) is None
    assert configured_worker_init("    worker_init: {{ worker_init | default(not json) }}\n") is None


class _Client:
    def __init__(self, template=None, exc=None):
        self.template, self.exc, self.calls = template, exc, 0

    def get_endpoint_metadata(self, eid):
        self.calls += 1
        if self.exc:
            raise self.exc
        return {"user_config_template": self.template}


async def test_a_reused_endpoint_keeps_the_worker_init_it_was_configured_with():
    from hpc_bridge.profile import Profile

    old_fac = _facility(_OLD_UV)
    old_tmpl = old_fac.config_template(Profile())[0].replace(json.dumps(json.dumps(_OLD_UV)), json.dumps(_OLD_UV))
    client = _Client(old_tmpl)
    new = discovery.uv_env_setup(python=PY, gce=PIN, sdk=SDK)
    fac = _facility(new, client=lambda: client)  # this session's profile: the edited / migrated env_setup
    await fac.adopt_endpoint("eid-1")
    await fac.adopt_endpoint("eid-1")  # once per endpoint
    assert client.calls == 1
    sent = fac.dispatch_uec({"worker_init": new, "compute": True})
    assert sent["worker_init"] == _OLD_UV  # the blocks run what the manager was started with
    assert "teardown_endpoint, then connect_facility, to apply the new one" in fac.worker_init_note
    assert fac.dispatch_uec({"compute": False}) == {"compute": False}  # nothing added where nothing was sent


async def test_an_endpoint_whose_template_matches_gets_no_note_and_an_unreadable_one_the_profiles():
    from hpc_bridge.profile import Profile

    new = discovery.uv_env_setup(python=PY, gce=PIN, sdk=SDK)
    tmpl = _facility(new).config_template(Profile())[0]
    fac = _facility(new, client=lambda: _Client(tmpl))
    await fac.adopt_endpoint("eid-1")
    assert fac.worker_init_note == "" and fac.dispatch_uec({"worker_init": new})["worker_init"] == new
    for client in (_Client(exc=RuntimeError("no metadata")), _Client("not our template")):
        fac = _facility(new, client=lambda c=client: c)
        await fac.adopt_endpoint("eid-2")
        assert fac.endpoint_worker_init is None and fac.worker_init_note == ""
        assert fac.dispatch_uec({"worker_init": new})["worker_init"] == new  # the pre-change behaviour


async def test_the_endpoint_is_adopted_before_its_first_task_is_built():
    # every path to an endpoint id passes _provision; the first runner must already carry the endpoint's own
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from hpc_bridge.server import _ensure_endpoint_up
    from tests.fakes import FakeFacility

    order: list[str] = []

    class _Fac(FakeFacility):
        def config_template(self, profile):
            return "tmpl", {"worker_init": "PROFILE"}

        async def adopt_endpoint(self, eid):
            order.append(f"adopt {eid}")

        def dispatch_uec(self, uec):
            return {**uec, "worker_init": "OWN"}

    class _Runner:
        def __init__(self, eid, uec):
            order.append(f"runner {uec.get('worker_init')}")
            self.endpoint_id = eid

        async def canary(self, timeout=8.0):
            from hpc_bridge.runner import CanaryResult
            return CanaryResult(ok=True, worker_host="n1", worker_python="3.13")

        def close(self):
            pass

    f = _Fac()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _Runner(eid, user_endpoint_config)
    await _ensure_endpoint_up(app, confirm_spend=True)
    assert order[:2] == ["adopt fake-eid", "runner OWN"], order


def test_a_finished_pilot_on_an_edited_reused_endpoint_says_why():
    import asyncio

    from hpc_bridge import scheduler_ops

    class _App:
        class facility:
            worker_init_note = "this endpoint runs with the environment it was started with. "
            profile = None

    async def run_login(cmd):
        from hpc_bridge.models import ShellOutcome
        return ShellOutcome(phase="complete", exit_code=0, stdout="F 1108 2 FAILED\n", stderr="", block_state="warm")

    out = asyncio.run(scheduler_ops._augment_provisioning_notice(_App(), "eid", "allocating nodes…", 30.0, run_login))
    assert "already FINISHED" in out and "runs with the environment it was started with" in out


# ---- a cached config from an earlier plugin is brought up to date -----------------------------------------------

_INIT = "type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.5; "
_OLD_LINES = {
    "uv (earliest)": ("[ -d {venv} ] || uv venv {venv}; . {venv}/bin/activate; "
                      "command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q globus-compute-endpoint"),
    "uv pinned": _OLD_UV,
    "uv, resolved path": _OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv"),
    "bootstrap": ('export PATH="$HOME/.local/bin:$PATH"; command -v uv >/dev/null 2>&1 || '
                  "curl -LsSf https://astral.sh/uv/install.sh | sh; " + _OLD_UV),
    "module uv": _INIT + _OLD_UV,
    "module pip": ("module load python/3.13; [ -d {venv} ] || python3 -m venv {venv}; . {venv}/bin/activate; "
                   "command -v globus-compute-endpoint >/dev/null 2>&1 || pip install -q 'globus-compute-endpoint==4.18.0'"),
}


@pytest.mark.parametrize("old", list(_OLD_LINES.values()), ids=list(_OLD_LINES))
def test_a_line_an_earlier_discovery_generated_is_migrated(old):
    new = discovery.migrate_env_setup(old)
    assert new != old and "command -v globus-compute-endpoint" not in new and "__hb_ok" in new
    assert ("/home/u/hpc-bridge/gce-venv" in new) is ("/home/u/hpc-bridge/gce-venv" in old)  # same venv base
    if old.startswith("export PATH"):
        assert new == discovery._BOOTSTRAP_UV_ENV_SETUP
    elif old.startswith(_INIT):
        assert new == _INIT + discovery._UV_ENV_SETUP
    elif "python3 -m venv" in old:
        assert new.startswith("module load python/3.13; ") and 'python3 -m venv "$__hb_venv"' in new
    assert discovery.migrate_env_setup(new) == new  # idempotent


@pytest.mark.parametrize("hand", [
    "module load gce && source ~/gce/bin/activate",
    _OLD_UV + " ",                                          # one byte off: not ours
    "echo site; " + _OLD_UV,                                # ours, with the user's own prefix: theirs now
    _OLD_UV.replace("uv pip install -q", "uv pip install"),
    _OLD_UV.replace("{venv}/bin/activate", "/elsewhere/bin/activate"),  # two different venvs: hand-edited
    "",
])
def test_a_hand_written_line_is_never_touched(hand):
    assert discovery.migrate_env_setup(hand) == hand


def test_a_cached_facility_gets_the_migrated_line():
    from hpc_bridge import binding
    from hpc_bridge.models import FacilityDetails

    d = FacilityDetails(ssh_host="globus1", interface="eth0", env_setup=_OLD_UV, scratch_root="/home/{user}/.x",
                        partition="main")
    assert binding._entry_from_details("globus1", d).compute.env_setup == discovery._UV_ENV_SETUP


# ---- the generated text ------------------------------------------------------------------------------------------

def test_a_generated_line_checks_the_install_instead_of_trusting_it():
    for es in (discovery._UV_ENV_SETUP, discovery._BOOTSTRAP_UV_ENV_SETUP,
               discovery._module_env_setup([f"python/{PY}"], None)[0]):
        assert "command -v globus-compute-endpoint" not in es and "[ -d {venv} ]" not in es
        assert "import globus_compute_endpoint.cli" not in es  # fails under -c on a real install
        assert es.count("{venv}") == 1 and '__hb_base="{venv}"; ' in es
        gce = discovery._gce_version()
        assert f'__hb_venv="$__hb_base-endpoint-{gce}-py{PY}";' in es if gce else \
            f'__hb_venv="$__hb_base-endpoint-py{PY}";' in es
        assert '"$1/bin/globus-compute-endpoint" version' in es and f"sys.version_info[:2]!=({PY.replace('.', ',')})" in es
        assert '[ -n "${SLURM_JOB_ID:-}${PBS_JOBID:-}" ]' in es and '[ "${HPCB_ENV_REPAIR:-}" = 0 ]' in es
        if gce:  # the SDK is a dependency, so in practice always: pinned, and globus-sdk with it
            assert f'*"version: {gce}")' in es and f"'globus-compute-endpoint=={gce}'" in es
            sdk = discovery._installed("globus-sdk")
            assert sdk is None or f"'globus-sdk=={sdk}'" in es
    pip = discovery._module_env_setup([f"python/{PY}"], None)[0]
    assert '"$__hb_venv/bin/python" -m pip install -q $__hb_nc' in pip and "__hb_nc=--no-cache-dir" in pip
    assert "__hb_nc=--no-cache;" in discovery._UV_ENV_SETUP
    b = discovery._BOOTSTRAP_UV_ENV_SETUP  # the installer runs only inside the build
    assert b.index("__hb_build()") < b.index("astral.sh") < b.index('if [ -n "${SLURM_JOB_ID')


def test_an_unpinned_text_still_checks_the_entry_point_and_python():
    es = discovery.uv_env_setup(python="3.12", gce=None, sdk="4.9.1")
    assert '__hb_venv="$__hb_base-endpoint-py3.12";' in es and "sys.version_info[:2]!=(3,12)" in es
    assert "version: " not in es.split("__hb_runs()")[1].split("__hb_undo()")[0]  # no pin to compare
    assert "globus-sdk" not in es and " globus-compute-endpoint && __hb_ok" in es


def test_the_keyed_venv_collides_with_no_seeds_venv():
    # Delta's and Anvil's MEP worker_init build `$HOME/hpc-bridge/gce-venv-<gce>-py<X.Y>` and upgrade it on every
    # block; the globus-cluster MEP's is `$HOME/hpc-bridge/gce-venv`: hpc-bridge's own venv must be none of them
    base = "/home/u/hpc-bridge/gce-venv"
    ours = {discovery.keyed_venv(base, python=py, gce=g) for py in ("3.11", "3.12", "3.13") for g in
            ("4.15.0", "4.16.0", "4.18.0", None)}
    theirs = set()
    for entry in BundledCatalog(SEED.parent).entries():
        for path in re.findall(r"\$HOME/hpc-bridge/[\w.{}-]+", entry.compute.env_setup):
            for py in ("3.11", "3.12", "3.13"):
                for g in ("4.15.0", "4.16.0", "4.18.0"):
                    theirs.add(path.replace("$HOME", "/home/u").replace("{gce_version}", g)
                               .replace("{python_version}", py))
    assert theirs and not (ours & theirs), ours & theirs


# ---- the curated seed is the generator's text --------------------------------------------------------------------

def _expanse() -> str:
    entry = next(e for e in BundledCatalog(SEED).entries() if e.id == "expanse")
    return entry.compute.env_setup


def test_the_expanse_seed_holds_the_generators_line():
    es = _expanse()
    py = re.search(r"--python (\d+\.\d+)", es).group(1)
    pin = re.search(r"globus-compute-endpoint==([\w.]+)", es).group(1)
    sdk = re.search(r"globus-sdk==([\w.]+)", es).group(1)
    assert es == discovery.uv_env_setup(python=py, gce=pin, sdk=sdk, bootstrap=True)


# ---- a build that cannot start, or is interrupted ----------------------------------------------------------------

def _not_root() -> None:
    if os.geteuid() == 0:
        pytest.skip("root ignores the permission this test takes away")


@pytest.mark.parametrize("shell", SHELLS)
def test_a_parent_that_cannot_be_created_fails_at_once(box, shell):
    # BYO's default {venv} is /home/{user}/…: a site whose home is elsewhere has no such directory to create
    _not_root()
    box["home"].chmod(0o500)
    try:
        t = time.monotonic()
        r = _run(box, "uv", shell)
        assert r.returncode != 0, r.stderr
        assert re.search(rf"^hpc-bridge: cannot create {re.escape(str(box['venv'].parent))}$", r.stderr, re.M), r.stderr
        assert time.monotonic() - t < 20 and _calls(box) == []  # not 200 × 2 s of waiting for a lock nobody holds
    finally:
        box["home"].chmod(0o700)


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_lock_that_cannot_be_created_fails_at_once(box, shell):
    # a full quota (which old venvs accumulating invites) or a read-only directory: not "held by another build"
    _not_root()
    box["venv"].parent.mkdir(parents=True)
    box["venv"].parent.chmod(0o500)
    try:
        t = time.monotonic()
        r = _run(box, "uv", shell)
        assert r.returncode != 0 and f"cannot create {box['key']}.lock: Permission denied" in r.stderr, r.stderr
        assert "held by another build" not in r.stderr and time.monotonic() - t < 20 and _calls(box) == []
    finally:
        box["venv"].parent.chmod(0o700)


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_build_whose_session_dropped_still_restores_and_releases(box, shell):
    # no pty: a dropped session leaves the remote shell running, writing to a closed pipe — without `trap '' PIPE` the
    # first echo would kill it, with the old venv moved aside and the lock held
    v = _venv(box, version=OLD)
    p = subprocess.Popen([shell, "-c", _cmd(box, "uv", flag="1", worker=None)], env=_env(box, HPCB_TEST_INSTALL="broken"),
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    p.stderr.close()  # the session is gone before the line says anything
    p.wait(timeout=120)
    assert p.returncode != 0 and (v / "sentinel").read_text() == "old" and _version(v) == OLD
    assert _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS)
def test_a_build_killed_by_term_restores_and_releases(box, shell):
    v = _venv(box, version=OLD)
    p = subprocess.Popen([shell, "-c", _cmd(box, "uv", flag="1", worker=None)], env=_env(box, HPCB_TEST_SLOW="1"),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 30
    while not any(c[:2] == ["uv", "venv"] for c in _calls(box)) and time.monotonic() < deadline:
        time.sleep(0.05)  # the old venv is aside, the (slow) new one is being made
    time.sleep(0.2)
    p.terminate()  # SIGTERM to the shell; its `uv venv` child finishes, then the trap runs
    p.communicate(timeout=60)
    assert (v / "sentinel").read_text() == "old" and _version(v) == OLD, _leftovers(box)
    assert _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_venv_that_cannot_be_moved_aside_is_left_alone(box, shell):
    v = _venv(box, broken=True)
    fake = box["tmp"] / "nomv"
    fake.mkdir()
    (fake / "mv").write_text(f'#!/bin/sh\n[ "$1" = "{v}" ] && {{ echo "mv: busy" >&2; exit 1; }}\nexec /bin/mv "$@"\n')
    (fake / "mv").chmod(0o755)
    r = _run(box, "uv", shell, path_extra=f"{fake}:")
    assert r.returncode != 0 and f"cannot move {v} aside" in r.stderr
    assert (v / "sentinel").exists() and not _built(box) and _leftovers(box) == []  # untouched, lock released


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_signal_while_moving_aside_never_removes_the_original(box, shell):
    # the undo trap is armed before the move: if the move then fails, the venv at the keyed path is still the
    # original — the undo must leave it alone (it only removes what the build created)
    v = _venv(box, broken=True)
    fake = box["tmp"] / "slowmv"
    fake.mkdir()
    started = box["tmp"] / "mv-started"
    # only the first move of the venv fails (slowly); any later one works, as a real mv would once it is free
    (fake / "mv").write_text(f'#!/bin/sh\nif [ "$1" = "{v}" ] && [ ! -e "{started}" ]; then : > "{started}"; '
                             'sleep 2; exit 1; fi\nexec /bin/mv "$@"\n')
    (fake / "mv").chmod(0o755)
    p = subprocess.Popen([shell, "-c", _cmd(box, "uv", flag="1", worker=None)], env=_env(box, path_extra=f"{fake}:"),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 30
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    p.terminate()  # the shell runs its trap once the (failing) mv returns
    p.communicate(timeout=60)
    assert (v / "sentinel").read_text() == "old" and not _built(box) and _leftovers(box) == []


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_build_that_fails_early_does_not_quote_the_old_reason(box, shell):
    v = _venv(box, broken=True)  # its check failed with "cannot import name 'X'"
    r = _run(box, "uv", shell, HPCB_TEST_UV_FAIL="venv")  # then `uv venv` itself fails
    failed = next(ln for ln in r.stderr.splitlines() if "could not build" in ln)
    assert failed.endswith(f"in {v}") and "cannot import" not in failed, failed
    assert (v / "sentinel").exists()


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_repair_skips_the_package_cache_and_a_first_build_does_not(box, shell):
    # live 2026-10-10: a corrupt file hardlinked from ~/.cache/uv came back with every rebuild
    r = _run(box, "uv", shell)
    assert r.returncode == 0 and not any("--no-cache" in c for c in _calls(box) if "install" in c)
    box["log"].unlink()
    box["lib"]["install_gce"](str(box["key"]), PIN, broken=True)
    r = _run(box, "uv", shell)
    assert r.returncode == 0 and any("--no-cache" in c for c in _calls(box) if "install" in c), _calls(box)


def test_a_name_for_a_moved_aside_venv_is_never_taken(tmp_path):
    # a leftover `.old.<pid>` from a reused pid would otherwise take the venv inside it
    r = subprocess.run(["sh", "-c", discovery._FREE + '; mkdir -p "$1.$$" "$1.$$x"; __hb_free "$1"; echo "$__hb_f"; '
                        'echo "$1.$$xx"', "sh", str(tmp_path / "venv.old")], capture_output=True, text=True, check=True)
    got, want = r.stdout.split()
    assert got == want


# ---- other plugin calls: the keyed venv if usable, else the first working one ----------------------------------

@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_half_built_keyed_venv_is_not_used_by_a_stop(box, shell):
    # a build killed during install leaves a venv without its entry point: stop must reach a working one instead
    legacy = _venv(box, path=box["venv"])
    v = _venv(box)
    (v / "bin" / "globus-compute-endpoint").unlink()
    r = _run(box, "uv", shell, flag="0")
    assert r.returncode == 0 and _activated(r) == str(legacy), r.stderr


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_stop_takes_the_first_working_venv(box, shell):
    first = _venv(box, path=box["venv"])                             # the unkeyed one is tried first
    _venv(box, path=_key(box["venv"], OLD), version=OLD)             # also working: not reached
    r = _run(box, "uv", shell, flag="0")
    assert r.returncode == 0 and _activated(r) == str(first), r.stderr


@pytest.mark.parametrize("shell", ["bash"])
def test_under_failglob_a_stop_still_finds_a_venv(box, shell):
    other = _venv(box, path=_key(box["venv"], OLD), version=OLD)
    es = _setup("uv").replace("{venv}", str(box["venv"]))
    cmd = "shopt -s failglob; " + RemoteEndpointCLI(SshTarget("h", "u"), es)._env_prefix(repair=False) + \
        'echo "HPCB_DONE $VIRTUAL_ENV"'
    r = subprocess.run([shell, "-c", cmd], env=_env(box), capture_output=True, text=True, timeout=60, check=False)
    assert r.returncode == 0 and _activated(r) == str(other), r.stderr


# ---- a plugin that sets no flag (up to 0.1.22, reading a re-ingested seed) ---------------------------------------

@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_plugin_with_no_flag_keeps_the_unkeyed_venv_it_used(box, shell):
    legacy = _venv(box, path=box["venv"], version=OLD)  # what its endpoint was started from
    r = _run(box, "uv", shell, flag=None)
    assert r.returncode == 0 and _activated(r) == str(legacy), r.stderr
    assert _calls(box) == [] and not box["key"].exists()  # nothing built, inside a 30 s call or not


@pytest.mark.parametrize("shell", SHELLS[:1])
def test_a_plugin_with_no_flag_builds_when_nothing_works(box, shell):
    _venv(box, path=box["venv"], broken=True)
    r = _run(box, "uv", shell, flag=None)
    assert r.returncode == 0 and _version(box["key"]) == PIN, r.stderr


# ---- the probe ---------------------------------------------------------------------------------------------------

def test_a_probe_that_times_out_does_not_call_a_slow_install_broken(box, monkeypatch):
    v = _venv(box, path=box["home"] / "tools" / "gce")
    slow = box["tmp"] / "slow"
    slow.mkdir()
    (slow / "timeout").write_text("#!/bin/sh\nexit 124\n")  # the 20 s cap hit
    (slow / "timeout").chmod(0o755)
    (draft, notes), out = _probe(box, monkeypatch, path_extra=f"{slow}:{v / 'bin'}:")
    assert "GCE_STATE=timed out" in out
    assert draft.env_setup.startswith("source ") and not any("BROKEN" in n for n in notes)
    assert any("timed out" in n and "confirm it works" in n for n in notes)


@pytest.mark.parametrize("launcher", ["'''exec' \"{py}\" \"$0\" \"$@\"", "'''exec' {py} \"$0\" \"$@\""],
                         ids=["double-quoted", "bare"])
def test_the_probe_reads_pips_long_path_launcher(box, monkeypatch, launcher):
    v = _venv(box, path=box["home"] / "tools" / "gce")
    ep = v / "bin" / "globus-compute-endpoint"
    ep.write_text("#!/bin/sh\n" + launcher.format(py=v / "bin" / "python") + "\n' '''\nimport sys\n"
                  "from globus_compute_endpoint.cli import main\nsys.exit(main())\n")
    _draft, out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out, out


# ---- a REAL install (opt-in: HPCB_TEST_REAL_GCE_VENV=<a venv with globus-compute-endpoint>) ----------------------

_REAL = os.environ.get("HPCB_TEST_REAL_GCE_VENV")


@pytest.mark.skipif(not _REAL, reason="set HPCB_TEST_REAL_GCE_VENV to a real globus-compute-endpoint venv")
def test_the_line_against_a_real_install(box, monkeypatch):
    real = Path(_REAL).resolve()
    out = subprocess.run([str(real / "bin" / "globus-compute-endpoint"), "version"], capture_output=True, text=True,
                         check=True).stdout
    gce = out.strip().rsplit(": ", 1)[-1]
    py = subprocess.run([str(real / "bin" / "python"), "-c", "import sys;print('%d.%d' % sys.version_info[:2])"],
                        capture_output=True, text=True, check=True).stdout.strip()
    key = _key(box["venv"], gce, py)
    key.parent.mkdir(parents=True)
    key.symlink_to(real)  # the keyed venv IS the real install
    marker = real / ".hpcb-arch"
    had_marker = marker.exists()
    try:
        es = discovery.uv_env_setup(python=py, gce=gce, sdk=None)
        for kw in ({}, {"flag": "0"}, {"worker": "SLURM_JOB_ID"}):
            r = _run(box, "uv", "bash", setup=es, **kw)
            assert r.returncode == 0 and "HPCB_DONE" in r.stdout and _calls(box) == [], (kw, r.stderr)
        monkeypatch.setattr(discovery, "_client_python", lambda: py)
        monkeypatch.setattr(discovery, "_gce_version", lambda: gce)
        monkeypatch.setenv("HPC_BRIDGE_REMOTE_VENV", str(box["venv"]))
        r = subprocess.run(["bash", "-c", discovery._probe_script()], env={"PATH": "/usr/bin:/bin", "LANG": "C",
                           "HOME": str(box["home"])}, capture_output=True, text=True, timeout=120, check=False)
        assert f"VENV_STATE={py} {gce}" in r.stdout, r.stdout
    finally:
        if not had_marker and marker.exists():
            marker.unlink()
