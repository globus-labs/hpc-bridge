"""Block environments (PR 3; vault `Planned/Block environments.md`): point work at a software environment — one that
exists on the facility (`activate`: a shell line) or one built from a requirements list (`requirements`) — for
interactive commands, batch rows, Jobs-mode jobs and handoffs. Written failing first: the whole module is a strict
xfail until the build lands (then the mark comes off; a test that passes before then is an XPASS, which fails).

Where a built env is built, and how it is used (the user's decisions 1-3, 2026-11-04):
- on a facility whose endpoint hpc-bridge stood up over SSH: a SEPARATE user env, built on the login node by the free
  login worker (its own runner, never the internal session), activated per command, row and job script — never the
  worker's env, so switching env never needs a new block (`how="login"`). A compute node of another architecture than
  the login node builds its own in a task on the confirmed block (`how="block"`: billed);
- on a facility endpoint whose template reads `requirements` (Delta, Anvil): passed through — the worker's env, built by
  the facility when the block starts; a new env is a new user endpoint and a new block (`how="facility"`);
- on one whose template does not (Polaris): built by the block's own `worker_init` under `flock` where the node has it,
  then activated per command as on SSH; the first build is billed (`how="worker_init"`);
- an existing env (`activate`) is the user's line, run before each command, row and job (`how="activate"`).

The surface these tests are written against (the stage-A checkpoint fixes it):

- `env` on ensure_endpoint_up, run_batch, submit_job and export_handoff (server `_ensure_endpoint_up(…, env=)`,
  `_run_batch(…, env=)`, `_submit_job(…, env=)`, `_export_handoff(…, env=)`): `{"activate": "<shell line>"}` |
  `{"requirements": "<requirements.txt text>", "python": "3.11"?, "root": "<dir>"?}`, either with `name` (a label) |
  `{}` (no env) | omitted (keep the session's). The session's env holds for both shapes of the session's user commands
  (never hpc-bridge's internal ones), as a partition does; run_batch's and submit_job's own env is that batch's or job's.
- `models.EnvInfo` {kind: activate|built, how: activate|login|block|facility|worker_init, key (16 hex), name, python,
  path, root, state: ready|building|failed|unchecked, task_id, cached, lock_sha256, size_mb, new_block, billed,
  index_lines, warning, error} on `EndpointStatus.env`, `BlockTerms.env`, `BatchStatus.env`, `JobInfo.env`,
  `JobTerms.env`; `LedgerEntry.env` (a label); `ShellOutcome.phase` gains `env_building` (nothing ran: the env is being
  built or rebuilt; call again); handoff: `HandoffRecord.env`, `HandoffValidation.env_ok`.
- `hpc_bridge.envs`: `parse(value) -> EnvSpec | None` (EnvRefusal by name), `EnvSpec(kind, activate, requirements,
  python, root, name)`, `normalize_requirements`, `index_lines`, `default_python()` (the client's major.minor),
  `env_key(spec)` (16 hex: sha256 of "hpcb-env-v1", the Python and the normalized text; the directory on the facility
  is `<root>/<key>-<uname -m>`), `env_root(scratch_root, root=None) -> (root, warning)`, `Activation(how, key, root,
  line)` and `activation_sh(act, nonce)`, `refusal(rc, stderr, nonce) -> Refused(why, key, detail) | None` (why:
  gone|failed|arch|activate; rc `ENV_RC` 88, the line `REFUSED <nonce> <why> <key> …`), `build_script(spec, *, key,
  root, python, token, probe=False, proxy=None)` (every one carries `BUILD_TAG`), `parse_build`, `check_script(*, key,
  root)` / `parse_check` (ready|partial|gone|failed|arch), `fallback_worker_init(spec, *, key, root, python,
  proxy=None)`, `pin_into_active(worker_init)`, `delivery(spec, *, access, reads_requirements, reads_worker_init,
  client_python)`, `MARKER` (".hpcb-env"), `BUILD_S` (a build's own runner's walltime), `VERIFY_EVERY_S`.
- `MEPFacility.reads(key) -> bool | None`: the published template uses the key and the schema lets it through (None:
  the template was not read).
- `session_shell.wrap(…, env_sh=)`, `batch.task_script(…, env_sh=)`: the activation, after the session's own env and
  before the row's slot env; the session never persists what the activation changed. `Ledger.add_batch(…, env=)`.

The world: the facility is this machine's bash (the handoff tests' block: a scheduler's env, HOME in tmp), with a `uv`
stub first on PATH that makes a venv the real interpreter runs as one (`pyvenv.cfg` home) and "installs" a
requirement as a package dir with its dist-info RECORD; `nosuchpkg` fails as uv's resolver does. Login and compute run
on the same machine and are told apart by the runner that sent each command (its config's `compute`); a compute node
of another architecture is a fake `uname` on that shape's PATH.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from hpc_bridge import server
from tests.test_handoff_phase2b import (
    FAKE_SCHEMA,
    FAKE_TEMPLATE,
    LOAM_UEC,
    PBS_MEP,
    POLARIS_SCHEMA,
    POLARIS_TEMPLATE,
    SSH_EID,
    _Block,
    _Out,
    _Runner,
    _Runners,
)
from tests.test_server import _PendingFuture, _Res

_BASH = shutil.which("bash")
_FLOCK = shutil.which("flock")
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash"),
              pytest.mark.xfail(strict=True, reason="block environments: stage A, failing first (the build removes "
                                                    "this)")]

PY = f"{sys.version_info.major}.{sys.version_info.minor}"
ARCH = os.uname().machine          # what `uname -m` prints here
REQS = "six==1.17.0\n"
NONCE = "a1b2c3d4e5f60718"
# The fake `pbs_mep` profile's second manager: the same Polaris-shaped template, which also builds `requirements` into
# the worker's env when the block starts (the Delta/Anvil kind); its schema lists `requirements`.
REQ_TEMPLATE = (PBS_MEP / "pbs_user_config_template_req.yaml.j2").read_text().replace("@@VER@@", "4.16.0")
REQ_SCHEMA = json.loads((PBS_MEP / "pbs_user_config_schema_req.json").read_text())

# Delta's kind (4.17+'s default template: the init script before any user worker_init; schema permissive)
DELTA_TEMPLATE = """engine:
  type: GlobusComputeEngine
  provider:
    type: SlurmProvider
    partition: {{ partition }}
    account: {{ account }}
    walltime: {{ walltime|default("00:30:00") }}
    worker_init: |
      export PATH="$HOME/.local/bin:$PATH"
{%- if requirements %}
      R=$(mktemp); printf '%s\\n' {{ requirements }} > "$R"
{%- endif %}
      {{ worker_init|default("") }}
"""
DELTA_SCHEMA = {"type": "object", "additionalProperties": True,
                "properties": {"worker_init": {"type": "string"}, "endpoint_setup": {"type": "string"}}}
# Anvil's kind: strict schema listing `requirements` and `venvs_dir`
ANVIL_TEMPLATE = """engine:
  type: GlobusComputeEngine
  provider:
    type: SlurmProvider
    partition: {{ partition|default("shared") }}
    account: {{ account }}
    walltime: {{ walltime|default("00:15:00") }}
    cores_per_node: {{ cores_per_node|default(1) }}
    worker_init: {{ worker_init|default("") }}
{%- if requirements is defined %}
    # built into {{ venvs_dir|default("~/.globus_compute/venvs") }}
{%- endif %}
"""
ANVIL_SCHEMA = {"type": "object", "additionalProperties": False, "properties": {k: {"type": "string"} for k in (
    "account", "partition", "qos", "walltime", "cores_per_node", "mem_per_node", "constraint", "exclusive",
    "min_blocks", "max_blocks", "init_blocks", "nodes_per_block", "scheduler_options", "resource_specification",
    "worker_init", "endpoint_setup", "venvs_dir", "requirements")}}
# neither: no worker_init, no requirements, and a strict schema that lists neither
NEITHER_TEMPLATE = """engine:
  type: GlobusComputeEngine
  provider:
    type: PBSProProvider
    queue: {{ queue }}
    account: {{ account }}
    walltime: {{ walltime|default("00:30:00") }}
    {# requirements and worker_init: this facility takes neither #}
"""
NEITHER_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["queue", "account"],
                  "properties": {"queue": {"type": "string"}, "account": {"type": "string"},
                                 "walltime": {"type": "string"}}}


# ---- the uv stub -------------------------------------------------------------------------------------------------------

_UV = r'''
import glob, json, os, shutil, sys, time

def log(args):
    p = os.environ.get("HPCB_TEST_UV_LOG")
    if p:
        with open(p, "a") as fh:
            fh.write(json.dumps({"argv": args, "venv": os.environ.get("VIRTUAL_ENV"),
                                 "https_proxy": os.environ.get("https_proxy")}) + "\n")

def site(venv):
    got = sorted(glob.glob(os.path.join(venv, "lib", "python*", "site-packages")))
    return got[0] if got else None

def parse_req(line):
    line = line.split(" #", 1)[0].strip()
    if not line or line.startswith(("#", "-")):
        return None
    if "==" in line:
        name, ver = line.split("==", 1)
        return name.strip().split("[")[0], ver.split(";")[0].split(",")[0].strip()
    for sep in (">=", "<=", "~=", ">", "<", ";", "["):
        line = line.split(sep, 1)[0]
    return line.strip(), "1.0"

def install(venv, reqs):
    sp = site(venv)
    for name, _ver in reqs:
        if name == "nosuchpkg":
            sys.stderr.write("  x No solution found when resolving dependencies:\n  `-> Because nosuchpkg was not found "
                             "in the package registry and you require nosuchpkg, we can conclude that your "
                             "requirements are unsatisfiable.\n")
            sys.exit(1)
    for name, ver in reqs:
        mod = name.replace("-", "_").lower()
        for old in glob.glob(os.path.join(sp, mod + "-*.dist-info")):
            shutil.rmtree(old)
        os.makedirs(os.path.join(sp, mod), exist_ok=True)
        with open(os.path.join(sp, mod, "__init__.py"), "w") as fh:
            fh.write("__version__ = %r\n" % ver)
        di = os.path.join(sp, "%s-%s.dist-info" % (mod, ver))
        os.makedirs(di, exist_ok=True)
        with open(os.path.join(di, "RECORD"), "w") as fh:
            fh.write("%s/__init__.py,,\n%s-%s.dist-info/RECORD,,\n" % (mod, mod, ver))

def venv_python(rest):
    if "--python" in rest:
        return rest[rest.index("--python") + 1]
    return os.path.join(os.environ["VIRTUAL_ENV"], "bin", "python")

def main(args):
    if args == ["--version"]:
        print("uv 0.12.9 (stub)")
        return 0
    log(args)
    if os.environ.get("HPCB_TEST_UV_SLOW"):
        time.sleep(float(os.environ["HPCB_TEST_UV_SLOW"]))
    if args[:1] == ["venv"]:
        py, path, it = None, None, iter(args[1:])
        for a in it:
            if a == "--python":
                py = next(it)
            elif not a.startswith("-"):
                path = a
        if os.environ.get("HPCB_TEST_UV_FAIL") == "venv":
            sys.stderr.write("error: No interpreter found for Python %s in managed installations or search path\n" % py)
            return 2
        real = os.path.realpath(sys.executable)
        os.makedirs(os.path.join(path, "bin"), exist_ok=True)
        os.makedirs(os.path.join(path, "lib", "python%d.%d" % sys.version_info[:2], "site-packages"), exist_ok=True)
        if not os.path.lexists(os.path.join(path, "bin", "python")):
            os.symlink(real, os.path.join(path, "bin", "python"))
        with open(os.path.join(path, "bin", "activate"), "w") as fh:
            fh.write('VIRTUAL_ENV="%s"; export VIRTUAL_ENV; PATH="$VIRTUAL_ENV/bin:$PATH"; export PATH\n' % path)
        with open(os.path.join(path, "pyvenv.cfg"), "w") as fh:
            fh.write("home = %s\ninclude-system-site-packages = false\nversion_info = %s\nrelocatable = %s\n"
                     % (os.path.dirname(real), py, str("--relocatable" in args).lower()))
        return 0
    if args[:2] == ["pip", "install"]:
        rest = args[2:]
        venv = os.path.dirname(os.path.dirname(venv_python(rest)))
        reqs, skip = [], False
        for i, a in enumerate(rest):
            if skip:
                skip = False
                continue
            if a in ("--python", "-r", "--requirement", "--link-mode", "--cache-dir", "-c", "--constraint"):
                skip = True
                if a in ("-r", "--requirement"):
                    with open(rest[i + 1]) as fh:
                        reqs += [r for r in map(parse_req, fh) if r]
                continue
            if not a.startswith("-"):
                r = parse_req(a)
                if r:
                    reqs.append(r)
        install(venv, reqs)
        return 0
    if args[:2] == ["pip", "freeze"]:
        sp = site(os.path.dirname(os.path.dirname(venv_python(args[2:]))))
        for d in sorted(glob.glob(os.path.join(sp, "*.dist-info"))):
            name, ver = os.path.basename(d)[:-len(".dist-info")].rsplit("-", 1)
            print("%s==%s" % (name, ver))
        return 0
    sys.stderr.write("uv stub: unexpected %r\n" % (args,))
    return 2

sys.exit(main(sys.argv[1:]))
'''


def _install_uv(bindir: Path) -> Path:
    """The uv stub, and a `curl` that reaches the index (a build inside a block probes it first): never the network."""
    bindir.mkdir(parents=True, exist_ok=True)
    p = bindir / "uv"
    p.write_text(f"#!{sys.executable}\n" + _UV)
    p.chmod(0o755)
    if not (bindir / "curl").exists():
        _tool(bindir, "curl", "exit 0")
    return p


def _tool(bindir: Path, name: str, body: str) -> Path:
    bindir.mkdir(parents=True, exist_ok=True)
    p = bindir / name
    p.write_text("#!/bin/sh\n" + body + "\n")
    p.chmod(0o755)
    return p


def _fake_arch(bindir: Path, arch: str) -> Path:
    """A `uname` that says the node is `arch` (everything else as the real one)."""
    return _tool(bindir, "uname", f'if [ "$1" = -m ]; then echo {arch}; else exec env PATH=/usr/bin:/bin uname "$@"; fi')


def _box(tmp: Path, **extra: str) -> dict[str, str]:
    """A login node's shell: the uv stub first on PATH, a home of its own."""
    _install_uv(tmp / "uvbin")
    (tmp / "home").mkdir(exist_ok=True)
    return {"PATH": f"{tmp / 'uvbin'}:{os.environ['PATH']}", "HOME": str(tmp / "home"), "LANG": "C",
            "HPCB_TEST_UV_LOG": str(tmp / "uv.jsonl"), **extra}


def _uv(tmp: Path) -> list[dict]:
    p = tmp / "uv.jsonl"
    return [json.loads(ln) for ln in p.read_text().splitlines()] if p.exists() else []


def _venvs(tmp: Path) -> int:
    return sum(1 for c in _uv(tmp) if c["argv"][:1] == ["venv"])


def _beside(root: Path) -> list[str]:
    """What an env root holds besides its build lock files and uv's own cache: envs, and anything a build left (a
    hidden build dir, a broken one) — which must not be there."""
    return sorted(x.name for x in root.iterdir() if not x.name.endswith(".lock") and x.name != ".uv-cache")


def _bash(script: str, env: dict[str, str], *, cwd: Path | None = None, timeout: float = 60):
    return subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=env, cwd=cwd, timeout=timeout)


def _built(tmp: Path, reqs: str = REQS, *, root: Path | None = None, python: str = PY, **extra: str):
    """An env built as the login node builds it: the build script under bash, the uv stub on PATH."""
    from hpc_bridge import envs

    spec = envs.parse({"requirements": reqs})
    key = envs.env_key(spec)
    root = root or tmp / "scratch" / "envs"
    p = _bash(envs.build_script(spec, key=key, root=str(root), python=python, token="t0"), _box(tmp, **extra), cwd=tmp)
    return spec, key, root, envs.parse_build(p.stdout), p


def _wrap_run(tmp: Path, command: str, act=None, *, nonce: str = NONCE, path_extra: str = ""):
    """One run_shell command as the session wrapper runs it (the env's activation inside), in session `default`."""
    from hpc_bridge import envs, session_shell

    s = session_shell.Session("default", str(tmp / "scratch"))
    script = session_shell.wrap(command, s, env_sh=envs.activation_sh(act, nonce) if act is not None else None)
    env = {"PATH": f"{path_extra}{os.environ['PATH']}", "HOME": str(tmp / "home"), "LANG": "C"}
    (tmp / "home").mkdir(exist_ok=True)
    return _bash(script, env, cwd=tmp), Path(s.state_dir)


# ---- the server's world: bash as the facility (the handoff tests'), the uv stub on its PATH ---------------------------

def _is_build(command: str) -> bool:
    from hpc_bridge import envs

    return envs.BUILD_TAG in command


class _EnvBlock(_Block):
    """The handoff tests' block, with the uv stub first on PATH and its calls logged; `compute_arch` makes the compute
    shape's nodes another architecture (a fake `uname`, on that shape's PATH only)."""

    def __init__(self, tmp: Path, manager: str, **kw) -> None:
        super().__init__(tmp, manager, **kw)
        self.tmp = tmp
        _install_uv(self.bin)
        self.compute_arch: str | None = None

    def env(self, compute: bool = False) -> dict[str, str]:
        e = {**super().env(), "HPCB_TEST_UV_LOG": str(self.tmp / "uv.jsonl"), "LANG": "C"}
        if compute and self.compute_arch:
            _fake_arch(self.tmp / "archbin", self.compute_arch)
            e["PATH"] = f"{self.tmp / 'archbin'}:{e['PATH']}"
        return e


class _EnvRunner(_Runner):
    """A runner that runs each command on its own shape's node (compute or login), and can hold a build out (a long
    install) until the test releases it."""

    def submit(self, command):
        from hpc_bridge.runner import _CANARY_SENTINEL, _escape_for_shellfunction

        if _CANARY_SENTINEL in command:
            return super().submit(command)
        self.commands.append(command)
        fut = _PendingFuture()
        self.futures.append(fut)
        if self.world.hold_builds and _is_build(command):
            fut.task_id = f"build-{len(self.world.held) + 1}"
            self.world.held.append((self, fut, command))
            return fut
        self.world.seq += 1
        fut.task_id = f"task-{self.world.seq}"                            # as the SDK's future has once sent
        fut.finish(self.run_here(_escape_for_shellfunction(command).format()))
        return fut

    def run_here(self, script: str) -> _Out:
        compute = bool(self.user_endpoint_config.get("compute", True))   # a facility's wire config carries no marker
        b = self.world.block
        p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=b.env(compute=compute), cwd=b.cwd,
                           timeout=60)
        return _Out(p.returncode, p.stdout, p.stderr)


class _EnvRunners(_Runners):
    def __init__(self, block: _EnvBlock) -> None:
        super().__init__(block)
        self.hold_builds = False
        self.held: list = []
        self.kwargs: list[tuple[dict, dict]] = []
        self.seq = 0

    def __call__(self, eid, user_endpoint_config=None, **kw):
        from hpc_bridge.runner import CanaryResult

        uec = dict(user_endpoint_config or {})
        r = _EnvRunner(self, eid, uec)
        if uec.get("compute", True):   # a block's worker answers with its job and walltime, read from inside it
            r._canary = CanaryResult(ok=True, worker_host="c1", worker_python=f"{PY}.0", worker_dill="0.3.9",
                                     worker_job=self.block.job, worker_remaining_s=900.0,
                                     answered_at=time.monotonic())
        self.built.append(r)
        self.kwargs.append((uec, dict(kw)))
        return r

    def release(self) -> None:
        """The held builds finish: each runs now, on its own shape's node."""
        from hpc_bridge.runner import _escape_for_shellfunction

        for r, fut, command in self.held:
            if not fut.done():
                fut.finish(r.run_here(_escape_for_shellfunction(command).format()))


async def _ssh(tmp: Path):
    """hpc-bridge's own endpoint over SSH (its own template), bound, its compute block confirmed and up."""
    from tests.test_handoff_phase2b import _ssh_app

    block = _EnvBlock(tmp, SSH_EID, scheduler="slurm", job="4242", pin=None, workers=2)
    app = _ssh_app(tmp, block=block)
    app.runners = _EnvRunners(block)
    app.runner_factory = app.runners
    app.batch_autoloop = False                                            # a batch's passes are the calls' own
    st = await server._ensure_endpoint_up(app, "compute", confirm_spend=True)
    assert st.status == "up", st
    return app


async def _mep(tmp: Path, monkeypatch, *, template: str, schema: dict, confirm: bool = True):
    """A facility endpoint (the fake pbs_mep kind) bound through the real connect path; its block confirmed and up."""
    from tests.fakes import MEP_UUID
    from tests.test_handoff_phase2b import _mep_app

    block = _EnvBlock(tmp, MEP_UUID, scheduler="pbs")
    app = await _mep_app(tmp, monkeypatch, template=template, schema=schema, block=block)
    app.runners = _EnvRunners(block)
    app.runner_factory = app.runners
    app.batch_autoloop = False
    if confirm:
        st = await server._ensure_endpoint_up(app, "compute", account="hpcb", confirm_spend=True)
        assert st.status == "up", st
    return app


def _builds(app, *, compute: bool | None = None) -> list[str]:
    """Every env build sent, on the login shape's runners (compute=False), the compute shape's (True) or any."""
    from hpc_bridge import envs

    return [c for r in app.runners.built
            if compute is None or bool(r.user_endpoint_config.get("compute", True)) == compute
            for c in r.commands if envs.BUILD_TAG in c]


def _key(reqs: str = REQS, **kw) -> str:
    from hpc_bridge import envs

    return envs.env_key(envs.parse({"requirements": reqs, **kw}))


def _home_env(tmp: Path, key: str, arch: str = ARCH) -> Path:
    return tmp / "home" / ".hpc-bridge" / "envs" / f"{key}-{arch}"


# ---- 1. the argument -------------------------------------------------------------------------------------------------

def test_the_env_argument_takes_three_forms_and_refuses_the_rest_by_name():
    from hpc_bridge import envs

    assert envs.parse(None) is None                                     # omitted: keep the session's
    assert envs.parse({}).kind == "none"                                # {}: no env
    a = envs.parse({"activate": "module load pytorch/2.4; conda activate ml", "name": "ml"})
    assert (a.kind, a.activate, a.name) == ("activate", "module load pytorch/2.4; conda activate ml", "ml")
    b = envs.parse({"requirements": "six==1.17.0\nnumpy>=2\n", "python": "3.11", "root": "/project/abc/envs"})
    assert (b.kind, b.requirements, b.python, b.root) == ("requirements", "six==1.17.0\nnumpy>=2", "3.11",
                                                        "/project/abc/envs")
    assert envs.parse({"requirements": "six", "root": "$HOME/envs"}).root == "$HOME/envs"
    for bad, word in [
        ({"activate": "x", "requirements": "y"}, "both"),
        ({"python": "3.11"}, "python"),
        ({"activate": "x", "python": "3.11"}, "python"),
        ({"requirements": "six", "python": "3.11; rm -rf ~"}, "python"),
        ({"requirements": "six", "pyhton": "3.11"}, "pyhton"),
        ({"requirements": ""}, "requirements"),
        ({"requirements": "# only a comment\n\n"}, "requirements"),
        ({"activate": "   "}, "activate"),
        ({"requirements": 5}, "requirements"),
        ({"requirements": "six\n" * (envs.MAX_REQUIREMENTS // 4)}, "requirements"),
        ({"activate": "x" * (envs.MAX_ACTIVATE + 1)}, "activate"),
        ({"requirements": "six", "root": "relative/envs"}, "root"),
        ({"requirements": "six", "root": "/scratch/$(id)/envs"}, "root"),
        ({"requirements": "six", "name": "my env!"}, "name"),
        ("six==1.17.0", "object"),
    ]:
        with pytest.raises(envs.EnvRefusal) as e:
            envs.parse(bad)
        assert word in str(e.value), (bad if not isinstance(bad, dict) else list(bad), str(e.value))


@pytest.mark.parametrize("line", ["-r other.txt", "--requirement=other.txt", "-c constraints.txt", "-e ./mypkg",
                                  "./mypkg", "../pkg", "/home/u/pkg-1.0-py3-none-any.whl", "file:///home/u/pkg",
                                  "mypkg @ file:///home/u/pkg", "--trusted-host example.org"])
def test_a_line_the_cache_key_cannot_see_or_that_turns_off_checks_is_refused_by_name(line):
    """The cache key is the hash of the TEXT: a file it names (another requirements file, a local package) can change
    under it unseen. And no line may switch off TLS checks."""
    from hpc_bridge import envs

    with pytest.raises(envs.EnvRefusal) as e:
        envs.parse({"requirements": f"six==1.17.0\n{line}\n"})
    assert line in str(e.value)


def test_the_users_own_index_lines_are_kept_and_listed_and_none_is_ever_added():
    from hpc_bridge import envs

    text = ("--index-url https://mirror.example.org/simple\n--extra-index-url https://download.pytorch.org/whl/cu121\n"
            "-f https://example.org/wheels\nsix==1.17.0 --hash=sha256:" + "a" * 64 + "\n--only-binary :all:\n")
    spec = envs.parse({"requirements": text})
    assert envs.index_lines(spec.requirements) == ["--index-url https://mirror.example.org/simple",
                                                   "--extra-index-url https://download.pytorch.org/whl/cu121",
                                                   "-f https://example.org/wheels"]
    plain = envs.parse({"requirements": "six==1.17.0"})
    for script in (envs.build_script(plain, key=envs.env_key(plain), root="/scratch/u/envs", python="3.11",
                                     token="t0", probe=True),
                   envs.fallback_worker_init(plain, key=envs.env_key(plain), root="/scratch/u/envs", python="3.11")):
        for flag in ("--index-url", "--extra-index-url", "--find-links", "--default-index", "--index ", "UV_INDEX",
                     "PIP_INDEX", "--allow-insecure-host", "--trusted-host"):
            assert flag not in script, flag


def test_the_cache_key_is_the_hash_of_the_text_and_the_python():
    from hpc_bridge import envs

    a = envs.parse({"requirements": "six==1.17.0\nnumpy>=2", "python": "3.11"})
    same = envs.parse({"requirements": "# my env\r\nsix==1.17.0   \r\n\r\nnumpy>=2  # the array lib\r\n",
                       "python": "3.11"})
    assert same.requirements == "six==1.17.0\nnumpy>=2" == envs.normalize_requirements(same.requirements)
    # the directory's name on the facility: pinned, so it never drifts silently (a drift would rebuild every env)
    want = hashlib.sha256(b"hpcb-env-v1\x003.11\x00six==1.17.0\nnumpy>=2").hexdigest()[:16]
    assert envs.env_key(a) == envs.env_key(same) == want
    # the order is the user's (with several indexes, the first wins); another Python is another env
    assert envs.env_key(envs.parse({"requirements": "numpy>=2\nsix==1.17.0", "python": "3.11"})) != want
    assert envs.env_key(envs.parse({"requirements": "six==1.17.0\nnumpy>=2", "python": "3.12"})) != want
    # the default Python is the client's (the proposal): the same env as naming it
    assert envs.default_python() == PY
    assert envs.env_key(envs.parse({"requirements": "six==1.17.0\nnumpy>=2"})) == envs.env_key(
        envs.parse({"requirements": "six==1.17.0\nnumpy>=2", "python": PY}))


def test_the_env_root_is_the_facilitys_scratch_else_home_with_a_size_warning():
    from hpc_bridge import envs

    root, warn = envs.env_root("/expanse/lustre/scratch/u/temp_project/.hpc-bridge")
    assert root == "/expanse/lustre/scratch/u/temp_project/.hpc-bridge/envs" and warn is None
    for home in ("$HOME/.hpc-bridge", "~/.hpc-bridge"):
        root, warn = envs.env_root(home)
        assert root == "$HOME/.hpc-bridge/envs" and warn and "home" in warn and "root" in warn, (root, warn)
    root, warn = envs.env_root("$HOME/.hpc-bridge", root="/project/abc/envs")   # the user's own root wins
    assert root == "/project/abc/envs" and warn is None


# ---- 2. where an env is built, and how it is used ----------------------------------------------------------------------

def test_where_each_kind_of_facility_builds_an_env_and_how_work_uses_it():
    from hpc_bridge import envs

    req, act = envs.parse({"requirements": "six"}), envs.parse({"activate": "module load x"})
    other = envs.parse({"requirements": "six", "python": "3.9" if PY != "3.9" else "3.10"})

    def d(spec, access, reads_requirements, reads_worker_init):
        return envs.delivery(spec, access=access, reads_requirements=reads_requirements,
                             reads_worker_init=reads_worker_init, client_python=PY)

    assert d(req, "ssh", None, True) == "login"            # our own endpoint: built on the login node, free
    assert d(other, "ssh", None, True) == "login"          # a separate user env: any Python
    assert d(act, "ssh", None, True) == "activate"
    assert d(act, "mep", None, None) == "activate"         # nothing to build, nothing to read
    assert d(req, "mep", True, True) == "facility"         # the template builds it into the worker's env
    assert d(req, "mep", False, True) == "worker_init"     # Polaris: the block's worker_init builds it
    assert d(other, "mep", False, True) == "worker_init"
    for spec, rr, rw, words in [
        (req, False, False, ("requirements", "worker_init")),      # nothing to carry it: refused by name
        (req, None, True, ("could not be read",)),                 # never a guess about the template
        (other, True, True, (other.python, PY)),                   # the worker's env runs the client's Python
    ]:
        with pytest.raises(envs.EnvRefusal) as e:
            d(spec, "mep", rr, rw)
        assert all(w in str(e.value) for w in words), str(e.value)


def test_the_gate_reads_the_published_template_and_schema_of_each_kind():
    from hpc_bridge.facility.mep import MEPFacility

    def fac(template, schema, key_map=None):
        f = MEPFacility("eid", "fac", {})
        f.template_text, f.schema, f.key_map = template, schema, dict(key_map or {})
        return f

    delta, anvil = fac(DELTA_TEMPLATE, DELTA_SCHEMA), fac(ANVIL_TEMPLATE, ANVIL_SCHEMA)
    polaris, pbs = fac(POLARIS_TEMPLATE, POLARIS_SCHEMA), fac(FAKE_TEMPLATE, FAKE_SCHEMA, {"partition": "queue"})
    req = fac(REQ_TEMPLATE, REQ_SCHEMA, {"partition": "queue"})
    assert delta.reads("requirements") is True and delta.reads("worker_init") is True
    assert anvil.reads("requirements") is True and anvil.reads("venvs_dir") is True and anvil.reads("worker_init")
    assert req.reads("requirements") is True
    assert polaris.reads("requirements") is False and polaris.reads("worker_init") is True
    assert pbs.reads("requirements") is False and pbs.reads("worker_init") is True
    neither = fac(NEITHER_TEMPLATE, NEITHER_SCHEMA)
    assert neither.reads("requirements") is False and neither.reads("worker_init") is False   # a comment is no use
    # a strict schema that does not list it drops it at the wire, whatever the template says
    strict = fac(DELTA_TEMPLATE, {"type": "object", "additionalProperties": False,
                                  "properties": {"worker_init": {"type": "string"}}})
    assert strict.reads("requirements") is False and strict.reads("worker_init") is True
    assert fac(None, None).reads("requirements") is None                # not read: unknown, never a guess


# ---- 3. the build, its check, its marker (bash, the uv stub) ----------------------------------------------------------

def test_the_build_makes_a_relocatable_venv_keeps_its_lock_and_marks_its_arch(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, b, p = _built(tmp_path)
    assert p.returncode == 0 and b is not None and b.ok and not b.cached, (p.stdout, p.stderr)
    d = root / f"{key}-{ARCH}"
    assert (b.key, b.path, b.arch) == (key, str(d), ARCH) and b.python.startswith(PY)
    assert (d / envs.MARKER).is_file() and (d / "bin" / "python").exists()
    lock = (d / "requirements.lock").read_bytes()
    assert b"six==1.17.0" in lock and b.lock_sha256 == hashlib.sha256(lock).hexdigest()
    venv = next(c["argv"] for c in _uv(tmp_path) if c["argv"][:1] == ["venv"])
    assert "--relocatable" in venv and venv[venv.index("--python") + 1] == PY        # built beside, renamed in
    # nothing of the build is left beside it (its lock file and uv's cache aside); a second build is a cache hit
    assert _beside(root) == [f"{key}-{ARCH}"]
    _spec, key2, root, again, _p = _built(tmp_path)
    assert key2 == key and again.ok and again.cached and _venvs(tmp_path) == 1


def test_the_requirements_text_reaches_the_installer_as_a_file_never_as_shell(tmp_path):
    from hpc_bridge import envs

    evil = 'six==1.17.0\n$(touch PWNED1)\n`touch PWNED2`\n"; touch PWNED3; "\n'
    spec = envs.parse({"requirements": evil})
    script = envs.build_script(spec, key=envs.env_key(spec), root=str(tmp_path / "envs"), python=PY, token="t1")
    for frag in ("touch PWNED", "$(touch", "`touch"):
        assert frag not in script, frag
    p = _bash(script, _box(tmp_path), cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    for name in ("PWNED1", "PWNED2", "PWNED3"):
        assert not (tmp_path / name).exists() and not (tmp_path / "home" / name).exists(), name
    d = tmp_path / "envs" / f"{envs.env_key(spec)}-{ARCH}"
    assert (d / "requirements.in").read_text().strip() == spec.requirements    # what the installer read, verbatim


def test_a_failed_build_says_why_in_the_installers_words_and_claims_nothing(tmp_path):
    _spec, key, root, b, p = _built(tmp_path, "nosuchpkg==1.0\n")
    assert p.returncode == 0 and b is not None and not b.ok and "nosuchpkg" in b.error and "No solution" in b.error
    assert not (root / f"{key}-{ARCH}").exists() and (root / f"{key}-{ARCH}.failed").is_file()
    assert [x.name for x in root.iterdir() if ".build" in x.name] == []


def test_the_check_tells_ready_partial_gone_failed_and_another_arch(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, _b, _p = _built(tmp_path)
    d = root / f"{key}-{ARCH}"

    def check(path_extra: str = ""):
        env = _box(tmp_path)
        env["PATH"] = path_extra + env["PATH"]
        out = _bash(envs.check_script(key=key, root=str(root)), env, cwd=tmp_path)
        return envs.parse_check(out.stdout)

    assert check().state == "ready"
    f = next(d.glob("lib/python*/site-packages/six/__init__.py"))
    text = f.read_text()
    f.unlink()                                                     # an atime purge took one file of it
    c = check()
    assert c.state == "partial" and c.missing >= 1
    f.write_text(text)
    extra = next(d.glob("lib/python*/site-packages")) / "extra-1.0.dist-info"
    extra.mkdir()
    assert check().state == "partial"                              # not the packages it was built with
    shutil.rmtree(extra)
    assert check().state == "ready"
    _fake_arch(tmp_path / "archbin", "fakearch")
    c = check(f"{tmp_path / 'archbin'}:")
    assert c.state == "arch" and c.arch == "fakearch"
    shutil.rmtree(d)
    assert check().state == "gone"
    (root / f"{key}-{ARCH}.failed").write_text("error: something\n")
    assert check().state == "failed"


# ---- 4. the activation: per command, never persisted (bash, the session wrapper) ---------------------------------------

def test_a_command_runs_in_the_env_and_the_session_keeps_nothing_of_it(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, _b, _p = _built(tmp_path)
    d = root / f"{key}-{ARCH}"
    act = envs.Activation(how="built", key=key, root=str(root))
    home = tmp_path / "home"
    p, sd = _wrap_run(tmp_path, 'mkdir -p "$HOME/mybin"; export PATH="$HOME/mybin:$PATH"', None)
    assert p.returncode == 0, p.stderr
    p, sd = _wrap_run(tmp_path, 'echo "PY=$(command -v python) VE=$VIRTUAL_ENV P=$PATH"; '
                                "python -c 'import six, sys; print(sys.prefix, six.__version__)'; export MINE=1", act)
    assert p.returncode == 0, p.stderr
    assert f"PY={d}/bin/python VE={d} P={d}/bin:{home}/mybin:" in p.stdout and f"{d} 1.17.0" in p.stdout
    saved = (sd / ".env").read_text()
    assert "MINE=1" in saved and f"{home}/mybin" in saved                # the command's own changes persist…
    assert str(d) not in saved and "VIRTUAL_ENV" not in saved            # …the env's never do
    # another env next: nothing of the first is left on PATH
    other = envs.Activation(how="activate", key="b" * 16, line="export HPCB_TAG=b")
    p, sd = _wrap_run(tmp_path, 'echo "T=$HPCB_TAG V=${VIRTUAL_ENV:-none} P=$PATH"', other)
    assert "T=b V=none" in p.stdout and str(d) not in p.stdout and f"{home}/mybin" in p.stdout
    assert "HPCB_TAG" not in (sd / ".env").read_text()
    p, _sd = _wrap_run(tmp_path, 'echo "T=${HPCB_TAG:-none}"')            # and no env at all
    assert "T=none" in p.stdout


def test_a_purged_failed_or_foreign_arch_env_runs_nothing_and_says_why(tmp_path):
    from hpc_bridge import envs

    _spec, key, root, _b, _p = _built(tmp_path)
    act = envs.Activation(how="built", key=key, root=str(root))
    _fake_arch(tmp_path / "archbin", "fakearch")
    p, sd = _wrap_run(tmp_path, "echo RAN", act, path_extra=f"{tmp_path / 'archbin'}:")
    r = envs.refusal(p.returncode, p.stderr, NONCE)
    assert p.returncode == envs.ENV_RC and "RAN" not in p.stdout and not (sd / ".cwd").exists()
    assert r is not None and (r.why, r.key) == ("arch", key) and "fakearch" in r.detail and ARCH in r.detail
    shutil.rmtree(root / f"{key}-{ARCH}")                                 # scratch purged
    p, _sd = _wrap_run(tmp_path, "echo RAN", act)
    assert "RAN" not in p.stdout and envs.refusal(p.returncode, p.stderr, NONCE).why == "gone"
    (root / f"{key}-{ARCH}.failed").write_text("No solution found\n")
    p, _sd = _wrap_run(tmp_path, "echo RAN", act)
    assert envs.refusal(p.returncode, p.stderr, NONCE).why == "failed"
    bad = envs.Activation(how="activate", key="c" * 16, line="false")
    p, _sd = _wrap_run(tmp_path, "echo RAN", bad)
    r = envs.refusal(p.returncode, p.stderr, NONCE)
    assert "RAN" not in p.stdout and r.why == "activate" and "rc=1" in r.detail
    # a command cannot forge one: the nonce is the dispatch's, and only the env's rc counts
    assert envs.refusal(envs.ENV_RC, "HPCB_ENV_REFUSED 0000000000000000 gone x\n", NONCE) is None
    assert envs.refusal(0, f"{envs.REFUSED} {NONCE} gone {key} {ARCH}\n", NONCE) is None
    assert envs.refusal(envs.ENV_RC, f"noise\n{envs.REFUSED} {NONCE} gone {key} {ARCH}\nmore\n", NONCE).why == "gone"


def test_a_batch_row_runs_in_its_env_and_its_slot_still_sizes_the_libraries(tmp_path):
    from hpc_bridge import batch, envs
    from tests.test_batch_sizing import _fake_tool, _result, _slot, _spawn, _wrapper_env

    _spec, key, root, _b, _p = _built(tmp_path)
    _fake_tool(tmp_path, "taskset", "true")
    built = envs.Activation(how="built", key=key, root=str(root))
    rep = _result(_spawn(batch.task_script("echo PY=$(command -v python)", results_dir=str(tmp_path / "rd"), index=0,
                                           attempt=1, task_walltime_s=60, slot=_slot(cores=1, cpu_ids=(0, 1)),
                                           env_sh=envs.activation_sh(built, NONCE)), _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.tail.strip() == f"PY={root}/{key}-{ARCH}/bin/python", rep
    # a module that sets its own thread count cannot widen the row: the slot's env comes after the activation
    mod = envs.Activation(how="activate", key="d" * 16, line="export OMP_NUM_THREADS=64 HPCB_TAG=act")
    rep = _result(_spawn(batch.task_script('echo "OMP=$OMP_NUM_THREADS T=$HPCB_TAG"', results_dir=str(tmp_path / "rd"),
                                           index=1, attempt=1, task_walltime_s=60, slot=_slot(cores=1, cpu_ids=(0, 1)),
                                           env_sh=envs.activation_sh(mod, NONCE)), _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.tail.strip() == "OMP=1 T=act", rep
    shutil.rmtree(root / f"{key}-{ARCH}")
    rep = _result(_spawn(batch.task_script("echo RAN", results_dir=str(tmp_path / "rd"), index=2, attempt=1,
                                           task_walltime_s=60, env_sh=envs.activation_sh(built, NONCE)),
                         _wrapper_env(tmp_path)))
    r = envs.refusal(rep.rc, rep.stderr_tail, NONCE)
    assert rep.rc == envs.ENV_RC and "RAN" not in rep.tail and r is not None and r.why == "gone", rep
    assert rep.ran is False                                               # the row ran nothing: no failure of its own


# ---- 5. the MEP fallback: the block's own worker_init builds it (bash) -------------------------------------------------

def test_the_fallback_worker_init_builds_once_and_never_touches_the_workers_env(tmp_path):
    from hpc_bridge import envs

    spec = envs.parse({"requirements": REQS})
    key, root = envs.env_key(spec), tmp_path / "home" / ".hpc-bridge" / "envs"
    wi = envs.fallback_worker_init(spec, key=key, root="$HOME/.hpc-bridge/envs", python=PY)
    env = _box(tmp_path)
    probe = wi + '\necho "P=$PATH|V=${VIRTUAL_ENV:-none}|D=$PWD"'
    p = _bash(probe, env, cwd=tmp_path)
    assert p.returncode == 0 and f"P={env['PATH']}|V=none|D={tmp_path}" in p.stdout, (p.stdout, p.stderr)
    assert (root / f"{key}-{ARCH}" / envs.MARKER).is_file() and _venvs(tmp_path) == 1
    p = _bash(probe, env, cwd=tmp_path)                                   # the next block: reused, not rebuilt
    assert p.returncode == 0 and _venvs(tmp_path) == 1
    # a build that fails is recorded beside it, never fatal to the block (its worker must still start and answer)
    bad = envs.parse({"requirements": "nosuchpkg==1.0"})
    kb = envs.env_key(bad)
    p = _bash(envs.fallback_worker_init(bad, key=kb, root="$HOME/.hpc-bridge/envs", python=PY) + "\necho ALIVE",
              env, cwd=tmp_path)
    assert p.returncode == 0 and "ALIVE" in p.stdout
    assert "nosuchpkg" in (root / f"{kb}-{ARCH}.failed").read_text() and not (root / f"{kb}-{ARCH}").exists()


def test_a_build_inside_a_block_probes_the_index_first_and_names_the_proxy_when_it_cannot_reach_it(tmp_path):
    from hpc_bridge import envs

    spec = envs.parse({"requirements": REQS})
    key = envs.env_key(spec)
    env = _box(tmp_path)
    _tool(tmp_path / "uvbin", "curl", f'echo "proxy=${{https_proxy:-}}" >> {tmp_path / "curl.log"}; exit 7')
    wi = envs.fallback_worker_init(spec, key=key, root=str(tmp_path / "envs"), python=PY)
    p = _bash(wi, env, cwd=tmp_path)
    failed = (tmp_path / "envs" / f"{key}-{ARCH}.failed").read_text()
    assert p.returncode == 0 and _venvs(tmp_path) == 0                    # nothing was tried that could not work
    assert "pypi.org" in failed and "proxy" in failed, failed
    # a facility's proxy, given: the probe and the build go through it
    (tmp_path / "envs" / f"{key}-{ARCH}.failed").unlink()
    _tool(tmp_path / "uvbin", "curl", f'echo "proxy=${{https_proxy:-}}" >> {tmp_path / "curl.log"}; exit 0')
    wi = envs.fallback_worker_init(spec, key=key, root=str(tmp_path / "envs"), python=PY,
                                   proxy={"https_proxy": "http://proxy.example.org:3128"})
    p = _bash(wi, env, cwd=tmp_path)
    assert "proxy=http://proxy.example.org:3128" in (tmp_path / "curl.log").read_text()
    assert all(c["https_proxy"] == "http://proxy.example.org:3128" for c in _uv(tmp_path)) and _venvs(tmp_path) == 1
    with pytest.raises(envs.EnvRefusal):
        envs.fallback_worker_init(spec, key=key, root="/x", python=PY, proxy={"https_proxy": "http://x; rm -rf ~"})
    with pytest.raises(envs.EnvRefusal):
        envs.fallback_worker_init(spec, key=key, root="/x", python=PY, proxy={"LD_PRELOAD": "/tmp/x.so"})


def test_two_blocks_building_at_once_leave_one_env_and_nothing_else(tmp_path):
    """Correctness is the rename (the endpoint venv's rule): of two builds, one claims the path and the other uses it.
    Where the node has `flock`, the second waits for the first instead of building too."""
    from hpc_bridge import envs

    spec = envs.parse({"requirements": REQS})
    key = envs.env_key(spec)
    wi = envs.fallback_worker_init(spec, key=key, root=str(tmp_path / "envs"), python=PY)
    env = _box(tmp_path, HPCB_TEST_UV_SLOW="1.5")
    procs = [subprocess.Popen([_BASH, "-c", wi], env=env, cwd=tmp_path, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) for _ in range(2)]
    assert all(p.wait(timeout=60) == 0 for p in procs)
    names = _beside(tmp_path / "envs")
    assert names == [f"{key}-{ARCH}"], names
    if _FLOCK:
        assert _venvs(tmp_path) == 1


def test_a_facility_pin_installs_into_the_env_the_template_built_never_over_it(tmp_path):
    """Delta's and Anvil's entries pin the worker (gce at the facility's version; Delta's parsl too) by a worker_init
    that makes and activates a venv of its own. A template that builds `requirements` (4.17+'s default) does so BEFORE
    the user's worker_init: left as it is, the pin's venv would shadow the env just built. With an env active, the pin
    installs into it instead; with none, it runs as before."""
    from hpc_bridge import envs
    from hpc_bridge.facility.mep import MEPFacility
    from tests.test_resource_review_r2 import _seed

    for fname, eid, pins in (("ncsa-delta.yaml", "delta", ("globus-compute-endpoint==4.16.0", "parsl==2026.8.10")),
                             ("anvil.yaml", "anvil", ("globus-compute-endpoint==",))):
        e = _seed(fname, eid)
        fac = MEPFacility.from_entry(e, client_factory=lambda: None)
        fac.endpoint_version = "4.16.0"
        wi = fac.sanitize_uec({"worker_init": e.compute.env_setup})["worker_init"]
        into = envs.pin_into_active(wi)
        assert into is not None, eid
        box = tmp_path / eid
        env = _box(box)
        req = box / "req-venv"
        _bash(f"uv venv -q --python {PY} {req}", env)                    # the env the template built from requirements
        p = _bash(f'. "{req}/bin/activate"\n{into}\necho "V=$VIRTUAL_ENV"', env, cwd=box)
        assert p.returncode == 0 and f"V={req}" in p.stdout, (eid, p.stdout, p.stderr)
        installs = [c for c in _uv(box) if c["argv"][:2] == ["pip", "install"]]
        assert installs and all(c["venv"] == str(req) for c in installs), (eid, installs)
        assert all(any(pin in a for a in installs[-1]["argv"]) for pin in pins), (eid, installs[-1])
        p = _bash(f'{into}\necho "V=$VIRTUAL_ENV"', env, cwd=box)          # no env active: the pin's own venv
        assert p.returncode == 0 and "V=" + str(box / "home" / "hpc-bridge") in p.stdout, (eid, p.stdout, p.stderr)


# ---- 6. the server on a facility hpc-bridge stood up (SSH) ------------------------------------------------------------

async def test_ssh_a_built_env_is_built_on_the_login_node_free_and_needs_no_new_block(tmp_path):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    rt = app.shapes["compute"]
    runner, epoch = rt.runner, rt.block_epoch
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    e, key = st.env, _key()
    assert st.status == "up" and rt.spend_confirmed and rt.block_epoch == epoch and rt.runner is runner, st
    assert (e.kind, e.how, e.state, e.key, e.cached, e.new_block, e.billed) == (
        "built", "login", "ready", key, False, False, None), e
    assert e.python.startswith(PY) and e.path == str(_home_env(tmp_path, key)), e
    assert e.lock_sha256 == hashlib.sha256(Path(e.path, "requirements.lock").read_bytes()).hexdigest()
    assert len(_builds(app, compute=False)) == 1 and _builds(app, compute=True) == []
    out = await server._run_shell(app, "python -c 'import six, sys; print(sys.prefix, six.__version__)'",
                                  agent_call=True)
    assert out.phase == "complete" and out.stdout.split() == [e.path, "1.17.0"], out
    login = await server._run_shell(app, "python -c 'import sys; print(sys.prefix)'", shape="login", agent_call=True)
    assert login.stdout.strip() == e.path                                  # the session's env holds on both shapes
    assert envs.REFUSED in app.shapes["login"].runner.commands[-1]


async def test_ssh_the_same_env_again_is_a_cache_hit_even_after_a_restart(tmp_path):
    app = await _ssh(tmp_path)
    first = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    again = (await server._ensure_endpoint_up(app, "compute", env={"requirements": "six==1.17.0   \n\n"})).env
    assert (again.key, again.cached, again.state, again.path) == (first.key, True, "ready", first.path)
    assert len(_builds(app)) == 1 and _venvs(tmp_path) == 1               # this server knew it: nothing was sent
    app2 = await _ssh(tmp_path)                                           # a restarted server, the same facility
    st = await server._ensure_endpoint_up(app2, "compute", env={"requirements": REQS})
    assert (st.env.cached, st.env.path, st.env.state) == (True, first.path, "ready")
    assert _venvs(tmp_path) == 1                                          # found by the build's own check


async def test_ssh_switching_env_needs_no_new_block_and_leaves_nothing_of_the_old(tmp_path):
    app = await _ssh(tmp_path)
    rt = app.shapes["compute"]
    runner, epoch = rt.runner, rt.block_epoch
    built = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    st = await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=b", "name": "tagged"})
    assert st.status == "up" and (st.env.how, st.env.name, st.env.state) == ("activate", "tagged", "ready"), st
    assert rt.spend_confirmed and rt.block_epoch == epoch and rt.runner is runner and not rt.runner_stale
    out = await server._run_shell(app, 'echo "T=$HPCB_TAG P=$PATH"', agent_call=True)
    assert "T=b" in out.stdout and built.path not in out.stdout, out
    st = await server._ensure_endpoint_up(app, "compute", env={})
    assert st.status == "up" and st.env is None
    out = await server._run_shell(app, 'echo "T=${HPCB_TAG:-none} V=${VIRTUAL_ENV:-none}"', agent_call=True)
    assert "T=none V=none" in out.stdout, out


async def test_ssh_an_existing_env_is_checked_on_the_login_node_and_a_failure_there_is_said(tmp_path):
    app = await _ssh(tmp_path)
    ok = await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=ok"})
    assert ok.env.state == "ready"
    bad = await server._ensure_endpoint_up(app, "compute", env={"activate": "echo no such module >&2; false"})
    # login and compute nodes may load different modules: said, kept, and checked again with each command
    assert bad.env.state == "unchecked" and "no such module" in (bad.env.error or "")
    assert "each command" in (bad.notice or ""), bad.notice
    out = await server._run_shell(app, "echo RAN", agent_call=True)
    assert out.phase == "failed" and "RAN" not in out.stdout and "no such module" in (out.notice or ""), out


async def test_ssh_a_failed_build_says_why_and_keeps_the_env_that_works(tmp_path):
    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=old"})
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": "nosuchpkg==1.0"})
    assert st.status == "up" and st.env.state == "failed" and "nosuchpkg" in st.env.error, st.env
    assert "not changed" in (st.notice or ""), st.notice
    out = await server._run_shell(app, 'echo "T=$HPCB_TAG"', agent_call=True)
    assert "T=old" in out.stdout, out


async def test_ssh_the_build_runs_on_its_own_runner_and_never_holds_the_internal_session(tmp_path):
    from hpc_bridge import batch, envs

    app = await _ssh(tmp_path)
    app.runners.hold_builds = True                                        # a long install
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert st.status == "up" and st.env.state == "building" and st.env.task_id == "build-1", st.env
    walls = [kw.get("walltime") for uec, kw in app.runners.kwargs if not uec.get("compute", True)]
    assert envs.BUILD_S in walls                                          # its own runner, a build's own walltime
    assert server._busy_session(app, "login", server._INTERNAL_SESSION) is None
    assert (await batch.login_runner(app)("echo internal")).phase == "complete"   # the scheduler ops still run
    out = await server._run_shell(app, "echo RAN", agent_call=True)
    assert out.phase == "env_building" and "RAN" not in out.stdout, out
    app.runners.release()
    st = await server._ensure_endpoint_up(app, "compute")
    assert st.env.state == "ready" and st.env.key == _key()
    out = await server._run_shell(app, "true", agent_call=True)
    internal = await batch.login_runner(app)("true")
    assert out.phase == internal.phase == "complete"
    assert envs.REFUSED in app.shapes["compute"].runner.commands[-1]          # the agent's command: activated
    assert envs.REFUSED not in app.shapes["login"].runner.commands[-1]        # hpc-bridge's own: never


async def test_ssh_a_purged_env_runs_nothing_and_is_rebuilt_on_the_login_node(tmp_path):
    app = await _ssh(tmp_path)
    e = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    shutil.rmtree(e.path)                                                 # scratch purged
    out = await server._run_shell(app, "echo RAN", agent_call=True)
    assert out.phase == "env_building" and "RAN" not in out.stdout and "purged" in (out.notice or ""), out
    assert len(_builds(app, compute=False)) == 2 and _builds(app, compute=True) == []
    out = await server._run_shell(app, "echo RAN", agent_call=True)
    assert out.phase == "complete" and "RAN" in out.stdout and _venvs(tmp_path) == 2, out


async def test_ssh_a_partly_purged_env_fails_its_check_and_is_rebuilt(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _ssh(tmp_path)
    e = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    victim = next(Path(e.path).glob("lib/python*/site-packages/six/__init__.py"))
    victim.unlink()                                                       # the marker survived, a module did not
    monkeypatch.setattr(envs, "VERIFY_EVERY_S", 0.0)
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert st.env.state == "ready" and st.env.cached is False and victim.is_file() and _venvs(tmp_path) == 2, st.env


async def test_ssh_a_compute_node_of_another_arch_builds_its_own_env_in_a_task_on_the_block(tmp_path):
    app = await _ssh(tmp_path)
    e = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env   # this machine's arch
    app.runners.block.compute_arch = "fakearch"
    out = await server._run_shell(app, "echo RAN", agent_call=True)
    assert out.phase == "env_building" and "RAN" not in out.stdout, out
    assert "fakearch" in (out.notice or "") and "billed" in (out.notice or ""), out.notice
    assert len(_builds(app, compute=True)) == 1                           # in a task on the confirmed block
    assert (_home_env(tmp_path, e.key, "fakearch") / ".hpcb-env").is_file()
    out = await server._run_shell(app, "python -c 'import sys; print(sys.prefix)'", agent_call=True)
    assert out.stdout.strip() == str(_home_env(tmp_path, e.key, "fakearch")), out
    login = await server._run_shell(app, "python -c 'import sys; print(sys.prefix)'", shape="login", agent_call=True)
    assert login.stdout.strip() == e.path                                 # each node its own arch's build


async def test_the_env_says_its_users_own_index_lines_its_root_and_installings_cost_once(tmp_path):
    app = await _ssh(tmp_path)
    first = await server._ensure_endpoint_up(
        app, "compute", env={"requirements": "--extra-index-url https://download.pytorch.org/whl/cpu\nsix==1.17.0"})
    assert first.env.index_lines == ["--extra-index-url https://download.pytorch.org/whl/cpu"]
    assert "download.pytorch.org" in (first.notice or "") and "build code" in (first.notice or ""), first.notice
    assert first.env.warning and "home" in first.env.warning               # the default root is under home here
    proj = tmp_path / "proj" / "envs"
    second = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS, "root": str(proj)})
    assert second.env.root == str(proj) and second.env.path == str(proj / f"{_key()}-{ARCH}")
    assert second.env.warning is None and "build code" not in (second.notice or "")   # said once a session


async def test_run_batch_takes_its_own_env_without_a_new_block_records_and_shows_it(tmp_path):
    from hpc_bridge import batch

    app = await _ssh(tmp_path)
    rt = app.shapes["compute"]
    epoch = rt.block_epoch
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=session"})
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 1}, {"x": 2}], task_walltime_s=60,
                                  env={"requirements": REQS})
    key = _key()
    assert out.batch_id and out.env is not None and (out.env.key, out.env.how) == (key, "login"), out
    assert rt.spend_confirmed and rt.block_epoch == epoch                 # the session's block, unchanged
    rec = json.loads(batch.ledger_of(app).batch(out.batch_id)["env"])
    assert (rec["kind"], rec["how"], rec["key"]) == ("built", "login", key) and rec["python"].startswith(PY)
    assert rec["path"] == str(_home_env(tmp_path, key))
    st = await server._status(app, out.batch_id)
    assert st.env.key == key
    listing = await server._status(app)
    assert key[:8] in (next(x for x in listing.entries if x.id == out.batch_id).env or "")
    # the session's env is still the session's
    sh = await server._run_shell(app, 'echo "T=$HPCB_TAG"', agent_call=True)
    assert "T=session" in sh.stdout


async def test_a_batch_row_whose_env_is_gone_ran_nothing_goes_back_and_the_env_is_rebuilt(tmp_path):
    """The batch engine's own world (rows in flight until the test settles them): a row refused by its env's check is
    no failure — it ran nothing — so it goes back to the backlog while the env is rebuilt on the login node."""
    from hpc_bridge import batch, envs
    from tests.test_batch_sizing import _app_with, _report

    app, _rt, runner = _app_with(None)
    app.scratch_root = str(tmp_path / "scratch")
    env = _box(tmp_path)

    def finish_builds():
        for fut, cmd in zip(runner.futures, runner.commands, strict=True):
            if envs.BUILD_TAG in cmd and not fut.done():
                p = _bash(cmd, env, cwd=tmp_path)
                fut.finish(_Res(p.returncode, p.stdout, p.stderr))

    await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    finish_builds()
    assert (await server._ensure_endpoint_up(app, "compute")).env.state == "ready"
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 0}, {"x": 1}], task_walltime_s=60)
    rows = [c for c in runner.commands if batch.RESULT_MARK in c]
    assert len(rows) == 2 and all(envs.REFUSED in c for c in rows), out
    nonce = re.search(envs.REFUSED + r" ([0-9a-f]{16})", rows[0]).group(1)
    i0 = runner.commands.index(rows[0])
    gone = f"{envs.REFUSED} {nonce} gone {_key()} {ARCH}\n"
    runner.futures[i0].finish(_report(0, rc=envs.ENV_RC, stderr_tail_b64=base64.b64encode(gone.encode()).decode()))
    runner.futures[runner.commands.index(rows[1])].finish(_report(1))
    await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id, detail="page")
    by = {r.index: r for r in st.results}
    assert by[0].state == "backlog" and by[1].state == "done" and st.counts.failed == 0, st
    assert st.phase == "waiting" and "env" in (st.reason or ""), st
    assert sum(envs.BUILD_TAG in c for c in runner.commands) == 2          # rebuilt (SSH: the login node, free)


# ---- 7. Jobs mode ------------------------------------------------------------------------------------------------------

async def test_a_jobs_script_activates_its_env_after_the_sessions_and_runs_in_it(tmp_path):
    from hpc_bridge import envs
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import _app, _confirmed

    sched = FakeSched(tmp_path / "sched", user="u")
    _install_uv(sched.bin)
    app = _app(tmp_path, sched)
    out = await _confirmed(app, command="python -c 'import six; print(\"SIX\", six.__version__)'",
                           env={"requirements": REQS})
    key = _key()
    assert out.job.env is not None and (out.job.env.key, out.job.env.how) == (key, "login"), out.job
    text = Path(out.job.script_path).read_text()
    lines = text.splitlines()
    at_session = next(i for i, ln in enumerate(lines) if "session.env" in ln)
    at_env = next(i for i, ln in enumerate(lines) if key in ln)
    at_cmd = next(i for i, ln in enumerate(lines) if ln.startswith("( eval"))
    assert at_session < at_env < at_cmd, text                               # frozen in the hashed script
    p = subprocess.run([_BASH, out.job.script_path], capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home")})
    assert p.returncode == 0 and "SIX 1.17.0" in p.stdout, (p.stdout, p.stderr)
    shutil.rmtree(Path(out.job.env.path))                                  # purged before it ran: it runs nothing
    p = subprocess.run([_BASH, out.job.script_path], capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home")})
    assert p.returncode == envs.ENV_RC and "SIX" not in p.stdout and envs.REFUSED in p.stderr


# ---- 8. the server on a facility's multi-user endpoint ----------------------------------------------------------------

async def test_mep_a_template_that_builds_requirements_gets_them_and_the_terms_say_a_new_block(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _mep(tmp_path, monkeypatch, template=REQ_TEMPLATE, schema=REQ_SCHEMA)
    rt = app.shapes["compute"]
    ask = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert ask.status == "needs_confirmation" and not rt.spend_confirmed, ask
    e = ask.block.env
    assert (e.how, e.kind, e.new_block, e.python) == ("facility", "built", True, PY), e
    assert "new user endpoint" in (ask.notice or "") and "new block" in (ask.notice or ""), ask.notice
    assert "billed" in (e.billed or "") and "package index" in (e.billed or ""), e.billed
    yes = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS}, confirm_spend=True)
    assert yes.status == "up" and yes.env.how == "facility" and yes.block is not None, yes
    sent = app.runners.built[-1].user_endpoint_config
    assert sent["requirements"] == "six==1.17.0" and "worker_init" not in sent, sent   # the facility builds it
    await server._run_shell(app, "true", agent_call=True)
    assert envs.REFUSED not in app.shapes["compute"].runner.commands[-1]      # it IS the worker's env: no activation
    other = "3.9" if PY != "3.9" else "3.10"
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS, "python": other})
    assert other in (st.notice or "") and PY in (st.notice or "") and app.shapes["compute"].spend_confirmed, st


async def test_mep_without_requirements_the_blocks_worker_init_builds_it_first_build_billed(tmp_path, monkeypatch):
    from hpc_bridge import envs

    app = await _mep(tmp_path, monkeypatch, template=FAKE_TEMPLATE, schema=FAKE_SCHEMA)
    ask = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    e = ask.block.env
    assert ask.status == "needs_confirmation" and (e.how, e.new_block) == ("worker_init", True), ask
    assert "first build" in (e.billed or "") and "billed" in (e.billed or ""), e.billed
    yes = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS}, confirm_spend=True)
    assert yes.status == "up", yes
    sent = app.runners.built[-1].user_endpoint_config
    key = _key()
    assert "requirements" not in sent and key in sent["worker_init"] and envs.BUILD_TAG in sent["worker_init"], sent
    # the block starts: the facility runs our worker_init before its own; it builds, and changes nothing of the worker
    b = app.runners.block
    p = subprocess.run([_BASH, "-c", sent["worker_init"] + '\necho "V=${VIRTUAL_ENV:-none}"'], capture_output=True,
                       text=True, env=b.env(compute=True), cwd=b.cwd, timeout=60)
    assert p.returncode == 0 and "V=none" in p.stdout, (p.stdout, p.stderr)
    out = await server._run_shell(app, "python -c 'import six, sys; print(sys.prefix)'", agent_call=True)
    assert out.phase == "complete" and out.stdout.strip() == str(_home_env(tmp_path, key)), out


async def test_mep_a_facility_that_can_carry_neither_is_refused_by_name_and_changes_nothing(tmp_path, monkeypatch):
    app = await _mep(tmp_path, monkeypatch, template=NEITHER_TEMPLATE, schema=NEITHER_SCHEMA)
    rt = app.shapes["compute"]
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})
    assert "requirements" in (st.notice or "") and "worker_init" in (st.notice or ""), st.notice
    assert "Nothing in this call was set or started" in (st.notice or "") and rt.spend_confirmed and st.env is None


async def test_mep_an_unread_template_is_never_guessed(tmp_path, monkeypatch):
    app = await _mep(tmp_path, monkeypatch, template=None, schema=FAKE_SCHEMA, confirm=False)
    st = await server._ensure_endpoint_up(app, "compute", account="hpcb", env={"requirements": REQS})
    assert "could not be read" in (st.notice or "") and st.env is None, st


async def test_mep_an_existing_env_is_activated_per_command_with_no_new_block(tmp_path, monkeypatch):
    app = await _mep(tmp_path, monkeypatch, template=FAKE_TEMPLATE, schema=FAKE_SCHEMA)
    rt = app.shapes["compute"]
    epoch = rt.block_epoch
    st = await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=m"})
    assert st.status == "up" and rt.spend_confirmed and rt.block_epoch == epoch, st
    assert (st.env.how, st.env.new_block, st.env.state) == ("activate", False, "unchecked")   # no login node here
    out = await server._run_shell(app, 'echo "T=$HPCB_TAG"', agent_call=True)
    assert out.phase == "complete" and "T=m" in out.stdout, out


async def test_anvils_pin_survives_a_requirements_pass_through(monkeypatch):
    from tests.test_resource_review_r2 import _anvil

    app, _res = await _anvil(monkeypatch, ANVIL_TEMPLATE)
    st = await server._ensure_endpoint_up(app, "compute", account="x-cis250223", confirm_spend=True)
    assert st.status == "up", st
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS}, confirm_spend=True)
    assert st.status == "up" and st.env.how == "facility", st
    sent = app.built[-1]
    assert sent["requirements"] == "six==1.17.0"
    assert "globus-compute-endpoint==" in sent["worker_init"] and "VIRTUAL_ENV" in sent["worker_init"], sent


# ---- 9. the handoff --------------------------------------------------------------------------------------------------

async def test_a_handoff_on_ssh_carries_the_env_it_was_validated_with(tmp_path):
    from tests.test_handoff_phase2b import _yes

    app = await _ssh(tmp_path)
    e = (await server._ensure_endpoint_up(app, "compute", env={"requirements": REQS})).env
    _ask, out = await _yes(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1},
                           env={"requirements": REQS})
    rec = out.record
    assert out.phase == "exported" and rec.env is not None, out
    assert (rec.env.key, rec.env.path, rec.env.how) == (e.key, e.path, "login") and rec.validation.env_ok is True
    assert "requirements" not in rec.user_endpoint_config      # a user env: the consumer activates it, never the worker


async def test_a_handoff_on_a_facility_that_builds_requirements_carries_them_in_the_config(tmp_path, monkeypatch):
    from tests.test_handoff_phase2b import _yes

    app = await _mep(tmp_path, monkeypatch, template=REQ_TEMPLATE, schema=REQ_SCHEMA, confirm=False)
    _ask, out = await _yes(app, dict(LOAM_UEC), env={"requirements": REQS})
    assert out.phase == "exported", out
    assert out.record.user_endpoint_config["requirements"] == "six==1.17.0" and out.record.env.how == "facility"


# ---- 10. the ledger and the tools -------------------------------------------------------------------------------------

def test_the_ledger_records_each_batchs_env(tmp_path):
    from hpc_bridge.ledger import Ledger
    from tests.test_batch_phase1 import _owner

    led = Ledger.open(tmp_path / "state")
    env = {"kind": "built", "how": "login", "key": "0123456789abcdef", "python": "3.12.4",
           "path": "/scratch/u/envs/0123456789abcdef-x86_64"}
    led.add_batch("batch-000000000001", command="echo {x}", rows=[1], task_walltime_s=60.0, capacity=1,
                  results_dir="/s/b", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=_owner(), env=env)
    assert json.loads(led.batch("batch-000000000001")["env"]) == env
    led.add_batch("batch-000000000002", command="echo {x}", rows=[1], task_walltime_s=60.0, capacity=1,
                  results_dir="/s/b", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=_owner())
    assert led.batch("batch-000000000002")["env"] is None


def test_the_four_tools_take_env():
    import inspect

    for tool in (server.ensure_endpoint_up, server.run_batch, server.submit_job, server.export_handoff):
        fn = getattr(tool, "fn", tool)
        assert "env" in inspect.signature(fn).parameters, fn.__name__
        assert "env" in (fn.__doc__ or ""), fn.__name__


def test_an_env_build_takes_its_time_from_its_own_cap_not_the_login_shapes():
    from hpc_bridge import envs

    # a login shape's own task ceiling is minutes (it has no walltime); a large install (a CUDA wheel) is longer
    assert envs.BUILD_S >= 1200 and envs.VERIFY_EVERY_S > 0 and envs.ENV_RC == 88
