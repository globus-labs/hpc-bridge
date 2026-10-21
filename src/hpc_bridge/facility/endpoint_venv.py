"""hpc-bridge's own endpoint venv on an SSH facility: the short `env_setup` line, the build, the teardown's search.

The `env_setup` that discovery proposes (`short_env_setup`) only ACTIVATES a venv keyed by endpoint version and Python,
`{venv}-endpoint-<gce>-py<X.Y>` (`keyed_venv`), by putting its `bin` on PATH. It never builds. On a compute worker
(worker_init replays it inside the batch job) it also checks that the venv's entry point runs there, and ends the job
with one line naming the venv, the architecture it was built on and this node's when it does not. A MISSING keyed venv
(no directory at all, not a broken one) falls back to the unkeyed `{venv}`: the one plugins up to 0.1.23 built, or one
the user prepared, so their endpoints and an older plugin reading a newer line keep working.

The build is Python (`ensure`): a few short commands over the facility's SSH channel, run before whoami/configure/start
of a bring-up and never by stop/delete/list.
1. Check the keyed venv through its entry point (`globus-compute-endpoint version`; `python -c "import
   globus_compute_endpoint.cli"` raises on every real install, since gce's cli reads `sys.argv[0]` at import), its
   python's major.minor, and that its texttable does what the endpoint's `list` does with it (texttable 1.7.2 broke
   that: `TEXTTABLE_PIN`). When the keyed venv is missing, an unkeyed `{venv}` that passes the same check is used as
   is; one that fails it is never changed (it may be the user's own) and the keyed venv is built instead.
2. Otherwise build a RELOCATABLE uv venv (`uv venv --relocatable`) in a directory of its own, `<keyed>.build.<token>`,
   with the package cache off and files copied (uv hardlinks venvs into ~/.cache/uv, so a corrupt cache entry would
   survive a rebuild), texttable pinned, installing uv first where the node has none; write the `.hpcb-arch` marker;
   verify it there.
3. Claim the keyed path with one atomic rename (`os.rename`, which refuses a non-empty target), after moving the broken
   venv the check saw aside to `.broken.<ts>` (only if the path still holds that same directory). Two bring-ups that
   build at once both finish; the one whose rename loses discards its own build and uses the winner's. There is no
   lock: nothing to wait for, take over or leave behind.
A build that fails or does not verify is kept as `.failed.<ts>`; a cancelled one is stopped by its token and leaves at
most its own `.build.*` directory. Nothing that worked is ever deleted. `find_working` serves teardown: the first
candidate whose `version` runs. `migrate_env_setup` is the upgrade-on-read: a line an earlier hpc-bridge generated,
matched exactly, becomes the short line; any other text is returned as given.

Nothing here imports the SSH transport: every remote step goes through a `Run(step, script, timeout)` callable (the
facility's ssh, or a fake in tests).
"""
from __future__ import annotations

import asyncio
import re
import shlex
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import NamedTuple

from .base import ConfigStepFailed

GCE = "globus-compute-endpoint"
# Where hpc-bridge's venv lives by default, as the login shell expands it: a teardown searches it even when the
# facility's env_setup is not hpc-bridge's line (emptied or hand-written after the endpoint was started).
HOME_VENV = "$HOME/hpc-bridge/gce-venv"
DEFAULT_REMOTE_VENV = "/home/{user}/hpc-bridge/gce-venv"
ARCH_MARKER = ".hpcb-arch"

# texttable 1.7.2 (2026-10-18) dropped the private `Texttable._width` that globus-compute-endpoint's `list` reads
# (`Endpoint.print_endpoint_table`: `_compute_cols_width()`, then `_width`; gce 4.18 and its main branch), so `list`
# raises AttributeError whenever an endpoint is configured, and hpc-bridge finds its endpoint's id with `list`. The
# endpoint leaves texttable unbounded (the SDK's `texttable>=1.7`): every build pins it, and the check runs exactly what
# `list` calls on a venv already built (`_PY_PROBE`), so it is rebuilt. The pin and the probe's texttable calls come
# out TOGETHER, once the endpoint stops reading `_width`: the probe tests texttable alone, never the endpoint, so a
# build without the pin would take 1.7.2 and fail its own verify (every bring-up, a `.failed.<ts>` each).
# `test_the_pin_and_the_probe_go_together` holds them to that.
TEXTTABLE_PIN = "texttable<1.7.2"
# what that `list` raises (its traceback's last line), and what to do; `facility.remote` adds it to the list failure
BROKEN_LIST = re.compile(r"'Texttable' object has no attribute '_width'")
BROKEN_LIST_HINT = (  # short: the notice keeps a message's first 500 characters
    "texttable 1.7.2 breaks globus-compute-endpoint's `list`. hpc-bridge repairs the venvs it builds (connect_facility "
    "again); in an environment it does not manage (a hand-written env_setup, a facility's own install) run "
    "`pip install 'texttable<1.7.2'` there.")

# (step, script, timeout) -> (rc, stdout, stderr). The production runner sends `bash -lc <script>` over the facility's
# SSH channel; rc 255 is ssh itself failing (no connection, a denial), which `ensure` reports as a transport failure.
Run = Callable[[str, str, float], Awaitable[tuple[int, str, str]]]

_CHECK_S = 150.0          # the check: two capped calls (60 s each) plus the login shell's start
_CHECK_CAP_S = 60         # each `version` / python call in the check (`timeout`, where the node has it)
_SHORT_S = 60.0           # claim, cleanup, marker
_BUILD_CAP_S = 840        # the build, capped on the login node (`timeout`)
_BUILD_S = 900.0          # the build's ssh timeout: past the remote cap, so the cap fires first
_FIND_S = 30.0            # teardown's search for a working CLI: the 30 s teardown budget
_FIND_CAP_S = 6           # each candidate's `version`
_FIND_DEADLINE_S = 18     # no new candidate is tried after this many seconds (bash's SECONDS)

_TOKEN_PREFIX = "hpcb-venv-build-"


class EndpointVenvError(RuntimeError):
    """hpc-bridge could not check or build its endpoint venv. The message says what to do; it is shown as it is."""


class EndpointVenvConfigError(EndpointVenvError, ConfigStepFailed):
    """…because of the configuration's own env_setup: the check did not run past the site's prefix, the build failed
    (not by running out of time), or the prefix leaves no uv, curl or python3 to build with. What a different env_setup
    can mend; a venv in the way, an old uv, a slow filesystem or a shadowing texttable stay EndpointVenvError."""


# ---------------------------------------------------------------------------------------------- versions and paths


def installed(dist: str) -> str | None:
    """The version of `dist` installed in THIS client's environment, or None (not installed here)."""
    try:
        from importlib.metadata import version

        return version(dist)
    except Exception:  # noqa: BLE001 - not installed here (hermetic tests without it)
        return None


def client_gce() -> str | None:
    """The endpoint version hpc-bridge installs: this client's SDK's (the two are released in lockstep)."""
    return installed("globus-compute-sdk")


def client_sdk() -> str | None:
    """The globus-sdk co-pinned in the build: the endpoint leaves it unbounded, and a fresh resolve took one its CLI
    could not import (globus1, 2026-10-10)."""
    return installed("globus-sdk")


def client_python() -> str:
    """This client's major.minor: workers deserialise task functions dill'd by this interpreter."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def keyed_venv(venv: str, *, python: str, gce: str | None) -> str:
    """hpc-bridge's venv for ONE endpoint version on ONE Python: `<venv>-endpoint-<gce>-py<X.Y>`. A pin or Python move
    builds a new venv beside the old one instead of replacing one a running manager may use. The `-endpoint-` stem keeps
    it apart from the facility-MEP seeds' worker venvs (Delta's and Anvil's `…/gce-venv-<gce>-py<X.Y>`)."""
    return f"{venv}-endpoint-{gce}-py{python}" if gce else f"{venv}-endpoint-py{python}"


def shell_path(path: str) -> str:
    """`~/x` → `$HOME/x`: the line puts paths in double quotes, where a `~` does not expand."""
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return path


# A base the line and the build can put inside double quotes: absolute or `$HOME/`-relative, and nothing the shell
# would expand or split (no quotes, `$`, backquote, backslash, whitespace). `{venv}` is the templated form.
_SAFE_BASE = re.compile(r"^(?:\{venv\}|(?:\$HOME/|/)[^\s\"'`$\\]*)$")
_SAFE_VERSION = re.compile(r"^[\w.+!-]+$")


def safe_base(path: str) -> bool:
    return bool(_SAFE_BASE.match(path))


# ------------------------------------------------------------------------------------------------------ the line

# `@B@` = the unkeyed `{venv}`, `@KEY@` = `<gce>-py<X.Y>`. `__hb_` names stay out of the user's way (the session shim
# uses the same prefix). Every `a || b || c` list here ends in a command that succeeds or in the worker's `exit 1`, so
# the line also runs under `set -e`. The fallback tests the keyed DIRECTORY: a venv is only ever renamed into place
# whole, so a keyed directory that exists is the one to use, broken or not. Activation is what `bin/activate` does
# (PYTHONHOME unset — first, so the worker's check runs clean — then VIRTUAL_ENV and PATH set), not `bin/activate`
# itself: a relocatable venv's `activate` cannot find itself under sh/dash. Nothing here builds.
_LINE_HEAD = '__hb_b="@B@"; __hb_v="$__hb_b-endpoint-@KEY@"; '
_LINE_TAIL = (
    'unset PYTHONHOME; [ -e "$__hb_v" ] || [ ! -e "$__hb_b" ] || __hb_v=$__hb_b; '
    '[ -z "${SLURM_JOB_ID:-}${PBS_JOBID:-}" ] || "$__hb_v/bin/globus-compute-endpoint" version >/dev/null || '
    '{ echo "hpc-bridge: endpoint venv $__hb_v (built on $(cat "$__hb_v/.hpcb-arch" 2>/dev/null || echo unknown)) '
    'does not run on $(hostname) ($(uname -m))" >&2; exit 1; }; export VIRTUAL_ENV="$__hb_v" PATH="$__hb_v/bin:$PATH"'
)
_LINE_RE = re.compile(
    r"(?P<prefix>.*?)__hb_b=\"(?P<base>[^\"]*)\"; "
    r"__hb_v=\"\$__hb_b-endpoint-(?:(?P<gce>[^\"]+)-)?py(?P<py>\d+\.\d+)\"; "
    + re.escape(_LINE_TAIL) + r"\Z",
    re.S,
)
# The one line the worker's check prints (also what the pilot probe quotes from a block's stderr).
WORKER_SAID = re.compile(r"^hpc-bridge: endpoint venv (?P<venv>\S+) \(built on (?P<built>[^)]*)\) does not run on "
                         r"(?P<host>\S+) \((?P<arch>[^)]*)\)$")


def short_env_setup(base: str = "{venv}", *, python: str | None = None, gce: str | None = None,
                    prefix: str = "", pinned: bool = True) -> str:
    """The env_setup for hpc-bridge's own venv beside `base` (`{venv}` until templated), at this client's Python and
    endpoint version unless given. `prefix` (a site `module load …; `) runs first."""
    py = python or client_python()
    pin = (gce or client_gce()) if pinned else None
    key = f"{pin}-py{py}" if pin else f"py{py}"
    return prefix + _LINE_HEAD.replace("@B@", shell_path(base)).replace("@KEY@", key) + _LINE_TAIL


@dataclass(frozen=True)
class ManagedVenv:
    """An env_setup that ends in hpc-bridge's line: what runs before it, and the venvs it activates."""

    prefix: str          # the site's own setup before the line ("" or `module load …; `)
    base: str            # the unkeyed `{venv}`, as written inside the line's double quotes
    gce: str | None      # the endpoint version in the key (None: unpinned)
    python: str          # the Python major.minor in the key

    @property
    def key(self) -> str:
        return keyed_venv(self.base, python=self.python, gce=self.gce)

    @property
    def parent(self) -> str:
        return self.key.rsplit("/", 1)[0] or "/"


def is_line(env_setup: str | None) -> bool:
    """Does `env_setup` end in hpc-bridge's line (resolved or not)?"""
    return _LINE_RE.fullmatch(env_setup or "") is not None


def managed_venv(env_setup: str | None) -> ManagedVenv | None:
    """The managed venv a (resolved) env_setup activates, or None when it is not hpc-bridge's line — a hand-written
    env_setup is used exactly as given and never triggers a build."""
    m = _LINE_RE.fullmatch(env_setup or "")
    if m is None:
        return None
    base, gce = m.group("base"), m.group("gce")
    if not safe_base(base) or base == "{venv}" or (gce is not None and not _SAFE_VERSION.match(gce)):
        return None  # an unresolved template or a path the shell would rewrite: not something to build into
    return ManagedVenv(prefix=m.group("prefix"), base=base, gce=gce, python=m.group("py"))


# ------------------------------------------------------------------------------------------- upgrade-on-read

# The env_setup texts hpc-bridge generated up to 0.1.23: discovery's (guarded by `[ -d ] ||` / `command -v ||`) and the
# curated Expanse seed's (the bootstrap form, pin in double quotes). Matched EXACTLY, with `{venv}` templated or
# resolved to one path; a hand-written line never matches and is never touched.
_OLD_VENV = r"(?P<v>\{venv\}|(?:/|~/|\$HOME/)[^\s;'\"`$\\]+)"  # a path the short line can quote as it is
_OLD_PIN = r"(?:'globus-compute-endpoint==[\w.+!-]+'|\"globus-compute-endpoint==[\w.+!-]+\"|globus-compute-endpoint)"
_OLD_UV = (r"\[ -d (?P=v) \] \|\| uv venv (?P=v)(?: --python \d+\.\d+)?; \. (?P=v)/bin/activate; "
           r"command -v globus-compute-endpoint >/dev/null 2>&1 \|\| uv pip install -q " + _OLD_PIN)
_OLD_PIP = (r"\[ -d (?P=v) \] \|\| python3 -m venv (?P=v); \. (?P=v)/bin/activate; "
            r"command -v globus-compute-endpoint >/dev/null 2>&1 \|\| pip install -q " + _OLD_PIN)
_OLD_INIT = r"(?:type module >/dev/null 2>&1 \|\| \. \S+; )?module load \S+; "
_OLD_BOOTSTRAP = (r"export PATH=\"\$HOME/\.local/bin:\$PATH\"; command -v uv >/dev/null 2>&1 \|\| "
                  r"curl -LsSf https://astral\.sh/uv/install\.sh \| sh; ")


def _first_v(rx: str) -> str:
    return rx.replace("(?P=v)", _OLD_VENV, 1)


# (kind, pattern): `keep` = the module prefix is kept (the build and the venv's python need it); the rest is replaced
_OLD_TEXTS = (
    ("drop", re.compile(r"(?P<pre>)" + _first_v(_OLD_UV))),
    ("drop", re.compile(r"(?P<pre>" + _OLD_BOOTSTRAP + r")" + _first_v(_OLD_UV))),
    ("keep", re.compile(r"(?P<pre>" + _OLD_INIT + r")" + _first_v(_OLD_UV))),
    ("keep", re.compile(r"(?P<pre>" + _OLD_INIT + r")" + _first_v(_OLD_PIP))),
)


def migrate_env_setup(env_setup: str, *, python: str | None = None, gce: str | None = None) -> str:
    """`env_setup` as this plugin writes it: a line an earlier hpc-bridge generated (EXACTLY) becomes the short line,
    and hpc-bridge's own line keyed for another endpoint version or Python is re-keyed to this client's (a version move
    builds a new venv beside the old one). Anything else is returned as given."""
    text = env_setup or ""
    py, pin = python or client_python(), gce or client_gce()
    m = _LINE_RE.fullmatch(text)
    if m is not None:
        if m.group("py") == py and m.group("gce") == pin:
            return text
        return short_env_setup(m.group("base"), python=py, gce=pin, prefix=m.group("prefix"))
    for kind, rx in _OLD_TEXTS:
        old = rx.fullmatch(text)
        if old is not None:
            return short_env_setup(old.group("v"), python=py, gce=pin,
                                   prefix=old.group("pre") if kind == "keep" else "")
    return env_setup


# ------------------------------------------------------------------------------------------------- the check

@dataclass
class VenvState:
    """What the check found at one venv path, and the build tools on the login node."""

    exists: bool = False
    pyvenv: bool = False
    empty: bool = False
    entry: bool = False
    inode: str = ""          # the directory's inode: a move aside only takes the one this check saw
    version_rc: int | None = None
    version: str = ""        # the entry point's version line (else its last output line)
    py_rc: int | None = None
    py: str = ""             # the venv python's last output line
    texttable: str = ""      # ok | absent | broken: what `list` calls on texttable (`_PY_PROBE`; "" = not reported)
    texttable_version: str = ""
    texttable_error: str = ""
    texttable_file: str = ""       # the texttable it imported (resolved)
    texttable_outside: bool = False  # …from outside the venv's own prefix: a rebuild cannot change it
    arch: str | None = None  # the .hpcb-arch marker
    machine: str = ""        # this login node's `uname -m`
    uv: str = ""
    uv_version: str = ""
    uv_relocatable: bool = False  # this uv's `venv` knows --relocatable (uv 0.2.31+)
    py3: str = ""
    curl: str = ""
    raw: dict[str, str] = field(default_factory=dict)


def _q(path: str) -> str:
    """A path inside double quotes (safe_base vetted it: `$HOME` is the only expansion left)."""
    return f'"{path}"'


def _cap(seconds: int) -> str:
    return (f"if command -v timeout >/dev/null 2>&1; then __hb_cap() {{ timeout {seconds} \"$@\"; }}; "
            "else __hb_cap() { \"$@\"; }; fi; ")


# The check's one python call: whether the venv's texttable does what globus-compute-endpoint's `list` does with it
# (`Endpoint.print_endpoint_table`, gce 4.18: the same calls, through `draw()`, so a texttable that brings `_width`
# back needs no change here; it tests texttable alone, never the endpoint: see TEXTTABLE_PIN), where the texttable it
# imported lives (`TEXTTABLE_OUTSIDE`: not under the venv's own prefix, e.g. a site module's PYTHONPATH shadowing it, a
# break no rebuild can mend), then the python's major.minor as the LAST line, as before. A venv with no texttable at
# all is not a problem: its endpoint cannot be reading it. Never raises, and no f-strings, so an old python still
# reports its version. The login shell's cwd is dropped from the path, as an entry point never has it.
_PY_PROBE = (
    "import os, sys\n"
    "sys.path[:1] = [p for p in sys.path[:1] if p]\n"
    "t, e, v, f = 'ok', '', '', ''\n"
    "try:\n"
    "    import texttable\n"
    "except ImportError as x:\n"
    "    t = 'absent' if getattr(x, 'name', None) == 'texttable' else 'broken'\n"
    "    e = '%s: %s' % (type(x).__name__, x)\n"
    "else:\n"
    "    v = str(getattr(texttable, '__version__', ''))\n"
    "    f = os.path.realpath(getattr(texttable, '__file__', None) or '')\n"
    "    try:\n"
    "        tb = texttable.Texttable()\n"
    "        tb.header(['Endpoint ID', 'Status', 'Endpoint Name'])\n"
    "        tb.add_row(['id', 'Running', 'name'])\n"
    "        tb.set_max_width(68)\n"
    "        tb._compute_cols_width()\n"
    "        tb._width[2] = max(10, tb._width[2])\n"
    "        tb.draw()\n"
    "    except Exception as x:\n"
    "        t, e = 'broken', '%s: %s' % (type(x).__name__, x)\n"
    "print('TEXTTABLE=' + t)\n"
    "print('TEXTTABLE_VERSION=' + v)\n"
    "print('TEXTTABLE_ERROR=' + ' '.join(e.split())[:200])\n"
    "print('TEXTTABLE_FILE=' + f)\n"
    "if f and not f.startswith(os.path.join(os.path.realpath(sys.prefix), '')):\n"
    "    print('TEXTTABLE_OUTSIDE=1')\n"
    "print(sys.version_info[:2])\n"
)
# sent as `exec(<its repr>)`: one line, so the check stays a single line, as it always was
_PY_CHECK = shlex.quote(f"exec({_PY_PROBE!r})")


def check_script(mv: ManagedVenv, path: str | None = None) -> str:
    """Read-only (bytecode writes off; `version` itself may create ~/.globus_compute): the venv's entry point
    `version`, its python's major.minor and whether its texttable serves `list` (parsed in Python), its inode and arch
    marker, and the tools a build could use. `path` defaults to the keyed venv."""
    k = _q(path or mv.key)
    return (
        f"export PYTHONDONTWRITEBYTECODE=1; {mv.prefix}export PATH=\"$HOME/.local/bin:$PATH\"; k={k}; "
        + _cap(_CHECK_CAP_S)
        + "echo HPCB_VENV_BEGIN; "
        'if [ -e "$k" ]; then echo EXISTS=1; [ -f "$k/pyvenv.cfg" ] && echo PYVENV=1; '
        '[ -z "$(ls -A "$k" 2>/dev/null)" ] && echo EMPTY=1; '
        'echo "INODE=$(stat -c %i "$k" 2>/dev/null || stat -f %i "$k" 2>/dev/null)"; fi; '
        f'if [ -e "$k/bin/{GCE}" ]; then echo ENTRY=1; o=$(__hb_cap "$k/bin/{GCE}" version 2>&1); '
        "echo \"VERSION_RC=$?\"; { printf '%s\\n' \"$o\" | grep 'endpoint version:' || printf '%s\\n' \"$o\"; } "
        "| tail -n 1 | sed 's/^/VERSION=/'; fi; "
        f'if [ -e "$k/bin/python" ]; then o=$(__hb_cap "$k/bin/python" -c {_PY_CHECK} '
        "2>&1); echo \"PY_RC=$?\"; printf '%s\\n' \"$o\" | tail -n 1 | sed 's/^/PY=/'; "
        "printf '%s\\n' \"$o\" | grep '^TEXTTABLE'; fi; "
        f'[ -f "$k/{ARCH_MARKER}" ] && echo "ARCH=$(head -n 1 "$k/{ARCH_MARKER}")"; '
        'echo "MACHINE=$(uname -m)"; echo "UV=$(command -v uv 2>/dev/null)"; '
        'if command -v uv >/dev/null 2>&1; then echo "UV_VERSION=$(uv --version 2>/dev/null)"; '
        'uv venv --help 2>/dev/null | grep -q -- --relocatable && echo UV_RELOC=1; fi; '
        "echo \"PY3=$(python3 -c 'import sys;print(*sys.version_info[:2],sep=chr(46))' 2>/dev/null)\"; "
        'echo "CURL=$(command -v curl 2>/dev/null)"; echo HPCB_VENV_END'
    )


def parse_check(out: str) -> VenvState:
    raw: dict[str, str] = {}
    inside = False
    for line in (out or "").splitlines():
        line = line.rstrip("\r")
        if line == "HPCB_VENV_BEGIN":
            inside = True
            continue
        if line == "HPCB_VENV_END":
            break
        if inside and "=" in line:
            k, _, v = line.partition("=")
            raw[k] = v
    s = VenvState(raw=raw)
    s.exists, s.pyvenv, s.empty, s.entry, s.uv_relocatable = (
        raw.get(x) == "1" for x in ("EXISTS", "PYVENV", "EMPTY", "ENTRY", "UV_RELOC"))
    s.uv_version = raw.get("UV_VERSION", "").strip()
    s.version_rc = int(raw["VERSION_RC"]) if raw.get("VERSION_RC", "").lstrip("-").isdigit() else None
    s.py_rc = int(raw["PY_RC"]) if raw.get("PY_RC", "").lstrip("-").isdigit() else None
    s.version, s.py = raw.get("VERSION", "").strip(), raw.get("PY", "").strip()
    s.texttable, s.texttable_version, s.texttable_error, s.texttable_file = (
        raw.get(x, "").strip() for x in ("TEXTTABLE", "TEXTTABLE_VERSION", "TEXTTABLE_ERROR", "TEXTTABLE_FILE"))
    s.texttable_outside = raw.get("TEXTTABLE_OUTSIDE") == "1"
    s.arch = raw["ARCH"].strip() if "ARCH" in raw else None
    s.inode, s.machine, s.uv, s.py3, s.curl = (raw.get(x, "").strip()
                                               for x in ("INODE", "MACHINE", "UV", "PY3", "CURL"))
    return s


_VERSION_RE = re.compile(r"endpoint version:\s*(\S+)\s*$", re.M)
_PY_TUPLE = re.compile(r"^\((\d+),\s*(\d+)\)$")
_TIMED_OUT = 124  # GNU timeout's exit status


def reported_version(text: str) -> str | None:
    """The version `globus-compute-endpoint version` reported, from any of its lines (a warning may follow it)."""
    found = _VERSION_RE.findall(text or "")
    return found[-1] if found else None


def reported_python(line: str) -> str | None:
    m = _PY_TUPLE.match((line or "").strip())
    return f"{m.group(1)}.{m.group(2)}" if m else None


def problem(s: VenvState, *, gce: str | None, python: str) -> str | None:
    """Why the venv does not serve this client (None: it does)."""
    if not s.exists:
        return "it does not exist yet"
    if not s.entry:
        return f"it has no bin/{GCE}"
    if s.version_rc != 0:
        if s.version_rc == _TIMED_OUT:
            return "timed out"
        return f"its {GCE} fails: {s.version or 'no output'}"
    got = reported_version(s.version)
    if gce and got != gce:
        return f"it reports {got or s.version or 'no version'}, not {gce}"
    if s.py_rc != 0:
        return "timed out" if s.py_rc == _TIMED_OUT else f"its python fails: {s.py or 'no output'}"
    have = reported_python(s.py)
    if have != python:
        return f"its python is {have or s.py or 'unknown'}, not {python}"
    if s.texttable == "broken" and not s.texttable_outside:  # `version` runs, `list` cannot draw (TEXTTABLE_PIN)
        tt = f"texttable {s.texttable_version}" if s.texttable_version else "texttable"
        return f"its {tt} breaks `{GCE} list` ({s.texttable_error or 'no error text'})"
    return None  # a texttable from OUTSIDE the venv is not the venv's to mend: `shadowed` says so


def shadowed(s: VenvState, path: str, host: str) -> EndpointVenvError | None:
    """The error for a venv whose python imports a texttable that breaks `list` from OUTSIDE the venv (a site module's
    PYTHONPATH, say), or None. Its own texttable is not what runs, so a rebuild cannot help: nothing is built, and the
    message names the file. `list` runs with the same environment, so the bring-up could not get past it either."""
    if s.texttable != "broken" or not s.texttable_outside:
        return None
    tt = f"texttable {s.texttable_version}" if s.texttable_version else "a texttable"
    return EndpointVenvError(
        f"{tt} from {s.texttable_file or 'an unknown file'}, outside hpc-bridge's endpoint venv {path} on {host}, "
        f"breaks `{GCE} list` ({s.texttable_error or 'no error text'}): it is shadowed from outside the venv "
        "(PYTHONPATH, from the site setup in env_setup or a login profile?), so a rebuild cannot help. Take that "
        "directory off the path, or make its texttable older than 1.7.2 (`pip install 'texttable<1.7.2'` there), then "
        "connect_facility again. Nothing was started or billed.")


# -------------------------------------------------------------------------------------------------- the build

@dataclass
class EnsureResult:
    path: str                       # the venv that serves this bring-up (the keyed one, or a prepared unkeyed one)
    built: bool = False
    moved_aside: str | None = None  # where a broken venv went (`.broken.<ts>`)
    reason: str = ""                # why it was (re)built


def _stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


# A venv build that could not reach the network — uv's or pip's fetch (DNS, route, a refused or reset connection, a
# timeout: uv's "due to network timeout"), curl's install of uv — or whose write the login node's filesystem refused for
# want of room or a writable mount (read-only, full, over quota) failed for the node, not for the configuration:
# EndpointVenvError (kept), never EndpointVenvConfigError. A "Permission denied" on the venv's own path is NOT here: the
# path is the configuration's choice (env_setup's base, or HPC_BRIDGE_REMOTE_VENV) — a made-up `/sw/hpc-bridge` in a
# details= is its own failure (review round 4). Read from the end of the build's error output (`_build_tail`).
_BUILD_NOT_THE_CONFIG = re.compile(
    r"temporary failure in name resolution|name or service not known|nodename nor servname|could not resolve host"
    r"|failed to lookup address|dns error|network is unreachable|no route to host|connection refused"
    r"|connection reset|curl: \((?:5|6|7|28|35|52|56)\)|network timeout|\btimed out\b"
    r"|read-only file system|no space left on device|disk quota exceeded",
    re.IGNORECASE,
)


def _tail(*texts: str, lines: int = 6) -> str:
    """The last `lines` non-empty lines of the outputs, joined: a build's error chain."""
    got = [ln.strip() for t in texts for ln in re.sub(r"\x1b\[[0-9;]*m", "", t or "").splitlines() if ln.strip()]
    return "\n".join(got[-lines:])


def _build_tail(err: str, out: str) -> str:
    """The build's error chain: the end of stderr — where uv and pip print it — else of stdout, as `_last_line` reads
    them. Never the two joined: a login shell's banner on stdout would push uv's "Caused by:" lines out of the
    window."""
    return _tail(err) if (err or "").strip() else _tail(out)


# exit statuses of a build that did not run to its end: GNU timeout's 124, and 137 (SIGKILL: `timeout -k`, the kernel)
_BUILD_CUT = (124, 137)


def _last_line(*texts: str) -> str:
    for t in texts:
        lines = [ln.strip() for ln in re.sub(r"\x1b\[[0-9;]*m", "", t or "").splitlines() if ln.strip()]
        if lines:
            return lines[-1][:300]
    return "no output"


def _transport(step: str, host: str, rc: int, out: str, err: str) -> RuntimeError:
    # an ssh-level failure: the caller's explainer turns the raw text into NO SSH ACCESS / CANNOT REACH / a handoff
    return RuntimeError(f"endpoint venv {step} on {host} failed: {(err or out).strip()[:600] or f'rc={rc}'}")


def _method(s: VenvState, python: str, skipped: str = "") -> str:
    """How to get uv: the login node's (after the site prefix), or installed first — from astral.sh with curl, else
    with the node's python3 and pip. A node with none of them cannot build. `skipped`: why a prepared unkeyed venv was
    not used, said in either refusal (it is why a build was needed at all)."""
    why = f" {skipped}." if skipped else ""
    if s.uv:
        if not s.uv_relocatable:
            raise EndpointVenvError(
                f"this node's uv ({s.uv_version or 'unknown version'}, {s.uv}) is too old to build hpc-bridge's venv "
                "(it has no `uv venv --relocatable`, uv 0.2.31+): upgrade uv (`uv self update`), or remove it from "
                f"PATH so hpc-bridge installs its own, then connect_facility again.{why} Nothing was started or "
                "billed.")
        return "uv"
    if s.curl or s.py3:
        return "bootstrap"
    raise EndpointVenvConfigError(  # env_setup's site prefix brings none of them: a module load there would
        f"no uv, no curl and no python3 on the login node, so hpc-bridge cannot build its endpoint venv (Python "
        f"{python}) there. Make uv or python3 available (a site `module load …` before hpc-bridge's line in "
        "env_setup), or prepare a venv with globus-compute-endpoint at HPC_BRIDGE_REMOTE_VENV, then connect_facility "
        f"again.{why} Nothing was started or billed.", said="")  # the check's finding, not words a node printed


# uv for a node that has none: astral.sh's installer (told not to edit the user's shell profiles), else pip in a
# throwaway venv (a site that proxies PyPI but not GitHub). Either way it lands in ~/.local/bin, which the build's PATH
# puts first; the build then runs it by its path (`$u`).
_BOOTSTRAP_UV = (
    "command -v uv >/dev/null 2>&1 || { curl -LsSf https://astral.sh/uv/install.sh "
    '| env UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null; } || true; '
    'command -v uv >/dev/null 2>&1 || { t=$(mktemp -d) && python3 -m venv "$t" && "$t/bin/python" -m pip install -q uv '
    '&& mkdir -p "$HOME/.local/bin" && cp "$t/bin/uv" "$HOME/.local/bin/uv"; rm -rf "$t"; }; '
    'command -v uv >/dev/null 2>&1 || { echo "hpc-bridge: could not install uv (astral.sh, then pip)" >&2; exit 1; }; '
)


# Our own builds a dropped session (an ssh drop, a killed server) or an unfinished verify left behind: removed once
# they are hours old (a live build is minutes), before a new one starts. Only `<keyed>.build.*`, nothing else.
_STALE_BUILD_MIN = 240
_SWEEP = ('find "${b%/*}" -maxdepth 1 -name "@KEY@.build.*" -mmin +' + str(_STALE_BUILD_MIN)
          + ' -prune -print -exec rm -rf {} + 2>/dev/null | sed "s/^/HPCB_SWEPT=/"; ')


def build_dir(mv: ManagedVenv, token: str) -> str:
    return f"{mv.key}.build.{token}"


def build_script(mv: ManagedVenv, *, token: str, method: str, gce: str | None, python: str, sdk: str | None) -> str:
    """Build a relocatable venv in its own directory (`build_dir`), cache off, files copied, and mark its arch. The one
    install (uv the node's, or the one `_BOOTSTRAP_UV` installed) also pins texttable (`TEXTTABLE_PIN`), quoted: `<`
    is a redirect to the shell."""
    reqs = ((f"'{GCE}=={gce}'" + (f" 'globus-sdk=={sdk}'" if sdk else "")) if gce else GCE) + f" '{TEXTTABLE_PIN}'"
    b = _q(build_dir(mv, token))
    return (f"{mv.prefix}export PATH=\"$HOME/.local/bin:$PATH\"; b={b}; mkdir -p \"${{b%/*}}\" || exit 1; "
            + _SWEEP.replace("@KEY@", mv.key.rsplit("/", 1)[-1])
            + (_BOOTSTRAP_UV if method == "bootstrap" else "")
            + 'u=$(command -v uv); '
            f'"$u" venv -q --relocatable --no-cache --python {python} "$b" && '
            f'"$u" pip install -q --no-cache --link-mode=copy --python "$b/bin/python" {reqs} && '
            f'uname -m > "$b/{ARCH_MARKER}"')


def _wrap_build(inner: str, token: str) -> str:
    """Run the build under `timeout` (where the node has it), named by `token` so a cancelled bring-up can stop it."""
    name = _TOKEN_PREFIX + token
    q = shlex.quote(inner)
    return (f"if command -v timeout >/dev/null 2>&1; then exec timeout -k 10 {_BUILD_CAP_S} bash -lc {q} {name}; fi; "
            f"exec bash -lc {q} {name}")


# Claim the keyed path: move the broken venv the check saw aside — only if the path still holds THAT directory, and
# checked again after the move (another bring-up may have renamed its build in between: then that one goes back, and
# ours has lost) — then rename the verified build onto the keyed path. os.rename refuses a non-empty target
# (ENOTEMPTY/EEXIST), so of two builds racing, one wins and the other is told it lost. Any other error (EACCES, ESTALE…)
# is reported as itself, never as a lost race. Run by the build's own python: it was just verified.
_CLAIM_PY = (
    "import errno,os,sys\n"
    "b,k,ino,aside=sys.argv[1:5]\n"
    "lost=(errno.ENOTEMPTY,errno.EEXIST)\n"
    "try:\n"
    " st=os.lstat(k)\n"
    "except OSError:\n"
    " st=None\n"
    "if st is not None and ino and str(st.st_ino)==ino:\n"
    " while os.path.lexists(aside):\n"
    "  aside+='x'\n"
    " try:\n"
    "  os.rename(k,aside)\n"
    " except OSError as e:\n"
    "  print('HPCB_CLAIM='+('lost ' if e.errno in lost+(errno.ENOENT,) else 'error ')+(e.strerror or '?'))\n"
    "  sys.exit(0)\n"
    " if str(os.lstat(aside).st_ino)!=ino:\n"
    "  try:\n"
    "   os.rename(aside,k)\n"
    "  except OSError:\n"
    "   print('HPCB_STRANDED='+aside)\n"
    "  print('HPCB_CLAIM=lost another bring-up claimed it first')\n"
    "  sys.exit(0)\n"
    " print('HPCB_MOVED='+aside)\n"
    "try:\n"
    " os.rename(b,k)\n"
    " print('HPCB_CLAIM=ok')\n"
    "except OSError as e:\n"
    " print('HPCB_CLAIM='+('lost ' if e.errno in lost else 'error ')+(e.strerror or '?'))\n"
)


def claim_script(mv: ManagedVenv, token: str, inode: str) -> str:
    b, k = _q(build_dir(mv, token)), _q(mv.key)
    aside = _q(f"{mv.key}.broken.{_stamp()}.{token[:8]}")  # unique per bring-up
    return f'b={b}; k={k}; "$b/bin/python" -c {shlex.quote(_CLAIM_PY)} "$b" "$k" {shlex.quote(inode)} {aside}'


def cleanup_script(mv: ManagedVenv, token: str, *, kill: bool, mode: str) -> str:
    """After a build that did not claim the keyed path: stop it if it may still run (`kill`: a cancelled bring-up's ssh
    is gone, the remote build is not); then `mode` — "failed": move it to `.failed.<ts>` (the evidence); "discard":
    remove our own duplicate or a cancelled partial build; "keep": leave a verified build where it is."""
    b = _q(build_dir(mv, token))
    s = f"b={b}; "
    if kill:
        # `buil[d]`: the pattern does not match this script's own command line
        t = _TOKEN_PREFIX.replace("build", "buil[d]") + token
        s += (f"t='{t}'; for p in $(pgrep -f \"$t\" 2>/dev/null); do "
              "g=$(ps -o pgid= -p \"$p\" 2>/dev/null | tr -d ' '); "
              '[ -n "$g" ] && kill -TERM -- "-$g" 2>/dev/null; done; i=0; '
              'while pgrep -f "$t" >/dev/null 2>&1 && [ "$i" -lt 10 ]; do sleep 1; i=$((i+1)); done; ')
    if mode == "discard":
        s += 'rm -rf "$b"; '
    elif mode == "failed":
        f = _q(f"{mv.key}.failed.{_stamp()}")
        s += (f'd={f}; if [ -e "$b" ]; then while [ -e "$d" ]; do d="${{d}}x"; done; '
              'mv "$b" "$d" && echo "HPCB_FAILED=$d"; fi; ')
    return s + "true"


def _marker_script(path: str) -> str:
    return f'uname -m > "{path}/{ARCH_MARKER}"'


def _field(out: str, key: str) -> str | None:
    for line in (out or "").splitlines():
        if line.startswith(key + "="):
            return line[len(key) + 1:].strip()
    return None


def _timed_out(path: str, host: str, what: str = "checking") -> EndpointVenvError:
    return EndpointVenvError(
        f"{what} hpc-bridge's endpoint venv {path} on {host} timed out (a slow filesystem?). It was left as it is; "
        "connect_facility again in a minute. Nothing was started or billed.")


async def ensure(run: Run, mv: ManagedVenv, *, host: str, gce: str | None, python: str,
                 sdk: str | None) -> EnsureResult:
    """Make the keyed venv serve this client — check, and build only when the check fails. See the module docstring.
    Raises EndpointVenvError (what to do, shown as it is) or RuntimeError (ssh itself failed)."""
    def serves(state: VenvState, path: str) -> None:
        """A venv that passed: unless a texttable from outside it breaks `list` (`shadowed`), which no build mends."""
        if (err := shadowed(state, path, host)) is not None:
            raise err

    s = await _check(run, mv, host, "check")
    why = problem(s, gce=gce, python=python)
    if why is None:
        serves(s, mv.key)
        await _ensure_marker(run, mv.key, s, host)
        return EnsureResult(path=mv.key)
    if why == "timed out":
        raise _timed_out(mv.key, host)
    skipped = ""  # why an unkeyed venv that is there was not used (it is never changed: it may be the user's own)
    if not s.exists:
        # no keyed venv at all: the line falls back to the unkeyed {venv}, so one there that passes the same check (one
        # the user prepared, or an older plugin's at this pin) is used as it is — a login node without internet works
        u = await _check(run, mv, host, "check unkeyed", mv.base)
        if u.exists:
            why_u = problem(u, gce=gce, python=python)
            if why_u is None:
                serves(u, mv.base)
                await _ensure_marker(run, mv.base, u, host)
                return EnsureResult(path=mv.base)
            skipped = f"{mv.base} was not used: {why_u}"
            why = f"{why}; {skipped}"
    if s.exists and not s.pyvenv and not s.empty:
        raise EndpointVenvError(
            f"{mv.key} on {host} exists and is not a venv: hpc-bridge will not move or replace it. Move it away "
            "(or set HPC_BRIDGE_REMOTE_VENV to another path), then connect_facility again. Nothing was started or "
            "billed.")
    method = _method(s, python, skipped)
    token = uuid.uuid4().hex[:16]
    b = build_dir(mv, token)
    what = f"{GCE} {gce or '(latest)'} on Python {python}"
    running, done, mode = True, False, "failed"  # mode: what the cleanup does with a build that did not claim the path

    async def failed(message: str, *, config: bool = False, said: str = "", tail: str = "") -> EndpointVenvError:
        """Move the failed build aside now, so the message says where it really is (if anywhere). `config`: the build
        failed through the configuration's env_setup (EndpointVenvConfigError, with `said`, the build's own last line,
        and `tail`, its error chain)."""
        nonlocal done
        done = True
        kept = None
        try:
            _rc, cout, _cerr = await run("cleanup", cleanup_script(mv, token, kill=False, mode="failed"), _SHORT_S)
            kept = _field(cout, "HPCB_FAILED")
        except Exception:  # noqa: BLE001 - best-effort: it then stays as our .build.* directory
            kept = None
        where = f"The failed build is kept as {kept}." if kept else "Nothing of it was kept."
        text = f"{message} {where} {skipped + '. ' if skipped else ''}Nothing was started or billed."
        return EndpointVenvConfigError(text, said=said, tail=tail) if config else EndpointVenvError(text)

    try:
        rc, out, err = await run("build", _wrap_build(
            build_script(mv, token=token, method=method, gce=gce, python=python, sdk=sdk), token), _BUILD_S)
        if rc == 255 or rc < 0:   # ssh itself failed, or the local ssh was killed: as `base.step_error` reads them
            raise _transport("build", host, rc, out, err)
        running = False  # the build process has ended: nothing left to stop
        for swept in (ln[len("HPCB_SWEPT="):] for ln in out.splitlines() if ln.startswith("HPCB_SWEPT=")):
            print(f"hpc-bridge: removed a stale build of the endpoint venv on {host}: {swept}", file=sys.stderr)
        if rc != 0:
            when = f"it did not finish within {_BUILD_CAP_S} s" if rc in _BUILD_CUT else _last_line(err, out)
            # the configuration's, unless it was cut short, could not reach the network (PyPI, astral.sh: the login
            # node's DNS or route) or found no room to write — the same env_setup may build next time
            tail = _build_tail(err, out)
            config = rc not in _BUILD_CUT and not _BUILD_NOT_THE_CONFIG.search(tail)
            raise await failed(f"could not build hpc-bridge's endpoint venv for {mv.key} on {host} ({what}): {when}. "
                               "Fix that, then connect_facility again.", config=config, said=when, tail=tail)
        v = await _check(run, mv, host, "verify", b)
        bad = problem(v, gce=gce, python=python)
        if bad == "timed out":
            mode = "keep"  # not a failed build: left where it is
            raise _timed_out(b, host, "verifying the newly built")
        if bad is not None:
            # a broken texttable INSIDE a build that asked for TEXTTABLE_PIN: the node's uv was told otherwise
            pin = (f" The build asked for '{TEXTTABLE_PIN}' and got {v.texttable_version or 'another'} all the same: a "
                   "uv override or constraint on the login node (UV_OVERRIDE, UV_CONSTRAINT, a uv.toml)?"
                   if v.texttable == "broken" else "")
            raise await failed(f"built hpc-bridge's endpoint venv for {mv.key} on {host} ({what}), but it fails its "
                               f"check: {bad}.{pin}")
        mode = "keep"  # verified: whatever happens next (an ssh blip, a rename error), it is never moved to .failed
        rc, out, err = await run("claim", claim_script(mv, token, s.inode if s.exists else ""), _SHORT_S)
        if rc == 255:
            raise _transport("claim", host, rc, out, err)
        claim = _field(out, "HPCB_CLAIM") or ""
        stranded = _field(out, "HPCB_STRANDED")
        if stranded:
            print(f"hpc-bridge: a venv another bring-up put in place was left at {stranded} on {host}", file=sys.stderr)
        if claim == "ok":
            done = True
            serves(v, mv.key)  # claimed all the same: the next connect then says so at its check, with no new build
            return EnsureResult(path=mv.key, built=True, moved_aside=_field(out, "HPCB_MOVED"), reason=why)
        if not claim.startswith("lost"):
            raise EndpointVenvError(
                f"could not put the newly built venv in place at {mv.key} on {host}: "
                f"{claim.removeprefix('error ') or _last_line(err, out)}. The verified build stays at {b}. "
                "Nothing was started or billed.")
        mode = "discard"  # another bring-up put its own build there first: ours is a duplicate; use theirs
        w = await _check(run, mv, host, "re-check")
        won = problem(w, gce=gce, python=python)
        if won == "timed out":
            raise _timed_out(mv.key, host)
        if won is not None:
            raise EndpointVenvError(
                f"another bring-up put a venv at {mv.key} on {host} first, and it fails its check ({won}). "
                "connect_facility again. Nothing was started or billed.")
        serves(w, mv.key)
        return EnsureResult(path=mv.key)
    except asyncio.CancelledError:
        if mode == "failed":
            mode = "discard"  # a cancelled partial build is no evidence of anything
        raise
    finally:
        if not done:
            try:
                await run("cleanup", cleanup_script(mv, token, kill=running, mode=mode), _SHORT_S)
            except Exception:  # noqa: BLE001 - best-effort: at most our own .build.* directory is left
                pass


async def _check(run: Run, mv: ManagedVenv, host: str, step: str, path: str | None = None) -> VenvState:
    rc, out, err = await run(step, check_script(mv, path), _CHECK_S)
    if "HPCB_VENV_BEGIN" not in out:
        if rc == 255 or rc < 0 or not out.strip():   # ssh's own failure, or the local ssh killed: as `step_error`
            raise _transport(step, host, rc, out, err)
        said = _last_line(err, out)
        raise EndpointVenvConfigError(  # ssh got through and the step stopped before its frame: env_setup's site prefix
            f"the {step} of hpc-bridge's endpoint venv on {host} did not run: {said}. Nothing was "
            "started or billed.", said=said, tail=_build_tail(err, out))
    return parse_check(out)


async def _ensure_marker(run: Run, path: str, s: VenvState, host: str) -> None:
    """A venv that works here was built for this architecture: record it where the marker is missing (an older build
    made it). An existing marker is left as it is."""
    if s.arch:
        return
    rc, out, err = await run("marker", _marker_script(path), _SHORT_S)
    if rc == 255:
        raise _transport("marker", host, rc, out, err)


# ------------------------------------------------------------------------------------------------- teardown

def find_script(mv: ManagedVenv, extra_bases: tuple[str, ...] = ()) -> str:
    """The first venv whose entry point runs: the keyed one, the unkeyed `{venv}`, each of `extra_bases` (other unkeyed
    venvs of hpc-bridge's, e.g. the default `$HOME/hpc-bridge/gce-venv`), then the other `<base>-endpoint-*` of every
    base (newest version first), each once, skipping builds, moved-aside venvs and locks. Each `version` is capped and
    no new candidate is tried once the deadline (counted from the loop's start, not the login profile's) passes, so the
    whole search fits the teardown's ssh budget. A candidate whose `version` only ran out of time is reported as
    `HPCB_SLOW`, and candidates the deadline left untried as `HPCB_UNTRIED`: unknown, not broken."""
    b, k = _q(mv.base), _q(mv.key)
    extra = [e for e in dict.fromkeys(extra_bases) if e != mv.base and safe_base(e)]
    unkeyed = "".join(f" {_q(e)}" for e in extra)
    keyed = "".join(f' $(find "${{__hb_e{i}%/*}}" -maxdepth 1 -name "${{__hb_e{i}##*/}}-endpoint-*" 2>/dev/null '
                    "| sort -rV)" for i in range(len(extra)))
    bind = "".join(f"__hb_e{i}={_q(e)}; " for i, e in enumerate(extra))
    return (
        f"{mv.prefix}b={b}; k={k}; {bind}__hb_seen=' '; __hb_t0=$SECONDS; " + _cap(_FIND_CAP_S)
        + f'for d in "$k" "$b"{unkeyed} $(find "${{b%/*}}" -maxdepth 1 -name "${{b##*/}}-endpoint-*" 2>/dev/null '
        f'| sort -rV){keyed}; do '
        f'[ "$((SECONDS - __hb_t0))" -lt {_FIND_DEADLINE_S} ] || {{ echo HPCB_UNTRIED=1; break; }}; '
        'case "$__hb_seen" in (*" $d "*) continue;; esac; __hb_seen="$__hb_seen$d "; '
        "case $d in (*.build.*|*.broken.*|*.failed.*|*.lock|*.old.*) continue;; esac; "
        f'[ -x "$d/bin/{GCE}" ] || continue; __hb_cap "$d/bin/{GCE}" version >/dev/null 2>&1; r=$?; '
        '[ "$r" -eq 0 ] && { echo "HPCB_CLI=$d"; break; }; '
        '[ "$r" -eq 124 ] && [ -z "${__hb_s:-}" ] && { __hb_s=$d; echo "HPCB_SLOW=$d"; }; done; true'
    )


class Found(NamedTuple):
    """What the teardown's search found (`find_working`)."""

    rc: int              # the search's own rc (255: ssh itself failed)
    cli: str | None      # the first venv whose entry point ran
    unknown: bool        # no CLI ran, but a candidate only ran out of time or the deadline left some untried
    err: str


async def find_working(run: Run, mv: ManagedVenv, extra_bases: tuple[str, ...] = ()) -> Found:
    """The first working venv (`cli`); when none ran, whether that is unknown (`HPCB_SLOW`/`HPCB_UNTRIED`: can't
    tell, never "none works") or every candidate was tried and none runs. Never a build."""
    rc, out, err = await run("find", find_script(mv, extra_bases), _FIND_S)
    cli = _field(out, "HPCB_CLI") or None
    unknown = cli is None and bool(_field(out, "HPCB_SLOW") or _field(out, "HPCB_UNTRIED"))
    return Found(rc, cli, unknown, err)
