"""Block environments (PR 3; vault `Modules/envs.md`, design and rulings `Planned/Block environments.md`): point work at
a software environment — one that exists on the facility (`activate`: the user's own shell line, a module system or a
conda env) or one BUILT from a requirements list (`requirements`).

On an endpoint hpc-bridge stood up over SSH a built env is a SEPARATE user env, never the worker's: built on the login
node — by a user endpoint of its own (the login shape's config plus `BUILD_UEC_KEY`), so a build never takes the login
shape's ONE worker, and run_shell(shape="login") and a stop's scancel go on while it runs — and activated per command,
batch row and job script. The block's config never changes, so switching env never needs a new block. An existing env
is the user's line, run before each command on every kind of facility (on a facility's multi-user endpoint too: no new
block). Building an env on a facility endpoint (its template's `requirements`, or its worker_init) is PR 4's, after a
live check: `delivery` refuses it by name here.

The pieces, pure first:
- the argument (`parse` → `EnvSpec`, `EnvRefusal` by name): a requirements text is normalized (`normalize_requirements`:
  continued lines joined, as pip-compile's hash-pinned form writes them) and refused where the cache key cannot see what
  a line names (`-r`, `-c`, `-e`, a local path however spelled, a `$VAR`) or where a line turns a check off
  (`--trusted-host`, any option off `_ALLOWED_OPTIONS`). The user's own index lines are kept and listed (`index_lines`,
  their credentials redacted wherever they are said: `redact`); hpc-bridge adds none, anywhere.
- the key (`env_key`): the hash of the text and the Python (by default the client's major.minor: `default_python`); the
  env lives at `<root>/<key>-<uname -m>`, the architecture appended ON THE NODE — so the activation finds the node's own
  build, and the server needs no architecture probe before building. The root (`env_root`): the facility's scratch
  root's `envs/`, a home one said (`warning`), the user's `root` first.
- the shell: `build_script` (a relocatable uv venv built BESIDE its path — bytecode compiled, then made READ-ONLY, so an
  install into the activated env fails loudly instead of changing it under its key — its marker, `requirements.in`,
  `requirements.lock`, then claimed by ONE rename: of two builds exactly one claims it, so correctness needs no lock;
  `flock`, where the node has it, only saves the second build's work; a partly purged env is moved aside only once its
  rebuild is ready, and swept later), `check_script` (the full check: every RECORD-listed file present and the
  installed set as built), `activation_sh` (per command: the root and the env this user's alone, the marker, then
  VIRTUAL_ENV and PATH — or the user's line, a line that ends the shell included; anything else prints ONE
  nonce-tagged `HPCB_ENV_REFUSED` line and exits `ENV_RC`, before the command; the shell's options and cwd as they
  were after it, its output on stderr), `refusal` (reads that line back: the nonce is the dispatch's, so output cannot
  forge it), `activate_check_script`, `canary_check`.

Then the state this server keeps (AppCtx.env_*): `choose` (ensure_endpoint_up's `env` — a new built env is PENDING
until it is ready, the session's own env serving meanwhile), `ensure_built`, `recheck`, `for_command` and `refused`
(run_shell), the build runner (`_runner`, `drop_runner`), and the ledger's record of a batch's or a job's env
(`record`, `info_of_record`, `activation_of`, `record_state`, `rebuild_record`, `current_record`, `label`).
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import shlex
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from . import session_shell
from .models import EnvInfo

if TYPE_CHECKING:
    from .context import AppCtx

# The exit code of a command its env refused (nothing ran), beside the batch wrapper's (batch.CANCEL_RC's table) and the
# wrong-block guard's 86; and the one stderr line that says why: `HPCB_ENV_REFUSED <nonce> <why> <key> <detail…>`.
ENV_RC = 88
REFUSED = "HPCB_ENV_REFUSED"
BUILD_TAG = "HPCB_ENV_BUILD"           # every build script carries it (the tests, and a reader of the task, find it)
MARKER = ".hpcb-env"                   # key=, short=, arch=, python=, dists=, lock=, built=, host=
# The key that makes the build's own user endpoint on the login node: the login shape's config plus this, so a build has
# a worker of its own (the login shape's user endpoint has ONE: shapes.shape_config). Our template ignores the key.
BUILD_UEC_KEY = "hpcb_env_build"
# A build's leftovers beside the env (a build cut off: `.build.*`; an env moved aside by its rebuild: `.broken.*`) are
# swept by a later build of the same env once older than this (minutes) — longer than any build can run (`_build_s`).
SWEEP_MIN = 240


def _build_s(raw: str | None) -> float:
    """A build's walltime on its runner (the task's own ceiling; `HPC_BRIDGE_ENV_BUILD_S`, default 30 min: a large
    install, a CUDA wheel, takes minutes) — capped half an hour below the sweep's age bound (review U4), so a sweep can
    never take a build that is still running."""
    try:
        v = float(raw) if raw else 1800.0
    except ValueError:
        v = 1800.0
    return max(60.0, min(v, (SWEEP_MIN - 30) * 60.0))


BUILD_S = _build_s(os.environ.get("HPC_BRIDGE_ENV_BUILD_S"))
# A build still unanswered this long past its walltime is lost (its user endpoint restarted, the login node rebooted,
# its result never delivered): given up, and sent again on the next ask (review S5).
LOST_GRACE_S = 300.0
# How long a call waits for a build or a check before answering `building` (the build runs on regardless).
WAIT_S = 20.0
# The full check (every file the install recorded) runs when the env is chosen and once this long has passed: a scratch
# purge runs daily at most. Between two, a command checks the marker alone.
VERIFY_EVERY_S = 24 * 3600.0
MAX_REQUIREMENTS = 64 * 1024
MAX_ACTIVATE = 4096
MAX_SAID = 300                         # what a facility said, quoted back as data: at most this many characters
_KEY_VERSION = "hpcb-env-v1"           # in the key: a change of how envs are built changes every key
_ACTIVATE_KEY_VERSION = "hpcb-env-activate-v1"

_KEYS = frozenset({"activate", "requirements", "python", "root", "name"})
# Each is matched WHOLE (`fullmatch`): `$` in `re.match` also matches before a final newline (review N10).
_PYTHON = re.compile(r"3\.\d{1,2}(\.\d{1,3})?")
_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_ROOT = re.compile(r"(\$HOME/|\$\{HOME\}/|~/|/)[A-Za-z0-9_./+-]{0,400}")   # batch._RESULTS_DIR's rule
_HEX16 = re.compile(r"[0-9a-f]{16}")
_NONCE = re.compile(r"[0-9a-f]{16,64}")
_TOKEN = re.compile(r"[0-9a-f]{1,32}|t[0-9]{1,3}")
# The options a requirements file may carry, as uv reads them: where packages come from and which forms it takes.
_ALLOWED_OPTIONS = frozenset({"--index-url", "-i", "--extra-index-url", "--find-links", "-f", "--no-index", "--pre",
                              "--prefer-binary", "--only-binary", "--no-binary"})
_INDEX_OPTIONS = ("--index-url", "-i", "--extra-index-url", "--find-links", "-f")
# The options that take a value (the next word, or `=value`)
_VALUE_OPTIONS = frozenset({*_INDEX_OPTIONS, "--only-binary", "--no-binary", "-r", "--requirement", "-c",
                            "--constraint", "-e", "--editable", "--trusted-host"})
# An archive a requirement line names by its file name alone (uv reads it as a path: review R2-N2)
_ARCHIVE = re.compile(r"\.(whl|tar\.gz|tgz|tar\.bz2|tbz2?|tar\.xz|txz|tar|zip)$", re.IGNORECASE)
_WHY = ("gone", "failed", "arch", "activate", "unsafe")
_SCHEME = re.compile(r"([A-Za-z][A-Za-z0-9+.-]*)://\S")
# An index URL's credentials (`https://user:token@host`): never said back — not in a notice, the terms, the ledger's
# record, a status or a handoff record (review U3). The text sent to the node is the user's, unchanged.
_USERINFO = re.compile(r"\b([A-Za-z][A-Za-z0-9+.-]*://)[^/\s@]+@")


class EnvRefusal(ValueError):
    """An env argument refused by name (`reason`: a short code), or an env this facility cannot build. Nothing was
    set."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class EnvSpec:
    """The env argument, checked. `kind`: activate | requirements | none (`{}`: no env). `requirements` is the
    normalized text; `python` as asked (None: the client's)."""

    kind: Literal["activate", "requirements", "none"]
    activate: str | None = None
    requirements: str | None = None
    python: str | None = None
    root: str | None = None
    name: str | None = None


def redact(text: str | None) -> str:
    """`text` with every URL's userinfo replaced by `***` (an index URL's credentials, review U3)."""
    return _USERINFO.sub(r"\1***@", text or "")


def said(text: str | None, limit: int = MAX_SAID) -> str:
    """What a facility (or an installer) said, as the notices quote it: its end, capped, credentials redacted — to be
    quoted `!r`, as data, never as instructions (review S12)."""
    return redact(" ".join(str(text or "").split())[-limit:])


# ---- the argument ----------------------------------------------------------------------------------------------------

def normalize_requirements(text: str) -> str:
    """The text as the key reads it: CRLF made LF; a line continued with a final backslash joined to the next (the
    hash-pinned form `pip-compile --generate-hashes` writes: review N6); blanks and comments (a whole line, or ` #…`
    after one) dropped; each line stripped — and the ORDER kept (with several indexes, the first wins)."""
    lines: list[str] = []
    cur = ""
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        s = raw.strip()
        if s.endswith("\\") and not s.startswith("#"):
            cur += s[:-1].strip() + " "
            continue
        lines.append((cur + s).strip())
        cur = ""
    if cur.strip():
        lines.append(cur.strip())
    out = []
    for line in lines:
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"\s+#.*$", "", line).strip()
        if line:
            out.append(line)
    return "\n".join(out)


def index_lines(text: str | None) -> list[str]:
    """The user's own lines that say where packages come from (shown in the terms, credentials redacted; hpc-bridge
    never adds one)."""
    return [redact(ln) for ln in (text or "").split("\n")
            if ln.startswith("-") and any(t.split("=", 1)[0] in _INDEX_OPTIONS for t in ln.split())]


def _remote(url: str) -> bool:
    """A URL the key's hash covers by name: a scheme other than file (http(s), git+https…), never a local path."""
    m = _SCHEME.match(url)
    return m is not None and m.group(1).lower() != "file" and not m.group(1).lower().endswith("+file")


_LOCAL = "a local path, which the env's key (the hash of this text) cannot see change: publish it to an index"


def _option_problem(opt: str, val: str) -> str | None:
    if opt in ("-r", "--requirement", "-c", "--constraint"):
        return ("it names another file, which the env's key (the hash of this text) cannot see change: paste that "
                "file's lines here instead")
    if opt in ("-e", "--editable"):
        return "an editable install is a local path, which the env's key cannot see change"
    if opt == "--trusted-host":
        return "it turns off the index's TLS check"
    if opt not in _ALLOWED_OPTIONS:
        return ("an option hpc-bridge does not pass to the installer (a requirements text may say where packages come "
                f"from and which forms they take: {', '.join(sorted(_ALLOWED_OPTIONS))})")
    if opt in _INDEX_OPTIONS and not _remote(val):
        return f"{opt} takes an index's URL here (https://…), never a local path: {_LOCAL}"
    return None


def _options_problem(line: str) -> str | None:
    """An option line, EVERY option and value on it (review R2-N2: `--no-index --find-links /dir` had passed on its
    first)."""
    try:
        toks = shlex.split(line)
    except ValueError:
        return "its quoting cannot be read"
    i = 0
    while i < len(toks):
        tok = toks[i]
        if not tok.startswith("-"):
            return f"{tok!r} is neither an option nor an option's value here"
        opt, eq, val = tok.partition("=")
        if opt in _VALUE_OPTIONS and not eq:
            i += 1
            val = toks[i] if i < len(toks) else ""
        if (why := _option_problem(opt, val)) is not None:
            return why
        i += 1
    return None


def _line_problem(line: str) -> str | None:
    if "$" in line:   # uv expands ${VAR} in a requirements file, on the node: a value the key cannot see (review N9)
        return "it names an environment variable, which the installer expands on the node: the env's key cannot see it"
    if line.startswith("-"):
        return _options_problem(line)
    if "@" in line:   # a named requirement on a URL (`pkg @ https://…`): a URL with a scheme, never a file (N9)
        target = line.split("@", 1)[1].strip()
        if not _remote(target):
            return _LOCAL
    elif "://" in line:
        if not _remote(line):
            return _LOCAL
    elif "/" in line or "\\" in line or line.startswith((".", "~")):
        return _LOCAL
    elif _ARCHIVE.search(re.split(r"[\s;]", line, maxsplit=1)[0]):
        return f"a bare archive name is a file the installer reads from its own directory: {_LOCAL}"   # R2-N2
    for m in re.finditer(r"\s(--?[A-Za-z][\w-]*)", line):
        if m.group(1) != "--hash":
            return f"a requirement may carry only --hash, not {m.group(1)}"
    return None


def parse(value: Any) -> EnvSpec | None:
    """The `env` argument, checked: None — omitted (keep the session's); `{}` — no env; `{"activate": line}`;
    `{"requirements": text, "python"?, "root"?}`; either with `name`. Raises EnvRefusal naming what is wrong."""
    if value is None:
        return None
    if hasattr(value, "model_dump"):   # the tool's EnvRequest (unknown keys kept, refused here by name)
        value = {k: v for k, v in value.model_dump().items() if v is not None}
    if not isinstance(value, dict):
        raise EnvRefusal("form", 'env must be an object: {"activate": "<shell line>"} for an existing env, '
                                 '{"requirements": "<requirements.txt text>"} for one to build, or {} for none')
    unknown = sorted(str(k) for k in value if k not in _KEYS)
    if unknown:
        raise EnvRefusal("unknown", f"env takes activate, requirements, python, root and name — not "
                                    f"{', '.join(unknown)}")
    if not value:
        return EnvSpec(kind="none")
    act, req = value.get("activate"), value.get("requirements")
    if act is not None and req is not None:
        raise EnvRefusal("both", "env takes activate (an existing env) or requirements (one to build), not both")
    for key in ("python", "root"):
        if value.get(key) is not None and req is None:
            raise EnvRefusal(key, f"env's {key} is for a built env: give requirements with it")
    name = value.get("name")
    if name is not None and (not isinstance(name, str) or not _NAME.fullmatch(name)):
        raise EnvRefusal("name", f"env name {name!r} must match [A-Za-z0-9_.-]{{1,64}}")
    if act is not None:
        if not isinstance(act, str) or not act.strip():
            raise EnvRefusal("activate", "env activate must be the shell line that activates an existing env")
        if len(act) > MAX_ACTIVATE or "\0" in act:
            raise EnvRefusal("activate", f"env activate is longer than {MAX_ACTIVATE} characters (or holds a NUL)")
        return EnvSpec(kind="activate", activate=act, name=name)
    if req is None:
        raise EnvRefusal("form", "env needs activate (an existing env) or requirements (one to build)")
    if not isinstance(req, str):
        raise EnvRefusal("requirements", "env requirements must be a requirements.txt text (lines, as a string)")
    if len(req.encode()) > MAX_REQUIREMENTS or "\0" in req:
        raise EnvRefusal("requirements", f"env requirements is longer than {MAX_REQUIREMENTS // 1024} KiB (or holds a "
                                         "NUL)")
    text = normalize_requirements(req)
    if not text:
        raise EnvRefusal("requirements", "env requirements names no package")
    for line in text.split("\n"):
        if (why := _line_problem(line)) is not None:
            raise EnvRefusal("requirements", f"env requirements: the line {redact(line)!r} is refused — {why}")
    py = value.get("python")
    if py is not None and (not isinstance(py, str) or not _PYTHON.fullmatch(py)):
        raise EnvRefusal("python", f"env python {py!r} must be a CPython version such as 3.12 (or 3.12.4)")
    root = value.get("root")
    if root is not None and (not isinstance(root, str) or not _ROOT.fullmatch(root) or ".." in root.split("/")):
        raise EnvRefusal("root", f"env root {root!r} must be an absolute path, or one under $HOME / ~, of plain "
                                 "characters (letters, digits, _ . / + -)")
    return EnvSpec(kind="requirements", requirements=text, python=py, root=root, name=name)


def default_python() -> str:
    """A built env's Python when none is asked: the client's major.minor (the user's ruling, 2026-11-05) — so one env
    spec is one Python on every facility, and its key is known before any remote call. uv finds it on the node, or
    fetches a managed build of it."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def env_key(spec: EnvSpec) -> str:
    """The env's key (16 hex): the hash of the normalized text and the Python as asked — or, for an existing env, of
    its line. Its directory on the facility is `<root>/<key>-<uname -m>`. Pinned by the tests: a drift would rebuild
    every env."""
    if spec.kind == "activate":
        raw = f"{_ACTIVATE_KEY_VERSION}\0{spec.activate}"
    else:
        raw = f"{_KEY_VERSION}\0{spec.python or default_python()}\0{spec.requirements}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _home_path(path: str) -> str:
    for prefix in ("${HOME}/", "~/"):
        if path.startswith(prefix):
            return "$HOME/" + path[len(prefix):]
    return path


def env_root(scratch_root: str, root: str | None = None) -> tuple[str, str | None]:
    """Where built envs live: the user's `root`, else the facility's scratch root's `envs/` — and, when that is under
    the home directory, a warning to say (homes are small and quota'd; a CUDA wheel alone is GBs). A root of the home
    directory itself (`~/`, `$HOME/`, `${HOME}/`) is `$HOME` (review N2)."""
    if root:
        return _home_path(root).rstrip("/") or "/", None
    base = _home_path(str(scratch_root or "~/.hpc-bridge")).rstrip("/")
    out = f"{base}/envs"
    if out.startswith("$HOME") or re.match(r"^/(home|Users|global/homes)/", out):
        return out, (f"the env root {out} is under your home directory, which is often small and quota'd: a large "
                     "env (a CUDA wheel alone is GBs) may fill it — pass root=… on project or scratch space to build "
                     "it there")
    return out, None


def _qroot(root: str) -> str:
    """The root as a shell word: `$HOME` (or `$HOME/…`) expands on the node, the rest is quoted (batch.quoted_dir's
    rule). A root that is not one line is refused: the build's one answer line must stay one."""
    if "\n" in root or "\r" in root or "\0" in root:
        raise ValueError(f"invalid env root {root!r}")
    if root == "$HOME":
        return '"$HOME"'
    if root.startswith("$HOME/"):
        return '"$HOME"' + shlex.quote(root[len("$HOME"):])
    return shlex.quote(root)


# ---- activation, per command ----------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Activation:
    """What a command, a batch row or a job script activates: a built env (`key` under `root`, built for `arch` — the
    login node's) or an existing env's `line`."""

    how: Literal["built", "activate"]
    key: str
    root: str | None = None
    line: str | None = None
    arch: str | None = None


# A refusal, written by every activation the same way (a function, so an EXIT trap can call it too): the line on
# stderr; the batch wrapper's (or a job's) mark when it set one (`__hb_env_mark`: it ran nothing — the mark carries the
# nonce, and only a mark with its own nonce is believed: review N12); nothing of the command run. Called from the EXIT
# trap (`trap`), it first runs the shell's own EXIT trap with the refusal's status, so the session's kept rc is still
# written (review R2-N6: a poll after a restart reads it).
_REFUSE = (
    "__hb_erefuse() {\n"
    "  set +eu\n"
    # a module line that found no `module` (127) where no module system is initialised: said as such (`NOMODULE`)
    '  case "$__hb_ew${__hb_em:-}:$__hb_ex" in (activate1:rc=127*)\n'
    '    type module >/dev/null 2>&1 || type ml >/dev/null 2>&1 || __hb_ex="$__hb_ex @@NOMODULE@@";; esac\n'
    '  __hb_el="@@REFUSED@@ @@NONCE@@ $__hb_ew $__hb_ek $__hb_ex"\n'
    "  printf '%s\\n' \"$__hb_el\" >&2\n"
    '  [ -z "${__hb_env_mark:-}" ] || printf \'%s\\n\' "$__hb_el" >| "$__hb_env_mark" 2>/dev/null\n'
    '  if [ "${1:-}" = trap ] && [ -n "${__hb_etrap:-}" ]; then\n'
    '    eval "set -- ${__hb_etrap#trap -- }"; (exit @@RC@@); eval "$1"\n'
    "  fi\n"
    "  exit @@RC@@\n"
    "}\n"
)
# Around every activation: the shell's errexit, nounset, noclobber, pipefail, allexport, xtrace, IFS and cwd as they
# were before it (a setup script's `set -euo pipefail`, a .bashrc's noclobber, a dotenv's `set -a`, a debugging `set
# -x`, a PATH walk's `IFS=:`, an activate line's `cd`: reviews N5, R2-N5), its output on stderr — a site banner never
# lands in a row's results (review N8) — and, after it, nothing of it left in the shell the command runs in: not the
# refusal function, not the nonce (review NS6; the user's own code reading the wrapper's text from
# $BASH_EXECUTION_STRING is a residual that can only make its own row run again). errexit is OFF while it runs: a `set
# -e` in the user's line is the line's own.
_HEAD = ("__hb_eo=$-; __hb_ewd=$PWD; __hb_epf=0; shopt -qo pipefail 2>/dev/null && __hb_epf=1\n"
         'if [ -n "${IFS+x}" ]; then __hb_eifs=$IFS; __hb_eifz=1; else __hb_eifz=0; fi\n'
         "set +e\n")
# (every `case` pattern carries its opening parenthesis: bash 3.2 misparses a bare `pat)` inside a `$( … )`, where the
# login node's check of an existing env runs this — review R2-N7)
_TAIL = (
    'case "$__hb_eo" in (*x*) set -x;; (*) set +x;; esac\n'
    'case "$__hb_eo" in (*a*) set -a;; (*) set +a;; esac\n'
    'case "$__hb_eo" in (*e*) set -e;; (*) set +e;; esac\n'
    'case "$__hb_eo" in (*u*) set -u;; (*) set +u;; esac\n'
    'case "$__hb_eo" in (*C*) set -C;; (*) set +C;; esac\n'
    'if [ "$__hb_epf" = 1 ]; then set -o pipefail; else set +o pipefail; fi\n'
    'if [ "$__hb_eifz" = 1 ]; then IFS=$__hb_eifs; else unset IFS; fi\n'
    '[ "$PWD" = "$__hb_ewd" ] || cd -- "$__hb_ewd" 2>/dev/null || :\n'
    "unset -f __hb_erefuse\n"
    "unset __hb_el __hb_ew __hb_ek __hb_ex __hb_erc __hb_etrap __hb_enow __hb_ea __hb_eb __hb_ed __hb_er __hb_eu "
    "__hb_ey __hb_et __hb_env_mark __hb_eifs __hb_eifz __hb_epf __hb_ewd __hb_eo __hb_em __hb_mi\n"
)
# A facility worker's shell is not a login shell: on Anvil's multi-user endpoint `module` was not defined there
# (`module: command not found`, rc 127) where `. /etc/profile.d/modules.sh; module load …` worked (the Anvil live test,
# F5). So an activate line that uses `module` or `ml`, in a shell where neither is a function or a command, first
# sources the first of the usual module inits that exists — its output on stderr with the rest of the activation, and
# under the line's EXIT trap (review round 1: an init that calls `exit` ended the shell before it, and the row read as
# one that ran). The system paths are read under $HPCB_SYSROOT when set (MEASURE_SH's read-only seam for the tests). A
# line that still finds no module command is refused with `nomodule` in its detail (`module_words` says what to add).
MODULE_INITS = ("$LMOD_PKG/init/bash", "/etc/profile.d/lmod.sh", "/etc/profile.d/modules.sh",
                "/usr/share/lmod/lmod/init/bash", "/usr/share/Modules/init/bash")
NOMODULE = "nomodule"
_MODULE_INIT = (
    "__hb_em=1; __hb_mi=\n"
    "if ! type module >/dev/null 2>&1 && ! type ml >/dev/null 2>&1; then\n"
    '  for __hb_mi in ${LMOD_PKG:+"$LMOD_PKG/init/bash"} "${HPCB_SYSROOT:-}/etc/profile.d/lmod.sh" '
    '"${HPCB_SYSROOT:-}/etc/profile.d/modules.sh" "${HPCB_SYSROOT:-}/usr/share/lmod/lmod/init/bash" '
    '"${HPCB_SYSROOT:-}/usr/share/Modules/init/bash"; do\n'
    '    if [ -r "$__hb_mi" ]; then . "$__hb_mi"; break; fi\n'
    "    __hb_mi=\n"
    "  done\n"
    "fi\n"
)
# `module` or `ml` as a COMMAND word of the line: at a line's start, or after `;`, `&&`, `||`, `|`, `(`, `{`, `!`, a
# backquote, or `then` / `do` / `else` — never an argument (review round 1: `conda activate ml`, `python -m module`)
_USES_MODULE = re.compile(r"(?:^|[;&|({!`]|\b(?:then|do|else)\b)[ \t]*(?:module|ml)(?=[ \t]|$|[;&|)}`])", re.M)


def uses_module(line: str | None) -> bool:
    """Whether an activate line runs `module` (or Lmod's `ml`) — then a shell without one gets its init first."""
    return bool(_USES_MODULE.search(line or ""))


def module_words(detail: str | None, line: str | None = None) -> str:
    """What a refusal says when the node's worker shell has no module system initialised (`nomodule` in its detail)."""
    if NOMODULE not in (detail or "").split():
        return ""
    fix = f". /etc/profile.d/modules.sh; {line}" if line else ". /etc/profile.d/modules.sh; <the module line>"
    return ("its line uses `module`, and this node's worker shell has no module system initialised: `module` is not "
            "defined there (a facility's worker runs a non-login shell), and none of the usual inits was found ("
            + ", ".join(MODULE_INITS) + "). Add the site's own module init before the line — e.g. "
            f'env={{"activate": "{fix}"}} — or ask the facility which file initialises its modules')
# The root and the env must be this user's alone (review U1: keys are predictable, so another user could plant an env
# where ours would be found): owned by the user, writable by neither its group nor anyone — the slot lock dir's rule.
_UNSAFE = ('find -H @@PATHS@@ -maxdepth 0 \\( ! -user "${EUID:-$(id -u)}" -o -perm -0002 -o -perm -0020 \\) -print '
           "2>/dev/null")
# What an unsafe directory was seen as: its owner's uid and its mode (a default ACL's mask, or an NFSv4 id-mapping to
# `nobody`, makes a directory the user created read as not theirs alone: review NS2/NS3).
_SEEN = "ls -ldn @@PATH@@ 2>/dev/null | awk '{print \"(owner uid \" $3 \", mode \" $1 \")\"}'"
# A missing env is looked at once more after this many seconds before it is refused: a rebuild swaps a new env in
# beside the old one, and the path is absent for an instant (review NS4).
GONE_RECHECK_S = 2.0

_ACTIVATE_BUILT = (
    "__hb_ek=@@KEY@@; __hb_ea=$(uname -m 2>/dev/null); __hb_er=@@ROOT@@; __hb_eb=@@BUILTFOR@@\n"
    '__hb_ed="$__hb_er/$__hb_ek-$__hb_ea"; __hb_et=0\n'
    "while :; do\n"
    '  __hb_eu=$(@@UNSAFE@@)\n'
    '  if [ -z "$__hb_eu" ] && grep -qx "short=$__hb_ek" "$__hb_ed/@@MARKER@@" 2>/dev/null; then\n'
    '    unset PYTHONHOME; VIRTUAL_ENV="$__hb_ed"; PATH="$__hb_ed/bin:$PATH"; export VIRTUAL_ENV PATH\n'
    "    break\n"
    "  fi\n"
    '  if [ "$__hb_et" = 0 ] && [ -z "$__hb_eu" ] && [ ! -f "$__hb_ed.failed" ] \\\n'
    '      && { [ -z "$__hb_eb" ] || [ "$__hb_eb" = "$__hb_ea" ]; }; then\n'
    "    __hb_et=1; sleep @@RECHECK@@; continue\n"
    "  fi\n"
    '  __hb_ew=gone; __hb_ex="$__hb_ea"\n'
    '  if [ -n "$__hb_eu" ]; then\n'
    "    __hb_ew=unsafe; __hb_ey=$(printf '%s\\n' \"$__hb_eu\" | head -n 1)\n"
    '    __hb_ex="$__hb_ey $(@@SEEN@@)"\n'
    '  elif [ -f "$__hb_ed.failed" ]; then __hb_ew=failed\n'
    '  elif [ -n "$__hb_eb" ]; then\n'
    '    [ "$__hb_eb" = "$__hb_ea" ] || { __hb_ew=arch; __hb_ex="$__hb_ea $__hb_eb"; }\n'
    "  else\n"
    '    for __hb_ey in "$__hb_er/$__hb_ek"-*/@@MARKER@@; do\n'
    '      [ -f "$__hb_ey" ] || continue\n'
    '      __hb_ey=${__hb_ey%/*}; __hb_ew=arch; __hb_ex="$__hb_ex ${__hb_ey##*/$__hb_ek-}"\n'
    "    done\n"
    "  fi\n"
    "  __hb_erefuse\n"
    "done\n"
)

# The user's line, run OUTSIDE any condition (review R2-N9: bash 4+ ignores errexit for everything inside an `if`, so a
# line's `set -e` would not stop it at its failing step) with an EXIT trap armed while it runs: a line that ends the
# shell — its own errexit, nounset, a `${X:?}` guard, `exit` — is a refusal, never a command that ran (review N1). The
# trap the shell had before is restored after it — or the one the line itself set, which is the line's to keep (review
# NS7).
_ACTIVATE_LINE = (
    "__hb_ek=@@KEY@@\n"
    "__hb_etrap=$(trap -p EXIT)\n"
    "trap '__hb_erc=$?; __hb_ew=activate; __hb_ex=\"rc=$__hb_erc (the line ended the shell)\"; __hb_erefuse trap' "
    "EXIT\n"
    "@@MODINIT@@"
    'eval "$(printf %s @@B64@@ | base64 --decode)"\n'
    "__hb_erc=$?\n"
    "set +e\n"
    "__hb_enow=$(trap -p EXIT)\n"
    'case "$__hb_enow" in (*"__hb_erefuse trap"*) ;; (*) __hb_etrap=$__hb_enow;; esac\n'
    "trap - EXIT\n"
    '[ -z "$__hb_etrap" ] || eval "$__hb_etrap"\n'
    'if [ "$__hb_erc" -ne 0 ]; then __hb_ew=activate; __hb_ex="rc=$__hb_erc"; __hb_erefuse; fi\n'
)
# The nonce of a check's own activation (the login node's check of an existing env, a handoff canary's look at its
# env): never a dispatch's — its refusal line is read back by its exit status alone.
_CHECK_NONCE = "c" * 16


def activation_sh(act: Activation, nonce: str) -> str:
    """The shell that activates `act` before a command (session_shell.wrap's `env_sh`, batch.task_script's, a job.sh's):
    a built env by its marker alone (one `grep`, after the root and the env are found to be this user's alone) — then
    what `bin/activate` does, without it (a relocatable venv's `activate` cannot find itself under sh) — or the user's
    line, carried base64 (it is code by design, the user's; never spliced). Anything else — the env gone (a scratch
    purge: a build for this node's own architecture missing), its build failed, built only for another architecture
    than this node's, a directory not the user's alone, the line's own failure — refuses: ONE `HPCB_ENV_REFUSED <nonce>
    <why> <key> …` line and exit ENV_RC, before the command runs. The shell's options and cwd are as they were after it
    (`_TAIL`), and what it prints goes to stderr."""
    if not _NONCE.fullmatch(nonce):
        raise ValueError(f"invalid env nonce {nonce!r}")
    if not _HEX16.fullmatch(act.key):
        raise ValueError(f"invalid env key {act.key!r}")
    refuse = (_REFUSE.replace("@@REFUSED@@", REFUSED).replace("@@NONCE@@", nonce).replace("@@RC@@", str(ENV_RC))
              .replace("@@NOMODULE@@", NOMODULE))
    if act.how == "activate":
        b64 = shlex.quote(base64.b64encode(str(act.line or "").encode()).decode())
        body = (_ACTIVATE_LINE.replace("@@KEY@@", act.key).replace("@@B64@@", b64)
                .replace("@@MODINIT@@", _MODULE_INIT if uses_module(act.line) else ""))
    else:
        arch = act.arch if act.arch and re.fullmatch(r"[A-Za-z0-9_.-]{1,32}", act.arch) else ""
        body = (_ACTIVATE_BUILT.replace("@@UNSAFE@@", _UNSAFE.replace("@@PATHS@@", '"$__hb_er" "$__hb_ed"'))
                .replace("@@SEEN@@", _SEEN.replace("@@PATH@@", '"$__hb_ey"'))
                .replace("@@RECHECK@@", f"{max(0.0, float(GONE_RECHECK_S)):g}")
                .replace("@@KEY@@", act.key).replace("@@ROOT@@", _qroot(str(act.root)))
                .replace("@@BUILTFOR@@", shlex.quote(arch)).replace("@@MARKER@@", MARKER))
    inner = "".join("  " + ln + "\n" for ln in body.rstrip("\n").split("\n"))
    return _HEAD + refuse + "{\n" + inner + "} >&2\n" + _TAIL


@dataclass(frozen=True)
class Refused:
    """A command its env refused (it ran nothing): `why` gone | failed | arch | activate | unsafe; `detail`: this
    node's architecture (and the ones it was built for), `rc=<n>` of the user's line, or the directory not the user's
    alone."""

    why: str
    key: str
    detail: str


def refusal(exit_code: int | None, stderr: str | None, nonce: str) -> Refused | None:
    """The env's refusal of THIS dispatch, or None: exit ENV_RC and a stderr line — anywhere — carrying the dispatch's
    nonce. A command that prints such a line itself does not know the nonce."""
    if exit_code != ENV_RC:
        return None
    head = f"{REFUSED} {nonce} "
    for ln in (stderr or "").splitlines():
        s = ln.strip()
        if not s.startswith(head):
            continue
        parts = s[len(head):].split(None, 2)
        if len(parts) >= 2 and parts[0] in _WHY and _HEX16.fullmatch(parts[1]):
            return Refused(parts[0], parts[1], parts[2] if len(parts) > 2 else "")
    return None


def refusal_mark(text: str | None, nonce: str) -> Refused | None:
    """A batch row's own refusal mark (`.env_refused`, written by its activation inside the wrapper) — believed only
    with THIS attempt's nonce (review N12: a row that writes the file itself is a row that ran)."""
    parts = (text or "").split(None, 4)
    if (len(parts) >= 4 and parts[0] == REFUSED and parts[1] == nonce and parts[2] in _WHY
            and _HEX16.fullmatch(parts[3])):
        return Refused(parts[2], parts[3], parts[4] if len(parts) > 4 else "")
    return None


# ---- building and checking (on the login node) ----------------------------------------------------------------------

_FNS = r"""__hb_sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -c1-64; else shasum -a 256 | cut -c1-64; fi
}
__hb_dists() {
  ls -d "$1"/lib/python*/site-packages/*.dist-info 2>/dev/null | sed 's#.*/##' | LC_ALL=C sort | __hb_sha
}
# the first of its paths not this user's alone: owned by someone else, or writable by its group or anyone (review U1)
__hb_unsafe() {
  @@UNSAFE@@ | head -n 1
}
# an env tree removed — read-only by design (review N3), so made writable first; links are never followed
__hb_rm() {
  find "$1" ! -type l -exec chmod u+w {} + 2>/dev/null
  rm -rf "$1"
}
# what an unsafe path was seen as (review NS2/NS3)
__hb_seen() {
  @@SEEN@@
}
# the files each RECORD lists that are not there, read as the csv it is — a path holding a comma is quoted (wcwidth
# ships `textwrap.py,cover`: review R2-N1) — by the env's own Python, isolated from the caller's PYTHON* settings
__hb_recpy='import csv, glob, os, sys
n = 0
for rec in glob.glob(os.path.join(sys.argv[1], "lib", "python*", "site-packages", "*.dist-info", "RECORD")):
    sp = os.path.dirname(os.path.dirname(rec))
    with open(rec, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.reader(fh):
            if row and row[0] and not os.path.exists(os.path.join(sp, row[0])):
                n += 1
print(n)'
__hb_check() {
  [ -d "$1" ] || { echo gone; return 0; }
  __hb_x=$(__hb_unsafe "$__hb_r" "$1")
  [ -z "$__hb_x" ] || { echo "unsafe $__hb_x"; return 0; }
  grep -qx "short=$__hb_k" "$1/@@MARKER@@" 2>/dev/null || { echo "partial 0"; return 0; }
  __hb_n=$("$1/bin/python" -I -c "$__hb_recpy" "$1" 2>/dev/null)
  case "$__hb_n" in ''|*[!0-9]*) echo "partial 0"; return 0;; esac
  if [ "$__hb_n" -eq 0 ] && [ "$(__hb_dists "$1")" = "$(sed -n 's/^dists=//p' "$1/@@MARKER@@")" ]; then echo ready
  else echo "partial $__hb_n"; fi
}
""".replace("@@UNSAFE@@", _UNSAFE.replace("@@PATHS@@", '"$@"')).replace("@@SEEN@@", _SEEN.replace("@@PATH@@", '"$1"'))

_BUILD = r"""# @@TAG@@ @@KEY@@ — hpc-bridge builds a user env (hpc_bridge.envs): beside its path, then one rename
# what it creates is never writable by the group or anyone (a node whose umask is 002 would make the root so, and the
# root and the env must be the user's alone: review U1) — and never looser than the node's own umask (review R2-N3: a
# node's 077 stays 077)
__hb_um=$(umask)
umask "$(printf '%o' $(( 8#$__hb_um | 8#22 )))"
__hb_k=@@KEY@@; __hb_r=@@ROOT@@; __hb_t=@@TOKEN@@
__hb_a=$(uname -m 2>/dev/null)
__hb_d="$__hb_r/$__hb_k-$__hb_a"
__hb_b=""; __hb_cwd=""
__hb_log="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t.log"
@@FNS@@# the login node's own installer settings (reviews U2, R2-N4): the NAMES of every UV_* and PIP_* variable set,
# never their values, and the uv (and pip) config files that apply — said with the answer, never changed
__hb_amb=$(env | sed -nE 's/^((UV|PIP)_[A-Za-z0-9_]*)=.*/\1/p' | LC_ALL=C sort -u | tr '\n' ',')
__hb_cfg=""
__hb_addcfg() {
  [ -f "$1" ] || return 0
  case ",$__hb_cfg" in *",$1,"*) ;; *) __hb_cfg="$__hb_cfg$1,";; esac
}
[ -z "${UV_CONFIG_FILE:-}" ] || __hb_addcfg "$UV_CONFIG_FILE"
for __hb_f in "${XDG_CONFIG_HOME:-$HOME/.config}/uv/uv.toml" "$HOME/.config/uv/uv.toml" /etc/uv/uv.toml \
    "${XDG_CONFIG_HOME:-$HOME/.config}/pip/pip.conf" "$HOME/.pip/pip.conf" /etc/pip.conf; do
  __hb_addcfg "$__hb_f"
done
__hb_fail() {
  __hb_m=$(printf '%s\n' "$1" | tail -c 2000)
  [ -z "$__hb_b" ] || __hb_rm "$__hb_b"
  rm -f "$__hb_log"
  [ -n "${2:-}" ] || ( umask 077; printf '%s\n' "$__hb_m" >| "$__hb_d.failed" ) 2>/dev/null
  printf 'HPCB_ENV_FAILED key=%s arch=%s ambient=%s config=%s error_b64=%s\n' "$__hb_k" "$__hb_a" "${__hb_amb%,}" \
    "${__hb_cfg%,}" "$(printf '%s' "$__hb_m" | base64 | tr -d '\n')"
  exit 0
}
# uv runs from a fresh, empty directory of the user's own: uv reads a project's uv.toml or pyproject.toml from its cwd
# and every parent — the env root's included (review R2-N4) — so it is moved off them; a user- or site-level uv.toml
# still applies (a site's mirror may need it), and any project file above this directory too is named with the answer
__hb_cwd=$(mktemp -d "$HOME/.hpcb-uv.XXXXXX" 2>/dev/null || mktemp -d "${TMPDIR:-/tmp}/hpcb-uv.XXXXXX" 2>/dev/null)
trap '[ -z "$__hb_cwd" ] || rmdir "$__hb_cwd" 2>/dev/null' EXIT
__hb_p=$__hb_cwd
while [ -n "$__hb_p" ]; do
  __hb_addcfg "$__hb_p/uv.toml"
  grep -qs '^\[tool\.uv' "$__hb_p/pyproject.toml" && __hb_addcfg "$__hb_p/pyproject.toml"
  [ "$__hb_p" = / ] && break
  __hb_p=${__hb_p%/*}; [ -n "$__hb_p" ] || __hb_p=/
done
__hb_ready() {
  printf 'HPCB_ENV_READY key=%s dir=%s arch=%s python=%s lock=%s size_mb=%s free_mb=%s cached=%s ambient=%s '\
'config=%s\n' "$__hb_k" "$__hb_d" "$__hb_a" "$(sed -n 's/^python=//p' "$__hb_d/@@MARKER@@")" \
    "$(sed -n 's/^lock=//p' "$__hb_d/@@MARKER@@")" "$(du -sm "$__hb_d" 2>/dev/null | cut -f1)" \
    "$(df -Pk "$__hb_r" 2>/dev/null | awk 'NR==2{print int($4/1024)}')" "$1" "${__hb_amb%,}" "${__hb_cfg%,}"
}
mkdir -p "$__hb_r" 2>/dev/null || __hb_fail "cannot create the env root $__hb_r" nomark
__hb_x=$(__hb_unsafe "$__hb_r")
[ -z "$__hb_x" ] || __hb_fail "the env root $__hb_x $(__hb_seen "$__hb_x") is not yours alone (owned by another user, \
or writable by its group or anyone — a default ACL, or an NFSv4 id-mapping to nobody, can make a directory you created \
read so): hpc-bridge builds envs only where no one else can change them — pass root=… on a directory you own alone" \
nomark
[ -n "$__hb_cwd" ] || __hb_fail "cannot make a private working directory for the installer (mktemp)"
# where the node has flock, a second build of the same env waits for the first, then finds it built
if command -v flock >/dev/null 2>&1; then
  { exec 9>"$__hb_r/$__hb_k-$__hb_a.lock" && flock -w @@CAP@@ 9; } 2>/dev/null || true
fi
__hb_s=$(__hb_check "$__hb_d")
case "$__hb_s" in
  ready) __hb_ready 1; exit 0;;
  unsafe*) __hb_fail "the env ${__hb_s#unsafe } $(__hb_seen "${__hb_s#unsafe }") is not yours alone (owned by another \
user, or writable by its group or anyone — a default ACL, or an NFSv4 id-mapping to nobody, can make a directory you \
created read so): hpc-bridge never uses or replaces it — remove it, or pass root=… on a directory you own alone" \
nomark;;
esac
rm -f "$__hb_d.failed"
# what builds cut off and rebuilds moved aside left beside it (review N11), once older than any build can run
find "$__hb_r" -maxdepth 1 \( -name ".$__hb_k-$__hb_a.build.*" -o -name ".$__hb_k-$__hb_a.broken.*" \) \
  -mmin +@@SWEEP@@ 2>/dev/null | while IFS= read -r __hb_x; do __hb_rm "$__hb_x"; done
export PATH="$HOME/.local/bin:$PATH" UV_CACHE_DIR="$__hb_r/.uv-cache"
cd "$__hb_cwd" || __hb_fail "cannot enter the installer's working directory $__hb_cwd"
# (uv, and the installer that fetches it, run with the lock's descriptor closed: a child left running never holds the
# lock — review NS9)
command -v uv >/dev/null 2>&1 || { curl -LsSf https://astral.sh/uv/install.sh 9>&- \
  | env UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1 9>&-; } || true
command -v uv >/dev/null 2>&1 || __hb_fail "no uv on the login node, and astral.sh's installer could not install one"
__hb_u=$(command -v uv)
__hb_b="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t"
# the requirements (an index URL's credentials included) and what the installer says are the user's alone: 0600, then
# 0400 once the env is made read-only (review R2-N3)
( umask 077; : >| "$__hb_log" ) 2>/dev/null
"$__hb_u" venv -q --relocatable --python @@PY@@ "$__hb_b" > "$__hb_log" 2>&1 9>&- \
  || __hb_fail "$(tail -n 20 "$__hb_log")"
( umask 077; printf %s @@B64@@ | base64 --decode >| "$__hb_b/requirements.in" ) \
  || __hb_fail "cannot write $__hb_b/requirements.in"
"$__hb_u" pip install -q --compile-bytecode --link-mode=copy --python "$__hb_b/bin/python" \
  -r "$__hb_b/requirements.in" > "$__hb_log" 2>&1 9>&- || __hb_fail "$(tail -n 20 "$__hb_log")"
( umask 077; "$__hb_u" pip freeze --python "$__hb_b/bin/python" >| "$__hb_b/requirements.lock" 2> "$__hb_log" 9>&- ) \
  || __hb_fail "$(tail -n 20 "$__hb_log")"
{
  printf 'key=%s\nshort=%s\narch=%s\n' @@KEY64@@ "$__hb_k" "$__hb_a"
  printf 'python=%s\n' "$("$__hb_b/bin/python" -c 'import platform; print(platform.python_version())' 2>/dev/null)"
  printf 'dists=%s\nlock=%s\n' "$(__hb_dists "$__hb_b")" "$(__hb_sha < "$__hb_b/requirements.lock")"
  printf 'built=%s\nhost=%s\n' "$(date +%s)" "$(hostname 2>/dev/null)"
} > "$__hb_b/@@MARKER@@" || __hb_fail "cannot write $__hb_b/@@MARKER@@"
# read-only from here (review N3): an install into the activated env fails loudly, never changing it under its key —
# adding a package is a change of the requirements (another env); links are never followed
find "$__hb_b" ! -type l -exec chmod a-w {} + 2>/dev/null
# another build of this env (no flock on the node, or its wait ran out) may have claimed a whole one meanwhile: that one
# stands — rows may already run in it — and this one is discarded (review R2-N8)
if [ "$(__hb_check "$__hb_d")" = ready ]; then
  __hb_rm "$__hb_b"; __hb_b=""; rm -f "$__hb_log"
  __hb_ready 1; exit 0
fi
# a partly purged env is moved aside only now that its rebuild is ready: work still running in it keeps its files, and
# a later build sweeps it once it is old (its age counted from now)
if [ -e "$__hb_d" ]; then
  mv "$__hb_d" "$__hb_r/.$__hb_k-$__hb_a.broken.$__hb_t" 2>/dev/null && touch "$__hb_r/.$__hb_k-$__hb_a.broken.$__hb_t"
fi
# the claim: ONE rename, which a non-empty target refuses — of two builds at once, the other's stays and this one goes
"$__hb_b/bin/python" -c 'import os, sys; os.rename(sys.argv[1], sys.argv[2])' "$__hb_b" "$__hb_d" 2>/dev/null \
  || __hb_rm "$__hb_b"
__hb_b=""
rm -f "$__hb_log"
case "$(__hb_check "$__hb_d")" in
  ready) __hb_ready 0;;
  *) __hb_fail "the env claimed at $__hb_d does not check out";;
esac
"""

_CHECK = r"""__hb_k=@@KEY@@; __hb_r=@@ROOT@@
__hb_a=$(uname -m 2>/dev/null)
__hb_d="$__hb_r/$__hb_k-$__hb_a"
@@FNS@@__hb_s=$(__hb_check "$__hb_d")
if [ "$__hb_s" = gone ] && [ -f "$__hb_d.failed" ]; then
  printf 'HPCB_ENV_CHECK state=failed key=%s arch=%s error_b64=%s\n' "$__hb_k" "$__hb_a" \
    "$(tail -c 2000 "$__hb_d.failed" | base64 | tr -d '\n')"
elif [ "$__hb_s" = gone ]; then
  __hb_o=""
  for __hb_x in "$__hb_r/$__hb_k"-*/@@MARKER@@; do
    [ -f "$__hb_x" ] || continue
    __hb_x=${__hb_x%/*}; __hb_o="$__hb_o,${__hb_x##*/$__hb_k-}"
  done
  if [ -n "$__hb_o" ]; then
    printf 'HPCB_ENV_CHECK state=arch key=%s arch=%s others=%s\n' "$__hb_k" "$__hb_a" "${__hb_o#,}"
  else printf 'HPCB_ENV_CHECK state=gone key=%s arch=%s\n' "$__hb_k" "$__hb_a"; fi
else
  case "$__hb_s" in
    partial*) __hb_m=${__hb_s#partial }; __hb_s=partial;;
    unsafe*) __hb_m=0; __hb_d=${__hb_s#unsafe }; __hb_s=unsafe;;
    *) __hb_m=0;;
  esac
  printf 'HPCB_ENV_CHECK state=%s key=%s arch=%s missing=%s dir=%s\n' "$__hb_s" "$__hb_k" "$__hb_a" "$__hb_m" "$__hb_d"
fi
"""


def build_script(spec: EnvSpec, *, key: str, root: str, python: str, token: str) -> str:
    """The script that builds `spec` on the login node (see the module doc), printing ONE line: `HPCB_ENV_READY key=
    dir= arch= python= lock= size_mb= free_mb= cached= ambient= config=` (cached=1: it was already built and checks
    out) or `HPCB_ENV_FAILED key= arch= ambient= config= error_b64=` (the installer's own words; a `<key>-<arch>.failed`
    mark beside the env says so to the next activation). The requirements reach the installer as a FILE (carried
    base64): no character of them is shell. No index option is added; the node's own installer settings are named
    (`ambient=`, `config=`), never changed."""
    if spec.kind != "requirements" or not spec.requirements:
        raise ValueError("build_script builds a requirements env")
    if not _HEX16.fullmatch(key) or not _TOKEN.fullmatch(token) or not _PYTHON.fullmatch(python):
        raise ValueError(f"invalid build parameters {key!r} {token!r} {python!r}")
    py = spec.python or default_python()
    full = hashlib.sha256(f"{_KEY_VERSION}\0{py}\0{spec.requirements}".encode()).hexdigest()
    b64 = shlex.quote(base64.b64encode((spec.requirements + "\n").encode()).decode())
    return (_BUILD.replace("@@FNS@@", _FNS).replace("@@TAG@@", BUILD_TAG).replace("@@KEY64@@", full)
            .replace("@@KEY@@", key).replace("@@ROOT@@", _qroot(root)).replace("@@TOKEN@@", token)
            .replace("@@PY@@", shlex.quote(python)).replace("@@B64@@", b64).replace("@@CAP@@", str(int(BUILD_S)))
            .replace("@@SWEEP@@", str(SWEEP_MIN)).replace("@@MARKER@@", MARKER))


def check_script(*, key: str, root: str) -> str:
    """The full check of the env `key` under `root` on THIS node, printing `HPCB_ENV_CHECK state=…`: ready (every file
    its install recorded present, the installed set as built), partial (`missing=`: a purge took part of it, or it is
    not ours), gone, failed (its build's mark), arch (built only for `others=`, never for this node), unsafe (`dir=`: a
    directory not the user's alone)."""
    if not _HEX16.fullmatch(key):
        raise ValueError(f"invalid env key {key!r}")
    return (_CHECK.replace("@@FNS@@", _FNS).replace("@@KEY@@", key).replace("@@ROOT@@", _qroot(root))
            .replace("@@MARKER@@", MARKER))


def activate_check_script(line: str) -> str:
    """An existing env's line, run once on the login node, printing `HPCB_ENV_ACTIVATE rc=<n> out_b64=<the end of what
    it said>`: the very activation every command runs (`activation_sh`), in a subshell (nothing of it stays) — so a
    line every command would refuse (one that ends the shell, one whose own errexit stops it) never passes here (review
    R2-N7). rc 0: it activated; else the activation refused (its refusal line itself is not in what it said)."""
    act = activation_sh(Activation("activate", env_key(EnvSpec(kind="activate", activate=line)), line=line),
                        _CHECK_NONCE)
    # (the endpoint venv scrubbed first, as every command's wrapper does: session_shell.ENDPOINT_VENV_SCRUB; a refusal
    # for no module system is said as such — ` nomodule` after the rc — before its line is taken out: review round 1)
    return ("__hb_o=$( (\n" + session_shell.ENDPOINT_VENV_SCRUB + act + ") 2>&1 ); __hb_rc=$?\n"
            f"__hb_nm=; printf '%s\\n' \"$__hb_o\" | grep -q '^{REFUSED} .* {NOMODULE}$' && __hb_nm=' {NOMODULE}'\n"
            f"__hb_o=$(printf '%s\\n' \"$__hb_o\" | grep -v '^{REFUSED} ')\n"
            "printf 'HPCB_ENV_ACTIVATE rc=%s%s out_b64=%s\\n' \"$__hb_rc\" \"$__hb_nm\" "
            "\"$(printf '%s' \"$__hb_o\" | tail -c 2000 | base64 | tr -d '\\n')\"\n")


def canary_check(rec: dict[str, Any]) -> str:
    """A handoff canary's look at its env from inside the block: `HPCB_ENV_OK 1` when the very activation every command
    runs (`activation_sh`, in a subshell) takes it — a built env's marker for this node, in a root and a directory the
    user owns alone (review SS7), or an existing env's line under the same errexit and EXIT-trap rules (review R2-N7);
    else `HPCB_ENV_OK 0`."""
    act = activation_sh(activation_of(rec), _CHECK_NONCE)
    # (the subshell stands alone, never on the left of `&&`: bash 4+ would ignore the line's errexit there; the endpoint
    # venv scrubbed first, as every command's wrapper does)
    return ('( \n' + session_shell.ENDPOINT_VENV_SCRUB + act + ') >/dev/null 2>&1\n'
            'if [ "$?" -eq 0 ]; then echo "HPCB_ENV_OK 1"; else echo "HPCB_ENV_OK 0"; fi')


@dataclass
class Built:
    """A build's (or a check's) answer: `ok` (ready), and what the node said."""

    ok: bool
    key: str | None = None
    path: str | None = None
    arch: str | None = None
    python: str | None = None
    lock_sha256: str | None = None
    size_mb: float | None = None
    free_mb: float | None = None
    cached: bool = False
    error: str | None = None
    ambient: list[str] | None = None   # the login node's installer settings that applied: names, and config files


def _fields(line: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in re.finditer(r"(\w+)=(\S*)", line)}


def _b64text(v: str | None) -> str:
    try:
        return base64.b64decode(v or "").decode(errors="replace")
    except (ValueError, TypeError):
        return ""


def _num(v: str | None) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def _ambient(f: dict[str, str]) -> list[str] | None:
    names = [f"{n} (set on the login node)" for n in f.get("ambient", "").split(",") if re.fullmatch(r"[A-Z_]+", n)]
    files = [f"{p} (a config file)" for p in f.get("config", "").split(",") if p]
    return names + files or None


def parse_build(stdout: str | None) -> Built | None:
    """The build script's line (the last one), or None when it printed none (cut off: its walltime, a teardown)."""
    for line in reversed(str(stdout or "").splitlines()):
        if line.startswith("HPCB_ENV_READY "):
            f = _fields(line)
            return Built(ok=True, key=f.get("key"), path=f.get("dir") or None, arch=f.get("arch") or None,
                         python=f.get("python") or None, lock_sha256=f.get("lock") or None,
                         size_mb=_num(f.get("size_mb")), free_mb=_num(f.get("free_mb")), cached=f.get("cached") == "1",
                         ambient=_ambient(f))
        if line.startswith("HPCB_ENV_FAILED "):
            f = _fields(line)
            return Built(ok=False, key=f.get("key"), arch=f.get("arch") or None, ambient=_ambient(f),
                         error=_b64text(f.get("error_b64")).strip() or "the build failed and said nothing")
    return None


@dataclass
class Check:
    """The full check's verdict on one node: `state` ready | partial | gone | failed | arch | unsafe."""

    state: str
    arch: str | None = None
    missing: int = 0
    path: str | None = None
    others: list[str] | None = None
    error: str | None = None


def parse_check(stdout: str | None) -> Check | None:
    for line in reversed(str(stdout or "").splitlines()):
        if line.startswith("HPCB_ENV_CHECK "):
            f = _fields(line)
            return Check(state=f.get("state", "gone"), arch=f.get("arch") or None,
                         missing=int(f["missing"]) if f.get("missing", "").isdigit() else 0, path=f.get("dir"),
                         others=[x for x in f.get("others", "").split(",") if x] or None,
                         error=_b64text(f.get("error_b64")).strip() or None)
    return None


# ---- where an env is built, and how it is used ----------------------------------------------------------------------

def delivery(spec: EnvSpec, *, access: str, client_python: str, reads_requirements: bool | None = None,
             reads_worker_init: bool | None = None) -> str:
    """How `spec` reaches the work: `activate` (the user's line, per command: any facility) or `login` (built on the
    login node of an endpoint hpc-bridge stood up over SSH, activated per command). A built env on a facility's
    multi-user endpoint is refused by name: building there (its template's `requirements`, or its worker_init) is PR
    4's, after a live check. (`reads_*`, `client_python`: what that delivery will read.)"""
    del reads_requirements, reads_worker_init, client_python   # PR 4's (the facility template's gate)
    if spec.kind == "activate":
        return "activate"
    if spec.kind == "requirements" and access == "ssh":
        return "login"
    if spec.kind == "requirements":
        raise EnvRefusal("facility", 'building an env on a facility endpoint is not supported yet: use an existing env '
                                     'there with env={"activate": "<shell line>"} — a module or a conda env the '
                                     "facility provides")
    raise EnvRefusal("form", "no env to deliver")


# ---- the state this server keeps ------------------------------------------------------------------------------------

@dataclass
class _Known:
    """An env this server knows (AppCtx.env_known, by root and key): its spec, what the last answer said of it, a build
    or a check still out (`future`, sent at `sent_at` to `sent_to`), whether that one is a QUIET re-check of an env
    that stays ready while it runs (review U5), when its full check last passed (monotonic), the root's own warning
    (the low-space one is added afresh from each answer: review S17), and what kind of failure its last one was
    (`fault`: installer | runner | lost | mark — the words differ: review U9). Its `info` carries no name: a name is
    the owner's (the session's, a batch's), never the shared env's (review U8). `sent_runner`: the runner `future`
    went out on — only that one is dropped when it fails (review SS5)."""

    spec: EnvSpec
    root: str
    key: str
    info: EnvInfo
    future: Any = None
    kind: str = "build"            # what `future` is: a build, or an existing env's check
    verified_at: float | None = None
    sent_at: float | None = None
    sent_to: str | None = None
    quiet: bool = False
    root_warning: str | None = None
    fault: str | None = None
    sent_runner: Any = None


def access_of(app: AppCtx) -> str:
    """ssh — an endpoint hpc-bridge stood up (it has a login node to build on) — or mep, a facility's endpoint."""
    from .context import _has_login_shape

    return "ssh" if _has_login_shape(app) else "mep"


def _known(app: AppCtx, spec: EnvSpec) -> _Known:
    if spec.kind == "activate":
        key = env_key(spec)
        k = app.env_known.get(f"activate\0{key}")
        if k is None:
            k = _Known(spec, "", key, EnvInfo(kind="activate", how="activate", key=key))
            app.env_known[f"activate\0{key}"] = k
        return k
    root, warning = env_root(app.scratch_root, spec.root)
    key = env_key(spec)
    k = app.env_known.get(f"{root}\0{key}")
    if k is None:
        k = _Known(spec, root, key, EnvInfo(kind="built", how="login", key=key, python=spec.python or default_python(),
                                            root=root, index_lines=index_lines(spec.requirements), warning=warning),
                   root_warning=warning)
        app.env_known[f"{root}\0{key}"] = k
    return k


def info_for(k: _Known, name: str | None) -> EnvInfo:
    """`k`'s info as one owner sees it: with that owner's name for it."""
    return k.info.model_copy(update={"name": name})


def drop_runner(app: AppCtx, only: Any = None) -> None:
    """Close and forget the build runner (its Executor shut down by the SDK, a new Globus login, an endpoint repaired,
    adopted or changed: review S7): the next build gets a fresh one. `only`: drop it only when it is that runner — a
    failure of an older one never closes a newer (review SS5)."""
    if only is not None and app.env_runner is not only:
        return
    r, app.env_runner, app.env_runner_key = app.env_runner, None, None
    if r is not None:
        try:
            r.close()
        except Exception:  # noqa: BLE001 - closing a client-side Executor: nothing on the facility depends on it
            pass


def _runner(app: AppCtx) -> Any:
    """The runner env builds and checks go to: the login node's own user endpoint for them — the login shape's config
    plus BUILD_UEC_KEY, so a build has its own worker and never takes the login shape's one — with a build's walltime.
    One per endpoint AND config (the worker_init the endpoint was adopted with is in it: a runner made before the
    adoption, or for another endpoint, is closed and replaced); never a session's, never hpc-bridge's internal one.
    Raises when no endpoint is up yet (review S8: a build is never sent to no endpoint)."""
    eid = app.state.endpoint_id
    if eid is None:
        raise RuntimeError("no endpoint is up yet to build on")
    from . import warmth

    uec = {**warmth._seed_uec(app, "login"), BUILD_UEC_KEY: True}
    uec = getattr(app.facility, "dispatch_uec", lambda d: d)(uec)
    want = (str(eid), json.dumps(uec, sort_keys=True, default=str))
    if app.env_runner is not None and app.env_runner_key == want:
        return app.env_runner
    drop_runner(app)
    r = app.runner_factory(eid, user_endpoint_config=uec, walltime=BUILD_S, timeout=WAIT_S)
    app.env_runner, app.env_runner_key = r, want
    return r


def _fail(k: _Known, error: str, fault: str) -> None:
    k.fault = fault
    k.info = k.info.model_copy(update={"state": "failed", "task_id": None, "error": said(error, 600), "cached": False})


def start_build(app: AppCtx, k: _Known, *, quiet: bool = False) -> None:
    """Send `k`'s build (its own check first: an env already there is a cache hit) — sync, so a batch's settling can
    start a rebuild too. `quiet`: a re-check of a READY env (the 24 h re-verify, a block newly confirmed) — the env
    stays ready while it runs, and commands go on (review U5); only a failed answer changes its state. A submit the
    runner refuses is a failure said in its own words, and that runner is replaced (review S7)."""
    script = build_script(k.spec, key=k.key, root=k.root, python=k.spec.python or default_python(),
                          token=secrets.token_hex(4))
    runner = None
    try:
        runner = _runner(app)
        fut = runner.submit(script)
    except Exception as exc:  # noqa: BLE001 - the Executor shut down, no endpoint: said, never raised out of a tool
        drop_runner(app, only=runner)
        k.future = None
        _fail(k, f"its build could not be sent to the login node: {type(exc).__name__}: {exc}", "runner")
        return
    k.future, k.kind, k.sent_at, k.sent_to = fut, "build", time.monotonic(), str(app.state.endpoint_id)
    k.sent_runner = runner
    k.quiet = quiet and k.info.state == "ready"
    upd: dict[str, Any] = {"task_id": getattr(fut, "task_id", None)}
    if not k.quiet:
        upd.update(state="building", error=None, cached=False)
    k.info = k.info.model_copy(update=upd)


def _absorb(app: AppCtx, k: _Known) -> None:
    """A finished build or check, read into what is known of the env."""
    fut, k.future = k.future, None
    quiet, k.quiet = k.quiet, False
    if fut.cancelled():   # the client let go of it (a teardown, a new binding, its runner replaced): checked again next
        k.verified_at = None
        if not quiet:
            k.info = k.info.model_copy(update={"state": "unchecked", "task_id": None})
        return
    exc = fut.exception()
    if exc is not None:   # the runner's own failure (the SDK shut its Executor down): never reused (review S7)
        drop_runner(app, only=k.sent_runner)
        if k.kind == "activate":   # the line was never run: not checked, never "its line did not work" (R2-S4)
            k.fault = "runner"
            k.info = k.info.model_copy(update={"state": "unchecked", "task_id": None, "error": said(
                f"its check could not be run on the login node: {type(exc).__name__}: {exc}")})
            return
        _fail(k, f"it could not be run on the login node: {type(exc).__name__}: {exc}", "runner")
        return
    res = fut.result()
    out = str(getattr(res, "stdout", "") or "")
    if k.kind == "activate":
        m = re.search(rf"^HPCB_ENV_ACTIVATE rc=(\d+)( {NOMODULE})? out_b64=(\S*)\s*$", out, re.M)
        words = _b64text(m.group(3)).strip() if m else out.strip()[-300:]
        ok = m is not None and m.group(1) == "0"
        err = None if ok else (said(words) or "its line failed and said nothing")
        k.fault = None if ok else "line"
        if not ok and m is not None and m.group(2):   # no module system on the login node: said in hpc-bridge's words
            k.fault, err = "nomodule", module_words(NOMODULE, k.spec.activate)
        k.info = k.info.model_copy(update={"state": "ready" if ok else "unchecked", "task_id": None, "error": err})
        return
    b = parse_build(out)
    if b is None:
        tail = (out + "\n" + str(getattr(res, "stderr", "") or "")).strip()[-300:]
        _fail(k, f"the build did not finish (rc {getattr(res, 'returncode', '?')}): {tail or 'it said nothing'}",
              "runner")
        return
    if not b.ok:
        _fail(k, str(b.error), "installer")
        k.info = k.info.model_copy(update={"ambient": b.ambient or []})
        return
    k.verified_at, k.fault = time.monotonic(), None
    k.info = k.info.model_copy(update={"state": "ready", "task_id": None, "error": None, "path": b.path,
                                       "arch": b.arch, "python": b.python or k.info.python,
                                       "lock_sha256": b.lock_sha256, "size_mb": b.size_mb, "cached": b.cached,
                                       "warning": _space_warning(k.root_warning, b), "ambient": b.ambient or []})


def _space_warning(warning: str | None, b: Built) -> str | None:
    """The root's own warning (never the last answer's: review S17), and — near quota — that the root has under twice
    the env's size free."""
    if b.size_mb is not None and b.free_mb is not None and b.free_mb < 2 * b.size_mb:
        low = (f"the env root has {b.free_mb:g} MB free, under twice the env's {b.size_mb:g} MB: another env may not "
               "fit there")
        return f"{warning}; {low}" if warning else low
    return warning


def _lost(app: AppCtx, k: _Known, why: str) -> None:
    """A build or check that will never answer (review S5): given up, and sent again on the next ask."""
    fut, k.future = k.future, None
    try:
        fut.cancel()
    except Exception:  # noqa: BLE001 - the client's copy only
        pass
    k.verified_at = None
    if k.quiet:   # a ready env's re-check: it stays as it was, and is checked again next time
        k.quiet = False
        k.info = k.info.model_copy(update={"task_id": None})
        return
    k.fault = "lost"
    k.info = k.info.model_copy(update={"state": "unchecked", "task_id": None, "error": why})


def refresh(app: AppCtx) -> None:
    """Read in every build or check that finished since the last look (no wait); give up one lost — unanswered long
    past a build's walltime, or sent to an endpoint that is no longer the bound one; and make a pending env the
    session's once it is ready (or drop it, saying why, once it failed)."""
    now = time.monotonic()
    eid = str(app.state.endpoint_id) if app.state.endpoint_id is not None else None
    for k in list(app.env_known.values()):
        if k.future is None:
            continue
        if k.future.done():
            _absorb(app, k)
        elif k.sent_at is not None and now - k.sent_at > BUILD_S + LOST_GRACE_S:
            _lost(app, k, f"its build, sent {int(now - k.sent_at)} s ago, never answered (its task is lost): it is "
                          "sent again on the next ask")
        elif k.sent_to is not None and k.sent_to != eid:
            _lost(app, k, "the endpoint it was sent to is no longer this session's: it is sent again on the next ask")
    _promote(app)


def _promote(app: AppCtx) -> None:
    """The pending env (review S4: a built env asked for while another serves) becomes the session's once it is ready;
    one whose build failed is dropped and the session's env kept — either is said once (`AppCtx.env_note`)."""
    spec = app.env_pending
    if spec is None:
        return
    k = _known(app, spec)
    if k.future is not None or k.info.state not in ("ready", "failed"):
        return   # still out — or lost, and sent again on the next ask (`resume_pending`): it stays pending
    app.env_pending = None
    label = spec.name or k.key
    if k.info.state == "ready":
        app.env_spec = spec
        app.env_failed = None
        app.env_note = (f"The env {label} is built and is now the session's env: commands, batch rows and jobs run in "
                        "it from here on.")
    else:
        app.env_failed = info_for(k, spec.name)   # the answer that says it carries it too (review G1): `env_failed`
        app.env_note = (f"The env {label} was not built, so the session's env was not changed: "
                        f"{failed_words(k.info, k.fault)}")


async def _wait(fut: Any, wait_s: float) -> None:
    if fut is None or fut.done() or wait_s <= 0:
        return
    try:
        await asyncio.to_thread(fut.result, wait_s)
    except Exception:  # noqa: BLE001 - not done within the wait (or failed): `_absorb` reads which, later
        return


async def ensure_built(app: AppCtx, spec: EnvSpec, *, wait_s: float = WAIT_S, quiet: bool = False) -> _Known:
    """The built env `spec`, as known after at most `wait_s`: a cache hit with nothing sent while its full check is
    under VERIFY_EVERY_S old; else its build is sent (the script's own check first: an env a restarted server finds
    there is a cache hit, a partly purged one is rebuilt beside it), and waited for a while. A failed one is tried
    again. A ready env's re-check is quiet (it stays ready while it runs: review U5)."""
    k = _known(app, spec)
    refresh(app)
    if k.future is None:
        fresh = k.verified_at is not None and time.monotonic() - k.verified_at < VERIFY_EVERY_S
        if k.info.state == "ready" and fresh:
            k.info = k.info.model_copy(update={"cached": True})
            return k
        start_build(app, k, quiet=quiet or k.info.state == "ready")
    await _wait(k.future, wait_s)
    refresh(app)
    return k


async def recheck(app: AppCtx, *, since: float) -> None:
    """A block newly confirmed (ruling 5's third time): the session's built env has its full check again on the login
    node (free), whatever its age — quiet, so commands go on while it runs (review U5); a purge between one block and
    the next is then a rebuild beside it. Skipped when its check already ran in this call (`since`:
    time.monotonic())."""
    spec = app.env_spec
    if spec is None or spec.kind != "requirements" or app.state.endpoint_id is None:
        return
    k = _known(app, spec)
    if k.future is None and (k.verified_at is None or k.verified_at < since):
        k.verified_at = None
    await ensure_built(app, spec, quiet=True)


async def _check_activate(app: AppCtx, spec: EnvSpec, wait_s: float = WAIT_S) -> _Known:
    """An existing env's line, run once on the login node where there is one (free); on a facility endpoint none can be
    run before the work: each command checks it."""
    k = _known(app, spec)
    if access_of(app) != "ssh" or app.state.endpoint_id is None:
        return k
    refresh(app)
    if k.future is None:
        runner = None
        try:
            runner = _runner(app)
            fut = runner.submit(activate_check_script(str(spec.activate)))
        except Exception as exc:  # noqa: BLE001 - said: each command checks the line anyway
            drop_runner(app, only=runner)
            k.fault = "runner"
            k.info = k.info.model_copy(update={"state": "unchecked", "error": said(
                f"its check could not be sent to the login node: {type(exc).__name__}: {exc}")})
            return k
        k.future, k.kind, k.sent_at, k.sent_to = fut, "activate", time.monotonic(), str(app.state.endpoint_id)
        k.sent_runner = runner
    await _wait(k.future, wait_s)
    refresh(app)
    return k


def resume_pending(app: AppCtx) -> None:
    """A pending env whose build was lost, or whose future a dropped runner cancelled, is sent again — on the next ask,
    as the pending answer promised (review R2-S3): ensure_endpoint_up, env omitted included."""
    refresh(app)
    spec = app.env_pending
    if spec is None or app.state.endpoint_id is None:
        return
    k = _known(app, spec)
    if k.future is None and k.info.state == "unchecked":
        start_build(app, k)
        _promote(app)


def believe_refusal(app: AppCtx, k: _Known | None, why: str) -> None:
    """What a node's refusal of an env says, believed — run_shell's, a poll's, a batch row's and a job's alike (review
    R2-S2: one path): its full check is due again (gone, failed); a `.failed` mark there makes the env failed here, so
    the next ask — the user's yes for a batch, a resubmit — sends the build (the build itself clears the mark)."""
    if k is None or k.spec.kind == "activate" or why not in ("gone", "failed"):
        return
    k.verified_at = None
    if why == "failed" and (k.future is None or k.quiet):
        k.quiet = False
        k.fault = "mark"
        k.info = k.info.model_copy(update={"state": "failed", "error": "its build failed on the node: its .failed mark "
                                                                       "there says so"})


@dataclass
class Chosen:
    """ensure_endpoint_up's `env`, applied: what the answer states (the env asked for), whether it is the session's
    now (False: pending, or not taken), and its words."""

    info: EnvInfo | None
    notice: str = ""
    taken: bool = True


def failed_words(info: EnvInfo | None, fault: str | None) -> str:
    """Why an env is not there, in the words of what failed (review U9: a runner's exception is not the installer)."""
    err = said(info.error if info is not None else None) or "it said nothing"
    if fault == "installer":
        return f"its build failed on the login node (the installer said, quoted as data: {err!r})."
    if fault == "lost":
        return f"its build never answered ({err})."
    if fault == "mark":
        return "its build failed on the node: its .failed mark there says so."
    return f"it could not be built on the login node (quoted as data: {err!r})."


INSTALL_SAID = "Installing runs each package's build code as you, on your allocation's facility."


def built_notice(app: AppCtx, info: EnvInfo) -> str:
    """What a built env's answer says — the session's, a batch's, a job's or a handoff's alike (review S14): the user's
    own index lines (credentials redacted), the login node's own installer settings that applied (review U2), that
    installing runs build code (once a session: marked said only by the answer that carries it, `mark_said` — review
    R2-S10), and its root's warning."""
    out = ""
    if info.index_lines:
        out += f" Its packages come from your own index lines: {'; '.join(info.index_lines)}."
    if info.ambient:
        out += (f" The login node's own installer settings applied to this build: {', '.join(info.ambient)} — "
                "hpc-bridge names them and never changes them.")
    if not app.env_said_install:
        out += f" {INSTALL_SAID}"
    if info.warning:
        out += f" Note: {info.warning}."
    return out


def mark_said(app: AppCtx, notice: str | None) -> None:
    """The install warning is said once a session: marked when an answer that carries it is returned (review R2-S10 —
    never when it is composed for an answer that drops it)."""
    if INSTALL_SAID in (notice or ""):
        app.env_said_install = True


def _words(info: EnvInfo) -> str:
    where = f" at {info.path}" if info.path else f" under {info.root}" if info.root else ""
    name = info.name or info.key
    if info.state == "ready":
        return (f"The session's env {name} is ready{where}"
                + (" (already built: nothing was rebuilt)" if info.cached else " (built on the login node, free)")
                + ": every command, batch row and job runs in it; switching env needs no new block.")
    if info.state == "building":
        return (f"The session's env {name} is being rebuilt on the login node (free){where}: a command sent meanwhile "
                "runs nothing and answers env_building; ensure_endpoint_up shows its state.")
    return ""


async def choose(app: AppCtx, spec: EnvSpec | None) -> Chosen:
    """ensure_endpoint_up's `env` (parsed: `parse`): the session's env from now on, for its commands on both shapes —
    never hpc-bridge's internal session. `{}` drops it (and any pending one). A built env still being built is PENDING
    (review S4): the session's own env (or none) serves meanwhile, and the new one takes over once it is ready — a
    failed build leaves the session's env as it was, saying why (ruling 9), and drops any older pending one (review
    R2-S5). Of two calls at once the LATER wins: one that waited on its build while a later choose ran applies nothing
    (review R2-S6). Raises EnvRefusal (nothing set)."""
    if spec is None:
        return Chosen(session_info(app))
    app.env_seq += 1
    mine = app.env_seq
    if spec.kind == "none":
        app.env_spec = app.env_pending = None
        return Chosen(None, "No env: commands run in the session's own shell as it stands.")
    delivery(spec, access=access_of(app), client_python=default_python())
    if spec.kind == "activate":
        k = await _check_activate(app, spec)
        info = info_for(k, spec.name)
        if app.env_seq != mine:
            return _superseded(info)
        app.env_spec, app.env_pending = spec, None
        return Chosen(info, f"The session's env is the existing one{f' {info.name}' if info.name else ''}: "
                            f"{_activate_words(app, k, info)} No new block.")
    k = await ensure_built(app, spec)
    info = info_for(k, spec.name)
    if app.env_seq != mine:
        return _superseded(info)
    cur = app.env_spec
    same = cur is not None and cur.kind == "requirements" and _known(app, cur) is k
    if info.state == "failed":
        dropped = ""
        if app.env_pending is not None and _known(app, app.env_pending) is not k:
            older = app.env_pending
            dropped = (f" The env {older.name or env_key(older)} that was still being built is dropped too: this ask "
                       "superseded it.")
        app.env_pending = None
        return Chosen(info, f"The session's env was not changed: env {info.key} {failed_words(info, k.fault)}"
                            + dropped, taken=same)
    if info.state == "ready":
        app.env_spec, app.env_pending = spec, None
        return Chosen(info, _words(info) + built_notice(app, info))
    if same:   # the session's own env, being rebuilt: nothing else to run in meanwhile
        app.env_pending = None
        return Chosen(info, _words(info) + built_notice(app, info))
    app.env_pending = spec
    serving = (f"the session's env {cur.name or env_key(cur)} serves meanwhile" if cur is not None
               else "commands run with no env meanwhile")
    where = f" under {info.root}" if info.root else ""
    return Chosen(info, (f"The env {info.name or info.key} is being built on the login node (free){where}: "
                         f"{serving}, and it becomes the session's env once it is ready (ensure_endpoint_up says "
                         "when).") + built_notice(app, info), taken=False)


def _activate_words(app: AppCtx, k: _Known, info: EnvInfo) -> str:
    """What choose() says of an existing env, from its check on the login node (if any)."""
    if info.state == "ready":
        return "it activates on the login node; each command, batch row and job runs it first."
    if k.future is not None:   # its check is queued behind a build on the build worker (review S13)
        return ("its check on the login node is still queued behind a build: it is kept, and each command checks it as "
                "it runs (nothing runs where its line does not work).")
    if access_of(app) == "ssh" and info.error and k.fault == "runner":   # never the line's fault (R2-S4)
        return (f"it could not be checked on the login node (quoted as data: {said(info.error)!r}): it is kept, and "
                "each command checks it as it runs (nothing runs where its line does not work).")
    if access_of(app) == "ssh" and info.error and k.fault == "nomodule":   # hpc-bridge's own words (review round 1)
        return (f"its line did not work on the login node — {info.error}. Compute nodes may differ, so it is kept and "
                "checked again with each command, which runs nothing if it fails there.")
    if access_of(app) == "ssh" and info.error:
        return (f"its line did not work on the login node, saying (quoted as data): {said(info.error)!r} — login and "
                "compute nodes may load different modules, so it is kept and checked again with each command, which "
                "runs nothing if it fails there.")
    return "it is checked with each command (a facility endpoint has no login node to try it on first)."


def _superseded(info: EnvInfo) -> Chosen:
    return Chosen(info, "A later call changed the session's env while this one waited on its env: this call's env was "
                        "not applied (the later call's stands).", taken=False)


def session_info(app: AppCtx) -> EnvInfo | None:
    """The session's env as it stands (a build that finished meanwhile read in, a pending one promoted)."""
    refresh(app)
    spec = app.env_spec
    if spec is None:
        return None
    return info_for(_known(app, spec), spec.name)


def pending_info(app: AppCtx) -> EnvInfo | None:
    """The env asked for that is still being built (review S4), or None."""
    spec = app.env_pending
    return info_for(_known(app, spec), spec.name) if spec is not None else None


def take_note(app: AppCtx) -> str:
    """What became of a pending env since the last answer (said once)."""
    refresh(app)
    note, app.env_note = app.env_note, None
    return note or ""


def take_failed(app: AppCtx) -> EnvInfo | None:
    """A pending env whose build failed since the last answer, as a structured field (`env_failed`; review G1) — given
    once, with the note that says it (`take_note`, called first)."""
    failed, app.env_failed = app.env_failed, None
    return failed


@dataclass
class ForCommand:
    """What a user command gets: the activation (None: no env), or why it cannot run now."""

    act: Activation | None = None
    info: EnvInfo | None = None
    building: bool = False
    failed: str | None = None


def for_command(app: AppCtx) -> ForCommand:
    refresh(app)
    spec = app.env_spec
    if spec is None:
        return ForCommand()
    k = _known(app, spec)
    info = info_for(k, spec.name)
    if spec.kind == "activate":
        return ForCommand(Activation("activate", k.key, line=spec.activate), info)
    if k.info.state == "building":   # its own rebuild (a quiet re-check keeps it ready, and commands go on)
        return ForCommand(info=info, building=True)
    if k.info.state == "failed":
        return ForCommand(info=info, failed=failed_words(k.info, k.fault))
    return ForCommand(Activation("built", k.key, root=k.root, arch=k.info.arch), info)


def building_words(info: EnvInfo | None) -> str:
    key = (info.name or info.key) if info is not None else "?"
    return (f"Nothing ran: the session's env {key} is being built on the login node (free). Send the command again "
            "once ensure_endpoint_up says it is ready.")


def _known_built(app: AppCtx, key: str, root: str | None) -> _Known | None:
    return next((x for x in app.env_known.values() if x.key == key and x.root == root and x.spec.kind != "activate"),
                None)


def refused(app: AppCtx, ref: Refused, act: Activation, stderr: str) -> tuple[Literal["env_building", "failed"], str]:
    """A command its env refused: (the phase, the notice). A built env gone from its root (a scratch purge) is rebuilt
    on the login node — `env_building`; anything a rebuild cannot mend now (built only for another architecture, its
    build failed, a directory not the user's alone, the user's line failed) is `failed`, saying why. What the node said
    clears what this server believed — the one path batch rows and jobs take too (`believe_refusal`: reviews S6,
    R2-S2): a `.failed` mark makes the next ask send the build. A command that ran in an env the session has since
    left is said about THAT env, and that a resend runs in the current one (review SS4). Never runs the command again
    by itself."""
    k = _known_built(app, act.key, act.root) if act.how == "built" else None
    believe_refusal(app, k, ref.why)
    cur = app.env_spec
    left = cur is None or env_key(cur) != act.key
    since = (" The session's env has changed since this command was sent: sending it again runs it in the current "
             "one." if left else "")
    if ref.why == "gone" and act.how == "built":
        if k is not None and not left:
            refresh(app)
            if k.future is None or k.quiet:
                k.quiet = False
                if k.future is None:
                    start_build(app, k)
                else:
                    k.info = k.info.model_copy(update={"state": "building"})
        if left:
            return "failed", (f"Nothing ran: the env {act.key} this command was sent with was not found under "
                              f"{act.root} on this node — purged from scratch, or removed.{since}")
        return "env_building", (f"Nothing ran: the session's env {act.key} was not found under {act.root} on this node "
                                "— purged from scratch, or removed. It is being rebuilt on the login node (free); send "
                                "the command again once ensure_endpoint_up says it is ready.")
    if ref.why == "arch":
        here, *built = ref.detail.split() or ["?"]
        return "failed", (f"Nothing ran: the env {act.key} was built for {', '.join(built) or 'another '}"
                          f"architecture; this node is {here}. Building an env on a node of another architecture than "
                          "the login node's needs per-architecture endpoint venvs (a follow-up, not built): use an "
                          'existing env here with env={"activate": "<shell line>"}.' + since)
    if ref.why == "failed":
        return "failed", (f"Nothing ran: the env {act.key} failed to build here (its mark on the node says so). Ask "
                          "for it again with ensure_endpoint_up(env=…) to build it again, or choose another." + since)
    if ref.why == "unsafe":
        return "failed", (f"Nothing ran: the env {act.key} is in a directory that is not yours alone "
                          f"({said(ref.detail)!r}: owned by another user, or writable by its group or anyone — a "
                          "default ACL, or an NFSv4 id-mapping to nobody, can make a directory you created read so). "
                          "hpc-bridge never runs in an env someone else could change: remove it, or choose a root you "
                          "own alone with ensure_endpoint_up(env={…, \"root\": …})." + since)
    words = "\n".join(ln for ln in (stderr or "").splitlines() if REFUSED not in ln)
    quoted = said(words, 600)
    detail = " ".join(w for w in ref.detail.split() if w != NOMODULE)
    nomodule = module_words(ref.detail, act.line)
    return "failed", (f"Nothing ran: the env could not be activated on this node — its line exited "
                      f"{detail.removeprefix('rc=')}" + (f", saying (quoted as data): {quoted!r}" if quoted else "")
                      + (f" — {nomodule}" if nomodule else "") + "." + since)


# ---- a batch's or a job's env, on the ledger ------------------------------------------------------------------------

def record(app: AppCtx, spec: EnvSpec) -> dict[str, Any]:
    """The env a batch or a job runs in, as the ledger records it: kind, how, key, its owner's name, Python, path — and
    what activates or rebuilds it after a restart: the line; the requirements (credentials redacted: a record that
    needed them is `redacted`, rebuilt only by the server that still holds the text), the Python the key was made with
    (never the default of a later client: review S3), the root, the root's warning (review S14)."""
    k = _known(app, spec)
    info = k.info
    rec: dict[str, Any] = {"kind": info.kind, "how": info.how, "key": k.key, "name": spec.name}
    if spec.kind == "activate":
        rec["line"] = spec.activate
    else:
        text = spec.requirements or ""
        shown = redact(text)
        rec.update({"python": info.python, "python_asked": spec.python or default_python(), "root": k.root,
                    "path": info.path, "arch": info.arch, "lock_sha256": info.lock_sha256, "requirements": shown,
                    "warning": info.warning, "redacted": True if shown != text else None})
    return {k_: v for k_, v in rec.items() if v is not None}


def _spec_of(rec: dict[str, Any]) -> EnvSpec:
    if rec.get("kind") == "activate":
        return EnvSpec(kind="activate", activate=str(rec.get("line") or ""), name=rec.get("name"))
    return EnvSpec(kind="requirements", requirements=str(rec.get("requirements") or ""), python=rec.get("python_asked"),
                   root=rec.get("root"), name=rec.get("name"))


def _known_of(app: AppCtx, rec: dict[str, Any]) -> _Known | None:
    """What this server knows of a record's env: the env it holds by key and root (its full text, a redacted record's
    too), else one made from the record — only when the record's own text makes the record's key (review S3: never
    another env under its key). None: it cannot be built from here."""
    if rec.get("kind") == "activate":
        return _known(app, _spec_of(rec))
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    if k is not None:
        return k
    if rec.get("redacted"):
        return None
    spec = _spec_of(rec)
    if not spec.requirements or env_key(spec) != rec.get("key"):
        return None
    return _known(app, spec)


def activation_of(rec: dict[str, Any]) -> Activation:
    if rec.get("kind") == "activate":
        return Activation("activate", str(rec["key"]), line=str(rec.get("line") or ""))
    return Activation("built", str(rec["key"]), root=str(rec.get("root") or ""), arch=rec.get("arch"))


def info_from_record(rec: dict[str, Any] | None) -> EnvInfo | None:
    """The env as its record says (a job's, a batch's), without asking anything."""
    if not rec:
        return None
    state: Literal["ready", "failed", "unchecked"] = (
        "failed" if rec.get("unbuildable") else "ready" if rec.get("path") or rec.get("kind") == "activate"
        else "unchecked")
    try:
        return EnvInfo(kind=rec.get("kind", "built"), how=rec.get("how", "login"), key=str(rec["key"]),
                       name=rec.get("name"), python=rec.get("python"), path=rec.get("path"), root=rec.get("root"),
                       arch=rec.get("arch"), lock_sha256=rec.get("lock_sha256"), warning=rec.get("warning"),
                       state=state, error=rec.get("unbuildable"), index_lines=index_lines(rec.get("requirements")))
    except (KeyError, ValueError):
        return None


def info_of_record(app: AppCtx | None, rec: dict[str, Any] | None) -> EnvInfo | None:
    """The env as its record says, its state as this server knows it — building while it is rebuilt, failed when its
    rebuild failed (review S11): a status never says `ready` for an env that is not."""
    info = info_from_record(rec)
    if info is None or app is None or rec is None or rec.get("kind") == "activate" or rec.get("unbuildable"):
        return info
    refresh(app)
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    if k is None:
        return info
    upd: dict[str, Any] = {"state": k.info.state, "error": k.info.error, "task_id": k.info.task_id}
    if k.info.state == "ready":
        upd.update({n: getattr(k.info, n) for n in ("path", "python", "arch", "lock_sha256")
                    if getattr(k.info, n) is not None})
    return info.model_copy(update=upd)


def record_state(app: AppCtx, rec: dict[str, Any]) -> str:
    """ready | building | failed, for admission: an existing env is always ready (each row checks it); a built one by
    what this server knows (one it does not know — a restart — is tried, and a row that finds it gone rebuilds it)."""
    if rec.get("kind") == "activate":
        return "ready"
    refresh(app)
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    if k is None:
        return "ready"
    if k.info.state == "building" or (k.future is not None and not k.quiet):
        return "building"
    return "failed" if k.info.state == "failed" else "ready"


def record_error(app: AppCtx, rec: dict[str, Any]) -> str:
    """Why a record's env is not there, in the words of what failed."""
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    return failed_words(k.info if k is not None else None, k.fault if k is not None else None)


def rebuild_record(app: AppCtx, rec: dict[str, Any]) -> bool:
    """A batch row found its built env gone, or the user's yes retries a failed one: rebuild it on the login node
    (free), once. False: it cannot be rebuilt from here (its text is not this server's to send: a redacted record after
    a restart)."""
    if rec.get("kind") == "activate":
        return True
    k = _known_of(app, rec)
    if k is None:
        return False
    refresh(app)
    if k.future is None:
        start_build(app, k)
    elif k.quiet:
        k.quiet = False
        k.info = k.info.model_copy(update={"state": "building"})
    return True


def current_record(app: AppCtx, rec: dict[str, Any]) -> dict[str, Any]:
    """`rec` with what has been learnt since it was recorded (its path, its Python, its lock, its warning, once built)
    — and, the env ready again, without `retried`: the user's one retry is spent only on the failure it was asked for
    (review R2-S7)."""
    if rec.get("kind") == "activate":
        return rec
    refresh(app)
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    if k is None or k.info.state != "ready":
        return rec
    info = k.info
    out = {**rec, **{n: v for n, v in (("path", info.path), ("python", info.python), ("arch", info.arch),
                                      ("lock_sha256", info.lock_sha256), ("warning", info.warning))
                     if v is not None}}
    out.pop("retried", None)
    return out


def nonce_for(rec: dict[str, Any], *parts: Any) -> str:
    """The activation nonce of one dispatch of a ledger record's env — a batch row's attempt, a job: made from the
    record's own secret (`nonce_key`, never sent anywhere), so this server, or one restarted on the same ledger,
    believes a refusal mark only from that dispatch's activation (reviews N12, SS2) and keeps no table of them."""
    seed = "\0".join([str(rec.get("nonce_key") or rec.get("key")), *(str(p) for p in parts)])
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def ready_for_billed(k: _Known) -> bool:
    """A job's or a handoff's billed work goes only against an env known ready here (review SS6): never one that is
    unchecked — a build lost, a future its runner's close cancelled — taken as built."""
    return k.spec.kind == "activate" or (k.info.state == "ready" and (k.future is None or k.quiet))


def label(rec: dict[str, Any] | None) -> str | None:
    """A batch's or a job's env in a few words (the ledger's listing)."""
    if not rec:
        return None
    name = f" {rec['name']}" if rec.get("name") else ""
    if rec.get("kind") == "activate":
        return f"existing env{name} ({rec.get('key')})"
    py = f", python {rec['python']}" if rec.get("python") else ""
    return f"built env{name} {rec.get('key')}{py}"
