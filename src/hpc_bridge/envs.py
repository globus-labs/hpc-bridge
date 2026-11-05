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
    return [redact(ln) for ln in (text or "").split("\n") if re.split(r"[\s=]", ln, maxsplit=1)[0] in _INDEX_OPTIONS]


def _remote(url: str) -> bool:
    """A URL the key's hash covers by name: a scheme other than file (http(s), git+https…), never a local path."""
    m = _SCHEME.match(url)
    return m is not None and m.group(1).lower() != "file" and not m.group(1).lower().endswith("+file")


_LOCAL = "a local path, which the env's key (the hash of this text) cannot see change: publish it to an index"


def _line_problem(line: str) -> str | None:
    if "$" in line:   # uv expands ${VAR} in a requirements file, on the node: a value the key cannot see (review N9)
        return "it names an environment variable, which the installer expands on the node: the env's key cannot see it"
    if line.startswith("-"):
        parts = re.split(r"[\s=]+", line, maxsplit=1)
        opt, val = parts[0], (parts[1].strip() if len(parts) > 1 else "")
        if opt in ("-r", "--requirement", "-c", "--constraint"):
            return ("it names another file, which the env's key (the hash of this text) cannot see change: paste "
                    "that file's lines here instead")
        if opt in ("-e", "--editable"):
            return "an editable install is a local path, which the env's key cannot see change"
        if opt == "--trusted-host":
            return "it turns off the index's TLS check"
        if opt not in _ALLOWED_OPTIONS:
            return ("an option hpc-bridge does not pass to the installer (a requirements text may say where packages "
                    f"come from and which forms they take: {', '.join(sorted(_ALLOWED_OPTIONS))})")
        if opt in _INDEX_OPTIONS and not _remote(val):
            return f"{opt} takes an index's URL here (https://…), never a local path: {_LOCAL}"
        return None
    if "@" in line:   # a named requirement on a URL (`pkg @ https://…`): a URL with a scheme, never a file (N9)
        target = line.split("@", 1)[1].strip()
        if not _remote(target):
            return _LOCAL
    elif "://" in line:
        if not _remote(line):
            return _LOCAL
    elif "/" in line or "\\" in line or line.startswith((".", "~")):
        return _LOCAL
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
# stderr; the batch wrapper's mark when it set one (`__hb_env_mark`: the row ran nothing — the mark carries the nonce,
# and only a mark with its own nonce is believed: review N12); nothing of the command run.
_REFUSE = (
    "__hb_erefuse() {\n"
    '  __hb_el="@@REFUSED@@ @@NONCE@@ $__hb_ew $__hb_ek $__hb_ex"\n'
    "  printf '%s\\n' \"$__hb_el\" >&2\n"
    '  [ -z "${__hb_env_mark:-}" ] || printf \'%s\\n\' "$__hb_el" >| "$__hb_env_mark" 2>/dev/null\n'
    "  exit @@RC@@\n"
    "}\n"
)
# Around every activation: the shell's errexit, nounset, noclobber, pipefail and cwd as they were before it (a setup
# script's `set -euo pipefail`, a .bashrc's noclobber, an activate line's `cd`: review N5), and its output on stderr — a
# site banner never lands in a row's results (review N8).
_HEAD = "__hb_eo=$-; __hb_ewd=$PWD; __hb_epf=0; shopt -qo pipefail 2>/dev/null && __hb_epf=1\n"
_TAIL = (
    'case "$__hb_eo" in *e*) set -e;; *) set +e;; esac\n'
    'case "$__hb_eo" in *u*) set -u;; *) set +u;; esac\n'
    'case "$__hb_eo" in *C*) set -C;; *) set +C;; esac\n'
    'if [ "$__hb_epf" = 1 ]; then set -o pipefail; else set +o pipefail; fi\n'
    '[ "$PWD" = "$__hb_ewd" ] || cd -- "$__hb_ewd" 2>/dev/null || :\n'
)
# The root and the env must be this user's alone (review U1: keys are predictable, so another user could plant an env
# where ours would be found): owned by the user, writable by neither its group nor anyone — the slot lock dir's rule.
_UNSAFE = ('find -H @@PATHS@@ -maxdepth 0 \\( ! -user "${EUID:-$(id -u)}" -o -perm -0002 -o -perm -0020 \\) -print '
           "2>/dev/null")

_ACTIVATE_BUILT = (
    "__hb_ek=@@KEY@@; __hb_ea=$(uname -m 2>/dev/null); __hb_er=@@ROOT@@; __hb_eb=@@BUILTFOR@@\n"
    '__hb_ed="$__hb_er/$__hb_ek-$__hb_ea"\n'
    '__hb_eu=$(@@UNSAFE@@)\n'
    'if [ -z "$__hb_eu" ] && grep -qx "short=$__hb_ek" "$__hb_ed/@@MARKER@@" 2>/dev/null; then\n'
    '  unset PYTHONHOME; VIRTUAL_ENV="$__hb_ed"; PATH="$__hb_ed/bin:$PATH"; export VIRTUAL_ENV PATH\n'
    "else\n"
    '  __hb_ew=gone; __hb_ex="$__hb_ea"\n'
    '  if [ -n "$__hb_eu" ]; then __hb_ew=unsafe; __hb_ex=$(printf \'%s\\n\' "$__hb_eu" | head -n 1)\n'
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
    "fi\n"
)

# The user's line, run in an `if` (its errexit cannot end the shell) with an EXIT trap armed while it runs: a line that
# ends the shell anyway (`exit`, nounset, a `${X:?}` guard) is a refusal too, never a command that ran (review N1). The
# trap the shell had before is restored after it.
_ACTIVATE_LINE = (
    "__hb_ek=@@KEY@@\n"
    "__hb_etrap=$(trap -p EXIT)\n"
    "trap '__hb_erc=$?; __hb_ew=activate; __hb_ex=\"rc=$__hb_erc (the line ended the shell)\"; __hb_erefuse' EXIT\n"
    'if eval "$(printf %s @@B64@@ | base64 --decode)"; then __hb_erc=0; else __hb_erc=$?; fi\n'
    "trap - EXIT\n"
    '[ -z "$__hb_etrap" ] || eval "$__hb_etrap"\n'
    'if [ "$__hb_erc" -ne 0 ]; then __hb_ew=activate; __hb_ex="rc=$__hb_erc"; __hb_erefuse; fi\n'
)


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
    refuse = _REFUSE.replace("@@REFUSED@@", REFUSED).replace("@@NONCE@@", nonce).replace("@@RC@@", str(ENV_RC))
    if act.how == "activate":
        b64 = shlex.quote(base64.b64encode(str(act.line or "").encode()).decode())
        body = _ACTIVATE_LINE.replace("@@KEY@@", act.key).replace("@@B64@@", b64)
    else:
        arch = act.arch if act.arch and re.fullmatch(r"[A-Za-z0-9_.-]{1,32}", act.arch) else ""
        body = (_ACTIVATE_BUILT.replace("@@UNSAFE@@", _UNSAFE.replace("@@PATHS@@", '"$__hb_er" "$__hb_ed"'))
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
__hb_check() {
  [ -d "$1" ] || { echo gone; return 0; }
  __hb_x=$(__hb_unsafe "$__hb_r" "$1")
  [ -z "$__hb_x" ] || { echo "unsafe $__hb_x"; return 0; }
  grep -qx "short=$__hb_k" "$1/@@MARKER@@" 2>/dev/null || { echo "partial 0"; return 0; }
  __hb_n=0
  for __hb_rec in "$1"/lib/python*/site-packages/*.dist-info/RECORD; do
    [ -f "$__hb_rec" ] || continue
    __hb_sp=${__hb_rec%/*}; __hb_sp=${__hb_sp%/*}
    while IFS=, read -r __hb_p __hb_q; do
      [ -z "$__hb_p" ] || [ -e "$__hb_sp/$__hb_p" ] || __hb_n=$((__hb_n + 1))
    done < "$__hb_rec"
  done
  if [ "$__hb_n" -eq 0 ] && [ "$(__hb_dists "$1")" = "$(sed -n 's/^dists=//p' "$1/@@MARKER@@")" ]; then echo ready
  else echo "partial $__hb_n"; fi
}
""".replace("@@UNSAFE@@", _UNSAFE.replace("@@PATHS@@", '"$@"'))

_BUILD = r"""# @@TAG@@ @@KEY@@ — hpc-bridge builds a user env (hpc_bridge.envs): beside its path, then one rename
# what it creates is never writable by the group or anyone (a node whose umask is 002 would make the root so, and the
# root and the env must be the user's alone: review U1)
umask 022
__hb_k=@@KEY@@; __hb_r=@@ROOT@@; __hb_t=@@TOKEN@@
__hb_a=$(uname -m 2>/dev/null)
__hb_d="$__hb_r/$__hb_k-$__hb_a"
__hb_b=""
__hb_log="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t.log"
@@FNS@@# the login node's own installer settings (review U2): the NAMES of those set, never their values, and the config
# files that exist — said with the answer, since they apply to this build
__hb_amb=$(env | sed -nE @@AMBIENT@@ | LC_ALL=C sort -u | tr '\n' ',')
__hb_cfg=""
__hb_xdg="${XDG_CONFIG_HOME:-$HOME/.config}"
for __hb_f in "$__hb_xdg/uv/uv.toml" /etc/uv/uv.toml "$__hb_xdg/pip/pip.conf" "$HOME/.pip/pip.conf" /etc/pip.conf; do
  [ -f "$__hb_f" ] || continue
  case ",$__hb_cfg" in *",$__hb_f,"*) ;; *) __hb_cfg="$__hb_cfg$__hb_f,";; esac
done
__hb_fail() {
  __hb_m=$(printf '%s\n' "$1" | tail -c 2000)
  [ -z "$__hb_b" ] || __hb_rm "$__hb_b"
  rm -f "$__hb_log"
  [ -n "${2:-}" ] || printf '%s\n' "$__hb_m" >| "$__hb_d.failed" 2>/dev/null
  printf 'HPCB_ENV_FAILED key=%s arch=%s ambient=%s config=%s error_b64=%s\n' "$__hb_k" "$__hb_a" "${__hb_amb%,}" \
    "${__hb_cfg%,}" "$(printf '%s' "$__hb_m" | base64 | tr -d '\n')"
  exit 0
}
__hb_ready() {
  printf 'HPCB_ENV_READY key=%s dir=%s arch=%s python=%s lock=%s size_mb=%s free_mb=%s cached=%s ambient=%s '\
'config=%s\n' "$__hb_k" "$__hb_d" "$__hb_a" "$(sed -n 's/^python=//p' "$__hb_d/@@MARKER@@")" \
    "$(sed -n 's/^lock=//p' "$__hb_d/@@MARKER@@")" "$(du -sm "$__hb_d" 2>/dev/null | cut -f1)" \
    "$(df -Pk "$__hb_r" 2>/dev/null | awk 'NR==2{print int($4/1024)}')" "$1" "${__hb_amb%,}" "${__hb_cfg%,}"
}
mkdir -p "$__hb_r" 2>/dev/null || __hb_fail "cannot create the env root $__hb_r" nomark
__hb_x=$(__hb_unsafe "$__hb_r")
[ -z "$__hb_x" ] || __hb_fail "the env root $__hb_x is not yours alone (owned by another user, or writable by its \
group or anyone): hpc-bridge builds envs only where no one else can change them — pass root=… on a directory of your \
own" nomark
# in the root, so a project's uv.toml or pyproject.toml beside the worker's own cwd never applies to the build
cd "$__hb_r" 2>/dev/null || :
# where the node has flock, a second build of the same env waits for the first, then finds it built
if command -v flock >/dev/null 2>&1; then
  { exec 9>"$__hb_r/$__hb_k-$__hb_a.lock" && flock -w @@CAP@@ 9; } 2>/dev/null || true
fi
__hb_s=$(__hb_check "$__hb_d")
case "$__hb_s" in
  ready) __hb_ready 1; exit 0;;
  unsafe*) __hb_fail "the env ${__hb_s#unsafe } is not yours alone (owned by another user, or writable by its group or \
anyone): hpc-bridge never uses or replaces it — remove it, or pass root=… on a directory of your own" nomark;;
esac
rm -f "$__hb_d.failed"
# what builds cut off and rebuilds moved aside left beside it (review N11), once older than any build can run
find "$__hb_r" -maxdepth 1 \( -name ".$__hb_k-$__hb_a.build.*" -o -name ".$__hb_k-$__hb_a.broken.*" \) \
  -mmin +@@SWEEP@@ 2>/dev/null | while IFS= read -r __hb_x; do __hb_rm "$__hb_x"; done
export PATH="$HOME/.local/bin:$PATH" UV_CACHE_DIR="$__hb_r/.uv-cache"
command -v uv >/dev/null 2>&1 || { curl -LsSf https://astral.sh/uv/install.sh \
  | env UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1; } || true
command -v uv >/dev/null 2>&1 || __hb_fail "no uv on the login node, and astral.sh's installer could not install one"
__hb_u=$(command -v uv)
__hb_b="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t"
"$__hb_u" venv -q --relocatable --python @@PY@@ "$__hb_b" > "$__hb_log" 2>&1 || __hb_fail "$(tail -n 20 "$__hb_log")"
printf %s @@B64@@ | base64 --decode > "$__hb_b/requirements.in" || __hb_fail "cannot write $__hb_b/requirements.in"
"$__hb_u" pip install -q --compile-bytecode --link-mode=copy --python "$__hb_b/bin/python" \
  -r "$__hb_b/requirements.in" > "$__hb_log" 2>&1 || __hb_fail "$(tail -n 20 "$__hb_log")"
"$__hb_u" pip freeze --python "$__hb_b/bin/python" > "$__hb_b/requirements.lock" 2> "$__hb_log" \
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

# The installer settings a login node may set itself (review U2), matched by NAME: `sed` keeps a name, never a value.
_AMBIENT = shlex.quote("s/^((UV_[A-Z_]*INDEX[A-Z_]*)|UV_INSECURE_HOST|UV_CONFIG_FILE|PIP_INDEX_URL|PIP_EXTRA_INDEX_URL|"
                       "PIP_TRUSTED_HOST|PIP_CONFIG_FILE)=.*/\\1/p")

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
            .replace("@@SWEEP@@", str(SWEEP_MIN)).replace("@@AMBIENT@@", _AMBIENT).replace("@@MARKER@@", MARKER))


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
    """An existing env's line, run once on the login node in a subshell (nothing of it stays), printing
    `HPCB_ENV_ACTIVATE rc=<n> out_b64=<the end of what it said>`."""
    b64 = shlex.quote(base64.b64encode(line.encode()).decode())
    return (f'__hb_o=$( ( eval "$(printf %s {b64} | base64 --decode)" ) 2>&1 ); __hb_rc=$?; '
            "printf 'HPCB_ENV_ACTIVATE rc=%s out_b64=%s\\n' \"$__hb_rc\" "
            "\"$(printf '%s' \"$__hb_o\" | tail -c 2000 | base64 | tr -d '\\n')\"")


def canary_check(rec: dict[str, Any]) -> str:
    """A handoff canary's look at its env from inside the block: `HPCB_ENV_OK 1` when the built env's marker is there
    for this node, in a directory the user owns, or the existing env's line runs (in a subshell); else
    `HPCB_ENV_OK 0`."""
    act = activation_of(rec)
    if act.how == "activate":
        b64 = shlex.quote(base64.b64encode(str(act.line or "").encode()).decode())
        test = f'( eval "$(printf %s {b64} | base64 --decode)" ) >/dev/null 2>&1'
    else:
        d = f'{_qroot(str(act.root))}/{act.key}-"$(uname -m 2>/dev/null)"'
        test = f'[ -O {d} ] && grep -qx "short={act.key}" {d}/{MARKER} 2>/dev/null'
    return f'{test} && echo "HPCB_ENV_OK 1" || echo "HPCB_ENV_OK 0"'


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
    (`fault`: installer | runner | lost — the words differ: review U9). Its `info` carries no name: a name is the
    owner's (the session's, a batch's), never the shared env's (review U8)."""

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


def drop_runner(app: AppCtx) -> None:
    """Close and forget the build runner (its Executor shut down by the SDK, a new Globus login, an endpoint repaired,
    adopted or changed: review S7): the next build gets a fresh one."""
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
    try:
        fut = _runner(app).submit(script)
    except Exception as exc:  # noqa: BLE001 - the Executor shut down, no endpoint: said, never raised out of a tool
        drop_runner(app)
        k.future = None
        _fail(k, f"its build could not be sent to the login node: {type(exc).__name__}: {exc}", "runner")
        return
    k.future, k.kind, k.sent_at, k.sent_to = fut, "build", time.monotonic(), str(app.state.endpoint_id)
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
        drop_runner(app)
        _fail(k, f"it could not be run on the login node: {type(exc).__name__}: {exc}", "runner")
        return
    res = fut.result()
    out = str(getattr(res, "stdout", "") or "")
    if k.kind == "activate":
        m = re.search(r"^HPCB_ENV_ACTIVATE rc=(\d+) out_b64=(\S*)\s*$", out, re.M)
        words = _b64text(m.group(2)).strip() if m else out.strip()[-300:]
        ok = m is not None and m.group(1) == "0"
        err = None if ok else (said(words) or "its line failed and said nothing")
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
        return   # still out — or lost, and sent again on the next ask: it stays pending
    app.env_pending = None
    label = spec.name or k.key
    if k.info.state == "ready":
        app.env_spec = spec
        app.env_note = (f"The env {label} is built and is now the session's env: commands, batch rows and jobs run in "
                        "it from here on.")
    else:
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
        try:
            fut = _runner(app).submit(activate_check_script(str(spec.activate)))
        except Exception as exc:  # noqa: BLE001 - said: each command checks the line anyway
            drop_runner(app)
            k.fault = "runner"
            k.info = k.info.model_copy(update={"state": "unchecked", "error": said(
                f"its check could not be sent to the login node: {type(exc).__name__}: {exc}")})
            return k
        k.future, k.kind, k.sent_at, k.sent_to = fut, "activate", time.monotonic(), str(app.state.endpoint_id)
    await _wait(k.future, wait_s)
    refresh(app)
    return k


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
    return f"it could not be built on the login node (quoted as data: {err!r})."


def built_notice(app: AppCtx, info: EnvInfo) -> str:
    """What a built env's answer says — the session's, a batch's, a job's or a handoff's alike (review S14): the user's
    own index lines (credentials redacted), the login node's own installer settings that applied (review U2), that
    installing runs build code (once a session), and its root's warning."""
    out = ""
    if info.index_lines:
        out += f" Its packages come from your own index lines: {'; '.join(info.index_lines)}."
    if info.ambient:
        out += (f" The login node's own installer settings applied to this build: {', '.join(info.ambient)} — "
                "hpc-bridge names them and never changes them.")
    if not app.env_said_install:
        app.env_said_install = True
        out += " Installing runs each package's build code as you, on your allocation's facility."
    if info.warning:
        out += f" Note: {info.warning}."
    return out


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
    failed build leaves the session's env as it was, saying why (ruling 9). Raises EnvRefusal (nothing set)."""
    if spec is None:
        return Chosen(session_info(app))
    if spec.kind == "none":
        app.env_spec = app.env_pending = None
        return Chosen(None, "No env: commands run in the session's own shell as it stands.")
    delivery(spec, access=access_of(app), client_python=default_python())
    if spec.kind == "activate":
        k = await _check_activate(app, spec)
        app.env_spec, app.env_pending = spec, None
        info = info_for(k, spec.name)
        if info.state == "ready":
            words = "it activates on the login node; each command, batch row and job runs it first."
        elif k.future is not None:   # its check is queued behind a build on the build worker (review S13)
            words = ("its check on the login node is still queued behind a build: it is kept, and each command checks "
                     "it as it runs (nothing runs where its line does not work).")
        elif access_of(app) == "ssh" and info.error:
            words = (f"its line did not work on the login node, saying (quoted as data): {said(info.error)!r} — login "
                     "and compute nodes may load different modules, so it is kept and checked again with each command, "
                     "which runs nothing if it fails there.")
        else:
            words = "it is checked with each command (a facility endpoint has no login node to try it on first)."
        return Chosen(info, f"The session's env is the existing one{f' {info.name}' if info.name else ''}: {words} "
                            "No new block.")
    k = await ensure_built(app, spec)
    info = info_for(k, spec.name)
    cur = app.env_spec
    same = cur is not None and cur.kind == "requirements" and _known(app, cur) is k
    if info.state == "failed":
        if app.env_pending is not None and _known(app, app.env_pending) is k:
            app.env_pending = None
        return Chosen(info, f"The session's env was not changed: env {info.key} {failed_words(info, k.fault)}",
                      taken=same)
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
    clears what this server believed (review S6): a `.failed` mark makes the next ask send the build. Never runs the
    command again by itself."""
    k = _known_built(app, act.key, act.root) if act.how == "built" else None
    if k is not None and ref.why in ("gone", "failed"):
        k.verified_at = None
    if ref.why == "gone" and act.how == "built":
        if k is not None:
            refresh(app)
            if k.future is None or k.quiet:
                k.quiet = False
                if k.future is None:
                    start_build(app, k)
                else:
                    k.info = k.info.model_copy(update={"state": "building"})
        return "env_building", (f"Nothing ran: the session's env {act.key} was not found under {act.root} on this node "
                                "— purged from scratch, or removed. It is being rebuilt on the login node (free); send "
                                "the command again once ensure_endpoint_up says it is ready.")
    if ref.why == "arch":
        here, *built = ref.detail.split() or ["?"]
        return "failed", (f"Nothing ran: the session's env {act.key} was built for {', '.join(built) or 'another '}"
                          f"architecture; this node is {here}. Building an env on a node of another architecture than "
                          "the login node's needs per-architecture endpoint venvs (a follow-up, not built): use an "
                          'existing env here with env={"activate": "<shell line>"}.')
    if ref.why == "failed":
        if k is not None and k.future is None:
            k.info = k.info.model_copy(update={"state": "unchecked"})
        return "failed", (f"Nothing ran: the session's env {act.key} failed to build here (its mark on the node says "
                          "so). Ask for it again with ensure_endpoint_up(env=…) to build it again, or choose another.")
    if ref.why == "unsafe":
        return "failed", (f"Nothing ran: the session's env {act.key} is in a directory that is not yours alone "
                          f"({said(ref.detail)!r}: owned by another user, or writable by its group or anyone). "
                          "hpc-bridge never runs in an env someone else could change: remove it, or choose a root of "
                          "your own with ensure_endpoint_up(env={…, \"root\": …}).")
    words = "\n".join(ln for ln in (stderr or "").splitlines() if REFUSED not in ln)
    quoted = said(words, 600)
    return "failed", (f"Nothing ran: the session's env could not be activated on this node — its line exited "
                      f"{ref.detail.removeprefix('rc=')}" + (f", saying (quoted as data): {quoted!r}" if quoted else "")
                      + ".")


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
    """`rec` with what has been learnt since it was recorded (its path, its Python, its lock, its warning, once
    built)."""
    if rec.get("kind") == "activate":
        return rec
    refresh(app)
    k = _known_built(app, str(rec.get("key")), rec.get("root"))
    if k is None or k.info.state != "ready":
        return rec
    info = k.info
    return {**rec, **{n: v for n, v in (("path", info.path), ("python", info.python), ("arch", info.arch),
                                       ("lock_sha256", info.lock_sha256), ("warning", info.warning))
                      if v is not None}}


def label(rec: dict[str, Any] | None) -> str | None:
    """A batch's or a job's env in a few words (the ledger's listing)."""
    if not rec:
        return None
    name = f" {rec['name']}" if rec.get("name") else ""
    if rec.get("kind") == "activate":
        return f"existing env{name} ({rec.get('key')})"
    py = f", python {rec['python']}" if rec.get("python") else ""
    return f"built env{name} {rec.get('key')}{py}"
