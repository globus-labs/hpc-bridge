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
- the argument (`parse` → `EnvSpec`, `EnvRefusal` by name): a requirements text is normalized (`normalize_requirements`)
  and refused where the cache key cannot see what a line names (`-r`, `-c`, `-e`, a local path) or where a line turns a
  check off (`--trusted-host`, any option off `_ALLOWED_OPTIONS`). The user's own index lines are kept and listed
  (`index_lines`); hpc-bridge adds none, anywhere.
- the key (`env_key`): the hash of the text and the Python (by default the client's major.minor: `default_python`); the
  env lives at `<root>/<key>-<uname -m>`, the architecture appended ON THE NODE — so the activation finds the node's own
  build, and the server needs no architecture probe before building. The root (`env_root`): the facility's scratch
  root's `envs/`, a home one said (`warning`), the user's `root` first.
- the shell: `build_script` (a relocatable uv venv built BESIDE its path, its marker, `requirements.in`,
  `requirements.lock`, then claimed by ONE rename — of two builds exactly one claims it, so correctness needs no lock;
  `flock`, where the node has it, only saves the second build's work), `check_script` (the full check: every
  RECORD-listed file present and the installed set as built), `activation_sh` (per command: the marker alone, then
  VIRTUAL_ENV and PATH — or the user's line; anything else prints ONE nonce-tagged `HPCB_ENV_REFUSED` line and exits
  `ENV_RC`, before the command), `refusal` (reads that line back: the nonce is the dispatch's, so output cannot forge
  it), `activate_check_script`, `canary_check`.

Then the state this server keeps (AppCtx.env_*): `choose` (ensure_endpoint_up's `env`), `ensure_built`, `for_command`
and `refused` (run_shell), the ledger's record of a batch's or a job's env (`record`, `info_from_record`,
`activation_of`, `record_state`, `rebuild_record`, `label`).
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
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
# A build's walltime on its runner (the task's own ceiling): a large install (a CUDA wheel) takes minutes.
BUILD_S = float(os.environ.get("HPC_BRIDGE_ENV_BUILD_S", "") or 1800.0)
# How long a call waits for a build or a check before answering `building` (the build runs on regardless).
WAIT_S = 20.0
# The full check (every file the install recorded) runs when the env is chosen and once this long has passed: a scratch
# purge runs daily at most. Between two, a command checks the marker alone.
VERIFY_EVERY_S = 24 * 3600.0
MAX_REQUIREMENTS = 64 * 1024
MAX_ACTIVATE = 4096
_KEY_VERSION = "hpcb-env-v1"           # in the key: a change of how envs are built changes every key
_ACTIVATE_KEY_VERSION = "hpcb-env-activate-v1"

_KEYS = frozenset({"activate", "requirements", "python", "root", "name"})
_PYTHON = re.compile(r"^3\.\d{1,2}(\.\d{1,3})?$")
_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_ROOT = re.compile(r"^(\$HOME/|\$\{HOME\}/|~/|/)[A-Za-z0-9_./+-]{0,400}$")   # batch._RESULTS_DIR's rule
_HEX16 = re.compile(r"^[0-9a-f]{16}$")
_NONCE = re.compile(r"^[0-9a-f]{16,64}$")
_TOKEN = re.compile(r"^[0-9a-f]{1,32}$|^t[0-9]{1,3}$")
# The options a requirements file may carry, as uv reads them: where packages come from and which forms it takes.
_ALLOWED_OPTIONS = frozenset({"--index-url", "-i", "--extra-index-url", "--find-links", "-f", "--no-index", "--pre",
                              "--prefer-binary", "--only-binary", "--no-binary"})
_INDEX_OPTIONS = ("--index-url", "-i", "--extra-index-url", "--find-links", "-f")
_WHY = ("gone", "failed", "arch", "activate")


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


# ---- the argument ----------------------------------------------------------------------------------------------------

def normalize_requirements(text: str) -> str:
    """The text as the key reads it: CRLF made LF, blanks and comments (a whole line, or ` #…` after one) dropped, each
    line stripped — and the ORDER kept (with several indexes, the first wins)."""
    out = []
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"\s+#.*$", "", line).strip()
        if line:
            out.append(line)
    return "\n".join(out)


def index_lines(text: str | None) -> list[str]:
    """The user's own lines that say where packages come from (shown in the terms; hpc-bridge never adds one)."""
    return [ln for ln in (text or "").split("\n") if re.split(r"[\s=]", ln, maxsplit=1)[0] in _INDEX_OPTIONS]


def _line_problem(line: str) -> str | None:
    if line.startswith("-"):
        opt = re.split(r"[\s=]", line, maxsplit=1)[0]
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
        return None
    if line.startswith(("./", "../", "/", "~", "file:")) or re.search(r"@\s*file:", line):
        return "a local path, which the env's key (the hash of this text) cannot see change: publish it to an index"
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
    if name is not None and (not isinstance(name, str) or not _NAME.match(name)):
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
            raise EnvRefusal("requirements", f"env requirements: the line {line!r} is refused — {why}")
    py = value.get("python")
    if py is not None and (not isinstance(py, str) or not _PYTHON.match(py)):
        raise EnvRefusal("python", f"env python {py!r} must be a CPython version such as 3.12 (or 3.12.4)")
    root = value.get("root")
    if root is not None and (not isinstance(root, str) or not _ROOT.match(root) or ".." in root.split("/")):
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
    the home directory, a warning to say (homes are small and quota'd; a CUDA wheel alone is GBs)."""
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
    """The root as a shell word: `$HOME/…` expands on the node, the rest is quoted (batch.quoted_dir's rule)."""
    if root.startswith("$HOME/"):
        return '"$HOME"' + shlex.quote(root[len("$HOME"):])
    return shlex.quote(root)


# ---- activation, per command ----------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Activation:
    """What a command, a batch row or a job script activates: a built env (`key` under `root`) or an existing env's
    `line`."""

    how: Literal["built", "activate"]
    key: str
    root: str | None = None
    line: str | None = None


# A refusal, written by every activation the same way: the line on stderr; the batch wrapper's mark file when it set one
# (`__hb_env_mark`: the row ran nothing); the session wrapper's snapshots removed; nothing of the command run.
_REFUSE = (
    '__hb_el="@@REFUSED@@ @@NONCE@@ $__hb_ew $__hb_ek $__hb_ex"\n'
    "printf '%s\\n' \"$__hb_el\" >&2\n"
    '[ -z "${__hb_env_mark:-}" ] || printf \'%s\\n\' "$__hb_el" > "$__hb_env_mark" 2>/dev/null\n'
    'rm -f "${__hb_base:-}" "${__hb_pre:-}" 2>/dev/null\n'
    "exit @@RC@@\n"
)

_ACTIVATE_BUILT = (
    "__hb_ek=@@KEY@@; __hb_ea=$(uname -m 2>/dev/null); __hb_er=@@ROOT@@\n"
    '__hb_ed="$__hb_er/$__hb_ek-$__hb_ea"\n'
    'if grep -qx "short=$__hb_ek" "$__hb_ed/@@MARKER@@" 2>/dev/null; then\n'
    '  unset PYTHONHOME; VIRTUAL_ENV="$__hb_ed"; PATH="$__hb_ed/bin:$PATH"; export VIRTUAL_ENV PATH\n'
    "else\n"
    '  __hb_ew=gone; __hb_ex="$__hb_ea"\n'
    '  if [ -f "$__hb_ed.failed" ]; then __hb_ew=failed\n'
    "  else\n"
    '    for __hb_ey in "$__hb_er/$__hb_ek"-*/@@MARKER@@; do\n'
    '      [ -f "$__hb_ey" ] || continue\n'
    '      __hb_ey=${__hb_ey%/*}; __hb_ew=arch; __hb_ex="$__hb_ex ${__hb_ey##*/$__hb_ek-}"\n'
    "    done\n"
    "  fi\n"
    "@@REFUSE@@"
    "fi\n"
)

_ACTIVATE_LINE = (
    "__hb_ek=@@KEY@@\n"
    'eval "$(printf %s @@B64@@ | base64 --decode)"\n'
    "__hb_erc=$?\n"
    'if [ "$__hb_erc" -ne 0 ]; then\n'
    '  __hb_ew=activate; __hb_ex="rc=$__hb_erc"\n'
    "@@REFUSE@@"
    "fi\n"
)


def activation_sh(act: Activation, nonce: str) -> str:
    """The shell that activates `act` before a command (session_shell.wrap's `env_sh`, batch.task_script's, a job.sh's):
    a built env by its marker alone (one `grep`) — then what `bin/activate` does, without it (a relocatable venv's
    `activate` cannot find itself under sh) — or the user's line, carried base64 (it is code by design, the user's;
    never spliced). Anything else — the env gone (a scratch purge), its build failed, built only for another
    architecture than this node's, the line's own failure — refuses: ONE `HPCB_ENV_REFUSED <nonce> <why> <key> …` line
    and exit ENV_RC, before the command runs."""
    if not _NONCE.match(nonce):
        raise ValueError(f"invalid env nonce {nonce!r}")
    if not _HEX16.match(act.key):
        raise ValueError(f"invalid env key {act.key!r}")
    refuse = "\n".join("  " + ln for ln in _REFUSE.replace("@@REFUSED@@", REFUSED).replace("@@NONCE@@", nonce)
                       .replace("@@RC@@", str(ENV_RC)).splitlines()) + "\n"
    if act.how == "activate":
        b64 = shlex.quote(base64.b64encode(str(act.line or "").encode()).decode())
        return _ACTIVATE_LINE.replace("@@REFUSE@@", refuse).replace("@@KEY@@", act.key).replace("@@B64@@", b64)
    return (_ACTIVATE_BUILT.replace("@@REFUSE@@", refuse).replace("@@KEY@@", act.key)
            .replace("@@ROOT@@", _qroot(str(act.root))).replace("@@MARKER@@", MARKER))


@dataclass(frozen=True)
class Refused:
    """A command its env refused (it ran nothing): `why` gone | failed | arch | activate; `detail`: this node's
    architecture (and the ones it was built for), or `rc=<n>` of the user's line."""

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
        if len(parts) >= 2 and parts[0] in _WHY and _HEX16.match(parts[1]):
            return Refused(parts[0], parts[1], parts[2] if len(parts) > 2 else "")
    return None


def refusal_mark(text: str | None) -> Refused | None:
    """A batch row's own refusal mark (`.env_refused`, written by its activation inside the wrapper, beside the row's
    attempt — a server that lost the nonce, a restarted one, still reads why)."""
    parts = (text or "").split(None, 4)
    if len(parts) >= 4 and parts[0] == REFUSED and parts[2] in _WHY and _HEX16.match(parts[3]):
        return Refused(parts[2], parts[3], parts[4] if len(parts) > 4 else "")
    return None


# ---- building and checking (on the login node) ----------------------------------------------------------------------

_FNS = r"""__hb_sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -c1-64; else shasum -a 256 | cut -c1-64; fi
}
__hb_dists() {
  ls -d "$1"/lib/python*/site-packages/*.dist-info 2>/dev/null | sed 's#.*/##' | LC_ALL=C sort | __hb_sha
}
__hb_check() {
  [ -d "$1" ] || { echo gone; return 0; }
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
"""

_BUILD = r"""# @@TAG@@ @@KEY@@ — hpc-bridge builds a user env (hpc_bridge.envs): beside its path, then one rename
__hb_k=@@KEY@@; __hb_r=@@ROOT@@; __hb_t=@@TOKEN@@
__hb_a=$(uname -m 2>/dev/null)
__hb_d="$__hb_r/$__hb_k-$__hb_a"
__hb_b=""
__hb_log="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t.log"
@@FNS@@__hb_fail() {
  __hb_m=$(printf '%s\n' "$1" | tail -c 2000)
  [ -z "$__hb_b" ] || rm -rf "$__hb_b"
  rm -f "$__hb_log"
  printf '%s\n' "$__hb_m" > "$__hb_d.failed" 2>/dev/null
  printf 'HPCB_ENV_FAILED key=%s arch=%s error_b64=%s\n' "$__hb_k" "$__hb_a" \
    "$(printf '%s' "$__hb_m" | base64 | tr -d '\n')"
  exit 0
}
__hb_ready() {
  printf 'HPCB_ENV_READY key=%s dir=%s arch=%s python=%s lock=%s size_mb=%s free_mb=%s cached=%s\n' "$__hb_k" \
    "$__hb_d" "$__hb_a" "$(sed -n 's/^python=//p' "$__hb_d/@@MARKER@@")" \
    "$(sed -n 's/^lock=//p' "$__hb_d/@@MARKER@@")" "$(du -sm "$__hb_d" 2>/dev/null | cut -f1)" \
    "$(df -Pk "$__hb_r" 2>/dev/null | awk 'NR==2{print int($4/1024)}')" "$1"
}
mkdir -p "$__hb_r" 2>/dev/null || __hb_fail "cannot create the env root $__hb_r"
# where the node has flock, a second build of the same env waits for the first, then finds it built
if command -v flock >/dev/null 2>&1; then
  { exec 9>"$__hb_r/$__hb_k-$__hb_a.lock" && flock -w @@CAP@@ 9; } 2>/dev/null || true
fi
case "$(__hb_check "$__hb_d")" in ready) __hb_ready 1; exit 0;; esac
# a partly purged or unmarked env is moved aside and removed, never used; a failure's mark goes with the next try
if [ -e "$__hb_d" ]; then
  mv "$__hb_d" "$__hb_r/.$__hb_k-$__hb_a.broken.$__hb_t" 2>/dev/null && rm -rf "$__hb_r/.$__hb_k-$__hb_a.broken.$__hb_t"
fi
rm -f "$__hb_d.failed"
find "$__hb_r" -maxdepth 1 -name ".$__hb_k-$__hb_a.build.*" -mmin +240 -exec rm -rf {} + 2>/dev/null
export PATH="$HOME/.local/bin:$PATH" UV_CACHE_DIR="$__hb_r/.uv-cache"
command -v uv >/dev/null 2>&1 || { curl -LsSf https://astral.sh/uv/install.sh \
  | env UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1; } || true
command -v uv >/dev/null 2>&1 || __hb_fail "no uv on the login node, and astral.sh's installer could not install one"
__hb_u=$(command -v uv)
__hb_b="$__hb_r/.$__hb_k-$__hb_a.build.$__hb_t"
"$__hb_u" venv -q --relocatable --python @@PY@@ "$__hb_b" > "$__hb_log" 2>&1 || __hb_fail "$(tail -n 20 "$__hb_log")"
printf %s @@B64@@ | base64 --decode > "$__hb_b/requirements.in" || __hb_fail "cannot write $__hb_b/requirements.in"
"$__hb_u" pip install -q --link-mode=copy --python "$__hb_b/bin/python" -r "$__hb_b/requirements.in" \
  > "$__hb_log" 2>&1 || __hb_fail "$(tail -n 20 "$__hb_log")"
"$__hb_u" pip freeze --python "$__hb_b/bin/python" > "$__hb_b/requirements.lock" 2> "$__hb_log" \
  || __hb_fail "$(tail -n 20 "$__hb_log")"
{
  printf 'key=%s\nshort=%s\narch=%s\n' @@KEY64@@ "$__hb_k" "$__hb_a"
  printf 'python=%s\n' "$("$__hb_b/bin/python" -c 'import platform; print(platform.python_version())' 2>/dev/null)"
  printf 'dists=%s\nlock=%s\n' "$(__hb_dists "$__hb_b")" "$(__hb_sha < "$__hb_b/requirements.lock")"
  printf 'built=%s\nhost=%s\n' "$(date +%s)" "$(hostname 2>/dev/null)"
} > "$__hb_b/@@MARKER@@" || __hb_fail "cannot write $__hb_b/@@MARKER@@"
# the claim: ONE rename, which a non-empty target refuses — of two builds at once, the other's stays and this one goes
"$__hb_b/bin/python" -c 'import os, sys; os.rename(sys.argv[1], sys.argv[2])' "$__hb_b" "$__hb_d" 2>/dev/null \
  || rm -rf "$__hb_b"
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
  case "$__hb_s" in partial*) __hb_m=${__hb_s#partial }; __hb_s=partial;; *) __hb_m=0;; esac
  printf 'HPCB_ENV_CHECK state=%s key=%s arch=%s missing=%s dir=%s\n' "$__hb_s" "$__hb_k" "$__hb_a" "$__hb_m" "$__hb_d"
fi
"""


def build_script(spec: EnvSpec, *, key: str, root: str, python: str, token: str) -> str:
    """The script that builds `spec` on the login node (see the module doc), printing ONE line: `HPCB_ENV_READY key=
    dir= arch= python= lock= size_mb= free_mb= cached=` (cached=1: it was already built and checks out) or
    `HPCB_ENV_FAILED key= arch= error_b64=` (the installer's own words; a `<key>-<arch>.failed` mark beside the env says
    so to the next activation). The requirements reach the installer as a FILE (carried base64): no character of them
    is shell. No index option is added."""
    if spec.kind != "requirements" or not spec.requirements:
        raise ValueError("build_script builds a requirements env")
    if not _HEX16.match(key) or not _TOKEN.match(token) or not _PYTHON.match(python):
        raise ValueError(f"invalid build parameters {key!r} {token!r} {python!r}")
    py = spec.python or default_python()
    full = hashlib.sha256(f"{_KEY_VERSION}\0{py}\0{spec.requirements}".encode()).hexdigest()
    b64 = shlex.quote(base64.b64encode((spec.requirements + "\n").encode()).decode())
    return (_BUILD.replace("@@FNS@@", _FNS).replace("@@TAG@@", BUILD_TAG).replace("@@KEY64@@", full)
            .replace("@@KEY@@", key).replace("@@ROOT@@", _qroot(root)).replace("@@TOKEN@@", token)
            .replace("@@PY@@", shlex.quote(python)).replace("@@B64@@", b64).replace("@@CAP@@", str(int(BUILD_S)))
            .replace("@@MARKER@@", MARKER))


def check_script(*, key: str, root: str) -> str:
    """The full check of the env `key` under `root` on THIS node, printing `HPCB_ENV_CHECK state=…`: ready (every file
    its install recorded present, the installed set as built), partial (`missing=`: a purge took part of it, or it is
    not ours), gone, failed (its build's mark), arch (built only for `others=`, never for this node)."""
    if not _HEX16.match(key):
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
    for this node, or the existing env's line runs (in a subshell); else `HPCB_ENV_OK 0`."""
    act = activation_of(rec)
    if act.how == "activate":
        b64 = shlex.quote(base64.b64encode(str(act.line or "").encode()).decode())
        test = f'( eval "$(printf %s {b64} | base64 --decode)" ) >/dev/null 2>&1'
    else:
        test = (f'grep -qx "short={act.key}" {_qroot(str(act.root))}/{act.key}-"$(uname -m 2>/dev/null)"/{MARKER} '
                "2>/dev/null")
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


def parse_build(stdout: str | None) -> Built | None:
    """The build script's line (the last one), or None when it printed none (cut off: its walltime, a teardown)."""
    for line in reversed(str(stdout or "").splitlines()):
        if line.startswith("HPCB_ENV_READY "):
            f = _fields(line)
            return Built(ok=True, key=f.get("key"), path=f.get("dir") or None, arch=f.get("arch") or None,
                         python=f.get("python") or None, lock_sha256=f.get("lock") or None,
                         size_mb=_num(f.get("size_mb")), free_mb=_num(f.get("free_mb")), cached=f.get("cached") == "1")
        if line.startswith("HPCB_ENV_FAILED "):
            f = _fields(line)
            return Built(ok=False, key=f.get("key"), arch=f.get("arch") or None,
                         error=_b64text(f.get("error_b64")).strip() or "the build failed and said nothing")
    return None


@dataclass
class Check:
    """The full check's verdict on one node: `state` ready | partial | gone | failed | arch."""

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
    or a check still out (`future`), and when its full check last passed (monotonic)."""

    spec: EnvSpec
    root: str
    key: str
    info: EnvInfo
    future: Any = None
    kind: str = "build"            # what `future` is: a build, or an existing env's check
    verified_at: float | None = None


def access_of(app: AppCtx) -> str:
    """ssh — an endpoint hpc-bridge stood up (it has a login node to build on) — or mep, a facility's endpoint."""
    from .context import _has_login_shape

    return "ssh" if _has_login_shape(app) else "mep"


def _known(app: AppCtx, spec: EnvSpec) -> _Known:
    if spec.kind == "activate":
        key = env_key(spec)
        k = app.env_known.get(f"activate\0{key}")
        if k is None:
            k = _Known(spec, "", key, EnvInfo(kind="activate", how="activate", key=key, name=spec.name))
            app.env_known[f"activate\0{key}"] = k
        return k
    root, warning = env_root(app.scratch_root, spec.root)
    key = env_key(spec)
    k = app.env_known.get(f"{root}\0{key}")
    if k is None:
        k = _Known(spec, root, key, EnvInfo(kind="built", how="login", key=key, name=spec.name,
                                            python=spec.python or default_python(), root=root,
                                            index_lines=index_lines(spec.requirements), warning=warning))
        app.env_known[f"{root}\0{key}"] = k
    elif spec.name is not None and spec.name != k.info.name:
        k.info = k.info.model_copy(update={"name": spec.name})
    return k


def _runner(app: AppCtx) -> Any:
    """The runner env builds and checks go to: the login node's own user endpoint for them — the login shape's config
    plus BUILD_UEC_KEY, so a build has its own worker and never takes the login shape's one — with a build's walltime.
    One per endpoint; never a session's, never hpc-bridge's internal one."""
    eid = app.state.endpoint_id
    r = app.env_runner
    if r is not None and getattr(r, "endpoint_id", eid) == eid:
        return r
    from . import warmth

    uec = {**warmth._seed_uec(app, "login"), BUILD_UEC_KEY: True}
    uec = getattr(app.facility, "dispatch_uec", lambda d: d)(uec)
    r = app.runner_factory(eid, user_endpoint_config=uec, walltime=BUILD_S, timeout=WAIT_S)
    app.env_runner = r
    return r


def start_build(app: AppCtx, k: _Known) -> None:
    """Send `k`'s build (its own check first: an env already there is a cache hit) — sync, so a batch's settling can
    start a rebuild too."""
    script = build_script(k.spec, key=k.key, root=k.root, python=k.spec.python or default_python(),
                          token=secrets.token_hex(4))
    fut = _runner(app).submit(script)
    k.future, k.kind = fut, "build"
    k.info = k.info.model_copy(update={"state": "building", "task_id": getattr(fut, "task_id", None), "error": None,
                                       "cached": False})


def _absorb(k: _Known) -> None:
    """A finished build or check, read into what is known of the env."""
    fut, k.future = k.future, None
    if fut.cancelled():   # the client let go of it (a teardown, a new binding): the next ask checks it again
        k.info = k.info.model_copy(update={"state": "unchecked", "task_id": None})
        return
    exc = fut.exception()
    if exc is not None:
        k.info = k.info.model_copy(update={"state": "failed", "task_id": None,
                                           "error": f"it could not be run on the login node: {type(exc).__name__}: "
                                                    f"{str(exc)[:300]}"})
        return
    res = fut.result()
    out = str(getattr(res, "stdout", "") or "")
    if k.kind == "activate":
        m = re.search(r"^HPCB_ENV_ACTIVATE rc=(\d+) out_b64=(\S*)\s*$", out, re.M)
        said = _b64text(m.group(2)).strip() if m else out.strip()[-300:]
        ok = m is not None and m.group(1) == "0"
        k.info = k.info.model_copy(update={"state": "ready" if ok else "unchecked", "task_id": None,
                                           "error": None if ok else (said or "its line failed and said nothing")})
        return
    b = parse_build(out)
    if b is None:
        tail = (out + "\n" + str(getattr(res, "stderr", "") or "")).strip()[-300:]
        k.info = k.info.model_copy(update={"state": "failed", "task_id": None,
                                           "error": f"the build did not finish (rc {getattr(res, 'returncode', '?')}): "
                                                    f"{tail or 'it said nothing'}"})
        return
    if not b.ok:
        k.info = k.info.model_copy(update={"state": "failed", "task_id": None, "error": b.error})
        return
    k.verified_at = time.monotonic()
    k.info = k.info.model_copy(update={"state": "ready", "task_id": None, "error": None, "path": b.path,
                                       "arch": b.arch, "python": b.python or k.info.python,
                                       "lock_sha256": b.lock_sha256, "size_mb": b.size_mb, "cached": b.cached,
                                       "warning": _space_warning(k.info.warning, b)})


def _space_warning(warning: str | None, b: Built) -> str | None:
    """The root's home warning, and — near quota — that the root has under twice the env's size free."""
    if b.size_mb is not None and b.free_mb is not None and b.free_mb < 2 * b.size_mb:
        low = (f"the env root has {b.free_mb:g} MB free, under twice the env's {b.size_mb:g} MB: another env may not "
               "fit there")
        return f"{warning}; {low}" if warning else low
    return warning


def refresh(app: AppCtx) -> None:
    """Read in every build or check that finished since the last look (no wait)."""
    for k in list(app.env_known.values()):
        if k.future is not None and k.future.done():
            _absorb(k)


async def _wait(fut: Any, wait_s: float) -> None:
    if fut is None or fut.done() or wait_s <= 0:
        return
    try:
        await asyncio.to_thread(fut.result, wait_s)
    except Exception:  # noqa: BLE001 - not done within the wait (or failed): `_absorb` reads which, later
        return


async def ensure_built(app: AppCtx, spec: EnvSpec, *, wait_s: float = WAIT_S) -> _Known:
    """The built env `spec`, as known after at most `wait_s`: a cache hit with nothing sent while its full check is
    under VERIFY_EVERY_S old; else its build is sent (the script's own check first: an env a restarted server finds
    there is a cache hit, a partly purged one is rebuilt), and waited for a while. A failed one is tried again."""
    k = _known(app, spec)
    refresh(app)
    if k.future is None:
        fresh = k.verified_at is not None and time.monotonic() - k.verified_at < VERIFY_EVERY_S
        if k.info.state == "ready" and fresh:
            k.info = k.info.model_copy(update={"cached": True})
            return k
        start_build(app, k)
    await _wait(k.future, wait_s)
    refresh(app)
    return k


async def recheck(app: AppCtx, *, since: float) -> None:
    """A block newly confirmed (ruling 5's third time): the session's built env has its full check again on the login
    node (free), whatever its age — a purge between one block and the next is then a rebuild said before the first
    command, not an ImportError in it. Skipped when its check already ran in this call (`since`: time.monotonic())."""
    spec = app.env_spec
    if spec is None or spec.kind != "requirements":
        return
    k = _known(app, spec)
    if k.future is None and (k.verified_at is None or k.verified_at < since):
        k.verified_at = None
    await ensure_built(app, spec)


async def _check_activate(app: AppCtx, spec: EnvSpec, wait_s: float = WAIT_S) -> _Known:
    """An existing env's line, run once on the login node where there is one (free); on a facility endpoint none can be
    run before the work: each command checks it."""
    k = _known(app, spec)
    if access_of(app) != "ssh":
        return k
    refresh(app)
    if k.future is None:
        fut = _runner(app).submit(activate_check_script(str(spec.activate)))
        k.future, k.kind = fut, "activate"
    await _wait(k.future, wait_s)
    refresh(app)
    return k


@dataclass
class Chosen:
    """ensure_endpoint_up's `env`, applied: what the answer states, and its words."""

    info: EnvInfo | None
    notice: str = ""


def _words(info: EnvInfo) -> str:
    where = f" at {info.path}" if info.path else f" under {info.root}" if info.root else ""
    if info.state == "ready":
        return (f"The session's env {info.name or info.key} is ready{where}"
                + (" (already built: nothing was rebuilt)" if info.cached else " (built on the login node, free)")
                + ": every command, batch row and job runs in it; switching env needs no new block.")
    if info.state == "building":
        return (f"The session's env {info.name or info.key} is being built on the login node (free){where}: a command "
                "sent meanwhile runs nothing and answers env_building; ensure_endpoint_up shows its state.")
    return ""


async def choose(app: AppCtx, value: Any) -> Chosen:
    """ensure_endpoint_up's `env`: the session's env from now on, for its commands on both shapes — never hpc-bridge's
    internal session. `{}` drops it. A built env whose build failed leaves the session's env as it was, saying why.
    Raises EnvRefusal (nothing set)."""
    spec = parse(value)
    if spec is None:
        return Chosen(session_info(app))
    if spec.kind == "none":
        app.env_spec = None
        return Chosen(None, "No env: commands run in the session's own shell as it stands.")
    delivery(spec, access=access_of(app), client_python=default_python())
    if spec.kind == "activate":
        k = await _check_activate(app, spec)
        app.env_spec = spec
        info = k.info
        if info.state == "ready":
            said = "it activates on the login node; each command, batch row and job runs it first."
        elif access_of(app) == "ssh":
            said = (f"its line failed on the login node ({info.error}) — login and compute nodes may load different "
                    "modules, so it is kept and checked again with each command, which runs nothing if it fails.")
        else:
            said = "it is checked with each command (a facility endpoint has no login node to try it on first)."
        return Chosen(info, f"The session's env is the existing one{f' {info.name}' if info.name else ''}: {said} "
                            "No new block.")
    k = await ensure_built(app, spec)
    info = k.info
    if info.state == "failed":
        return Chosen(info, f"The session's env was not changed: env {info.key} could not be built on the login node "
                            f"(the installer said, quoted as data: {info.error!r}).")
    app.env_spec = spec
    notice = _words(info)
    if info.index_lines:
        notice += f" Its packages come from your own index lines: {'; '.join(info.index_lines)}."
    if not app.env_said_install:
        app.env_said_install = True
        notice += " Installing runs each package's build code as you, on your allocation's facility."
    if info.warning:
        notice += f" Note: {info.warning}."
    return Chosen(info, notice)


def session_info(app: AppCtx) -> EnvInfo | None:
    """The session's env as it stands (a build that finished meanwhile read in)."""
    spec = app.env_spec
    if spec is None:
        return None
    refresh(app)
    return _known(app, spec).info


@dataclass
class ForCommand:
    """What a user command gets: the activation (None: no env), or why it cannot run now."""

    act: Activation | None = None
    info: EnvInfo | None = None
    building: bool = False
    failed: str | None = None


def for_command(app: AppCtx) -> ForCommand:
    spec = app.env_spec
    if spec is None:
        return ForCommand()
    refresh(app)
    k = _known(app, spec)
    if spec.kind == "activate":
        return ForCommand(Activation("activate", k.key, line=spec.activate), k.info)
    if k.future is not None or k.info.state == "building":
        return ForCommand(info=k.info, building=True)
    if k.info.state == "failed":
        return ForCommand(info=k.info, failed=k.info.error or "its build failed")
    return ForCommand(Activation("built", k.key, root=k.root), k.info)


def building_words(info: EnvInfo | None) -> str:
    key = (info.name or info.key) if info is not None else "?"
    return (f"Nothing ran: the session's env {key} is being built on the login node (free). Send the command again "
            "once ensure_endpoint_up says it is ready.")


def refused(app: AppCtx, ref: Refused, act: Activation, stderr: str) -> tuple[Literal["env_building", "failed"], str]:
    """A command its env refused: (the phase, the notice). A built env gone from its root (a scratch purge) is rebuilt
    on the login node — `env_building`; anything a rebuild cannot mend (built only for another architecture, its
    build failed, the user's line failed) is `failed`, saying why. Never runs the command again by itself."""
    if ref.why == "gone" and act.how == "built":
        k = next((x for x in app.env_known.values() if x.key == act.key and x.root == act.root), None)
        if k is not None:
            refresh(app)
            if k.future is None:
                start_build(app, k)
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
        return "failed", f"Nothing ran: the session's env {act.key} failed to build here (its mark says so)."
    said = "\n".join(ln for ln in (stderr or "").splitlines() if REFUSED not in ln).strip()[-600:]
    return "failed", (f"Nothing ran: the session's env could not be activated on this node — its line exited "
                      f"{ref.detail.removeprefix('rc=')}" + (f", saying (quoted as data): {said!r}" if said else "")
                      + ".")


# ---- a batch's or a job's env, on the ledger ------------------------------------------------------------------------

def record(app: AppCtx, spec: EnvSpec) -> dict[str, Any]:
    """The env a batch or a job runs in, as the ledger records it: kind, how, key, Python, path — and what activates or
    rebuilds it after a restart (the line; the requirements and the Python asked)."""
    k = _known(app, spec)
    info = k.info
    rec: dict[str, Any] = {"kind": info.kind, "how": info.how, "key": info.key, "name": info.name}
    if spec.kind == "activate":
        rec["line"] = spec.activate
    else:
        rec.update({"python": info.python, "python_asked": spec.python, "root": k.root, "path": info.path,
                    "arch": info.arch, "lock_sha256": info.lock_sha256, "requirements": spec.requirements})
    return {k_: v for k_, v in rec.items() if v is not None}


def _spec_of(rec: dict[str, Any]) -> EnvSpec:
    if rec.get("kind") == "activate":
        return EnvSpec(kind="activate", activate=str(rec.get("line") or ""), name=rec.get("name"))
    return EnvSpec(kind="requirements", requirements=str(rec.get("requirements") or ""), python=rec.get("python_asked"),
                   root=rec.get("root"), name=rec.get("name"))


def activation_of(rec: dict[str, Any]) -> Activation:
    if rec.get("kind") == "activate":
        return Activation("activate", str(rec["key"]), line=str(rec.get("line") or ""))
    return Activation("built", str(rec["key"]), root=str(rec.get("root") or ""))


def info_from_record(rec: dict[str, Any] | None) -> EnvInfo | None:
    """The env as its record says (a job's, a batch's), without asking the facility."""
    if not rec:
        return None
    try:
        return EnvInfo(kind=rec.get("kind", "built"), how=rec.get("how", "login"), key=str(rec["key"]),
                       name=rec.get("name"), python=rec.get("python"), path=rec.get("path"), root=rec.get("root"),
                       arch=rec.get("arch"), lock_sha256=rec.get("lock_sha256"),
                       state="ready" if rec.get("path") or rec.get("kind") == "activate" else "unchecked",
                       index_lines=index_lines(rec.get("requirements")))
    except (KeyError, ValueError):
        return None


def record_state(app: AppCtx, rec: dict[str, Any]) -> str:
    """ready | building | failed, for admission: an existing env is always ready (each row checks it); a built one by
    what this server knows (one it does not know — a restart — is tried, and a row that finds it gone rebuilds it)."""
    if rec.get("kind") == "activate":
        return "ready"
    refresh(app)
    k = _known(app, _spec_of(rec))
    if k.future is not None or k.info.state == "building":
        return "building"
    return "failed" if k.info.state == "failed" else "ready"


def rebuild_record(app: AppCtx, rec: dict[str, Any]) -> None:
    """A batch row found its built env gone: rebuild it on the login node (free), once."""
    if rec.get("kind") == "activate":
        return
    k = _known(app, _spec_of(rec))
    refresh(app)
    if k.future is None:
        start_build(app, k)


def current_record(app: AppCtx, rec: dict[str, Any]) -> dict[str, Any]:
    """`rec` with what has been learnt since it was recorded (its path, its Python, its lock, once built)."""
    if rec.get("kind") == "activate":
        return rec
    refresh(app)
    info = _known(app, _spec_of(rec)).info
    if info.state != "ready":
        return rec
    return {**rec, **{k: v for k, v in (("path", info.path), ("python", info.python), ("arch", info.arch),
                                       ("lock_sha256", info.lock_sha256)) if v is not None}}


def label(rec: dict[str, Any] | None) -> str | None:
    """A batch's or a job's env in a few words (the ledger's listing)."""
    if not rec:
        return None
    name = f" {rec['name']}" if rec.get("name") else ""
    if rec.get("kind") == "activate":
        return f"existing env{name} ({rec.get('key')})"
    py = f", python {rec['python']}" if rec.get("python") else ""
    return f"built env{name} {rec.get('key')}{py}"
