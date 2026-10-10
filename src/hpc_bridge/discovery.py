"""Probe an un-indexed facility's login node over raw SSH and propose a FacilityDetails draft.

The cache-miss path of the catalog: a single batched SSH round-trip (MFA-once-friendly) →
deterministic parse → a *proposed* config the user confirms (above all the low-confidence
`interface`). Never trusted — the login-shape canary validates it after the user confirms. Needs only
the SSH transport (`ssh_host` + env creds), not the rest of `FacilityDetails`, so it runs before any
catalog entry or `SlurmFacility` exists.
"""
from __future__ import annotations

import re
import shlex
from typing import Literal

from .facility.remote import (
    DEFAULT_REMOTE_VENV,
    NeedsPreauth,
    SshTarget,
    is_interactive_auth_failure,
    offers_one_time_code,
    ssh_exec,
)
from .models import FacilityDetails


def _installed(dist: str) -> str | None:
    """The version of `dist` installed in THIS client's environment, or None (hermetic tests without it)."""
    try:
        from importlib.metadata import version

        return version(dist)
    except Exception:  # noqa: BLE001 - not installed here
        return None


def _gce_version() -> str | None:
    """The endpoint version the login-node install is pinned to: the installed SDK's (the two are released in
    lockstep), so the remote side matches the client. None only if the SDK isn't importable here."""
    return _installed("globus-compute-sdk")


def _client_python() -> str:
    """This client's major.minor: the worker venv must deserialise task functions dill'd by this interpreter,
    and a facility's system python may be too old for the endpoint at all (uv downloads the version if needed)."""
    import sys

    return f"{sys.version_info.major}.{sys.version_info.minor}"


def keyed_venv(venv: str, *, python: str, gce: str | None) -> str:
    """hpc-bridge's venv for ONE endpoint version on ONE Python: `<venv>-endpoint-<gce>-py<X.Y>`. A pin or Python move
    builds a new venv beside the old one instead of replacing a venv a running manager may be using. The `-endpoint-`
    stem keeps it apart from the facility-MEP seeds' worker venvs (Delta's and Anvil's `…/gce-venv-<gce>-py<X.Y>`,
    which their worker_init upgrades on every block)."""
    return f"{venv}-endpoint-{gce}-py{python}" if gce else f"{venv}-endpoint-py{python}"


# The pieces of venv_env_setup's line. `__hb_venv` is the keyed venv, `__hb_base` the unkeyed `{venv}` (the venv plugins
# up to 0.1.22 built); `__hb_` names stay out of the caller's way (the session shim uses the same prefix). @TOKENS@ are
# filled in by venv_env_setup.

# The full check, through the entry point the manager runs (`python -c "import …cli"` cannot be used: gce's cli reads
# argv[0] at import and fails under -c): it reports the pin, and the venv's python is the client's. On success it
# records the architecture; on failure `$__hb_why` says why.
_OK = (
    '__hb_ok() { __hb_why=$("$1/bin/globus-compute-endpoint" version 2>&1) || '
    "{ __hb_why=$(printf '%s\\n' \"$__hb_why\" | tail -n 1); return 1; }; "
    "@PINCHECK@"
    '"$1/bin/python" -c "import sys;sys.exit(sys.version_info[:2]!=(@MAJOR@,@MINOR@))" || '
    '{ __hb_why="its python is not @PY@"; return 1; }; { uname -m >"$1/.hpcb-arch"; } 2>/dev/null; return 0; }'
)
# The pin part of the check: the entry point's `version` line must name the pinned endpoint version.
_PINCHECK = ('case $__hb_why in *"version: @GCE@") ;; '
             '*) __hb_why="it reports ${__hb_why##*: }, not @GCE@"; return 1;; esac; ')
# A venv whose entry point runs at all, any version: one an older endpoint or an older plugin started from.
_RUNS = '__hb_runs() { "$1/bin/globus-compute-endpoint" version >/dev/null 2>&1; }'
# `$__hb_f` = `$1.<pid>`, made unique: a leftover from a reused pid would otherwise take the venv inside it.
_FREE = '__hb_free() { __hb_f="$1.$$"; while [ -e "$__hb_f" ]; do __hb_f="${__hb_f}x"; done; }'
# Undo a build: the venv THIS build created (`__hb_mine`) moved aside and removed (best-effort), the old venv put back,
# the lock released. Never touches a venv the build did not create.
_UNDO = (
    '__hb_undo() { if [ -n "$__hb_mine" ] && [ -e "$__hb_venv" ]; then __hb_free "$__hb_venv.failed"; '
    'mv "$__hb_venv" "$__hb_f" && rm -rf "$__hb_f" 2>/dev/null; fi; '
    '[ -z "$__hb_old" ] || [ -e "$__hb_venv" ] || mv "$__hb_old" "$__hb_venv"; rmdir "$__hb_venv.lock" 2>/dev/null; }'
)
# A build under a mkdir lock. A directory that cannot be created fails at once (a missing home, a full quota); a lock
# that exists is waited for, bounded, and one older than any bring-up's ssh timeout is a dead build's and is taken over
# (two waiters can both judge it stale — check, then rmdir — and both build; the re-check below makes that rare and
# it costs only a duplicate build). With the lock: check again, never touch a directory that is not a venv, move the
# old venv aside (mv works where rm fails on NFS) — that is a repair, built without the package cache so a corrupt
# cached file cannot defeat it — then build and check, and undo on failure. SIGPIPE is ignored and HUP/INT/TERM undo,
# so a dropped session still restores and releases; a SIGKILL cannot (the vault says how the next bring-up recovers).
_BUILD = (
    '__hb_build() { mkdir -p "${__hb_venv%/*}" || { echo "hpc-bridge: cannot create ${__hb_venv%/*}" >&2; return 1; }; '
    '__hb_n=0; until __hb_e=$(mkdir "$__hb_venv.lock" 2>&1); do '
    '[ -d "$__hb_venv.lock" ] || { echo "hpc-bridge: cannot create $__hb_venv.lock: ${__hb_e##*: }" >&2; return 1; }; '
    '[ -n "$(find "$__hb_venv.lock" -prune -mmin +15 2>/dev/null)" ] && '
    'rmdir "$__hb_venv.lock" 2>/dev/null && continue; '
    '__hb_n=$((__hb_n+1)); [ "$__hb_n" -le 200 ] || { echo "hpc-bridge: $__hb_venv.lock is held by another build; '
    'one older than 15 minutes is taken over automatically" >&2; return 1; }; sleep 2; done; '
    "trap '' PIPE; __hb_old=; __hb_mine=; __hb_r=; "
    'if __hb_ok "$__hb_venv"; then __hb_r=0; '
    'elif [ -e "$__hb_venv" ] && [ ! -f "$__hb_venv/pyvenv.cfg" ]; then '
    'echo "hpc-bridge: $__hb_venv exists and is not a venv: move it away" >&2; __hb_r=1; '
    "else echo \"hpc-bridge: building $__hb_venv ($__hb_why)\" >&2; __hb_nc=; trap '__hb_undo; exit 1' HUP INT TERM; "
    'if [ -e "$__hb_venv" ]; then __hb_free "$__hb_venv.old"; __hb_old=$__hb_f; __hb_nc=@NOCACHE@; '
    'mv "$__hb_venv" "$__hb_old" || { echo "hpc-bridge: cannot move $__hb_venv aside" >&2; __hb_old=; __hb_r=1; }; '
    'fi; if [ -z "$__hb_r" ]; then __hb_why=; __hb_mine=1; '
    'if @CREATE@ && @INSTALL@ $__hb_nc @REQS@ && __hb_ok "$__hb_venv"; then __hb_r=0; trap - HUP INT TERM; '
    '[ -z "$__hb_old" ] || rm -rf "$__hb_old" 2>/dev/null; '
    'else __hb_r=1; echo "hpc-bridge: could not build @WANT@ in $__hb_venv${__hb_why:+: $__hb_why}" >&2; __hb_undo; '
    'fi; fi; fi; trap - PIPE HUP INT TERM; rmdir "$__hb_venv.lock" 2>/dev/null; return $__hb_r; }'
)

# Inside a batch job: the line is the block's worker_init.
_ON_WORKER = '[ -n "${SLURM_JOB_ID:-}${PBS_JOBID:-}" ]'
# A worker runs the venv its manager was configured with: checked, never built. Only a MISSING keyed venv falls back,
# to the unkeyed one when its entry point runs — the venv a manager started by a plugin up to 0.1.22 runs from.
_WORKER = (
    'if [ ! -e "$__hb_venv" ] && __hb_runs "$__hb_base"; then __hb_venv=$__hb_base; '
    'else "$__hb_venv/bin/globus-compute-endpoint" version >/dev/null || { echo "hpc-bridge: the endpoint venv '
    '$__hb_venv (built on $(cat "$__hb_venv/.hpcb-arch" 2>/dev/null || echo unknown)) does not run on $(hostname) '
    '($(uname -m)); see the error above" >&2; exit 1; }; fi'
)
# The plugin's list/stop/delete calls (30 s for teardown's): never a build.
_NO_BUILD = '[ "${HPCB_ENV_REPAIR:-}" = 0 ]'
# The keyed venv if its entry point exists, else the first hpc-bridge venv whose entry point runs (an endpoint another
# version started, adopted over the web and now torn down). Candidates come from `find`, not a glob, so a `failglob`
# shell cannot abort the loop; moved-aside venvs and locks are not candidates.
_REUSE = (
    '[ -x "$__hb_venv/bin/globus-compute-endpoint" ] || { for __hb_d in "$__hb_base" $(find "${__hb_base%/*}" '
    '-maxdepth 1 -name "${__hb_base##*/}-endpoint-*" ! -name "*.old.*" ! -name "*.failed.*" ! -name "*.lock" '
    '2>/dev/null | sort); do __hb_runs "$__hb_d" && { __hb_venv=$__hb_d; break; }; done; '
    '[ -x "$__hb_venv/bin/globus-compute-endpoint" ] || { echo "hpc-bridge: no working hpc-bridge venv beside '
    '$__hb_base (one is built when hpc-bridge starts the endpoint)" >&2; false; }; }'
)
# A plugin that sets no flag (up to 0.1.22, reading a re-ingested seed): the keyed venv when it works, else — when it
# is MISSING — the unkeyed one if it runs (what that plugin started its endpoint from), else a build. So such a plugin
# never breaks a setup that worked; workers fall back the same way.
_NO_FLAG = (
    '__hb_ok "$__hb_venv" || { [ ! -e "$__hb_venv" ] && __hb_runs "$__hb_base" && __hb_venv=$__hb_base; } '
    '|| __hb_build'
)


def venv_env_setup(*, python: str, gce: str | None, sdk: str | None, create: str, install: str, nocache: str,
                   prefix: str = "") -> str:
    """A self-provisioning env_setup for hpc-bridge's own venv, keyed by version (`keyed_venv`). It runs one of four
    ways:

    - on a compute worker (`$SLURM_JOB_ID`/`$PBS_JOBID` set): the venv's entry point must run, else the job ends with
      the node's and the venv's architecture named. Never a build.
    - `HPCB_ENV_REPAIR=0` (the plugin's list/stop/delete): never a build — the keyed venv, else a working one beside it.
    - `HPCB_ENV_REPAIR=1` (a bring-up): the full check; on failure a locked build that puts the old venv back if it
      fails.
    - no flag (an older plugin): the keyed venv, the unkeyed one it used, or a build.

    `create` makes an empty venv at "$__hb_venv"; `install` takes the requirements; `nocache` is its no-cache flag for a
    repair; `sdk` co-pins globus-sdk (the endpoint leaves it unbounded, so a fresh build at an older pin would get the
    newest)."""
    major, minor = python.split(".")[:2]
    if gce:
        reqs = f"'globus-compute-endpoint=={gce}'" + (f" 'globus-sdk=={sdk}'" if sdk else "")
        want = f"globus-compute-endpoint {gce} (Python {python})"
    else:
        reqs, want = "globus-compute-endpoint", f"globus-compute-endpoint (Python {python})"
    ok = _OK.replace("@PINCHECK@", _PINCHECK if gce else "")  # first: _PINCHECK carries @GCE@ itself
    build = _BUILD
    fill = {"@GCE@": gce or "", "@MAJOR@": major, "@MINOR@": minor, "@PY@": python, "@CREATE@": create,
            "@INSTALL@": install, "@NOCACHE@": nocache, "@REQS@": reqs, "@WANT@": want}
    for token, value in fill.items():
        ok, build = ok.replace(token, value), build.replace(token, value)
    key = keyed_venv("$__hb_base", python=python, gce=gce)
    return (f'{prefix}__hb_base="{{venv}}"; __hb_venv="{key}"; {_RUNS}; {_FREE}; {ok}; {_UNDO}; {build}; '
            f"if {_ON_WORKER}; then {_WORKER}; elif {_NO_BUILD}; then {_REUSE}; "
            f'elif [ "${{HPCB_ENV_REPAIR:-}}" = 1 ]; then __hb_ok "$__hb_venv" || __hb_build; '
            f'else {_NO_FLAG}; fi && . "$__hb_venv/bin/activate"')


# `bootstrap`: a facility with NEITHER globus-compute-endpoint nor uv (SDSC Expanse, live 2026-09-04) — install uv
# into the user's home first (what the Delta and Anvil facility templates do in their own worker_init), inside the
# build so only a build needs it. Outbound HTTPS from the login node is assumed.
_CURL_UV = "{ command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh; } && "


def uv_env_setup(*, python: str, gce: str | None, sdk: str | None, bootstrap: bool = False) -> str:
    """`venv_env_setup` built with uv (the venv at `python`, which uv downloads if the node lacks it)."""
    create = f'uv venv -q "$__hb_venv" --python {python}'
    return venv_env_setup(python=python, gce=gce, sdk=sdk, create=(_CURL_UV + create) if bootstrap else create,
                          install='uv pip install -q --python "$__hb_venv/bin/python"', nocache="--no-cache",
                          prefix='export PATH="$HOME/.local/bin:$PATH"; ' if bootstrap else "")


def pip_env_setup(*, python: str, gce: str | None, sdk: str | None, prefix: str = "") -> str:
    """`venv_env_setup` built with the site Python's own venv and pip (a module matching the client's Python)."""
    return venv_env_setup(python=python, gce=gce, sdk=sdk, create='python3 -m venv "$__hb_venv"',
                          install='"$__hb_venv/bin/python" -m pip install -q', nocache="--no-cache-dir", prefix=prefix)


_UV_ENV_SETUP = uv_env_setup(python=_client_python(), gce=_gce_version(), sdk=_installed("globus-sdk"))
_BOOTSTRAP_UV_ENV_SETUP = uv_env_setup(python=_client_python(), gce=_gce_version(), sdk=_installed("globus-sdk"),
                                       bootstrap=True)


# The env_setup texts discovery generated up to 0.1.22 (guarded by `[ -d ] ||` / `command -v ||`), which a cached
# facilities.json still holds: a returning user never re-probes, so the globus1 bug would come back. Matched EXACTLY
# (with `{venv}` templated or resolved to one path); a hand-written line never matches and is never touched.
_OLD_VENV = r"(?P<v>\{venv\}|/[^\s;'\"]+)"
_OLD_PIN = r"(?:'globus-compute-endpoint==[\w.+!-]+'|globus-compute-endpoint)"
_OLD_UV = (r"\[ -d (?P=v) \] \|\| uv venv (?P=v)(?: --python \d+\.\d+)?; \. (?P=v)/bin/activate; "
           r"command -v globus-compute-endpoint >/dev/null 2>&1 \|\| uv pip install -q " + _OLD_PIN)
_OLD_INIT = r"(?P<init>(?:type module >/dev/null 2>&1 \|\| \. \S+; )?module load (?P<mod>\S+); )"
_OLD_TEXTS = (
    ("uv", re.compile(r"(?P<pre>)" + _OLD_UV.replace("(?P=v)", _OLD_VENV, 1) + r"\Z")),
    ("bootstrap", re.compile(r"(?P<pre>export PATH=\"\$HOME/\.local/bin:\$PATH\"; command -v uv >/dev/null 2>&1 \|\| "
                             r"curl -LsSf https://astral\.sh/uv/install\.sh \| sh; )"
                             + _OLD_UV.replace("(?P=v)", _OLD_VENV, 1) + r"\Z")),
    ("module-uv", re.compile(r"(?P<pre>" + _OLD_INIT + r")" + _OLD_UV.replace("(?P=v)", _OLD_VENV, 1) + r"\Z")),
    ("module-pip", re.compile(r"(?P<pre>" + _OLD_INIT + r")" + rf"\[ -d {_OLD_VENV} \] \|\| python3 -m venv (?P=v); "
                              r"\. (?P=v)/bin/activate; command -v globus-compute-endpoint >/dev/null 2>&1 \|\| "
                              r"pip install -q " + _OLD_PIN + r"\Z")),
)


def migrate_env_setup(env_setup: str) -> str:
    """`env_setup` as the current generator writes it when it is EXACTLY one of the lines discovery generated up to
    0.1.22 (at this client's Python and pin, as a fresh probe would propose); anything else, unchanged."""
    for kind, rx in _OLD_TEXTS:
        m = rx.fullmatch(env_setup or "")
        if m is None:
            continue
        py, gce, sdk = _client_python(), _gce_version(), _installed("globus-sdk")
        if kind == "module-pip":
            new = pip_env_setup(python=py, gce=gce, sdk=sdk, prefix=m.group("pre"))
        else:
            new = uv_env_setup(python=py, gce=gce, sdk=sdk, bootstrap=kind == "bootstrap")
            if kind == "module-uv":
                new = m.group("pre") + new
        return new.replace("{venv}", m.group("v")) if m.group("v") != "{venv}" else new
    return env_setup

# Dedicated HPC high-speed fabrics — a strong `interface` signal vs a management eth. Ordered by
# priority: the dedicated compute fabric (hsn/ipogif/ib/hsi) outranks a `bond` mgmt NIC.
_FAST_NIC_ORDER = ("hsn", "ipogif", "ib", "hsi", "bond")
_VIRTUAL_NIC_PREFIXES = ("docker", "virbr", "veth", "br-", "cni", "flannel", "tap")

# One batched probe. Each command is guarded so one failure can't abort the sweep; output is framed by
# sentinels so a login banner can't pollute the parse. Multi-valued facts (PART/NIC) repeat their key.
_PROBE = r"""
echo HPCB_PROBE_BEGIN
echo "USER=$(whoami 2>/dev/null)"
echo "HOME=${HOME:-}"
echo "SCRATCH=${SCRATCH:-}"
echo "WORK=${WORK:-}"
echo "PSCRATCH=${PSCRATCH:-}"
if command -v sbatch >/dev/null 2>&1; then echo SCHED=slurm;
elif command -v qsub >/dev/null 2>&1; then echo SCHED=pbs;
else echo SCHED=none; fi
export PYTHONDONTWRITEBYTECODE=1
hpcb_capped() {  # run "$@" for at most 20 s where `timeout` exists: a slow filesystem must not eat the probe's budget
  if command -v timeout >/dev/null 2>&1; then timeout 20 "$@"; else "$@"; fi
}
hpcb_py() {  # the interpreter an entry point runs: its shebang, or the python a /bin/sh launcher execs (uv's, pip's)
  l=$(head -n 1 "$1" 2>/dev/null)
  case $l in
    "#!/bin/sh"*) sed -n "2s/^'''exec' ['\"]\{0,1\}\([^'\" ]*\).*/\1/p" "$1" ;;
    "#!/usr/bin/env "*) command -v "${l#\#!/usr/bin/env }" ;;
    "#!"*) l=${l#\#!}; echo "${l%% *}" ;;
  esac
}
hpcb_state() {  # "<python X.Y> <gce version>" from the entry point itself, "timed out", or why it fails
  o=$(hpcb_capped "$1" version 2>&1); r=$?
  [ "$r" -eq 124 ] && { echo "timed out"; return; }
  [ "$r" -eq 0 ] || { printf '%s\n' "$o" | tail -n 1; return; }
  p=$(hpcb_capped "$2" -c "import sys;print('%d.%d' % sys.version_info[:2])" 2>/dev/null); r=$?
  [ "$r" -eq 124 ] && { echo "timed out"; return; }
  [ "$r" -eq 0 ] || { echo "its python ($2) fails"; return; }
  echo "$p ${o##*: }"
}
G=$(command -v globus-compute-endpoint 2>/dev/null); echo "GCE=$G"
[ -n "$G" ] && echo "GCE_STATE=$(hpcb_state "$G" "$(hpcb_py "$G")")"
[ -n "$G" ] && { [ -f "${G%/*}/activate" ] && echo GCE_ACTIVATE=yes || echo GCE_ACTIVATE=no; }
V=@@VENV@@
if [ -e "$V" ]; then
  echo "VENV=$V"
  if [ -f "$V/pyvenv.cfg" ]; then echo "VENV_STATE=$(hpcb_state "$V/bin/globus-compute-endpoint" "$V/bin/python")"
  else echo "VENV_STATE=not a venv"; fi
fi
echo "UV=$(command -v uv 2>/dev/null)"
echo "MYBALANCE=$(command -v mybalance 2>/dev/null)"
echo "XDUSAGE=$(command -v xdusage 2>/dev/null)"
for f in /etc/profile.d/lmod.sh /etc/profile.d/modules.sh /usr/share/lmod/lmod/init/bash; do
  [ -r "$f" ] && { echo "LMOD_INIT=$f"; break; }
done
if type module >/dev/null 2>&1; then
  module -t avail 2>&1 | grep -vE ':$|^$|^-|/$' | head -300 | awk '{print "MODULE="$1}'
fi
sinfo -h -o 'PART=%P|%a' 2>/dev/null || true
qstat -Q 2>/dev/null | awk 'NR>2 && $1!="" {print "QUEUE="$1}' || true
ip -o -4 addr show 2>/dev/null | awk '{print "NIC="$2"|"$4}' || true
echo HPCB_PROBE_END
""".strip()


def _probe_script() -> str:
    """`_PROBE` with the keyed venv the bootstrap will use (HPC_BRIDGE_REMOTE_VENV, else the convention), checked
    through its entry point with bytecode writes off, so the proposal can say whether it is reused or rebuilt."""
    from . import config

    venv = config.remote_venv() or DEFAULT_REMOTE_VENV.replace("{user}", "$(whoami)")
    return _PROBE.replace("@@VENV@@", keyed_venv(venv, python=_client_python(), gce=_gce_version()))


async def discover_facility_details(target: SshTarget) -> tuple[FacilityDetails, list[str]]:
    """Run the batched probe over `target` and parse it into a proposed FacilityDetails + notes.

    `notes` names the low-confidence / unresolved fields the agent must confirm with the user
    (`interface` above all). Raises RuntimeError if the probe SSH itself can't reach the host."""
    rc, out, err = await ssh_exec(target, f"bash -lc {shlex.quote(_probe_script())}")
    if "HPCB_PROBE_BEGIN" not in out:  # never even ran the script (auth/connect failure)
        if is_interactive_auth_failure(rc, err):  # needs a one-time interactive pre-auth, not broken
            raise NeedsPreauth(target, otp_ok=offers_one_time_code(err))
        raise RuntimeError(f"discovery probe failed (rc={rc}): {(err or out).strip()[:300]}")
    return parse_probe(out, ssh_host=target.host)


def parse_probe(stdout: str, *, ssh_host: str) -> tuple[FacilityDetails, list[str]]:
    """Pure parse of the probe stdout into a proposed FacilityDetails + low-confidence notes."""
    f = _collect(stdout)
    user = f.get("USER") or ""
    notes: list[str] = []

    scratch_root, scratch_note = _scratch(
        f.get("SCRATCH") or f.get("WORK") or f.get("PSCRATCH"), f.get("HOME"), user)
    if scratch_note:
        notes.append(scratch_note)

    sched = f.get("SCHED")
    if sched == "pbs":
        scheduler: Literal["slurm", "pbs"] = "pbs"
        partition = _default_queue(f.get("QUEUE", []))
        if not f.get("QUEUE"):
            notes.append("queue: `qstat -Q` returned nothing; confirm the PBS queue to use.")
    elif sched == "slurm":
        scheduler = "slurm"
        partition = _default_partition(f.get("PART", []))
        if not f.get("PART"):
            notes.append("partition: `sinfo` returned nothing; confirm the queue to use.")
    else:
        scheduler = "slurm"
        partition = _default_partition(f.get("PART", []))
        notes.append("scheduler: neither `sbatch` nor `qsub` found; defaulted scheduler='slurm' — "
                     "confirm the facility's scheduler before proceeding.")

    interface, nic_note = _interface(f.get("NIC", []))
    notes.append(nic_note)  # interface is always flagged — it's the field the canary most often fails on

    env_setup, env_note = _env_setup(f.get("GCE"), f.get("UV"), user,
                                     modules=f.get("MODULE", []), lmod_init=f.get("LMOD_INIT"),
                                     gce_state=f.get("GCE_STATE"), gce_activate=f.get("GCE_ACTIVATE"),
                                     venv=f.get("VENV"), venv_state=f.get("VENV_STATE"))
    notes.append(env_note)

    alloc_cmd, alloc_parser, alloc_note = _allocation(f)
    if alloc_note:
        notes.append(alloc_note)

    draft = FacilityDetails(
        ssh_host=ssh_host,
        interface=interface,
        env_setup=env_setup,
        scratch_root=scratch_root,
        partition=partition,
        scheduler=scheduler,
        allocation_command=alloc_cmd,
        allocation_parser=alloc_parser,
    )
    return draft, notes


def _collect(stdout: str) -> dict:
    """Frame-bounded parse of `KEY=value` lines; PART/NIC accumulate into lists."""
    scalars: dict[str, str] = {}
    multi: dict[str, list[str]] = {"PART": [], "NIC": [], "QUEUE": [], "MODULE": []}
    in_block = False
    for raw in stdout.splitlines():
        line = raw.strip()
        if line == "HPCB_PROBE_BEGIN":
            in_block = True
            continue
        if line == "HPCB_PROBE_END":
            break
        if not in_block or "=" not in line:
            continue
        key, _, val = line.partition("=")
        if key in multi:
            multi[key].append(val)
        else:
            scalars[key] = val
    return {**scalars, **multi}


def _templatize(path: str, user: str) -> str:
    """Replace the discovered login name with the `{user}` template so the path is per-user."""
    return path.replace(user, "{user}") if user and user in path else path


def _scratch(scratch_src: str | None, home: str | None, user: str) -> tuple[str, str | None]:
    """Propose `scratch_root` + a low-confidence note. `$SCRATCH` is per-user on many facilities
    (NERSC/Anvil — it contains the login name, so we templatize it) but a SHARED base on others
    (Midway: `/scratch/midway3`). When the login name ISN'T in the path we can't tell which, so we
    append a per-user subdir and flag it — a shared base ⇒ `Permission denied` on every session cd
    (seen live on Midway, where the un-flagged shared path stranded the run)."""
    if scratch_src:
        base = scratch_src.rstrip("/")
        if user and user in base:
            return _templatize(base, user) + "/.hpc-bridge", None
        return base + "/{user}/.hpc-bridge", (
            f"scratch_root: $SCRATCH={base} has no per-user component; proposed {base}/{{user}}/"
            ".hpc-bridge — CONFIRM it's a writable per-user path (a shared base ⇒ Permission denied "
            "on every session cd).")
    return _templatize((home or "/tmp").rstrip("/"), user) + "/.hpc-bridge", (  # noqa: S108 - a REMOTE path default, not a local temp file
        "scratch_root: no $SCRATCH/$WORK set; defaulted under $HOME — confirm a shared, non-purged path.")


def _default_partition(part_lines: list[str]) -> str:
    """Pick a sensible default queue: prefer a cheap/quick one, else the Slurm default (`*`), else first."""
    names = [p.split("|", 1)[0] for p in part_lines]
    clean = [n.rstrip("*") for n in names]
    for pref in ("debug", "shared", "test", "standard", "batch"):
        if pref in clean:
            return pref
    starred = [n[:-1] for n in names if n.endswith("*")]
    if starred:
        return starred[0]
    return clean[0] if clean else "debug"


def _default_queue(queue_lines: list[str]) -> str:
    """Pick a sensible default PBS queue: prefer a debug/short one, else the first listed."""
    names = [q.strip() for q in queue_lines if q.strip()]
    for pref in ("debug", "debug-scaling", "prod", "batch"):
        if pref in names:
            return pref
    return names[0] if names else "debug"


def _interface(nic_lines: list[str]) -> tuple[str, str]:
    """Propose the worker-binding NIC from `ip addr` candidates — always low-confidence."""
    cands: list[str] = []
    for raw in nic_lines:
        iface = raw.split("|", 1)[0]
        if iface == "lo" or iface.startswith(_VIRTUAL_NIC_PREFIXES) or iface in cands:
            continue
        cands.append(iface)
    if not cands:
        return "ib0", ("interface: no candidate NIC found via `ip addr`; defaulted to `ib0` — VERIFY "
                       "(a wrong interface ⇒ workers never register).")
    fast = [c for c in cands if c.startswith(_FAST_NIC_ORDER)]

    def _rank(nic: str) -> int:
        return next((i for i, p in enumerate(_FAST_NIC_ORDER) if nic.startswith(p)), len(_FAST_NIC_ORDER))

    pick = min(fast, key=_rank) if fast else cands[0]
    return pick, (f"interface: proposed `{pick}` from {cands} — CONFIRM (a wrong interface ⇒ workers "
                  "never register; the login canary catches it).")


_PYTHON_MODULE = re.compile(
    r"^(python|python3|anaconda|anaconda3|miniconda|miniconda3|miniforge|conda|cray-python)(/|$)", re.I)
_UV_MODULE = re.compile(r"^uv(/|$)", re.I)


def _module_env_setup(modules: list[str], lmod_init: str | None) -> tuple[str, str] | None:
    """A `module load` based env_setup when the facility's module system (Lmod/Tmod) offers uv or a Python matching
    this client's — the site-supported way to a toolchain, ahead of curl-installing uv. None when no usable module.

    The setup re-initialises the module command itself when it is missing: a compute node's batch script is NOT a
    login shell (Slurm's and PBS's start `#!/bin/bash`), so `/etc/profile.d` never ran there and `module` is
    undefined unless the site exports BASH_ENV — `worker_init` replays env_setup exactly there."""
    modules = [m for m in modules if m and not m.endswith("/")]  # Lmod's terse `avail` also prints `name/` dir rows
    if not modules:
        return None
    client_py = _client_python()
    init = f"type module >/dev/null 2>&1 || . {lmod_init}; " if lmod_init else ""
    uv_mods = [m for m in modules if _UV_MODULE.match(m)]
    py_mods = [m for m in modules if _PYTHON_MODULE.match(m)]
    py_match = [m for m in py_mods if client_py in m]
    if uv_mods:
        return (f"{init}module load {uv_mods[0]}; " + _UV_ENV_SETUP,
                "env_setup: no usable globus-compute-endpoint and no uv on the default PATH, but the module system "
                f"offers `{uv_mods[0]}` — proposed `module load {uv_mods[0]}` then hpc-bridge's own checked uv venv "
                f"(Python {client_py}, this client's). Confirm the module name (`module avail`).")
    if py_match:
        mod = py_match[0]
        return (pip_env_setup(python=client_py, gce=_gce_version(), sdk=_installed("globus-sdk"),
                              prefix=f"{init}module load {mod}; "),
                "env_setup: no usable globus-compute-endpoint and no uv on the default PATH, but the module system "
                f"offers `{mod}` (matches this client's Python {client_py}, so tasks deserialise) — proposed "
                f"`module load {mod}`, a venv and a pip install of the endpoint. Confirm the module name "
                "(`module avail`).")
    if py_mods:
        return None  # a Python module of the WRONG minor: fall through to the uv bootstrap (uv fetches the right one)
    return None


TIMED_OUT = "timed out"  # what the probe reports when its time cap stopped a check (exit 124)
_INSTALL_OK = re.compile(r"^(\d+\.\d+) (\S+)$")


def _install_state(raw: str | None) -> tuple[str, str] | str | None:
    """A probed install: `(python, gce version)` when its entry point ran, `TIMED_OUT` when the probe's time cap hit,
    the failure's last line otherwise, None when it was not probed (no install there, or an older probe)."""
    if raw is None:
        return None
    m = _INSTALL_OK.match(raw.strip())
    return (m.group(1), m.group(2)) if m else (raw.strip()[:200] or "no output")


def _venv_note(venv: str | None, state: str | None) -> str:
    """What the first connect will do with an existing hpc-bridge venv: reuse it, rebuild it, or refuse it."""
    if not venv:
        return ""
    s = _install_state(state)
    if s == TIMED_OUT:
        return (f" The existing hpc-bridge venv {venv} could not be checked in time (a slow filesystem?); the first "
                "connect checks it.")
    if s == "not a venv":
        return (f" {venv} already exists and is NOT a venv: hpc-bridge will not replace it, so the bootstrap fails — "
                "move it away, or set HPC_BRIDGE_REMOTE_VENV to another path.")
    want_py, want_gce = _client_python(), _gce_version()
    if isinstance(s, tuple):
        if s[0] == want_py and (want_gce is None or s[1] == want_gce):
            return f" The existing hpc-bridge venv {venv} (globus-compute-endpoint {s[1]}, Python {s[0]}) is reused."
        return (f" The existing hpc-bridge venv {venv} has globus-compute-endpoint {s[1]} on Python {s[0]}, not "
                f"{want_gce or 'the pin'} on Python {want_py}: the first connect rebuilds it (that directory only).")
    if s is not None:
        return (f" The existing hpc-bridge venv {venv} is BROKEN ({s}): the first connect rebuilds it "
                "(that directory only).")
    return ""


def _env_setup(gce: str | None, uv: str | None, user: str, modules: list[str] | None = None,
               lmod_init: str | None = None, *, gce_state: str | None = None, gce_activate: str | None = None,
               venv: str | None = None,
               venv_state: str | None = None) -> tuple[str, str]:
    """Propose how to put globus-compute-endpoint on PATH for the bootstrap's `bash -lc`: a WORKING endpoint already
    there > uv on PATH > the site's module system (uv or a matching Python module) > curl-install uv. An install on
    PATH whose entry point fails, or that runs another Python than this client's (task functions would not
    deserialise), is passed over for hpc-bridge's own venv — never repaired: it is not ours."""
    found = _install_state(gce_state)
    skip = ""
    if gce and isinstance(found, str) and found != TIMED_OUT:
        skip = (f"globus-compute-endpoint at {gce} is BROKEN ({found}) — not used; ")
    elif gce and isinstance(found, tuple) and found[0] != _client_python():
        skip = (f"globus-compute-endpoint at {gce} runs Python {found[0]}, not this client's {_client_python()} — not "
                "used; ")
    elif gce:
        ver = (f" {found[1]} (Python {found[0]})" if isinstance(found, tuple) else
               " (its check timed out, a slow filesystem? — confirm it works)" if found == TIMED_OUT else "")
        # a venv's bin/ has `activate`; a `pip install --user` entry point in ~/.local/bin does not (the probe says)
        if gce.endswith("/bin/globus-compute-endpoint") and gce_activate != "no":
            path = _templatize(gce[: -len("/bin/globus-compute-endpoint")], user)
            return f"source {path}/bin/activate", (f"env_setup: globus-compute-endpoint{ver} already at "
                    f"{gce}; proposed activating its venv — confirm.")
        return "true", (f"env_setup: globus-compute-endpoint{ver} already on PATH ({gce}); using a no-op — "
                        "confirm it's on the *bootstrap* PATH (a non-interactive `bash -lc`).")
    ours = _venv_note(venv, venv_state)
    if uv:
        return _UV_ENV_SETUP, (f"env_setup: {skip or 'no globus-compute-endpoint found, but '}`uv` is present — "
                               "proposed hpc-bridge's own uv venv for this endpoint version and Python, checked each "
                               "time the endpoint starts and rebuilt when it fails; compute nodes only check it."
                               + ours)
    via_modules = _module_env_setup(list(modules or []), lmod_init)
    if via_modules:
        return via_modules[0], f"env_setup: {skip}" + via_modules[1].removeprefix("env_setup: ") + ours
    py_mods = [m for m in (modules or []) if _PYTHON_MODULE.match(m)]
    hint = (f" The module system lists Python modules ({', '.join(py_mods[:4])}) but none matches this client's "
            f"Python {_client_python()}; uv fetches a matching one." if py_mods else
            " If the facility provides Python/uv via `module load`, say so and use that instead.")
    return (_BOOTSTRAP_UV_ENV_SETUP,
            f"env_setup: {skip}neither a working globus-compute-endpoint nor uv found — proposed installing uv into "
            "$HOME/.local/bin (astral.sh installer, ~20 MB) and then the endpoint into a venv at this client's Python, "
            "checked each time the endpoint starts and rebuilt when it fails; needs outbound HTTPS from the login node."
            + hint + ours)


def _allocation(f: dict) -> tuple[str | None, Literal["sbank", "iris", "mybalance"] | None, str | None]:
    """Detect an allocation-listing command; only `mybalance` has a parser today."""
    if f.get("MYBALANCE"):
        return "mybalance", "mybalance", None
    if f.get("XDUSAGE"):
        return "xdusage", None, ("allocation: found `xdusage` but no parser is implemented for it; "
                                 "left the parser unset — you'll pass account= directly.")
    return None, None, None
