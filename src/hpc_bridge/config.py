"""Configuration: every environment variable hpc-bridge reads, and the runtime tunables (split step 2,
2026-09-03). One place to read them, one `or None` idiom — an empty HPC_BRIDGE_ACCOUNT used to reach a
Slurm template as `account: ""` because callers disagreed on that (code-quality review D7). The vault's
Reference/Configuration.md is the human-readable twin of the accessors below.
"""
from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

from .catalog.search import PUBLIC_REGISTRY_INDEX


def env(name: str) -> str | None:
    """A stripped environment value, or None when unset/empty — the one idiom."""
    val = os.environ.get(name, "").strip()
    return val or None


def ssh_user() -> str | None: return env("HPC_BRIDGE_SSH_USER")
def ssh_key() -> str | None: return env("HPC_BRIDGE_SSH_KEY")
def ssh_host() -> str | None: return env("HPC_BRIDGE_SSH_HOST")
def account() -> str | None: return env("HPC_BRIDGE_ACCOUNT")
def partition() -> str | None: return env("HPC_BRIDGE_PARTITION")
def remote_venv() -> str | None: return env("HPC_BRIDGE_REMOTE_VENV")
def machine() -> str | None: return env("HPC_BRIDGE_MACHINE")
def endpoint_name() -> str | None: return env("HPC_BRIDGE_ENDPOINT_NAME")
def scratch() -> str | None: return env("HPC_BRIDGE_SCRATCH")


def search_index() -> str:
    """The registry to read: the built-in public one unless HPC_BRIDGE_SEARCH_INDEX overrides it."""
    return env("HPC_BRIDGE_SEARCH_INDEX") or PUBLIC_REGISTRY_INDEX


def omit_instructions() -> bool:
    """Suppress the MCP `instructions` guidance pointer. A host that already delivers the driving-hpc guidance by
    another route sets HPC_BRIDGE_OMIT_INSTRUCTIONS=1 — Claude Code does, in .mcp.json, because it loads the skill —
    so it gets no pointer (no duplication, no extra tokens). Every other host leaves it unset and gets the pointer,
    then fetches the full guidance resource on demand (the lazy, Claude-Code-like path)."""
    v = env("HPC_BRIDGE_OMIT_INSTRUCTIONS")
    return v is not None and v.strip().lower() not in ("", "0", "false", "no", "off")


def catalog_file() -> str | None:
    """A LOCAL catalog (seed-format YAML file or directory) that replaces the registry entirely — a dev/test seam:
    the agentic fake cluster's facility MEPs get fresh UUIDs per cluster and cannot live in the public registry, and a
    curator can rehearse a seed entry before ingesting it. Never set in normal use (the registry is the catalog)."""
    return env("HPC_BRIDGE_CATALOG_FILE")


def user_dir() -> Path:
    """The local globus_compute dir for the endpoint CLI subprocess (NOT the SDK's token store, which
    follows GLOBUS_COMPUTE_USER_DIR / ~/.globus_compute — see Reference/Configuration)."""
    return Path(env("HPC_BRIDGE_USER_DIR") or str(Path.home() / ".globus_compute"))


def plugin_data_dir() -> Path:
    return Path(env("CLAUDE_PLUGIN_DATA") or str(Path.home() / ".hpc-bridge"))


def release_attempts() -> int:
    return max(1, int(env("HPC_BRIDGE_RELEASE_ATTEMPTS") or "3"))


def release_backoff_s() -> float:
    return float(env("HPC_BRIDGE_RELEASE_BACKOFF_S") or "6")


def provisioning_release_attempts() -> int:
    """Release attempts when a block was REQUESTED but never confirmed running (stop-during-provisioning): the pilot's
    sbatch may not be in the scheduler yet, so a one-shot scancel finds nothing — keep polling for it to land and
    cancel it (× release_backoff_s ≈ a ~30 s window by default) rather than falsely reporting 'down' (issue: the
    spend_revoked race, 2026-09-05)."""
    return max(release_attempts(), int(env("HPC_BRIDGE_PROVISIONING_RELEASE_ATTEMPTS") or "6"))


def charge_factor() -> float:
    return _env_float("HPC_BRIDGE_CHARGE_FACTOR", 0.0)


def max_task_s() -> float:
    return _env_float("HPC_BRIDGE_MAX_TASK_S", 0.0)


def login_wait_s() -> float:
    """How long a tool call waits for a browser login to land before returning needs_login. Long enough
    for a real IdP round-trip (password + Duo), short enough to stay well under the flow's TTL and any
    MCP tool timeout (run_shell already blocks far longer)."""
    return _env_float("HPC_BRIDGE_LOGIN_WAIT_S", 90.0)


def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"{name} is required for the selected HPC_BRIDGE_MACHINE")
    return val

def _control_settings() -> tuple[str | None, int]:
    """ControlMaster socket dir + persist for SSH multiplexing — one authentication for the whole
    bootstrap+discovery. Shared by _slurm_facility and the discovery probe so they reuse ONE master
    (same user@host ⇒ same %C socket). HPC_BRIDGE_SSH_CONTROL_PERSIST=0 disables it (control_dir=None)."""
    try:
        persist = int((os.environ.get("HPC_BRIDGE_SSH_CONTROL_PERSIST", "60") or "60").strip())
    except ValueError:
        persist = 60
    if persist <= 0:
        return None, 60
    from .state import _state_dir

    cd = _short_control_dir(str(_state_dir() / "cm"))
    os.makedirs(cd, mode=0o700, exist_ok=True)
    os.chmod(cd, 0o700)  # the socket lets commands run on the master without re-auth
    return cd, persist

def _short_control_dir(preferred: str) -> str:
    if len(preferred) <= _CONTROL_PATH_BUDGET:
        return preferred
    for cand in (str(Path.home() / ".hpc-bridge" / "cm"), f"/tmp/hpcb-cm-{os.getuid()}"):
        if len(cand) <= _CONTROL_PATH_BUDGET:
            if os.path.lexists(cand):
                st = os.lstat(cand)  # lstat: a planted SYMLINK at our predictable name must not pass as ours (B-04)
                if stat.S_ISLNK(st.st_mode) or st.st_uid != os.getuid():
                    continue  # a shared-host squat on our predictable name: never put our socket in someone else's dir
            return cand
    return preferred  # nothing short exists; ssh will say so

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"hpc-bridge: ignoring invalid {name}={raw!r}; using {default}", file=sys.stderr)
        return default

def _env_mode(default: str = "batch") -> str:
    mode = os.environ.get("HPC_BRIDGE_PROFILE", default)
    if mode not in ("interactive", "batch"):
        print(f"hpc-bridge: ignoring invalid HPC_BRIDGE_PROFILE={mode!r}; using {default}", file=sys.stderr)
        return default
    return mode

def _env_endpoint_id() -> str | None:
    """An existing endpoint UUID to dispatch to instead of provisioning a local one.

    Set HPC_BRIDGE_ENDPOINT_ID to skip local provisioning entirely. Required on
    macOS/Windows, where globus-compute-endpoint (the local endpoint daemon) cannot
    run — the SDK dispatch path still reaches a remote/Linux endpoint by UUID.
    """
    eid = os.environ.get("HPC_BRIDGE_ENDPOINT_ID", "").strip()
    return eid or None


# Unix socket paths are capped (104 bytes on macOS, 108 on Linux). ssh expands the `%C` in our
# ControlPath to a 40-hex hash and checks the WHOLE path, so a long state dir fails every SSH with
# "ControlPath too long" before authentication (found on the stranger's walk with a deep temp dir;
# a long username under the plugin data dir gets there too). Keep the socket dir short.
_CONTROL_PATH_BUDGET = 100 - 1 - 40  # dir + '/' + %C must stay under the cap with margin


CANARY_TTL_S = 45.0  # trust a confirmed worker this long before re-canarying. Safe: an idle
# block needs >= max_idletime (default 600s) of SILENCE to release, so a worker seen <45s ago
# cannot have idle-released out from under us — except that parsl only resets its idle timer for a
# task one of its scaling passes sees, so short commands may not keep a block alive (vault: Concepts/Cost control.md).
TRANSIENT_CONFLICT_LIMIT = 3  # consecutive RESOURCE_CONFLICT dispatch refusals before we stop saying 'call again'
CANARY_TIMEOUT_S = 8.0  # a live worker answers in ~1-2s; a cold block blows past this -> not warm

# --- long-task submit/poll bounds (#21) ---
# The client blocks up to SYNC_WAIT_S for a task's result; a task still running past it is NOT cut —
# the caller gets a poll handle (poll_task) and the task runs on up to its ceiling. _runner_for clamps
# the effective wait strictly below the ceiling so a task finishing near the boundary still returns.
SYNC_WAIT_S = _env_float("HPC_BRIDGE_SYNC_WAIT_S", 120.0)
# A task's ceiling = the block walltime − this margin, so the worker kills it (exit 124) gracefully
# BEFORE the scheduler tears the block down (preserving the result). See _task_ceiling_s.
TASK_CEILING_MARGIN_S = 20.0

# A just-submitted pilot takes a beat to appear in squeue/qstat, so "no pilot" during the first ~45s
# of a block's cold-start is NORMAL, not a rejection. Only past this grace do we surface the rejection
# hint — else every healthy warm-up cries wolf (caught live on globus1, [#32]).
PROVISION_GRACE_S = 45.0


def _parse_hhmmss(s: str | None) -> int:
    """HH:MM:SS (also H:MM:SS / MM:SS / SS) -> seconds. Deterministic and total: returns 0 on anything
    missing or malformed so callers fall back rather than crash; never negative."""
    if not s:
        return 0
    text = str(s).strip()
    days = 0
    if "-" in text:  # Slurm's "days-hours[:minutes[:seconds]]" (a 2-day walltime parsed as 0 -> a 300 s ceiling)
        d, _, text = text.partition("-")
        if not d.isdigit() or not text:
            return 0
        days = int(d)
        parts = text.split(":")
        if not 1 <= len(parts) <= 3 or not all(p.strip().isdigit() for p in parts):
            return 0
        parts = parts + ["0"] * (3 - len(parts))  # after a day count the first field is HOURS
    else:
        parts = text.split(":")
        if not 1 <= len(parts) <= 3 or not all(p.strip().isdigit() for p in parts):
            return 0
        if len(parts) == 1:  # Slurm: a bare number is MINUTES (`--time=30`), not seconds (review 2)
            return int(parts[0]) * 60
    secs = 0
    for p in parts:
        secs = secs * 60 + int(p)
    return days * 86400 + secs

def _task_ceiling_s(uec: dict) -> float:
    """The per-task kill ceiling (seconds) passed to the runner as the ShellFunction walltime: the block
    walltime minus a margin (so a task dies with a 124 result just BEFORE the scheduler reclaims the
    block), optionally capped by HPC_BRIDGE_MAX_TASK_S (unset = the full block walltime — the
    deterministic default). Falls back to a safe non-zero value when the block walltime is absent."""
    block_s = _parse_hhmmss(uec.get("walltime"))
    ceiling = block_s - TASK_CEILING_MARGIN_S
    if ceiling <= 0:  # missing/tiny walltime (e.g. LocalFacility has none) -> a safe default
        ceiling = max(SYNC_WAIT_S + TASK_CEILING_MARGIN_S, 300.0)
    cap = max_task_s()
    if cap > 0:
        ceiling = min(ceiling, cap)
    return float(ceiling)
