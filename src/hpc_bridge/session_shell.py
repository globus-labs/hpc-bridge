from __future__ import annotations

import base64
import re
import secrets
import shlex
from dataclasses import dataclass

_VALID_SESSION_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Scheduler/runtime-injected vars that must never be carried across calls: they belong to
# the *current* allocation, so persisting them would freeze a past job/node and replay it
# (e.g. `$SLURM_JOB_ID` reporting a stale block). Expressed as a bash `case` glob over
# variable NAMES (applied in wrap() below). Belt-and-suspenders next to the ambient-diff:
# this also self-heals a `.env` already dirtied by an older build.
# NB: HOSTNAME (scheduler/compute-node injected) is filtered, but NOT bare HOST — that's a
# common user var (e.g. HOST=0.0.0.0 for a dev server) we must let the session persist.
_VOLATILE_NAME_GLOBS = "SLURM*|HOSTNAME|PBS_*|PMI_*|OMPI_*|FLUX_*"

# -- the wrong-block guard ---------------------------------------------------------------------------------------------
# The scheduler job a worker runs in: Slurm's or PBS's job id, empty on a LocalProvider block. The canary reports this
# SAME expression as HPCB_JOB (runner._CANARY_CMD), which is where a block's `block_job` comes from. NB a worker that a
# PBS site starts on a SISTER node (mpiexec / pbsdsh) may not inherit PBS_JOBID: there the expression is empty and a
# command runs unguarded, as on a block with no job — the canary, which can land on any worker, says the same.
JOB_ID_EXPR = "${SLURM_JOB_ID:-${PBS_JOBID:-}}"
# The block's remaining walltime, read from inside it (batch phase 1, spike S8): sets `__hb_rem` to whole seconds, or
# leaves it empty (unknown). Slurm: SLURM_JOB_END_TIME, exact with no RPC; else `squeue -h -j <job> -o %L` (+1 s: it
# rounds down). PBS: `qstat -f` stime + Resource_List.walltime (never resources_used.walltime, which lags). The block's
# END time is cached once per block on node-local ${TMPDIR:-/tmp}, so the next task there asks nothing. No job id (the
# login shape) is unknown — `squeue -j ""` answers rc 0 and nothing. POSIX sh plus bash's $(…); bash 3.2 runs it too.
REMAINING_SH = (
    '__hb_rem=""; __hb_j="${SLURM_JOB_ID:-${PBS_JOBID:-}}"; '
    'if [ -n "$__hb_j" ]; then '
    '__hb_now=$(date +%s); '
    '__hb_c="${TMPDIR:-/tmp}/hpcb-end-$(printf %s "$__hb_j" | tr -c "A-Za-z0-9._-" _)"; '
    '__hb_end="${SLURM_JOB_END_TIME:-}"; '
    'case "$__hb_end" in ""|*[!0-9]*) __hb_end=$(cat "$__hb_c" 2>/dev/null);; esac; '
    'case "$__hb_end" in ""|*[!0-9]*) __hb_end="";; esac; '
    'if [ -z "$__hb_end" ] && [ -n "${SLURM_JOB_ID:-}" ]; then '
    '__hb_s=$(squeue -h -j "$SLURM_JOB_ID" -o %L 2>/dev/null | head -n 1 | awk \'{ s=$1; d=0; '
    'if (index(s,"-")) { d=substr(s,1,index(s,"-")-1); s=substr(s,index(s,"-")+1) } '
    'if (s !~ /^[0-9:]+$/ || d !~ /^[0-9]*$/) exit 1; n=split(s,a,":"); t=0; for (i=1;i<=n;i++) t=t*60+a[i]; '
    'print d*86400+t }\'); '
    'case "$__hb_s" in ""|*[!0-9]*) ;; *) __hb_end=$((__hb_now + __hb_s + 1));; esac; fi; '
    'if [ -z "$__hb_end" ] && [ -n "${PBS_JOBID:-}" ]; then '
    '__hb_q=$(qstat -f "$PBS_JOBID" 2>/dev/null); '
    '__hb_st=$(printf "%s\\n" "$__hb_q" | sed -n "s/^ *stime = //p" | head -n 1); '
    '__hb_w=$(printf "%s\\n" "$__hb_q" | sed -n "s/^ *Resource_List.walltime = //p" | head -n 1 | '
    'awk -F: \'NF==3{print $1*3600+$2*60+$3}\'); '
    '__hb_t0=$(date -d "$__hb_st" +%s 2>/dev/null); '
    'case "$__hb_t0$__hb_w" in ""|*[!0-9]*) ;; *) __hb_end=$((__hb_t0 + __hb_w));; esac; fi; '
    'case "$__hb_end" in ""|*[!0-9]*) ;; *) printf %s "$__hb_end" > "$__hb_c" 2>/dev/null; '
    '__hb_rem=$((__hb_end - __hb_now));; esac; '
    'fi'
)
# A command sent for one block must not run on another. The block the user confirmed spend for can be replaced
# (released, cancelled, preempted) between two commands, and a command sent while its last proof of life is fresh
# (CANARY_TTL_S) is not checked first, so it would run on the replacement unasked (live 2026-10-13: job 112 scancel'd,
# `sleep 75` ran on job 113). So the wrapper refuses first: when the worker's job is set and is not the expected one, it
# prints ONE line, `HPCB_WRONG_BLOCK <nonce> <job>`, to stderr and exits 86 — before the cd, the session state and the
# command. POSIX sh, so it holds whatever the rest of the wrapper needs.
#
# Read back by the line carrying THIS dispatch's nonce, wherever it sits on stderr (`refusal`). Other lines come before
# it — a shell's start-up (a setlocale warning, BASH_ENV, a conda libtinfo note) — and after it: the SDK's own sandbox
# warning on an endpoint without task sandboxing (a facility's, NCSA Delta's) is flushed AFTER the child's output, and a
# `set -x` (`+ exit 86`) or an EXIT trap prints after the marker too. A first-line rule lost the refusal to the first
# kind, a last-line rule to the second. The nonce, not the position, is what output cannot forge: a command never saw
# it (it would have to read its own wrapper, $BASH_EXECUTION_STRING, on purpose).
WRONG_BLOCK_MARKER = "HPCB_WRONG_BLOCK"
WRONG_BLOCK_EXIT = 86
_GUARD_TEMPLATE = (
    f"__hb_job={JOB_ID_EXPR}\n"
    'if [ -n "$__hb_job" ] && [ "$__hb_job" != @@EXPECT@@ ]; then\n'
    f"  printf '{WRONG_BLOCK_MARKER} @@NONCE@@ %s\\n' \"$__hb_job\" >&2; exit {WRONG_BLOCK_EXIT}\n"
    "fi\n"
)
_NONCE = re.compile(r"[0-9a-f]{16,64}")
# What a job id may hold before it is named in a notice: Slurm (`123`, `123_4`, `123+0`) and PBS (`123.server.domain`,
# `123[4].server`) ids, starting with a digit as the canary's do, and bounded. Anything else is "a job hpc-bridge cannot
# read", never echoed.
_JOB_ID = re.compile(r"[0-9][A-Za-z0-9._+:\[\]-]{0,95}")


@dataclass(frozen=True)
class Guard:
    """The wrong-block guard of ONE dispatch: the block job it must run in, and a fresh nonce only its refusal
    carries. The server keeps it with the dispatch (and a poll handle, TaskHandle.guard) to read the result back."""

    job: str
    nonce: str

    def __post_init__(self) -> None:
        if not _NONCE.fullmatch(self.nonce):  # spliced into the guard's printf format: hex only
            raise ValueError(f"invalid guard nonce {self.nonce!r}")

    @classmethod
    def new(cls, job: str) -> Guard:
        return cls(job, secrets.token_hex(8))


@dataclass(frozen=True)
class Refusal:
    """A guarded command the worker refused: it did NOT run. `job`: the job it reached, or None when that id is not one
    hpc-bridge can read (then it is never put in a notice)."""

    job: str | None


def _guard(guard: Guard | None) -> str:
    if guard is None:
        return ""
    return _GUARD_TEMPLATE.replace("@@EXPECT@@", shlex.quote(guard.job)).replace("@@NONCE@@", guard.nonce)


def guard_prefix(guard: Guard | None) -> str:
    """The wrong-block guard as a script's first lines (a batch task's wrapper carries it too)."""
    return _guard(guard)


def refusal(exit_code: int | None, stderr: str, guard: Guard) -> Refusal | None:
    """The guard's refusal of this dispatch, or None when the command ran: exit 86, and a stderr line — any — that is
    `HPCB_WRONG_BLOCK <this nonce> <job>`. A command that exits 86 by itself, or prints a marker without the nonce, is
    an ordinary result."""
    if exit_code != WRONG_BLOCK_EXIT:
        return None
    head = f"{WRONG_BLOCK_MARKER} {guard.nonce} "
    line = next((ln.strip() for ln in stderr.splitlines() if ln.strip().startswith(head)), None)
    if line is None:
        return None
    job = line[len(head):].strip()
    if job == guard.job:  # the guard never refuses the job it expects
        return None
    return Refusal(job if _JOB_ID.fullmatch(job) else None)


@dataclass(frozen=True)
class Session:
    """A persistent shell session backed by cwd/env files on the shared filesystem.

    `root` is a directory the worker can read/write (e.g. $SCRATCH on HPC, or
    ~/.hpc-bridge locally). Each session keeps its working directory and exported
    environment in `<root>/sessions/<session_id>/{.cwd,.env}` so they survive the
    fresh-subprocess model of Globus Compute ShellFunction.

    `session_id` comes from an untrusted MCP parameter, so it is validated against a
    strict allowlist to prevent path traversal (`../`) and shell-metacharacter abuse.
    """

    session_id: str
    root: str

    def __post_init__(self) -> None:
        if not _VALID_SESSION_ID.match(self.session_id):
            raise ValueError(
                f"invalid session_id {self.session_id!r}: must match [A-Za-z0-9_-]{{1,64}}"
            )

    @property
    def state_dir(self) -> str:
        return f"{self.root}/sessions/{self.session_id}"

    def quoted_state_dir(self) -> str:
        """The state dir as a shell word safe to splice into the wrapper.

        A root may be written relative to the worker's home — ``$HOME/...``, ``${HOME}/...`` or
        ``~/...`` — for a facility whose local username we CANNOT know client-side (a multi-user
        endpoint maps our Globus identity to a local account the facility owns; the SSH path, by
        contrast, resolves ``{user}`` from the login name). Quoting such a root whole would make
        ``'$HOME/...'`` a literal string — no expansion, so ``mkdir`` fails — so we emit
        ``"$HOME"'/rest'``: the variable expands on the worker, the remainder stays quoted.
        Anything else is quoted whole, exactly as before.
        """
        for prefix in ("$HOME/", "${HOME}/", "~/"):
            if self.state_dir.startswith(prefix):
                return '"$HOME"' + shlex.quote("/" + self.state_dir[len(prefix):])
        return shlex.quote(self.state_dir)


_WRAP_TEMPLATE = r"""umask 077
mkdir -p @@SD@@
__hb_cwd=$(cat @@SD@@/.cwd 2>/dev/null)
if [ -n "$__hb_cwd" ]; then cd "$__hb_cwd" 2>/dev/null || cd @@SD@@; else cd @@SD@@; fi
# Fingerprint each exported var as ONE line "NAME=<base64 value>" so the diff below is
# record-safe: a line-oriented diff on raw `export -p` corrupts multi-line values (a mutated
# multi-line var leaves an orphan line that breaks the next `. .env`, silently dropping the
# whole session env). base64 keeps any value on a single line.
__hb_snap() {
  local __hb_k
  for __hb_k in $(compgen -A export); do
    printf '%s=%s\n' "$__hb_k" "$(printf %s "${!__hb_k}" | base64 | tr -d '\n')"
  done
}
# No outer quotes: @@SD@@ may carry MIXED quoting ("$HOME"'/rest'), which double quotes would turn
# literal (breaking the baseline path -> the whole ambient env gets persisted). An assignment RHS
# doesn't word-split, so the bare form is safe for every quoting shlex/quoted_state_dir emits.
__hb_base=@@SD@@/.env.base.$$
__hb_snap > "$__hb_base"
[ -f @@SD@@/.env ] && . @@SD@@/.env 2>/dev/null
eval "$(printf %s @@B64@@ | base64 -d)"@@TEE@@
__hb_rc=$?
@@RESULT@@pwd > @@SD@@/.cwd
# Persist only vars the command added/changed vs the ambient baseline, dropping scheduler
# runtime vars (@@VOLATILE@@). `printf %q` keeps each record single-line and re-sourceable
# even for multi-line values, so the persisted .env can never corrupt the next source.
{
  for __hb_n in $(compgen -A export); do
    case "$__hb_n" in @@VOLATILE@@) continue;; esac
    __hb_v="$(printf %s "${!__hb_n}" | base64 | tr -d '\n')"
    grep -qxF "$__hb_n=$__hb_v" "$__hb_base" && continue
    printf 'export %s=%q\n' "$__hb_n" "${!__hb_n}"
  done
} > @@SD@@/.env 2>/dev/null || true
rm -f "$__hb_base"
exit $__hb_rc
"""


# The interactive result kept in the session dir (batch phase 1: poll_task by Globus task id, spike S2): the command's
# stdout passes through `tee` as it always did, its last 4 kB also go to `results/<key>/tail`, and its exit status to
# `results/<key>/rc` — readable after the service's 30-minute window and after a restart that lost the handle.
_RESULT_TEE = ' > >(tee >(tail -c 4096 > @@SD@@/results/@@KEY@@/tail 2>/dev/null))'
# An EXIT trap, so a command that calls `exit` itself (it runs in this shell) still leaves its status
_RESULT_RC = ("__hb_rcf=@@SD@@/results/@@KEY@@/rc\n"
              "trap 'printf \"%s\\n\" \"$?\" > \"$__hb_rcf\" 2>/dev/null' EXIT\n")
_RESULT_KEY = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def wrap(command: str, session: Session, *, guard: Guard | None = None, result_key: str | None = None) -> str:
    """Wrap a command so cwd and env persist across calls (invisible to the model).

    ``guard``: the scheduler job the command must run in (the confirmed block's) and this dispatch's nonce. On any
    other job the wrapper refuses before touching anything (the wrong-block guard above); with the job unset on the
    worker (a LocalProvider block), or no guard, it runs as before.

    The command is base64-encoded and decoded+`eval`'d in the *current* shell, so
    arbitrary shell (brace groups, quotes, ``${VAR}``) cannot textually break out of
    the wrapper, while ``cd``/``export`` inside it still affect the persisted cwd/env.

    Only env vars the command itself set/changed are persisted: we fingerprint the ambient
    (scheduler-injected) environment *before* layering the session's own vars, persist only
    what differs, and drop known runtime vars (``SLURM_*``/``HOSTNAME``/...). Otherwise the
    first command's values would freeze and be replayed into a later, *different* allocation,
    so ``echo $SLURM_JOB_ID`` would report a stale node/job. Fingerprinting per-var (not a
    line diff) and emitting with ``printf %q`` make this correct for multi-line values too.
    Runs under bash (Globus Compute ShellFunction executes via /bin/bash).
    """
    sd = session.quoted_state_dir()
    b64 = shlex.quote(base64.b64encode(command.encode()).decode())
    tee = rc = ""
    if result_key is not None:
        if not _RESULT_KEY.match(result_key):
            raise ValueError(f"invalid result key {result_key!r}")
        tee = _RESULT_TEE.replace("@@KEY@@", result_key)
        rc = _RESULT_RC.replace("@@KEY@@", result_key)
    head = f"mkdir -p {sd}/results/{result_key}\n{rc}" if result_key is not None else ""
    return _guard(guard) + head.replace("@@SD@@", sd) + (
        _WRAP_TEMPLATE.replace("@@TEE@@", tee).replace("@@RESULT@@", "")
        .replace("@@SD@@", sd)
        .replace("@@B64@@", b64)
        .replace("@@VOLATILE@@", _VOLATILE_NAME_GLOBS)
    )


def reset_command(session: Session, *, guard: Guard | None = None) -> str:
    """A command that clears the session's persisted cwd/env (fresh slate). ``guard``: as for `wrap` — a reset is
    work on the block too, so on a replacement it is refused and the state left as it was."""
    sd = session.quoted_state_dir()
    # also sweep any .env.base.* snapshot a command leaked by exiting mid-wrapper
    return _guard(guard) + f"rm -f {sd}/.cwd {sd}/.env {sd}/.env.base.*"
