"""Batch phase 1 (vault `Planned/Batch and swarm execution.md` §10): many independent tasks fanned out and collected
through the session's ONE confirmed compute block, instead of an agent looping run_shell.

- **The window.** At most `capacity` tasks are out at once: the block's workers less one kept for interactive work
  (`max(1, workers - 1)`; an unknown count is 1 until a result reports PARSL_WORKER_COUNT). Nothing is retried or
  refilled on a mere presumption of loss, so the window never overfills the interchange.
- **The admission gate.** A task is admitted only while the block's remaining walltime, read from INSIDE the block (the
  canary and every result: SLURM_JOB_END_TIME, `squeue %L`, PBS stime + walltime), is at least task_walltime_s + the
  margin; unknown admits nothing. Past it the batch pauses at needs_confirmation: a task admitted as the block ends
  waits at the interchange, and parsl starts a block nobody confirmed (#204's pattern).
- **No new spend semantics.** The batch rides the block the user confirmed (ensure_endpoint_up's floor, or
  run_batch(confirm_spend=True), the same floor). A block that ends, a reap, a stop or a server restart pauses the
  batch at needs_confirmation; only run_batch(batch_id=…, confirm_spend=True) resumes it. A plain status never does.
- **The ledger** (hpc_bridge.ledger) is the record: the rows, every attempt, its Globus task id and settlement, the
  lease (one admitter and collector per batch), the retired configs.
- **The template.** `{name}` is replaced by a REFERENCE to an environment variable holding the row's value
  (`"$HPCB_F_name"`), never by the value: a value is data, whatever it holds. A placeholder inside quotes or a
  here-document is refused (write it bare: each value reaches the command as one word).
- **The wrapper** around each task (`task_script`) works in the batch's own directory, `<results_dir>/<batch id>/`:
  it checks free space, then claims `tasks/<i>/attempt-<n>/` with an atomic mkdir (only an existing attempt is a
  duplicate — a retried POST that ran twice exits early; any other failure fails the row with its cause), caps each
  stream at the source, runs the command in its own process group under its own timeout (rc 124) and a watch for the
  batch's `CANCELLED` marker (rc 143: an abort), ends what is left of that group when the command exits, and prints ONE
  small `HPCB_RESULT {json}` line — the output stays in files, never in the result (no 10 MB limit).
- **Settling.** A result wins whenever it arrives. On an SSH endpoint `lost` needs evidence (a ManagerLost/WorkerLost
  result, or sacct saying the block's job ended); on a MEP `presumed_lost` is a clock that starts at evidence a block
  started, keeps its window slot and is never retried. Retries only for idempotent batches, and only on evidence. Any
  submit error leaves its row `submit_unknown`: reconciled against the batch's own task group before an idempotent
  row is sent again; a non-idempotent one never is.
"""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import os
import re
import secrets
import shlex
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from . import session_shell, warmth
from .config import _parse_hhmmss, _task_ceiling_s
from .context import AppCtx, ShapeRuntime, _has_login_shape
from .ledger import (
    HOLDING,
    TERMINAL,
    Ledger,
    LedgerError,
    NotLeaseHolder,
    Owner,
    RetiredConfig,
    UnknownBatch,
)
from .models import (
    BatchCounts,
    BatchStatus,
    BatchTaskResult,
    BlockLeft,
    CancelResult,
    EndpointStatus,
    ShellOutcome,
)

MARGIN_S = float(os.environ.get("HPC_BRIDGE_BATCH_MARGIN_S", "") or 30.0)
OUTPUT_CAP_BYTES = 1_000_000          # per stream, at the source
TAIL_BYTES = 4096                      # stdout's tail in the result
STDERR_TAIL_BYTES = 1024
MIN_FREE_MB = int(os.environ.get("HPC_BRIDGE_BATCH_MIN_FREE_MB", "") or 256)
PRESUME_GRACE_S = 600.0                # a MEP task presumed lost: block evidence + task_walltime_s + this
MAX_ROWS = 10_000
MAX_COMMAND = 64 * 1024
MAX_WAIT_S = 60.0
KILL_GRACE_S = 5
READER_GRACE_S = 2                     # after the leftover group is killed: the stream readers' time to reach EOF
TICK_S = 2.0
IDLE_TICK_S = 10.0
EVIDENCE_EVERY_S = 30.0                # how often the block's job is looked up while tasks are out (SSH)
EVIDENCE_AFTER_S = 20.0                # …once the oldest task out is older than this
REST_EVERY_S = 10.0                    # after a restart: how often in-flight rows are pulled by REST
UNKNOWN_SETTLE_S = 120.0              # an unknown submit's POST is long over this far past its failure
CALL_BUDGET_S = 100.0                  # one status / cancel call's passes, past its wait_s (under every client timeout)
MARKER_WAIT_S = 30.0                   # how long a MEP cancel waits for its marker task to answer
MARKER_MIN_REMAINING_S = 60.0          # a MEP block with less walltime left gets no marker task (it may be ending)
RESULT_MARK = "HPCB_RESULT"
CANCEL_MARKER = "CANCELLED"
CANCEL_RC = 143
TIMEOUT_RC = 124
NO_SPACE_RC = 75

_ID = re.compile(r"^batch-[0-9a-f]{12}$")
# `{name}` — never `${name}`, which is the shell's own parameter expansion
_FIELD = re.compile(r"(?<!\$)\{([A-Za-z_][A-Za-z0-9_]*)\}")
FIELD_VAR = "HPCB_F_"   # a row's value `name` reaches its command as the environment variable HPCB_F_<name>
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
# A results directory hpc-bridge splices into the wrapper: plain path characters, optionally under $HOME / ${HOME} / ~.
_RESULTS_DIR = re.compile(r"^(\$HOME/|\$\{HOME\}/|~/|/)[A-Za-z0-9_./+-]{0,400}$")
_LOST = re.compile(r"ManagerLost|WorkerLost|lost its worker|worker died|manager .*lost", re.I)
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Set by the server at import (no import cycle): the free login-shape channel (`server._login_runner`), for the
# CANCELLED marker and the block's job state on an SSH endpoint.
login_runner: Callable[[AppCtx], Callable[[str], Awaitable[ShellOutcome]]] | None = None


def _now() -> float:
    return time.time()


class BatchRefusal(ValueError):
    """run_batch's arguments cannot make a batch (the reason is the refusal's text)."""


# ---- the template ----------------------------------------------------------------------------------------------------

def _text(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, dict | list):
        return json.dumps(v)
    return str(v)


def row_fields(row: Any, index: int) -> dict[str, Any]:
    """The names a row binds: a dict row's own keys, a scalar row as `row`; and `index` (unless the row names it)."""
    fields = {str(k): v for k, v in row.items()} if isinstance(row, dict) else {"row": row}
    fields.setdefault("index", index)
    return fields


# ---- where a placeholder stands: a conservative reading of bash's own grammar --------------------------------------
# The guarantee (review rounds 1 and 2): the OUTER shell never parses a row's value as code. A value is passed as an
# environment variable and its placeholder becomes `"$HPCB_F_name"` — which is data only where it is a bare word of a
# command the outer shell runs. So a placeholder is substituted ONLY where this reader is certain of that, and refused
# wherever it is not: inside quotes or a here-document (bash would read the inserted quotes as closing its own), inside
# arithmetic, `[[ ]]`, `${…}`, an array subscript or backticks (bash evaluates those), as the code argument of a shell
# (`sh -c`), an interpreter (`python -c`, `perl -e`), `awk`/`sed`'s program, `ssh`/`watch` (a remote or inner shell),
# or in a command that reads its arguments as code, names or arithmetic (`eval`, `trap`, `let`, `read`, `declare`…),
# and after any `#` this reader may judge otherwise than bash does.

_BLANK = " \t"
_META = ";&|()<>"
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIRECT_REF = re.compile(r"\$\{?" + "HPCB_F_")
_SHELLS = frozenset({"sh", "bash", "zsh", "dash", "ksh", "mksh", "ash", "yash", "fish", "csh", "tcsh", "busybox",
                     "rbash", "posh", "oksh", "su", "flock", "script", "nsenter", "chroot"})
_INTERP_FLAGS: tuple[tuple[str, frozenset[str]], ...] = (
    ("python", frozenset({"-c"})), ("pypy", frozenset({"-c"})), ("perl", frozenset({"-e", "-E"})),
    ("ruby", frozenset({"-e"})), ("node", frozenset({"-e", "-p", "--eval", "--print"})),
    ("Rscript", frozenset({"-e"})), ("julia", frozenset({"-e", "--eval"})), ("php", frozenset({"-r"})),
    ("lua", frozenset({"-e"})), ("osascript", frozenset({"-e"})), ("pwsh", frozenset({"-c", "-command"})),
    ("powershell", frozenset({"-c", "-command"})), ("tclsh", frozenset()), ("deno", frozenset({"eval"})))
_EVAL_LIKE = frozenset({"eval", "trap", "let", "declare", "typeset", "local", "readonly", "export", "unset", "read",
                        "mapfile", "readarray", "getopts", "compgen", "complete", "bind", "alias", "fc", "coproc"})
_REMOTE = frozenset({"ssh", "rsh", "pdsh", "clush", "watch", "parallel", "sshpass", "mosh", "autossh"})
_AWK = frozenset({"awk", "gawk", "mawk", "nawk"})
_SED = frozenset({"sed", "gsed"})
_ARITH_TEST = frozenset({"-eq", "-ne", "-lt", "-le", "-gt", "-ge"})
_RESET_WORDS = frozenset({"then", "do", "else", "elif", "if", "while", "until", "!", "{", "}", "time", "fi", "done",
                          "esac", ";;"})


# Programs that read COMMANDS from their standard input whatever their operands (a value fed to one runs as code)
_STDIN_ALWAYS = frozenset({"dc", "ed", "ex", "at", "batch", "crontab", "parallel", "ssh", "rsh", "sqlite3", "mysql",
                           "psql", "gdb", "ftp", "expect", "m4", "octave", "gnuplot", "R", "su"})
# Shells and interpreters: they read their program from standard input when given no script file
_INTERPRETERS = ("python", "pypy", "perl", "ruby", "node", "nodejs", "Rscript", "php", "lua", "luajit", "tclsh",
                 "wish", "julia", "osascript", "pwsh", "powershell", "deno", "bun")
_STDIN_FILES = frozenset({"-", "/dev/stdin", "/dev/fd/0", "/proc/self/fd/0"})
# their options that take the next word as an argument (so it is no script file)
_OPT_ARG = frozenset({"-W", "-X", "-I", "-M", "-r", "--require", "-o", "+o", "-O", "+O", "-C", "--input-type"})
# options that give the program in an argument (`-c` code, `-e` code, `-m` a module): standard input is then data
_CODE_IN_ARG = frozenset({"-c", "-e", "-E", "-m", "-p", "--eval", "--print", "-r", "-command"})
_WRAPPERS = frozenset({"env", "sudo", "doas", "timeout", "nice", "nohup", "exec", "command", "builtin", "stdbuf",
                       "xargs", "srun", "mpirun", "mpiexec", "chrt", "taskset", "ionice", "setsid", "numactl",
                       "strace", "ltrace", "valgrind", "unbuffer", "caffeinate"})
_NUMERIC = re.compile(r"^[0-9][0-9.]*[smhd]?$|^0x[0-9a-fA-F]+$")
_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
# what `xargs` may run on stdin's items without this reader having to vouch for its options
_XARGS_SAFE = frozenset({"echo", "printf", "rm", "rmdir", "ls", "cat", "wc", "grep", "egrep", "fgrep", "mkdir", "touch",
                         "cp", "mv", "ln", "stat", "head", "tail", "gzip", "gunzip", "bzip2", "xz", "md5sum",
                         "sha1sum", "sha256sum", "basename", "dirname", "readlink", "realpath", "du", "chmod",
                         "true", "false", "uniq", "cut", "tr", "tee"})
_XARGS_OPT_ARG = frozenset({"-I", "-L", "-n", "-P", "-s", "-d", "-E", "-a", "--arg-file", "--delimiter", "--max-args",
                            "--max-procs", "--max-lines", "--max-chars"})


def _is_shell_or_interp(n: str) -> bool:
    return n in _SHELLS or any(n.startswith(p) for p in _INTERPRETERS)


def _reads_code_from_stdin(words: list[str]) -> str | None:
    """Why this simple command would read CODE from its standard input (a value piped or here-stringed into it runs),
    or None. Errs toward saying it does: a shell or interpreter with no script file, `xargs` running one or a command
    this reader cannot vouch for, `parallel`, `source /dev/stdin`, awk/sed `-f -`, and programs that read commands
    from stdin whatever their operands (`dc`, `ed`, `at`, `ssh`, `sqlite3`…)."""
    names = [_bare(w) for w in words]
    paths = [w.replace("'", "").replace('"', "").replace("\\", "") for w in words]
    for k, n in enumerate(names):
        rest = names[k + 1:]
        rest = [p if p in _STDIN_FILES else r for p, r in zip(paths[k + 1:], rest, strict=True)]
        if n in _STDIN_ALWAYS:
            return f"`{n}` reads commands from its standard input"
        if n in ("source", ".") and rest and rest[0] in _STDIN_FILES:
            return f"`{n} {rest[0]}` runs its standard input"
        if (n in _AWK or n in _SED) and any(rest[j] == "-f" and j + 1 < len(rest) and rest[j + 1] in _STDIN_FILES
                                            for j in range(len(rest))):
            return f"`{n} -f -` reads its program from standard input"
        if n == "xargs":
            if any(_is_shell_or_interp(w) for w in rest):
                return "`xargs` runs a shell or an interpreter on its standard input's items"
            j = 0
            while j < len(rest) and rest[j].startswith("-"):
                j += 2 if rest[j] in _XARGS_OPT_ARG else 1
            cmd = rest[j] if j < len(rest) else None
            if cmd is not None and cmd not in _XARGS_SAFE:
                return f"`xargs` runs `{cmd}` on its standard input's items, which this reader cannot vouch for"
            return None
        if _is_shell_or_interp(n):
            j = 0
            while j < len(rest) and (rest[j].startswith(("-", "+")) and rest[j] not in _STDIN_FILES):
                if rest[j] in _CODE_IN_ARG or (n in _SHELLS and re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", rest[j])):
                    return None   # its program is an argument (checked as such): standard input is data
                if n in _SHELLS and re.fullmatch(r"-[A-Za-z]*s[A-Za-z]*", rest[j]):
                    return f"`{n} -s` reads its program from standard input"
                j += 2 if rest[j] in _OPT_ARG else 1
            if j >= len(rest) or rest[j] in _STDIN_FILES:
                return f"`{n}` with no script file reads its program from standard input"
            return None
    return None


def _wrapper_slot(names: list[str]) -> str | None:
    """The wrapper (`env`, `sudo`, `timeout`, `xargs`…) whose options or command the word being written would be — no
    command word after its options yet — or None."""
    last = max((k for k, n in enumerate(names) if n in _WRAPPERS), default=None)
    if last is None:
        return None
    for w in names[last + 1:]:
        if w.startswith("-") or _ASSIGN.match(w) or _NUMERIC.match(w):
            continue
        return None   # the wrapped command's own word is there: what follows is its argument
    return names[last]


def _bare(word: str) -> str:
    w = word.replace("'", "").replace('"', "").replace("\\", "")
    return w.rsplit("/", 1)[-1]


def _first_operand_after(names: list[str], k: int, takes_arg: frozenset[str] = frozenset()) -> bool:
    """Whether every word after index `k` is an option (or an option's argument): the next operand is the one being
    written now."""
    j = k + 1
    while j < len(names):
        w = names[j]
        if w in takes_arg:
            if j + 1 == len(names):
                return False   # the word being written is that option's own argument
            j += 2
            continue
        if not w.startswith("-") or w == "-":
            return False
        j += 1
    return True


def _code_arg(words: list[str]) -> str | None:
    """Why the word being written after these (the current simple command's words so far) would be read as code — a
    shell's or an interpreter's program, a remote command, `eval`'s argument, a variable name or arithmetic — or
    None."""
    names = [_bare(w) for w in words]
    for n in names:
        if n in _EVAL_LIKE:
            return f"in a `{n}` command, which reads its arguments as shell code, a variable name or arithmetic"
        if n in _REMOTE:
            return f"in a `{n}` command, which hands its arguments to another shell as code"
    if "printf" in names and "-v" in names:
        return "in a `printf -v` command, whose variable name bash evaluates"
    if "printf" in names:
        k = len(names) - 1 - names[::-1].index("printf")
        if "--" not in names[k + 1:] and all(w.startswith("-") for w in names[k + 1:]):
            return "as printf's format (a value starting with `-v` would make printf assign a variable bash evaluates)"
    if (w := _wrapper_slot(names)) is not None:
        return (f"where `{w}` reads its options and the command it runs: a value there could be an option (`env -S …`) "
                "or the command itself")
    if {"test", "["} & set(names) and ({"-v", "-R"} & set(names) or _ARITH_TEST & set(names)):
        return "in a `test` with an integer or variable-name operator, whose operands bash may evaluate"
    if "wait" in names and "-p" in names:
        return "in a `wait -p` command, whose variable name bash evaluates"
    if any(n in _SHELLS for n in names):
        for k in range(len(names) - 1, -1, -1):
            if re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", names[k]) and _first_operand_after(names, k):
                return "as the code a shell runs (`-c`)"
    for n in names:
        for interp, flags in _INTERP_FLAGS:
            if n.startswith(interp) and flags:
                for k in range(len(names) - 1, -1, -1):
                    if names[k] in flags and _first_operand_after(names, k):
                        return f"as the program `{n} {names[k]}` runs"
    for k, n in enumerate(names):
        if n in _AWK and "-f" not in names[k + 1:] and _first_operand_after(names, k, frozenset({"-v", "-F"})):
            return f"as `{n}`'s program"
        if n in _SED:
            rest = names[k + 1:]
            if rest and rest[-1] in ("-e", "--expression"):
                return f"as `{n}`'s script"
            if "-e" not in rest and "-f" not in rest and "--expression" not in rest and _first_operand_after(names, k):
                return f"as `{n}`'s script"
    return None


@dataclass
class _Frame:
    kind: str                  # top | cmdsub | arith | arith2 | sub | param | cond | sq | dq | ansi | bt
    danger: str | None = None  # why a placeholder anywhere in it is refused (inherited by the frames it opens)
    depth: int = 0             # its own brackets' nesting, for its closer
    start: int = 0             # where its text begins
    words: list[str] = field(default_factory=list)   # top / cmdsub: the current simple command's words so far
    wstart: int | None = None                        # …and where the word being read began
    marks: list[int] = field(default_factory=list)   # …and the placeholders it substitutes (checked once it ends)
    stage: int = 0             # top / cmdsub: the current simple command's place in its pipeline (0: the first)
    herestr: bool = False      # …and whether it has a here-string (`<<<`)
    skip_word: bool = False    # …the next word is the here-string's data, not one of the command's words
    feeds_from_outer: bool = False   # a `>(…)`: its program reads the enclosing command's output


def _template_decisions(t: str, use: set[str]) -> tuple[dict[int, str | None], str | None, set[str]]:
    """Read the template as bash does — conservatively. Returns, per placeholder start, None (substitute: a bare word
    of the outer shell, read as data) or "keep" (escaped or in a comment: left as written) or the reason it is refused;
    a reason the whole template is refused (None when it is not); and the names substituted where a value starting
    with `-` could be read as an option (a word's start, no `--` before it in its command)."""
    n = len(t)
    stack: list[_Frame] = [_Frame("top")]
    escaped: set[int] = set()
    decisions: dict[int, str | None] = {}
    exposed: set[str] = set()
    whole: str | None = None
    uncertain: str | None = None
    pending: list[tuple[str, bool, bool]] = []   # here-documents opened on this line: (delimiter, strip tabs, quoted)

    def plain(f: _Frame) -> bool:
        return f.kind in ("top", "cmdsub")

    def end_word(f: _Frame, i: int) -> None:
        if f.wstart is not None:
            w = t[f.wstart:i]
            if f.skip_word:
                f.skip_word = False
            elif not (not f.words and _bare(w) in _RESET_WORDS):
                f.words.append(w)
            if _bare(w) == "case" and f.kind == "cmdsub":
                nonlocal uncertain
                uncertain = uncertain or "a `case` inside `$(…)`, whose `)` this reader may take for the end"
            f.wstart = None

    def close_cmd(f: _Frame, i: int, *, pipe: bool = False) -> None:
        """The simple command ended: what only its whole words show (a `test` whose operator comes after the
        placeholder; a pipeline stage or a here-string feeding a program that reads code from stdin) refuses the
        placeholders it substituted — for stdin, every one before it in this frame (a loop or a group feeds a pipe
        too)."""
        end_word(f, i)
        names = {_bare(w) for w in f.words}
        if f.marks and {"test", "["} & names and (_ARITH_TEST | {"-v", "-R"}) & names:
            for k in f.marks:
                decisions[k] = "in a `test` with an integer or variable-name operator, whose operands bash may evaluate"
        if f.words and (f.stage > 0 or f.herestr) and (why := _reads_code_from_stdin(f.words)):
            for k in [k for k, d in decisions.items() if d is None and f.start <= k < i]:
                decisions[k] = f"fed to a program's standard input that it reads as code ({why})"
        f.words, f.marks, f.herestr = [], [], False
        f.stage = f.stage + 1 if pipe else 0

    def outer_words() -> list[str]:
        for f in reversed(stack):
            if plain(f):
                return f.words
        return []

    def push(kind: str, i: int, why: str | None = None) -> None:
        stack.append(_Frame(kind, danger=stack[-1].danger or why, start=i))

    i = 0
    while i < n:
        f = stack[-1]
        ch = t[i]
        m = _FIELD.match(t, i) if ch == "{" and (i == 0 or t[i - 1] != "$") else None
        if m is not None and m.group(1) in use:
            if f.kind in ("sq", "dq", "ansi"):
                decisions[i] = "quotes"
            elif f.kind == "bt":
                decisions[i] = "backticks — write $(…) instead"
            elif f.danger:
                decisions[i] = f.danger
            elif plain(f) and uncertain:
                decisions[i] = uncertain
            elif plain(f):
                at_start = f.wstart is None
                if f.wstart is None:
                    f.wstart = i
                decisions[i] = _code_arg(f.words)
                if decisions[i] is None:
                    f.marks.append(i)
                    if at_start and "--" not in {_bare(w) for w in f.words}:
                        exposed.add(m.group(1))
            else:
                decisions[i] = "a place this reader cannot vouch for"
            i = m.end()
            continue
        if f.kind == "sq":
            if ch == "'":
                stack.pop()
            i += 1
            continue
        if f.kind in ("ansi", "bt"):
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if (f.kind == "ansi" and ch == "'") or (f.kind == "bt" and ch == "`"):
                stack.pop()
            i += 1
            continue
        if f.kind == "dq":
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == '"':
                stack.pop()
                i += 1
                continue
            if _DIRECT_REF.match(t, i) and (why := _code_arg(outer_words())):
                whole = whole or f"a row's variable is expanded inside a string {why}"
        if f.kind != "dq":
            # plain-like frames: top, cmdsub (with words, comments, here-documents), arith, sub, param, cond
            if ch == "\\" and i + 1 < n:
                escaped.update((i, i + 1))
                if plain(f) and f.wstart is None:
                    f.wstart = i
                i += 2
                continue
            if ch == "'" or (ch == "$" and t.startswith("$'", i)):
                if plain(f) and f.wstart is None:
                    f.wstart = i
                push("ansi" if ch == "$" else "sq", i)
                i += 2 if ch == "$" else 1
                continue
            if ch == '"':
                if plain(f) and f.wstart is None:
                    f.wstart = i
                push("dq", i)
                i += 1
                continue
            if f.kind == "cmdsub" and ch == ")" and f.depth == 0:
                end_word(f, i)
                reader = _reads_code_from_stdin(f.words) if f.feeds_from_outer and f.words else None
                close_cmd(f, i)
                stack.pop()
                if reader:   # `cmd > >(sh)`: the enclosing command's output is that program's code
                    parent = stack[-1]
                    for k in [k for k, d in decisions.items() if d is None and parent.start <= k < i]:
                        decisions[k] = f"fed to a program's standard input that it reads as code ({reader})"
                i += 1
                continue
            if f.kind == "arith" and ch == ")" and f.depth == 0 and t.startswith("))", i):
                stack.pop()
                i += 2
                continue
            if f.kind in ("arith2", "sub") and ch == "]" and f.depth == 0:
                stack.pop()
                i += 1
                continue
            if f.kind == "param" and ch == "}" and f.depth == 0:
                stack.pop()
                i += 1
                continue
            if f.kind == "cond" and t.startswith("]]", i) and (i == 0 or t[i - 1] in _BLANK + "\n"):
                stack.pop()
                i += 2
                continue
            if plain(f) and _DIRECT_REF.match(t, i) and (why := _code_arg(f.words)):
                whole = whole or f"a row's variable is expanded {why}"
        # openers (in dq too: `$(`, `$((`, `${`, `$[`, backticks)
        if ch == "`":
            if plain(f) and f.wstart is None:
                f.wstart = i
            push("bt", i, "backticks — write $(…) instead")
            i += 1
            continue
        if t.startswith("$((", i):
            if plain(f) and f.wstart is None:
                f.wstart = i
            push("arith", i, "arithmetic, which bash evaluates (a value there can run code)")
            i += 3
            continue
        if t.startswith("$(", i):
            if plain(f) and f.wstart is None:
                f.wstart = i
            push("cmdsub", i + 2)
            i += 2
            continue
        if t.startswith("${", i):
            if plain(f) and f.wstart is None:
                f.wstart = i
            push("param", i, "a `${…}` expansion, whose subscripts and offsets bash evaluates")
            i += 2
            continue
        if t.startswith("$[", i):
            push("arith2", i, "arithmetic, which bash evaluates (a value there can run code)")
            i += 2
            continue
        if f.kind == "dq":
            i += 1
            continue
        if not plain(f):
            # the nesting its own closer counts
            if f.kind == "arith" and ch == "(":
                f.depth += 1
            elif f.kind == "arith" and ch == ")":
                f.depth -= 1
            elif f.kind in ("arith2", "sub") and ch == "[":
                f.depth += 1
            elif f.kind in ("arith2", "sub") and ch == "]":
                f.depth -= 1
            elif f.kind == "param" and ch == "{":
                f.depth += 1
            elif f.kind == "param" and ch == "}":
                f.depth -= 1
            i += 1
            continue
        # ---- top / cmdsub: plain shell
        if t.startswith("((", i):
            push("arith", i, "arithmetic, which bash evaluates (a value there can run code)")
            i += 2
            continue
        if f.wstart is None and t.startswith("[[", i) and (i + 2 >= n or t[i + 2] in _BLANK + "\n"):
            push("cond", i, "a `[[ ]]` test, whose operands bash may evaluate as arithmetic")
            i += 2
            continue
        if ch == "[" and f.wstart is not None and _IDENT_RE.match(t[f.wstart:i]):
            push("sub", i, "an array subscript, which bash evaluates")
            i += 1
            continue
        if ch == "#" and f.wstart is None:
            j, folded = i - 1, False
            while j >= 1 and t[j] == "\n" and t[j - 1] == "\\" and (j - 1) in escaped:
                j, folded = j - 2, True
            subtle = folded or (j >= f.start and (j in escaped or t[j] in _META))
            eol = t.find("\n", i)
            eol = n if eol < 0 else eol
            if subtle and any(c in t[i:eol] for c in "'\"`\\$"):
                uncertain = uncertain or "a `#` this reader and bash may read differently (a comment or not)"
            for mm in _FIELD.finditer(t, i, eol):
                decisions.setdefault(mm.start(), "keep")
            i = eol
            continue
        if ch in "<>" and t.startswith("(", i + 1):
            # a process substitution: a program may read its output as code (`sh <(…)`) — refused inside
            end_word(f, i)
            push("cmdsub", i + 2, "a process substitution, whose output a program may read as code")
            stack[-1].feeds_from_outer = ch == ">"
            i += 2
            continue
        if t.startswith("<<<", i):
            end_word(f, i)
            f.herestr = f.skip_word = True
            i += 3
            continue
        if ch == "<" and t.startswith("<<", i) and not t.startswith("<<<", i):
            end_word(f, i)
            j = i + 2
            strip = j < n and t[j] == "-"
            j += 1 if strip else 0
            while j < n and t[j] in _BLANK:
                j += 1
            k = j
            while k < n and t[k] not in _BLANK + "\n" + _META:
                k += 1
            raw = t[j:k]
            quoted = any(c in raw for c in "'\"\\")
            delim = raw.replace("'", "").replace('"', "").replace("\\", "")
            if delim:
                pending.append((delim, strip, quoted))
            i = k
            continue
        if ch == "\n":
            close_cmd(f, i)
            i += 1
            for delim, strip, quoted in pending:
                while i < n:
                    e = t.find("\n", i)
                    eol = n if e < 0 else e
                    line = t[i:eol]
                    if (line.lstrip("\t") if strip else line) == delim:
                        i = eol
                        break
                    for mm in _FIELD.finditer(t, i, eol):
                        if mm.group(1) in use:
                            decisions[mm.start()] = "a here-document"
                    if not quoted and _DIRECT_REF.search(line):
                        whole = whole or ("a row's variable is expanded inside a here-document whose delimiter is not "
                                          "quoted: the value would become part of the text an inner program reads")
                    i = eol + 1
            pending = []
            continue
        if ch in _BLANK:
            end_word(f, i)
        elif ch == "&" and (t.startswith(">", i + 1) or t[i - 1:i] in (">", "<", "|")):
            end_word(f, i)   # a redirection (`&>`, `>&`, `2>&1`) or the `&` of `|&`: no new command
        elif ch in ";&|()":
            close_cmd(f, i, pipe=ch == "|" and t[i + 1:i + 2] != "|" and t[i - 1:i] != "|")
            if ch == "(":
                f.depth += 1
            elif ch == ")":
                f.depth -= 1
        elif ch in "<>":
            end_word(f, i)
        elif f.wstart is None:
            f.wstart = i
        i += 1
    for f in reversed(stack):
        if plain(f):
            close_cmd(f, n)
    if len(stack) > 1 or pending:
        whole = whole or "its quotes, brackets or here-documents do not close where this reader can tell"
    return decisions, whole, exposed


def _refusal_text(name: str, why: str) -> str:
    v = f"{FIELD_VAR}{name}"
    if why in ("quotes", "a here-document"):
        return (f"the command has {{{name}}} inside {why}: write it bare — e.g. `wc -l {{{name}}}`, not "
                f"`wc -l \"{{{name}}}\"` — each row's value reaches the command as ONE word, already quoted (it is the "
                f"environment variable {v}). For an inner program, give it the value as data: a here-document with a "
                f"QUOTED delimiter that reads it from the environment (`python3 - <<'EOF'` … `os.environ[\"{v}\"]`; "
                f"`bash <<'EOF'` … `\"${v}\"`), or as an argument (`bash -c '… \"$1\" …' _ {{{name}}}`). Never hand a "
                "value to eval, sh -c or an interpreter as code.")
    return (f"the command has {{{name}}} {why}: a row's value must stay data. Write it as a bare word of an ordinary "
            f"command; for an inner program pass it as an argument (`bash -c '… \"$1\" …' _ {{{name}}}`), write it to "
            f"a file and pass the file, or read {v} from the environment there (a here-document with a quoted "
            "delimiter). Never hand a value to eval, sh -c, an interpreter's stdin or a wrapper's options as code.")


def substitute(template: str, row: Any, index: int, names: set[str] | None = None) -> str:
    """The template with this row's fields in: `{name}` for each name the batch's rows define (`names`; default: this
    row's own) becomes `"$HPCB_F_name"`, a reference to the environment variable the wrapper sets to the value
    (`row_values`, `task_script(fields=…)`) — the value itself never enters the command's text. Substituted only where
    `_template_decisions` is certain the reference is a bare word the outer shell treats as data; refused (BatchRefusal,
    saying how to write it) everywhere else. Escaped (`\\{x}`) or commented placeholders and any other brace text stay
    as written (`${HOME}`, `awk '{print $1}'`). The result is plain shell; GlobusRunner.submit brace-escapes it for
    ShellFunction's `cmd.format()`."""
    fields = row_fields(row, index)
    use = set(fields) if names is None else names
    decisions, whole, _exposed = _template_decisions(template, use)
    if whole is not None:
        raise BatchRefusal(f"the command cannot be checked safe: {whole}. Never hand a row's value to eval, sh -c or "
                           "an interpreter as code; pass it as a bare word, an argument, or read from the environment "
                           "in a here-document with a quoted delimiter.")

    def one(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in use:
            return m.group(0)
        d = decisions.get(m.start(), "keep")
        if d == "keep":
            return m.group(0)
        if d is not None:
            raise BatchRefusal(_refusal_text(name, d))
        if name not in fields:
            raise BatchRefusal(f"row {index} has no field {name!r}, which the command uses")
        return f'"${FIELD_VAR}{name}"'

    return _FIELD.sub(one, template)


def dash_rows(template: str, rows: list[Any], names: set[str]) -> list[int]:
    """The rows whose value for a placeholder that starts a word (with no `--` before it in its command) begins with
    `-`: a command may read it as an option. run_batch warns of them (a negative number as an option's argument is
    legitimate, so it never refuses)."""
    _d, _w, exposed = _template_decisions(template, names)
    if not exposed:
        return []
    return [i for i, r in enumerate(rows)
            if any(_text(v).startswith("-") for k, v in row_fields(r, i).items() if k in exposed)]


def row_values(row: Any, index: int, names: set[str]) -> dict[str, str]:
    """The values this row's task gets as HPCB_F_<name> (the names its batch's command uses)."""
    fields = row_fields(row, index)
    return {k: _text(fields[k]) for k in sorted(names) if k in fields}


def _field_exports(fields: dict[str, str] | None) -> str:
    """The shell lines that set HPCB_F_<name> to each value, carried base64-encoded (a value is data: no character of
    it is ever read as shell), its trailing newlines kept."""
    lines = []
    for name, value in sorted((fields or {}).items()):
        if not _IDENT.match(name):
            raise ValueError(f"invalid field name {name!r}")
        v = f"{FIELD_VAR}{name}"
        b64 = base64.b64encode(value.encode()).decode()
        lines.append(f"{v}=\"$(printf %s '{b64}' | base64 --decode; printf x)\"; {v}=\"${{{v}%x}}\"; export {v}")
    return "\n  ".join(lines)


def validate(command: Any, rows: Any, task_walltime_s: Any) -> tuple[str, list[Any], float, set[str]]:
    """The arguments of a new batch, checked: (command, rows, task_walltime_s, the field names substituted)."""
    if not isinstance(command, str) or not command.strip():
        raise BatchRefusal("command is required: the shell command each row runs, with {name} for the row's fields")
    if len(command) > MAX_COMMAND:
        raise BatchRefusal(f"command is {len(command):,} characters; at most {MAX_COMMAND:,}")
    if not isinstance(rows, list) or not rows:
        raise BatchRefusal("rows is required: a non-empty list of objects (or of plain values, bound as {row})")
    if len(rows) > MAX_ROWS:
        raise BatchRefusal(f"{len(rows):,} rows; at most {MAX_ROWS:,} in one batch")
    names: set[str] = {"index"}
    for i, r in enumerate(rows):
        if isinstance(r, dict):
            for k, v in r.items():
                if not isinstance(k, str) or not _IDENT.match(k):
                    raise BatchRefusal(f"row {i}: field name {k!r} is not an identifier ([A-Za-z_][A-Za-z0-9_]*)")
                if not isinstance(v, str | int | float | bool) and v is not None:
                    raise BatchRefusal(f"row {i}: field {k!r} must be a string, a number or a boolean")
                if "\x00" in _text(v):
                    raise BatchRefusal(f"row {i}: field {k!r} holds a NUL character, which no shell variable can")
            names |= set(r)
        elif isinstance(r, str | int | float | bool):
            if "\x00" in _text(r):
                raise BatchRefusal(f"row {i} holds a NUL character, which no shell variable can")
            names.add("row")
        else:
            raise BatchRefusal(f"row {i} is {type(r).__name__}: a row is an object or a plain value")
    try:
        wall = float(task_walltime_s)
    except (TypeError, ValueError):
        raise BatchRefusal("task_walltime_s is required: the most seconds one row may run") from None
    if not wall > 0:
        raise BatchRefusal("task_walltime_s must be more than 0 seconds")
    for i, r in enumerate(rows):
        substitute(command, r, i, names)   # a field some rows lack is refused here, before anything is recorded
    return command, rows, wall, names


def quoted_dir(path: str) -> str:
    """A results directory as a shell word: `$HOME/…` / `~/…` expand on the worker, the rest is quoted."""
    for prefix in ("$HOME/", "${HOME}/", "~/"):
        if path.startswith(prefix):
            return '"$HOME"' + shlex.quote("/" + path[len(prefix):])
    return shlex.quote(path)


def batch_dir(results_dir: str, batch_id: str) -> str:
    """The batch's own directory under its results_dir: every attempt, and its CANCELLED marker, live there — so two
    batches that share a results_dir never read each other's claims or markers."""
    return f"{results_dir.rstrip('/')}/{batch_id}"


def under_home(path: str) -> bool:
    return path.startswith(("$HOME", "${HOME}", "~/")) or bool(re.match(r"^/(home|Users|global/homes)/", path))


# ---- the wrapper -----------------------------------------------------------------------------------------------------

_WRAPPER = r"""umask 077
exec 3>&1 4>&2 2>/dev/null
__hb_rd=@@RD@@
__hb_t="$__hb_rd/tasks/@@I@@"
__hb_a="$__hb_rd/tasks/@@I@@/attempt-@@N@@"
@@REMAINING@@
__hb_host=$(hostname 2>/dev/null)
__hb_out() {
  printf '@@MARK@@ {"index": @@I@@, "attempt": @@N@@, "job": "%s", "host": "%s", "workers": "%s"' \
    "$__hb_j" "$__hb_host" "${PARSL_WORKER_COUNT:-}" >&3
  printf ', "remaining_s": %s%s}\n' "${__hb_rem:-null}" "$1" >&3
}
__hb_fail() {
  __hb_out ', "rc": 1, "error_b64": "'"$(printf %s "$1" | base64 | tr -d '\n')"'"'
  exit 0
}
__hb_e=$(mkdir -p "$__hb_t" 2>&1) || __hb_fail "cannot create $__hb_t: $__hb_e"
__hb_free=$(df -Pk "$__hb_t" | awk 'NR==2{print $4}')
case "$__hb_free" in ''|*[!0-9]*) ;; *)
  if [ "$__hb_free" -lt @@MINKB@@ ]; then
    __hb_out ', "rc": @@NRC@@, "no_space": true, "free_kb": '"$__hb_free"; exit 0; fi;;
esac
if ! __hb_e=$(mkdir "$__hb_a" 2>&1); then
  [ -d "$__hb_a" ] && { __hb_out ', "duplicate": true'; exit 0; }
  __hb_fail "cannot claim $__hb_a: $__hb_e"
fi
if [ -e "$__hb_rd/@@CANCEL@@" ]; then
  echo @@CRC@@ > "$__hb_a/rc"; __hb_out ', "rc": @@CRC@@, "cancelled": true, "ran": false'; exit 0
fi
__hb_start=$(date +%s)
printf '{"index": @@I@@, "attempt": @@N@@, "job": "%s", "host": "%s", "start": %s, "remaining_s": %s}\n' \
  "$__hb_j" "$__hb_host" "$__hb_start" "${__hb_rem:-null}" > "$__hb_a/meta.json"
__hb_f="${TMPDIR:-/tmp}/hpcb-$$-@@I@@-@@N@@"
rm -f "$__hb_f.o" "$__hb_f.e" "$__hb_f.t" "$__hb_f.u" "$__hb_f.p" "$__hb_f.q"
mkfifo "$__hb_f.o" "$__hb_f.e" "$__hb_f.t" "$__hb_f.u" "$__hb_f.p" "$__hb_f.q"
tail -c @@TAIL@@ < "$__hb_f.t" > "$__hb_a/.stdout.tail" 3>&- 4>&- &
__hb_p1=$!
{ head -c @@CAP@@ > "$__hb_a/stdout"; wc -c | tr -d ' ' > "$__hb_a/.stdout.dropped"; } \
  < "$__hb_f.p" > /dev/null 3>&- 4>&- &
__hb_p2=$!
tail -c @@ETAIL@@ < "$__hb_f.u" > "$__hb_a/.stderr.tail" 3>&- 4>&- &
__hb_p3=$!
{ head -c @@CAP@@ > "$__hb_a/stderr"; wc -c | tr -d ' ' > "$__hb_a/.stderr.dropped"; } \
  < "$__hb_f.q" > /dev/null 3>&- 4>&- &
__hb_p4=$!
tee "$__hb_f.t" < "$__hb_f.o" > "$__hb_f.p" 3>&- 4>&- &
__hb_t1=$!
tee "$__hb_f.u" < "$__hb_f.e" > "$__hb_f.q" 3>&- 4>&- &
__hb_t2=$!
set -m
(
  export HPCB_ROW_INDEX=@@I@@ HPCB_ATTEMPT=@@N@@ HPCB_TASK_DIR="$__hb_a"
  @@SESSION@@
  @@FIELDS@@
  eval "$(printf %s @@B64@@ | base64 --decode)"
) > "$__hb_f.o" 2> "$__hb_f.e" < /dev/null 3>&- 4>&- &
__hb_c=$!
set +m
(
  __hb_dl=$((__hb_start + @@WALL@@))
  while :; do
    sleep 1
    if [ -e "$__hb_rd/@@CANCEL@@" ]; then echo cancelled > "$__hb_a/.why"; break; fi
    if [ "$(date +%s)" -ge "$__hb_dl" ]; then echo timeout > "$__hb_a/.why"; break; fi
    kill -0 "$__hb_c" || exit 0
  done
  kill -TERM -- "-$__hb_c" || kill -TERM "$__hb_c"
  __hb_k=0
  while [ "$__hb_k" -lt @@GRACE@@ ] && kill -0 "$__hb_c"; do sleep 1; __hb_k=$((__hb_k + 1)); done
  kill -KILL -- "-$__hb_c" || kill -KILL "$__hb_c"
) > /dev/null 3>&- 4>&- &
__hb_w=$!
wait "$__hb_c"
__hb_rc=$?
kill "$__hb_w"
kill -TERM -- "-$__hb_c"
( sleep @@GRACE@@; kill -KILL -- "-$__hb_c"; sleep @@RGRACE@@; kill "$__hb_t1" "$__hb_t2"; sleep @@RGRACE@@
  kill "$__hb_p1" "$__hb_p2" "$__hb_p3" "$__hb_p4" ) > /dev/null 3>&- 4>&- &
__hb_g=$!
wait "$__hb_t1" "$__hb_t2" "$__hb_p1" "$__hb_p2" "$__hb_p3" "$__hb_p4"
kill "$__hb_g"
[ -s "$__hb_a/.stdout.tail" ] || tail -c @@TAIL@@ "$__hb_a/stdout" > "$__hb_a/.stdout.tail"
[ -s "$__hb_a/.stderr.tail" ] || tail -c @@ETAIL@@ "$__hb_a/stderr" > "$__hb_a/.stderr.tail"
rm -f "$__hb_f.o" "$__hb_f.e" "$__hb_f.t" "$__hb_f.u" "$__hb_f.p" "$__hb_f.q"
__hb_why=$(cat "$__hb_a/.why")
case "$__hb_why" in timeout) __hb_rc=@@TRC@@;; cancelled) __hb_rc=@@CRC@@;; esac
echo "$__hb_rc" > "$__hb_a/rc"
@@REMAINING@@
__hb_ob=$(wc -c < "$__hb_a/stdout" | tr -d ' ')
__hb_od=$(cat "$__hb_a/.stdout.dropped")
case "$__hb_od" in ''|*[!0-9]*) __hb_od=0;; esac
__hb_tr=false; [ "$__hb_od" -gt 0 ] && __hb_tr=true
__hb_tl=$(base64 < "$__hb_a/.stdout.tail" | tr -d '\n')
__hb_el=$(base64 < "$__hb_a/.stderr.tail" | tr -d '\n')
__hb_r=", \"rc\": $__hb_rc, \"started\": $__hb_start, \"ended\": $(date +%s)"
__hb_r="$__hb_r, \"stdout_path\": \"$__hb_a/stdout\", \"stdout_bytes\": ${__hb_ob:-0}"
__hb_r="$__hb_r, \"stdout_dropped\": $__hb_od, \"truncated\": $__hb_tr, \"why\": \"$__hb_why\""
__hb_r="$__hb_r, \"tail_b64\": \"$__hb_tl\", \"stderr_tail_b64\": \"$__hb_el\""
__hb_out "$__hb_r"
exit 0
"""


def task_script(command: str, *, results_dir: str, index: int, attempt: int, task_walltime_s: float,
                session: session_shell.Session | None = None, cap_bytes: int = OUTPUT_CAP_BYTES,
                guard: session_shell.Guard | None = None, min_free_mb: int = MIN_FREE_MB,
                fields: dict[str, str] | None = None) -> str:
    """The script one batch task runs on the worker (see the module doc). `results_dir`: the batch's OWN directory
    (`batch_dir`). `session`: the cwd/env it replays READ-ONLY (never written back). `guard`: the wrong-block guard,
    refusing (exit 86) before anything is claimed. `fields`: the row's values, set as HPCB_F_<name> after the session's
    env (`row_values`)."""
    replay = ""
    if session is not None:
        sd = session.quoted_state_dir()
        replay = (f'__hb_cwd=$(cat {sd}/.cwd 2>/dev/null); [ -n "$__hb_cwd" ] && cd "$__hb_cwd"; '
                  f"[ -f {sd}/.env ] && . {sd}/.env")
    b64 = shlex.quote(base64.b64encode(command.encode()).decode())
    body = (_WRAPPER.replace("@@RD@@", quoted_dir(results_dir)).replace("@@I@@", str(int(index)))
            .replace("@@N@@", str(int(attempt))).replace("@@REMAINING@@", session_shell.REMAINING_SH)
            .replace("@@MARK@@", RESULT_MARK).replace("@@CANCEL@@", CANCEL_MARKER)
            .replace("@@CRC@@", str(CANCEL_RC)).replace("@@TRC@@", str(TIMEOUT_RC)).replace("@@NRC@@", str(NO_SPACE_RC))
            .replace("@@MINKB@@", str(int(min_free_mb) * 1024)).replace("@@CAP@@", str(int(cap_bytes)))
            .replace("@@SESSION@@", replay).replace("@@FIELDS@@", _field_exports(fields) or ":").replace("@@B64@@", b64)
            .replace("@@WALL@@", str(max(1, round(task_walltime_s)))).replace("@@GRACE@@", str(KILL_GRACE_S))
            .replace("@@RGRACE@@", str(READER_GRACE_S))
            .replace("@@TAIL@@", str(TAIL_BYTES)).replace("@@ETAIL@@", str(STDERR_TAIL_BYTES)))
    return session_shell.guard_prefix(guard) + body


@dataclass
class TaskReport:
    """One attempt's HPCB_RESULT line, parsed."""

    index: int
    attempt: int
    rc: int | None = None
    tail: str = ""
    stderr_tail: str = ""
    stdout_path: str | None = None
    stdout_bytes: int | None = None
    truncated: bool = False
    job: str | None = None
    host: str | None = None
    workers: int | None = None
    remaining_s: float | None = None
    started: float | None = None
    duplicate: bool = False
    cancelled: bool = False
    no_space: bool = False
    timed_out: bool = False
    error: str | None = None


def _b64(v: Any) -> str:
    try:
        return base64.b64decode(str(v or "")).decode(errors="replace")
    except (ValueError, TypeError):
        return ""


def _num(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int | float):
        return float(v)
    try:
        return float(str(v))
    except (TypeError, ValueError):
        return None


def parse_result(stdout: str | None) -> TaskReport | None:
    """The attempt's report: the LAST `HPCB_RESULT {json}` line of its stdout, or None."""
    for line in reversed(str(stdout or "").splitlines()):
        if line.startswith(RESULT_MARK + " "):
            try:
                d = json.loads(line[len(RESULT_MARK) + 1:])
            except ValueError:
                return None
            if not isinstance(d, dict) or not isinstance(d.get("index"), int):
                return None
            rc = d.get("rc")
            workers = _num(d.get("workers"))
            return TaskReport(
                index=int(d["index"]), attempt=int(d.get("attempt") or 0), rc=int(rc) if isinstance(rc, int) else None,
                tail=_b64(d["tail_b64"]) if "tail_b64" in d else str(d.get("tail") or ""),
                stderr_tail=_b64(d["stderr_tail_b64"]) if "stderr_tail_b64" in d else str(d.get("stderr_tail") or ""),
                stdout_path=d.get("stdout_path"), stdout_bytes=int(d["stdout_bytes"])
                if isinstance(d.get("stdout_bytes"), int) else None, truncated=bool(d.get("truncated")),
                job=str(d["job"]) if d.get("job") else None, host=d.get("host") or None,
                workers=int(workers) if workers else None, remaining_s=_num(d.get("remaining_s")),
                started=_num(d.get("started")), duplicate=bool(d.get("duplicate")),
                cancelled=bool(d.get("cancelled")) or d.get("why") == "cancelled",
                no_space=bool(d.get("no_space")), timed_out=d.get("why") == "timeout",
                error=_b64(d["error_b64"]) if d.get("error_b64") else d.get("error"))
    return None


# ---- pure rules ------------------------------------------------------------------------------------------------------

def admit_ok(remaining_s: float | None, task_walltime_s: float, margin_s: float = MARGIN_S) -> bool:
    """The admission gate: the block's remaining walltime is known and covers the task plus the margin."""
    return remaining_s is not None and remaining_s >= task_walltime_s + margin_s


def presumed_lost(*, admitted_at: float, block_seen_at: float | None, task_walltime_s: float, now: float,
                  grace_s: float = PRESUME_GRACE_S) -> bool:
    """MEP only: a task out with no result is presumed lost once task_walltime_s + the grace have passed since BOTH
    its admission and the first evidence a block started — never counted from the submit alone."""
    if block_seen_at is None:
        return False
    return now - max(admitted_at, block_seen_at) > task_walltime_s + grace_s


def uec_hash(rt: ShapeRuntime | dict) -> str:
    """The identity of the block request a task goes out on (its user endpoint config: on SSH its `hpcb_block` token
    makes each confirmation unique) — what a stop or a reap retires."""
    uec = rt if isinstance(rt, dict) else rt.user_endpoint_config
    return hashlib.sha256(json.dumps(uec, sort_keys=True, default=str).encode()).hexdigest()[:24]


def workers_of(rt: ShapeRuntime) -> int | None:
    w = rt.user_endpoint_config.get("max_workers_per_node")
    try:
        return int(w) if w is not None else None
    except (TypeError, ValueError):
        return None


def capacity_for(workers: int | None) -> int:
    """The window: the block's workers less one kept for interactive work; 1 when the count is unknown or 1."""
    return max(1, (workers or 1) - 1)


@dataclass
class RestResult:
    state: str              # success | failed | pending
    result: Any = None
    error: str = ""


def collect_rest(client: Any, task_ids: list[str]) -> dict[str, RestResult]:
    """Results by REST with the raw `v2.get_task_batch` (Client.get_batch_result drops a failed task silently: S4):
    task id -> success (its deserialized result), failed (the exception text) or pending."""
    out: dict[str, RestResult] = {}
    if not task_ids:
        return out
    r = client._compute_web_client.v2.get_task_batch(list(task_ids))
    results = (r.get("results") if hasattr(r, "get") else r["results"]) or {}
    ser = getattr(client, "fx_serializer", None)
    for tid in task_ids:
        d = results.get(tid) or results.get(str(tid))
        if not isinstance(d, dict):
            out[tid] = RestResult("pending")
            continue
        status = str(d.get("status") or "").lower()
        if "exception" in d or status == "failed":
            out[tid] = RestResult("failed", error=str(d.get("exception") or d.get("reason") or "failed"))
        elif status == "success" and "result" in d:
            try:
                res = ser.deserialize(d["result"]) if ser is not None else d["result"]
            except Exception as exc:  # noqa: BLE001 - an unreadable result is a failed collection, not a crash
                out[tid] = RestResult("failed", error=f"the result could not be read: {exc}")
                continue
            out[tid] = RestResult("success", result=res)
        else:
            out[tid] = RestResult("pending")
    return out


def _shell_fields(res: Any) -> tuple[int | None, str, str]:
    if isinstance(res, dict):
        return res.get("returncode"), str(res.get("stdout") or ""), str(res.get("stderr") or "")
    out, err = getattr(res, "stdout", "") or "", getattr(res, "stderr", "") or ""
    return getattr(res, "returncode", None), str(out), str(err)


# ---- this process's part of a batch ----------------------------------------------------------------------------------

@dataclass
class _Run:
    batch_id: str
    rt: ShapeRuntime | None
    runner: Any = None
    runner_hash: str | None = None     # the block config `runner` sends with (uec_hash at its build)
    old_runners: list[Any] = field(default_factory=list)   # earlier configs' runners, closed once nothing is out
    futures: dict[tuple[int, int], Any] = field(default_factory=dict)
    guards: dict[tuple[int, int], Any] = field(default_factory=dict)
    sent_on: dict[tuple[int, int], ShapeRuntime] = field(default_factory=dict)
    block_end_at: float | None = None
    block_job: str | None = None
    block_seen_at: float | None = None
    workers_seen: int | None = None
    waiting: str | None = None
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    loop: asyncio.Task[None] | None = None
    last_evidence: float = 0.0
    last_rest: float = 0.0
    last_unsent: float = 0.0
    last_unknown_look: float = 0.0
    adopted: bool = False
    # The user's yes (a resume with confirm_spend) covers the NEXT block while the one in front cannot take a row: on an
    # endpoint hpc-bridge stood up, once nothing of ours runs on it, the batch asks for a fresh block (`_maybe_switch`)
    next_block: bool = False
    ensure_up: Any = None
    last_warm: float = 0.0
    # what the block knowledge above is about: the confirmation it was learnt under (a new one forgets it)
    confirmed_at: float | None = None
    sent_job: dict[tuple[int, int], str | None] = field(default_factory=dict)
    # after a reap on SSH: the reaped block's user endpoint (`uep.<eid>.<uep id>`), whose relaunched pilots — parsl's,
    # for the task that was out on the dead block — this batch cancels for a while (no one confirmed them)
    old_uep: str | None = None
    old_uep_until: float = 0.0
    last_release: float = 0.0
    # one pass at a time (the background loop's and a status call's), and no admission while the next block is
    # being confirmed (`_maybe_switch` drops the shape, then awaits the new block's start)
    passing: asyncio.Lock = field(default_factory=asyncio.Lock)
    switching: bool = False
    # the config hash of the reaped block's user endpoint (`old_uep`): a block confirmed with the SAME config after the
    # reap goes to that same user endpoint, and is the user's, never released
    old_uep_hash: str | None = None
    # blocks of ours that can still bill though no shape holds them: one the next-block switch left to run out its
    # last minutes (job, the wall-clock end), and a block the facility started for a task of ours that reached it
    # after the confirmed one went (the wrong-block guard's job)
    switched_left: list[tuple[str | None, float | None]] = field(default_factory=list)
    relaunched: list[str] = field(default_factory=list)
    marker_future: Any = None          # a MEP abort's marker task, while it is out: never a second one
    marker_written: bool = False       # …and once it wrote the marker, none again
    groups_recorded: set[str] = field(default_factory=set)
    last_change: tuple[Any, ...] | None = None   # what status(wait_s) waits to see change

    def remaining(self) -> float | None:
        return None if self.block_end_at is None else self.block_end_at - _now()


def ledger_of(app: AppCtx) -> Ledger:
    if app.ledger is None:
        app.ledger = Ledger.open()
    led: Ledger = app.ledger
    return led


def owner_of(app: AppCtx) -> Owner:
    if app.batch_owner is None:
        app.batch_owner = Owner.here()
    own: Owner = app.batch_owner
    return own


def _journal(record: dict[str, Any]) -> None:
    """An event row in the server's journal (HPC_BRIDGE_JOURNAL): `event`, no `tool` — what the harness grades the
    window and the gate from."""
    path = os.environ.get("HPC_BRIDGE_JOURNAL", "").strip()
    if not path:
        return
    row = {"t": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "ts": round(_now(), 6), "pid": os.getpid(), **record}
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a") as fh:
            fh.write(json.dumps(row, default=str) + "\n")
    except OSError as exc:
        print(f"hpc-bridge: journal write failed ({exc})", file=sys.stderr)


def _note_block_from_canary(app: AppCtx, run: _Run, rt: ShapeRuntime) -> None:
    """What the shape's last check said of the block, from inside it: its job, its remaining walltime (aged by when
    it answered), its worker count — a first evidence a block started."""
    c = rt.last_canary
    if c is None or not c.ok:
        return
    if rt.block_job is not None and c.worker_job is not None and c.worker_job != rt.block_job:
        return
    if rt.confirmed_at is not None and c.answered_at is not None and c.answered_at < rt.confirmed_at:
        return   # an answer from before this block's confirmation: another block's
    age = max(0.0, time.monotonic() - c.answered_at) if c.answered_at else 0.0
    if c.worker_remaining_s is not None:
        run.block_end_at = _now() - age + c.worker_remaining_s
    if c.worker_job:
        run.block_job = c.worker_job
        if run.block_seen_at is None:
            run.block_seen_at = _now() - age
        try:   # a block the batch runs on (or was confirmed for): on the record, as a result naming it would put it
            ledger_of(app).add_block_seen(run.batch_id, c.worker_job)
        except LedgerError:
            pass
    if c.worker_count:
        run.workers_seen = c.worker_count


def _sync_block(app: AppCtx, run: _Run, rt: ShapeRuntime) -> None:
    """A new confirmation (a resume after a reap, a switch) is a new block: forget what was known of the old one."""
    if rt.confirmed_at == run.confirmed_at:
        return
    if run.confirmed_at is not None or run.block_job is not None:
        run.block_end_at = run.block_job = run.block_seen_at = None
        run.workers_seen = None
    run.confirmed_at = rt.confirmed_at
    _note_block_from_canary(app, run, rt)


def _batch_runner(app: AppCtx, rt: ShapeRuntime) -> Any:
    """The batch's own runner: the compute shape's config (the same user endpoint, so the same confirmed block) on an
    Executor of its own, whose task group the service mints at its first submit and whose results nobody else
    consumes (S1). A COPY of the config: the shape's own changes with the next confirmation (a fresh `hpcb_block`)."""
    eid = app.state.endpoint_id
    ceiling = _task_ceiling_s(rt.user_endpoint_config)
    uec = copy.deepcopy(getattr(app.facility, "dispatch_uec", lambda d: d)(rt.user_endpoint_config))
    return app.runner_factory(eid, user_endpoint_config=uec, walltime=ceiling, timeout=5.0)


def _runner_for_block(app: AppCtx, run: _Run, rt: ShapeRuntime) -> Any:
    """The batch's runner for the block the shape is confirmed for NOW: a new one when the shape's config changed (a
    new confirmation's fresh `hpcb_block` is a new user endpoint — tasks sent with the old config would go to the
    reaped block's user endpoint, live 2026-10-31). The old one is kept until its tasks out have settled."""
    h = uec_hash(rt)
    if run.runner is None or run.runner_hash != h:
        if run.runner is not None:
            run.old_runners.append(run.runner)
        run.runner, run.runner_hash = _batch_runner(app, rt), h
    return run.runner


def _close_old_runners(run: _Run) -> None:
    if run.futures or not run.old_runners:
        return
    for r in run.old_runners:
        try:
            r.close()
        except Exception:  # noqa: BLE001
            pass
    run.old_runners.clear()


def _counts(led: Ledger, bid: str) -> dict[str, int]:
    return led.counts(bid)


def _held(c: dict[str, int]) -> int:
    return sum(c.get(s, 0) for s in HOLDING)


def _out_now(c: dict[str, int]) -> int:
    """Tasks that may still be running: in flight or with a submit of unknown fate (a presumed one does not hold a
    stop, as today on a MEP)."""
    return c.get("in_flight", 0) + c.get("submit_unknown", 0)


# ---- pausing ---------------------------------------------------------------------------------------------------------

def pause(app: AppCtx, bid: str, reason: str) -> None:
    led = ledger_of(app)
    b = led.batch(bid)
    if b["state"] in ("done", "cancelled"):
        return
    if b["state"] != "needs_confirmation" or b["pause_reason"] != reason:
        led.update_batch(bid, state="needs_confirmation", pause_reason=reason)
        led.event(bid, "batch.pause", {"reason": reason})
        _journal({"event": "batch.pause", "batch_id": bid, "reason": reason})
    run = app.batches.get(bid)
    if run is not None:
        run.next_block = False   # any pause withdraws the user's yes for the next block: it is asked for again
        run.wake.set()


def pause_all(app: AppCtx, reason: str) -> list[str]:
    """Pause every batch this process admits for (stop_endpoint, before anything is released — callers hold
    app.lock). Returns their ids."""
    out = []
    for bid in list(app.batches):
        try:
            if ledger_of(app).batch(bid)["state"] == "admitting":
                pause(app, bid, reason)
                out.append(bid)
        except LedgerError:
            continue
    return out


def retire_all(app: AppCtx, rt: ShapeRuntime | None, why: str) -> None:
    """Retire the block config of every batch this process runs on `rt` (a stop released it, a reap found it gone)."""
    if rt is None or app.state.endpoint_id is None or not app.batches:
        return
    try:
        ledger_of(app).retire(app.state.endpoint_id, uec_hash(rt), why=why)
    except LedgerError:
        pass


def in_flight_note(app: AppCtx) -> str | None:
    """What of the batches this process runs holds the compute block, and how to end it — for stop_endpoint's busy
    refusal. None when nothing does. The ledger's batches on this endpoint count too, not only this process's: tasks
    a killed server sent run on (status(id) collects them here), and releasing the block under them would have the
    endpoint relaunch one for them."""
    parts = []
    for bid in _ledger_batch_ids(app):
        run = app.batches.get(bid)
        try:
            c = _counts(ledger_of(app), bid)
        except LedgerError:
            continue
        out = c.get("in_flight", 0)   # the ledger's: a presumed-lost row keeps its future, never the stop
        if out:
            mine = "" if run is not None else " (sent by an earlier hpc-bridge server: status first collects them here)"
            parts.append(f"{bid} has {out} task(s) in flight on the block{mine} (admission is now "
                         f"paused; {c.get('backlog', 0)} row(s) wait in its backlog) — end them with "
                         f"cancel('{bid}', 'abort'), or wait for them (status('{bid}', wait_s=60)) and then call "
                         "stop_endpoint again")
    return "; ".join(parts) or None


def _ledger_batch_ids(app: AppCtx) -> list[str]:
    ids = list(app.batches)
    if app.state.endpoint_id is not None:
        try:
            ids += [b["id"] for b in ledger_of(app).batches(endpoint_id=app.state.endpoint_id,
                                                            states=("admitting", "needs_confirmation", "cancelled"))
                    if b["id"] not in app.batches]
        except LedgerError:
            pass
    return ids


def unknown_note(app: AppCtx) -> str | None:
    """The batches with a submit of unknown fate: its POST may have reached the service — in a task group the client
    never learnt — so the task may be queued, and the endpoint can start a block to run it. A row holds the stop from
    its failure until evidence says where the task could run (`_unknown_evidence`: on SSH, its config retired and its
    user endpoint gone with nothing queued) or its copy's result came back; absence from the groups we know is no
    evidence. A stop is never `down` while one holds it — this names them. None when there is none."""
    parts = []
    ssh = _has_login_shape(app)
    for bid in _ledger_batch_ids(app):
        try:
            rows = ledger_of(app).unknown_holding(bid)
        except LedgerError:
            continue
        if rows:
            idx = ", ".join(str(r["idx"]) for r in rows[:10]) + ("…" if len(rows) > 10 else "")
            parts.append(
                f"{bid} has {len(rows)} submit(s) of unknown fate (row(s) {idx}): the service may have queued them, "
                "and the endpoint can start a block to run them. " + (
                    f"Once this block's config is retired (stop_endpoint does that) and its user endpoint has exited "
                    f"with nothing queued, they can no longer run: status('{bid}', wait_s=60) looks"
                    if ssh else "On a facility endpoint nothing here can tell when they can no longer run"))
    return "; ".join(parts) or None


def shared_worker_note(app: AppCtx, rt: ShapeRuntime | None) -> str | None:
    """A one-worker block that a batch task holds: an interactive command waits behind it — named."""
    if rt is None:
        return None
    for bid, run in app.batches.items():
        if run.rt is rt and run.futures and (run.workers_seen or workers_of(rt) or 1) <= 1:
            return (f"The block has one worker and {bid}'s task holds it: this command waits behind the batch's "
                    f"current task (cancel('{bid}', 'drain') stops further batch tasks).")
    return None


# ---- the run_batch / status / cancel seams ---------------------------------------------------------------------------

EnsureUp = Callable[[AppCtx, bool], Awaitable[EndpointStatus]]


def _refused(reason: str, *, bid: str | None = None) -> BatchStatus:
    return BatchStatus(batch_id=bid, phase="refused", reason=reason,
                       notice=f"Nothing was recorded or started: {reason}")


def _new_id() -> str:
    return "batch-" + secrets.token_hex(6)


def _home_warning(results_dir: str | None) -> str:
    if results_dir and under_home(results_dir):
        return (f" The results directory {results_dir} is under $HOME: a full home breaks logins and the "
                "endpoint's own logs — on a facility with project or scratch space, pass results_dir= there.")
    return ""


async def run_batch(app: AppCtx, *, ensure_up: EnsureUp, command: str | None = None, rows: list[Any] | None = None,
                    task_walltime_s: float | None = None, results_dir: str | None = None, session_id: str = "default",
                    shape: str = "compute", idempotent: bool = False, confirm_spend: bool = False,
                    batch_id: str | None = None) -> BatchStatus:
    if shape != "compute":
        return _refused(f"run_batch runs on the compute block (shape='compute'); shape {shape!r} is not served")
    try:
        session_shell.Session(session_id, app.scratch_root)
    except ValueError as exc:
        return _refused(str(exc))
    if batch_id is not None:
        return await _resume(app, batch_id, ensure_up=ensure_up, confirm_spend=confirm_spend)
    try:
        command, rows, wall, _names = validate(command, rows, task_walltime_s)
    except BatchRefusal as exc:
        return _refused(str(exc))
    if results_dir is not None and not _RESULTS_DIR.match(results_dir):
        return _refused(f"results_dir {results_dir!r} is not a plain absolute path (or one under $HOME / ~)")
    st = await ensure_up(app, confirm_spend)
    if st.status in ("needs_confirmation", "needs_account"):
        return BatchStatus(phase=st.status, tasks=len(rows), block=st.block, session_spend=st.session_spend,
                           reason="the compute block's spend is not confirmed" if st.status == "needs_confirmation"
                           else "the facility needs an allocation account for the block",
                           notice=f"{st.notice or ''} Nothing was recorded: once the user confirms, call run_batch "
                                  "again (with confirm_spend=True, or after ensure_endpoint_up(confirm_spend=True)).")
    rt = app.shapes.get("compute")
    if st.status not in ("up", "provisioning") or rt is None or app.state.endpoint_id is None:
        return _refused(f"the compute block is not available ({st.status}): {st.notice or ''}".strip())
    block_wall = _parse_hhmmss(rt.user_endpoint_config.get("walltime"))
    if block_wall and wall > block_wall - MARGIN_S:
        fits = int(block_wall - MARGIN_S)
        return _refused(f"task_walltime_s {wall:g} s does not fit the block: its walltime is {int(block_wall)} s, and "
                        f"a task needs task_walltime_s + {MARGIN_S:g} s of it left to be admitted, so this batch would "
                        f"never start a row. Ask for a longer block, or rows of at most {fits} s.")
    bid = _new_id()
    rd = results_dir or f"{app.scratch_root.rstrip('/')}/batches"
    if not _RESULTS_DIR.match(batch_dir(rd, bid)):
        return _refused(f"the default results directory {rd!r} cannot be used; pass results_dir=")
    led = ledger_of(app)
    owner = owner_of(app)
    run = _Run(bid, rt, runner=_batch_runner(app, rt), runner_hash=uec_hash(rt), ensure_up=ensure_up)
    _note_block_from_canary(app, run, rt)
    cap = capacity_for(run.workers_seen or workers_of(rt))
    led.add_batch(bid, command=command, rows=rows, task_walltime_s=wall, capacity=cap, results_dir=rd,
                  idempotent=idempotent, endpoint_id=app.state.endpoint_id, uec_hash=uec_hash(rt),
                  session_id=session_id, owner=owner, facility=app.machine,
                  script_hash=hashlib.sha256(command.encode()).hexdigest()[:16], now=_now())
    if run.block_job:   # the block its first check saw, on the record (a restart's presumption counts from it)
        led.add_block_seen(bid, run.block_job)
    app.batches[bid] = run
    async with app.lock:
        admitted = _admit_locked(app, run)
    _start_loop(app, run)
    out = _status_of(app, bid, detail="summary", offset=0, limit=50)
    out.admitted = admitted
    out.block = st.block
    dashed = dash_rows(command, rows, _names)
    if dashed:
        shown = ", ".join(str(i) for i in dashed[:10]) + ("…" if len(dashed) > 10 else "")
        out.notice = ((out.notice or "") + f" Row(s) {shown} give a placeholder a value starting with '-': a command "
                      "may read it as an option. Put `--` before the placeholder (`rm -- {f}`), or give paths as "
                      "`./-name`.")
    out.notice = (f"{bid}: {len(rows)} row(s) accepted; {admitted} admitted now, at most {out.capacity} out at once "
                  f"(the block's workers less one kept for your interactive commands). Results land in "
                  f"{batch_dir(rd, bid)}. Follow "
                  f"it with status('{bid}', wait_s=60); stop it with cancel('{bid}'). " + (out.notice or "")).strip()
    return out


async def _resume(app: AppCtx, bid: str, *, ensure_up: EnsureUp, confirm_spend: bool) -> BatchStatus:
    if not _ID.match(bid):
        return _refused(f"{bid!r} is not a batch id (batch-<12 hex>)", bid=bid)
    led = ledger_of(app)
    if not led.has_batch(bid):
        return _refused(f"no batch {bid} in the ledger", bid=bid)
    owner = owner_of(app)
    if bid not in app.batches:
        # a batch this process does not run: taken over as a restart's is (`_adopt`) — one that was admitting is paused
        # first, so only the user's yes (confirm_spend) resumes it (rule 4: its spend lived in the dead process)
        if _adopt(app, bid) is None:
            lease = led.holder(bid)
            return _refused(f"{bid} is held by another hpc-bridge server ({lease.holder}, heartbeat "
                            f"{int(_now() - lease.heartbeat)} s ago): only that server admits for it. "
                            f"status('{bid}') reads it from here.", bid=bid)
    elif not led.take_lease(bid, owner, now=_now()).ok:
        app.batches.pop(bid, None)
        return _refused(f"{bid} is held by another hpc-bridge server now: status('{bid}') reads it from here.", bid=bid)
    b = led.batch(bid)
    if b["state"] in ("done", "cancelled"):
        return _status_of(app, bid)
    if not confirm_spend and b["state"] == "needs_confirmation":
        out = _status_of(app, bid)
        out.notice = (f"{bid} is paused ({b['pause_reason']}); it resumes only on the user's yes: "
                      f"run_batch(batch_id='{bid}', confirm_spend=True). " + (out.notice or "")).strip()
        return out
    pre = app.shapes.get("compute")
    if (confirm_spend and pre is not None and pre.reaped and b["state"] == "needs_confirmation"
            and "reaped" in str(b["pause_reason"] or "")):
        # the batch's own pause already told the user the block was reaped (its reason names it): this yes is for
        # the new block, given knowing the old one is gone — the first call to find the reap was the batch's
        pre.reap_told = True
    st = await ensure_up(app, confirm_spend)
    if st.status in ("needs_confirmation", "needs_account"):
        out = _status_of(app, bid)
        out.phase, out.block = st.status, st.block
        out.notice = f"{st.notice or ''} {bid} stays paused until the block is confirmed.".strip()
        return out
    rt = app.shapes.get("compute")
    if st.status not in ("up", "provisioning") or rt is None or app.state.endpoint_id is None:
        out = _status_of(app, bid)
        out.notice = f"the compute block is not available ({st.status}): {st.notice or ''}".strip()
        return out
    h = uec_hash(rt)
    if confirm_spend:
        led.unretire(app.state.endpoint_id, h)   # the user's yes is consent to this block's config again
    run = app.batches.get(bid)
    if run is None or run.rt is not rt:
        old = run
        run = _Run(bid, rt, runner=_batch_runner(app, rt), runner_hash=uec_hash(rt))
        if old is not None:   # results of tasks sent on the old block still settle through their own futures
            run.futures, run.guards, run.sent_on = old.futures, old.guards, old.sent_on
            run.sent_job, run.old_runners = old.sent_job, old.old_runners + ([old.runner] if old.runner else [])
            run.adopted = old.adopted   # rows a dead server queued are still reconciled after the resume
            run.old_uep, run.old_uep_until, run.old_uep_hash = old.old_uep, old.old_uep_until, old.old_uep_hash
            run.switched_left, run.relaunched = old.switched_left, old.relaunched
            if old.loop is not None and not old.loop.done():
                old.loop.cancel()
        app.batches[bid] = run
    run.ensure_up = ensure_up
    if run.block_end_at is None:   # never over what a result said since: a check can be older than that
        _note_block_from_canary(app, run, rt)
    led.update_batch(bid, state="admitting", pause_reason=None, uec_hash=h, endpoint_id=app.state.endpoint_id,
                     cancel_requested=None)
    led.event(bid, "batch.resume", {"by": str(owner)})
    _journal({"event": "batch.resume", "batch_id": bid})
    wall = float(led.batch(bid)["task_walltime_s"])
    if confirm_spend and run.remaining() is not None and not admit_ok(run.remaining(), wall):
        run.next_block = True   # the block in front cannot take a row: the yes is for the next one
        receipt = await _maybe_switch(app, run)
        if receipt is not None:
            st = receipt
    async with app.lock:
        admitted = _admit_locked(app, run)
    _start_loop(app, run)
    out = _status_of(app, bid)
    out.admitted, out.block = admitted, st.block
    out.notice = f"{bid} resumed: {admitted} admitted now. " + (out.notice or "")
    return out


async def status(app: AppCtx, bid: str, *, detail: str = "summary", offset: int = 0, limit: int = 50,
                 wait_s: float = 0.0) -> BatchStatus:
    if not isinstance(bid, str) or not _ID.match(bid):
        kind = bid.split("-", 1)[0] if isinstance(bid, str) and "-" in bid else ""
        return _refused(f"{bid!r} is no batch id (batch-<12 hex>)" + (f"; {kind}- ids come with a later phase"
                                                                       if kind in ("job", "camp") else ""), bid=None)
    if detail not in ("summary", "failed", "page"):
        return _refused(f"detail must be summary, failed or page, not {detail!r}", bid=bid)
    try:
        led = ledger_of(app)
    except LedgerError as exc:
        return _refused(str(exc), bid=bid)
    if not led.has_batch(bid):
        return _refused(f"no batch {bid} in the ledger", bid=bid)
    run = app.batches.get(bid)
    if run is None:
        run = _adopt(app, bid)
    if run is not None:
        wait = max(0.0, min(float(wait_s or 0.0), MAX_WAIT_S))
        deadline = time.monotonic() + wait + CALL_BUDGET_S
        busy = False
        try:
            await tick(app, bid, deadline=deadline, lock_wait_s=min(CALL_BUDGET_S, LOCK_WAIT_S))
        except PassBusy:
            busy = True
        if app.batches.get(bid) is run and isinstance(run, _Run):
            _start_loop(app, run)   # the steps a tool call leaves (a switch, a block check) are the loop's
            if wait > 0 and not busy:
                busy = await _wait_for_change(app, run, wait, deadline)
        out = _status_of(app, bid, detail=detail, offset=offset, limit=limit)
        if busy:
            out.notice = (out.notice or "") + _BUSY_NOTE
        return out
    return _status_of(app, bid, detail=detail, offset=offset, limit=limit)


def _change_key(led: Ledger, bid: str) -> tuple[Any, ...]:
    """What status(wait_s) waits to see change: the phase, and every count but the backlog's move into flight (an
    admission is no settlement)."""
    c = led.counts(bid)
    return (led.batch(bid)["state"], *(c.get(k, 0) for k in ("done", "failed", "lost", "cancelled", "presumed_lost",
                                                              "submit_unknown")))


async def _wait_for_change(app: AppCtx, run: _Run, wait: float, deadline: float) -> bool:
    """Until a row settles or the phase changes, or `wait` is up. The background loop does the passes when it runs;
    otherwise this call does, as the loop would. Returns whether a pass was found busy (the answer says so)."""
    led = ledger_of(app)
    bid = run.batch_id
    before = _change_key(led, bid)
    end = time.monotonic() + wait
    while (left := end - time.monotonic()) > 0 and app.batches.get(bid) is run:
        looping = run.loop is not None and not run.loop.done()
        run.wake.clear()
        try:
            await asyncio.wait_for(run.wake.wait(), timeout=min(left, 0.5 if looping else TICK_S))
        except TimeoutError:
            pass
        if not looping:
            try:
                await tick(app, bid, deadline=deadline)
            except PassBusy:
                return True
        if _change_key(led, bid) != before:
            return False
    return False


def _adopt(app: AppCtx, bid: str) -> _Run | None:
    """A batch this process does not run: take its lease when the holder is gone (a stale heartbeat, a dead pid on
    this host) — to collect, never to admit: a batch that was admitting asks again (spend lived in that process)."""
    led = ledger_of(app)
    lease = led.take_lease(bid, owner_of(app), now=_now())
    if not lease.ok:
        return None
    b = led.batch(bid)
    run = _Run(bid, None, adopted=True)
    app.batches[bid] = run
    if b["state"] == "admitting":
        pause(app, bid, f"the hpc-bridge server that ran {bid} stopped (a restart): its spend confirmation lived in "
                        "that process, so admission resumes only on the user's yes")
    _start_loop(app, run)   # what the dead server sent is collected (or presumed) without a tool call
    return run


async def cancel(app: AppCtx, bid: str, *, mode: str = "drain") -> CancelResult:
    if mode not in ("drain", "abort"):
        return CancelResult(id=str(bid), status="refused", notice=f"mode must be drain or abort, not {mode!r}")
    if not isinstance(bid, str) or not _ID.match(bid):
        return CancelResult(id=str(bid), status="refused", mode=mode,  # type: ignore[arg-type]
                            notice=f"{bid!r} is no batch id (batch-<12 hex>)")
    led = ledger_of(app)
    if not led.has_batch(bid):
        return CancelResult(id=bid, status="refused", mode=mode,  # type: ignore[arg-type]
                            notice=f"no batch {bid} in the ledger")
    run = app.batches.get(bid) or _adopt(app, bid)
    b = led.batch(bid)
    if run is None:   # another live server holds it: ask it (any process may — a cancel only lowers spend)
        led.update_batch(bid, cancel_requested=mode)
        c = _counts(led, bid)
        return CancelResult(id=bid, status="draining" if _out_now(c) else "down", mode=mode,  # type: ignore[arg-type]
                            in_flight=_out_now(c),
                            notice=f"{bid} is held by another hpc-bridge server ({b['owner_host']} pid "
                                   f"{b['owner_pid']}); the {mode} was recorded in the ledger and that server applies "
                                   "it at its next pass.")
    deadline = time.monotonic() + CALL_BUDGET_S
    dropped, marker = await _apply_cancel(app, run, mode, deadline=deadline)
    busy = False
    try:
        await tick(app, bid, deadline=deadline, lock_wait_s=min(CALL_BUDGET_S, LOCK_WAIT_S))
    except PassBusy:
        busy = True
    _start_loop(app, run)
    c = _counts(led, bid)
    out = _out_now(c)
    left = _blocks_left(app, run)
    wall = int(float(b["task_walltime_s"]))
    if mode == "abort" and out and marker is None:
        how = (f"each one's wrapper sees the {CANCEL_MARKER} marker within ~2 s and kills its command (rc 143); call "
               "cancel again or status to see them settle")
    elif mode == "abort" and out:
        how = (f"the {CANCEL_MARKER} marker was not written ({marker}), so nothing has told them to stop: each runs to "
               f"its end — at most task_walltime_s ({wall} s), when its wrapper kills it — or until the block ends")
    if mode == "abort":
        word = f"aborted: {dropped} backlog row(s) cancelled; " + (
            f"{out} task(s) still in flight — {how}" if out else "nothing of it is in flight")
    else:
        word = (f"drained: admission stopped, {c.get('backlog', 0)} row(s) kept in the backlog (resume with "
                f"run_batch(batch_id='{bid}', confirm_spend=True)); " +
                (f"{out} task(s) in flight finish on their own" if out else "nothing of it is in flight"))
    tail = (f" {len(left)} block(s) of ours can still bill (the session's until stop_endpoint, or its idle release): "
            "see blocks_left." if left else "")
    return CancelResult(id=bid, status="draining" if out else "down", mode=mode,  # type: ignore[arg-type]
                        backlog_dropped=dropped, in_flight=out, blocks_left=left,
                        notice=f"{bid} {word}.{tail}" + (_BUSY_NOTE if busy else ""))


async def _apply_cancel(app: AppCtx, run: _Run, mode: str, *, deadline: float | None = None) -> tuple[int, str | None]:
    """Apply a drain or an abort. Returns (backlog rows dropped, why the CANCELLED marker was not written — None when
    it was, or when nothing was out to need it)."""
    led = ledger_of(app)
    bid = run.batch_id
    b = led.batch(bid)
    led.update_batch(bid, cancel_requested=None)
    run.next_block = False   # a cancel never asks for another block
    if mode == "drain":
        if b["state"] not in ("done", "cancelled"):
            pause(app, bid, f"drained by cancel('{bid}', 'drain'): the backlog is kept")
        return 0, None
    dropped = led.cancel_backlog(bid, now=_now())
    led.update_batch(bid, state="cancelled", pause_reason="aborted by cancel")
    led.event(bid, "batch.abort", {"backlog_dropped": dropped})
    _journal({"event": "batch.pause", "batch_id": bid, "reason": "abort"})
    if _out_now(_counts(led, bid)) or run.futures:
        ok, why = await write_cancel_marker(app, run, batch_dir(str(b["results_dir"]), bid), deadline=deadline)
        return dropped, None if ok else why
    return dropped, None


def _marker_block_why_not(app: AppCtx, run: _Run) -> str | None:
    """Why a MEP marker task may NOT be sent now, or None when it may. It is a submit: it may only go to the block the
    batch's tasks run on, while that block is confirmed, live and not ending, its config not retired, with a worker the
    batch keeps free — else it would revive the user endpoint and start a block nobody confirmed (#204's pattern), or
    wait behind the very row it is meant to end."""
    rt = app.shapes.get("compute")
    if run.runner is None or rt is None or run.rt is not rt:
        return "no confirmed block of this batch's is bound here (a restart, a stop or a switch)"
    if not rt.spend_confirmed or rt.reaped:
        return "the block's spend is no longer confirmed" + (f" (reaped: {rt.reaped})" if rt.reaped else "")
    if run.runner_hash != uec_hash(rt) or (app.state.endpoint_id is not None
                                           and ledger_of(app).retired(app.state.endpoint_id, uec_hash(rt))):
        return "the block's config was retired"
    rem = run.remaining()
    if rem is None or rem < MARKER_MIN_REMAINING_S:
        return "the block's remaining walltime is unknown or nearly gone"
    if (run.workers_seen or workers_of(rt) or 1) <= 1:
        return ("the block has one worker, which the batch's task holds: a marker task would wait behind the very row "
                "it is meant to end")
    return None


async def write_cancel_marker(app: AppCtx, run: _Run, results_dir: str, *,
                              deadline: float | None = None) -> tuple[bool, str]:
    """The CANCELLED marker in the batch's own directory, which each running wrapper watches: over the free login shape
    on an SSH endpoint; on a facility MEP as ONE small task on the batch's confirmed, live block
    (`_marker_block_why_not`) — never a second while one is out. Returns (written, why not)."""
    cmd = f"mkdir -p {quoted_dir(results_dir)} && touch {quoted_dir(results_dir)}/{CANCEL_MARKER} && echo HPCB_MARKED"
    try:
        if _has_login_shape(app) and login_runner is not None:
            out = await login_runner(app)(cmd)
            if "HPCB_MARKED" in (out.stdout or ""):
                return True, ""
            return False, "the login shape did not answer for it"
        if run.marker_written:   # a marker is never taken back: no second task for it
            return True, ""
        fut = run.marker_future
        if fut is None:
            if (why := _marker_block_why_not(app, run)) is not None:
                return False, why
            rt = app.shapes.get("compute")
            guard = warmth._new_guard(rt) if rt is not None else None
            fut = run.marker_future = run.runner.submit(session_shell.guard_prefix(guard) + cmd)
        budget = MARKER_WAIT_S if deadline is None else max(0.0, min(MARKER_WAIT_S, deadline - time.monotonic()))
        try:
            res = await asyncio.to_thread(fut.result, budget)
        except TimeoutError:
            return False, (f"its task was sent to the block but has not run within {int(budget)} s — no other is "
                           "sent; it lands once a worker frees")
        run.marker_future = None
        if "HPCB_MARKED" in _shell_fields(res)[1]:
            run.marker_written = True
            return True, ""
        return False, "its task ran but did not write it (another block answered, or the results dir is unwritable)"
    except Exception as exc:  # noqa: BLE001 - the marker is best effort; the answer says the tasks are still out
        run.marker_future = None
        print(f"hpc-bridge: the cancel marker could not be written ({type(exc).__name__}: {exc})", file=sys.stderr)
        return False, f"{type(exc).__name__}: {exc}"


def _iso(wall: float | None) -> str | None:
    return None if wall is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(wall))


def _blocks_left(app: AppCtx, run: _Run) -> list[BlockLeft]:
    """Every block of ours that can still bill: the session's confirmed block; the ones a stop, detach, reconnect or
    switch left draining on this endpoint (`app.left_blocks`, a facility endpoint's); the one the batch's next-block
    switch left to run out its last minutes; a block the facility started for a task of ours after the confirmed one
    went; and, for a while after a reap, the reaped block's user endpoint, which may start a pilot for the task that
    was out on it."""
    out: list[BlockLeft] = []
    named: set[str] = set()
    ssh = _has_login_shape(app)
    rt = app.shapes.get("compute")
    if rt is not None and rt.spend_confirmed:
        job = rt.block_job or run.block_job
        how = ("stop_endpoint() releases it" if ssh else
               "the facility idle-releases it once no work is sent (nothing here can cancel it); the user's own "
               f"scancel {job} ends it now" if job else "the facility idle-releases it once no work is sent")
        out.append(BlockLeft(job_id=job, bill_until=_iso(run.block_end_at), how_to_end=how))
        if job:
            named.add(str(job))
    now_m, now_w = time.monotonic(), _now()
    for left in warmth._prune_left(app):
        if left.endpoint_id not in (None, app.state.endpoint_id) or (left.job and left.job in named):
            continue
        until = None if left.until is None else now_w + (left.until - now_m)
        what = "a check of ours still queued there" if left.kind == "check" else f"left draining by a {left.how}"
        out.append(BlockLeft(job_id=left.job, bill_until=_iso(until), how_to_end=(
            f"{what}: the facility idle-releases it once no work is sent (nothing here can cancel it)"
            + (f"; the user's own scancel {left.job} ends it now" if left.job else ""))))
        if left.job:
            named.add(str(left.job))
    for job, end in run.switched_left:   # a job unknown here is never taken for another block: always listed
        if (end is None or end > now_w) and not (job and str(job) in named):
            out.append(BlockLeft(job_id=job, bill_until=_iso(end), how_to_end=(
                "the block in front when the batch moved to the next one: it runs out its last minutes with nothing of "
                "ours on it" + ("; stop_endpoint() cancels it" if ssh else f"; the user's own scancel {job} ends it"))))
            if job:
                named.add(str(job))
    for job in run.relaunched:
        if job not in named:
            out.append(BlockLeft(job_id=job, how_to_end=(
                "the facility started it for a task of ours that reached it after the confirmed block went; it "
                "idle-releases once no work is sent" + ("; stop_endpoint() cancels it" if ssh else
                                                        f"; the user's own scancel {job} ends it now"))))
            named.add(job)
    if run.old_uep and now_w < run.old_uep_until:
        out.append(BlockLeft(job_id=None, bill_until=_iso(run.old_uep_until), how_to_end=(
            f"the reaped block's user endpoint ({run.old_uep}) may start a pilot for the task that was out on it: "
            f"hpc-bridge cancels its pilots by that marker until {_iso(run.old_uep_until)}")))
    return out


# ---- one pass: collect, settle, admit -------------------------------------------------------------------------------

def _start_loop(app: AppCtx, run: _Run) -> None:
    if not app.batch_autoloop or (run.loop is not None and not run.loop.done()):
        return
    run.loop = asyncio.get_running_loop().create_task(_loop(app, run.batch_id))


async def _loop(app: AppCtx, bid: str) -> None:
    while True:
        run = app.batches.get(bid)
        if run is None:
            return
        try:
            await tick(app, bid)
            b = ledger_of(app).batch(bid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a pass that fails is logged; the next one tries again
            print(f"hpc-bridge: batch {bid} pass failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            b = {"state": "admitting"}
        idle = not run.futures and b["state"] != "admitting" and not _still_settling(app, run)
        if idle and b["state"] in ("done", "cancelled", "needs_confirmation"):
            if run.runner is not None and b["state"] in ("done", "cancelled"):
                try:
                    run.runner.close()
                except Exception:  # noqa: BLE001
                    pass
            return
        run.wake.clear()
        try:
            await asyncio.wait_for(run.wake.wait(), timeout=TICK_S if run.futures else IDLE_TICK_S)
        except TimeoutError:
            pass


def _still_settling(app: AppCtx, run: _Run) -> bool:
    """Something of the batch may still settle without this process's futures — a row in flight it did not send (a
    restart), a submit of unknown fate — or the reaped block's user endpoint is still watched: the loop keeps going."""
    if run.old_uep and _now() < run.old_uep_until:
        return True
    try:
        c = _counts(ledger_of(app), run.batch_id)
        holding = bool(_has_login_shape(app) and ledger_of(app).unknown_holding(run.batch_id))
    except LedgerError:
        return False
    return bool(c.get("in_flight", 0) + c.get("submit_unknown", 0)) or holding


class PassBusy(Exception):
    """A tool call's pass could not start within its budget: the background loop's pass holds the batch (a block being
    confirmed, a slow lookup). The call answers from the ledger instead."""


LOCK_WAIT_S = 10.0   # the most a tool call waits for the loop's pass to finish before answering from the ledger


async def tick(app: AppCtx, bid: str, *, deadline: float | None = None, lock_wait_s: float = LOCK_WAIT_S) -> int:
    """One pass over a batch this process holds: settle what came back, reconcile unknown submits, presume or look for
    evidence, then admit what the window, the gate and the lease allow. Returns how many it admitted. `deadline`
    (monotonic): a TOOL CALL's pass — it waits for a running pass at most until then (PassBusy), never starts a
    next-block switch or a block check (the background loop's, which has no budget to keep), and skips the lookups
    that could outlast it."""
    run = app.batches.get(bid)
    if run is None:
        return 0
    if deadline is None:
        async with run.passing:
            return await _tick(app, bid, run, None)
    try:
        await asyncio.wait_for(run.passing.acquire(),
                               timeout=max(0.0, min(lock_wait_s, deadline - time.monotonic())))
    except TimeoutError:
        raise PassBusy(bid) from None
    try:
        return await _tick(app, bid, run, deadline)
    finally:
        run.passing.release()


_BUSY_NOTE = (" A pass of this batch's background loop is still running (a block being confirmed, or a slow lookup), "
              "so this answer is the ledger's view as of now.")


def _room(deadline: float | None, need_s: float) -> bool:
    return deadline is None or deadline - time.monotonic() >= need_s


async def _tick(app: AppCtx, bid: str, run: _Run, deadline: float | None = None) -> int:
    led = ledger_of(app)
    if not led.heartbeat(bid, owner_of(app)):
        app.batches.pop(bid, None)   # another process took the lease: this one reads, never admits
        return 0
    b = led.batch(bid)
    if b.get("cancel_requested"):
        await _apply_cancel(app, run, str(b["cancel_requested"]), deadline=deadline)
    _collect(app, run)
    if _room(deadline, 20):
        await _rest_pull(app, run)
        await _reconcile_unsent(app, run)
        await _reconcile(app, run)
    _presume(app, run)
    if _room(deadline, 40):
        await _evidence(app, run)
        await _release_old_uep(app, run)
        await _unknown_evidence(app, run)
    if deadline is None:   # the background loop's alone: a switch confirms a block and may take minutes
        await _maybe_switch(app, run)
        await _maybe_warm(app, run)
    async with app.lock:
        n = _admit_locked(app, run)
    _maybe_done(app, run)
    _close_old_runners(run)
    run.wake.set()
    return n


def _admit_locked(app: AppCtx, run: _Run) -> int:
    """Admit what may go now (callers hold app.lock: a stop, a switch, a teardown cannot interleave)."""
    led = ledger_of(app)
    bid = run.batch_id
    b = led.batch(bid)
    if b["state"] != "admitting" or run.switching:
        return 0
    rt = app.shapes.get("compute")
    if rt is None or (run.rt is not None and rt is not run.rt):
        pause(app, bid, "the compute block this batch ran on was released (stop_endpoint, a switch or a teardown)")
        return 0
    if run.rt is None:
        run.rt = rt
    if rt.reaped:
        pause(app, bid, f"the block is gone (reaped): {rt.reaped}")
        return 0
    if not rt.spend_confirmed:
        pause(app, bid, "the block's spend is no longer confirmed")
        return 0
    _runner_for_block(app, run, rt)
    _sync_block(app, run, rt)
    if run.block_end_at is None:
        _note_block_from_canary(app, run, rt)
    if not led.counts(bid).get("backlog", 0):
        return 0   # nothing left to admit: the gate has nothing to hold back, and asks for no block
    remaining = run.remaining()
    need = float(b["task_walltime_s"]) + MARGIN_S
    if remaining is None:
        run.waiting = ("the block's remaining walltime is not known yet (its first check has not answered from inside "
                       "it): nothing is admitted until it is")
        return 0
    run.waiting = None
    if not admit_ok(remaining, float(b["task_walltime_s"])) and run.next_block:
        run.waiting = (f"the block in front (job {run.block_job or '?'}) has ~{max(0, int(remaining))} s of walltime "
                       "left, too little for a row; the next block, which the user confirmed, is asked for once the "
                       "tasks on this one finish" + ("" if _fresh_ok(app) else " and this one ends"))
        return 0
    if not admit_ok(remaining, float(b["task_walltime_s"])):
        job = f" (job {run.block_job})" if run.block_job else ""
        pause(app, bid, f"the block{job} has ~{max(0, int(remaining))} s of walltime left, under task_walltime_s + "
                        f"the {MARGIN_S:g} s margin ({int(need)} s): the rest needs a new block, which needs the "
                        f"user's yes — run_batch(batch_id='{bid}', confirm_spend=True)")
        return 0
    cap = capacity_for(run.workers_seen or workers_of(rt))
    if cap != int(b["capacity"] or 0):
        led.update_batch(bid, capacity=cap)
    try:
        rows = led.reserve(bid, owner_of(app), cap, uec_hash=uec_hash(rt), now=_now())
    except RetiredConfig as exc:
        pause(app, bid, f"the block's config was retired ({exc}): a new block needs the user's yes")
        return 0
    except NotLeaseHolder:
        app.batches.pop(bid, None)
        return 0
    names = {"index"}
    for row in led.tasks(bid):
        r = json.loads(row["row_json"])
        names |= set(r) if isinstance(r, dict) else {"row"}
    session = session_shell.Session(str(b["session_id"] or "default"), app.scratch_root)
    sent = 0
    for t in rows:
        if _send(app, run, rt, b, t.index, t.attempt, t.row, names, session, remaining, need):
            sent += 1
    return sent


def _send(app: AppCtx, run: _Run, rt: ShapeRuntime, b: dict[str, Any], index: int, attempt: int, row: Any,
          names: set[str], session: session_shell.Session, remaining: float, need: float) -> bool:
    led = ledger_of(app)
    bid = run.batch_id
    guard = warmth._new_guard(rt)
    script = task_script(substitute(str(b["command"]), row, index, names),
                         results_dir=batch_dir(str(b["results_dir"]), bid), index=index, attempt=attempt,
                         task_walltime_s=float(b["task_walltime_s"]), session=session, guard=guard,
                         fields=row_values(row, index, names))
    try:
        fut = run.runner.submit(script)
    except Exception as exc:  # noqa: BLE001 - the Executor refused before sending (shut down): never sent
        led.settle(bid, index, attempt, "backlog", only_from=("in_flight",))
        rt.runner_stale = True
        run.runner = None
        print(f"hpc-bridge: batch {bid} row {index} was not sent ({type(exc).__name__}: {exc})", file=sys.stderr)
        return False
    key = (index, attempt)
    run.futures[key] = fut
    run.guards[key] = guard
    run.sent_on[key] = rt
    run.sent_job[key] = run.block_job
    rt.batch_inflight += 1
    tid = getattr(fut, "task_id", None)
    if tid:
        led.settle(bid, index, attempt, "in_flight", only_from=("in_flight",), sent_job=run.block_job,
                   sent_uec=run.runner_hash, task_id=str(tid))
    else:
        led.settle(bid, index, attempt, "in_flight", only_from=("in_flight",), sent_job=run.block_job,
                   sent_uec=run.runner_hash)
    try:
        loop = asyncio.get_running_loop()
        fut.add_done_callback(lambda _f, ev=run.wake: loop.call_soon_threadsafe(ev.set))
    except RuntimeError:
        pass
    c = _counts(led, bid)
    _journal({"event": "batch.admit", "batch_id": bid, "index": index, "attempt": attempt, "task_id": tid,
              "in_flight": _held(c), "capacity": int(led.batch(bid)["capacity"] or 1),
              "remaining_s": round(remaining, 1), "need_s": round(need, 1), "job_id": run.block_job})
    return True


def _fresh_ok(app: AppCtx) -> bool:
    """A new block can be asked for beside the one in front: an endpoint hpc-bridge stood up, whose each confirmation
    gets its own compute user endpoint (`fresh_ueps`); a facility MEP keeps one user endpoint, max_blocks 1."""
    return _has_login_shape(app) and getattr(app.facility, "fresh_ueps", False) is True


async def _maybe_switch(app: AppCtx, run: _Run) -> EndpointStatus | None:
    """The confirmed next block, once nothing of ours runs on the one in front: drop that shape (its block runs out its
    last minutes on its own — no task of ours left on it) and confirm a fresh one (a new user endpoint, a new block).
    Returns the confirming answer (its receipt), or None when nothing was switched."""
    rt = app.shapes.get("compute")
    led = ledger_of(app)
    b = led.batch(run.batch_id)
    rem = run.remaining()
    c = led.counts(run.batch_id)
    if not c.get("backlog", 0) or b["state"] != "admitting":
        run.next_block = False   # nothing left for a next block to run, or the batch was paused, drained or stopped
        return None
    if (not run.next_block or run.ensure_up is None or rt is None or rt is not run.rt or not _fresh_ok(app)
            or rem is None or admit_ok(rem, float(b["task_walltime_s"])) or run.futures or rt.inflight
            or rt.batch_inflight or c.get("submit_unknown", 0) or c.get("in_flight", 0)
            or warmth._live_task_handles(app, "compute")):
        return None
    old_job = run.block_job
    async with app.lock:
        if app.shapes.get("compute") is not rt or led.batch(run.batch_id)["state"] != "admitting":
            return None
        # the block in front runs out its last minutes on its own: banked to its end, and named while it can bill
        warmth._bank_warm_interval(rt, app)
        if rem > 0:
            rt.spend_accrued += warmth.estimate_spend(rem, warmth._block_nodes(rt, app), app.charge_factor)
        run.switched_left.append((old_job, _now() + max(0.0, rem)))
        warmth._drop_compute_shape_locked(app)
        run.switching = True
    try:
        st: EndpointStatus = await run.ensure_up(app, True)
    finally:
        run.switching = False
    if st.status in ("needs_confirmation", "needs_account", "down"):
        pause(app, run.batch_id, f"the next block could not be confirmed ({st.status}): {st.notice or ''}".strip())
        return st
    new = app.shapes.get("compute")
    run.rt = new
    if new is not None:
        _runner_for_block(app, run, new)
    run.block_end_at = run.block_job = run.block_seen_at = None
    run.workers_seen, run.next_block = None, False
    if new is not None:
        _note_block_from_canary(app, run, new)
        led.update_batch(run.batch_id, uec_hash=uec_hash(new))
    led.event(run.batch_id, "batch.next_block", {"after": old_job})
    _journal({"event": "batch.resume", "batch_id": run.batch_id, "reason": f"the next block, after job {old_job}"})
    return st


async def _maybe_warm(app: AppCtx, run: _Run) -> None:
    """Nothing known of the block yet (it is still starting, or a new one was just asked for) and nothing of the batch
    out: check it, as a run_shell would, at most every 10 s, so the batch learns when it answers."""
    if run.futures or run.remaining() is not None or _now() - run.last_warm < 10.0:
        return
    rt = app.shapes.get("compute")
    if rt is None or not rt.spend_confirmed or ledger_of(app).batch(run.batch_id)["state"] != "admitting":
        return
    run.last_warm = _now()
    async with app.lock:
        if app.shapes.get("compute") is rt:
            await warmth._ensure_warm_runner(app, "compute")
    if app.shapes.get("compute") is rt:
        _note_block_from_canary(app, run, rt)


def _drop(run: _Run, key: tuple[int, int]) -> None:
    run.futures.pop(key, None)
    run.guards.pop(key, None)
    run.sent_job.pop(key, None)
    rt = run.sent_on.pop(key, None)
    if rt is not None and rt.batch_inflight > 0:
        rt.batch_inflight -= 1


def _collect(app: AppCtx, run: _Run) -> None:
    led = ledger_of(app)
    for r in [run.runner, *run.old_runners]:
        tg = getattr(r, "task_group_id", None) if r is not None else None
        if tg and str(tg) not in run.groups_recorded:
            # on the record as soon as the service minted it: every runner's (reconciliation reads them all, and a
            # server that restarts too)
            led.add_task_group(run.batch_id, str(tg))
            run.groups_recorded.add(str(tg))
    for key, fut in list(run.futures.items()):
        index, attempt = key
        tid = getattr(fut, "task_id", None)
        if tid:
            t = led.task(run.batch_id, index)
            if t is not None and t.get("attempt") == attempt and not t.get("task_id"):
                led.set_task_id(run.batch_id, index, attempt, str(tid))
        if not fut.done():
            continue
        guard = run.guards.get(key)
        _drop(run, key)
        if fut.cancelled():
            led.settle(run.batch_id, index, attempt, "backlog", only_from=("in_flight",))
            continue
        exc = fut.exception()
        if exc is not None and tid and _client_side(exc):
            # the SDK's own machinery failed (its result watcher quit: a laptop asleep, a network down) — not the task,
            # which runs on in the block. It keeps its slot (nothing is refilled on it), the runner is rebuilt, and its
            # result is pulled by REST (`_rest_pull`: a row out with a task id and no future here)
            _runner_broken(run, exc)
            continue
        if exc is not None:
            _settle_failure(app, run, index, attempt, exc, sent=bool(tid))
            continue
        _settle_result(app, run, index, attempt, fut.result(), guard)


def _client_side(exc: BaseException) -> bool:
    """A future's exception that is the SDK's own (its AMQP result watcher gave up and failed every open future: "SDK
    thread quit unexpectedly.", a connection it could not reopen, a channel it could not keep) — not the task's result,
    which a TaskExecutionFailed is (a ManagerLost / WorkerLost included)."""
    return not (type(exc).__name__ == "TaskExecutionFailed" or _LOST.search(str(exc)))


def _runner_broken(run: _Run, exc: BaseException) -> None:
    if run.runner is not None:
        print(f"hpc-bridge: batch {run.batch_id}: the SDK's result channel failed ({type(exc).__name__}: {exc}); "
              "rebuilding its runner, results by REST", file=sys.stderr)
        run.old_runners.append(run.runner)
        run.runner, run.runner_hash = None, None
    run.last_rest = 0.0


def _settle_failure(app: AppCtx, run: _Run, index: int, attempt: int, exc: BaseException | str, *, sent: bool) -> None:
    led = ledger_of(app)
    bid = run.batch_id
    text = exc if isinstance(exc, str) else f"{type(exc).__name__}: {exc}"
    if not sent:   # the submit itself failed: the POST may still have reached the service (S4)
        led.settle(bid, index, attempt, "submit_unknown", only_from=("in_flight",),
                   evidence=f"the submit failed: {text[:300]}", unknown_since=_now(), unheld=None)
        _event(bid, "batch.settle", index, attempt, "submit_unknown")
        return
    holding = list(HOLDING)
    if _LOST.search(text):
        if led.batch(bid)["idempotent"]:
            led.settle(bid, index, attempt, "backlog", only_from=holding,
                       evidence=f"retry on evidence: {text[:300]}")
            _event(bid, "batch.settle", index, attempt, "retry")
        else:
            led.settle(bid, index, attempt, "lost", only_from=holding, settled_by="evidence: " + text[:200],
                       evidence=text[:1000])
            _event(bid, "batch.settle", index, attempt, "lost")
        return
    led.settle(bid, index, attempt, "failed", only_from=holding, settled_by="result", tail=text[-4000:],
               evidence=text[:1000])
    _event(bid, "batch.settle", index, attempt, "failed")


def _event(bid: str, kind: str, index: int, attempt: int, state: str, **extra: Any) -> None:
    _journal({"event": kind, "batch_id": bid, "index": index, "attempt": attempt, "state": state, **extra})


def _settle_result(app: AppCtx, run: _Run, index: int, attempt: int, res: Any, guard: Any) -> None:
    led = ledger_of(app)
    bid = run.batch_id
    rc, out, err = _shell_fields(res)
    holding = list(HOLDING)
    refused = None
    if guard is not None and rc is not None:
        refused = session_shell.refusal(rc, err, guard)
    elif rc == session_shell.WRONG_BLOCK_EXIT and session_shell.WRONG_BLOCK_MARKER in err:
        refused = session_shell.Refusal(None)
    if refused is not None:   # it reached ANOTHER block than the confirmed one and ran nothing: the block is gone
        led.settle(bid, index, attempt, "backlog", only_from=holding, evidence="refused on a replacement block")
        rt = app.shapes.get("compute")
        if refused.job and refused.job not in run.relaunched and refused.job != run.block_job:
            run.relaunched.append(refused.job)
            led.add_block_seen(bid, refused.job)
        why = (f"the confirmed block (job {run.block_job or '?'}) is gone: a task of {bid} reached another block "
               f"({'job ' + refused.job if refused.job else 'a job hpc-bridge cannot read'}) and refused to run there")
        if rt is not None and rt is run.rt and rt.reaped is None:
            warmth._mark_reaped(app, rt, why, warmth._release_bound(app, "compute", rt), kicked=True)
        retire_all(app, run.rt, why)
        pause(app, bid, f"the block was reaped — {why}")
        return
    rep = parse_result(out)
    if rep is None:
        led.settle(bid, index, attempt, "failed", only_from=holding, rc=rc, settled_by="result",
                   tail=(out or "")[-TAIL_BYTES:], stderr_tail=(err or "")[-STDERR_TAIL_BYTES:],
                   evidence="the task's wrapper printed no result line")
        _event(bid, "batch.settle", index, attempt, "failed")
        return
    _learn_block(app, run, rep)
    if rep.duplicate:   # this task was a second copy of an attempt that already ran: its own result is elsewhere
        led.settle(bid, index, attempt, "submit_unknown", only_from=("in_flight",),
                   evidence="a duplicate of this attempt answered; the one that ran is reconciled from the task group",
                   unknown_since=_now(), unheld=None)
        return
    fields: dict[str, Any] = {"rc": rep.rc, "tail": rep.tail, "stderr_tail": rep.stderr_tail,
                              "stdout_path": rep.stdout_path, "stdout_bytes": rep.stdout_bytes,
                              "truncated": int(rep.truncated), "job_id": rep.job, "host": rep.host,
                              "workers": rep.workers, "remaining_s": rep.remaining_s, "started_at": rep.started}
    if rep.cancelled:
        state, by = "cancelled", f"evidence: the {CANCEL_MARKER} marker (cancel abort)"
    elif rep.no_space:
        state, by = "failed", "result: too little free space in the results directory"
    elif rep.error:
        state, by = "failed", f"result: {rep.error}"
    else:
        state = "done" if rep.rc == 0 else "failed"
        by = "result: the wrapper's own timeout (rc 124)" if rep.timed_out else "result"
    led.settle(bid, index, attempt, state, only_from=holding, settled_by=by, **fields)
    _event(bid, "batch.settle", index, attempt, state, rc=rep.rc, job_id=rep.job, remaining_s=rep.remaining_s)


def _learn_block(app: AppCtx, run: _Run, rep: TaskReport) -> None:
    led = ledger_of(app)
    if rep.remaining_s is not None:
        run.block_end_at = _now() + rep.remaining_s
    if rep.job:
        if run.block_job != rep.job:
            run.block_job = rep.job
        if run.block_seen_at is None:
            run.block_seen_at = _now()
        led.add_block_seen(run.batch_id, rep.job)
    if rep.workers:
        run.workers_seen = rep.workers
    rt = run.rt
    if rt is not None and app.shapes.get("compute") is rt:
        rt.warm_confirmed_at = time.monotonic()   # a worker of the block answered: liveness, as a task result is


def _maybe_done(app: AppCtx, run: _Run) -> None:
    led = ledger_of(app)
    c = _counts(led, run.batch_id)
    if c.get("backlog", 0) == 0 and _held(c) == 0 and not run.futures:
        b = led.batch(run.batch_id)
        if b["state"] not in ("done", "cancelled"):
            led.update_batch(run.batch_id, state="done", pause_reason=None)
            led.event(run.batch_id, "batch.done", c)


def _lookup_client(app: AppCtx, run: _Run | None = None) -> Any:
    candidates = [run.runner] if run is not None and run.runner is not None else []
    candidates += [getattr(app.shapes.get(n), "runner", None) for n in ("login", "compute")]
    for r in candidates:
        client = getattr(r, "task_client", lambda: None)() if r is not None else None
        if client is not None:
            return client
    return None


async def _rest_pull(app: AppCtx, run: _Run) -> None:
    """Rows in flight whose future this process does not hold (a restart): pulled by REST, results first."""
    led = ledger_of(app)
    held = {t for (t, _a) in run.futures}
    rows = [t for t in led.tasks(run.batch_id, states=HOLDING) if t.get("task_id") and t["idx"] not in held]
    if not rows or (run.last_rest and _now() - run.last_rest < REST_EVERY_S):
        return
    client = _lookup_client(app, run)
    if client is None:
        return
    run.last_rest = _now()
    try:
        got = await asyncio.to_thread(collect_rest, client, [str(t["task_id"]) for t in rows])
    except Exception as exc:  # noqa: BLE001 - the service unreachable: try again next pass
        print(f"hpc-bridge: batch {run.batch_id}: REST pull failed ({exc})", file=sys.stderr)
        return
    for t in rows:
        r = got.get(str(t["task_id"]))
        if r is None or r.state == "pending":
            continue
        if r.state == "failed":
            _settle_failure(app, run, int(t["idx"]), int(t["attempt"]), r.error, sent=True)
        else:
            _settle_result(app, run, int(t["idx"]), int(t["attempt"]), r.result, None)


async def _reconcile_unsent(app: AppCtx, run: _Run) -> None:
    """After a restart: a row the dead server admitted whose task id it never recorded (it died between its queueing and
    the service's answer). The batch's task group says what was sent: a task in it the ledger does not know, once it
    answers, settles the row its HPCB_RESULT names; while one is still pending nothing is decided; once every task of
    the group is accounted for, a row still without one was never sent — the server that queued it is dead, so it never
    will be — and it goes back to the backlog (no attempt of it ever ran)."""
    if not run.adopted:
        return
    led = ledger_of(app)
    mine = set(run.futures)   # this process's own sends: their ids come with the service's answer
    unsent = [t for t in led.tasks(run.batch_id, states=("in_flight",))
              if not t.get("task_id") and (int(t["idx"]), int(t["attempt"])) not in mine]
    groups = led.task_groups(run.batch_id)
    client = _lookup_client(app, run)
    # no group on the record (the server died before the service's first answer): nothing can be reconciled — those
    # rows are presumed in time instead (`_presume`)
    if not unsent or not groups or client is None or (run.last_unsent and _now() - run.last_unsent < REST_EVERY_S):
        return
    run.last_unsent = _now()
    ids = await _group_tasks(client, run.batch_id, groups)
    if ids is None:
        return
    known = {str(t["task_id"]) for t in led.tasks(run.batch_id) if t.get("task_id")}
    strays = [i for i in ids if i not in known]
    got = await asyncio.to_thread(collect_rest, client, strays) if strays else {}
    pending = False
    for tid, res in got.items():
        if res.state == "pending":
            pending = True
            continue
        rep = parse_result(_shell_fields(res.result)[1]) if res.state == "success" else None
        if rep is None or rep.duplicate:
            continue
        row = led.task(run.batch_id, rep.index)
        if row is not None and row["state"] == "in_flight" and int(row["attempt"]) == rep.attempt:
            led.set_task_id(run.batch_id, rep.index, rep.attempt, tid)
            _settle_result(app, run, rep.index, rep.attempt, res.result, None)
    if pending:
        return
    for t in led.tasks(run.batch_id, states=("in_flight",)):
        if not t.get("task_id") and (int(t["idx"]), int(t["attempt"])) not in mine:
            led.settle(run.batch_id, int(t["idx"]), int(t["attempt"]), "backlog", only_from=("in_flight",),
                       evidence="never sent: the server that queued it died first, and its task group lists nothing "
                                "for it")
            _event(run.batch_id, "batch.settle", int(t["idx"]), int(t["attempt"]), "never_sent")


async def _reconcile(app: AppCtx, run: _Run) -> None:
    """submit_unknown rows: the batch's task group, asked for every task in it. A task the ledger does not know is
    pulled and, once it has a result, settles the row its HPCB_RESULT names; while one is still pending, nothing is
    sent again. With none left, an idempotent row goes back to the backlog (a new attempt); a non-idempotent one
    never does — it keeps its slot until the user cancels."""
    led = ledger_of(app)
    unknown = led.tasks(run.batch_id, states=("submit_unknown",))
    if not unknown:
        return
    groups = led.task_groups(run.batch_id)
    client = _lookup_client(app, run)
    if not groups or client is None:
        return
    ids = await _group_tasks(client, run.batch_id, groups)
    if ids is None:
        return
    known = {str(t["task_id"]) for t in led.tasks(run.batch_id) if t.get("task_id")}
    strays = [i for i in ids if i not in known]
    pending = False
    if strays:
        got = await asyncio.to_thread(collect_rest, client, strays)
        for tid, res in got.items():
            if res.state == "pending":
                pending = True
                continue
            if res.state != "success":
                continue
            rep = parse_result(_shell_fields(res.result)[1])
            if rep is None or rep.duplicate:
                continue
            row = led.task(run.batch_id, rep.index)
            if row is not None and row["state"] == "submit_unknown" and int(row["attempt"]) == rep.attempt:
                led.settle(run.batch_id, rep.index, rep.attempt, "submit_unknown", task_id=tid,
                           unheld="its copy's result came back (the batch's task group)")
                _settle_result(app, run, rep.index, rep.attempt, res.result, None)
    if pending or not led.batch(run.batch_id)["idempotent"]:
        return   # a non-idempotent row is never sent again, nor called lost on absence: the failed POST may have
        # queued its task in a group of its own, which no runner ever learnt
    for t in led.tasks(run.batch_id, states=("submit_unknown",)):
        # idempotent: sent again. Its first copy may still be queued in a group we never learnt, so the row keeps
        # holding the stop (`unknown_since`) until evidence clears it
        led.settle(run.batch_id, int(t["idx"]), int(t["attempt"]), "backlog", only_from=("submit_unknown",),
                   evidence="reconciled: no task of ours in the batch's known task groups answers for it; sent again "
                            "(idempotent)")


async def _group_tasks(client: Any, bid: str, groups: list[str]) -> list[str] | None:
    """Every task in the batch's task groups (each runner's), or None when one could not be read (try again later)."""
    ids: list[str] = []
    for tg in groups:
        try:
            r = await asyncio.to_thread(client._compute_web_client.v2.get_task_group, tg)
        except Exception as exc:  # noqa: BLE001 - try again next pass
            print(f"hpc-bridge: batch {bid}: task-group read failed ({exc})", file=sys.stderr)
            return None
        ids += [str(x.get("id")) for x in (r.get("tasks") or []) if x.get("id")]
    return ids


def _presume(app: AppCtx, run: _Run) -> None:
    """A task out past its clock (block evidence + task_walltime_s + the grace) is presumed lost — it keeps its slot, is
    never retried, and only unblocks a stop or a cancel; a later result still wins. On a MEP every row out (no evidence
    channel exists there); on SSH only a row this process holds no future for (a dead server sent it: its evidence may
    never come). Never a submit of unknown fate: its task may be queued where no clock of ours can see it
    (`_unknown_evidence`). The block evidence is this process's, else the ledger's (a block seen before the restart:
    any admission needed one)."""
    led = ledger_of(app)
    wall = float(led.batch(run.batch_id)["task_walltime_s"])
    seen = run.block_seen_at
    held = set(run.futures)
    ssh = _has_login_shape(app)
    any_seen = bool(json.loads(led.batch(run.batch_id).get("blocks_seen") or "[]"))
    for t in led.tasks(run.batch_id, states=("in_flight",)):
        key = (int(t["idx"]), int(t["attempt"]))
        if ssh and key in held:
            continue
        if not t.get("admitted_at"):
            continue
        evidence = seen if seen is not None else (float(t["admitted_at"]) if any_seen else None)
        if presumed_lost(admitted_at=float(t["admitted_at"]), block_seen_at=evidence, task_walltime_s=wall,
                         now=_now()):
            led.settle(run.batch_id, int(t["idx"]), int(t["attempt"]), "presumed_lost", only_from=("in_flight",),
                       settled_by="presumption", evidence="no result past task_walltime_s + the grace since a block "
                                                          "was seen; it keeps its slot and is never retried")
            _event(run.batch_id, "batch.settle", int(t["idx"]), int(t["attempt"]), "presumed_lost")


RELEASE_OLD_UEP_S = 300.0   # how long the reaped block's user endpoint is watched for a relaunched pilot
RELEASE_EVERY_S = 15.0


async def _release_old_uep(app: AppCtx, run: _Run) -> None:
    """After a reap on SSH: parsl starts a block for the task that was out on the dead one (it stays outstanding until
    the interchange declares its manager lost, ~105 s: S8) — a block nobody confirmed (#204's pattern). That user
    endpoint's config is retired and nothing of ours runs there any more (the batch and the interactive shape wait for
    the user's yes, which asks for a fresh one), so its pilots are cancelled, by its own `uep.<eid>.<uep id>` marker —
    never the endpoint-wide one, which would take the new block too."""
    if (not run.old_uep or _now() > run.old_uep_until or login_runner is None or not _has_login_shape(app)
            or _now() - run.last_release < RELEASE_EVERY_S):
        return
    cur = app.shapes.get("compute")
    if cur is not None and cur.spend_confirmed and run.old_uep_hash is not None and uec_hash(cur) == run.old_uep_hash:
        # the block the user confirmed after the reap went to the SAME user endpoint (the same config: no fresh one per
        # confirmation here): its pilot carries that marker and is the user's block — never released
        run.old_uep = None
        return
    run.last_release = _now()
    mark = shlex.quote(run.old_uep)
    cmd = (f"ids=$(squeue -h -u \"$USER\" -o '%i|%o' 2>/dev/null | awk -F'|' -v m={mark} 'index($2,m){{print $1}}'); "
           '[ -n "$ids" ] && scancel $ids 2>/dev/null; echo "HPCB_RELEASED $ids"')
    try:
        res = await login_runner(app)(cmd)
    except Exception:  # noqa: BLE001 - try again next pass
        return
    m = re.search(r"HPCB_RELEASED ?(.*)", res.stdout or "")
    jobs = [j for j in (m.group(1).split() if m else []) if session_shell._JOB_ID.fullmatch(j)]
    led = ledger_of(app)
    for j in jobs:
        led.add_block_seen(run.batch_id, j)
        led.event(run.batch_id, "batch.release", {"job": j, "uep": run.old_uep})
        _journal({"event": "batch.release", "batch_id": run.batch_id, "job_id": j, "uep": run.old_uep})


async def _unknown_evidence(app: AppCtx, run: _Run) -> None:
    """SSH only: an unknown submit stops holding the stop once evidence says its task can never run nor start a block
    — the config it went out with is retired (a stop or a reap: nothing of ours submits with it again), and the user
    endpoint that config targets has exited, with no pilot of it queued, in two looks a few seconds apart. A task
    left pending at the service does not start a user endpoint again; only a new submit with its config would (spike
    S5). Counted from the failure (`unknown_since`), never the admission: the POST is long over by then. The row stays
    what it was in the ledger (its fate is still unknown), with that evidence (`unheld`)."""
    if not _has_login_shape(app) or login_runner is None or app.state.endpoint_id is None:
        return
    now = _now()
    if now - run.last_unknown_look < EVIDENCE_EVERY_S:
        return
    led = ledger_of(app)
    rows = [r for r in led.unknown_holding(run.batch_id)
            if r.get("unknown_since") and now - float(r["unknown_since"]) >= UNKNOWN_SETTLE_S
            and r.get("sent_uec") and led.retired(app.state.endpoint_id, str(r["sent_uec"]))]
    if not rows:
        return
    run.last_unknown_look = now
    seen = json.loads(led.batch(run.batch_id).get("blocks_seen") or "[]")
    by_job: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        job = str(r.get("sent_job") or (seen[-1] if seen else ""))
        if session_shell._JOB_ID.fullmatch(job):
            by_job.setdefault(job, []).append(r)
    for job, group in by_job.items():
        q = shlex.quote(job)
        cmd = (f"u=$(sacct -X -n -P -j {q} -o SubmitLine%800 2>/dev/null | grep -o 'uep\\.[0-9a-f.-]*' | head -n 1); "
               'if [ -z "$u" ]; then echo "HPCB_UEPX none"; else '
               'a=$(ps -eo args 2>/dev/null); sleep 3; b=$(ps -eo args 2>/dev/null); '
               'q=$(squeue -h -u "$USER" -o %o 2>/dev/null | grep -cF "$u"); '
               "echo \"HPCB_UEPX $u $(printf '%s\\n' \"$a\" | grep -cF \"$u\") "
               "$(printf '%s\\n' \"$b\" | grep -cF \"$u\") $q ${#a} ${#b}\"; fi")
        try:
            res = await login_runner(app)(cmd)
        except Exception:  # noqa: BLE001 - no evidence this pass
            continue
        m = re.search(r"HPCB_UEPX (uep\.[0-9a-f-]{36}\.[0-9a-f-]{36}) (\d+) (\d+) (\d+) (\d+) (\d+)", res.stdout or "")
        if not m or m.group(2) != "0" or m.group(3) != "0" or m.group(4) != "0" or "0" in (m.group(5), m.group(6)):
            continue
        why = (f"its user endpoint ({m.group(1)}) has exited with no pilot of it queued, and the config it went out "
               "with is retired: a task left pending at the service does not start it again (only a new submit with "
               "that config would), so this submit can neither run nor start a block")
        for r in group:
            led.settle(run.batch_id, int(r["idx"]), int(r["attempt"]), str(r["state"]), unheld=why)
            _event(run.batch_id, "batch.unheld", int(r["idx"]), int(r["attempt"]), str(r["state"]), reason=why)


_ENDED = ("CANCELLED", "TIMEOUT", "FAILED", "COMPLETED", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY", "BOOT_FAIL",
          "DEADLINE", "F")


async def _evidence(app: AppCtx, run: _Run) -> None:
    """SSH only: while tasks are out, look up the job(s) they were sent to on the free login shape now and then; a job
    the scheduler says has ended settles the tasks out on it `lost`, on that evidence (a reaped block: sacct says
    CANCELLED). The rows are the ledger's — a dead server's too — and each one's job is the one it was sent to (this
    process's record, else the ledger's `sent_job`, else the last block the batch saw)."""
    if not _has_login_shape(app) or login_runner is None:
        return
    now = _now()
    led = ledger_of(app)
    out_rows = led.tasks(run.batch_id, states=("in_flight",))
    if not out_rows:
        return
    oldest = min((float(t["admitted_at"]) for t in out_rows if t.get("admitted_at")), default=None)
    if oldest is None or now - oldest < EVIDENCE_AFTER_S or now - run.last_evidence < EVIDENCE_EVERY_S:
        return
    run.last_evidence = now
    seen = json.loads(led.batch(run.batch_id).get("blocks_seen") or "[]")
    fallback = run.block_job or (str(seen[-1]) if seen else None)
    by_job: dict[str, list[tuple[int, int]]] = {}
    for t in out_rows:
        key = (int(t["idx"]), int(t["attempt"]))
        job = run.sent_job.get(key) if key in run.sent_job else (t.get("sent_job") or None)
        job = job or fallback
        if job and session_shell._JOB_ID.fullmatch(str(job)):
            by_job.setdefault(str(job), []).append(key)
    for job, keys in by_job.items():
        await _evidence_for(app, run, job, keys, now)


async def _evidence_for(app: AppCtx, run: _Run, job: str, keys: list[tuple[int, int]], now: float) -> None:
    led = ledger_of(app)
    q = shlex.quote(job)
    cmd = (f"s=$(squeue -h -j {q} -o %T 2>/dev/null | head -n 1); "
           f"a=$(sacct -X -n -P -j {q} -o State 2>/dev/null | head -n 1 | awk '{{print $1}}'); "
           f"u=$(sacct -X -n -P -j {q} -o SubmitLine%800 2>/dev/null | grep -o 'uep\\.[0-9a-f.-]*' | head -n 1); "
           f"p=$(qstat -x -f {q} 2>/dev/null | sed -n 's/^ *job_state = //p' | head -n 1); "
           'echo "HPCB_JOB_STATE squeue=$s sacct=$a pbs=$p uep=$u"')
    assert login_runner is not None
    try:
        res = await login_runner(app)(cmd)
    except Exception:  # noqa: BLE001 - no evidence this pass
        return
    m = re.search(r"HPCB_JOB_STATE squeue=(\S*) sacct=(\S*) pbs=(\S*) uep=(\S*)", res.stdout or "")
    if not m:
        return
    sq, sa, pb = m.group(1), m.group(2), m.group(3)
    if sq in ("RUNNING", "COMPLETING", "PENDING", "CONFIGURING", "SUSPENDED") or pb in ("R", "Q", "E", "H"):
        return
    state = sa if sa in _ENDED else ("F" if pb == "F" else "")
    if not state:
        return
    why = f"sacct says the block's job {job} is {state}" if state != "F" else f"PBS says the block's job {job} finished"
    rt = run.rt
    if re.fullmatch(r"uep\.[0-9a-f-]{36}\.[0-9a-f-]{36}", m.group(4) or "") and job == run.block_job:
        run.old_uep, run.old_uep_until = m.group(4), now + RELEASE_OLD_UEP_S
        run.old_uep_hash = run.runner_hash or (uec_hash(rt) if rt is not None else None)
    for key in keys:
        index, attempt = key
        _drop(run, key)
        if led.batch(run.batch_id)["idempotent"]:
            led.settle(run.batch_id, index, attempt, "backlog", only_from=list(HOLDING),
                       evidence=f"retry on evidence: {why}")
        else:
            led.settle(run.batch_id, index, attempt, "lost", only_from=list(HOLDING), settled_by=f"evidence: {why}",
                       evidence=why)
        _event(run.batch_id, "batch.settle", index, attempt, "lost", reason=why)
    full = f"the block (job {job}) ended under the batch — {why}"
    if job == run.block_job and rt is not None:
        if app.shapes.get("compute") is rt and rt.reaped is None:
            warmth._mark_reaped(app, rt, full, warmth._release_bound(app, "compute", rt), kicked=False)
        retire_all(app, rt, full)
    if led.batch(run.batch_id)["state"] in ("admitting", "needs_confirmation"):
        pause(app, run.batch_id, f"the block was reaped — {full}; its tasks out were settled lost, the backlog is kept")


# ---- the answer ------------------------------------------------------------------------------------------------------

def _task_result(t: dict[str, Any]) -> BatchTaskResult:
    return BatchTaskResult(
        index=int(t["idx"]), state=t["state"], attempt=int(t["attempt"] or 0), row=json.loads(t["row_json"]),
        exit_code=t.get("rc"), tail=t.get("tail") or "", stderr_tail=t.get("stderr_tail") or "",
        stdout_path=t.get("stdout_path"), stdout_bytes=t.get("stdout_bytes"), truncated=bool(t.get("truncated")),
        job_id=t.get("job_id"), host=t.get("host"), started_at=t.get("started_at"), settled_by=t.get("settled_by"),
        task_id=t.get("task_id"))


def _status_of(app: AppCtx, bid: str, *, detail: str = "summary", offset: int = 0, limit: int = 50) -> BatchStatus:
    led = ledger_of(app)
    b = led.batch(bid)
    c = led.counts(bid)
    run = app.batches.get(bid)
    total = sum(c.values())
    limit = max(1, min(int(limit or 50), 500))
    offset = max(0, int(offset or 0))
    if detail == "page":
        rows = led.tasks(bid, offset=offset, limit=limit)
        nxt = offset + len(rows) if offset + len(rows) < total else None
    elif detail == "failed":
        rows = led.tasks(bid, states=("failed", "lost", "presumed_lost", "submit_unknown"), offset=offset, limit=limit)
        nxt = offset + len(rows) if len(rows) == limit else None
    else:
        rows, nxt = [], None
    state = str(b["state"])
    phase: Any
    if state in ("done", "cancelled", "needs_confirmation"):
        phase = state
    elif _held(c) == 0 and c.get("done", 0) + c.get("failed", 0) == 0 and run is not None and run.waiting:
        phase = "waiting"
    else:
        phase = "admitting"
    owner = None
    gone = None
    if run is None and state not in ("done", "cancelled"):
        beat = int(_now() - float(b["heartbeat"] or 0))
        owner = f"pid {b['owner_pid']} on {b['owner_host']} (heartbeat {beat} s ago)"
        holder = led.holder(bid).holder
        heartbeating = time.time() - float(b["heartbeat"] or 0) <= led.lease_stale_s and led.pid_alive(holder.pid)
        if not heartbeating and not led.holder_alive(holder):
            # its server is gone (that pid is dead here): nothing admits for it, whatever its last state said; this
            # server takes the lease over once the heartbeat is stale, and then asks again before sending more
            phase = "needs_confirmation"
            gone = (f"the hpc-bridge server that ran this batch is gone ({owner}); this server takes it over once "
                    f"its heartbeat is {int(led.lease_stale_s)} s old, collects what finished, and sends nothing more "
                    "before the user's yes")
    reason = gone or (b.get("pause_reason") if phase in ("needs_confirmation", "cancelled") else (
        run.waiting if run is not None and phase == "waiting" else None))
    parts = [f"{bid}: {c.get('done', 0)} done, {c.get('failed', 0)} failed, {_held(c)} out "
             f"({c.get('in_flight', 0)} in flight), {c.get('backlog', 0)} in the backlog of {total}"]
    for k in ("lost", "presumed_lost", "submit_unknown", "cancelled"):
        if c.get(k):
            parts.append(f"{c[k]} {k.replace('_', ' ')}")
    text = "; ".join(parts) + "."
    if phase == "needs_confirmation":
        text += (f" Paused: {reason}. It resumes only with run_batch(batch_id='{bid}', confirm_spend=True) — ask the "
                 "user first.")
    if owner:
        text += f" Another hpc-bridge server holds this batch ({owner}): this is a read of the ledger."
    if c.get("failed") and detail == "summary":
        text += f" status('{bid}', detail='failed') lists the failed rows with their exit codes."
    text += _home_warning(b.get("results_dir"))
    return BatchStatus(
        batch_id=bid, phase=phase, tasks=total, chunk=int(b.get("chunk") or 1), capacity=int(b["capacity"] or 1),
        counts=BatchCounts(**{k: c.get(k, 0) for k in BatchCounts.model_fields}),
        results=[_task_result(t) for t in rows], offset=offset, next_offset=nxt,
        results_dir=batch_dir(str(b["results_dir"]), bid) if b.get("results_dir") else None,
        blocks_seen=json.loads(b.get("blocks_seen") or "[]"), owner=owner, reason=reason, notice=text)


# ---- poll_task by Globus task id -------------------------------------------------------------------------------------

def is_task_uuid(task_id: str) -> bool:
    return bool(_UUID.match(str(task_id or "")))


async def poll_uuid(app: AppCtx, task_id: str) -> ShellOutcome | None:
    """poll_task with a Globus task id: a batch row (this process's future, else the ledger), or an interactive task
    the ledger recorded at send — its result by REST. None when the ledger knows nothing of it."""
    try:
        led = ledger_of(app)
    except LedgerError:
        return None
    t = led.task_by_tid(task_id)
    if t is not None:
        run = app.batches.get(str(t["batch_id"]))
        if run is not None:
            _collect(app, run)
            await _rest_pull(app, run)
            t = led.task_by_tid(task_id) or t
        if t["state"] in TERMINAL:
            return ShellOutcome(phase="complete" if t["state"] in ("done", "failed") else "failed",
                                exit_code=t.get("rc"), stdout=t.get("tail") or "",
                                stderr_snippet=t.get("stderr_tail") or "", block_state="warm",
                                notice=f"row {t['idx']} of {t['batch_id']}: {t['state']}"
                                       + (f" ({t['settled_by']})" if t.get("settled_by") else "")
                                       + (f"; the whole stdout is at {t['stdout_path']}"
                                          if t.get("stdout_path") else ""))
        return ShellOutcome(phase="running", block_state="warm", task_id=task_id, globus_task_id=task_id,
                            notice=f"row {t['idx']} of {t['batch_id']} is {t['state'].replace('_', ' ')}; "
                                   f"status('{t['batch_id']}') follows the whole batch")
    it = led.itask(task_id)
    if it is None:
        return None
    client = _lookup_client(app)
    got: RestResult | None = None
    if client is not None:
        try:
            got = (await asyncio.to_thread(collect_rest, client, [task_id])).get(task_id)
        except Exception as exc:  # noqa: BLE001 - the service unreachable: the session dir may still answer
            print(f"hpc-bridge: poll_task {task_id}: REST failed ({exc})", file=sys.stderr)
    if got is not None and got.state == "failed":
        return ShellOutcome(phase="failed", exit_code=1, stderr_snippet=got.error[-4000:], block_state="warm",
                            notice="the task failed (its result from the service)")
    if got is not None and got.state == "success":
        rc, out, err = _shell_fields(got.result)
        return ShellOutcome(phase="complete", exit_code=rc, stdout=out, stderr_snippet=err, block_state="warm")
    # the service cannot answer (no client yet, unreachable, or it has no result: past its window, or not yet
    # delivered) — the session dir the command wrote its rc and tail to may (session_shell.wrap(result_key=…))
    kept = await _session_result(app, it)
    if kept is not None:
        rc, tail = kept
        return ShellOutcome(phase="complete", exit_code=rc, stdout=tail, block_state="warm",
                            notice="read from the session dir on the facility (the service had no result for it): the "
                                   "exit code and the output's last 4 kB")
    if client is None:
        return ShellOutcome(phase="running", block_state="cold", task_id=task_id, globus_task_id=task_id,
                            notice="the ledger knows this task, but no Globus client is up yet to look it up: "
                                   "connect_facility first, then poll_task again")
    return ShellOutcome(phase="running", block_state="warm", task_id=task_id, globus_task_id=task_id,
                        notice="the service lists the task as still pending")


async def _session_result(app: AppCtx, it: dict[str, Any]) -> tuple[int | None, str] | None:
    """An interactive task's rc and output tail from its session dir, read over the free login shape (an SSH
    endpoint's; a facility MEP has no free channel to read it): None when there is none, or no channel."""
    key, root, sid = it.get("result_key"), it.get("state_dir"), it.get("session_id") or "default"
    if not key or not root or login_runner is None or not _has_login_shape(app):
        return None
    try:
        d = f"{session_shell.Session(str(sid), str(root)).quoted_state_dir()}/results/{shlex.quote(str(key))}"
    except ValueError:
        return None
    cmd = (f'if [ -f {d}/rc ]; then echo "HPCB_RC $(cat {d}/rc)"; '
           f'echo "HPCB_TAIL $(base64 < {d}/tail 2>/dev/null | tr -d \'\\n\')"; fi')
    try:
        res = await login_runner(app)(cmd)
    except Exception:  # noqa: BLE001 - no answer from the files either
        return None
    m = re.search(r"^HPCB_RC (\S*)", res.stdout or "", re.M)
    if m is None:
        return None
    t = re.search(r"^HPCB_TAIL (\S*)", res.stdout or "", re.M)
    rc = int(m.group(1)) if m.group(1).lstrip("-").isdigit() else None
    return rc, _b64(t.group(1)) if t else ""


def record_interactive(app: AppCtx, task_id: str | None, *, shape: str, session_id: str, result_key: str | None,
                       handle: str | None) -> None:
    """An interactive task the agent may poll by its Globus id after a restart."""
    if not task_id:
        return
    try:
        ledger_of(app).record_itask(str(task_id), endpoint_id=app.state.endpoint_id, shape=shape,
                                    session_id=session_id, result_key=result_key, state_dir=app.scratch_root,
                                    handle=handle)
    except LedgerError:
        pass


_ = (UnknownBatch,)
