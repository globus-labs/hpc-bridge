# src/hpc_bridge/catalog/entry.py
from __future__ import annotations

import datetime
import hashlib
import json
import re
import unicodedata
import uuid
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, Field, field_validator, model_validator

from ..models import validate_host


class Allocation(BaseModel):
    """How to LIST a user's allocations on this machine — not the allocations themselves.

    `parser` names a deterministic, plugin-side parser (Plan 2). The command's stdout is
    parsed in code, never handed to the model — inference is exactly what the catalog removes.
    """

    command: str
    parser: Literal["sbank", "iris", "mybalance"]


SAFE_ENDPOINT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

# The hpc-bridge-fixed UEC keys a MEP facility's own template is allowed to rename via Compute.key_map
# (module-level, not a class attribute: pydantic v2 silently turns an underscore-prefixed class attribute
# into a ModelPrivateAttr wrapper, which broke `set(v) - cls._X` with a TypeError on every Compute
# validation, key_map present or not — found while validating this exact patch, 2026-09-07).
MEP_RENAMEABLE_KEYS = frozenset({
    "account", "partition", "walltime", "max_workers_per_node",
    "nodes_per_block", "init_blocks", "max_blocks", "interface",
})


_VERSION = re.compile(r"^v?[0-9][0-9A-Za-z.+-]{0,31}$")
WORKER_STRATEGIES = ("pin", "float")


class WorkerEnv(BaseModel):
    """Facility-MEP entries: how the worker pool's Python packages are kept in step with the facility's USER
    endpoint process. parsl's interchange<->worker protocol changes between releases without compatibility, and
    globus-compute-endpoint only sets a parsl FLOOR (4.16.0: parsl>=2026.7.27) — so the same endpoint version can
    run different parsl, and a skew makes the block run (and bill) while every result is dropped (2026-10-06:
    Anvil's UEP floated to a newer parsl than our worker; Delta's fixed install sat on an older one than a fresh
    worker venv picks).

    The install itself stays in `env_setup` (literal pins / `--upgrade`), so EVERY plugin version reading the
    registry gets a working worker — a new placeholder there would reach older plugins unresolved. This records
    the strategy and what the entry was proven against.

    Read LENIENTLY: every installed plugin parses registry entries, so a strategy a later curator adds must not
    make this version drop the facility. `worker_env_problems` is the strict check, run at ingest."""

    # "pin": the facility runs a FIXED install; env_setup pins the worker's packages to it (explicit parsl==…).
    # "float": the facility rebuilds the UEP's environment at the latest versions at every start; env_setup
    # installs with --upgrade so the worker resolves the same (racy across a parsl release while a UEP lives).
    strategy: str
    # The facility endpoint version (`endpoint_version` in its metadata) the entry was last proven against. At
    # attach, a different live version means the facility upgraded since — its packages may have moved. Also the
    # `{gce_version}` fallback when the live version can't be read.
    verified_with: str | None = None

    @field_validator("verified_with")
    @classmethod
    def _version(cls, v: str | None) -> str | None:
        v = (v or "").strip()
        return v.lstrip("vV") if _VERSION.match(v) else None  # unusable -> no claim, never a broken entry


def _install_segments(env_setup: str) -> list[str]:
    """The `… pip install …` commands in an env_setup line that install globus-compute-endpoint."""
    parts = re.split(r";|&&|\|\||\n", env_setup)
    return [p for p in parts if "pip install" in p and "globus-compute-endpoint" in p]


def _upgrades(segment: str) -> bool:
    """Whether a pip/uv install command upgrades every package: `--upgrade`, or `-U` in a short-flag cluster
    (`-qU`). `-P`/`--upgrade-package X` upgrades only X — not a float."""
    for tok in segment.split():
        if tok == "--upgrade" or (tok.startswith("-") and not tok.startswith("--") and "U" in tok[1:]):
            return True
    return False


def verification_problems(entry: Any) -> list[str]:
    """A facility-endpoint entry's verification block, if present, must be COMPLETE: a field left out is a comparison
    the health check silently skips (review of 0.1.21)."""
    ver = getattr(entry, "verification", None)
    if ver is None or not getattr(entry, "compute_mep_uuid", None):
        return []
    # only what every facility endpoint publishes and the client relies on: a field a facility does not publish must
    # not block its entry for good (the health check reports the gap as "incomplete" instead)
    fields = ("verified_on", "endpoint_version")
    missing = [f for f in fields if getattr(ver, f) is None]
    if not missing:
        return []
    return [f"verification lacks {', '.join(missing)} (re-prove with agentic/registry_reprove.py --record)"]


def verification_raw_problems(raw: Any) -> list[str]:
    """The curator's check of a seed row's RAW `verification` block, before the lenient parse can forgive it: every
    field given must survive the parse (an unquoted version, a short digest, a bare `on:` key would be dropped)."""
    if raw is None:
        return []
    if not isinstance(raw, dict):
        return [f"verification must be a mapping, got {type(raw).__name__}"]
    known = set(Verification.model_fields)
    unknown = sorted(str(k) for k in set(raw) - known)
    problems = [f"verification has unknown key(s) {unknown}"] if unknown else []
    try:
        parsed = Verification.model_validate({k: v for k, v in raw.items() if k in known})
    except (ValueError, TypeError) as exc:
        return problems + [f"verification does not parse: {exc}"[:200]]
    for k in known & set(raw):
        if raw[k] is not None and getattr(parsed, k) is None:
            problems.append(f"verification.{k} = {raw[k]!r} is not usable (quote versions; 64-hex digests; a date)")
    return problems


def worker_env_raw_problems(raw: Any) -> list[str]:
    """The curator's check of a seed row's RAW `compute.worker_env`, before the lenient parse can forgive it: a block
    the client would silently drop (a list, a missing strategy, an unquoted `verified_with: 4.16` YAML float) must
    be refused at ingest, not published as null — that would quietly remove the entry's verified-version fallback."""
    if raw is None:
        return []
    if not isinstance(raw, dict):
        return [f"worker_env must be a mapping, got {type(raw).__name__}"]
    problems = []
    if not isinstance(raw.get("strategy"), str):
        problems.append("worker_env.strategy must be a string (pin | float)")
    vw = raw.get("verified_with")
    if not isinstance(vw, str) or not _VERSION.match(vw.strip()):
        problems.append(f"worker_env.verified_with must be a quoted version string like \"4.16.0\", got {vw!r}")
    unknown = sorted(set(raw) - {"strategy", "verified_with"})
    if unknown:
        problems.append(f"worker_env has unknown key(s) {unknown}")
    return problems


# Characters a hint may not carry: it is quoted (") in agent-facing text, and a tag-like `<…>` or markup could pass for
# something other than the registry's words. Read leniently: backticks and double quotes become ', angle brackets go.
_HINT_MARKUP = frozenset('"`<>')


def _hidden_char(ch: str) -> bool:
    """A character an agent-facing line must not carry: a control or format character (ESC/ANSI, NUL, bidi overrides,
    zero-width), or a line/paragraph separator."""
    return unicodedata.category(ch) in ("Cc", "Cf", "Zl", "Zp")


def account_hint_raw_problems(raw: Any) -> list[str]:
    """The curator's check of a seed row's RAW `account_hint`, before the lenient parse can forgive it: a hint the
    client would drop, cut or clean (not text, longer than one short line, a control/format character or a line break)
    is refused at ingest, not published altered."""
    if raw is None:
        return []
    if not isinstance(raw, str) or not raw.strip():
        return [f"account_hint must be a non-empty string, got {type(raw).__name__}"]
    if any(_hidden_char(ch) for ch in raw):
        return ["account_hint has a control/format character or a line break: one line of plain text only"]
    if any(ch in _HINT_MARKUP for ch in raw):
        return ["account_hint has a quote, a backtick or an angle bracket: plain text only (single quotes are fine)"]
    if len(" ".join(raw.split())) > ACCOUNT_HINT_MAX:
        return [f"account_hint is longer than {ACCOUNT_HINT_MAX} characters"]
    return []


# Slurm short options that take no argument: in a bundle ("-vA x") the scan goes on past them; any other letter takes
# the rest of the token as its argument, so the scan stops there.
_SLURM_NO_ARG = frozenset("hHIkOQsvVW")
# A directive line of any batch system sbatch or qsub reads, with or without a space after its word (`#SBATCH-A x`,
# `#PBS-Ax` are directives too): sbatch reads `#SLURM` as `#SBATCH`, and translates `#PBS` and `#BSUB` lines after its
# own pass — so `#BSUB -q gpu` would override the partition parsl writes.
_DIRECTIVE = re.compile(r"\s*#(SBATCH|SLURM|PBS|BSUB)(.*)")
# What a directive the block's terms (or hpc-bridge's own hold on its pilots) cannot take sets, in a refusal's words.
TERMS_WORDS = {"account": "the account or the partition/queue", "queue": "the account or the partition/queue",
               "walltime": "the walltime or the node count", "nodes": "the walltime or the node count",
               "select": "the node count (a select)", "output": "the pilot's output file",
               "error": "the pilot's error file", "array": "a job array"}
# Slurm short options for those, and their kinds: -o (the output file: parsl writes its own in the pilot's user
# endpoint directory, the path squeue's StdOut shows, which hpc-bridge's probe and stop find the pilot by), -e (the
# error file, beside it: where the probe reads why a pilot died) and -a (an array: several blocks behind one spend
# question) besides the terms.
_SLURM_SHORT = {"A": "account", "p": "queue", "N": "nodes", "t": "walltime", "o": "output", "e": "error",
                "a": "array"}


def _slurm_terms(toks: list[str]) -> str | None:
    """Slurm's options for the block's terms: -A/--account and -p/--partition (long options by any prefix sbatch
    accepts: --acco…, --part…; short ones bundled too, "-vA x"; its -q is the QOS), the size parsl writes before
    scheduler_options and a later directive overrides (-N/--nodes, -t/--time), the pilot's files (-o/--output: parsl's
    own carries the `uep.<eid>` marker squeue's StdOut shows — the stop cancels by it, the probe finds the pilot by it;
    -e/--error: parsl's own, beside it, is where the probe reads why a dead pilot exited) and an array (-a/--array:
    several blocks)."""
    for tok in toks:
        if m := re.match(r"--(acco|part)[a-z-]*(?:=|$)", tok):
            return "account" if m.group(1) == "acco" else "queue"
        if re.match(r"--nodes(?:=|$)", tok):
            return "nodes"
        if re.match(r"--time(?:=|$)", tok):
            return "walltime"
        if re.match(r"--ou[a-z]*(?:=|$)", tok):
            return "output"
        if re.match(r"--er[a-z]*(?:=|$)", tok):
            return "error"
        if re.match(r"--ar[a-z]*(?:=|$)", tok):
            return "array"
        if re.match(r"-[^-]", tok):
            for ch in tok[1:]:
                if ch in _SLURM_SHORT:
                    return _SLURM_SHORT[ch]
                if ch not in _SLURM_NO_ARG:
                    break
    return None


def _pbs_select(listed: str) -> str | None:
    """The `select=` value in a PBS `-l` resource list, or None."""
    for item in listed.split(","):
        name, _eq, value = item.partition("=")
        if name.strip().lower() == "select":
            return value.strip()
    return None


def _pbs_l_lists(toks: list[str]) -> list[str]:
    """Every `-l` resource list of a PBS directive's options (`-l a=1,b=2` or `-la=1`)."""
    return [tok[2:] or (toks[i + 1] if i + 1 < len(toks) else "") for i, tok in enumerate(toks) if tok.startswith("-l")]


def _pbs_terms(toks: list[str]) -> str | None:
    """PBS's options for the block's terms: -A and -q, the time and size parsl writes as `-l walltime=…` and
    `-l select=…` before scheduler_options (a `-l` list naming walltime, nodes or nodect; select apart: its chunk
    resources go in `select_options`), and an array (-J). -o and -e stay: a PBS pilot keeps the `uep.<eid>` marker in
    its record's Submit_arguments (the script's path) whatever its output file."""
    for tok in toks:
        if tok.startswith("-A"):
            return "account"
        if tok.startswith("-q"):
            return "queue"
        if tok.startswith("-J"):
            return "array"
    for listed in _pbs_l_lists(toks):
        for item in listed.split(","):
            name = item.split("=", 1)[0].strip().lower()
            if name == "walltime":
                return "walltime"
            if name in ("nodes", "nodect"):
                return "nodes"
            if name == "select":
                return "select"
    return None


def _bsub_terms(toks: list[str]) -> str | None:
    """LSF's (#BSUB, which sbatch translates): -q the queue, -P the project (account), -W the run limit, -n/-nnodes."""
    for tok in toks:
        for flag, kind in (("-q", "queue"), ("-P", "account"), ("-W", "walltime"), ("-n", "nodes")):
            if tok.startswith(flag):
                return kind
    return None


# A chunk resource list for PBSProProvider's `select_options` (parsl writes `select=<nodes>:ncpus=<cpus_per_node>`
# and appends `:<select_options>`): `name=value` pairs joined by ":", e.g. `ngpus=4`, `system=polaris:ngpus=4`.
_SELECT_PAIR = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=[A-Za-z0-9_.@/-]+")
# What the chunk may not name: hpc-bridge's own (the count is the block's nodes; ncpus is `cpus_per_node`) and what
# is no chunk resource at all.
_SELECT_NOT = {"select": "a select of its own", "ncpus": "ncpus (that is `cpus_per_node`)", "walltime": "the walltime",
               "nodes": "the node count", "nodect": "the node count", "place": "a placement (`-l place=` belongs in "
               "scheduler_options)", "account": "the account", "queue": "the queue"}


def select_options_problem(opts: str | None) -> str | None:
    """Why `opts` is not a chunk resource list for `select_options` (PBS), or None: one `name=value` pair or several
    joined by ":" (a leading ":" is fine: parsl adds one), with no chunk count of its own (`2:` or `+` — the count is
    the block's nodes, hpc-bridge's), no `select=`, and none of the block's terms or hpc-bridge's own (ncpus is
    `cpus_per_node`)."""
    if opts is None:
        return None
    text = opts[1:] if opts.startswith(":") else opts
    if not text:
        return "select_options is empty"
    if "+" in text:
        return "select_options has a second chunk ('+'): the block is one chunk per node, hpc-bridge's count"
    for part in text.split(":"):
        if part.isdigit():
            return (f"select_options starts a chunk count ({part!r}): the count is the block's nodes, which "
                    "hpc-bridge writes (select=<nodes>:…)")
        if not _SELECT_PAIR.fullmatch(part):
            return (f"select_options has {part[:60]!r}, not a chunk resource: write name=value pairs joined by ':' "
                    "(e.g. ngpus=4:system=polaris)")
        name = part.split("=", 1)[0].lower()
        if name in _SELECT_NOT:
            return f"select_options names {_SELECT_NOT[name]} ({part[:60]!r})"
    return None


class SelectAdvice(NamedTuple):
    """What a refused one-chunk `-l select=<n>:…` line becomes (`select_advice`): the chunk's other resources as
    `select_options`, its ncpus as `cpus_per_node`, and the line's OTHER resources and options (`-l filesystems=…`
    beside the select, or another `-l` list) as a directive line of their own — None for each that the line has not."""

    options: str | None
    cpus: int | None
    rest: str | None


def select_advice(line: str, nodes: int = 1) -> SelectAdvice | None:
    """For a PBS directive line with a one-chunk `-l select=<n>:…` (n the block's nodes) whose chunk resources are
    `select_options`' own: what it becomes (`SelectAdvice`) — the select taken out of its `-l` list, and whatever else
    the line names kept as `#PBS <the rest>`, so nothing beside the select (a Polaris `filesystems=`) is lost. None
    where it is no such line, or where the rest sets one of the block's terms too (a second select, a walltime)."""
    m = _DIRECTIVE.match(line)
    if m is None or m.group(1) != "PBS":
        return None
    toks = m.group(2).split()
    value: str | None = None
    kept: list[str] = []
    i = 0
    while i < len(toks):
        tok = toks[i]
        if not tok.startswith("-l"):
            kept.append(tok)
            i += 1
            continue
        listed = tok[2:] or (toks[i + 1] if i + 1 < len(toks) else "")
        i += 1 if tok[2:] else 2
        items = listed.split(",")
        at = next((k for k, it in enumerate(items) if it.partition("=")[0].strip().lower() == "select"), None)
        if at is not None and value is None:
            value = items[at].partition("=")[2].strip()
            items = items[:at] + items[at + 1:]
        if items := [it for it in items if it.strip()]:
            kept += ["-l", ",".join(items)]
    if value is None:
        return None
    count, _sep, chunk = value.partition(":")
    if not count.isdigit() or int(count) != nodes or "+" in chunk:
        return None
    parts = [p for p in chunk.split(":") if p]
    cpus = next((p.split("=", 1)[1] for p in parts if p.lower().startswith("ncpus=")), None)
    others = ":".join(p for p in parts if not p.lower().startswith("ncpus="))
    if others and select_options_problem(others) is not None:
        return None
    rest = "#PBS " + " ".join(kept) if kept else None
    if rest is not None and scheduler_terms_kind(rest) is not None:
        return None
    return SelectAdvice(others or None, int(cpus) if cpus is not None and cpus.isdigit() else None, rest)


def scheduler_terms_kind(line: str) -> str | None:
    """Which of the block's terms a directive line sets ("account", "queue", "walltime", "nodes"), or None. Any batch
    system's directive word is read (`_DIRECTIVE`), with its own options."""
    m = _DIRECTIVE.match(line)
    if m is None:
        return None
    word, rest = m.groups()
    toks = rest.split()
    if word in ("SBATCH", "SLURM"):
        return _slurm_terms(toks)
    if word == "PBS":
        return _pbs_terms(toks)
    return _bsub_terms(toks)


def scheduler_terms_directive(opts: str | None) -> str | None:
    """The first directive line in `opts` that sets one of the block's terms (`scheduler_terms_kind`: the account, the
    partition/queue, the walltime or the node count) behind the spend question, or None."""
    for line in (opts or "").splitlines():
        if scheduler_terms_kind(line) is not None:
            return line.strip()
    return None


def _without_terms_directives(opts: str | None) -> str | None:
    """`opts` with every directive line that sets one of the block's terms dropped (None when nothing is left)."""
    kept = [ln for ln in (opts or "").splitlines() if scheduler_terms_directive(ln) is None]
    return "\n".join(kept) or None


# The prefix each scheduler reads a directive by, at the start of a line of a submit script's header.
DIRECTIVE_PREFIX = {"slurm": "#SBATCH", "pbs": "#PBS"}
# A bare option, as each scheduler's directive is shown in a refusal (sbatch has no -l).
_OPTION_EXAMPLE = {"slurm": "--<option>=…", "pbs": "-l <resource>=…"}
# A plain comment: "#" alone, or "#" and a space or a tab.
_PLAIN_COMMENT = re.compile(r"#(?:[ \t].*)?")


class SchedulerOptionsError(ValueError):
    """scheduler_options with a line that is neither a directive nor an option hpc-bridge writes as one."""


def directive_lines(opts: str | None, scheduler: str | None) -> tuple[str | None, list[tuple[str, str]], list[str]]:
    """`opts` as a submit script's header carries them, for `scheduler`'s directive prefix (`DIRECTIVE_PREFIX`).

    parsl writes scheduler_options into the header verbatim, so a line there must be a directive. A line that starts
    with "-" is an option written bare, as a scheduler's own message quotes one ("-l filesystems=…"): it gets the prefix
    ("#PBS -l filesystems=…"). A line of THIS scheduler's directive — its prefix and a space, at the line's start — is
    kept as given; one with whitespace before it (a comment to the scheduler) has it taken off. A plain comment (`# …`)
    is kept. A blank line carries nothing and is dropped. Any other line is refused (`other`): a shell command (in the
    script it runs, and the scheduler stops reading directives at it — on Slurm, parsl writes the partition and account
    directives AFTER scheduler_options, so they would be ignored), another batch system's directive word (sbatch reads
    `#SLURM` and translates `#PBS` and `#BSUB` lines — after its own, so they override; `#SBATCH` on PBS does nothing),
    or a directive with no space after its word (`#SBATCH-A x`). Returns (the text, None when nothing is left; each
    rewrite as (given, written); each refused line). Lines are split as Python splits them (`scheduler_terms_directive`
    reads them so) and joined with "\n", so every break that check reads is a line break in the script too. A scheduler
    with no prefix here (LSF, not supported) gets `opts` back unchanged."""
    prefix = DIRECTIVE_PREFIX.get(scheduler or "")
    if prefix is None or not opts:
        return opts or None, [], []
    own = re.compile(rf"{re.escape(prefix)}[ \t]")
    kept: list[str] = []
    rewrites: list[tuple[str, str]] = []
    other: list[str] = []
    for line in opts.splitlines():
        given = line.strip()
        if not given:
            continue
        if given.startswith("-"):
            kept.append(f"{prefix} {given}")
            rewrites.append((given, kept[-1]))
        elif own.match(given):
            if line[:1].isspace():  # indented: the scheduler would read it as a comment
                kept.append(given)
                rewrites.append((line.rstrip(), given))
            else:
                kept.append(line)
        elif _PLAIN_COMMENT.fullmatch(given):
            kept.append(line)
        else:
            other.append(given)
    return "\n".join(kept) or None, rewrites, other


def scheduler_options_problem(other: list[str], scheduler: str | None) -> str:
    """Why scheduler_options with `other` (lines `directive_lines` refuses) are refused, and what to write instead."""
    prefix = DIRECTIVE_PREFIX.get(scheduler or "", "#PBS")
    example = _OPTION_EXAMPLE.get(scheduler or "", "-l <resource>=…")
    # at most two lines, each cut to 80 characters: the remedy after them must survive the connect's cut
    shown = ", ".join(repr(ln[:80]) for ln in other[:2]) + (f" and {len(other) - 2} more" if len(other) > 2 else "")
    return (f"scheduler_options has a line that is not a {prefix} directive or a plain comment ({shown}): a shell "
            "command would run in the submit script and end the scheduler's reading of directives (on Slurm the "
            "partition and account directives come after it), another batch system's directive word may be read as "
            "this one's or ignored, and a directive needs a space after its word. Write each line as a directive "
            f"({prefix} …), as an option ({example}, written as {prefix} {example}), or as a comment (# …); a shell "
            "command belongs in env_setup")


def ssh_scheduler_options(opts: str | None, scheduler: str | None) -> str | None:
    """An SSH entry's `compute.scheduler_options` read leniently (the registry's, the local cache's): as
    `directive_lines` writes them, with any line that is not a directive dropped — it would end the header's directives
    in our submit script. The ingest refuses both a bare option and such a line (`defaults_extra_raw_problems`)."""
    return directive_lines(opts, scheduler)[0]


def _extra_terms_keys(extra: dict | None, key_map: dict | None) -> list[str]:
    """`defaults.extra` keys that would set the account or the partition behind hpc-bridge's own (and the block's
    terms): `account`/`partition` themselves, or the facility template's names for them (`compute.key_map`)."""
    names = {"account", "partition"} | {v for k, v in (key_map or {}).items() if k in ("account", "partition")}
    return sorted(k for k in (extra or {}) if k in names)


def defaults_extra_raw_problems(row: Any) -> list[str]:
    """The curator's check: a seed row's `defaults.extra` must not carry the account or the partition, and no
    `scheduler_options` (compute's, or extra's) may set them by directive (the client drops them on read, so they would
    be published only to be ignored)."""
    if not isinstance(row, dict):
        return []
    extra = (row.get("defaults") or {}).get("extra") if isinstance(row.get("defaults"), dict) else None
    compute: dict = row["compute"] if isinstance(row.get("compute"), dict) else {}
    key_map = compute.get("key_map")
    bad = _extra_terms_keys(extra if isinstance(extra, dict) else None, key_map if isinstance(key_map, dict) else None)
    problems = [f"defaults.extra sets {bad}: the account and the partition are ensure_endpoint_up's (account=, "
                "partition=) and defaults.partition, so the block's terms name them"] if bad else []
    sel = (row.get("defaults") or {}).get("select_options") if isinstance(row.get("defaults"), dict) else None
    if sel is not None and (why := select_options_problem(str(sel))):  # the client drops it on read: never publish it
        problems.append(f"defaults.{why}")
    elif sel is not None and (compute.get("scheduler") != "pbs" or row.get("compute_mep_uuid")):
        # only hpc-bridge's own PBS template renders it: anywhere else it would be published only to be ignored
        problems.append("defaults.select_options is for an SSH entry on PBS (hpc-bridge's own PBS template writes it "
                        "into the block's select): a Slurm entry's resources are #SBATCH lines in "
                        "compute.scheduler_options, and a facility endpoint's template is its own")
    extra_opts = extra.get("scheduler_options") if isinstance(extra, dict) else None
    opts = compute.get("scheduler_options")
    if isinstance(opts, str) and not row.get("compute_mep_uuid"):
        # an SSH entry's options reach hpc-bridge's own submit script: each line a directive, as the curator writes it
        # (the client prefixes a bare option and drops any other line; a facility endpoint's template is its own)
        written, rewrites, other = directive_lines(opts, compute.get("scheduler"))
        if rewrites:
            problems.append(f"compute.scheduler_options has a bare option ({rewrites[0][0][:120]!r}): write it as "
                            f"the directive {rewrites[0][1][:120]!r}")
        if other:
            problems.append("compute." + scheduler_options_problem(other, compute.get("scheduler")))
        # the terms check reads the lines as the script would carry them, and the refused ones too (`#SLURM --account`)
        opts = "\n".join([*([written] if written else []), *other]) or None
    for where, o in (("compute.scheduler_options", opts), ("defaults.extra.scheduler_options", extra_opts)):
        if isinstance(o, str) and (line := scheduler_terms_directive(o)):
            what = TERMS_WORDS[scheduler_terms_kind(line) or "account"]
            problems.append(f"{where} sets {what} ({line[:120]!r}): the block's terms must name them")
    return problems


def worker_env_problems(compute: Compute) -> list[str]:
    """The curator's strict check of `worker_env` against env_setup (run at ingest; clients read leniently)."""
    we = compute.worker_env
    if we is None:
        return []
    problems = []
    if we.strategy not in WORKER_STRATEGIES:
        problems.append(f"worker_env.strategy {we.strategy!r} is not one of {list(WORKER_STRATEGIES)}")
    if we.verified_with is None:
        problems.append("worker_env.verified_with is missing (the facility endpoint version the entry was proven with)")
    segs = _install_segments(compute.env_setup)
    if not segs:
        problems.append("env_setup has no `pip install … globus-compute-endpoint…` command for worker_env to describe")
    elif we.strategy == "float" and not any(_upgrades(s) for s in segs):
        problems.append("worker_env.strategy 'float' needs `--upgrade` (or -U) on the globus-compute-endpoint install")
    elif we.strategy == "pin":
        if any(_upgrades(s) for s in segs):
            problems.append("worker_env.strategy 'pin' must not upgrade the install (it would float)")
        if not any(re.search(r"parsl==[0-9]", s) for s in segs):
            problems.append("worker_env.strategy 'pin' needs an explicit parsl==… on the install "
                            "(gce sets only a floor)")
    return problems


class Compute(BaseModel):
    """Machine-invariant facts the plugin PINS — the user/agent cannot override these.

    Getting one wrong breaks the endpoint silently (e.g. the wrong `interface` means workers
    never phone home), so this is the same "look up, never infer" category as the UUIDs.
    """

    scheduler: Literal["slurm", "pbs", "lsf"]
    interface: str  # address_by_interface ifname (e.g. ib0)
    env_setup: str  # bash that puts globus-compute-endpoint on PATH (module + venv)
    scratch_root: str  # session-shell root on the shared filesystem; {user} templated
    endpoint_name: str | None = None  # None ⇒ derive hpc-bridge-<id> (never the bare collision name)

    @field_validator("endpoint_name")
    @classmethod
    def _safe_endpoint_name(cls, v: str | None) -> str | None:
        # The name is spliced into a remote shell path (`cat > "$HOME/.globus_compute/<name>/…"`) where
        # `$(…)` and backticks EXECUTE — and the endpoint's own validator lets them through (found in
        # review). Same allowlist the harness/session naming uses.
        if v is not None and not SAFE_ENDPOINT_NAME.match(v):
            raise ValueError("endpoint_name must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
        return v
    amqp_port: int = 443  # facilities firewall AMQPS 5671; 443 is the near-universal allowed port
    # Facility-MEP entries: what `{gce_version}` in env_setup resolves to — the version the facility's USER endpoint
    # process runs, which its template decides and which the metadata does not expose. "manager": the manager's
    # `endpoint_version` from metadata (Delta: the UEP runs the manager's code). "client": this client's SDK
    # version (Anvil: the template builds the UEP's environment at the client's SDK version — pinning the
    # manager's 4.12 there launched a 4.12 worker pool under a 4.16 UEP: `unrecognized arguments: --logconf`).
    # Or an explicit version string. Set from the live check that validates the entry.
    worker_version: str = "manager"
    # scheduler directives for the submit script's header (#SBATCH for Slurm, #PBS for PBS): on an SSH entry read as
    # directives (`ssh_scheduler_options`); a facility endpoint's are `defaults.extra`'s, for its own template
    scheduler_options: str | None = None
    # Facility-MEP entries only: renames for the user_endpoint_config keys hpc-bridge itself picks names for
    # (account/partition/walltime/max_workers_per_node/nodes_per_block/init_blocks/max_blocks/interface —
    # see MEPFacility.from_entry's `opts` dict). A facility's OWN template picks these names, case-sensitive,
    # and disagrees with ours and with each other: Delta/Anvil already match ours (empty map, no rename
    # needed). NeSI's reannz-slurm wants {"account": "ACCOUNT_ID", "walltime": "WALL_TIME"} — without the
    # first, EVERY submit fails outright (ACCOUNT_ID is schema-required); without the second, walltime is
    # silently dropped (additionalProperties: false) and every job silently runs at the facility's own
    # default. ALCF's Polaris/Crux reportedly want {"partition": "queue"}. A facility-specific key with NO
    # hpc-bridge equivalent (Anvil's qos, NeSI's MEM_PER_CPU/GPUS_PER_NODE) is not a rename — that's
    # `Defaults.extra`, passed through verbatim under the facility's own name already.
    # See docs/hpc-bridge-vault/Reference/MEP facilities survey.md and .../Planned/Endpoint reuse and MEP
    # integration.md (M3: "curate the allowed user_endpoint_config in the entry").
    key_map: dict[str, str] = Field(default_factory=dict)
    worker_env: WorkerEnv | None = None  # facility-MEP entries: see WorkerEnv

    @field_validator("worker_env", mode="wrap")
    @classmethod
    def _lenient_worker_env(cls, v: Any, handler: Any) -> WorkerEnv | None:
        # A malformed worker_env costs this facility its staleness check, never the whole entry.
        try:
            return handler(v)
        except (ValueError, TypeError):
            return None

    @field_validator("key_map")
    @classmethod
    def _known_rename_source(cls, v: dict[str, str]) -> dict[str, str]:
        bad = sorted(set(v) - MEP_RENAMEABLE_KEYS)
        if bad:
            raise ValueError(
                f"key_map can only rename hpc-bridge's own fixed UEC keys, got unknown source key(s) "
                f"{bad}: valid sources are {sorted(MEP_RENAMEABLE_KEYS)} (a facility-native key with no "
                "hpc-bridge equivalent belongs in defaults.extra, passed through verbatim, not key_map)"
            )
        return v


class Defaults(BaseModel):
    """Per-run tunables the agent/user MAY override at submit time via user_endpoint_config."""

    partition: str
    walltime: str = "00:30:00"
    max_workers_per_node: int = 2
    nodes_per_block: int = 1
    cpus_per_node: int | None = None  # PBSProProvider.cpus_per_node; Slurm ignores it
    # PBSProProvider.select_options: chunk resources parsl appends to its own `select=<nodes>:ncpus=<cpus_per_node>`
    # (`ngpus=4`, `system=polaris`), so the node count stays hpc-bridge's. Read leniently: one that would name the
    # count, the terms or ncpus is dropped (`select_options_problem`; the ingest refuses it). Slurm ignores it.
    select_options: str | None = None

    @field_validator("select_options")
    @classmethod
    def _chunk_only(cls, v: str | None) -> str | None:
        return None if v is None or select_options_problem(v) is not None else v
    # SlurmProvider.exclusive (Parsl defaults it to True: `#SBATCH --exclusive`, a WHOLE node). On a per-core
    # partition that is a node's worth of cores, which a per-job CPU cap can refuse forever — Expanse `shared`
    # (QOS max 127 CPUs/job, 128-core nodes): PENDING QOSMaxCpuPerJobLimit, 2026-10-06. None = Parsl's default.
    exclusive: bool | None = None
    init_blocks: int = 0  # blocks to pre-spawn; 0 = lazy (block on first task). A MEP entry sets 1 for a warm, low-latency block (its login-shape replacement)  # noqa: E501
    max_blocks: int = 1
    available_accelerators: int | list[str] | None = None
    # Facility-MEP entries only: extra `user_endpoint_config` keys the facility's own template consumes
    # (Anvil/Delta: qos, cores_per_node, exclusive, mem_per_node, scheduler_options, resource_specification…).
    # Passed through verbatim, then filtered against the facility's published schema at attach time.
    extra: dict[str, str | int | bool] | None = None


class CatalogSummary(BaseModel):
    """The agent-safe view of an entry — identity only, no executable config or raw UUIDs."""

    subject: str
    id: str
    facility: str
    description: str
    display_name: str
    provenance: Literal["curated", "community", "scraped", "plugin-validated", "session"]
    last_validated: datetime.date
    # How a user gets in — the one thing a stranger must know before choosing (stranger's walk, 2026-09-03).
    # Derived, never stored: mep = the facility runs the endpoint (zero SSH, but their identity mapping
    # must include you); ssh = you need an account + key-based SSH on the login host.
    access: Literal["mep", "ssh"]
    access_note: str
    scheduler: str


_SAFE_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")  # the derived endpoint name is hpc-bridge-<id>
ACCOUNT_HINT_MAX = 200  # an account hint is one short line of agent-facing text
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def facility_fingerprint(metadata: dict) -> dict[str, str | None]:
    """What a facility multi-user endpoint publishes about itself, reduced to what a registry entry is proven
    against: its endpoint and Python versions, and digests of its user template (+ schema) and its manager config.
    The manager's parsl is NOT published — a change there shows only in a live block (the re-prove), though a
    redeploy usually moves one of these too (2026-10-06: Delta's manager reports python 3.13.13, its UEP log the
    same)."""
    md = metadata or {}
    tmpl = str(md.get("user_config_template") or "")
    schema = json.dumps(md.get("user_config_schema") or {}, sort_keys=True, separators=(",", ":"))
    config = str(md.get("endpoint_config") or "")

    def digest(text: str) -> str | None:
        return hashlib.sha256(text.encode()).hexdigest() if text.strip() else None

    return {
        "endpoint_version": str(md.get("endpoint_version") or "").strip() or None,
        "python_version": str(md.get("python_version") or "").strip() or None,
        "template_sha256": digest(tmpl + "\n" + schema) if (tmpl.strip() or schema != "{}") else None,
        "config_sha256": digest(config),
    }


class Verification(BaseModel):
    """What the facility looked like when this entry was last proven live (a block ran and answered): the
    registry health check compares the facility's live metadata against it, and an attach warns when it moved.
    Facility-MEP entries; written by the re-prove tool, not by hand. Read LENIENTLY, like WorkerEnv."""

    verified_on: datetime.date | None = None  # (not `on`: YAML 1.1 reads a bare `on:` key as boolean true)
    endpoint_version: str | None = None
    python_version: str | None = None
    template_sha256: str | None = None
    config_sha256: str | None = None
    # what the worker reported on that run (free text, for the record), e.g. "py3.13.12 dill0.3.9 on a000"
    worker: str | None = None

    @field_validator("endpoint_version", "python_version")
    @classmethod
    def _version(cls, v: str | None) -> str | None:
        v = (v or "").strip()
        return v.lstrip("vV") if _VERSION.match(v) else None

    @field_validator("template_sha256", "config_sha256")
    @classmethod
    def _sha(cls, v: str | None) -> str | None:
        v = (v or "").strip().lower()
        return v if _SHA256.match(v) else None

    @field_validator("worker")
    @classmethod
    def _short(cls, v: str | None) -> str | None:
        return (v or "").strip()[:200] or None


class CatalogEntry(BaseModel):
    """One machine. A superset of MachineProfile; `profile_kwargs()` is the binding seam."""

    # identity
    id: str
    facility_key: str  # short slug for the subject, e.g. "purdue" (distinct from display `facility`)
    facility: str  # display, e.g. "Purdue / ACCESS"
    description: str
    display_name: str

    # identifiers (look up, never infer)
    compute_mep_uuid: str | None = None
    transfer_endpoint_uuid: str | None = None  # Globus Transfer (not wired yet) — compute-only entries omit it

    # access
    ssh_host: str | None = None  # None ⇒ a MEP entry (dispatch-only, no SSH); an SSH-bootstrap entry MUST set it
    auth_method: Literal["ssh-key", "mfa-otp", "sfapi"] = "ssh-key"  # only ssh-key wired in v1

    allocation: Allocation | None = None  # a facility may have no auto-listable allocation tool
    # Whether a scheduler block needs an --account to charge. False for an unmetered machine whose
    # scheduler doesn't enforce accounting (e.g. the lab cluster) — then the agent must NOT be sent
    # hunting for an allocation; it may confirm spend and proceed without one.
    account_required: bool = True
    # Where a user finds the account to charge (the facility's own words or tool), e.g. "your ACCESS project id,
    # lowercased (e.g. cis250223)". Agent-facing: the spend question and connect_facility carry it. Optional, and read
    # LENIENTLY (a malformed one is dropped, never the entry); a plugin before it ignores the key.
    account_hint: str | None = None
    compute: Compute
    defaults: Defaults

    # trust / provenance
    provenance: Literal["curated", "community", "scraped", "plugin-validated", "session"] = "curated"
    last_validated: datetime.date
    verification: Verification | None = None  # facility-MEP entries: see Verification

    @field_validator("verification", mode="wrap")
    @classmethod
    def _lenient_verification(cls, v: Any, handler: Any) -> Verification | None:
        # A malformed block costs this facility its drift check, never the whole entry (every plugin reads these).
        try:
            return handler(v)
        except (ValueError, TypeError):
            return None

    @field_validator("account_hint", mode="before")
    @classmethod
    def _lenient_hint(cls, v: Any) -> str | None:
        # One line of plain text, bounded: control and format characters (ESC/ANSI, NUL, bidi, zero-width) are dropped
        # and line breaks become spaces; anything not text costs the facility its hint, never the entry.
        if not isinstance(v, str):
            return None
        text = " ".join("".join(" " if unicodedata.category(ch) in ("Zl", "Zp") else ch
                                for ch in v if unicodedata.category(ch) not in ("Cc", "Cf") or ch in "\t\n\r").split())
        text = text.replace('"', "'").replace("`", "'").replace("<", "").replace(">", "")
        return text[:ACCOUNT_HINT_MAX] or None

    @field_validator("ssh_host")
    @classmethod
    def _safe_host(cls, v: str | None) -> str | None:
        return None if v is None else validate_host(v)  # a registry entry's host reaches ssh's argv (review 2026-09-04)

    @field_validator("id", "facility_key")
    @classmethod
    def _safe_slug(cls, v: str) -> str:
        if not _SAFE_SLUG.match(v or ""):
            raise ValueError(f"invalid id/facility_key {v!r}: letters, digits, '.', '_', '-' only "
                             "(it names the endpoint)")
        return v

    @field_validator("compute_mep_uuid", "transfer_endpoint_uuid")
    @classmethod
    def _valid_uuid(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return str(uuid.UUID(v))  # normalize to canonical lowercase hyphenated form

    @model_validator(mode="after")
    def _no_terms_in_extra(self) -> CatalogEntry:
        """Read leniently: an account or partition in `defaults.extra` (or the template's name for one), or a
        `scheduler_options` directive setting one (compute's or extra's), would reach the facility past
        ensure_endpoint_up's choice and the block's terms — drop it (the key, or the directive line), keep the entry.
        The ingest refuses them (`defaults_extra_raw_problems`).

        An SSH entry's `compute.scheduler_options` reach hpc-bridge's own submit script: they are read as directives
        first (`ssh_scheduler_options`: a bare option prefixed, any other non-directive line dropped), so the account
        and queue check reads the lines the script carries — a bare "-A x" is "#PBS -A x" there. A facility endpoint's
        options go to the facility's own template, which hpc-bridge does not rewrite."""
        if self.compute_mep_uuid is None and self.compute.scheduler_options is not None:
            self.compute.scheduler_options = ssh_scheduler_options(self.compute.scheduler_options,
                                                                   self.compute.scheduler)
        bad = _extra_terms_keys(self.defaults.extra, self.compute.key_map)
        if bad and self.defaults.extra is not None:
            self.defaults.extra = {k: v for k, v in self.defaults.extra.items() if k not in bad}
        if scheduler_terms_directive(self.compute.scheduler_options):
            self.compute.scheduler_options = _without_terms_directives(self.compute.scheduler_options)
        extra_opts = (self.defaults.extra or {}).get("scheduler_options")
        if isinstance(extra_opts, str) and scheduler_terms_directive(extra_opts) and self.defaults.extra is not None:
            kept = _without_terms_directives(extra_opts)
            self.defaults.extra = {k: v for k, v in self.defaults.extra.items() if k != "scheduler_options"}
            if kept:
                self.defaults.extra["scheduler_options"] = kept
        return self

    @model_validator(mode="after")
    def _reachable(self) -> CatalogEntry:
        """An entry must be reachable: a facility MEP to dispatch to, or an SSH host to bootstrap.

        `ssh_host` is optional now (MEP entries have none), so this guards the failure mode that
        optionality opens — a non-MEP entry that silently omits `ssh_host` and only fails, opaquely,
        at connect time. A future facility could carry both (MEP + SSH fallback); we require *at
        least* one, not exactly one.
        """
        if self.compute_mep_uuid is None and self.ssh_host is None:
            raise ValueError(
                "a catalog entry needs a reach: set compute_mep_uuid (facility MEP) "
                "or ssh_host (SSH bootstrap)"
            )
        if self.compute_mep_uuid is not None:
            # Any entry with a MEP is CONSUMED as a MEP (MEP wins over ssh_host in _facility_from_entry),
            # so this guard must not be gated on ssh_host being absent (found in review: a dual-reach
            # entry shipped literal templates to the worker).
            # A MEP entry is consumed with NO SSH, so there is no login name to resolve `{user}`
            # and no client-managed venv for `{venv}` — the facility maps our identity to a local
            # account we never learn. Those tokens would reach the worker LITERALLY. Such an entry
            # must use worker-side forms ($HOME, $USER) that the mapped user's shell expands.
            for field in ("env_setup", "scratch_root"):
                val = getattr(self.compute, field)
                if "{user}" in val or "{venv}" in val:
                    raise ValueError(
                        f"compute.{field} uses client-side templating ({{user}}/{{venv}}) but this is a "
                        "MEP-only entry (no ssh_host): nothing can resolve it — use worker-side forms "
                        "($HOME for scratch_root; $HOME/$USER in env_setup)"
                    )
            # scratch_root is spliced into the session-shell wrapper by `Session.quoted_state_dir`,
            # which expands ONLY a leading $HOME/ (or ${HOME}/, ~/); any other `$VAR` — `$USER`
            # included — is shell-quoted LITERAL, so `/scratch/$USER/…` would create a directory
            # literally named `$USER`. Catch it at validation, not at the first run_shell.
            sr = self.compute.scratch_root
            home_rel = any(sr.startswith(p) for p in ("$HOME/", "${HOME}/", "~/"))
            rest = sr.split("/", 1)[1] if home_rel and "/" in sr else sr
            if not ((home_rel and "$" not in rest) or (sr.startswith("/") and "$" not in sr)):
                raise ValueError(
                    f"compute.scratch_root {sr!r} on a MEP-only entry must be $HOME/… (or an absolute "
                    "path with no $VAR): only a leading $HOME expands on the worker — $USER or any "
                    "other variable would be quoted literal by the session shell"
                )
        return self

    @property
    def subject(self) -> str:
        return f"{self.facility_key}:{self.id}"

    def summary(self) -> CatalogSummary:
        access: Literal["mep", "ssh"]
        if self.compute_mep_uuid:
            access, note = "mep", (
                "zero SSH — the facility runs the endpoint; you need an account there with your Globus "
                "identity mapped to it (no account ⇒ a terminal NO ACCOUNT on first use, nothing billed). "
                "connect_facility only attaches: hpc-bridge has no login shape through this channel (the facility's "
                "login nodes are outside it), so no allocation list — the first billed block is where your access is "
                "actually tested"
            )
        else:
            access, note = "ssh", (
                f"SSH bootstrap on {self.ssh_host}: you need an account and key-based SSH there (login name and "
                "key from ~/.ssh/config, or HPC_BRIDGE_SSH_USER / HPC_BRIDGE_SSH_KEY); hpc-bridge stands up a "
                "personal endpoint once, then reuses it with no further SSH"
            )
        return CatalogSummary(
            subject=self.subject,
            id=self.id,
            facility=self.facility,
            description=self.description,
            display_name=self.display_name,
            provenance=self.provenance,
            last_validated=self.last_validated,
            access=access,
            access_note=note,
            scheduler=self.compute.scheduler,
        )

    def profile_kwargs(self) -> dict[str, Any]:
        """Constructor kwargs for MachineProfile (Plan 2 builds the profile from these).

        `account` is intentionally absent — it is per-user, from allocation selection.
        `worker_init` is absent — in the code it is derived as `= env_setup`.
        `ssh_host` is also absent — consumed by the transport layer (SshTarget) / facility
        selection, not MachineProfile. `init_blocks` is likewise absent — the SSH path derives
        eager start from the run mode (`@@EAGER@@`), and the MEP UEC builder reads
        `defaults.init_blocks` directly. `scheduler` IS passed through to the profile (the
        template dispatches on it), and so is `account_required` (the server's account floor reads it
        on an SSH facility as on a MEP). `auth_method` is reserved (only ssh-key is wired; nothing
        reads it yet).
        """
        return {
            "name": self.id,
            "endpoint_name": self.compute.endpoint_name,
            "display_name": self.display_name,
            "env_setup": self.compute.env_setup,
            "interface": self.compute.interface,
            "partition": self.defaults.partition,
            "walltime": self.defaults.walltime,
            "max_workers_per_node": self.defaults.max_workers_per_node,
            "nodes_per_block": self.defaults.nodes_per_block,
            "max_blocks": self.defaults.max_blocks,
            "available_accelerators": self.defaults.available_accelerators,
            "amqp_port": self.compute.amqp_port,
            "scheduler_options": self.compute.scheduler_options,
            "scratch_root": self.compute.scratch_root,
            "scheduler": self.compute.scheduler,
            "cpus_per_node": self.defaults.cpus_per_node,
            "select_options": self.defaults.select_options,
            "exclusive": self.defaults.exclusive,
            "account_required": self.account_required,
        }
