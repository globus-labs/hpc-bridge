"""Batch phase 2b: `export_handoff` v1, the endpoint handoff (vault `Planned/Batch and swarm execution.md` §8, §12 D6;
[[handoff]]).

A consumer — an orchestrator that runs its own agents — wants a Globus Compute endpoint and a user endpoint config
(UEC) it can run them on. hpc-bridge builds that config from the facility's template and the caller's keys, states
its terms, and — on the user's yes — validates EXACTLY that config with one billed canary (any other config is another
user endpoint), then exports a versioned, data-only record: `hpc-bridge.handoff`, version 1.

The rules this module keeps:
- **Terms first.** The canary bills, so the first call states the terms and sends nothing; `confirm_spend=True` (the
  user's yes) sends ONE canary, and later calls with the same config collect it — never a second.
- **The terms never understate.** `worst_case` is what the consumer's pool can bill from the yes until the record
  expires: max_blocks × each block's rate × (the horizon × the minimum-charge factor + one block started just before it
  expires), in the facility's unit where its billing weights are known, else node-hours.
- **`enforced_by: "uec-bounds-only"`.** Once the consumer holds the record, hpc-bridge does not police the endpoint.
- **Consumer-neutral.** The record names no consumer: a consumer maps its fields (the endpoint id, the wire-ready
  config, the nodes, the facility) to its own settings itself.
- **D6.** On a facility multi-user endpoint a pool with `min_blocks >= 1` is refused: parsl replaces a cancelled block
  to keep its minimum, and no client can stop a facility's user endpoint. `release` names the end that really works.
- **Data, never code.** Every value is validated and bounded; the record carries no shell; the one command it names
  (the scheduler's cancel for a block) is built from a validated job id and quoted.
"""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from . import resources, runner
from .config import _parse_hhmmss
from .context import AppCtx, _has_login_shape, _no_facility_bound
from .models import EndpointStatus, HandoffRef

SCHEMA = "hpc-bridge.handoff"
VERSION = 1
ENFORCED_BY = "uec-bounds-only"
DEFAULT_EXPIRES_H = 24.0
MAX_EXPIRES_H = 168.0
WAIT_S = 60.0               # one call waits this long for the canary's answer, then says `validating`
CANARY_WALLTIME_S = 120.0   # the canary's own ceiling on the worker
MAX_KEYS = 64
MAX_COUNT = 100_000
HEARTBEAT_S = 30            # globus-compute-endpoint's heartbeat period, where the template sets none
SHA256_OF = ("the published user_config_template, a newline, then its user_config_schema as canonical JSON "
             "(sorted keys, no spaces) — the registry's verification digest")

# The canary: the warmth check's own command (sentinel, versions, host, job, remaining walltime, workers) plus the
# device pin a worker was given and where it runs — its user endpoint's own directory, `uep.<manager>.<uep>/…`.
_PIN_VARS = ("CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES", "ZE_AFFINITY_MASK")
CANARY_CMD = (runner._CANARY_CMD + "; "
              + "; ".join(f'echo "HPCB_PIN {v}=${{{v}:-}}"' for v in _PIN_VARS)
              + '; echo "HPCB_WHERE $PWD"')

_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_WALL = re.compile(r"^(\d{1,3}-)?\d{1,3}:\d{2}:\d{2}$")
_TOKEN = re.compile(r"^[A-Za-z0-9_.,:=+/@%-]{1,256}$")
_ACCEL = re.compile(r"^[A-Za-z0-9_.:-]{1,32}$")
_NATIVE = re.compile(r"^[0-9]{1,12}(\[\])?(\.[A-Za-z0-9][A-Za-z0-9.-]{0,63})?$")
_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
_PIN = re.compile(r"^[A-Za-z0-9,._:-]{1,64}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_ID = re.compile(r"^hoff-[0-9a-f]{12}$")
_NAME_KEYS = frozenset({"account", "queue", "partition", "qos", "constraint", "interface"})
_INT_KEYS = frozenset({"nodes_per_block", "init_blocks", "min_blocks", "max_blocks", "max_workers_per_node",
                       "cores_per_node", "cpus_per_node", "mem_per_node", "gpus_per_node", "hpcb_block"})
RESERVED = frozenset({"endpoint_setup", "compute", "hpcb_block", "hpcb_pool"})   # never the caller's to set


class HandoffRefusal(ValueError):
    def __init__(self, reason: str, notice: str) -> None:
        super().__init__(notice)
        self.reason, self.notice = reason, notice


# ---- the record (v1) -------------------------------------------------------------------------------------------------

class _Rec(BaseModel):
    """Every part of the record: unknown fields are ignored (a later v1 may add some; a breaking change makes v2)."""

    model_config = ConfigDict(extra="ignore")


class HandoffTerms(_Rec):
    """What the user's yes approves: the canary now (`canary_worst_case`) and the consumer's pool until it expires
    (`worst_case`), both in `unit`; `enforced_by` says who keeps the bounds afterwards — only the config's own."""

    facility: str | None = None
    scheduler: str = "slurm"
    account: str | None = None
    queue: str | None = None
    walltime: str | None = None
    nodes_per_block: int = 1
    init_blocks: int = 0
    min_blocks: int = 0
    max_blocks: int = 1
    max_workers_per_node: int | None = None
    available_accelerators: int | list[str | int] | None = None
    # the block's size as the config sends it (hpc-bridge's names; None: the config does not carry the key) — the rate
    # is costed from it, and a config with no slice is the whole node parsl asks for
    cores_per_node: int | None = None
    cpus_per_node: int | None = None
    mem_per_node: int | None = None
    gpus_per_node: int | None = None
    exclusive: bool | None = None
    select_options: str | None = None
    max_idletime_s: int | None = None
    unit: str = "node-hours"
    rate: float = 0.0                  # what one block bills per hour, in `unit`
    min_charge_h: float | None = None  # the queue's minimum charge per block (a catalog fact)
    canary_worst_case: float = 0.0
    worst_case: float = 0.0
    basis: str = ""
    expires_hours: float = DEFAULT_EXPIRES_H
    expires_at: str | None = None
    approved_at: str | None = None
    enforced_by: Literal["uec-bounds-only"] = "uec-bounds-only"
    binds: str = ""
    site_backstop: str = "none"


class HandoffFacility(_Rec):
    id: str
    system: str
    scheduler: str
    access: Literal["mep", "ssh"]


class HandoffEndpoint(_Rec):
    manager_id: str
    kind: Literal["facility_mep", "hpc_bridge_ssh"]
    uep_id_seen: str | None = None     # the user endpoint the canary ran on (it differs from the manager by design)
    uep_id_note: str | None = None     # why it is null, when it is


class HandoffTemplate(_Rec):
    sha256: str | None = None
    sha256_of: str = SHA256_OF
    renders: dict[str, bool] = Field(default_factory=dict)


class HandoffWorker(_Rec):
    python: str | None = None
    dill: str | None = None
    parsl: str | None = None
    globus_compute_endpoint: str | None = None
    host: str | None = None


class HandoffClient(_Rec):
    python: str | None = None           # the client Python the consumer must run (the workers', major.minor: dill)
    dill: str | None = None
    globus_compute_sdk: str | None = None


class QueueCheck(_Rec):
    check: str
    ok: bool


class HandoffValidation(_Rec):
    ok: bool
    task_id: str | None = None
    job_id: str | None = None
    host: str | None = None
    device_pin: dict[str, str] = Field(default_factory=dict)
    identity: str | None = None        # the local account the facility mapped the Globus identity to
    remaining_s: float | None = None
    workers: int | None = None
    queue_checks: list[QueueCheck] = Field(default_factory=list)


class HandoffRelease(_Rec):
    """The end that really works, in order: stop submitting with this config; its blocks idle-release (min_blocks 0
    starts none for an idle pool); a block can be cancelled sooner, and nothing replaces it. Never the cancel alone."""

    min_blocks: int = 0
    ends_by_itself: bool = True
    block_idle_release_s: int | None = None
    uep_idle_exit_s: int | None = None
    cancel_block: str | None = None
    list_blocks: str | None = None
    steps: list[str] = Field(default_factory=list)
    hpc_bridge_scope: str = ""


class HandoffRecord(_Rec):
    """The v1 record. Consumer-neutral: nothing in it is named for one consumer; a consumer maps `endpoint.manager_id`,
    `user_endpoint_config`, `terms.nodes_per_block` and `facility` to its own settings (vault `Modules/handoff.md` has
    a worked example), and must refuse it past `expires_at` or when the facility's template digest has moved."""

    model_config = ConfigDict(extra="ignore", serialize_by_alias=True)

    schema_: str = Field(SCHEMA, alias="schema")
    version: int = VERSION
    handoff_id: str
    created_at: str
    expires_at: str
    hpc_bridge_version: str
    facility: HandoffFacility
    endpoint: HandoffEndpoint
    user_endpoint_config: dict[str, Any]
    template: HandoffTemplate
    worker: HandoffWorker
    client_requirements: HandoffClient
    terms: HandoffTerms
    validation: HandoffValidation
    release: HandoffRelease


class HandoffFormatError(ValueError):
    """Not a `hpc-bridge.handoff` version 1 record."""


def load_record(text: str) -> HandoffRecord:
    """A record from its JSON: unknown fields ignored at every level; another schema or version is refused."""
    try:
        raw = json.loads(text)
    except ValueError as exc:
        raise HandoffFormatError(f"not JSON: {exc}") from None
    if not isinstance(raw, dict) or raw.get("schema") != SCHEMA:
        raise HandoffFormatError(f"not a {SCHEMA} record")
    if raw.get("version") != VERSION:
        raise HandoffFormatError(f"{SCHEMA} version {raw.get('version')!r}: this reader knows version {VERSION}")
    try:
        return HandoffRecord.model_validate(raw)
    except ValueError as exc:
        raise HandoffFormatError(str(exc)[:400]) from None


class HandoffResult(BaseModel):
    """export_handoff's answer. phase: needs_confirmation (nothing sent: `terms`, and the config the canary would
    send) | validating (the one canary is out: call again with the same config to collect it) | exported (`record`,
    written to `path`) | needs_account | refused (`reason`: mep_min_blocks — D6 — invalid, schema, not_rendered,
    unbounded, expires, canary_failed, no_facility, template_unknown)."""

    phase: Literal["needs_confirmation", "validating", "exported", "needs_account", "refused"]
    reason: str | None = None
    terms: HandoffTerms | None = None
    user_endpoint_config: dict[str, Any] | None = None
    record: HandoffRecord | None = None
    path: str | None = None
    task_id: str | None = None
    notice: str | None = None


# ---- reading the facility's template ---------------------------------------------------------------------------------

_COMMENT = re.compile(r"\{#.*?#\}", re.S)
_UNSET = object()


def _strip(text: str) -> str:
    return _COMMENT.sub("", text or "")


def renders(text: str, key: str) -> bool:
    """Whether the template uses the config's `key` at all — inside a `{{ … }}` or a `{% … %}` (an `{% if mem_per_node
    is defined %}`, `{{ 'true' if exclusive else 'false' }}`): a key it never reads would be dropped on the way, and the
    record would not be the config that ran."""
    pat = r"\{[{%][^}]*?(?<![\w.])" + re.escape(key) + r"(?![\w])"
    return re.search(pat, _strip(text)) is not None


def _literal(s: str) -> Any:
    s = s.strip()
    s = re.sub(r"\s*\|\s*tojson\s*$", "", s)
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        return s[1:-1]
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("none", "null", "~"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except ValueError:
        return _UNSET


def readings(text: str, yaml_key: str) -> list[tuple[str, Any, Any]]:
    """Every value the template gives its YAML key `yaml_key`: ("lit", value, None) for a literal, ("var", <config
    key>, <its default or _UNSET>) for `{{ key | default(…) }}`, ("expr", None, None) for anything else."""
    out: list[tuple[str, Any, Any]] = []
    for m in re.finditer(rf"^\s*{re.escape(yaml_key)}:[ \t]*(.*?)[ \t]*$", _strip(text), re.M):
        v = m.group(1)
        if v.startswith("{{"):
            inner = v.strip("{}- \t")
            var = re.match(r"([A-Za-z_]\w*)\b", inner)
            if var is None or inner[var.end():].lstrip()[:1] not in ("", "|"):
                out.append(("expr", None, None))
                continue
            d = re.search(r"default\(\s*(.*?)\s*\)\s*(?:\||$)", inner)
            out.append(("var", var.group(1), _literal(d.group(1)) if d else _UNSET))
        else:
            if v[:1] not in ('"', "'"):
                v = v.split(" #", 1)[0]
            out.append(("lit", _literal(v), None))
    return out


_STMT = re.compile(r"^\s*\{%-?\s*(if|elif|else|endif)\b\s*(.*?)\s*-?%\}\s*$")
_COND_DEFAULT = re.compile(r"^([A-Za-z_]\w*)\s*\|\s*default\(\s*(.*?)\s*\)$")
_COND_DEFINED = re.compile(r"^([A-Za-z_]\w*)\s+is\s+defined(?:\s+and\s+([A-Za-z_]\w*)(\s+is\s+not\s+none)?)?$")


def _cond(expr: str, wire: dict[str, Any], closed: bool) -> bool | None:
    """An `{% if %}` condition's value for this config, or None where it cannot be told: only `x | default(lit)`,
    `x is defined`, `x is defined and x` and `x is defined and x is not none` are read. A string is never judged by its
    truth (the endpoint's manager JSON-quotes every string value, so '' arrives truthy). `closed`: the template is
    hpc-bridge's own, rendered with the config alone, so a name the config does not carry is undefined."""
    m = _COND_DEFAULT.match(expr)
    if m:
        lit = _literal(m.group(2))
        v = wire.get(m.group(1), lit if closed else _UNSET)
    elif (m := _COND_DEFINED.match(expr)) is not None and m.group(2) in (None, m.group(1)):
        name = m.group(1)
        if name not in wire:
            return False if closed else None
        if m.group(2) is None:
            return True
        if m.group(3):
            return wire[name] is not None
        v = wire[name]
    else:
        return None
    if v is _UNSET or isinstance(v, str):
        return None
    return bool(v)


def taken(text: str, wire: dict[str, Any], *, closed: bool = False) -> str:
    """The template's lines this config can reach: a branch of an `{% if %}` whose condition `_cond` can tell is
    dropped when it is not taken; every other branch is kept (a reading there still counts). The demo of 2026-11-03:
    hpc-bridge's own template sets `init_blocks: 1` in its LOGIN branch, and the terms took it over the 0 the compute
    config sends. Unbalanced statements: the whole text, as it is."""
    out: list[str] = []
    stack: list[dict[str, Any]] = []
    for line in _strip(text).splitlines():
        m = _STMT.match(line)
        if m is None:
            if all(f["cur"] is not False for f in stack):
                out.append(line)
            continue
        kw, expr = m.group(1), m.group(2)
        if kw == "if":
            c = _cond(expr, wire, closed)
            stack.append({"cur": c, "done": c is True, "maybe": c is None})
            continue
        if not stack:
            return text
        f = stack[-1]
        if kw == "elif":
            c = _cond(expr, wire, closed)
            cur = False if f["done"] else (None if f["maybe"] and c is not False else c)
            f.update(cur=cur, done=f["done"] or cur is True, maybe=f["maybe"] or cur is None)
        elif kw == "else":
            f["cur"] = False if f["done"] else (None if f["maybe"] else True)
        else:
            stack.pop()
    return text if stack else "\n".join(out)


def effective(text: str, yaml_key: str, wire: dict[str, Any], parsl_default: Any, *, pick: str = "max",
              closed: bool = False) -> Any:
    """What the template gives `yaml_key` for this config: the config's value where it renders it, else its default
    or its literal; parsl's default where the template never sets it; _UNSET where it cannot be read (an expression,
    a key required and not sent, readings that disagree under pick="one"). Only the branches the config can reach are
    read (`taken`; `closed`: hpc-bridge's own template); readings left in a branch that cannot be told apart take the
    most (pick="max")."""
    vals = []
    for kind, a, b in readings(taken(text, wire, closed=closed), yaml_key):
        if kind == "lit":
            vals.append(a)
        elif kind == "var":
            vals.append(wire.get(a, b))
        else:
            vals.append(_UNSET)
    if not vals:
        return parsl_default
    if any(v is _UNSET for v in vals):
        return _UNSET
    if len(set(map(repr, vals))) == 1:
        return vals[0]
    if pick == "max" and all(isinstance(v, int | float) and not isinstance(v, bool) for v in vals):
        return max(vals)
    return _UNSET


# ---- the config: validated, fitted, rendered -------------------------------------------------------------------------

def _scheduler(app: AppCtx) -> str:
    return str(getattr(app.facility, "scheduler", None)
               or getattr(getattr(app.facility, "profile", None), "scheduler", None) or "slurm")


def _check_value(key: str, v: Any, scheduler: str) -> str | None:
    """Why `key`'s value cannot be sent (it must be data a template renders, never code), or None."""
    if isinstance(v, bool):
        return None if key == "exclusive" or key not in _INT_KEYS else "must be a whole number, not true/false"
    if key in _INT_KEYS:
        if not isinstance(v, int):
            return "must be a whole number"
        top = 2 ** 48 if key == "hpcb_block" else MAX_COUNT
        return None if 0 <= v <= top else f"must be between 0 and {top}"
    if key == "max_idletime":
        return (None if isinstance(v, int | float) and math.isfinite(v) and 0 <= v <= 86400
                else "must be a number of seconds, 0 to 86400")
    if key == "walltime":
        return None if isinstance(v, str) and _WALL.match(v) else "must be HH:MM:SS (or D-HH:MM:SS)"
    if key in _NAME_KEYS:
        return None if isinstance(v, str) and _NAME.match(v) else "must match [A-Za-z0-9_.:-]{1,64}"
    if key == "available_accelerators":
        if isinstance(v, int):
            return None if 1 <= v <= 1024 else "a count must be 1 to 1024"
        if isinstance(v, list) and 0 < len(v) <= 1024 and all(
                isinstance(x, str | int) and not isinstance(x, bool) and _ACCEL.match(str(x)) for x in v):
            return None
        return "must be a count or a list of device ids ([A-Za-z0-9_.:-], at most 1024)"
    if key == "worker_init":
        if not isinstance(v, str) or len(v) > 8192:
            return "must be text of at most 8192 characters"
        bad = [c for c in v if not (c.isprintable() or c in "\n\t")]
        return None if not bad else "carries a control character (only printable text, newlines and tabs)"
    if key == "scheduler_options":
        if not isinstance(v, str) or len(v) > 4096:
            return "must be text of at most 4096 characters"
        word = "#PBS " if scheduler == "pbs" else "#SBATCH "
        for line in v.split("\n"):
            if line.strip() and (not line.startswith(word) or not line.isprintable()):
                return f"every line must be a scheduler directive starting {word.strip()!r}: {line[:60]!r}"
        return None
    if isinstance(v, str):
        return None if _TOKEN.match(v) else "must be a short token ([A-Za-z0-9_.,:=+/@%-], at most 256)"
    if isinstance(v, int):
        return None if abs(v) <= 10 ** 9 else "is out of range"
    if isinstance(v, float):
        return None if math.isfinite(v) and abs(v) <= 10 ** 9 else "is out of range"
    return "must be a string, a number or true/false"


_TYPES: dict[str, Any] = {"string": str, "integer": int, "number": (int, float), "boolean": bool, "array": list,
                          "object": dict}


def _schema_problem(key: str, v: Any, prop: dict[str, Any]) -> str | None:
    """The facility's JSON schema's word on one value (the subset its manager's validator would refuse): type,
    bounds, length, pattern, enum."""
    kinds = prop.get("type")
    if kinds is not None:
        names = kinds if isinstance(kinds, list) else [kinds]
        ok = False
        for n in names:
            t = _TYPES.get(n)
            if t is None or (isinstance(v, t) and not (n in ("integer", "number") and isinstance(v, bool))):
                ok = True
        if not ok:
            return f"the facility's schema wants {kinds}"
    if isinstance(v, int | float) and not isinstance(v, bool):
        if "minimum" in prop and v < prop["minimum"]:
            return f"the facility's schema allows minimum {prop['minimum']}"
        if "maximum" in prop and v > prop["maximum"]:
            return f"the facility's schema allows maximum {prop['maximum']}"
    if isinstance(v, str):
        if "minLength" in prop and len(v) < prop["minLength"]:
            return f"the facility's schema wants at least {prop['minLength']} characters"
        if "maxLength" in prop and len(v) > prop["maxLength"]:
            return f"the facility's schema allows at most {prop['maxLength']} characters"
        if isinstance(prop.get("pattern"), str):
            try:
                if re.search(prop["pattern"], v) is None:
                    return f"the facility's schema wants the pattern {prop['pattern']!r}"
            except re.error:
                pass
    if isinstance(prop.get("enum"), list) and v not in prop["enum"]:
        return f"the facility's schema allows only {prop['enum']}"
    return None


# Scheduler directives a caller's `scheduler_options` may carry (review round 1, item 1): sbatch and qsub take the LAST
# of a repeated option and parsl writes these after its own, so any directive that sets the block's size, time, queue,
# account, GPUs, an array, or its output path (by which a stop finds the consumer's blocks: `uep.<eid>`) would move the
# bounds the terms and the release are built on. Allow-listed: what changes none of them.
_SLURM_OK = frozenset({"--job-name", "-J", "--comment", "--mail-type", "--mail-user", "--begin", "--signal",
                       "--requeue", "--no-requeue", "--open-mode", "--export", "--get-user-env"})
_SLURM_FLAGS = frozenset({"--requeue", "--no-requeue", "--get-user-env"})   # take no value
_PBS_OK = frozenset({"-N", "-m", "-M", "-r"})
_PBS_RESOURCES = frozenset({"filesystems"})
_SELECT_OK = frozenset({"ngpus", "system", "mem"})
_HINT = {
    "--nodes": "nodes_per_block", "-N": "nodes_per_block", "--time": "walltime", "-t": "walltime",
    "--partition": "partition (or queue)", "-p": "partition (or queue)", "--account": "account", "-A": "account",
    "--cpus-per-task": "cores_per_node", "-c": "cores_per_node", "--mem": "mem_per_node",
    "-l select": "nodes_per_block and select_options", "-l walltime": "walltime", "-q": "queue",
    "-l ncpus": "cpus_per_node", "-l nodes": "nodes_per_block",
}


def _directive_problem(text: str, scheduler: str) -> str | None:
    """The first directive of a caller's `scheduler_options` that is not allow-listed, as written, or None."""
    pbs = scheduler == "pbs"
    for line in text.split("\n"):
        rest = line.strip()
        if not rest:
            continue
        rest = rest[len("#PBS"):] if pbs else rest[len("#SBATCH"):]
        try:
            tokens = shlex.split(rest)
        except ValueError:
            return line.strip()[:60]
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if pbs:
                if t == "-l" or (t.startswith("-l") and len(t) > 2):
                    value = t[2:] if len(t) > 2 else (tokens[i + 1] if i + 1 < len(tokens) else "")
                    i += 1 if len(t) > 2 else 2
                    for part in value.split(","):
                        name = part.split("=", 1)[0].strip()
                        if name not in _PBS_RESOURCES:
                            return f"-l {name}"
                    continue
                if t in _PBS_OK:
                    i += 2
                    continue
                return t
            if t.startswith("--"):
                name, eq, _v = t.partition("=")
                if name not in _SLURM_OK:
                    return name
                i += 1 if (eq or name in _SLURM_FLAGS) else 2
                continue
            if t.startswith("-") and len(t) >= 2 and t[:2] in _SLURM_OK:
                i += 1 if len(t) > 2 else 2
                continue
            return t[:2] if t.startswith("-") else t
    return None


def _select_problem(v: Any) -> str | None:
    """PBS `select_options` (appended to parsl's `select=<nodes>:ncpus=<n>`) may name only per-node resources that move
    no count: never a second chunk (`+…`), cores, MPI ranks or nodes."""
    if not isinstance(v, str):
        return None
    if "+" in v:
        return "adds a chunk (`+`) to the block's select"
    for part in v.split(":"):
        name = part.split("=", 1)[0].strip()
        if name not in _SELECT_OK:
            return f"sets `{name}`, which moves the block's size: only {sorted(_SELECT_OK)} may be given here"
    return None


def _directive_refusal(opt: str) -> HandoffRefusal:
    hint = _HINT.get(opt) or _HINT.get(opt.split("=")[0]) or "the config's own key, where the template takes one"
    return HandoffRefusal("invalid", (
        f"Refused, nothing was sent: `scheduler_options` sets `{opt}`, which the terms or the release depend on (the "
        "block's size, time, queue, account, GPUs, arrays or output path — the scheduler takes it over the config's "
        f"own). Set it through {hint} instead. Only harmless directives pass (a job name, mail, a comment; on PBS "
        "-N, -m, -M, -r and -l filesystems=…)."))


@dataclass
class Plan:
    """The config to send, and what the template makes of it."""

    wire: dict[str, Any]
    hb: dict[str, Any]                 # the same in hpc-bridge's names (key_map undone): sizes, partitions
    template: str
    access: str
    scheduler: str
    facts: dict[str, Any] = field(default_factory=dict)


_NEEDS_ACCOUNT = ("Nothing was sent: this facility charges every block to an account, and the config names none. Ask "
                  "the user which account (project) to charge, then send the config with it.")


def _bad(key: str, why: str) -> HandoffRefusal:
    return HandoffRefusal("invalid", f"Refused, nothing was sent: `{key}` {why}. The record must be data a template "
                                     "renders, never code — send the config again without it, or with a value that is.")


def _kmap(app: AppCtx, access: str) -> dict[str, str]:
    return dict(getattr(app.facility, "key_map", None) or {}) if access == "mep" else {}


def normalize(app: AppCtx, given: Any, access: str) -> dict[str, Any]:
    """The caller's own config, in the template's key names, every value checked as data: what a handoff is keyed on
    (with the endpoint) and what is laid over the facility's defaults. Raises HandoffRefusal."""
    sched = _scheduler(app)
    kmap = _kmap(app, access)
    if not isinstance(given, dict):
        raise HandoffRefusal("invalid", "Refused, nothing was sent: user_endpoint_config must be an object of keys.")
    if len(given) > MAX_KEYS:
        raise HandoffRefusal("invalid", f"Refused, nothing was sent: {len(given)} keys; at most {MAX_KEYS}.")
    caller: dict[str, Any] = {}
    for k, v in given.items():
        if not isinstance(k, str) or not _KEY.match(k):
            raise _bad(str(k)[:40], "is not a plain key name")
        if k in RESERVED:
            raise _bad(k, "is hpc-bridge's to write, not the caller's")
        wk = kmap.get(k, k)
        if wk in caller:
            other = next(src for src, dst in kmap.items() if dst == wk)
            raise HandoffRefusal("invalid", f"Refused, nothing was sent: both `{wk}` and `{other}` were given — they "
                                            f"are the same setting here (the facility's `{wk}`); send one.")
        if (why := _check_value(wk, v, sched)) is not None:   # the caller's values first, whatever else is wrong
            raise _bad(k, why)
        if wk == "scheduler_options" and (opt := _directive_problem(v, sched)) is not None:
            raise _directive_refusal(opt)
        if wk == "select_options" and (why := _select_problem(v)) is not None:
            raise HandoffRefusal("invalid", f"Refused, nothing was sent: `select_options` {why}. Set the block's size "
                                            "through nodes_per_block (and cpus_per_node where the template takes it).")
        caller[wk] = v
    return caller


def build(app: AppCtx, caller: dict[str, Any], md: dict[str, Any], access: str) -> Plan:
    """The wire-ready config for the caller's own (`normalize`): the facility's own compute defaults under it, every
    value checked, fitted to the facility's schema, every key rendered; the server's own `init_blocks: 0` /
    `min_blocks: 0` where the template renders them and the caller left them out; on an endpoint hpc-bridge stood up,
    a block request of its own. Raises HandoffRefusal."""
    from . import warmth

    sched = _scheduler(app)
    text = md.get("user_config_template")
    if not isinstance(text, str) or not text.strip():
        raise HandoffRefusal("template_unknown", "Refused, nothing was sent: the facility's published template could "
                             "not be read, and the terms need its values (blocks, walltime, idle release). Try again "
                             "later.")
    raw_schema = md.get("user_config_schema")
    schema: dict[str, Any] = raw_schema if isinstance(raw_schema, dict) else {}
    kmap = _kmap(app, access)
    base_hb = warmth._seed_uec(app, "compute", session_size=False)   # the session's block size is that block's alone
    base = {kmap.get(k, k): v for k, v in base_hb.items() if k not in RESERVED and v not in (None, "")}
    wire = {**base, **caller}
    raw_props = schema.get("properties")
    props: dict[str, Any] = raw_props if isinstance(raw_props, dict) else {}
    strict = schema.get("additionalProperties") is False
    for k in list(wire):
        if strict and k not in props:
            if k in caller:
                raise HandoffRefusal("schema", f"Refused, nothing was sent: the facility's schema takes no `{k}` (it "
                                               f"accepts only {sorted(props)}); send the config without it.")
            wire.pop(k)
            continue
        if not renders(text, k):
            if k in caller:
                fixed = effective(text, k, {}, _UNSET)
                said = f" (it fixes it at {fixed!r})" if fixed is not _UNSET else ""
                raise HandoffRefusal("not_rendered", f"Refused, nothing was sent: the facility's template does not use "
                                                     f"`{k}`{said}, so it would be dropped on the way and the record "
                                                     "would not be the config that ran. Send the config without it.")
            wire.pop(k)
    for k, v in wire.items():   # the facility's own defaults that are sent: data too
        if k not in caller and (why := _check_value(k, v, sched)) is not None:
            raise _bad(k, why + " (the facility's default — send your own value for it)")
    for k in ("init_blocks", "min_blocks"):
        if k not in caller and renders(text, k):
            wire[k] = 0
    if access == "ssh" and renders(text, "hpcb_block"):
        wire["hpcb_block"] = secrets.randbelow(2 ** 48 - 1) + 1   # its own user endpoint, never the session's
    for k, v in wire.items():
        if isinstance(props.get(k), dict) and (why := _schema_problem(k, v, props[k])) is not None:
            raise HandoffRefusal("schema", f"Refused, nothing was sent: `{k}`={v!r}: {why}.")
    missing = [k for k in (schema.get("required") or []) if isinstance(k, str) and k not in wire]
    if missing == [kmap.get("account", "account")]:
        raise HandoffRefusal("needs_account", _NEEDS_ACCOUNT)
    if missing:
        raise HandoffRefusal("schema", f"Refused, nothing was sent: the facility's schema requires {missing}.")
    back = {dst: src for src, dst in kmap.items()}
    hb = {back.get(k, k): v for k, v in wire.items()}
    plan = Plan(wire=wire, hb=hb, template=text, access=access, scheduler=sched)
    _facts(plan)
    if access == "mep" and isinstance(plan.facts["min_blocks"], int | float) and plan.facts["min_blocks"] >= 1:
        raise HandoffRefusal("mep_min_blocks", (
            f"Refused, nothing was sent or asked: min_blocks {plan.facts['min_blocks']} on a facility's multi-user "
            "endpoint. Parsl keeps that many blocks running and replaces a cancelled one, and no client can stop a "
            "facility's user endpoint, so such a pool cannot be bounded (design D6, until spike S3 proves the user "
            "endpoint exits when idle). Send min_blocks: 0 — blocks then start on demand and idle-release."))
    if plan.facts["min_blocks"] is _UNSET:
        raise HandoffRefusal("unbounded", "Refused, nothing was sent: the template's min_blocks cannot be read, so the "
                                          "pool cannot be bounded. Send min_blocks: 0 if the template takes it.")
    ib, mb = plan.facts["init_blocks"], plan.facts["max_blocks"]
    if isinstance(ib, int) and isinstance(mb, int) and ib > mb:
        raise HandoffRefusal("invalid", (
            f"Refused, nothing was sent: init_blocks {ib} is more than max_blocks {mb}. Parsl starts init_blocks at "
            "once, with no max_blocks check, each time the user endpoint starts — so max_blocks would not bound the "
            "pool. Send init_blocks no higher than max_blocks (0 starts blocks on demand)."))
    return plan


def _facts(plan: Plan) -> None:
    t, w = plan.template, plan.wire
    wall_default = "00:20:00" if plan.scheduler == "pbs" else "00:10:00"   # parsl's PBSPro / Slurm providers'
    f = plan.facts
    own = plan.access == "ssh"   # hpc-bridge's own template: a name the config does not carry is undefined in it

    def eff(key: str, default: Any, pick: str = "max") -> Any:
        return effective(t, key, w, default, pick=pick, closed=own)

    f["min_blocks"] = eff("min_blocks", 0)
    f["max_blocks"] = eff("max_blocks", 1)
    f["init_blocks"] = eff("init_blocks", 1)
    f["nodes_per_block"] = eff("nodes_per_block", 1)
    f["walltime"] = eff("walltime", wall_default, pick="one")
    idle = eff("max_idletime", None, pick="one")
    f["max_idletime_s"] = int(idle) if isinstance(idle, int | float) and not isinstance(idle, bool) and idle > 0 \
        else None
    f["max_workers_per_node"] = eff("max_workers_per_node", None, pick="one")
    f["available_accelerators"] = w.get("available_accelerators")
    hb = eff("idle_heartbeats_soft", None, pick="one")
    period = eff("heartbeat_period", HEARTBEAT_S, pick="one")
    f["uep_idle_exit_s"] = (int(hb * period) if isinstance(hb, int) and hb > 0 and isinstance(period, int | float)
                            else None)
    for k in ("max_workers_per_node", "available_accelerators"):
        if f[k] is _UNSET:
            f[k] = None


# ---- the terms -------------------------------------------------------------------------------------------------------

def _up(x: float) -> float:
    return math.ceil(x * 10_000 - 1e-6) / 10_000


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hours(v: Any) -> float:
    if v is None:
        return DEFAULT_EXPIRES_H
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v) or not 0 < v <= MAX_EXPIRES_H:
        raise HandoffRefusal("expires", f"Refused, nothing was sent: expires_hours={v!r}: the record's life in hours, "
                                        f"more than 0 and at most {MAX_EXPIRES_H:g} (default {DEFAULT_EXPIRES_H:g}).")
    return float(v)


BINDS = ("The user endpoint config bounds the consumer's pool — at most max_blocks blocks at once, each its nodes for "
         "its walltime — and nothing else does: once the consumer holds this record hpc-bridge does not police the "
         "endpoint, count its spend or stop it at expires_at (the consumer's preflight must refuse an expired "
         "record). The config's worker_init is the consumer's own code and runs as shell in the canary's block and in "
         "every block of the consumer's pool, by design. Any task the consumer runs can submit scheduler jobs of its "
         "own; a hard cap needs the site (an sbank suballocation, a Slurm GrpTRESMins).")


def terms_of(app: AppCtx, plan: Plan, hours: float) -> HandoffTerms:
    """The terms of `plan` over `hours` — never understated. Raises HandoffRefusal (unbounded)."""
    from . import notices

    f, hb = plan.facts, plan.hb
    for k in ("max_blocks", "nodes_per_block", "init_blocks"):
        if not (isinstance(f[k], int) and not isinstance(f[k], bool)):
            raise HandoffRefusal("unbounded", f"Refused, nothing was sent: the template's {k} cannot be read, so the "
                                              f"pool cannot be bounded. Send {k}= if the template takes it.")
    wall = f["walltime"]
    wall_s = float(_parse_hhmmss(wall)) if isinstance(wall, str) else 0.0
    if wall_s <= 0:
        raise HandoffRefusal("unbounded", "Refused, nothing was sent: the block's walltime cannot be read from the "
                                          "template; send walltime= (HH:MM:SS).")
    nodes, max_blocks = int(f["nodes_per_block"]), int(f["max_blocks"])
    sized = {**hb, "nodes_per_block": nodes, "compute": True}
    size_of = getattr(app.facility, "block_size", None)
    if plan.access == "ssh" and callable(size_of):
        # the config goes out as it is — no wire mapping gives it D3's proposed slice: what it does not ask for is
        # parsl's default block (on a per-core partition the terms had costed a 2-core slice parsl never asked for)
        size = size_of(dict(sized), propose=False)
        shape = resources.shape_of(getattr(app.facility, "partitions", None), sized.get("partition"))
    else:
        size, shape = notices._block_size(app, sized)
    if size.nodes != nodes:
        size = dataclasses.replace(size, nodes=nodes)
    got = resources.charge(size, shape, 3600.0)
    if got is not None:
        # the exact hourly rate: `charge` rounds its figure to 2 decimals, so it is read over a million hours and
        # divided back — only the final figures are rounded, and up (review round 1, item 6)
        exact = resources.charge(size, shape, 3600.0 * 1_000_000)
        rate = (exact[0] / 1_000_000) if exact is not None else got[0]
        per = got[1]
        unit = getattr(app, "charge_unit", None) or "billing units"
    else:
        rate, per, unit = float(nodes), f"{nodes} node{'s' if nodes > 1 else ''}", "node-hours"
    m = shape.min_charge_h if shape is not None else None
    idle = f["max_idletime_s"]
    wall_h = wall_s / 3600
    if m and not idle:
        raise HandoffRefusal("unbounded", (
            f"Refused, nothing was sent: this queue charges every block at least {m:g} h, and the template's idle "
            "release cannot be read, so how many blocks a pool can start (each charged the minimum) has no bound. "
            "Send max_idletime= if the template takes it."))
    # a block lives at least until its idle release — or its walltime, when work is queued and parsl starts the next
    # one at once (TIMEOUT is no block failure): each slot starts at most hours ÷ that many blocks, each charged at
    # least the minimum (review round 1, item 5)
    life_h = min(idle / 3600, wall_h) if idle else wall_h
    factor = max(1.0, m / life_h) if m else 1.0
    last = max(wall_h, m or 0.0)
    worst = max_blocks * rate * (hours * factor + last)
    canary = max(1, int(f["init_blocks"])) * rate * last
    basis = (f"{max_blocks} block{'s' if max_blocks > 1 else ''} at once × {rate:g} {unit}/h ({per}) × "
             f"({hours:g} h until the record expires"
             + (f" × {factor:g}: every block is charged at least {m:g} h and may last only {life_h * 60:g} min "
                "(its idle release, or its walltime with work queued)" if factor > 1 else "")
             + f" + {last:g} h for a block started just before it expires)")
    queue = hb.get("partition")

    def sent_int(key: str) -> int | None:
        v = hb.get(key)
        return v if isinstance(v, int) and not isinstance(v, bool) else None

    excl, sel = hb.get(resources.WHOLE), hb.get("select_options")
    return HandoffTerms(
        facility=app.machine or getattr(app.facility, "name", None), scheduler=plan.scheduler,
        account=hb.get("account"), queue=str(queue) if queue is not None else None, walltime=wall,
        nodes_per_block=nodes, init_blocks=int(f["init_blocks"]), min_blocks=int(f["min_blocks"]),
        max_blocks=max_blocks,
        max_workers_per_node=f["max_workers_per_node"] if isinstance(f["max_workers_per_node"], int) else None,
        available_accelerators=f["available_accelerators"], cores_per_node=sent_int(resources.CORES),
        cpus_per_node=sent_int("cpus_per_node"), mem_per_node=sent_int(resources.MEM),
        gpus_per_node=sent_int(resources.GPUS), exclusive=excl if isinstance(excl, bool) else None,
        select_options=sel if isinstance(sel, str) else None, max_idletime_s=idle, unit=unit, rate=rate,
        min_charge_h=m, canary_worst_case=_up(canary), worst_case=_up(worst), basis=basis, expires_hours=hours,
        binds=BINDS)


# ---- the canary, collected -------------------------------------------------------------------------------------------

@dataclass
class Pending:
    """A handoff whose one canary is out. Kept in memory (`app.handoffs`) and on disk (`pending-<key>.json`), so a later
    call — of this server or a restarted one — collects it by its task id and never sends another (review round 1,
    item 3). `future` / `runner`: this process's, while it has them; None after a restart, or once the client lost
    track of the task (its future failed on the client's side alone: the service may still hold it)."""

    key: str
    runner: Any
    future: Any
    plan: Plan
    terms: HandoffTerms
    md: dict[str, Any]
    eid: str
    approved: float
    hours: float
    sent_at: float = 0.0
    task_id: str | None = None


NO_ID_S = 300.0   # a canary with no task id that long after its send was never registered (warmth's _NO_ID_BOUND_S)


def _pending_path(key: str) -> Path:
    return handoffs_dir() / f"pending-{key[:32]}.json"


def _persist(p: Pending) -> None:
    """The pending canary on disk: what a restarted server needs to collect it (its task id) and to build the record
    from the config it validated (its wire config, terms and the template's access/scheduler)."""
    d = handoffs_dir()
    try:
        d.mkdir(parents=True, exist_ok=True, mode=0o700)
        raw = {"key": p.key, "eid": p.eid, "task_id": p.task_id, "sent_at": p.sent_at, "approved": p.approved,
               "hours": p.hours, "wire": p.plan.wire, "access": p.plan.access, "scheduler": p.plan.scheduler,
               "terms": p.terms.model_dump(mode="json")}
        tmp = d / f".pending-{p.key[:32]}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(json.dumps(raw))
        tmp.replace(_pending_path(p.key))
    except OSError as exc:
        print(f"hpc-bridge: could not record the handoff canary on disk ({exc})", file=sys.stderr)


def _unpersist(key: str) -> None:
    try:
        _pending_path(key).unlink(missing_ok=True)
    except OSError:
        pass


def _from_disk(raw: dict[str, Any], md: dict[str, Any]) -> Pending | None:
    """A pending canary read back from disk (another life of this server): no future of its own — the service is asked
    by its task id."""
    try:
        text = md.get("user_config_template") or ""
        wire = dict(raw["wire"])
        plan = Plan(wire=wire, hb=wire, template=str(text), access=str(raw["access"]), scheduler=str(raw["scheduler"]))
        _facts(plan)
        tid = raw.get("task_id")
        return Pending(key=str(raw["key"]), runner=None, future=None, plan=plan,
                       terms=HandoffTerms.model_validate(raw["terms"]), md=md, eid=str(raw["eid"]),
                       approved=float(raw["approved"]), hours=float(raw["hours"]),
                       sent_at=float(raw.get("sent_at") or 0.0), task_id=str(tid) if tid else None)
    except (KeyError, TypeError, ValueError):
        return None


def _disk_pendings(eid: str | None = None) -> list[dict[str, Any]]:
    d = handoffs_dir()
    out = []
    for path in sorted(d.glob("pending-*.json")) if d.is_dir() else []:
        try:
            raw = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(raw, dict) and (eid is None or raw.get("eid") == eid):
            out.append(raw)
    return out


def _lookup(app: AppCtx) -> Any:
    """An SDK Client (or the facility's own client) that looks a task up by id."""
    factory = getattr(app.facility, "_client_factory", None)
    return factory() if callable(factory) else None


async def _metadata(app: AppCtx, eid: str) -> dict[str, Any]:
    from .facility.base import endpoint_metadata

    if eid in app.handoff_meta:
        return app.handoff_meta[eid]
    factory = getattr(app.facility, "_client_factory", None)
    if not callable(factory):
        return {}
    try:
        md = await endpoint_metadata(factory, eid)
    except Exception:  # noqa: BLE001 - the template is needed: unreadable is said so by the caller
        return {}
    if md.get("user_config_template"):
        app.handoff_meta[eid] = md
    return md


def _key(app: AppCtx, eid: str, caller: dict[str, Any]) -> str:
    """What one handoff is: the caller's OWN config (never the facility's or the session's defaults under it, which may
    move — review round 1, item 3), on this facility and endpoint."""
    who = {"facility": app.machine or getattr(app.facility, "name", None), "eid": eid, "uec": caller}
    return hashlib.sha256(json.dumps(who, sort_keys=True, default=str).encode()).hexdigest()


def _task_id(fut: Any) -> str | None:
    tid = getattr(fut, "task_id", None)
    return str(tid) if tid else None


_SIZE_WORDS = ("cores_per_node", "cpus_per_node", "mem_per_node", "gpus_per_node", "exclusive", "select_options")


def _block_words(t: HandoffTerms) -> str:
    """Each block as the config asks for it — every size key it carries, by the value it sends."""
    said = [f"{t.nodes_per_block} node{'s' if t.nodes_per_block != 1 else ''} for {t.walltime}"]
    for k in _SIZE_WORDS:
        v = getattr(t, k)
        if v is not None:
            said.append(f"{k} {str(v).lower() if isinstance(v, bool) else v}")
    return (f"Each block: {', '.join(said)}; init_blocks {t.init_blocks}, min_blocks {t.min_blocks}, max_blocks "
            f"{t.max_blocks}.")


def _ask_notice(terms: HandoffTerms, plan: Plan) -> str:
    return (f"Nothing was sent. {_block_words(terms)} Exporting this handoff runs ONE billed canary with exactly "
            f"this config (the `user_endpoint_config` above) — up to {terms.canary_worst_case:g} {terms.unit} — and "
            f"then hands the endpoint to its consumer: until the record expires ({terms.expires_hours:g} h after the "
            f"yes) its pool "
            f"can bill up to {terms.worst_case:g} {terms.unit} ({terms.basis}). {terms.binds} Ask the user; on their "
            "yes call export_handoff again with the same config and confirm_spend=True.")


async def export(app: AppCtx, user_endpoint_config: Any = None, expires_hours: Any = DEFAULT_EXPIRES_H,
                 confirm_spend: bool = False) -> HandoffResult:
    try:
        return await _export(app, user_endpoint_config, expires_hours, confirm_spend)
    except HandoffRefusal as r:
        phase: Literal["refused", "needs_account"] = "needs_account" if r.reason == "needs_account" else "refused"
        return HandoffResult(phase=phase, reason=r.reason, notice=r.notice)


async def _export(app: AppCtx, given: Any, expires_hours: Any, confirm_spend: bool) -> HandoffResult:
    hours = _hours(expires_hours)
    if _no_facility_bound(app) or app.state.endpoint_id is None:
        raise HandoffRefusal("no_facility", "Refused: no facility is connected — connect_facility first.")
    ssh = _has_login_shape(app)
    if ssh and not isinstance(_config_defaults(app), dict):
        raise HandoffRefusal("no_facility", "Refused: this endpoint has no scheduler template to hand off.")
    access = "ssh" if ssh else "mep"
    eid = str(app.state.endpoint_id)
    caller = normalize(app, given, access)
    key = _key(app, eid, caller)
    md = await _metadata(app, eid)
    pend = app.handoffs.get(key)
    if pend is None:   # another life of this server sent it: collect it by its task id, never send another
        raw = next((r for r in _disk_pendings(eid) if r.get("key") == key), None)
        if raw is not None and (pend := _from_disk(raw, md)) is not None:
            if time.time() > pend.approved + pend.hours * 3600:
                _unpersist(key)
                pend = None
            else:
                app.handoffs[key] = pend
    if pend is not None:
        return await _collect(app, pend)
    plan = build(app, caller, md, access)
    if getattr(app.facility, "account_required", False) and not plan.hb.get("account"):
        raise HandoffRefusal("needs_account", _NEEDS_ACCOUNT)
    terms = terms_of(app, plan, hours)
    if not confirm_spend:
        return HandoffResult(phase="needs_confirmation", terms=terms, user_endpoint_config=dict(plan.wire),
                             notice=_ask_notice(terms, plan))
    approved = time.time()
    terms = terms.model_copy(update={"approved_at": _iso(approved), "expires_at": _iso(approved + hours * 3600)})
    run = app.runner_factory(eid, user_endpoint_config=dict(plan.wire), walltime=CANARY_WALLTIME_S,
                             timeout=WAIT_S)
    try:
        fut = run.submit(CANARY_CMD)
    except Exception as exc:  # noqa: BLE001 - the dispatch path refused it: nothing is queued
        return HandoffResult(phase="refused", reason="canary_failed",
                             notice=f"The canary could not be sent: {runner.dispatch_error_text(exc)}")
    pend = Pending(key=key, runner=run, future=fut, plan=plan, terms=terms, md=md, eid=eid, approved=approved,
                   hours=hours, sent_at=time.time(), task_id=_task_id(fut))
    app.handoffs[key] = pend
    _persist(pend)
    await _note_check(app, pend)
    return await _collect(app, pend)


async def _note_check(app: AppCtx, pend: Pending) -> None:
    """The canary is a task of ours that can start a block while it is queued: it joins the endpoint's sent checks,
    in memory and on disk, where every stop's honesty looks (`down` only once each has resolved) — whatever becomes of
    the handoff, a cancel included (review round 1, item 4)."""
    from . import warmth
    from .context import SentCheck

    blk = pend.plan.wire.get("hpcb_block")
    client = getattr(pend.runner, "task_client", None)
    check = SentCheck(future=pend.future, client=client() if callable(client) else None, task_id=pend.task_id,
                      sent_at=pend.sent_at, what="check",
                      block=blk if isinstance(blk, int) and not isinstance(blk, bool) else None)
    async with app.lock:
        warmth._keep_checks(app, pend.eid, [check])
        warmth._watch_ids(app, pend.eid)


def _config_defaults(app: AppCtx) -> Any:
    ct = getattr(app.facility, "config_template", None)
    out = ct(app.profile) if callable(ct) else None
    return out[1] if isinstance(out, tuple) else None


def _validating(pend: Pending, why: str = "") -> HandoffResult:
    return HandoffResult(phase="validating", task_id=pend.task_id, terms=pend.terms,
                         user_endpoint_config=dict(pend.plan.wire), notice=(
                             "The canary is out (one billed task with exactly this config): its block is still queued "
                             f"or starting{why}. Call export_handoff again with the same config to collect it — "
                             "nothing more is sent."))


async def _collect(app: AppCtx, pend: Pending) -> HandoffResult:
    """The canary's answer: from this process's own future while it has one; else from the service, by task id."""
    from globus_compute_sdk.errors import TaskExecutionFailed

    fut = pend.future
    if fut is not None:
        if (tid := _task_id(fut)) is not None and tid != pend.task_id:
            pend.task_id = tid
            _persist(pend)
            from . import warmth

            async with app.lock:
                warmth._persist_checks(app, pend.eid)
        try:
            res = await asyncio.to_thread(fut.result, WAIT_S)
        except TimeoutError:
            exc = fut.exception() if fut.done() and not fut.cancelled() else None
            if not isinstance(exc, TimeoutError):
                return _validating(pend)
            return _failed(app, pend, exc)
        except TaskExecutionFailed as exc:   # the endpoint's own verdict: nothing of it is left queued
            return _failed(app, pend, exc)
        except Exception as exc:  # noqa: BLE001 - the client's side alone: the service may still hold it
            pend.task_id = pend.task_id or _task_id(fut)
            if runner.never_sent(fut) or (pend.task_id is None and time.time() - pend.sent_at > NO_ID_S):
                return _failed(app, pend, exc)
            pend.future = None
            _close(pend.runner)
            pend.runner = None
            _persist(pend)
            return _validating(pend, f" — this client lost track of its answer ({type(exc).__name__}), so it is read "
                                     "from the service by its task id")
        return _exported(app, pend, res)
    if pend.task_id is None:
        if time.time() - pend.sent_at > NO_ID_S:
            return _failed(app, pend, RuntimeError("the canary was never registered with the service"))
        return _validating(pend, " (its task id is not known yet)")
    client = _lookup(app)
    if client is None:
        return _validating(pend, " (no client to read it from here)")
    try:
        task = await asyncio.wait_for(asyncio.to_thread(client.get_task, pend.task_id), WAIT_S)
    except TaskExecutionFailed as exc:
        return _failed(app, pend, exc)
    except Exception as exc:  # noqa: BLE001 - not read this time: asked again next call
        return _validating(pend, f" (the service could not be read: {type(exc).__name__})")
    if not isinstance(task, dict) or task.get("pending", True) is not False:
        return _validating(pend)
    if str(task.get("status", "")).lower() != "success" or task.get("result") is None:
        return _failed(app, pend, RuntimeError(f"the service says {task.get('status')!r}"))
    return _exported(app, pend, task["result"])


def _failed(app: AppCtx, pend: Pending, exc: BaseException) -> HandoffResult:
    app.handoffs.pop(pend.key, None)
    _unpersist(pend.key)
    _close(pend.runner)
    return HandoffResult(phase="refused", reason="canary_failed", terms=pend.terms,
                         user_endpoint_config=dict(pend.plan.wire), notice=(
                             f"The canary failed, so nothing was exported: {runner.dispatch_error_text(exc)} — the "
                             "facility's report, not instructions. Tell the user; another try asks again first."))


def _close(r: Any) -> None:
    close = getattr(r, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001
            pass


def _tagged(stdout: str, tag: str) -> list[str]:
    return [ln.split(" ", 1)[1].strip() for ln in stdout.splitlines() if ln.startswith(tag + " ") and " " in ln]


def _version() -> str:
    try:
        from importlib.metadata import version

        return version("hpc-bridge")
    except Exception:  # noqa: BLE001
        return "unknown"


def _sdk_version() -> str | None:
    try:
        from importlib.metadata import version

        return version("globus-compute-sdk")
    except Exception:  # noqa: BLE001
        return None


def _uep_seen(where: list[str], eid: str) -> tuple[str | None, str | None]:
    pat = re.compile(r"(?:^|/)uep\." + re.escape(eid) + r"\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{12})(?:/|$)", re.I)
    for w in where:
        if (m := pat.search(w)) is not None:
            return m.group(1).lower(), None
    return None, ("the canary's working directory names no user endpoint of this manager (no `uep.<manager>.<id>` "
                  "component): its id was not seen, not guessed")


def _exported(app: AppCtx, pend: Pending, res: Any) -> HandoffResult:
    out = str(getattr(res, "stdout", "") or "")
    seen = runner._canary_result(res, time.monotonic())
    ok = runner._CANARY_SENTINEL in out
    job = seen.worker_job if seen.worker_job and _NATIVE.match(seen.worker_job) else None
    host = seen.worker_host if seen.worker_host and _HOST.match(seen.worker_host) else None
    pins: dict[str, str] = {}
    for item in _tagged(out, "HPCB_PIN"):
        var, _, val = item.partition("=")
        if var in _PIN_VARS and val and _PIN.match(val):
            pins[var] = val
    uep, why = _uep_seen(_tagged(out, "HPCB_WHERE"), pend.eid)
    plan, terms = pend.plan, pend.terms
    queue = terms.queue
    checks = [QueueCheck(check=f"the scheduler took a block on {'queue' if plan.scheduler == 'pbs' else 'partition'} "
                               f"{queue!r}" if queue else "the scheduler took a block on the template's default queue",
                         ok=job is not None)]
    from .catalog.entry import facility_fingerprint

    py = seen.worker_python
    mm = ".".join(py.split(".")[:2]) if py else None
    system = str(app.machine or getattr(app.facility, "name", None) or "unknown")
    created = time.time()
    rid = "hoff-" + secrets.token_hex(6)
    rec = HandoffRecord(
        schema=SCHEMA, version=VERSION, handoff_id=rid, created_at=_iso(created),
        expires_at=terms.expires_at or _iso(created),
        hpc_bridge_version=_version(),
        facility=HandoffFacility(id=system, system=system, scheduler=plan.scheduler,
                                 access="ssh" if plan.access == "ssh" else "mep"),
        endpoint=HandoffEndpoint(manager_id=pend.eid,
                                 kind="hpc_bridge_ssh" if plan.access == "ssh" else "facility_mep",
                                 uep_id_seen=uep, uep_id_note=why),
        user_endpoint_config=dict(plan.wire),
        template=HandoffTemplate(sha256=facility_fingerprint(pend.md).get("template_sha256"),
                                 renders={k: renders(plan.template, k) for k in plan.wire}),
        worker=HandoffWorker(python=py, dill=seen.worker_dill, parsl=seen.worker_parsl,
                             globus_compute_endpoint=seen.worker_gce, host=host),
        client_requirements=HandoffClient(python=mm, dill=seen.worker_dill, globus_compute_sdk=_sdk_version()),
        terms=terms,
        validation=HandoffValidation(ok=ok, task_id=pend.task_id or _task_id(pend.future), job_id=job, host=host,
                                     device_pin=pins,
                                     identity=seen.worker_user, remaining_s=seen.worker_remaining_s,
                                     workers=seen.worker_count, queue_checks=checks),
        release=release_of(plan, job, seen.worker_user),
    )
    path = _write(rec)
    app.handoffs.pop(pend.key, None)
    _unpersist(pend.key)
    _close(pend.runner)
    if pend.task_id is not None:
        app.resolved_checks[pend.task_id] = "ok"   # it ran: no stop waits on it any more
    return HandoffResult(phase="exported", record=rec, path=str(path), task_id=pend.task_id, terms=terms,
                         user_endpoint_config=dict(plan.wire), notice=(
                             f"Exported {rid} to {path}: the canary ran on job {job or '(unknown)'}"
                             f"{f' on {host}' if host else ''}. The consumer reads the record (the endpoint id, the "
                             "config, the nodes and the facility map to its own settings; any shell file it writes "
                             f"from them quotes each value); it expires at {rec.expires_at}. {terms.binds} The end "
                             f"that works: {' '.join(rec.release.steps)}"))


def release_of(plan: Plan, job: str | None, user: str | None) -> HandoffRelease:
    f = plan.facts
    pbs = plan.scheduler == "pbs"
    cancel = "qdel" if pbs else "scancel"
    who = user if user and runner._USER_NAME.match(user) else None
    listing = (f"qstat -u {shlex.quote(who)}" if pbs else f"squeue -u {shlex.quote(who)}") if who else (
        'qstat -u "$USER"' if pbs else 'squeue -u "$USER"')
    cmd = f"{cancel} {shlex.quote(job)}" if job else None
    idle, uep = f["max_idletime_s"], f["uep_idle_exit_s"]
    steps = ["Stop submitting tasks with this user_endpoint_config: while one is queued its user endpoint starts "
             "blocks for it, and any submit with this config starts the user endpoint again."]
    steps.append((f"With nothing queued its blocks idle-release after {idle} s" if idle else
                  "With nothing queued its blocks idle-release after the facility's own idle window (the template "
                  "sets none hpc-bridge can read)")
                 + (f", and the user endpoint itself exits after {uep} s idle." if uep else
                    "; the template sets no idle exit for the user endpoint itself, which bills nothing once its "
                    "blocks are gone."))
    steps.append(f"To end a block sooner, once nothing is queued or running: `{cmd or cancel + ' <job id>'}` (list "
                 f"them with `{listing}`) — with min_blocks 0 nothing replaces a cancelled block.")
    if plan.access == "ssh":
        scope = ("On this endpoint hpc-bridge stood up, its stop_endpoint asks first while this record is unexpired: "
                 "handoffs=\"cancel\" cancels this pool's blocks, handoffs=\"keep\" leaves its user endpoint running; "
                 "teardown_endpoint stops the manager and every user endpoint it runs, this one included.")
        steps.append(scope)
    else:
        scope = ("hpc-bridge does not police this user endpoint once the consumer holds the record: its stop_endpoint "
                 "names "
                 "this handoff and this release, and stops nothing of it (a facility's endpoint gives hpc-bridge no "
                 "cancel channel).")
    return HandoffRelease(min_blocks=int(f["min_blocks"]), ends_by_itself=int(f["min_blocks"]) == 0,
                          block_idle_release_s=idle, uep_idle_exit_s=uep, cancel_block=cmd, list_blocks=listing,
                          steps=steps, hpc_bridge_scope=scope)


# ---- the store: exported records, and what a stop does with them -----------------------------------------------------

def handoffs_dir() -> Path:
    from .state import _state_dir

    return _state_dir() / "handoffs"


def _write(rec: HandoffRecord) -> Path:
    d = handoffs_dir()
    d.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = d / f"{rec.handoff_id}.json"
    tmp = d / f".{rec.handoff_id}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(rec.model_dump_json(indent=1))
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    return path


def _ended_path(hid: str) -> Path:
    return handoffs_dir() / f"{hid}.ended"


def _parse_iso(s: str | None) -> float | None:
    try:
        return datetime.strptime(str(s), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp()
    except (TypeError, ValueError):
        return None


@dataclass
class Live:
    """A handoff of the bound endpoint a stop must reckon with: an exported, unexpired, not-ended record, or one whose
    canary is still out."""

    ref: HandoffRef
    record: HandoffRecord | None = None
    pending: Pending | None = None


def live(app: AppCtx, now: float | None = None) -> list[Live]:
    eid = app.state.endpoint_id
    if eid is None:
        return []
    now = time.time() if now is None else now
    out: list[Live] = []
    seen = set()
    for p in list(app.handoffs.values()):
        if p.eid == eid:
            seen.add(p.key)
            out.append(Live(ref=HandoffRef(handoff_id="(validating)", state="validating",
                                           expires_at=p.terms.expires_at, task_id=p.task_id or _task_id(p.future),
                                           uep_id_seen=None), pending=p))
    for raw in _disk_pendings(eid):   # one another life of this server sent (a restart): it is out all the same
        if raw.get("key") in seen or not isinstance(raw.get("approved"), int | float):
            continue
        if now > float(raw["approved"]) + float(raw.get("hours") or 0) * 3600:
            continue
        p2 = _from_disk(raw, app.handoff_meta.get(eid, {}))
        if p2 is not None:
            out.append(Live(ref=HandoffRef(handoff_id="(validating)", state="validating",
                                           expires_at=p2.terms.expires_at, task_id=p2.task_id), pending=p2))
    d = handoffs_dir()
    for path in sorted(d.glob("hoff-*.json")) if d.is_dir() else []:
        try:
            rec = load_record(path.read_text())
        except (OSError, HandoffFormatError):
            continue
        exp = _parse_iso(rec.expires_at)
        if (rec.endpoint.manager_id != eid or not _ID.match(rec.handoff_id) or exp is None or exp <= now
                or _ended_path(rec.handoff_id).exists()):
            continue
        out.append(Live(ref=HandoffRef(handoff_id=rec.handoff_id, state="exported", expires_at=rec.expires_at,
                                       path=str(path), uep_id_seen=rec.endpoint.uep_id_seen,
                                       task_id=rec.validation.task_id,
                                       jobs=[rec.validation.job_id] if rec.validation.job_id else []),
                        record=rec))
    return out


def scope_of(items: list[Live]) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """The user endpoints of these handoffs, for a scoped release: the ids their canaries saw, and the block requests
    in their configs (an endpoint hpc-bridge stood up names its user endpoint's in its log)."""
    ueps: list[str] = []
    nums: list[int] = []
    for it in items:
        wire = it.record.user_endpoint_config if it.record is not None else (
            it.pending.plan.wire if it.pending is not None else {})
        if it.ref.uep_id_seen and _UUID.match(it.ref.uep_id_seen):
            ueps.append(it.ref.uep_id_seen)
        n = wire.get("hpcb_block")
        if isinstance(n, int) and not isinstance(n, bool) and 0 < n < 2 ** 48:
            nums.append(n)
    return tuple(ueps), tuple(nums)


def _named(items: list[Live]) -> str:
    parts = []
    for it in items:
        if it.pending is not None:
            parts.append(f"a handoff still validating (its canary, task {it.ref.task_id or '(no id yet)'}, is out)")
        else:
            jobs = f", block job {', '.join(it.ref.jobs)}" if it.ref.jobs else ""
            parts.append(f"{it.ref.handoff_id} (the consumer's pool, user endpoint {it.ref.uep_id_seen or 'unknown'}"
                         f"{jobs}, "
                         f"its record expires {it.ref.expires_at})")
    return "; ".join(parts)


def question(app: AppCtx, items: list[Live], *, op: Literal["stop", "teardown"]) -> EndpointStatus:
    """A stop or teardown on an endpoint hpc-bridge stood up, with a handoff of it unexpired: ask first, change
    nothing."""
    tool = "stop_endpoint" if op == "stop" else "teardown_endpoint"
    what = ("A stop with handoffs=\"cancel\" cancels the consumer's blocks too (and its user endpoint then exits once "
            "idle); with handoffs=\"keep\" it releases only this session's block and leaves the consumer's user "
            "endpoint running (the answer is then draining, naming it)." if op == "stop" else
            "A teardown stops this endpoint's manager and every user endpoint it runs — the consumer's included, "
            "ending its pool: teardown_endpoint(handoffs=\"cancel\") does that. To keep the consumer's pool, do not "
            "tear down: stop_endpoint(handoffs=\"keep\") releases only this session's block.")
    return EndpointStatus(status="needs_confirmation", block_state="cold", endpoint_id=app.state.endpoint_id,
                          handoffs=[it.ref for it in items], notice=(
                              f"Nothing was done: this endpoint was handed off (export_handoff) — {_named(items)}. "
                              f"{what} Ask the "
                              f"user, then call {tool}(handoffs=\"cancel\")"
                              + (" or stop_endpoint(handoffs=\"keep\")." if op == "stop" else ".")))


def end(items: list[Live], how: str) -> None:
    """These handoffs are over (the user's choice): a pending canary is dropped (its task may still be queued) and an
    exported record is marked ended beside it, so no later stop asks about it again."""
    for it in items:
        if it.record is not None:
            try:
                p = _ended_path(it.record.handoff_id)
                p.write_text(json.dumps({"ended_at": _iso(time.time()), "how": how}))
                os.chmod(p, 0o600)
            except OSError as exc:
                print(f"hpc-bridge: could not mark {it.record.handoff_id} ended ({exc})", file=sys.stderr)


def drop_pending(app: AppCtx, items: list[Live]) -> None:
    """The user ended these handoffs: no record will be exported for them. Their canaries stay among the endpoint's
    sent checks (`_note_check`), so a later stop still sees one that is queued."""
    for it in items:
        if it.pending is not None:
            app.handoffs.pop(it.pending.key, None)
            _unpersist(it.pending.key)
            _close(it.pending.runner)


def mep_note(items: list[Live]) -> str:
    lines = []
    for it in items:
        if it.record is not None:
            lines.append(f"{it.ref.handoff_id} (the consumer's, record expires {it.ref.expires_at}): hpc-bridge does "
                         f"not stop it — {' '.join(it.record.release.steps)}")
        else:
            lines.append(f"a handoff still validating: its canary (task {it.ref.task_id or '(no id yet)'}) is queued "
                         "on the facility's endpoint and starts a block to run once; nothing here can cancel it")
    return "Handoffs of this endpoint (export_handoff): " + " | ".join(lines)
