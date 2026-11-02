"""Batch phase 2b's graders and world checks: `export_handoff` v1, the loam handoff (vault `Planned/Batch and swarm
execution.md` §8, §10, §12 D6). The scenarios that use them: handoff_polaris_shape and handoff_mep_min_blocks_refused
(fake `pbs_mep`), handoff_ssh_shape (fake `default`). Hermetic tests: test_handoff_scenarios.py.

What they grade is the record and the answers as the plugin delivers them — `phase`, `terms`, `record`, `task_id` —
never the wording, except where a word IS the contract (`enforced_by: "uec-bounds-only"`, the cancel command)."""
from __future__ import annotations

import re
import shlex
import time
from typing import Any

import batch_checks as bc
from invariants import Result, ToolCall, Trace

TOOL = "export_handoff"
SCHEMA = "hpc-bridge.handoff"
ENFORCED_BY = "uec-bounds-only"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_PBS_JOB = re.compile(r"^\d+(?:\[\d*\])?\.[A-Za-z0-9][\w.-]*$")
_SLURM_JOB = re.compile(r"^\d+$")
POLL_S = 10.0
EXPORT_WAIT_S = 480.0


# ---- reading the trace ---------------------------------------------------------------------------------------------

def answers(t: Trace) -> list[tuple[int, ToolCall]]:
    """Every export_handoff call, in order."""
    return t.named(TOOL)


def _r(c: ToolCall) -> dict:
    return c.result or {}


def _yes(c: ToolCall) -> bool:
    return (c.input or {}).get("confirm_spend") is True


def record_of(t: Trace) -> dict | None:
    """The record of the first export_handoff answer that exported one."""
    for _i, c in answers(t):
        rec = _r(c).get("record")
        if _r(c).get("phase") == "exported" and isinstance(rec, dict):
            return rec
    return None


def _task_ids(t: Trace) -> list[str]:
    out = []
    for _i, c in answers(t):
        r = _r(c)
        tid = r.get("task_id") or ((r.get("record") or {}).get("validation") or {}).get("task_id")
        if r.get("phase") in ("validating", "exported") and tid and tid not in out:
            out.append(str(tid))
    return out


def _main(t: Trace) -> str | None:
    """On main the tool does not exist: the answer names an unknown tool."""
    for _i, c in answers(t):
        if bc.unknown_tool(c.result):
            return f"export_handoff is not a tool on this server: {str(_r(c).get('text'))[:80]!r}"
    return None


# ---- the script's steps --------------------------------------------------------------------------------------------

async def export(ops, uec: dict, *, expires_hours: float | None = None, wait_s: float = EXPORT_WAIT_S) -> dict:
    """The handoff as an agent drives it: the call asks (the terms); the user's yes is the same call with
    confirm_spend=True; then the same call (no yes) until the canary's answer is collected. Returns the last answer
    (on main: the unknown-tool text, said so)."""
    kw: dict[str, Any] = {"user_endpoint_config": uec}
    if expires_hours is not None:
        kw["expires_hours"] = expires_hours
    r = await ops.call(TOOL, **kw)
    if bc.unknown_tool(r):
        ops.say("main: export_handoff is not a tool on this server")
        return r
    if r.get("phase") != "needs_confirmation":
        return r
    ops.say(f"the terms: worst case {(r.get('terms') or {}).get('worst_case')} "
            f"{(r.get('terms') or {}).get('unit')}; the user says yes")
    r = await ops.call(TOOL, confirm_spend=True, **kw)
    deadline = time.monotonic() + wait_s
    while r.get("phase") == "validating" and time.monotonic() < deadline:
        await ops.sleep(POLL_S)
        r = await ops.call(TOOL, **kw)
    return r


# ---- graders ---------------------------------------------------------------------------------------------------------

def asked_first(t: Trace) -> Result:
    """The first export_handoff answer states the terms and sends nothing (needs_confirmation, a worst case > 0 in a
    named unit, `enforced_by: uec-bounds-only`); nothing was validating or exported before a call with
    confirm_spend=True."""
    name = "handoff_asked_first"
    if (m := _main(t)) is not None:
        return Result(name, False, m)
    seen = answers(t)
    if not seen:
        return Result(name, False, "no export_handoff call")
    _i0, first = seen[0]
    r, terms = _r(first), _r(first).get("terms") or {}
    why = []
    if _yes(first):
        why.append("the first call already carried confirm_spend=True: nothing was asked")
    if r.get("phase") != "needs_confirmation":
        why.append(f"the first answer is {r.get('phase')!r}, not needs_confirmation")
    if not (isinstance(terms.get("worst_case"), int | float) and terms["worst_case"] > 0 and terms.get("unit")):
        why.append(f"the terms name no worst case in a unit: {terms!r}"[:200])
    if terms.get("enforced_by") != ENFORCED_BY:
        why.append(f"the terms' enforced_by is {terms.get('enforced_by')!r}, not {ENFORCED_BY!r}")
    yes_at = next((i for i, c in seen if _yes(c)), None)
    early = [i for i, c in seen if _r(c).get("phase") in ("validating", "exported") and (yes_at is None or i < yes_at)]
    if early:
        why.append(f"a canary was out before any yes, at call(s) {early}")
    return Result(name, not why, f"ok: asked first ({terms.get('worst_case')} {terms.get('unit')}, "
                  f"{ENFORCED_BY}), the canary only after the yes" if not why else "; ".join(why))


def one_canary(t: Trace) -> Result:
    """Exactly one billed canary: every validating / exported answer names the same task, and one was exported."""
    name = "handoff_one_canary"
    if (m := _main(t)) is not None:
        return Result(name, False, m)
    tids = _task_ids(t)
    rec = record_of(t)
    if len(tids) != 1:
        return Result(name, False, f"{len(tids)} canary task(s) named: {tids}")
    if rec is None:
        return Result(name, False, f"canary {tids[0]} was sent and no record was exported")
    return Result(name, True, f"ok: one canary ({tids[0]}), exported")


def record_shape(t: Trace, *, uec: dict | None = None, scheduler: str, access: str, identity: str | None = None,
                 pinned: bool = False) -> Result:
    """The exported record is the v1 contract: schema and version; a wire-ready config (the facility's own key names —
    `queue` on PBS —, no hpc-bridge marker, `uec`'s values where given); a template digest and every key rendered;
    the terms with `enforced_by: uec-bounds-only` and approved_at; a validation the canary made (ok, a job id of the
    scheduler's shape, the mapped identity, a device pin where `pinned`); the user endpoint seen, not the manager;
    the client Python; loam's environment."""
    name = "handoff_record_shape"
    if (m := _main(t)) is not None:
        return Result(name, False, m)
    rec = record_of(t)
    if rec is None:
        return Result(name, False, "no export_handoff answer exported a record")
    why: list[str] = []
    if rec.get("schema") != SCHEMA or rec.get("version") != 1:
        why.append(f"schema {rec.get('schema')!r} version {rec.get('version')!r}")
    wire = rec.get("user_endpoint_config") or {}
    if scheduler == "pbs" and ("queue" not in wire or "partition" in wire):
        why.append(f"not the PBS template's key names: {sorted(wire)}")
    if {"compute", "interface", "endpoint_setup"} & set(wire) and access == "mep":
        why.append(f"an hpc-bridge marker reached a facility's config: {sorted(set(wire) & {'compute', 'interface'})}")
    for k, v in (uec or {}).items():
        if wire.get(k) != v:
            why.append(f"{k}={wire.get(k)!r} in the record, {v!r} asked")
    tmpl = rec.get("template") or {}
    if not re.fullmatch(r"[0-9a-f]{64}", str(tmpl.get("sha256") or "")):
        why.append(f"template.sha256 {tmpl.get('sha256')!r}")
    renders = tmpl.get("renders") or {}
    if set(renders) != set(wire) or not all(renders.values()):
        why.append(f"renders {renders!r} does not cover the config with every key rendered"[:200])
    terms = rec.get("terms") or {}
    if terms.get("enforced_by") != ENFORCED_BY or not terms.get("approved_at"):
        why.append(f"terms enforced_by={terms.get('enforced_by')!r} approved_at={terms.get('approved_at')!r}")
    if terms.get("expires_at") != rec.get("expires_at"):
        why.append("the terms' expires_at is not the record's")
    v = rec.get("validation") or {}
    job = str(v.get("job_id") or "")
    if v.get("ok") is not True or not (_PBS_JOB if scheduler == "pbs" else _SLURM_JOB).match(job):
        why.append(f"validation ok={v.get('ok')!r} job_id={job!r}")
    if identity is not None and v.get("identity") != identity:
        why.append(f"validation identity {v.get('identity')!r}, want {identity!r}")
    if pinned and not any(str(x) for x in (v.get("device_pin") or {}).values()):
        why.append(f"no device pin seen: {v.get('device_pin')!r}")
    ep, fac = rec.get("endpoint") or {}, rec.get("facility") or {}
    if fac.get("access") != access or fac.get("scheduler") != scheduler:
        why.append(f"facility access={fac.get('access')!r} scheduler={fac.get('scheduler')!r}")
    uep = str(ep.get("uep_id_seen") or "")
    if not _UUID.match(uep) or uep == ep.get("manager_id"):
        why.append(f"uep_id_seen {uep!r} (manager {ep.get('manager_id')!r})")
    if not re.fullmatch(r"3\.\d+", str((rec.get("client_requirements") or {}).get("python") or "")):
        why.append(f"client_requirements.python {(rec.get('client_requirements') or {}).get('python')!r}")
    env = (rec.get("loam") or {}).get("env") or {}
    if env.get("HPC_GLOBUS_ENDPOINT_ID") != ep.get("manager_id") or not env.get("HPC_ENDPOINT_CONFIG_JSON"):
        why.append(f"loam env {sorted(env)}")
    return Result(name, not why, f"ok: v1 record, job {job} as {v.get('identity')!r}, uep {uep[:8]}…, "
                  f"{len(wire)} keys all rendered" if not why else "; ".join(why))


def release_honest(t: Trace, *, cancel: str) -> Result:
    """The record's release names the end that really works: min_blocks 0, so the pool ends by itself once nothing is
    submitted; the first step is to stop submitting with this config; the scheduler's cancel (`cancel`) names the
    canary's own block and comes after that step, saying why it ends it (min_blocks 0: nothing replaces it). Never the
    cancel alone, never daemon.pid (D6 refused those pools)."""
    name = "handoff_release_honest"
    if (m := _main(t)) is not None:
        return Result(name, False, m)
    rec = record_of(t)
    if rec is None:
        return Result(name, False, "no export_handoff answer exported a record")
    rel, job = rec.get("release") or {}, str((rec.get("validation") or {}).get("job_id") or "")
    steps = [str(s) for s in rel.get("steps") or []]
    why = []
    if rel.get("min_blocks") != 0 or rel.get("ends_by_itself") is not True:
        why.append(f"min_blocks={rel.get('min_blocks')!r} ends_by_itself={rel.get('ends_by_itself')!r}")
    if not steps or not re.search(r"\bstop\b", steps[0], re.I) or "submit" not in steps[0].lower():
        why.append(f"the first step is not to stop submitting: {steps[:1]!r}")
    if rel.get("cancel_block") != f"{cancel} {shlex.quote(job)}":
        why.append(f"cancel_block {rel.get('cancel_block')!r}, want {cancel} {job}")
    at = [i for i, s in enumerate(steps) if re.search(rf"\b{cancel}\b", s)]
    if not at or at[0] == 0 or not all("min_blocks" in steps[i] for i in at):
        why.append(f"the {cancel} step is missing, first, or does not say why it ends the block")
    if "daemon.pid" in " ".join(steps):
        why.append("a daemon.pid removal was offered")
    return Result(name, not why, f"ok: stop submitting, then idle release ({rel.get('block_idle_release_s')} s), "
                  f"or `{rel.get('cancel_block')}`" if not why else "; ".join(why))


def d6_refused(t: Trace) -> Result:
    """D6: a MEP handoff with min_blocks >= 1 is refused before any terms — the ask and the yes both answer refused,
    reason mep_min_blocks, with no terms, no record and no canary, saying min_blocks must be 0."""
    name = "handoff_d6_refused"
    if (m := _main(t)) is not None:
        return Result(name, False, m)
    seen = answers(t)
    if not any(_yes(c) for _i, c in seen) or not any(not _yes(c) for _i, c in seen):
        return Result(name, False, f"want an ask and a yes; {len(seen)} call(s)")
    why = []
    for i, c in seen:
        r = _r(c)
        if r.get("phase") != "refused" or r.get("reason") != "mep_min_blocks":
            why.append(f"call {i}: {r.get('phase')!r}/{r.get('reason')!r}")
        if r.get("terms") or r.get("record") or r.get("task_id"):
            why.append(f"call {i} carried terms, a record or a canary")
        if "min_blocks" not in str(r.get("notice") or ""):
            why.append(f"call {i}'s notice does not name min_blocks")
    return Result(name, not why, f"ok: {len(seen)} answers refused (mep_min_blocks), nothing asked or sent"
                  if not why else "; ".join(why))


# ---- world checks ------------------------------------------------------------------------------------------------------

async def canary_job_as_recorded(ctx, *, scheduler: str, queue: str, account: str | None = None,
                                 walltime: str | None = None) -> Result:
    """world: the scheduler ran the canary's block as the record says — in `queue`, charged to `account`, for
    `walltime` (PBS `qstat -x -f`; Slurm `sacct`)."""
    name = "world:canary_job_as_recorded"
    rec = record_of(ctx.trace)
    job = str(((rec or {}).get("validation") or {}).get("job_id") or "")
    if not job:
        return Result(name, False, "premise not met: no record names a canary job")
    if scheduler == "pbs":
        rc, out = ctx.ssh(f"qstat -x -f {shlex.quote(job)} 2>&1", 60)
        got = {k: v.strip() for k, v in re.findall(r"^\s*(queue|Account_Name|Resource_List\.walltime) = (.*)$", out,
                                                    re.M)}
        want = {"queue": queue, "Account_Name": account, "Resource_List.walltime": walltime}
    else:
        rc, out = ctx.ssh(f"sacct -X -n -P -j {shlex.quote(job)} -o Partition,Account,Timelimit 2>&1", 60)
        cols = (out.strip().splitlines() or [""])[0].split("|")
        got = dict(zip(("queue", "Account_Name", "Resource_List.walltime"), [c.strip() for c in cols], strict=False))
        want = {"queue": queue, "Account_Name": account, "Resource_List.walltime": walltime}
    bad = {k: (got.get(k), v) for k, v in want.items() if v is not None and got.get(k) != v}
    return Result(name, rc == 0 and not bad, f"job {job}: {got}" if not bad else f"job {job}: got/want {bad} (rc {rc})")


async def no_job_since_start(ctx, *, user: str) -> Result:
    """world (D6): no scheduler job of `user` was created after the run's first hpc-bridge call — the refusal sent
    nothing (PBS: every job's ctime from `qstat -x -f`, read as epoch seconds on the login node)."""
    name = "world:no_job_since_start"
    t0 = ctx.trace.t0
    if t0 is None:
        return Result(name, False, "premise not met: the trace has no start time")
    age = ("python3 -c 'import sys, time; print(int(time.mktime(time.strptime(sys.argv[1], "
           "\"%a %b %d %H:%M:%S %Y\"))))' \"$c\" 2>/dev/null || echo -1")
    cmd = (f"command -v qselect >/dev/null || {{ echo NO-QSELECT; exit 3; }}; "
           f"for j in $(qselect -x -u {shlex.quote(user)} 2>/dev/null); do "
           "c=$(qstat -x -f \"$j\" 2>/dev/null | sed -n 's/^ *ctime = //p' | head -n 1); "
           f"echo \"JOB $j $({age})\"; done; echo END")
    rc, out = ctx.ssh(cmd, 90)
    if rc != 0 or "END" not in out:
        return Result(name, False, f"unverifiable (rc {rc}): {out[-200:]!r}")
    new = [(j, int(e)) for j, e in re.findall(r"^JOB (\S+) (-?\d+)$", out, re.M) if int(e) >= t0 - 5 or int(e) < 0]
    return Result(name, not new, f"no job of {user} since the run began" if not new
                  else f"job(s) created since the run began (or unreadable): {new}")
