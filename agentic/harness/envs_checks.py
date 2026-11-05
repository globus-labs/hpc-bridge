"""Block environments (PR 3; vault `Planned/Block environments.md`): what the env scenarios share — the probe each command
prints, the script steps that ask for an env, the graders that read a trace, and the world checks that read the
facility. PR 3 is the SSH half (a built env on the free login node, activated per command) and existing envs
everywhere; a facility endpoint's builds are PR 4's, with their cells (branch `feat/block-envs-mep`).

Pure and SDK-free (scenarios import it on the host, as they import batch_checks).

The tool surface the graders read (the checkpoint fixed it): `ensure_endpoint_up(…, env=…)`, `run_batch(…, env=…)`,
`submit_job(…, env=…)` and `export_handoff(…, env=…)`, where env is `{"activate": "<line>"}` |
`{"requirements": "<text>", "python"?, "root"?}` (+ `name`) | `{}`. Their answers carry `env` (EndpointStatus.env,
BatchStatus.env, JobInfo.env) and, on a spend answer, `block.env` (BlockTerms.env): {kind: activate|built, how:
activate|login, key, name, python, path, root, state: ready|building|failed|unchecked, task_id, cached,
lock_sha256, size_mb, new_block, billed, index_lines, warning, error}. run_shell answers phase `env_building` when
nothing ran because the env is being built or rebuilt on the login node.

THE PROBE. Each env scenario's command prints one line of what the command saw, so the graders need nothing from the
wrapper's internals (they read the same line on main, where they fail on their own terms):

    HPCB_ENV_PROBE tag=<$HPCB_TAG or none> venv=<$VIRTUAL_ENV or none> first=<the first PATH entry>
    HPCB_ENV_PY <sys.prefix> <six.__version__>        (or `HPCB_ENV_PY none` when python cannot import six)

`six` is the cells' one requirement: small, pure Python, on PyPI since 2010 — the fake cluster's login node must reach
PyPI, as the endpoint venv's own build already needs.
"""
from __future__ import annotations

import re
import shlex
import time
from typing import Any

import batch_checks as bc
from invariants import Result, Trace

REQUIREMENTS = "six==1.17.0\n"
SIX = "1.17.0"
ENV_WAIT_S = 600.0          # a login build of one small package is seconds; the wait only bounds a broken one
POLL_S = 5.0
PROBE = ('echo "HPCB_ENV_PROBE tag=${HPCB_TAG:-none} venv=${VIRTUAL_ENV:-none} first=${PATH%%:*}"; '
         "python -c 'import sys, six; print(\"HPCB_ENV_PY\", sys.prefix, six.__version__)' 2>/dev/null "
         '|| echo "HPCB_ENV_PY none"')
_PROBE = re.compile(r"^HPCB_ENV_PROBE tag=(\S*) venv=(\S*) first=(\S*)\s*$", re.M)
_PY = re.compile(r"^HPCB_ENV_PY (\S+)(?: (\S+))?\s*$", re.M)


def root_for(ops) -> str:
    """A root of this run's own (the facility id is the run's): a cell's first build is a build, never a cache hit
    left by an earlier run on the same pool user."""
    return f"$HOME/.hpc-bridge/envs-{re.sub(r'[^A-Za-z0-9_.-]', '-', str(ops.facility))}"


# ---- the script's steps ----------------------------------------------------------------------------------------------

def env_of(r: dict | None) -> dict:
    """The env an answer states: its own (`env`), else its block's terms' (`block.env`)."""
    r = r or {}
    return dict(r.get("env") or (r.get("block") or {}).get("env") or {})


async def ask_env(ops, env: dict, **args: Any) -> dict:
    """ensure_endpoint_up(shape="compute", env=…, **args); polls (without env: the session's is kept) while the env is
    still being built. The answer must STATE the env (on main, `env` is no argument: the cell aborts at its premise)."""
    r = await ops.call("ensure_endpoint_up", shape="compute", env=env, **args)
    if bc.unknown_tool(r) or r.get("is_error") or (env and not env_of(r)):
        ops.abort(f"ensure_endpoint_up did not take the env: {bc._brief(r)!r}")
    deadline = time.monotonic() + ENV_WAIT_S
    while env and env_of(r).get("state") == "building" and r.get("status") != "needs_confirmation":
        if time.monotonic() > deadline:
            ops.abort(f"the env was still building after {ENV_WAIT_S:.0f} s: {env_of(r)!r}")
        await ops.sleep(POLL_S)
        r = await ops.call("ensure_endpoint_up", shape="compute")
    ops.say(f"HPCB_ENV {env_of(r)!r}")
    return r


async def probe(ops, *, shape: str = "compute") -> dict:
    """The probe command, once more while the answer is `env_building` (a rebuild in flight: nothing ran)."""
    deadline = time.monotonic() + ENV_WAIT_S
    r = await ops.call("run_shell", command=PROBE, shape=shape)
    while r.get("phase") in ("env_building", "cold_start") and time.monotonic() < deadline:
        await ops.sleep(POLL_S)
        r = await ops.call("run_shell", command=PROBE, shape=shape)
    if r.get("phase") == "running" and r.get("task_id"):
        r = await ops.call("poll_task", task_id=r["task_id"], wait=60)
    ops.say(f"probe ({shape}): {r.get('phase')!r} {str(r.get('stdout') or '')[-300:]!r}")
    return r


# ---- reading a trace -------------------------------------------------------------------------------------------------

def env_calls(t: Trace) -> list[tuple[int, dict, dict]]:
    """Every call that named an env, with the env it asked for and its answer."""
    return [(i, dict(c.input.get("env") or {}), c.result or {}) for i, c in enumerate(t.calls)
            if c.name in ("ensure_endpoint_up", "run_batch", "submit_job", "export_handoff") and "env" in c.input]


def probes(t: Trace) -> list[tuple[int, dict]]:
    """What each probe command saw: {tag, venv, first, prefix, six} by call index (run_shell and poll_task answers)."""
    out = []
    for i, c in enumerate(t.calls):
        if c.name not in ("run_shell", "poll_task"):
            continue
        text = str((c.result or {}).get("stdout") or "")
        m, p = _PROBE.search(text), _PY.search(text)
        if m is None:
            continue
        out.append((i, {"tag": m.group(1), "venv": m.group(2), "first": m.group(3),
                        "prefix": p.group(1) if p and p.group(1) != "none" else None,
                        "six": p.group(2) if p else None}))
    return out


def _asks_from(t: Trace, i: int) -> list[int]:
    """Spend questions (an answer needs_confirmation) from call `i` on."""
    return [k for k, c in enumerate(t.calls) if k >= i and "needs_confirmation" in (
        (c.result or {}).get("status"), (c.result or {}).get("phase"))]


# ---- graders (trace) -------------------------------------------------------------------------------------------------

def env_built_on_login(t: Trace) -> Result:
    """The first built env was built on the LOGIN node (`how` login): stated ready, free (nothing billed), no new block
    — the call that named it asked no spend question, and none came after it."""
    name = "env_built_on_login"
    built = [(i, env_of(r)) for i, want, r in env_calls(t) if "requirements" in want]
    if not built:
        return Result(name, False, "no call asked for a built env")
    i, e = built[0]
    later = [env_of(c.result) for c in t.calls[i:] if env_of(c.result).get("key") == e.get("key")]
    ready = next((x for x in later if x.get("state") == "ready"), {})
    bad = []
    if e.get("how") != "login":
        bad.append(f"how={e.get('how')!r}")
    if not ready:
        bad.append("never stated ready")
    if e.get("billed") or e.get("new_block"):
        bad.append(f"billed={e.get('billed')!r} new_block={e.get('new_block')!r}")
    if _asks_from(t, i):
        bad.append(f"a spend question from it on (calls {_asks_from(t, i)})")
    return Result(name, not bad, f"ok: env {e.get('key')} at {ready.get('path')}" if not bad else "; ".join(bad))


def env_cache_hit(t: Trace) -> Result:
    """The same env asked for again (in other words) was a cache hit: the same key and path, `cached`, ready."""
    name = "env_cache_hit"
    built = [(i, env_of(r)) for i, want, r in env_calls(t) if "requirements" in want]
    if len(built) < 2:
        return Result(name, False, f"{len(built)} call(s) asked for a built env; the cell asks twice")
    first = next((env_of(c.result) for c in t.calls if env_of(c.result).get("state") == "ready"
                  and env_of(c.result).get("key") == built[0][1].get("key")), {})
    _i, e = built[1]
    ok = (e.get("key") == first.get("key") and e.get("cached") is True and e.get("state") == "ready"
          and e.get("path") == first.get("path"))
    return Result(name, ok, f"second ask: key={e.get('key')!r} cached={e.get('cached')!r} state={e.get('state')!r} "
                  f"path={e.get('path')!r} (first ready at {first.get('path')!r})")


def env_active_in_command(t: Trace, *, after: int = 0) -> Result:
    """A command run while the built env was the session's ran IN it: python's prefix is the env's path and six
    imports at its version."""
    name = "env_active_in_command"
    paths = {env_of(c.result).get("path") for c in t.calls if env_of(c.result).get("state") == "ready"
             and env_of(c.result).get("kind") == "built"} - {None}
    seen = [p for i, p in probes(t) if i >= after and p.get("prefix")]
    hit = [p for p in seen if p["prefix"] in paths and p["six"] == SIX]
    return Result(name, bool(hit), f"env path(s) {sorted(paths)}; probes saw prefixes "
                  f"{[(p['prefix'], p['six']) for p in seen][:4]}")


def env_switch_no_new_block(t: Trace) -> Result:
    """Switching to another env — and to none — was no new block: each switch's answer was `up`, never a spend
    question, and the run confirmed exactly one block."""
    name = "env_switch_no_new_block"
    switches, prev = [], None
    for i, want, r in env_calls(t):
        if t.calls[i].name != "ensure_endpoint_up":
            continue
        key = env_of(r).get("key") if want else None
        if key != prev:
            switches.append((i, r))
        prev = key
    bad = [f"call {i}: status={r.get('status')!r}" for i, r in switches if r.get("status") != "up"]
    one = bc.one_confirmation(t)
    if not switches:
        bad.append("no env switch in the run")
    if not one.ok:
        bad.append(one.detail)
    return Result(name, not bad, f"ok: {len(switches)} switch(es), each `up`; {one.detail}" if not bad
                  else "; ".join(bad))


def env_switch_clean(t: Trace, *, tag: str) -> Result:
    """After the switch to the existing env (`activate`, which sets HPCB_TAG), a command saw that env and NOTHING of
    the built one before it: no VIRTUAL_ENV, the built env's bin not first on PATH; after the switch to no env, neither."""
    name = "env_switch_clean"
    built = {env_of(c.result).get("path") for c in t.calls if env_of(c.result).get("kind") == "built"} - {None}
    act = [i for i, want, _r in env_calls(t) if "activate" in want]
    none = [i for i, want, _r in env_calls(t) if want == {}]
    if not act or not built:
        return Result(name, False, f"premise not met: built envs {sorted(built)}, activate calls {act}")
    after_act = [p for i, p in probes(t) if i > act[0] and (not none or i < none[0])]
    after_none = [p for i, p in probes(t) if none and i > none[0]]
    bad = []
    if not after_act or after_act[0]["tag"] != tag or after_act[0]["venv"] != "none" or any(
            str(after_act[0]["first"]).startswith(b) for b in built):
        bad.append(f"after the switch: {after_act[:1]!r}")
    if none and (not after_none or after_none[0]["tag"] != "none" or after_none[0]["venv"] != "none"):
        bad.append(f"after no env: {after_none[:1]!r}")
    return Result(name, not bad, "ok" if not bad else "; ".join(bad))


def batch_rows_in_env(t: Trace, *, rows: int) -> Result:
    """A batch given its own env ran every row in it (its answer stated the env; each row's tail shows the env's
    prefix and six), and the session's env stayed the session's."""
    name = "batch_rows_in_env"
    accepted = [(i, r) for i, want, r in env_calls(t) if t.calls[i].name == "run_batch" and r.get("batch_id")]
    if not accepted:
        return Result(name, False, "no run_batch with an env was accepted")
    _i, r = accepted[0]
    e = env_of(r)
    seen = bc.rows_seen(t, r.get("batch_id"))
    good = [x for x in seen.values() if x.get("state") == "done" and f"HPCB_ENV_PY {e.get('path')} {SIX}"
            in str(x.get("tail") or "")]
    ok = bool(e.get("path")) and len(good) == rows
    return Result(name, ok, f"batch {r.get('batch_id')} env {e.get('key')} at {e.get('path')}: {len(good)}/{rows} "
                  "rows ran in it")


# ---- world checks (ctx: after the script, before the teardown) -------------------------------------------------------

def _ready_paths(t: Trace) -> list[str]:
    out: list[str] = []
    for c in t.calls:
        e = env_of(c.result)
        if e.get("state") == "ready" and e.get("kind") == "built" and e.get("path") and e["path"] not in out:
            out.append(str(e["path"]))
    return out


def _marker(text: str) -> dict[str, str]:
    return dict(ln.split("=", 1) for ln in text.splitlines() if "=" in ln)


async def env_on_login_disk(ctx) -> Result:
    """world: the env the run built is on the facility's shared filesystem where it said — its marker naming its key and
    the LOGIN node's architecture, built before the second ask (a cache hit rebuilds nothing) — with nothing of a
    build left beside it."""
    name = "world:env_on_login_disk"
    paths = _ready_paths(ctx.trace)
    if not paths:
        return Result(name, False, "premise not met: no ready built env in the run's answers")
    p = paths[0]
    q = shlex.quote(p)
    rc, out = ctx.ssh(f'cat {q}/.hpcb-env 2>&1; echo "HPCB_ARCH=$(uname -m)"; '
                      f'ls -a "$(dirname {q})" 2>&1; echo HPCB_DONE', 60)
    if "HPCB_DONE" not in out:
        return Result(name, False, f"UNVERIFIABLE: could not read {p} (rc={rc}): {out[-200:]!r}")
    m = _marker(out)
    got = re.search(r"HPCB_ARCH=(\S+)", out)
    arch = got.group(1) if got else None
    leftovers = [ln for ln in out.splitlines() if ".build." in ln or ".broken." in ln]
    asks = [i for i, want, _r in env_calls(ctx.trace) if "requirements" in want]
    second = bc.epoch(ctx.trace, ctx.trace.calls[asks[1]].t_call) if len(asks) > 1 else None
    built = float(m.get("built", "nan")) if m.get("built", "").replace(".", "", 1).isdigit() else None
    bad = []
    if not m.get("key") or not p.rstrip("/").split("/")[-1].startswith(str(m.get("short", ""))):
        bad.append(f"marker {m!r}")
    if m.get("arch") != arch:
        bad.append(f"arch {m.get('arch')!r} (login node: {arch!r})")
    if second is not None and (built is None or built > second):
        bad.append(f"built at {built!r}, after the second ask ({second:.0f}): the cache hit rebuilt it")
    if leftovers:
        bad.append(f"left beside it: {leftovers}")
    return Result(name, not bad, f"ok: {p} (arch {arch}, built {built})" if not bad else "; ".join(bad))
