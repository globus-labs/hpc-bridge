"""topology_roundtrip — every vantage-sensitive SSH path, once, from wherever the network topology puts the client.
Fake cluster; scripted (no model). Run it under each topology (harness/topology.py; fakecluster README "Topologies"):

    agentic/fakecluster/bin/up.sh --profile default --topology host_forward
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default HPCB_FAKE_TOPOLOGY=host_forward \
      ./agentic/run_smoke.sh topology_roundtrip
    # the sweep: agentic/sweep_profiles.sh --profiles topology (in_network, host_forward, bastion on `default`; vip on
    # `site`, whose two login nodes it fronts)

Why: after the bootstrap hpc-bridge pins the login node by the address `$SSH_CONNECTION` reached
(facility/remote.py `_routable_pin`), and every later SSH goes there. Behind NAT, a port-forward, a ProxyJump bastion or
a VIP that is the node's INTERNAL address, which the client cannot reach: a user driving the fake cluster from the host
(`hpcb-fake` -> localhost:2222) found every op after the bootstrap broken, and the dead-pin check probing port 22, not
the alias's. No cell saw it: each ran the MCP server inside the cluster's network, where every internal address answers
and SSH is always port 22. On main (49e342e, 2026-11-03): green under in_network; RED under host_forward, bastion and
vip. Each red cell pins the login node's internal address, and after the restart login_shell and teardown cannot reach
it, leaving the manager running.

THE SCRIPT (the alias is `ops.ssh_host`: `login` in_network, the ssh-config alias `hpcb-fake` elsewhere):
connect (probe -> details -> the login shape up) -> a confirmed compute block -> run_shell -> stop_endpoint -> a NEW
server process on the same state dir (`ops.restart_server()`), which reconnects with the stored record and its pin ->
login_shell `hostname` -> teardown_endpoint. The pin hpc-bridge recorded (endpoints.json) is said after every step; a
watcher lists every ControlMaster the server's ssh opened (each one is an authentication).

GRADED:
- `every_step_succeeded` — each of the eight steps did what it should (from the tool results);
- `world:pin_reachable` — every recorded pin answers from the jail on the alias's path: hpc-bridge's own target for it
  (harness/topology_probe.py: binding.py's facility, rebound to the pin) runs a command over a fresh connection;
- `no_internal_pin` — no pin is an address on a network the jail is not on (the stack's other subnets, run_smoke.sh);
- `world:no_manager_after_product_teardown` — after hpc-bridge's OWN teardown (before the harness' cleanup), no process
  of the endpoint on any login node;
- `world:preauth_runnable` — the MFA-style pre-auth command runs from the jail: every `preauth_command` the flow
  surfaced, and the one hpc-bridge's code builds for the alias and for each pin (`SshTarget.preauth_command`), run with
  BatchMode and a scratch ControlPath (topology.run_preauth);
- `no_reauth` — no SSH destination is authenticated twice: each ControlMaster socket of the server's opened once, the new
  server process riding the old one's masters. ControlPersist is raised to 900 s for the cell
  (HPC_BRIDGE_SSH_CONTROL_PERSIST), so an idle master's expiry is not counted as the topology's doing. The pin's own
  master is a second destination by design (`%C` differs after `rebind`): reported, not failed.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import batch_checks as bc
import topology
from byo_script import PROPOSED, UP, brief, login_pin, proposal_details, state_dir, until_settled
from cluster_ops import leftover_manager_cmd
from invariants import Result, Trace, first_contact_host

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = ("every vantage-sensitive SSH path (bootstrap, block, stop, a new server on the stored pin, login_shell, "
           "teardown, the pre-auth command) from where the network topology puts the client")
TAGS = ["fake", "topology", "login-pin", "ssh-config", "restart", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SSH_CONTROL_PERSIST": "900"}

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` through `{ssh_host}`, confirm a compute "
          "block, run a command on it, stop, start a new server, reconnect, run `hostname` on the login node, tear down.")

ACCOUNT = "hpcb"               # every Slurm profile's pool account (default: not enforced; site: enforced)
UP_WAIT_S = 420.0
BLOCK_WAIT_S = 420.0
POLL_S = 5.0
SHELL_MARK = "HPCB_TOPO_OK"
SETUP_MARK = "TOPO setup"      # the script's words the graders read (regradable: they are in the bundle)
PIN_MARK = "TOPO pin"
MASTERS_MARK = "TOPO masters"
STEPS = ("connect", "compute_block", "run_shell", "stop", "restart", "reconnect", "login_shell", "teardown")
REPO = Path(__file__).resolve().parents[2]


# ---- the script -------------------------------------------------------------------------------------------------------

def _masters(cm_dir: str) -> dict[str, str]:
    """{pid: control socket} of every ssh ControlMaster of this user under `cm_dir` now (OpenSSH titles a backgrounded
    master `ssh: <ControlPath> [mux]`). Read off /proc: nothing to install in the jail."""
    out: dict[str, str] = {}
    for p in Path("/proc").glob("[0-9]*"):
        try:
            if p.stat().st_uid != os.getuid():
                continue
            args = (p / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
        except OSError:
            continue
        if "[mux]" in args and cm_dir in args:
            out[p.name] = args.split("ssh: ", 1)[-1].rsplit("[mux]", 1)[0].strip()
    return out


async def _watch_masters(stop: asyncio.Event, seen: dict[str, str], cm_dir: str) -> None:
    while True:
        seen.update({k: v for k, v in (await asyncio.to_thread(_masters, cm_dir)).items() if k not in seen})
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except TimeoutError:
            pass


def _pin(ops, alias: str, step: str) -> None:
    ops.say(f"{PIN_MARK} {step} {login_pin(alias) or '-'}")


async def _connect(ops, alias: str) -> dict:
    fid = ops.facility
    r = await ops.call("connect_facility", facility=fid, ssh_host=alias)
    if r.get("phase") == PROPOSED:
        r = await ops.call("connect_facility", facility=fid, ssh_host=alias, details=proposal_details(r))
    return await until_settled(ops, r, {"facility": fid, "ssh_host": alias}, wait_s=UP_WAIT_S, poll_s=POLL_S)


async def _drive(ops, alias: str) -> str:
    r = await _connect(ops, alias)
    _pin(ops, alias, "connect")
    up = r.get("phase") == UP
    if up:
        e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True, account=ACCOUNT)
        deadline = time.monotonic() + BLOCK_WAIT_S
        while e.get("status") == "provisioning" and not e.get("is_error") and time.monotonic() < deadline:
            await ops.sleep(POLL_S)
            e = await ops.call("ensure_endpoint_up", shape="compute", account=ACCOUNT)
        _pin(ops, alias, "compute_block")
        if e.get("status") == "up":
            s = await ops.call("run_shell", command=f"hostname; echo {SHELL_MARK}", shape="compute")
            for _ in range(24):
                if s.get("phase") != "running" or not s.get("task_id"):
                    break
                s = await ops.call("poll_task", task_id=s["task_id"], wait=10)
        else:
            ops.say(f"the block did not come up: {e.get('status')!r} {brief(e)!r} — no run_shell")
    else:
        ops.say(f"the connect did not come up: {r.get('phase')!r} {brief(r, 400)!r} — no block")
    s = await bc.stop(ops)
    _pin(ops, alias, "stop")
    await ops.restart_server()
    r2 = await _connect(ops, alias)
    _pin(ops, alias, "reconnect")
    ls = await ops.call("login_shell", command="hostname")
    _pin(ops, alias, "login_shell")
    t = await ops.call("teardown_endpoint")
    for _ in range(6):
        if t.get("status") != "tearing_down":
            break
        await ops.sleep(20)
        t = await ops.call("teardown_endpoint")
    _pin(ops, alias, "teardown")
    return (f"connect {r.get('phase')!r}; stop {s.get('status')!r}; after the restart: connect {r2.get('phase')!r}, "
            f"login_shell rc={ls.get('exit_code')!r} {str(ls.get('stdout') or ls.get('notice') or '')[:80]!r}; "
            f"teardown {t.get('status')!r}")


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    alias = ops.ssh_host
    internal = os.environ.get(topology.INTERNAL_ENV, "").split()
    ops.say(f"{SETUP_MARK} topology={os.environ.get('HPCB_FAKE_TOPOLOGY') or topology.DEFAULT} ssh_host={alias} "
            f"internal={','.join(internal) or '-'}")
    seen: dict[str, str] = {}
    stop = asyncio.Event()
    watcher = asyncio.create_task(_watch_masters(stop, seen, str(state_dir() / "cm")))
    try:
        return await _drive(ops, alias)
    finally:
        stop.set()
        await watcher
        ops.say(f"{MASTERS_MARK} {json.dumps(seen, sort_keys=True)}")


# ---- what the script said, read back ----------------------------------------------------------------------------------

def _said(t: Trace, mark: str) -> list[str]:
    return [x[len(mark):].strip() for x in t.texts if x.startswith(mark)]


def _setup(t: Trace) -> dict[str, str]:
    words = (_said(t, SETUP_MARK) or [""])[0].split()
    return dict(w.split("=", 1) for w in words if "=" in w)


def pins(t: Trace) -> list[str]:
    """Every login-node pin the run recorded: the first-contact note's host when it is not the alias, and every pin the
    script read from endpoints.json after a step — in order, once each."""
    alias = _setup(t).get("ssh_host", "")
    out: list[str] = []
    fc = first_contact_host(t)
    if fc and fc[1] != alias:
        out.append(fc[1])
    for row in _said(t, PIN_MARK):
        parts = row.split()
        if len(parts) == 2 and parts[1] != "-":
            out.append(parts[1])
    return list(dict.fromkeys(out))


# ---- graders over the trace -------------------------------------------------------------------------------------------

def _r(c) -> dict:
    return dict(c.result or {})


def step_outcomes(t: Trace) -> list[tuple[str, bool, str]]:
    """(step, ok, what showed it) for the eight steps, split at the restart."""
    rs = bc.restart_index(t)
    before = [(i, c) for i, c in enumerate(t.calls) if rs is None or i < rs]
    after = [(i, c) for i, c in enumerate(t.calls) if rs is not None and i >= rs]

    def named(calls, *names):
        return [_r(c) for _i, c in calls if c.name in names]

    def last(rows):
        return rows[-1] if rows else {}

    con, ens = named(before, "connect_facility"), named(before, "ensure_endpoint_up")
    shells = named(before, "run_shell", "poll_task")
    stops, recon = named(before, "stop_endpoint"), named(after, "connect_facility")
    logins, tds = named(after, "login_shell"), named(after, "teardown_endpoint")
    ran = [s for s in shells if s.get("phase") == "complete" and s.get("exit_code") == 0
           and SHELL_MARK in str(s.get("stdout") or "")]
    logged = [x for x in logins if x.get("exit_code") == 0 and str(x.get("stdout") or "").strip()]

    def why(rows, key):
        x = last(rows)
        return f"{x.get(key)!r} {brief(x, 200)!r}" if rows else "never called"

    return [
        ("connect", any(x.get("phase") == UP for x in con), why(con, "phase")),
        ("compute_block", any(x.get("status") == "up" for x in ens), why(ens, "status")),
        ("run_shell", bool(ran), "ok" if ran else why(shells, "phase")),
        ("stop", last(stops).get("status") == "down", why(stops, "status")),
        ("restart", rs is not None, "a new server" if rs is not None else "the server was never restarted"),
        ("reconnect", any(x.get("phase") == UP for x in recon), why(recon, "phase")),
        ("login_shell", bool(logged), f"ok: {str(logged[0].get('stdout')).strip()[:60]!r}" if logged
         else why(logins, "exit_code")),
        ("teardown", last(tds).get("status") == "down", why(tds, "status")),
    ]


def every_step_succeeded(t: Trace) -> Result:
    rows = step_outcomes(t)
    bad = [(s, d) for s, ok, d in rows if not ok]
    return Result("every_step_succeeded", not bad,
                  "ok: " + " → ".join(s for s, _ok, _d in rows) if not bad
                  else "; ".join(f"{s} FAILED: {d}" for s, d in bad)[:900])


def no_internal_pin(t: Trace) -> Result:
    """No pin is an address on a network the jail is not on (the subnets run_smoke.sh listed for this stack)."""
    subnets = [s for s in _setup(t).get("internal", "").split(",") if s and s != "-"]
    ps = pins(t)
    bad = [p for p in ps if topology.internal_only(p, subnets)]
    if bad:
        return Result("no_internal_pin", False, f"pinned to an address the client cannot route to: {', '.join(bad)} "
                      f"(the cluster's internal subnets from here: {', '.join(subnets)})")
    return Result("no_internal_pin", True, f"ok: pins {', '.join(ps) or 'none'}; internal subnets "
                  f"{', '.join(subnets) or 'none'}")


def no_reauth(t: Trace) -> Result:
    """Each ControlMaster socket opened once: no destination authenticated twice (see the module doc)."""
    raw = (_said(t, MASTERS_MARK) or [""])[-1]
    try:
        seen = json.loads(raw) if raw else None
    except ValueError:
        seen = None
    if not isinstance(seen, dict):
        return Result("no_reauth", False, "no master record (the watcher never reported)")
    by_dest: dict[str, list[str]] = {}
    for pid, path in seen.items():
        by_dest.setdefault(path, []).append(pid)
    again = {d: p for d, p in by_dest.items() if len(p) > 1}
    detail = f"{len(seen)} authentication(s) over {len(by_dest)} destination(s)"
    if again:
        return Result("no_reauth", False, f"{detail}; re-authenticated: "
                      + "; ".join(f"{Path(d).name} ×{len(p)}" for d, p in again.items()))
    return Result("no_reauth", True, f"ok: {detail} (the alias, and the pin's own master if one was opened)")


# ---- world checks (the jail's network vantage; before the harness' own teardown) ---------------------------------------

def _probe(alias: str, ps: list[str]) -> list[dict]:
    argv = ["uv", "run", "--frozen", "--offline", "--directory", str(REPO), "--extra", "integration", "python",
            str(REPO / "agentic" / "harness" / "topology_probe.py"), alias, *ps]
    r = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=600, check=False)
    rows = []
    for line in r.stdout.splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    if not rows:
        raise RuntimeError(f"topology_probe printed nothing (rc={r.returncode}): {r.stderr.strip()[-300:]}")
    return rows


async def pin_and_preauth(ctx) -> list[Result]:
    """`world:pin_reachable` and `world:preauth_runnable` — from the jail, through hpc-bridge's own target builder."""
    alias = _setup(ctx.trace).get("ssh_host") or os.environ.get("HPC_BRIDGE_SSH_HOST", "")
    ps = pins(ctx.trace)
    rows = await asyncio.to_thread(_probe, alias, ps)
    base = next((x for x in rows if not x.get("pin")), {})
    pinned = [x for x in rows if x.get("pin")]
    bad = [x for x in pinned if x.get("reach_rc") != 0 or "HPCB_REACHED" not in str(x.get("reach_out"))]
    control = (f"the alias itself: rc={base.get('reach_rc')} {str(base.get('reach_out'))[-80:]!r}" if base
               else "the alias was not probed")
    if not pinned:
        reach = Result("world:pin_reachable", True, f"ok: no pin recorded (every op stayed on {alias}); {control}")
    elif bad:
        reach = Result("world:pin_reachable", False, "; ".join(
            f"pin {x['pin']} (ssh to {x.get('host')}, HostKeyAlias {x.get('host_key_alias')}) does not answer from "
            f"the client: rc={x.get('reach_rc')} {str(x.get('reach_out'))[-160:]!r}" for x in bad) + f" — {control}")
    else:
        reach = Result("world:pin_reachable", True, "ok: " + ", ".join(f"{x['pin']} answers" for x in pinned)
                       + f"; {control}")
    # every pre-auth command: the ones the flow handed out, then the ones hpc-bridge builds for the alias and each pin
    flow = [str(_r(c).get("preauth_command")) for c in ctx.trace.calls if _r(c).get("preauth_command")]
    checks = [(f"surfaced by {c}", c) for c in dict.fromkeys(flow)]
    failed = []
    for label, cmd in checks:
        rc, out = await asyncio.to_thread(topology.run_preauth, cmd)
        if rc != 0:
            failed.append(f"{label}: rc={rc} {out.strip()[-160:]!r}")
    for x in rows:
        if x.get("preauth_rc") != 0:
            failed.append(f"built for {x.get('pin') or 'the alias'} ({x.get('preauth_command')}): "
                          f"rc={x.get('preauth_rc')} {str(x.get('preauth_out'))[-160:]!r}")
    n = len(checks) + len(rows)
    pre = Result("world:preauth_runnable", not failed,
                 f"ok: {n} pre-auth command(s) run from the client ({len(checks)} surfaced by the flow)" if not failed
                 else "; ".join(failed)[:900])
    return [reach, pre]


async def no_manager_after_product_teardown(ctx) -> Result:
    """After hpc-bridge's OWN teardown — the harness' cleanup has not run yet — no process of this run's endpoint on any
    login node (the world channel reaches each one; harness/topology.py)."""
    name = ctx.endpoint_name
    if not name:
        return Result("world:no_manager_after_product_teardown", False, "no endpoint name for this run")
    try:
        hosts = list(json.loads(os.environ.get("HPCB_TARGET_CAPS") or "{}").get("login_hosts") or []) or [None]
    except ValueError:
        hosts = [None]
    cmd = leftover_manager_cmd(name, bc.endpoint_ids(ctx.trace), tries=3, pause_s=2)
    left, blind = [], []
    for h in hosts:
        rc, out = await asyncio.to_thread(ctx.ssh, cmd, 90, h)
        if rc != 0 or "HPCB_UNVERIFIABLE" in out:
            blind.append(f"{h or 'login'} (rc={rc}: {out.strip()[-100:]!r})")
        else:
            left += [f"[{h or 'login'}] {ln.strip()[:140]}" for ln in out.splitlines() if ln.strip()]
    ok = not left and not blind
    return Result("world:no_manager_after_product_teardown", ok,
                  f"ok: no process of {name} on {', '.join(h or 'login' for h in hosts)}" if ok else
                  "; ".join(([f"left running after teardown_endpoint: {' | '.join(left)}"] if left else [])
                            + ([f"UNVERIFIABLE on {', '.join(blind)}"] if blind else []))[:900])


EXTRA_INVARIANTS = [every_step_succeeded, no_internal_pin, no_reauth]
WORLD_CHECKS = [pin_and_preauth, no_manager_after_product_teardown]
EXPECT_OK = ["agent_engaged", "every_step_succeeded", "no_internal_pin", "no_reauth"]
