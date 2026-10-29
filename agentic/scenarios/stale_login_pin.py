"""A login node that stopped answering must not stay PINNED when the facility is reached through an ssh-config Host
alias — the Perlmutter report, item 1 (plugin 0.1.20, 2026-10-29). Fake cluster, `site` profile (two login nodes behind
the round-robin name `login`); scripted (no model). A RED proof: it fails on main, by design, until the fix.

On Perlmutter connect_facility failed `CANNOT REACH login22.chn.perlmutter.nersc.gov: … Operation timed out`, again and
again: ~/.hpc-bridge/endpoints.json pinned the endpoint to login22, which no longer accepted connections, while the
facility's own host answered. `_drop_dead_pin` exists for exactly this, but it probes the ALIAS with a raw TCP connect,
and the user's alias was an ssh-config `Host perlmutter` — no DNS name — so the probe read a client-side outage and kept
the dead pin (its notice "The remembered login-node pin was dropped…" never came).

    agentic/fakecluster/bin/up.sh --profile site
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=site ./agentic/run_smoke.sh stale_login_pin

The world: LOCAL_SETUP gives the jail's ~/.ssh/config a `Host hpcb-cluster` (HostName login), a name no resolver knows.
NODE_ACTIONS lets the script take one login node away (harness/node_actions.py: its port-22 sshd stops answering on the
network, the pool user's sessions there are killed; the host side gives it back when the cell ends).
The SCRIPT:
1. connects through the alias from the probe's proposal and polls until the login shape is up; reads the pin the
   plugin recorded (endpoints.json in the jail) — one of login01/login02.hpcb.test;
2. takes that node away as it went away on Perlmutter: stops this run's endpoint manager there (a clean
   `globus-compute-endpoint stop`, so the Globus service sees it offline and no connect reuses it), then the node's
   sshd (`sshd_off`); checks it no longer answers;
3. connects again, as the user did — `connect_facility(<id>)`, the session's config — and once more, polling a
   bring-up, each answer said;
4. gives the node back (`sshd_on`), whatever happened.
Graded: `dead_pin_dropped` (the first answer after the outage is the dead node's CANNOT REACH with "pin was dropped",
or the bring-up already reached the other node) and `bringup_reached_live_node` (a connect after the outage brought the
login shape up — through the alias, on the node that answers).
"""
from __future__ import annotations

import asyncio
import os

from byo_script import PROPOSED, UP, brief, login_pin, proposal_details, until_settled
from cluster_ops import stop_here_cmd, stop_outcome
from invariants import Result, Trace
from node_actions import NodeActions

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"login_nodes": 2, "mfa": "none"}
NODE_ACTIONS = True            # run_smoke.sh serves the script's sshd_off / sshd_on on the host
KIND = "regression"
SUMMARY = ("an endpoint pinned to a login node that stopped answering, reached through an ssh-config Host alias: the "
           "dead pin is dropped and the bring-up reaches the node that answers (Perlmutter report 1)")
TAGS = ["fake", "site", "login-pin", "ssh-config", "chaos", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False     # the login shape only
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` through the ssh-config alias "
          "`hpcb-cluster`, bring its login shape up, lose the login node it is pinned to, and connect again.")

ALIAS = "hpcb-cluster"         # the jail's ~/.ssh/config Host: HostName login (the round-robin name)
LOGIN_NODES = ("login01.hpcb.test", "login02.hpcb.test")   # `site`'s pool: what the pin must name
LOCAL_SETUP = [
    f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && printf 'Host {ALIAS}\\n  HostName login\\n' >> ~/.ssh/config "
    "&& chmod 600 ~/.ssh/config",
    f"ssh -G {ALIAS} | grep -qx 'hostname login'",          # ssh reads the alias…
    f"! getent hosts {ALIAS}",                               # …and no resolver knows it (the premise)
]
UP_WAIT_S = 480.0
AFTER_WAIT_S = 420.0
OFFLINE_WAIT_S = 20.0          # after the clean stop: the service has the endpoint offline
_REACHED = (UP, "provisioning")


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid = ops.facility
    r = await ops.call("connect_facility", facility=fid, ssh_host=ALIAS)
    if r.get("phase") != PROPOSED:
        ops.abort(f"the probe did not propose details through {ALIAS!r}: phase={r.get('phase')!r} {brief(r)!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=ALIAS, details=proposal_details(r))
    r = await until_settled(ops, r, {"facility": fid, "ssh_host": ALIAS}, wait_s=UP_WAIT_S)
    if r.get("phase") != UP:
        ops.abort(f"the login shape did not come up through {ALIAS!r}: phase={r.get('phase')!r} {brief(r)!r}")
    pin = login_pin(ALIAS)
    if pin not in LOGIN_NODES:
        ops.abort(f"the endpoint is not pinned to a login node of the pool ({pin!r}): the premise is unmet")
    ops.say(f"the endpoint is pinned to {pin}")
    name = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip()
    rc, out = await ops.world(stop_here_cmd(name), host=pin, timeout=120)
    if stop_outcome(out)[0] != "rc=0":
        ops.abort(f"could not stop the manager on {pin} (rc={rc}): {out.strip()[-300:]!r}")
    nodes = NodeActions.from_env()
    rc, out = await asyncio.to_thread(nodes.run, "sshd_off", pin)
    if rc != 0:
        ops.abort(f"could not take {pin} away (rc={rc}): {out.strip()[-300:]!r}")
    try:
        rc, out = await ops.world("true", host=pin, timeout=30)
        if rc == 0:
            ops.abort(f"{pin} still answers ssh after sshd_off")
        ops.say(f"{pin} is gone: the manager stopped, its sshd refuses ({out.strip()[-120:]!r})")
        await ops.sleep(OFFLINE_WAIT_S)
        first = await ops.call("connect_facility", facility=fid)
        if first.get("reused") or "zero-SSH" in str(first.get("notice") or ""):
            ops.abort(f"the stopped endpoint was reused as online: the premise is unmet ({brief(first)!r})")
        ops.say(f"after the outage: {first.get('phase')!r} {brief(first, 400)!r}; pin now {login_pin(ALIAS)!r}")
        last = first
        if first.get("phase") not in _REACHED:
            last = await ops.call("connect_facility", facility=fid)
            ops.say(f"again: {last.get('phase')!r} {brief(last, 400)!r}; pin now {login_pin(ALIAS)!r}")
        last = await until_settled(ops, last, {"facility": fid}, wait_s=AFTER_WAIT_S)
    finally:
        rc_on, out_on = await asyncio.to_thread(nodes.run, "sshd_on", pin)
        ops.say(f"{pin} given back (rc={rc_on}): {out_on.strip()[-120:]!r}")
    return (f"pinned to {pin}; after it went away the connects answered {first.get('phase')!r} then "
            f"{last.get('phase')!r} ({brief(last, 200)}); the pin is now {login_pin(ALIAS)!r}.")


def _after(t: Trace) -> list[dict]:
    """The answers to the connects AFTER the outage: the ones that name no ssh_host (the session's config, as the user
    called it) — every connect before it names the alias."""
    return [dict(c.result or {}) for _i, c in t.named("connect_facility") if "ssh_host" not in (c.input or {})]


def dead_pin_dropped(t: Trace) -> Result:
    """The first settled answer after the outage: CANNOT REACH the dead node WITH the "pin was dropped" notice — or a
    login shape already up (the bring-up went around the dead node)."""
    rs = [r for r in _after(t) if r.get("phase") != "provisioning"]
    if not rs:
        return Result("dead_pin_dropped", False, "premise not met: no settled connect after the outage")
    r = rs[0]
    notice = str(r.get("notice") or "")
    if r.get("phase") == UP:
        return Result("dead_pin_dropped", True, "ok: the bring-up reached a live login node at once")
    ok = r.get("phase") == "failed" and "pin was dropped" in notice
    return Result("dead_pin_dropped", ok, (f"ok: {notice[:200]!r}" if ok else
                  f"the dead pin was kept: {r.get('phase')!r} {notice[:400]!r}"))


def bringup_reached_live_node(t: Trace) -> Result:
    """A connect after the outage brought the login shape up (needs_account): through the alias, on the node that
    answers."""
    rs = _after(t)
    if not rs:
        return Result("bringup_reached_live_node", False, "premise not met: no connect after the outage")
    if any(r.get("phase") == UP for r in rs):
        return Result("bringup_reached_live_node", True, "ok: the login shape came up again")
    return Result("bringup_reached_live_node", False,
                  f"no connect after the outage came up; the last: {rs[-1].get('phase')!r} {brief(rs[-1], 300)!r}")


EXTRA_INVARIANTS = [dead_pin_dropped, bringup_reached_live_node]
EXPECT_OK = ["agent_engaged", "dead_pin_dropped", "bringup_reached_live_node"]
