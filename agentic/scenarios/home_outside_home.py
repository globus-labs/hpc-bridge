"""A facility whose homes are NOT under /home must still get its endpoint venv, under the user's real $HOME — the
Perlmutter report, item 4 (plugin 0.1.20, 2026-10-29). Fake cluster, `nersc` profile; scripted (no model). A RED proof:
it fails on main, by design, until the fix.

On Perlmutter the setup failed: "directory /home/<user>/hpc-bridge/gce-venv: Permission denied (os error 13)". Homes
there are /global/homes/<initial>/<user> and /home/<user> is not writable; the plugin's default venv path is
`/home/{user}/hpc-bridge/gce-venv` (facility/endpoint_venv.py DEFAULT_REMOTE_VENV), whatever $HOME the probe learned —
and the probe's proposed env_setup activates that venv (`{venv}`), so the proposal inherits the bad path. The `nersc`
profile puts every pool user's home at /global/homes/h/<user> (login AND compute nodes) and leaves /home root-owned.

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh home_outside_home

The SCRIPT is the plain bring-your-own connect: the probe's proposal, confirmed as proposed (env_setup left out, as the
tool asks), then the connect polled until the login shape is up or the bring-up fails. Graded: `login_shape_up` (a
connect answered needs_account) and `world:venv_under_home` (the world check reads, as the pool user, where the
endpoint venv landed: a working `globus-compute-endpoint` under $HOME/hpc-bridge/). `venv_failure_names_home`
(reported) says whether a failed bring-up named a /home/<user> path — the bug's own words.
"""
from __future__ import annotations

import re

from byo_script import PROPOSED, UP, brief, proposal_details, until_settled
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"home_root": "/global/homes"}
KIND = "regression"
SUMMARY = ("on a facility whose homes are under /global/homes, the confirmed proposal builds the endpoint venv under "
           "the user's $HOME and the login shape comes up (Perlmutter report 4)")
TAGS = ["fake", "nersc", "discovery", "venv", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False     # the login shape only
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` at login host `{ssh_host}` from the "
          "probe's proposal and bring its login shape up.")

UP_WAIT_S = 480.0              # the first venv build (uv + globus-compute-endpoint) and the login shape's canary


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != PROPOSED:
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {brief(r)!r}")
    ops.say(f"the probe proposed scratch_root {(r.get('proposed_details') or {}).get('scratch_root')!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=proposal_details(r))
    r = await until_settled(ops, r, {"facility": fid, "ssh_host": host}, wait_s=UP_WAIT_S)
    ops.say(f"the bring-up settled: phase={r.get('phase')!r} {brief(r, 400)!r}")
    return f"connect_facility answered {r.get('phase')!r}: {brief(r, 300)}"


def _connects(t: Trace) -> list[dict]:
    return [dict(c.result or {}) for _i, c in t.named("connect_facility")]


def login_shape_up(t: Trace) -> Result:
    """A connect answered needs_account: the venv was built, the endpoint started and the login shape's canary ran."""
    rs = _connects(t)
    if any(r.get("phase") == UP for r in rs):
        return Result("login_shape_up", True, "ok: the login shape came up (needs_account)")
    if not rs:
        return Result("login_shape_up", False, "premise not met: no connect_facility call")
    last = rs[-1]
    return Result("login_shape_up", False, f"the bring-up never came up: {last.get('phase')!r} {brief(last, 400)!r}")


_SLASH_HOME = re.compile(r"/home/hpcbridge-test[^\s:'\"]*")


def venv_failure_names_home(t: Trace) -> Result:
    """Reported: a failed bring-up's notice names a path under /home/<user> — the default venv path, not $HOME."""
    for r in _connects(t):
        if r.get("phase") == "failed":
            m = _SLASH_HOME.search(str(r.get("notice") or ""))
            return Result("venv_failure_names_home", m is None,
                          f"the failure names {m.group(0)!r}" if m else f"the failure names no /home path: {brief(r)!r}")
    return Result("venv_failure_names_home", True, "no failed bring-up")


VENV_CMD = ('echo "HPCB_HOME=$HOME"; for d in "$HOME"/hpc-bridge/gce-venv*; do '
            '[ -x "$d/bin/globus-compute-endpoint" ] && echo "HPCB_VENV=$d"; done; '
            '[ -e "/home/$(whoami)" ] && echo "HPCB_SLASH_HOME=present"; echo HPCB_DONE')


def read_venvs(out: str) -> tuple[str | None, list[str], bool]:
    """(the login node's $HOME, the endpoint venvs under it, whether a /home/<user> exists) — None when not reported."""
    home = next((ln.split("=", 1)[1].strip() for ln in out.splitlines() if ln.startswith("HPCB_HOME=")), None)
    venvs = [ln.split("=", 1)[1].strip() for ln in out.splitlines() if ln.startswith("HPCB_VENV=")]
    return (home if "HPCB_DONE" in out else None), venvs, "HPCB_SLASH_HOME=present" in out


async def venv_under_home(ctx) -> Result:
    """world: hpc-bridge's endpoint venv is under the pool user's real $HOME (/global/homes/…), with a working
    globus-compute-endpoint in it — read on the login node as the pool user, before the teardown."""
    name = "world:venv_under_home"
    rc, out = ctx.ssh(VENV_CMD, 60)
    home, venvs, slash_home = read_venvs(out)
    if home is None:
        return Result(name, False, f"UNVERIFIABLE: the login node did not report (ssh rc={rc}): {out.strip()[-200:]!r}")
    if venvs:
        return Result(name, True, f"ok: {venvs[0]} (HOME={home})")
    return Result(name, False, f"no endpoint venv under $HOME={home}/hpc-bridge/"
                  + ("; a /home/<user> exists" if slash_home else "; /home/<user> does not exist (not writable)"))


WORLD_CHECKS = [venv_under_home]
EXTRA_INVARIANTS = [login_shape_up, venv_failure_names_home]
EXPECT_OK = ["agent_engaged", "login_shape_up"]
