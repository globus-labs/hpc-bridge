"""A facility proven in an earlier session whose login now wants a PASSWORD + ONE-TIME CODE must hand the login to the
user's own terminal (needs_preauth), not read as NO SSH ACCESS — the Perlmutter report, item 2 (plugin 0.1.20,
2026-10-29). Fake cluster, `nersc` profile; scripted (no model). A RED proof: it fails on main, by design, until the fix.

The reporter had connected Perlmutter before (its config PROVEN, so cached in ~/.hpc-bridge/facilities.json); weeks
later their key was no longer accepted (NERSC: a key OR a password + one-time code). The cached facility goes straight
to the bring-up, whose only handoff trigger is `key_accepted_second_factor_pending` — written for key + code sites
(Expanse, #87): methods offered that include keyboard-interactive and NOT publickey. NERSC's denial still lists
publickey — "Permission denied (publickey,password,keyboard-interactive)" — so it read NO SSH ACCESS. A first contact
(the discovery probe, `is_interactive_auth_failure`) hands off on the same denial; so the cached path is the bug.

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh cached_password_otp

The world: SEED_FACILITY_CACHE writes the proven record for the login host into the jail's facilities.json before the
session (as an earlier session would have), and SETUP revokes the pool user's key on port 22 — ~/.ssh/authorized_keys
moved aside, the way an expired key stops working (the harness' own sshd on :2200 reads a root-owned key file, so the
world channel is untouched; CLEANUP puts it back). The SCRIPT connects the cached facility (graded:
`cached_facility_handoff` — needs_preauth, the terminal handoff with its `ssh -fN …` command and no code prompt), then,
as the control, a first contact under a new facility id through another of the pool's host names (no record for it:
the probe runs) — `first_contact_handoff`, which passes on main.
"""
from __future__ import annotations

from byo_script import brief
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"mfa": "password_otp"}
KIND = "regression"
SUMMARY = ("a facility proven before whose login now wants a password + one-time code hands off to the user's terminal "
           "(needs_preauth), not NO SSH ACCESS (Perlmutter report 2)")
TAGS = ["fake", "nersc", "mfa", "preauth", "cache", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` at `{ssh_host}`, which this machine "
          "connected before; the login now asks for a password and a one-time code.")

CACHED_HOST = "login"                  # the run's ssh host on the fake target (HPC_BRIDGE_SSH_HOST)
FRESH_HOST = "login01.hpcb.test"       # another name for the same login pool: no cached record, so the probe runs
SEED_FACILITY_CACHE = {
    CACHED_HOST: {
        "ssh_host": CACHED_HOST, "interface": "eth0", "env_setup": 'export PATH="$HOME/.local/bin:$PATH"',
        "scratch_root": "/global/homes/h/{user}/.hpc-bridge", "partition": "system", "scheduler": "slurm",
    },
}
_REVOKED = "~/.ssh/authorized_keys.hpcb-revoked"
SETUP = [f"if [ -e ~/.ssh/authorized_keys ]; then mv ~/.ssh/authorized_keys {_REVOKED}; fi; "
         "[ ! -e ~/.ssh/authorized_keys ]"]
CLEANUP = [f"if [ -e {_REVOKED} ]; then mv {_REVOKED} ~/.ssh/authorized_keys; fi; [ -e ~/.ssh/authorized_keys ]"]


def _said(r: dict) -> str:
    """An answer in the script's own words: its phase and handoff fields, never its notice — a handoff's notice talks
    of passwords, and the script's words are graded as an agent's prose (no_password_handling)."""
    return (f"{r.get('phase')!r} (preauth_code_ok={r.get('preauth_code_ok')!r}, "
            f"terminal command={'-fN' in str(r.get('preauth_command') or '')})")


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    cached = await ops.call("connect_facility", facility=ops.facility, ssh_host=CACHED_HOST)
    ops.say(f"the cached facility answered {_said(cached)}")
    fresh = await ops.call("connect_facility", facility=f"{ops.facility}-first", ssh_host=FRESH_HOST)
    ops.say(f"a first contact answered {_said(fresh)}")
    return f"cached: {_said(cached)}; first contact: {_said(fresh)}"


def _answer(t: Trace, host: str) -> dict | None:
    for _i, c in t.named("connect_facility"):
        if (c.input or {}).get("ssh_host") == host:
            return dict(c.result or {})
    return None


def _handoff(name: str, r: dict | None) -> Result:
    if r is None:
        return Result(name, False, "premise not met: no such connect")
    cmd = str(r.get("preauth_command") or "")
    ok = r.get("phase") == "needs_preauth" and r.get("preauth_code_ok") is False and "-fN" in cmd
    return Result(name, ok, (f"ok: needs_preauth, the terminal handoff ({cmd[:120]!r})" if ok else
                             f"{r.get('phase')!r} (preauth_code_ok={r.get('preauth_code_ok')!r}): {brief(r, 400)!r}"))


def cached_facility_handoff(t: Trace) -> Result:
    """The cached (proven) facility, denied `(publickey,password,keyboard-interactive)`, answers needs_preauth: the
    `ssh -fN …` the user runs in their own terminal (password + code typed there, never in the session)."""
    return _handoff("cached_facility_handoff", _answer(t, CACHED_HOST))


def first_contact_handoff(t: Trace) -> Result:
    """The control: a first contact (the probe) on the same denial hands off already (passes on main)."""
    return _handoff("first_contact_handoff", _answer(t, FRESH_HOST))


EXTRA_INVARIANTS = [cached_facility_handoff, first_contact_handoff]
EXPECT_OK = ["agent_engaged", "cached_facility_handoff", "first_contact_handoff"]
