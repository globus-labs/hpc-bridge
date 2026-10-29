"""A step that fails on the login node must be reported by its OWN last line, not by the user's shell-startup noise —
the Perlmutter report, item 5b (plugin 0.1.20, 2026-10-29). Fake cluster, `nersc` profile; scripted (no model). A RED
proof: it fails on main, by design, until the fix.

On Perlmutter a remote step failed with `RuntimeError: remote list failed:` and ~600 characters of the user's .bashrc
output — Lmod's module swaps and a stale ~/.gitconfig.lock's git error — and the real error was cut off: every remote
step runs in a login shell (by design), so the startup files' stderr comes FIRST, and the failure notice keeps the
HEAD of the remote's stderr (configure/start/list, the probe, notices.py's 500-character cut). The `nersc` profile gives
every pool user that .bashrc (~700 characters on stderr, above Ubuntu's non-interactive guard).

    agentic/fakecluster/bin/up.sh --profile nersc
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=nersc ./agentic/run_smoke.sh noisy_failure_tail

The SCRIPT takes the probe's proposal and confirms it with ONE change: env_setup sources a site script that does not
exist (`. /global/common/software/hpcb/gce-env.sh`), so the first remote step that runs env_setup fails with bash's
own last line, "…/gce-env.sh: No such file or directory". Graded: `failure_quotes_last_line` (the failed connect's
notice quotes that line) and, reported, `failure_not_startup_noise` (the notice is not the .bashrc's words).
"""
from __future__ import annotations

from byo_script import PROPOSED, UP, brief, proposal_details, until_settled
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"shell_noise": "bashrc"}
KIND = "regression"
SUMMARY = ("a bring-up step that fails on a login node with a noisy .bashrc is reported by its own last line, not by "
           "the startup noise ahead of it (Perlmutter report 5b)")
TAGS = ["fake", "nersc", "notices", "stderr", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect the facility `{facility}` at login host `{ssh_host}` with the "
          "probe's proposal, but an env_setup that sources a missing site script; read the failure.")

SITE_SCRIPT = "/global/common/software/hpcb/gce-env.sh"
FAILING_ENV_SETUP = f". {SITE_SCRIPT}"
LAST_LINE = "gce-env.sh: No such file or directory"   # bash's own last words for a `.` of a missing file
NOISE = ("Lmod is automatically replacing", "could not lock config file", "MODULEPATH changes")
FAIL_WAIT_S = 300.0
# the details= connect (the 2nd connect_facility) fails ON PURPOSE: the #39 gate (first_details_connect_succeeds) judges
# the first bring-up after it (there is none), never this premise
STAGED_FAILED_CONNECTS = (1,)


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != PROPOSED:
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {brief(r)!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host,
                       details=proposal_details(r, env_setup=FAILING_ENV_SETUP))
    r = await until_settled(ops, r, {"facility": fid, "ssh_host": host}, wait_s=FAIL_WAIT_S)
    if r.get("phase") == UP:
        ops.abort("the bring-up came up although env_setup sources a missing script: the premise is unmet")
    ops.say(f"the bring-up answered {r.get('phase')!r}: {brief(r, 600)!r}")
    return f"connect_facility answered {r.get('phase')!r}: {brief(r, 300)}"


def _failed(t: Trace) -> dict | None:
    """The answer to the connect that brought the failing env_setup up (or a poll of it): the first `failed`."""
    for _i, c in t.named("connect_facility"):
        if (c.result or {}).get("phase") == "failed":
            return dict(c.result or {})
    return None


def failure_quotes_last_line(t: Trace) -> Result:
    """The failed connect's notice quotes the failing step's own last line (bash's "…: No such file or directory")."""
    r = _failed(t)
    if r is None:
        return Result("failure_quotes_last_line", False, "premise not met: no connect answered `failed`")
    notice = str(r.get("notice") or "")
    ok = LAST_LINE in notice
    return Result("failure_quotes_last_line", ok, (f"ok: the notice quotes {LAST_LINE!r}" if ok else
                  f"the notice does not quote the step's last line {LAST_LINE!r}; it reads {notice[:500]!r}"))


def failure_not_startup_noise(t: Trace) -> Result:
    """Reported: the notice carries none of the login's startup noise (Lmod's swaps, git's lock error)."""
    r = _failed(t)
    if r is None:
        return Result("failure_not_startup_noise", False, "premise not met: no connect answered `failed`")
    notice = str(r.get("notice") or "")
    hit = [n for n in NOISE if n in notice]
    return Result("failure_not_startup_noise", not hit, f"the notice quotes startup noise: {hit}" if hit else "ok")


EXTRA_INVARIANTS = [failure_quotes_last_line, failure_not_startup_noise]
EXPECT_OK = ["agent_engaged", "failure_quotes_last_line"]
