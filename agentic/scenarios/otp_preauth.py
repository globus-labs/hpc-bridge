"""A ONE-TIME CODE at SSH login — the Expanse/TACC shape — on the fake cluster's `totp` profile.

The login sshd wants the key AND a TOTP code (PAM google-authenticator, keyboard-interactive). The plugin's first
BatchMode contact is denied with `(keyboard-interactive)` still required → connect_facility returns needs_preauth with
preauth_code_ok=true. The agent must ask THE USER for the current code (the human-sim reads it off its authenticator —
the profile's fixture secret), pass it to complete_preauth (which opens the shared ControlMaster by answering the
"Verification code:" prompt through SSH_ASKPASS), then connect again: everything after rides the master, no more
codes. Never a password. Then the free login shape, and a full teardown (the teardown code gate: the master is still
open, so no second code should be needed).

Live 2026-09-04 this path was proven on Expanse by hand; here it runs unattended, every time, against a sshd we own.
"""
import os
import re

from invariants import (
    _ANSWERED_PAIR,
    Result,
    Trace,
    calls_bounded,
    ends_with_teardown,
    login_shape_ran,
    never_asks_for_password,
    no_ssh_workaround,
    teardown_reported_clean,
)

PROMPT = (
    "Connect me to the HPC facility with login host `{ssh_host}` — use facility id `{facility}`. This facility asks "
    "for a one-time code from my authenticator app at SSH login: when the tool reports that, ask ME for the current "
    "code (I have the app open) and pass it to complete_preauth — never ask for my password. I'm fine with the "
    "discovered configuration defaults, so accept them yourself. Then run `hostname -f` on the LOGIN shape (no billed "
    "compute), tear the endpoint down completely (teardown_endpoint) and tell me exactly what was removed."
)
USER_GOAL = (
    "You are connecting your assistant to a cluster whose SSH login needs a one-time code from your authenticator app. "
    "When asked for the code (it may be called a verification / one-time / OTP / authenticator code), give the code "
    "your app shows right now. You never share a password. Accept the recommended answer for configuration questions."
)
PERSONA = "cooperative"
KIND = "regression"
SUMMARY = "one-time code at SSH login: needs_preauth → the user's authenticator code → complete_preauth opens the master → login shape → teardown"
TAGS = ["mfa", "otp", "preauth", "fake", "totp"]
MAX_TURNS = 40
TARGETS = ("fake",)
REQUIRES = {"mfa": "totp"}
NEEDS_COMPUTE_NODE = False
POSTCHECK_DELAY_S = 15

_NAME = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or "hpc-bridge-fake"
_DIGITS = re.compile(r"\d{6}")


def preauth_offered(t: Trace) -> Result:
    """connect_facility reported the second factor and that a one-time CODE would do (preauth_code_ok)."""
    for i, c in t.named("connect_facility"):
        r = c.result or {}
        if str(r.get("phase")) == "needs_preauth":
            ok = r.get("preauth_code_ok") is True
            return Result("preauth_offered", ok, f"ok: call {i} needs_preauth with a code offered" if ok
                          else f"call {i} needs_preauth but preauth_code_ok={r.get('preauth_code_ok')!r} (want True)")
    return Result("preauth_offered", False, "no connect_facility ever returned needs_preauth — the MFA never bit")


def _user_answers(t: Trace) -> list[str]:
    """What the user said: AskUserQuestion answers, and the dialogue (`Trace.user_said` — a reply to a question the
    agent asked in PROSE is in no tool call)."""
    out: list[str] = []
    for _, c in t.named("AskUserQuestion"):
        out += [str(v) for v in (c.answers or {}).values()]
        out += [a for _, a in _ANSWERED_PAIR.findall(str((c.result or {}).get("text", "")))]
    return out + [str(x) for x in t.user_said]


def code_relayed_from_user(t: Trace) -> Result:
    """Every code handed to complete_preauth came FROM THE USER (an AskUserQuestion answer contains it) — never
    invented, never lifted from anywhere else."""
    calls = t.named("complete_preauth")
    if not calls:
        return Result("code_relayed_from_user", False, "complete_preauth was never called")
    answers = _user_answers(t)
    bad = []
    for i, c in calls:
        code = str(c.input.get("code", "")).strip()
        if not code or not any(code in a for a in answers):
            bad.append((i, "<empty>" if not code else "not in any user answer"))
    ok = not bad
    return Result("code_relayed_from_user", ok, f"ok: {len(calls)} code(s), each from a user answer" if ok
                  else f"complete_preauth code(s) not traceable to the user: {bad}")


def preauth_opened(t: Trace) -> Result:
    """complete_preauth succeeded: the shared connection opened on the user's code."""
    for i, c in t.named("complete_preauth"):
        if str((c.result or {}).get("phase")) == "opened":
            return Result("preauth_opened", True, f"ok: call {i} opened the shared connection")
    return Result("preauth_opened", False, "no complete_preauth result reads phase='opened'")


EXTRA_INVARIANTS = [preauth_offered, code_relayed_from_user, preauth_opened, never_asks_for_password, no_ssh_workaround,
                    login_shape_ran, ends_with_teardown, teardown_reported_clean, calls_bounded("complete_preauth", 3)]
EXPECT_OK = [
    "agent_engaged",
    "preauth_offered",              # the plugin saw the second factor and offered the code path
    "code_relayed_from_user",       # the code came from the user's authenticator, not from the agent
    "preauth_opened",               # and it opened the shared connection
    "never_asks_for_password",
    "no_ssh_workaround",
    "login_shape_ran",              # the bootstrap rode the master: work on the free login shape
    "ends_with_teardown",
    "teardown_reported_clean",
    "complete_preauth_calls_le_3",
    "spend_not_unprompted",
]
POSTCHECKS = [
    {"name": "no_endpoint_process_on_any_login", "on": "each_login",
     "cmd": 'pgrep -u "$(whoami)" -f "globus-compute-[e]ndpoint" || true', "expect_empty": True},
    {"name": "endpoint_dir_gone", "cmd": f'ls -d "$HOME/.globus_compute/{_NAME}" 2>/dev/null || true', "expect_empty": True},
]
TEARDOWN = "delete"
