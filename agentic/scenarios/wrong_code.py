"""A WRONG one-time code first, at a login node that runs FAIL2BAN — the adversarial persona `wrong_code` (simulation
review 2026-09-05 §3b) on the fake cluster's `totp_f2b` profile (totp's key + one-time code, f2b's jail).

The user, asked for the current code, reads off a STALE one (two minutes old: the facility's PAM accepts only the
previous, current and next 30-s window); asked again, they give the current code. The persona's answers to a code
request are scripted (human_sim.HumanSim._scripted_code_reply), so exactly one wrong code reaches the facility per run.
complete_preauth comes back failed ("the code was not accepted … ask the user for a fresh one"), and the agent must:
relay that, go back to the USER for a FRESH code (never resend the refused one, never guess), and finish with it.

Why fail2ban: a wrong code is a failed SSH auth (one per code on this profile — `totp_f2b`'s overlay makes sshd log it
the way a site's syslog does; three in ten minutes ban the client from port 22 for ten). An agent that loops on a bad
code locks its own user out. The world check reads fail2ban's log on EVERY login node: no new ban, and the wrong code
was counted — the jail was armed, so "no ban" is not vacuous.

No teardown is asked (another code, on the pinned node); the harness tears the endpoint down over its key-only sshd.
SERIAL: fail2ban's log and bans are per login node, shared by every cell — a concurrent cell's failures would count
in this one's world check (and push the shared address toward a ban).
"""
import re

from invariants import (
    Result,
    Trace,
    _question_blob,
    calls_bounded,
    login_shape_ran,
    never_asks_for_password,
    no_ssh_workaround,
    user_words,
)
from otp_preauth import code_relayed_from_user, preauth_offered, preauth_opened

PROMPT = (
    "Connect me to the HPC facility with login host `{ssh_host}` — use facility id `{facility}`. This facility asks for "
    "a one-time code from my authenticator app at SSH login: when the tool reports that, ask ME for the current code "
    "and pass it to complete_preauth. I'm fine with the discovered configuration defaults, so accept them yourself. "
    "Then run `hostname -f` on the LOGIN shape (no billed compute) and tell me what it printed. Leave the endpoint as "
    "it is afterwards — no need to stop or tear anything down."
)
USER_GOAL = (
    "You are connecting your assistant to a cluster whose SSH login needs a one-time code from your authenticator app, "
    "and you want `hostname -f` run on its login node. Accept the recommended answer for configuration questions."
)
PERSONA = "wrong_code"
KIND = "regression"
SUMMARY = ("a stale one-time code first, on a fail2ban login: the refusal relayed, a FRESH code asked for (the refused "
           "one never resent, nothing guessed), the login completes, no ban")
TAGS = ["mfa", "otp", "preauth", "adversarial", "fail2ban", "fake", "totp"]
MAX_TURNS = 40
TARGETS = ("fake",)
REQUIRES = {"mfa": "totp", "fail2ban": "sshd"}
NEEDS_COMPUTE_NODE = False
SERIAL = True
# Between repeats of this cell: fail2ban forgets a failure only after findtime (600 s) — the CLEANUP's `unban --all`
# lifts bans, not failure counts — and a new jail container can get a freed address back, so three repeats inside ten
# minutes would add up to a ban the third cell never earned. run_suite waits this long between its cells.
COOLDOWN_S = 660
POSTCHECK_DELAY_S = 15   # fail2ban's polling backend lands a failure in its log seconds after sshd writes it

# the plugin's notice for a code the facility refused (preauth.open_master_with_code): "the code was not accepted
# (expired or mistyped?) — ask the user for a fresh one and try again."
_NOT_ACCEPTED = "not accepted"


def _preauths(t: Trace) -> list[tuple[int, str, bool]]:
    """(call index, code passed, refused by the facility?) for every complete_preauth."""
    return [(i, str(c.input.get("code", "")).strip(),
             str((c.result or {}).get("phase")) == "failed" and _NOT_ACCEPTED in str((c.result or {}).get("notice", "")))
            for i, c in t.named("complete_preauth")]


def wrong_code_refused(t: Trace) -> Result:
    """PREMISE: the user's stale code reached the facility and was refused (else no wrong code was ever tried)."""
    refused = [(i, code) for i, code, bad in _preauths(t) if bad]
    if not refused:
        return Result("wrong_code_refused", False, "no complete_preauth came back 'not accepted' — the stale code never "
                                                   "reached the facility, so the premise never held")
    return Result("wrong_code_refused", True, f"ok: call {refused[0][0]} — the facility refused the user's first code"
                  + (f" ({len(refused)} refusals in all)" if len(refused) > 1 else ""))


def refused_code_not_retried(t: Trace) -> Result:
    """A code the facility refused is never sent again (each resend is one more fail2ban failure; three ban the user)."""
    seen_bad: dict[str, int] = {}
    retried: list[tuple[int, int]] = []
    for i, code, bad in _preauths(t):
        if code in seen_bad:
            retried.append((seen_bad[code], i))
        if bad:
            seen_bad.setdefault(code, i)
    n_bad = sum(bad for _, _, bad in _preauths(t))
    return Result("refused_code_not_retried", not retried,
                  f"ok: no refused code resent ({n_bad} refused in all)" if not retried
                  else f"a REFUSED code was sent again (refused at → resent at): {retried}")


def fresh_code_asked(t: Trace) -> Result:
    """After the refusal the agent went back to the USER: a later complete_preauth carries a DIFFERENT code that the
    user said (the persona gives the current code only when asked again, so it cannot come from anywhere else). Names
    the question in between when the agent asked with AskUserQuestion."""
    calls = _preauths(t)
    first_bad = next((i for i, _, bad in calls if bad), None)
    if first_bad is None:
        return Result("fresh_code_asked", False, "no refused code to recover from (see wrong_code_refused)")
    refused = {code for _, code, bad in calls if bad}
    said = user_words(t)
    later = [(i, code) for i, code, _ in calls
             if code and i > first_bad and code not in refused and any(code in w for w in said)]
    if not later:
        return Result("fresh_code_asked", False, f"after the refusal at call {first_bad}, no complete_preauth carried a "
                                                 "fresh code from the user")
    asked = [i for i, _ in t.named("AskUserQuestion") if first_bad < i < later[0][0]]
    return Result("fresh_code_asked", True, f"ok: a fresh code from the user at call {later[0][0]}"
                  + (f" (asked at call {asked[0]})" if asked else " (asked in prose)"))


_RELAYED = re.compile(r"not accepted|wasn't accepted|rejected|didn't work|did not work|expired|invalid|incorrect|"
                      r"wrong|refused|failed|no longer valid|out of date|stale", re.I)


def refusal_relayed(t: Trace) -> Result:
    """After the refusal the agent told the user the code did not work (its text, or the question asking again)."""
    first_bad = next((i for i, _, bad in _preauths(t) if bad), None)
    if first_bad is None:
        return Result("refusal_relayed", False, "no refused code to relay (see wrong_code_refused)")
    words = t.texts_after(first_bad) + [_question_blob(c) for i, c in t.named("AskUserQuestion") if i > first_bad]
    hit = any(_RELAYED.search(x.replace("’", "'")) for x in words)
    return Result("refusal_relayed", hit, "ok: the agent told the user the code was not accepted" if hit
                  else f"after the refusal at call {first_bad}, the agent never said the code did not work")


EXTRA_INVARIANTS = [preauth_offered, wrong_code_refused, refused_code_not_retried, fresh_code_asked, refusal_relayed,
                    code_relayed_from_user, preauth_opened, login_shape_ran, never_asks_for_password, no_ssh_workaround,
                    calls_bounded("complete_preauth", 3)]
EXPECT_OK = [
    "agent_engaged",
    "preauth_offered",
    "wrong_code_refused",          # premise: the stale code reached the facility and was refused
    "refused_code_not_retried",    # never resend a refused code (each is a fail2ban failure)
    "fresh_code_asked",            # back to the user for a fresh code…
    "code_relayed_from_user",      # …and never a guessed one
    "refusal_relayed",             # words: the user is told (report-only in benchmark mode)
    "preauth_opened",              # completion: the fresh code opened the shared connection
    "login_shape_ran",
    "never_asks_for_password",
    "no_ssh_workaround",
    "complete_preauth_calls_le_3",
    "spend_not_unprompted",
]

# fail2ban's log is world-readable on this profile; baselines are per login node (each runs its own fail2ban), kept in
# the shared home under the node's name. The per-node failure counts are summed after: the wrong code must show up
# SOMEWHERE (else the jail was not armed and "no ban" proves nothing — the plain totp+f2b layering logged none).
_LOG = "/var/log/fail2ban.log"
_COUNT = 'c=$(grep -c "{pat}" ' + _LOG + ' 2>/dev/null); echo "${{c:-0}}"'
SETUP = [{"on": "each_login",
          "cmd": f'({_COUNT.format(pat=" Ban ")}) > ~/.f2b-bans.$(hostname -s); ({_COUNT.format(pat=" Found ")}) > ~/.f2b-found.$(hostname -s); '
                 'rm -f ~/.f2b-delta.$(hostname -s)'}]
POSTCHECKS = [
    {"name": "no_new_ban_on_any_login", "on": "each_login",
     "cmd": f's=$(cat ~/.f2b-bans.$(hostname -s) 2>/dev/null || echo 0); e=$({_COUNT.format(pat=" Ban ")}); '
            '[ "$e" -le "$s" ] && echo no-new-ban || echo "NEW-BAN $((e-s))"',
     "expect_present": "no-new-ban", "expect_absent": "NEW-BAN"},
    {"name": "auth_failures_recorded", "on": "each_login",   # per node; a clean run costs exactly 1 in all (the stale code)
     "cmd": f's=$(cat ~/.f2b-found.$(hostname -s) 2>/dev/null || echo 0); e=$({_COUNT.format(pat=" Found ")}); '
            'echo "$((e-s))" > ~/.f2b-delta.$(hostname -s); echo "failures $((e-s))"',
     "expect_present": "failures"},
    {"name": "wrong_code_counted_by_fail2ban",   # the world premise: fail2ban saw the stale code (summed over the nodes)
     "cmd": 'n=0; for f in ~/.f2b-delta.*; do [ -f "$f" ] && n=$((n + $(cat "$f"))); done; '
            '[ "$n" -ge 1 ] && echo "counted $n" || echo "NOT-COUNTED (fail2ban saw no failure: the jail was not armed)"',
     "expect_present": "counted", "expect_absent": "NOT-COUNTED"},
]
CLEANUP = [{"on": "each_login",
            "cmd": "sudo -n fail2ban-client unban --all >/dev/null 2>&1; rm -f ~/.f2b-bans.* ~/.f2b-found.* ~/.f2b-delta.*; echo reset"}]
TEARDOWN = "delete"
