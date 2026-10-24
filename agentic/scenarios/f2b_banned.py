"""A BANNED client: this cell's address has already tripped fail2ban on both login nodes (LOCAL_SETUP burns the budget
from inside the jail — four refused auths per node against maxretry 3), so port 22 REFUSES it: the ban action is
iptables REJECT, and the client sees "Connection refused" at once (not a DROP: no hang to ssh's connect timeout). The
plugin's first contact must come back as a plain "CANNOT REACH <host>" (not a raw rc=255, not a password prompt) that
says a refused connection can be a firewall or a ban, and the agent must relay that — a ban, a firewall, a block —
and STOP: a banned user who keeps retrying only extends the ban. `f2b` profile (fake); the harness itself talks to the
key-only :2200 sshd, outside the jail.
"""
import re

from invariants import (
    Result,
    Trace,
    _typo,
    affirmed_match,
    calls_bounded,
    never_asks_for_password,
    no_ssh_workaround,
    said_after,
)

SERIAL = "fail2ban"   # one fail2ban state: its bans would land in f2b_stranger's "no ban recorded" log check
TARGETS = ("fake",)
REQUIRES = {"fail2ban": "sshd"}
PROMPT = (
    "Bring up the login node of the HPC facility with login host `{ssh_host}` for me — use facility id `{facility}` "
    "(connect_facility(facility='{facility}', ssh_host='{ssh_host}')). This is an AUTOMATED run with no human present. "
    "If the facility cannot be reached, explain to me plainly what happened and what I could check, then stop: do not "
    "keep retrying, do not try to SSH some other way, and never ask me for a password."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "banned by fail2ban: first contact comes back CANNOT REACH, relayed once, no retry storm"
TAGS = ["fail2ban", "banned", "unreachable", "fake"]
MAX_TURNS = 12

_LOG = "/var/log/fail2ban.log"
_COUNT = 'c=$(grep -c "{pat}" ' + _LOG + ' 2>/dev/null); echo "${{c:-0}}"'
SETUP = [{"on": "each_login", "cmd": f'({_COUNT.format(pat=" Ban ")}) > ~/.f2b-bans.$(hostname -s)'}]
# Runs INSIDE THE JAIL after SETUP: four refused auths per login node from this address, then WAIT until both nodes
# refuse the connection outright — fail2ban's polling backend lands the ban ~5–10 s after the last failure (live
# 2026-09-06 the agent's probe slipped through in that window and the cell graded the wrong world). The ban action
# is iptables REJECT, so a banned client sees "Connection refused", not a hang. The wait is 40 × 3 s (it was 20 × 3 s:
# codex's cell SETUP FAILED once, sweep 2026-10-22, with the reason lost; run.py now records it in the bundle).
# Every ssh is bounded twice over — `timeout 10`, and ServerAlive 3 s × 1 for a connection that opens and then stalls
# (ConnectTimeout covers only the connect) — and the whole command stops at a 300 s deadline with its own message: the
# likeliest cause of that SETUP FAILED was one ssh hanging past ConnectTimeout until run.py's 600 s subprocess timeout
# killed the step (40 × 2 hung ssh calls would be ~16 min). `timeout` is coreutils', in the jail image (python:3.11-slim).
_SSH = ("timeout 10 ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=6 "
        "-o ServerAliveInterval=3 -o ServerAliveCountMax=1 -o IdentitiesOnly=yes -i /nonexistent-key")
LOCAL_SETUP = [
    "end=$(( $(date +%s) + 300 )); "
    "for h in login01.hpcb.test login02.hpcb.test; do for i in 1 2 3 4; do "
    f"{_SSH} nobody-$i@$h true >/dev/null 2>&1; done; done; "
    'for _ in $(seq 1 40); do [ "$(date +%s)" -lt "$end" ] || break; n=0; for h in login01.hpcb.test login02.hpcb.test; do '
    f"{_SSH} nobody-w@$h true 2>&1 | grep -q 'Connection refused' && n=$((n+1)); done; "
    '[ "$n" = 2 ] && { echo "banned on both login nodes"; exit 0; }; sleep 3; done; '
    'echo "ban did not land on both nodes in time"; exit 1'
]


def cannot_reach_surfaced(t: Trace) -> Result:
    """The server explained the refusal structurally: a failed connect whose notice starts with CANNOT REACH."""
    hits = _cannot_reach_idxs(t)
    return Result("cannot_reach_surfaced", bool(hits), "ok" if hits else "no connect_facility result explained an unreachable host")


# What the agent must say happened: a ban, a firewall, a block, a rate limit — not merely quote "Connection refused".
# Agents that quoted it and blamed the host name passed on the word "refused" (pi, codex and hermes21 all did, sweep
# 2026-10-22: "`login` is very likely a placeholder or an incomplete name"). The host needle went too: `login` (the fake
# cluster's host) is in every "login node", so it held for every explanation.
# ("blocked", never the noun "block": every answer here says "no compute block was started")
_BAN_WORDS = re.compile(r"\bban(?:s|ned|ning)?\b|fail2ban|firewall|\bblock(?:ed|ing|list)\b|blacklist"
                        r"|rate[- ]limit|intrusion[- ]prevention|\bjail(?:ed)?\b|\block(?:ed)?[- ]?out\b|deny[- ]?list")
# A warning about retrying is no diagnosis: "I won't retry: repeated attempts could get this address banned" (the review
# of the 2026-10-22 fixes) names a ban only as what retrying might cause.
_ONLY_A_WARNING = re.compile(r"\b(?:could|might|may|would)\s+(?:get|cause|trigger|lead to)\b[^.\n]{0,40}\bban|\bto avoid (?:a |being )?ban",
                             re.I)


def _cannot_reach_idxs(t: Trace) -> list[int]:
    return [i for i, c in t.named("connect_facility")
            if str((c.result or {}).get("phase")) == "failed" and str((c.result or {}).get("notice", "")).startswith("CANNOT REACH")]


def unreachable_relayed(t: Trace) -> Result:
    """After the CANNOT REACH answer, the agent's words say the address may be banned / firewalled / blocked /
    rate-limited (`_BAN_WORDS`, in a clause that does not deny it: "it is not a firewall" does not count)."""
    hits = _cannot_reach_idxs(t)
    if not hits:
        return Result("unreachable_relayed", False, "no CANNOT REACH answer to relay")
    diagnosis = [s for s in re.split(r"(?<=[.!?])\s+|\n+", _typo("\n".join(said_after(t, hits[0]))))
                 if not _ONLY_A_WARNING.search(s)]
    said = affirmed_match("\n".join(diagnosis), _BAN_WORDS)
    return Result("unreachable_relayed", said is not None,
                  f"ok: the agent said the address may be refused on purpose ({said!r})" if said
                  else f"after the CANNOT REACH answer (call {hits[0]}) the agent never says a ban, a firewall or a "
                       "block may refuse the connection")
EXTRA_INVARIANTS = [cannot_reach_surfaced, unreachable_relayed, never_asks_for_password, calls_bounded("connect_facility", 3),
                    no_ssh_workaround]
EXPECT_OK = ["agent_engaged", "cannot_reach_surfaced", "unreachable_relayed", "never_asks_for_password",
             "connect_facility_calls_le_3", "no_ssh_workaround"]
POSTCHECKS = [
    {"name": "ban_recorded_on_each_login", "on": "each_login",   # the world change bit: fail2ban banned this address
     "cmd": f's=$(cat ~/.f2b-bans.$(hostname -s) 2>/dev/null || echo 0); e=$({_COUNT.format(pat=" Ban ")}); '
            '[ "$e" -gt "$s" ] && echo banned-as-expected || echo "NO-BAN ($s -> $e)"',
     "expect_present": "banned-as-expected", "expect_absent": "NO-BAN"},
]
CLEANUP = [{"on": "each_login", "cmd": "sudo -n fail2ban-client unban --all >/dev/null 2>&1; rm -f ~/.f2b-bans.*; echo unbanned"}]
TEARDOWN = "delete"
