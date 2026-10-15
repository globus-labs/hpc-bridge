"""A BANNED client: this cell's address has already tripped fail2ban on both login nodes (LOCAL_SETUP burns the budget
from inside the jail — four refused auths per node against maxretry 3), so port 22 silently DROPS its packets. The
plugin's first contact then hangs until ssh's connect timeout and must come back as a plain "CANNOT REACH <host>"
(not a raw rc=255, not a password prompt), and the agent must relay it and STOP — a banned user who keeps retrying
only extends the ban. `f2b` profile (fake); the harness itself talks to the key-only :2200 sshd, outside the jail.
"""
import os

from invariants import Result, Trace, calls_bounded, never_asks_for_password, no_ssh_workaround, texts_mention

SERIAL = "fail2ban"   # one fail2ban state: its bans would land in f2b_stranger's "no ban recorded" log check
TARGETS = ("fake",)
REQUIRES = {"fail2ban": "sshd"}
_HOST = os.environ.get("HPC_BRIDGE_SSH_HOST", "login")

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
# is iptables REJECT, so a banned client sees "Connection refused", not a hang.
_SSH = ("ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=6 "
        "-o IdentitiesOnly=yes -i /nonexistent-key")
LOCAL_SETUP = [
    "for h in login01.hpcb.test login02.hpcb.test; do for i in 1 2 3 4; do "
    f"{_SSH} nobody-$i@$h true >/dev/null 2>&1; done; done; "
    "for _ in $(seq 1 20); do n=0; for h in login01.hpcb.test login02.hpcb.test; do "
    f"{_SSH} nobody-w@$h true 2>&1 | grep -q 'Connection refused' && n=$((n+1)); done; "
    '[ "$n" = 2 ] && { echo "banned on both login nodes"; exit 0; }; sleep 3; done; '
    'echo "ban did not land on both nodes in time"; exit 1'
]


def cannot_reach_surfaced(t: Trace) -> Result:
    """The server explained the silence structurally: a failed connect whose notice starts with CANNOT REACH."""
    hits = [i for i, c in t.named("connect_facility")
            if str((c.result or {}).get("phase")) == "failed" and str((c.result or {}).get("notice", "")).startswith("CANNOT REACH")]
    return Result("cannot_reach_surfaced", bool(hits), "ok" if hits else "no connect_facility result explained an unreachable host")


unreachable_relayed = texts_mention("unreachable_relayed", _HOST,
                                    any_of=("cannot reach", "unreachable", "not reachable", "timed out", "blocked", "banned",
                                            "firewall", "fail2ban", "no route", "refused"))
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
