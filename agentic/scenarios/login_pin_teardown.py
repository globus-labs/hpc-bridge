"""ROUND-ROBIN LOGIN POOL: the endpoint lands on ONE login node behind a shared name; later control-plane SSH must
reach THAT node. Fake cluster, `site` profile (REQUIRES two login nodes).

The name `login` resolves to login01.hpcb.test and login02.hpcb.test (Docker DNS round-robins). The bootstrap
records the node the manager landed on (`hostname -f`, routable → the login-node PIN) and later ops rebind to it —
otherwise `gce stop`/`delete` hit whichever node the alias picks next, the endpoint DIRECTORY (shared /home) goes
but the manager PROCESS survives on the other node: an orphan the alias-only world check can't see. This is the
class behind this week's Expanse pin bugs (HostKeyAlias, the second code, the pin overwrite), reproduced locally.

World checks run on EACH login node: no endpoint process, and (shared home) no endpoint directory. EXTRA_ENV turns SSH
multiplexing OFF for the server (HPC_BRIDGE_SSH_CONTROL_PERSIST=0, config._control_settings): with the alias's
ControlMaster alive (ControlPersist 60 s) a teardown sent to the alias rides the master to the node it first reached, so
a pin regression passed here unseen. `pinned_to_a_node` (gated) reads the pin itself: the first-contact note must name
a node — login0N or an address — never the round-robin alias.
"""
import os
import re

from invariants import Result, Trace, first_contact_host, no_compute_ensure, no_ssh_workaround, texts_mention

PROMPT = (
    "Connect me to the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — and run "
    "`hostname -f` on the LOGIN shape so I can see which login node the endpoint runs on. Do NOT provision any "
    "billed compute block. Then tear the endpoint down completely (teardown_endpoint) and tell me exactly what "
    "was removed.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions: accept discovered facility config yourself."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "round-robin login pool: the endpoint is pinned to the node it landed on and teardown reaches that node"
TAGS = ["pin", "login-nodes", "teardown", "fake", "site"]
MAX_TURNS = 30
TARGETS = ("fake",)
REQUIRES = {"login_nodes": 2}
POSTCHECK_DELAY_S = 15

# No ControlMaster: every control-plane ssh resolves its host again, so one sent to the alias may land on the other node
EXTRA_ENV = {"HPC_BRIDGE_SSH_CONTROL_PERSIST": "0"}

_NAME = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or "hpc-bridge-fake"
_NODE = re.compile(r"^(?:login0\d\b|\d{1,3}(?:\.\d{1,3}){3}$)")   # login01[.hpcb.test], or the address reached

node_named = texts_mention("node_named", "login0", any_of=("login01", "login02"))  # the agent reported WHICH node


def pinned_to_a_node(t: Trace) -> Result:
    """The bootstrap pinned the node it landed on: the first-contact note names login0N or an address, never the
    round-robin alias the user gave (`login`), which later ops would resolve afresh."""
    fc = first_contact_host(t)
    if fc is None:
        return Result("pinned_to_a_node", False, "no first-contact note")
    i, host = fc
    ok = bool(_NODE.match(host))
    return Result("pinned_to_a_node", ok, f"ok: call {i} pinned {host}" if ok
                  else f"the first contact (call {i}) names {host!r}, not a node — the pin is the alias")


EXTRA_INVARIANTS = [no_ssh_workaround, no_compute_ensure, node_named, pinned_to_a_node]
EXPECT_OK = ["agent_engaged", "no_ssh_workaround", "no_compute_ensure", "node_named", "pinned_to_a_node",
             "spend_not_unprompted"]

POSTCHECKS = [
    # the manager process must be gone on BOTH login nodes — a pin that reached the wrong node leaves it on one
    {"name": "no_endpoint_process_on_any_login", "on": "each_login",
     "cmd": 'pgrep -u "$(whoami)" -f "globus-compute-[e]ndpoint" || true', "expect_empty": True},
    {"name": "endpoint_dir_gone", "cmd": f'ls -d "$HOME/.globus_compute/{_NAME}" 2>/dev/null || true', "expect_empty": True},
]
TEARDOWN = "delete"
