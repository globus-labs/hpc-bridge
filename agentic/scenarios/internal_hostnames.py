"""INTERNAL login hostnames — the `internal` profile (site + `hostname -f` = login0N.int.hpcb.test, a name resolvable only
inside the cluster). Midway's `*.rcc.local`, Aurora's `*.hostmgmt.cm.*`, and every site whose login pool answers to
public round-robin names but reports private ones.

The plugin pins later control-plane SSH to the node the manager landed on (the login-node PIN, so teardown reaches
THAT node and not whichever the alias picks next). Here the node's own name is useless to the client: pinning it breaks
teardown ("Could not resolve hostname"); falling back to the round-robin alias may hit the other node and orphan the
manager. The right pin is the ADDRESS the client actually reached (0.1.12: `$SSH_CONNECTION`'s server address, key
still verified against the alias the user trusted). World checks on EACH login node tell the truth either way — with
SSH multiplexing OFF (EXTRA_ENV: HPC_BRIDGE_SSH_CONTROL_PERSIST=0), since the alias's ControlMaster (ControlPersist 60 s)
carried a teardown sent to the alias to the node it first reached, masking a pin regression. `pinned_by_address` is
gated: the pin is what this profile tests.
"""
import os
import re

from invariants import (
    Result,
    Trace,
    ends_with_teardown,
    first_contact_host,
    login_shape_ran,
    no_compute_ensure,
    no_ssh_workaround,
    teardown_reported_clean,
)

TARGETS = ("fake",)
REQUIRES = {"hostnames": "internal", "login_nodes": 2}
NEEDS_COMPUTE_NODE = False
POSTCHECK_DELAY_S = 15

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
SUMMARY = "internal login hostnames: the pin survives a node name the client cannot resolve; teardown reaches the right node"
TAGS = ["pin", "hostnames", "internal", "teardown", "fake"]
MAX_TURNS = 30

# No ControlMaster: every control-plane ssh resolves its host again, so one sent to the alias may land on the other node
EXTRA_ENV = {"HPC_BRIDGE_SSH_CONTROL_PERSIST": "0"}

_NAME = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or "hpc-bridge-fake"
_IPV4 = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def internal_name_seen(t: Trace) -> Result:
    """The login shape reported the node's INTERNAL name (the world really is the internal-names one)."""
    for i, c in t.named("run_shell"):
        if str(c.input.get("shape")) == "login" and ".int.hpcb.test" in str((c.result or {}).get("stdout") or ""):
            return Result("internal_name_seen", True, f"ok: call {i} saw the internal hostname")
    return Result("internal_name_seen", False, "no login-shape run_shell output carried a *.int.hpcb.test name")


def pinned_by_address(t: Trace) -> Result:
    """GATED: the first-contact notice names the pinned node by ADDRESS (the internal name is not routable from the
    client, and the alias would be resolved afresh)."""
    fc = first_contact_host(t)
    if fc is None:
        return Result("pinned_by_address", False, "no first-contact notice")
    i, host = fc
    ok = bool(_IPV4.match(host))
    return Result("pinned_by_address", ok, f"ok: pinned to {host}" if ok
                  else f"first contact (call {i}) pinned by name, not address: {host!r}")


EXTRA_INVARIANTS = [internal_name_seen, pinned_by_address, no_ssh_workaround, no_compute_ensure,
                    login_shape_ran, ends_with_teardown, teardown_reported_clean]
EXPECT_OK = ["agent_engaged", "internal_name_seen", "pinned_by_address", "login_shape_ran", "no_ssh_workaround",
             "no_compute_ensure", "ends_with_teardown", "teardown_reported_clean", "spend_not_unprompted"]
POSTCHECKS = [
    {"name": "no_endpoint_process_on_any_login", "on": "each_login",
     "cmd": 'pgrep -u "$(whoami)" -f "globus-compute-[e]ndpoint" || true', "expect_empty": True},
    {"name": "endpoint_dir_gone", "cmd": f'ls -d "$HOME/.globus_compute/{_NAME}" 2>/dev/null || true', "expect_empty": True},
]
TEARDOWN = "delete"
