"""EDGE: the tools before any facility is connected (#177). Fake cluster only; no compute, nothing billed.

Every tool that needs a facility answers with ONE notice and starts nothing: `run_shell` is `failed` and
`ensure_endpoint_up` is `down`, both with "No facility connected". After a connect that only PROPOSED a config (the
probe's draft, not confirmed) the notice names that pending proposal. Why: before #177 an unpinned server stood up a
local `hpc-bridge-dev` endpoint on the MCP host for a `run_shell` made before any connect.

The run: run_shell and ensure_endpoint_up(confirm_spend=True) with nothing connected, then a proposal-only connect, then
run_shell again. Autonomous, so the free CLI operators drive it. Graded from the results, and from the world:

- `nothing_stood_up` (trace): no result before the connect carries an endpoint_id.
- world checks: no directory for this run's endpoint in the pool user's `~/.globus_compute` on the cluster, and no
  `hpc-bridge-*` endpoint directory in the jail's own Globus Compute dirs (where #177's local endpoint appeared).

The world is read once, after the run. Mid-run hooks (the only way to look at the world BETWEEN two calls) would keep
the free operators from driving the cell, so "before the connect" rests on the results. The end state implies it
anyway: nothing in this run is allowed to stand an endpoint up, before the connect or after it.

TEARDOWN is harmless: nothing is bound, so it finds no endpoint uuid, cancels nothing and deletes nothing that exists.
"""
import os
import re

from invariants import _UP_PHASES, Result, Trace

PROMPT = (
    "I'm testing how hpc-bridge behaves before a facility is connected. Use the hpc-bridge tools exactly as follows.\n\n"
    "1. Before anything else, call `run_shell(command='hostname', shape='login')`.\n"
    "2. Then call `ensure_endpoint_up(confirm_spend=True)`.\n"
    "   Report exactly what each of those two returned (phase or status, and the notice). Do NOT connect yet.\n"
    "3. Then call `connect_facility(facility='{facility}', ssh_host='{ssh_host}')`. It will propose a configuration: do "
    "NOT confirm it — do not call connect_facility again, and never pass details=.\n"
    "4. Call `run_shell(command='hostname', shape='login')` again and report exactly what it returned.\n\n"
    "Do nothing else. This is an AUTOMATED run — no one is available to answer questions."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "no facility connected: run_shell fails and ensure_endpoint_up is down with one notice; a proposal-only connect is named; nothing stood up"
TAGS = ["fake", "no-facility", "issue-177", "cheap", "login"]
MAX_TURNS = 16
TARGETS = ("fake",)
REQUIRES = {"mfa": "none"}   # a one-time-code login answers needs_preauth, not a proposal (that notice is another variant)
NEEDS_COMPUTE_NODE = False

_NO_FACILITY = "No facility connected"


def _first_connect(t: Trace) -> int:
    i = t.first_index("connect_facility")
    return len(t.calls) if i is None else i


def no_facility_shell(t: Trace) -> Result:
    """The FIRST run_shell, made before any connect, is `failed` with the no-facility notice."""
    shells = t.named("run_shell")
    if not shells:
        return Result("no_facility_shell", False, "no run_shell call")
    i, c = shells[0]
    r = c.result or {}
    if i > _first_connect(t):
        return Result("no_facility_shell", False, f"the first run_shell (call {i}) came after the connect")
    ok = str(r.get("phase")) == "failed" and _NO_FACILITY in str(r.get("notice", ""))
    return Result("no_facility_shell", ok, f"ok: run_shell (call {i}) failed with {_NO_FACILITY!r}" if ok
                  else f"run_shell (call {i}): phase {r.get('phase')!r}, notice {str(r.get('notice', ''))[:120]!r}")


def no_facility_ensure(t: Trace) -> Result:
    """The first ensure_endpoint_up, made before any connect, is `down` with the same notice — the spend
    acknowledgement it carried (confirm_spend=True) started nothing."""
    ups = [(i, c) for i, c in t.named("ensure_endpoint_up") if i < _first_connect(t)]
    if not ups:
        return Result("no_facility_ensure", False, "no ensure_endpoint_up before the connect")
    i, c = ups[0]
    r = c.result or {}
    ok = str(r.get("status")) == "down" and _NO_FACILITY in str(r.get("notice", ""))
    return Result("no_facility_ensure", ok, f"ok: ensure_endpoint_up (call {i}) down with {_NO_FACILITY!r}" if ok
                  else f"ensure_endpoint_up (call {i}): status {r.get('status')!r}, notice {str(r.get('notice', ''))[:120]!r}")


def proposed_variant(t: Trace) -> Result:
    """After the connect that only PROPOSED a config, run_shell is `failed` and its notice names that pending proposal
    (notices.py `_no_facility_notice`). Keyed on stable tokens — the `proposed_facility_details` phase name, or the
    proposed facility's id — with the prose ("only PROPOSED a config") only as a fallback, so a rewording of the
    sentence does not fail the cell."""
    props = [(i, c) for i, c in t.named("connect_facility")
             if str((c.result or {}).get("phase")) == "proposed_facility_details"]
    if not props:
        phases = [str((c.result or {}).get("phase")) for _, c in t.named("connect_facility")]
        return Result("proposed_variant", False, f"no connect_facility proposed a config (phases {phases})")
    pi, pc = props[0]
    fid = str(pc.input.get("facility") or "")
    later = [(i, c) for i, c in t.named("run_shell") if i > pi]
    if not later:
        return Result("proposed_variant", False, f"no run_shell after the proposal (call {pi})")
    i, c = later[0]
    r = c.result or {}
    notice = str(r.get("notice", ""))
    signals = [name for name, hit in (("proposed_facility_details", "proposed_facility_details" in notice),
                                      (f"the facility id {fid!r}", bool(fid) and fid in notice),
                                      ("the prose", "only proposed a config" in notice.lower())) if hit]
    ok = str(r.get("phase")) == "failed" and bool(signals)
    return Result("proposed_variant", ok, f"ok: run_shell (call {i}) names the pending proposal ({', '.join(signals)})"
                  if ok else f"run_shell (call {i}): phase {r.get('phase')!r}, notice {notice[:160]!r}")


def nothing_stood_up(t: Trace) -> Result:
    """No result before the connect carries an endpoint_id: nothing was stood up for a call made with no facility.
    (The world checks below cover the end state; see the module docstring for why the timing rests on the results.)"""
    before = t.calls[:_first_connect(t)]
    tools = [c for c in before if c.name in ("run_shell", "ensure_endpoint_up", "reset_session", "login_shell")]
    if not tools:
        return Result("nothing_stood_up", False, "no hpc-bridge call before the connect to judge")
    with_id = [(k, c.name) for k, c in enumerate(before) if (c.result or {}).get("endpoint_id")]
    return Result("nothing_stood_up", not with_id,
                  f"ok: {len(tools)} call(s) before the connect, none with an endpoint_id" if not with_id
                  else f"endpoint_id in results before the connect: {with_id}")


def proposal_not_confirmed(t: Trace) -> Result:
    """The agent left the proposal unconfirmed, as asked: no connect_facility with details=, none reached a bind."""
    bad = [i for i, c in t.named("connect_facility")
           if c.input.get("details") or str((c.result or {}).get("phase")) in _UP_PHASES]
    return Result("proposal_not_confirmed", not bad, "ok: the proposal was left unconfirmed" if not bad
                  else f"connect_facility confirmed or bound at calls {bad}")


EXTRA_INVARIANTS = [no_facility_shell, no_facility_ensure, proposed_variant, nothing_stood_up, proposal_not_confirmed]
EXPECT_OK = ["agent_engaged", "no_facility_shell", "no_facility_ensure", "proposed_variant", "nothing_stood_up",
             "proposal_not_confirmed", "ends_with_stop"]   # NOT spend_not_unprompted: step 2's confirm_spend precedes the connect by design

# This run's endpoint name: run_smoke.sh sets it for the server and the harness alike. Without it the server names the
# endpoint `hpc-bridge-<ssh_host>` (run.py `_endpoint_name` mirrors that), which is the fallback here.
_NAME = (os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip()
         or "hpc-bridge-" + (re.sub(r"[^a-z0-9]+", "-", os.environ.get("HPC_BRIDGE_SSH_HOST", "login").lower()).strip("-")
                             or "session"))
POSTCHECKS = [
    {"name": "nothing_stood_up_on_cluster", "cmd": f"ls -d ~/.globus_compute/{_NAME} 2>/dev/null; true", "expect_empty": True},
    # the jail's own endpoint dirs (the MCP host): where #177's implicit local `hpc-bridge-dev` endpoint was configured
    {"name": "nothing_stood_up_in_jail", "on": "local", "expect_empty": True,
     "cmd": 'for d in "${HPC_BRIDGE_USER_DIR:-$HOME/.globus_compute}" "$HOME/.globus_compute"; do '
            'ls -d "$d"/hpc-bridge-* 2>/dev/null; done; true'},
]
TEARDOWN = "delete"   # harmless: nothing bound — no uuid is found, nothing is cancelled, the delete finds no endpoint
