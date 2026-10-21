"""A SLOW teardown (#91) on the fake cluster: `tearing_down`, the call again, and a teardown that removes only what this
run created. BYO bring-up on the login shape only — no block, no node. Autonomous: the free operators drive it. ~2 min.

`teardown_endpoint` runs the login-node ops (gce stop + delete over SSH) in a server-side task and waits a bounded time
for them (60 s); past that the call answers `tearing_down` ("call teardown_endpoint again … do NOT call run_shell or
connect_facility meanwhile") and a later call reports the finished teardown. Expanse's ops take ~3 min (live
2026-09-04); the fake cluster's whole teardown takes ~5 s, so the path never showed live. The test seam
HPC_BRIDGE_TEARDOWN_WAIT_S=1 (EXTRA_ENV, the server's only — config.teardown_wait_s) makes those 5 s outlive the wait:
the first teardown_endpoint answers `tearing_down`, and the agent, following the notice, calls it again — nothing else
meanwhile — until it answers `down`. The prompt only asks for the endpoint to be fully gone; it does not mention
`tearing_down`.

What `ours` guards (server._finish_teardown): the finished teardown clears the session's state only while that state is
still the endpoint it tore down — a binding made meanwhile is not its to clear. Live, the call again shows that branch:
the `down` it gets is the finished teardown's own report for the SAME endpoint, replayed only once the state was cleared
(`_replayable_teardown`) — not a second teardown, nor "no endpoint was up". Its other branch (a connect re-binding while
the ops run) cannot be reached through the tools since 0.1.18 — connect_facility refuses while a teardown runs — and a
connect sent during the wait here would race the ~4 s left of the ops, so it is not prompted (tests/test_server.py
covers it). The token store follows the same rule (`_seeded_by_us`): the harness clears the pool user's store before the
cell, so the plugin seeds one, and the teardown removes it (`token_store_gone`). What the run did NOT create must stay:
SETUP plants a sibling `uep.<another uuid>.*` dir and a file beside them in ~/.globus_compute (invisible to
`globus-compute-endpoint list`, which reads only dirs holding a config.*), and the world check finds both afterwards.
"""
import os

from invariants import (
    HPC_BRIDGE_TOOL_NAMES,
    Result,
    Trace,
    acted_on_nothing,
    ends_with_teardown,
    first_contact_noted,
    login_shape_ran,
    no_false_reuse_claim,
    no_ssh_workaround,
    teardown_reported_clean,
)

PROMPT = (
    "Connect me to a NEW HPC cluster that is not in the registry: facility id `{facility}`, SSH login host "
    "`{ssh_host}`. This is an AUTOMATED run — there is NO human to answer questions, so accept the "
    "configuration the probe proposes yourself (do not ask me to confirm it). Do everything through the "
    "hpc-bridge tools: never run ssh or scp yourself and do not use login_shell. Once the login node is up, run "
    "`hostname` on the LOGIN shape (run_shell with shape=\"login\") — do NOT start a compute block. Then tear the "
    "endpoint down completely with teardown_endpoint, make sure the teardown has finished, and report exactly what "
    "EACH teardown_endpoint call said."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("a slow teardown (#91): `tearing_down`, the agent calls teardown_endpoint again (nothing else) until `down` "
           "— the finished teardown's own report for the same endpoint; only what the run created is removed")
TAGS = ["fake", "teardown", "issue-91", "login", "free-operators"]
MAX_TURNS = 40
TARGETS = ("fake",)
REQUIRES = {"mfa": "none"}          # an autonomous BYO bring-up: no one-time code to give (not the totp profile)
NEEDS_COMPUTE_NODE = False

# The test seam (config.teardown_wait_s): one teardown call waits 1 s for the login-node ops, not 60 s.
EXTRA_ENV = {"HPC_BRIDGE_TEARDOWN_WAIT_S": "1"}

# The tools that act on the facility; reading the registry (list_facilities) during the wait acts on nothing.
_ACTING = HPC_BRIDGE_TOOL_NAMES - {"teardown_endpoint", "list_facilities"}


def _res(c) -> dict:
    return c.result or {}


def _first_tearing_down(t: Trace) -> tuple[int, dict] | None:
    for i, c in t.named("teardown_endpoint"):
        if str(_res(c).get("status")) == "tearing_down":
            return i, _res(c)
    return None


def _down_after(t: Trace, k: int) -> tuple[int, dict] | None:
    for i, c in t.named("teardown_endpoint"):
        if i > k and str(_res(c).get("status")) == "down":
            return i, _res(c)
    return None


def teardown_handed_back(t: Trace) -> Result:
    """The server-task path showed: the FIRST teardown_endpoint that found an endpoint answered `tearing_down`, naming
    the endpoint, with a notice to call teardown_endpoint again. The premise is checked, never assumed: a first such
    teardown that answered `down` (or anything else) outright means the seam did not reach the server or the ops beat a
    1 s wait — the path was not exercised, and the detail says so. Skipped: a teardown sent before anything was
    connected ("no endpoint was up", invariants.acted_on_nothing) — hermes21 opened with one in a blind parallel batch,
    and its `down` failed this premise while the real teardown answered `tearing_down` then `down` (live 2026-10-15)."""
    every = t.named("teardown_endpoint")
    if not every:
        return Result("teardown_handed_back", False, "no teardown_endpoint call")
    tds = [(i, c) for i, c in every if not acted_on_nothing(t, i)]
    skipped = [i for i, _ in every if acted_on_nothing(t, i)]
    if not tds:
        return Result("teardown_handed_back", False,
                      f"every teardown_endpoint (calls {skipped}) answered 'no endpoint was up' — none found an "
                      "endpoint to tear down")
    i, c = tds[0]
    r = _res(c)
    if str(r.get("status")) != "tearing_down":
        return Result("teardown_handed_back", False,
                      f"premise not met: the first teardown_endpoint on an endpoint (call {i}) answered "
                      f"{r.get('status')!r}, not "
                      "'tearing_down' — HPC_BRIDGE_TEARDOWN_WAIT_S=1 did not reach the server, or the login-node ops "
                      "beat the 1 s wait; the tearing_down path was not exercised")
    why = []
    if not r.get("endpoint_id"):
        why.append("it names no endpoint_id")
    if "teardown_endpoint again" not in str(r.get("notice") or "").lower():
        why.append(f"its notice does not say to call teardown_endpoint again: {str(r.get('notice') or '')[:120]!r}")
    skipped = [k for k in skipped if k < i]
    before = f" (call(s) {skipped} before it answered 'no endpoint was up': nothing to tear down)" if skipped else ""
    return Result("teardown_handed_back", not why,
                  f"ok: the first teardown (call {i}) answered tearing_down for {r.get('endpoint_id')}{before}" if not why
                  else "; ".join(why))


def teardown_awaited(t: Trace) -> Result:
    """After `tearing_down`, the agent called teardown_endpoint again — and no other hpc-bridge tool — until one
    answered `down`. The tool's doc says "call nothing else meanwhile", so a harmless call (poll_task, authenticate,
    complete_preauth) counts too; only reading the registry (list_facilities) is exempt. The notice names the two that
    do harm ("Do NOT call run_shell or connect_facility meanwhile": a run_shell would provision a new endpoint). Waiting
    in its own shell in between is fine."""
    first = _first_tearing_down(t)
    if first is None:
        return Result("teardown_awaited", False, "no teardown_endpoint answered tearing_down")
    k = first[0]
    down = _down_after(t, k)
    end = down[0] if down is not None else len(t.calls)
    acted = [(i, c.name) for i, c in enumerate(t.calls) if k < i < end and c.name in _ACTING]
    again = [i for i, _ in t.named("teardown_endpoint") if k < i <= end]
    why = []
    if acted:
        why.append(f"called {acted} while the teardown ran (after tearing_down at call {k})")
    if down is None:
        why.append(f"no teardown_endpoint answered 'down' after the tearing_down at call {k}"
                   + (f" ({len(again)} call(s) again)" if again else " — it was never called again"))
    return Result("teardown_awaited", not why,
                  f"ok: tearing_down at call {k}; called again {len(again)} time(s), nothing else; down at call {end}"
                  if not why else "; ".join(why))


def teardown_down_is_ours(t: Trace) -> Result:
    """The `down` that ended the wait is THIS teardown's own report — the `ours` branch held: the same endpoint the
    `tearing_down` named, and the finished teardown's account ("fully torn down", no DELETE FAILED, the token copy
    hpc-bridge placed removed) — a stop the login node did not confirm in time but whose delete went through ("manager
    deleted (its stop was not confirmed in time, …)") is a finished teardown too. Not "no endpoint was up" (a state
    cleared under a call that then started nothing), not a second teardown's. Read on THAT `down`, not the last
    teardown result: a call after it answers "no endpoint was up", which is fine."""
    first = _first_tearing_down(t)
    if first is None:
        return Result("teardown_down_is_ours", False, "no teardown_endpoint answered tearing_down")
    k, waiting = first
    down = _down_after(t, k)
    if down is None:
        return Result("teardown_down_is_ours", False, f"no teardown_endpoint answered 'down' after call {k}")
    j, r = down
    notice = str(r.get("notice") or "")
    why = []
    if r.get("endpoint_id") != waiting.get("endpoint_id"):
        why.append(f"the down names endpoint {r.get('endpoint_id')!r}, the tearing_down {waiting.get('endpoint_id')!r}")
    if "fully torn down" not in notice:
        why.append(f"not the finished teardown's report: {notice[:140]!r}")
    if "DELETE FAILED" in notice:
        why.append("the endpoint's delete failed: its directory and registration remain")
    if "token copy hpc-bridge placed on the login node removed" not in notice:
        why.append("the token store hpc-bridge seeded was not reported removed")
    return Result("teardown_down_is_ours", not why,
                  f"ok: the down at call {j} is the finished teardown of {r.get('endpoint_id')} (tearing_down at "
                  f"call {k})" if not why else "; ".join(why))


EXTRA_INVARIANTS = [first_contact_noted, no_false_reuse_claim, login_shape_ran, teardown_handed_back, teardown_awaited,
                    teardown_down_is_ours, ends_with_teardown, teardown_reported_clean, no_ssh_workaround]

EXPECT_OK = [
    "agent_engaged",
    "login_shape_ran",
    "teardown_handed_back",        # the premise: the first teardown answered tearing_down (the seam reached the server)
    "teardown_awaited",            # ...the agent called it again, nothing else, until down
    "teardown_down_is_ours",       # ...and that down is the finished teardown's own report for the same endpoint
    "ends_with_teardown",
    "no_false_reuse_claim",
    "no_ssh_workaround",
    "spend_not_unprompted",
]
# teardown_reported_clean reads the LAST teardown result — a call after the `down` answers "no endpoint was up", which
# is harmless here — so it is reported; teardown_down_is_ours checks the same words on the `down` that ended the wait.

# The decoys: what this run did NOT create, beside what it did. Another endpoint's per-user dir (a different uuid —
# the plugin's `rm -rf uep.<its eid>.*` must not reach it) and a plain file. Neither holds a config.*, so
# `globus-compute-endpoint list` never sees them.
_DECOY_UEP = "uep.00000000-0000-4000-8000-0000000000d0.not-ours"
_DECOY_FILE = "hpcb-not-ours.txt"
SETUP = [f"mkdir -p ~/.globus_compute/{_DECOY_UEP} && echo hpcb-not-ours > ~/.globus_compute/{_DECOY_UEP}/marker "
         f"&& echo hpcb-not-ours > ~/.globus_compute/{_DECOY_FILE} && echo planted"]

# World checks run as the pool user AFTER the agent and BEFORE the harness teardown: what the agent's teardown left.
_NAME = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or "hpc-bridge-login"
POSTCHECKS = [
    {"name": "endpoint_dir_gone", "cmd": f"ls -d ~/.globus_compute/{_NAME} 2>/dev/null; true", "expect_empty": True},
    {"name": "token_store_gone", "cmd": "ls ~/.globus_compute/storage.db 2>/dev/null; true", "expect_empty": True},
    # a HARM check: `expect_absent` names the harm, so a removed decoy is never read as "nothing ran" (infra.absences)
    {"name": "not_ours_kept",
     "cmd": (f"test -f ~/.globus_compute/{_DECOY_UEP}/marker && test -f ~/.globus_compute/{_DECOY_FILE} "
             "&& echo NOT_OURS_KEPT || echo NOT_OURS_REMOVED"),
     "expect_present": "NOT_OURS_KEPT", "expect_absent": "NOT_OURS_REMOVED"},
]
CLEANUP = [f"rm -rf ~/.globus_compute/{_DECOY_UEP} ~/.globus_compute/{_DECOY_FILE}; echo cleaned"]
TEARDOWN = "delete"  # the harness' own cleanup afterwards is then a no-op — the agent already did it
