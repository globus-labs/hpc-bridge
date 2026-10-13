"""INTER-agent reuse across an MCP-server restart (issue #20, the cross-restart half).

The intra-agent scenario ([[endpoint_reuse]]) proves reuse WITHIN one session — the same server
process connects twice. This proves the stronger, real-world case: a **fresh** hpc-bridge server
(a new agent session, a cold process) reattaches to an endpoint a *previous* session stood up —
zero SSH, over the Globus web service. That's the "SSH-once across restarts" keystone.

Mechanism (a harness CHAIN — see run.py `_run_chain`): two PHASES, each a SEPARATE agent session
(hence a fresh MCP server = the "restart"), sharing this run's facility id + pool user with NO
teardown between. Phase 1 stands the endpoint up FRESH; phase 2's cold server must find it online
and reattach. Both phases run on the same container/cluster, so the endpoint persists between them;
the harness tears down once, after phase 2.

STATUS: GREEN with #20's `reused` signal live — the spec is that phase 1 reports `reused=False`
(genuine fresh bootstrap) and phase 2 reports `reused=True` (reattached). Graded per phase via
`ToolCall.phase` (see `reuse_across_restart`). Phase 1 is judged loosely: a retry after a failed first connect
already reads `reused=True` (#39, fixed in #72; `first_details_connect_succeeds` now gates the details= bring-up).

The LOCAL-DISCOVERY CACHE half (merged from `facility_cache`): the same chain
also proves phase 2's cold server resolved the facility's CONFIG from `~/.hpc-bridge/facilities.json` (both phases
share the container) WITHOUT re-probing over SSH — `cache_served_reconnect`. The two graders are not nested (they
disagree on some stored chain bundles), so both gate. Phase 1 discovers the un-catalogued
facility (a `proposed_facility_details` probe) → confirms → proves the login shape (since #78 a config is cached only
once the login shape ran work on it) → phase 2 reconnects with no probe.
Login-shape only, no billed block: ~6 min for the pair.
"""
from invariants import _UP_PHASES, Result, Trace

# Each phase is its own agent session (fresh server). Both phases connect to the SAME ssh_host, so the
# server computes the same endpoint name (hpc-bridge-<ssh_host> since #27) and phase 2 reattaches to
# phase 1's still-online manager. Sharing {facility} keeps them one session facility across the restart.
PHASES = [
    (
        "Connect me to the HPC facility with login host `{ssh_host}` — use facility "
        "id `{facility}` — and run `hostname` on the LOGIN shape to confirm it's up. The first bring-up installs "
        "a toolchain on the login node and can take several minutes: keep calling connect_facility while it "
        "reports `provisioning` (it is not stuck) until the login node is up, THEN run `hostname`. Do NOT "
        "provision any billed compute block, and do NOT tear anything down.\n\n"
        "This is an AUTOMATED run — no one is available to answer questions: accept discovered "
        "facility config yourself."
    ),
    (
        "Reconnect me to the SAME HPC facility — facility id `{facility}`, login host "
        "`{ssh_host}` — and tell me explicitly whether the existing endpoint was "
        "REUSED or a brand-new one was set up. Do NOT provision a billed block and do NOT tear "
        "anything down.\n\n"
        "This is an AUTOMATED run — no one is available to answer questions: accept discovered "
        "facility config yourself."
    ),
]

USER_GOAL = ""       # autonomous
PERSONA = None
KIND = "regression"
SUMMARY = ("inter-agent reuse: a fresh server (new session) reattaches to an endpoint a prior session stood up — "
           "cross-restart, zero-SSH, the config served from the local cache with no re-probe")
TAGS = ["reuse", "zero-ssh", "inter-agent", "chain", "cache", "local-discovery"]
INTERPHASE_DELAY_S = 30  # let phase 1's endpoint register 'online' before phase 2 tries to reattach


def _phase_of(c) -> str:
    return str((c.result or {}).get("phase"))


def reuse_across_restart(t: Trace) -> Result:
    """The chain spec, keyed on `ToolCall.phase` (run.py `_combine` stamps each session's calls;
    phase 0 = the bring-up session, phase 1 = the restarted one):

    - phase 1 BUILT the endpoint: some phase-1 connect is `reused=False` (a genuine fresh bring-up)
      and some phase-1 connect reached the endpoint (an `_UP_PHASES` result);
    - phase 2 REATTACHED CLEANLY: its FIRST connect that reaches the endpoint reads `reused=True`
      (the FIELD — a "reuse" substring in the notice is not evidence), with NO non-up connect
      (`failed` / `proposed_facility_details` / `needs_facility_details` …) before it within phase
      2 — a cold server that re-probes or fails a connect before finding the endpoint did not reattach.

    Phase 1 is judged loosely on purpose: when its first `connect(details=…)` fails, the retry already
    reads `reused=True` (the #39 shape, fixed in #72), so "first up-connect was fresh" would be a false
    negative. `reused=False` on the probe/failed connect is sufficient proof the endpoint was built by
    this chain (per-run unique facility ids + TEARDOWN=delete rule out a leftover). Phase 2 gets the
    strict form: the reattach must be the first thing that works."""
    if t.n_phases < 2:
        return Result("reuse_across_restart", False,
                      f"needs a two-phase chain trace; saw {t.n_phases} phase(s)")
    p1 = t.named("connect_facility", phase=0)
    built_fresh = any(not bool((c.result or {}).get("reused")) for _, c in p1)
    p1_up = any(_phase_of(c) in _UP_PHASES for _, c in p1)
    p2 = t.named("connect_facility", phase=1)
    p2_ups = [(i, c) for i, c in p2 if _phase_of(c) in _UP_PHASES]
    if not p2_ups:
        return Result("reuse_across_restart", False,
                      f"phase 2 never reached the endpoint ({len(p2)} connect(s), none up-phase)")
    first_i, first = p2_ups[0]
    reattached = bool((first.result or {}).get("reused"))
    dirty = [(i, _phase_of(c)) for i, c in p2 if i < first_i]   # non-up connects before the reattach
    ok = built_fresh and p1_up and reattached and not dirty
    detail = ("ok: phase 1 built fresh, phase 2's first connect reattached cleanly" if ok else
              f"built_fresh={built_fresh} p1_up={p1_up} (want True); phase-2 first up-connect "
              f"(call {first_i}) reused={reattached} (want True); pre-reattach connects in phase 2: "
              f"{dirty} (want none)")
    return Result("reuse_across_restart", ok, detail)


# connect_facility phases that mean the cache did NOT serve the config: the server either probed the
# login node again (proposed) or asked for details outright (needs_facility_details).
_DISCOVERY_PHASES = {"proposed_facility_details", "needs_facility_details"}


def cache_served_reconnect(t: Trace) -> Result:
    """The cache spec, keyed on `ToolCall.phase` (phase 0 = the discovering session, phase 1 = the
    restarted one): phase 1 DISCOVERS the BYO facility (a `proposed_facility_details` probe in phase
    0), and phase 2's fresh server RECONNECTS from the local cache — its FIRST connect that reaches
    the endpoint reads `reused=True` (the field) and NO phase-2 connect is a discovery phase. A
    probe / details request in phase 2 means a cache MISS (it re-discovered instead of reading
    `facilities.json`). Keyed per phase: a flat-trace form was satisfied by phase 1's own retry."""
    if t.n_phases < 2:
        return Result("cache_served_reconnect", False,
                      f"needs a two-phase chain trace; saw {t.n_phases} phase(s)")
    p1 = t.named("connect_facility", phase=0)
    discovered = any(str((c.result or {}).get("phase")) == "proposed_facility_details" for _, c in p1)
    p2 = t.named("connect_facility", phase=1)
    p2_ups = [(i, c) for i, c in p2 if str((c.result or {}).get("phase")) in _UP_PHASES]
    if not discovered or not p2_ups:
        return Result("cache_served_reconnect", False,
                      f"needs a phase-1 discovery then a phase-2 reconnect "
                      f"(discovered={discovered}, phase-2 up-connects={len(p2_ups)})")
    # Since #78 a BYO config is cached only once PROVEN (the login shape reached warm in the discovering session).
    # A phase 1 that ended while still `provisioning` never wrote the cache, so a phase-2 re-probe is the agent's
    # impatience in phase 1, not a cache miss — say which (first block-tier run after #78, 2026-09-05).
    p1_proved = any(str(c.input.get("shape")) == "login" and str((c.result or {}).get("phase")) == "complete"
                    for _, c in t.named("run_shell", phase=0))
    if not p1_proved:
        return Result("cache_served_reconnect", False,
                      "phase 1 never ran work on a warm login shape, so the config was never PROVEN and (since #78) "
                      "never cached — phase 2 had nothing to reconnect from (agent impatience in phase 1, not a cache miss)")
    reattached = bool((p2_ups[0][1].result or {}).get("reused"))
    reprobed = [i for i, c in p2 if str((c.result or {}).get("phase")) in _DISCOVERY_PHASES]
    ok = reattached and not reprobed
    detail = ("ok: phase-2 reconnect served from the local cache (no re-probe), endpoint reused" if ok else
              f"phase-2 first up-connect reused={reattached} (want True), "
              f"reprobed_in_phase2={bool(reprobed)} (want False; calls {reprobed})")
    return Result("cache_served_reconnect", ok, detail)


EXTRA_INVARIANTS = [reuse_across_restart, cache_served_reconnect]

EXPECT_OK = [
    "reuse_across_restart",          # the spec: fresh bootstrap, then reattach across the restart
    "cache_served_reconnect",        # …with the config from the local cache: no re-probe (the merged facility_cache)
    "no_raw_ssh_after_endpoint_up",  # the reattach rides the web service, not a fresh SSH
    "spend_not_unprompted",
    "ends_with_stop",                # login-only: trivially satisfied (no billed block)
]

TEARDOWN = "delete"  # same-container chain: reclaim the shared endpoint once, after phase 2
