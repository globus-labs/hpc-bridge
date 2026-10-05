"""The facility multi-user-endpoint (MEP) path: a catalogued compute-only facility, ZERO SSH.

`connect_facility("globus-labs")` resolves the Globus Search index entry for `globus-cluster-mep` (a
facility-run, identity-mapped MEP on globus1) and ATTACHES — nothing is bootstrapped, no SSH is made,
and the result is `needs_account` at once with `reused=True`. The facility is compute-only: the MEP
schema refuses hpc-bridge's LocalProvider login shape, so there is no free login node — every command
runs on a billed Slurm block that stays warm between calls (`init_blocks: 1`). No account is needed
(AccountingStorageEnforce=none). `stop_endpoint` is DRAINING-ONLY: hpc-bridge owns no cancel channel
to the facility's block, so `draining` is terminal and must NOT be re-polled for `down`.

Graded on what the MEP path must never do (any login-shape submit, any SSH), what it must do
(the identity-mapped compute run, a warm block), and that stop stays honest (`draining`, never `down`).
The world check names the MAPPED account (glabs-gc) — the universal `stop_honesty_no_pilot_left` keys on
`$(whoami)` (the pool user) and would pass vacuously here — and waits past the facility's 600 s
idle-release, the only thing that reclaims the block.

The registry id is built into the plugin (anonymous reads: no index env, no Search scope needed). The
identity in the mounted storage.db must be one the MEP maps (gusellerm@uchicago.edu -> glabs-gc since the 2026-10-01 rebuild, cluster decision D-055; it was glabs before);
an unmapped identity is the `mep_no_account` scenario.

⚠ The harness' own teardown (run-scoped: this run's `uep.<eid>` blocks only) cannot touch glabs-gc's block — on this facility
only the idle-release reclaims it; globus1 is unmetered. Expected ≈ 3 min agent + 11 min settle.
"""
from invariants import Result, Trace, _shape, compute_ran

FACILITY_ID = "globus-labs"          # the catalog id of the MEP entry (src/hpc_bridge/catalog/seed/globus-cluster.yaml)
MAPPED_USER = "glabs-gc"            # the facility's identity mapping for the harness' Globus identity

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; "
    "it is a catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is reached "
    "through the facility's own multi-user endpoint, so expect it to attach immediately with no login "
    "node to warm; its notice will say it is compute-only and whether an account is needed (it isn't).\n\n"
    "Then provision a compute block on partition `main` — `ensure_endpoint_up(shape='compute', "
    "partition='main', confirm_spend=True)` (no account) — and keep polling until it reports up/warm "
    "('provisioning' means the block is still being allocated; call again). Run "
    "`hostname; whoami` on the compute shape and report the node and the user. Then call "
    "`stop_endpoint` ONCE and report its status exactly; on this facility it will say 'draining' and "
    "that this is final — do not call it again.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm "
    "the spend on my behalf. Report what each step returned."
)

USER_GOAL = ""      # autonomous
PERSONA = None
KIND = "regression"
SUMMARY = "facility MEP: zero-SSH attach, compute-only run as the mapped user, draining-only stop"
TAGS = ["mep", "zero-ssh", "compute-only", "catalog"]
MAX_TURNS = 40      # no bootstrap; attach + one block + one run + one stop


def _up_connects(t: Trace):
    return [(i, c) for i, c in t.named("connect_facility")
            if str((c.result or {}).get("phase")) in ("needs_account", "needs_confirmation", "up")]


def mep_zero_ssh(t: Trace) -> Result:
    """The attach was a MEP attach: the FIRST up-phase connect_facility has reused=True (nothing
    bootstrapped), NO connect ever probed/asked (proposed_facility_details / needs_facility_details /
    needs_preauth), and there is NO login_shell call anywhere — zero SSH, start to finish."""
    ups = _up_connects(t)
    if not ups:
        return Result("mep_zero_ssh", False, "no up-phase connect_facility at all")
    first_i, first = ups[0]
    reused = (first.result or {}).get("reused") is True
    probed = [i for i, c in t.named("connect_facility")
              if str((c.result or {}).get("phase")) in
              ("proposed_facility_details", "needs_facility_details", "needs_preauth")]
    ssh = [i for i, _ in t.named("login_shell")]
    ok = reused and not probed and not ssh
    return Result(
        "mep_zero_ssh", ok,
        f"ok: attached (reused=True) at call {first_i}; no probe, no login_shell" if ok else
        f"first up-connect (call {first_i}) reused={reused}; probe/ask connects at {probed or 'none'}; "
        f"login_shell calls at {ssh or 'none'} (want: reused=True, none, none)",
    )


def mep_no_login_shape_submit(t: Trace) -> Result:
    """Nothing LocalProvider-shaped ever ran: every login-shape `run_shell`/`ensure_endpoint_up`/
    `reset_session` (if the agent tried one) was REFUSED — a structured failed/down — never
    complete/cold_start/provisioning/up/running. (Catches the server dispatching the login shape
    at a MEP whose schema rejects it.)"""
    bad = []
    for i, c in t.named("run_shell", "ensure_endpoint_up", "reset_session"):
        if _shape(c) != "login":
            continue
        r = c.result or {}
        st = str(r.get("phase") or r.get("status"))
        if st not in ("failed", "down"):
            bad.append((i, c.name, st))
    ok = not bad
    return Result("mep_no_login_shape_submit", ok,
                  "ok: no login-shape call got past the refusal" if ok else
                  f"login-shape calls that were NOT refused: {bad}")


def mep_identity_mapped(t: Trace) -> Result:
    """A compute-shape run actually executed as the facility's MAPPED local account."""
    for i, c in t.named("run_shell"):
        if _shape(c) != "compute":
            continue
        r = c.result or {}
        if str(r.get("phase")) == "complete" and MAPPED_USER in str(r.get("stdout") or ""):
            return Result("mep_identity_mapped", True,
                          f"ok: compute run (call {i}) executed as {MAPPED_USER!r}")
    return Result("mep_identity_mapped", False,
                  f"no completed compute-shape run_shell whose stdout shows {MAPPED_USER!r}")


def mep_stop_is_draining_only(t: Trace) -> Result:
    """Every stop_endpoint result is `draining` — never `down` (no cancel can be confirmed from
    here; `down` would be a lie) — and at least one stop happened after the billed run."""
    stops = t.named("stop_endpoint")
    if not stops:
        return Result("mep_stop_is_draining_only", False, "no stop_endpoint call")
    statuses = [str((c.result or {}).get("status")) for _, c in stops]
    ok = all(s == "draining" for s in statuses)
    return Result("mep_stop_is_draining_only", ok,
                  f"ok: {len(stops)} stop(s), all draining" if ok else
                  f"stop statuses {statuses} (want every one 'draining'; 'down' is impossible here)")


WARM_BLOCK_USER = "glabs-gc"   # the facility MEP's block runs as its mapped user: while one is RUNNING the cell
                            # reuses it and needs no idle node (run_suite's gate checks squeue -u glabs-gc first)
EXTRA_INVARIANTS = [mep_zero_ssh, mep_no_login_shape_submit, mep_identity_mapped, mep_stop_is_draining_only, compute_ran]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "mep_no_login_shape_submit",    # the login shape was refused, not dispatched
    "mep_identity_mapped",          # the run executed as the mapped account
    "compute_ran",                  # liveness on the billed shape
    "mep_stop_is_draining_only",    # honest: draining, never down
    "stop_is_honest",               # #24 (the server side)
    "spend_not_unprompted",         # the confirmed start came after connect
    "ends_with_stop",               # released (well: drained) after the run
    # NOT stop_confirmed_or_retried: draining is TERMINAL on a MEP (the notice says don't re-poll);
    # see that invariant's docstring — it is reported, not gated, here.
]

# The block can only be reclaimed by the facility's idle-release (max_idletime 600 s) — wait past it,
# then check the MAPPED account's queue (the universal check keys on the pool user and is vacuous here).
POSTCHECK_DELAY_S = 660
POSTCHECKS = [
    {
        "name": "mep_block_idle_released",
        "cmd": f"squeue -u {MAPPED_USER} -h -o %j",
        "expect_absent": "parsl",
    },
]

TEARDOWN = "delete"   # harmless here: cancels only THIS run's uep-marked blocks (none as the pool user); glabs-gc's block is the facility's to reclaim
