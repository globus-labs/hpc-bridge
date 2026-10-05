"""The facility-MEP path on the FAKE cluster (`mep` profile): a STRICT-schema multi-user endpoint in login01, our
Globus identity mapped to the local account `hpcbmep`. Zero SSH: connect_facility attaches (reused=True,
needs_account), the block is a billed Slurm block charged to `hpcb` (site enforces accounting), `stop_endpoint` is
draining-only (no cancel channel of ours), and the facility's idle-release (180 s here) reclaims the block.

The STRICT manager (Anvil's shape) refuses any key its schema does not list — compute, interface, worker_init — so
the plugin must DROP them and rely on the template's defaults (MEPFacility.sanitize_uec / dispatch_uec). That is the
'template-key gap' that blocks adding real MEPs to the registry, reproduced locally: if the plugin sends a forbidden
key the manager rejects the submission and no block ever comes.

The catalog is LOCAL (run_smoke.sh mounts this cluster's generated entries as HPC_BRIDGE_CATALOG_FILE): the MEP UUIDs
are minted per cluster. Graded like mep_compute_only, with the mapped user and idle-release of this facility.
"""
from invariants import Result, Trace, _shape, compute_ran
from mep_compute_only import mep_no_login_shape_submit, mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"       # the local catalog's id for the strict-schema manager
MAPPED_USER = "hpcbmep"
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER          # a RUNNING block of the mapped user satisfies the node gate (like glabs-gc on globus1)
SERIAL = True                          # one mapped identity — cells would share the user endpoint
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce"}

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is reached through the facility's "
    "own multi-user endpoint, so expect it to attach immediately with no login node to warm; its notice says it is "
    "compute-only and that an account is needed.\n\n"
    "Then provision a compute block on partition `compute` charged to account `hpcb` — "
    "`ensure_endpoint_up(shape='compute', partition='compute', account='hpcb', confirm_spend=True)` — and keep "
    "polling until it reports up/warm ('provisioning' means the block is still being allocated; call again; the first "
    "block on this facility takes a few minutes because the worker environment installs itself). Run "
    "`hostname; whoami` on the compute shape and report the node and the user. Then call `stop_endpoint` ONCE and "
    "report its status exactly; on this facility it will say 'draining' and that this is final — do not call it "
    "again.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf. Report what each step returned."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "fake facility MEP (strict schema): zero-SSH attach, forbidden keys dropped, compute as the mapped user, draining-only stop"
TAGS = ["mep", "zero-ssh", "strict-schema", "fake", "catalog-seam"]
MAX_TURNS = 45


def mep_identity_mapped(t: Trace) -> Result:
    """A compute-shape run executed as the facility's MAPPED local account."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and str((c.result or {}).get("phase")) == "complete" \
                and MAPPED_USER in str((c.result or {}).get("stdout") or ""):
            return Result("mep_identity_mapped", True, f"ok: compute run (call {i}) executed as {MAPPED_USER!r}")
    return Result("mep_identity_mapped", False, f"no completed compute-shape run_shell whose stdout shows {MAPPED_USER!r}")


def strict_keys_dropped(t: Trace) -> Result:
    """The plugin TOLD the agent it dropped what the strict schema forbids (MEPFacility's 'keys not in the facility's
    template schema were dropped' note in a connect/ensure notice). REPORTED, not gated: the dropping itself is proven
    by compute_ran (the strict manager rejects a forbidden key outright, so a block only comes up if the plugin sent
    none) — live 2026-09-05 the block came up and this read FAIL: the note is recorded at the first dispatch, after
    connect's notice was built, and no later result carries it. A legibility follow-up for the plugin, not a defect."""
    for i, c in t.named("connect_facility", "ensure_endpoint_up"):
        n = str((c.result or {}).get("notice") or "")
        if "not in the facility's template schema were dropped" in n:
            return Result("strict_keys_dropped", True, f"ok: call {i} noted the dropped keys")
    return Result("strict_keys_dropped", False, "no notice said keys were dropped for the strict schema")


EXTRA_INVARIANTS = [mep_zero_ssh, mep_no_login_shape_submit, mep_identity_mapped, mep_stop_is_draining_only, strict_keys_dropped, compute_ran]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "mep_no_login_shape_submit",    # the login shape was refused, not dispatched
    "mep_identity_mapped",          # the run executed as hpcbmep
    "compute_ran",                  # the strict manager accepted what the plugin sent
    "mep_stop_is_draining_only",    # honest: draining, never down
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# Only the facility's idle-release (max_idletime 180 s in the template) reclaims the block: wait past it, then check
# the MAPPED account's queue. ADMIN_CLEANUP then makes sure nothing of the mapped user outlives the cell.
POSTCHECK_DELAY_S = 240
POSTCHECKS = [{"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"}]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]
TEARDOWN = "delete"
