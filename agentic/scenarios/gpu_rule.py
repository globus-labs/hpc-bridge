"""A partition with a SITE RULE: the gpu partition rejects a block that requests no GPU (Delta's shape; the Anvil
gpu-debug QOS policy seen live 2026-09-04). Fake cluster, `site` profile.

The product path under test is #32: an sbatch the scheduler REFUSES is invisible to the Globus Compute client — the
manager is online, no worker ever comes, every poll reads 'allocating nodes…'. The plugin asks the scheduler (over
the free login shape) whether its pilot exists; past the cold-start grace a missing pilot is reported as 'likely
REJECTED'. The agent then has to find WHY (the endpoint log carries sbatch's stderr — the rule message) and either
satisfy the rule (a GPU request via the facility's scheduler_options) or tell the user the exact rule, instead of
polling a block that will never come.

Gated on the OUTCOME either way (`gpu_rule_handled`): the block ran on the GPU node with a GPU allocated (an agent
may know the rule up front and request the GPU from the start — nothing to surface then), OR the rule reached the agent
— the tool's REJECTED notice, or the facility's own sbatch error read through the login shape (`rule_found_in_log`) —
AND the agent relayed it to the user after that. `no_endless_wait` bounds the polling after a REJECTED notice.
"""
import re

from invariants import (
    Result,
    Trace,
    _rejected_idxs,
    _slurm_work_idxs,
    compute_ran,
    no_endless_wait,
    rejection_surfaced,
    said_after,
)

NEEDS_COMPUTE_NODE = True

PROMPT = (
    "I need a GPU node on the HPC facility with login host `{ssh_host}` — facility id `{facility}`. Bring up a "
    "compute block on the GPU partition charged to the GPU allocation, run `hostname; echo "
    "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES` on it, and then stop the endpoint.\n\n"
    "If the scheduler will not start the block, do NOT sit there polling it: find out why — the scheduler and the "
    "endpoint's own log on the login node are both readable through the login shape — and tell me the exact site "
    "rule. If the rule is something you can satisfy yourself (a resource request the partition demands), satisfy "
    "it and carry on.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions: accept discovered facility config "
    "yourself; you are authorised to confirm the spend on my behalf."
)

USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "gpu partition rule: a rejected block is surfaced (not polled forever) and the rule is relayed or satisfied"
TAGS = ["scheduler-rule", "gpu", "rejection", "fake", "site"]
MAX_TURNS = 70
TARGETS = ("fake",)
REQUIRES = {"gres": "gpu", "accounting": "enforce"}

GPU_NODE = "c3"
_RULE_WORDS = re.compile(r"gpus-per-node|--gpus\b|--gres\b|request(s|ed|ing)? (a|an|one|the) gpu|gpu request|must request", re.I)
_CUDA = re.compile(r"CUDA_VISIBLE_DEVICES=\d\S*")


def _rule_evidence(t: Trace) -> int | None:
    """The first call that put the rule in front of the agent: the tool's REJECTED notice, or a login-shape result
    carrying the site rule's own message (`_log_read_idxs`)."""
    return min([*_rejected_idxs(t)[:1], *_log_read_idxs(t)[:1]], default=None)


def rule_relayed(t: Trace) -> Result:
    """The agent's own words, AFTER the rule reached it (`_rule_evidence`), name the rule: the gpu partition demands a
    GPU request. Its words before then are not read (invariants.said_after): the prompt's own "tell me the exact site
    rule … a resource request the partition demands", restated as a plan, named it before anything was rejected."""
    k = _rule_evidence(t)
    if k is None:
        return Result("rule_relayed", False, "the rule never reached the agent (no REJECTED notice, no log read)")
    blob = "\n".join(said_after(t, k))
    ok = "gpu" in blob.lower() and bool(_RULE_WORDS.search(blob))
    return Result("rule_relayed", ok, f"ok: the agent named the GPU-request rule after call {k}" if ok
                  else f"the agent's text after call {k} never names the rule (a GPU request the partition demands)")


def gpu_block_ran(t: Trace) -> Result:
    """Compute work completed ON the GPU node with a GPU allocated (Slurm's gres plugin sets CUDA_VISIBLE_DEVICES)."""
    for i in _slurm_work_idxs(t):
        out = str((t.calls[i].result or {}).get("stdout", ""))
        m = _CUDA.search(out)
        if GPU_NODE in out and m:
            return Result("gpu_block_ran", True, f"ok: call {i} ran on {GPU_NODE} with {m.group(0)}")
    return Result("gpu_block_ran", False, f"no compute-shape run_shell completed on {GPU_NODE} with a CUDA_VISIBLE_DEVICES value")


def _log_read_idxs(t: Trace) -> list[int]:
    return [i for i, c in t.named("run_shell")
            if str(c.input.get("shape")) == "login" and "site rule" in str((c.result or {}).get("stdout", ""))]


def rule_found_in_log(t: Trace) -> Result:
    """The agent read the rule where sbatch left it: a login-shape run_shell (the endpoint log / submit-script dir)
    returned the site rule's own message. Reported, not gated, as a row — legibility for which path the run took — but
    it is evidence `gpu_rule_handled` accepts. Live 2026-09-05 the agent got here in ~40 s, BEFORE the #32 probe's 45 s
    grace had elapsed (so rejection_surfaced read FAIL while the rejection was real and diagnosed): the faster
    diagnosis, not a product miss."""
    idx = _log_read_idxs(t)
    return Result("rule_found_in_log", bool(idx), f"ok: call {idx[0]} read the rule's message from the login node" if idx
                  else "no login-shape command returned the site rule's message")


def gpu_rule_handled(t: Trace) -> Result:
    """The outcome: the rule was SATISFIED (work ran on the GPU node with a GPU) or SURFACED + RELAYED. Surfaced: the
    tool's REJECTED notice (`rejection_surfaced`), or the facility's own sbatch error reaching the agent through a
    login-shape result (`rule_found_in_log`) — the agent that reads the endpoint log before the #32 probe's grace has
    diagnosed the real rejection, and failed here for being faster than the tool (live 2026-09-05; the review of the
    2026-10-22 sweep). rejection_surfaced stays a report-only row."""
    ran = gpu_block_ran(t)
    if ran.ok:
        return Result("gpu_rule_handled", True, f"satisfied — {ran.detail}")
    surfaced, read, relayed = rejection_surfaced(t), rule_found_in_log(t), rule_relayed(t)
    seen = surfaced if surfaced.ok else read
    ok = seen.ok and relayed.ok
    return Result("gpu_rule_handled", ok, f"surfaced and relayed — {seen.detail}" if ok
                  else f"neither satisfied nor explained — {ran.detail}; {surfaced.detail}; {read.detail}; {relayed.detail}")


EXTRA_INVARIANTS = [gpu_rule_handled, no_endless_wait, rejection_surfaced, rule_relayed, rule_found_in_log, gpu_block_ran, compute_ran]

EXPECT_OK = [
    "gpu_rule_handled",             # the point: satisfied, or surfaced + relayed
    "no_endless_wait",              # a rejected block is not polled forever
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
    "stop_confirmed_or_retried",
]
# REPORTED, not gated: rejection_surfaced / rule_relayed / rule_found_in_log / gpu_block_ran / compute_ran — which branch
# the run took and how (gpu_rule_handled accepts either rejection_surfaced or rule_found_in_log as the evidence). Live 2026-09-05: rejected → log read at ~40 s → reconnect with scheduler_options → c3, GPU 0.

TEARDOWN = "delete"
