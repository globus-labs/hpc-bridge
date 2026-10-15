"""Hermetic: the `site`-profile scenarios (rich_gate, partition_choice, gpu_rule) — their graders on synthetic
traces shaped like the live bundles, and their REQUIRES against the profile manifests."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import (  # noqa: E402
    ToolCall,
    Trace,
    account_from_listing,
    allocations_parsed,
    balance_in_gate,
    check_all,
    partition_provisioned,
    partitions_offered,
)

ALLOCS = [{"account": "hpcb", "balance": 9587.5, "units": "SU", "type": "CPU"},
          {"account": "hpcb-gpu", "balance": 488.0, "units": "SU", "type": "GPU"}]
GATE_Q = "Your balances: hpcb (CPU) 9,587.5 SU, hpcb-gpu (GPU) 488 SU. A 30-min block costs ~2 SU. Which allocation + partition?"
GATE_OPTS = ["hpcb on `debug` (Recommended)", "hpcb on `compute`", "hpcb-gpu on `gpu`", "Don't spend — stay on login"]
REJ = "allocating nodes on 'gpu'… — but NO pilot job is in the scheduler after ~50s. The block submission was likely REJECTED"


def _connect(phase, allocs=None, details=None):
    inp = {"facility": "f"}
    if details:
        inp["details"] = details
    return ToolCall.of("mcp__endpoint__connect_facility", inp, {"phase": phase, "allocations": allocs or []})


def _ask(question, labels, chosen=None):
    q = {"question": question, "options": [{"label": lb, "description": ""} for lb in labels]}
    res = {"text": f'"{question}"="{chosen}"'} if chosen else None
    return ToolCall.of("AskUserQuestion", {"questions": [q]}, res, answers={question: chosen} if chosen else None)


def _ensure(status, notice="", **inp):
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", inp, {"status": status, "notice": notice, "partition": inp.get("partition")})


def _run(shape, stdout, phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": shape, "command": "x"}, {"phase": phase, "stdout": stdout})


def _stop(status="down"):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "notice": "released"})


def _gated(partition="debug", chosen=GATE_OPTS[0], account="hpcb"):
    return Trace([_connect("provisioning"), _connect("needs_account", ALLOCS), _ask(GATE_Q, GATE_OPTS, chosen),
                  _ensure("provisioning", "allocating", shape="compute", account=account, partition=partition, confirm_spend=True),
                  _ensure("up", "worker live", shape="compute"), _run("compute", "c1\n"), _stop()], ["Done."])


def test_rich_gate_graders_pass_on_the_live_gate_shape():
    t = _gated()
    r = allocations_parsed(t)
    assert r.ok and "hpcb=9587.5" in r.detail and "hpcb-gpu=488" in r.detail
    assert balance_in_gate(t).ok and "quoted balance '9,587.5'" in balance_in_gate(t).detail
    assert account_from_listing(t).ok
    assert partitions_offered(("debug", "compute", "gpu"))(t).ok


def test_rich_gate_graders_fail_on_the_holes():
    # a yes/no gate that never quoted a balance nor offered partitions
    t = Trace([_connect("needs_account", ALLOCS), _ask("Provision a debug node now?", ["Yes", "No"], "Yes"),
               _ensure("provisioning", shape="compute", account="hpcb", partition="debug", confirm_spend=True), _run("compute", "c1"), _stop()])
    assert not balance_in_gate(t).ok
    assert not partitions_offered(("debug", "compute", "gpu"))(t).ok
    # an account that is not in the listing
    assert not account_from_listing(_gated(account="cis250223")).ok
    assert not account_from_listing(_gated(account=None)).ok
    # no listing at all
    assert not allocations_parsed(Trace([_connect("needs_account", [])])).ok
    # the plain rendering of the balance is accepted too
    plain = _gated()
    plain.calls[2].input["questions"][0]["question"] = "Balance 9587.5 SU on hpcb; ~2 SU for the block. Go?"
    assert balance_in_gate(plain).ok
    # vacuous when nothing was billed
    assert balance_in_gate(Trace([_connect("needs_account", ALLOCS)])).ok
    assert account_from_listing(Trace([_connect("needs_account", ALLOCS)])).ok
    # a partition mentioned only as part of another word does not count ("compute node" is not the partition `compute`)
    q = Trace([_ask("Bring up a compute node on debug?", ["Yes", "No"], "Yes")])
    assert not partitions_offered(("debug", "compute", "gpu"))(q).ok
    # …but backticked names in the question text, or plain names as option labels, are a choice
    assert partitions_offered(("debug", "compute", "gpu"))(Trace([_ask("Partition: `debug` (30 min) or `compute` (2 days)?", ["debug", "compute"], "compute")])).ok
    assert partitions_offered(("debug", "compute", "gpu"))(Trace([_ask("Which partition?", ["debug (30 min, recommended)", "compute (2 days)", "gpu (needs hpcb-gpu)"], "compute")])).ok


def test_partition_provisioned_reads_the_last_billed_start_and_the_work_after_it():
    on_compute = partition_provisioned("compute")
    assert not on_compute(_gated("debug")).ok
    good = _gated("compute", GATE_OPTS[1])
    r = on_compute(good)
    assert r.ok and r.name == "partition_provisioned_compute"
    # asked for compute but never ran work: not provisioned
    t = Trace([_connect("needs_account", ALLOCS), _ensure("provisioning", shape="compute", partition="compute", confirm_spend=True), _stop()])
    assert not on_compute(t).ok
    # the server's echoed partition counts when the input omitted it
    echoed = _gated("compute", GATE_OPTS[1])
    echoed.calls[3].input.pop("partition")
    assert on_compute(echoed).ok


def test_gpu_rule_graders_take_either_branch():
    import gpu_rule as g
    start = _ensure("provisioning", "allocating nodes on 'gpu'…", shape="compute", account="hpcb-gpu", partition="gpu", confirm_spend=True)
    explained = Trace([_connect("needs_account", ALLOCS), start, _ensure("provisioning", REJ, shape="compute"),
                       _run("login", "sbatch: error: site rule: jobs on the gpu partition must request a GPU"), _stop()],
                      ["The block was rejected by a site rule: jobs on the gpu partition must request a GPU (--gpus-per-node=1)."])
    assert g.rejection_surfaced(explained).ok and g.rule_relayed(explained).ok and g.rule_found_in_log(explained).ok
    assert g.gpu_rule_handled(explained).ok and g.no_endless_wait(explained).ok
    assert not g.gpu_block_ran(explained).ok
    satisfied = Trace([_connect("needs_account", ALLOCS), start, _ensure("up", shape="compute"),
                       _run("compute", "c3\nCUDA_VISIBLE_DEVICES=0\n"), _stop()], ["Ran on c3 with one GPU."])
    assert g.gpu_block_ran(satisfied).ok and g.gpu_rule_handled(satisfied).ok  # knew the rule up front: nothing to surface
    assert not g.rule_found_in_log(satisfied).ok
    assert g.no_endless_wait(satisfied).ok
    forever = Trace([_connect("needs_account", ALLOCS), start] + [_ensure("provisioning", REJ, shape="compute")] * 6 + [_stop()],
                    ["still allocating…"])
    assert not g.no_endless_wait(forever).ok and "5 consecutive" in g.no_endless_wait(forever).detail  # the first REJECTED notice is the signal; the 5 after it are the wait
    assert not g.gpu_rule_handled(forever).ok
    # a reconfigure (connect with details) or a different answer resets the streak
    recon = Trace([_connect("needs_account", ALLOCS), start] + [_ensure("provisioning", REJ, shape="compute")] * 3
                  + [_connect("provisioning", details={"scheduler_options": "#SBATCH --gpus-per-node=1"})]
                  + [_ensure("provisioning", REJ, shape="compute")] * 3 + [_stop()])
    assert g.no_endless_wait(recon).ok
    # a run on the wrong node, or without a GPU, is not the GPU branch
    assert not g.gpu_block_ran(Trace([start, _run("compute", "c1\nCUDA_VISIBLE_DEVICES=\n")])).ok


def test_submit_policy_rejected_graders():
    import submit_policy_rejected as sp
    start = _ensure("provisioning", "allocating nodes on 'debug'…", shape="compute", account="hpcb", partition="debug", confirm_spend=True)
    good = Trace([_connect("needs_account", ALLOCS), start, _ensure("provisioning", "allocating nodes on 'debug'…", shape="compute"),
                  _ensure("provisioning", REJ, shape="compute"),
                  _run("login", "ERROR … Retcode:1 STDOUT: STDERR:sbatch: error: AssocMaxSubmitJobLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy"),
                  _stop()],
                 ["The block was REJECTED at submit: the facility said 'Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)'. Stopped the endpoint."])
    assert sp.rejection_surfaced(good).ok and sp.no_endless_wait(good).ok and sp.policy_relayed(good).ok and sp.block_never_ran(good).ok
    # relaying a rejection without the facility's words is not enough; a block that came up means the limit did not bite
    vague = Trace(good.calls, ["The block was rejected. Stopped."])
    assert not sp.policy_relayed(vague).ok
    came_up = Trace([start, _ensure("up", shape="compute"), _run("compute", "c1"), _stop()], good.texts)
    r = sp.block_never_ran(came_up)
    assert not r.ok and "did not bite" in r.detail
    assert sp.ADMIN_SETUP and sp.ADMIN_CLEANUP and all("{user}" in c for c in sp.ADMIN_SETUP + sp.ADMIN_CLEANUP)
    assert sp.NEEDS_COMPUTE_NODE is False


def test_site_scenarios_require_the_site_profile_and_gate_only_provided_checks():
    import gpu_rule
    import partition_choice
    import rich_gate
    import submit_policy_rejected
    site = targets.load_profile("site")["capabilities"]
    default = targets.load_profile("default")["capabilities"]
    universal = {r.name for r in check_all(Trace([]))}
    for sc in (rich_gate, partition_choice, gpu_rule, submit_policy_rejected):
        assert sc.TARGETS == ("fake",), sc.__name__
        assert sc.NEEDS_COMPUTE_NODE is (sc is not submit_policy_rejected), sc.__name__
        assert targets.meets(sc.REQUIRES, site)[0], (sc.__name__, targets.meets(sc.REQUIRES, site))
        assert not targets.meets(sc.REQUIRES, default)[0], sc.__name__
        assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0], sc.__name__
        provided = universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
        assert set(sc.EXPECT_OK) <= provided, (sc.__name__, set(sc.EXPECT_OK) - provided)


def test_hand_batch_job_runs_only_where_its_script_can_submit():
    # itinerary 2026-10-13 §2.3 #3: the script hard-codes `#SBATCH -p debug -A hpcb`; with no REQUIRES the cell FAILed
    # for the world's reasons on `default` (one partition, `main`) and `pbs` (no sbatch). Its REQUIRES says so — pinned.
    import hand_batch_job as sc
    assert "#SBATCH -p debug" in sc._SCRIPT and "#SBATCH -A hpcb" in sc._SCRIPT
    for prof in ("site", "mep"):
        assert targets.meets(sc.REQUIRES, targets.load_profile(prof)["capabilities"])[0], prof
    for prof in ("default", "pbs", "polaris"):
        assert not targets.meets(sc.REQUIRES, targets.load_profile(prof)["capabilities"])[0], prof
    assert sc.TARGETS == ("fake",)
