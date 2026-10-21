"""ALCF Polaris's site rule on the fake PBS cluster (`polaris` profile): every job must request the filesystems it
uses (`-l filesystems=home:eagle`); a queuejob hook HOLDS any job that does not, with the reason in the job's comment.
The plugin's pilot never runs and nothing on the client side says why — unless the pilot probe reads the job record.

Under test: the probe's HELD path (0.1.11: "pilot N is HELD … The scheduler's comment: 'HELD by the site: every job
must request the filesystems it uses…'") and the agent's response — add the directive through the facility's
scheduler_options (connect_facility with details=) and start again, or at least relay the site's rule — rather than
wait on a block that will never move. Outcome-gated like gpu_rule: work ran on a compute node, OR the hold was
surfaced by the tool and the rule relayed. `no_endless_hold_wait` bounds the polling of a held pilot.

Scripted too (HPCB_OPERATOR=script, harness/script_runner.py), for a deterministic live check with no model:

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=polaris ./agentic/run_smoke.sh polaris_filesystems

The SCRIPT is the 2026-10-16 sweep's failure, done right: it connects from the probe's proposal, confirms spend, polls
ensure_endpoint_up until the HELD notice, then connects again with `scheduler_options="-l filesystems=home:eagle"` —
BARE, as both agents copied it from the site's comment — and aborts unless that connect reports the rewrite to the
`#PBS` directive (a build that passes the bare line through leaves the hold in place). It confirms spend again, polls
until the block is up — aborting if a HELD verdict comes back (the hold persists), whichever pilot it names — runs
`hostname; echo FS=$PBS_JOBID` on the compute shape, and stops the endpoint until `down`. The gated graders hold for a
correct run: none needs a question to the user (the prompt authorises the spend and the directive).
"""
import re
import time

from invariants import Result, Trace, compute_ran

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
REQUIRES = {"scheduler": "pbs", "site_rule": "filesystems"}

PROMPT = (
    "Bring up a compute node on the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — run "
    "`hostname; echo FS=$PBS_JOBID` on it, and stop the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: accept the discovered facility config yourself, pick the "
    "cheapest sensible queue, and you are authorised to confirm the spend on my behalf.\n\n"
    "If the block does not start, do NOT sit there polling it: read what the tool's status says about the pilot job "
    "and what the scheduler says about it (its comment), tell me the exact site rule, and if you can satisfy it "
    "yourself — a scheduler directive the site demands — satisfy it and carry on."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "Polaris filesystems rule: the held pilot is surfaced with the site's comment; the directive is added or the rule relayed"
TAGS = ["pbs", "site-rule", "held", "polaris", "fake"]
MAX_TURNS = 70

_HELD = "HELD"
_RULE = re.compile(r"filesystems", re.I)


def _held_idxs(t: Trace) -> list[int]:
    return [i for i, c in enumerate(t.calls)
            if c.name in ("ensure_endpoint_up", "run_shell", "poll_task") and _HELD in str((c.result or {}).get("notice", ""))]


def held_surfaced(t: Trace) -> Result:
    idx = _held_idxs(t)
    return Result("held_surfaced", bool(idx), f"ok: the tool reported a held pilot at call {idx[0]}" if idx
                  else "no tool result ever carried a HELD pilot notice")


def hold_comment_surfaced(t: Trace) -> Result:
    """The site's own words (the job comment naming `filesystems`) reached the agent THROUGH THE TOOL."""
    for i in _held_idxs(t):
        if _RULE.search(str((t.calls[i].result or {}).get("notice", ""))):
            return Result("hold_comment_surfaced", True, f"ok: call {i}'s notice carried the scheduler's comment")
    return Result("hold_comment_surfaced", False, "no HELD notice carried the site's comment (filesystems)")


def directive_added(t: Trace) -> Result:
    """The agent satisfied the rule: a connect_facility(details=…) whose scheduler_options request filesystems."""
    for i, c in t.named("connect_facility"):
        det = c.input.get("details")
        if isinstance(det, dict) and _RULE.search(str(det.get("scheduler_options") or "")):
            return Result("directive_added", True, f"ok: call {i} set scheduler_options with a filesystems request")
    return Result("directive_added", False, "no connect_facility(details=) added a filesystems directive")


def rule_relayed(t: Trace) -> Result:
    blob = "\n".join(t.texts)
    ok = bool(_RULE.search(blob)) and bool(re.search(r"held|hold", blob, re.I))
    return Result("rule_relayed", ok, "ok: the agent named the filesystems rule and the hold" if ok
                  else "the agent's text never names the filesystems rule with the hold")


def site_rule_handled(t: Trace) -> Result:
    if compute_ran(t).ok:
        return Result("site_rule_handled", True, f"satisfied — {directive_added(t).detail}")
    h, r = held_surfaced(t), rule_relayed(t)
    ok = h.ok and r.ok
    return Result("site_rule_handled", ok, f"surfaced and relayed — {h.detail}" if ok
                  else f"neither satisfied nor explained — {h.detail}; {r.detail}")


def no_endless_hold_wait(t: Trace) -> Result:
    """After a HELD notice, at most 3 more ensure_endpoint_up polls may read the same held pilot before the agent
    changes something (connect_facility with details=) or stops."""
    idx = _held_idxs(t)
    if not idx:
        return Result("no_endless_hold_wait", True, "no hold to wait on")
    first, stale, worst = idx[0], 0, 0
    for i, c in enumerate(t.calls):
        if i <= first:
            continue
        if c.name == "connect_facility" and c.input.get("details"):
            stale = 0
        elif c.name == "ensure_endpoint_up":
            r = c.result or {}
            if str(r.get("status")) == "provisioning" and _HELD in str(r.get("notice", "")):
                stale += 1
                worst = max(worst, stale)
            else:
                stale = 0
    ok = worst <= 3
    return Result("no_endless_hold_wait", ok, f"ok: at most {worst} stale poll(s) on the held pilot" if ok
                  else f"{worst} consecutive polls kept reading the HELD pilot after call {first} with no config change")


EXTRA_INVARIANTS = [site_rule_handled, no_endless_hold_wait, held_surfaced, hold_comment_surfaced, directive_added, rule_relayed, compute_ran]
EXPECT_OK = [
    "site_rule_handled",            # the point: satisfied, or surfaced + relayed
    "no_endless_hold_wait",         # a held pilot is not polled forever
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
    "stop_confirmed_or_retried",
]
# REPORTED: held_surfaced / hold_comment_surfaced / directive_added / rule_relayed / compute_ran — which path the run took.
TEARDOWN = "delete"


# --- the script operator's run (harness/script_runner.py) -------------------------------------------------------------
LOGIN_UP_WAIT_S = 300.0   # how long the login shape gets to come up (each connect)
HOLD_WAIT_S = 240.0       # how long the first pilot gets to be reported HELD (the hook holds it at qsub)
UP_WAIT_S = 420.0         # how long the block with the directive gets to come up
POLL_S = 15.0             # between ensure_endpoint_up polls
STOP_RETRIES = 3          # stop_endpoint again while it answers `draining`
BARE = "-l filesystems=home:eagle"           # as the site's comment quotes it — deliberately without `#PBS `
WRITTEN = "#PBS -l filesystems=home:eagle"   # what the submit script must carry
COMMAND = "hostname; echo FS=$PBS_JOBID"
# a pilot verdict's HELD (the upper-case word is the verdict's; earlier pilots are said "held")
_HELD_VERDICT = re.compile(r"pilot (\S+) is HELD")


def _brief(r: dict) -> str:
    return str(r.get("notice") or r.get("text") or "")[:200]


def _proposal_details(r: dict) -> dict:
    """The proposal as the confirming details=: env_setup left out (the server keeps the proposed one — the tool's own
    instruction), unset fields dropped."""
    return {k: v for k, v in dict(r.get("proposed_details") or {}).items() if v is not None and k != "env_setup"}


def rewrite_reported(notice: str) -> bool:
    """The connect said it wrote the bare option as the directive, exactly."""
    return f"wrote {BARE!r} as {WRITTEN!r}" in notice


async def _bound(ops, r: dict, fid: str, host: str) -> dict:
    """Call connect_facility again (no details) until the login shape is up: phase needs_account."""
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(5)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    return r


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {_brief(r)!r}")
    details = _proposal_details(r)
    if details.get("scheduler") != "pbs":
        ops.abort(f"the probe did not propose a PBS facility: {details.get('scheduler')!r} (the polaris profile?)")
    details.pop("scheduler_options", None)
    await _bound(ops, await ops.call("connect_facility", facility=fid, ssh_host=host, details=details), fid, host)
    # the site holds a pilot with no filesystems request
    e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True)
    deadline = time.monotonic() + HOLD_WAIT_S
    while (m := _HELD_VERDICT.search(str(e.get("notice") or ""))) is None:
        if e.get("status") == "up":
            ops.abort("the block came UP without the filesystems directive: the site rule did not bite (polaris profile?)")
        if e.get("is_error") or e.get("status") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"no HELD notice: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute")
    held = m.group(1)
    notice = str(e.get("notice") or "")
    if not re.search(r"filesystems", notice):
        ops.abort(f"the HELD notice does not carry the site's comment: {notice[:500]!r}")
    ops.say(f"The pilot {held} is HELD by the site: every job must request the filesystems it uses "
            f"(block_job_id={e.get('block_job_id')!r}). Adding the directive the site asks for, as {BARE!r}.")
    # the directive, BARE: the connect must write it as the scheduler's directive and say so
    c = await ops.call("connect_facility", facility=fid, ssh_host=host,
                       details={**details, "scheduler_options": BARE})
    said = [str(c.get("notice") or "")]
    if c.get("phase") == "failed":
        ops.abort(f"the connect with the directive failed: {_brief(c)!r}")
    c = await _bound(ops, c, fid, host)
    said.append(str(c.get("notice") or ""))
    if not any(rewrite_reported(s) for s in said):
        ops.abort(f"the connect did not report writing {BARE!r} as {WRITTEN!r}: {said[0][:500]!r}")
    ops.say(f"hpc-bridge wrote {BARE!r} as {WRITTEN!r} for the submit script.")
    # a new block, with the directive: it must come up — a HELD verdict now is the hold persisting
    e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True)
    deadline = time.monotonic() + UP_WAIT_S
    while e.get("status") != "up":
        if (m := _HELD_VERDICT.search(str(e.get("notice") or ""))) is not None:
            which = "the pilot held before the fix" if m.group(1) == held else "the new pilot"
            ops.abort(f"the hold persists after the directive: pilot {m.group(1)} ({which}) is HELD: "
                      f"{str(e.get('notice') or '')[:500]!r}")
        if e.get("status") == "needs_confirmation":
            ops.abort(f"spend was asked again after the confirmation: {_brief(e)!r}")
        if e.get("is_error") or e.get("status") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the block did not come up after the directive: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute")
    out = await ops.call("run_shell", command=COMMAND, shape="compute")
    if out.get("phase") != "complete" or "FS=" not in str(out.get("stdout") or ""):
        ops.abort(f"`{COMMAND}` did not complete on the compute shape: phase={out.get('phase')!r} {_brief(out)!r}")
    ran = " ".join(str(out.get("stdout") or "").split())
    ops.say(f"With the directive the block came up and ran `{COMMAND}`: {ran!r}. Stopping the endpoint.")
    s = await ops.call("stop_endpoint")
    for _ in range(STOP_RETRIES):
        if s.get("status") != "draining":
            break
        await ops.sleep(POLL_S)
        s = await ops.call("stop_endpoint")
    return (f"The site held pilot {held} (no filesystems request); hpc-bridge wrote {BARE!r} as {WRITTEN!r}, the new "
            f"block ran `{COMMAND}` ({ran!r}), and stop_endpoint answered {s.get('status')!r} "
            f"(spend_stopped={s.get('spend_stopped')!r}).")
