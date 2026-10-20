"""A MODULE-SYSTEM site — Lmod, the way most facilities ship a toolchain: nothing useful on the default PATH (no
endpoint, no uv), `module load python/3.11` or `module load uv/0.12.9` is how you get one. Fake cluster, `lmod`
profile (layered on site).

Two things under test. Discovery (0.1.10) must notice the module system and PROPOSE a `module load …` env_setup —
the site-supported route — instead of curl-installing uv into $HOME (`proposal_uses_modules`, `no_curl_bootstrap`).
And that env_setup must work where it is replayed: the login node's `bash -lc` AND a compute node's batch script,
which is NOT a login shell (Lmod's /etc/profile.d init never ran there) — so the proposal re-initialises `module`
itself. A block that comes up and runs `hostname` proves the second (`compute_ran`).

Autonomous: the agent accepts discovered config; if the proposal is wrong it may correct env_setup, but the graders
read the PLUGIN's proposal (the proposed_facility_details result), so a fix by the agent does not mask a bad proposal.

A details= whose bring-up failed is never run again by a connect without details (`failed_details_not_rerun`). Live
2026-10-16 (free sweep, hermes21, run 1792183016): the agent's FIRST connect carried a made-up env_setup (`module load
gcc/10.2.0`), the remote configure failed with Lmod's "module(s) are unknown", and the connect without details that
the next refusal's advice named answered the same Lmod error: the session had kept the unproven entry and re-ran it.

Scripted too (HPCB_OPERATOR=script, harness/script_runner.py), to stage exactly that with no model:

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=lmod ./agentic/run_smoke.sh lmod_bootstrap

The SCRIPT sends that made-up `module load` first (details= marked `invariants.STAGED_FAILURE`, so the #39 gate judges
the first bring-up after it), aborts unless it fails on the login node naming the module, then connects without
details and aborts unless the answer is a FRESH proposal — module-based, its own line, opening with what failed (a
build that runs the failed details= again, 7012984 and before, aborts here). It confirms the proposal (env_setup left
out: the server keeps the proposed line), waits for the login shape, confirms spend on the first non-GPU allocation,
waits for the block, runs `hostname; module list` on it and stops the endpoint. Every grader the scenario gates holds
for such a run.
"""
import time

from invariants import STAGED_FAILURE, Result, Trace, compute_ran

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
REQUIRES = {"module_system": "lmod"}

PROMPT = (
    "Bring up a compute node on the HPC facility with login host `{ssh_host}` — use facility id `{facility}`. On this "
    "facility the software stack comes through a module system (`module load …`); there is no `uv` or endpoint on the "
    "default PATH. Accept the discovered configuration yourself (this is an AUTOMATED run — nobody can answer "
    "questions), pick the cheapest sensible partition, and you are authorised to confirm the spend. Run "
    "`hostname; module list 2>&1 | tail -3` on the compute node to show which modules the worker ended up with, then "
    "stop the endpoint."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "Lmod site: discovery proposes a module-based env_setup (no curl bootstrap) and it survives the compute node's batch shell"
TAGS = ["modules", "lmod", "env_setup", "discovery", "fake"]
MAX_TURNS = 60


def _proposals(t: Trace) -> list[str]:
    out = []
    for _, c in t.named("connect_facility"):
        r = c.result or {}
        if str(r.get("phase")) == "proposed_facility_details":
            det = r.get("details") or r.get("proposed_details") or {}
            if isinstance(det, dict) and det.get("env_setup"):
                out.append(str(det["env_setup"]))
    return out


def proposal_uses_modules(t: Trace) -> Result:
    """The PLUGIN's proposal (proposed_facility_details) loads a module — discovery saw the module system."""
    props = _proposals(t)
    if not props:
        return Result("proposal_uses_modules", False, "no proposed_facility_details with an env_setup in any connect result")
    ok = all("module load" in p for p in props)
    return Result("proposal_uses_modules", ok, f"ok: proposed {props[0][:90]!r}" if ok
                  else f"a proposal did not use the module system: {[p[:80] for p in props]}")


def no_curl_bootstrap(t: Trace) -> Result:
    """Neither the proposal nor the accepted details fetch uv from the internet (the site provides the toolchain)."""
    texts = _proposals(t)
    for _, c in t.named("connect_facility"):
        det = c.input.get("details")
        if isinstance(det, dict) and det.get("env_setup"):
            texts.append(str(det["env_setup"]))
    bad = [x[:80] for x in texts if "astral.sh" in x or "install.sh" in x]
    return Result("no_curl_bootstrap", not bad, "ok: no curl-installed uv" if not bad else f"curl bootstrap used: {bad}")


def module_reinit_in_env_setup(t: Trace) -> Result:
    """The proposal re-initialises `module` before loading (a compute node's batch script is not a login shell)."""
    props = _proposals(t)
    ok = bool(props) and all("type module" in p or "init/bash" in p or "profile.d" in p for p in props)
    return Result("module_reinit_in_env_setup", ok, "ok" if ok else f"proposal(s) assume `module` exists: {[p[:80] for p in props]}")


# A failed bring-up of a details= config whose cause is outside it (SSH, the network, a timeout): the product KEEPS that
# config for the retry the notice asks for, so a connect without details may meet the same failure again, rightly.
_KEPT_FOR_RETRY = ("UNKNOWN HOST KEY", "REVOKED HOST KEY", "NO SSH ACCESS", "CANNOT REACH", "REMOTE FILESYSTEM",
                   "TIMED OUT", "hpc-bridge error: the SSH ControlMaster")


# A details= the server refused before taking it as the session's config (the login gate, a bad host, no env_setup to
# take, invalid details): it replaced nothing, so the config before it is still the session's.
_REFUSED_UNREAD = ("details= has no env_setup", "invalid facility details", "hpc-bridge error: invalid login host")


def _took_details(c) -> bool:
    """A connect_facility whose details= the server took as this session's config (it reached the server, answered a
    phase, and was not refused unread)."""
    r = c.result or {}
    return (r.get("phase") is not None and str(r.get("phase")) != "needs_login"
            and not str(r.get("notice") or "").startswith(_REFUSED_UNREAD))


def failed_details_not_rerun(t: Trace) -> Result:
    """No connect without details= answers the very failure of a details= bring-up of the same facility before it (the
    live bug: the session kept the unproven config and ran it again); a details= the server took in between starts
    over (one it refused unread, or a call that never reached it, does not). A failure outside the config
    (`_KEPT_FOR_RETRY`) is retried as it is by design, and does not count."""
    failed: dict[str, tuple[int, str]] = {}   # facility -> (call, the failure's opening) of its last failed details=
    for i, c in t.named("connect_facility"):
        fid, r = str(c.input.get("facility") or ""), c.result or {}
        notice = " ".join(str(r.get("notice") or "").split())
        if c.input.get("details"):
            if not _took_details(c):
                continue
            failed.pop(fid, None)
            if str(r.get("phase")) == "failed" and notice and not notice.startswith(_KEPT_FOR_RETRY):
                failed[fid] = (i, notice[:80])
        elif fid in failed and str(r.get("phase")) == "failed" and notice.startswith(failed[fid][1]):
            return Result("failed_details_not_rerun", False,
                          f"connect_facility without details (call {i}) answered the failure of the details= at call "
                          f"{failed[fid][0]} again — it ran that config again: {failed[fid][1]!r}")
    return Result("failed_details_not_rerun", True, "ok")


EXTRA_INVARIANTS = [proposal_uses_modules, no_curl_bootstrap, module_reinit_in_env_setup, compute_ran,
                    failed_details_not_rerun]
EXPECT_OK = [
    "proposal_uses_modules",        # discovery saw Lmod and proposed module load
    "no_curl_bootstrap",            # and did not fall back to fetching uv
    "module_reinit_in_env_setup",   # robust to the compute node's non-login batch shell
    "compute_ran",                  # the worker actually came up through that env_setup
    "failed_details_not_rerun",     # a details= that failed is not run again by a connect without details
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
    "stop_confirmed_or_retried",
]
TEARDOWN = "delete"


# --- the script operator's run (harness/script_runner.py) -------------------------------------------------------------
STAGED_ENV_SETUP = "module load gcc/10.2.0"   # a module the lmod profile does not have: the made-up line of 2026-10-16
STAGED_MODULE = "gcc/10.2.0"
LOGIN_UP_WAIT_S = 300.0   # how long the login shape gets to come up
WARM_WAIT_S = 300.0       # how long the block gets to answer its check
POLL_S = 5.0
STOP_RETRIES = 3          # stop_endpoint again while it answers `draining`
COMMAND = "hostname; module list 2>&1 | tail -3"   # the prompt's


def _brief(r: dict) -> str:
    return " ".join(str(r.get("notice") or r.get("text") or r.get("stderr_snippet") or "").split())[:240]


def staged_details(host: str) -> dict:
    """The staged failure: a config with a made-up `module load` before any proposal, marked STAGED_FAILURE."""
    return {"ssh_host": host, "interface": "eth0", "scratch_root": "/home/{user}/.hpc-bridge", "partition": "debug",
            "env_setup": STAGED_ENV_SETUP, "display_name": STAGED_FAILURE}


def _proposal_details(r: dict) -> dict:
    """The proposal as the confirming details=: env_setup left out (the server keeps the proposed one — the tool's own
    instruction), unset fields dropped."""
    return {k: v for k, v in dict(r.get("proposed_details") or {}).items() if v is not None and k != "env_setup"}


def _account(r: dict) -> str | None:
    """The allocation to charge: the first non-GPU one connect listed (the site profile's: hpcb), else none."""
    accounts = [str(a.get("account")) for a in (r.get("allocations") or []) if isinstance(a, dict) and a.get("account")]
    return next((a for a in accounts if "gpu" not in a.lower()), accounts[0] if accounts else None)


def fresh_proposal_problem(r: dict) -> str | None:
    """Why the answer to the connect without details after the staged failure is not a fresh, module-based proposal
    that names what failed — None when it is."""
    notice = " ".join(str(r.get("notice") or "").split())
    if r.get("phase") != "proposed_facility_details":
        if r.get("phase") == "failed" and STAGED_MODULE in notice and "failed to come up" not in notice:
            return f"it ran the failed details= again (the bug this cell guards): {notice[:240]!r}"
        return f"it did not propose afresh: phase={r.get('phase')!r} {notice[:200]!r}"
    env = str((r.get("proposed_details") or {}).get("env_setup") or "")
    if STAGED_MODULE in env or "module load" not in env:
        return f"the proposal's env_setup is not a module-based one of its own: {env[:160]!r}"
    if "failed to come up" not in notice or STAGED_MODULE not in notice:
        return f"the proposal does not say what failed before it: {notice[:240]!r}"
    return None


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host = ops.facility, ops.ssh_host
    # 1. the staged failure: what the agent sent first on 2026-10-16
    bad = await ops.call("connect_facility", facility=fid, ssh_host=host, details=staged_details(host))
    if bad.get("phase") != "failed" or STAGED_MODULE not in str(bad.get("notice") or ""):
        ops.abort(f"the staged env_setup did not fail on the login node naming {STAGED_MODULE}: "
                  f"phase={bad.get('phase')!r} {_brief(bad)!r}")
    # 2. the connect without details that the failure's advice names: a fresh proposal, never the failed config again
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if (why := fresh_proposal_problem(r)) is not None:
        ops.abort(f"after the failed details=, connect_facility without details: {why}")
    proposed = str((r.get("proposed_details") or {}).get("env_setup") or "")
    ops.say(f"The made-up `{STAGED_ENV_SETUP}` failed on the login node; the connect without details proposed afresh, "
            f"with the site's modules: {proposed[:120]!r}. Accepting the proposal.")
    # 3. the proposal confirmed, the login shape brought up on it
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=_proposal_details(r))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up on the proposal: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(POLL_S)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    account = _account(r)
    # 4. a block on the proposal's default (cheapest) partition, spend authorised by the prompt
    args: dict = {"shape": "compute", "confirm_spend": True, **({"account": account} if account else {})}
    e = await ops.call("ensure_endpoint_up", **args)
    poll = {k: v for k, v in args.items() if k != "confirm_spend"}
    deadline = time.monotonic() + WARM_WAIT_S
    while e.get("status") != "up":
        if e.get("status") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the block never answered its check: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", **poll)
    h = await ops.call("run_shell", shape="compute", command=COMMAND)
    if h.get("phase") != "complete":
        ops.abort(f"`{COMMAND}` did not complete on the block: phase={h.get('phase')!r} {_brief(h)!r}")
    out = " ".join(str(h.get("stdout") or "").split())
    ops.say(f"On the compute node: {out[:200]!r}. Stopping the endpoint.")
    # 5. stop, until it says down
    s = await ops.call("stop_endpoint")
    for _ in range(STOP_RETRIES):
        if s.get("status") != "draining":
            break
        await ops.sleep(POLL_S)
        s = await ops.call("stop_endpoint")
    return (f"The made-up details= failed and was not run again; the fresh proposal ({proposed[:80]!r}) came up, the "
            f"compute node said {out[:160]!r}, and stop_endpoint answered {s.get('status')!r} "
            f"(spend_stopped={s.get('spend_stopped')!r}).")
