"""RED / TDD spec — reconnecting must REUSE the online endpoint, and SAY so (issue #20).

hpc-bridge's SSH-once keystone: `find_online_endpoint` reuses a still-online endpoint (by
identity+name, over the Globus web service — zero SSH). But reuse is INVISIBLE in the tool
results: `connect_facility` returns the same phases whether it bootstrapped over SSH or
reused over the web, so neither an agent (on an MFA facility: "will this re-auth me?") nor
this harness can observe it. The spec: `connect_facility`'s result must carry a
`reused: true` signal (field or notice) when an online endpoint was reused.

In-process version (this scenario): connect twice to the SAME facility id in one session.
Each connect resets state and re-runs bootstrap, so the second one calls
`find_online_endpoint` (Globus web service, by identity+name) and reattaches to the manager
the first connect started — zero SSH — and now returns `reused=True`. The config cache
(`session_facilities`) only skips catalog re-resolution; the endpoint reuse itself is the
find-online path. The cross-restart version (a keep-chain across two containers, where even
the in-process state is gone — the full #20 cache problem) is deferred until suite chain
support; `TEARDOWN="keep"` + a stable `FACILITY_ID` are the waiting hooks.

STATUS: GREEN — the signal is IMPLEMENTED (#20 — `ConnectFacilityResult.reused` + notice) and
confirmed live. The grader (`reuse_signalled`) keys on the `reused` FIELD only and requires a fresh
bring-up → login work → reconnect structure (see its docstring for how #39 shapes the first connect).
`reuse_answer_matches` (gated) grades the answer the prompt asks for — "REUSED or a new one" — against what the tool
said about that reconnect. Login-shape only (no billed block): cheap (~3 min) and fast.
"""
import re

from invariants import _UP_PHASES, Result, Trace, _typo, affirmed_match, said_after

PROMPT = (
    "Connect me to the HPC facility with login host `{ssh_host}` — use "
    "facility id `{facility}` — and run `hostname` on the LOGIN shape (do not provision "
    "any billed compute block). Then, to test reconnection, connect to the SAME facility "
    "id a second time and tell me explicitly whether the existing endpoint was REUSED or "
    "a new one was set up. Do not tear anything down.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions: accept discovered "
    "facility config yourself."
)

USER_GOAL = ""      # autonomous
PERSONA = None
KIND = "regression"
SUMMARY = "intra-agent reuse: within ONE session, a second connect_facility reattaches to the endpoint the first stood up"
TAGS = ["reuse", "zero-ssh", "intra-agent"]


# The product's exact intra-session reconnect notice (connect._reuse_note, #78). Pinned here as a CONTRACT string:
# a reword in the product flips this grader, which is the point — the harness must notice.
INTRA_SESSION_RECONNECT = "reconnected to the endpoint this session started"


def reuse_signalled(t: Trace) -> Result:
    """A genuine bring-up first, work on the login shape, then a RECONNECT that reattached instead of
    re-probing or re-bootstrapping. Structure, per the prompt: connect → `hostname` on the login shape →
    connect again.

    The reconnect's evidence, since #78 (2026-09-04) — "a session's own bootstrap is proven, not a reuse":
    - `reused=True` is the FIELD for an endpoint from a PREVIOUS session (a different process found it online);
    - an endpoint THIS session stood up reads `reused=False` with the notice INTRA_SESSION_RECONNECT — the
      intra-session case this scenario tests. Either is a reattach. The first block-tier run after #78 failed
      this grader on the field alone (2026-09-05): the product had changed by design, the grader had not.
    - Structurally, no connect AFTER the first completed login-shape work may be a discovery phase
      (`proposed_facility_details` / `needs_facility_details` / `needs_preauth`): that would be a re-probe or a
      re-bootstrap, i.e. the reconnect did not reattach.
    A `reused=False` connect must exist BEFORE the first up-phase connect (the fresh bring-up)."""
    connects = t.named("connect_facility")
    ups = [(i, c) for i, c in connects if str((c.result or {}).get("phase")) in _UP_PHASES]
    if not ups:
        return Result("reuse_signalled", False, "no connect ever reached the endpoint")
    first_up_i = ups[0][0]
    fresh = any(i < first_up_i and not bool((c.result or {}).get("reused")) for i, c in connects)
    work = [
        i for i, c in t.named("run_shell")
        if str(c.input.get("shape")) == "login" and str((c.result or {}).get("phase")) == "complete"
    ]
    if not work:
        return Result("reuse_signalled", False,
                      "no completed login-shape run_shell — the first connection was never used")
    recon_i, recon = ups[-1]
    after_work = recon_i > min(work)
    res = recon.result or {}
    reattached_field = bool(res.get("reused"))
    reattached_notice = str(res.get("notice") or "").startswith(INTRA_SESSION_RECONNECT)
    reprobed = [i for i, c in connects if i > min(work)
                and str((c.result or {}).get("phase")) in ("proposed_facility_details", "needs_facility_details", "needs_preauth")]
    ok = fresh and after_work and (reattached_field or reattached_notice) and not reprobed
    how = ("reused=True (an earlier session's endpoint)" if reattached_field
           else "the intra-session reconnect notice" if reattached_notice else "NEITHER the reused field NOR the notice")
    return Result(
        "reuse_signalled", ok,
        f"ok: fresh bring-up, work on the login shape, then a reconnect that reattached via {how}" if ok else
        f"fresh_bringup_first={fresh} (want True); reconnect (call {recon_i}) after first work={after_work} (want True); "
        f"reattached via {how} (want the field or the notice); re-probe after work at calls {reprobed} (want none)",
    )

# How the answer says each way. Reused: "reused", "reattached", "the same / existing endpoint", "reconnected to the
# endpoint (this session started)". New: "a new one / a new endpoint was set up / created / registered / bootstrapped".
# Each only where its own clause does not deny it (invariants.affirmed_match): "REUSED, not a new one", "no new endpoint
# was set up" say reused.
_SAYS_REUSED = re.compile(r"\breus(?:ed|ing)\b|\breattach\w*|\b(?:same|existing)(?: still-online| online)? endpoint\b"
                          r"|\breconnected to the (?:same |existing )?endpoint\b")
_SAYS_NEW = re.compile(r"\b(?:a )?new (?:one|endpoint)\b[^.\n]{0,40}?\b(?:set up|created|registered|bootstrapped|started|"
                       r"stood up|provisioned)\b|\b(?:set up|created|registered|bootstrapped|stood up) a (?:new|fresh|second) "
                       r"(?:one|endpoint)\b")


def reuse_answer_matches(t: Trace) -> Result:
    """The prompt asks "tell me explicitly whether the existing endpoint was REUSED or a new one was set up": after the
    reconnect (the last up-phase connect, as reuse_signalled reads it) the agent's answer matches the tool's. The tool
    said reused when the reconnect's `reused` field is true (an earlier session's endpoint) or its notice is the
    intra-session reconnect (`reused` false by design since #78 — every agent in the 2026-10-22 sweep said REUSED and
    noted the false field); otherwise a new endpoint. The agent's words before the reconnect are not read
    (invariants.said_after)."""
    ups = [(i, c) for i, c in t.named("connect_facility") if str((c.result or {}).get("phase")) in _UP_PHASES]
    if len(ups) < 2:
        return Result("reuse_answer_matches", False, "no reconnect to answer about")
    i, recon = ups[-1]
    res = recon.result or {}
    reused = bool(res.get("reused")) or str(res.get("notice") or "").startswith(INTRA_SESSION_RECONNECT)
    blob = _typo("\n".join(said_after(t, i)))
    said_reused, said_new = affirmed_match(blob, _SAYS_REUSED), affirmed_match(blob, _SAYS_NEW)
    truth = "reused" if reused else "new"
    ok = (said_reused is not None and said_new is None) if reused else (said_new is not None and said_reused is None)
    return Result("reuse_answer_matches", ok,
                  f"ok: the tool said {truth} (call {i}), and so did the agent" if ok
                  else f"the tool said {truth} (call {i}); the agent said reused: {said_reused!r}, new: {said_new!r}")


EXTRA_INVARIANTS = [reuse_signalled, reuse_answer_matches]

EXPECT_OK = [
    "reuse_signalled",              # the spec: reuse must be observable
    "reuse_answer_matches",         # ...and the agent's answer says what the tool said
    "no_raw_ssh_after_endpoint_up",
    "spend_not_unprompted",
    "ends_with_stop",               # login-only: trivially satisfied (no billed block)
]

TEARDOWN = "delete"
