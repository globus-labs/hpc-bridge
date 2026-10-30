"""A cached config whose bring-up FAILED must not keep winning in the session — not over an explicit other ssh_host,
and not after the user deleted its record — the Perlmutter report, items 1 (second half) and 3 (plugin 0.1.20,
2026-10-29). Fake cluster, `site` profile; scripted (no model). A RED proof: it fails on main, by design, until the fix.

On Perlmutter a config cached in ~/.hpc-bridge/facilities.json failed to come up; re-calling connect_facility with
ssh_host="perlmutter.nersc.gov" gave the SAME failure (the kept config's host, silently), and so did a call after the
user removed the record from facilities.json by hand. A config read from the cache is put in `app.session_facilities`
(connect.py) and never leaves it — not when its bring-up fails, not when its record is removed — and that session entry
wins over every other rung, the ssh_host argument included. Only a new facility id got through.

    agentic/fakecluster/bin/up.sh --profile site
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=site ./agentic/run_smoke.sh kept_entry_after_failure

The world: SEED_FACILITY_CACHE writes two "proven" records left from an earlier plugin's run — one for each login
node's own name — whose env_setup sources a venv that no longer exists (RETIRED: the bring-up's first step that runs
it fails on the login node, the cause `retired-gce-venv` in its words). The SCRIPT runs two independent cases, each
under its own facility id, so neither's outcome can shape the other's:
(a) `<id>-a`: connect with ssh_host=login01.hpcb.test (the cached record: it fails — the premise), then again with an
    explicit, DIFFERENT ssh_host=login → graded `explicit_host_not_ignored`: the call probes `login` (a proposal) or
    refuses naming it — never the kept config's bring-up on login01 again;
(b) `<id>-b`: connect with ssh_host=login02.hpcb.test (fails — the premise), the record deleted from facilities.json in
    the jail (the user's hand edit), then the same connect again → graded `removed_record_resolved_afresh`: the facility
    is resolved afresh — the probe's proposal for login02 — never the deleted config's bring-up.
"""
from __future__ import annotations

from byo_script import PROPOSED, brief, cached_hosts, forget_cached
from cluster_ops import token_store_cleanup_cmd
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"login_nodes": 2, "mfa": "none"}
KIND = "regression"
SUMMARY = ("a cached config whose bring-up failed does not win over an explicit other ssh_host, nor after its record "
           "is deleted (Perlmutter report 1b, 3)")
TAGS = ["fake", "site", "cache", "session", "scripted", "perlmutter-report"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = False
TEARDOWN = "delete"

PROMPT = ("(Scripted — no agent reads this.) Connect two facilities whose cached configs fail; then name another host "
          "for one, and delete the other's cached record and connect it again.")

HOST_A, HOST_A_NEW = "login01.hpcb.test", "login"
HOST_B = "login02.hpcb.test"
MARKER = "retired-gce-venv"
RETIRED = f'. "$HOME/hpc-bridge/{MARKER}/bin/activate"'


def _record(host: str) -> dict:
    return {"ssh_host": host, "interface": "eth0", "env_setup": RETIRED, "scratch_root": "/home/{user}/.hpc-bridge",
            "partition": "compute", "scheduler": "slurm"}


SEED_FACILITY_CACHE = {HOST_A: _record(HOST_A), HOST_B: _record(HOST_B)}
# the user connected to each node by its own name before (that is how the records were proven): its key is trusted
LOCAL_SETUP = [f"ssh-keyscan -T 10 {HOST_A} {HOST_B} 2>/dev/null >> ~/.ssh/known_hosts "
               f"&& ssh-keygen -F {HOST_A} >/dev/null && ssh-keygen -F {HOST_B} >/dev/null"]


def _kept_failed(r: dict) -> bool:
    return r.get("phase") == "failed" and MARKER in str(r.get("notice") or "")


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fa, fb = f"{ops.facility}-a", f"{ops.facility}-b"
    # (a) an explicit, different ssh_host after the cached config failed
    r = await ops.call("connect_facility", facility=fa, ssh_host=HOST_A)
    if not _kept_failed(r):
        ops.abort(f"the cached config for {HOST_A} did not fail as seeded: {r.get('phase')!r} {brief(r)!r}")
    a = await ops.call("connect_facility", facility=fa, ssh_host=HOST_A_NEW)
    ops.say(f"(a) ssh_host={HOST_A_NEW!r} after the cached config failed: {a.get('phase')!r} {brief(a, 300)!r}")
    # (a)'s bring-up seeded the pool user's Globus token store on the shared home before its config failed; (b) under
    # another facility id would meet it as a store it cannot prove its own — removed, as between cells (run.py)
    await ops.world(token_store_cleanup_cmd(), timeout=60)
    # (b) the record deleted by hand, then the same connect again
    r = await ops.call("connect_facility", facility=fb, ssh_host=HOST_B)
    if not _kept_failed(r):
        ops.abort(f"the cached config for {HOST_B} did not fail as seeded: {r.get('phase')!r} {brief(r)!r}")
    if not forget_cached(HOST_B) or HOST_B in cached_hosts():
        ops.abort(f"could not delete {HOST_B}'s record from facilities.json (have: {cached_hosts()})")
    ops.say(f"deleted {HOST_B}'s record from facilities.json (left: {cached_hosts()})")
    b = await ops.call("connect_facility", facility=fb, ssh_host=HOST_B)
    ops.say(f"(b) after the record was deleted: {b.get('phase')!r} {brief(b, 300)!r}")
    return f"(a) {a.get('phase')!r}: {brief(a, 160)} | (b) {b.get('phase')!r}: {brief(b, 160)}"


def _calls(t: Trace, facility_suffix: str, host: str) -> list[dict]:
    return [dict(c.result or {}) for _i, c in t.named("connect_facility")
            if str((c.input or {}).get("facility", "")).endswith(facility_suffix)
            and (c.input or {}).get("ssh_host") == host]


def explicit_host_not_ignored(t: Trace) -> Result:
    """(a) After the cached config for login01 failed, connect_facility(ssh_host="login") runs nothing of that config:
    it probes `login` (a proposal for it) or refuses, naming the host — never the kept config's bring-up again."""
    rs = _calls(t, "-a", HOST_A_NEW)
    if not rs:
        return Result("explicit_host_not_ignored", False, "premise not met: no connect naming the other host")
    r = rs[0]
    notice = str(r.get("notice") or "")
    proposed = (r.get("proposed_details") or {}).get("ssh_host")
    if _kept_failed(r) or HOST_A in notice:
        return Result("explicit_host_not_ignored", False,
                      f"ssh_host={HOST_A_NEW!r} was ignored: the kept config for {HOST_A} ran again: {notice[:400]!r}")
    ok = (r.get("phase") == PROPOSED and proposed == HOST_A_NEW) or (r.get("phase") == "failed" and HOST_A_NEW in notice)
    return Result("explicit_host_not_ignored", ok, (f"ok: {r.get('phase')!r} for {HOST_A_NEW!r}" if ok else
                  f"neither a proposal for nor a refusal naming {HOST_A_NEW!r}: {r.get('phase')!r} {notice[:300]!r}"))


def removed_record_resolved_afresh(t: Trace) -> Result:
    """(b) After the user deleted the failed config's record, the same connect resolves the facility afresh: the probe
    of login02 (a proposal) — never the deleted config's bring-up."""
    rs = _calls(t, "-b", HOST_B)
    if len(rs) < 2:
        return Result("removed_record_resolved_afresh", False, "premise not met: no connect after the record was deleted")
    r = rs[1]
    if _kept_failed(r):
        return Result("removed_record_resolved_afresh", False,
                      f"the deleted config was brought up again: {str(r.get('notice') or '')[:400]!r}")
    ok = r.get("phase") == PROPOSED and (r.get("proposed_details") or {}).get("ssh_host") == HOST_B
    return Result("removed_record_resolved_afresh", ok, ("ok: the probe proposed details for login02" if ok else
                  f"not resolved afresh: {r.get('phase')!r} {brief(r, 300)!r}"))


EXTRA_INVARIANTS = [explicit_host_not_ignored, removed_record_resolved_afresh]
EXPECT_OK = ["agent_engaged", "explicit_host_not_ignored", "removed_record_resolved_afresh"]
