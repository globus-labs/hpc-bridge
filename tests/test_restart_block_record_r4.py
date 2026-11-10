"""Review round 4 of the restart block record (on d5ff14b), test-first, from the regression sweep's probes. In conftest's
isolated state.

When the record of billed blocks could not be read (a "database is locked" past busy_timeout), connect and the billed
start failed OPEN: `verify` stored an empty unread view (wiping a block an earlier read had verified), `connect_note`
said nothing of an unread view (so connect said "Nothing is started or billed yet" while job 40 ran), and `unread()`
took any stored view for a read one — a yes on other terms started job 41 unasked. Now a failed read keeps the last
good blocks, unverified, and marks the view unread; connect says the record could not be read and that an earlier
block may still bill; the billed start answers needs_confirmation, never a fresh block; a later good read clears it.
"""
from __future__ import annotations

from dataclasses import replace

from hpc_bridge import earlier
from hpc_bridge.server import _ensure_endpoint_up
from tests.test_restart_block_record import EID, _answer, _connect, _listing, _server, _session_1, _Slurm
from tests.test_server import _scripted_login

LOCKED = "OperationalError: database is locked"


def _computes(app) -> list:
    return [u for u in app.built if u.get("compute")]


def _unreadable(monkeypatch) -> None:
    monkeypatch.setattr(earlier, "_records", lambda app, eid=None: ([], LOCKED))


async def test_an_unreadable_record_at_connect_never_says_nothing_is_billed(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    _unreadable(monkeypatch)
    s2 = _server(monkeypatch, fac)
    res, _seen = await _connect(s2, _listing(n=n))
    assert "Nothing is started or billed yet" not in res.notice, res.notice
    assert "could not be read" in res.notice and "database is locked" in res.notice and "may still bill" in res.notice


async def test_an_unreadable_record_at_a_yes_starts_nothing(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    _unreadable(monkeypatch)
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing(n=n))
    s2.compute._canary = _answer("41")
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert st.status == "needs_confirmation" and not _computes(s2), (st.status, st.block_job_id, st.notice)
    assert "Nothing was started" in st.notice and "database is locked" in st.notice


async def test_a_failed_reread_keeps_the_known_block_and_a_good_read_clears_it(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    res, _seen = await _connect(s2, _listing(n=n))
    assert "block job 40 (RUNNING)" in res.notice                      # read fine at connect
    s2.earlier[EID] = replace(s2.earlier[EID], at=s2.earlier[EID].at - 120)   # older than VIEW_TTL_S
    real, fails = earlier._records, [2]

    def flaky(app, eid=None):
        if fails[0] > 0:
            fails[0] -= 1
            return [], LOCKED
        return real(app, eid)

    monkeypatch.setattr(earlier, "_records", flaky)
    s2.compute._canary = _answer("41")
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert st.status == "needs_confirmation" and not _computes(s2), (st.status, st.block_job_id, st.notice)
    assert "block job 40" in st.notice and "database is locked" in st.notice   # kept from the last good read
    # the record reads again: the view is read afresh (never left unread), and the yes after that question is informed
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert [k for k, _c in log] == ["probe"] and s2.earlier[EID].unread is None
    assert _computes(s2) and st.status in ("up", "provisioning"), (st.status, st.notice)
