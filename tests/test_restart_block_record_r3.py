"""Review round 3 of the restart block record (on 4c40c5c), test-first. In conftest's isolated state.

A. One choke point before a billed block starts: the record of the endpoint a bring-up just learnt is read there
   (warmth._provision, by that endpoint id), whatever path led to the id. Round 2 read it only for a startup pin whose
   facility string matched exactly: a connect whose login bring-up failed (app.machine set, no endpoint id) and a pin by
   the entry's subject (`example:mine` against a record of `mine`) both started a second block, unread.
B. Liveness as one spec (the table in `test_b_the_liveness_spec`): "unknown == this machine" and a namespace test that
   needed both sides known released another server's live block (a Mac reading a Linux container's record; a Linux
   container with no /etc/machine-id reading a Mac's).
C. A cold stop that could not read the record again answered `down` "no endpoint was up" while job 40 ran: it fails
   closed (`draining`, with the reason).
D. Nits: combined answers deduplicate jobs by `job_id`; a failed bring-up answer keeps the call's terms; on a facility
   endpoint a batch whose window a dead server's presumed-lost rows fill says what unblocks it.
"""
from __future__ import annotations

import itertools
import subprocess
import sys

from hpc_bridge import batch, earlier
from hpc_bridge.catalog.entry import CatalogEntry
from hpc_bridge.context import AppCtx
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.ledger import Ledger, Owner
from hpc_bridge.models import EndpointStatus, HandoffRef, JobInfo
from hpc_bridge.profile import Profile
from hpc_bridge.server import _ensure_endpoint_up, _stop_endpoint
from tests.fakes import fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_restart_block_record import EID, _listing, _open, _server, _session_1, _Slurm
from tests.test_restart_block_record_r2 import _Counted, _pinned, _set
from tests.test_server import _scripted_login

# --- A. the record of the endpoint a bring-up learnt is read before its block --------------------------------------


def _entry() -> CatalogEntry:
    return CatalogEntry.model_validate_json(_open(EID)[0]["entry"])


async def _connect_failed(monkeypatch, fac):
    """Session 1 left job 40; a new server connected the same facility, and its login bring-up failed: the facility is
    bound (app.machine, its entry), no endpoint id (connect.py's failure path)."""
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    s2.machine, s2.bound_entry = "mine", _entry()
    assert s2.state.endpoint_id is None
    return s2, n


async def test_a_after_a_failed_connect_a_yes_on_other_terms_reads_the_record_and_asks_first(monkeypatch):
    s2, n = await _connect_failed(monkeypatch, _Slurm())
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert [k for k, _c in log] == ["probe"]                                    # read by the endpoint id it learnt
    assert not [u for u in s2.built if u.get("compute")], "a second block started on a record nobody read"
    assert st.status == "needs_confirmation" and "block job 40 (RUNNING)" in st.notice
    assert "second block" in st.notice


async def test_a_after_a_failed_connect_a_yes_on_its_terms_reuses_the_block(monkeypatch):
    s2, n = await _connect_failed(monkeypatch, _Slurm())
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] == n and "Reused block job 40" in st.notice


async def test_a_a_pin_by_the_entrys_subject_names_the_record_and_reads_it_after_the_yes(monkeypatch):
    fac = _Counted()
    _s1, n = await _session_1(monkeypatch, fac)
    subject = _entry().subject
    assert subject != "mine"                                  # the record's facility string is "mine"
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", subject)
    fac.attempts, fac.manager_up = 0, False
    s2 = _server(monkeypatch, fac)
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    ask = await _ensure_endpoint_up(s2)                       # the spend question: named, nothing brought up
    assert fac.attempts == 0 and log == [] and "block job 40" in ask.notice
    fac.manager_up = None                                     # the yes brings it up (the facility's own bring-up)
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert [k for k, _c in log] == ["probe"] and "Reused block job 40" in st.notice
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] == n


# --- B. liveness: one spec ----------------------------------------------------------------------------------------------


def _parse(spec: str) -> list[tuple[list[set[str] | None], str]]:
    rows = []
    for line in spec.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 5 or cells[0] in ("machine ids", "") or set(cells[0]) <= {"-"}:
            continue
        conds = [None if c == "any" else {x.strip() for x in c.split(",")} for c in cells[:4]]
        rows.append((conds, cells[4]))
    return rows


_MACHINES = {"known-same": ("M1", "M1"), "known-different": ("M1", "M2"), "reader-unknown": ("", "M1"),
             "record-unknown": ("M1", None), "both-unknown": ("", None)}       # (reader's _MACHINE, the record's)
_NAMESPACES = {"same": ("N1", "N1"), "different": ("N1", "N2"), "record-only": (None, "N1"),
               "reader-only": ("N1", None), "both-unknown": (None, None)}       # (reader's, the record's)
_HOSTS = {"same": ("hostA", "hostA"), "different": ("hostA", "hostB")}


def test_b_the_liveness_spec(monkeypatch):
    """Whose a record of ANOTHER session is (its own session's is "mine"), from the reader's side — "earlier" (a gone
    server of this machine: reused or released here), "live" (one that still runs here: its own stop releases it), or
    "elsewhere" (whether it runs cannot be told: read through the scheduler where it can be — closed when gone, else
    named with the user's own cancel, `draining` — never reused or released here). A machine id is known when it could
    be read (/etc/machine-id, macOS IOPlatformUUID); a pid namespace on Linux only. The pid is "dead" when it is not
    running or runs with another start time (`Ledger.holder_alive`).

    | machine ids                                  | pid namespaces                      | hostnames | pid   | whose     |
    |----------------------------------------------|-------------------------------------|-----------|-------|-----------|
    | known-different                              | any                                 | any       | any   | elsewhere |
    | known-same                                   | same, both-unknown                  | any       | dead  | earlier   |
    | known-same                                   | same, both-unknown                  | any       | alive | live      |
    | known-same                                   | different, record-only, reader-only | any       | any   | elsewhere |
    | reader-unknown, record-unknown, both-unknown | same, both-unknown                  | same      | dead  | earlier   |
    | reader-unknown, record-unknown, both-unknown | same, both-unknown                  | same      | alive | live      |
    | reader-unknown, record-unknown, both-unknown | same, both-unknown                  | different | any   | elsewhere |
    | reader-unknown, record-unknown, both-unknown | different, record-only, reader-only | any       | any   | elsewhere |
    """
    spec = _parse(test_b_the_liveness_spec.__doc__ or "")
    assert len(spec) == 8
    app = AppCtx(facility=None, profile=Profile())
    app.ledger = Ledger.open()
    seen = 0
    for (m, (my_m, rec_m)), (ns, (my_ns, rec_ns)), (h, (my_h, rec_h)), pid in itertools.product(
            _MACHINES.items(), _NAMESPACES.items(), _HOSTS.items(), ("alive", "dead")):
        case = (m, ns, h, pid)
        rows = [want for conds, want in spec if all(c is None or v in c for c, v in zip(conds, case, strict=True))]
        assert len(rows) == 1, f"the spec must name {case} exactly once: {rows}"
        monkeypatch.setattr(earlier, "_MACHINE", my_m)
        monkeypatch.setattr(earlier, "_me_ns", lambda my_ns=my_ns: my_ns)
        monkeypatch.setattr(earlier, "_me", lambda my_h=my_h: Owner(my_h, 1, None))
        app.ledger.holder_alive = lambda _o, pid=pid: pid == "alive"
        r = {"session": "another", "owner_machine": rec_m, "owner_ns": rec_ns, "owner_host": rec_h,
             "owner_pid": 4242, "owner_start": None}
        assert earlier._whose(app, r) == rows[0], case
        seen += 1
    assert seen == 100


def test_b_a_liveness_read_that_fails_is_live(monkeypatch):
    app = AppCtx(facility=None, profile=Profile())
    app.ledger = Ledger.open()

    def boom(_o):
        raise OSError("ps failed")

    app.ledger.holder_alive = boom
    r = {"session": "another", "owner_machine": earlier._machine_id(), "owner_ns": earlier._me_ns(),
         "owner_host": earlier._me().host, "owner_pid": 4242, "owner_start": None}
    assert earlier._whose(app, r) == "live"


async def _cold_stop_left_alone(monkeypatch, fac) -> EndpointStatus:
    s2 = _server(monkeypatch, fac, bound=False)               # the earlier server reads as dead by its pid here
    log = _scripted_login(monkeypatch, probes=[_listing()])
    st = await _stop_endpoint(s2)
    assert [k for k, _c in log] == ["probe"], "another server's live block was released"
    assert st.status == "draining" and st.spend_stopped is not True and "scancel 40" in st.notice
    return st


async def test_b_a_mac_never_releases_a_linux_containers_block(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine=None, owner_ns="pid:[4026532198]", owner_host="3f2a9c1d0b7e")    # no machine-id in there
    monkeypatch.setattr(earlier, "_MACHINE", "A1B2C3D4-MAC")
    monkeypatch.setattr(earlier, "_me_ns", lambda: None)
    await _cold_stop_left_alone(monkeypatch, fac)


async def test_b_a_linux_container_with_no_machine_id_never_releases_a_macs_block(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine="A1B2C3D4-MAC", owner_ns=None, owner_host="the-mac.local")
    monkeypatch.setattr(earlier, "_MACHINE", "")               # read, and unknown (the project's image)
    monkeypatch.setattr(earlier, "_me_ns", lambda: "pid:[4026532198]")
    await _cold_stop_left_alone(monkeypatch, fac)


def _darwin(monkeypatch, tmp_path, run=None, *, ioreg: bool = True, which: str | None = None):
    """A Mac with no Linux machine-id file: ioreg at /usr/sbin/ioreg (`ioreg`), else on PATH at `which`; `run` stands
    for subprocess.run. Returns the argv lists run was called with."""
    calls: list[list[str]] = []
    monkeypatch.setattr(earlier, "_MACHINE_ID_FILES", (str(tmp_path / "absent"),))
    monkeypatch.setattr(sys, "platform", "darwin")
    found = {"/usr/sbin/ioreg": "/usr/sbin/ioreg" if ioreg else None, "ioreg": which}
    monkeypatch.setattr(earlier.shutil, "which", lambda name: found.get(name))

    def fake(argv, **kw):
        calls.append(list(argv))
        if run is None:
            raise AssertionError("ioreg run when none is there")
        return run(argv, **kw)

    monkeypatch.setattr(earlier.subprocess, "run", fake)
    return calls


def _out(stdout: str):
    return lambda argv, **kw: subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")


def _raises(exc: BaseException):
    def run(argv, **kw):
        raise exc
    return run


def test_b_the_machine_id_from_ioreg_by_its_absolute_path(monkeypatch, tmp_path):
    calls = _darwin(monkeypatch, tmp_path, _out('  "IOPlatformUUID" = "564D1234-ABCD-0000-1111-222233334444"\n'))
    assert earlier._read_machine_id() == "564D1234-ABCD-0000-1111-222233334444"
    assert calls[0][0] == "/usr/sbin/ioreg"


def test_b_ioreg_off_its_usual_path_is_found_on_path(monkeypatch, tmp_path):
    calls = _darwin(monkeypatch, tmp_path, _out('"IOPlatformUUID" = "X-1"'), ioreg=False, which="/opt/bin/ioreg")
    assert earlier._read_machine_id() == "X-1" and calls[0][0] == "/opt/bin/ioreg"


def test_b_no_ioreg_at_all_is_unknown(monkeypatch, tmp_path):
    calls = _darwin(monkeypatch, tmp_path, None, ioreg=False, which=None)
    assert earlier._read_machine_id() is None and calls == []


def test_b_an_ioreg_that_fails_is_unknown_never_a_crash(monkeypatch, tmp_path):
    for run in (_raises(FileNotFoundError("ioreg")), _raises(subprocess.TimeoutExpired(["ioreg"], 5)),
                _raises(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")),
                _raises(PermissionError("denied")), _out("garbage, no UUID here"), _out("")):
        _darwin(monkeypatch, tmp_path, run)
        assert earlier._read_machine_id() is None


def test_b_the_linux_machine_id_file(monkeypatch, tmp_path):
    f = tmp_path / "machine-id"
    monkeypatch.setattr(earlier, "_MACHINE_ID_FILES", (str(f),))
    monkeypatch.setattr(sys, "platform", "linux")
    for content, want in ((b"0123abcd\n", "0123abcd"), (b"", None), (b"uninitialized\n", None), (b"\xff\xfe\x00", None)):
        f.write_bytes(content)
        assert earlier._read_machine_id() == want, content


# --- C. a cold stop that cannot read the record again fails closed -----------------------------------------------------


async def test_c_a_record_read_that_fails_in_the_cold_stop_is_draining_never_down(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine="0f0e0d0c-another-machine")             # read first through the scheduler, then read again
    real = earlier._records

    def locked(app, eid=None):
        return real(app) if eid is None else ([], "OperationalError: database is locked")

    monkeypatch.setattr(earlier, "_records", locked)
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, probes=[_listing()])
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and st.spend_stopped is not True, st.notice
    assert "job 40" in st.notice and "database is locked" in st.notice and "no endpoint was up" not in st.notice


# --- D. nits -------------------------------------------------------------------------------------------------------------


def test_d_combined_and_questions_keep_each_job_once_by_its_id():
    def part(status, *ids):
        return EndpointStatus(status=status, block_state="cold", notice=status, jobs=[JobInfo(job_id=i) for i in ids],
                              handoffs=[HandoffRef(handoff_id="h-" + ids[0], state="exported")])

    out = earlier.combined([part("down", "job-1"), part("draining", "job-1", "job-2")])
    assert [j.job_id for j in out.jobs] == ["job-1", "job-2"] and len(out.handoffs) == 2
    q = earlier._questions([part("needs_confirmation", "job-1"), part("needs_confirmation", "job-1", "job-2")])
    assert [j.job_id for j in q.jobs] == ["job-1", "job-2"]


async def test_d_a_failed_bring_up_keeps_the_calls_terms_in_its_answer(monkeypatch):
    fac = _Counted()
    s2, _n = await _pinned(monkeypatch, fac, online=False)
    fac.fail = True
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert fac.attempts == 1 and st.status == "down" and "Permission denied" in st.notice
    assert st.partition == "other", "the call's partition was dropped unsaid"


def test_d_a_facility_endpoint_batch_held_by_presumed_lost_rows_says_what_unblocks_it():
    from tests.test_batch_phase1 import _owner

    app = AppCtx(facility=MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online")),
                 profile=Profile())
    led = app.ledger = Ledger.open()
    led.add_batch("b1", command="x", rows=[{"n": 0}, {"n": 1}], task_walltime_s=60, capacity=1, results_dir="/r",
                  idempotent=False, endpoint_id="mep", uec_hash="h", session_id="s", owner=_owner())
    assert len(led.reserve("b1", _owner(), 1, uec_hash="h", by_sender=False)) == 1
    led._con.execute("update task set state = 'presumed_lost' where state = 'in_flight'")
    text = batch._status_of(app, "b1").notice or ""
    assert "presumed lost" in text and "keeps its slot" in text and "cancel('b1')" in text
