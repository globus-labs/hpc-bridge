# tests/test_returning_user_fixes.py — what the returning-user cells (agentic/harness/returning.py, 2026-10-30) found:
#   A. a manager killed hard leaves its daemon.pid on the shared home; globus-compute's start refuses (exit 73, nothing on
#      either stream) while that file is younger than three heartbeats, and hpc-bridge gave up at once, quoting the
#      login shell's .bashrc noise as the cause (cells c03 c05 c12 c18 of the first table);
#   B. a manager reused over the web whose venv was deleted cannot start a user endpoint ("No module named 'jinja2'",
#      exit 73), and the notice said "fix the config/partition", then "likely REJECTED" (c09);
#   C. a confirmed configuration brought up on a REUSED endpoint was never cached, so the next session probed again (c14).
# Hermetic: ssh is a fake; the warm path runs on FakeFacility with fake runners.
import asyncio

import pytest

from hpc_bridge import binding, connect, notices
from hpc_bridge.facility import remote
from hpc_bridge.facility.base import ConfigStepFailed
from hpc_bridge.facility.remote import RemoteEndpointCLI, SshTarget
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up
from hpc_bridge.state import FacilityStore
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _Res

NAME = "hpc-bridge-fake-1793372043-16534"
EID = "85f323be-159c-4e62-a40a-aa74681fb602"
LISTED = f"| {EID} | Running | {NAME} |\n"
# what every remote step on the nersc profile prints first: the noisy .bashrc (Lmod's swaps, git's lock error)
NOISE = ("Lmod is automatically replacing \"PrgEnv-gnu/8.5.0\" with \"PrgEnv-nvidia/8.5.0\".\n"
         "The following have been reloaded with a version change:\n"
         "  1) cudatoolkit/12.2 => cudatoolkit/12.4     2) craype-accel-nvidia80 => craype-accel-nvidia80\n"
         "error: could not lock config file /global/homes/h/u/.gitconfig: File exists\n")
REFUSAL = ("2026-10-30 14:55:50,114650 ERROR MainProcess-33873 MainThread-281473584443424 "
           "globus_compute_endpoint.cli:705 _pidfile_check Another instance of this endpoint is running.  (Perhaps on "
           "another login node?)  Refusing to start.  If this is not correct, then remove the PID file before starting "
           "again.")


class _Login:
    """The login node as the start sees it: a remote clock the fake sleep moves, a pid file and its mtime, the
    endpoint's log, and what each `start` answers."""

    def __init__(self, *, mtime, starts, touch_every=None, hb=""):
        self.now, self.mtime, self.hb = 1000.0, mtime, hb
        self.starts_left = list(starts)
        self.touch_every = touch_every   # a live instance elsewhere refreshes its pid file this often
        self.started, self.calls, self.slept, self.up = 0, [], 0.0, False

    async def ssh(self, target, cmd, **kw):
        self.calls.append(cmd)
        if "endpoint start" in cmd:
            self.started += 1
            got = self.starts_left.pop(0)
            self.up = self.up or got[0] == 0
            return got
        if "endpoint list" in cmd:   # the manager is listed running once a start succeeded
            return (0, LISTED if self.up else "No endpoints configured!\n", NOISE)
        if "daemon.pid" in cmd:   # the pid file's probe
            pid = f"HPCB_PID_MTIME={int(self.mtime)}\n" if self.mtime is not None else ""
            return (0, f"{NOISE}HPCB_NOW={int(self.now)}\n{pid}HPCB_HB={self.hb}\nHPCB_LOG={REFUSAL}\nHPCB_PROBE_END\n",
                    NOISE)
        return (0, "", "")

    async def sleep(self, s):
        self.slept += s
        before = self.now
        self.now += s
        if self.touch_every is not None and int(before // self.touch_every) != int(self.now // self.touch_every):
            self.mtime = self.now   # the live instance touched its pid file


def _cli(monkeypatch, login):
    monkeypatch.setattr(remote, "ssh_exec", login.ssh)
    monkeypatch.setattr(remote, "_sleep", login.sleep)
    return RemoteEndpointCLI(SshTarget("login", "u", "k"), "true")


# ---- A: a start refused on a stale-but-fresh pid file -----------------------------------------------------------------

async def test_a_refused_start_waits_out_the_pid_files_window_then_starts(monkeypatch):
    # c18: the restart came 66 s after the dead manager's last touch; globus-compute treats the file as stale past
    # 3 × 30 s, and removes it itself then
    login = _Login(mtime=1000 - 66, starts=[(73, "", NOISE), (0, "HPCB_HOST=login01.example\nHPCB_ADDR=10.0.0.5\n", NOISE)])
    cli = _cli(monkeypatch, login)
    eid, host, _addr = await cli.start(NAME)
    assert (eid, host) == (EID, "login01.example") and login.started == 2
    assert 24 <= login.slept <= 24 + 30   # until the file is older than 90 s, not a fixed long sleep
    assert cli.start_note and "pid file" in cli.start_note and "90 s" in cli.start_note


async def test_a_pid_file_that_is_touched_meanwhile_is_a_live_instance_and_nothing_is_retried(monkeypatch):
    login = _Login(mtime=1000 - 10, starts=[(73, "", NOISE)], touch_every=15)
    cli = _cli(monkeypatch, login)
    with pytest.raises(RuntimeError) as e:
        await cli.start(NAME)
    msg = str(e.value)
    assert login.started == 1 and not isinstance(e.value, ConfigStepFailed)   # not the configuration's fault
    assert "running" in msg and "another login node" in msg and "Lmod" not in msg
    assert "rm " not in msg.replace("remove", "")   # never told to delete the pid file


async def test_a_window_too_long_to_wait_in_the_call_says_when_to_retry(monkeypatch):
    login = _Login(mtime=1000 - 5, starts=[(73, "", NOISE)], hb="300")   # 3 × 300 s: far past the call's budget
    cli = _cli(monkeypatch, login)
    with pytest.raises(RuntimeError) as e:
        await cli.start(NAME)
    assert login.started == 1 and login.slept == 0
    assert "connect_facility again in" in str(e.value) and "895 s" in str(e.value)


async def test_a_refused_start_that_printed_nothing_quotes_the_endpoint_log_never_the_bashrc(monkeypatch):
    login = _Login(mtime=None, starts=[(73, "", NOISE)])   # no pid file: another cause
    cli = _cli(monkeypatch, login)
    with pytest.raises(RuntimeError) as e:
        await cli.start(NAME)
    msg = str(e.value)
    assert "Lmod" not in msg and "gitconfig" not in msg and "cudatoolkit" not in msg
    assert "Another instance of this endpoint is running" in msg and "no output of its own" in msg


# ---- B: a reused manager whose venv is gone ---------------------------------------------------------------------------

LIVE_B = ("ComputeAPIError[SEMANTICALLY_INVALID]: Request payload failed validation: Unable to start user endpoint "
          "process for hpcbridge-test [exit code: 73; (ModuleNotFoundError) No module named 'jinja2']")


def test_a_user_endpoint_that_cannot_import_is_a_broken_venv_not_a_config_or_queue_problem():
    assert notices.broken_endpoint_env(LIVE_B) == "No module named 'jinja2'"
    assert notices.broken_endpoint_env("ImportError: cannot import name 'x' from 'parsl'") is not None
    assert notices.broken_endpoint_env("ComputeAPIError[FORBIDDEN]: no") is None
    suffix = notices._dispatch_error_suffix(CanaryResult(ok=False, error=LIVE_B))
    assert "fix the config/partition" not in suffix and "environment" in suffix and "jinja2" in suffix


class _VenvGoneFacility(FakeFacility):
    def __init__(self, *, repair_fails=None):
        super().__init__()
        self.workers = 1
        self.repaired = 0
        self.repair_fails = repair_fails
        self.fixed = False

    async def repair_endpoint_env(self, profile, endpoint_id):
        self.repaired += 1
        if self.repair_fails:
            raise RuntimeError(self.repair_fails)
        self.fixed = True
        return ("rebuilt hpc-bridge's endpoint venv $HOME/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11 (it does not "
                "exist), and restarted the endpoint on login01", endpoint_id)


def _venv_gone_app(fac):
    app = AppCtx(facility=fac, profile=Profile())

    def factory(eid, user_endpoint_config=None, **kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))
        ok = fac.fixed
        return _FakeRunner(eid, _Res(0, "", ""), canary_result=None if ok else CanaryResult(ok=False, error=LIVE_B))

    app.runner_factory = factory
    return app


async def test_a_broken_venv_seen_at_a_user_endpoints_start_is_repaired_once_and_the_block_comes_up():
    fac = _VenvGoneFacility()
    app = _venv_gone_app(fac)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert fac.repaired == 1 and res.status == "up", res.notice
    assert "rebuilt hpc-bridge's endpoint venv" in res.notice and "jinja2" in res.notice
    again = await _ensure_endpoint_up(app, shape="compute")
    assert fac.repaired == 1 and again.status == "up"   # once per endpoint


async def test_a_repair_that_fails_says_so_never_a_rejection_or_the_config():
    fac = _VenvGoneFacility(repair_fails="ssh: Permission denied (publickey)")
    app = _venv_gone_app(fac)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert fac.repaired == 1 and res.status == "provisioning"
    assert "Permission denied" in res.notice and "jinja2" in res.notice
    assert "fix the config/partition" not in res.notice and "REJECTED" not in res.notice
    assert "teardown_endpoint" in res.notice   # what still rebuilds it


# ---- B, through a dead pin (c08, 2026-10-30): the venv repair needs no particular login node ---------------------------

DEAD = "login01.hpcb.test"
REFUSED = f"endpoint venv check on {DEAD} failed: ssh: connect to host {DEAD} port 22: Connection refused"
# nersc with the key revoked: password + one-time code only, and the denial still lists publickey
DENIED = "hpcbridge-test@login: Permission denied (publickey,keyboard-interactive)."


class _RepairCli:
    """hpc-bridge's CLI as the repair drives it: the venv check over `target`; the pid file on the shared home, on a
    clock the fake sleep moves (`tick`, monkeypatched over remote._sleep), which a live manager refreshes on every
    tick (`touching`); `list`'s view (`state`); and the real `watch_pid_file` over those."""

    async def watch_pid_file(self, name, first, up_to):
        return await RemoteEndpointCLI.watch_pid_file(self, name, first, up_to)

    def __init__(self, *, alias_says=None, pid_age=None, touching=False, pinned_answers=False, exits_after=None):
        self.target = SshTarget(DEAD, "u", "k", host_key_alias="login")   # pinned: rebound to the node of the record
        self.venv_ready, self.venv, self.venv_built = None, None, None
        self.alias_says, self.pinned_answers = alias_says, pinned_answers
        self.now, self.mtime = 1000.0, (None if pid_age is None else 1000.0 - pid_age)
        self.touching, self.exits_after = touching, exits_after   # after a stop: the daemon exits this many s later
        self.state = "running" if pid_age is not None else "stopped"
        self.stopped_at, self.starts = None, []
        self.checked_on, self.stopped = [], 0

    async def tick(self, s):
        self.now += s
        if self.stopped_at is not None and self.exits_after is not None and self.now - self.stopped_at >= self.exits_after:
            self.mtime, self.state, self.touching = None, "stopped", False   # the daemon exited: it removed its file
        if self.touching and self.mtime is not None:
            self.mtime = self.now

    async def ensure_venv(self):
        self.checked_on.append(self.target.host)
        if self.target.host == DEAD and not self.pinned_answers:
            raise RuntimeError(REFUSED)
        if self.target.host != DEAD and self.alias_says:
            raise RuntimeError(self.alias_says)
        self.venv = "$HOME/hpc-bridge/gce-venv"
        self.venv_built = remote_venv_result()

    async def pid_file(self, name):
        return remote.PidFile(now=self.now, mtime=self.mtime, heartbeat_s=30.0, log="", noise=frozenset())

    async def status(self, name):
        return self.state

    async def stop(self, name):
        self.stopped += 1
        if not self.pinned_answers:
            return 255, f"ssh: connect to host {DEAD} port 22: Connection refused"
        self.stopped_at = self.now   # `stop` asks; the daemon exits `exits_after` s later (None: not in time)
        return 0, ""

    # what the real `provision` drives once the manager is gone
    async def endpoint_id(self, name):
        return EID

    async def clean_uep_pidfiles(self, eid):
        pass

    async def configure(self, name):
        pass

    async def write_config(self, name, manager, uep):
        pass

    async def start(self, name):
        self.starts.append(self.target.host)
        self.state, self.mtime = "running", self.now
        return EID, ("login02.hpcb.test" if self.target.host == "login" else self.target.host), None

    def rebind(self, host):
        from dataclasses import replace
        self.target = replace(self.target, host=host, host_key_alias="login")


def remote_venv_result():
    from types import SimpleNamespace
    return SimpleNamespace(built=True, path="/global/homes/h/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11",
                           reason="it does not exist", moved_aside=None)


def _pinned_facility(tmp_path, cli, monkeypatch=None, *, seeded=True):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility
    from hpc_bridge.state import EndpointRecord, LoginNodeStore
    store = LoginNodeStore(tmp_path / "endpoints.json")
    store.put(EndpointRecord(endpoint_id=EID, login_host=DEAD, alias="login", user="u", key_path="k", name=NAME,
                             provisioned_at="2026-10-30T00:00:00Z", seeded_credentials=seeded))
    prof = MachineProfile(name="m", endpoint_name=NAME, env_setup="true", interface="eth0", partition="debug",
                          account="", worker_init="true")
    fac = SlurmFacility(prof, cli, store=store, alias="login")
    fac.config_template = lambda hpc: ("tmpl", {})
    if monkeypatch is not None:
        monkeypatch.setattr(remote, "_sleep", cli.tick)
        monkeypatch.setattr(remote, "_routable_pin", lambda host, addr=None, **k: host)
    return fac, store


async def test_a_repair_whose_pinned_node_is_dead_rebuilds_through_the_facilitys_host_and_drops_the_pin(
        tmp_path, monkeypatch):
    # c08: the manager runs on login01, closed to new logins (its processes run on); the venv check went to the pin,
    # met "Connection refused" and gave up. The home is shared: the venv is rebuilt through the facility's own host
    cli = _RepairCli(pid_age=5, touching=True)   # the manager on the dead node is alive: it refreshes its pid file
    fac, store = _pinned_facility(tmp_path, cli, monkeypatch)
    done, eid = await fac.repair_endpoint_env(Profile(), EID)
    assert cli.checked_on == [DEAD, "login"] and eid == EID
    rec = store.get(alias="login", name=NAME)   # #236's dead-pin handling: the pin dropped — the record kept
    assert rec is not None and rec.login_host is None and rec.seeded_credentials
    assert DEAD in done and "pin" in done and "dropped" in done and "rebuilt" in done
    assert cli.stopped == 0 and not cli.starts and "still runs" in done   # left to the rebuilt venv, never restarted
    assert cli.target.host == "login"                  # later SSH (the handoff, a teardown) goes to the facility's host


async def test_a_manager_killed_seconds_ago_on_the_dead_node_is_not_called_running(tmp_path, monkeypatch):
    # review round 1 (2026-10-31): one read of the pid file's age called a manager killed 5 s ago on a draining node
    # "still runs" — its file has not moved since; watched across a touch interval, it is gone, and started again
    cli = _RepairCli(pid_age=5, touching=False)
    fac, _store = _pinned_facility(tmp_path, cli, monkeypatch)
    cli.state = "stopped"
    done, _eid = await fac.repair_endpoint_env(Profile(), EID)
    assert "still runs" not in done and cli.starts == ["login"] and "restarted on login02.hpcb.test" in done


async def test_a_restart_past_a_dead_pin_is_pinned_to_the_node_it_started_on_and_keeps_who_placed_the_store(
        tmp_path, monkeypatch):
    # review round 1: the whole record went (seeded_credentials with it), and the restart wrote none — the next session
    # stopped and tore down through the round-robin alias, and never wiped the token store it had placed
    cli = _RepairCli(pid_age=None)   # no pid file: the manager on the dead node is gone too
    fac, store = _pinned_facility(tmp_path, cli, monkeypatch)
    done, _eid = await fac.repair_endpoint_env(Profile(), EID)
    assert cli.starts == ["login"] and "login02.hpcb.test" in done
    rec = store.get(alias="login", name=NAME)
    assert rec is not None and rec.login_host == "login02.hpcb.test" and rec.seeded_credentials
    assert fac._seeded_by_us()


async def test_a_pinned_repair_waits_for_the_stopped_manager_to_exit_before_it_starts_one(tmp_path, monkeypatch):
    # review round 1: `stop` returns before the daemon exits (Expanse, live 2026-09-04); provision then saw it running,
    # took its reuse branch and started nothing — and the answer said "restarted the endpoint"
    cli = _RepairCli(pid_age=5, touching=True, pinned_answers=True, exits_after=25)
    fac, store = _pinned_facility(tmp_path, cli, monkeypatch)
    done, _eid = await fac.repair_endpoint_env(Profile(), EID)
    assert cli.stopped == 1 and cli.starts == [DEAD] and f"restarted on {DEAD}" in done
    assert store.get(alias="login", name=NAME).login_host == DEAD


async def test_a_pinned_repair_whose_manager_does_not_exit_claims_no_restart(tmp_path, monkeypatch):
    cli = _RepairCli(pid_age=5, touching=True, pinned_answers=True, exits_after=None)   # never exits in time
    fac, _store = _pinned_facility(tmp_path, cli, monkeypatch)
    with pytest.raises(RuntimeError, match="had not exited") as e:
        await fac.repair_endpoint_env(Profile(), EID)
    assert cli.starts == [] and "not restarted" in str(e.value)


async def test_a_repair_whose_facility_host_does_not_answer_either_keeps_the_pin(tmp_path, monkeypatch):
    # the client's own network, not a dead pin (connect._drop_dead_pin's rule): nothing is dropped
    cli = _RepairCli(alias_says="ssh: connect to host login port 22: Network is unreachable")
    fac, store = _pinned_facility(tmp_path, cli, monkeypatch)
    with pytest.raises(RuntimeError):
        await fac.repair_endpoint_env(Profile(), EID)
    assert store.get(alias="login", name=NAME).login_host == DEAD


async def test_a_repair_past_a_dead_pin_that_needed_the_users_login_finishes_on_the_next_call(tmp_path, monkeypatch):
    # c08 again: the facility's host asks for the user's password + one-time code (the key revoked). It answered, so the
    # pin is what is dead; once the user opened the connection, the repair goes on from there
    cli = _RepairCli(alias_says=DENIED, pid_age=5, touching=True)
    fac, store = _pinned_facility(tmp_path, cli, monkeypatch)
    with pytest.raises(RuntimeError, match="Permission denied"):
        await fac.repair_endpoint_env(Profile(), EID)
    assert store.get(alias="login", name=NAME).login_host is None and cli.target.host == "login"
    cli.alias_says = None
    done, _eid = await fac.repair_endpoint_env(Profile(), EID)
    assert DEAD in done and "still runs" in done and cli.stopped == 0


async def test_a_repair_that_needs_the_users_login_hands_it_to_their_terminal_then_finishes():
    # c08 has the key revoked too: the facility's host asks for a password + one-time code. The repair hands the login
    # to the user's terminal (preauth_command, as a connect does) and finishes on the next call over that connection
    from types import SimpleNamespace
    fac = _VenvGoneFacility(repair_fails=DENIED)
    fac.cli = SimpleNamespace(target=SshTarget("login", "u", "k", control_dir="/tmp/hpcb-cm"))
    app = _venv_gone_app(fac)
    app.machine = "f"
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning" and res.preauth_command and "ssh" in res.preauth_command
    assert "OWN terminal" in res.notice and res.preauth_command in res.notice and "REJECTED" not in res.notice
    assert "call ensure_endpoint_up(shape='compute') again" in res.notice
    assert app.pending_preauth is not None and "ensure_endpoint_up" in str(app.preauth_resume)
    fac.repair_fails = None   # the user opened the connection
    # …and the next check meets something else (c08: a TRANSIENT conflict): the waiting repair is tried all the same
    transient = CanaryResult(ok=False, error="ComputeAPIError[RESOURCE_CONFLICT]: Endpoint x is already in use")
    for rt in app.shapes.values():
        if rt.runner is not None:
            rt.runner._canary = transient
    again = await _ensure_endpoint_up(app, shape="compute")
    assert fac.repaired == 2 and again.status == "up" and not again.preauth_command, again.notice


async def test_a_repair_waits_while_work_runs_on_the_endpoint_it_would_restart():
    # review round 1 (2026-10-31): the repair restarts the manager, which ends every user endpoint it runs — a login-
    # shape check that met the broken venv while a compute task ran would have killed the user's job
    from concurrent.futures import Future

    from hpc_bridge.context import TaskHandle
    fac = _VenvGoneFacility()
    app = _venv_gone_app(fac)
    app.tasks["task-1"] = TaskHandle(future=Future(), shape="login", session_id="s", command="sleep 600",
                                     submitted_at=0.0, ceiling_s=1780.0)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert fac.repaired == 0 and res.status == "provisioning"
    assert "task(s) task-1 still running" in res.notice and "waits until" in res.notice
    app.tasks["task-1"].future.set_result(None)   # the work ended: the next check repairs
    again = await _ensure_endpoint_up(app, shape="compute")
    assert fac.repaired == 1 and again.status == "up" and "rebuilt" in again.notice


async def test_conflicts_while_a_repaired_endpoint_starts_its_user_endpoints_are_not_a_streak():
    # c08 live (2026-10-31): right after the repair, the service answered RESOURCE_CONFLICT while the user endpoints
    # started again — counted with one from before the repair, three read "NO LONGER transient" and the block went down
    from tests.test_server import _FakeRunner, _Res
    busy = CanaryResult(ok=False, error="ComputeAPIError[RESOURCE_CONFLICT]: Endpoint x is already in use: possibly due "
                                        "to concurrent requests -- please try again")
    fac = _VenvGoneFacility()
    app = AppCtx(facility=fac, profile=Profile())
    after = [busy, busy, busy, busy]

    class _Runner(_FakeRunner):
        async def canary(self, timeout=8.0):
            self.canaries += 1
            if not fac.fixed:
                return CanaryResult(ok=False, error=LIVE_B)
            return after.pop(0) if after else CanaryResult(ok=True, worker_host="c1")

    def factory(eid, user_endpoint_config=None, **kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))
        return _Runner(eid, _Res(0, "", ""))
    app.runner_factory = factory
    answers = [await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)]
    while answers[-1].status == "provisioning" and len(answers) < 8:
        answers.append(await _ensure_endpoint_up(app, shape="compute"))
    assert fac.repaired == 1 and [a.status for a in answers][-1] == "up", [a.notice[:80] for a in answers]
    assert not any("NO LONGER transient" in (a.notice or "") for a in answers)


async def test_a_cancelled_call_does_not_leave_the_repair_half_done():
    # review round 1: cancelled between the manager's stop and its start, the call took the repair with it and left the
    # endpoint stopped — with the repair marked done. It runs on in its own task; the next check reads how it ended
    gate = asyncio.Event()

    class _Slow(_VenvGoneFacility):
        async def repair_endpoint_env(self, profile, endpoint_id):
            self.repaired += 1
            await gate.wait()   # stopped, not yet started again
            self.fixed = True
            return "rebuilt hpc-bridge's endpoint venv /v (it does not exist) — the endpoint restarted on login01", \
                endpoint_id
    fac = _Slow()
    app = _venv_gone_app(fac)
    call = asyncio.ensure_future(_ensure_endpoint_up(app, shape="compute", confirm_spend=True))
    for _ in range(50):
        await asyncio.sleep(0)
        if fac.repaired:
            break
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    gate.set()   # the restart goes on without the call
    await asyncio.sleep(0)
    again = await _ensure_endpoint_up(app, shape="compute")
    assert fac.repaired == 1 and again.status == "up" and "restarted on login01" in again.notice


async def test_a_repair_whose_note_meets_a_refused_block_is_still_said():
    # review round 1: the note was marked said before the `down` answers (a refused submit, no account, an expired
    # login, the conflict limit) that then dropped it — the repair went unsaid
    from tests.test_server import _FakeRunner, _Res
    rej = ("parsl.providers.errors.SubmitException: Cannot launch job hpcb: Could not read job ID from submit command "
           "standard output; recode=1, stdout='', stderr='sbatch: error: Invalid qos specification'")
    fac = _VenvGoneFacility()
    app = AppCtx(facility=fac, profile=Profile())

    def factory(eid, user_endpoint_config=None, **kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))
        return _FakeRunner(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=rej if fac.fixed else LIVE_B))
    app.runner_factory = factory
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert fac.repaired == 1 and res.status == "down" and "rebuilt" in res.notice, res.notice


async def test_a_pid_probe_that_times_out_is_unknown_never_a_bare_timeout(monkeypatch):
    # review round 1: ssh_exec's bound raises a bare TimeoutError(); only rc 255 read as "unknown", so one slow read
    # in the wait escaped it with an empty message
    login = _Login(mtime=1000 - 66, starts=[(73, "", NOISE), (0, "HPCB_HOST=login01.example\n", NOISE)])
    real = login.ssh
    probes = []

    async def ssh(target, cmd, **kw):
        if "daemon.pid" in cmd:
            probes.append(cmd)
            if len(probes) == 2:   # one slow answer mid-wait (a loaded login node, a big endpoint.log)
                raise TimeoutError()
        return await real(target, cmd, **kw)
    cli = _cli(monkeypatch, login)
    monkeypatch.setattr(remote, "ssh_exec", ssh)
    eid, host, _addr = await cli.start(NAME)
    assert login.started == 2 and (eid, host) == (EID, "login01.example")


def test_a_probe_through_the_endpoints_worker_proposes_hpc_bridges_line_for_its_own_venv():
    # review round 1: through the endpoint's own login worker the probe found hpc-bridge's venv on PATH (the worker's
    # env_setup put it there) and proposed `source …/activate` — a hand-written line, so nothing built, checked or
    # repaired that venv again (managed_venv None), adopt_endpoint warned, and the reuse never cached the config
    from hpc_bridge import discovery
    from hpc_bridge.facility.endpoint_venv import is_line, managed_venv
    py, gce = discovery._client_python(), discovery._gce_version() or "4.18.0"
    venv = f"/global/homes/h/u/hpc-bridge/gce-venv-endpoint-{gce}-py{py}"
    out = PROBE_OUT.replace("GCE=\n", f"GCE={venv}/bin/globus-compute-endpoint\nGCE_STATE={py} {gce}\nGCE_ACTIVATE=yes\n")
    draft, notes = discovery.parse_probe(out, ssh_host="login")
    over_ssh, _ = discovery.parse_probe(PROBE_OUT, ssh_host="login")
    assert draft.env_setup == over_ssh.env_setup and is_line(draft.env_setup) and "{venv}" in draft.env_setup
    assert managed_venv(draft.env_setup.replace("{venv}", "$HOME/hpc-bridge/gce-venv")) is not None
    env = next(n for n in notes if n.startswith("env_setup"))
    assert venv in env and "hpc-bridge's own venv" in env


# ---- C: a confirmed configuration on a reused endpoint is cached when none is -----------------------------------------

def _details(env="[ -x $HOME/x ] && . $HOME/x"):
    return FacilityDetails(ssh_host="login", interface="eth0", env_setup=env, scratch_root="/s/{user}",
                           partition="debug").model_dump(mode="json")


class _Reused(FakeFacility):
    def __init__(self, own, note=""):
        super().__init__()
        self.endpoint_worker_init, self.worker_init_note = own, note


def _reused_app(store, fac, details):
    app = AppCtx(facility=fac, profile=Profile())
    app.state.reused = True                          # reattached to an endpoint from before this session
    app.pending_facility_cache["f"] = ("login", details)
    return app


def test_a_reused_endpoint_that_runs_the_confirmed_env_setup_caches_it_when_none_is(monkeypatch, tmp_path):
    store = FacilityStore(tmp_path / "facilities.json")
    monkeypatch.setattr(binding, "_facility_store", lambda: store)
    d = _details()
    connect._commit_proven_facility(_reused_app(store, _Reused(d["env_setup"]), d), "f")
    assert store.get("login") == d


def test_a_reused_endpoint_never_caches_what_it_does_not_run_nor_over_a_proven_record(monkeypatch, tmp_path):
    store = FacilityStore(tmp_path / "facilities.json")
    monkeypatch.setattr(binding, "_facility_store", lambda: store)
    d = _details()
    # it runs another env_setup (worker_init_note says so): these details were never run
    connect._commit_proven_facility(_reused_app(store, _Reused("module load x", note="a different env_setup"), d), "f")
    assert store.get("login") is None
    # its own is unknown (the template was not read): nothing says these details run
    connect._commit_proven_facility(_reused_app(store, _Reused(None), d), "f")
    assert store.get("login") is None
    # a proven record is never replaced by a reuse
    proven = _details(env="module load proven")
    store.put("login", proven)
    connect._commit_proven_facility(_reused_app(store, _Reused(d["env_setup"]), d), "f")
    assert store.get("login") == proven


def test_the_connect_answer_says_the_start_waited_out_a_dead_managers_pid_file():
    from types import SimpleNamespace
    fac = SimpleNamespace(cli=SimpleNamespace(start_note="the endpoint's previous manager had died without cleaning "
                                                         "up (its pid file was 66 s old …): hpc-bridge waited 27 s"))
    assert "waited 27 s" in connect._start_note(fac)
    assert connect._start_note(SimpleNamespace()) == "" and connect._start_note(SimpleNamespace(cli=None)) == ""


# ---- D: a new laptop reattaches to its endpoint with zero SSH (approved 2026-10-30) ------------------------------------
# A user with no ~/.hpc-bridge (a new laptop) and a facility not in the registry: connect_facility(id, ssh_host=…) used to
# probe the login node over SSH before anything else — a password + one-time code on nersc — although their endpoint
# for that host was online. Now the web service is asked first; a login worker that answers runs the same probe THROUGH
# the endpoint, and the proposal says so (`reused`). A ghost (online, but no worker answers) and no endpoint at all fall
# back to the SSH probe; a registry facility (a facility endpoint above all) never reaches this.

PROBE_OUT = ("HPCB_PROBE_BEGIN\nUSER=u\nHOME=/global/homes/h/u\nSCRATCH=\nWORK=\nPSCRATCH=\nSCHED=slurm\nGCE=\n"
             "UV=/usr/bin/uv\nMYBALANCE=\nXDUSAGE=\nPART=debug*|up\nNIC=lo|127.0.0.1/8\nNIC=eth0|10.0.0.5/24\n"
             "HPCB_PROBE_END\n")


class _Shell:
    def __init__(self, stdout, returncode=0):
        self.stdout, self.stderr, self.returncode = stdout, "", returncode


class _LoginRunner:
    """The online endpoint's login worker, as connect's reattach probe drives it. `answers`: True (a worker answers),
    False (silence: a still-queued check), or the error text every check comes back with at once."""

    def __init__(self, eid, *, answers=True, out=PROBE_OUT):
        self.eid, self.answers, self.out, self.ran, self.closed = eid, answers, out, [], False
        self.checks = 0

    async def canary(self, timeout=8.0):
        self.checks += 1   # returns at once, never awaiting: the loop itself must yield (review round 1)
        if self.answers is True:
            return CanaryResult(ok=True)
        return CanaryResult(ok=False, error="timeout" if self.answers is False else self.answers)

    async def run(self, command):
        self.ran.append(command)
        return _Shell(self.out)

    def close(self):
        self.closed = True


class _NewLaptop:
    """The world of a connect with no record: the registry misses, the web service names (or not) our endpoint, the
    SSH probe is a tripwire."""

    def __init__(self, monkeypatch, *, online="eid-online", answers=True, catalog=(), registry_down=False,
                 fake_clock=False):
        from hpc_bridge import config as _config
        from tests.fakes import FakeCatalog

        self.ssh_probes, self.asked, self.runners = [], [], []

        class _Cat(FakeCatalog):
            async def get(self_, facility):
                if registry_down:
                    raise RuntimeError("Globus Search: 503 Service Unavailable")
                return await FakeCatalog.get(self_, facility)
        monkeypatch.setattr(binding, "make_catalog", lambda: _Cat(list(catalog)))
        monkeypatch.setattr(_config, "_control_settings", lambda: (None, 60))

        async def ssh_probe(target):
            self.ssh_probes.append(target.host)
            return (FacilityDetails(ssh_host=target.host, interface="eth0", env_setup="true",
                                    scratch_root="/home/{user}/.hpc-bridge", partition="debug"), ["over ssh"])
        monkeypatch.setattr(connect, "discover_facility_details", ssh_probe)

        async def online_for_host(app, facility, host):
            self.asked.append(host)
            name = f"hpc-bridge-{host}"
            return (name, online, "") if online else (name, None, f"no endpoint of yours named {name} is online. ")
        monkeypatch.setattr(connect, "_online_for_host", online_for_host)
        # the real wait loop — on real time, or (`fake_clock`) on a clock its sleeps move, every sleep still yielding
        self.now, self.slept = 0.0, []

        async def sleep(s):
            self.slept.append(s)
            self.now += s
            await asyncio.sleep(0)
        if fake_clock:
            monkeypatch.setattr(connect, "_monotonic", lambda: self.now, raising=False)
            monkeypatch.setattr(connect, "_sleep", sleep, raising=False)
        self.answers = answers

    def app(self):
        app = AppCtx(facility=FakeFacility(), profile=Profile())

        def factory(eid, user_endpoint_config=None, **kw):
            r = _LoginRunner(eid, answers=self.answers)
            r.uec = user_endpoint_config
            self.runners.append(r)
            return r
        app.runner_factory = factory
        return app


async def test_a_new_laptop_proposes_through_its_online_endpoint_with_no_ssh(monkeypatch):
    from hpc_bridge import server
    w = _NewLaptop(monkeypatch)
    app = w.app()
    res = await server._connect_facility(app, "f-new", ssh_host="login")
    assert w.ssh_probes == [] and w.asked == ["login"]                     # no SSH at all
    assert res.phase == "proposed_facility_details" and res.reused is True, res.notice
    assert res.proposed_details.ssh_host == "login" and res.proposed_details.interface == "eth0"
    assert "zero SSH" in res.notice and "eid-online" in res.notice and "THROUGH" in res.notice
    (r,) = w.runners
    assert r.eid == "eid-online" and r.closed and "HPCB_PROBE_BEGIN" in r.ran[0]
    # the login worker runs on the template the endpoint was configured with: no interface or env_setup of ours
    assert r.uec.get("compute") is False and "interface" not in r.uec and "worker_init" not in r.uec
    assert app.proposed_drafts["f-new"].ssh_host == "login"                # a details= without env_setup takes it


async def test_a_ghost_or_no_endpoint_falls_back_to_the_ssh_probe_saying_why(monkeypatch):
    from hpc_bridge import server
    w = _NewLaptop(monkeypatch, answers=False, fake_clock=True)   # online per the web service, no worker answers: a ghost
    app = w.app()
    res = await server._connect_facility(app, "f-new", ssh_host="login")
    assert w.ssh_probes == ["login"] and res.phase == "proposed_facility_details" and not res.reused
    assert "did not answer within 60 s" in res.notice and w.runners[0].closed
    assert w.now >= 60.0 and "eid-online" in app.ghost_endpoints   # it waited the whole wait out: the real loop
    w = _NewLaptop(monkeypatch, online=None)     # nothing online: said too (review round 1: silent before)
    res = await server._connect_facility(w.app(), "f-new", ssh_host="login")
    assert w.ssh_probes == ["login"] and w.runners == [] and not res.reused
    assert res.notice.startswith("no endpoint of yours named hpc-bridge-login is online")


async def test_the_reattach_wait_never_spins_a_manager_that_answers_with_an_error(monkeypatch):
    # review round 1 (2026-10-31): a manager that fails fast got a fresh check on every pass — 433 tasks in 0.5 s, each
    # a user-endpoint start — and was then called a ghost, "did not answer". It answered: that ends the wait, once.
    # (Real time, a 1 s wait: the loop as it runs.)
    w = _NewLaptop(monkeypatch, answers=LIVE_B)
    monkeypatch.setattr(connect, "_REATTACH_WAIT_S", 1.0)
    app = w.app()
    got = await connect._probe_through_endpoint(app, "f-new", "login")
    (r,) = w.runners
    assert r.checks == 1 and isinstance(got, str)
    assert "did not answer" not in got and "No module named 'jinja2'" in got and "broken" in got
    assert "repairs its environment" in got and "eid-online" not in app.ghost_endpoints   # alive: the confirm repairs it


async def test_a_busy_manager_is_asked_again_only_after_a_growing_back_off(monkeypatch):
    busy = "ComputeAPIError[RESOURCE_CONFLICT]: Endpoint eid-online is already in use: possibly due to concurrent requests"
    w = _NewLaptop(monkeypatch, answers=busy, fake_clock=True)
    app = w.app()
    got = await connect._probe_through_endpoint(app, "f-new", "login")
    (r,) = w.runners
    assert 3 <= r.checks <= 10 and w.slept[:3] == [2.0, 4.0, 8.0] and max(w.slept) <= connect._REATTACH_BACKOFF_MAX_S
    assert "kept answering that it is busy" in got and "eid-online" not in app.ghost_endpoints


async def test_the_reattach_wait_yields_to_the_event_loop_while_it_waits(monkeypatch):
    # a check that comes back at once (a fake, a fast failure) never starves the heartbeat: every pass sleeps. Real time,
    # a 1 s wait: the loop as it runs
    w = _NewLaptop(monkeypatch, answers=False)
    monkeypatch.setattr(connect, "_REATTACH_WAIT_S", 1.0)
    app = w.app()
    ticks = []

    async def heartbeat():
        while True:
            ticks.append(1)
            await asyncio.sleep(0.05)
    beat = asyncio.ensure_future(heartbeat())
    await asyncio.sleep(0)
    before = len(ticks)
    await connect._probe_through_endpoint(app, "f-new", "login")
    beat.cancel()
    assert len(ticks) - before >= 5 and w.runners[0].checks <= 4


async def test_the_whole_reattach_is_bounded_and_says_so(monkeypatch):
    w = _NewLaptop(monkeypatch)
    app = w.app()
    monkeypatch.setattr(connect, "_REATTACH_BUDGET_S", 0.0, raising=False)   # what is left for the probe: the 1 s floor

    class _Hung(_LoginRunner):
        async def run(self, command):
            await asyncio.Event().wait()
    app.runner_factory = lambda eid, user_endpoint_config=None, **kw: _Hung(eid)
    got = await asyncio.wait_for(connect._probe_through_endpoint(app, "f-new", "login"), 10)
    assert isinstance(got, str) and "did not finish within 1 s" in got


async def test_a_web_lookup_that_fails_says_so_and_a_ghost_is_not_adopted_again(monkeypatch):
    # review round 1: a swallowed web or login error fell back to SSH silently; and a ghost found here was adopted again
    # by the bring-up of the confirm that followed (find_online_endpoint), after the notice said "probed over SSH"
    from types import SimpleNamespace

    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = SimpleNamespace()   # a login gate is installed (the web service is asked only then)

    class _Client:
        def get_endpoints(self, role):
            raise RuntimeError("401 Unauthorized: token expired")
    monkeypatch.setattr(connect, "_endpoint_client", _Client)
    _name, eid, why = await connect._online_for_host(app, "f-new", "login")
    assert eid is None and "could not say whether your endpoint" in why and "401 Unauthorized" in why

    class _Listed:
        def get_endpoints(self, role):
            return [{"name": "hpc-bridge-login", "uuid": "eid-ghost"}]

        def get_endpoint_status(self, eid):
            return {"status": "online"}
    monkeypatch.setattr(connect, "_endpoint_client", _Listed)
    app.ghost_endpoints.add("eid-ghost")
    _name, eid, why = await connect._online_for_host(app, "f-new", "login")
    assert eid is None and "does not answer" in why
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-login", env_setup="true", interface="eth0",
                          partition="debug", account="", worker_init="true")
    fac = SlurmFacility(prof, RemoteEndpointCLI(SshTarget("login", "u", "k"), "true"), client_factory=_Listed)
    assert await fac.find_online_endpoint("hpc-bridge-login") == "eid-ghost"
    fac.ghosts = app.ghost_endpoints
    assert await fac.find_online_endpoint("hpc-bridge-login") is None
    # …and connect hands the session's ghosts to each facility it builds: the confirm's bring-up skips them
    from hpc_bridge import config as _config
    from hpc_bridge import server, warmth

    class _Ssh(FakeFacility):
        def __init__(self):
            super().__init__()
            self.ghosts = set()
    built = []

    def build(entry, *, account):
        built.append(_Ssh())
        return built[-1]

    async def provision(app_, shape, **kw):
        return "warm"
    monkeypatch.setattr(binding, "_facility_from_entry", build)
    monkeypatch.setattr(warmth, "_provision", provision)
    monkeypatch.setattr(_config, "_control_settings", lambda: (None, 60))
    app.login_flow = None   # no login gate for the connect itself (the hermetic tests' default)
    await server._connect_facility(app, "f-new", ssh_host="login", details=FacilityDetails(
        ssh_host="login", interface="eth0", env_setup="true", scratch_root="/s/{user}", partition="debug"))
    assert built and built[0].ghosts is app.ghost_endpoints


async def test_a_registry_facility_or_a_registry_outage_never_takes_the_reattach_rung(monkeypatch):
    from hpc_bridge import server
    from tests.fakes import fake_mep_entry
    # the ladder: a facility endpoint first — a catalogued MEP attaches as before, our own endpoint never asked for
    w = _NewLaptop(monkeypatch, catalog=[fake_mep_entry()])
    entry = fake_mep_entry()
    await server._connect_facility(w.app(), entry.id, ssh_host="login")
    assert w.asked == [] and w.ssh_probes == []
    # the registry down: the facility may be a catalogued one (a MEP above all) — today's answer, no reattach
    w = _NewLaptop(monkeypatch, registry_down=True)
    await server._connect_facility(w.app(), "f-new", ssh_host="login")
    assert w.asked == []
