# tests/test_returning_user_fixes.py — what the returning-user cells (agentic/harness/returning.py, 2026-10-30) found:
#   A. a manager killed hard leaves its daemon.pid on the shared home; globus-compute's start refuses (exit 73, nothing on
#      either stream) while that file is younger than three heartbeats, and hpc-bridge gave up at once, quoting the
#      login shell's .bashrc noise as the cause (cells c03 c05 c12 c18 of the first table);
#   B. a manager reused over the web whose venv was deleted cannot start a user endpoint ("No module named 'jinja2'",
#      exit 73), and the notice said "fix the config/partition", then "likely REJECTED" (c09);
#   C. a confirmed configuration brought up on a REUSED endpoint was never cached, so the next session probed again (c14).
# Hermetic: ssh is a fake; the warm path runs on FakeFacility with fake runners.
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

    async def repair_endpoint_env(self, profile, endpoint_id):
        self.repaired += 1
        if self.repair_fails:
            raise RuntimeError(self.repair_fails)
        return ("rebuilt hpc-bridge's endpoint venv $HOME/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11 (it does not "
                "exist), and restarted the endpoint on login01", endpoint_id)


def _venv_gone_app(fac):
    app = AppCtx(facility=fac, profile=Profile())

    def factory(eid, user_endpoint_config=None, **kw):
        if not (user_endpoint_config or {}).get("compute"):
            return _FakeRunner(eid, _Res(0, "", ""))
        ok = fac.repaired and not fac.repair_fails
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
