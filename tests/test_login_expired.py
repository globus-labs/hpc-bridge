# tests/test_login_expired.py — a stored Globus login that no longer works (refresh token revoked or expired).
# Live 2026-10-14: every dispatch failed with `AuthAPIError: ('POST', 'https://auth.globus.org/v2/oauth2/token', None,
# 400, None, 'Bad Request')` and the notice said "fix the config/partition"; authenticate() then said `logged_in`
# (the credential still held every scope); connect said only "facility template not readable (AuthAPIError)".
# Hermetic: the errors are the SDK's real AuthAPIError/GlobusAPIError, built from a local requests.Response.
import json

import pytest
import requests
from globus_sdk import AuthAPIError, GlobusAPIError

from hpc_bridge import binding, dispatch, server
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.login import LOGIN_EXPIRED, LoginFlow, login_expired
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult, GlobusRunner
from hpc_bridge.server import AppCtx, _authenticate, _cold_outcome, _ensure_endpoint_up, _run_shell, _shape_runtime
from tests.fakes import FakeCatalog, FakeFacility, fake_entry, fake_mep_entry
from tests.test_server import _FakeRunner, _Res

TOKEN_URL = "https://auth.globus.org/v2/oauth2/token"
DEAD = LOGIN_EXPIRED.removeprefix("the ")  # what every notice names, mid-sentence or at its start


def _response(method, url, status, body, reason):
    r = requests.Response()
    r.status_code, r.reason, r.url = status, reason, url
    r._content = json.dumps(body).encode()
    r.headers["Content-Type"] = "application/json"
    r.request = requests.Request(method, url).prepare()
    return r


def _dead_refresh(status=400):
    """What the SDK raises when Globus Auth refuses a refresh token — the live error's exact shape."""
    return AuthAPIError(_response("POST", TOKEN_URL, status, {"error": "invalid_grant"}, "Bad Request"))


def test_the_dead_refresh_is_the_live_error():
    exc = _dead_refresh()
    assert str(exc) == "('POST', 'https://auth.globus.org/v2/oauth2/token', None, 400, None, 'Bad Request')"


# ---- the classifier ----------------------------------------------------------------------------------------------

def test_a_refused_refresh_is_a_dead_login():
    assert login_expired(_dead_refresh(400))
    assert login_expired(_dead_refresh(401))
    # the code alone (a body the SDK reads as {"code": "invalid_grant"}) and a token URL with a query
    assert login_expired(AuthAPIError(_response("POST", "https://auth.globus.org/v2/oauth2/x", 400,
                                                {"code": "invalid_grant", "message": "m"}, "Bad Request")))
    # the token endpoint alone: a body that names no grant error, a URL with a query
    assert login_expired(AuthAPIError(_response("POST", TOKEN_URL + "?x=1", 400, {"error": "invalid_request"}, "Bad")))
    assert login_expired(AuthAPIError(_response("POST", TOKEN_URL, 401, {}, "Unauthorized")))
    # an explicit `raise … from` the refusal
    try:
        try:
            raise _dead_refresh()
        except AuthAPIError as inner:
            raise RuntimeError("submit failed") from inner
    except RuntimeError as outer:
        assert login_expired(outer)


def test_other_errors_are_not_a_dead_login():
    compute = "https://compute.api.globus.org/v3/endpoints/e/submit"
    assert not login_expired(GlobusAPIError(_response("POST", compute, 403, {"code": "FORBIDDEN", "message": "no"},
                                                      "Forbidden")))  # a 403 from Compute: not the login
    assert not login_expired(GlobusAPIError(_response("POST", compute, 401, {"error": "invalid_grant"}, "Unauthorized")))
    assert not login_expired(AuthAPIError(_response("GET", "https://auth.globus.org/v2/oauth2/userinfo", 401,
                                                    {"error": "invalid_token"}, "Unauthorized")))  # another Auth call
    assert not login_expired(AuthAPIError(_response("POST", TOKEN_URL, 500, {}, "Server Error")))  # Auth is down
    assert not login_expired(AuthAPIError(_response("POST", TOKEN_URL, 403, {"error": "x"}, "Forbidden")))
    for exc in (ConnectionError("no route"), OSError("network is unreachable"), TimeoutError(),
                requests.ConnectionError("dns"), RuntimeError("Executor is shutdown"), None):
        assert not login_expired(exc)
    # only an EXPLICIT cause counts: an error raised while handling one is not that error
    try:
        try:
            raise _dead_refresh()
        except AuthAPIError:
            raise RuntimeError("unrelated")  # noqa: B904 - the implicit context is the point
    except RuntimeError as outer:
        assert outer.__context__ is not None and not login_expired(outer)


# ---- the canary carries it to the notices ------------------------------------------------------------------------

class _Executor:
    def __init__(self, exc):
        self.exc = exc

    def submit(self, fn):
        raise self.exc

    def shutdown(self, wait=False, cancel_futures=True):
        pass


async def test_the_canary_flags_a_dead_login_and_keeps_its_text():
    res = await GlobusRunner("eid", executor_factory=lambda: _Executor(_dead_refresh())).canary()
    assert res.ok is False and res.login_expired is True
    assert res.error.startswith("AuthAPIError: ('POST', 'https://auth.globus.org/v2/oauth2/token'")
    other = await GlobusRunner("eid", executor_factory=lambda: _Executor(RuntimeError("Executor is shutdown"))).canary()
    assert other.ok is False and other.login_expired is False


def _dead_login_canary():
    return CanaryResult(ok=False, error="AuthAPIError: ('POST', '" + TOKEN_URL + "', None, 400, None, 'Bad Request')",
                        login_expired=True)


def test_cold_run_shell_on_a_dead_login_says_log_in_not_fix_the_config():
    out = _cold_outcome("cold", _dead_login_canary())
    assert out.phase == "failed"  # not a cold start to wait out
    assert DEAD in out.notice and "authenticate(force=True)" in out.notice
    assert "fix the config" not in out.notice and "allocating" not in out.notice
    assert "nothing is wrong with the facility, its config or the partition" in out.notice
    # any other dispatch failure keeps today's notice
    other = _cold_outcome("cold", CanaryResult(ok=False, error="ComputeAPIError[FORBIDDEN]: no"))
    assert other.phase == "cold_start" and "Not a queue wait: fix the config/partition" in other.notice
    assert "authenticate" not in other.notice


def _auth_failing_app():
    f = FakeFacility()
    f.workers = 1  # the manager answers; the dispatch is what fails
    app = AppCtx(facility=f, profile=Profile())

    def factory(eid, user_endpoint_config=None, **kw):
        if not (user_endpoint_config or {}).get("compute"):  # the pilot-query login runner: warm, ignore
            return _FakeRunner(eid, _Res(0, "", ""))
        return GlobusRunner(eid, executor_factory=lambda: _Executor(_dead_refresh()),
                            user_endpoint_config=user_endpoint_config, **kw)

    app.runner_factory = factory
    return app


async def test_ensure_endpoint_up_on_a_dead_login_says_log_in_again():
    app = _auth_failing_app()
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"  # never a `down` that could hide a block still billing
    assert DEAD in res.notice and "authenticate(force=True)" in res.notice
    assert "fix the config" not in res.notice and "allocating" not in res.notice
    assert _shape_runtime(app, "compute").provisioning_since is None


async def test_run_shell_on_a_dead_login_says_log_in_again():
    app = _auth_failing_app()
    _shape_runtime(app, "compute").spend_confirmed = True
    out = await _run_shell(app, "echo hi", shape="compute")
    assert out.phase == "failed" and DEAD in out.notice and "authenticate(force=True)" in out.notice


def test_a_dispatch_failing_on_a_dead_login_says_so():
    out = dispatch.failure_outcome(_dead_refresh(), "warm")
    assert out.phase == "failed" and out.notice.startswith("Dispatch error: AuthAPIError: " + LOGIN_EXPIRED)
    assert "authenticate(force=True)" in out.notice and "oauth2/token" in out.stderr_snippet
    assert dispatch.failure_outcome(RuntimeError("boom"), "warm").notice == "Dispatch error: RuntimeError"


# ---- authenticate() checks that the stored login still works ------------------------------------------------------

class _Authorizer:
    """`valid`: the access token is still within its lifetime, so ensure_valid_token makes no refresh call unless
    handle_missing_authorization (the SDK's "this token is no good") invalidated it first."""

    def __init__(self, fail=None, valid=False):
        self.fail, self.valid, self.ensured, self.refreshes = fail, valid, 0, 0

    def handle_missing_authorization(self):
        self.valid = False
        return True

    def ensure_valid_token(self):  # the SDK refreshes here only when the access token has expired
        self.ensured += 1
        if self.valid:
            return
        self.refreshes += 1
        if self.fail is not None:
            raise self.fail


class _StoredApp:
    """The non-prompting app: holds every scope (login_required False); its tokens refresh — or don't."""

    def __init__(self, fail=None, required=False, valid=False):
        self.required, self.logins = required, 0
        self.authorizers = {rs: _Authorizer(fail, valid) for rs in ("auth.globus.org", "funcx_service")}

    @property
    def scope_requirements(self):
        return {rs: [] for rs in self.authorizers}

    def login_required(self):
        return self.required

    def get_authorizer(self, rs):
        return self.authorizers[rs]

    def login(self, force=False):  # the SDK's own login: must never run from a check
        self.logins += 1


def _flow(app):
    return LoginFlow(app_factory=lambda manager: app, mode_override="paste")


def test_check_stored_login_verdicts():
    good = _StoredApp()
    assert _flow(good).check_stored_login() == ("works", None)
    assert all(a.ensured == 1 for a in good.authorizers.values())  # every resource server, as a dispatch would
    dead = _StoredApp(fail=_dead_refresh())
    verdict, why = _flow(dead).check_stored_login()
    assert verdict == "expired" and "oauth2/token" in why and dead.logins == 0
    for err in (requests.ConnectionError("Max retries exceeded"), OSError("network is unreachable"),
                AuthAPIError(_response("POST", TOKEN_URL, 503, {}, "Service Unavailable"))):
        verdict, why = _flow(_StoredApp(fail=err)).check_stored_login()
        assert verdict == "unchecked" and why
    changed = _StoredApp(required=True)  # the store changed under the check: never reach the SDK's own login
    assert _flow(changed).check_stored_login()[0] == "unchecked" and changed.logins == 0

    def unreadable(manager):
        raise OSError("no storage.db")

    assert LoginFlow(app_factory=unreadable).check_stored_login()[0] == "unchecked"


def _app_with(flow):
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = flow
    return app


async def test_authenticate_on_a_dead_stored_login_arms_a_new_one():
    flow = _flow(_StoredApp(fail=_dead_refresh()))
    st = await _authenticate(_app_with(flow))
    assert st.phase == "needs_login" and st.login_mode == "paste" and st.login_url
    assert DEAD in st.notice and "first use" not in st.notice
    assert flow.status() == "waiting"  # armed like a first login


async def test_authenticate_on_a_working_stored_login_is_unchanged():
    app = _StoredApp()
    st = await _authenticate(_app_with(_flow(app)))
    assert st.phase == "logged_in" and st.notice == "Globus login present with every scope hpc-bridge needs."
    assert st.login_url is None


@pytest.mark.parametrize("err", [requests.ConnectionError("Max retries exceeded"), OSError("unreachable"),
                                 AuthAPIError(_response("POST", TOKEN_URL, 502, {}, "Bad Gateway"))])
async def test_authenticate_when_the_check_cannot_answer_does_not_call_the_login_dead(err):
    flow = _flow(_StoredApp(fail=err))
    st = await _authenticate(_app_with(flow))
    assert st.phase == "logged_in" and st.login_url is None
    assert "could not be checked" in st.notice and DEAD not in st.notice
    assert flow.status() == "idle"  # no login armed


async def test_a_landed_relogin_names_what_it_replaced(monkeypatch):
    from hpc_bridge import login_gate

    async def landed(flow, mode=None):
        return object(), "done"

    monkeypatch.setattr(login_gate, "_start_login_and_wait", landed)
    monkeypatch.setattr(login_gate, "globus_identity_label", lambda: "alice@uni.edu")
    st = await _authenticate(_app_with(_flow(_StoredApp(fail=_dead_refresh()))))
    assert st.phase == "logged_in" and "as alice@uni.edu" in st.notice and DEAD in st.notice


# ---- connect: the facility template read names the dead login ----------------------------------------------------

class _DeadLoginClient:
    def get_endpoint_status(self, endpoint_id):
        raise _dead_refresh()

    def get_endpoint_metadata(self, endpoint_id):
        raise _dead_refresh()


class _BrokenClient(_DeadLoginClient):
    def get_endpoint_metadata(self, endpoint_id):
        raise RuntimeError("metadata API down")


async def test_template_read_on_a_dead_login_names_it():
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=_DeadLoginClient)
    await fac.load_template()
    await fac.load_template()  # each attach reads again: one note
    assert len(fac.template_notes) == 1
    note = fac.template_notes[0]
    assert DEAD in note and "authenticate(force=True)" in note and "as-is" not in note
    other = MEPFacility.from_entry(fake_mep_entry(), client_factory=_BrokenClient)
    await other.load_template()
    assert other.template_notes == ["facility template not readable (RuntimeError): sending the entry's config as-is"]


async def test_connect_on_a_dead_login_names_it(monkeypatch):
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=_DeadLoginClient)
    app = AppCtx(facility=fac, profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    res = await server._connect_facility(app, "globus1")
    assert res.phase == "needs_account"  # it still attaches (the MEP's status is best-effort)
    assert DEAD in res.notice and "authenticate(force=True)" in res.notice


# ---- connect gates on a stored login that no longer works, before anything else --------------------------------

def _record(calls, name, result=None):
    def fn(*a, **k):
        calls.append(name)
        return result
    return fn


@pytest.mark.parametrize("route", ["registry-ssh", "registry-mep", "details", "cached", "session-reconnect"])
async def test_connect_on_a_dead_stored_login_asks_for_a_login_before_anything(route, monkeypatch):
    from hpc_bridge.facility import remote
    from hpc_bridge.models import FacilityDetails

    calls = []
    entries = {"registry-ssh": [fake_entry(id="anvil", facility_key="purdue")], "registry-mep": [fake_mep_entry()]}
    monkeypatch.setattr(binding, "make_catalog", _record(calls, "catalog", FakeCatalog(entries.get(route, []))))
    monkeypatch.setattr(binding, "_facility_from_entry", _record(calls, "facility"))
    monkeypatch.setattr(binding, "_facility_store", _record(calls, "local cache"))
    monkeypatch.setattr(remote, "build_minimal_storage_db", _record(calls, "token copy"))
    app = _app_with(_flow(_StoredApp(fail=_dead_refresh())))
    kw = {}
    facility = {"registry-ssh": "anvil", "registry-mep": "globus1"}.get(route, "byo")
    if route == "details":
        kw["details"] = FacilityDetails(ssh_host="h.example.edu", interface="ib0", env_setup="true",
                                        scratch_root="/s/{user}", partition="main")
    if route == "session-reconnect":
        app.session_facilities["byo"] = fake_entry(id="byo", facility_key="session")
    res = await server._connect_facility(app, facility, **kw)
    assert res.phase == "needs_login" and res.login_url and res.login_mode == "paste"
    assert DEAD in res.notice and "first use" not in res.notice
    assert calls == []  # no registry read, no local cache, no facility, nothing bootstrapped, no token copied
    assert app.login_flow.status() == "waiting"


async def _mep_connect(app, monkeypatch):
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=_OkClient)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    return await server._connect_facility(app, "globus1")


class _OkClient:
    def get_endpoint_status(self, endpoint_id):
        return {"status": "online"}

    def get_endpoint_metadata(self, endpoint_id):
        return {}


@pytest.mark.parametrize("stored", [_StoredApp(), _StoredApp(fail=requests.ConnectionError("Max retries exceeded"))],
                         ids=["works", "unchecked"])
async def test_connect_on_a_working_or_unchecked_login_carries_on(stored, monkeypatch):
    flow = _flow(stored)
    res = await _mep_connect(_app_with(flow), monkeypatch)
    assert res.phase == "needs_account" and DEAD not in res.notice
    assert flow.status() == "idle"  # no login armed


async def test_connect_on_a_missing_login_keeps_the_first_use_reason(monkeypatch):
    res = await _mep_connect(_app_with(_flow(_StoredApp(required=True))), monkeypatch)
    assert res.phase == "needs_login" and "first use" in res.notice and DEAD not in res.notice


async def test_connect_carries_on_when_the_relogin_lands_and_says_what_it_replaced(monkeypatch):
    from hpc_bridge import login_gate

    async def landed(flow, mode=None):
        return object(), "done"

    monkeypatch.setattr(login_gate, "_start_login_and_wait", landed)
    monkeypatch.setattr(login_gate, "globus_identity_label", lambda: "alice@uni.edu")
    res = await _mep_connect(_app_with(_flow(_StoredApp(fail=_dead_refresh()))), monkeypatch)
    assert res.phase == "needs_account"
    assert res.notice.startswith("Globus login landed as alice@uni.edu (") and DEAD in res.notice.split(". ")[0]


# ---- the bare "hpc-bridge error: AuthAPIError …" sites name the dead login -------------------------------------

def test_error_outcome_names_a_dead_login():
    out = server._error_outcome(_dead_refresh())
    assert DEAD in out.notice and "authenticate(force=True)" in out.notice and "oauth2/token" not in out.notice
    assert out.notice.startswith("hpc-bridge error: AuthAPIError: ")
    assert server._error_outcome(RuntimeError("boom")).notice == "hpc-bridge error: RuntimeError: boom"


async def test_ensure_endpoint_up_provision_error_names_a_dead_login(monkeypatch):
    from hpc_bridge import warmth

    errors = {"exc": _dead_refresh()}

    async def provision(app, shape, **kw):
        raise errors["exc"]

    monkeypatch.setattr(warmth, "_provision", provision)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "down" and DEAD in res.notice and "authenticate(force=True)" in res.notice
    errors["exc"] = RuntimeError("boom")
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.notice == "hpc-bridge error: RuntimeError: boom"


async def test_connect_mep_attach_error_names_a_dead_login(monkeypatch):
    from hpc_bridge import connect as connect_mod

    errors = {"exc": _dead_refresh()}

    async def ensure_warm(*a, **k):
        raise errors["exc"]

    monkeypatch.setattr(connect_mod, "ensure_warm", ensure_warm)
    res = await _mep_connect(_app_with(None), monkeypatch)
    assert res.phase == "failed" and DEAD in res.notice and "authenticate(force=True)" in res.notice
    errors["exc"] = RuntimeError("status API down")
    res = await _mep_connect(_app_with(None), monkeypatch)
    assert res.phase == "failed" and res.notice == "hpc-bridge error: RuntimeError: status API down"


# ---- the bootstrap gate: no SSH to copy a login Globus refuses to renew -----------------------------------------

class _BootstrapFacility(FakeFacility):
    """An SSH-style facility: no endpoint until `bootstrap` (which, for real, SSHes in and may copy the login)."""

    def __init__(self):
        super().__init__()
        self.workers, self.bootstraps = 1, 0

    async def bootstrap(self, profile):
        self.bootstraps += 1
        return await self.provision(profile)


def _pinned_app(stored):
    """A server started with HPC_BRIDGE_MACHINE (or after a teardown): a facility bound, no endpoint, no connect."""
    app = AppCtx(facility=_BootstrapFacility(), profile=Profile())
    app.login_flow = _flow(stored)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "ok\n", ""))
    return app


class _ToolCtx:
    def __init__(self, app):
        self.request_context = type("R", (), {"lifespan_context": app})()

    async def report_progress(self, *a, **k):
        pass


@pytest.mark.parametrize("valid", [False, True], ids=["access-token-expired", "access-token-still-valid"])
async def test_bootstrap_on_a_dead_login_makes_no_ssh_and_says_why(valid):
    app = _pinned_app(_StoredApp(fail=_dead_refresh(), valid=valid))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "down" and app.facility.bootstraps == 0  # no SSH, nothing copied
    assert DEAD in res.notice and "authenticate(force=True)" in res.notice and "no login was copied" in res.notice
    # a refresh token revoked within the access token's lifetime is caught too: this gate forces the refresh
    assert sum(a.refreshes for a in app.login_flow.app_factory(None).authorizers.values()) == 1


async def test_run_shell_before_a_bootstrap_on_a_dead_login():
    app = _pinned_app(_StoredApp(fail=_dead_refresh()))
    _shape_runtime(app, "compute").spend_confirmed = True
    out = await server.run_shell("echo hi", _ToolCtx(app), shape="compute")
    assert out.phase == "failed" and DEAD in out.notice and "authenticate(force=True)" in out.notice
    assert app.facility.bootstraps == 0


@pytest.mark.parametrize("stored", [_StoredApp(valid=True), _StoredApp(fail=requests.ConnectionError("no route"))],
                         ids=["works", "unchecked"])
async def test_bootstrap_carries_on_when_the_login_works_or_cannot_be_checked(stored):
    app = _pinned_app(stored)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "up" and app.facility.bootstraps == 1


def test_a_forced_check_sees_a_refresh_token_revoked_while_the_access_token_is_valid():
    app = _StoredApp(fail=_dead_refresh(), valid=True)
    assert _flow(app).check_stored_login() == ("works", None)  # the lazy check makes no call: nothing to see
    assert sum(a.refreshes for a in app.authorizers.values()) == 0
    verdict, why = _flow(app).check_stored_login(force_refresh=True)
    assert verdict == "expired" and "oauth2/token" in why


async def test_connect_ssh_bootstrap_on_a_login_revoked_within_its_access_token(monkeypatch):
    # connect's own gate is lazy (no refresh while the access token is valid); the forced gate before the
    # bootstrap is what stops the copy
    fac = _BootstrapFacility()
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    app = _app_with(_flow(_StoredApp(fail=_dead_refresh(), valid=True)))
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "failed" and fac.bootstraps == 0
    assert DEAD in res.notice and "authenticate(force=True)" in res.notice


# ---- the real SDK app: bounded, and the forced refresh -----------------------------------------------------------

@pytest.fixture
def real_app(monkeypatch, tmp_path):
    """`_default_app_factory(None)` — the real UserApp — over an in-memory token store (never the real storage.db),
    with every HTTP request answered by `net` (no network)."""
    import time

    from globus_compute_sdk.sdk.auth import token_storage as gc_storage
    from globus_sdk.token_storage import MemoryTokenStorage, TokenStorageData

    from hpc_bridge.login import required_scopes

    monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(tmp_path / "gc"))
    net = {"calls": [], "answer": None, "sleeps": []}

    def store(*, valid):
        st = MemoryTokenStorage()
        at = int(time.time()) + (3600 if valid else -10)
        st.store_token_data_by_resource_server({
            rs: TokenStorageData(resource_server=rs, identity_id="ident-1", scope=" ".join(scopes),
                                 access_token="AT-" + rs, refresh_token="RT-" + rs, expires_at_seconds=at,
                                 token_type="Bearer")
            for rs, scopes in required_scopes().items()})
        monkeypatch.setattr(gc_storage, "get_token_storage", lambda: st)

    def send(self, req, **kw):
        net["calls"].append((req.method, req.url, kw.get("timeout")))
        assert req.url.startswith(TOKEN_URL), req.url  # only the token endpoint may be asked
        return net["answer"](req)

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr("globus_sdk.transport.requests.time.sleep", lambda s: net["sleeps"].append(s))
    net["store"] = store
    return net


def test_the_real_check_is_bounded_on_a_black_holed_network(real_app):
    def black_hole(req):
        raise requests.ConnectTimeout("connect timed out")

    real_app["store"](valid=False)
    real_app["answer"] = black_hole
    verdict, why = LoginFlow().check_stored_login()
    assert verdict == "unchecked" and why
    timeouts = [c[2] for c in real_app["calls"]]
    assert len(timeouts) == 2, timeouts  # one try and ONE retry, not the SDK's six
    assert all(t is not None and t <= 15 for t in timeouts), timeouts  # not the SDK's 60 s each


def test_the_real_forced_check_sees_a_revoked_refresh_token(real_app):
    real_app["store"](valid=True)
    real_app["answer"] = lambda req: _response("POST", req.url, 400, {"error": "invalid_grant"}, "Bad Request")
    assert LoginFlow().check_stored_login() == ("works", None) and real_app["calls"] == []
    verdict, why = LoginFlow().check_stored_login(force_refresh=True)
    assert verdict == "expired" and "oauth2/token" in why and len(real_app["calls"]) == 1


# ---- a relogin that lands inside connect forgets the old identity ----------------------------------------------

@pytest.mark.parametrize("stored", [_StoredApp(fail=_dead_refresh()), _StoredApp(required=True)],
                         ids=["expired", "missing"])
async def test_a_relogin_landing_in_connect_forgets_the_old_identity(stored, monkeypatch):
    from hpc_bridge import login as login_mod
    from hpc_bridge import login_gate

    async def landed(flow, mode=None):
        return object(), "done"

    monkeypatch.setattr(login_gate, "_start_login_and_wait", landed)
    monkeypatch.setattr(login_mod, "_IDENTITY_LABEL", "old@uni.edu")  # cached while the old login worked
    # the label helper's own semantics: the cache if set, else a fetch (which names whoever logged in now)
    monkeypatch.setattr(login_gate, "globus_identity_label", lambda: login_mod._IDENTITY_LABEL or "new@uni.edu")
    app = _app_with(_flow(stored))
    rt = _shape_runtime(app, "compute")
    rt.runner, rt.runner_stale = _FakeRunner("old-eid", _Res(0, "", "")), False  # built on the old token
    res = await _mep_connect(app, monkeypatch)
    assert res.notice.startswith("Globus login landed as new@uni.edu"), res.notice[:80]
    assert rt.runner_stale is True
