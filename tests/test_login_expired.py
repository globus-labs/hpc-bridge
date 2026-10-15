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
from tests.fakes import FakeCatalog, FakeFacility, fake_mep_entry
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
    def __init__(self, fail=None):
        self.fail, self.ensured = fail, 0

    def ensure_valid_token(self):  # the SDK refreshes here only when the access token has expired
        self.ensured += 1
        if self.fail is not None:
            raise self.fail


class _StoredApp:
    """The non-prompting app: holds every scope (login_required False); its tokens refresh — or don't."""

    def __init__(self, fail=None, required=False):
        self.required, self.logins = required, 0
        self.authorizers = {rs: _Authorizer(fail) for rs in ("auth.globus.org", "funcx_service")}

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
