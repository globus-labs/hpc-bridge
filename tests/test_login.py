# tests/test_login.py — the in-terminal Globus login: the flow state machine (hermetic; the Globus SDK
# is used only for the paste-mode URL, which is built locally — no network) and the connect gate.
import time

import pytest

from hpc_bridge import binding, connect
from hpc_bridge import login as login_mod
from hpc_bridge.login import FLOW_TTL_S, LoginFlow, required_scopes
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx, _authenticate, _complete_login
from tests.fakes import FakeCatalog, FakeFacility, fake_entry


class _FakeApp:
    """Stands in for the Compute-SDK-backed UserApp: `login_required()` is a knob; `login()` runs
    the given manager's URL callback (as the SDK does inside run_login_flow) then 'stores tokens'."""

    def __init__(self, required: bool, manager=None, fail: Exception | None = None):
        self.required, self.manager, self.fail = required, manager, fail
        self.logged_in = False

    def login_required(self):
        return self.required

    def login(self, force=False):
        if self.manager is not None and hasattr(self.manager, "emit"):
            self.manager.emit("https://auth.globus.org/v2/oauth2/authorize?fake=1")
        if self.fail:
            raise self.fail
        self.logged_in = True
        self.required = False


class _FakeManager:
    def __init__(self, on_url):
        self.on_url, self.aborted = on_url, None

    def emit(self, url):
        self.on_url(url)

    def abort(self, why):
        self.aborted = why


@pytest.fixture
def browser_flow(monkeypatch):
    """A LoginFlow whose browser mode uses the fake manager + fake app (no SDK, no browser)."""
    from hpc_bridge import login_flow_manager as lfm

    monkeypatch.setattr(lfm.CapturingLocalServerManager, "build", staticmethod(lambda *, on_url: _FakeManager(on_url)))
    apps = {}

    def factory(manager):
        apps["app"] = _FakeApp(required=True, manager=manager)
        return apps["app"]

    return LoginFlow(app_factory=factory, mode_override="browser"), apps


def test_required_scopes_are_the_endpoint_floor_and_nothing_more():
    # minimum = exactly what globus_compute_endpoint.auth.get_globus_app_with_scopes hard-requires
    # (parity with `globus-compute-endpoint login`); Search is NOT in the default consent — registry
    # reads are anonymous for public entries
    rs = required_scopes()
    assert set(rs) == {"auth.globus.org", "funcx_service"}, rs
    assert any("manage_projects" in s for s in rs["auth.globus.org"]) and any(s.endswith("openid") or s == "openid" for s in rs["auth.globus.org"])
    assert rs["funcx_service"] and all(s.endswith("/all") for s in rs["funcx_service"])
    with_search = required_scopes(include_search=True)
    assert any("search.api.globus.org:search" in s for s in with_search["search.api.globus.org"])


def test_login_required_is_non_prompting_and_conservative():
    assert LoginFlow(app_factory=lambda m: _FakeApp(required=True)).login_required() is True
    assert LoginFlow(app_factory=lambda m: _FakeApp(required=False)).login_required() is False

    def broken(m):
        raise OSError("no storage.db")

    assert LoginFlow(app_factory=broken).login_required() is True  # unreadable credential == absent


def test_browser_flow_returns_the_captured_url_then_completes(browser_flow):
    flow, apps = browser_flow
    start = flow.start()
    assert start.mode == "browser" and start.login_url.startswith("https://auth.globus.org/")
    # idempotent while waiting: the same URL, no second thread/flow
    assert flow.start().login_url == start.login_url
    flow._thread.join(timeout=5)
    assert apps["app"].logged_in and flow.status() == "done"


def test_headless_browser_failure_after_url_switches_next_start_to_paste(monkeypatch):
    # The SDK produces the URL FIRST and only then tries the browser (review finding): on a headless
    # host the first browser attempt yields a URL nobody can complete, then dies. The failure must be
    # REMEMBERED so the next start() goes to paste — not re-arm browser forever.
    from hpc_bridge import login_flow_manager as lfm
    monkeypatch.setattr(lfm.CapturingLocalServerManager, "build", staticmethod(lambda *, on_url: _FakeManager(on_url)))
    monkeypatch.setattr(login_mod, "_browser_available", lambda: True)  # pre-flight thinks a browser exists
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True, manager=m, fail=RuntimeError("Failed to open browser")))
    first = flow.start()  # the fake emits the URL, then fails — exactly the SDK's order
    assert first.mode == "browser"
    flow._thread.join(timeout=5)
    assert flow.status() == "failed" and flow._browser_failed is True
    second = flow.start()
    assert second.mode == "paste" and "auth-code" in second.login_url


def test_browser_preflight_failure_goes_straight_to_paste(monkeypatch):
    monkeypatch.setattr(login_mod, "_browser_available", lambda: False)
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True))
    assert flow.start().mode == "paste"


def test_browser_flow_failure_before_url_falls_back_to_paste(monkeypatch):
    from hpc_bridge import login_flow_manager as lfm

    class _NoUrlManager(_FakeManager):
        def emit(self, url):  # the flow died before producing a URL at all
            pass

    monkeypatch.setattr(lfm.CapturingLocalServerManager, "build", staticmethod(lambda *, on_url: _NoUrlManager(on_url)))
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True, manager=m, fail=RuntimeError("no browser")), mode_override="browser")
    start = flow.start()
    assert start.mode == "paste"
    assert "auth.globus.org/v2/oauth2/authorize" in start.login_url and "4cf29807-cf21-49ec-9443-ff9a3fb9f81c" in start.login_url
    assert flow.error and "paste-back" in flow.error


def test_expired_worker_cannot_clobber_the_rearmed_flow(monkeypatch):
    # review finding: after the TTL expires and a new attempt is armed, the OLD worker wakes up failing
    # and used to mark the NEW flow failed (and orphan its listener). Generation-guarded now.
    import threading as _th

    from hpc_bridge import login_flow_manager as lfm
    monkeypatch.setattr(lfm.CapturingLocalServerManager, "build", staticmethod(lambda *, on_url: _FakeManager(on_url)))
    monkeypatch.setattr(login_mod, "_browser_available", lambda: True)
    release = _th.Event()

    hold = _th.Event()
    attempts = []

    class _SlowFailApp(_FakeApp):
        def login(self, force=False):
            attempts.append(self)
            self.manager.emit(f"https://auth.globus.org/v2/oauth2/authorize?attempt={len(attempts)}")
            if len(attempts) == 1:
                release.wait(timeout=5)  # the OLD worker wakes AFTER the new flow is armed…
                raise RuntimeError("login URL expired")  # …and fails: must not touch the new flow
            hold.wait(timeout=5)  # the re-armed attempt just waits (a user at the browser)

    flow = LoginFlow(app_factory=lambda m: _SlowFailApp(required=True, manager=m), mode_override="browser", ttl_s=0.05)
    flow.start()
    time.sleep(0.1)
    assert flow.status() == "expired"
    flow.ttl_s = 60  # the re-armed attempt must outlive the assertion window (only the FIRST was meant to expire)
    second = flow.start()  # a new generation (browser again: _browser_failed is still False)
    release.set()
    time.sleep(0.3)
    assert flow.status() == "waiting", (flow.status(), flow.error)  # the stale failure was ignored
    assert flow._start is second and len(attempts) == 2
    hold.set()


def test_paste_flow_url_carries_every_required_scope_and_pkce():
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True), mode_override="paste")
    url = flow.start().login_url
    assert "manage_projects" in url and "openid" in url and "code_challenge=" in url
    assert "search.api.globus.org" not in url  # not part of the minimum consent
    assert "access_type=offline" in url  # refresh tokens: one round-trip for the life of the install


def test_expiry_rearms_a_fresh_flow():
    # paste mode: nothing completes on its own, so the flow WAITS until its TTL lapses
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True), mode_override="paste", ttl_s=0.01)
    first = flow.start()
    time.sleep(0.05)
    assert flow.status() == "expired"
    second = flow.start()
    assert second.expires_at > first.expires_at  # a new flow, not the stale one


def test_complete_with_code_requires_a_waiting_paste_flow(monkeypatch):
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True), mode_override="paste")
    with pytest.raises(RuntimeError, match="authenticate"):
        flow.complete_with_code("abc")  # nothing waiting
    flow.start()
    stored = {}
    monkeypatch.setattr("hpc_bridge.login_flow_manager.store_paste_tokens", lambda client, code: stored.update(code=code))
    flow.complete_with_code("  the-code  ")
    assert stored["code"] == "the-code" and flow.status() == "done"


# --- the connect gate + tools -----------------------------------------------------------------------


class _StubFlow:
    def __init__(self, required, wait_result="waiting"):
        self.required, self.error, self.started = required, None, 0
        self.wait_result, self.waited = wait_result, []
        self._browser_failed = False

    def login_required(self):
        return self.required

    def start(self, mode=None):
        self.started += 1
        from hpc_bridge.login import LoginStart
        mode = mode or ("paste" if self._browser_failed else "browser")
        return LoginStart(login_url="https://auth.globus.org/v2/oauth2/authorize?x=1", mode=mode, expires_at=time.monotonic() + FLOW_TTL_S)

    def wait(self, timeout_s, poll_s=0.25):
        self.waited.append(timeout_s)
        if self.wait_result == "done":
            self.required = False
        if self.wait_result == "failed":
            self._browser_failed = True
        return self.wait_result

    def complete_with_code(self, code):
        self.required = False

    def check_stored_login(self, force_refresh=False):  # a stored login here always works (tests/test_login_expired.py covers the rest)
        return "works", None


async def test_connect_gates_on_login_before_any_ssh(monkeypatch):
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    built = []
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: built.append(1))
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_login" and res.login_mode == "browser"
    assert res.login_url.startswith("https://auth.globus.org/")
    assert "connect_facility again" in res.notice and "never ask" in res.notice.lower()
    assert built == []  # no facility, no SSH, no bootstrap until the login lands
    assert app.login_flow.started == 1


async def test_connect_proceeds_when_logged_in_and_when_ungated(monkeypatch):
    from hpc_bridge import server
    from tests.test_server import _FakeRunner, _Res

    for flow in (_StubFlow(required=False), None):
        f = FakeFacility()
        f.workers = 1
        app = AppCtx(facility=FakeFacility(), profile=Profile())
        app.login_flow = flow
        app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
        entry = fake_entry(id="anvil", facility_key="purdue")
        entry.allocation = None
        monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))  # noqa: B023 - bound per iteration by the immediate call
        monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: f)  # noqa: B023 - bound per iteration by the immediate call
        res = await server._connect_facility(app, "anvil")
        assert res.phase == "needs_account", (flow, res.notice)


async def test_authenticate_and_complete_login_tools():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True)
    st = await _authenticate(app)
    assert st.phase == "needs_login" and st.login_url and st.login_mode == "browser"
    done = await _complete_login(app, "code")
    assert done.phase == "logged_in"
    assert (await _authenticate(app)).phase == "logged_in"
    # force re-login re-arms even when logged in
    assert (await _authenticate(app, force=True)).phase == "needs_login"


async def test_complete_login_without_a_flow_is_structured():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    assert (await _complete_login(app, "x")).phase == "failed"


def test_needs_login_is_a_known_phase():
    from hpc_bridge.models import ConnectFacilityResult
    r = ConnectFacilityResult(phase="needs_login", facility="f", login_url="https://x", login_mode="paste")
    assert r.login_mode == "paste"


def test_loopback_handler_never_logs_the_request_line():
    # L4 showed the SDK's RedirectHandler logging "GET /?code=<one-time code>&state=…" to stderr — in the
    # real server that lands in logs/transcripts. Our manager installs a quiet handler and keeps the
    # server handle for abort(). (Binds an ephemeral localhost port; no network beyond that.)
    from globus_sdk.login_flows.local_server_login_flow_manager.local_server import RedirectHandler

    from hpc_bridge.login_flow_manager import CapturingLocalServerManager

    m = CapturingLocalServerManager.build(on_url=lambda u: None)
    with m.background_local_server() as server:
        cls = server.RequestHandlerClass
        assert issubclass(cls, RedirectHandler) and cls is not RedirectHandler
        assert cls.log_message(None, "%s", "GET /?code=SECRET") is None  # a no-op, not stderr
        assert server.server_address[0] in ("127.0.0.1", "::1", "localhost")
        assert m._server is server
    assert m._server is None


def test_complete_with_code_after_expiry_is_refused(monkeypatch):
    flow = LoginFlow(app_factory=lambda m: _FakeApp(required=True), mode_override="paste", ttl_s=0.01)
    flow.start()
    time.sleep(0.03)
    with pytest.raises(RuntimeError, match="expired"):
        flow.complete_with_code("late")


async def test_connect_gate_runs_before_the_catalog_read(monkeypatch):
    # review MERGE-BLOCKER: the gate used to sit after make_catalog(), whose Client() would run the
    # SDK's command-line login on the MCP transport (stdout URL + stdin input()) on a fresh install.
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True)

    def no_catalog():
        raise AssertionError("catalog must not be touched before the login gate")

    monkeypatch.setattr(binding, "make_catalog", no_catalog)
    monkeypatch.setattr(connect, "_propose_or_ask", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no SSH probe before login")))
    res = await server._connect_facility(app, "anything", ssh_host="login.example.edu")
    assert res.phase == "needs_login"


async def test_authenticate_mode_override_forces_paste():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True)
    st = await _authenticate(app, mode="paste")
    assert st.login_mode == "paste"


def test_wait_returns_done_when_the_browser_flow_lands(monkeypatch):
    import threading as _th

    from hpc_bridge import login_flow_manager as lfm
    monkeypatch.setattr(lfm.CapturingLocalServerManager, "build", staticmethod(lambda *, on_url: _FakeManager(on_url)))
    monkeypatch.setattr(login_mod, "_browser_available", lambda: True)
    go = _th.Event()

    class _LandsLater(_FakeApp):
        def login(self, force=False):
            self.manager.emit("https://auth.globus.org/v2/oauth2/authorize?late=1")
            go.wait(timeout=5)  # the redirect arrives a moment after the tool started waiting

    flow = LoginFlow(app_factory=lambda m: _LandsLater(required=True, manager=m))
    start = flow.start()
    assert start.mode == "browser"
    assert flow.wait(0.05) == "waiting"  # not yet
    _th.Timer(0.1, go.set).start()
    assert flow.wait(3) == "done"  # the wait returns as soon as the flow completes, not at the deadline


async def test_connect_continues_in_the_same_call_when_the_login_lands(monkeypatch):
    # the Cloudflare-plugin feel: one tool call — browser opens, user approves, the connection proceeds.
    # (Live: with a Globus web session + prior consent the redirect landed in ~4 s; the old behaviour
    # returned needs_login at once and the agent told the user to 'say when done'.)
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    flow = app.login_flow = _StubFlow(required=True, wait_result="done")
    monkeypatch.setenv("HPC_BRIDGE_LOGIN_WAIT_S", "42")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, account="": (_ for _ in ()).throw(RuntimeError("past the gate")))
    res = await server._connect_facility(app, "anvil")
    assert res.phase != "needs_login" and "past the gate" in (res.notice or "")
    assert flow.waited == [42.0] and flow.started == 1


async def test_connect_reports_the_wait_when_the_login_is_still_open(monkeypatch):
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    flow = app.login_flow = _StubFlow(required=True, wait_result="waiting")
    monkeypatch.setenv("HPC_BRIDGE_LOGIN_WAIT_S", "7")
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_login" and res.login_mode == "browser"
    assert "waited 7s" in res.notice and "SINGLE-USE" in res.notice and flow.waited == [7.0]


async def test_connect_rearms_in_paste_mode_when_the_browser_attempt_fails_during_the_wait():
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    flow = app.login_flow = _StubFlow(required=True, wait_result="failed")
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_login" and res.login_mode == "paste" and flow.started == 2
    assert "waited" not in res.notice


async def test_authenticate_returns_logged_in_when_the_browser_flow_lands():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True, wait_result="done")
    st = await _authenticate(app)
    assert st.phase == "logged_in" and "browser" in st.notice


def test_globus_identity_label_uses_the_v4_userinfo_call(monkeypatch):
    # globus-sdk 4 renamed oauth2_userinfo() -> userinfo(); the helper swallowed the AttributeError and
    # returned None silently (found live by the no-account driver). Pin the real method name.
    import globus_sdk
    monkeypatch.setattr(login_mod, "_IDENTITY_LABEL", None)

    class _App:
        def login_required(self): return False
        def get_authorizer(self, rs): return object()

    class _Auth:
        def __init__(self, *, authorizer): pass
        def userinfo(self): return {"sub": "x-id"}  # openid alone: NO preferred_username (found live)
        def get_identities(self, *, ids): return {"identities": [{"id": ids, "username": "alice@example.edu"}]}

    assert hasattr(globus_sdk.AuthClient, "userinfo") and not hasattr(globus_sdk.AuthClient, "oauth2_userinfo")
    monkeypatch.setattr(login_mod, "_default_app_factory", lambda m: _App())
    monkeypatch.setattr(globus_sdk, "AuthClient", _Auth)
    assert login_mod.globus_identity_label() == "alice@example.edu"
    assert login_mod.globus_identity_label(fetch=False) == "alice@example.edu"  # cached
    monkeypatch.setattr(login_mod, "_IDENTITY_LABEL", None)


async def test_authenticate_landing_forgets_no_account_verdicts():
    from hpc_bridge.server import _shape_runtime
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = _StubFlow(required=True, wait_result="done")
    rt = _shape_runtime(app, "compute")
    rt.no_account = "Identity failed to map to a local user name."
    st = await _authenticate(app)
    assert st.phase == "logged_in" and rt.no_account is None and rt.runner_stale is True
