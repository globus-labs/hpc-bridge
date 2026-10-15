"""In-terminal Globus login — the Cloudflare-shaped OAuth flow, surfaced as a *phase*, never a prompt.

The MCP server runs over stdio and can never prompt. So when Globus credentials are missing or
under-scoped, `connect_facility` returns `phase="needs_login"` carrying an authorize URL: the agent
shows it, the user's browser completes the login, Globus redirects to a **loopback listener inside
this process**, tokens are stored, and the next `connect_facility` proceeds. Paste-back (a one-time
auth CODE, not a token, handed to `complete_login`) is the fallback for remote/headless sessions.
Design: docs/hpc-bridge-vault/Planned/In-terminal Globus login.md.

Two constraints are load-bearing:
- **Ride the Compute SDK's own client id and token storage.** The remote `globus-compute-endpoint`
  refreshes tokens with the Compute SDK's native client id; a token issued to any other client can't
  be refreshed there, and credential seeding (`credentials.build_minimal_storage_db`) would ship a
  dead credential. So this module builds a `UserApp` with the SAME client id + the SAME
  `storage.db`/namespace the SDK uses — it differs only in the login-flow manager. (Registering a
  separate OAuth client, the literal Cloudflare approach, is exactly what NOT to do here.)
- **One consent, the minimum:** exactly what a started endpoint hard-requires — the Compute scope +
  Auth `openid` + `manage_projects` (`globus_compute_endpoint.auth.get_globus_app_with_scopes`; the
  same set `globus-compute-endpoint login` requests) — with refresh tokens, so it is one browser
  round-trip for the lifetime of the install. Search is NOT requested: registry reads are anonymous
  for public entries; a `visible_to`-restricted registry would ask for it lazily via the same flow.

Globus Auth allows native clients an implicit redirect to `localhost:<any-port>` (the SDK's own
LocalServerLoginFlowManager relies on it), so nothing needs registering.
"""
from __future__ import annotations

import os
import platform
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

LoginMode = Literal["browser", "paste"]
FLOW_TTL_S = 600.0  # a login URL / listener lives this long, then a fresh needs_login re-arms one

SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:search"

# What every notice says when Globus Auth refuses the stored login's refresh (`login_expired`).
LOGIN_EXPIRED = "the stored Globus login no longer works (expired or revoked)"
_TOKEN_PATH = "/v2/oauth2/token"
# The throwaway app's login client (the stored-login check, the identity label): a black-holed network must not hold
# a tool call for the SDK's default 60 s timeout times six attempts.
CHECK_HTTP_TIMEOUT_S = 15.0
CHECK_MAX_RETRIES = 1


class LoginExpiredError(RuntimeError):
    """`LoginFlow.check_stored_login` found the stored login dead (Globus Auth refused its refresh): raised before a
    bootstrap, which would otherwise copy it to the login node. `login_expired` recognises it."""


def login_expired(exc: BaseException | None) -> bool:
    """Is `exc` Globus Auth refusing to refresh the stored login, so that only a new login helps? That is the SDK's
    AuthAPIError, HTTP 400 or 401, from the token endpoint (POST …/v2/oauth2/token) or with the `invalid_grant` code:
    the refresh token was revoked or has expired. Seen live (2026-10-14) as `('POST',
    'https://auth.globus.org/v2/oauth2/token', None, 400, None, 'Bad Request')` on every dispatch. Not a dead login:
    any other Globus API error (a 403 from the Compute service), another Auth call, a network error. Follows an
    explicit `raise … from` chain."""
    try:
        from globus_sdk import AuthAPIError
    except ImportError:  # pragma: no cover - the SDK is a core dependency
        return False
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, LoginExpiredError):
            return True
        if isinstance(exc, AuthAPIError) and getattr(exc, "http_status", None) in (400, 401):
            url = exc.args[1] if len(exc.args) > 1 and isinstance(exc.args[1], str) else ""
            try:
                grant = getattr(exc, "code", None) or (exc.raw_json or {}).get("error")
            except Exception:  # noqa: BLE001 - an unreadable body: the endpoint alone decides
                grant = None
            if url.split("?", 1)[0].rstrip("/").endswith(_TOKEN_PATH) or grant == "invalid_grant":
                return True
        exc = exc.__cause__
    return False


def required_scopes(*, include_search: bool = False) -> dict[str, list[str]]:
    """resource_server -> scopes to request in ONE consent. Default = the MINIMUM: exactly what a
    started endpoint hard-requires (Compute + Auth openid/manage_projects — from `credentials`,
    resolved from the SDK so it tracks upstream; parity with `globus-compute-endpoint login`).
    `include_search=True` adds Search read — only for a registry with non-public entries; public
    entries are read anonymously, so it is not part of the default consent."""
    from .credentials import _required_scopes

    out = {rs: list(dict.fromkeys(scopes)) for rs, scopes in _required_scopes().items()}  # deduped, ordered
    if include_search:
        out.setdefault("search.api.globus.org", [])
        if SEARCH_SCOPE not in out["search.api.globus.org"]:
            out["search.api.globus.org"].append(SEARCH_SCOPE)
    return out


def _remote_session() -> bool:
    """A browser on THIS machine can't reach a loopback listener in a remote/headless session."""
    return bool(os.environ.get("SSH_TTY") or os.environ.get("SSH_CONNECTION"))


_TEXT_BROWSERS = ("lynx", "www-browser", "links", "elinks", "w3m")  # the SDK's own deny list


def _browser_available() -> bool:
    """Pre-flight for browser mode: is there a graphical browser this process can open? The SDK
    only discovers 'no browser' AFTER it has produced the URL (URL first, then webbrowser.open), so
    without this check a headless host would arm a loopback flow nobody can complete."""
    if _remote_session():
        return False
    try:
        import webbrowser

        b = webbrowser.get()
        name = getattr(b, "name", None) or getattr(b, "_name", "")
        return name not in _TEXT_BROWSERS
    except Exception:  # noqa: BLE001 - webbrowser.Error (no browser) or anything odd -> paste
        return False


@dataclass
class LoginStart:
    """What the agent gets: a URL to show the user, and how the login will complete."""

    login_url: str
    mode: LoginMode
    expires_at: float  # time.monotonic()


@dataclass
class LoginFlow:
    """The one in-flight login (per server process). `start()` is idempotent while a flow is live.

    `app_factory(manager)` builds the Globus app to log in with — the real one rides the Compute SDK
    (see `_default_app_factory`); tests inject a fake. `mode_override` forces browser/paste."""

    app_factory: Callable | None = None
    mode_override: LoginMode | None = None
    ttl_s: float = FLOW_TTL_S
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _start: LoginStart | None = None
    _thread: threading.Thread | None = None
    _manager: object | None = None
    _state: Literal["idle", "waiting", "done", "failed", "expired"] = "idle"
    error: str | None = None
    _gen: int = 0  # per-attempt generation: a dead worker from an EARLIER attempt must not touch state
    _browser_failed: bool = False  # a browser attempt died (no browser / Globus rejected the redirect) -> paste next
    _paste_client: object | None = field(default=None, repr=False)

    # ---- state ----------------------------------------------------------------------------------

    def login_required(self) -> bool:
        """Non-prompting: does the stored credential satisfy every required scope? Errors (no
        storage.db, unreadable db) count as 'required' — the safe default for a first run."""
        try:
            app = (self.app_factory or _default_app_factory)(None)
            return bool(app.login_required())
        except Exception as exc:  # noqa: BLE001 - treat an unreadable credential as absent
            import sys

            print(f"hpc-bridge: login_required check failed ({type(exc).__name__}: {exc}); treating as login"
                  " required", file=sys.stderr)
            return True

    def check_stored_login(self, *, force_refresh: bool = False
                           ) -> tuple[Literal["works", "expired", "unchecked"], str | None]:
        """Non-prompting: does the stored login still WORK, not only hold every scope (`login_required`)? Each
        required resource server's token is made valid the way a dispatch makes it: one past its expiry is refreshed
        (one call to Globus Auth's token endpoint; none while it is still valid). `force_refresh`: refresh every one
        regardless — a refresh token revoked while its access token is still valid is seen only then (the gate before
        a bootstrap copies the login to a login node). 'expired' (with the error): Globus Auth refused the refresh
        (`login_expired`), so only a new login helps. 'unchecked' (with why): no answer either way — no network,
        Globus unreachable, the stored login changed while it was read."""
        try:
            app = (self.app_factory or _default_app_factory)(None)
            # login_required() builds and caches every resource server's authorizer with the app's login-on-error
            # handler OFF, so get_authorizer() below returns those: it can never start the SDK's own stdin login
            if app.login_required():
                return "unchecked", "the stored login changed while it was being checked"
            for rs in app.scope_requirements:
                authorizer = app.get_authorizer(rs)
                invalidate = getattr(authorizer, "handle_missing_authorization", None)
                if force_refresh and callable(invalidate):
                    invalidate()  # the SDK's own "this access token is no good": the next ensure refreshes it
                ensure = getattr(authorizer, "ensure_valid_token", None)
                if callable(ensure):
                    ensure()
        except Exception as exc:  # noqa: BLE001 - a refused refresh is a verdict; anything else says nothing
            why = f"{type(exc).__name__}: {' '.join(str(exc).split())}"[:200]
            return ("expired" if login_expired(exc) else "unchecked"), why
        return "works", None

    def status(self) -> str:
        with self._lock:
            self._expire_locked()
            return self._state

    def wait(self, timeout_s: float, poll_s: float = 0.25) -> str:
        """Block until the armed login leaves 'waiting' (done/failed/expired) or `timeout_s` passes;
        returns the status. This is what makes the browser flow feel like the Cloudflare plugin's:
        the tool call that started it waits for the redirect and carries on — the user never has to
        say 'done'. Found live: with a Globus web session + prior consent the redirect lands in ~4 s."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            st = self.status()
            if st != "waiting" or time.monotonic() >= deadline:
                return st
            time.sleep(min(poll_s, max(0.0, deadline - time.monotonic())))

    def _expire_locked(self) -> None:
        if self._state == "waiting" and self._start and time.monotonic() > self._start.expires_at:
            self._state = "expired"
            self._stop_listener_locked("login URL expired")

    # ---- the flow -------------------------------------------------------------------------------

    def start(self, mode: LoginMode | None = None) -> LoginStart:
        """(Re)arm a login. Idempotent while one is waiting: the same URL comes back. Browser mode
        runs the SDK's loopback flow in a background thread (it opens the browser itself; the URL
        is ALSO returned so the agent can show it if that failed). Paste mode builds the auth-code
        URL for `complete_login`."""
        with self._lock:
            self._expire_locked()
            if self._state == "waiting" and self._start is not None:
                if mode is None or mode == self._start.mode:
                    return self._start
                # An explicit different mode re-arms (e.g. authenticate(mode="paste") while a browser
                # flow nobody can complete is waiting — found in review). Abort the listener first.
                self._stop_listener_locked("mode switch")
                self._state = "idle"
            if mode is None:
                mode = self.mode_override or (
                    "paste" if (self._browser_failed or not _browser_available()) else "browser"
                )
            self.error = None
            if mode == "browser":
                return self._start_browser_locked()
            return self._start_paste_locked()

    def _start_browser_locked(self) -> LoginStart:
        from .login_flow_manager import CapturingLocalServerManager

        url_ready = threading.Event()
        captured: dict[str, str] = {}

        def on_url(url: str) -> None:
            captured["url"] = url
            url_ready.set()

        manager = CapturingLocalServerManager.build(on_url=on_url)
        app = (self.app_factory or _default_app_factory)(manager)

        self._gen += 1
        gen = self._gen

        def run() -> None:
            # NB: plain attribute writes only — start() holds self._lock while it waits for the URL,
            # so taking the lock here would deadlock the fallback path (found by the unit test). And
            # only THIS attempt's worker may write: after our TTL expires and a new attempt is armed,
            # the old worker wakes up failing — it must not mark the new flow failed (found in review).
            try:
                app.login(force=True)  # blocks until the loopback receives the code (or fails)
                if self._gen == gen and self._state == "waiting":
                    self._state = "done"
            except Exception as exc:  # noqa: BLE001 - browser missing / remote / Globus error
                if self._gen == gen and self._state == "waiting":
                    # A GENUINE browser failure while we were still waiting: remember it (next start()
                    # goes to paste). An abort from TTL expiry / a mode switch raises here too and must
                    # NOT demote every later login to paste (found in review).
                    self._browser_failed = True
                    self._state = "failed"
                    self.error = f"{type(exc).__name__}: {exc}"[:300]
            finally:
                url_ready.set()  # never leave start() hanging

        self._manager = manager
        self._state = "waiting"
        self._thread = threading.Thread(target=run, name="hpc-bridge-login", daemon=True)
        self._thread.start()
        url_ready.wait(timeout=15)
        url = captured.get("url")
        if not url:  # the flow died before producing a URL (e.g. no browser): fall back to paste
            self._state = "idle"
            return self._start_paste_locked(reason=self.error)
        self._start = LoginStart(login_url=url, mode="browser", expires_at=time.monotonic() + self.ttl_s)
        return self._start

    def _start_paste_locked(self, reason: str | None = None) -> LoginStart:
        from .login_flow_manager import paste_flow_url

        self._gen += 1
        url, self._paste_client = paste_flow_url()
        self._state = "waiting"
        self._start = LoginStart(login_url=url, mode="paste", expires_at=time.monotonic() + self.ttl_s)
        if reason:
            self.error = f"browser flow unavailable ({reason}); using paste-back"
        return self._start

    def complete_with_code(self, code: str) -> None:
        """Paste-back: exchange the one-time auth code and store the tokens where the Compute SDK
        (and the seeding) read them. Raises on a bad/expired code."""
        from .login_flow_manager import store_paste_tokens

        with self._lock:
            self._expire_locked()
            client = self._paste_client
            if client is None or self._state != "waiting" or (self._start and self._start.mode != "paste"):
                raise RuntimeError("no paste-back login is waiting (or it expired) — call authenticate() first")
        store_paste_tokens(client, code.strip())
        with self._lock:
            self._state = "done"

    def _stop_listener_locked(self, why: str) -> None:
        m = self._manager
        if m is not None and hasattr(m, "abort"):
            try:
                m.abort(why)
            except Exception:  # noqa: BLE001 - best-effort
                pass


_IDENTITY_LABEL: str | None = None


def reset_identity_label() -> None:
    """Forget the cached label — a new login may be a different identity."""
    global _IDENTITY_LABEL
    _IDENTITY_LABEL = None


def globus_identity_label(*, fetch: bool = True) -> str | None:
    """Best-effort 'who am I' for notices — the stored login's preferred_username (openid userinfo,
    silent refresh through the SDK app's EXISTING auth.globus.org authorizer; never prompts). Cached
    after the first success; `fetch=False` returns only the cache (for sync callers). None on any
    failure. Do NOT build AuthClient(app=app): that registers scopes the login may not hold and would
    make the app want a new login (found by agentic/whoami_globus.py)."""
    global _IDENTITY_LABEL
    if _IDENTITY_LABEL or not fetch:
        return _IDENTITY_LABEL
    try:
        from globus_sdk import AuthClient

        app = _default_app_factory(None)
        if app.login_required():
            return None
        ac = AuthClient(authorizer=app.get_authorizer("auth.globus.org"))
        info = ac.userinfo()  # with `openid` alone this carries `sub` but NO preferred_username
        label = info.get("preferred_username")
        sub = info.get("sub")
        if not label and sub:
            try:
                idents = ac.get_identities(ids=sub).get("identities") or []
                label = (idents[0].get("username") if idents else None) or sub
            except Exception:  # noqa: BLE001 - get_identities may need a scope we don't hold: the id is still a label
                label = sub
        _IDENTITY_LABEL = label or None
    except Exception:  # noqa: BLE001 - a label is a courtesy, never a failure
        return None
    return _IDENTITY_LABEL


def _default_app_factory(manager):
    """The REAL app: the Compute SDK's client id + token storage (so the endpoint can refresh what
    we obtain), our scope set, and — when given — our capturing loopback manager. With
    manager=None it is a non-prompting instance for `login_required()` only."""
    from globus_compute_sdk.sdk.auth.globus_app import DEFAULT_CLIENT_ID
    from globus_compute_sdk.sdk.auth.token_storage import get_token_storage
    from globus_sdk import GlobusAppConfig, NativeAppAuthClient, UserApp
    from globus_sdk.transport import RequestsTransport, RetryConfig

    config = GlobusAppConfig(
        token_storage=get_token_storage(),
        request_refresh_tokens=True,
        **({"login_flow_manager": manager} if manager is not None else {}),
    )
    name = f"hpc-bridge on {platform.node()}"
    if manager is not None:
        app = UserApp(app_name=name, client_id=DEFAULT_CLIENT_ID, config=config)
    else:  # the non-prompting instance: its token refreshes are bounded (CHECK_HTTP_TIMEOUT_S, CHECK_MAX_RETRIES)
        client = NativeAppAuthClient(client_id=DEFAULT_CLIENT_ID, app_name=name, environment=config.environment,
                                     transport=RequestsTransport(http_timeout=CHECK_HTTP_TIMEOUT_S),
                                     retry_config=RetryConfig(max_retries=CHECK_MAX_RETRIES))
        app = UserApp(app_name=name, login_client=client, config=config)
    app.add_scope_requirements(required_scopes())
    return app
