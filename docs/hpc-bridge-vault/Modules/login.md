# login.py

> [!abstract] Role
> The **in-terminal Globus login** — surfaced as a *phase* (`needs_login`), never a prompt. `LoginFlow` arms one OAuth login per server process (a browser loopback flow, or paste-back for headless sessions), riding the Compute SDK's **own** client id and `storage.db` so the credential it stores is the one a started endpoint can refresh. Design record + live findings: [[In-terminal Globus login]] (Tier-2 B of [[V1 release]], [#48](https://github.com/globus-labs/hpc-bridge/issues/48)).

## What it does

- **`required_scopes(include_search=False)`** (`login.py:41`) — the ONE consent, and the **minimum**: exactly what a started endpoint hard-requires (Compute + Auth `openid`/`manage_projects`, resolved from the SDK through [[credentials]] `_required_scopes` — parity with `globus-compute-endpoint login`), with refresh tokens. Search is *not* requested — registry reads are anonymous ([[Facility catalog]]); `include_search=True` exists for a future `visible_to`-restricted registry.
- **`LoginFlow`** (`:91`) — the one in-flight login per process (`AppCtx.login_flow`, installed by `lifespan` in [[server]]; `None` ⇒ no login gating, for hermetic tests):
  - `login_required()` (`:112`) — non-prompting (a local SQLite read): does the stored credential satisfy every required scope? A missing or unreadable `storage.db` counts as *required* — the safe first-run default.
  - `start(mode)` (`:149`) — (re)arm a login; **idempotent while one is waiting** (the same URL comes back). The mode defaults to `browser` when `_browser_available()` (`:65`) finds a graphical browser (not an `SSH_TTY`/`SSH_CONNECTION` session, not a text browser from the SDK's deny list), else `paste`; a remembered browser failure (`_browser_failed`) goes straight to paste next time. Returns a `LoginStart` (`:82`: `login_url`, `mode`, `expires_at`).
  - `_start_browser_locked` (`:167`) — runs the SDK's loopback flow (`CapturingLocalServerManager`, [[login_flow_manager]]) in a daemon thread: the SDK opens the browser itself, and the captured authorize URL is *also* returned so the agent can show it if that failed. No URL within 15 s ⇒ falls back to paste **in the same call**.
  - `wait(timeout_s)` (`:130`) — block until the armed login leaves `waiting` (`done` / `failed` / `expired`) or the timeout passes. This is what lets a tool call **wait and continue** ([[server]] `_start_login_and_wait`, bounded by `HPC_BRIDGE_LOGIN_WAIT_S`, default 90 s): with a Globus web session + prior consent the redirect lands in ~4 s and `connect_facility` simply carries on.
  - `complete_with_code(code)` (`:223`) — paste-back: exchange the one-time auth code and store the tokens (`store_paste_tokens`). Refused unless a *paste* login is waiting and unexpired.
  - States: `idle` → `waiting` → `done` | `failed` | `expired` (`FLOW_TTL_S` = 600 s, `:36`; an expired listener is aborted, and the next `needs_login` re-arms a fresh one).
- **`globus_identity_label(fetch=True)`** (`:249`) — best-effort "who am I" for notices (the NO ACCOUNT verdict on a [[facility-mep|MEP]] names the refused identity): `preferred_username` from openid userinfo, else the identity's username via `get_identities` (with `openid` alone userinfo carries only `sub`). Cached after the first success; `fetch=False` returns only the cache (for sync callers); never prompts; `None` on any failure.
- **`_default_app_factory(manager)`** (`:277`) — the REAL app: `UserApp(client_id=DEFAULT_CLIENT_ID, token_storage=get_token_storage(), request_refresh_tokens=True)` plus our scope set, and — when given — our capturing loopback manager. `manager=None` builds the non-prompting instance `login_required()` uses.

> [!warning] Ride the Compute SDK's client id and storage — never register our own OAuth client
> The remote `globus-compute-endpoint` refreshes tokens with the SDK's native client id; a token issued to any other client can't be refreshed there, and [[Credential seeding]] would ship a dead credential. So this module differs from the SDK's own login **only in the flow manager**. (Registering a separate OAuth client — the literal Cloudflare-plugin approach — is exactly what not to do here.)

> [!warning] Per-attempt generation, and no lock in the worker
> A dead worker thread from an EARLIER attempt (its TTL expired, a new flow was armed) must not mark the new flow `failed` — `_gen` guards every state write. And the worker writes plain attributes only: `start()` holds `_lock` while it waits for the URL, so taking the lock in the thread deadlocks the fallback path (found by the unit tests, `tests/test_login.py`).

> [!warning] Do not build `AuthClient(app=app)` for the identity label
> That registers scopes the login may not hold and makes the app want a *new* login (found by `agentic/whoami_globus.py`). `globus_identity_label` uses the app's existing `auth.globus.org` authorizer instead.

> [!note] Hermetic by construction
> `login.py` imports no Globus SDK at module level — the SDK-touching pieces live in [[login_flow_manager]] — and tests inject `app_factory` / `mode_override`.

## See also
[[login_flow_manager]] · [[server]] · [[models]] · [[credentials]] · [[Credential seeding]] · [[In-terminal Globus login]] · [[The MCP tools]] · [[Configuration]]
