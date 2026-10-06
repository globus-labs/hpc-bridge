# In-terminal Globus login

> [!abstract] In one line
> Replace the manual prerequisite (`globus-compute-endpoint login` in a terminal, before first use) with a **Cloudflare-shaped OAuth login that the agent surfaces and the browser completes** — a `needs_login` phase carrying an authorize URL; the browser redirects back to a loopback listener inside the server and the session continues; paste-back as the fallback. One consent covers every scope hpc-bridge needs. It rides the **Compute SDK's own `UserApp`** (same client id, same `storage.db`) so endpoint credential seeding keeps working unchanged. Tier-2 item B of [[V1 release]]; the same mechanism later carries a MEP's consent (M2).

> [!success] Built and merged — 2026-09-03, [PR #48](https://github.com/globus-labs/hpc-bridge/issues/48)
> This note is the design record and its live findings. The implementation is documented as ground truth in [[login]] (`LoginFlow`, the wait-and-continue, `globus_identity_label`) and [[login_flow_manager]] (the quiet loopback manager, paste-back); the tools and phases in [[The MCP tools]] / [[models]]; the gate's position in [[server]]. `HPC_BRIDGE_LOGIN_WAIT_S` in [[Configuration]].

## Why

A stranger's first run today: hpc-bridge needs a Globus login carrying `openid` + `manage_projects` + refresh tokens ([[Credential seeding]]); a plain SDK login isn't enough; the MCP server **cannot prompt** (stdio), so `credentials.MissingCredentials` surfaces as a generic `failed` telling the user to go run a CLI command elsewhere and come back. That is the single worst moment of the new-user experience — and it's the exact problem the Cloudflare MCP solved with `authenticate` / `complete_authentication`.

## Verified facts this rests on (2026-09-03)

- **globus-sdk 4.8 ships the flow.** `GlobusAppConfig(login_flow_manager="local-server")` → `LocalServerLoginFlowManager`: opens the browser, runs a loopback HTTP server on an **ephemeral `localhost` port**, receives the auth code on redirect, exchanges it, returns tokens — the terminal continues. `"command-line"` is the paste-back variant (Globus shows a one-time code page). `_check_remote_session()` refuses the local-server flow in SSH/headless sessions — the natural switch to paste-back.
- **The client id is pinned — and must be.** The Compute SDK builds `UserApp(client_id=DEFAULT_CLIENT_ID …)` (`4cf29807-…`); an override requires id **and** secret (a *confidential* client — a different thing). The remote `globus-compute-endpoint` refreshes tokens with **that** client id, so any token the endpoint must refresh has to be issued to it. ⟹ hpc-bridge drives the **same** `UserApp` (same client, same `storage.db` at `GLOBUS_COMPUTE_USER_DIR`), only choosing the login-flow manager. **Registering our own OAuth client (the Cloudflare way) would break seeding — don't.**
- **Localhost redirect for that client:** an unauthenticated fetch of the authorize URL with `redirect_uri=http://localhost:8642/` rendered Globus's normal login page (HTTP 200), identical to the control with the client's known redirect — no up-front rejection. Suggestive, not conclusive (Globus may re-validate at consent). **The definitive check is one real login (~10 s) once built**; if it fails, the code falls back to paste-back automatically.
- **Anonymous registry reads** ([[Facility catalog]]): Search "requires authentication only for non-public entries" — so the *registry* does **not** need login. Login is needed only to **dispatch** (Compute) and to **start an endpoint** (`manage_projects`). Consequence: `list_facilities` never triggers login; `connect_facility` does, lazily, at first need.

## Design

**Phase, not prompt.** `connect_facility` checks, before any SSH, whether the Compute app is logged in with adequate scopes (`app.login_required()` against the required scope set, plus the seeding adequacy check `credentials` already does). If not, it returns **`phase="needs_login"`** with:
- `login_url` — the Globus authorize URL (PKCE, `refresh_tokens=True`, all scopes below);
- `login_mode` — `"browser"` (a loopback listener is waiting; completing the login in the browser finishes it) or `"paste"` (remote/headless: Globus will show a code; hand it to `complete_login`);
- a `notice` telling the agent exactly what to say: show the link, wait for the user, call `connect_facility` again. **The agent never sees or handles a token**; in browser mode it never sees the code either (browser → `localhost` → server).

**Tools.**
- `authenticate()` — explicit trigger: (re)starts the flow and returns the same `{login_url, login_mode}`; for "log me in first" or an expired URL.
- `complete_login(code)` — paste-back only: exchanges a one-time auth code (not a token) for tokens into the Compute `storage.db`. Mirrors Cloudflare's `complete_authentication`.
- `connect_facility` re-called after login proceeds as today (probe / bootstrap / seeding all unchanged).

**Minimum consent (implemented, verified live 2026-09-03):** exactly the endpoint floor the Compute SDK's own `globus-compute-endpoint login` asks for — Compute (`…facd7ccc…/all`) + Auth `openid` + `manage_projects` (`globus_compute_endpoint/auth.py:get_globus_app_with_scopes`), requested through the SDK's OWN `UserApp` / client id / `storage.db` so the remote endpoint can refresh what we stored. **Search is NOT requested:** the registry is public, so `list_facilities` and catalog reads are anonymous (`SearchClient()` without an app — see [[Globus index discovery channel]]); a Search scope only ever enters via the curator CLI (`hpc-bridge-catalog`). One consent screen; refresh tokens make later runs silent. *Why the login may bounce through UChicago / ACCESS:* Globus is a federation broker — a facility's high-assurance / session policy demands a *recent* authentication of the linked identity, so Globus redirects to that IdP. That is Globus's rule, not a scope of ours (the same URL logs a user with no such policy in with Globus alone).

**The listener.** Bound to `127.0.0.1` only, ephemeral port, single-use `state`, PKCE, **10-minute lifetime** then it stops and the URL is dead (a fresh `needs_login` re-arms it — idempotent). Runs as a background thread in the server process; completing the login writes tokens via the SDK's own token storage. It exists only between `needs_login` and the next call.

**Fallbacks (implemented).** Browser mode is armed only when a graphical browser is available on this machine — a pre-flight `webbrowser.get()` against the SDK's text-browser deny list; `SSH_TTY`/`SSH_CONNECTION` ⇒ paste — because the SDK only discovers 'no browser' *after* producing the URL. A browser attempt that dies (no browser, Globus rejected the redirect) is remembered, so the next `authenticate` goes straight to paste; `authenticate(mode="paste")` forces it; a flow whose URL was never produced falls back to paste in-call. Paste-back = Globus's auth-code page: the user hands the one-time code to `complete_login(code)` — never a password, and the code is stored via the SDK's *validating* storage (same unchanging-identity check a browser login gets). 10-min TTL: an expired attempt can't be completed, and a stale worker from an earlier attempt can't touch a re-armed flow (per-attempt generation). The login gate runs **first** in `connect_facility` — before the catalog read (whose SDK `Client` would otherwise run its own command-line login on the MCP transport) and before any SSH.

**Wait-and-continue (added after the first fresh-user test, 2026-09-03).** The first design returned `needs_login` the instant the listener was armed and left the agent to ask the user to 'say when done'. Live, with a Globus web session and this client's consent already in the browser, the redirect landed **4 s** after the URL was issued — the login was over before the agent finished writing its message, the user then re-opened the single-use link (the loopback had already closed) and read the failure as 'it's reusing the token'. Now the tool call that arms a browser flow **waits for it** (`LoginFlow.wait`, up to `HPC_BRIDGE_LOGIN_WAIT_S` = 90 s — a real IdP round-trip fits; well under the 10-min TTL and any MCP tool timeout, which `run_shell` already exceeds routinely) and, when it lands, **continues the connection in the same call** — the Cloudflare-plugin feel the design was after. A browser attempt that fails during the wait is re-armed in paste mode at once. Only a still-open (slow) login returns `needs_login`, and its notice says how long it waited, that a finished page means 'just call again', and that the link is single-use. The listener lives in the MCP server process: quitting the session mid-login kills it (the tokens already stored survive).

## What the agent is taught (`skills/driving-hpc/SKILL.md` — [[Plugin packaging]])
On `needs_login`: present `login_url` as a link and say what will happen ("your browser will log you in to Globus; when it says you can return, tell me"); then call `connect_facility` again. In paste mode: ask the user to paste the code Globus shows and call `complete_login(code)`. **Never** ask for a Globus password; never paste a URL into a shell. Same discipline as `needs_preauth`.

## Not in scope / later
- M2: a facility MEP's consent-required 401 becomes a `needs_login` with the consent URL — same phase, same tools.
- Multiple Globus identities / choosing an identity: the Globus login page handles it.
- ~~Logout / re-login (`force`): `authenticate(force=True)` is a natural extension; not V1.~~ *Superseded: `authenticate(force=True)` shipped in the same PR (it re-arms a login even when one is present), and `authenticate(mode="paste")` forces paste mode. A new login also drops the sticky MEP no-account verdicts and rebuilds the runners (`_forget_identity_verdicts`, [[server]]).*

## Milestones
| L | Deliverable |
|---|---|
| **L1** | ✅ **built** (`login.py`, `login_flow_manager.py`, `server.py`) — detection at connect → `needs_login` + `login_url`/`login_mode`; loopback browser flow; `authenticate(force)` |
| **L2** | ✅ **built** — `complete_login(code)`; paste mode chosen for remote/headless sessions or when the browser flow can't produce a URL |
| **L3** | ✅ **built** — SKILL.md + `hpc-connect.md` guidance; models; 12 hermetic tests (a start()/worker-thread lock deadlock in the fallback path was found by them and fixed) |
| **L4** | ✅ **PASSED 2026-09-03** — scratch token dir → `needs_login` (browser) → login + one consent → Globus redirected to the loopback on `127.0.0.1` → `done` → `login_required()` False → all three resource servers stored with the required scopes **and refresh tokens**. **Globus honours the `localhost` redirect for the Compute client — confirmed.** Follow-up found by the check: the SDK's redirect handler logs the request line (with the one-time code) to stderr — silenced in our manager subclass. |
| L5 fresh-user walk | ✅ 2026-09-03 |

## See also
[[login]] · [[login_flow_manager]] · [[V1 release]] · [[Credential seeding]] · [[Facility catalog]] · [[Endpoint reuse and MEP integration]] (M2) · [[MFA and interactive SSH auth]] (the `needs_preauth` precedent) · [[credentials]]
