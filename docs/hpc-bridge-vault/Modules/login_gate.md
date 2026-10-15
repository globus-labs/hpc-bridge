# login_gate

> [!abstract] Role
> The Globus login gate as the tools see it: `_start_login_and_wait` (arm, wait up to `HPC_BRIDGE_LOGIN_WAIT_S` in browser mode and continue in the same call; re-arm in paste mode if the browser attempt dies), and the tool bodies `_authenticate` / `_complete_login`, which also forget the sticky no-account verdicts because a new login may be a different identity.

`_authenticate` (non-force) answers `logged_in` only when the stored login still works ([[login]] `check_stored_login`): one that Globus refuses to renew (revoked or expired) gets `needs_login` with that reason (`_login_notice(why=…)`) and a login armed like a first one; a check that cannot reach Globus keeps `logged_in` and says it could not be checked — a network error never calls a login dead (live 2026-10-14: a dead refresh token answered `logged_in`).

Split step 9 (2026-09-03). The flow machinery stays in [[login]] (kept free of server/runtime imports); this module is the seam between the flow and the runtime. Nothing here is monkeypatched; `server` re-exports the three names and its tools call through the module.

## See also
[[login]] · [[login_flow_manager]] · [[server]] · [[In-terminal Globus login]]
