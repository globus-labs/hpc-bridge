# notices

> [!abstract] Role
> Every pure agent-facing text builder and result factory in one module: the first-contact explanations (`_explain_provision_error`: NO SSH ACCESS / CANNOT REACH / REMOTE FILESYSTEM), the spend floor (`_spend_floor_guidance`, used by both `ensure_endpoint_up` and `run_shell`/`reset_session`), the warm-block bounds note, the Globus-login instructions (`_login_notice`, `_needs_login_result`), the pre-auth hand-off, the terminal refusals (`_no_account_notice`, the transient-conflict classifier and dispatch-error suffix), and the `ShellOutcome` factories (cold, needs_confirmation, busy, running, past-ceiling, orphaned, error, shape-reject, released-under-the-call, never-sent). The busy and running factories say when a task came from a call the client cancelled (`_CLIENT_CANCELLED`), because the agent never received that task id.

Split step 3 (2026-09-03). A notice IS the product — the agent reads it and acts, and the harness graders key on some phrases (`NO ACCOUNT`, `not confirmed`, `ORPHANED`) — so the words live in one place. No I/O and no state mutation here: a builder reads [[context]] / [[config]] and returns text or a model. [[server]] re-exports every name, so tests that import `server._explain_provision_error` keep working; none of these is monkeypatched.

## See also
[[server]] · [[models]] · [[context]] · [[config]] · [[cost]]
