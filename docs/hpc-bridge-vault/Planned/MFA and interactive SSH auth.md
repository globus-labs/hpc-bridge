# MFA and interactive SSH auth

> [!abstract] In one line
> Extend SSH auth beyond key-only to **Duo/MFA** and **password** facilities (NERSC, Midway) **without the agent ever handling a secret** — passwords and passcodes ride a side-channel (a pre-opened ControlMaster, `SSH_ASKPASS`, or a password manager) that bypasses the LLM; the agent only orchestrates the flow and relays *non-secret* challenges (a Duo "push" selection). The ControlMaster "authenticate once" property (already shipped) makes any handshake a **one-time, per-session** cost, not per-call.

> [!success] The pre-open path is BUILT ([#27](https://github.com/globus-labs/hpc-bridge/issues/27)) — the push relay is not
> `is_interactive_auth_failure` + `NeedsPreauth` + `SshTarget.preauth_command()` ([[facility-remote]]) and `_needs_preauth_result` ([[server]]) implement the "pre-open a ControlMaster in your own terminal" hand-off: `connect_facility` returns `phase="needs_preauth"` with the exact `ssh -fN -o ControlMaster=yes …` line on the server's `%C` ControlPath (the ControlPath-alignment detail below is settled that way), the user authenticates there, and the next call multiplexes. Validated live on Aurora (two MFA passcodes). Requires multiplexing on (`HPC_BRIDGE_SSH_CONTROL_PERSIST` > 0 — else the result says so). The agent-relayed Duo **push** tier (`needs_2fa`, a parked non-`BatchMode` handshake) remains designed-not-built. The sibling Globus-side hand-off — `needs_login` — is [[login]].

## Context
Today `SshTarget.argv` ([[facility-remote]], `remote.py:66`) uses **`BatchMode=yes`** — it *fails fast* on any password or keyboard-interactive prompt. Key-only facilities (globus1, Anvil) work; Duo/password facilities dead-end without the pre-open hand-off above. Two things make MFA tractable rather than a per-call tax:
- **SSH is already a one-time bootstrap.** The reuse keystone (`find_online_endpoint`, [[Standing up the endpoint]]) + the ControlMaster mean a session authenticates **once**, then rides AMQP / the multiplexed master. So an interactive handshake happens at most once per session — the whole leverage.
- **Real targets:** NERSC (**key + Duo**), Midway (**password + Duo** — the harder case, and a facility the maintainer can reach for a live spot-check). Tracking: [#3](https://github.com/globus-labs/hpc-bridge/issues/3).

## The load-bearing security invariant
> [!warning] The agent NEVER handles a secret
> Passwords and Duo **passcodes** must never appear in a tool argument, a tool result, the agent's context, or an `AskUserQuestion` answer — the system safety rules prohibit the agent entering/handling credentials in plaintext. Secrets flow via a **side-channel that bypasses the LLM**: a pre-opened ControlMaster the *user* authenticated, `SSH_ASKPASS` → a user-facing prompt hpc-bridge orchestrates (server code handles it, the LLM doesn't — as it already does a key file), or a password-manager / credential-request tool. The agent **may** relay only *non-secret* challenge selections (Duo `push` = option `1`; the actual approval is a phone tap) and orchestrate the workflow (tell the user to pre-open a master). A Duo **passcode** is a secret → side-channel, never the agent.

## Credential-handling policy (passwords vs MFA)
Classify every auth factor by whether it is a **secret the agent must never see** — that one distinction drives everything:

| Factor | Secret to the agent? | Provided via | Agent's role |
|---|---|---|---|
| SSH key | No — a file ssh reads | `key_path` (config / `~/.ssh/config`) | none |
| **Password** | **Yes** | user types it into ssh in their **own external terminal** (opens the ControlMaster there); or an askpass-to-GUI / password-manager helper | detect → hand off → resume; **never sees it** |
| Duo **push** | **No** — select `1`, approve on phone | agent relays the selection | may relay (non-secret) |
| Duo **passcode** / OTP / TOTP | **Yes** | user types it into ssh directly | detect → hand off → resume; **never sees it** |

**Rules:**
1. **Isolation (hard line).** A *secret* factor never appears in a tool argument, tool result, or `AskUserQuestion` answer.
2. **Relay non-secrets only.** The agent may forward a non-secret selection (Duo push) and orchestrate the workflow (instruct, wait, resume).
3. **Hand off, then resume.** For any secret, the agent hands to an **interactive channel where the user authenticates ssh directly** — opening a ControlMaster — then resumes and multiplexes over it. Authenticate-once ⇒ one handoff per session.
4. **hpc-bridge classifies; the agent doesn't guess.** `connect_facility` returns `needs_preauth` / `needs_2fa` from hpc-bridge's read of the facility's auth (config/probe). The agent acts on that structured signal — it does **not** parse "a password was asked" out of raw ssh output (which would pull the prompt, and risk the response, into context).
5. **Secret channel = the user's own external terminal** (or an opt-in `SSH_ASKPASS`-to-GUI / password-manager helper that prompts *outside* the agent). **Never** the in-session `!` bang-command — no interactive TTY, and typed input would hit the plaintext transcript (verified). Never store a password unless the user opts into a server-held/askpass path.
6. **Confirm before resume.** Wait for an explicit "authenticated" before re-calling; the agent can't observe the master until it tries to use it.

> [!warning] Secrets go through the user's OWN terminal — never in-session (verified)
> The in-session `!` bang-command is **not** a viable secret channel: it runs without an interactive TTY, so the prompt never even appears (Claude Code [#26353](https://github.com/anthropics/claude-code/issues/26353), [#47103](https://github.com/anthropics/claude-code/issues/47103)), and any typed input that *did* land would be written to the plaintext session transcript (`~/.claude/projects/`). So a password/passcode is entered only in the user's **own external terminal** (opening the ControlMaster there); the agent detects the need, instructs, waits for "authenticated", and resumes over the master. An `SSH_ASKPASS`-to-GUI helper (prompting outside the agent) is the only smoother opt-in.

## The auth spectrum (best-first)

| Facility (example) | Auth | hpc-bridge path | Agent's role |
|---|---|---|---|
| globus1, Anvil | key only | `ControlMaster=auto` opens non-interactively | none (works today) |
| **Aurora (ALCF)** | bastion + **two MFA passcodes** (one per hop) | **pre-opened ControlMaster** — `ProxyJump` via `~/.ssh/config`; **validated live** | instruct the user to pre-open once, then multiplex |
| NERSC | key + Duo **push** | agent-relayed push **or** pre-open | relay the push option (*non-secret*) |
| Midway | **password** + Duo | **pre-opened ControlMaster** (secret never touches agent *or* server) | instruct the user to pre-open, then multiplex |

## Mechanism
**Tool round-trip is the portable baseline.** MCP *elicitation* (`ctx.elicit`) works only where the client implements a handler — Claude Code CLI does (v2.1.76+) but the Claude Agent SDK (our test harness) does not, and `ctx.elicit()` **raises** on an unsupported client. So the baseline is a tool round-trip that works with any client; elicitation is an optional Claude-Code-only fast-path (guard with a capability check + round-trip fallback; **never elicit a secret**).

- **Password + Duo (Midway) — pre-open, the primary secure path.** `connect_facility` detects it can't authenticate non-interactively and returns a new phase `needs_preauth` with a *non-secret* instruction: *"open a master yourself — `ssh -fN <host>` (you'll enter your password + approve Duo once) — then re-call `connect_facility`."* The agent relays that; the user runs it in their **own terminal** (not the `!` seam — no TTY, and it would capture the secret), then the server finds the master and multiplexes. **The password + Duo happen entirely in the user's own terminal.**
  - **ControlPath alignment** is the one integration detail: hpc-bridge must look for the master where the user opened it — either hpc-bridge adopts the user's `~/.ssh/config` `ControlPath`, or the instruction names hpc-bridge's `%C` path under `control_dir`.
- **Key + Duo push (no password) — optional agent-relayed push.** A `needs_2fa` phase carrying the *non-secret* challenge; the agent relays *"approve the Duo push"*; the response is the option `1`, injected into a **parked** (non-`BatchMode`, PTY) handshake. Enhancement only — pre-open already covers this case.
- **ControlMaster is the substrate** (shipped): whatever authenticates, it opens the master once and every later `BatchMode` call multiplexes. The interactive step is once per session.

## Testing — the gap globus1 can close

> [!note] Aurora is now a live MFA facility (partial gap-closure)
> ALCF [[Aurora (PBS + bastion) bring-up|Aurora]] — a bastion two-hop wanting **two MFA passcodes** — exercised the **pre-open ControlMaster** path *end-to-end live* (2026-07): the user opens the master once (`ssh -fN aurora`, both passcodes in their own terminal), the server's `BatchMode` calls multiplex over it, and the secret never touches the agent. It's **token-based, not Duo push**, so the push-relay tier is still unexercised, and its compute block is allocation-gated — but the *primary secure path* (pre-open) is proven on a real MFA facility, not just globus1's key-only red cell.

The harness itself still has no *automated* MFA facility (globus1 is key-only). Two ways to close that:
- **Enable Duo/PAM on globus1** (the maintainer admins it via Ansible) → the dedicated auth-gap test facility, on the one cluster we fully control. Strongest, and it makes the relay path CI-testable.
- **Mock keyboard-interactive** for unit tests: a fake sshd / PTY emitting a Duo challenge → test the challenge parse + push-relay + parked-handshake logic with no real facility.
- **Midway** is a real password+Duo facility for a manual spot-check of the pre-open path.

## Deferred (note, don't build first)
`SSH_ASKPASS` / server-held-password path (opt-in automation; still bypasses the agent) — pre-open is the default. Password-manager / credential-request-tool integration (when the client exposes one). Auto-push Duo config detection. Full non-`BatchMode` PTY handshake driver (only needed for agent-relayed push, tier 2).

## Vault updates (when built)
[[Two-channel architecture]] (reframe the MFA-once story to include the relay + password side-channel), [[facility-remote]] (the interactive master-open path / non-`BatchMode` branch), [[Configuration]] (any new env, the pre-open ControlPath convention), [[Home]] (link).

## See also
[[Two-channel architecture]] · [[Standing up the endpoint]] · [[facility-remote]] · [[Endpoint reuse and MEP integration]]

## Update 2026-09-04 — one-time codes in-session (`complete_preauth`)

The external-terminal handoff stays for passwords. For a facility whose remaining prompt is a **one-time code**
(the denial lists `keyboard-interactive` and a key is installed — SDSC Expanse, TACC's token app, Duo passcodes),
the agent may now ask the user for the CURRENT code and call `complete_preauth(code)`: `preauth.py` opens the
ControlMaster with OpenSSH's `SSH_ASKPASS` hook (`SSH_ASKPASS_REQUIRE=force`), a helper that answers a code-shaped
prompt from a 0600 file and REFUSES any prompt mentioning a password (→ `needs_terminal` with the manual command).
Rationale: the same trade as the Globus paste code — single-use, expires in seconds, a transcript that holds it
leaks nothing reusable. Guards: code shape `^[A-Za-z0-9]{4,16}$` (rejects obvious non-codes before any ssh — a short password still matches, so the load-bearing guard is the helper's refusal of password prompts, not the regex);
the code is never in argv, logs or notices; the temp dir is removed after the attempt. `connect_facility`'s
`needs_preauth` result carries `preauth_code_ok` so the agent knows which case it is; `AppCtx.pending_preauth`
remembers the target. Exercised on Expanse: the probe's denial `(gssapi-with-mic,keyboard-interactive,hostbased)`.
Follow-on: Duo *push* (no code; wait for approval) can ride the same askpass hook by answering the menu prompt.

**Ordering (2026-09-04, later).** Only a *bootstrap* needs the SSH connection: reuse asks the Globus web service
whether our endpoint is online and the login shape runs over AMQP. The first cut asked for the code before that
check, so a session against a running endpoint requested (and then never used) a code. `connect_facility` now runs
the web-only reuse check first (`_online_endpoint`) and hands off only when nothing is online; on reuse it seeds
`app.state` so `_provision` skips the bootstrap. The one later op that must SSH is **teardown** (`gce stop` +
delete on the login node): `_teardown_preauth_gate` raises the same handoff — after the block release, which is
AMQP, so spend halts before the user is asked — instead of letting `stop`/`delete` fail their BatchMode logins and
reporting "DELETE FAILED" about a manager that is in fact still running. `AppCtx.preauth_resume` names the call to
repeat, so `complete_preauth` says "call teardown_endpoint() again" when teardown asked. Facilities carry their
`auth_method` (`SlurmFacility.auth_method`, from the catalog entry) so the gate needs no probe. Login-node pinning
is unaffected: it exists for teardown, and costs nothing on reuse now that reuse asks for no code.
