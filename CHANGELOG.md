# Changelog

All notable changes to hpc-bridge. The plugin version lives in `.claude-plugin/plugin.json` (Claude Code updates an
installed plugin only when that version changes); git tags mark releases.

## 0.1.17 — 2026-09-09 — the account floor: an account-required facility starts nothing without an account

### Fixed
- **`account_required` is now enforced at the billed start.** A catalog entry that requires an allocation account
  (NCSA Delta, Anvil, the fake MEP profile) gets **no block** from `ensure_endpoint_up(confirm_spend=True)` — or from
  the implicit provision inside `run_shell(shape="compute")` — until an account is set: the call returns the new
  status/phase **`needs_account`**, spend stays unconfirmed, and the notice says what an account is (a project /
  allocation id, never a login name) and where it comes from. Before, the flag was stored on the facility and never
  read: live on Delta (2026-09-09) the agent confirmed spend after the user offered their *login name*, the MEP
  submitted a GPU block Slurm could only reject, and — a MEP having no login shape to run the rejection probe — the
  plugin reported "allocating nodes…" for five minutes until the user cancelled. Graders treat `needs_account` as
  "nothing started", like `needs_confirmation`.
- **A scheduler-rejected submission is now a terminal `down`, not "allocating nodes…".** When the block's submit
  is refused (parsl's "could not read job ID from submit command" / "failed to start block", or the scheduler's own
  "invalid account/qos/partition"), `ensure_endpoint_up` returns `status="down"` with a **REJECTED** notice naming
  the partition, the account and the one-line cause, and the implicit provision inside `run_shell` fails the same
  way — instead of reporting "allocating nodes…" with the cause buried in a suffix (live on the fake MEP,
  2026-09-09: five polls before the agent read it). Change the account/partition and confirm again; never retry
  unchanged. **Best-effort on a MEP:** this fires only when the executor's failure reaches a canary — it did on one
  of two live runs; on the other nothing reached the client and only the 300-second "a rejection is invisible from
  here" hint spoke (the agent still recovered from it). The remaining blind spot is `Planned/MEP block rejection
  visibility.md`.
- **Wording: "no login node" → "no login shape through this channel".** The compute-only notices and the catalog
  access note said the facility had no login node; the facility's login nodes exist — a multi-user endpoint just
  does not expose them — so allocation names and balances come from the facility's own tools or the user's own SSH
  session. The skill adds: a login name is not an account; ask for the project/allocation id instead of confirming.

## 0.1.16 — 2026-09-05 — ship SKILL.md in the wheel so an installed server serves the guidance

### Fixed
- **`SKILL.md` is now bundled into the built package** (`hpc_bridge/_guidance/SKILL.md`, via a hatchling
  force-include), so an installed server — `uvx hpc-bridge` / `uvx --from git+…` on non-Claude-Code hosts — can serve
  the `hpcbridge://guidance/operations` resource. Before, the wheel packaged only `src/hpc_bridge`, so the guidance
  file (a sibling of `src/`) was absent from an install and the resource fell back to "unavailable". The resolver now
  finds SKILL.md whether the server is installed (the bundled copy) or run from the source tree. Claude Code's
  marketplace install and skill are unaffected. Verified by building the wheel and resolving the resource from an
  isolated install with no repo present.

## 0.1.15 — 2026-09-05 — operational guidance travels over MCP, for hosts beyond Claude Code

### Added
- **The driving-hpc guidance is now available over MCP itself**, so hosts without Claude Code's skill system (e.g.
  NousResearch hermes-agent) can get it. The server exposes an `@mcp.resource` `hpcbridge://guidance/operations` that
  serves `SKILL.md` **verbatim** (one source, zero drift, fetched only on demand), and a small always-on **pointer**
  in the MCP `instructions` field telling the model to read that resource before consequential actions — the lazy,
  Claude-Code-like path (a host loads the full text only when relevant), verified live on hermes-on-ALCF and Claude
  Code. `SKILL.md` is unchanged and remains Claude Code's channel.

### Changed
- **Claude Code opts out of the pointer** via `HPC_BRIDGE_OMIT_INSTRUCTIONS=1` in `.mcp.json` — it already loads the
  skill, so it gets no `instructions` pointer and does not re-read the resource: no behaviour change, no extra tokens.
  Any host that already delivers the guidance another way can set the same flag; every other host gets the pointer.

## 0.1.14 — 2026-09-05 — stopping during provisioning waits for the pilot instead of falsely reporting `down`

### Changed
- **`stop_endpoint` during provisioning polls for the block's pilot to land and cancels it, rather than reporting
  `down` on a `scancel` that found nothing.** The stop-during-provisioning race: a user who approves a spend then
  revokes it while the block is still coming up (the `spend_revoked` flip-flopper) triggered a `scancel` before
  parsl's `sbatch` had reached the scheduler, so the one-shot release found no job and the tool said `down` — then the
  pilot appeared and burned until idle-release. Now, when the compute shape was requested (`spend_confirmed`) but never
  confirmed running, the release keeps polling (`HPC_BRIDGE_PROVISIONING_RELEASE_ATTEMPTS`, default 6 × the release
  backoff ≈ a 30 s window) until it actually cancels the pilot — reporting `down` with the job it cancelled. Only if no
  pilot appears within that window does it return the honest `draining` (call `stop_endpoint` again; idle-release,
  min_blocks=0, is the backstop). A block confirmed running whose `scancel` finds it already finished is still `down`.

## 0.1.13 — 2026-09-06 — a Slurm pilot that died, or will never start, says so

### Changed
- **On Slurm the pilot probe reads finished pilots from accounting and the PENDING reason from the queue.** The
  Slurm twin of the PBS fix in 0.1.9: `squeue` shows live jobs only, so a pilot whose worker died was invisible and the
  tool said the submission was "likely REJECTED"; now `sacct` (matched by the submit line's endpoint marker) reports
  "pilot N already FINISHED (exit status 42) … read its stdout/stderr in submit_scripts". A cancelled pilot (our own
  release or a re-bind), a walltime end, or a clean exit is not a diagnosis and is ignored. A PENDING pilot whose
  reason means it will never start as submitted — `PartitionTimeLimit`, `AssocMaxJobsLimit`, `JobHeldUser`, an invalid
  account or QOS — is reported like a PBS hold, with the reason and what to fix; an ordinary wait shows its reason too.

## 0.1.12 — 2026-09-06 — the login-node pin survives an internal hostname

### Changed
- **The reconnect pin is the address the bootstrap actually reached when the node's own name is not routable.**
  Many login pools answer to public round-robin names but report private ones from `hostname -f` (Midway's
  `*.rcc.local`, Aurora's `*.hostmgmt.cm.*`, a site's internal domain). The pin used suffix heuristics only, so a
  plain-looking internal name was pinned and later SSH — teardown above all — failed with "Could not resolve
  hostname"; falling back to the round-robin alias could reach the other node and orphan the manager. Now a name
  must also resolve from the client; when it does not, the pin is the server address of the very connection that
  started the manager (`$SSH_CONNECTION`), key-checked against the alias the user trusted as before. Found on the
  fake cluster's internal-hostnames profile.

## 0.1.11 — 2026-09-06 — a held PBS pilot explains itself

### Changed
- **On PBS, a HELD pilot's notice carries the scheduler's own comment.** A site hook that holds jobs missing a
  required directive (ALCF Polaris: `-l filesystems=home:eagle`) writes why into the job's comment; the pilot probe
  now reads it, so the tool says "pilot N is HELD … The scheduler's comment: 'HELD by the site: every job must request
  -l filesystems=…'" and points at `scheduler_options`, instead of a generic "bad scheduler directive" hint. Found on
  the fake cluster's Polaris-style profile — the first place the held-pilot path ran.

## 0.1.10 — 2026-09-06 — discovery knows the site's module system

### Added
- **Module-aware discovery.** The login-node probe now records the module system (Lmod/Tmod init script and
  `module -t avail`). When neither `globus-compute-endpoint` nor `uv` is on the default PATH — the normal state of a
  site where everything comes through `module load` — discovery proposes the site-supported route instead of
  curl-installing uv: `module load <uv module>` and the usual uv venv, or `module load <python module>` matching this
  client's Python plus a venv and a pip install. The proposed `env_setup` re-initialises the `module` command itself
  first, because a compute node's batch script is not a login shell and `worker_init` replays `env_setup` exactly
  there. A Python module of the wrong minor is named in the note and the uv bootstrap stays the fallback.

## 0.1.9 — 2026-09-06 — PBS without an account; a pilot that died is not "never submitted"

Both found on the new fake OpenPBS cluster, the first place the PBS path ran end to end (every PBS facility we had
met before required an account).

### Fixed
- **A PBS facility with no account no longer breaks the block submit.** The submit template emitted `account: ""`,
  and Parsl's PBS provider adds `-A <account>` for any non-None value — so `qsub` took the script path as the account
  name and read an empty script from stdin: a job named STDIN that exited 127, no worker, and a tool that then said
  the submission was "likely REJECTED". The account line is now emitted only when there is one.
- **On PBS the pilot probe sees finished jobs (`qstat -x`) and names a pilot that ran and died** — "pilot N already
  FINISHED: the block started and its worker exited … read that job's stdout/stderr in submit_scripts" — instead of
  the misleading "NO pilot job … likely REJECTED". A live pilot beside an old finished one still reports the live
  state. (Slurm's probe has the same blind spot; `sacct` would close it — follow-up.)

## 0.1.8 — 2026-09-05 — a local catalog for development and testing

### Added
- **`HPC_BRIDGE_CATALOG_FILE`: a local catalog that stands in for the registry.** A seed-format YAML file (or a
  directory of them) becomes *the* catalog for that process — the public registry is not consulted. This is a
  development and test seam, not a user setting: the agentic harness' fake cluster runs facility multi-user endpoints
  whose UUIDs are minted per cluster and can never be registry entries, and a curator can rehearse a seed entry before
  ingesting it. Unset (the default) nothing changes.

## 0.1.7 — 2026-09-05 — stopping under a running task

### Fixed
- **`stop_endpoint` refuses while a task is still running on the compute block.** It used to release the block,
  report `down`, and the endpoint then relaunched a fresh block for the orphaned task, so spend continued after a
  false "released" and the task's result was lost. Found by the new chaos scenario on the local fake cluster. The stop
  now answers `up`, names the task, and offers the two honest ways out: poll it to completion, then stop; or tear down.
  This matches what the facility-endpoint stop already did.

## 0.1.6 — 2026-09-05 — teardown tells the truth; licensed

### Fixed
- **Teardown reports only what it measured.** "Stopped", "deleted", "token copy removed" and "connection closed"
  each come from the remote command's own result. If SSH itself fails (a one-time-code facility whose shared
  connection expired, an unreachable host), nothing further is attempted, the tool answers `up` with
  "TEARDOWN FAILED", the endpoint stays bound so a retry can finish, and the record that lets a later teardown
  remove the token copy is kept. Previously every one of those words was assumed, the record was deleted, and
  the endpoint was reported down while its manager still ran.
- **The teardown code gate works for bring-your-own facilities.** It no longer depends on the registry's
  one-time-code flag: with no shared connection open it probes the login node once; a key that works proceeds,
  a denial offering a second factor asks for the code, anything else is reported as a failed teardown.
- The unit tier is hermetic again (#93): thirteen tests had been reaching the live registry.

### Changed
- **Licensed under Apache-2.0** (LICENSE and `pyproject.toml`; the plugin manifests have no license field). The
  first public release had no license.
- Docs that still said the agent never handles a one-time code (login, troubleshooting, bring-your-own) now
  describe the 0.1.2 behaviour; the Expanse registry description too. The README no longer names a tag that was
  never cut. The skill's balance re-read no longer points at a tool on a facility without a login shape.

## 0.1.5 — 2026-09-04 — registry: Anvil is its facility endpoint; Globus Labs is `globus-labs`

### Changed
- **Anvil is reached through its facility-run multi-user endpoint only.** The SSH-bootstrap entry left the
  registry (the registry encourages facility endpoints over SSH); plain `anvil` is now the endpoint entry and
  the `anvil-mep` id is retired. Anvil over SSH still works as a bring-your-own cluster.
- **The Globus Labs cluster is `globus-labs`** in the registry, not `globus1` (that is the machine's hostname).
  Say "connect me to globus-labs"; the old id no longer resolves.
- The install guide's SSH example is Expanse; the catalog tool can delete index subjects (`--delete-subject`).

## 0.1.4 — 2026-09-04 — teardown no longer holds the tool call

### Fixed
- **Teardown runs its login-node work in the background of the server.** Stopping and deleting the endpoint
  took about three minutes on Expanse's filesystem, longer than a client's tool window; the call relied on the
  client rescuing it. One call now waits up to a minute and otherwise answers `tearing_down`; calling again
  confirms `down`. A client that times out can no longer interrupt the work half-way.
- **Teardown's report says the shared SSH connection was closed.** The agent had told a user it was still open.

## 0.1.3 — 2026-09-04 — no code needed to reconnect

### Fixed
- **Reconnecting to a running endpoint on a one-time-code facility asked for a code it never used.** Reuse needs
  no SSH (the web service says the endpoint is online; the login shape runs over the network), but the code was
  requested before that check. It is now requested only when a bootstrap actually needs the connection, so a
  session against a running endpoint asks for nothing and a cold session asks once.
- **Teardown on a one-time-code facility** now asks for the code before touching SSH, after the compute block has
  been released, instead of failing its login-node commands and reporting a failed delete about an endpoint that
  was still running. `complete_preauth` then names the call to repeat.

## 0.1.2 — 2026-09-04 — one-time codes in the chat; three more facilities (`v0.1.2-beta.1`)

The second beta. It exists so installed plugins pick up this week's fixes (Claude Code refreshes a plugin only when the
manifest version moves) and to add the facilities validated in the ACCESS campaign.

### Added
- **One-time codes in the conversation.** A facility that requires a TOTP or Duo passcode at SSH login (SDSC Expanse)
  no longer needs your own terminal: the agent asks for the current code, `complete_preauth(code)` opens the shared
  SSH connection with it, and the bootstrap rides that connection. The code is single-use and never stored. A
  **password** is still never handled: the opener refuses a password prompt (and an unknown-host-key prompt) and
  hands you the `ssh` line for your own terminal instead. (#86, #87, #88)
- **Facility multi-user endpoints read their own contract.** At attach, the endpoint's template metadata is fetched
  and the configuration is checked against its schema; keys the facility rejects are dropped before dispatch, so a
  strict schema (Anvil) gets the account and nothing it refuses. Worker environments can pin the endpoint version
  the facility's manager runs, or the client's. (#83, #84)
- **Registry entries:** **NCSA Delta** (multi-user endpoint), **Purdue Anvil's multi-user endpoint** alongside its SSH
  entry, and **SDSC Expanse** (SSH with a one-time code). Each validated live before ingest. (#83, #84, #85)
- Bring-your-own on a bare login node (no `uv`, no usable Python): the bootstrap installs `uv` and builds the
  endpoint's environment itself, with a longer budget and a plain explanation when it runs out. (#84)

### Fixed
- A curated one-time-code facility came back as "NO SSH ACCESS" on the registry path, and the agent went editing
  `~/.ssh/config`. A key that is accepted but still awaits a second factor is now recognised as the pre-auth handoff,
  the handoff is raised before any failing attempt, and the skill forbids touching the user's SSH configuration. (#87)
- The second code for a pinned login node hung until timeout when that node's key was not in `known_hosts`: the
  code-opener now verifies the pinned node against the alias you trusted, and the notice explains why a pinned
  node needs its own connection. (#88)
- Teardown waits for the endpoint to stop before deleting, and a token store seeded by an aborted bootstrap is
  remembered so teardown still removes it. (#84)

### Verified
- Live on **NCSA Delta** (multi-user endpoint, `hostname` on a compute node), **Purdue Anvil** (multi-user endpoint,
  account applied on the strict schema) and **SDSC Expanse** (SSH bootstrap with one code, login shape on `login02`).
- A fresh-user session against Expanse from a clean plugin install: one code requested with the reason, the
  connection opened in three seconds, the endpoint reattached with no further prompt.

### Known limitations
- Duo *push* (approve on your phone, no code) is not yet driven from the chat; use the `ssh` line in your own
  terminal for those facilities.
- A pinned login node on a one-time-code facility costs one code per session even when the endpoint is already
  running; whether to pin at all on such facilities is an open design question.
- ACES and Stampede3 are not yet validated (accounts pending); ALCF and NeSI multi-user endpoints are not catalogued.
- Windows is untested; the SSH path relies on OpenSSH connection multiplexing.
- The registry index is a trial index owned by one maintainer until the org transfer.

## 0.1.1 — 2026-09-04 — first public beta (`v0.1.1-beta.1`)

hpc-bridge lets Claude Code work on a supercomputer for you: it finds the facility, logs you in to Globus once, starts
a one-node scheduler job (asking before it spends anything), runs your commands on that node, and releases it. This is
the first release offered to test users. Expect rough edges, and please report them at
https://github.com/ryanchard/hpc-bridge/issues. (0.1.0 was the pre-release the marketplace served during development;
the version moves so existing installs update.)

### What's in it
- **Install from Claude Code** — the repository is its own plugin marketplace: `/plugin marketplace add
  ryanchard/hpc-bridge`, `/plugin install hpc-bridge@hpc-bridge`. Eleven tools, a skill the agent follows on its own,
  and the `/hpc-bridge:hpc-connect` command. The server is launched through a small script that finds `uv` even
  from the desktop app or an IDE, whose PATH lacks the usual install locations. (#52, #74)
- **Public facility registry** — "What HPC facilities can I use?" answers from a curated Globus Search index, read
  anonymously, and each entry says how you get in. The registry wins over a local cache for any catalogued id. (#49)
- **In-terminal Globus login** — the first connect opens your browser, you approve once, the agent carries on in the
  same call and names the identity that landed; a paste fallback when no browser can reach the loopback. (#48, #75)
- **Two ways in** — an SSH-once bootstrap that installs a personal Globus Compute endpoint in your home directory on
  the login node and reconnects with no SSH afterwards; or a facility-run multi-user endpoint attached with zero SSH,
  the identity mapping done by the facility. (#41, #20)
- **Bring your own cluster** — an un-catalogued login host is probed, a configuration proposed and confirmed with you
  in this conversation, and remembered only once it has proven to work. (#50, #57, #78)
- **Cost control** — no billed block starts without your confirmation; idle blocks self-release; stopping confirms the
  release or says honestly that the block is draining. (#24, #44)
- **Honest first-contact failures** — "NO ACCOUNT at this facility" (naming the identity), "NO SSH ACCESS to <host>",
  "CANNOT REACH", "UNKNOWN HOST KEY": each terminal, explained once, with the remedy. (#49, #50, #75)
- **Long-running work** — a command that outlives the sync wait becomes a task you can poll; a task on a dead
  endpoint is reported as orphaned, never "running forever". (#44)
- **A user guide** — install, quickstart, facilities, the Globus login, costs and stopping, troubleshooting — written
  for someone who has never seen the project, and checked by an agent playing exactly that person. (#52, #73, #74)

### Security (review of 2026-09-04, record in the vault)
- **hpc-bridge trusts exactly the hosts your own `ssh` trusts.** No host-key auto-accept; an unknown or changed key is
  refused as UNKNOWN HOST KEY, and the remedy is one `ssh` from your own terminal. A pinned login node is verified
  against the alias you trusted. The host string is validated before it can reach ssh's argument list. (#75)
- **The first SSH contact is in the transcript:** the connect result names the login `user@host` and the shell line it
  will run there, before anything runs. Confirmation must come from the user in the current conversation; a
  remembered or cached confirmation is not consent, and the agent is told not to re-probe over its own ssh. (#75, #78)
- **Tokens:** a trimmed Globus token store is placed on an SSH facility's login node so the endpoint can register; the
  docs now say so, and teardown removes it — only if hpc-bridge put it there. (#75, #76, #79)
- **Registry and cache:** the offline cache is served only when the registry is unreachable and forgotten on a clean
  miss; a cached record whose host doesn't match is ignored; entry ids are allowlisted. The registry is trusted code;
  the user docs say so. Its index moves to a production, group-owned index with the org transfer. (#75)
- **Supply chain:** the server runs against its lockfile (`uv run --locked`); the endpoint installed on the login node
  is pinned to the SDK's version. (#75)

### Fixed
- The first connect after registering an endpoint no longer fails with "could not find endpoint … in list output":
  the CLI's table wrapped long names at 80 columns without a TTY. Fixed at the source, with a bounded wait for the
  UUID and adoption of an already-running manager. (#72, closes #39)
- Teardown really deletes the endpoint (directory, registration, worker directories) and reports what it did; it
  used to stop only, while claiming "deleted". (#79)
- A fresh bring-up is cached as proven and is no longer reported as "reused the already-online endpoint". (#78)
- Twenty-nine bugs from two adversarial review rounds, each pinned by a regression test. (#54, #67)

### Verified
- Live on **Purdue Anvil** and **Midway** (Slurm), **ALCF Polaris** (PBS) and Globus Labs' cluster.
- Agentic regression tier across Opus 5, Sonnet 5 and Haiku 4.5: cheap tier 29/30, block tier 14/14, plus the two
  host-key cells (`byo_teardown_clean`, `unknown_host_key`) that replace the manual fresh-user walk. (#51, #69–#71,
  #80, #81)
- 469 unit tests, mypy and ruff clean, all enforced in CI.

### Known limitations
- Facilities not in the registry need the bring-your-own path; ALCF, NCSA Delta and NeSI multi-user endpoints are
  not catalogued yet.
- Windows is untested; the SSH path relies on OpenSSH connection multiplexing.
- Interactive-login (password / Duo) facilities: the agent hands you an `ssh` command for your own terminal and never
  handles the secret.
- The agent runs as you on the login node; treat what it runs there as you would your own shell.
- The registry index is a trial index owned by one maintainer until the org transfer.
