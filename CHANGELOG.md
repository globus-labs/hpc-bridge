# Changelog

All notable changes to hpc-bridge. The plugin version lives in `.claude-plugin/plugin.json` (Claude Code updates an
installed plugin only when that version changes); git tags mark releases.

## Unreleased

### Added
- **The spend question names the block.** The spend question never said which project a block would charge, and named
  the queue only sometimes. Now every spend answer, from `ensure_endpoint_up` and from `run_shell`, carries the block's
  terms in a `block` field and at the head of its notice: the queue and the account, each with where it came from (the
  registry's default, an `HPC_BRIDGE_PARTITION` / `HPC_BRIDGE_ACCOUNT` preset, the details of a cluster you brought,
  the login-node probe's pick, this call, an earlier call), or that no account is needed or none is set; the nodes, the
  walltime, and the most node-hours the block can bill. On a facility's own endpoint it adds that every command bills
  and the block stays warm between commands until the facility's idle window ends it. The same words in both tools.
  The call that confirms spend carries the same `block` in its answer, once, so a start made in one call still shows
  what it started. The terms are only what the facility receives: where a facility's endpoint has no setting for the
  partition or the node count (its published template drops them), they read as the facility's own default, and a
  partition or account it would drop is refused with a notice instead of being set — the answer then reports the
  block as it stands (up and billing, or the spend question on its current terms), never a `down`, and says to send
  the rest of the call again without it — and the connect neither asks about nor offers it. A facility named by `HPC_BRIDGE_MACHINE` reads its template before that check, so
  its first call refuses too. On a PBS facility every one of these texts says "queue".
- **Each registry facility says where to find your account.** A registry entry can now carry an `account_hint`, and
  the spend question and `connect_facility` pass it on, as the registry's own words: Polaris, your ALCF project name
  as the ALCF portal lists it (e.g. `Diaspora`); Anvil, your ACCESS project id in lower case (e.g. `cis250223`);
  Expanse, `expanse-client user -r expanse`, which the agent can run itself on the free login node; Delta, `accounts`
  in your own SSH session on a Delta login node. `globus-labs` needs no account. A plugin before this one ignores the
  field. A hint is one line of plain text: control, format and bidirectional characters are dropped on read, double
  quotes and backticks become single quotes and angle brackets go, a malformed hint is dropped (never the facility),
  and the curator's ingest refuses all of these.
- **`connect_facility` states the defaults before any spend.** Its answer gives the terms the first block would start
  on: the default queue and where it comes from, whether an account is needed (and where to find it), the nodes and
  the walltime. Nothing is started or billed by it. What fitting the configuration to a facility endpoint's template
  drops is said there too, before any spend.

### Changed
- **The agent asks you once, only for what you haven't said, and your yes is the confirmation.** `connect_facility`
  no longer tells the agent to confirm spend unasked: it says to ask you, in one question, for the account and whether
  the queue is fine, unless you already said, and to send your answer and your yes in one `ensure_endpoint_up` call.
  A preset (`HPC_BRIDGE_ACCOUNT`, `HPC_BRIDGE_PARTITION`) counts as said: with both, the agent states the terms in one
  line and starts. Only a preset removes the question: where there is nothing to choose (the facility decides its
  queue and needs no account), the agent still asks a plain yes or no on the terms. Where an account is needed and none is set, the spend question asks for it in the same breath. The
  skill says the same, with example questions; an account the agent remembers from an earlier session is only offered
  as the suggestion, never used without asking; after a block is gone it asks a plain yes or no, naming the terms
  again. The skill and the tool descriptions no longer say that `connect_facility` lists your allocations: no registry
  facility has a listing hpc-bridge can read.
- **A cluster you bring may not set the account or the queue in `scheduler_options`.** `#SBATCH -A/--account`,
  `-p/--partition` (by any prefix sbatch accepts, such as `--part=`, or in a bundle such as `-vA`) and `#PBS -A`/`-q`
  there would charge an account or pick a queue the spend question never names; the connect now refuses them and says
  to give the partition in the details and the account to `ensure_endpoint_up`. A configuration cached from an earlier
  session that carries one is refused with that reason. A registry entry may not set them either, in
  `defaults.extra` or by a directive in its `scheduler_options`: dropped on read, refused at ingest.

### Fixed
- **A Globus login that no longer works is named, not mistaken for a config fault.** When Globus refuses to renew
  the stored login (it was revoked, or it expired), every dispatch fails. Seen live (2026-10-14), each one said
  "allocating nodes… — last dispatch failed: AuthAPIError … Not a queue wait: fix the config/partition and retry";
  `authenticate` then answered `logged_in`, because the stored login still held every scope; and the connect said only
  "facility template not readable (AuthAPIError)". Now:
  - `ensure_endpoint_up` and `run_shell` say the stored Globus login no longer works (expired or revoked), that
    nothing is wrong with the facility, its config or the partition, and that the user must log in again with
    `authenticate(force=True)`. `run_shell` answers `failed`, not `cold_start`; `ensure_endpoint_up` stays
    `provisioning`, because a block started earlier may still be running;
  - `authenticate` checks that the stored login still works before it answers `logged_in`: it renews any token past
    its expiry, as a dispatch would, and never opens a browser or asks for input to do it. When Globus refuses, it
    answers `needs_login` with the reason and starts a new login, as for a first one. When the check cannot reach
    Globus, it still answers `logged_in` and says the login could not be checked;
  - the connect to a facility-run endpoint names the failed login instead of only "template not readable";
  - a command sent to a block still taken for warm, whose dispatch fails on the login (`Dispatch error:
    AuthAPIError`), says so too;
  - `connect_facility` checks the same way before anything else, and answers `needs_login` with the reason, as for a
    missing login. Before, it went on: on an SSH facility the bootstrap would have copied the dead login to the
    login node. When the check cannot reach Globus, the connect carries on as before. A login that completes
    during the connect now names the identity that logged in, not the previous one, and work sent afterwards uses
    the new login;
  - starting an endpoint over SSH without a connect (a server started with `HPC_BRIDGE_MACHINE`, or a command
    after a teardown) checks the login first, renewing it even when it has not expired yet, so a login revoked
    minutes ago is caught too. On a dead login nothing is started, no SSH is made and nothing is copied to the
    login node; the answer says so;
  - the check gives up after about 30 seconds when Globus cannot be reached (one retry, 15-second timeout),
    instead of up to six minutes;
  - the remaining bare "hpc-bridge error: AuthAPIError …" answers (a failed endpoint start in `ensure_endpoint_up`,
    a failed attach to a facility-run endpoint, a tool error) say the login no longer works and point at
    `authenticate(force=True)`.
- **The plugin starts on Windows on Arm.** One of the server's dependencies, `cryptography`, has published no
  Windows-on-Arm build since 46.0.3. On an Arm Python, the first start tried to build it from source, failed for
  want of OpenSSL, and Claude Code showed only `Failed to reconnect to plugin:hpc-bridge … CONNECTION_CLOSED`.
  Now the plugin's launcher asks `uv` for an x64 Python there, which Windows runs under emulation, unless
  `UV_PYTHON` is already set. A failed earlier start's Arm environment is replaced. On another MCP host, pass
  `--python cpython-3.13-windows-x86_64-none` to `uvx` (see Other hosts). The install page now says what is
  checked on Windows, and Troubleshooting names the `CONNECTION_CLOSED` message.
- **A command never runs on a block the user did not confirm.** This fixes 0.1.24's known issue. hpc-bridge checks
  the block before a command only when its last answer is 45 s old, and every completed command counts as an answer.
  So commands sent less than 45 s apart could run on a replacement block without a question for as long as they kept
  coming. Seen on the fake cluster (2026-10-13): job 112 was cancelled and the next command, `sleep 75`, ran on job 113
  unasked; a probe sent 20 commands 40 s apart over 13 minutes to a replaced block, and no check ran. Now each command
  on a confirmed block carries that block's scheduler job, and the compute node refuses it on any other job before it
  touches anything:
  - the tool answers `needs_confirmation` and says the command did NOT run, which job was replaced by which, and that
    the new block is already running and billing. Nothing runs on the new block until the user confirms;
  - the session's working directory and environment are left exactly as they were, so the same session continues on
    the new block after the user confirms;
  - the old block is billed as for a replacement a check finds, and the new one from the user's confirmation;
  - it works the same when the answer comes back later through `poll_task`, also for a call that was cancelled;
  - `reset_session` is refused the same way;
  - it applies to a billed block on Slurm or PBS, through SSH or a facility's endpoint, when the endpoint runs one
    block. The free login shape is not affected;
  - a refusal is recognised whatever else the compute node writes around it: a warning from its shell first (a locale
    it lacks, say), or after it the warning a facility's endpoint without task sandboxing adds, a `set -x` trace or an
    exit trap. A command's own output cannot pass for one: each command's refusal carries a code only that command
    was sent with.
- **`run_shell`'s spend question reports the session spend.** When `run_shell` or `reset_session` answers
  `needs_confirmation` or `needs_account`, it now reports the session spend, as `ensure_endpoint_up` already did, so a
  block that was just found gone shows in it at once.
- **A stop on a facility endpoint names the block it cannot cancel (#178).** A facility multi-user endpoint gives
  hpc-bridge no cancel channel, so a stop there issues no cancel, and the block bills on until the facility's idle
  window or its walltime. The stop used to say only `draining` and "the user can scancel each block", without naming
  one. Live on the lab cluster (2026-10-12), the agent told the user spend would stop in about ten minutes, and Slurm
  job 1226 was still running when the user found it by hand. Now:
  - the notice leads with "NO CANCEL WAS ISSUED", then names the block's scheduler job (`block_job_id`), the latest
    it can bill until as a clock time (its idle window after the last task, capped at its walltime; its walltime when
    the idle window is unknown or a task of ours may still be on it; possibly sooner, not verified), and the command
    that ends it now (`scancel <id>`, or `qdel <id>` on PBS) and checks on it;
  - the job comes from the check that confirmed the block, else from a check that found a replacement, else from a
    check that answered after its wait, else from the block the idle presumption took for gone. With none, the notice
    says so, and says plainly when a check of ours is still queued (the facility will start or keep a block for it),
    with `squeue -u` for the account the facility maps your identity to: the check now reports that account (on
    `globus-labs`, `glabs-gc`, not your login name, and possibly shared, so the notice says to check the start time
    and partition of a `parsl.Gl…` job before cancelling it);
  - a second stop, a stop or a second teardown after a detach, a stop after a reconnect and the reconnect itself
    still name it, and so does a block left behind by a partition or account switch, until it can no longer be
    running (its walltime); a check still queued is kept, dated, one per partition and account, since the queue
    wait has no bound — and a block it starts is not in the session spend, which the notice says;
  - `spend_stopped` (new) is true only when an SSH stop's cancel was confirmed (`down`), never on a facility endpoint;
    `block_state` reports the block's presumed state, not "cold" while it may still run;
  - the detach (`teardown_endpoint` on a facility endpoint) said `down` while the block could still bill. It keeps
    `down` for our state and now gives the same notice and fields;
  - the session spend counts the block up to that latest time: an upper bound. A block confirmed again after a stop
    counts from its new confirmation, and a later stop adds only the time past what an earlier stop already counted
    for that job, so stop/re-confirm cycles never recount a tail (each can count its own run twice). The stop says the
    spend counts its own block; a block an earlier stop or detach left was counted in the spend reported then (a new
    binding starts at 0);
  - the five-minute still-allocating notice names the mapped account and only a confirmed block of that partition
    and account, with its cancel command only when it is the current block;
  - the guidance says no cancel is issued, to relay the job id and the command, and never to check from here after a
    stop (`run_shell` or a confirmed `ensure_endpoint_up`). A block cancelling itself was considered and rejected:
    its task stays outstanding until the endpoint notices the worker gone, so the endpoint starts a replacement
    block, which then idles out its window.
- **A registry SSH facility that requires an account starts nothing without one.** The account floor (0.1.17) held
  only on a facility's multi-user endpoint. A registry SSH entry marked `account_required` (Expanse) built its
  facility without the flag, so a confirmed start with no account submitted a block the scheduler could only reject,
  or charged it to the user's default account. Now every facility built from a registry entry carries the flag, over
  SSH on Slurm and PBS as on a facility endpoint, and `ensure_endpoint_up(confirm_spend=True)` with no account answers
  `needs_account` and starts nothing. The floor reads the account the block would be charged to: the one this session
  chose, or the facility's own configured default (the startup pin's `HPC_BRIDGE_ACCOUNT`). On an SSH facility its
  notice says where to find an account: `connect_facility`'s allocations, or the facility's allocation tool, which
  runs free on the login shape. A facility you bring yourself is unchanged: nothing curated says it needs one.
- **`HPC_BRIDGE_ACCOUNT` and `HPC_BRIDGE_PARTITION` apply only to the facility `HPC_BRIDGE_MACHINE` names.** They
  reached every facility, one you bring yourself included: a `connect_facility` anywhere charged its blocks to
  `HPC_BRIDGE_ACCOUNT`, and every SSH facility took `HPC_BRIDGE_PARTITION` as its default. A user who once exported an
  account for one machine had every other machine's blocks charged to it unasked; on an account-required facility it
  even answered the account question. Now each applies only to the machine `HPC_BRIDGE_MACHINE` names, by id, subject
  or the alias the startup pin resolved. The startup pin is unchanged and still requires `HPC_BRIDGE_ACCOUNT`; a
  partition you give in `details=` wins over the preset, and a facility endpoint never takes the partition preset. A
  connect where a preset goes unused says so in one clause that names the variable, never its value: "(HPC_BRIDGE_ACCOUNT
  applies only to HPC_BRIDGE_MACHINE=anvil, so it was not used here.)".
- **Changing the partition or account asks for spend again.** The spend confirmation covers one block, and another
  partition or account is another block. Yet `ensure_endpoint_up(partition=…)` after a confirmed block started the new
  block under the old confirmation. Now a real change on a confirmed block answers `needs_confirmation`, and so does a
  `run_shell` after it, unless the changing call passes `confirm_spend=True` itself; then that call's own answer says
  what became of the old block. Nothing cancels it on a switch. On an SSH facility it idle-releases on its own; to end
  it now, call `stop_endpoint` before confirming the new block. On a facility endpoint it is named as a reconnect names
  one (its scheduler job, and `scancel <id>`, or `qdel <id>` on PBS), and a later stop names it in full. Either way the
  session spend counts whatever block of ours the switch knows up (the confirmed one, one a late check answer proved
  up, a replacement a check or a refused command found) to the latest it can bill, an upper bound. A command the
  wrong-block guard refused that nobody has polled yet is taken in before a stop, detach, reconnect or switch lets go
  of the block, so the replacement it found is what gets named and billed, and the call that switches answers with
  that reap. A job met again, after a switch back, a stop or a reconnect, keeps its real age for its walltime (dated by
  its scheduler job, on any number of switches), unless it answers more than 30 s past that walltime (the time a
  scheduler may let a job run on before killing it), counted to when it answered: then it was requeued under its id
  and runs a fresh one. It bills afresh from that confirmation: switching back and forth can count some time twice,
  never less than ran. A switch back to the confirmed terms says so. A task that finished on the old block and is polled later no
  longer marks the new block warm (which skipped its check and left it unguarded). The same partition or account again
  is not a change.
- **A stop keeps the partition and account the session chose.** `stop_endpoint` forgot them, so the next block went
  to the facility's default partition and account without a word, although the tool says the choice persists for the
  session. Now the next block after a stop uses them, after the usual spend question, which names the partition; its
  result carries both. A new `connect_facility` or a teardown forgets them; a new Globus login does not.
- **The facilities page no longer offers a scheduler option no tool takes.** `docs/user/facilities.md` told Polaris
  users to pass `scheduler_options: "#PBS -l filesystems=home:eagle"`, and Anvil users `"#SBATCH --gpus-per-node=1"`
  for a GPU partition. No tool takes `scheduler_options` for a registry facility. The rows now say what each entry
  sends (Polaris: none, so the facility's default `-l filesystems=home`; Anvil: no GPU request) and that a job needing
  more cannot ask for it through that entry yet. The skill says the same, and a held pilot's notice offers
  `connect_facility(details=…)` only for a facility you bring yourself.

### Testing
- **A Windows check in CI** (`.github/workflows/windows.yml`), on every pull request and push to `main`. It checks
  that Claude Code (its latest release) loads the plugin and connects to its server on Windows Server (x64), with
  `uv` on PATH and with `uv` only in `~/.local/bin`, and on Windows 11 on Arm after an earlier failed start. On
  x64 it also checks that the server starts and lists its tools.
- `reap_replaced_block` now waits until the cancelled block has left the queue, then sends `echo JOB=$SLURM_JOB_ID`
  inside the 45 s window. A new critical grader, `no_work_on_replacement`, checks that nothing ran on the new block
  before the user confirmed it, whether the command was refused there or lost with the old block. A report-only row,
  `replacement_found_by`, says which happened, so a sweep shows how often the refusal itself fired. The graders also
  read results that arrive through `poll_task`.
- `revocation_honoured` no longer counts a facility endpoint's detach (`down`) as a release, and
  `stop_is_honest` flags a `down` whose `spend_stopped` is false. New scenario `fake_mep_stop_names_block` (fake
  `mep` profile, free operators): the stop names the job the block printed, leads with "NO CANCEL WAS ISSUED",
  gives `scancel <id>`, the agent relays it and sends nothing that could reach the block after the stop (no
  `run_shell`, `reset_session` or confirmed `ensure_endpoint_up`); a world hook checks the block is still in the
  mapped account's queue at stop time. Graders read `confirm_spend` through one check (`_confirms_spend`), so a
  `"True"` counts like `true`.
- Three scenarios for these fixes, autonomous so the free operators drive them too:
  - `term_switch_reasks` (fake `mep`): after a confirmed `debug` block, the switch to `compute` without a confirm
    answers `needs_confirmation` and names the `debug` block's job and `scancel <id>`. Nothing runs before the
    confirmed switch, and the next block's own job reports `compute`. If the old block idled out before the switch, the
    switch graders say "inconclusive";
  - `preset_account_scoped` (fake `mep`; the server runs with `HPC_BRIDGE_ACCOUNT=hpcb-gpu` and no
    `HPC_BRIDGE_MACHINE`): the connect says the preset was not used, a confirm with no account answers
    `needs_account`, and sacct shows no job of the mapped account submitted with `hpcb-gpu`. An admin step makes
    `hpcb-gpu` a valid account for that user (and fails the cell if it did not take), so a leak would bill rather than
    be refused. An agent that passes the preset itself (a CLI operator can read it) is reported apart;
  - `ssh_account_floor` (fake `site`): a registry SSH entry with `account_required`, written for the cell by
    `agentic/harness/fake_ssh_catalog.py` and served through `HPC_BRIDGE_CATALOG_FILE`. A confirm with no account
    answers `needs_account`, and sacct shows no block submitted. A connect with `details=` is a broken premise.

  Their graders are tested hermetically in `agentic/harness/test_spend_terms_scenarios.py`; it and the existing
  `test_mep_scenarios.py` and `test_site_scenarios.py` are now in the CI list.
- A new grader, `quote_before_spend`: before each billed start of a new block, the agent's words to you name the queue
  and the account that start used (or say no account is charged), read from the start's `block`. A term that changed
  since the last start must be named since then; a start on unchanged terms (after a reap or a stop) needs a spend
  question since then. Its text counts, and so do its questions, their option labels ("debug (Recommended)"), a table
  column headed partition or queue, a preset named as the preset, and an account you typed in answer to a question
  asking for it; a name counts only whole (`debug` is not in `debug-scaling`, `hpcb` not in a host name). A poll that
  confirms the same block again is not a new start. It is reported on every run and gates only where a scenario lists
  it in `EXPECT_OK`; in benchmark mode (`HPCB_BENCHMARK_MODE`) it stays report-only, and it is not graded for an
  operator whose record does not place the agent's texts among its calls. `choice_respected` no longer reads an answer
  that asks a question back as a partition pick.
- Two persona scenarios on the fake `mep` profile, for the Claude operator and, report-only, hermes-acp:
  `terms_prestated` (you name the partition and account in the request: the agent states them back and starts without
  a question, and the start charges exactly those; a question after a refused start is a broken premise, not a
  redundant one) and `terms_wanted`, with a new persona `wants_terms` that answers a question asking which account or
  queue, approves a spend question only when it names both the queue and the account, and otherwise asks "Which queue
  and which project would this charge?" (a question back, not a refusal). Its grader `approved_with_terms` checks
  that the start followed a yes to a question naming both — counting the one question the plugin teaches ("Which
  project should it charge? I'd use `compute` …", answered with the project id), which is now a spend question when it
  carries a cue about the block (a queue, starting, a charge, a node or block, a backticked name); "What account should
  I SSH as?" stays an identity question, so a later answer to one never supersedes a spend decline. The persona takes its own project id given to such a question as its yes. A markdown
  table counts as naming the queue only when it names that one queue (a listing of every partition does not). The
  fake `mep` profile's catalog no longer gives the account away in its description (takes effect after a cluster
  reset).
- **A PBS facility endpoint on the fake cluster, shaped like ALCF Polaris's.** The new `pbs_mep` profile is the
  `polaris` cluster (OpenPBS, a filesystems request required) with a root-run multi-user endpoint on its login node.
  Your Globus identity maps to the local account `hpcbmep`. Its template is modelled on the one Polaris publishes:
  PBS Pro; a queue and an account are required; `#PBS -l filesystems=home` by default; a 240 s idle release; the
  workers run the facility's own endpoint install (here the manager's, mounted read-only on the compute nodes), so the
  catalog entry sends no worker setup, as the real `polaris` entry sends none. Unlike Polaris's, its schema refuses
  unknown keys, so a `partition` the plugin forgot to rename to `queue` fails loudly instead of being ignored. It has
  its own catalog entry, `fake-pbs-mep`, shaped like the registry's `polaris` one (the partition-to-queue rename, an
  account hint, no account named), its own volumes, and its record is deleted with Globus on a wipe. A new scenario,
  `fake_pbs_mep_compute` (autonomous, so the free operators drive it), attaches with no SSH, starts a block charged
  to `hpcb` on the facility's default queue, runs one command and stops. Its graders check what the plugin answered:
  the start names queue `debug` from the registry and the account set by the call; every statement of the terms says
  "queue"; PBS ran the job in `debug` (so the rename reached `qsub -q`); the stop leads with "NO CANCEL WAS ISSUED",
  names the PBS job and gives `qdel <id>`; nothing is sent after the stop. A hook right after the stop finds the
  mapped account's job still running in `debug` charged to `hpcb`, and after the idle window the queue is empty. The
  harness's admin channel now reaches the PBS server on a PBS profile, the node gate reads a PBS warm block with
  `qselect`, and the two Slurm-only fake-MEP scenarios now require Slurm. Hermetic tests in
  `agentic/harness/test_pbs_mep.py` (in the CI list) drive hpc-bridge's own connect, start, run and stop against the
  profile's catalog entry and template, and render the template as the endpoint manager would.

## 0.1.24 — 2026-10-13 — the endpoint's environment is checked, not trusted

### Fixed
- **A broken endpoint environment on the login node is repaired, not reused.** On an SSH facility with no endpoint
  installed, hpc-bridge keeps its own Python environment under `~/hpc-bridge/`. It used to trust whatever was there,
  so an environment whose dependencies had drifted (live on the lab cluster: the endpoint's command failed at import)
  broke every first connect. Now each bring-up checks the environment through the endpoint's own command and its
  Python version. When the check fails it builds a new, relocatable environment beside it (package cache off), checks
  it, and only then renames it into place, keeping the broken one as `.broken.<time>`. Two sessions building at once
  both finish and one wins the rename; there is no lock to wait on or leave behind. A failed build is kept as
  `.failed.<time>` with its last error line reported, and nothing is ever deleted. Stop and teardown never build; they
  use any environment of hpc-bridge's that answers. An environment you prepared yourself at `HPC_BRIDGE_REMOTE_VENV`
  is used when it passes the same check, so a login node without internet works. Where the login node has no `uv`,
  hpc-bridge installs one into `~/.local/bin` without editing your shell profiles; a `uv` too old to build (before
  0.2.31) is named in the error.
- **A compute node that cannot run the environment says so at once.** A block on a node of another CPU type than the
  login node used to die with a bare exit code (the lab cluster, for an endpoint hpc-bridge stands up over SSH: an
  x86_64 login node, aarch64 compute nodes, a shared home; its facility endpoint is not affected). Its job now ends
  with one line naming the environment, the architecture it was built on and the node's. The pilot notice quotes that
  job's own line and says a rebuild cannot help when the architectures differ. It also tells the agent not to change
  `env_setup`. What works is a partition of the login node's type, or the facility's own multi-user endpoint where
  `list_facilities` has one.
- **Teardown stops an endpoint even after `env_setup` was changed.** Stop, delete and list now run the endpoint's
  command in the environment the endpoint was started with, not the facility's current `env_setup`. Live on the lab
  cluster, an agent reconnected with an empty `env_setup` to "fix" an architecture mismatch. The teardown then failed
  with `globus-compute-endpoint: command not found` and left the endpoint running. An endpoint started in this
  session is stopped through the `env_setup` it was started with. When the start is unknown, teardown searches
  hpc-bridge's default environment under `~/hpc-bridge/`, and never builds one. That choice ends with the teardown,
  so the next bring-up runs in the current `env_setup` again. A reconnect that replaces hpc-bridge's line on a running
  endpoint now gets a warning saying what that does and doesn't change.
- **A block replaced after a failed check is asked about.** If a check failed with an error (a task lost with its
  worker, a refused submit, a transient conflict), hpc-bridge forgot which block the user had confirmed. The block
  that came up next was then used under the old confirmation, with no new question. Seen on the fake cluster
  (2026-10-13): job 112 was cancelled and a check failed with `TaskExecutionFailed`. Job 113 then read `up` and ran
  work. Now the confirmed block's job id survives a failed check, as it already survived a failed command and a
  runner rebuild:
  - a different job answering means the old block is gone. The user is asked again, the notice says the new block is
    already running and billing, and nothing more runs on it once a check has seen it;
  - a check that times out reads as the block gone, not a cold start;
  - the same job answering keeps its age, and the time since the failed check is billed;
  - the idle presumption keeps counting from the block's last proof of life, so it still asks before a check could
    start a new block.
- **A `down` stops the spend clock.** After a confirmed block, the following now stop the block's spend clock where
  the block was last known up, at the latest:
  - a rejected block submission (when the endpoint runs one block);
  - a no-account verdict;
  - the repeated conflict that reads `down`;
  - a check that fails with an error.

  An orphaned task stops its block's clock too. Before, the clock could run on while the status read `down`. If
  the same block answers again later, its clock picks up where it stopped, so the gap is still counted. Before, the
  gap counted 0 s where the facility publishes no idle window (Anvil, Delta). `needs_confirmation`,
  `needs_account` and an error's `down` now report the session spend, so a block that was just released shows in
  it at once. The rejected-submission notice no longer says "nothing was billed": only the new block wasn't
  created.

### Changed
- **The environment line is short.** The `env_setup` discovery proposes is now about 460 characters and only
  activates the environment, named for the endpoint version and Python it was built for
  (`~/hpc-bridge/gce-venv-endpoint-<version>-py<X.Y>`); a newer version gets a new one beside the old. **`details=`
  no longer needs `env_setup`** after a proposal: the server keeps the proposed one, and the proposal tells the agent
  to leave it out unless the user wants to change it. (An open model corrupted the old, much longer line when it
  echoed it back.)
- **Older configurations keep working.** A line an earlier hpc-bridge generated, in your local facility cache or in
  the registry (the Expanse entry), is read as the new line; the stored text is not changed, so earlier plugins
  reading it are unaffected. An endpoint an earlier hpc-bridge started is reused as it is: its blocks keep the
  environment it was started with, and hpc-bridge says so only when that is not just this upgrade ("only if its
  blocks fail or you changed env_setup").

### Known issues
- **A replaced block can run work before a check sees it.** A check runs only once the last answer is 45 s old, and
  every completed command counts as an answer. So commands sent less than 45 s apart run on a replacement block
  without a question until a pause lets a check run, or the old block's walltime is reached. A follow-up closes it:
  each command will report the scheduler job it ran in.

### Testing
- Three fake-cluster scenarios join the profile sweep: `venv_broken_repair`, `venv_arch_mismatch`,
  `endpoint_upgrade_reuse`. The harness finds the endpoint's environment the way the plugin's teardown does.

## 0.1.23 — 2026-10-13 — faster first results, honest stops and output, batch jobs counted as spend, ALCF Polaris

### Added
- **ALCF Polaris** joins the registry as `polaris`: ALCF's facility multi-user endpoint (PBS Pro), zero SSH, an ALCF
  project as the account (default queue `debug`, 15 min). Workers run ALCF's own Python 3.13 install. Proven live
  2026-10-13. Your ALCF identity must be linked to your Globus login.

### Changed
- **The first SSH result comes in about 40 s, not 95–114 s.** Endpoints hpc-bridge stands up run parsl's scaling pass
  every 5 s instead of 30 s; scheduler queries stay at once a minute. An endpoint configured before this keeps 30 s
  until it is torn down.
- **A replaced block is caught.** The canary now reports the block's scheduler job id. When a check is answered by a
  different job (the old block was released, cancelled or preempted), the user is asked about spend again before any
  work runs on it. An idle block is presumed released 15 s after its idle window, not 60 s.
- **A batch job submitted by hand is spend.** The skill no longer suggests `sbatch` over the free login shape without
  asking: a job submitted from the login node, or from inside a block, bills the allocation like a block, so the user
  confirms it first. `run_shell`, `login_shell` and `poll_task` results that submitted a job say so and how to cancel
  it; hpc-bridge doesn't track or stop such jobs. The agentic graders count it as a billed start.
- **With no facility connected, the tools say so and start nothing.** Before, a `run_shell` sent before
  `connect_facility` finished got raw internals, and on Linux could stand up a local `hpc-bridge-dev` endpoint. The
  notice now says what to call, including how to finish a connect that only proposed a configuration or is waiting on
  a one-time login. **The server never stands up a local endpoint any more:** `HPC_BRIDGE_ENDPOINT_ID` is the only
  local-development path, on every OS.

### Fixed
- **Long output is never cut silently.** The Compute SDK kept only the last 1000 lines of each stream and said
  nothing, so a long `cat` came back as its tail passing for the whole file. Each stream the agent gets now keeps its
  end (16,000 chars), opens with a marker saying what was dropped, and the notice says how to read the rest (redirect
  to a file, read ranges). A result over Compute's 10 MiB limit is reported as what it is: the command ran, its output
  and exit code are lost.
- **Pressing Esc on a long `run_shell` no longer loses the command.** Cancelling a call does not cancel its command,
  which kept running untracked: `stop_endpoint` answered `down` while the endpoint relaunched a block for it. Now the
  command stays tracked (`poll_task` returns it) and stop refuses honestly, naming what holds the block. Stop also
  refuses while a call is still in its sync-wait. A `reset_session` that outlives its wait becomes a poll handle
  instead of a false "timed out".
- **A one-time-login resume keeps the login host**, so following it reaches the proposed configuration instead of
  asking for the facility details again.

## 0.1.22 — 2026-10-09 — Codex, Pi and Hermes: tested recipes, and long calls that survive their timeouts

### Added
- **Tested recipes for Codex, Pi and Hermes** (`docs/user/other-hosts.md`), at their latest releases (Codex 0.162.0,
  Pi 1.1.0, Hermes v0.21.6): the settings each needs (Codex: a startup timeout and, for `codex exec`, the tool
  approval mode; Pi: `--exposure direct` and a 300 s timeout; Hermes: `--connect-timeout 180`), the skill install for
  all three, `--python 3.13`, and how to update a `uvx` build. `agentic/install_check/` follows them in a clean
  container and checks each harness ends up with the 12 tools and the skill.
- **`HPC_BRIDGE_JOURNAL=<path>`**: the server appends one JSON line per tool call (tool, arguments, the result the
  host received, duration, and a sequence number in the order calls started), whatever host drives it; one-time codes
  are redacted and the file is private (0600).

### Changed
- **Long tool calls report progress every 15 s.** Pi gives an MCP request 60 s and restarts the clock on progress; a
  `run_shell` sync-wait or a first SSH bring-up was cut off. A host that sends no progress token sees no change, and
  a cancelled call still cancels the work.
- **Tool annotations**: `list_facilities` and `poll_task` are read-only (Codex no longer asks before them),
  `teardown_endpoint` is destructive.

### Fixed
- **The skill's frontmatter is strict YAML.** An unquoted `bootstrap: stand up` made it invalid; Pi dropped the
  skill without a word. A regression test parses it strictly.

## 0.1.21 — 2026-10-09 — registry health: drift is checked, recorded, and a fresh install gets the tested client

### Added
- **`hpc-bridge-registry-health`**, a zero-cost check meant to run on a schedule (`scripts/registry_monitor.sh` installs
  it as a launchd job that runs main's build via `uvx`): the live index serves main's seeds field for field; each
  facility endpoint is online and still what its entry was proven against (endpoint version, Python, template and
  manager-config digests — a field missing on either side is reported, never read as a match); SSH login hosts answer;
  a parsl newer than the one a floating facility's worker was proven on; and what a fresh `uvx --from git+…` install
  resolves against the lock. Exit 0/1/2; alerts only on new findings (an escalation is new); a missing Globus login is
  a finding, not a crash.
- **Entries record what they were proven against** (`verification`: date, versions, digests, and what the worker
  reported). `agentic/registry_reprove.py ENTRY --record` runs one real block and writes it into the seed; it stops the
  endpoint whatever happens from the first submit on (an error, a timeout, Ctrl-C) and, on a run that did not pass,
  warns that a check task may still be queued at the facility. Read leniently by the plugin; checked strictly at
  ingest (a facility-endpoint block must carry at least its date and endpoint version).
- **The worker check reports the worker's parsl and globus-compute-endpoint**, parsl shown in the warm notice — the
  version that must match the facility's endpoint and that no facility publishes.

### Changed
- **The client SDK is pinned to the version the facilities were proven with** (`globus-compute-sdk==4.18.0`) and the
  lock moves to it. `uvx --from git+…` (the Pi / Hermes / Codex install) ignores `uv.lock`, so it was resolving SDK
  4.18.0 against a 4.16.0 lock — every such user ran an untested client. The three facility endpoints (Delta, Anvil,
  globus-labs) were re-proven on 4.18 (2026-10-09); Expanse (SSH, one-time code) still carries its 0.1.20 proof on
  the 4.16 client.
- **The stale-entry note at connect also fires when a facility's Python (major.minor) or user template changed**
  since the entry was verified, not only its endpoint version.

## 0.1.20 — 2026-10-06 — a block on a shared partition no longer asks for a whole node

### Fixed
- **Expanse's `shared` partition can run a block.** Parsl's Slurm provider defaults to `--exclusive`, which asks
  for a whole node; Expanse's `shared` partition caps a job at 127 CPUs on 128-core nodes, so every hpc-bridge block
  there pended forever (`QOSMaxCpuPerJobLimit`). Entries can now set `defaults.exclusive`, passed to the Slurm
  provider only when set (every other facility keeps Parsl's default), and the Expanse entry sets `false`. Found
  re-proving the registry: Expanse had only ever been run on its login node. An endpoint already running on a
  facility keeps the template it was started with until it is torn down and bootstrapped again.

## 0.1.19 — 2026-10-06 — worker environments that match each facility's endpoint

### Added
- **Registry entries say how their worker keeps pace with the facility.** A facility-endpoint entry carries
  `compute.worker_env`: `strategy` `pin` (the facility runs a fixed install, and the entry's worker install pins
  its packages to it) or `float` (the facility rebuilds at the latest versions at every start, and the worker
  installs with `--upgrade`), checked against the install line, plus `verified_with`, the facility endpoint version
  the entry was proven against. The install stays literal in `env_setup`, so plugins older than this one, reading
  the same registry, also get a working worker. Clients read `worker_env` leniently: an unknown strategy or a
  garbled block costs the staleness check, never the facility. The strict check (a `pin` names its `parsl==`, a
  `float` upgrades the endpoint install itself, `verified_with` is set) runs when the curator ingests.
- **An upgraded facility is flagged at attach.** When the facility's live endpoint version differs from the one the
  entry was verified with, `connect_facility` says so (STALE ENTRY): its packages may have moved, and a block that
  starts but never answers is then the likely result.

### Changed
- **One worker check in flight at a time.** Every probe of a cold block used to submit a new check task (24 in one
  15-minute wait), and a facility endpoint keeps relaunching billed blocks for as long as any task is queued — on
  Delta for up to 48 hours, with no way for the client to cancel. A pending check is now waited on again instead of
  resubmitted; one still pending after five minutes is abandoned for a single fresh one (an endpoint that restarts
  can drop a task), and warmth is dated from when the worker actually answered.
- **A long wait on a facility endpoint names the silent, billing case.** Still "allocating" after five minutes, the
  notice names the three things it can be — a long queue, a scheduler rejection, or a block running while its
  worker cannot return results, which the facility keeps relaunching — and how the user's `squeue` tells them apart
  (PENDING, no job, RUNNING), and repeats the attach's stale-entry note when there is one. The facility stop notice
  no longer implies a queued check task can't keep blocks coming.

### Fixed
- **Anvil and Delta run again.** Re-proving the registry on 0.1.18 found both broken by the same skew, in opposite
  directions: parsl's interchange↔worker protocol changes between releases and `globus-compute-endpoint` only sets a
  parsl floor. Anvil rebuilds its user endpoint at the latest versions and had moved to parsl 2026.10.05 while our
  worker venv kept 2026.08.10; Delta's fixed install runs 2026.08.10 while a fresh worker venv picks 2026.10.05.
  Either way the block ran and billed while the endpoint dropped every result, and on Delta the endpoint kept
  relaunching GPU blocks for its queued tasks. Anvil's entry now floats (`--upgrade`), Delta's pins
  `parsl==2026.8.10`; `globus-labs` states the parsl its gce version pins. **These reach installed plugins
  through the live registry**, so they take effect once the entries are re-ingested, whatever plugin version is
  installed. A `float` facility can still skew if its endpoint process stays up across a parsl release (weekly)
  and then starts a new block; reusing the facility endpoint's own venv would close that, and is a follow-up.
- **A failed status call no longer drops the worker pin.** When the facility's status API failed, the template
  was never read, the worker setup lost its version and was dropped, and the facility's own default (no parsl pin
  on Delta) ran instead. The template is read regardless, and an unreadable version falls back to the one the
  entry was verified with.

## 0.1.18 — 2026-10-05 — fixes from the 2026-09-05 plugin review: teardown, pins, credentials, the numbers the agent relays

### Changed
- **Spend is asked again after a block is gone.** The `confirm_spend` acknowledgement used to last the whole session,
  so a block that idle-released, ran out its walltime, or was cancelled was silently replaced by a new billed one on
  the next call. It now covers one block. The server presumes a block gone by the clock alone (no task for longer
  than the known idle window plus a minute's grace, or older than the walltime) before submitting anything — also
  before a partition or account switch — or learns it when a check of a block it had confirmed warm goes unanswered
  (including after a failed task or a runner rebuild). Either way the next call returns `needs_confirmation` with the
  reason — even one that passes `confirm_spend=True`, which was given before the reap was known. When a check found
  the block gone, `run_shell` and `ensure_endpoint_up` report `block_state="provisioning"` because that check may
  already be bringing up a new block; `stop_endpoint` waits for that pilot and cancels it on an SSH facility, while
  on a facility endpoint the notice says it idles out. A block the clock presumes released is billed to that time,
  on the next call and on `stop_endpoint`/teardown, not to whenever the agent came back; a task polled long after
  it ended counts from when it ended. A check queued behind a
  command still inside its synchronous wait is not read as a reap, and local dev's held block never is.

### Fixed
- **Teardown can no longer be retargeted, run twice, or replay a stale answer.** `teardown_endpoint` decides what it
  tears down in one locked step with no await before it: an SSH endpoint's teardown is claimed and snapshotted there
  and runs (release, one-time-code gate, login-node ops) as one task on that facility and endpoint; a facility
  endpoint is detached there outright. Before, a `connect_facility` in the same tool batch could rebind while
  teardown awaited, and the background task then ran `gce stop`/`delete`, removed the pin and wiped the token store of
  the NEW facility (and on a facility endpoint, the detach wiped the new binding); two overlapping calls ran the
  login-node ops twice. `connect_facility` refuses to rebind while a teardown is still running, and a rebind drops the
  previous binding's pending one-time-code handoff. A finished result is replayed only as a `down` whose state is
  still cleared — a code request already answered, or a `down` followed by a `run_shell` that bound an endpoint again
  (even under the same id), starts a fresh teardown.
- **What teardown says is what happened.** The interim `tearing_down` and the code request say whether the block
  release went through, is still in progress, or was dispatched but not confirmed (a cold login channel) — not
  "already went through" regardless. A login-node `stop`, status check or delete that times out is reported as
  unconfirmed — with what the report says was done (the token copy removed, the connection closed), and as `down`
  when the delete went through — not as "nothing was removed, still running".
- **An adopted endpoint keeps its login-node pin.** When bootstrap adopts an endpoint that is already running, the
  stored record keeps the node pinned at launch (for the same endpoint id only) instead of being rewritten with no
  node, which sent the next session's SSH to the round-robin alias and orphaned the manager (a regression since
  0.1.1). The early record written while seeding credentials carries the pin too.
- **Credential seeding never overwrites a token store hpc-bridge did not place.** Seeding happens when
  `globus-compute-endpoint whoami` fails on the login node, but that also happens when a store exists and the
  facility's `env_setup`, PATH, scopes or network are broken. The write now refuses on the node itself if a store
  holding tokens is present — unless hpc-bridge's own record, for the same SSH login, says it placed that store (a
  stale copy of ours is refreshed; the flag is cleared when a wipe ran but the delete did not). A store with no tokens
  is replaced: `whoami` itself creates one on a node where nobody has logged in, and the first fake-cluster run of
  this change refused every first bring-up on it. The check runs with the endpoint environment's `python3`; where
  no python3 with sqlite3 exists, the connect says it could not check instead of guessing. When it refuses,
  `connect_facility` says so with whoami's own error. The check-and-write runs in `sh` whatever the login shell is
  (under tcsh the guard would have been skipped). Before, hpc-bridge replaced the user's credential and teardown later
  deleted the replacement.
- **The numbers the agent relays are the facility's.** On a facility multi-user endpoint the idle-release window is
  read from the facility's published template (`max_idletime`), and when the template sets none the notices say so
  instead of quoting hpc-bridge's 600 s default. Spend and the confirm-spend notice use the block's own node count (and
  the notice names the walltime) instead of always one node. An SSH teardown stops the compute spend clock at the
  release, as `stop_endpoint` does, and a released block's spend now stays in every later result of the session (a
  teardown resumed after its one-time code reported 0).
- **A changed host key is not coached like an unknown one** in `complete_preauth`: it says the key differs from the
  trusted one, not to accept it until the facility confirms the fingerprint, and names the `known_hosts` entry exactly
  as OpenSSH reports it. A revoked host key (`known_hosts` `@revoked` or `RevokedHostKeys`) is refused outright, in
  `complete_preauth` and on the connect path (where it also no longer drops the login-node pin).
- **Skill:** the session's working directory and environment survive a new block (they live on the shared
  filesystem) — the `block_reaped_resume` scenario caught an agent telling the user the opposite.

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
https://github.com/globus-labs/hpc-bridge/issues. (0.1.0 was the pre-release the marketplace served during development;
the version moves so existing installs update.)

### What's in it
- **Install from Claude Code** — the repository is its own plugin marketplace: `/plugin marketplace add
  globus-labs/hpc-bridge`, `/plugin install hpc-bridge@hpc-bridge`. Eleven tools, a skill the agent follows on its own,
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
