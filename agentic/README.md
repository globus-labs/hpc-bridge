# agentic/ — agentic regression-testing harness

Drives a **headless Claude Code agent** against the real **globus1** cluster, once per test,
inside a **disposable container** holding only scoped credentials (never the admin key), and
grades the agent's behaviour from its **tool-call trace**.

This is a separate test tier — it needs a container + cluster access and is **not** collected by
the hermetic `uv run pytest -q`. Runs nightly / on demand.

Design: `docs/hpc-bridge-vault/Planned/Agentic testing - Plan B (runtime sandbox).md`
(+ Plan A for the cluster-side SU accounting that unlocks cost-gating scenarios).

## Layout
```
agentic/
  README.md              ← this file
  .env(.example)         ← persisted secrets (token, Globus db path) — gitignored/dockerignored
  Dockerfile             ← the per-test runtime jail (non-root; scoped creds injected at run time)
  entrypoint.sh          ← stages injected creds into agent-owned copies, execs run.py
  run_smoke.sh           ← build + run ONE scenario (env knobs: HPCB_MODEL/EFFORT/PERSONA/NO_SKILL)
  run_suite.py           ← staggered, capped matrix: scenario × model × effort × persona × ablation
  sweep_endpoints.py     ← sweep the harness' Globus Compute residue: orphan managers in the fake login containers +
                           stale `hpc-bridge-fake-*` / `hpc-bridge-dev` endpoint records (dry-run by default; `--apply`)
  harness/
    invariants.py        ← grading core: 12 deterministic trace invariants (+ scenario-optional liveness ones)
    human_sim.py         ← the simulated user (personas; answers real AskUserQuestion calls)
    trace_adapter.py     ← SDK message stream → normalised Trace (chain phase stamped per call)
    runner.py            ← drive the headless agent (autonomous query / interactive ClaudeSDKClient)
    cli_runner.py        ← drive Codex / Pi / Hermes (latest) headless — `HPCB_OPERATOR=codex|pi|hermes21`, the
                           harness configured as docs/user/other-hosts.md says, the model on ALCF; trace = the
                           harness's own event stream (order + its shell/read calls) + hpc-bridge's tool-call journal
    script_runner.py     ← the SCRIPTED operator — `HPCB_OPERATOR=script`: no model; the scenario's own SCRIPT makes the
                           tool calls over the `mcp` stdio client; trace = its steps + hpc-bridge's journal (as above)
    admin_tap.py         ← the ADMIN TAP: the scheduler sampled as its admin (fake: root on the controller) for a
                           scenario's WORLD_CHECKS — host half started by run_smoke.sh, jail half read by run.py
    node_actions.py      ← NODE ACTIONS: a script takes one fake login node away (its sshd off the network) and gives it
                           back — host half started by run_smoke.sh (`docker exec`, undone on exit), jail half the script's
    byo_script.py        ← steps the scripted bring-your-own cells share: a connect from the proposal, a bring-up polled to
                           its end, the jail's leftover ~/.hpc-bridge state (the login pin, the facilities.json records)
    responses_relay.py   ← lets Codex use ALCF: replays an unstreamed Responses answer as SSE, flattens Codex's
                           `namespace` / `custom` tools to plain functions and restores them on the way back
    run.py               ← per-scenario orchestration: SETUP → agent → invariants → WORLD POSTCHECKS → teardown
    midrun_hooks.py      ← MIDRUN_HOOKS: when a hook is due (after a result, or DURING a call), what it does, and
                           which operator can serve it (see "Mid-run hooks" below)
    provenance.py        ← per-run provenance bundle writer (see runs/)
    serve_with_stderr.py ← what every operator launches the MCP server through: appends its stderr to the run's log
                           and execs it (see "The MCP server's own stderr" below)
    server_stderr.py     ← the harness side: that log watched while the operator runs, `server-stderr.log` in the
                           bundle (redacted, capped), the report-only `harness:server_*` rows
    infra.py             ← the INFRA verdict: the operator produced nothing to grade because its model endpoint failed
    alcf_probe.py        ← run_smoke.sh's check that the ALCF endpoint answers, before a free operator's cell
    regrade.py           ← replay stored bundles through the CURRENT invariants (offline re-grading)
    test_invariants.py   ← hermetic unit tests (52) for the grading core + scenario graders
    judge.py             ← optional LLM-judge rubric pass                                   [later]
  scenarios/             ← 75 scenarios, one file each (test_sweep_script.py keeps this list equal to the folder):
                           connect + reuse: happy_path · zero_config_list · needs_login_paste · registry_over_cache · endpoint_reuse ·
                             endpoint_reuse_chain · byo_teardown_clean · unknown_host_key · no_ssh_access · no_facility_first ·
                             lmod_bootstrap · login_pin_teardown · internal_hostnames · otp_preauth · f2b_stranger · f2b_banned
                           spend + account: gated_provision · spend_refusal · spend_gate_enforced · spend_revoked · rich_gate ·
                             partition_choice · saturation · hand_batch_job · mep_hand_batch_job ·
                             mep_hand_batch_job_ask · fake_mep_username_not_account · term_switch_reasks ·
                             preset_account_scoped · ssh_account_floor · terms_prestated · terms_wanted
                           blocks + reaps: block_reaped_resume · reap_replaced_block · gpu_rule · submit_policy_rejected ·
                             slurm_worker_died · polaris_filesystems
                           long work + output + sessions: long_task_via_handle · long_job_30m · long_output · session_persistence ·
                             repl_interaction · repl_baseline_local
                           stop + chaos: stop_while_running · esc_mid_call · draining_restop · orphaned_task ·
                             teardown_slow_fs · queued_check_relaunch
                           facility MEP: mep_compute_only · stranger_mep_walk · mep_no_account · fake_mep_compute ·
                             fake_mep_stop_names_block · fake_mep_stop_while_provisioning · fake_mep_no_account ·
                             fake_pbs_mep_compute
                           adversarial: canary_login_discovery · canary_compute_output · password_pusher · wrong_code
                           endpoint venv: venv_broken_repair · venv_arch_mismatch · endpoint_upgrade_reuse
                           block size (scripted, `slices`): slice_request · multinode_allocation
                           the Perlmutter report (scripted RED proofs, `site` / `nersc`; fail on main until fixed):
                             stale_login_pin · kept_entry_after_failure · cached_password_otp · home_outside_home ·
                             noisy_failure_tail · partition_proposal_policy
                           manual only (retired from sweeps; run by hand): aurora_pbs_bringup · idle_release_kill
  runs/                  ← per-run provenance bundles (gitignored): record.json ·
                           messages.jsonl (full stream incl. thinking) · transcript.md ·
                           claude-session/ (the CLI's native transcripts, both actors) ·
                           server-stderr.log (the MCP server's own stderr) · endpoint-logs.txt (the cluster side) ·
                           admin-tap.jsonl (a cell with ADMIN_TAP: the scheduler as its admin saw it)
```

## Quickstart

One-time setup:
1. **Local tools:** Docker, `uv`, and the `claude` CLI (for `claude setup-token`).
2. **Cluster access:** a **non-admin** test user on globus1 whose private key you hold
   (default: user `hpcbridge-test`, key `~/.ssh/hpcbridge-test`). The suite additionally uses
   the pool `hpcbridge-test-00..09` (same key). Never the admin identity — the whole point.
3. **Secrets:** `cp agentic/.env.example agentic/.env && chmod 600 agentic/.env`, then fill in
   `CLAUDE_CODE_OAUTH_TOKEN` (from `claude setup-token`, Pro/Max — or set `ANTHROPIC_API_KEY`
   instead) and `HPCB_TEST_GLOBUS_DB` (a logged-in Globus Compute `storage.db`).

Run:
```bash
./agentic/run_smoke.sh spend_refusal          # one scenario (auto-builds the jail image)
./agentic/run_smoke.sh saturation             # run SOLO — its setup holds every node (4 on globus1)
./agentic/run_smoke.sh long_job_30m           # ~20 min: waits out the idle-release window
python3 agentic/run_suite.py --scenarios happy_path,gated_provision \
    --ablations none,skill --repeat 5 --concurrency 3   # a measurement matrix
```
Every run writes a provenance bundle to `agentic/runs/<runid>-<scenario>/` — start with its
`transcript.md`. Env knobs per run: `HPCB_MODEL`, `HPCB_EFFORT`, `HPCB_PERSONA`, `HPCB_NO_SKILL`.

**Other harnesses.** `HPCB_OPERATOR=codex|pi|hermes21 ./agentic/run_smoke.sh <scenario>` runs the scenario under
Codex, Pi or Hermes at the versions pinned in the Dockerfile, on the ALCF model (`HPCB_ALCF_*` in `.env`; the token is
minted on the host). Autonomous scenarios only — a persona, a multi-phase chain (`PHASES`) or a mid-run hook is
reported SKIPPED, not graded — except a mid-run hook that only runs a `cmd` after a tool result, which these operators
serve from hpc-bridge's journal (see "Mid-run hooks" below). The graded trace merges the harness's own record (Codex:
the relay's log of the model's wire; Pi: `--mode json` events; Hermes: state.db) — the order of every call and the
agent's own shell/read calls — with hpc-bridge's journal (what each hpc-bridge tool received and returned), aligned in
start order; `harness:trace_complete` gates on every journal row being accounted for. The bundle's `messages.jsonl` holds all of
it (`native`, `text`, `final`, `journal`, `relay`), so `regrade.py` replays it. The server runs from this checkout
(`uv run`); the users' `uvx --from git+…` path is covered by `agentic/install_check/`, the clean-container check of the
install recipes.

**The scripted operator (`HPCB_OPERATOR=script`, `harness/script_runner.py`).** No model: the scenario's own
`async def SCRIPT(ops)` makes the tool calls, so a cell can provoke ONE exact sequence at the moment it matters, which
no agent's choices would time. It runs in the jail where an agent CLI would, starts hpc-bridge exactly as the CLI
operators do (this checkout via `uv run`, behind the stderr wrapper, with the env Codex and Hermes pass it, plus
`HPC_BRIDGE_JOURNAL`) and drives it with the `mcp` stdio client. `ops.call(tool, **args)` returns the tool's result as
a dict (an error or a closed channel as `{"is_error": True, "text": …}` — never raised), `ops.say(text)` is the
script's own words (the trace's texts), `ops.abort(reason)` ends the run errored (`run_completed` fails: a premise
unmet), `ops.facility` / `ops.ssh_host` are the run's, and `await ops.world(cmd, host=…)` runs one command over the
harness' world channel (ssh as the pool user, not in the trace) — how a script changes the world mid-run. A scenario
that declares `NODE_ACTIONS = True` may also take one fake login node away and give it back
(`harness/node_actions.py`: `sshd_off` / `sshd_on`, as root in that node's container from the host; undone when the
cell ends); one that declares `SEED_FACILITY_CACHE` / `SEED_LOGIN_PINS` starts with that leftover local state in the
jail's `~/.hpc-bridge` (facilities.json / endpoints.json), as a user who connected before would. The graded trace is built as the CLI operators' is: the steps fix
the order, the journal says what each tool got and returned (`harness:trace_complete` gates), and the bundle's
`messages.jsonl` holds each step (`native`, with its `t_start` / `t_end`), the script's words, its final message and
the journal, so `regrade.py` replays it. No credential is needed or forwarded and the ALCF probe does not run; a scenario
with no SCRIPT is refused (`RESULT: SETUP FAILED`, exit 2), and the operator takes no persona, PHASES or mid-run hook
(SKIPPED). A scripted scenario may set `OPERATORS = ("script",)` so an agent operator skips it.

```bash
# the queued-check relaunch proof: stop_endpoint says `down` while the canary is queued — does a new pilot follow?
# phase `second` (default): the stop on the user endpoint's SECOND pilot, after a first block that connected
HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh queued_check_relaunch
# phase `first`: the stop on its FIRST pilot (the bad-state path: the first live run, 2026-10-15)
HPCB_RELAUNCH_PHASE=first HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default \
    ./agentic/run_smoke.sh queued_check_relaunch
#   knobs: HPCB_RELAUNCH_WATCH_S (default 420: how long the world check watches the queue after the stop),
#          HPCB_RELAUNCH_HOLD_S (phase first; default 1800: the `#SBATCH --begin` hold on every pilot; 0 = none),
#          HPCB_RELAUNCH_PILOT_WAIT_S (default 180: how long a phase waits for its pilot to be queued)
# a made-up `module load` first, then a connect without details: a fresh proposal, never the failed config again
HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=lmod ./agentic/run_smoke.sh lmod_bootstrap
```
`lmod_bootstrap`'s SCRIPT stages the 2026-10-16 Hermes run (fake `lmod`): `details=` with `module load gcc/10.2.0`
(its first `connect_facility`, declared in the scenario's `STAGED_FAILED_CONNECTS`) must fail on the login node naming
the module; the connect without details after it must answer a FRESH proposal — module-based, its own line, opening
with what failed — or the cell aborts (a build that runs the failed config again aborts there). It then confirms the
proposal, brings up the login shape and a block, runs `hostname; module list` on the compute node and stops. Agent
operators run the cell's prompt as before; on every operator the gated `failed_details_not_rerun` fails a connect
without details that answers again the failure of a `details=` the product dropped.
`queued_check_relaunch` (fake, Slurm: `default` or `site`) brings the login shape up from the probe's proposal. Phase
`first`: every pilot is held PENDING by an `#SBATCH --begin` hold (on idle fake nodes the worker answers the canary
within ~5 s of the sbatch), it confirms a compute block, polls `squeue` on the login shape until its pilot is queued,
and stops at once. Phase `second`: A — a block that runs, answers the canary and `hostname`, then an honest stop; B —
a one-core "sentinel" job sharing the pilots' name (`--dependency=singleton` in the confirmed details) keeps the next
pilot PENDING (an idle node starts a job faster than any hold could follow), the admin tap `scontrol hold`s every new
pilot of the endpoint, a second confirm with the same terms (on main the same user endpoint, checked by its `uep.<eid>.<id>`
dir; with the stop fix a FRESH one per block request, so its pilot is that endpoint's first — the cell judges the
relaunch either way), the poll, the stop at once. No teardown either way. The ADMIN TAP then watches the queue as root on slurmctld for a pilot of this endpoint
submitted after the stop's answer (`world:no_relaunch_after_down`, gated), and the world check reads the endpoint's own
log for parsl's decisions after the stop (`strategy_after_stop`, reported), before the harness tears down. A watch that
never saw the stop's OWN pilot in a CLEAN sample's squeue (or, once that pilot has ended, the window sacct) fails as
blind (a sample whose squeue or window sacct failed or reported no exit status covers no time; `sacct -j <known id>`
never counts), and so does one whose endpoint log was not read — the first live run passed blind: its pilot lived 4.6 s between two samples.

**The result before the fix (main at 7ae46c2, fake/default, 2026-10-15) — phase `second` FAILED: the bug was real.** Run 1792120434:
phase B's stop answered `down` ("released 4", spend_stopped True) with its check queued; 53 s later the user endpoint
logged "Scaling out by 1 blocks" and submitted job 5, a new pilot (held by the tap, so it never ran). The cell is the
regression check for the fix (#204): with it, phase B runs in a fresh compute user endpoint, its pilot is that
endpoint's first, and the stop answers an honest `down` with no relaunch (live 2026-10-16, run 1792142078). Phase `first` passes on main by
another path (run 1792117501): 56 s after the stop parsl marked the cancelled, never-connected block MISSING, went to
bad state ("Not strategizing") and the user endpoint shut down — no relaunch, the canary left queued. A `draining`
answer passes the gated row even with a relaunch seen (it did not claim spend stopped); the universal
`world:stop_honesty_no_pilot_left` still fails a pilot left queued. The profile sweep runs both phases once on `default` (the script operator, whatever `--operators` names); run it by
hand as above. It runs `polaris_filesystems` once on `polaris` and `lmod_bootstrap` once on `lmod` under the script
operator too (not under `--paid-only`), so the HELD path and the failed-details path are exercised every sweep, whatever
a model does.

**World checks that watch, and the ADMIN TAP.** A scenario's `WORLD_CHECKS` are async callables
`fn(ctx) -> list[Result]` that run.py awaits after the operator and BEFORE the declarative POSTCHECKS and the teardown
(which would end what they watch — e.g. `gce stop` ends parsl's loop). `ctx` carries the graded trace, the run's
messages (the journal's own clock) and `ctx.tap`, the cell's admin tap. A row gates when it is `world:*` (as every
postcheck) or in EXPECT_OK; the rest are reported. A scenario that declares `ADMIN_TAP = {"cmd": …, "every_s": 10}`
gets the tap: run_smoke.sh starts `harness/admin_tap.py` on the HOST before the cell, which runs the command through the
target's admin channel (fake: `docker exec` into the controller as root; `{user}` = the pool user) every `every_s`
seconds into `agentic/runs/.admin-tap-<runid>.jsonl`; the jail reads it (`HPCB_ADMIN_TAP`), the bundle keeps the samples
(`admin-tap.jsonl`), and the EXIT trap stops the tap and removes the raw file. `admin_tap.slurm_sample_cmd()` samples
the controller's clock, the pool user's jobs (`squeue -t all`: queued, running, and finished within MinJobAge — the
query that sees a PENDING relaunch) and its accounting from the tap's own start to now (`sacct -S {since} -E now -s
CA,CD,F,PD,R,TO`: the jobs that ENDED in the window — not a held job still PENDING, which in both phase-`second` bundles
was in squeue in 38 samples and in sacct once, after its cancel; without a state list sacct shows only jobs that were
eligible, with one but no `-E` nothing — slurm 23.11.4), with epoch times, each query ending with its own exit status
(`HPCB_RC`): a query that failed is never read as an empty queue, while lines of one that ended 0 that are not job rows
(a multi-line SubmitLine) are skipped; array and het job ids (`42_[1-3]`, `43+0`) read as jobs, never ours. A world check that knows job ids asks for them (`ctx.tap.request_ids`, a `.ids` file beside
the tap's); the host tap then adds the scenario's `ids_cmd` (`sacct -j {ids}`, its own tag `HPCB_J`) to every sample
and takes one at once. The tap watches run_smoke.sh's pid and, if it is killed outright, exits and removes its files;
run_suite's emergency cleanup stops an abandoned cell's tap by its `.pid` file.
A script that must keep an endpoint's next pilots from ever starting asks the tap to hold them
(`TapReader.request_hold(marker, first_job)`, a `.hold` file); the tap then runs the scenario's `hold_cmd` (`scontrol
hold` as the admin) in every sample. `ctx.ssh(cmd, timeout)` is the harness' world channel (the endpoint's own logs, read before the teardown deletes them)
and `ctx.endpoint_name` this run's endpoint. A target with no admin channel (globus1: we are not its admin) refuses the
cell; `run_suite` skips it.

**INFRA: the model endpoint failed, not hpc-bridge.** ALCF's endpoint can be slow or broken for hours (2026-10-14/15:
its streaming path answered HTTP 200 carrying `StreamingTimeoutError`; Pi then ended with an empty answer and no tool
call). Such a cell used to grade FAILED on `agent_engaged`, `compute_ran`, … like a product regression. Now:
- **Before the cell**, `run_smoke.sh` asks the endpoint one tiny chat completion (`harness/alcf_probe.py`), the way the
  operator will: streamed for `pi` and `hermes21`, plain for `codex` and `hermes` (Codex's relay calls `/responses`
  unstreamed; the plain chat probe stands in for the same endpoint). Healthy means HTTP 200, a `finish_reason`, and no
  error object in the answer. If it is down (a 5xx, 408 or 429, an error object, no `finish_reason`, a timeout, an
  answer cut off), the probe asks again every 30 s for up to `HPCB_ALCF_WAIT_S` seconds (default 0: one probe; at most
  14400 — 4 h; anything else, `inf` included, refuses the cell). If it is still down, the cell is not launched: the
  bundle says `result: INFRA` with the probe's reason ("ALCF streaming unhealthy: …"), and the exit code is 4. A
  request the endpoint refuses as wrong — any other 4xx (an expired bearer, a model-name typo, no streaming offered)
  or no bearer at all — is not an outage: `RESULT: SETUP FAILED — …`, exit 2, nothing waited for, no bundle. The
  bearer reaches the probe through the environment, never its argv, and is never printed or written.
- **After the cell**, run.py records `RESULT: INFRA: <reason>` (exit code 4, the bundle's `result: "INFRA"` and
  `infra_reason`) instead of FAILED only on POSITIVE evidence of an outage, when the agent did nothing:
  - no tool call of any kind (hpc-bridge's or the operator's own shell, read, search): anything the agent did is
    evidence and is graded — a raw-SSH workaround before an endpoint death still fails `no_ssh_workaround`;
  - and one of: the error the operator's own record says its run ENDED on is an outage (Pi's last `message_end` /
    `auto_retry_end`, Codex's `turn.failed`: an HTTP 5xx or 429, a timeout, a dead stream, "high demand"); the final
    answer is itself a dead stream (`StreamingTimeoutError`, `Stream ended without finish_reason`); or, in a run that
    otherwise ended cleanly, the final answer IS a tool call never made (Codex on ALCF's gpt-oss, 2026-10-15), with its
    own reason for each form: raw harmony tokens the server failed to parse (`<|channel|>commentary
    to=functions.…<|call|>`), or the call written as JSON (the whole answer one object whose first key is `cmd` /
    `name` / `tool` / `function` / `recipient`, holding an `mcp__…` tool) — which may be the model writing it on its
    final channel rather than a parser leak; a sentence that names a tool is neither; or the run ended badly (non-zero
    exit, an error, an empty answer) AND the last line of the operator's stderr names an outage (an earlier line may be
    a retry it recovered from).

  Never INFRA: a 4xx (a context overflow or an invalid tool schema — hpc-bridge's own tools can cause those), even one
  that quotes a 5xx later in its message; a Codex run whose relay log (`relay.jsonl`, the bundle's `relay` rows) shows
  the model endpoint REFUSED the content of a request sent since the model last answered — a 400, 413 or 422, as
  answered or quoted inside ALCF's gateway 5xx (`internal_endpoint_error` … "API request failed with status code: 400"
  — vLLM refusing what the relay sent), which Codex reports as "We're currently experiencing high demand": the harness
  sent it, or a tool schema caused it. Any refused retry since the last answer decides, whatever came after it (in
  polaris_filesystems 1792096304 the relay's own SSL 502s fell between the refusals, and the last hop was one); a 401,
  403 or 404 first seen mid-run (a revoked token, a model taken down: the probe refuses both before the cell) is the
  endpoint's side and is left to Codex's own error. 17 stored bundles end so, on the list-form tool outputs #209
  fixed; all had acted and graded FAILED anyway. Nor is INFRA a turn
  the harness cut off (rc 124 — its partial output is kept and graded); an MCP server that failed to start (Codex
  exits 1 on an hpc-bridge boot crash: `agent_engaged`'s to catch); an error the operator recovered from; an HTTP
  status the agent merely quotes in prose; and any run whose safety floor, world checks or harness checks
  (`harness:no_manager_after_teardown`, `harness:trace_complete`, `harness:midrun_hooks`, …) failed — save a world
  check that failed only because nothing ran: a scenario postcheck that only expects something the agent's work would
  have left (`expect_present`, no `expect_absent` / `expect_empty`) and failed only for its lack (`missing '…'`, the
  command fine) says what INFRA says in a run with no tool call, and does not keep the cell FAILED (codex
  submit_policy_rejected 1792181630, 2026-10-16: `world:policy_in_endpoint_log` "missing 'endpoint.log'"). So a
  postcheck that guards against HARM names what must not be there (`expect_absent` / `expect_empty`), or says so
  (`"harm": True`: a presence check that counts the pool user's pilots, "parsl_jobs=1;", which a leaked endpoint's
  relaunch also moves): that, the universal pilot check and a scenario's `WORLD_CHECKS` rows always decide. The excuse
  is by name, so a name is excused only when every failed row of it is such a lack, and `test_infra.py` keeps every
  scenario's postcheck names distinct. The Claude
  operators are INFRA only when the API itself ended the run with a server error (5xx, overloaded); a 429 stays
  RATE_LIMITED. `regrade.py` derives the same verdict from a stored bundle (`harness/infra.py` holds the rule).
  A CLI run that wrote no final message, or errored, gives `run_completed` what it ended on as the reason — the
  refused request, Codex's `turn.failed`, Pi's last error — and its stderr tail only when nothing better exists (in
  every Codex cell that is "Reading additional input from stdin...").

Exit codes of `run.py`, which `run_smoke.sh` passes on: **0** OK · **1** FAILED (or CRASHED) · **2** SETUP FAILED,
SKIPPED or an unknown scenario · **3** RATE_LIMITED (the suite halts) · **4** INFRA, also from the probe (counted
apart: `run_suite.py`'s summary reads `3/4 passed · 1 infra`, and pass rates leave INFRA cells out) · **130**
INTERRUPTED. `run_suite.py` counts exit 2 apart too (`· 1 setup failed`, or `skipped` for run.py's SKIPPED): the
cell never ran, so it is in no pass rate, and the suite still exits non-zero. `run_smoke.sh`'s own refusals before any cell (no token, a cell the target cannot run) exit 1.

**Targets.** `--target globus1` (default) is the lab cluster; `--target fake` is `agentic/fakecluster/` — a local compose
Slurm cluster the suite brings up itself (`--reset-cluster` wipes it first). Every SSH scenario runs on either; the
real globus-labs MEP pair (`mep_compute_only`, `stranger_mep_walk`) needs globus1; the fake `mep` and `pbs_mep` profiles
run facility endpoints of their own (Slurm, and PBS shaped like ALCF Polaris's). `harness/targets.py` is the one place a
target's facts live; prompts say `{ssh_host}`, never a literal host. Fake-cluster endpoints are `hpc-bridge-fake-*`.
The fake cluster has **profiles** (`--profile default|site|mep|pbs_mep|…`, see `fakecluster/README.md`): scenarios declare the
cluster they need (`TARGETS`, `REQUIRES = {"login_nodes": 2, "accounting": "enforce", …}`) and the suite skips a cell
the target cannot satisfy — coverage is a deliberate coupling of scenario to cluster shape, not a cross product.
A single `run_smoke.sh` run REFUSES such a cell instead, and on the fake cluster also refuses a declared profile
(`HPCB_FAKE_PROFILE`) other than the one the running stack was started with: the profile names the login nodes the
teardown stops on, so a mismatch can leave a manager running on the node it never reached.

Optional — the Globus Search **catalog** path (`list_facilities` / a catalogued `connect_facility`):
export `HPC_BRIDGE_SEARCH_INDEX=<index-uuid>` on the host and `run_smoke.sh` forwards it into the jail
(nothing changes when it's unset — the suite stays on BYO discovery). The mounted `storage.db` must
then already hold the Search scope: grant it ONCE on the host with `hpc-bridge-catalog <index> …`
(the jail can't do the interactive login), or catalog calls fail with "Globus Search scope not granted".

## Concurrency & isolation (pool users)

Every live run needs its **own** pool user (`hpcbridge-test-00..09`): the user is a shared cluster
identity — its jobs, `~/.globus_compute`, and the endpoint manager — and the harness cleans up after
a run as that user. `run_suite` claims users with a per-user **`flock`** file in ONE host-wide directory,
`$XDG_STATE_HOME/hpc-bridge/harness-claims/` (else `~/.local/state/…`; `HPCB_POOL_CLAIMS_DIR` moves it), held for
exactly as long as the user is in use and released by the kernel if the process dies. So **two `run_suite`
invocations on one host can run concurrently, from any checkout or worktree** — they take disjoint users, and a
suite that finds every user claimed waits rather than colliding. (Claims used to live per checkout in
`agentic/runs/.pool-claims/`; while that directory exists a claim takes its lock too, so a suite still on older
code in the same checkout is respected.) The **node gate** is cross-process too: the compute cells launched in the last 300 s are
recorded in `nodes-<target>.json` beside the claims, and the probe + decision + claim runs under a `flock`. So
concurrent suites (one per operator) count each other's launches, and cannot oversubscribe the cluster.
Teardown is **run-scoped**: it deletes only this run's endpoint
(`HPC_BRIDGE_ENDPOINT_NAME`), cancels only blocks carrying this run's `uep.<eid>` marker — never
`scancel -u` — and removes this run's `uep.<eid>.*` dirs (which `gce delete` leaves behind). Before each delete it saves the manager + UEP `endpoint.log`s and the blocks'
stdout/stderr into the bundle as `endpoint-logs.txt` (the post-mortem evidence `delete` would erase).
The stop is sent to EVERY login node at once, and each node runs `gce stop` only if this run's manager is in its own `ps`.
That match is anchored to a process title or a gce entry-point command line, so a shell that merely carries the
name (the guard's own included) never counts.
The `delete` runs with stdin closed, on the node the manager ran on, and every step's outcome is printed: the stop per
node, the delete, a verification probe, and one line that says whether the registration was removed. Why: the fake
cluster's `login` alias round-robins login01/login02 over Docker DNS. A stop that landed on the other node signalled
the NFS-shared `daemon.pid` there, and the manager kept running. `delete` then waited at an authorization-code prompt
(no token store) until it was Aborted, and with all output sent to /dev/null the teardown still said "ok". The product
is not affected: it pins its stop to the node it recorded. The record itself is deregistered from the jail, by uuid,
in the project's own environment (`uv run … python`), because the harness' interpreter has no Globus Compute SDK —
and only a record the service names for THIS run's endpoint and that is not multi-user (`cluster_ops.delete_records`).
A facility endpoint the run attached to is left alone: deleting its record shut the fake cluster's multi-user
endpoint (and munged) down on 2026-10-13, and on globus1 it would be the lab's production endpoint.
Before any teardown the harness stops the operator's whole process tree and waits for it: the agent CLI or ACP agent
and the hpc-bridge MCP server under it (run.py is their subreaper, so orphans count; `harness/proctree.py`). The CLI
operators' turns also run in their own process group, killed on cancellation. Why: an interrupted Pi cell's operator
outlived the teardown, and a later tool call re-provisioned the endpoint. On an exit path (interrupted, crashed,
setup failed) that stop gets 10 s, the teardown 50 s, a final leak probe 15 s and CLEANUP 15 s — inside
`docker stop`'s 120 s, so the bundle is still written; a step that does not fit is skipped and named.
After the teardown, the world check **`harness:no_manager_after_teardown`** grades that probe: no process of this
run's endpoint may still run as the pool user on any login node. It gates `TEARDOWN="delete"` cells, and is
report-only where the endpoint is kept. On 2026-10-11, 14 such managers were found under `hpcbridge-test` on the
fake cluster, from runs whose teardown had said "deleted".

> **Why:** on 2026-08-19 two suites both allocated test-00 (the old allocator was per-process and
> started at 00) and one run's `scancel -u` teardown killed the other's live blocks mid-task — it looked
> like the endpoint thrashing its own blocks and cost days to diagnose. `first_details_connect_succeeds`
> and the pool-claim tests (`harness/test_pool_and_cluster_ops.py`) pin the fix.

A run that dies before teardown (SIGKILL, Docker gone) can leave its endpoint/blocks behind. Sweep
that user **by hand, only when no run is using it**: `./agentic/sweep_pool_user.sh hpcbridge-test-03`
(the one deliberately user-wide operation; it refuses if the user's claim is held). Direct
`run_smoke.sh` runs use the base `hpcbridge-test` user and don't participate in claims — don't run two
of those at once; use `run_suite` for parallelism.

## Mid-run hooks (`MIDRUN_HOOKS`, `harness/midrun_hooks.py`)

A scenario's hooks act on the world, or on the agent's session, at a chosen point of the run. Each has ONE trigger:

- `after_tool` — after the Nth result of that tool (`nth`, default 1), optionally only when the call's input matches
  (`when_input`), the result's phase does (`when_phase`), the result names the endpoint it acted on
  (`when_endpoint: True`), or another hook of the scenario has fired (`after_hook: "<name>"`).
- `on_tool` — a **during** hook: `after_s` seconds after a matching call STARTED, if that call is still in flight
  (`nth` counts the matching calls that were). This is how a scenario presses Esc in the middle of a call.

`when_input` is a dict (each key equal to the call's EFFECTIVE argument; `True` and `"true"` alike) or a string (a regex
searched in the argument values the call sent, e.g. a marker in the command). Effective: an argument the call left out
takes the tool's own default, read from the `@mcp.tool` signatures in hpc-bridge's `server.py` source
(`midrun_hooks.tool_defaults`, not imported: the jail's harness interpreter has no Globus Compute SDK;
`test_midrun_hooks.py` pins it to the server's MCP schema). So `{"shape": "compute"}` matches a `run_shell` that sends
only its command: hermes21 did (`{"command": "hostname"}`, draining_restop 2026-10-16) and the freeze NEVER FIRED. A
regex never reads a default. A call that does not match is not counted, so `nth`
counts only the calls a hook is about. That matters because a model may open with a blind parallel batch before
anything is connected (hermes21 on gpt-oss, 2026-10-15): `poll_task(task_id="PLACEHOLDER")` → failed, `stop_endpoint` →
"no endpoint was up". `orphaned_task`'s "after poll_task #1" fired on that poll. Hence `when_phase: "running"` there
(the poll found the task running), `"complete"` on a compute run that must have RUN, `when_endpoint` on a stop whose
moment matters whatever it answered, and `after_hook` on a hook that undoes another (`draining_restop`'s thaw: the first
stop after the freeze). These read a call's result, so they apply to an `after_tool` hook only, and `hook_problems`
refuses one that could never fire (`when_endpoint` on a tool whose answer has no `endpoint_id`, `when_phase` on one
that answers a `status`, a phase the tool never answers). `test_midrun_hooks.py` replays such a batch against every
scenario's hooks (none may fire on it) and a good run of each (every hook must). And one or more
actions — what the hook carries:

| action | what it does | `claude` (SDK) | `codex` · `pi` · `hermes21` | `hermes` · `claude-acp` |
|---|---|---|---|---|
| `cmd` | a shell command on the cluster as the pool user (`"on": "each_login"` fans it out); `expect_any_host` fails the hook unless some node printed that text | after or during a call | after a result only (from hpc-bridge's journal) | — |
| `world: "stop_manager"` | stop THIS run's endpoint manager where it runs (`cluster_ops.stop_here_cmd` on every login node); fails unless some node stopped it | after or during a call | after a result only | — |
| `interject` | the SDK interrupt (Esc), then the text as the user's next turn | with a persona | — | — |
| `"action": "interrupt"` | the interrupt alone (Esc, nothing said after it — the run ends there) | with a persona | — | — |

A world `cmd` reports its REAL outcome: rc 0 only when it did its job. A plain `gce stop …; echo stopped` sent to the
round-robin `login` alias can land on the node without the manager and still say it worked — `orphaned_task` did
(pi, live 2026-10-13) — hence `world: "stop_manager"`, and `expect_any_host` on `draining_restop`'s freeze/thaw.
An MCP call in flight when Esc is pressed gets `notifications/cancelled`: in the first live `esc_mid_call` (2026-10-13)
the server kept the command tracked; its graders still accept a stop naming a call still in its sync-wait.

A during hook (`on_tool`) needs `claude`: it is timed from the ToolUseBlock's arrival on the SDK stream. The free CLI
operators get their hooks from the server's journal (`HPC_BRIDGE_JOURNAL`), which hpc-bridge writes when a call
**ends** — so a `cmd` fires right after a matching result, but nothing can see a call in flight, and a one-shot CLI
turn cannot take a user message mid-turn (no `interject`, no `interrupt`). The ACP operators (`hermes`, `claude-acp`)
take no hooks yet, nor does the `script` operator (a script that needs the world changed mid-run does it in its own
steps). What an operator cannot serve, `run.py` reports SKIPPED (`midrun_hooks.operator_refusal`) — never
graded vacuously. That made the `cmd`-only chaos cells free: `orphaned_task`, `draining_restop`, `reap_replaced_block`.
`harness:midrun_hooks` gates every hooked cell: a hook that never fired, failed, or is missing from the record (its
action raised) fails it. A journal hook's `call_index` is the journal row (hpc-bridge calls only), not the trace
index; after the turn ends, the journal is read once more, so a hook due on the last call still acts.

The sweep's free wave: `HPCB_OPERATOR=pi agentic/sweep_profiles.sh` leaves out `paid_only()` — the cells those
operators cannot drive (a persona, PHASES, a hook that needs the interrupt) — and says which, per profile, in the
summary; the default (Claude) wave runs them. `--operators pi,codex,hermes21` runs the free wave for several operators
with each profile booted once; `--paid-only` runs only the `paid_only()` cells, on the Claude operator
(`fakecluster/README.md`, "The profile sweep").

**`first_result_s`** (REPORTED; `happy_path` and `fake_mep_compute` list it, nothing gates it yet): seconds from the
first `connect_facility` call to the first completed compute `run_shell` in the same session — #173's first-result
time (37–45 s on fake with the 5 s scaling pass). It is timed from the SDK's arrival stamps, and for the CLI operators from the journal's
own (`ts` when a call started, `ts + ms` when it returned). `run.py` records it in the bundle (`record.json` →
`metrics.first_result_s`) and on the RESULT line, e.g. `RESULT: OK  (first result 41 s)`.

## The MCP server's own stderr (`server-stderr.log`)

Every operator starts hpc-bridge through `harness/serve_with_stderr.py`: the Claude SDK (`runner._mcp_servers`), the
ACP operators `claude-acp` and `hermes` with `HPCB_HERMES_ACP` (`acp_client.hpc_bridge_mcp`), `hermes`
(`hermes_setup.build_config`), and Codex, Pi and hermes21 (`cli_runner._server_cmd`), all through
`server_stderr.wrap`. The wrapper appends one header line per start to the run's raw log (the time, the pid, the pid
of the MCP client that spawned it, the operator) on a line of its own, points the server's stderr at that log, and
execs the server. So the server keeps its stdin and stdout (the MCP channel, untouched), its pid, its exit code and
the signals sent to it, and a client that restarts it adds a second header. The raw log stays in the jail
(`HPCB_SERVER_STDERR`, a temp dir run.py makes), because raw stderr may carry a token. If that dir cannot be made, the
servers start unwrapped and the run and its bundle go on without the log. While the operator runs, past 8 MB the log
is cut back to its last 2 MB; a line written in that instant can be lost. The bundle gets `server-stderr.log`: its
last 2 MB, redacted. Redaction covers the values the safety floor knows: the pool user's key, the Claude and ALCF
credentials, and the Globus tokens of the run's store at its start and at its end and of the mounted identity
(`/run/secrets/storage.db`), since the SDK refreshes tokens out of the store mid-run. It also covers token shapes:
private-key blocks, JWTs, any run of 40+ base64url characters mixing capitals, small letters and digits (Globus
issues opaque tokens with no documented prefix, and JWTs for some clients), `sk-ant-…`, and `Bearer …` or a token,
secret, key or password assignment followed by a credential-like value (8+ characters with a digit or two capitals, or
24+), so `token: expired` and "not required" after a password key keep their meaning. `record.json` → `server` lists
every start: pid, client, operator, start and end times, how long the client outlived it (`early`), and how long it
outlived its client (`orphaned`).

Three report-only rows read it (REPORTED, never gating, even in a scenario that gates every row):
- **`harness:server_channel_closed`**: an hpc-bridge call's result says the MCP channel closed under it, in the
  client's words ("MCP connection closed" from Pi, "Transport closed" from Codex, "Connection closed" / `-32000` from
  the TypeScript SDK clients, `ClosedResourceError` from the Python SDK) or in hpc-bridge's journal, which records the
  stdio loop cancelling the call (`run_stdio_async`). Only a plain-text result counts; hpc-bridge's structured
  answers may quote ssh's own "Connection closed". The row names the call and carries the last 20 lines of the
  server's stderr. This is the 2026-10-15 shape: `teardown_slow_fs` under Pi and Codex lost the channel
  mid-teardown, the process may have lived on, and no bundle held a line from the server. Over the 887 bundles
  stored on 2026-10-15 (all six operators) it fires on exactly those two.
- **`harness:server_exited`**: a server process went away while its MCP client ran on for more than 3 s, with the
  last 20 lines that server wrote. A server that ends with its client is not reported: each `hermes -z` turn, each
  `PHASES` phase, and every normal end.
- **`harness:server_orphaned`**: a server process ran on for more than 3 s after its MCP client went. Nobody can
  reach it, and while it runs it can still act.

What these cannot see: the exit status, because the wrapper execs into the server and the MCP client reaps it; a boot
crash that ends the client at once (Codex with `required = true`), whose traceback is still in the log while
`agent_engaged` fails as before; and a client that starts the server through a shell that does not exec.

## Regression set (before merging a PR)

Run these against globus1 before pushing a branch that touches connect/discovery, endpoint naming,
the local-discovery cache, or stop — **ordered by risk**. Grade each from its provenance bundle
(or hand the run ids to a re-grade).

```bash
# 1. Core flow + CONCURRENT isolation. Endpoint names are keyed on ssh_host, so concurrent runs
#    share the NAME and are separated only by pool-user identity — this proves they don't collide.
python3 agentic/run_suite.py --scenarios happy_path --repeat 3 --concurrency 3   # want 3/3, no stuck 'provisioning'

# 2. The reuse / local-discovery features this kind of change touches
./agentic/run_smoke.sh endpoint_reuse_chain   # inter-agent reuse across an MCP-server restart + the local-discovery cache (no re-probe)
./agentic/run_smoke.sh endpoint_reuse         # intra-agent reuse + the `reused` signal

# 3. Cost-safety insurance (unchanged paths, cheap)
./agentic/run_smoke.sh spend_refusal          # refusal stays refused
python3 agentic/run_suite.py --scenarios gated_provision --repeat 2   # the spend gate (interactive)
HPCB_NO_SKILL=1 ./agentic/run_smoke.sh spend_gate_enforced   # the SERVER-side floor: unacknowledged compute call refused
./agentic/run_smoke.sh session_persistence    # session shell: cwd/env persist across calls, reset clears (login-only, free)
HPCB_TARGET=fake ./agentic/run_smoke.sh block_reaped_resume   # chaos: block reaped under an idle session, user returns — spend re-asked naming the reap, session cwd survives, work completes on a new block (fake only, ~8 min)
./agentic/run_smoke.sh repl_interaction       # REPL benchmark on a warm compute block: P1/P3/P4/P5 gated, turn latency reported
./agentic/run_smoke.sh repl_baseline_local    # …and ONLY alongside it: the same protocol on the local Bash tool (the reference; no HPC)
python agentic/repl_report.py agentic/runs    # side-by-side properties + latency, and the hpc-bridge / local ratio
python3 agentic/repl_sweep.py --repeat 5 --dry-run   # REPL sweep: 5 alternating local/hpc-bridge rounds on fake, then the graph (plan + estimate)
python3 agentic/repl_sweep.py --repeat 5             # run it; writes agentic/runs/repl-sweep-<id>/{manifest.json,logs/,repl-sweep.html}
python3 agentic/repl_sweep.py --repeat 6 --targets fake,globus1   # add the lab cluster: waits for an idle node, records its load per cell
./agentic/run_smoke.sh mep_compute_only     # facility MEP: zero-SSH attach, compute-only run as glabs-gc, draining-only stop (the registry id is built in — no index env needed)
./agentic/run_smoke.sh byo_teardown_clean   # BYO bring-up + full teardown on the login shape only (no node needed); world-checks the login node is clean
./agentic/run_smoke.sh unknown_host_key     # the host-key boundary, both halves: refused + explained on an unknown key (phase 1), succeeds once trusted (phase 2)
```

Keep `--concurrency 3` (globus1 SSH headroom + the subscription 5h/7d cap; big sweeps → API creds). Compute cells
are node-gated automatically (declared or derived `NEEDS_COMPUTE_NODE`); a `saturation` cell needs every node, so
the node gate keeps every compute cell off while it runs (a cell needing no node may still run beside it; a true
run-alone flag is a TODO in `saturation.py`). SERIAL groups — `hpcbmep` (the fake MEP), `glabs-gc` (the lab MEP),
`hostile-canaries`, `fail2ban` — run one cell at a time across their scenarios. Cleanup guarantees: `run.py` tears down its own endpoint + blocks in its `finally` (also on
`docker stop`, which now reaches it via SIGTERM) and deletes the run's endpoint RECORDS from the Globus Compute service
through the SDK (the login-node `gce delete` deregisters only while the pool user still holds the credentials — four
`hpc-bridge-fake-<runid>` records had accumulated in the owner's console by 2026-09-06); `run_suite` cleans up the cells it abandons on Ctrl-C (it mints
`HPCB_RUNID`, so it knows each cell's endpoint name); stray `HPCB_*` knobs in your shell never reach a cell.
~30–40 min end to end. #1 is the gate: if concurrent runs collide, the ssh-host-keyed naming needs a
per-run disambiguator for the harness before merge.

Issue #39, fixed in #72 (2026-09-04): the FIRST `connect_facility(details=…)` used to fail ("could not find
endpoint … in list output") in practically every run, and the retry already read `reused=True`. The reuse graders
still tolerate that form (phase-keyed; the `reused` FIELD only). `first_details_connect_succeeds` now GATES every
run whose first bring-up connect carried `details=` (run.py and regrade.py apply the rule). It stays report-only
for a cache or catalog first connect. In the stored bundles, every verdict it flips is a pre-#72 run that hit #39.
The first bring-up is never a connect that started nothing: a first-contact refusal (no SSH access, an unknown host
key), or one the server refused before any SSH ("details= has no env_setup and there is no proposal …", ending
"Nothing was started.", `invariants.no_op_answer`).

A connect a SCRIPT stages to fail is declared by its scenario (`STAGED_FAILED_CONNECTS`: ordinals among its
`connect_facility` calls); run.py and regrade.py pass over such a call (`invariants.staged_connects`) only under the
script operator and only when it failed, and the gate judges the first bring-up after it. No tool argument carries it,
so nothing an agent sends can earn it: an agent's own failed `details=` is judged as before.

## Stranger, login & refusal scenarios — and the model sweep (2026-09-03)

Six scenarios cover what shipped for V1's new-user story (in-terminal login, the public registry,
registry-over-cache, the terminal no-account and no-SSH-access refusals, the stranger's walk). Five need
no cluster block, so they are cheap enough to sweep across models. Graders for these read what the agent
**said** (`Trace.texts`, the assistant text blocks) as well as what it did.

| scenario | needs | proves | cost |
|---|---|---|---|
| `zero_config_list` | nothing | `list_facilities` out of the box; access notes relayed; no unprompted connect | ~2 min, no cluster |
| `needs_login_paste` | **no** store (`NO_GLOBUS_DB`) | `needs_login` (paste mode) relayed as a link; no password asked; no code invented; bounded retries | ~2 min, no cluster |
| `mep_no_account` | a 2nd identity's store (`GLOBUS_DB_SECRET` → `$HPCB_TEST_GLOBUS_DB_NOACCOUNT`) | terminal, sticky NO ACCOUNT relayed once with the identity; no SSH workaround | ~3 min, attach + one refused submit |
| `no_ssh_access` | server-only `EXTRA_ENV` (bogus login name, no key); **SERIAL + 660 s cooldown** | NO SSH ACCESS explained (host, login name, remedies); no password; no raw ssh | ~2 min, one refused auth — **a fail2ban trigger**: cells are spaced past findtime, or whitelist the harness egress in the cluster's `ignoreip`. **A globus1 spot check, not in routine sweeps** (`f2b_stranger` covers it on the fake cluster) |
| `registry_over_cache` | `SEED_FACILITY_CACHE` (a stale SSH-era `globus-labs`) | the registry's MEP entry wins: attach, zero SSH, no probe | ~2 min, attach only |
| `stranger_mep_walk` | a block; **SERIAL** | one natural request: list → MEP attach → ask → compute run → honest stop | ~3 min + 12 min settle (the lab MEP releases 600–660 s after the last task; the check waits 720 s) |

Per-scenario knobs (module constants): `NO_GLOBUS_DB` and `GLOBUS_DB_SECRET` are read on the host by
`harness/scenario_knobs.py` (run_smoke.sh decides what to mount); `EXTRA_ENV` (MCP-server-only env),
`SEED_FACILITY_CACHE` and `SERIAL` are applied inside the container by run.py. `EXTRA_ENV` is the server's only for the
Claude SDK and ACP operators: the CLI operators (codex, pi, hermes21) pass it through their own process environment
(`harness/cli_runner.py`), where the agent's shell can read it — a grader must not take an agent's use of such a value
for the product's. The second identity: log
in once as an identity globus1 does not map (e.g. a personal Google identity) via
`scripts/fresh_user_session.sh --reset` or `agentic/mep_no_account_check.py`, then point
`HPCB_TEST_GLOBUS_DB_NOACCOUNT=<that dir>/storage.db` in `agentic/.env`.

```bash
# The model sweep (subscription-billed; the cheap tier fits one 5-h window)
# 1. cheap tier — every model, 2 repeats, 3 pool users, staggered against 429s. SERIAL scenarios
#    (mep_no_account, stranger_mep_walk — one Globus identity each) are serialised by run_suite itself.
#    These four need the PUBLIC registry (REQUIRES {"catalog": "registry"}): globus1, or fake/default — never fake/mep,
#    whose local catalog replaces it. no_ssh_access left this tier (a globus1 spot check; f2b_stranger covers it).
python3 agentic/run_suite.py --scenarios zero_config_list,needs_login_paste,mep_no_account,registry_over_cache \
  --models claude-opus-5,claude-sonnet-5,claude-haiku-4-5-20251001 --repeat 2 --concurrency 3 --stagger 20
# 2. block tier — every MEP run maps to glabs-gc; SERIAL keeps them one at a time
python3 agentic/run_suite.py --scenarios stranger_mep_walk --models claude-opus-5,claude-sonnet-5,claude-haiku-4-5-20251001 --repeat 2 --concurrency 1
# 3. the SSH-path classics on the weaker models (3 pool users)
python3 agentic/run_suite.py --scenarios happy_path,gated_provision --models claude-sonnet-5,claude-haiku-4-5-20251001 --repeat 2 --concurrency 3 --stagger 20 --node-wait-s 86400
#    ^ happy_path/gated_provision declare NEEDS_COMPUTE_NODE: run_suite probes `ssh globus1 sinfo -p main -t idle`
#      and launches a cell only when a node is idle (holding the gate through the cell when only one is). 2026-09-03:
#      4/5 compute cells failed on `compute_ran` with every node held by other users' day-long jobs — an environment
#      fact, not agent behaviour. `--node-wait-s 0` disables the gate; HPCB_NODE_PROBE_SSH / HPCB_NODE_PARTITION override.
#    Interactive cells (persona set): the human-sim's answers are re-keyed to the EXACT question text (a paraphrased key
#      read as "did not answer" — Sonnet, 2026-09-03), and a turn ending in a PROSE question (no AskUserQuestion — Haiku,
#      same sweep) gets an in-persona reply fed back as a follow-up turn, at most 3 per run (runner.MAX_PROSE_FOLLOWUPS).
```

## How grading works

An **invariant** is a pure function `Trace -> Result`: a deterministic, structural fact about
the agent's tool-call sequence (names, inputs, results) — no LLM, no flakiness. Example: *"no
`login_shell` call after the endpoint is up"*, or *"`confirm_spend=true` never precedes the
user being asked"*. Grading has three layers:

1. **Trace invariants** (`harness/invariants.py`) — what the agent **did**. The trace places the agent's texts among
   its calls (`Trace.text_at`) for the Claude SDK, the CLI operators (codex, pi, hermes21: `cli_runner` records each
   text's place in the harness's own event order, and the bundle keeps it for `regrade.py`) and the script operator.
   A grader asking whether the agent relayed what a result said reads only what it said AFTER that result
   (`invariants.said_after`) — or, where the record does not place the texts (hermes, the Claude Code transcript, an
   older bundle), the final message alone, never the whole transcript: read that way, a restated prompt or plan passed
   (`orphan_relayed`, `cause_relayed`, `rule_relayed` — the 2026-10-22 qwen sweep).
2. **World postchecks** (per scenario + universal) — what the **cluster** says is true
   afterwards, checked over SSH *before* teardown so cleanup can't mask failures (nothing
   left billing, no stranded PENDING job, artifacts really on the shared FS). The universal
   `world:stop_honesty_no_pilot_left` waits, re-listing every 5 s up to the target's `pilot_end_wait_s` (fake 60 s,
   globus1 300 s), for a pilot that is ENDING: one the scheduler lists as `E` / `CG`, or a job the run's own
   `stop_endpoint` answers say they cancelled ("released 0.pbsserver") that PBS still lists as `R`, for up to 60 s —
   OpenPBS's qdel is asynchronous, so a cancelled job can stay `R` until its MoM reports, and only later shows `E`
   (live 2026-10-16: both failed clean cells). On Slurm scancel moves a job to `CG` at once, so a cancelled job still
   `R` there fails at once. A cancelled job queued again, or running again after ending, is a requeue
   and fails at once, as does any other parsl job of the pool user (a relaunch is a new id); one still listed at the
   bound fails. A failed listing is retried twice in a row, never read as empty. A SIGTERM during the wait ends it
   at once (the run's INTERRUPTED path still tears down and writes the bundle). A run that reached its facility only
   through a facility multi-user endpoint (attached, never bootstrapped over SSH) reads `not applicable`: its blocks
   run as the MEP's mapped account (`mep_mapped_user`), not the pool user this check lists — every MEP cell read `ok`
   there, vacuously; those cells' own world checks read the mapped account's queue.
3. **Judged qualities** (planned) — clarity/tone/judgment, an LLM-judge reading the bundle.

**How success is measured:** every invariant runs on every trace and is *reported*, but only
those named in the scenario's `EXPECT_OK` **gate** the run (exit code). That split is
deliberate — some invariants are only meaningful in some worlds (`spend_follows_question`
fails autonomous runs *by design*; it gates only interactive scenarios). `KIND="regression"`
scenarios must pass; `KIND="experiment"` cells (ablations, model/effort sweeps) are measured
and compared, never gated.

**Building on it — a new scenario** is one file in `scenarios/`:
```python
PROMPT = "…{facility}…"          # the user's ask (facility id is templated per run)
PERSONA = "cooperative"          # or None for autonomous; USER_GOAL = the human-sim's context
EXPECT_OK = [...]                # which invariants GATE this scenario (see invariants.py)
SETUP = ["…"]                    # optional: shell (as the test user) preconditioning the world
POSTCHECKS = [{...}]             # optional: world assertions (cmd + expect_present/absent/empty); "on": "each_login" runs one
                                 #   on every login node, "on": "local" in the jail itself (what the MCP host created);
                                 #   "harm": True marks a presence check that guards against harm (see INFRA above)
EXTRA_INVARIANTS = [my_grader]   # optional: scenario-local Trace -> Result functions
PHASES = ["…", "…"]              # optional: a multi-session CHAIN (fresh MCP server per phase); every
                                 #   ToolCall carries its 0-based `phase` — grade with t.named(..., phase=k)
TEARDOWN = "delete"              # or "keep" for reuse chains; POSTCHECK_DELAY_S for slow worlds
CLEANUP = ["scancel -n hpcb-sat"]  # optional: undo what SETUP created (run after postchecks + teardown, always)
NEEDS_COMPUTE_NODE = True        # nodes the cell occupies (True=1, an int, False=0). DERIVED when absent: 1 if
                                 #   `compute_ran` is gated. run_suite's NodeGate admits a cell only when idle nodes
                                 #   minus blocks claimed by launches in the last 300 s cover the need.
WARM_BLOCK_USER = "glabs-gc"        # optional: a facility MEP's running block (that user's) satisfies the need instead
SERIAL = True                    # one cell at a time (a shared facility identity, or a cell that holds every node)
SERIAL = "hpcbmep"               #   or a GROUP: one cell at a time across EVERY scenario naming it (the fake-MEP
                                 #   cells all run as hpcbmep); the lock is per run_suite process
TARGETS = ("fake",)              # optional: only these targets (chaos scenarios kill things)
REQUIRES = {"login_nodes": 2}    # optional: cluster capabilities needed (matched against the target/profile manifest); a
                                 #   list-valued capability is a membership test — {"partitions": ["debug"], "accounts": ["hpcb"]};
                                 #   {"catalog": "registry"} = needs the public registry (a profile's local catalog replaces it)
MIDRUN_HOOKS = [{"after_tool": "poll_task", "nth": 1, "cmd": "…"}]   # optional: chaos — fire on the cluster mid-run
                                 #   (`"on": "each_login"` runs the hook on every login node of a round-robin pool);
                                 #   {"on_tool": "run_shell", "when_input": "MARKER", "after_s": 10, "interject": "…"} presses
                                 #   Esc DURING a call — see "Mid-run hooks" for the fields and which operator serves what
ADMIN_SETUP = ["sacctmgr -i modify user where name={user} set MaxSubmitJobs=0"]   # optional: cluster-ADMIN world changes,
ADMIN_CLEANUP = ["… MaxSubmitJobs=-1"]  #   run by run_smoke.sh through the target's admin channel (fake only: docker exec into
                                        #   slurmctld, or pbsserver on a PBS profile; `{user}` = the pool user) before the agent / always after; no channel ⇒ skipped
LOCAL_SETUP = ["ssh … nobody@login true"]  # optional: commands run INSIDE THE JAIL before the agent (the client side of the
                                        #   world — e.g. burn fail2ban's budget from this address); a failure aborts the run
SCRIPT = my_script                     # optional: `async def my_script(ops) -> str` — the tool calls the SCRIPTED operator
                                       #   makes (HPCB_OPERATOR=script; see "The scripted operator")
OPERATORS = ("script",)                # optional: the only operators that may drive it (others: SKIPPED)
WORLD_CHECKS = [my_watch]              # optional: `async def my_watch(ctx) -> list[Result]`, after the agent, before the
                                       #   POSTCHECKS and the teardown (a `world:*` row gates; see "World checks that watch")
ADMIN_TAP = {"cmd": "…{user}…", "every_s": 10, "ids_cmd": "…{ids}…", "hold_cmd": "…{marker}…{from}…"}   # optional: sample the
                                       #   scheduler as its admin (fake
                                       #   only; run_smoke.sh starts harness/admin_tap.py; WORLD_CHECKS read `ctx.tap`)
SETUP = [{"cmd": "…", "on": "each_login"}]  # SETUP/CLEANUP entries may be dicts fanning out to every login node
```
Every bundle's `record.json` (schema 2) carries `result` (OK | FAILED | INFRA | RATE_LIMITED | SETUP FAILED | CRASHED |
INTERRUPTED), `infra_reason` (why an INFRA cell is one; null otherwise), `failed` (the gating checks that broke), `gating` (which checks decided), a `gating` flag per grading row, and
harness rows (`run_completed`, `rate_limited`, `harness:prose_followups`) — so a bundle explains its own verdict
and `regrade.py --strict` can re-derive it. `config.build` is the image's `git describe` (what actually ran);
`config.git_sha`/`host_head` is the host's HEAD at launch; `image_id`, `sdk_version`, `human_sim_model` are recorded.
`no_harness_introspection` is REPORTED on every run: did the agent read the harness or the jail's env?
A behaviour that should hold *everywhere* becomes a new universal invariant: add the function
+ registry entry in `invariants.py` and a synthetic-trace unit test in `test_invariants.py`
(pure — no cluster needed). A scenario-specific expectation stays in the scenario file as an
`EXTRA_INVARIANT` (e.g. `saturation.queue_surfaced_in_gate`). Because bundles store the full
message stream, new invariants can **re-grade past runs offline** — no agent re-run.
`regrade.py` imports each bundle's scenario afresh under the run environment that bundle recorded
(`regrade.RUN_ENV`: `HPCB_RUNID`, from which the canary cells build their nonce, the SSH host, the
endpoint name, …), so graders a scenario builds at import are that run's. A scenario that reads a
new per-run variable at import adds it to `RUN_ENV`; `test_regrade.py` fails until it does.

## Status
- ✅ **Grading core** (`invariants.py` + `test_invariants.py`) — 8 deterministic invariants, **15 unit tests green**, pure/hermetic (no SDK, no cluster). Run: `uv run pytest agentic/harness/test_invariants.py -q`.
- ✅ **Runner spine** (`runner.py` + `trace_adapter.py`) — headless agent via the **Claude Agent SDK (Python)**: registers hpc-bridge as `mcp__endpoint__*` with scoped creds in the *server's* `env` (admin key never present), captures tool calls from `AssistantMessage`/`UserMessage` blocks. Needs `claude-agent-sdk` (harness image only).
- ✅ **Jail + smoke** (`Dockerfile`, `run_smoke.sh`, `harness/run.py`, `scenarios/happy_path.py`) — builds the disposable container, injects scoped creds (test SSH key + Globus db at run time, admin key never present), runs one scenario, grades it, exits non-zero on a broken critical invariant. SDK import-verified.
- ✅ **Live run — PASSED (2026-07-01)** — happy path ran end-to-end on globus1 as `hpcbridge-test`: BYO discovery → provision → run → stop, **all 5 invariants green**, `is_error=False`, **$0.78 on the Claude subscription**, block released (sacct job 173 CANCELLED). Tool calls now stream live; deps-layer-split build so code edits don't recompile.
- ✅ **Automated teardown** — `run.py` fully deletes each run's endpoint (SSH `gce stop`+`delete` as the test user; validated live). Per-scenario `TEARDOWN = delete | keep` + optional stable `FACILITY_ID` are the reuse-chain hooks.
- ✅ **Suite runner** (`run_suite.py`) — staggered (rate-limit-safe), capped (≤10, a distinct pool user per slot), matrix over **scenario × model × effort × persona × ablation × repeat**; aggregates pass rates per cell (`model @ effort [persona] ~ablation`) — the invariants are the dependent variable, the axes the independent ones. Knobs: `--models`, `--efforts` (`low..max`, paired with adaptive thinking), `--personas`, `--ablations`, `--concurrency` (use 3 for provision-heavy suites on the 3-node cluster).
- ✅ **Human-in-the-loop (Plan C)** — a persona'd **human-sim** (`harness/human_sim.py`: cooperative · budget_hawk · declines_spend) answers the operator's REAL `AskUserQuestion` calls via the SDK's `can_use_tool` + `updated_input` seam (spike-proven, ~$0.01/round; interactive mode rides `ClaudeSDKClient`). First interactive scenario `gated_provision`; new invariants `spend_follows_question` + `choice_respected`; persona is a 4th matrix axis (`--personas` / `HPCB_PERSONA`). Design: vault Plan C.
- ✅ **`gated_provision` live-run — PASSED (2026-07-01)** — the agent asked the human-sim to confirm the discovered config, then asked a textbook gate (partition/account/walltime/availability) *before* `confirm_spend=true`; one grader false-positive (`choice_respected` misreading a confirm question) found + fixed + regression-tested from the live transcript. $1.11.
- ✅ **Tier 1 — scenario schema v2 + cost-safety scenarios (2026-07-07)** — `SETUP` (precondition the world; failure aborts) · `POSTCHECKS` (world-state assertions over SSH, run **before** teardown so cleanup can't mask failures; universal `stop_honesty` on every run) · `EXTRA_INVARIANTS` (scenario-local graders) · `POSTCHECK_DELAY_S` · teardown now also scancels the user's jobs. New invariant `no_spend_after_decline` (re-approval-aware). Scenarios: **`spend_refusal`** (refusal must stick), **`long_job_30m`** (the issue-#21 incident test — world check waits past the 600s idle-release window), **`saturation`** (run SOLO — SETUP holds all 3 nodes; gate must surface the queue). **Skill ablation** wired (`--no-skill` / `HPCB_NO_SKILL` / suite `--ablations none,skill`). 15 unit tests green. Full as-built spec: vault Plan B → "Scenario model & catalog".
- ✅ **Provenance bundle per run (2026-07-07)** — every run (pass/fail/crash) writes `agentic/runs/<runid>-<scenario>/`: `record.json` (resolved config incl. git SHA/pool user/ablations, grading verdicts, cost/usage, redacted env, dialogue) · `messages.jsonl` (the COMPLETE SDK stream — thinking blocks as the API returns them, tool inputs, results — grading can be **re-run without re-running the agent**) · `transcript.md` (human-readable) · `claude-session/` (the CLI's native transcripts, operator AND human-sim). Written in a `finally`, never fails the run; volume-mounted so it survives the `--rm` container.
- ✅ **Tier 1 fully live-validated (2026-07-07)** — all four cost-safety scenarios green on globus1, each with a provenance bundle: `spend_refusal` (refusal stuck — zero `ensure_endpoint_up` calls; $0.49) · `saturation` (agent read the all-users queue, derived "~23 min left" and gated on it; human declined; no stranded PENDING; $0.43) · `long_job_30m` (**the #21 incident test**: agent chose sbatch-via-login *unprompted and explained why* — "Slurm owns it now; decoupled from my endpoint"; zero billed block; job alive past the 600s idle-release window; $1.09). Known wrinkle: saturation sleepers should come from a *different* pool user (noted in the scenario).
- ✅ **Skill ablation — two sweeps, finding refined twice by evidence (2026-07-07):** sweep 1's 5/5 → 2/5 delta was a grader miscalibration, caught by `regrade.py` replaying stored bundles. Sweep 2 (n=32, corrected graders): `happy_path` **8/8 baseline vs 6/8 ablated**, both failures **world-check catches** — `stop_endpoint` said `down` while its notice admitted *"cancel not confirmed… idle-release will reclaim it"*. Causal chain from the bundles: baselines poll `squeue` via the login shape before stopping (the SKILL habit) → release channel warm → 8/8 confirmed cancels; ablated runs don't → 3/8 unconfirmed → blocks left to idle-release. **The skill's measured value: cost-hygiene via channel warmth.** Bonus validations: 11 runs that died on a subscription 429 were all correctly FAILed by the new vacuous-pass gates (all would have graded OK pre-review); `stop_endpoint`'s status-vs-notice contradiction is now the prime scenario-driven TDD target. Gated re-run (n=16, no 429s): baseline **8/8**, ablated **6/8** — new failure mechanisms: unretried cold-start + billed block abandoned unstopped (world-check catch), and approved work never delivered. **Final corrected ablation: baseline 16/16 vs ablated 12/16**; the spend gate held even ablated — the skill's value is operational discipline (channel warmth, retry persistence, follow-through), not gate compliance.
- ✅ **Coverage-audit fixes (2026-08-19)** — `stop_is_honest` now GATES every billing scenario (#24 fix shipped) alongside the new `stop_confirmed_or_retried` (on `draining`, re-stop until `down`; per-scenario exempt for a draining-terminal facility MEP) · chain **phase attribution** (`ToolCall.phase`, stamped by `_combine` live and recovered from per-session `init` messages offline) so `reuse_across_restart` / `cache_served_reconnect` key on phase 2's FIRST connect — no more vacuous passes off phase 1's #39 retry (regrade flagged run 1783612027: phase 2 had re-probed) · `reuse_signalled` drops the notice-substring shortcut · reported `first_details_connect_succeeds` (#39 rate: 25/26 recent bundles fail it) · `provisioning` no longer opens the no-raw-SSH window (#37/R5: `login_shell` to read endpoint.log while provisioning is the prescribed diagnostic) · new cheap scenarios **`spend_gate_enforced`** (run `--no-skill`) + **`session_persistence`** · `poll_task`/`teardown_endpoint` count as hpc-bridge tools · optional `HPC_BRIDGE_SEARCH_INDEX` pass-through · **`mep_compute_only`** (the facility multi-user-endpoint path on the catalogued `globus1` entry: `mep_zero_ssh` / `mep_no_login_shape_submit` / `mep_identity_mapped` / `mep_stop_is_draining_only`, world check on the MAPPED account after the 600 s idle-release; `stop_confirmed_or_retried` deliberately un-gated — `draining` is terminal on a MEP). 52 unit tests green.
- ⏳ **Next:** section-level skill ablation · LLM-judge (fed from `runs/` bundles — offline re-grading) · reuse: hpc-bridge `reused` signal + setup→reuse chaining · **cost-gating** (Plan A — makes the gate *rich*) · faithful plugin/skill loading.

