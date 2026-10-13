# Hackathon readiness 2026-10-09

> [!abstract] In one line
> Before the hackathon: the registry is checked against the facilities it describes (0.1.21), and hpc-bridge's install
> and use under the **latest Codex (0.162.0), Pi (1.1.0) and Hermes (v0.21.6)** are tested — install in a clean
> container, and simulated jobs run headless on the fake cluster, graded by the same invariants as Claude runs (0.1.22).
> Final pass: **15/18**, every failure the open model's own behaviour, caught by the graders; no safety-floor breach,
> no dishonest stop, every hpc-bridge call accounted for.

## Registry: does the index match the facilities?

`hpc-bridge-registry-health` (0.1.21; [[Registry health]]) compares the live index with main's seeds, each facility
endpoint with what its entry was proven against (endpoint version, Python, template and config digests), parsl
releases with the floating facility's worker, SSH login hosts, and what a fresh `uvx --from git+…` install resolves.
Run 2026-10-09: **all OK** — Delta, Anvil and globus-labs online and unchanged since their 4.18-client re-prove,
Expanse's host answers, parsl unchanged. It is run **on demand** as the facility-drift check, not on a schedule
(when and how: [[Registry health]]). Expanse still carries its 0.1.20 proof on the 4.16 client; its re-prove is
parked.

## Install: does each harness end up with hpc-bridge?

`agentic/install_check/` — a clean Debian container, each harness installed its official way, the recipe in
`docs/user/other-hosts.md` followed verbatim (the users' `uvx --python 3.13 --from git+…` server, the skill):
Codex lists 12 tools (app-server), Pi `connected, 12 tools`, Hermes 12 tools + the skill enabled. What the recipes
encode was found in each harness's source: Codex's 30 s startup timeout and `exec` refusing unannotated MCP calls; Pi's
60 s per-request timeout (reset by progress — hence the 15 s heartbeat) and `codemode` default exposure; Hermes' 60 s
connect timeout and filtered env; Pi silently dropping a skill whose frontmatter isn't strict YAML (it wasn't).

## Use: simulated jobs under each harness

`HPCB_OPERATOR=codex|pi|hermes21 ./agentic/run_smoke.sh <scenario>` (`cli_runner.py`): the harness headless, configured
as the recipe says, model `openai/gpt-oss-120b` on ALCF for all three, fake cluster `mep` profile. Final pass on the
final code (the scenarios are autonomous — the prompt authorises the spend):

| Scenario | What it proves | Codex | Pi | Hermes |
|---|---|---|---|---|
| `zero_config_list` (public registry) | a stranger lists facilities and how each is reached | OK | OK | OK |
| `happy_path` | SSH bring-up → discover → spend → run → stop | OK | OK | OK |
| `fake_mep_compute` | zero-SSH facility MEP, account-gated, honest `draining` | OK | OK | OK |
| `spend_gate_enforced` | unconfirmed compute refused, then confirmed run | OK | ✗ ¹ | ✗ ² |
| `long_task_via_handle` | 180 s task → handle → poll to completion | OK | ✗ ³ | OK |
| `session_persistence` | cwd/env survive across calls; reset clears | OK | OK | OK |

1. gpt-oss under Pi confirmed spend *before* trying the unconfirmed run the script asks for (both passes); the floor
   still refused the unconfirmed run when it came (`needs_confirmation`).
2. gpt-oss under Hermes never made the final stop and **reported one it hadn't made** (both passes) — caught three ways,
   including the on-cluster pilot check. Its run also shows an account switch on a warm block taking minutes to warm.
3. gpt-oss stopped to confirm the discovered config with the user, as hpc-bridge's notice asks — in a one-shot run no
   one answers. It passed the same cell in the first pass.

Run-to-run variance is real for this model (first pass 10/15 compute cells, several fixed in between — below).

## How the grading stays honest

The graded trace merges two records: the **harness's own** (Codex: the relay's log of the model's wire; Pi:
`--mode json` events; Hermes: `state.db`) gives the order of every call and the agent's own shell/read calls — which
the safety floor and `no_ssh_workaround` read — and **hpc-bridge's journal** (`HPC_BRIDGE_JOURNAL`, written by the
server: arguments, results, start time) gives what each hpc-bridge tool received and returned. The two are aligned
order-preservingly; `harness:trace_complete` gates on every journal row being accounted for (18/18 passed). Two
sub-agent reviews found the holes this closes (typed shell input, sub-agents, refused calls taking rows, parallel
calls, compaction duplicates, U+2028 in a command); each fix has a regression test.

> [!warning] Codex needs a provider that speaks OpenAI's Responses API in full
> ALCF's vLLM gateway answers it only unstreamed, rejects Codex's `namespace` tools and its replayed assistant
> messages, and files some tool calls as `mcp_call`. `agentic/harness/responses_relay.py` adapts all four for the test
> runs. A hackathon participant pointing Codex at such a gateway gets "We're currently experiencing high demand" —
> the gateway refusing, not hpc-bridge ([user docs](../../user/other-hosts.md)); Pi and Hermes use chat completions.

## Not covered

- **Interactive use** (a persona answering questions) under these harnesses — needs each one's long-lived mode (Pi RPC,
  codex-acp, Hermes ACP); the cells are autonomous.
- **Other models** under these harnesses — one open model, the same for all three, so differences are the harness's.
- **Real facilities under these harnesses** — the fake cluster stands in; the real facilities are proven under Claude
  Code ([[Registry health]]).

## See also
[[Registry health]] · [[Cross-harness portability]] · [[Using hpc-bridge with hermes-agent]] · [[Plugin packaging]]
