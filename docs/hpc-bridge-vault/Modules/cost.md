# cost.py

> [!abstract] Role
> Small pure helpers: estimate node-hours of spend, and cut a long output stream to its marked end for the agent.

## What it does

- **`estimate_spend(elapsed_s, nodes, charge_factor)`** (`cost.py:9`) → node-hours = `elapsed/3600 × nodes × charge_factor`. `charge_factor` is the facility's QOS multiplier (`0.0` = free local dev). Used by the [[Cost control|spend clock]] in [[server]].
- **`cut_output(text, max_chars, stream=, sdk_lines=)`** (`:27`) — bounds one output stream for the agent and keeps its **end**, opening with a marker line: `[hpc-bridge: stdout too long — showing only its last N lines (C chars); M lines (D chars) before them were dropped]`. The kept part starts at a whole line when that still keeps half the cap; otherwise (a long line just before a short last one — a one-line JSON result, then a trailer) it starts mid-line and the marker says so. `sdk_lines` is the SDK's `snippet_lines` ([[runner]]): received text that long may already be the SDK's silent tail, so the counts read "at least" — or, when only the SDK's line limit bound, the marker says earlier lines may have been dropped, how many unknown. **`cut_streams`** (`:64`) cuts stdout and stderr and returns the notice for a cut (redirect to a file on the facility, read ranges with `wc -l`/`head -n`/`sed -n`/`tail -n`). Called by [[dispatch]] `for_agent` — at the tool boundary only.

> [!warning] Output is cut to its END, never silently — and only for the agent
> The cap is `MAX_OUTPUT_CHARS` per stream (16,000, `runner.py:79`) — sized so a result stays inline in the hosts that show it to the model (Claude Code moves an MCP result over 50,000 chars to a file, Hermes spills one over 50,000; Codex cuts past ~10,000 tokens, Pi past 20 KiB): one full stream renders to ~17,500 chars of tool result, both full to ~34,500. The end is kept because errors and summaries land there **and** because the SDK's own cut keeps the end: before 2026-10 a `ShellFunction` kept its default last 1,000 lines with no marker (`cat` of a longer file came back as its tail, passing for the whole file), while hpc-bridge kept the *head* of a 1,000,000-char cap — far above what any host shows inline. A head cut over the SDK's tail would show a middle chunk as if it were the start. hpc-bridge's own consumers read the uncut result ([[dispatch]]).

Pure and dependency-free — no SDK, easy to unit-test.

## See also
[[Cost control]] · [[dispatch]] · [[server]]

> [!note] Split step 4 (2026-09-03) — the session spend clock lives here now
> `_bank_warm_interval`, `_billable`, `_settle_billing`, `_session_spend`, `_total_session_spend` and `_with_spend` moved in from `server.py` (the vault always documented cost as their home). `server` re-exports them.
