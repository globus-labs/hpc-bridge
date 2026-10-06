# Demos

> [!warning] Point-in-time artifacts — read the era column
> **Most** are recorded snapshots from **earlier** hpc-bridge and may **not** reflect current behaviour — for how it works *now*, see [[Happy path]]. (A clear tell: `architecture.html` describes *four* tools; there are now eleven — [[The MCP tools]].) Each entry is stamped with when it was recorded and the era it reflects; the **top row tracks current behaviour**.

| Demo | Recorded | Era | Shows |
|---|---|---|---|
| [Un-indexed facility · happy path](Demos/un-indexed-facility-happy-path.html) | 2026-06-29 | **current** — discovery-first | an **un-catalogued** cluster end-to-end: probe the login node → propose config → confirm → provision → run → release. Zero env vars (SSH from `~/.ssh/config`); the agent discovers `interface`/`scheduler`/`scratch`/`env_setup`, the user gives only an SSH host |
| [Anvil discovery logbook](Demos/anvil-discovery-showcase.html) | 2026-06-05 | early discovery (pre-[#10](https://github.com/globus-labs/hpc-bridge/pull/10)) | *"what can I run / what will it cost"* — partition + cost discovery on Anvil |
| [POC transcript](Demos/poc-transcript.html) | 2026-06-03 | original proof-of-concept | running real HPC compute through Claude end-to-end (spin up the endpoint → run a benchmark) |
| [Architecture sketch](Demos/architecture.html) | 2026-06-03 | early architecture (four-tool era) | the original "how it works" + lifecycle of a request |

> [!note] How to view
> These are standalone HTML — open the file in a browser, or view it through GitHub (raw/blob). They're archived here for **provenance**, not embedded inline (HTML doesn't transclude cleanly in Obsidian or GitHub). When a demo is re-recorded against current behaviour, add a new dated row rather than overwriting — the old one is the historical record.

## See also
[[Home]] · [[Happy path]] · [[Discovery today]]
