# Plugin packaging

> [!abstract] Role
> How hpc-bridge installs into Claude Code: an MCP stdio server plus the agent-facing skill and command.

## The surfaces

- **`.mcp.json`** — declares the stdio server under the key **`endpoint`** (*not* `hpc-bridge`): Claude Code namespaces a plugin's tools `plugin:<plugin>:<mcpServers-key>`, so an `endpoint` key reads `plugin:hpc-bridge:endpoint` instead of the doubled `plugin:hpc-bridge:hpc-bridge`. `FastMCP("endpoint")` ([[server]]) mirrors the key. Launched as `uv run --directory ${CLAUDE_PLUGIN_ROOT} --extra integration hpc-bridge` — the trailing `hpc-bridge` is the **console script** → `main()`, unchanged by the rename — with `HPC_BRIDGE_USER_DIR=${CLAUDE_PLUGIN_DATA}/globus_compute`.
- **`.claude-plugin/plugin.json`** — plugin manifest (name, description, version).
- **`skills/driving-hpc/SKILL.md`** — the **agent recipe**: how to drive HPC well — `connect_facility` is always the first call (it decides reuse vs discovery vs pre-auth vs login); tell the user what a facility needs from its `access_note` before they choose; relay a `needs_login` link (never a password) or a `needs_preauth` command (never the secret); discover via the login shape; present the partition + budget gate; provision with `confirm_spend`; wait by polling the scheduler through the endpoint; run long work as a foreground task; read `stop_endpoint`'s status honestly; and the **compute-only facilities** section for a facility MEP (no login shape, the terminal NO ACCOUNT, draining-only stop). This is where agent *judgment* lives ([[Discovery today]]).
- **`commands/hpc-connect.md`** — the `/hpc-connect` slash command: leads with `connect_facility` (never a bare `ensure_endpoint_up`, which with no facility bound only answers "No facility connected", [[facility-local]]), lists facilities with their access notes when none is named, and reports a compute-only attach for what it is.

## Install / run

```bash
uv sync --extra dev
uv run pytest -q                 # the test suite
uv run hpc-bridge                # run the MCP server standalone (stdio)
claude --plugin-dir .            # install into Claude Code for local testing
scripts/fresh_user_session.sh    # the same, AS A FRESH USER: scratch token + state dirs, launched outside the repo
                                 #   (--reset = brand-new user again; REGISTRY=<uuid> overrides the built-in registry)
```

> [!note] In-repo development vs an installed plugin
> The repo's `.mcp.json` launches the server with `uv run` (not `uvx`, which served stale builds — see `[tool.uv] cache-keys` in `pyproject.toml`), and `.claude/settings.local.json` (gitignored) may carry the maintainer's env (e.g. `HPC_BRIDGE_SEARCH_INDEX`) that an installed plugin never sees — `scripts/fresh_user_session.sh` deliberately strips such overrides so the built-in defaults are what gets exercised. "No hpc-bridge tools" in a session means a server boot crash (check stderr), not "disabled".

> [!note] The `integration` extra
> Core deps are just `mcp` / `pydantic` / `pyyaml`. `globus-compute-sdk` (and, Linux-only, `globus-compute-endpoint`) live in the optional `integration` extra — unit tests are hermetic and don't need it. `globus_sdk` comes transitively (used for the Search index query and `get_endpoints`).

## See also
[[server]] · [[Configuration]] · [[Discovery today]] · [[New-user testing (clean-session)]] · [[Happy path]]
