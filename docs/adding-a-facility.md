# Adding a facility to the registry

The facility registry is public, curated data: a Globus Search index the plugin reads anonymously, so a
fresh install can list facilities with no login. The plugin never writes to it. Executable configuration
(the `env_setup` shell line, endpoint UUIDs) is an injection vector, so new machines arrive by review.

1. Copy a seed under `src/hpc_bridge/catalog/seed/`: `anvil.yaml` for an **SSH-bootstrap** facility
   (hpc-bridge stands up a personal endpoint on the login node), or `globus-cluster.yaml` for a
   **facility-run multi-user endpoint** (zero SSH: `compute_mep_uuid`, no `ssh_host`, `$HOME`-relative
   scratch, a version-pinned `env_setup`).
2. Fill in the scheduler, network interface, `env_setup`, scratch root, defaults and `last_validated`,
   then run `python -m pytest tests/test_catalog_bundled.py -q`; the schema validates the seed. For an
   SSH facility whose `env_setup` installs the endpoint itself, generate it with
   `hpc_bridge.discovery.uv_env_setup(python=…, gce=…, sdk=…, bootstrap=…)` (`sdsc-expanse.yaml` holds its output)
   rather than hand-writing `command -v globus-compute-endpoint ||` guards, which keep a broken install:
   `python` and `gce` are the Python and endpoint version you proved the facility with, `sdk` the
   globus-sdk that venv has (`<venv>/bin/python -c "import importlib.metadata as m; print(m.version('globus-sdk'))"`),
   and `bootstrap=True` when the login node has no `uv` of its own.
3. Open a pull request. A curator ingests it with `hpc-bridge-catalog <index-uuid> <seed>.yaml`
   (idempotent, keyed by subject) and it is live for everyone at the next `list_facilities()`.

Because an entry is trusted code, write access to the index is the trust root. Today the index is a
Globus Search trial index owned by one maintainer identity; before the first stable release it moves to
a production index owned by a maintainers group, and the seeds in this repository remain the public,
reviewable record of what it contains.

An un-catalogued cluster works today without any of this: give the agent an SSH login host and it
discovers the configuration and caches it locally. For a catalogued id the registry always wins over that
local cache, so curated entries are the stable ones.

Facilities that run a multi-user endpoint but are not yet in the registry (ALCF Polaris and Crux, NCSA
Delta, NeSI) need per-facility template settings hpc-bridge does not model yet; the vault's
[MEP facilities survey](hpc-bridge-vault/Reference/MEP%20facilities%20survey.md) tracks them.
