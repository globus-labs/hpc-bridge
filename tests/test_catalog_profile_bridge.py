"""The catalog -> MachineProfile bridge (profile_from_catalog_entry)."""
import asyncio
from pathlib import Path

from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.facility.remote import profile_from_catalog_entry

# The SSH Anvil entry left the registry 2026-09-04 (Anvil is reached through its multi-user endpoint); the
# known-good SSH config lives on as a fixture — still the oracle for the catalog -> MachineProfile bridge.
_ANVIL_SSH = Path(__file__).parent / "catalog_fixtures" / "anvil-ssh.yaml"


def _anvil_entry():
    return asyncio.run(BundledCatalog(_ANVIL_SSH).get("purdue:anvil"))


def test_bridge_reconstructs_the_anvil_config():
    # The bundled anvil entry resolves to the known-good profile we've stood up live. With the
    # hardcoded anvil_profile() removed, this is the anvil-config oracle.
    p = profile_from_catalog_entry(_anvil_entry(), user="u1", account="ACCT-CPU")
    assert p.name == "anvil"
    assert p.endpoint_name == "hpc-bridge-anvil" and p.display_name == "HPC-Bridge Anvil"
    assert p.interface == "ib0" and p.partition == "debug" and p.account == "ACCT-CPU"
    assert p.env_setup == (  # the default venv: under the login node's own $HOME, expanded there
        "module load anaconda/2024.02-py311 && source $HOME/hpc-bridge/gce-venv/bin/activate"
    )
    assert p.worker_init == p.env_setup
    assert p.scratch_root == "/anvil/scratch/u1/.hpc-bridge"
    assert p.amqp_port == 443
    assert (p.max_workers_per_node, p.nodes_per_block, p.max_blocks) == (2, 1, 1)


def test_bridge_resolves_templates_and_overrides():
    p = profile_from_catalog_entry(
        _anvil_entry(), user="bob", account="A", partition="wholenode", venv="/opt/gce"
    )
    assert "{venv}" not in p.env_setup
    assert "/opt/gce/bin/activate" in p.env_setup
    assert p.worker_init == p.env_setup  # worker_init is the resolved env_setup
    assert "{user}" not in p.scratch_root
    assert "/anvil/scratch/bob/" in p.scratch_root
    assert p.partition == "wholenode"  # explicit override beats the entry default
    assert p.account == "A"


def test_bridge_defaults_venv_to_convention():
    # `$HOME/…`, not `/home/<user>/…`: a home is not always under /home (NERSC: /global/homes/<x>/<user>; Perlmutter
    # report 4, 2026-10-29)
    p = profile_from_catalog_entry(_anvil_entry(), user="alice", account="A")
    assert "source $HOME/hpc-bridge/gce-venv/bin/activate" in p.env_setup and "/home/alice" not in p.env_setup


def test_bridge_resolves_dollar_user_idiom():
    # A BYO facility's user naturally gives the shell idiom $USER / ${USER}, not the {user} template.
    # scratch_root is embedded *single-quoted* in the session shim, so the remote shell can't expand
    # it — the bridge must, or `mkdir` hits a literal /scratch/$USER (the live BYO-test bug).
    import datetime

    from hpc_bridge.catalog.entry import CatalogEntry, Compute, Defaults

    e = CatalogEntry(
        id="byo", facility_key="session", facility="BYO", description="d", display_name="BYO",
        transfer_endpoint_uuid=None, ssh_host="h", allocation=None,
        compute=Compute(
            scheduler="slurm", interface="ib0",
            env_setup="source /home/$USER/v/bin/activate", scratch_root="/scratch/${USER}/.hpc-bridge",
        ),
        defaults=Defaults(partition="p"), provenance="session", last_validated=datetime.date.today(),
    )
    p = profile_from_catalog_entry(e, user="carol", account="A")
    assert p.scratch_root == "/scratch/carol/.hpc-bridge"  # ${USER} resolved (not left literal)
    assert "$USER" not in p.env_setup and "/home/carol/v/bin/activate" in p.env_setup
