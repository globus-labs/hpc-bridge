from __future__ import annotations

import asyncio

import yaml

from ..profile import Profile
from .base import EndpointHandle


class LocalFacility:
    name = "local"

    def __init__(self, cli, endpoint_name: str = "hpc-bridge-dev") -> None:
        self.cli = cli
        self.endpoint_name = endpoint_name

    def keeps_block_warm(self, profile: Profile) -> bool:
        """The interactive profile holds its LocalProvider block (min_blocks=1): it never idle-releases, so the
        clock alone must never presume it gone."""
        return profile.mode == "interactive"

    def config_template(self, profile: Profile) -> dict:
        # The per-user-process (UEP) template content. In globus-compute-endpoint 4.x
        # the engine lives here, not in the manager config.yaml. The interactive profile
        # holds a warm block (min_blocks>=1) — a LocalProvider block costs no allocation, so
        # unlike SlurmFacility (which forces min_blocks=0 + an idle timer to avoid leaking
        # SU) we keep it warm for snappy local dev; batch scales to zero.
        warm = self.keeps_block_warm(profile)
        return {
            "engine": {
                "type": "GlobusComputeEngine",
                "max_workers_per_node": 1,
                # ShellFunctions expect a per-task sandbox dir; our session shim cd's to an
                # absolute <scratch>/sessions/<id> path so the sandbox landing dir is harmless.
                "run_in_sandbox": True,
                "provider": {
                    "type": "LocalProvider",
                    "init_blocks": 1 if warm else 0,
                    "min_blocks": 1 if warm else 0,
                    "max_blocks": 1,
                },
            },
        }

    async def provision(self, profile: Profile) -> EndpointHandle:
        # configure() forces --multi-user false (personal, no identity-mapping);
        # then write our engine into the UEP template, leaving config.yaml as the
        # engine-free manager config that `start` requires.
        await self.cli.configure(self.endpoint_name)
        template = self.cli.user_template_path(self.endpoint_name)
        template.write_text(yaml.safe_dump(self.config_template(profile), sort_keys=False))
        eid = await self.cli.start(self.endpoint_name)
        return EndpointHandle(endpoint_id=eid, name=self.endpoint_name)

    async def manager_online(self, endpoint_id: str) -> bool:
        # globus-compute-endpoint 4.x exposes only {"status": "online"|"offline"} here —
        # NOT a worker count (confirmed against 4.12). The EndpointManager being online is
        # our readiness signal; true warm/cold worker-block readiness isn't exposed and is
        # a dispatch-time concern.
        from globus_compute_sdk import Client

        status = await asyncio.to_thread(Client().get_endpoint_status, endpoint_id)
        return status.get("status") == "online"
