from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

Runner = Callable[..., Awaitable[tuple[int, str, str]]]


class EndpointCLI:
    def __init__(self, user_dir: Path | None = None, runner: Runner | None = None) -> None:
        self.user_dir = user_dir
        self._run = runner or self._default_run

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        if self.user_dir is not None:
            env["GLOBUS_COMPUTE_USER_DIR"] = str(self.user_dir)
        return env

    def _ep_dir(self, name: str) -> Path:
        base = self.user_dir or (Path.home() / ".globus_compute")
        return base / name

    def user_template_path(self, name: str) -> Path:
        # globus-compute-endpoint 4.x runs `start` as an EndpointManager whose
        # config.yaml must be engine-free; the compute engine lives here, in the
        # per-user-process (UEP) template.
        return self._ep_dir(name) / "user_config_template.yaml.j2"

    def endpoint_id(self, name: str) -> str:
        return json.loads((self._ep_dir(name) / "endpoint.json").read_text())["endpoint_id"]

    async def _default_run(self, *args: str) -> tuple[int, str, str]:
        if sys.platform != "linux":
            raise RuntimeError(
                f"globus-compute-endpoint runs only on Linux (this host is {sys.platform!r}); "
                "it cannot provision a local endpoint here. For local development run an endpoint "
                "yourself (on Linux: a container, WSL or a login node) and set "
                "HPC_BRIDGE_ENDPOINT_ID=<uuid> to dispatch to it."
            )
        proc = await asyncio.create_subprocess_exec(
            "globus-compute-endpoint",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env(),
        )
        out, err = await proc.communicate()
        return proc.returncode or 0, out.decode(), err.decode()

    async def configure(self, name: str, multi_user: bool = False) -> None:
        # hpc-bridge invariant: always a PERSONAL (single-user) endpoint, never a MEP.
        # globus-compute-endpoint's default auto-selects multi-user from the configuring
        # user's POSIX capabilities, which can silently create a multi-user (identity-
        # mapping) endpoint — the exact thing this project avoids. Force it off.
        rc, _out, err = await self._run(
            "configure", "--multi-user", "true" if multi_user else "false", name
        )
        if rc != 0:
            raise RuntimeError(f"configure failed: {err}")

    async def start(self, name: str) -> str:
        # 4.x `start` runs in the FOREGROUND by default; --detach daemonizes it and
        # returns promptly. The registered UUID is written to endpoint.json, not stdout.
        rc, _out, err = await self._run("start", name, "--detach")
        if rc != 0:
            raise RuntimeError(f"start failed: {err}")
        return self.endpoint_id(name)

    async def stop(self, name: str) -> None:
        rc, _out, err = await self._run("stop", name)
        if rc != 0:
            raise RuntimeError(f"stop failed: {err}")
