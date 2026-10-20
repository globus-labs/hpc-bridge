from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..profile import Profile


class ConfigStepFailed(RuntimeError):
    """A bring-up step that ran THROUGH the configuration's env_setup on the login node failed there: `configure`,
    `start` or `list` of globus-compute-endpoint, or the check or build of hpc-bridge's endpoint venv (after the site's
    prefix). Raised only when ssh itself got through — never for its own exit status 255 — so the login node ran the
    configuration and refused it. `notices._failure_of_the_config` still keeps one whose words name the network, a
    timeout, Globus or a full disk; connect drops a details= configuration for the rest
    (connect._drop_failed_details)."""


def step_error(rc: int, message: str) -> RuntimeError:
    """The error for a failed step that ran through env_setup: ConfigStepFailed when the login node ran it, a plain
    RuntimeError when ssh itself failed (rc 255: nothing of the configuration ran)."""
    return RuntimeError(message) if rc == 255 else ConfigStepFailed(message)


async def endpoint_metadata(client_factory: Callable[[], Any], endpoint_id: str) -> dict:
    """The web service's metadata for an endpoint — its published UEP template, schema, version — read off the event
    loop; `{}` when the service returns nothing. Raises what the client raises; callers decide how best-effort to be."""
    md = await asyncio.to_thread(lambda: client_factory().get_endpoint_metadata(endpoint_id))
    return md or {}


@dataclass(frozen=True)
class EndpointHandle:
    endpoint_id: str
    name: str
    login_host: str | None = None  # resolved FQDN the manager daemon landed on
    reused: bool = False  # attached to an already-online endpoint (no fresh provision, no SSH)


@runtime_checkable
class Facility(Protocol):
    name: str

    async def provision(self, profile: Profile) -> EndpointHandle: ...
    async def manager_online(self, endpoint_id: str) -> bool: ...
    def config_template(self, profile: Profile) -> dict | tuple[str, dict]: ...
