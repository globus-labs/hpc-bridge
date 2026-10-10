from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..profile import Profile


async def endpoint_metadata(client_factory: Callable[[], Any], endpoint_id: str) -> dict:
    """The web service's metadata for an endpoint — its published UEP template, schema, version — read off the event
    loop. Raises what the client raises; callers decide how best-effort to be."""
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
