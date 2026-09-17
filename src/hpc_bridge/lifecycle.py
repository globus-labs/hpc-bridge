from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .facility.base import Facility
from .profile import Profile

BlockState = Literal["warm", "cold", "provisioning"]
# What _provision may answer: a block state, or the spend floor refusing to start one.
ProvisionResult = Literal["warm", "cold", "provisioning", "needs_confirmation", "needs_account"]


@dataclass
class EndpointState:
    endpoint_id: str | None = None
    reused: bool = False  # endpoint_id came from reattaching to an already-online endpoint


async def probe(facility: Facility, state: EndpointState) -> BlockState:
    if state.endpoint_id is None:
        return "cold"
    return "warm" if await facility.manager_online(state.endpoint_id) else "provisioning"


async def ensure_warm(
    facility: Facility, profile: Profile, state: EndpointState
) -> tuple[BlockState, EndpointState]:
    if state.endpoint_id is None:
        handle = await facility.provision(profile)
        state = EndpointState(endpoint_id=handle.endpoint_id, reused=handle.reused)
    block = await probe(facility, state)
    return block, state
