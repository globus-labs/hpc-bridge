from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

MODES = ("interactive", "batch")


@dataclass(frozen=True)
class Profile:
    mode: Literal["interactive", "batch"] = "batch"
    nodes_per_block: int = 1
    max_idletime_s: int = 600  # idle grace (s) before the block + UEP auto-release
    # Heartbeats (30 s each) a COMPUTE user endpoint stays up once idle — no task outstanding, at least one forwarded —
    # before it exits (globus-compute-endpoint's `idle_heartbeats_soft`; 0 = never, which also leaves its hard idle
    # limit off). 20 = 10 min, the block's own idle window: hpc-bridge starts a fresh compute user endpoint per block
    # request (warmth._fresh_block), so each must go once its block has. The login user endpoint never exits. It is
    # WRITTEN into the template when hpc-bridge configures an endpoint, so it governs only endpoints configured under
    # it: an endpoint is given fresh user endpoints by what its own published template says (`fresh_ueps`), whatever
    # this value is now.
    uep_idle_heartbeats: int = 20

    def __post_init__(self) -> None:
        # Literal is not enforced at runtime; reject unknown modes rather than silently
        # treating them as batch downstream.
        if self.mode not in MODES:
            raise ValueError(f"invalid profile mode {self.mode!r}: must be one of {MODES}")
        if self.max_idletime_s < 1:
            raise ValueError(f"max_idletime_s must be >= 1s, got {self.max_idletime_s}")
        if self.uep_idle_heartbeats < 0:
            raise ValueError(f"uep_idle_heartbeats must be >= 0, got {self.uep_idle_heartbeats}")
