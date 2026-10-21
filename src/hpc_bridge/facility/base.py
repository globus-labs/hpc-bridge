from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..profile import Profile

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def last_said(text: str) -> str:
    """The last meaningful line of what a step printed (colour codes dropped): the decisive one — a traceback's
    exception, a command's final error — never a login banner or a frame before it. "" when it printed nothing."""
    lines = [ln.strip() for ln in _ANSI.sub("", text or "").splitlines() if ln.strip()]
    return lines[-1][:400] if lines else ""


class ConfigStepFailed(RuntimeError):
    """A bring-up step that ran THROUGH the configuration's env_setup on the login node failed there: `configure`,
    `start` or `list` of globus-compute-endpoint, or the check or build of hpc-bridge's endpoint venv (after the site's
    prefix). Raised only when ssh itself got through — never for its own exit status 255, nor a negative one (the local
    ssh killed by a signal) — so the login node ran the configuration and refused it. `said`: the step's own last
    meaningful line (`last_said` of its output), which `notices._failure_of_the_config` reads to keep one that failed
    for the network, a timeout, Globus, the login node's own state or its filesystem; connect drops a details=
    configuration for the rest (connect._drop_failed_details)."""

    def __init__(self, message: str, *, said: str | None = None) -> None:
        super().__init__(message)
        self.said = last_said(message) if said is None else said


def step_error(rc: int, message: str, *, output: str = "") -> RuntimeError:
    """The error for a failed step that ran through env_setup: ConfigStepFailed when the login node ran it, a plain
    RuntimeError when ssh itself failed (rc 255, or a negative rc: nothing of the configuration is known to have run).
    `output`: what the step printed, whose last meaningful line decides (`ConfigStepFailed.said`)."""
    if rc == 255 or rc < 0:
        return RuntimeError(message)
    return ConfigStepFailed(message, said=last_said(output) if output else None)


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
