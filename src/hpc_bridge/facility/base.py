from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..profile import Profile

# Terminal control sequences a login shell may print around a command's words: CSI (colours, cursor) and OSC (a window
# title, `\x1b]0;user@host:~\x07`, ended by BEL or ST).
_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)?")
# Lines a login shell prints after the command, never its cause: `clear` in ~/.bash_logout with no TERM.
_SHELL_NOISE = re.compile(r"^(?:TERM environment variable not set\.?|tput: No value for \$TERM.*)$")
TAIL_LINES = 4   # how many of a step's last meaningful lines a cause may sit in (`said_tail`)


def said_lines(text: str) -> list[str]:
    """The meaningful lines of what a step printed: control sequences, blank lines and the shell's own trailers out."""
    lines = (ln.strip() for ln in _ANSI.sub("", text or "").splitlines())
    return [ln for ln in lines if ln and not _SHELL_NOISE.match(ln)]


def last_said(text: str) -> str:
    """The last meaningful line of what a step printed: the decisive one — a traceback's exception, a command's final
    error — never a login banner or a frame before it. "" when it printed nothing."""
    lines = said_lines(text)
    return lines[-1][:400] if lines else ""


def said_tail(text: str, lines: int = TAIL_LINES) -> str:
    """The last `lines` meaningful lines, joined: where a cause printed just before the final error sits (pip's
    "Connection to pypi.org timed out" warning before its "No matching distribution"), but a banner far above never."""
    return "\n".join(ln[:400] for ln in said_lines(text)[-lines:])


class ConfigStepFailed(RuntimeError):
    """A bring-up step that ran THROUGH the configuration's env_setup on the login node failed there: `configure`,
    `start` or `list` of globus-compute-endpoint, or the check or build of hpc-bridge's endpoint venv (after the site's
    prefix). Raised only when ssh itself got through — never for its own exit status 255, nor a negative one (the local
    ssh killed by a signal) — so the login node ran the configuration and refused it. `said`: the step's own last
    meaningful line (`last_said`), what an explanation quotes; `tail`: its last few (`said_tail`), which
    `notices._failure_of_the_config` reads to keep one that failed for the network, a timeout, Globus, the login node's
    own state or its filesystem; connect drops a details= configuration for the rest (connect._drop_failed_details)."""

    def __init__(self, message: str, *, said: str | None = None, tail: str | None = None) -> None:
        super().__init__(message)
        self.said = last_said(message) if said is None else said
        self.tail = (said_tail(message) if said is None else said) if tail is None else tail


def step_error(rc: int, message: str, *, output: str | None = None) -> RuntimeError:
    """The error for a failed step that ran through env_setup: ConfigStepFailed when the login node ran it, a plain
    RuntimeError when ssh itself failed (rc 255, or a negative rc: nothing of the configuration is known to have run).
    `output`: what the step printed — only its own words decide (`ConfigStepFailed.said` / `tail`), never a note the
    message adds after them; "" when it printed nothing."""
    if rc == 255 or rc < 0:
        return RuntimeError(message)
    if output is None:
        return ConfigStepFailed(message)
    return ConfigStepFailed(message, said=last_said(output), tail=said_tail(output))


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
