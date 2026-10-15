"""The Globus login GATE as the tools see it (split step 9, 2026-09-03): arm a login and wait for it
(`_start_login_and_wait`: browser mode waits up to HPC_BRIDGE_LOGIN_WAIT_S and continues in the same call;
a browser attempt that fails during the wait is re-armed in paste mode), and the two tool bodies
`_authenticate` / `_complete_login`. A new login may be a different identity, so both forget the sticky
no-account verdicts and mark the runners stale (warmth._forget_identity_verdicts).

The flow itself — loopback listener, paste-back, the SDK's own client id and token store — is `login.py`,
kept free of any import of the server or the runtime; this module is the seam between the two.
"""
from __future__ import annotations

import asyncio

from . import config
from .context import AppCtx
from .login import LOGIN_EXPIRED, LoginFlow, LoginMode, LoginStart, globus_identity_label
from .models import LoginStatus
from .notices import _login_notice
from .warmth import _forget_identity_verdicts


async def _start_login_and_wait(flow: LoginFlow, mode: LoginMode | None = None) -> tuple[LoginStart, str]:
    """Arm a login and, in browser mode, wait for it. Returns (start, status). A browser attempt that
    FAILS during the wait (no browser after all, Globus rejected the redirect) is re-armed at once in
    paste mode — the failure is remembered by the flow — so the caller shows a usable link, not an error."""
    start = await asyncio.to_thread(flow.start, mode)
    if start.mode != "browser":
        return start, "waiting"  # paste mode: nothing to wait for — the user must hand us a code
    status = await asyncio.to_thread(flow.wait, config.login_wait_s())
    if status == "failed":
        start = await asyncio.to_thread(flow.start)  # goes to paste (browser failure remembered)
        status = "waiting"
    return start, status

async def _authenticate(app: AppCtx, force: bool = False, mode: LoginMode | None = None) -> LoginStatus:
    flow = app.login_flow
    if flow is None:
        flow = app.login_flow = LoginFlow()
    why = None
    if not force and not await asyncio.to_thread(flow.login_required):
        # Holding every scope is not working: a revoked or expired refresh token passes login_required() and fails
        # every dispatch (live 2026-10-14). So the stored login is used once, as a dispatch would use it.
        verdict, detail = await asyncio.to_thread(flow.check_stored_login)
        if verdict == "works":
            return LoginStatus(phase="logged_in", notice="Globus login present with every scope hpc-bridge needs.")
        if verdict == "unchecked":  # no answer either way: never call a login dead for a network error
            return LoginStatus(phase="logged_in", notice=(
                "Globus login present with every scope hpc-bridge needs; whether it still works could not be checked "
                f"just now ({detail}). If dispatches then fail with a Globus login error, call "
                "authenticate(force=True)."))
        why = f"{LOGIN_EXPIRED}: Globus refused to refresh it"
    start, status = await _start_login_and_wait(flow, mode)
    if status == "done":
        _forget_identity_verdicts(app)
        replaced = f" ({LOGIN_EXPIRED}; this one replaces it)" if why else ""
        return LoginStatus(phase="logged_in", notice=(
            "Globus login completed in the browser" + await _as_identity() + replaced + "; carry on."))
    return LoginStatus(phase="needs_login", login_url=start.login_url, login_mode=start.mode,
                       notice=_login_notice(start, flow.error,
                                            waited_s=config.login_wait_s() if status == "waiting" else None, why=why))

async def _complete_login(app: AppCtx, code: str) -> LoginStatus:
    flow = app.login_flow
    if flow is None:
        return LoginStatus(phase="failed", notice="no login is waiting — call authenticate() first")
    try:
        await asyncio.to_thread(flow.complete_with_code, code)
    except Exception as exc:  # noqa: BLE001 - a bad/expired code is a structured outcome, not a crash
        return LoginStatus(phase="failed", notice=f"login code not accepted: {type(exc).__name__}: {exc}"[:300]
                           + " — call authenticate() for a fresh link.")
    _forget_identity_verdicts(app)
    return LoginStatus(phase="logged_in",
                       notice="Globus login complete" + await _as_identity() + ". Continue: connect_facility again.")


async def _as_identity() -> str:
    """" as <username>" for a login that just landed — so a foreign identity completing the link (anyone who
    held the URL during its window) is visible at once in the transcript (security review 2026-09-04, B-02)."""
    try:
        label = await asyncio.to_thread(globus_identity_label)
    except Exception:  # noqa: BLE001 - the label is a courtesy; the login itself already succeeded
        label = None
    return f" as {label}" if label else ""
