"""In-session one-time code for an MFA facility (2026-09-04).

A facility that demands a TOTP / Duo passcode at every SSH login (SDSC Expanse, TACC) used to need the
user's own terminal: hpc-bridge handed over an `ssh -fN …` line and waited. This module lets the agent
ask the user for the CURRENT code instead and open the shared ControlMaster itself — the same trade the
Globus paste-mode login already makes: a one-time code is single-use and expires in seconds, so a
transcript that holds it leaks nothing reusable. **A password is not a code** and never enters the
session: the askpass helper refuses any prompt that mentions a password, and the code is shape-checked.

Mechanism: OpenSSH calls `$SSH_ASKPASS` for every keyboard-interactive prompt when it has no terminal
(`SSH_ASKPASS_REQUIRE=force`, OpenSSH >= 8.4). Our helper answers a code-looking prompt with the code read
from a 0600 file, and exits non-zero on a password prompt. The code is never placed in argv or logs.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from .facility.remote import SshTarget

# Code-SHAPED, not "not a password": a short password matches too. The load-bearing guard is the askpass helper
# refusing any password prompt (below); this regex only rejects obvious non-codes before any ssh is spawned.
_CODE_RE = re.compile(r"^[A-Za-z0-9]{4,16}$")
_ASKPASS = r"""#!/bin/sh
# hpc-bridge askpass: answer ONE one-time-code prompt; refuse a password prompt, and refuse a host-key
# confirmation (answering it with the code would loop until the timeout — live 2026-09-04).
case "$1" in
  *[Pp]assword*|*[Pp]assphrase*) echo "hpc-bridge: refusing a PASSWORD prompt (one-time codes only)" >&2; exit 1 ;;
  *continue\ connecting*|*fingerprint*|*authenticity*) echo "hpc-bridge: refusing a HOST KEY prompt" >&2; exit 1 ;;
esac
cat "$HPCB_CODE_FILE"
"""


def looks_like_code(code: str) -> bool:
    return bool(_CODE_RE.match((code or "").strip()))


def open_master_with_code(target: SshTarget, code: str, *, state_dir: Path, timeout_s: float = 90.0,
                          ssh_bin: str = "ssh") -> tuple[bool, str]:
    """Open `target`'s ControlMaster, answering the one keyboard-interactive prompt with `code`.
    Returns (opened, why). `why` never contains the code."""
    code = (code or "").strip()
    if not looks_like_code(code):
        return False, ("that is not a one-time code (4–16 letters/digits). hpc-bridge never sends passwords: if the "
                       "facility asks for one, the user opens the session in their own terminal.")
    if not target.control_dir:
        return False, ("SSH multiplexing is off (HPC_BRIDGE_SSH_CONTROL_PERSIST): a pre-opened connection "
                       "cannot be shared.")
    work = Path(tempfile.mkdtemp(prefix="otp-", dir=str(state_dir)))
    try:
        os.chmod(work, 0o700)
        helper = work / "askpass.sh"
        helper.write_text(_ASKPASS)
        helper.chmod(0o700)
        code_file = work / "code"
        code_file.write_text(code + "\n")
        code_file.chmod(0o600)
        env = {**os.environ, "SSH_ASKPASS": str(helper), "SSH_ASKPASS_REQUIRE": "force", "DISPLAY": ":0",
               "HPCB_CODE_FILE": str(code_file)}
        argv = [ssh_bin, *target.preauth_argv()[1:]]
        argv[1:1] = ["-o", "BatchMode=no", "-o", "NumberOfPasswordPrompts=1"]
        try:
            proc = subprocess.run(argv, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                  timeout=timeout_s, start_new_session=True, check=False)
        except subprocess.TimeoutExpired:
            return False, (f"ssh did not finish within {int(timeout_s)}s — the facility may be waiting on a second "
                           "step (a push approval?) or be unreachable. Try again with a fresh code, or open the "
                           "session in your own terminal.")
        err = (proc.stderr or "").strip()
        if proc.returncode != 0:
            low = err.lower()
            if "refusing a password prompt" in low:
                return False, ("the login asked for a PASSWORD, which hpc-bridge never handles. Open the session "
                               "in your own terminal with the preauth_command instead (a key on the facility "
                               "removes the password prompt).")
            if "identification has changed" in low or ("host key for" in low and "changed" in low):
                # NOT the unknown-key case: the key this host presents differs from the one already trusted — the
                # signature of a man-in-the-middle as well as of a reinstalled node. Never coach acceptance
                # (review 2026-09-05, low: the bootstrap path already split the two; this one folded them).
                return False, (f"HOST KEY CHANGED for {target.host}: the key it presents is not the one your ssh "
                               "already trusts. Do NOT accept it until the facility confirms the new fingerprint "
                               "(its documentation or support). Once verified, replace the old key "
                               f"(`ssh-keygen -R {target.host_key_alias or target.host}`), connect once from your "
                               f"own terminal (`{target.preauth_command()}`), then try again with a fresh code.")
            if "refusing a host key prompt" in low or "host key verification failed" in low:
                return False, (f"UNKNOWN HOST KEY for {target.host}: your ssh does not trust this host's key yet. "
                               f"Connect once from your own terminal (`{target.preauth_command()}`), verify and "
                               "accept the fingerprint, then try again with a fresh code.")
            if "permission denied" in low:
                return False, ("the code was not accepted (expired or mistyped?) — ask the user for a fresh one "
                               "and try again.")
            return False, f"ssh failed (rc={proc.returncode}): {err[-300:] or 'no output'}"
        check = subprocess.run([ssh_bin, *target.control_argv("check")[1:]], capture_output=True, text=True,
                               timeout=20, check=False)
        if check.returncode != 0:
            return False, f"ssh returned but no shared connection answered: {(check.stderr or '').strip()[-200:]}"
        return True, f"shared connection to {target.host} is open (the facility's one-time code was accepted)"
    finally:
        shutil.rmtree(work, ignore_errors=True)  # the code file lives only for the duration of the login
