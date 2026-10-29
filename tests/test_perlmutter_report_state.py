"""Proofs for the Perlmutter (NERSC) report of 2026-10-29 (plugin 0.1.20): the "left-over local state" group.

A user who had connected `perlmutter` before (its config PROVEN and cached in facilities.json, its endpoint pinned to
`login22` in endpoints.json) came back weeks later: login22 no longer answered, their key no longer worked (password +
OTP site), and each piece of local state stood in the way. Each test with a docstring starting "On 0.1.20" pinned one bug
as a strict xfail until its fix; the controls passed throughout. Item 8's endpoint reuse is by design (#27) and gets a
passing regression test.

Hermetic, faked only at the boundary: the `ssh` binary on PATH (a fake OpenSSH client: `ssh -G` answers from a fake
~/.ssh/config, a connection is refused or times out per host, every connection is logged), the TCP connect the alias
probe makes, the Globus web service's `find_online_endpoint`, and the registry (Perlmutter is not catalogued). The
connect flow, the facility build, the login-node pin, the bring-up's first SSH step (the endpoint-venv check), the
error explainer, `_drop_dead_pin` and the discovery probe are the real code. No real username: the login is `alice`.
"""
from __future__ import annotations

import contextlib
import os
import socket
import stat
import sys

import pytest

from hpc_bridge import binding, config, server
from hpc_bridge.context import AppCtx
from hpc_bridge.facility.endpoint_venv import short_env_setup
from hpc_bridge.facility.remote import SlurmFacility
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from hpc_bridge.state import EndpointRecord, FacilityStore, LoginNodeStore
from tests.fakes import FakeCatalog, FakeFacility

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="the fake OpenSSH client is a sh script")

ALIAS = "perlmutter"                       # the user's ~/.ssh/config Host — not a DNS name
CANONICAL = "perlmutter.nersc.gov"         # its HostName: answers on :22
PINNED = "login22.chn.perlmutter.nersc.gov"  # the node the endpoint landed on weeks ago: no longer answers
ENDPOINT = "hpc-bridge-perlmutter"         # `_session_endpoint_name(ALIAS)`
NERSC_DENIAL = "Permission denied (publickey,password,keyboard-interactive,hostbased)."  # password+OTP, no key

FAKE_SSH = r'''#!/bin/sh
# A fake OpenSSH client. `-G`: the user's ~/.ssh/config (Host perlmutter -> HostName perlmutter.nersc.gov). `-O`: no
# master. Otherwise: log "<host> <probe|bringup>" and fail as the host does (timed out, or the password+OTP denial).
if [ "$1" = "-G" ]; then
  shift; [ "$1" = "--" ] && shift
  if [ "$1" = "perlmutter" ]; then echo "hostname perlmutter.nersc.gov"; else echo "hostname $1"; fi
  echo "user alice"; exit 0
fi
for a in "$@"; do [ "$a" = "-O" ] && { echo "Control socket connect: No such file or directory" >&2; exit 255; }; done
dest=""; cmd=""; seen=0
for a in "$@"; do
  if [ "$seen" = 1 ]; then if [ -z "$dest" ]; then dest="$a"; else cmd="$a"; fi; continue; fi
  [ "$a" = "--" ] && seen=1
done
host="${dest#*@}"
case "$cmd" in *HPCB_PROBE_BEGIN*) kind=probe;; *) kind=bringup;; esac
echo "$host $kind" >> "$FAKE_SSH_LOG"
case " $FAKE_SSH_DOWN " in *" $host "*)
  echo "ssh: connect to host $host port 22: Operation timed out" >&2; exit 255;; esac
echo "$dest: Permission denied (publickey,password,keyboard-interactive,hostbased)." >&2
exit 255
'''


class World:
    """The user's machine: the fake ssh on PATH, DNS/TCP as seen from it, the Globus web service, the registry."""

    def __init__(self, monkeypatch, tmp_path, *, dns: tuple[str, ...] = (CANONICAL, PINNED)) -> None:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        ssh = bin_dir / "ssh"
        ssh.write_text(FAKE_SSH)
        ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
        self.log_path = tmp_path / "ssh.log"
        self.log_path.write_text("")
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
        monkeypatch.setenv("FAKE_SSH_LOG", str(self.log_path))
        monkeypatch.setenv("FAKE_SSH_DOWN", PINNED)
        monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "alice")
        for var in ("HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_SSH_KEY", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_ENDPOINT_NAME",
                    "HPC_BRIDGE_REMOTE_VENV", "HPC_BRIDGE_ACCOUNT", "HPC_BRIDGE_PARTITION", "HPC_BRIDGE_SCRATCH"):
            monkeypatch.delenv(var, raising=False)
        # a control dir is set (as in production: the preauth handoff needs one); the fake ssh never opens it
        monkeypatch.setattr(config, "_control_settings", lambda: (str(tmp_path / "cm"), 60))
        monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))  # Perlmutter is not catalogued

        async def nothing_online(self, name):  # the Globus web service: no endpoint of ours is online
            return None

        monkeypatch.setattr(SlurmFacility, "find_online_endpoint", nothing_online)

        def create_connection(address, timeout=None, *a, **kw):  # the client's resolver and TCP, for the alias probe
            host = address[0]
            if host not in dns:  # an ssh-config Host is no DNS name
                raise socket.gaierror(8, "nodename nor servname provided, or not known")
            if host == PINNED:
                raise TimeoutError("timed out")
            return contextlib.nullcontext()

        monkeypatch.setattr(socket, "create_connection", create_connection)

    def ssh_log(self) -> list[tuple[str, str]]:
        return [tuple(line.split()) for line in self.log_path.read_text().splitlines() if line.strip()]

    def clear_log(self) -> None:
        self.log_path.write_text("")


def _proven_details() -> FacilityDetails:
    """What the probe proposed and the login-shape canary proved for `perlmutter` (so the cache kept it)."""
    return FacilityDetails(ssh_host=ALIAS, interface="hsn0", env_setup=short_env_setup("{venv}"),
                           scratch_root="/pscratch/sd/a/{user}/.hpc-bridge", partition="debug")


def _cache_proven_perlmutter() -> None:
    FacilityStore().put(ALIAS, _proven_details().model_dump(mode="json"))  # as `_commit_proven_facility` writes it


def _pin_login22() -> None:
    LoginNodeStore().put(EndpointRecord(
        endpoint_id="0f5e8a52-0000-4000-8000-000000000022", login_host=PINNED, alias=ALIAS, user="alice",
        key_path=None, name=ENDPOINT, provisioned_at="2026-10-13T00:00:00+00:00"))


def _app() -> AppCtx:
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = None  # the Globus login gate is not what these tests are about
    return app


# ---- item 1: the stale login-node pin ---------------------------------------------------------------------------------

async def test_1_dead_pin_is_dropped_when_the_alias_is_an_ssh_config_host(monkeypatch, tmp_path):
    """On 0.1.20 `_drop_dead_pin` probed the ALIAS with a raw TCP connect: an ssh-config Host such as 'perlmutter' is no
    DNS name, so the probe read a client-side outage and kept the dead pin. It now probes the HostName ssh resolves."""
    w = World(monkeypatch, tmp_path)
    _cache_proven_perlmutter()
    _pin_login22()
    res = await server._connect_facility(_app(), "perlmutter")
    # the report's error, reproduced through the real bring-up: the cached config, the pin, the first SSH step
    assert res.phase == "failed" and res.notice.startswith(f"CANNOT REACH {PINNED}"), res.notice
    assert w.ssh_log() == [(PINNED, "bringup")]
    # the canonical host answers (CANONICAL is reachable), so the pin is dead and must be dropped
    assert "pin was dropped" in res.notice, res.notice
    assert LoginNodeStore().get(alias=ALIAS, name=ENDPOINT) is None


async def test_1_control_the_same_dead_pin_is_dropped_when_the_alias_is_a_dns_name(monkeypatch, tmp_path):
    """The control for the test above (passed on 0.1.20): the same state and the same timeout, but the alias resolves
    in DNS — and the pin IS dropped. So it is the alias's resolution that keeps the pin, not timeout vs refused."""
    World(monkeypatch, tmp_path, dns=(ALIAS, CANONICAL, PINNED))
    _cache_proven_perlmutter()
    _pin_login22()
    res = await server._connect_facility(_app(), "perlmutter")
    assert res.notice.startswith(f"CANNOT REACH {PINNED}") and "pin was dropped" in res.notice, res.notice
    assert LoginNodeStore().get(alias=ALIAS, name=ENDPOINT) is None


async def test_1_explicit_ssh_host_is_not_silently_ignored_after_the_kept_config_failed(monkeypatch, tmp_path):
    """On 0.1.20 an explicit ssh_host was silently ignored: the config read from facilities.json stayed the session's and
    won over every rung, so the retry went to the same pinned node, unsaid."""
    w = World(monkeypatch, tmp_path)
    _cache_proven_perlmutter()
    _pin_login22()
    app = _app()
    first = await server._connect_facility(app, "perlmutter")
    assert first.notice.startswith(f"CANNOT REACH {PINNED}"), first.notice
    w.clear_log()
    again = await server._connect_facility(app, "perlmutter", ssh_host=CANONICAL)
    # the host the call names is used (or the call is refused without SSH): never the old alias or its pinned node
    went = {host for host, _ in w.ssh_log()}
    assert went <= {CANONICAL}, (went, again.notice)


# ---- item 2: a cached password+OTP facility ----------------------------------------------------------------------------

async def test_2_cached_facility_password_otp_denial_is_the_terminal_handoff(monkeypatch, tmp_path):
    """On 0.1.20 only `key_accepted_second_factor_pending` raised the handoff on the cached/registry bring-up path; a
    password+OTP denial still lists publickey, so a config PROVEN before read NO SSH ACCESS."""
    w = World(monkeypatch, tmp_path)
    _cache_proven_perlmutter()  # proven on an earlier day: the user HAS access (no pin: they hand-deleted it)
    app = _app()
    res = await server._connect_facility(app, "perlmutter")
    assert w.ssh_log() == [(ALIAS, "bringup")]
    assert res.phase == "needs_preauth", res.notice
    assert res.preauth_code_ok is False and res.preauth_command and "-fN" in res.preauth_command  # the terminal handoff
    assert app.pending_preauth is not None and app.pending_preauth[0] == "perlmutter"


async def test_2_first_contact_with_the_same_denial_is_already_the_terminal_handoff(monkeypatch, tmp_path):
    """The contrast the report drew (and its #3 confirmed): with nothing cached the discovery probe classifies the same
    denial with `is_interactive_auth_failure` and hands off. Passed on 0.1.20; guards the probe path."""
    w = World(monkeypatch, tmp_path)
    res = await server._connect_facility(_app(), "perlmutter-nersc", ssh_host=ALIAS)
    assert w.ssh_log() == [(ALIAS, "probe")]
    assert res.phase == "needs_preauth" and res.preauth_code_ok is False and "-fN" in (res.preauth_command or "")


# ---- item 3: the cache entry the user removed lives on in the session -------------------------------------------------

async def test_3_a_removed_cache_record_is_not_brought_up_again_in_the_same_session(monkeypatch, tmp_path):
    """On 0.1.20 a config read from facilities.json stayed in app.session_facilities for the session — not dropped when
    its bring-up failed, nor when its record was removed — so the next connect brought the removed config up again
    instead of probing afresh."""
    w = World(monkeypatch, tmp_path)
    _cache_proven_perlmutter()
    app = _app()
    first = await server._connect_facility(app, "perlmutter")
    assert first.phase in ("failed", "needs_preauth"), first.notice  # NO SSH ACCESS on main
    FacilityStore().remove(ALIAS)  # the user's hand edit of facilities.json
    w.clear_log()
    again = await server._connect_facility(app, "perlmutter", ssh_host=ALIAS)
    # resolved afresh: the record is gone, so a probe of the login node — never the removed config's bring-up
    assert w.ssh_log() == [(ALIAS, "probe")], (w.ssh_log(), again.notice)
    assert again.phase == "needs_preauth", again.notice


async def test_3_a_dropped_cached_config_is_read_again_for_its_host_when_the_call_names_none(monkeypatch, tmp_path):
    """The drop costs nothing: a connect that names no ssh_host (the handoff's resume, `connect_facility(<id>)`) re-reads
    the record for the host the dropped config was read under — here not the facility id — instead of asking for a host."""
    w = World(monkeypatch, tmp_path)
    _cache_proven_perlmutter()
    app = _app()
    first = await server._connect_facility(app, "nersc", ssh_host=ALIAS)
    assert first.phase == "needs_preauth" and app.preauth_resume == "connect_facility('nersc')", first.notice
    again = await server._connect_facility(app, "nersc")
    assert w.ssh_log() == [(ALIAS, "bringup"), (ALIAS, "bringup")] and again.phase == "needs_preauth", again.notice


# ---- item 8: a new facility id for the same host reuses that host's endpoint — by design (#27) ------------------------

def test_8_a_renamed_facility_id_shares_the_hosts_endpoint_by_design():
    """The endpoint name is keyed on the SSH host, not the facility id (`binding._session_endpoint_name`, #27: `midway`
    and `midway3` share one registration). So `perlmutter-nersc` on the same host reuses `hpc-bridge-perlmutter`."""
    details = _proven_details()
    old = binding._entry_from_details("perlmutter", details)
    new = binding._entry_from_details("perlmutter-nersc", details)
    assert old.compute.endpoint_name == new.compute.endpoint_name == ENDPOINT
    other_host = binding._entry_from_details("perlmutter-nersc", details.model_copy(update={"ssh_host": CANONICAL}))
    assert other_host.compute.endpoint_name == "hpc-bridge-perlmutter-nersc-gov"
