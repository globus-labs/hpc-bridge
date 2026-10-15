"""In-session one-time code (complete_preauth) — hermetic, with a fake `ssh` that emulates OpenSSH's askpass
behaviour: it calls $SSH_ASKPASS with a prompt, compares the answer with the expected code, and honours
`-O check` against a marker file it writes on success."""
from __future__ import annotations

import stat

import pytest

from hpc_bridge import preauth
from hpc_bridge.facility.remote import NeedsPreauth, RemoteEndpointCLI, SshTarget, offers_one_time_code
from hpc_bridge.notices import _needs_preauth_result

FAKE_SSH = r'''#!/bin/sh
# fake ssh: -O check -> 0 iff marker exists; otherwise a keyboard-interactive login answered via $SSH_ASKPASS
marker="$FAKE_MARKER"
for a in "$@"; do [ "$a" = "-O" ] && { [ -f "$marker" ] && exit 0 || exit 255; }; done
[ -n "$SSH_ASKPASS" ] || { echo "no askpass" >&2; exit 255; }
[ "$SSH_ASKPASS_REQUIRE" = "force" ] || { echo "askpass not forced" >&2; exit 255; }
answer=$("$SSH_ASKPASS" "$FAKE_PROMPT") || { echo "Permission denied (keyboard-interactive)." >&2; exit 255; }
if [ "$answer" = "$FAKE_CODE" ]; then touch "$marker"; exit 0; fi
echo "Permission denied (keyboard-interactive)." >&2; exit 255
'''


@pytest.fixture
def fake_ssh(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ssh = bin_dir / "ssh"
    ssh.write_text(FAKE_SSH)
    ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("FAKE_MARKER", str(tmp_path / "master.sock"))
    monkeypatch.setenv("FAKE_CODE", "123456")
    monkeypatch.setenv("FAKE_PROMPT", "Verification code: ")
    return str(ssh)


def _target(tmp_path):
    cd = tmp_path / "cm"
    cd.mkdir(exist_ok=True)
    return SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir=str(cd))


def test_valid_code_opens_the_master(tmp_path, fake_ssh):
    ok, why = preauth.open_master_with_code(_target(tmp_path), "123456", state_dir=tmp_path, ssh_bin=fake_ssh)
    assert ok, why
    assert "123456" not in why and "one-time code was accepted" in why
    assert not list(tmp_path.glob("otp-*"))  # the code file is gone


def test_wrong_or_expired_code_is_reported_without_echoing_it(tmp_path, fake_ssh):
    ok, why = preauth.open_master_with_code(_target(tmp_path), "999999", state_dir=tmp_path, ssh_bin=fake_ssh)
    assert not ok and "not accepted" in why and "999999" not in why


def test_password_prompt_is_refused_and_routed_to_the_terminal(tmp_path, fake_ssh, monkeypatch):
    monkeypatch.setenv("FAKE_PROMPT", "u@login.expanse.sdsc.edu's password: ")
    ok, why = preauth.open_master_with_code(_target(tmp_path), "123456", state_dir=tmp_path, ssh_bin=fake_ssh)
    assert not ok and "PASSWORD" in why and "own terminal" in why


@pytest.mark.parametrize("bad", ["hunter2!", "correct horse battery", "", "x" * 40, "12 34"])
def test_non_codes_are_rejected_before_any_ssh(tmp_path, bad):
    assert not preauth.looks_like_code(bad)
    ok, why = preauth.open_master_with_code(_target(tmp_path), bad, state_dir=tmp_path, ssh_bin="/nonexistent/ssh")
    assert not ok and "never sends passwords" in why


def test_multiplexing_off_cannot_share_a_connection(tmp_path, fake_ssh):
    t = SshTarget(host="h", user="u")  # no control_dir
    ok, why = preauth.open_master_with_code(t, "123456", state_dir=tmp_path, ssh_bin=fake_ssh)
    assert not ok and "multiplexing is off" in why


def test_needs_preauth_result_offers_the_code_path_only_when_keyboard_interactive():
    t = SshTarget(host="h", user="u", control_dir="/tmp/cm")
    res = _needs_preauth_result("f", t, otp_ok=True)
    assert res.preauth_code_ok and "complete_preauth(code)" in res.notice and "NEVER ask for a password" in res.notice
    res2 = _needs_preauth_result("f", t)
    assert not res2.preauth_code_ok and "complete_preauth" not in res2.notice and "THEIR OWN terminal" in res2.notice


def test_offers_one_time_code_means_the_key_passed_and_only_the_second_factor_remains():
    # Expanse: publickey is NO LONGER listed -> the key was accepted; keyboard-interactive is the code prompt
    assert offers_one_time_code("Permission denied (gssapi-with-mic,keyboard-interactive,hostbased).")
    # Anvil stranger: publickey STILL listed -> the key was refused; 2FA being offered does not make it a handoff
    assert not offers_one_time_code("Permission denied (publickey,gssapi-keyex,gssapi-with-mic,keyboard-interactive,hostbased).")
    assert not offers_one_time_code("Permission denied (publickey,password).")
    assert not offers_one_time_code("Permission denied (publickey).")
    assert NeedsPreauth(SshTarget(host="h"), otp_ok=True).otp_ok is True


async def test_complete_preauth_tool_flow(tmp_path, fake_ssh, monkeypatch):
    from hpc_bridge import server, state
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    monkeypatch.setattr(state, "_state_dir", lambda: tmp_path)
    real = preauth.open_master_with_code
    monkeypatch.setattr(preauth, "open_master_with_code",
                        lambda target, code, *, state_dir, **kw: real(target, code, state_dir=state_dir, ssh_bin=fake_ssh))
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._complete_preauth(app, "123456")
    assert res.phase == "failed" and "connect_facility first" in res.notice  # nothing pending
    app.pending_preauth = ("expanse", _target(tmp_path))
    res = await server._complete_preauth(app, "not a code!")
    assert res.phase == "failed" and "never sends passwords" in res.notice and res.preauth_command
    res = await server._complete_preauth(app, "123456")
    assert res.phase == "opened" and app.pending_preauth is None and "connect_facility('expanse')" in res.notice
    assert "123456" not in (res.notice or "")
    # the call to repeat is whichever asked for the code — teardown's gate names itself
    app.pending_preauth = ("expanse", _target(tmp_path))
    app.preauth_resume = "teardown_endpoint()"
    res = await server._complete_preauth(app, "123456")  # the fake ssh accepts exactly $FAKE_CODE
    assert res.phase == "opened" and "Call teardown_endpoint() again" in res.notice and app.preauth_resume is None


async def test_a_password_passed_as_the_code_is_refused_and_the_user_told_to_change_it_without_repeating_it(tmp_path):
    # A live password_pusher run (2026-10-15) asked the user to rotate the password by quoting it back. The refusal of
    # a non-code is where the server knows a password may just have gone through the chat: it says to have it changed,
    # and never says it again.
    from hpc_bridge import server
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.pending_preauth = ("expanse", _target(tmp_path))
    pw = "-".join(("Granite", "Heron", "Velvet", "2026"))
    res = await server._complete_preauth(app, pw)
    assert res.phase == "failed" and res.preauth_command and app.pending_preauth is not None
    assert "transcript" in res.notice and "change it" in res.notice and "never repeat" in res.notice
    assert pw not in res.notice and "Granite" not in res.notice


# ---- the registry / cached-entry bootstrap path raises the handoff too (Expanse live, 2026-09-04) ------------

_NO_FINDER = "no-finder"


async def _registry_connect(monkeypatch, denial: str, *, auth_method="ssh-key", master_alive=False,
                            online=_NO_FINDER, provision_ok=False):
    """`online`: what the facility's web-only `find_online_endpoint` answers (None = nothing online); the
    default leaves the facility without a finder at all. `provision_ok`: the bootstrap succeeds ('warm')
    instead of raising `denial`. `calls` records ("online", name) for the finder and the shape for provision."""
    from types import SimpleNamespace

    from hpc_bridge import binding, connect, warmth
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility, fake_entry

    class _CLI:
        target = SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir="/tmp/cm")

    fac = FakeFacility()
    fac.cli = _CLI()
    calls = []
    if online != _NO_FINDER:
        fac.profile = SimpleNamespace(endpoint_name="hpc-bridge-login.expanse.sdsc.edu")

        async def find_online_endpoint(name):
            calls.append(("online", name))
            return online

        fac.find_online_endpoint = find_online_endpoint
    entry = fake_entry(id="expanse", facility_key="sdsc")
    entry.ssh_host = "login.expanse.sdsc.edu"
    entry.auth_method = auth_method
    entry.allocation = None  # like Expanse: no allocation listing, so a warm login shape ends in needs_account

    class _Cat:
        async def get(self, fid):
            return entry if fid == "expanse" else None

    monkeypatch.setattr(binding, "make_catalog", lambda: _Cat())
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    monkeypatch.setattr(connect, "_master_alive", lambda target: master_alive)

    async def provision(app, shape, **kw):
        calls.append(shape)
        if provision_ok:
            return "warm"
        raise RuntimeError(f"remote whoami failed: amcsweeneyellerm@login.expanse.sdsc.edu: {denial}")

    monkeypatch.setattr(warmth, "_provision", provision)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = None

    async def _no_login(*a, **k):
        raise AssertionError("unused")

    res = await connect._connect_facility(app, "expanse", run_login=_no_login)
    return res, app, calls


async def test_registry_entry_denied_with_keyboard_interactive_is_the_code_handoff(monkeypatch):
    res, app, _ = await _registry_connect(monkeypatch, "Permission denied (gssapi-with-mic,keyboard-interactive,hostbased).")
    assert res.phase == "needs_preauth" and res.preauth_code_ok and "complete_preauth" in res.notice
    assert app.pending_preauth is not None and app.pending_preauth[0] == "expanse"


async def test_registry_entry_denied_publickey_only_is_still_no_ssh_access(monkeypatch):
    res, app, _ = await _registry_connect(monkeypatch, "Permission denied (publickey).")
    assert res.phase == "failed" and res.notice.startswith("NO SSH ACCESS") and app.pending_preauth is None


async def test_mfa_otp_entry_hands_off_before_any_ssh_when_no_master_is_open(monkeypatch):
    res, _app, calls = await _registry_connect(monkeypatch, "irrelevant", auth_method="mfa-otp", master_alive=False)
    assert res.phase == "needs_preauth" and res.preauth_code_ok and calls == []  # no failing bootstrap attempt


# ---- reuse needs no SSH, so it is checked BEFORE a code is requested (live 2026-09-04: a code was typed, then
# ---- the very next step reused the online endpoint over the web — the code was never used) --------------------

async def test_mfa_otp_entry_reuses_an_online_endpoint_without_asking_for_a_code(monkeypatch):
    res, app, calls = await _registry_connect(monkeypatch, "irrelevant", auth_method="mfa-otp", master_alive=False,
                                              online="eid-online", provision_ok=True)
    assert calls == [("online", "hpc-bridge-login.expanse.sdsc.edu"), "login"]  # web check, then provision — no handoff
    assert res.phase == "needs_account" and res.reused and app.pending_preauth is None
    assert app.state.endpoint_id == "eid-online" and app.state.reused


async def test_mfa_otp_entry_hands_off_only_when_nothing_is_online(monkeypatch):
    res, app, calls = await _registry_connect(monkeypatch, "irrelevant", auth_method="mfa-otp", master_alive=False,
                                              online=None)
    assert calls == [("online", "hpc-bridge-login.expanse.sdsc.edu")]  # the web was asked; SSH was not attempted
    assert res.phase == "needs_preauth" and res.preauth_code_ok and app.pending_preauth is not None
    assert app.preauth_resume == "connect_facility('expanse')"


async def test_mfa_otp_entry_proceeds_when_the_master_is_already_open(monkeypatch):
    # with a live master the bootstrap runs (here it raises a non-auth error, proving provision was attempted)
    res, _app, calls = await _registry_connect(monkeypatch, "Connection timed out", auth_method="mfa-otp", master_alive=True)
    assert calls == ["login"] and res.phase == "failed" and res.notice.startswith("CANNOT REACH")


async def test_registry_stranger_on_a_2fa_site_is_no_ssh_access_not_a_handoff(monkeypatch):
    # Anvil offers keyboard-interactive to everyone; a stranger's key is refused (publickey still listed)
    res, app, _ = await _registry_connect(
        monkeypatch, "Permission denied (publickey,gssapi-keyex,gssapi-with-mic,keyboard-interactive,hostbased).")
    assert res.phase == "failed" and res.notice.startswith("NO SSH ACCESS") and app.pending_preauth is None


def test_preauth_argv_carries_the_host_key_alias_for_a_pinned_node():
    # after `rebind` the pinned FQDN is verified against the alias the user trusted — the code-opener too
    cli = RemoteEndpointCLI(SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir="/tmp/cm"), "true")
    cli.rebind("login02.expanse.sdsc.edu")
    argv = cli.target.preauth_argv()
    assert "HostKeyAlias=login.expanse.sdsc.edu" in argv and argv[-1] == "u@login02.expanse.sdsc.edu"
    assert "HostKeyAlias" not in " ".join(SshTarget(host="h", user="u", control_dir="/tmp/cm").preauth_argv())


def test_host_key_prompt_is_refused_and_explained(tmp_path, fake_ssh, monkeypatch):
    monkeypatch.setenv("FAKE_PROMPT", "The authenticity of host 'login02 (1.2.3.4)' can't be established. Are you sure you want to continue connecting (yes/no)?")
    ok, why = preauth.open_master_with_code(_target(tmp_path), "123456", state_dir=tmp_path, ssh_bin=fake_ssh)
    assert not ok and why.startswith("UNKNOWN HOST KEY for login.expanse.sdsc.edu") and "fresh code" in why


def test_needs_preauth_notice_explains_the_pinned_node_second_connection():
    cli = RemoteEndpointCLI(SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir="/tmp/cm"), "true")
    cli.rebind("login02.expanse.sdsc.edu")
    res = _needs_preauth_result("expanse", cli.target, otp_ok=True)
    assert res.notice.startswith("The endpoint runs on login node login02.expanse.sdsc.edu")
    assert "one code per session" in res.notice and "not to login.expanse.sdsc.edu" in res.notice and "complete_preauth" in res.notice

