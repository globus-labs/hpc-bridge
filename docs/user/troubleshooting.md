# Troubleshooting

Most problems announce themselves in the agent's own words. This page says what each one means and
what to do. The phrases in bold are the ones hpc-bridge itself uses, so you can match them. Anything
not covered here: [open an issue](https://github.com/globus-labs/hpc-bridge/issues).

## First contact

**"NO ACCOUNT at this facility"** (facility-run endpoint). The facility's endpoint could not map
your Globus identity to a local account. It is terminal: no retry changes it, nothing was started or
billed. You need an account on that machine with your Globus identity added to the endpoint's
mapping; ask the facility's support and quote the identity the agent shows. If you have an account
but logged in with a different, unlinked identity, log out of Globus in your browser and log in with
the one the facility knows; see [The Globus login](login.md).

**The agent asks for a one-time code.** A facility that requires a TOTP or Duo passcode at SSH login is
asking hpc-bridge for it, and the agent relays the request; give it the current code from your authenticator.
It is single-use and expires within a minute. The agent must never ask for a *password*; if it does, see below.

**"UNKNOWN HOST KEY for <host>"**. hpc-bridge connects only to hosts your own `ssh` already trusts.
Open your own terminal, run `ssh <user>@<host>` once, check the fingerprint the facility publishes and
accept it, then ask the agent to connect again. If the message says the key *changed*, stop and check
with the facility before accepting anything. Nothing was started or billed.

**"NO SSH ACCESS to <host> as <login name>"** (SSH bootstrap). The login node refused your SSH. The
message names the login name that was tried and where it came from. You need an account there and
key-based SSH: put the host's `User` and `IdentityFile` in `~/.ssh/config`, or set
`HPC_BRIDGE_SSH_USER` and `HPC_BRIDGE_SSH_KEY`, and connect again.

**"CANNOT REACH <host>"**. The login node did not answer at all: wrong host name, no network or VPN,
or the facility is down. Nothing was started.

**A request to run an `ssh` command in your own terminal** (the agent calls this pre-authentication).
The facility needs a password, which the agent never handles (a one-time code is different: see above).
Run the command it gives you in your own terminal; it opens a session hpc-bridge then shares, and the
connect continues with no further prompts.

**The login link fails when opened**. It is single-use: if the page already said you could return,
the login has completed and you can just carry on. If it has expired, the agent issues a fresh one.

## While a block is starting

**"allocating nodes"** is a normal queue wait; the agent polls. Two variants are not:

- **"Not a queue wait: fix the config/partition"**: the scheduler refused the request, usually a
  partition or account the facility does not accept. Change it and try again.
- **"NO LONGER transient"** after repeated **"refused as TRANSIENT"**: another session using the
  same Globus identity is starting or holding an endpoint at this facility, or the facility's
  endpoint manager is wedged. End the other session or wait a few minutes.

**"reports OFFLINE"** on a facility-run endpoint. The facility's endpoint is down. Only the facility
can restart it.

## During and after work

**A command comes back "running" with a task id**. It is still going; the agent polls it. This is
expected for anything longer than about two minutes.

**"ORPHANED"**. The endpoint went away while a task was pending, so the task's result is lost.
Reconnect and run it again.

**Stop reports "draining"** on a facility-run endpoint. That is the honest final state; the
facility's idle timeout reclaims the block. See [Costs and stopping](costs-and-stopping.md).

## The agent has no hpc-bridge tools

The plugin's server did not start. Look at the server's error output in `/plugin` (its **Errors** tab)
or `/mcp`. The usual cause is `uv` not being found by the process that starts the server: the message there reads
**"'uv' was not found on PATH or in the usual install locations"**, and the fix is to install `uv` or
symlink yours into `~/.local/bin`, then `/reload-plugins`. `uv run` resolves the server's Python
dependencies itself, so a dependency error shows in the same place.

## SSH multiplexing errors

**"ControlPath too long"** means hpc-bridge's local state directory is deep enough that the SSH
socket path exceeds the operating system's limit. hpc-bridge falls back to a short path on its own;
if you set `HPC_BRIDGE_STATE_DIR` yourself, keep it short.

## Too many failed logins

Facilities run intrusion protection on their login nodes. Repeated failed SSH attempts from your
address, for instance connecting several times with the wrong key, can get the address banned for an
hour. Fix the key before retrying rather than retrying in a loop.

## The agent asked me for a password

It should never. Do not provide one; end the session and
[report it](https://github.com/globus-labs/hpc-bridge/issues). Every credential hpc-bridge
uses is entered by you in a browser or in your own terminal.
