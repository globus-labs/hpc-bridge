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

**"No facility connected"**. The agent tried to run a command, start a block or read the login node
before connecting to a facility. Nothing was started. It should call `connect_facility` first; if the
message says a connect only *proposed* a configuration, review that proposal with the agent and let
it finish the connect. If it says a connect is **waiting on a one-time login**, finish that first: give
the agent the current one-time code, or run the `ssh` command it shows in your own terminal, then let
it connect again. Connecting again before that only repeats the failed login.

**"could not build hpc-bridge's endpoint venv for <path>"** (SSH facility). Each time hpc-bridge starts the
endpoint it checks the Python environment it keeps on the login node (under `~/hpc-bridge/`), and builds a new one
when it is missing or broken. This time the build failed; the message ends with the build's own last error line: no
network to PyPI (or to astral.sh when `uv` is being installed), a full home quota, or a home directory that is not
where hpc-bridge expects it (`Permission denied` creating `/home/<you>/hpc-bridge`: set `HPC_BRIDGE_REMOTE_VENV` to a
path under your real home). The message says where the failed build is kept (`<path>.failed.<time>`), if anything
of it was made, and the environment that was there before is left as it was. Fix the cause and connect again. A login node with no internet can still be used: put a
venv with `globus-compute-endpoint` at `HPC_BRIDGE_REMOTE_VENV` yourself (same endpoint version and Python as your
client); hpc-bridge uses it when it passes the same check.

**"this node's uv (…) is too old to build hpc-bridge's venv"**. The login node's own `uv` predates
`uv venv --relocatable` (0.2.31). Upgrade it (`uv self update`, or a newer site module), or remove it from your PATH
there so hpc-bridge installs its own into `~/.local/bin`, and connect again. Nothing was started.

**"<path> exists and is not a venv"**. Something other than a Python environment sits where hpc-bridge keeps its
own; hpc-bridge never replaces it. Move it, or set `HPC_BRIDGE_REMOTE_VENV` to another path, and connect again.

**"another bring-up put a venv at <path> first, and it fails its check"**. Two sessions built the environment at the
same moment and the other one's arrived first but does not work. Connect again; hpc-bridge builds a working one.

**"checking hpc-bridge's endpoint venv … timed out"**. The login node's filesystem was too slow to answer within a
minute. Nothing was changed; connect again shortly.

**"this endpoint was started with a different env_setup"**. The facility's `env_setup` was changed, but the endpoint
was already running and is being reused, so its blocks keep the environment it was started with. That only matters
if its blocks fail or you meant the change: then ask the agent to tear the endpoint down and connect again.

**"no working globus-compute-endpoint venv of hpc-bridge's"** (teardown). A teardown never builds an environment.
It runs the endpoint's command in the environment the endpoint was started with, even after a reconnect changed
`env_setup`, and none under `~/hpc-bridge/` answered. The endpoint's blocks were cancelled; stop the `globus-compute-endpoint`
process on the login node yourself, and the next connect builds a fresh environment.

## While a block is starting

**"allocating nodes"** is a normal queue wait; the agent polls. Two variants are not:

- **"Not a queue wait: fix the config/partition"**: the scheduler refused the request, usually a
  partition or account the facility does not accept. Change it and try again.
- **"NO LONGER transient"** after repeated **"refused as TRANSIENT"**: another session using the
  same Globus identity is starting or holding an endpoint at this facility, or the facility's
  endpoint manager is wedged. End the other session or wait a few minutes.

**"reports OFFLINE"** on a facility-run endpoint. The facility's endpoint is down. Only the facility
can restart it.

**"the stored Globus login no longer works (expired or revoked)"**. Globus refused to renew the login stored on
this machine: it was revoked (for instance at app.globus.org) or it has expired. Every command and every block start
fails until you log in again, and a connect to a facility-run endpoint may add that the facility template was not
readable. Nothing is wrong with the facility, its configuration or the partition. Ask the agent to force a fresh
Globus login (`authenticate(force=True)`), complete it in the browser, then carry on.

**A pilot "already FINISHED" whose job said "hpc-bridge: endpoint venv <path> (built on <arch>) does not run on
<node> (<arch>)"**. The compute node cannot use the environment hpc-bridge built on the login node, and the job
stopped there rather than fail later; a new block fails the same way. If the two architectures differ (built on
`x86_64`, the node `aarch64`, as on a cluster with x86 login nodes and Arm compute nodes), that partition's nodes are a
different CPU type from the login node, and rebuilding does not help: the environment is built on the login node
again. Choose a partition of the login node's type, or the facility's own multi-user endpoint if `list_facilities`
shows one (its workers bring their own environment). Changing `env_setup` does not fix it: the running endpoint keeps
the environment it was started with, and an empty or hand-written one only means hpc-bridge builds no venv. If the
facility provides an environment for both (a site `module load`), tear the endpoint down first, then connect with that
`env_setup`. hpc-bridge cannot yet build a second environment for them. If they match, the
environment runs on the login node but not on that node (the line above it in the job's error output says why): ask
the agent to tear the endpoint down and connect again, which rebuilds it if it is broken on the login node too.

## During and after work

**A command comes back "running" with a task id**. It is still going; the agent polls it. This is
expected for anything longer than about two minutes.

**"ORPHANED"**. The endpoint went away while a task was pending, so the task's result is lost.
Reconnect and run it again.

**Stop reports "draining"** on a facility-run endpoint. That is the honest final state; the
facility's idle timeout reclaims the block. See [Costs and stopping](costs-and-stopping.md).

## The agent has no hpc-bridge tools

The plugin's server did not start. Claude Code may show it as **`Failed to reconnect to plugin:hpc-bridge …
CONNECTION_CLOSED`**: the server stopped as it started. Look at the server's error output in `/plugin` (its
**Errors** tab) or `/mcp`. The usual cause is `uv` not being found by the process that starts the server: the message there reads
**"'uv' was not found on PATH or in the usual install locations"**, and the fix is to install `uv` or
symlink yours into `~/.local/bin`, then `/reload-plugins`. `uv run` resolves the server's Python
dependencies itself, so a dependency error shows in the same place.

On Windows on Arm, a **`Failed to build cryptography`** error there means the server ran on an Arm Python,
for which that dependency has no build. The plugin picks an x64 Python by itself unless `UV_PYTHON` is set;
set it to `cpython-3.13-windows-x86_64-none`, or unset it, then `/reload-plugins`. To see the error
outside Claude Code, start the server by hand from the plugin's folder:
`uv run --locked --extra integration hpc-bridge` (it waits silently when it works).

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
