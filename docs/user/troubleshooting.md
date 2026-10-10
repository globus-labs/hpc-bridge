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

**"hpc-bridge: could not build globus-compute-endpoint <version> (Python <version>) in <path>"** (SSH
bootstrap). Each time hpc-bridge starts the endpoint it checks the environment it keeps on the login
node (under `~/hpc-bridge/`, named for the endpoint version and Python) and builds it when it is missing
or broken; this time the build failed, and any previous environment was put back as it was. The text after
the path, and the lines before it, say why: no network to PyPI (or to astral.sh, when `uv` is being
installed), a full home quota, or a fresh install that still does not run. Fix that and connect again.

**"hpc-bridge: <path> exists and is not a venv: move it away"**. Something other than a Python
environment sits where hpc-bridge keeps its own; hpc-bridge never replaces it. Move it, or set
`HPC_BRIDGE_REMOTE_VENV` to another path, and connect again.

**"hpc-bridge: cannot create <path>"** or **"cannot create <path>.lock: <reason>"**. hpc-bridge could not
make its directory under `~/hpc-bridge/` at all: the home directory it expects does not exist there (set
`HPC_BRIDGE_REMOTE_VENV` to a path under your real home), or the reason says the disk quota is full
(remove environments you no longer use; see [What hpc-bridge does on your cluster](facilities.md)).

**"hpc-bridge: <path>.lock is held by another build"**. Another session was building the same
environment and had not finished after several minutes. A lock older than 15 minutes is taken over
automatically, so if this connect comes later, it needs no help. A younger lock belongs to a build that
may still be running: wait, then connect again.

**"hpc-bridge: cannot move <path> aside"**. The environment needs rebuilding but could not be renamed
(it is busy or not yours to rename); nothing was changed. Fix that and connect again.

**"this endpoint runs with the environment it was started with"**. The facility's `env_setup` was
changed, but the endpoint was already running and is being reused, so its blocks keep the environment it
was started with. To apply the new one, ask the agent to tear the endpoint down and connect again.

**"hpc-bridge: no working hpc-bridge venv beside <path>"** (stop or teardown). A stop never builds an
environment (it would not finish in time), and none that runs is left under `~/hpc-bridge/`: the one the
endpoint was started from was removed or broke. Stop the `globus-compute-endpoint` process on the login
node yourself; the next connect builds a fresh environment.

## While a block is starting

**"allocating nodes"** is a normal queue wait; the agent polls. Two variants are not:

- **"Not a queue wait: fix the config/partition"**: the scheduler refused the request, usually a
  partition or account the facility does not accept. Change it and try again.
- **"NO LONGER transient"** after repeated **"refused as TRANSIENT"**: another session using the
  same Globus identity is starting or holding an endpoint at this facility, or the facility's
  endpoint manager is wedged. End the other session or wait a few minutes.

**"reports OFFLINE"** on a facility-run endpoint. The facility's endpoint is down. Only the facility
can restart it.

**A pilot "already FINISHED"** whose job stderr (in
`~/.globus_compute/hpc-bridge-<host>/submit_scripts/` on the login node) says **"hpc-bridge: the
endpoint venv <path> (built on <arch>) does not run on <node> (<arch>)"**. The compute node cannot use
the environment hpc-bridge built on the login node, and the job stopped there rather than fail later. If
the two architectures differ (built on `x86_64`, the node `aarch64`), that partition's nodes are a
different CPU type from the login node: choose a partition of the login node's type; hpc-bridge cannot
yet build a second environment for them. If they match, the environment broke after it was built (the
error above that line says how): ask the agent to tear the endpoint down and connect again, and the
login node rebuilds it. A reconnect alone reuses the running endpoint and rebuilds nothing. If the
teardown fails too, with the same error, stop the `globus-compute-endpoint` process on the login node
yourself first.

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
