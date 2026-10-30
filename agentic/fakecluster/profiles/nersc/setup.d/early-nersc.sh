#!/usr/bin/env bash
# `nersc` profile, EVERY role, before the role's own work (the entrypoint's apply_profile → `run_setup early`):
#   * the pool users' homes move to /global/homes/<initial>/<user> (`usermod -d`: /etc/passwd is per container, so the
#     login AND the compute nodes must agree — a batch job's $HOME comes from the compute node's passwd)
#   * /global -> /home/.global: the homes stay on the shared `home` volume (mounted at /home on the login and compute
#     nodes), so every node sees them; /home itself stays root-owned 755 and holds no user's home, so a path built as
#     /home/<user> is refused (`mkdir: Permission denied`) as it is on Perlmutter
#   * the noisy startup file: a block prepended to /etc/skel/.bashrc, ABOVE Ubuntu's "If not running interactively,
#     don't do anything" guard, so a non-interactive shell (an ssh command, `bash -lc`) prints it too. The login
#     entrypoint copies skel into each new home. Lmod's module-swap chatter (echoed: no Lmod here) and git's error on a
#     stale ~/.gitconfig.lock (real: setup.d/login-nersc.sh leaves the lock), all on STDERR, ~700 characters.
set -u
mkdir -p /home/.global/homes && chmod 755 /home/.global /home/.global/homes
ln -sfn /home/.global /global
for u in "${POOL_USERS[@]}"; do
  h="/global/homes/${u:0:1}/$u"
  [ "$(getent passwd "$u" | cut -d: -f6)" = "$h" ] || usermod -d "$h" "$u"
done
if ! grep -q 'site login environment' /etc/skel/.bashrc; then
  {
    cat <<'NOISE'
# --- site login environment (module defaults, git settings) ---
echo 'Lmod is automatically replacing "PrgEnv-gnu/8.5.0" with "PrgEnv-nvidia/8.5.0".' >&2
echo 'Lmod is automatically replacing "gcc-native/12.3" with "nvidia/24.5".' >&2
echo 'Due to MODULEPATH changes, the following have been reloaded:' >&2
echo '  1) cray-libsci/24.07.0     2) cray-mpich/8.1.30' >&2
echo 'The following have been reloaded with a version change:' >&2
echo '  1) cudatoolkit/12.2 => cudatoolkit/12.4     2) craype-accel-nvidia80 => craype-accel-nvidia80' >&2
git config --global core.editor vim
git config --global pull.rebase false
git config --global init.defaultBranch main
# --- end site login environment ---

NOISE
    cat /etc/skel/.bashrc
  } > /etc/skel/.bashrc.new && mv /etc/skel/.bashrc.new /etc/skel/.bashrc
fi
echo "[nersc] homes under /global/homes (-> /home/.global); noisy skel .bashrc"
