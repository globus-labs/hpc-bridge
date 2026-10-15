#!/usr/bin/env bash
# The PROFILE SWEEP: every fake-cluster profile, brought up clean (--reset-cluster), with the scenarios that belong to
# it — the regression baseline across cluster shapes. Sequential (one cluster at a time); each profile's cells run
# through run_suite with the usual node gate. Results land in agentic/runs/ (bundles) and a per-sweep summary file.
#
#   agentic/sweep_profiles.sh [--models claude-opus-5] [--profiles default,site,...]
#   HPCB_OPERATOR=pi agentic/sweep_profiles.sh      # the FREE wave: codex / pi / hermes21 on the ALCF model
#
# The agent model defaults to run_suite's DEFAULT_MODEL (claude-opus-5); the human-sim is Haiku. Never Fable unless
# asked. `mep` is skipped unless HPCB_MEP_EMAIL is set in agentic/.env (its managers register a contact address).
# What is NOT swept, and why, is the list in excluded() below (the summary prints it): every scenario that can run on
# the fake target is in cells() or there — test_sweep_script.py enforces it, so a new scenario cannot drop out of the
# sweeps unnoticed. The paid wave is the default (the Claude operator runs every cell); with HPCB_OPERATOR=codex|pi|hermes21
# the sweep is the free wave and leaves out paid_only() — what those one-shot operators cannot drive (a persona, PHASES, a
# hook that needs the SDK interrupt). test_sweep_script.py keeps that list equal to what run.py would report SKIPPED.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODELS="claude-opus-5"; PROFILES="default,site,totp,pbs,lmod,f2b,polaris,internal,hostile,mep"
while [ $# -gt 0 ]; do case "$1" in --models) MODELS="$2"; shift 2;; --profiles) PROFILES="$2"; shift 2;; *) echo "unknown arg $1"; exit 2;; esac; done
[ -f "$HERE/agentic/.env" ] && while IFS= read -r line || [ -n "$line" ]; do case "$line" in ''|\#*) continue ;; esac; k="${line%%=*}"; [ -z "${!k+x}" ] && export "$line"; done < "$HERE/agentic/.env"
STAMP="$(date +%Y%m%d-%H%M)"
SUMMARY="$HERE/agentic/runs/sweep-$STAMP.md"; mkdir -p "$HERE/agentic/runs"
echo "# profile sweep $STAMP — target fake, models $MODELS" > "$SUMMARY"

cells() {  # cells <profile> -> the scenario list (and concurrency) for that profile
  case "$1" in
    default)  echo "happy_path,gated_provision,long_task_via_handle,endpoint_reuse,endpoint_reuse_chain,session_persistence,byo_teardown_clean,spend_refusal,unknown_host_key,zero_config_list,orphaned_task,draining_restop,stop_while_running,spend_revoked,block_reaped_resume,long_output,no_facility_first,venv_broken_repair,venv_arch_mismatch,endpoint_upgrade_reuse,reap_replaced_block,esc_mid_call,teardown_slow_fs 2" ;;
    site)     echo "rich_gate,partition_choice,gpu_rule,submit_policy_rejected,login_pin_teardown,slurm_worker_died,gated_provision,happy_path,hand_batch_job,ssh_account_floor 3" ;;
    totp)     echo "otp_preauth 1" ;;
    pbs)      echo "happy_path,gated_provision 2" ;;
    lmod)     echo "lmod_bootstrap 1" ;;
    f2b)      echo "f2b_stranger,f2b_banned 1" ;;
    polaris)  echo "polaris_filesystems 1" ;;
    internal) echo "internal_hostnames 1" ;;
    hostile)  echo "canary_login_discovery,canary_compute_output 1" ;;  # per-node canary channels are shared → SERIAL, one at a time
    mep)      echo "fake_mep_compute,fake_mep_stop_names_block,fake_mep_no_account,fake_mep_username_not_account,term_switch_reasks,preset_account_scoped,terms_prestated,terms_wanted,fake_mep_stop_while_provisioning,mep_hand_batch_job,mep_hand_batch_job_ask 1" ;;
    *) echo "" ;;
  esac
}

excluded() {  # scenarios that could run on the fake target but are NOT in this sweep — one per line: <name> <why>
  cat <<'EXCLUDED'
aurora_pbs_bringup            manual only: a live ALCF Aurora recipe (allocation expired; cannot run in the jail)
idle_release_kill             manual only: never green; documents the #21 kill that no_detached_long_job_on_slurm grades
no_ssh_access                 a globus1 spot check (fail2ban cooldown 11 min); f2b_stranger covers it here
repl_interaction              the REPL benchmark: agentic/repl_sweep.py runs it, alternating with repl_baseline_local
repl_baseline_local           only alongside repl_interaction (agentic/repl_sweep.py): the local reference, no HPC
mep_compute_only              globus1: the real globus-labs MEP (its world check reads glabs-gc's queue there)
stranger_mep_walk             globus1: the real globus-labs MEP, a block + the facility's 12-min settle
registry_over_cache           the registry tier: the real globus-labs MEP entry (--target globus1, or fake/default)
mep_no_account                the registry tier: the real globus-labs MEP + a second, unmapped Globus identity
needs_login_paste             the registry tier: a logged-out connect to the real registry's globus-labs entry
saturation                    globus1: holds every node for 25 min (re-homing it to fake is a later change)
long_job_30m                  globus1: a 20-min job past the idle-release window (re-baselining on fake is later)
EXCLUDED
}

paid_only() {  # swept scenarios the free CLI operators cannot drive — the paid (Claude) wave only; one name per line
  cat <<'PAID'
gated_provision
spend_refusal
endpoint_reuse_chain
unknown_host_key
spend_revoked
block_reaped_resume
esc_mid_call
rich_gate
partition_choice
otp_preauth
fake_mep_username_not_account
terms_prestated
terms_wanted
mep_hand_batch_job_ask
PAID
}

OPERATOR="${HPCB_OPERATOR:-claude}"
case "$OPERATOR" in
  codex|pi|hermes21) FREE_WAVE=1; echo "free wave: operator $OPERATOR — leaving out paid_only()" | tee -a "$SUMMARY" ;;
  *) FREE_WAVE= ;;
esac

IFS=, read -ra PROFS <<< "$PROFILES"
for prof in "${PROFS[@]}"; do
  read -r scen conc <<< "$(cells "$prof")"
  [ -n "$scen" ] || { echo "sweep: unknown profile $prof"; continue; }
  if [ -n "$FREE_WAVE" ]; then
    dropped="$(tr ',' '\n' <<< "$scen" | grep -xF -f <(paid_only) | paste -sd, -)"
    scen="$(tr ',' '\n' <<< "$scen" | grep -vxF -f <(paid_only) | paste -sd, -)"
    [ -z "$dropped" ] || echo "## $prof — free wave leaves out (paid_only): $dropped" | tee -a "$SUMMARY"
    [ -n "$scen" ] || { echo "## $prof — SKIPPED (every cell is paid-only)" | tee -a "$SUMMARY"; continue; }
  fi
  if [ "$prof" = mep ] && [ -z "${HPCB_MEP_EMAIL:-}" ]; then
    echo "## $prof — SKIPPED (HPCB_MEP_EMAIL unset in agentic/.env)" | tee -a "$SUMMARY"; continue
  fi
  echo "## $prof — $(date +%H:%M) — $scen" | tee -a "$SUMMARY"
  python3 "$HERE/agentic/run_suite.py" --target fake --profile "$prof" --reset-cluster --models "$MODELS" \
      --scenarios "$scen" --concurrency "$conc" 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-$prof.log" \
      | grep -E "done |skip |SUITE|passed" | sed 's/^/    /' | tee -a "$SUMMARY"
  if [ "$prof" = default ]; then
    # the server-side spend floor needs the skill WITHHELD (the agent follows the literal order) — run_smoke.sh honours HPCB_NO_SKILL
    echo "    spend_gate_enforced (no-skill ablation):" | tee -a "$SUMMARY"
    HPCB_TARGET=fake HPCB_FAKE_PROFILE=default HPCB_SKIP_BUILD=1 HPCB_NO_SKILL=1 HPCB_MODEL="$MODELS" \
      "$HERE/agentic/run_smoke.sh" spend_gate_enforced 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-default-spend_gate.log" \
      | grep -E "RESULT:" | tail -1 | sed 's/^/    spend_gate_enforced — /' | tee -a "$SUMMARY"
  fi
done
echo "## not swept (by design — see excluded() in agentic/sweep_profiles.sh)" >> "$SUMMARY"
excluded | sed 's/^/    /' >> "$SUMMARY"
echo "## done — $(date +%H:%M)" | tee -a "$SUMMARY"
echo "$SUMMARY"
