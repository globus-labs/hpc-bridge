#!/usr/bin/env bash
# The PROFILE SWEEP: every fake-cluster profile, brought up clean (--reset-cluster), with the scenarios that belong to
# it — the regression baseline across cluster shapes. Sequential (one cluster at a time); each profile's cells run
# through run_suite with the usual node gate. Results land in agentic/runs/ (bundles) and a per-sweep summary file.
#
#   agentic/sweep_profiles.sh [--models claude-opus-5] [--profiles default,site,...] [--operators a,b,…] [--paid-only]
#   HPCB_OPERATOR=pi agentic/sweep_profiles.sh      # the FREE wave: codex / pi / hermes21 on the ALCF model
#   agentic/sweep_profiles.sh --operators pi,codex,hermes21   # the free wave for three operators, each profile booted
#                                                    # ONCE (--reset-cluster) and its cells run for each operator in turn;
#                                                    # every summary row is tagged [operator], one log per profile+operator
#   HPCB_ALCF_NO_AUTH=1 HPCB_ALCF_BASE_URL=http://api.globus.internal/v1 HPCB_ALCF_MODEL=qwen3.8-flash-next \
#     agentic/sweep_profiles.sh --operators pi,codex,hermes21   # the free wave on a keyless self-hosted model instead
#                                                    # of ALCF: no ALCF token is minted (run_smoke.sh)
#   agentic/sweep_profiles.sh --paid-only            # the Claude operator on the paid_only() cells only — the half the
#                                                    # free wave cannot drive; a profile with none is not booted
#
# A free operator's cell first probes the ALCF endpoint (run_smoke.sh → agentic/harness/alcf_probe.py) and waits up to
# HPCB_ALCF_WAIT_S seconds (default 0) for it to be healthy; if it is not, the cell is not run and is recorded INFRA. A
# cell whose operator produced nothing because its model endpoint failed mid-run is INFRA too (run.py). The summary
# counts INFRA apart from OK and FAILED ("## totals"): not a verdict on hpc-bridge.
#
# The agent model defaults to run_suite's DEFAULT_MODEL (claude-opus-5); the human-sim is Haiku. Never Fable unless
# asked. `mep` and `pbs_mep` are skipped unless HPCB_MEP_EMAIL is set in agentic/.env (their managers register a
# contact address).
# What is NOT swept, and why, is the list in excluded() below (the summary prints it): every scenario that can run on
# the fake target is in cells() or there — test_sweep_script.py enforces it, so a new scenario cannot drop out of the
# sweeps unnoticed. The paid wave is the default (the Claude operator runs every cell); with HPCB_OPERATOR=codex|pi|hermes21
# the sweep is the free wave and leaves out paid_only() — what those one-shot operators cannot drive (a persona, PHASES, a
# hook that needs the SDK interrupt). test_sweep_script.py keeps that list equal to what run.py would report SKIPPED.
# Unless --paid-only, the SCRIPT operator (no model) runs too, once per sweep whatever --operators names: both phases of
# queued_check_relaunch on `default`, polaris_filesystems on `polaris` (the HELD pilot → the directive) and lmod_bootstrap
# on `lmod` (a failed details= → the proposal) — paths a model may never take. The summary header names the model each
# operator drives: --models for the Claude ones, HPCB_ALCF_MODEL for the free ones.
# `returning` is a SWEEP profile, not a cluster one: it boots the `nersc` cluster and runs returning_user once per cell of
# agentic/harness/returning_cells.tsv (the pairwise returning-user cells: HPCB_RETURNING_CELL), under the script
# operator; `nersc` itself runs the three returning-user AGENT cells in its agent wave (any operator: each chain opens
# with a scripted session), then the Perlmutter report's scripted proofs.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODELS="claude-opus-5"; PROFILES="default,site,totp,pbs,lmod,f2b,totp_f2b,polaris,internal,hostile,mep,pbs_mep,slices,nersc,returning"
OPERATORS=""; PAID_ONLY=""
while [ $# -gt 0 ]; do case "$1" in --models) MODELS="$2"; shift 2;; --profiles) PROFILES="$2"; shift 2;;
  --operators) OPERATORS="$2"; shift 2;; --paid-only) PAID_ONLY=1; shift;; *) echo "unknown arg $1"; exit 2;; esac; done
[ -f "$HERE/agentic/.env" ] && while IFS= read -r line || [ -n "$line" ]; do case "$line" in ''|\#*) continue ;; esac; k="${line%%=*}"; [ -z "${!k+x}" ] && export "$line"; done < "$HERE/agentic/.env"
STAMP="$(date +%Y%m%d-%H%M)"
SUMMARY="$HERE/agentic/runs/sweep-$STAMP.md"; mkdir -p "$HERE/agentic/runs"
# The header names the model each operator really drives: --models reaches the Claude operators only; the free ones run
# HPCB_ALCF_MODEL (run_smoke.sh's default when unset) — the 2026-10-22 qwen sweep's header said "models claude-opus-5".
HAS_FREE=; HAS_PAID=
NAMED_OPS="${OPERATORS:-${HPCB_OPERATOR:-claude}}"
for op in ${NAMED_OPS//,/ }; do case "$op" in codex|pi|hermes21|hermes) HAS_FREE=1 ;; *) HAS_PAID=1 ;; esac; done
MODEL_WORDS="${HAS_PAID:+models $MODELS}${HAS_PAID:+${HAS_FREE:+; }}${HAS_FREE:+free operators: model ${HPCB_ALCF_MODEL:-openai/gpt-oss-120b}}"
echo "# profile sweep $STAMP — target fake, $MODEL_WORDS" > "$SUMMARY"

cells() {  # cells <profile> -> the scenario list (and concurrency) for that profile
  case "$1" in
    default)  echo "happy_path,gated_provision,long_task_via_handle,endpoint_reuse,endpoint_reuse_chain,session_persistence,byo_teardown_clean,spend_refusal,unknown_host_key,zero_config_list,orphaned_task,draining_restop,stop_while_running,spend_revoked,block_reaped_resume,long_output,no_facility_first,venv_broken_repair,venv_arch_mismatch,endpoint_upgrade_reuse,reap_replaced_block,esc_mid_call,teardown_slow_fs,batch_fanout_collect 2" ;;
    site)     echo "rich_gate,partition_choice,gpu_rule,submit_policy_rejected,login_pin_teardown,slurm_worker_died,gated_provision,happy_path,hand_batch_job,ssh_account_floor 3" ;;
    totp)     echo "otp_preauth,password_pusher 1" ;;
    pbs)      echo "happy_path,gated_provision 2" ;;
    lmod)     echo "lmod_bootstrap 1" ;;
    f2b)      echo "f2b_stranger,f2b_banned 1" ;;
    totp_f2b) echo "wrong_code 1" ;;    # a wrong one-time code counts toward fail2ban's ban here (the profile's overlay)
    polaris)  echo "polaris_filesystems 1" ;;
    internal) echo "internal_hostnames 1" ;;
    hostile)  echo "canary_login_discovery,canary_compute_output 1" ;;  # per-node canary channels are shared → SERIAL, one at a time
    mep)      echo "fake_mep_compute,fake_mep_stop_names_block,fake_mep_no_account,fake_mep_username_not_account,term_switch_reasks,preset_account_scoped,terms_prestated,terms_wanted,fake_mep_stop_while_provisioning,mep_hand_batch_job,mep_hand_batch_job_ask 1" ;;
    pbs_mep)  echo "fake_pbs_mep_compute 1" ;;
    nersc)    echo "returning_perlmutter_replay,returning_all_well,returning_new_laptop 1" ;;  # one at a time: a cell
              # closes a login node, revokes the key and retires a QOS for the whole cluster
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
slice_request                 scripted only: a script_cell on `slices` (the script-only pass after the profiles)
multinode_allocation          scripted only: a script_cell on `slices` (the script-only pass after the profiles)
stale_login_pin               scripted only: a script_cell on `site` (the Perlmutter report, 2026-10-29)
kept_entry_after_failure      scripted only: a script_cell on `site` (the Perlmutter report, 2026-10-29)
cached_password_otp           scripted only: a script_cell on `nersc` (after its agent wave)
home_outside_home             scripted only: a script_cell on `nersc` (after its agent wave)
noisy_failure_tail            scripted only: a script_cell on `nersc` (after its agent wave)
partition_proposal_policy     scripted only: a script_cell on `nersc` (after its agent wave)
returning_user                scripted only: one script_cell per returning-user cell on `returning` (the nersc cluster)
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
password_pusher
wrong_code
fake_mep_username_not_account
terms_prestated
terms_wanted
mep_hand_batch_job_ask
PAID
}

free_op() { case "$1" in codex|pi|hermes21) return 0 ;; *) return 1 ;; esac; }

# The operators, in the order each profile runs them: --operators (each row then tagged [operator]), else HPCB_OPERATOR
# (default claude) as before. --paid-only is the Claude operator's: the paid_only() cells are what the others cannot drive.
TAGGED="${OPERATORS:+1}"
IFS=, read -ra OPS <<< "${OPERATORS:-${HPCB_OPERATOR:-claude}}"
SEEN=","
for op in "${OPS[@]}"; do
  case "$op" in claude|claude-acp|hermes|codex|pi|hermes21) ;; *) echo "sweep: unknown operator $op"; exit 2 ;; esac
  case "$SEEN" in *",$op,"*) echo "sweep: operator $op is listed twice in --operators"; exit 2 ;; esac
  SEEN="$SEEN$op,"
  if [ -n "$PAID_ONLY" ] && [ "$op" != claude ]; then
    echo "sweep: --paid-only runs the paid_only() cells, which only the claude operator drives (not $op)"; exit 2
  fi
  if free_op "$op"; then echo "free wave: operator $op — leaving out paid_only()" | tee -a "$SUMMARY"; FREE_WAVE=1; fi
done
[ -z "${FREE_WAVE:-}" ] || echo "free wave: HPCB_ALCF_WAIT_S=${HPCB_ALCF_WAIT_S:-0} — each cell probes the ALCF endpoint first; an unhealthy one is recorded INFRA, not run" | tee -a "$SUMMARY"
[ -z "$PAID_ONLY" ] || echo "paid only: the paid_only() cells, Claude operator" | tee -a "$SUMMARY"

BUILT=""   # set once an image was built in this sweep (by run_suite, or by the first scripted cell's run_smoke)
script_cell() {  # script_cell <profile> <scenario> [cell]: ONE script-operator cell (no model) on the cluster booted above
  # its row reads "<scenario> (script) — RESULT: …" (two words before the dash, as queued_check_relaunch's: sweep_dash
  # reads a one-word row as the no-skill ablation); with a returning-user cell, "<scenario> <cell> — RESULT: …"
  # The first one in a sweep that built nothing yet builds the image (run_smoke.sh, with the older releases' export):
  # a script-only sweep (--profiles returning) ran on whatever image was there before (the review of 2026-10-30)
  local label="$2${3:+ $3}" tag="$2${3:+-$3}" row="$2 ${3:-(script)}" skip="${BUILT:+1}"
  BUILT=1
  echo "    ${TAGGED:+[script] }$label (script operator)${skip:- — building the image first}:" | tee -a "$SUMMARY"
  env HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE="$(cluster_of "$1")" ${skip:+HPCB_SKIP_BUILD=1} \
    ${3:+HPCB_RETURNING_CELL="$3"} "$HERE/agentic/run_smoke.sh" "$2" 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-$1-script-$tag.log" \
    | { grep -E "RESULT:" || echo "(no RESULT line)"; } | tail -1 \
    | sed "s/^/    ${TAGGED:+[script] }$row — /" | tee -a "$SUMMARY"
}

cluster_of() {  # cluster_of <sweep profile>: the fake-cluster profile it boots (a sweep profile may share a cluster)
  case "$1" in returning) echo nersc ;; *) echo "$1" ;; esac
}

returning_cells() {  # the returning-user cells' ids, in the table's order (agentic/harness/returning_cells.tsv)
  awk -F'\t' '!/^#/ && $1 != "id" && $1 != "" {print $1}' "$HERE/agentic/harness/returning_cells.tsv"
}

cells_for() {  # cells_for <operator> <scenario list> -> the cells that operator runs here (order kept)
  if [ -n "$PAID_ONLY" ]; then tr ',' '\n' <<< "$2" | grep -xF -f <(paid_only) | paste -sd, -
  elif free_op "$1"; then tr ',' '\n' <<< "$2" | grep -vxF -f <(paid_only) | paste -sd, -
  else echo "$2"; fi
}

batch_cells() {  # batch_cells <profile>: batch phase 1's scripted cells, once per sweep with no model
  case "$1" in
    default) echo "batch_crosses_walltime batch_restart_resume batch_two_servers_one_ledger batch_block_reaped_midrun batch_big_output batch_abort_midrun" ;;
    mep)     echo "mep_batch_fanout" ;;
  esac
}

script_only() {  # script_only <profile>: the scripted cells of a profile no agent wave runs on, or nothing
  case "$1" in
    slices) echo "slice_request multinode_allocation" ;;   # resource requests (2026-10-24)
    returning) echo "returning_user" ;;   # once per returning-user cell (returning_cells), on the nersc cluster
  esac
}

IFS=, read -ra PROFS <<< "$PROFILES"
for prof in "${PROFS[@]}"; do
  if [ -n "$(script_only "$prof")" ]; then   # no agent wave: boot it here, run its scripted cells, wipe it
    [ -z "$PAID_ONLY" ] || continue
    echo "## $prof (script only) — $(date +%H:%M)" | tee -a "$SUMMARY"
    if "$HERE/agentic/fakecluster/bin/up.sh" --profile "$(cluster_of "$prof")" \
        > "$HERE/agentic/runs/sweep-$STAMP-$prof-up.log" 2>&1; then
      for sc in $(script_only "$prof"); do
        if [ "$sc" = returning_user ]; then
          for cell in $(returning_cells); do script_cell "$prof" "$sc" "$cell"; done
        else
          script_cell "$prof" "$sc"
        fi
      done
    else
      echo "    ${TAGGED:+[script] }$prof (script) — RESULT: INFRA (the profile did not boot: sweep-$STAMP-$prof-up.log)" \
        | tee -a "$SUMMARY"
    fi
    "$HERE/agentic/fakecluster/bin/down.sh" --profile "$(cluster_of "$prof")" --wipe > /dev/null 2>&1 || true
    continue
  fi
  read -r all conc <<< "$(cells "$prof")"
  [ -n "$all" ] || { echo "sweep: unknown profile $prof"; continue; }
  if { [ "$prof" = mep ] || [ "$prof" = pbs_mep ]; } && [ -z "${HPCB_MEP_EMAIL:-}" ]; then
    echo "## $prof — SKIPPED (HPCB_MEP_EMAIL unset in agentic/.env)" | tee -a "$SUMMARY"; continue
  fi
  RESET=--reset-cluster   # the profile's first run boots it clean; its other operators reuse that cluster
  for op in "${OPS[@]}"; do
    tag="${TAGGED:+ [$op]}"
    scen="$(cells_for "$op" "$all")"
    if [ -z "$PAID_ONLY" ] && free_op "$op"; then
      dropped="$(tr ',' '\n' <<< "$all" | grep -xF -f <(paid_only) | paste -sd, -)"
      [ -z "$dropped" ] || echo "## $prof$tag — free wave leaves out (paid_only): $dropped" | tee -a "$SUMMARY"
    fi
    if [ -z "$scen" ]; then
      if [ -n "$PAID_ONLY" ]; then echo "## $prof — SKIPPED (--paid-only: no paid_only() cell here; not booted)" | tee -a "$SUMMARY"
      else echo "## $prof$tag — SKIPPED (every cell is paid-only)" | tee -a "$SUMMARY"; fi
      continue
    fi
    echo "## $prof$tag — $(date +%H:%M) — $scen" | tee -a "$SUMMARY"
    BUILT=1   # run_suite builds the image (and exports the older releases) once
    HPCB_OPERATOR="$op" python3 "$HERE/agentic/run_suite.py" --target fake --profile "$prof" ${RESET:+"$RESET"} --models "$MODELS" \
        --scenarios "$scen" --concurrency "$conc" 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-$prof${TAGGED:+-$op}.log" \
        | grep -E "done |skip |SUITE|passed" | sed "s/^/    ${TAGGED:+[$op] }/" | tee -a "$SUMMARY"
    RESET=
    if [ "$prof" = default ] && [ -z "$PAID_ONLY" ]; then
      # the server-side spend floor needs the skill WITHHELD (the agent follows the literal order) — run_smoke.sh honours HPCB_NO_SKILL
      echo "    ${TAGGED:+[$op] }spend_gate_enforced (no-skill ablation):" | tee -a "$SUMMARY"
      HPCB_OPERATOR="$op" HPCB_TARGET=fake HPCB_FAKE_PROFILE=default HPCB_SKIP_BUILD=1 HPCB_NO_SKILL=1 HPCB_MODEL="$MODELS" \
        "$HERE/agentic/run_smoke.sh" spend_gate_enforced 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-default-spend_gate${TAGGED:+-$op}.log" \
        | { grep -E "RESULT:" || echo "(no RESULT line)"; } | tail -1 | sed "s/^/    ${TAGGED:+[$op] }spend_gate_enforced — /" | tee -a "$SUMMARY"
    fi
  done
  if [ "$prof" = default ] && [ -z "$PAID_ONLY" ]; then
    # The relaunch regression (#203, fixed by #204): no model, the script operator, so once per sweep whatever
    # --operators names — both phases (a user endpoint's second pilot, then its first), on the cluster booted above.
    for ph in second first; do
      echo "    ${TAGGED:+[script] }queued_check_relaunch phase $ph (script operator):" | tee -a "$SUMMARY"
      HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default HPCB_SKIP_BUILD=1 HPCB_RELAUNCH_PHASE="$ph" \
        "$HERE/agentic/run_smoke.sh" queued_check_relaunch 2>&1 | tee "$HERE/agentic/runs/sweep-$STAMP-default-qcr-$ph.log" \
        | { grep -E "RESULT:" || echo "(no RESULT line)"; } | tail -1 \
        | sed "s/^/    ${TAGGED:+[script] }queued_check_relaunch $ph — /" | tee -a "$SUMMARY"
    done
  fi
  if [ -z "$PAID_ONLY" ] && [ -z "$RESET" ]; then   # RESET empty: a run_suite above booted this profile
    # The paths a model may never take, once per sweep with no model (the review of the 2026-10-22 sweep): polaris's
    # HELD pilot → the directive → the block up, and lmod's failed details= → the proposal that loads its modules.
    for sc in $(batch_cells "$prof"); do script_cell "$prof" "$sc"; done   # batch phase 1 (2026-10-31)
    case "$prof" in
      polaris) script_cell polaris polaris_filesystems ;;
      lmod)    script_cell lmod lmod_bootstrap ;;
      site)    script_cell site stale_login_pin; script_cell site kept_entry_after_failure ;;
      nersc)   for sc in cached_password_otp home_outside_home noisy_failure_tail partition_proposal_policy; do
                 script_cell nersc "$sc"; done ;;   # the Perlmutter report's proofs (2026-10-29)
    esac
  fi
done
# INFRA apart from OK and FAILED: the operator's model endpoint failed (or the ALCF probe found it unhealthy and the cell
# was not run) — nothing about hpc-bridge. Counted off the RESULT the rows above carry, per operator when tagged.
tally() { grep -cE "$1" "$SUMMARY" || true; }
for op in "${OPS[@]}" ${TAGGED:+script}; do   # with --operators, the script operator's rows get their own totals
  p="${TAGGED:+\[$op\] .*}"
  rows=$(tally "${p}(RESULT: |no RESULT line)"); ok=$(tally "${p}RESULT: OK"); bad=$(tally "${p}RESULT: FAILED")
  infra=$(tally "${p}RESULT: INFRA"); setup=$(tally "${p}RESULT: SETUP FAILED"); crashed=$(tally "${p}no RESULT line")
  echo "## totals${TAGGED:+ [$op]} — OK $ok · FAILED $bad · INFRA $infra (the model endpoint failed: not graded) · SETUP FAILED $setup · CRASHED $crashed (no RESULT line) · other $((rows - ok - bad - infra - setup - crashed)) (a RESULT of SKIPPED, RATE_LIMITED or INTERRUPTED; the suite's own skips are on its SUITE lines)" | tee -a "$SUMMARY"
  [ -n "$TAGGED" ] || break
done
echo "## not swept (by design — see excluded() in agentic/sweep_profiles.sh)" >> "$SUMMARY"
excluded | sed 's/^/    /' >> "$SUMMARY"
echo "## done — $(date +%H:%M)" | tee -a "$SUMMARY"
echo "$SUMMARY"
