#!/usr/bin/env bash
# Core pair: claude-sonnet-4.6 via hermes/Argo vs claude-sonnet-4.6 via Claude Code (adapter default), 3 scenarios × n=5,
# one cell at a time, harnesses interleaved per repeat. Summary row per cell; stops at the Argo spend cap.
set -u
REPO=/Users/gusellerm/Projects/hpc-bridge
OUT=${HPCB_CAMPAIGN_OUT:-$(dirname "$0")/out}
SUMMARY=$OUT/summary.tsv
CAP=45
[ -f "$SUMMARY" ] || printf "runid\toperator\tscenario\trc\tresult\targo_spend\n" > "$SUMMARY"
first=1
for r in 1 2 3 4 5; do
  for scen in gated_provision rich_gate spend_refusal; do
    for op in hermes claude-acp; do
      ts=$(date +%s)
      if [ "$op" = hermes ]; then
        label=s46hermes
        envs=(HPCB_OPERATOR=hermes HPCB_HERMES_ACP=1 HPCB_ALCF_BASE_URL=http://host.docker.internal:44497/v1 HPCB_ALCF_MODEL=argo:claude-sonnet-4.6)
      else
        label=s46claude
        envs=(HPCB_OPERATOR=claude-acp)
      fi
      # RESUME: a cell whose log already carries a verdict is done (the driver runs detached; a relaunch skips it)
      if ls "$OUT/$label-$scen-r$r-"*.log >/dev/null 2>&1 && grep -q "^RESULT: " "$OUT/$label-$scen-r$r-"*.log 2>/dev/null; then
        continue
      fi
      runid="$label-$scen-r$r-$ts"
      log="$OUT/$runid.log"
      if [ "$first" = 1 ]; then skip=(); first=0; else skip=(HPCB_SKIP_BUILD=1); fi
      env HPCB_TARGET=fake HPCB_FAKE_PROFILE=site HPCB_BENCHMARK_MODE=1 HPCB_RUNID="$runid" ${skip[@]+"${skip[@]}"} ${envs[@]+"${envs[@]}"} \
        "$REPO/agentic/run_smoke.sh" "$scen" > "$log" 2>&1
      rc=$?
      result=$(grep -o "^RESULT: .*" "$log" | tail -1 | cut -c1-160)
      spend=$(argo-dash --since-mark 2>/dev/null | grep -o 'equivalent \$[0-9.]*' | tail -1)
      printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$runid" "$op" "$scen" "$rc" "$result" "$spend" >> "$SUMMARY"
      dollars=$(printf "%s" "$spend" | grep -o '[0-9.]*$')
      if [ -n "$dollars" ] && awk "BEGIN{exit !($dollars > $CAP)}"; then
        printf "STOP\t-\t-\t-\tspend cap %s reached (%s)\t%s\n" "$CAP" "$spend" "$spend" >> "$SUMMARY"; exit 3
      fi
    done
  done
done
printf "DONE\t-\t-\t-\tcampaign complete\t%s\n" "$(argo-dash --since-mark 2>/dev/null | grep -o 'equivalent \$[0-9.]*' | tail -1)" >> "$SUMMARY"
