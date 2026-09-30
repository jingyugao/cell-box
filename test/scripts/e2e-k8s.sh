#!/bin/bash
# End-to-end lifecycle test through Kubernetes only; requires a Running counter CR.
set -euo pipefail
[[ $# == 1 ]] || { echo "Usage: $0 KUBECTL_CONTEXT" >&2; exit 2; }
context=$1
k=(kubectl --context "$context" -n cell-box)
evidence=${EVIDENCE_DIR:-tmp/validation/k8s}
mkdir -p "$evidence"

wait_phase() { "${k[@]}" wait --for=jsonpath='{.status.phase}'="$1" cb/counter --timeout=180s; }
state() { "${k[@]}" exec "$1" -- /server get "http://127.0.0.1:8000/$2"; }

wait_phase Running
oldpod=$("${k[@]}" get cb counter -o jsonpath='{.status.podName}')
"${k[@]}" get pod "$oldpod" -o json > "$evidence/pod-before.json"
"${k[@]}" get svc counter -o json > "$evidence/service-before.json"
state "$oldpod" increment > /dev/null
state "$oldpod" state > "$evidence/state-before.json"

"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
"${k[@]}" get cb counter -o json > "$evidence/cr-suspended.json"
if "${k[@]}" get pod "$oldpod" >/dev/null 2>&1; then
  echo 'Old Pod still exists after suspension' >&2
  exit 1
fi

"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Running"}}'
wait_phase Running
newpod=$("${k[@]}" get cb counter -o jsonpath='{.status.podName}')
"${k[@]}" get pod "$newpod" -o json > "$evidence/pod-after.json"
"${k[@]}" get svc counter -o json > "$evidence/service-after.json"
state "$newpod" state > "$evidence/state-after.json"

jq -e -s '.[0] as $a | .[1] as $b | ["started","instance","memory_bytes","memory_sha256","marker","count"] | all(.[]; $a[.] == $b[.])' "$evidence/state-before.json" "$evidence/state-after.json" >/dev/null
jq -e -s '.[0].metadata.uid != .[1].metadata.uid and .[1].status.containerStatuses[0].restartCount == 0' "$evidence/pod-before.json" "$evidence/pod-after.json" >/dev/null
jq -e -s '.[0].metadata.uid == .[1].metadata.uid and .[0].spec.clusterIP == .[1].spec.clusterIP' "$evidence/service-before.json" "$evidence/service-after.json" >/dev/null
echo 'PASS: process memory and writable root restored, Pod replaced, Service preserved'
