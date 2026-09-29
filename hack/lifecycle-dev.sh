#!/bin/bash
set -euo pipefail
[[ ${1:-} == --local-dev ]]
[[ $EUID == 0 ]]
release_dir=$(cd "$(dirname "$0")/.." && pwd)
k=(k3s kubectl -n cell-box)
evidence=${EVIDENCE_DIR:-$release_dir/evidence}
mkdir -p "$evidence"
wait_phase() { "${k[@]}" wait --for=jsonpath='{.status.phase}'="$1" cb/counter --timeout=120s; }
assert_clean() {
 local uid=$1
 test ! -e "/var/lib/cellbox/workloads/$uid"
 /usr/local/bin/runsc --root=/run/containerd/runsc/k8s.io list --format=json | jq -e '(. // []) | length == 0'
 k3s crictl pods -o json | jq -e '[.items[] | select(.metadata.namespace=="cell-box")] | length==0'
 test -z "$(find /var/lib/cellbox/requests /var/lib/cellbox/tickets /var/lib/cellbox/claims -type f -print -quit)"
}
wait_phase Running
uid=$("${k[@]}" get cb counter -o jsonpath='{.metadata.uid}')
pod=$("${k[@]}" get cb counter -o jsonpath='{.status.podName}')
"${k[@]}" delete pod "$pod" --wait=false
wait_phase Failed
"${k[@]}" get cb counter -o json > "$evidence/external-pod-deletion.json"
jq -e '.status.message|contains("no automatic rollback or cold start")' "$evidence/external-pod-deletion.json"
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"retryNonce":"must-not-cold-start"}}'
sleep 3
"${k[@]}" get cb counter -o json | jq -e '.status.phase=="Failed" and .status.retryNonce=="must-not-cold-start"'
"${k[@]}" get pods -o json | jq -e '.items|length==0'
echo 'PASS lost active execution never silently cold-starts, including explicit retry without snapshot'
"${k[@]}" delete cb counter --timeout=120s
assert_clean "$uid"
! "${k[@]}" get svc counter >/dev/null 2>&1
echo 'PASS Failed CR deletion reclaimed runtime, authorization records and Service'
bash "$release_dir/hack/create-example.sh" --local-dev
wait_phase Running
uid=$("${k[@]}" get cb counter -o jsonpath='{.metadata.uid}')
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
"${k[@]}" get cb counter -o json > "$evidence/delete-suspended-before.json"
"${k[@]}" delete cb counter --timeout=120s
assert_clean "$uid"
! "${k[@]}" get svc counter >/dev/null 2>&1
echo 'PASS Suspended CR deletion removed retained checkpoint and Service'
# Leave the installation with a usable example snapshot and no running app Pod.
bash "$release_dir/hack/create-example.sh" --local-dev
wait_phase Running
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
"${k[@]}" get cb counter -o json > "$evidence/final-example.json"
echo 'PASS installed example left Suspended, ready for user-driven resume'
