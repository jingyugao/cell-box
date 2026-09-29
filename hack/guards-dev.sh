#!/bin/bash
# Additional contract checks; begins and ends with the example Suspended.
set -euo pipefail
[[ ${1:-} == --local-dev ]]
[[ $EUID == 0 ]]
release_dir=$(cd "$(dirname "$0")/.." && pwd)
k=(k3s kubectl -n cell-box)
evidence=${EVIDENCE_DIR:-$release_dir/evidence}
wait_phase() { "${k[@]}" wait --for=jsonpath='{.status.phase}'="$1" cb/counter --timeout=120s; }
wait_phase Suspended
uid=$("${k[@]}" get cb counter -o jsonpath='{.metadata.uid}')
snap=$("${k[@]}" get cb counter -o jsonpath='{.status.snapshot}')
path=/var/lib/cellbox/workloads/$uid/$snap
mv "$path" "$path.missing-test"
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Running"}}'
wait_phase Failed
"${k[@]}" get cb counter -o json > "$evidence/missing-snapshot.json"
jq -e '.status.message | contains("no such file")' "$evidence/missing-snapshot.json"
"${k[@]}" get pods -o json | jq -e '.items|length==0'
mv "$path.missing-test" "$path"
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"retryNonce":"found-snapshot"}}'
wait_phase Running
ip=$("${k[@]}" get svc counter -o jsonpath='{.spec.clusterIP}')
curl --noproxy '*' -fsS --retry 10 --retry-all-errors --retry-delay 1 "http://$ip:8000/state" > "$evidence/guard-before.json"
echo 'PASS missing snapshot fails closed and can be repaired explicitly'
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"container":{"args":["idle"]}}}'
wait_phase Failed
"${k[@]}" get cb counter -o json > "$evidence/changed-template.json"
jq -e '.status.message|contains("immutable")' "$evidence/changed-template.json"
"${k[@]}" get pods -o json | jq -e '.items|length==0'
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"container":{"args":null},"retryNonce":"reverted-template","desiredState":"Running"}}'
wait_phase Running
curl --noproxy '*' -fsS --retry 10 --retry-all-errors --retry-delay 1 "http://$ip:8000/state" > "$evidence/guard-after.json"
jq -e -s '.[0] as $a | .[1] as $b | ["started","instance","memory_bytes","memory_sha256","marker","count"] | all(.[]; $a[.] == $b[.])' "$evidence/guard-before.json" "$evidence/guard-after.json"
echo 'PASS incompatible template rejected before native restore; reverting and retrying preserves state'
"${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
"${k[@]}" get cb counter -o json > "$evidence/final-example.json"
