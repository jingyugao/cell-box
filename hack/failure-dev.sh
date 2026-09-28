#!/bin/bash
# Destructive injection ONLY into the example CR's local dev checkpoint.
set -euo pipefail
[[ ${1:-} == --local-dev ]]
[[ $EUID == 0 ]]
release_dir=$(cd "$(dirname "$0")/.." && pwd)
k=(k3s kubectl -n recoverable-system)
evidence=${EVIDENCE_DIR:-$release_dir/evidence}
mkdir -p "$evidence"
wait_phase() { "${k[@]}" wait --for=jsonpath='{.status.phase}'="$1" rp/counter --timeout=150s; }
wait_phase Running
ip=$("${k[@]}" get svc counter -o jsonpath='{.spec.clusterIP}')
curl --noproxy '*' -fsS "http://$ip:8000/state" > "$evidence/failure-before.json"
"${k[@]}" patch rp counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
wait_phase Suspended
uid=$("${k[@]}" get rp counter -o jsonpath='{.metadata.uid}')
snap=$("${k[@]}" get rp counter -o jsonpath='{.status.snapshot}')
path=/var/lib/resumablepod/workloads/$uid/$snap
backup=$(mktemp -d /var/lib/resumablepod/failure-backup.XXXXXX)
cp -a "$path/." "$backup/"
printf 'corrupted checkpoint\n' > "$path/checkpoint.img"
"${k[@]}" patch rp counter --type=merge -p '{"spec":{"desiredState":"Running"}}'
wait_phase Failed
"${k[@]}" get rp counter -o json > "$evidence/failure-checksum.json"
jq -e '.status.message | contains("integrity mismatch")' "$evidence/failure-checksum.json"
"${k[@]}" get pods -o json | jq -e --arg uid "$uid" '[.items[] | select(.metadata.labels["recovery.gvisor.dev/owner"]==$uid)] | length==0'
echo 'PASS corrupt checksum rejected before scheduling and Pod automatically removed'
# Bypass integrity detection deliberately, to exercise the native Restore error path.
hash=$(sha256sum "$path/checkpoint.img" | cut -d' ' -f1)
jq --arg hash "$hash" '.files["checkpoint.img"]=$hash' "$path/manifest.json" > "$path/manifest.next"
mv "$path/manifest.next" "$path/manifest.json"
"${k[@]}" patch rp counter --type=merge -p '{"spec":{"retryNonce":"native-failure"}}'
wait_phase Failed
"${k[@]}" get rp counter -o json > "$evidence/failure-native.json"
# Wait must observe the NEW nonce: Failed initially belongs to the previous attempt.
for i in {1..150}; do
 "${k[@]}" get rp counter -o json > "$evidence/failure-native.json"
 if jq -e '.status.phase=="Failed" and .status.retryNonce=="native-failure"' "$evidence/failure-native.json" >/dev/null; then break; fi
 sleep 1
done
jq -e '.status.phase=="Failed" and .status.retryNonce=="native-failure" and (.status.message|contains("startup timeout"))' "$evidence/failure-native.json"
"${k[@]}" get pods -o json | jq -e --arg uid "$uid" '[.items[] | select(.metadata.labels["recovery.gvisor.dev/owner"]==$uid)] | length==0'
/usr/local/bin/runsc --root=/run/containerd/runsc/k8s.io list --format=json > "$evidence/failure-native-runtime.json"
jq -e '(. // []) | length == 0' "$evidence/failure-native-runtime.json"
k3s crictl pods -o json | jq -e '[.items[] | select(.metadata.namespace=="recoverable-system")] | length==0'
test -z "$(find /var/lib/resumablepod/requests /var/lib/resumablepod/tickets /var/lib/resumablepod/claims -type f -print -quit)"
echo 'PASS native Restore failure automatically cleaned without manual runtime intervention'
cp -a "$backup/." "$path/"
rm -rf -- "$backup"
"${k[@]}" patch rp counter --type=merge -p '{"spec":{"retryNonce":"repaired"}}'
wait_phase Running
curl --noproxy '*' -fsS --retry 10 --retry-all-errors --retry-delay 1 "http://$ip:8000/state" > "$evidence/failure-after.json"
jq -e -s '.[0] as $a | .[1] as $b | ["started","instance","memory_bytes","memory_sha256","marker","count"] | all(.[]; $a[.] == $b[.])' "$evidence/failure-before.json" "$evidence/failure-after.json"
echo 'PASS explicit retry restored original process state after snapshot repair'
