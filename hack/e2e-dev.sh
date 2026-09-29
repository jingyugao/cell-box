#!/bin/bash
# Run as root on the installed local dev node. No direct checkpoint/restore calls.
set -euo pipefail
[[ ${1:-} == --local-dev ]]
[[ $EUID == 0 ]]
release_dir=$(cd "$(dirname "$0")/.." && pwd)
k=(k3s kubectl -n cell-box)
evidence=${EVIDENCE_DIR:-$release_dir/evidence}
mkdir -p "$evidence"
wait_phase() { "${k[@]}" wait --for=jsonpath='{.status.phase}'="$1" cb/counter --timeout=120s; }
state() { curl --noproxy '*' -fsS --retry 10 --retry-all-errors --retry-delay 1 "http://$ip:8000/$1"; }
wait_phase Running
ip=$("${k[@]}" get svc counter -o jsonpath='{.spec.clusterIP}')
"${k[@]}" get svc counter -o json > "$evidence/service-before.json"
for cycle in 1 2 3; do
 state increment
 state state > "$evidence/cycle-$cycle-before.json"
 "${k[@]}" get cb counter -o json > "$evidence/cycle-$cycle-cr-before.json"
 oldpod=$("${k[@]}" get cb counter -o jsonpath='{.status.podName}')
 "${k[@]}" get pod "$oldpod" -o json > "$evidence/cycle-$cycle-pod-before.json"
 "${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Suspended"}}'
 wait_phase Suspended
 "${k[@]}" get cb counter -o json > "$evidence/cycle-$cycle-suspended.json"
 ! "${k[@]}" get pod "$oldpod" >/dev/null 2>&1
 /usr/local/bin/runsc --root=/run/containerd/runsc/k8s.io list --format=json > "$evidence/cycle-$cycle-suspended-runtime.json"
 jq -e '(. // []) | length == 0' "$evidence/cycle-$cycle-suspended-runtime.json"
 if curl --noproxy '*' -fsS --max-time 2 "http://$ip:8000/healthz"; then echo 'Service still live while suspended'; exit 1; fi
 if [[ $cycle == 2 ]]; then systemctl restart resumablepod-controller; fi
 "${k[@]}" patch cb counter --type=merge -p '{"spec":{"desiredState":"Running"}}'
 wait_phase Running
 state state > "$evidence/cycle-$cycle-after.json"
 newpod=$("${k[@]}" get cb counter -o jsonpath='{.status.podName}')
 "${k[@]}" get pod "$newpod" -o json > "$evidence/cycle-$cycle-pod-after.json"
 jq -e -s '.[0] as $a | .[1] as $b | ["started","instance","memory_bytes","memory_sha256","marker","count"] | all(.[]; $a[.] == $b[.])' "$evidence/cycle-$cycle-before.json" "$evidence/cycle-$cycle-after.json"
 jq -e -s '.[0].metadata.uid != .[1].metadata.uid and .[0].status.podIP != .[1].status.podIP and .[1].status.containerStatuses[0].restartCount == 0' "$evidence/cycle-$cycle-pod-before.json" "$evidence/cycle-$cycle-pod-after.json"
 "${k[@]}" exec "$newpod" -- /server get http://127.0.0.1:8000/state > "$evidence/cycle-$cycle-exec.json"
 state network > "$evidence/cycle-$cycle-network.json"
 "${k[@]}" get endpointslice -l kubernetes.io/service-name=counter -o json > "$evidence/cycle-$cycle-endpoints.json"
 echo "PASS cycle-$cycle including memory, counter, writable root, UID/IP, Service, exec and network"
done
"${k[@]}" get svc counter -o json > "$evidence/service-after.json"
jq -e -s '.[0].metadata.uid == .[1].metadata.uid and .[0].spec.clusterIP == .[1].spec.clusterIP' "$evidence/service-before.json" "$evidence/service-after.json"
