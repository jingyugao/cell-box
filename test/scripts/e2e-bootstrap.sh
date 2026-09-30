#!/bin/bash
# Verify a deployed node installer can run again without restarting its runtime.
set -euo pipefail
[[ $# == 1 || $# == 2 ]] || { echo "Usage: $0 KUBECTL_CONTEXT [NODE_NAME]" >&2; exit 2; }
context=$1
k=(kubectl --context "$context" -n cell-box)
evidence=${EVIDENCE_DIR:-tmp/validation/bootstrap}
mkdir -p "$evidence"

"${k[@]}" rollout status ds/cellbox-controller --timeout=600s
"${k[@]}" get ds cellbox-controller -o json > "$evidence/daemonset.json"
node=${2:-$("${k[@]}" get pods -l app=cellbox-controller -o json | jq -r '.items[0].spec.nodeName')}
controller_pod() {
  "${k[@]}" get pods -l app=cellbox-controller --field-selector="spec.nodeName=$node" -o json | \
    jq -er '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name'
}
unit=$(jq -r '.spec.template.spec.initContainers[] | select(.name == "install-runtime-adapter") | .env[] | select(.name == "CELLBOX_RUNTIME_SERVICE") | .value' "$evidence/daemonset.json")
if [[ -z $unit ]]; then
  unit=$("${k[@]}" exec "$(controller_pod)" -c controller -- nsenter --target=1 --mount --root -- /bin/sh -c '
    if test -f /var/lib/rancher/k3s/agent/etc/containerd/config.toml; then
      if systemctl cat k3s.service >/dev/null 2>&1; then echo k3s.service; else echo k3s-agent.service; fi
    else echo containerd.service; fi')
fi
runtime_started() {
  "${k[@]}" exec "$(controller_pod)" -c controller -- nsenter --target=1 --mount --root -- \
    /bin/systemctl show "$unit" --property=ActiveEnterTimestampMonotonic --value
}
runtime_started > "$evidence/runtime-start-before.txt"
"${k[@]}" exec "$(controller_pod)" -c controller -- nsenter --target=1 --mount --root -- \
  /usr/local/bin/runsc --version > "$evidence/runsc-version.txt"
"${k[@]}" logs "$(controller_pod)" -c install-runtime-adapter > "$evidence/install-before.log"

"${k[@]}" rollout restart ds/cellbox-controller
"${k[@]}" rollout status ds/cellbox-controller --timeout=600s
runtime_started > "$evidence/runtime-start-after.txt"
"${k[@]}" logs "$(controller_pod)" -c install-runtime-adapter > "$evidence/install-after.log"
cmp "$evidence/runtime-start-before.txt" "$evidence/runtime-start-after.txt"
rg -q 'already configured; no restart needed' "$evidence/install-after.log"
echo 'PASS: repeated node bootstrap preserves the running container runtime'
