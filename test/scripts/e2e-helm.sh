#!/bin/bash
# Test install/upgrade in a dedicated namespace, using an already installed runtime.
set -euo pipefail
[[ $# == 2 ]] || { echo "Usage: $0 KUBECTL_CONTEXT TEST_VALUES_FILE" >&2; exit 2; }
context=$1
values=$2
namespace="cellbox-helm-test-$(date +%s)-$$"
release=cellbox-helm-test
evidence=${EVIDENCE_DIR:-tmp/validation/helm}
port=${PORT_FORWARD_PORT:-18091}
mkdir -p "$evidence"
k=(kubectl --context "$context" -n "$namespace")
h=(helm --kube-context "$context" -n "$namespace")
forward_pid=
cleanup() {
  if [[ -n $forward_pid ]]; then kill "$forward_pid" 2>/dev/null || true; fi
  # Only the namespace created by this test is removed; shared node runtime stays.
  "${h[@]}" uninstall "$release" --ignore-not-found --wait >/dev/null 2>&1 || true
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT
settings=( -f "$values" --set api.namespace="$namespace" --set api.manageSecrets=true
  --set crd.create=false --set runtimeClass.create=false
  --set controller.runtimeInstaller.enabled=false --set buildkit.enabled=false )
"${h[@]}" install "$release" charts/cellbox --create-namespace "${settings[@]}" --wait --timeout 5m
"${k[@]}" port-forward --address 127.0.0.1 svc/cellbox-api "$port:8090" > "$evidence/port-forward.log" 2>&1 &
forward_pid=$!
verify() {
  uv run --no-project python - "$context" "$namespace" "$port" "$evidence" "$1" <<'PY'
import base64, hashlib, json, pathlib, subprocess, sys, time, urllib.request
context, namespace, port, evidence, stage = sys.argv[1:]
k = ['kubectl', '--context', context, '-n', namespace]
secret = json.loads(subprocess.check_output(k + ['get', 'secret', 'cellbox-api-client-tokens', '-o', 'json']))
token = base64.b64decode(secret['data']['CELLBOX_CLIENT_TOKEN']).decode()
fingerprint = hashlib.sha256(token.encode()).hexdigest()
record = pathlib.Path(evidence) / 'token.sha256'
if stage == 'install':
    record.write_text(fingerprint)
else:
    assert record.read_text() == fingerprint, 'upgrade rotated the generated client token'
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for _ in range(30):
    try:
        request = urllib.request.Request('http://127.0.0.1:' + port + '/v1/profiles', headers={'Authorization': 'Bearer ' + token})
        with opener.open(request, timeout=5) as response:
            assert response.status == 200
        break
    except OSError:
        time.sleep(1)
else:
    raise RuntimeError('API did not become reachable')
print('PASS: authenticated API after ' + stage + '; generated token stable')
PY
}
verify install
"${k[@]}" get pod -l app=cellbox-api -o jsonpath='{.items[0].metadata.uid}' > "$evidence/api-before.uid"
"${h[@]}" upgrade "$release" charts/cellbox "${settings[@]}" --set api.config.startupTimeoutSeconds=180 --wait --timeout 5m
# Forwarding targets the old Pod, so reconnect after its rollout.
kill "$forward_pid" 2>/dev/null || true
wait "$forward_pid" 2>/dev/null || true
"${k[@]}" port-forward --address 127.0.0.1 svc/cellbox-api "$port:8090" > "$evidence/port-forward.log" 2>&1 &
forward_pid=$!
verify upgrade
"${k[@]}" get pod -l app=cellbox-api -o jsonpath='{.items[0].metadata.uid}' > "$evidence/api-after.uid"
if cmp -s "$evidence/api-before.uid" "$evidence/api-after.uid"; then
  echo 'API configuration update did not trigger a rollout' >&2
  exit 1
fi
"${h[@]}" upgrade "$release" charts/cellbox "${settings[@]}" --set api.config.startupTimeoutSeconds=180 --wait --timeout 5m
verify 'repeat upgrade'
"${k[@]}" get pod -l app=cellbox-api -o jsonpath='{.items[0].metadata.uid}' > "$evidence/api-repeat.uid"
cmp "$evidence/api-after.uid" "$evidence/api-repeat.uid"
echo 'PASS: single-command Helm install, config rollout, and repeat upgrade'
