#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
common=(
  --set server.nodeName=cellbox-node \
)
expect_failure() {
  local expected=$1
  shift
  if helm template cellbox "$root/charts/cellbox" --namespace cellbox "$@" >"$tmp/failure.yaml" 2>&1; then
    echo "expected Helm rendering to fail with: $expected" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$tmp/failure.yaml"; then
    cat "$tmp/failure.yaml" >&2
    echo "Helm failed for a different reason; expected: $expected" >&2
    exit 1
  fi
}
assert_backup_config() {
  local file=$1 expected_enabled=$2
  local encoded
  encoded=$(awk '
    /^  name: cellbox-api-config$/ { in_config_secret=1; next }
    in_config_secret && /^  config\.json: / {
      sub(/^  config\.json: "/, "")
      sub(/"$/, "")
      print
      exit
    }
    in_config_secret && /^---$/ { in_config_secret=0 }
  ' "$file")
  [[ -n $encoded ]] || { echo "cellbox-api-config Secret/config.json was not rendered" >&2; return 1; }
  printf '%s' "$encoded" | base64 --decode | jq -e --argjson enabled "$expected_enabled" \
    '.volumeBackups.enabled == $enabled and .volumeBackups.binary == "restic" and .volumeBackups.keepLast == 3 and (.volumeBackups | has("passwordEnv") | not)' \
    >/dev/null
  ! grep -qE 'CELLBOX_BACKUP_PASSWORD|cellbox-volume-backups|checksum/backup-password' "$file"
}

helm lint "$root/charts/cellbox" "${common[@]}"
helm template cellbox "$root/charts/cellbox" --namespace cellbox "${common[@]}" \
  --set buildkit.enabled=true \
  --set buildkit.repository=registry.example.invalid/cellbox > "$tmp/rendered.yaml"

grep -q '^kind: Deployment$' "$tmp/rendered.yaml"
! grep -q '^kind: DaemonSet$' "$tmp/rendered.yaml"
grep -q 'name: cellbox-server' "$tmp/rendered.yaml"
grep -q 'name: cellbox-api' "$tmp/rendered.yaml" # stable SDK service name
grep -q 'command: \[/opt/cellbox/bin/cellbox-server\]' "$tmp/rendered.yaml"
grep -q -- '--host-mount-namespace' "$tmp/rendered.yaml"
grep -q -- '--warm-pool-size=2' "$tmp/rendered.yaml"
grep -q 'name: buildkit' "$tmp/rendered.yaml"
grep -q 'mountPath: /var/lib/cellbox-data' "$tmp/rendered.yaml"
grep -q 'mountPath: /var/lib/cellbox$' "$tmp/rendered.yaml"
# The node backend reads the runsc digest in this container before invoking
# the same absolute binary path through nsenter in the host namespace.
grep -q 'mountPath: /usr/local/bin$' "$tmp/rendered.yaml"
grep -q 'path: "/var/lib/cellbox-data"' "$tmp/rendered.yaml"
[[ $(grep -c '^kind: Deployment$' "$tmp/rendered.yaml") == 1 ]]
! grep -q 'cellbox-server-leader' "$tmp/rendered.yaml"
! grep -q -- '--catalog-dir\|--local-metadata-dir\|--initialize-local-metadata' "$tmp/rendered.yaml"

helm template cellbox "$root/charts/cellbox" --namespace cellbox "${common[@]}" \
  --set storage.existingClaim=cellbox-data > "$tmp/claim.yaml"
grep -q 'claimName: "cellbox-data"' "$tmp/claim.yaml"

# Backups are disabled by default and have no password Secret, env, or checksum.
assert_backup_config "$tmp/rendered.yaml" false
helm template cellbox "$root/charts/cellbox" --namespace cellbox "${common[@]}" \
  --set api.config.volumeBackups.enabled=true > "$tmp/backups-enabled.yaml"
assert_backup_config "$tmp/backups-enabled.yaml" true

cat > "$tmp/profile-mismatch.yaml" <<'YAML'
api:
  config:
    profiles:
      - id: invalid
        namespace: another-namespace
YAML
expect_failure 'api.config.profiles[0].namespace must match the release namespace' "${common[@]}" -f "$tmp/profile-mismatch.yaml"

cat > "$tmp/profile-wrong-node.yaml" <<'YAML'
api:
  config:
    profiles:
      - id: invalid
        provider: resumable-k8s-pod
        nodeName: another-node
        namespace: cellbox
YAML
expect_failure 'api.config.profiles[0].nodeName must match server.nodeName' "${common[@]}" -f "$tmp/profile-wrong-node.yaml"
expect_failure 'server.nodeName is required' --set server.nodeName=
expect_failure 'api.namespace must match the release namespace' "${common[@]}" --set api.namespace=other
expect_failure 'api.nodeSelector kubernetes.io/hostname conflicts with server.nodeName' "${common[@]}" --set api.nodeSelector.kubernetes\\.io/hostname=another-node
expect_failure 'api.config.dataDir must be /var/lib/cellbox-data' "${common[@]}" --set api.config.dataDir=/tmp/cellbox
expect_failure 'storage.hostPath must be a clean absolute path other than /' "${common[@]}" --set storage.hostPath=/

echo 'PASS: one node-pinned server deployment, shared persistent data path, BuildKit sidecar, and configuration guards'
