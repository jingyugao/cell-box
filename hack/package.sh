#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
version=$(cat VERSION)
arch=$(go env GOARCH)
name=cellbox-v$version-linux-$arch
# Reject dirty or uncommitted release source; dev builds remain available via make build.
[[ $(git rev-parse --is-inside-work-tree) == true ]]
test -z "$(git status --porcelain)" || { echo 'Commit source changes before packaging'; exit 1; }
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
bash hack/build.sh "$staging/$name"
epoch=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}
mkdir -p dist
(cd "$staging/$name" && sha256sum cellbox cellbox-guest cellbox-image resumablepod-controller resumablepod-runtime server > SHA256SUMS)
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner -C "$staging" -cf - "$name" | gzip -n > "dist/$name.tar.gz"
(cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
printf 'Package: dist/%s.tar.gz\n' "$name"
