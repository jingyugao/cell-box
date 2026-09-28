#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(realpath -m "${1:-dist/release}")
version=$(cat VERSION)
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][a-zA-Z0-9.-]+)?$ ]] || { echo 'Invalid VERSION'; exit 1; }
revision=$(git rev-parse --short=12 HEAD 2>/dev/null || printf uncommitted)
module=$(go list -m)
if test -n "$(git status --porcelain)"; then revision=$revision-dirty; fi
mkdir -p "$out"
ldflags="-s -w -X $module/internal/version.Version=$version -X $module/internal/version.Revision=$revision"
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out/cellbox" ./cmd/cellbox
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$out/cellbox-guest" ./cmd/cellbox-guest
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$out/cellbox-image" ./cmd/cellbox-image
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out/resumablepod-controller" ./cmd/resumablepod-controller
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out/resumablepod-runtime" ./cmd/resumablepod-runtime
CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$out/server" ./test/counter
cp test/counter/Dockerfile "$out/"
cp -r deploy k8s hack "$out/"
cp LICENSE VERSION "$out/"
mkdir -p "$out/config"
cp config/sample.json "$out/config/"
mkdir -p "$out/api"
cp api/openapi.yaml "$out/api/"
cp README.rst "$out/"
docs_out="$out/tmp/docs"
if [[ -L $docs_out ]]; then
  echo "Refusing to replace symlinked documentation directory: $docs_out" >&2
  exit 1
fi
mkdir -p "$docs_out"
for old_doc in "$docs_out"/*.md; do
  if [[ -f $old_doc ]]; then rm -- "$old_doc"; fi
done
cp -r tmp/docs/. "$docs_out/"
printf 'Release directory: %s\n' "$out"
