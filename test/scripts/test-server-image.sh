#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: test-server-image.sh IMAGE}
empty_host_bin=$(mktemp -d)
trap 'rmdir -- "$empty_host_bin"' EXIT

# The server chart mounts the node's binaries over /usr/local/bin. Image-owned
# backup tools must remain executable with that directory hidden.
output=$(docker run --rm --network none \
  --mount "type=bind,src=$empty_host_bin,dst=/usr/local/bin,readonly" \
  --entrypoint /bin/sh "$image" -ec \
  'restic version; /opt/cellbox/bin/cellbox-server --version')
printf '%s\n' "$output"
grep -q '^restic 0.18.1 ' <<<"$output"
grep -q '^cellbox-server ' <<<"$output"
