#!/usr/bin/env bash
# Fetch the Cellbox-tested runtime into a new directory; never change a node.
set -euo pipefail
[[ $# == 1 ]] || { echo "Usage: $0 DESTINATION (must not exist)" >&2; exit 2; }
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || {
  echo 'The published Cellbox runtime supports Linux amd64 only' >&2; exit 2;
}
destination=$(realpath -m -- "$1")
[[ ! -e "$destination" && ! -L "$destination" ]] || { echo 'Destination already exists' >&2; exit 2; }
for command in curl sha512sum tar; do command -v "$command" >/dev/null; done

# Own the version and checksum here, so clients only pin the Cellbox revision.
release=cellbox-gvisor-20261030
archive=gvisor-linux-amd64.tar.bz2
asset=694438256
sha512=9ffd09eadfccbc1f5b5eb5520c099f9a6e77836cceb6de81ca9c303746750aa020a221eb380987d15a1d43c7862f2d21cf39b502d8e0d11d21c5ac7f4a818e32
mkdir -p -- "$(dirname -- "$destination")"
work=$(mktemp -d "$(dirname -- "$destination")/.cellbox-runtime.XXXXXX")
trap 'rm -rf -- "$work"' EXIT
# The asset API serves the same public file and avoids the release-page CDN.
curl --fail --location --header 'Accept: application/octet-stream' \
  --connect-timeout 15 --max-time 300 --retry 2 --retry-max-time 660 \
  "https://api.github.com/repos/jingyugao/gvisor/releases/assets/$asset" -o "$work/$archive"
(cd "$work" && printf '%s  %s\n' "$sha512" "$archive" | sha512sum --check -)
mkdir "$work/runtime"
tar -xjf "$work/$archive" -C "$work/runtime" --no-same-owner
for executable in runsc containerd-shim-runsc-v1 gvisor-bin/gvisor_sentry gvisor-bin/checkpointgofer; do
  [[ -x "$work/runtime/$executable" ]] || { echo "Missing runtime executable: $executable" >&2; exit 1; }
done
"$work/runtime/runsc" --version
printf '%s\n' "$release" > "$work/runtime/CELLBOX_RUNTIME_RELEASE"
# Keep gvisor-bin adjacent to runsc. Never overwrite an existing runtime: its
# checkpoints are tied to the runsc version and file digest.
mv -T --no-clobber -- "$work/runtime" "$destination"
[[ ! -e "$work/runtime" ]] || { echo 'Destination appeared during download' >&2; exit 1; }
printf 'Cellbox runtime: %s\n' "$destination"
