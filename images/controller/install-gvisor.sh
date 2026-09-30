#!/bin/sh
# Install the complete, checksum-verified gVisor distribution into a host mount.
# HOST_ROOT=/ permits running the same script directly on a node as root.
set -eu

host=${HOST_ROOT:-/host}
version=${GVISOR_VERSION:-20260914.0}
url=${GVISOR_RELEASE_URL:-https://storage.googleapis.com/gvisor/releases/release}
bin="$host/usr/local/bin"
pending="$bin/.cellbox-gvisor-installing"

case "$version" in
  ''|*[!0-9.]*|.*|*..*) echo 'GVISOR_VERSION must be a pinned release such as 20260914.0' >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|aarch64) arch=$(uname -m) ;;
  *) echo 'gVisor requires an x86_64 or aarch64 Linux node' >&2; exit 1 ;;
esac

exec 9>"$host/run/cellbox-install.lock"
flock -x 9
if test -f "$pending"; then
  test "$(cat "$pending")" = "$version" || {
    echo 'Interrupted gVisor installation has a different version' >&2; exit 1;
  }
elif test -e "$bin/runsc" || test -e "$bin/containerd-shim-runsc-v1" || test -e "$bin/gvisor-bin"; then
  test -x "$bin/runsc" && test -x "$bin/containerd-shim-runsc-v1" &&
    test -x "$bin/gvisor-bin/gvisor_sentry" || {
      echo 'Incomplete existing gVisor install; refusing to replace node binaries' >&2; exit 1;
    }
  echo 'Existing complete gVisor installation preserved'
  exit 0
fi

work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
curl --fail --silent --show-error --location --retry 3 --connect-timeout 15 --max-time 300 \
  "$url/$version/$arch/gvisor.tar.bz2" -o "$work/gvisor.tar.bz2"
curl --fail --silent --show-error --location --retry 3 --connect-timeout 15 --max-time 60 \
  "$url/$version/$arch/gvisor.tar.bz2.sha512" -o "$work/gvisor.tar.bz2.sha512"
(cd "$work" && sha512sum -c gvisor.tar.bz2.sha512)
mkdir "$work/unpacked"
tar -xjf "$work/gvisor.tar.bz2" -C "$work/unpacked"
test -x "$work/unpacked/runsc"
test -x "$work/unpacked/containerd-shim-runsc-v1"
test -x "$work/unpacked/gvisor-bin/gvisor_sentry"

install -d -m 755 "$bin"
printf '%s\n' "$version" > "$pending"
install -d -m 755 "$bin/gvisor-bin"
# Sidecars must be adjacent to runsc; install them before exposing the binary.
cp -R "$work/unpacked/gvisor-bin/." "$bin/gvisor-bin/"
install -m 755 "$work/unpacked/containerd-shim-runsc-v1" "$bin/containerd-shim-runsc-v1"
install -m 755 "$work/unpacked/runsc" "$bin/runsc"
rm -- "$pending"
echo "Installed complete gVisor release $version ($arch)"
