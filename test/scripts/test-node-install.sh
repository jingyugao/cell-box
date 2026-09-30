#!/bin/bash
# Run installer regression checks in an isolated container with mocked systemd.
set -euo pipefail
[[ $# == 1 ]] || { echo "Usage: $0 CONTROLLER_IMAGE" >&2; exit 2; }
docker run --rm -i --entrypoint /bin/sh "$1" -s <<'TEST'
set -eu
host=/tmp/host
mkdir -p "$host/run/systemd/system" "$host/usr/local/bin" "$host/etc/containerd" /tmp/mock
printf '#!/bin/sh\nexit 0\n' > "$host/usr/local/bin/runsc"
cp "$host/usr/local/bin/runsc" "$host/usr/local/bin/containerd-shim-runsc-v1"
chmod +x "$host/usr/local/bin/"*
printf '100\n' > "$host/run/generation"
cat > /tmp/mock/chroot <<'MOCK'
#!/bin/sh
set -eu
host=$1
shift 2
case "$1" in
  cat) exit 0 ;;
  show) cat "$host/run/generation" ;;
  --no-block)
    echo restart >> "$host/run/restarts"
    value=$(cat "$host/run/generation")
    echo "$((value + 1))" > "$host/run/generation"
    ;;
  *) echo "Unexpected host command: $*" >&2; exit 1 ;;
esac
MOCK
chmod +x /tmp/mock/chroot
export PATH=/tmp/mock:$PATH HOST_ROOT=$host CELLBOX_NODE_RUNTIME=containerd
printf 'version = 3\n[grpc]\naddress = "/custom/containerd.sock"\n' > "$host/etc/containerd/config.toml"
install_node() { /bin/sh /usr/local/bin/install-node.sh; }
restart_count() { wc -l < "$host/run/restarts"; }

install_node
test "$(restart_count)" -eq 1
install_node
test "$(restart_count)" -eq 1
# Binary and shim configuration updates must not restart the node runtime.
printf 'outdated wrapper\n' > "$host/usr/local/libexec/cellbox-runsc-wrapper"
printf 'outdated shim config\n' > "$host/var/lib/cellbox/runsc.toml"
install_node
test "$(restart_count)" -eq 1
grep -q 'address = "/custom/containerd.sock"' "$host/etc/containerd/config.toml"
# A failed attempt still needs a restart; a completed restart must not loop.
cat "$host/run/generation" > "$host/var/lib/cellbox/restart-pending"
install_node
test "$(restart_count)" -eq 2
printf '100\n' > "$host/var/lib/cellbox/restart-pending"
install_node
test "$(restart_count)" -eq 2

# Migrating an already-active K3s drop-in only changes formatting/ownership.
directory="$host/var/lib/rancher/k3s/agent/etc/containerd"
mkdir -p "$directory/config-v3.toml.d"
printf 'version = 3\nimports = ["/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/*.toml"]\n' > "$directory/config.toml"
sed -n '/^\[plugins\./,/^# END CELLBOX RUNTIME/{ /^# END CELLBOX RUNTIME/d; p; }' \
  "$host/etc/containerd/config.toml" > "$directory/config-v3.toml.d/91-cellbox.toml"
export CELLBOX_NODE_RUNTIME=k3s
install_node
test "$(restart_count)" -eq 2
install_node
test "$(restart_count)" -eq 2
echo 'PASS: fresh install restarts once; upgrades, repeat installs, and completed restarts do not'
TEST
