#!/bin/sh
# Runs as a privileged DaemonSet init container. gVisor itself is preinstalled.
set -eu

host=/host
base="$host/var/lib/cellbox"
dropin="$host/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/91-cellbox.toml"
legacy_base="$host/var/lib/resumablepod"
legacy_dropin="$host/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/91-resumablepod.toml"
adapter="$host/usr/local/libexec/resumablepod-runtime"

test -x "$host/usr/local/bin/runsc" || { echo 'gVisor runsc is missing on this node' >&2; exit 1; }
test -x "$host/usr/local/bin/containerd-shim-runsc-v1" || { echo 'gVisor shim is missing on this node' >&2; exit 1; }
grep -Fq 'config-v3.toml.d/*.toml' "$host/var/lib/rancher/k3s/agent/etc/containerd/config.toml" || {
  echo 'K3s containerd does not import config-v3.toml.d/*.toml' >&2
  exit 1
}
test -d "$host/run/systemd/system" || { echo 'systemd is not running on this node' >&2; exit 1; }
if test -f "$host/etc/systemd/system/k3s.service"; then
  k3s_unit=k3s.service
elif test -f "$host/etc/systemd/system/k3s-agent.service"; then
  k3s_unit=k3s-agent.service
else
  echo 'Neither k3s.service nor k3s-agent.service is installed' >&2
  exit 1
fi
if test -e "$dropin" && ! test -f "$base/installed-by-cellbox"; then
  echo 'Unowned Cellbox storage or containerd drop-in; refusing to overwrite' >&2
  exit 1
fi
if test -e "$legacy_dropin" && ! test -f "$legacy_base/installed-by-resumablepod"; then
  echo 'Unowned legacy containerd drop-in; refusing to replace' >&2
  exit 1
fi
if test -d "$base" && ! test -f "$base/installed-by-cellbox" &&
    test -n "$(find "$base" -mindepth 1 -print -quit)"; then
  echo 'Unowned nonempty Cellbox storage; refusing to overwrite' >&2
  exit 1
fi

install -d -m 700 "$base" "$base/tickets" "$base/requests" "$base/claims" "$base/workloads" "$base/logs"
install -d -m 755 "$host/usr/local/libexec" "$(dirname "$dropin")"
touch "$base/installed-by-cellbox"

changed=0
install_if_changed() {
  source=$1 target=$2 mode=$3
  if ! cmp -s "$source" "$target"; then
    install -m "$mode" "$source" "$target.next"
    mv -f "$target.next" "$target"
    changed=1
  fi
}
install_if_changed /usr/local/bin/resumablepod-runtime "$adapter" 755
install_if_changed /opt/resumablepod/runsc.toml "$base/runsc.toml" 600
install_if_changed /opt/resumablepod/runtime.toml "$dropin" 644
if test -e "$legacy_dropin"; then
  rm -- "$legacy_dropin"
  changed=1
fi

# Migrating from the old host install must leave only one controller per node.
if test -f "$host/etc/systemd/system/resumablepod-controller.service"; then
  chroot "$host" /bin/systemctl disable --now resumablepod-controller.service
fi

if test "$changed" -eq 1; then
  echo 'Runtime configuration changed; asking systemd to restart K3s once'
  # The restart may terminate this init container. On rescheduling, the file
  # comparison succeeds and the script exits without another restart.
  chroot "$host" /bin/systemctl --no-block restart "$k3s_unit"
fi
