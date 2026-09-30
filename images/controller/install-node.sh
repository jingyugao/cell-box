#!/bin/sh
# Runs as a privileged hostPID init container; no SSH or node-side downloads.
set -eu

host=${HOST_ROOT:-/host}
mode=${CELLBOX_NODE_RUNTIME:-auto}
config=${CELLBOX_CONTAINERD_CONFIG:-}
unit=${CELLBOX_RUNTIME_SERVICE:-}
platform=${CELLBOX_RUNSC_PLATFORM:-systrap}
base="$host/var/lib/cellbox"
helper=/usr/local/bin/cellbox-runtime-config
adopt=

exec 9>"$host/run/cellbox-install.lock"
flock -x 9
case "$platform" in
  systrap) ;;
  kvm) test -c "$host/dev/kvm" || { echo 'KVM platform requires /dev/kvm on the node' >&2; exit 1; } ;;
  *) echo "Unsupported gVisor platform: $platform" >&2; exit 1 ;;
esac

test -x "$host/usr/local/bin/runsc" && test -x "$host/usr/local/bin/containerd-shim-runsc-v1" || {
  echo 'gVisor is missing; run install-gvisor.sh first' >&2; exit 1;
}
test -d "$host/run/systemd/system" || { echo 'Only systemd-managed nodes are supported' >&2; exit 1; }
if test "$mode" = auto; then
  if test -f "$host/var/lib/rancher/k3s/agent/etc/containerd/config.toml"; then
    mode=k3s
  else
    mode=containerd
  fi
fi
case "$mode" in
  containerd)
    config=${config:-/etc/containerd/config.toml}
    unit=${unit:-containerd.service}
    ;;
  k3s)
    config=${config:-/var/lib/rancher/k3s/agent/etc/containerd/config.toml}
    if test -z "$unit"; then
      if chroot "$host" /bin/systemctl cat k3s.service >/dev/null 2>&1; then
        unit=k3s.service
      else
        unit=k3s-agent.service
      fi
    fi
    ;;
  *) echo "Unsupported node runtime: $mode" >&2; exit 1 ;;
esac
case "$config" in /*) ;; *) echo 'containerd config must be an absolute host path' >&2; exit 1 ;; esac
chroot "$host" /bin/systemctl cat "$unit" >/dev/null
test -f "$host$config" || { echo "Missing containerd config: $config" >&2; exit 1; }

if test -f "$base/installed-by-cellbox"; then
  adopt=--adopt
elif test -d "$base" && test -n "$(find "$base" -mindepth 1 -print -quit)"; then
  echo 'Unowned nonempty Cellbox storage; refusing to overwrite' >&2; exit 1
fi

version=$($helper --host "$host" --config "$host$config" --version-only $adopt)
ready=$($helper --host "$host" --config "$host$config" --ready-only $adopt)
kind=config
target="$host$config"
if test "$mode" = k3s; then
  directory=$(dirname "$config")
  # Preserve an existing Cellbox-owned drop-in layout when K3s already imports it.
  if test -n "$adopt" && test -f "$host$directory/config-v3.toml.d/91-cellbox.toml" &&
      grep -Fq 'config-v3.toml.d/*.toml' "$host$config"; then
    kind=dropin
    target="$host$directory/config-v3.toml.d/91-cellbox.toml"
  else
    kind=template
    if test "$version" = 3; then
      target="$host$directory/config-v3.toml.tmpl"
    else
      target="$host$directory/config.toml.tmpl"
    fi
  fi
fi

work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
$helper --host "$host" --config "$host$config" --target "$target" --kind "$kind" \
  --output "$work/runtime.toml" $adopt
cat /opt/cellbox/runsc.toml > "$work/runsc.toml"
printf '  platform = "%s"\n' "$platform" >> "$work/runsc.toml"

install_if_changed() {
  source=$1 destination=$2 permissions=$3
  if ! cmp -s "$source" "$destination"; then
    if test -e "$destination" && ! test -f "$destination.cellbox-backup"; then
      cp -p "$destination" "$destination.cellbox-backup"
    fi
    install -m "$permissions" "$source" "$destination.cellbox-next"
    mv -f "$destination.cellbox-next" "$destination"
  fi
}

install -d -m 700 "$base" "$base/tickets" "$base/requests" "$base/claims" "$base/workloads" "$base/logs"
install -d -m 755 "$host/usr/local/libexec"
touch "$base/installed-by-cellbox"
install_if_changed /usr/local/bin/cellbox-runsc-wrapper "$host/usr/local/libexec/cellbox-runsc-wrapper" 755
install_if_changed "$work/runsc.toml" "$base/runsc.toml" 600
# Save the service generation before changing runtime config. If the restart
# interrupts this init container, a subsequent run can recognize its completion.
if test "$ready" != true && ! test -f "$base/restart-pending"; then
  chroot "$host" /bin/systemctl show "$unit" --property=ActiveEnterTimestampMonotonic --value > "$base/restart-pending"
fi
install_if_changed "$work/runtime.toml" "$target" 644

if test -f "$base/restart-pending"; then
  started=$(chroot "$host" /bin/systemctl show "$unit" --property=ActiveEnterTimestampMonotonic --value)
  previous=$(cat "$base/restart-pending")
  if test "$ready" = true && test -n "$previous" && test "$started" != "$previous"; then
    rm -f -- "$base/restart-pending"
  fi
fi
if test -f "$base/restart-pending"; then
  echo "Cellbox runtime configured for $mode (containerd config v$version); restarting $unit once"
  # systemd survives the init container; identical files prevent a restart loop.
  chroot "$host" /bin/systemctl --no-block restart "$unit"
  rm -f -- "$base/restart-pending"
else
  echo 'Cellbox node runtime is already configured; no restart needed'
fi
