#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/install-local-runsc.sh --ssh HOST [--image REPOSITORY@sha256:DIGEST]
  sudo bash ./install-local-runsc.sh [--platform systrap|kvm] [--port 8090]

The SSH form builds and transfers a self-contained Linux amd64 bundle.
The local form installs that bundle on Ubuntu/Debian with systemd.
Options: --image REF, --platform systrap|kvm, --port PORT, --skip-smoke
Existing configuration and sandbox data are preserved on repeat installation.
EOF
}

die() { printf '%s\n' "$*" >&2; exit 1; }
ssh_host= platform=systrap port=8090 image= skip_smoke=false
while (($#)); do
  case "$1" in
    --ssh|--image|--platform|--port)
      (($# >= 2)) || die "Missing value for $1"
      case "$1" in
        --ssh) ssh_host=$2 ;;
        --image) image=$2 ;;
        --platform) platform=$2 ;;
        --port) port=$2 ;;
      esac
      shift 2 ;;
    --skip-smoke) skip_smoke=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done
[[ $platform == systrap || $platform == kvm ]] || die 'Platform must be systrap or kvm'
[[ $port =~ ^[0-9]{1,5}$ ]] && ((10#$port >= 1 && 10#$port <= 65535)) || die 'Invalid port'
[[ -z $image || $image =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || die 'Image must be repository@sha256:digest'
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

if [[ -n $ssh_host ]]; then
  [[ $ssh_host =~ ^[A-Za-z0-9_][A-Za-z0-9_.@-]*$ ]] || die 'Use an SSH host or configured SSH alias'
  repo=$(cd -- "$script_dir/.." && pwd)
  [[ -f $repo/go.mod ]] || die 'Run --ssh from the source checkout'
  remote_arch=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$ssh_host" uname -m)
  [[ $remote_arch == x86_64 ]] || die 'The current image import API supports Linux amd64 only'
  make -C "$repo" local-package LOCAL_ARCH=amd64
  remote_dir=$(ssh -o BatchMode=yes "$ssh_host" 'mktemp -d /tmp/cellbox-local.XXXXXXXX')
  [[ $remote_dir =~ ^/tmp/cellbox-local\.[A-Za-z0-9]+$ ]] || die 'Unexpected remote staging directory'
  cleanup_remote() { ssh -o BatchMode=yes "$ssh_host" "rm -rf -- '$remote_dir'" || true; }
  trap cleanup_remote EXIT
  scp -q "$repo/dist/cellbox-local-runsc-linux-amd64.tar.gz" "$ssh_host:$remote_dir/bundle.tar.gz"
  remote_args=(--platform "$platform" --port "$port")
  [[ -z $image ]] || remote_args+=(--image "$image")
  $skip_smoke && remote_args+=(--skip-smoke)
  printf -v quoted_args ' %q' "${remote_args[@]}"
  ssh -o BatchMode=yes "$ssh_host" "tar -xzf '$remote_dir/bundle.tar.gz' -C '$remote_dir' && if [ \"\$(id -u)\" = 0 ]; then bash '$remote_dir/cellbox-local-runsc/install-local-runsc.sh'$quoted_args; else sudo -n bash '$remote_dir/cellbox-local-runsc/install-local-runsc.sh'$quoted_args; fi"
  printf 'Remote API: ssh -N -L %s:127.0.0.1:%s %s\n' "$port" "$port" "$ssh_host"
  exit 0
fi

[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || die 'Requires Linux amd64'
((EUID == 0)) || die 'Installation and this networked provider require root; run with sudo'
[[ -d /run/systemd/system ]] || die 'Requires systemd as the service manager'
command -v apt-get >/dev/null || die 'Automatic dependency installation supports Ubuntu/Debian'
[[ -f $script_dir/SHA256SUMS ]] || die 'Build with make local-package and extract the archive before local installation'
(cd -- "$script_dir" && sha256sum --check SHA256SUMS)
for binary in cellbox-server cellbox-container-agent; do
  [[ -x $script_dir/$binary ]] || die "Missing executable: $binary"
done
if [[ $platform == kvm && ! -c /dev/kvm ]]; then die 'KVM selected but /dev/kvm is absent; use systrap'; fi

unit=/etc/systemd/system/cellbox-local.service
if [[ -e $unit ]] && ! head -1 "$unit" | grep -Fxq '# Managed by Cellbox local-runsc installer.'; then
  die "Refusing to replace an unmanaged service: $unit"
fi
packages=()
for dependency in skopeo:skopeo umoci:umoci ip:iproute2 iptables:iptables curl:curl jq:jq bzip2:bzip2 sysctl:procps; do
  command -v "${dependency%%:*}" >/dev/null || packages+=("${dependency#*:}")
done
[[ -s /etc/ssl/certs/ca-certificates.crt ]] || packages+=(ca-certificates)
if ((${#packages[@]})); then
  apt-get update
  DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l apt-get install -y --no-install-recommends "${packages[@]}"
fi

prefix=/opt/cellbox-local
install -d -m 0755 "$prefix" "$prefix/releases" "$prefix/runtime"
if ! command -v runsc >/dev/null && [[ ! -x $prefix/runtime/runsc ]]; then
  # New gVisor releases require the complete sidecar layout next to runsc.
  download=$(mktemp -d)
  trap 'rm -rf -- "$download"' EXIT
  url=https://storage.googleapis.com/gvisor/releases/release/latest/x86_64
  curl -fL --retry 3 --connect-timeout 10 -o "$download/gvisor.tar.bz2" "$url/gvisor.tar.bz2"
  curl -fL --retry 3 --connect-timeout 10 -o "$download/gvisor.tar.bz2.sha512" "$url/gvisor.tar.bz2.sha512"
  (cd -- "$download" && sha512sum --check gvisor.tar.bz2.sha512)
  tar -xjf "$download/gvisor.tar.bz2" -C "$prefix/runtime"
  rm -rf -- "$download"
  trap - EXIT
fi
export PATH="$prefix/runtime:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
runsc --version

install -d -m 0700 /etc/cellbox-local /var/lib/cellbox-local
config=/etc/cellbox-local/config.json
if [[ ! -e $config ]]; then
  [[ -n $image ]] || image=$(jq -er '.profiles[0].image' "$script_dir/local-runsc.sample.json")
  jq --arg listen "127.0.0.1:$port" --arg image "$image" \
    '.listen=$listen | .dataDir="/var/lib/cellbox-local" | .profiles[0].image=$image' \
    "$script_dir/local-runsc.sample.json" > "$config.tmp"
  chmod 0600 "$config.tmp"
  mv -T "$config.tmp" "$config"
else
  printf 'Preserving existing configuration: %s\n' "$config"
fi
listen=$(jq -er '.listen' "$config")
[[ $listen =~ ^127\.0\.0\.1:[0-9]+$ ]] || die 'Installer health checks require an IPv4 loopback listen address'
api_url="http://$listen"
client=$(jq -r '.clientId // "internal"' "$config")
profile=$(jq -er '[.profiles[] | select(.provider=="local-runsc")][0].id' "$config")
image=$(jq -er --arg profile "$profile" '.profiles[] | select(.id==$profile) | .image' "$config")
revision=$(sha256sum "$script_dir/cellbox-server" | cut -c1-16)
release="$prefix/releases/$revision"
install -d -m 0755 "$release"
for binary in cellbox-server cellbox-container-agent; do
  # Reuse identical binaries so repeat installation never truncates a live executable.
  if [[ ! -f $release/$binary ]] || ! cmp -s "$script_dir/$binary" "$release/$binary"; then
    install -m 0755 "$script_dir/$binary" "$release/$binary.tmp"
    mv -T "$release/$binary.tmp" "$release/$binary"
  fi
done
ln -sfn "$release" "$prefix/current.tmp"
mv -Tf "$prefix/current.tmp" "$prefix/current"
printf 'net.ipv4.ip_forward=1\n' > /etc/sysctl.d/90-cellbox-local.conf
sysctl -w net.ipv4.ip_forward=1
cat > "$unit" <<EOF
# Managed by Cellbox local-runsc installer.
[Unit]
Description=Cellbox local gVisor sandbox service
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=root
UMask=0077
Environment=PATH=$prefix/runtime:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=$prefix/current/cellbox-server --config $config --local-runsc-platform=$platform
Restart=on-failure
RestartSec=3
TimeoutStopSec=90
KillMode=process
Delegate=yes
TasksMax=infinity
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable cellbox-local.service
systemctl restart cellbox-local.service
api() {
  local path=$1; shift
  curl -fsS --connect-timeout 3 --max-time 20 -H 'Content-Type: application/json' -H "X-Cellbox-Client-ID: $client" "$@" "$api_url$path"
}
ready=false
for ((i=0; i<30; i++)); do
  if curl -fsS --max-time 2 "$api_url/v1/profiles" >/dev/null; then ready=true; break; fi
  sleep 1
done
if ! $ready; then journalctl -u cellbox-local.service -n 40 --no-pager; die 'Cellbox did not become ready'; fi

wait_operation() {
  local op_id=$1 result status
  for ((attempt=0; attempt<300; attempt++)); do
    result=$(api "/v1/operations/$op_id?waitMs=1000")
    status=$(jq -er '.status' <<< "$result")
    case "$status" in
      succeeded) printf '%s\n' "$result"; return 0 ;;
      queued|running) sleep 0.1 ;;
      *) printf '%s\n' "$result" >&2; return 1 ;;
    esac
  done
  die "Operation did not finish: $op_id (query it before retrying)"
}

if ! $skip_smoke; then
  template=$(api '/v1/templates?name=local-runsc' | jq -r '.[0].id // empty')
  if [[ -z $template ]]; then template=$(api '/v1/templates' -X POST -d '{"name":"local-runsc"}' | jq -er '.id'); fi
  template_record=$(api "/v1/templates/$template")
  stable=$(jq -r '.stableRevisionId // empty' <<< "$template_record")
  if [[ -z $stable ]]; then
    input=$(jq -nc --arg profile "$profile" --arg image "$image" '{userVersion:"local",profileId:$profile,baseImage:$image}')
    build=$(api "/v1/templates/$template/revisions" -X POST -d "$input" | jq -er '.id')
    result=$(wait_operation "$build")
    stable=$(jq -er '.result.revisionId' <<< "$result")
    input=$(jq -nc --arg revision "$stable" --argjson generation "$(jq -r '.generation' <<< "$template_record")" '{stableRevisionId:$revision,expectedGeneration:$generation}')
    api "/v1/templates/$template" -X PATCH -d "$input" >/dev/null
  fi
  smoke_id="install-check-$(date +%s)-$$"
  smoke_volume=
  cleanup_smoke() {
    local op record
    if [[ -n $smoke_id ]]; then
      record=$(api "/v1/boxes/$smoke_id") || return 1
      smoke_volume=$(jq -r '.homeVolumeId // empty' <<< "$record")
      if [[ $(jq -r '.phase' <<< "$record") != deleted ]]; then
        op=$(api "/v1/boxes/$smoke_id:destroy" -X POST -d '{}' | jq -er '.id') || return 1
        wait_operation "$op" >/dev/null || return 1
      fi
    fi
    if [[ -n $smoke_volume ]]; then
      record=$(api "/v1/volumes/$smoke_volume") || return 1
      if [[ $(jq -r '.phase' <<< "$record") != deleted ]]; then
        op=$(api "/v1/volumes/$smoke_volume" -X DELETE | jq -er '.id') || return 1
        wait_operation "$op" >/dev/null || return 1
      fi
    fi
  }
  trap 'cleanup_smoke || true' EXIT
  input=$(jq -nc --arg id "$smoke_id" --arg revision "$stable" '{id:$id,ownerKey:"installer-smoke",revisionId:$revision}')
  op=$(api '/v1/boxes' -X POST -d "$input" | jq -er '.id')
  wait_operation "$op" >/dev/null
  generation=$(api "/v1/boxes/$smoke_id" | jq -er '.generation')
  input=$(jq -nc --arg token "$smoke_id" --argjson generation "$generation" '{argv:["/bin/sh","-c","id; test -r /etc/resolv.conf && printf %s \"$1\" >/tmp/cellbox-checkpoint-token && printf cellbox-gvisor-ok","smoke",$token],expectedGeneration:$generation}')
  op=$(api "/v1/boxes/$smoke_id/execs" -X POST -d "$input" | jq -er '.id')
  result=$(wait_operation "$op")
  execution=$(api "/v1/execs/$(jq -er '.result.execId' <<< "$result")")
  jq -e '.result.exitCode==0 and (.result.stdout | contains("cellbox-gvisor-ok"))' <<< "$execution" >/dev/null
  printf '%s\n' "$execution" | jq -r '.result.stdout'
  input=$(jq -nc --argjson generation "$generation" '{expectedGeneration:$generation}')
  op=$(api "/v1/boxes/$smoke_id:suspend" -X POST -d "$input" | jq -er '.id')
  wait_operation "$op" >/dev/null
  api '/v1/checkpoints' | jq -e --arg id "$smoke_id" 'any(.[]; .id==$id and .phase=="suspended" and .imageId!=null)' >/dev/null
  op=$(api "/v1/boxes/$smoke_id:resume" -X POST -d "$input" | jq -er '.id')
  wait_operation "$op" >/dev/null
  generation=$(api "/v1/boxes/$smoke_id" | jq -er '.generation')
  ((generation >= 2)) || die 'Resume did not advance the sandbox generation'
  input=$(jq -nc --argjson generation "$generation" '{argv:["/bin/cat","/tmp/cellbox-checkpoint-token"],expectedGeneration:$generation}')
  op=$(api "/v1/boxes/$smoke_id/execs" -X POST -d "$input" | jq -er '.id')
  result=$(wait_operation "$op")
  execution=$(api "/v1/execs/$(jq -er '.result.execId' <<< "$result")")
  jq -e --arg token "$smoke_id" '.result.exitCode==0 and .result.stdout==$token' <<< "$execution" >/dev/null
  api '/v1/checkpoints' | jq -e --arg id "$smoke_id" 'all(.[]; .id!=$id)' >/dev/null
  printf 'Checkpoint/resume: restored tmpfs token, generation %s\n' "$generation"
  cleanup_smoke
  trap - EXIT
fi
systemctl is-active --quiet cellbox-local.service
printf '\nInstalled: cellbox-local.service\nAPI: %s\nConfig: %s\n' "$api_url" "$config"
$skip_smoke || printf 'Template: local-runsc\n'
