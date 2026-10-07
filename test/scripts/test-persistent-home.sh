#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$repo_root/test/fixtures/persistent-home/main.c"
runsc_bin="${RUNSC_BIN:-/usr/local/bin/runsc}"
image="${GVISOR_HOME_TEST_IMAGE:-busybox:1.37.0}"
overlay_mode="${GVISOR_HOME_TEST_OVERLAY2:-root:self}"
test_root="$(mktemp -d /tmp/gvisor-persistent-home.XXXXXX)"
runtime_root="$test_root/runsc-root"
baseline_id="ph-base-$$"
workload_id="ph-work-$$"
baseline_created=0
workload_created=0
rootfs_created=0
rootfs_container="ph-rootfs-$$"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  if (( rootfs_created )); then
    docker rm "$rootfs_container" >/dev/null 2>&1 || true
  fi
  if (( workload_created )); then
    sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
      --root "$runtime_root" delete --force "$workload_id" >/dev/null 2>&1 || true
  fi
  if (( baseline_created )); then
    sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
      --root "$runtime_root" delete --force "$baseline_id" >/dev/null 2>&1 || true
  fi
  if mountpoint -q "$runtime_root/null-netns"; then
    sudo -n umount "$runtime_root/null-netns" || true
  fi
  if [[ -e "$test_root" ]]; then sudo -n rm -r "$test_root"; fi
}
trap cleanup EXIT

command -v docker >/dev/null || fail 'docker is required to export the local rootfs'
command -v jq >/dev/null || fail 'jq is required'
command -v gcc >/dev/null || fail 'gcc is required'
command -v sha256sum >/dev/null || fail 'sha256sum is required'
[[ -x "$runsc_bin" ]] || fail "runsc is not executable: $runsc_bin"
docker image inspect "$image" >/dev/null 2>&1 || fail "image must already exist locally: $image"
sudo -n true || fail 'sudo must already be authorized (run sudo -v in an interactive terminal)'

printf 'runsc=%s\n' "$("$runsc_bin" --version | tr '\n' ' ')"
printf 'host_page_size=%s\n' "$(getconf PAGESIZE)"
printf 'platform=ptrace network=none overlay2=%s\n' "$overlay_mode"
df -h "$test_root"

mkdir -p "$test_root/rootfs" "$test_root/bundle" "$test_root/home" \
  "$test_root/control-baseline" "$test_root/control-workload" "$runtime_root"
sudo -n chown 1000:1000 "$test_root/home"
sudo -n chmod 0750 "$test_root/home"
chmod 0777 "$test_root/control-baseline" "$test_root/control-workload"

docker create --network=none --name "$rootfs_container" "$image" >/dev/null
rootfs_created=1
docker export "$rootfs_container" | tar -xf - -C "$test_root/rootfs"
docker rm "$rootfs_container" >/dev/null
rootfs_created=0
mkdir -p "$test_root/rootfs/probe"
chmod 0777 "$test_root/rootfs/probe"

gcc -O2 -Wall -Wextra -Werror -static "$fixture" -o "$test_root/rootfs/persistent-home"
chmod 0755 "$test_root/rootfs/persistent-home"

make_bundle() {
  local bundle="$1"
  local control="$2"
  local mode="$3"
  mkdir -p "$bundle"
  cp -a "$test_root/rootfs" "$bundle/rootfs"
  sudo -n "$runsc_bin" spec --bundle "$bundle" -- \
    /persistent-home "$mode" /control
  jq --arg home "$test_root/home" --arg control "$control" '
    .process.user.uid = 1000 |
    .process.user.gid = 1000 |
    .root.readonly = false |
    .mounts += [
      {destination:"/home/agent", type:"none", source:$home, options:["bind","rw","rprivate"]},
      {destination:"/control", type:"none", source:$control, options:["bind","rw","rprivate"]}
    ]
  ' "$bundle/config.json" > "$bundle/config.json.tmp"
  mv "$bundle/config.json.tmp" "$bundle/config.json"
}

wait_for_file() {
  local path="$1"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    [[ -f "$path" ]] && return 0
    sleep 0.1
  done
  fail "timed out waiting for $path"
}

checkpoint() {
  local id="$1"
  local image_path="$2"
  local label="$3"
  local begin end
  begin="$(date +%s%N)"
  sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
    --root "$runtime_root" checkpoint --image-path "$image_path" "$id"
  end="$(date +%s%N)"
  printf '%s_checkpoint_ms=%s\n' "$label" "$(((end - begin) / 1000000))"
  printf '%s_checkpoint_bytes=%s\n' "$label" "$(du -sb "$image_path" | cut -f1)"
  [[ -f "$image_path/pages.img" ]] || fail "$label checkpoint has no pages.img"
  printf '%s_pages_img_bytes=%s\n' "$label" "$(stat -c %s "$image_path/pages.img")"
  find "$image_path" -maxdepth 3 -type f -printf '%P %s bytes\n' | sort | sed "s/^/${label}_checkpoint_file=/"
  find "$runtime_root" -maxdepth 6 -type f -printf '%P %s bytes\n' | sort | sed "s/^/${label}_runtime_file=/"
}

make_bundle "$test_root/baseline-bundle" "$test_root/control-baseline" baseline
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" create --bundle "$test_root/baseline-bundle" "$baseline_id"
baseline_created=1
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" start "$baseline_id"
wait_for_file "$test_root/control-baseline/ready"
checkpoint "$baseline_id" "$test_root/checkpoint-baseline" baseline
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" delete "$baseline_id"
baseline_created=0
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" restore --bundle "$test_root/baseline-bundle" \
  --image-path "$test_root/checkpoint-baseline" --detach "$baseline_id"
baseline_created=1
touch "$test_root/control-baseline/continue"
wait_for_file "$test_root/control-baseline/post.txt"
grep -q '^stage=baseline-restored$' "$test_root/control-baseline/post.txt" || fail 'baseline restore did not retain guest root overlay marker'
cat "$test_root/control-baseline/post.txt"
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" delete --force "$baseline_id"
baseline_created=0

make_bundle "$test_root/workload-bundle" "$test_root/control-workload" workload
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" create --bundle "$test_root/workload-bundle" "$workload_id"
workload_created=1
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" start "$workload_id"
wait_for_file "$test_root/control-workload/ready"

payload="$test_root/home/payload-2g.bin"
payload_size="$(stat -c %s "$payload")"
payload_blocks="$(stat -c %b "$payload")"
payload_block_size="$(stat -c %B "$payload")"
[[ "$payload_size" == 2147483648 ]] || fail "payload size is $payload_size, expected 2147483648"
(( payload_blocks * payload_block_size >= payload_size )) || fail 'payload is sparse or not fully allocated'
hash_before="$(sha256sum "$payload" | cut -d' ' -f1)"
printf 'payload_size=%s payload_allocated=%s payload_sha256=%s\n' \
  "$payload_size" "$((payload_blocks * payload_block_size))" "$hash_before"
cat "$test_root/control-workload/pre.txt"

shared_host="$test_root/home/shared.dat"
private_host="$test_root/home/private.dat"
[[ "$(head -c 24 "$shared_host")" == 'shared-memory-checkpoint' ]] || fail 'MAP_SHARED was not written through to host'
[[ "$(od -An -tx1 -N 1 "$private_host" | tr -d ' \n')" == '00' ]] || fail 'MAP_PRIVATE changed host backing file'

checkpoint "$workload_id" "$test_root/checkpoint-workload" workload
baseline_bytes="$(du -sb "$test_root/checkpoint-baseline" | cut -f1)"
workload_bytes="$(du -sb "$test_root/checkpoint-workload" | cut -f1)"
printf 'checkpoint_growth_bytes=%s\n' "$((workload_bytes - baseline_bytes))"
(( workload_bytes - baseline_bytes < 16 * 1024 * 1024 )) || fail 'checkpoint grew by 16 MiB or more after writing external HOME data'

sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" delete "$workload_id"
workload_created=0
sudo -n "$runsc_bin" --ignore-cgroups --platform=ptrace --network=none --overlay2="$overlay_mode" \
  --root "$runtime_root" restore --bundle "$test_root/workload-bundle" \
  --image-path "$test_root/checkpoint-workload" --detach "$workload_id"
workload_created=1

wait_for_file "$test_root/control-workload/ready"
touch "$test_root/control-workload/continue"
wait_for_file "$test_root/control-workload/post.txt"
cat "$test_root/control-workload/post.txt"
grep -q '^stage=restored$' "$test_root/control-workload/post.txt" || fail 'restored process did not finish checks'
pre_pid="$(sed -n 's/^pid=//p' "$test_root/control-workload/pre.txt")"
post_pid="$(sed -n 's/^pid=//p' "$test_root/control-workload/post.txt")"
pre_checksum="$(sed -n 's/^checksum=//p' "$test_root/control-workload/pre.txt")"
post_checksum="$(sed -n 's/^checksum=//p' "$test_root/control-workload/post.txt")"
[[ "$pre_pid" == "$post_pid" ]] || fail "process identity changed across restore ($pre_pid -> $post_pid)"
[[ "$pre_checksum" == "$post_checksum" ]] || fail 'resident-memory marker changed across restore'
[[ ! -e "$test_root/home/deleted.dat" ]] || fail 'deleted-open path unexpectedly reappeared'
hash_after="$(sha256sum "$payload" | cut -d' ' -f1)"
[[ "$hash_before" == "$hash_after" ]] || fail 'payload hash changed across checkpoint/restore'
printf 'payload_sha256_after=%s\n' "$hash_after"
printf 'PASS: root:self guest root overlay and bind-mounted /home/agent survived checkpoint/delete/restore; fd offset, deleted-open fd, MAP_SHARED, MAP_PRIVATE, process identity, resident memory marker, and 2 GiB external payload verified\n'
