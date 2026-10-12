#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
runsc_bin="${RUNSC_BIN:-}"
platform="${WARM_HOME_PLATFORM:-}"
overlay_mode="${WARM_HOME_OVERLAY2:-root:memory}"
log_file="${WARM_HOME_LOG:-/tmp/cellbox-warm-home-$(date +%Y%m%d-%H%M%S).log}"
exec > >(tee -a "$log_file") 2>&1
[[ -n "$runsc_bin" ]] || { echo 'RUNSC_BIN must name the runsc binary to test' >&2; exit 2; }
[[ -n "$platform" ]] || { echo 'WARM_HOME_PLATFORM must explicitly be kvm or systrap' >&2; exit 2; }
[[ "$platform" == kvm || "$platform" == systrap ]] || { echo 'WARM_HOME_PLATFORM must be kvm or systrap' >&2; exit 2; }
[[ -x "$runsc_bin" ]] || { echo "RUNSC_BIN is not executable: $runsc_bin" >&2; exit 2; }
command -v go >/dev/null || { echo 'go is required' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required' >&2; exit 2; }
command -v strings >/dev/null || { echo 'strings is required' >&2; exit 2; }
sudo -n true || { echo 'sudo must already be authorized (run sudo -v interactively)' >&2; exit 2; }

test_root="$(mktemp -d /tmp/cellbox-warm-home.XXXXXX)"
runtime_root="$test_root/runsc-root"
rootfs="$test_root/rootfs"
state="$test_root/cellbox"
adapter_bin="$test_root/adapter-runsc"
probe_bin="$test_root/probe"
namespace="warm-home-test"
spec_hash="warm-home-test-spec"
created_ids=()
background_pids=()

fail() { echo "FAIL: $*" >&2; exit 1; }
id_for() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
host_jq() { sudo -n cat -- "$1" | jq -r "$2"; }

cleanup() {
  local status=$? cleanup_failed=0 pid id
  trap - EXIT
  for pid in "${background_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  for id in "${created_ids[@]}"; do
    if ! sudo -n "$adapter_bin" runsc --base="$state" --runsc="$runsc_bin" -- \
      --root "$runtime_root" --platform="$platform" --network=none --ignore-cgroups=true \
      --overlay2="$overlay_mode" delete --force "$id" >/dev/null 2>&1; then
      if sudo -n "$adapter_bin" runsc --base="$state" --runsc="$runsc_bin" -- \
        --root "$runtime_root" --platform="$platform" --network=none --ignore-cgroups=true \
        --overlay2="$overlay_mode" state "$id" >/dev/null 2>&1; then
        echo "cleanup: runtime $id still exists after delete" >&2
        cleanup_failed=1
      fi
    fi
  done
  for id in "${created_ids[@]}"; do
    if sudo -n "$adapter_bin" runsc --base="$state" --runsc="$runsc_bin" -- \
      --root "$runtime_root" --platform="$platform" --network=none --ignore-cgroups=true \
      --overlay2="$overlay_mode" state "$id" >/dev/null 2>&1; then
      echo "cleanup: runtime $id remains in the isolated runsc root" >&2
      cleanup_failed=1
    fi
  done
  if mountpoint -q "$runtime_root/null-netns" && ! sudo -n umount "$runtime_root/null-netns"; then
    echo "cleanup: failed to unmount $runtime_root/null-netns" >&2
    cleanup_failed=1
  fi
  if (( status != 0 || cleanup_failed != 0 )) && [[ -d "$test_root" ]]; then
    echo "warm-home artifacts retained at $test_root" >&2
  elif [[ -d "$test_root" ]]; then
    sudo -n rm -rf -- "$test_root" || cleanup_failed=1
  fi
  if (( status == 0 && cleanup_failed != 0 )); then exit 1; fi
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$runtime_root" "$rootfs/proc" "$rootfs/dev" "$rootfs/tmp" \
  "$rootfs/home/agent" "$test_root/bundles"
chmod 0755 "$rootfs" "$rootfs/proc" "$rootfs/dev" "$rootfs/tmp" "$rootfs/home" "$rootfs/home/agent"
CGO_ENABLED=0 go build -o "$probe_bin" ./test/fixtures/warm-home/probe
go build -o "$adapter_bin" ./test/fixtures/warm-home/adapter-runsc
install -m 0755 "$probe_bin" "$rootfs/probe"

printf 'runsc=%s\n' "$("$runsc_bin" --version | tr '\n' ' ')"
printf 'platform=%s overlay2=%s\n' "$platform" "$overlay_mode"
printf 'artifacts=%s\n' "$test_root"
printf 'log=%s\n' "$log_file"

run_adapter() {
  sudo -n "$adapter_bin" runsc --base="$state" --runsc="$runsc_bin" -- \
    --root "$runtime_root" --platform="$platform" --network=none --ignore-cgroups=true \
    --overlay2="$overlay_mode" --restore-spec-validation=enforce "$@"
}

make_bundle() {
  local cid="$1" kind="$2" pod_uid="$3" pod_name="$4" sid="${5:-}" home_source="${6:-}"
  local bundle="$test_root/bundles/$cid"
  mkdir -p "$bundle"
  local args='["/probe","pause"]'
  local annotations
  if [[ "$kind" == child ]]; then
    args='["/probe","worker"]'
    annotations="{\"io.kubernetes.cri.container-type\":\"container\",\"io.kubernetes.cri.container-name\":\"agent\",\"io.kubernetes.cri.sandbox-id\":\"$sid\",\"io.kubernetes.cri.sandbox-uid\":\"$pod_uid\",\"io.kubernetes.cri.sandbox-name\":\"$pod_name\",\"io.kubernetes.cri.sandbox-namespace\":\"$namespace\"}"
  else
    annotations="{\"io.kubernetes.cri.container-type\":\"sandbox\",\"io.kubernetes.cri.sandbox-uid\":\"$pod_uid\",\"io.kubernetes.cri.sandbox-name\":\"$pod_name\",\"io.kubernetes.cri.sandbox-namespace\":\"$namespace\"}"
  fi
  jq -n --arg root "$rootfs" --arg home "$home_source" --argjson args "$args" --argjson annotations "$annotations" '{
    ociVersion:"1.2.0", root:{path:$root,readonly:true},
    process:{terminal:false,user:{uid:0,gid:0},args:$args,env:["PATH=/","HOME=/home/agent"],cwd:"/",
      capabilities:{bounding:["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FOWNER"],effective:["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FOWNER"],permitted:["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FOWNER"]}},
    mounts:([{destination:"/proc",type:"proc",source:"proc"},
      {destination:"/dev",type:"tmpfs",source:"tmpfs",options:["nosuid","strictatime","mode=755","size=65536k"]},
      {destination:"/tmp",type:"tmpfs",source:"tmpfs"}] +
      (if $home == "" then [] else [{destination:"/home/agent",type:"bind",source:$home,options:["rbind","rprivate","rw"]}] end)),
    linux:{namespaces:[{type:"pid"},{type:"ipc"},{type:"uts"},{type:"mount"},{type:"network"}]},
    annotations:$annotations
  }' > "$bundle/config.json"
  printf '%s' "$bundle"
}

add_ticket_annotation() {
  local bundle="$1" uid="$2"
  local temp="$bundle/config.json.tmp"
  jq --arg key 'dev.gvisor.internal.recovery.ticket' --arg uid "$uid" '.annotations[$key]=$uid' \
    "$bundle/config.json" > "$temp"
  mv "$temp" "$bundle/config.json"
}

create_root() {
  local id="$1" bundle="$2"
  run_adapter create --bundle "$bundle" "$id"
  created_ids+=("$id")
}

create_child() {
  local id="$1" bundle="$2"
  run_adapter create --bundle "$bundle" "$id"
  created_ids+=("$id")
  run_adapter start "$id"
}

wait_json_predicate() {
  local path="$1" jq_expr="$2" seconds="${3:-120}"
  local deadline=$((SECONDS + seconds)) content
  while (( SECONDS < deadline )); do
    if sudo -n test -f "$path"; then
      content="$(sudo -n cat -- "$path")" || fail "cannot read heartbeat $path"
      jq -e 'type == "object"' <<<"$content" >/dev/null || fail "invalid JSON in $path"
      if jq -e "$jq_expr" <<<"$content" >/dev/null; then return 0; fi
    fi
    sleep 0.1
  done
  fail "timed out waiting for condition $jq_expr in $path"
}

delete_pair() {
  local child="$1" root="$2" id index
  for id in "$child" "$root"; do
    if ! run_adapter delete --force "$id"; then
      if run_adapter state "$id" >/dev/null 2>&1; then fail "runtime $id remains after delete"; fi
    fi
    for index in "${!created_ids[@]}"; do
      if [[ "${created_ids[$index]}" == "$id" ]]; then unset 'created_ids[index]'; fi
    done
  done
}

declare -a owners=(owner-a owner-b)
declare -a home_paths snapshots old_root_ids old_child_ids old_pod_uids old_tokens old_guest_pids old_counters old_open_sizes old_deleted_sizes

for index in 0 1; do
  owner="${owners[$index]}"
  initial_uid="$(id_for "initial-$owner")"
  root_id="$(id_for "original-root-$owner")"
  child_id="$(id_for "original-child-$owner")"
  pod_name="original-$owner"
  sudo -n "$adapter_bin" prepare --base="$state" --owner="$owner" --node=warm-home-test-node --spec="$spec_hash" >/dev/null
  home_path="$state/homes/$owner/data"
  home_paths[$index]="$home_path"
  printf 'HOME-%s\n' "$owner" | sudo -n tee "$home_path/identity" >/dev/null
  sudo -n chmod 0600 "$home_path/identity"
  sudo -n "$adapter_bin" ticket --base="$state" --owner="$owner" --uid="$initial_uid" \
    --name="$pod_name" --namespace="$namespace" --spec="$spec_hash" >/dev/null
  root_bundle="$(make_bundle "$root_id" sandbox "$initial_uid" "$pod_name")"
  add_ticket_annotation "$root_bundle" "$initial_uid"
  create_root "$root_id" "$root_bundle"
  run_adapter start "$root_id"
  child_bundle="$(make_bundle "$child_id" child "$initial_uid" "$pod_name" "$root_id" "$home_path")"
  create_child "$child_id" "$child_bundle"
  wait_json_predicate "$home_path/heartbeat.json" '.counter >= 2'
  old_pod_uids[$index]="$initial_uid"
  old_root_ids[$index]="$root_id"
  old_child_ids[$index]="$child_id"
  old_tokens[$index]="$(host_jq "$home_path/heartbeat.json" .token)"
  old_guest_pids[$index]="$(host_jq "$home_path/heartbeat.json" .pid)"
  old_counters[$index]="$(host_jq "$home_path/heartbeat.json" .counter)"
  old_open_sizes[$index]="$(host_jq "$home_path/heartbeat.json" .fd.size)"
  old_deleted_sizes[$index]="$(host_jq "$home_path/heartbeat.json" .deleted_fd.size)"
  snapshot="snapshot-$owner-1"
  snapshots[$index]="$snapshot"
  checkpoint_path="$state/workloads/$owner/$snapshot"
  sudo -n mkdir -p "$checkpoint_path"
  run_adapter checkpoint --image-path="$checkpoint_path" --compression=none "$root_id"
  sudo -n "$adapter_bin" checkpoint --base="$state" --owner="$owner" --uid="$initial_uid" \
    --snapshot="$snapshot" --runsc="$runsc_bin" --spec="$spec_hash" >/dev/null
  delete_pair "$child_id" "$root_id"
done

for cycle in 1 2; do
  # Prepare both roots before either ticket selects a HOME.
  declare -a warm_uids warm_names warm_root_ids warm_root_pids warm_root_bundles
  for index in 0 1; do
    owner="${owners[$index]}"
    uid="$(id_for "warm-$owner-cycle-$cycle")"
    name="warm-$owner-cycle-$cycle"
    root_id="$(id_for "warm-root-$owner-cycle-$cycle")"
    warm_uids[$index]="$uid"
    warm_names[$index]="$name"
    warm_root_ids[$index]="$root_id"
    sudo -n "$adapter_bin" register-warm --base="$state" --uid="$uid" --name="$name" \
      --namespace="$namespace" --spec="$spec_hash"
    root_bundle="$(make_bundle "$root_id" sandbox "$uid" "$name")"
    create_root "$root_id" "$root_bundle"
    warm_root_bundles[$index]="$root_bundle"
    warm_root_pids[$index]="$(run_adapter state "$root_id" | jq -r .pid)"
  done

  for index in 0 1; do
    owner="${owners[$index]}"
    uid="${warm_uids[$index]}"
    name="${warm_names[$index]}"
    root_id="${warm_root_ids[$index]}"
    child_id="$(id_for "warm-child-$owner-cycle-$cycle")"
    snapshot="${snapshots[$index]}"
    restore_started_ns="$(date +%s%N)"
    run_adapter start "$root_id" > "$test_root/start-$owner-$cycle.log" 2>&1 &
    starter_pid=$!
    background_pids+=("$starter_pid")
    sudo -n "$adapter_bin" claim-warm --base="$state" --owner="$owner" --uid="$uid" \
      --name="$name" --namespace="$namespace" --snapshot="$snapshot" --spec="$spec_hash"
    if ! wait "$starter_pid"; then cat "$test_root/start-$owner-$cycle.log" >&2; fail "warm restore start failed for $owner cycle $cycle"; fi
    for bg_index in "${!background_pids[@]}"; do
      if [[ "${background_pids[$bg_index]}" == "$starter_pid" ]]; then unset 'background_pids[bg_index]'; fi
    done

    child_bundle="$(make_bundle "$child_id" child "$uid" "$name" "$root_id")"
    create_child "$child_id" "$child_bundle"
    home_path="${home_paths[$index]}"
    deadline=$((SECONDS + 120))
    advanced=0
    while (( SECONDS < deadline )); do
      if sudo -n test -f "$home_path/heartbeat.json"; then
        current_counter="$(host_jq "$home_path/heartbeat.json" .counter)" || fail "cannot read $owner heartbeat"
        if (( current_counter > ${old_counters[$index]} )); then advanced=1; break; fi
      fi
      sleep 0.1
    done
    (( advanced == 1 )) || fail "restored worker did not advance for $owner cycle $cycle"
    restore_elapsed_ms="$(( ($(date +%s%N) - restore_started_ns) / 1000000 ))"
    beat="$home_path/heartbeat.json"
    [[ "$(host_jq "$beat" .token)" == "${old_tokens[$index]}" ]] || fail "$owner restarted its guest process"
    [[ "$(host_jq "$beat" .pid)" == "${old_guest_pids[$index]}" ]] || fail "$owner guest PID changed"
    [[ "$(host_jq "$beat" .cwd)" == /home/agent/workspace ]] || fail "$owner working directory was not restored"
    [[ "$(host_jq "$beat" .identity)" == "HOME-$owner" ]] || fail "$owner received the wrong HOME"
    (( $(host_jq "$beat" .fd.size) > ${old_open_sizes[$index]} )) || fail "$owner open file descriptor did not continue"
    (( $(host_jq "$beat" .deleted_fd.size) > ${old_deleted_sizes[$index]} )) || fail "$owner deleted-open descriptor did not continue"
    sudo -n test ! -e "$home_path/deleted-open.txt" || fail "$owner deleted-open path reappeared"
    mmap_value="$(host_jq "$beat" .mmap_value)"
    [[ "$mmap_value" == *"token=${old_tokens[$index]}"* ]] || fail "$owner shared mmap lost its process state"
    map_host="$(sudo -n cat "$home_path/shared-map.bin" | strings -n 1)"
    [[ "$map_host" == *"token=${old_tokens[$index]}"* ]] || fail "$owner shared mmap write was not visible on host"
    [[ "$(run_adapter state "$root_id" | jq -r .pid)" == "${warm_root_pids[$index]}" ]] || fail "$owner did not reuse the preheated root sandbox"

    peer_index=$((1 - index))
    peer_path="${home_paths[$peer_index]}"
    result="$(run_adapter exec --user=0 "$child_id" /probe absent "$peer_path" "$peer_path/identity" "$peer_path/../$owner/data/identity")"
    [[ "$(jq -r .all_inaccessible <<<"$result")" == true ]] || fail "$owner root could access peer HOME"
    result="$(run_adapter exec --user=0 "$child_id" /probe present /home/agent/identity)"
    [[ "$(jq -r .content <<<"$result")" == "HOME-$owner" ]] || fail "$owner cannot read its own HOME"

    printf 'PASS owner=%s cycle=%s restore_to_live_ms=%s preheated_sentry_pid=%s guest_pid=%s counter=%s open_fd=%s deleted_fd=%s mmap=%s isolation=root-denied-peer\n' \
      "$owner" "$cycle" "$restore_elapsed_ms" "${warm_root_pids[$index]}" "${old_guest_pids[$index]}" \
      "$(host_jq "$beat" .counter)" "$(host_jq "$beat" .fd.size)" "$(host_jq "$beat" .deleted_fd.size)" "$mmap_value"

    # Advance the durable HOME/checkpoint pair for the next warm cycle.
    if [[ "$cycle" == 1 ]]; then
      next_snapshot="snapshot-$owner-2"
      checkpoint_path="$state/workloads/$owner/$next_snapshot"
      sudo -n mkdir -p "$checkpoint_path"
      run_adapter checkpoint --image-path="$checkpoint_path" --compression=none "$root_id"
      sudo -n "$adapter_bin" checkpoint --base="$state" --owner="$owner" --uid="$uid" \
        --snapshot="$next_snapshot" --runsc="$runsc_bin" --spec="$spec_hash" >/dev/null
      snapshots[$index]="$next_snapshot"
      old_counters[$index]="$(host_jq "$beat" .counter)"
      old_open_sizes[$index]="$(host_jq "$beat" .fd.size)"
      old_deleted_sizes[$index]="$(host_jq "$beat" .deleted_fd.size)"
      delete_pair "$child_id" "$root_id"
    fi
  done
  unset warm_uids warm_names warm_root_ids warm_root_pids warm_root_bundles
done

printf 'PASS: two independent local HOME trees restored through two warm-root cycles with production Adapter.Rewrite, real homevolume tickets/state, continuous memory/fds/cwd/mmap, and root peer-path denial\n'
