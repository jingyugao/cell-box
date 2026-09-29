#!/bin/bash
# Deliberately uses the node-local K3s kubeconfig, never the caller's remote context.
set -euo pipefail
[[ ${1:-} == --local-dev ]] || { echo 'Usage: create-example.sh --local-dev'; exit 2; }
[[ $EUID == 0 ]]
release_dir=$(cd "$(dirname "$0")/.." && pwd)
node_name=$(hostname)
k3s kubectl get node "$node_name" >/dev/null
k3s kubectl create --dry-run=client -f "$release_dir/examples/counter.yaml" -o json |
  jq --arg node "$node_name" '.spec.nodeName=$node' |
  k3s kubectl apply -f -
