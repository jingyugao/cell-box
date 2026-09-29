#!/bin/bash
# Run as root on the dedicated local K3s node, from an uploaded release directory.
set -euo pipefail
[[ ${1:-} == --local-dev ]] || { echo 'Usage: install-dev.sh --local-dev'; exit 2; }
[[ $EUID == 0 ]]
cd "$(dirname "$0")/.."
node_name=$(hostname)
k=(k3s kubectl)
"${k[@]}" get node "$node_name" >/dev/null
[[ $("${k[@]}" get node "$node_name" -o jsonpath='{.metadata.labels.kubernetes\.io/hostname}') == "$node_name" ]]
test -x /usr/local/bin/runsc
test -x /usr/local/bin/containerd-shim-runsc-v1
grep -q 'config-v3.toml.d/\*.toml' /var/lib/rancher/k3s/agent/etc/containerd/config.toml || {
 echo 'K3s config must already import config-v3.toml.d/*.toml'; exit 1;
}
base=/var/lib/cellbox
dropin=/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/91-cellbox.toml
if [[ -e $base && ! -f $base/installed-by-cellbox ]]; then echo 'Unowned storage directory'; exit 1; fi
if [[ -e $dropin && ! -f $base/installed-by-cellbox ]]; then echo 'Unowned runtime drop-in'; exit 1; fi
mkdir -p "$base"/{tickets,requests,claims,workloads,logs} /etc/resumablepod /usr/local/libexec
chmod 700 "$base" /etc/resumablepod
touch "$base/installed-by-cellbox"
for binary in resumablepod-controller resumablepod-runtime; do
 install -m 755 "$binary" "/usr/local/libexec/$binary.next"
 mv "/usr/local/libexec/$binary.next" "/usr/local/libexec/$binary"
done
install -m 600 deploy/runsc.toml "$base/runsc.toml"
install -m 644 deploy/runtime.toml "$dropin"
"${k[@]}" apply -f deploy/crd.yaml -f deploy/access.yaml
"${k[@]}" wait --for=condition=Established crd/cellboxes.cellbox.local --timeout=60s
cat <<YAML | "${k[@]}" apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: cellbox-controller-token
  namespace: cell-box
  annotations:
    kubernetes.io/service-account.name: cellbox-controller
type: kubernetes.io/service-account-token
YAML
for i in {1..30}; do
 token=$("${k[@]}" -n cell-box get secret cellbox-controller-token -o jsonpath='{.data.token}' | base64 -d)
 [[ -z $token ]] || break
 sleep 1
done
[[ -n $token ]]
ca=$("${k[@]}" -n cell-box get secret cellbox-controller-token -o jsonpath='{.data.ca\.crt}')
umask 077
printf '%s' "$token" | jq -Rs --arg ca "$ca" '{apiVersion:"v1",kind:"Config",clusters:[{name:"local",cluster:{server:"https://127.0.0.1:6443","certificate-authority-data":$ca}}],users:[{name:"controller",user:{token:.}}],contexts:[{name:"local",context:{cluster:"local",user:"controller",namespace:"cell-box"}}],"current-context":"local"}' > /etc/resumablepod/kubeconfig
unset token ca
printf 'NODE_NAME=%s\n' "$node_name" > /etc/resumablepod/controller.env
install -m 644 deploy/resumablepod-controller.service /etc/systemd/system/
systemctl daemon-reload
systemctl restart k3s
for i in {1..60}; do if "${k[@]}" get node "$node_name" >/dev/null 2>&1; then break; fi; sleep 1; done
"${k[@]}" wait --for=condition=Ready node/"$node_name" --timeout=60s
systemctl enable resumablepod-controller
systemctl restart resumablepod-controller
