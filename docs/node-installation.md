# Node installation

This guide prepares a Kubernetes node to run Cellbox sandboxes: it installs the gVisor build Cellbox needs, the Cellbox runsc wrapper, and a containerd runtime handler. Do it before deploying the Helm chart. The chart only creates Kubernetes resources; it never installs host software, edits runtime configuration, or restarts node services.

`cellbox-server` runs privileged with `hostPID` on exactly one node (`server.nodeName`), and drives checkpoint and restore on that node. Everything below happens on that node.

## Requirements and tested environment

| Item | Requirement |
| --- | --- |
| Platform | Linux amd64 only |
| Kubernetes | A node already joined to a cluster, with containerd as the CRI (plain containerd or k3s) |
| Privileges | root on the node (the examples use `sudo`) |
| Tools on the node | `curl`, `tar`, `bzip2`, `sha512sum` |

Tested with k3s v1.36 (containerd 2.3), Ubuntu 24.04, kernel 6.8, systrap platform. Other combinations may work but have not been validated.

### gVisor build

Cellbox needs a gVisor build with three options that upstream gVisor does not have yet: `runsc create --no-root-container`, `runsc restore --defer-checkpoint`, and `runsc restore --background-load`. Do not install an upstream gVisor release; warm sandboxes will fail to start.

| Cellbox version | gVisor build |
| --- | --- |
| 0.1.0 | [`cellbox-gvisor-20261029`](https://github.com/jingyugao/gvisor/releases/tag/cellbox-gvisor-20261029) |

The build is upstream gVisor `ac7b4e0fdc` (2026-10-28) plus pull requests [#15721](https://github.com/google/gvisor/pull/15721), [#15722](https://github.com/google/gvisor/pull/15722) and [#16368](https://github.com/google/gvisor/pull/16368). Source: [`jingyugao/gvisor`](https://github.com/jingyugao/gvisor), tag `cellbox-gvisor-20261029`.

## How the pieces fit

```text
kubelet -> containerd -> containerd-shim-runsc-v1 -> runsc-wrapper (shell script, sets environment)
                                                       -> cellbox-runsc-wrapper (checks tickets, rewrites start -> restore)
                                                            -> runsc (the gVisor build above)
```

The shim runs the wrapper script in place of runsc. The script sets the wrapper's environment and executes `cellbox-runsc-wrapper`, which checks the node-local ticket for the Pod, binds the persistent HOME, and turns a sandbox `start` into `runsc restore`. It finally `exec`s the real `runsc`. The wrapper never calls `cellbox-server`; it only reads files under the node base directory.

## 1. Check the environment and back up

Do not replace gVisor while Boxes are running or suspended on the node. Checkpoints made by one gVisor build are not guaranteed to restore under another, so validate compatibility first.

```bash
uname -m                                   # expect x86_64
sudo systemctl cat containerd.service      # plain containerd
sudo systemctl cat k3s.service             # k3s server (k3s-agent.service for agents)
```

Find the configuration containerd actually uses (the service arguments are authoritative). Read its `version` and `imports`, confirm CRI is not disabled, and check whether a `runsc-recoverable` handler already exists; merge by hand instead of defining it twice. For k3s the generated file is `/var/lib/rancher/k3s/agent/etc/containerd/config.toml`.

Before changing anything, copy the files you will touch (containerd configuration or k3s template and drop-ins, any existing wrapper, `/var/lib/cellbox/runsc.toml`) and note which files did not exist, so rollback can remove them.

## 2. Install gVisor

Install into a versioned directory so any existing gVisor stays untouched. The directory must contain `runsc`, `containerd-shim-runsc-v1`, and a `gvisor-bin/` directory next to `runsc`.

```bash
(
  set -euo pipefail
  tag=cellbox-gvisor-20261029
  dest=/usr/local/bin/gvisor-cellbox/$tag
  base=https://github.com/jingyugao/gvisor/releases/download/$tag
  work=$(mktemp -d); trap 'rm -rf -- "$work"' EXIT; cd "$work"

  curl --fail --location -O "$base/gvisor-linux-amd64.tar.bz2" -O "$base/gvisor-linux-amd64.tar.bz2.sha512"
  sha512sum -c gvisor-linux-amd64.tar.bz2.sha512
  mkdir unpacked && tar -xjf gvisor-linux-amd64.tar.bz2 -C unpacked
  test -x unpacked/runsc && test -x unpacked/containerd-shim-runsc-v1 && test -x unpacked/gvisor-bin/gvisor_sentry

  test ! -e "$dest"          # never overwrite an installed build
  sudo install -d -m 755 "$dest/gvisor-bin"
  sudo install -m 755 unpacked/runsc unpacked/containerd-shim-runsc-v1 "$dest/"
  sudo cp -a unpacked/gvisor-bin/. "$dest/gvisor-bin/"
  "$dest/runsc" --version
)
```

`runsc --version` should print `cellbox-gvisor-20261029`. Confirm the three options exist:

```bash
d=/usr/local/bin/gvisor-cellbox/cellbox-gvisor-20261029
$d/runsc restore --help 2>&1 | grep -E 'defer-checkpoint|background-load'
$d/runsc create  --help 2>&1 | grep 'no-root-container'
```

## 3. Install the wrapper and runtime configuration

Take `cellbox-runsc-wrapper` from the Cellbox release package, or build it with `make build` (output in `dist/release/`). Then install it and create the node base directory:

```bash
sudo install -d -m 755 /usr/local/libexec/cellbox
sudo install -m 755 cellbox-runsc-wrapper /usr/local/libexec/cellbox/cellbox-runsc-wrapper
sudo /usr/local/libexec/cellbox/cellbox-runsc-wrapper --adapter-version

sudo install -d -m 700 /var/lib/cellbox \
  /var/lib/cellbox/tickets /var/lib/cellbox/requests /var/lib/cellbox/claims \
  /var/lib/cellbox/workloads /var/lib/cellbox/logs
```

Create the wrapper script. It is what containerd actually runs, and it carries the settings that make fast resume work:

```bash
sudo tee /usr/local/libexec/cellbox/runsc-wrapper >/dev/null <<'EOF'
#!/bin/sh
export CELLBOX_NODE_BASE=/var/lib/cellbox
export CELLBOX_RUNSC=/usr/local/bin/gvisor-cellbox/cellbox-gvisor-20261029/runsc
export CELLBOX_NO_ROOT_CONTAINER=true
export CELLBOX_DEFER_CHECKPOINT=true
export CELLBOX_BACKGROUND_LOAD=lazy
exec /usr/local/libexec/cellbox/cellbox-runsc-wrapper "$@"
EOF
sudo chmod 755 /usr/local/libexec/cellbox/runsc-wrapper
```

| Variable | Meaning |
| --- | --- |
| `CELLBOX_NODE_BASE` | Directory for tickets, requests, claims, warm leases and logs. Must equal the chart's `controller.nodeBase`. Default `/var/lib/cellbox`. |
| `CELLBOX_RUNSC` | The real `runsc` to execute. Must be the gVisor build from section 2. Default `/usr/local/bin/runsc`. |
| `CELLBOX_NO_ROOT_CONTAINER` | Boot sandboxes without a root container process, so warm-pool slots can be prepared before any Box is assigned. |
| `CELLBOX_DEFER_CHECKPOINT` | Start the warm sandbox without a checkpoint and bind the checkpoint when a Box claims the slot. Requires `CELLBOX_NO_ROOT_CONTAINER`. |
| `CELLBOX_BACKGROUND_LOAD` | `lazy` (default) reads checkpoint pages when the application touches them; `eager` reads them all in the background. Requires the gVisor build above. |
| `CELLBOX_POD_UID_AUTH` | Leave unset. |

`CELLBOX_NO_ROOT_CONTAINER` and `CELLBOX_DEFER_CHECKPOINT` are required for the validated warm-resume path. Without them the wrapper falls back to a mode that this gVisor build and the warm pool are not validated with; in our tests warm-slot Pods failed with `StartError` and resumes with `READINESS_FAILED`.

Now point the runtime at the script. Save as `/var/lib/cellbox/runsc.toml` (owner root, mode `0600`):

```toml
binary_name = "/usr/local/libexec/cellbox/runsc-wrapper"
log_path = "/var/lib/cellbox/logs/%ID%/shim.log"
log_level = "info"
[runsc_config]
  network = "host"
  restore-spec-validation = "enforce"
  platform = "systrap"
```

```bash
sudo chown root:root /var/lib/cellbox/runsc.toml && sudo chmod 600 /var/lib/cellbox/runsc.toml
```

This is the configuration used in our validation runs, including `network = "host"` and the systrap platform. To use KVM instead, set `platform = "kvm"` after confirming `/dev/kvm` is available. Shim options are described in the [gVisor containerd configuration guide](https://gvisor.dev/docs/user_guide/containerd/configuration/).

## 4. Register the containerd handler

Add a `runsc-recoverable` handler that uses the shim from section 2 and the `runsc.toml` above. Keep all existing settings, do not overwrite the whole file, and do not change the default runtime.

The Pod annotation `dev.gvisor.internal.recovery.ticket` must be allowed through to the runtime; it is how the wrapper finds the Pod's ticket.

### 4A. Plain containerd

For a `version = 3` configuration add the following; for `version = 2` replace both occurrences of `io.containerd.cri.v1.runtime` with `io.containerd.grpc.v1.cri`.

```toml
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  runtime_path = "/usr/local/bin/gvisor-cellbox/cellbox-gvisor-20261029/containerd-shim-runsc-v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
```

If you use an imports or drop-in layout, put the snippet in a file that is really imported. Check how the configuration parses and merges before restarting:

```bash
sudo containerd --config /etc/containerd/config.toml config dump
```

### 4B. k3s

Never edit the generated `config.toml`; k3s rewrites it on every restart. Use a drop-in. Recent k3s versions import `/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/*.toml` (check the `imports` line of the generated file). Save the snippet below as `config-v3.toml.d/98-cellbox.toml`, mode `0644`:

```toml
version = 3

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  runtime_path = "/usr/local/bin/gvisor-cellbox/cellbox-gvisor-20261029/containerd-shim-runsc-v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
```

If your k3s does not import drop-ins, use a template instead (see the [k3s containerd documentation](https://docs.k3s.io/advanced#configuring-containerd)): `config-v3.toml.tmpl` for `version = 3` configurations, or `config.toml.tmpl` for `version = 2`. A new template starts with `{{ template "base" . }}` followed by the snippet above. If a template already exists, merge into it and do not add the `base` line again. Do not define the same handler in two places.

## 5. Restart and check

Pick a maintenance window and deal with the workloads on the node first. For a worker node, drain it following your cluster's rules:

```bash
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets
```

Then restart exactly one service on the node:

```bash
sudo systemctl restart containerd.service      # plain containerd
sudo systemctl restart k3s.service             # k3s server: also restarts the local control plane
sudo systemctl restart k3s-agent.service       # k3s agent
sudo journalctl -u <service> -n 100 --no-pager # look for config parse or CRI load errors
```

Check that containerd registered the handler with the right shim and config path:

```bash
sudo crictl info            # k3s: sudo k3s crictl info
# the runtimes list must contain runsc-recoverable with runtimePath and ConfigPath as configured
```

Wait for the node to be Ready and uncordon it:

```bash
kubectl wait --for=condition=Ready node/<node> --timeout=180s
kubectl uncordon <node>
```

On a single-node cluster you cannot drain. In our validation, restarting k3s kept running Pods alive, but the API server was unavailable for a short time. Plan for that.

## 6. Deploy and verify

Deploy the chart following the [deployment guide](deployment.md). Set `server.nodeName` to this node; for k3s also set `controller.criDirectory: /run/k3s/containerd` and `controller.criSocket: /run/k3s/containerd/containerd.sock`. Make `controller.nodeBase`, `controller.runsc` and `controller.shim` match the paths used above.

```bash
kubectl get runtimeclass runsc-recoverable -o yaml      # handler must be runsc-recoverable
kubectl -n <namespace> rollout status deploy/cellbox-server --timeout=180s
kubectl -n <namespace> logs deploy/cellbox-server -c server
```

A running `cellbox-server` only proves the basics. Finish by creating a Box, suspending it and resuming it, as in the [REST quick start](rest-quickstart.md). After the first suspend, the warm pool creates a slot Pod; it shows `ContainerCreating` while it waits for a Box, which is expected (see [warm pool](warm-pool.md)).

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Warm-slot Pods end in `StartError` with "the sandbox exited before the container started"; resume fails with `READINESS_FAILED` | The wrapper script is missing `CELLBOX_NO_ROOT_CONTAINER=true` or `CELLBOX_DEFER_CHECKPOINT=true`, or `CELLBOX_RUNSC` points to a gVisor without the Cellbox patches |
| `flag provided but not defined: -background-load` (or `-defer-checkpoint`) | `CELLBOX_RUNSC` is not the Cellbox gVisor build |
| `missing/invalid controller authorization; refusing cold start` | The Pod has no ticket: the server uses a different `controller.nodeBase` than `CELLBOX_NODE_BASE` |
| Handler missing from `crictl info` | The snippet is not in an imported file, or containerd was not restarted; check `imports` and the service logs |
| Sandbox logs | `/var/lib/cellbox/logs/<sandbox-id>/` holds one runsc log per subcommand (`boot`, `create`, `gofer`, `state`, `restore`, `wait`, `delete`) and a `panic` log |

## Upgrade, migration and rollback

- A new Cellbox server image does not update host components. To update the wrapper, install the matching version, check `--adapter-version`, and replace the file as in section 3. Changing the wrapper or `runsc.toml` does not need a containerd restart, but existing shims may keep the old settings; verify with a newly created Box.
- To change the gVisor build, install it side by side (section 2), point the wrapper script and the handler's `runtime_path` at it, and restart the runtime service. Suspended Boxes may not restore under a different build, so test existing checkpoints first, or keep the old build and handler for them.
- If the restart fails, keep the node cordoned, restore the backed-up configuration, remove files you added, restore the previous wrapper and `runsc.toml`, then restart the service and verify. For k3s restore the template or drop-in, not the generated file.
- Never delete `tickets`, `requests`, `claims`, `workloads` or other runtime state under the node base to "reinstall". `helm uninstall` does not remove node components; deal with existing Boxes first, then remove the handler and components by hand.
