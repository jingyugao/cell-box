# Single-node Kubernetes deployment and upgrade

This guide covers deploying Cellbox to a single Kubernetes node (plain containerd or k3s). Prepare the node first, following [node installation](node-installation.md). That means the Cellbox gVisor build (upstream gVisor is not enough), the Cellbox runsc wrapper and the containerd handler. The Helm chart does not install any of these host components. For using Cellbox once it runs, see [usage](cellbox-usage.md).

The chart is not published to a chart registry. Install it from the repository path `charts/cellbox`.

## Build and publish the image

The backend needs Go 1.26 or later. From the repository root, run:

```sh
make check
make build
```

`make build` writes to `dist/release`. The output contains the binaries, the chart, `docs/`, the OpenAPI spec and the `sdk/go/` sources. Building the output directory does not upgrade a cluster.

It produces `dist/release/cellbox-server`, the guest agent and the wrapper. The Cellbox API and the node runtime run as one process in one image:

```sh
docker build -f images/cellbox-server.Dockerfile -t registry.example.com/team/cellbox-server:v1 dist/release
docker push registry.example.com/team/cellbox-server:v1
```

After the push, set the image (preferably by digest) in `server.image`. The chart's default is `ghcr.io/jingyugao/cellbox-server:0.1.0`, which is only a default value: the chart never uses an image you built unless you set `server.image`. The host's gVisor build, containerd handler and wrapper are maintained separately, as described in [node installation](node-installation.md).

## Configure storage, node and builder

Use [`charts/cellbox/values.yaml`](../charts/cellbox/values.yaml) as the reference. Keep your overrides in `charts/cellbox/values_local.yaml`, which Git ignores. When you upgrade an existing installation, keep maintaining its full configuration, including the profiles, storage paths and Secret references still in use.

| Setting | Requirement for a single-node deployment |
| --- | --- |
| `server.nodeName` | Required. The name of the Kubernetes node. The single `cellbox-server` process is pinned to it. |
| `server.image` | The server image. Set it to the image you built. |
| `api.replicas` | Keep at 1. Single-node operation does not scale out; a Kubernetes Lease selects the one active process. |
| `storage.hostPath` | The single durable data directory, default `/var/lib/cellbox-data`. Holds `cellbox.db`, `runtime-journal.log` (recent resume state not yet written to the database), build logs and temporary files used to validate archive downloads. |
| `storage.existingClaim` | Optional. If you provide an existing PVC, the data directory uses it instead of `storage.hostPath`. |
| `api.config.dataDir` | Keep `/var/lib/cellbox-data`. It is the only service data directory; do not override it with another container path. |
| `objectStorage.endpoint`, `bucket`, `region`, `prefix`, `pathStyle` | S3-compatible archive storage. Without it, archive capture, download, restore and delete return `UNSUPPORTED_CAPABILITY`. Template and operation metadata is always stored in SQLite. |
| `objectStorage.credentialsSecret` | Name of an existing Secret in the same namespace. The default keys are `OSS_ACCESS_KEY` and `OSS_SECRET_KEY`; change them with `accessKeyKey` / `secretKeyKey`. |
| `api.config.profiles` | Profile ID, provider, image, CPU, memory and guest settings. For a Kubernetes profile, the namespace must be the release namespace and `nodeName` must match `server.nodeName`. See [`config/sample.json`](../config/sample.json) for the structure. |
| `controller.nodeSelector` | Platform constraint for the server. The node is also fixed by `server.nodeName`. Default is Linux amd64. |
| `controller.criDirectory` / `criSocket` | Where the server reaches the node's CRI. Plain containerd: `/run/containerd` and `/run/containerd/containerd.sock` (the defaults). k3s: `/run/k3s/containerd` and `/run/k3s/containerd/containerd.sock`. |
| `controller.warmPoolSize` | Total budget of idle warm slots for the node, default 2, allowed 0-32. Demand comes from the checkpoints that exist on the node; see [warm pool](warm-pool.md). |
| `buildkit.enabled`, `repository` | Building revisions needs BuildKit enabled and a full image repository that the build can push to. |
| `buildkit.registryCredentialsSecret` | If the registry is private, reference an existing `kubernetes.io/dockerconfigjson` Secret. |
| `buildkit.registryHost` / `registryHTTP` | Set these explicitly for a plain-HTTP registry. Leave the HTTP option off for a normal HTTPS registry. |

Service metadata and the template catalog share one `cellbox.db` on the `storage` mount. The database has four business tables: `templates`, `template_revisions`, `operations` (build tasks) and `runtime_records`. `runtime_records` holds Boxes, running operations, Execs, exec results, archive metadata and the checkpoint index. Queries read the database directly. Archive payloads are stored only in the configured S3-compatible object storage; the local data directory holds only SQLite, build logs and temporary files for download validation.

An older `state.json`, object metadata ledger or separate `catalog.db` is not converted automatically. When you deploy this version, use a new data directory.

For Kubernetes profiles, the desired and observed state of each Box is still stored in Cellbox custom resources (API group `cellbox.jingyugao.github.io`). The gVisor checkpoint files stay on the node under `/var/lib/cellbox`.

Creating a template or building a revision does not create a warm pool. After a Box is suspended and produces a checkpoint, the background loop prepares warm slots for the matching configuration, within the node's total budget. See [warm pool](warm-pool.md) for the details.

The API has no built-in client authentication. Put a gateway in front of it to authenticate callers and restrict the client data namespace; `X-Cellbox-Client-ID` is not an authentication credential. Do not write Secret values into version-controlled values files or documents.

## One process and its runtime state

Helm deploys a single node-pinned `cellbox-server` Deployment, exposed by the `cellbox-api` Service. Inside the server, API lifecycle writes and node reconciliation are linked by function calls and a queue per Box key. When state is committed, the completed snapshot notifies waiting requests directly. The warm pool and image warm-up requests are also handed over between in-process components, not through ConfigMaps.

Kubernetes still stores the Cellbox custom resources as the durable desired state, observed state and Pod/Service ownership record of each Box. Creating and deleting Pods, and CRI/gVisor checkpoint operations, are still performed by Kubernetes and the node runtime. Merging the API and the runtime shortens the local notification path; it does not remove Kubernetes writes, Pod lifecycle operations or their latency.

Because the API and the node runtime share one container, the `cellbox-server` Pod uses `hostPID`, runs privileged, and mounts host paths such as containerd, gVisor and `/var/lib/cellbox`. Point `server.nodeName` only at a node you have prepared and trust. Do not treat this Deployment like an ordinary unprivileged API Deployment.

## Apply the configuration

The commands below use Bash. Choose your target environment first and replace the values:

```sh
export CELLBOX_CONTEXT=<context>
export CELLBOX_NAMESPACE=<namespace>
export CELLBOX_RELEASE=<release>
export CELLBOX_VALUES=charts/cellbox/values_local.yaml

helm lint charts/cellbox -f "$CELLBOX_VALUES"
helm upgrade --install "$CELLBOX_RELEASE" charts/cellbox \
  --kube-context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" --create-namespace \
  -f "$CELLBOX_VALUES" --wait --timeout 10m
```

On upgrade, keep `storage.hostPath` or `storage.existingClaim` unchanged. It decides where SQLite data and build logs persist. The S3 prefix affects only the path of archive objects.

## Check the deployment and call the API

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" get pods
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  rollout status deploy/cellbox-server --timeout=180s
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs deploy/cellbox-server -c server --tail=100
```

Run the port-forward in a separate terminal and keep it open:

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

In another terminal, set `CELLBOX_API_URL=http://127.0.0.1:18092`. Then follow the [REST quickstart](rest-quickstart.md) or the [Go SDK guide](go-sdk.md) to check build, create, exec, file access and suspend/resume. An existing port-forward may break when the Pod is replaced; start it again.

A Running `cellbox-server` Pod does not replace that functional check. A warm Pod whose business container has not started can show `ContainerCreating`. To decide whether warm-up is ready, read the fresh `readySlots` in the revision's `/warm-pool` status, which this process maintains. That readiness only means the slot is prepared for a resume; it says nothing about the health of the application.
