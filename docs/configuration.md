# Configuration reference

Everything you can set on Cellbox, where it is set, and what the server checks at startup. Values are taken from the code; where a default is applied it is stated. For the meaning of the terms see [concepts](concepts.md).

## Where configuration comes from

| Source | What it holds |
| --- | --- |
| Command-line flags of `cellbox-server` | Where the server runs, how it reaches the node runtime, the warm pool budget, image-build tools |
| JSON configuration file (`--config`) | The listen address, data directory, profiles, retention, image-build and archive settings |
| Object storage file (`--object-storage-config`) | The S3-compatible store for archives |
| Environment | Credentials named by the object storage file, tracing, and the node wrapper's settings |
| Helm values | Rendered into the three sources above; see [Helm values](#helm-values) |

The configuration file is strict: unknown fields, trailing content, or a file larger than 1 MiB are rejected, and the server refuses to start.

## Command-line flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config` | none, required | Path to the JSON configuration file |
| `--kubeconfig` | in-cluster credentials | Explicit kubeconfig path |
| `--object-storage-config` | none | Path to the object storage JSON; archives are unsupported without it |
| `--leader-election-namespace` | the managed namespace | Namespace of the Lease that keeps one server process active |
| `--node` | none | The one Kubernetes node this server manages. Required for `resumable-k8s-pod` profiles |
| `--namespace` | `cell-box` | The Kubernetes namespace this server manages |
| `--cri-socket` | `/run/containerd/containerd.sock` | The node's CRI socket (k3s uses `/run/k3s/containerd/containerd.sock`) |
| `--runtime-class` | `runsc-recoverable` | RuntimeClass used for execution Pods |
| `--runtime-handler` | the runtime class | CRI handler name |
| `--node-base` | `/var/lib/cellbox` | Node-local directory for tickets, claims, warm leases, checkpoints and HOME volumes |
| `--runsc` | `/usr/local/bin/runsc` | The runsc binary the node runtime uses |
| `--runsc-root` | `/run/containerd/runsc/k8s.io` | runsc state root used by the CRI handler |
| `--shim` | `/usr/local/bin/containerd-shim-runsc-v1` | The containerd runsc shim |
| `--host-mount-namespace` | off | Run runsc in the host mount namespace (needs the privileged `hostPID` Pod) |
| `--warm-pool-size` | `2` | Maximum idle warm Pods on the node, 0-32; `0` disables the pool |
| `--buildkit-addr` | none | Unix socket of BuildKit, `unix:///...` |
| `--image-repository` | none | Registry repository that receives built images |
| `--guest-binary` | none | Static guest binary injected into built images |
| `--buildctl` | none | BuildKit client executable |
| `--image-insecure-registry` | none | A registry host that may be reached over HTTP for image import |
| `--docker` | `docker` | Docker CLI used by the Docker provider |
| `--local-guest-binary` | `cellbox-container-agent` next to the server | Guest binary for `local-runsc` profiles |
| `--local-runsc-platform` | `systrap` | gVisor platform for `local-runsc`: `systrap` or `kvm` |
| `--version` | | Print the version and exit |

The four path flags `--node-base`, `--runsc`, `--runsc-root` and `--shim` must be clean absolute paths other than `/`. A server that has a `resumable-k8s-pod` profile also needs `--node`, `--namespace` and `--cri-socket`, and every such profile must name the same node and namespace.

The BuildKit flags are used only if at least one of `--buildkit-addr`, `--image-repository`, `--guest-binary` or `--buildctl` is given; they then replace the `imageBuild` section of the file.

## Configuration file

```json
{
  "listen": "127.0.0.1:8090",
  "dataDir": "data",
  "clientId": "internal",
  "profiles": [ ... ]
}
```

A complete sample is in [`config/sample.json`](../config/sample.json).

| Field | Default | Meaning and limits |
| --- | --- | --- |
| `listen` | `127.0.0.1:8090` | HTTP listen address. The API has no authentication; do not expose it directly |
| `dataDir` | `data` | Directory for `cellbox.db` and build logs. A clean path other than `.` and `/` |
| `clientId` | `internal` | Default client namespace for requests without `X-Cellbox-Client-ID`. Letters, digits, `_` and `-`, up to 80 characters, starting with a letter or digit. It selects data; it does not authenticate |
| `profiles` | none | The list of profiles, see below |
| `startupTimeoutSeconds` | `120` | How long to wait for a Box to start, 1-600 |
| `operationRetentionSeconds` | `86400` | How long finished operations are kept, 1 hour to 30 days |
| `executionRetentionSeconds` | `3600` | How long exec records are kept, 5 minutes up to the operation retention |
| `imageBuild` | none | `address`, `repository`, `guestBinary`, `buildctlBinary`, `insecureRegistry`. If any is set, `address` must be a `unix:///` socket, `repository` a valid registry repository and `guestBinary` must be given |
| `objectStorage` | none | Same object as the object storage file below |
| `volumeBackups` | disabled | `enabled`, `binary` (default `restic`), `keepLast` (default 3, 1-100) |

## Profiles

A profile is the runtime recipe a revision is built and run with. IDs are unique, use letters, digits, `_` and `-`, and are at most 80 characters.

| Field | Meaning and rules |
| --- | --- |
| `id` | Profile ID |
| `provider` | `resumable-k8s-pod`, `local-runsc` or `docker`. See [concepts](concepts.md) and the capability notes in the [README](../README.md) |
| `image` | Base image of the profile, an immutable reference: `sha256:<digest>` or `repository@sha256:<digest>`. `resumable-k8s-pod` and `local-runsc` require the `repository@sha256:` form |
| `nodeName`, `namespace` | Required for `resumable-k8s-pod` and must match `--node` and `--namespace`. Rejected for `docker` and `local-runsc` |
| `cpu` | CPU limit in cores. Omitted or `0` means unlimited. For `local-runsc`, `0` or at least `0.01` |
| `memoryMiB` | Memory limit in MiB. Omitted or `0` means unlimited |
| `guest` | Guest agent configuration, see below |
| `sharedReadOnlyHostPath` | Host directory mounted read-only into Boxes (`resumable-k8s-pod`, `local-runsc`); an absolute clean path |
| `debugReadOnlyHostPath`, `debugReadWriteHostPath` | Host directories for debug access; the read-write one cannot be combined with the read-only one |
| `trustedToolImages` | Exact prepared images allowed to run the profile's protected `guest.tools` (`repository@sha256:` references) |
| `mountedToolRuntimeImages` | Images whose protected runner may read a mounted tool-runtime descriptor |
| `debugHomeImages` | Images whose runner may authorize access to single files in the mounted HOME |
| `autoTrustImportedImages` | Let client-imported images inherit the profile's protected tools and managed HOME without per-digest approval; for private installations only |

New Boxes on `resumable-k8s-pod` and `local-runsc` profiles always get a persistent HOME volume; this is derived, not a setting.

### Guest configuration (`guest`)

| Field | Default | Rules |
| --- | --- | --- |
| `workspace` | `/workspace`; `/home/agent/workspace` for persistent-HOME profiles | A clean absolute directory other than `/`; must not overlap the guest's protected paths |
| `agent` | uid 11000, gid 11000 | The identity business processes run as. Must be non-root and differ from `debug` |
| `debug` | uid 11001, gid 11001 | The identity of debug tools. Root debug needs uid and gid both `0` |
| `command` | none | Command that starts the business program, at most 128 arguments |
| `commandDir` | none | Working directory for the command; a clean absolute path |
| `env` | none | Environment for the business program |
| `tools` | none | Protected tools, see below |
| `debugHome` | | Managed by Cellbox; setting it is an error |

Each entry of `tools`:

| Field | Meaning |
| --- | --- |
| `id` | Letters, digits and `_`, starting with a letter, up to 64 characters |
| `executable` | An absolute path under `/opt/cellbox/tools` |
| `args` | Fixed arguments |
| `inputPatterns` | Up to 32 regular expressions; each caller argument must match its pattern in full |
| `passThroughArgs` | Accept any number of caller arguments; cannot be combined with `inputPatterns` |
| `credentialEnv` | Maps environment names to private credential slots |
| `workspaceRead`, `workspaceWrite` | Let the tool read or write the workspace; write requires read |

## Object storage file

Used for archives. Without it, archive operations return `UNSUPPORTED_CAPABILITY`.

| Field | Default | Meaning |
| --- | --- | --- |
| `bucket` | required | Bucket name, without `/` |
| `endpoint` | AWS S3 | An `http` or `https` origin with no path, user or query |
| `region` | `us-east-1` | |
| `prefix` | `cellbox` | Relative key prefix for everything Cellbox writes |
| `pathStyle` | `false` | Use path-style addressing (needed by most self-hosted stores) |
| `accessKeyEnv`, `secretKeyEnv` | none | Names of environment variables that hold the access and secret keys; set both or neither |
| `sessionTokenEnv` | none | Name of the environment variable that holds a session token |

The credentials themselves are never in the file. With Helm, the file is rendered only when `objectStorage.bucket` is set. If `objectStorage.credentialsSecret` is also set, the chart names the variables `OSS_ACCESS_KEY` and `OSS_SECRET_KEY` in the file and fills them from the keys `accessKeyKey` and `secretKeyKey` (defaults `OSS_ACCESS_KEY` and `OSS_SECRET_KEY`) of that existing Secret.

## Environment variables

### Node wrapper (set in the node's wrapper script)

| Variable | Meaning |
| --- | --- |
| `CELLBOX_NODE_BASE` | Must equal the server's `--node-base`. Default `/var/lib/cellbox` |
| `CELLBOX_RUNSC` | The real runsc to execute. Default `/usr/local/bin/runsc` |
| `CELLBOX_NO_ROOT_CONTAINER` | `true` boots sandboxes without a root container process |
| `CELLBOX_DEFER_CHECKPOINT` | `true` starts warm sandboxes without a checkpoint; requires the previous one |
| `CELLBOX_BACKGROUND_LOAD` | `lazy` (default) or `eager` page loading after a restore |
| `CELLBOX_TRACE_TIMINGS` | `true` appends per-step timing lines to `<node base>/timings.log`; for measurements only |
| `CELLBOX_POD_UID_AUTH` | Leave unset |

Details and the reasoning are in [node installation](node-installation.md).

### Tracing

Tracing is off unless an OTLP endpoint is set. It uses the standard variables: `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`), `OTEL_EXPORTER_OTLP_PROTOCOL` (or `..._TRACES_PROTOCOL`, must be `http/protobuf`), `OTEL_SERVICE_NAME` and `OTEL_SDK_DISABLED=true` to turn it off. With Helm, pass them through `api.extraEnv` and `controller.extraEnv`.

## Helm values

[`charts/cellbox/values.yaml`](../charts/cellbox/values.yaml) is the reference. How the main values map:

| Value | Becomes |
| --- | --- |
| `imageRegistry` | Prefix for relative image names |
| `server.nodeName`, release namespace | `--node`, `--namespace` |
| `server.image` | The `cellbox-server` image (use a digest) |
| `runtimeClass.name`, `runtimeClass.handler` | `--runtime-class`, `--runtime-handler`, and the RuntimeClass object |
| `controller.nodeBase`, `runsc`, `runscRoot`, `shim` | `--node-base`, `--runsc`, `--runsc-root`, `--shim`. Must match the node installation |
| `controller.criSocket`, `criDirectory` | `--cri-socket` and the directory mounted for it |
| `controller.warmPoolSize` | `--warm-pool-size` |
| `storage.hostPath` or `storage.existingClaim` | Where `cellbox.db` and build logs live |
| `api.config` | The JSON configuration file, merged over `listen: 0.0.0.0:8090`, `dataDir: /var/lib/cellbox-data` and disabled volume backups |
| `api.configSecret`, `api.manageSecrets` | Which Secret holds the file, and whether the chart creates it |
| `objectStorage.*` | The object storage file (only when `bucket` is set) and the credential Secret reference |
| `buildkit.*` | A BuildKit sidecar and `--buildkit-addr`, `--image-repository`, `--image-insecure-registry` |
| `crd.create` | Whether the chart installs the CRD |

Keep `storage.hostPath` (or the claim) and `controller.nodeBase` unchanged when you upgrade: the first holds the catalog, the second holds the HOME volumes.
