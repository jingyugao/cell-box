# Cellbox workflow and template semantics

This page describes how the resources relate in the current version and where the boundaries of use are. Before installing, read [deployment and upgrade](deployment.md) and [node installation](node-installation.md). For runnable examples see the [Go SDK guide](go-sdk.md) and the [REST quickstart](rest-quickstart.md). For terms, see [Concepts and terminology](concepts.md). The full HTTP contract is the [OpenAPI document](../api/openapi.yaml).

## Resource relationships

| Resource | Purpose |
| --- | --- |
| Client namespace | Isolates one caller's template catalog and records from another's. The `X-Cellbox-Client-ID` header selects it; if omitted, the server default is used. It is not an authentication credential |
| Profile | Runtime parameters configured by the administrator: provider, node, Kubernetes namespace, CPU, memory and guest settings. A snapshot is stored when a revision is built |
| Template | A family of revisions. Holds a name, a description, `defaultWarmSlots` (kept for compatibility; no longer decides warm-pool demand) and an optional `stableRevisionId` |
| Revision | The result of one successful build. Fixes the image digest, the build inputs and the runtime configuration. Its description, `active`/`retired` status and `warmSlots` (kept for compatibility; no longer decides warm-pool demand) can be changed |
| Warm pool | Prepared slots shared by compatible runtime configurations, used to speed up checkpoint restore. Created and released automatically based on the checkpoints that exist on the node. It does not belong to any one Box. See [warm pool](warm-pool.md) |
| Box | A running or suspended instance created from a revision, pinned to the revision resolved when it was created. The SDK calls it a Sandbox |
| Operation | An asynchronous task: build, create, exec, lifecycle change. Track the result with the Operation ID |
| Exec | The record of one time-limited command, with exit code, stdout and stderr |

The API has no built-in client authentication. When you expose it, put a gateway in front that verifies identity and controls which client namespaces a caller may use. The token in the SDK is only sent to that gateway, as a Bearer header.

## Single-process operation

Cellbox runs as one `cellbox-server` pinned to a single Kubernetes node. The HTTP API and the node runtime cooperate inside the same process through function calls. The API submits Box changes to a shared keyed queue, a runtime worker processes them and writes the completed state back, and a full state snapshot is published to wake up waiters. This removes the notification path that would otherwise exist between the API and a separate controller.

The Cellbox custom resource in Kubernetes remains the durable record of a Box and the owner of its Pod and Service. After a process restart, the server can continue reconciliation and cleanup from the custom resources and their owner references. This does not mean unconfirmed API operations are replayed: an Operation whose outcome cannot be confirmed when the process is interrupted fails with `UNKNOWN_OUTCOME` and is not rerun. Pod scheduling, Pod creation and deletion, CRI operations and checkpoint I/O still go through Kubernetes and the node runtime. Warm-pool configuration, observed state and image cache requests call the shared runtime components directly instead of passing messages through a ConfigMap. Merging the processes removes steps from the notification path, but it does not remove every create or resume cost, and no numeric latency gain is claimed without measurements.

## From image to Box

1. Create a template and set its description. There is no runnable image yet and no warm pool.
2. Build a revision for the template. Provide `userVersion`, `baseImage` and `profileId`, and optionally `buildCommand` and `runCommand`.
3. Wait for the build Operation to succeed and read `result.revisionId`. The base image tag is resolved to a digest, and the build injects the Cellbox guest.
4. Point the template's stable pointer at the new revision. A successful build does not move the pointer by itself. Warm pools need no separate switch: once a Box of this revision is suspended and produces a checkpoint, a warm pool appears automatically.
5. Create a Box with `templateId` to follow the stable revision at that moment, or with `revisionId` to pin a version. Pass exactly one of the two.
6. Wait for the create to finish, then run commands, access files or manage the lifecycle. Destroy the Box explicitly when you are done.

Creating a Box no longer accepts the old flow of choosing an image by `profileId` alone. `ownerKey` is a label tying the Box to your own records; it is not unique and is not used to de-duplicate. Submitting a request again creates an independent request. If a request times out or the connection drops, query the Box and the Operation first, then decide whether to retry.

`buildCommand` runs during the image build as `/bin/sh -c` with root privileges, and can install extra tools, for example `npm install -g ...`. The base image must provide the shell and package manager you need, and the installed files must be usable by the Box's agent user. Build inputs are stored, so do not put passwords or tokens in the command.

`runCommand` is a configuration input written in literal `docker run ...` syntax that expresses the start command, environment variables, working directory and ports. The server parses these fields. It does not run a Docker command and does not rewrite the image's OCI ENTRYPOINT. It must reference the same base image as `baseImage`. For details and limits see the [Go SDK guide](go-sdk.md).

## Versions and the stable pointer

Within one template, the business can keep `userVersion=1.01`, and each successful build produces a different revision, for example `1.01-r1` and `1.01-r2`. If the base image version is unchanged but the tool installation command changed, build a new revision.

An existing Box stays pinned to the revision resolved when it was created; moving the stable pointer does not replace existing instances. Metadata updates to a template or revision require `expectedGeneration`, which rejects overwriting a concurrent change. Use the new generation from the response afterwards.

Retiring a revision blocks selections that need an active revision, such as creating new Boxes from it. Existing Boxes keep running and their checkpoints can still be restored. Before retiring the stable revision, switch or clear the stable pointer. Currently only an empty template can be deleted, meaning one with no revisions and no active build. There is no API to delete a revision yet.

## When warm pools are created and released

The warm pool only serves checkpoint restore. Demand comes from **the checkpoints that exist on this node**, not from a revision's `warmSlots`. Every 500 ms the server counts the Boxes on this node that are `Suspended` and have a checkpoint, and groups them by compatible runtime configuration. Each group gets 1 slot, and 2 slots once it has 3 checkpoints. The node-wide budget is limited by `controller.warmPoolSize`, which defaults to 2 and accepts 0 to 32. During rotation, at most one extra idle slot may be prepared.

| Change | Behavior |
| --- | --- |
| A Box is suspended and produces a checkpoint | A warm slot is created for its compatible configuration and is ready within seconds |
| A Box is resumed or destroyed, or its checkpoint is deleted after an archive | Demand drops. After the last checkpoint of a configuration disappears, one slot is kept for 20 minutes and then released |
| `warmSlots` or `defaultWarmSlots` is changed | No effect on warm-up. `desiredSlots` in `/warm-pool` only echoes the configured value; read `pool.requestedSlots` for the real demand |
| The stable pointer is moved or a revision is retired | No effect on warm-up. Warm-up depends only on checkpoints |
| Several revisions have compatible runtime configurations | They share one pool. The same image alone does not guarantee compatibility |
| You want to turn warm-up off | Set `controller.warmPoolSize` to 0 |

Warm slots are used to restore resumable Boxes from a checkpoint; a first cold create does not claim a slot. When no matching slot is available, resume takes the standard checkpoint restore path. Do not treat a ready warm slot as proof that the business process has started. For the full semantics, slot lifetime and troubleshooting, see [warm pool](warm-pool.md).

## Query state and resources

| Endpoint | Data source and meaning |
| --- | --- |
| Template, revision, build Operation and build log endpoints | SQLite and build logs in the server's persistent data directory. These requests do not query Kubernetes. Resolving and building images additionally needs the registry and BuildKit |
| `GET /v1/templates/{id}/runtime` | Per-revision Box count, phases and latest warm-pool state, queried on every request |
| `GET /v1/template-revisions/{id}/warm-pool` | Desired configuration from SQLite plus the controller's latest warm-pool observation, pool capacity and resource settings |
| `GET /v1/boxes/{id}` | The stored state of one Box. It does not query Kubernetes during the request and never resumes the Box implicitly |
| `GET /v1/boxes` | The runtime inventory. The Kubernetes provider queries the cluster. Supports `templateId`, `revisionId`, `phase`, `limit` and `offset` |

For a Kubernetes profile, `GET /v1/templates/{id}/runtime` lists the current custom resources on every request and does not list Pods; Docker usage is read from SQLite. If the listing fails, `sandboxCount` is `null` and the response is marked `stale`. Its `warmPool` is the controller's latest observation and may lag. `GET /v1/template-revisions/{id}/warm-pool` reads the desired revision configuration from SQLite and then returns that observation; it does not query Kubernetes during the request. Check `stale`, `observedAt`, `generation` and `observedGeneration`. `syncStatus=applied` means the observed demand hash matches the current desired value in SQLite. It does not guarantee that all slots are ready, so also check the pool's `readySlots`.

`sandboxCount: null`, or a missing pool observation, means unknown, not 0. The warm-pool `observedAt` is the time the controller collected the data, and it is marked stale after about 15 seconds. For the usage endpoint, `observedAt` is the time of this listing. A pool's `requestedSlots`, `allocatedSlots` and resource figures are statistics of the shared pool; the same `poolId` can appear under several revisions, so de-duplicate when you sum. The CPU and memory fields are the configured requests and limits of idle Pods, not live utilization. `failedSlots` includes Pods that are currently failed and failures the runtime loop recorded in the last 15 seconds for Pods that were already cleaned up.

## Commands, files and lifecycle

The SDK provides `Commands.Run`, `Files.Read/Write/Open/List`, `Suspend/Resume` and more. Runnable examples are in [`sdk/go/examples`](../sdk/go/examples); the commands to run them are in the [Go SDK guide](go-sdk.md).

A command must have a time limit. The server default is 30 seconds and the maximum is 5 minutes. There is no PTY and no streamed output. A successful Operation means an Exec result was obtained; whether the business command succeeded still depends on its exit code. Start a long-running business process as the workload of a revision.

File paths are relative to the profile's workspace. Absolute paths, parent-directory traversal and following symbolic links are rejected. Writes are limited to 16 MiB, and the SDK `Read` buffer is limited to 16 MiB; download large files with `Open` and close the returned stream. The parent directory must exist first, a write overwrites the existing content, and it is not atomic. File access never resumes a suspended Box.

The Kubernetes provider supports `running -> suspend -> suspended -> resume -> running`, restoring process state from a checkpoint on the same node. The Docker provider supports only basic create, query, commands, files, direct service proxying and destroy. Suspend and resume, archive restore, activating a staged Box, and the protected tool and credential endpoints return `UNSUPPORTED_CAPABILITY`. A running command can make a lifecycle operation return `BUSY`.

The Box generation and the template generation are different concepts. The Box generation can change after resume. The SDK's `Resume` and `Activate` return a new handle that you must use from then on; REST callers should read the Box generation again. A runtime that is ready is not necessarily healthy at the business level.

In-place upgrade, same-ID rebuild, `ensure-running`, manual `reconcile`, leases, domain route registration and the old `/v1/images*` endpoints have been removed. After upgrading a template, create new Boxes. To migrate a workspace, the layer above composes suspend, archive, create-and-restore, verify and activate. The `Idempotency-Key` in a request is ignored.

## Error recovery and cleanup

A `202` from a lifecycle, create, command or build request only means the request was accepted. Keep the Operation ID and query the result. Cellbox does not promise request replay. A network timeout does not tell you whether the request was accepted, so query state first and do not automatically resend a command, build or archive. After a server restart, a runtime Operation whose outcome cannot be confirmed fails with `UNKNOWN_OUTCOME` and is not rerun. Cancelling a client context only stops the local wait; a template build can be cancelled explicitly through its Operation ID.

Destroy a Box with `POST /v1/boxes/{id}:destroy` or the SDK `Destroy`, and wait for it to finish. It does not implicitly cancel a command that is running. The example programs try to clean up the Boxes they create but keep the template and revision. Real applications should also use a separate cleanup timeout and handle failures.

Archive is supported only for suspended or failed Kubernetes Boxes. It requires a persistent HOME, archive support on the node and a configured S3-compatible object store. It does not suspend or resume the source Box automatically. A restore creates a new blank staged Box, which the caller activates explicitly after credentials are configured. A restore candidate that fails is not retried automatically and should be destroyed by the caller. Docker does not support archive or restore. For the exact requests see the [OpenAPI document](../api/openapi.yaml).
