# REST quickstart

This guide creates a template, builds a revision, makes it the stable revision, creates a Box and runs a command in it. It also shows how to query warm pool status. For the Go client, see the [SDK guide](go-sdk.md). For complete request and response schemas, see [`api/openapi.yaml`](../api/openapi.yaml). Terms are defined in [concepts](concepts.md).

## Connect to the service

Forward the API Service to your machine. Replace the context and namespace with those of your deployment (see [deployment](deployment.md)):

```sh
kubectl --context <context> -n <namespace> port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

In another terminal, set the API address:

```sh
export CELLBOX_API_URL=http://127.0.0.1:18092
```

The Cellbox API does not authenticate clients. Send `Authorization: Bearer ...` only if a gateway in front of the API requires it; the gateway validates it. The `X-Cellbox-Client-ID` header selects the client data namespace and is not an authentication credential. If you omit it, the server's default namespace is used.

## Create a template

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/templates" \
  -H 'Content-Type: application/json' \
  -d '{"name":"coding-agent","description":"Agent workspace","defaultWarmSlots":0}'
```

Save the `id` from the response as `TEMPLATE_ID`. A template holds editable metadata, `defaultWarmSlots` (a compatibility field that no longer decides warm-up) and an optional stable revision pointer. A new template has no runnable image yet, so no warm pool is created. Template data is stored in `cellbox.db` in the service's data directory.

## Build a revision

Replace `profileId` with a profile ID configured in `api.config.profiles` of your deployment. A build also needs a working BuildKit configuration, and permission to pull `baseImage` and to push the built image.

```sh
export TEMPLATE_ID='<template ID returned by the previous step>'
export CELLBOX_PROFILE_ID='<profile ID configured in api.config.profiles>'

curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID/revisions" \
  -H 'Content-Type: application/json' \
  --data-binary @- <<JSON
{
  "userVersion": "1.01",
  "baseImage": "docker.io/library/ubuntu:24.04",
  "profileId": "$CELLBOX_PROFILE_ID",
  "buildCommand": "apt-get update && apt-get install -y git ca-certificates",
  "platform": "linux/amd64"
}
JSON
```

The response is an asynchronous build Operation. Save its `id` as `BUILD_ID`. The build resolves the base image to a digest and records the build inputs. A build with `userVersion` `1.01` creates an immutable numbered revision such as `1.01-r1`; a later successful build can produce `1.01-r2`. Every submission creates its own build task; submissions are not de-duplicated.

Query the build Operation. Each request waits up to 10 seconds:

```sh
export BUILD_ID='<operation id returned above>'
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/operations/$BUILD_ID?waitMs=10000"
```

Repeat while `status` is `queued` or `running`. When it finishes, read `result.revisionId` and save it:

```sh
export REVISION_ID='<result.revisionId of the succeeded operation>'
```

If the build fails, check the Operation's `error` and read the log in chunks from `/v1/operations/$BUILD_ID/logs?offset=0&limit=65536`; use `nextOffset` from the response as the next offset. Logs come back as size-limited JSON chunks; streaming is not supported. A successful build does not change the stable revision. Building a revision and preparing warm slots are independent.

The template, revision, build Operation and log endpoints use the service's local `cellbox.db` and do not query Kubernetes. Resolving or building an image contacts the image registry and BuildKit. When you choose a profile, the system stores a snapshot of its configuration in the revision.

Changing the stable revision affects only Boxes created afterwards. Warm pool demand comes from the checkpoints that exist on the node, not from a revision's `warmSlots`. Compatible revisions share one pool, and each node has a shared capacity limit (default 2, configurable up to 32); see [warm pool](warm-pool.md). The slot counts for a given `poolId` are totals for the shared pool and repeat in the status of every revision that maps to it; node capacity can make `allocatedSlots` smaller than `requestedSlots`. When you add up several revisions, de-duplicate by `poolId`. Withdrawing demand only cleans up idle warm Pods; a Pod already claimed by a Box is not deleted. A warm Pod serves the resume path of suspended Boxes, so do not assume it speeds up the first cold create.

## Query warm pool and template runtime status

The warm pool status endpoint reads the revision's desired configuration from SQLite and returns the latest snapshot that the controller observed and kept in the provider. It does not query Kubernetes. The template runtime endpoint, by contrast, lists the current Cellbox custom resources for Kubernetes profiles on every call (it does not list Pods), and reads Docker usage from SQLite. To see a revision's warm pool synchronization and pool state:

```sh
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/template-revisions/$REVISION_ID/warm-pool"
```

The response includes `desiredSlots`, `syncStatus`, `generation` / `observedGeneration`, `observedAt`, `stale` and a nullable `pool`. `stale` means the controller's observation is out of date or does not match the current desired configuration in SQLite. `pool: null` means the revision is not associated with a warm pool. In `pool`, `requestedSlots`, `allocatedSlots`, `readySlots`, `preparingSlots` and `failedSlots` separate demand from actual preparation progress. `failedSlots` counts Pods that are still failed, plus creation failures that the runtime loop recorded within the last 15 seconds and has since cleaned up.

To get the Box count, lifecycle phases and warm pool status across all revisions of a template:

```sh
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID/runtime"
```

`sandboxCount: null` means the listing failed and the count is unknown; it is not 0. In usage data, `observedAt` is the time of this query; in warm pool data, `observedAt` is when the controller collected it. `resources` holds the configured requests and limits of the idle Pods allocated to the pool. It is not live CPU or memory usage. A shared pool can map to several revisions, so de-duplicate by `poolId` when you add resources up.

## Set the stable revision

Use the `generation` returned when the template was created, or by the most recent GET. An update must include `expectedGeneration`. After a successful update, use the new generation from the response for the next operation.

```sh
curl --fail-with-body -sS -X PATCH "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID" \
  -H 'Content-Type: application/json' \
  -d "{\"stableRevisionId\":\"$REVISION_ID\",\"expectedGeneration\":1}"
```

The stable revision is stored in SQLite metadata and must point to an active revision of the same template. To pin a Box to a specific version, pass `revisionId` when you create it, and do not pass the template's `templateId`.

## Create a Box and run a command

This creates a Box from the template's current stable revision. `ownerKey` is a label you choose to link the Box to your own records. It is not unique and not used for authentication. Cellbox does not promise to replay requests: if the outcome of a submission is uncertain, look it up by Operation or by your own business data before you submit again.

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes" \
  -H 'Content-Type: application/json' \
  -d "{\"templateId\":\"$TEMPLATE_ID\",\"ownerKey\":\"job-42\"}"
```

Save the Operation `id` from the response as `CREATE_OP_ID`. Poll `/v1/operations/$CREATE_OP_ID?waitMs=10000` until the Operation ends, then read the Box `id` and `generation` from `box`. You can also take `targetId` from the finished Operation and GET `/v1/boxes/{id}`. The configured provider creates the runtime; with a Kubernetes profile, the provider queries Kubernetes.

To run a time-limited shell command, pass `/bin/sh -c` explicitly. First save the Box ID and generation from the create result:

```sh
export BOX_ID='<box id>'
export BOX_GENERATION='<generation>'

curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes/$BOX_ID/execs" \
  -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"/bin/sh\",\"-c\",\"git --version && pwd\"],\"timeoutMs\":30000,\"expectedGeneration\":$BOX_GENERATION}"
```

The response contains an Operation ID and `result.execId`. Poll the Operation until it ends, then GET `/v1/execs/{execId}` to read the final `stdout`, `stderr` and `exitCode`. The default timeout is 30 seconds and the maximum is 5 minutes. Output is returned once, after the command ends; PTY, stdin and streaming output are not supported. Every submission runs a new command. After a timeout, check the Operation and the Exec record before you send the command again.

When the test is done, destroy the Box explicitly, using the generation from the create result:

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes/$BOX_ID:destroy" \
  -H 'Content-Type: application/json' \
  -d "{\"expectedGeneration\":$BOX_GENERATION}"
```

The request returns HTTP 202 and an Operation; poll its ID until it reaches a final state. If the client stops waiting, the accepted destroy is not cancelled.

## Behavior notes

- Template and revision metadata operations use `cellbox.db` in the service's data directory. The runtime loop of the single-process server coordinates the warm pool directly from the checkpoints that exist on the node. Warm pool status queries read the background snapshot and do not query the cluster. Box creation and lifecycle operations write the Kubernetes Cellbox custom resource and notify the runtime worker through an in-process queue; the custom resource remains the durable desired and observed state and owns the actual Pod and Service. Commands, files and listing operations run through the selected runtime provider.
- A template's stable revision affects only Boxes created afterwards. An existing Box keeps the immutable revision it was created with.
- Warm pool demand is determined only by `Suspended` Boxes that have a checkpoint. It does not depend on `warmSlots` or on the stable pointer; `desiredSlots` only echoes the configured value. The status endpoint returns a background snapshot; use `stale` and `observedAt` to judge how fresh it is.
- Only an empty template, with no revisions and no pending builds, can be deleted. There is no endpoint to delete a revision. Marking a revision `retired` stops it from being selected for new Boxes and keeps its history.
- A client that disconnects or cancels its local wait does not cancel an accepted server-side Operation; query it by Operation ID. After a network failure, do not automatically resubmit an operation with side effects. After a service restart, an Operation whose outcome cannot be confirmed fails with `UNKNOWN_OUTCOME` and is not rerun. To cancel a build, call `POST /v1/operations/{id}:cancel`.

## Archive and restore

Archiving accepts only a suspended or failed Kubernetes Box that uses a persistent HOME. It needs archive support on the node and S3-compatible object storage. Without object storage, capture, download, restore and delete return `UNSUPPORTED_CAPABILITY`. Cellbox does not suspend or resume the source Box for you. Submit `POST /v1/boxes/{id}/archives` and wait for the Operation; a successful result gives the `archiveId`. Use `GET /v1/archives/{id}` for metadata, `GET /v1/archives/{id}/content` to download the archive and `DELETE /v1/archives/{id}` to delete it.

To restore, call `POST /v1/boxes:restore` with `archiveId`, `ownerKey` and exactly one of `templateId` or `revisionId`. The service creates a new staged Box. After the restore Operation succeeds, your application writes any credentials it needs and then calls `POST /v1/boxes/{id}:activate` to start the workload. A candidate Box whose restore failed is not retried automatically; destroy it explicitly. The Docker provider does not support archive, restore or staged activation.
