# Go SDK guide

The Go SDK lives in [`sdk/go`](../sdk/go). It requires Go 1.22 and its module path is `github.com/jingyugao/cell-box/sdk/go`. It is developed together with the Cellbox source and is not yet published as a tagged Go module, so applications reference this repository through a local `replace`.

Terms such as Box, Template, Revision and Operation are defined in [Concepts and terminology](concepts.md). The SDK type for a Box is named `Sandbox`.

## Add the SDK to an application

In the application's Go module directory, replace the SDK with a local checkout and add the dependency:

```sh
go mod edit -replace=github.com/jingyugao/cell-box/sdk/go=/path/to/cell-box/sdk/go
go get github.com/jingyugao/cell-box/sdk/go@v0.0.0
```

Use the absolute path of your own Cellbox checkout. Until a tagged release exists, keep the `replace` line in the application's `go.mod`. Run `go mod tidy` when needed to update `go.sum`.

## Connect to Cellbox

If a field in `Config` is empty, the SDK reads these environment variables:

| Variable | Purpose |
| --- | --- |
| `CELLBOX_API_URL` | Cellbox API address, for example `http://127.0.0.1:18092` |
| `CELLBOX_CLIENT_ID` | Optional. Client namespace for templates and other client-owned data |
| `CELLBOX_CLIENT_TOKEN` | Bearer token, only for an authenticating gateway placed in front of Cellbox. The Cellbox API itself does not use it for catalog authentication |

`ClientID` selects a catalog namespace. It is not a password, a unique business ID or an authentication identity. `OwnerKey` is only a label that ties a Box to your own records; it is not unique and is not access control. If you omit `ClientID`, the server's default namespace is used. Configure TLS and proxies through `HTTPClient`. The SDK disables HTTP redirects so that a token is never forwarded to another host.

To reach a Cellbox deployed in Kubernetes from your machine, forward the API Service to a local port. The namespace, context, Service name and port below are examples; use the values of your deployment:

```sh
kubectl --context <context> -n <namespace> port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

Then point the SDK at it and name a template that exists in your catalog:

```sh
export CELLBOX_API_URL=http://127.0.0.1:18092
export CELLBOX_TEMPLATE_ID=<template ID>
```

Run the example from the repository root:

```sh
go -C sdk/go run ./examples/basic
```

The example creates a Box from the template's stable revision, runs a short command, prints the result, and tries to destroy the Box with a separate 60-second context. A failed cleanup is logged. To pin an immutable revision instead, set `CELLBOX_REVISION_ID` and leave `CELLBOX_TEMPLATE_ID` unset. Exactly one of the two variables must be set.

## Create a template and build a revision

A **Template** is a family of revisions with mutable metadata. A **Revision** is the immutable result of a build: a runnable image plus a snapshot of the profile. For example, building user version `1.01` twice successfully gives `1.01-r1` and `1.01-r2`. A Box stays pinned to the revision that was selected when it was created.

```go
package main

import (
	"context"
	"fmt"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

func build(ctx context.Context, c *cellbox.Client) error {
	template, err := c.CreateTemplate(ctx, cellbox.CreateTemplateOptions{
		Name: "coding-agent",
	})
	if err != nil {
		return err
	}

	op, err := c.BuildRevision(ctx, template.ID, cellbox.CreateRevisionOptions{
		UserVersion: "1.01",
		BaseImage:   "docker.io/library/ubuntu:24.04",
		ProfileID:   "coding-k8s",
		BuildCommand: "apt-get update && apt-get install -y git ca-certificates",
		// Optional. This literal docker run form describes sandbox workload
		// settings and must reference BaseImage; Cellbox does not run Docker here.
		RunCommand: "docker run ubuntu:24.04 /bin/sh -c 'sleep infinity'",
	})
	if err != nil {
		return err
	}

	revision, err := c.WaitBuildRevision(ctx, op)
	if err != nil {
		return err
	}
	fmt.Printf("built %s (%s)\n", revision.Version, revision.ID)

	// Stable selection is an explicit metadata update using optimistic
	// concurrency. A successful build never publishes itself automatically.
	_, err = c.UpdateTemplate(ctx, template.ID, cellbox.UpdateTemplateOptions{
		StableRevisionID:   &revision.ID,
		ExpectedGeneration: template.Generation,
	})
	return err
}

func main() {
	client, err := cellbox.NewClient(cellbox.Config{})
	if err != nil { panic(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := build(ctx, client); err != nil { panic(err) }
}
```

A build needs a usable profile and a configured image builder and registry. Before building, `baseImage` is resolved to an immutable digest. `buildCommand` runs as root during the image build. `runCommand` is an input written in literal `docker run ...` syntax that describes the workload of the Box. Cellbox parses the command, entrypoint, environment variables, working directory and ports it supports. It does not run `docker run` and does not rewrite the image's OCI ENTRYPOINT. It must reference the same base image as `baseImage`. Builds support `linux/amd64`. Build inputs are stored in the revision and in the Operation record, so never put passwords or tokens in `buildCommand`.

A build is an asynchronous Operation. The SDK returns the persisted Operation, and `WaitBuildRevision` waits for it to finish and then reads the resulting revision. Use `GetOperation`, `ListOperations` and `GetOperationLogs` to follow progress. Every submission creates an independent build; requests are never replayed. After a server restart, an Operation whose outcome cannot be confirmed fails with `UNKNOWN_OUTCOME` and is not rerun. `CancelOperation` requests cancellation of a build, but it does not guarantee that already published image artifacts are cleaned up.

A wait timeout only stops the SDK from waiting. It does not cancel the accepted build; call `WaitOperation` with a new context to keep waiting. If the submit request fails with a network error, its outcome may be unknown. Look up existing Operations or build state before deciding what to do next.

```go
import (
	"context"
	"errors"
	"fmt"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

op, err := client.BuildRevision(ctx, templateID, buildInput)
if err != nil { return err }

waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Minute)
defer cancelWait()
revision, err := client.WaitBuildRevision(waitCtx, op)
if err != nil {
	var operationErr *cellbox.OperationError
	if errors.As(err, &operationErr) && operationErr.Operation != nil {
		// Save the ID; you can call WaitOperation again later with a new context.
		fmt.Println("operation:", operationErr.Operation.ID)
	}
	return err
}
_ = revision
```

Read logs page by page using `NextOffset`. The SDK reads at most 1 MiB per call:

```go
offset := int64(0)
for {
	chunk, err := client.GetOperationLogs(ctx, op.ID, offset, 64*1024)
	if err != nil { return err }
	fmt.Print(chunk.Text)
	if chunk.EOF { break }
	if chunk.NextOffset == offset {
		// No new log output yet; pause to avoid busy polling.
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	offset = chunk.NextOffset
}
```

`GetOperation` and `WaitOperation` work for both build and runtime Operations. `ListOperations`, logs and cancellation currently apply only to template builds. After the log reaches EOF, still check the Operation status to tell success, failure and cancellation apart.

The main error types are:

- `APIError`: an HTTP or API error, with the status code and error code.
- `RequestError`: a failed submit request. After a network error the server may have accepted it.
- `OperationError`: carries the latest Operation snapshot and the underlying error.
- `CommandExitError`: returned when a command exits non-zero. `Run` returns it together with the `CommandResult`, so stdout, stderr and the exit code are preserved.

Extract them with `errors.As`. Each command submission runs independently. After a network failure, look up the Operation and the Exec record first; do not resend automatically.

To cancel a build that is still running, request cancellation explicitly with its Operation ID. This only works for cancellable template builds:

```go
_, err := client.CancelOperation(ctx, operationID)
if err != nil { return err }
```

`DefaultWarmSlots` on a template and `WarmSlots` on a revision are stored in the SQLite catalog but no longer decide warm-pool demand. Demand comes from checkpoints that exist on the node; see [warm pool](warm-pool.md). When you build a revision, `WarmSlots == nil` copies the template default, and an explicit value (including `0`) is used as given.

A template name cannot be changed after creation. `description`, `defaultWarmSlots` and `stableRevisionId` can be changed with `UpdateTemplate`, which requires `ExpectedGeneration` (the template generation). `SetTemplateStableRevision(ctx, templateID, "", generation)` clears the stable pointer. A revision's image, base image, build command, runtime configuration and profile snapshot are immutable; `description`, `status` (`active` or `retired`) and `warmSlots` can be updated. Before retiring the stable revision, switch or clear the stable pointer. Retiring a revision blocks new selections of it but does not affect Boxes already pinned to it. `GetTemplateRuntime` returns the Box count and phases per revision, and `GetRevisionWarmPool` returns warm-pool sync and pool state. Only an empty template, with no revisions and no queued or running build, can be deleted.

The common methods, grouped by resource:

| Area | SDK methods | Notes |
| --- | --- | --- |
| Templates | `CreateTemplate`, `ListTemplates`, `GetTemplate`, `UpdateTemplate`, `DeleteTemplate` | Delete works only on an empty template. Updates use the generation to avoid overwriting concurrent changes |
| Revisions | `BuildRevision`, `WaitBuildRevision`, `ListTemplateRevisions`, `GetTemplateRevision`, `UpdateTemplateRevision`, `GetRevisionWarmPool` | A successful build produces an immutable revision. Description, status and warm-pool settings can be changed |
| Build Operations | `GetOperation`, `WaitOperation`, `ListOperations`, `GetOperationLogs`, `CancelOperation` | Inspect, wait for and cancel builds; read logs by offset. Build commands are never replayed |
| Profiles and Execs | `ListProfiles`, `GetExecution` | Profiles are server configuration. An Exec record holds the result of a finished command |
| Boxes | `Create`, `CreateAsync`, `Connect`, `GetSandbox`, `ListSandboxes`, `GetTemplateRuntime`, `Sandbox.Destroy` | `GetSandbox` reads the stored record. `ListSandboxes` and `GetTemplateRuntime` query the runtime inventory (the cluster, for a Kubernetes profile). `GetRevisionWarmPool` reads the server's latest warm-pool observation |
| Commands | `Commands.Run`, `RunArgv`, `Start`, `StartArgv`, `Command.Wait` | One time-limited command per call. No PTY and no streamed output |
| Files | `Files.Open`, `Read`, `Write`, `List` | Paths are relative to the workspace. A suspended Box is not resumed automatically |
| Lifecycle | `Suspend`, `Resume`, `Activate`, `Destroy` | Kubernetes supports suspend, resume and activating a staged Box according to its capabilities. Docker supports only destroy. `Resume` and `Activate` return a new handle, so use the return value |
| Volumes | `CreateVolume`, `GetVolume`, `ListVolumes`, `DeleteVolume`, `CreateVolumeBackup`, `ListVolumeBackups`, `RestoreVolume`, `CreateVolumeExport`, `OpenVolumeExport` and related methods | Persistent volumes and their backups and exports. See the method documentation in [`sdk/go`](../sdk/go) |

## Create and use a Box

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
defer cancel()

sandbox, err := client.Create(ctx, cellbox.CreateOptions{
	TemplateID: templateID, // follows the current stable revision
	OwnerKey:   "job-42",
})
if err != nil {
	return err
}
defer func() {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Minute)
	defer cancelCleanup()
	if err := sandbox.Destroy(cleanupCtx); err != nil {
		log.Printf("destroy sandbox %s: %v", sandbox.ID, err)
	}
}()

result, err := sandbox.Commands.Run(ctx, "git --version && pwd", &cellbox.CommandOptions{
	Timeout: 15 * time.Second,
})
if result != nil {
	fmt.Print(result.Stdout)
}
if err != nil {
	return err // nonzero exit returns both result and *CommandExitError
}
```

To pin a specific revision, pass `RevisionID` instead of `TemplateID`. Exactly one of the two must be set. `OwnerKey` is a label you choose; it is not unique and is not used for authentication. A `Sandbox` handle stores the Box generation observed when it was created or connected. If someone else changes the Box, reconnect with `Client.Connect`.

`Run` and `Start` execute the command with `/bin/sh -c`. `RunArgv` and `StartArgv` call a program by absolute path directly. Commands are time-limited; there is no interactive terminal and no indefinitely running background process. The server's default timeout is 30 seconds and you can set 1 millisecond to 5 minutes. `Start` only returns a handle for the accepted command earlier; the command is still bound by the server timeout. Output is returned in one piece when the command ends, not streamed.

The default HTTP client applies a 30-second total timeout to each request, including reading the response body. A context you pass can set a shorter overall deadline. When you wait for a long build, leave enough time in the context. To download a large file that takes longer, supply a suitable timeout through `Config.HTTPClient`. Cancelling a context only stops the client's request or wait; it does not cancel an accepted server Operation. A network timeout does not mean the request was not accepted. Cellbox does not replay requests, so query the Operation status first. After a server restart, an Operation whose outcome cannot be confirmed fails with `UNKNOWN_OUTCOME` and is not rerun.

## Lifecycle and files

Which lifecycle operations are available depends on the runtime profile. Kubernetes can support `Suspend` and `Resume`, and `Activate` for a staged Box. Docker supports only the basic operations; for suspend, resume, archive, activating a staged Box, or protected tools and credentials, the server returns `UNSUPPORTED_CAPABILITY`. `Resume` and `Activate` return a **new Box handle**; use it for every later command. Requests carry no idempotency or automatic replay guarantee. Check `SandboxInfo.Capabilities` first to see which operations the current profile supports.

`Client.ListSandboxes` returns the current Box list and queries the runtime inventory directly; with a Kubernetes profile it queries Kubernetes. `GetTemplateRuntime` also lists the current Cellbox custom resources on every call (it does not list Pods). `GetRevisionWarmPool` reads the desired configuration from SQLite and the server's latest warm-pool observation; it does not query Kubernetes during the request.

`Sandbox.Files` reads, writes and lists files under the configured workspace, with paths relative to the workspace. File access never resumes a suspended Box. `Read` returns at most 16 MiB. `Write` accepts at most 16 MiB and overwrites the target file; it is not atomic, and the parent directory must already exist. `Open` streams a download and returns an `io.ReadCloser` that the caller must close. File requests act on the current Box and do not use the generation compare-and-swap that command and lifecycle calls use. If a network error leaves the outcome of a write uncertain, do not retry automatically.

The repository also has full template-build and lifecycle examples:

```sh
# Create or reuse a template, build a revision, make it stable,
# then create a Box and run a tool in it
export CELLBOX_PROFILE_ID='<profile ID configured in your deployment>'
export CELLBOX_BASE_IMAGE='docker.io/library/ubuntu:24.04'
go -C sdk/go run ./examples/template

# On the template's stable revision, test the file API and Kubernetes suspend/resume
export CELLBOX_TEMPLATE_ID='<template ID created or reused in the previous step>'
go -C sdk/go run ./examples/lifecycle
```

The template example reads these environment variables:

| Variable | Required | Purpose |
| --- | --- | --- |
| `CELLBOX_API_URL` | Yes | API address. It can also be set with `Config.BaseURL` |
| `CELLBOX_PROFILE_ID`, `CELLBOX_BASE_IMAGE` | Yes | Profile and base image used for the build |
| `CELLBOX_TEMPLATE_ID` | No | Reuse an existing template. If unset, a new template is created |
| `CELLBOX_TEMPLATE_NAME`, `CELLBOX_USER_VERSION` | No | Name and user version of a new template. Defaults: a random name and `1.01` |
| `CELLBOX_BUILD_COMMAND`, `CELLBOX_RUN_COMMAND` | No | Custom image build command and workload settings |
| `CELLBOX_VERIFY_COMMAND` | No | Check command run in a Box after the revision is built |
| `CELLBOX_PLATFORM` | No | Build platform. Default `linux/amd64` |
| `CELLBOX_CLIENT_ID`, `CELLBOX_CLIENT_TOKEN` | No | Data namespace. Set the token only when a gateway in front of Cellbox authenticates requests |

The default build command installs an example tool, `template-tool`. If you set a custom `CELLBOX_BUILD_COMMAND`, the example by default only prints a note and lists the working directory, because it cannot assume the custom image contains that tool. Use `CELLBOX_VERIFY_COMMAND` to run a real check. Both the template example and the lifecycle example try to destroy the Boxes they create; the template and the built revision are kept. The lifecycle example needs `CELLBOX_TEMPLATE_ID` or `CELLBOX_REVISION_ID`, and a profile that supports same-node checkpoints. More examples are in [`sdk/go/examples`](../sdk/go/examples). The HTTP contract and the per-provider capability fields are in the [OpenAPI document](../api/openapi.yaml). The SDK currently has no PTY or streaming support and no archive methods; the archive REST endpoints apply only to Kubernetes Boxes that meet the persistent HOME requirement.

## Warm pool and runtime state

The SDK calls the HTTP API through the `cellbox-api` Service. In a single-node deployment one `cellbox-server` runs behind the Service and handles both the API and the node runtime. Lifecycle requests reach the runtime worker through an in-process keyed queue, and the state snapshot published after the custom resource is written wakes up waiters. The Cellbox custom resource and Pod ownership stay in Kubernetes, so this path still includes custom resource writes and the necessary Pod and CRI operations.

Warm-pool demand comes from Boxes on the node that are suspended and have a checkpoint. It does not depend on a revision's `warmSlots` or on the stable pointer. When the last checkpoint of a configuration disappears, its slot is kept for 20 minutes and then released. Creating a template or building a revision does not create a warm pool by itself. `warm.DesiredSlots` only echoes the configured `warmSlots`; read `Pool.RequestedSlots` for the real demand. A `nil` `Pool` is normal when there are no checkpoints.

Warm Pods are shared between compatible runtime configurations. The total capacity of the node is limited by the runtime loop in the server (default at most 2, configurable up to 32). The slot counts of one `poolId` are totals for the shared pool and repeat in the status of every revision that uses it. `allocatedSlots` can be lower than `requestedSlots` when node capacity is smaller, and when you aggregate over several revisions, de-duplicate by `poolId`. `resources` is the Kubernetes requests and limits configured for the idle warm Pods currently allocated, not measured CPU or memory use. `failedSlots` counts warm Pods currently in a failed state, plus creation failures that the runtime loop recorded in the last 15 seconds for Pods that have since been cleaned up. Warm Pods serve the resume path of resumable Boxes; do not assume they make a first cold create faster. Withdrawing demand removes only idle warm Pods and never kills a Box that has already claimed one.

```go
runtime, err := client.GetTemplateRuntime(ctx, templateID)
if err != nil { return err }
for _, revision := range runtime.Revisions {
	if revision.Stale || revision.ObservedAt == nil {
		log.Printf("revision %s runtime status is not current", revision.RevisionID)
		continue // SandboxCount nil means unknown, not zero.
	}
	log.Printf("revision %s sandboxes=%d phases=%v", revision.RevisionID, *revision.SandboxCount, revision.Phases)
	if revision.WarmPool.Pool != nil {
		log.Printf("pool=%s ready=%d/%d stale=%t", revision.WarmPool.Pool.PoolID, revision.WarmPool.Pool.ReadySlots, revision.WarmPool.Pool.RequestedSlots, revision.WarmPool.Pool.Stale)
	}
}

warm, err := client.GetRevisionWarmPool(ctx, revisionID)
if err != nil { return err }
log.Printf("sync=%s desired=%d applied-generation=%d current=%t", warm.SyncStatus, warm.DesiredSlots, warm.ObservedGeneration, !warm.Stale)
```

For a Kubernetes profile, `GetTemplateRuntime` lists the current custom resources on every call (not Pods); Docker usage is read from SQLite. If the listing fails, `SandboxCount == nil` and `Stale` is true. `GetRevisionWarmPool` reads the desired configuration from SQLite and returns the controller's latest warm-pool observation; it does not query Kubernetes itself. The warm-pool `ObservedAt` is the time the controller collected the data. After about 15 seconds, or before the first snapshot exists, the state is stale or unknown. For the usage endpoint, `ObservedAt` is the time of this listing. Check `Error`, `SyncStatus`, `Stale` and `ObservedAt`. When no pool exists, `Pool` is nil.
