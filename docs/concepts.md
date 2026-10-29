# Concepts and terminology

This page fixes the meaning of the words used across the Cellbox documentation, the REST API and the Go SDK. Read it first if a term in another guide is unclear.

## Naming conventions

Three pairs of words are easy to confuse. Cellbox uses them as follows.

| Say | Meaning | Not to be confused with |
| --- | --- | --- |
| **Box** | The API object you create, query, suspend and destroy (`/v1/boxes`). "Sandbox" is used informally for the same thing. | **gVisor sandbox**: the isolated runtime environment inside a Pod that a Box runs in. |
| **Exec** | One time-limited command run in a Box (`/v1/boxes/{id}/execs`, `/v1/execs/{id}`). | **Runtime execution**: one run of a Box's Pod and container, identified by a Pod UID. A Box gets a new runtime execution after each resume. |
| **Box generation** | Counter of runtime executions of a Box. Changes after resume. Send it as `expectedGeneration`; a stale value gives `STALE_GENERATION`. | **Template generation**: optimistic-concurrency counter for template metadata edits. |

## Resource model

```text
Client namespace
 +-- Template ---- stable pointer ---> Revision (immutable) <--- built from Profile + base image
 |                                         |
 |                                         +--> Box (pinned to one Revision)
 |                                                +-- Operations (create, suspend, resume, ...)
 |                                                +-- Execs (commands)
 |                                                +-- Volume / HOME, Archives
```

| Term | What it is |
| --- | --- |
| **Client namespace** | Isolates one caller's templates and records from another's. Selected with the `X-Cellbox-Client-ID` header; if omitted, the server default is used. It is a data namespace, not an authentication credential. |
| **Profile** | Runtime parameters configured by the administrator: provider, node, Kubernetes namespace, CPU, memory and guest settings. A revision stores a snapshot of the profile it was built with. |
| **Template** | A family of revisions: name, description and an optional `stableRevisionId`. A template has no runnable image of its own. |
| **Revision** | One successful build of a template. Immutable: it fixes the base image digest, the build inputs, the resulting image and the runtime configuration. Named by `userVersion` plus a counter, for example `1.01-r2`. Can be `active` or `retired`. Building a revision does not move the stable pointer. |
| **Stable revision** | The revision a template currently points to. Creating a Box with `templateId` resolves to it at that moment; creating with `revisionId` pins a specific version. Changing the pointer never affects existing Boxes. |
| **Box** | A running or suspended instance created from one revision. Carries an `ownerKey`, a free-form label you choose; it is not unique and is not used for de-duplication. |
| **Operation** | An asynchronous task: build, create, exec, suspend, resume, destroy, archive. Requests return `202` with an operation ID. `202` means accepted, not done. Statuses: `queued`, `running`, `cancelling`, `succeeded`, `failed`, `cancelled`. |
| **Volume** | An independently managed persistent volume that can be mounted into Boxes, for example as HOME. Has its own lifecycle and can be backed up or exported. |
| **Archive** | A `tar.gz` copy of a stopped Box's workspace, stored in S3-compatible object storage. See "Checkpoint versus archive" below. |

## Box lifecycle

A Box reports a runtime `phase`. `running` does not mean the application is healthy: it means the runtime confirmed the execution and opened access to the guest agent.

| Phase | Meaning |
| --- | --- |
| `creating` | The Box is being provisioned. |
| `running` | The Box has a live runtime execution. |
| `checkpointing`, `suspending` | A suspend is in progress. |
| `suspended` | Process state is saved in a checkpoint and the Pod is gone. |
| `resuming`, `restoring` | A resume is in progress; the checkpoint is being restored. |
| `staged` | The guest agent is up but the business program has not started. Used after restoring an archive, until you activate the Box. |
| `deleting`, `deleted` | Destroy in progress / finished. |
| `failed` | The runtime failed. Failures are reported, never hidden by silently starting a fresh instance. |

Suspend and resume keep the same Box ID, but resume runs in a different Pod, so the Box generation changes. After resume use the new generation (the Go SDK returns a new handle).

## Runtime terms (Kubernetes)

| Term | What it is |
| --- | --- |
| **`cellbox-server`** | The single process on one node that serves the HTTP API, runs the controller and drives the node runtime. |
| **Cellbox custom resource** | The Kubernetes object holding a Box's desired state, current execution and checkpoint reference. It owns the Pod and Service. (The Go type is still named `ResumablePod`.) |
| **Pod** | One run of a Box. Deleted on suspend; a different Pod is created or claimed on resume. |
| **Guest agent** | `cellbox-container-agent` inside the sandbox. Starts the business program and serves commands, files and service proxying on port 40000. |
| **Wrapper** | `cellbox-runsc-wrapper`, installed on the node and called in place of `runsc`. Checks the Pod's ticket and rewrites a sandbox start into a restore. |
| **Ticket** | A node-local record authorizing one execution (Pod identity, checkpoint, HOME). The server writes it; the wrapper reads it. |
| **Node base** | The node directory (default `/var/lib/cellbox`) holding tickets, claims, warm leases, checkpoints and HOME data. |

## Suspend and resume

| Term | What it is |
| --- | --- |
| **Checkpoint** | A gVisor snapshot of a Box's whole sandbox, including process memory. Stored on the node that took it, so a Box resumes only on that node. Used once: restoring consumes it, and a later suspend makes a new one. Persistent HOME is stored separately and is not part of the checkpoint. |
| **Warm pool** | Pods prepared ahead of time so a resume does not create a Pod inside the request. Demand comes from checkpoints that exist on the node, not from template settings. |
| **Slot** | One prepared Pod in the warm pool, waiting for a Box. It shows `ContainerCreating` while waiting; that is normal. `readySlots` counts slots in this waiting state. |
| **Claim** | Assigning a slot to a Box that is resuming: the node prepares the Box's ticket, checkpoint and HOME and the waiting sandbox restores. The Pod and its IP do not change at claim time. |
| **Deferred checkpoint** | Starting a warm sandbox without a checkpoint and binding the checkpoint only when a Box claims the slot. Enabled in the node's wrapper script. |
| **Lazy load** | After restore, checkpoint pages are read when the application touches them, instead of all at once in the background. The default; `eager` is available. |
| **Resume path** | `warm_pool` when a slot was claimed, `standard` when a normal restore Pod was used. A resume that misses the pool is slower but keeps process memory. |

See [warm pool](warm-pool.md) for how demand, budget and slot lifetime work, and [node installation](node-installation.md) for the node-side settings that enable this path.

## Checkpoint versus archive

| | Checkpoint | Archive |
| --- | --- | --- |
| Captures | Process memory and sandbox state | The workspace directory only |
| Stored | Node-local | S3-compatible object storage |
| Used to | Resume the same Box on the same node | Move a workspace into a new Box |
| Restores into | The same Box | A new `staged` Box; you activate it explicitly |

## Consistency rules

- `202` and timeouts prove nothing about the final outcome. Keep the operation ID and query it.
- `Idempotency-Key` and `ownerKey` do not de-duplicate requests. Repeating a create, build or exec makes an independent request.
- After a server restart, an operation whose outcome is unknown ends as `UNKNOWN_OUTCOME` and is not replayed.
- A Box request with an old `expectedGeneration` is rejected, so a request cannot land on a runtime execution you did not mean.
