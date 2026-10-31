# Warm pool

The warm pool prepares Pods and gVisor root sandboxes ahead of time, so resuming from a checkpoint does not have to create a Pod inside the request. This page explains where warm demand comes from, how slots are created and retired, which resumes hit the pool, and how to observe it. For API fields see the [REST quick start](rest-quickstart.md) and the [Go SDK guide](go-sdk.md); for the call chain see [architecture](architecture.md). Terms are defined in [concepts](concepts.md).

With a warm-pool hit on the same node, processes in the Box are running again in about 100 ms, also with 1 GB of memory in the Pod (see [the sample measurement](#8-sample-measurement) for the conditions).

## 1. Scope

- The pool serves **checkpoint resume only**. Creating a new Box never claims a slot, and a Box without a checkpoint cannot use one.
- An idle slot is a Pod whose sandbox has been prepared but is not yet assigned to a Box. No Box-specific state is loaded and no business program runs in it. `readySlots` only means the slot reached this waiting state; it does not mean any application is ready.
- The start call that waits for assignment does not return while the slot is idle, so Kubernetes shows an idle slot as `ContainerCreating`. This is expected.
- With no usable matching slot, the resume takes the standard checkpoint restore path. Process memory is preserved in both cases; the standard path is slower.

### Two wrapper modes

How a slot waits depends on the environment the node's wrapper script sets (see [node installation](node-installation.md)). `CELLBOX_NO_ROOT_CONTAINER=true` creates the root sandbox without a root container process, and `CELLBOX_DEFER_CHECKPOINT=true` (which requires it) starts the warm sandbox without a checkpoint.

| | Deferred mode (validated) | Standard mode |
| --- | --- | --- |
| Environment | `CELLBOX_NO_ROOT_CONTAINER=true` and `CELLBOX_DEFER_CHECKPOINT=true` | Neither variable set |
| Root sandbox start | Starts immediately with `runsc restore --defer-checkpoint --background --detach` and no checkpoint | The wrapper blocks in the root sandbox `start` until a Box claims the slot |
| Business container | Created ahead of time without Box-specific mounts; its `start` waits for assignment (`WaitWarm`, 60 s lease) | Not started before the claim |
| After a claim | The wrapper verifies and consumes the checkpoint, binds HOME, gives the container the claiming Box's command line (the slot was built from another Box, and gVisor refuses a restore whose arguments differ from the checkpoint's), then restores with `--background --background-load=<mode>` | The wrapper verifies and consumes the checkpoint, binds HOME, then restores with the same flags |

Deferred mode is the one the warm pool is validated with. Run the standard mode only if you have validated it yourself. In our tests without these variables, warm-slot Pods failed to start (see the troubleshooting table in [node installation](node-installation.md)).

`CELLBOX_BACKGROUND_LOAD` selects how checkpoint pages are loaded after a restore that binds a checkpoint: `lazy` (the default) reads pages when the application touches them, `eager` reads them all in the background. The wrapper passes it as `--background-load` to those restores, not to the deferred root restore, which has no checkpoint. In both modes the process resumes before all pages are loaded, so the first requests after a resume may be slower than later ones.

## 2. Where demand comes from

Demand is set by **the checkpoints that exist on this node**, not by `warmSlots` on a revision. Every 500 ms the pool loop lists the Cellbox custom resources for this node and counts a Box only if all of these hold:

| Condition | Why |
| --- | --- |
| `spec.nodeName` is this node | Checkpoints are node-local; there is no cross-node restore |
| `status.phase` is `Suspended` | Boxes that are suspending or resuming are not counted |
| `status.snapshot` is set | Once a restore has completed the old checkpoint is revoked, so such a Box no longer counts |
| Not being deleted | |

Counted Boxes are grouped by **compatible runtime configuration**. The compatibility fingerprint covers the image, command, environment, resources, Service ports and similar fields. It **ignores the persistent HOME and mounts**, so Boxes with different HOME volumes can share a pool. The same image alone does not guarantee compatibility.

Slots per group:

| Checkpoints in the group | Slots |
| --- | --- |
| 1-2 | 1 |
| 3 or more | 2 (per-group maximum) |

Slots reflect **concurrent resume** capacity, not checkpoint stock. Two slots let two simultaneous resumes hit the pool; a third goes through the standard restore path.

The pool's Pod template is taken from the Boxes themselves (with HOME and mounts removed), so a pool always matches the fingerprint of the Boxes it serves.

## 3. Retirement

- When a project is archived, a Box is deleted, or a restore completes, its checkpoint disappears and demand drops. No manual change is needed.
- When the last checkpoint of a group disappears, **one slot is kept for 20 minutes** before release. A resumed Box suspends again after idling, and the grace period avoids recreating a Pod on every resume and suspend cycle. The timer lives in memory only; after a server restart demand is recomputed from the checkpoints that exist.
- A Pod that a Box has claimed no longer belongs to the pool, and lowering demand does not delete it.

## 4. Total budget

| Item | Value |
| --- | --- |
| Setting | `controller.warmPoolSize` (`--warm-pool-size`), 0-32, default 2; 0 disables the pool |
| Scope | One budget per node, shared by all groups, allocated max-min fairly; no group gets more than it needs |
| Rotation | A successor is created before an old slot is retired, so idle Pods can reach the budget plus 1 |
| Creation rate | At most one Pod per reconcile pass; creation pauses for about 2 seconds after a resume starts, so it does not compete with it |

The budget counts Pods; it does not look at CPU or memory. Each slot has a full container configuration, and the Pod's requests and limits equal those of the revision it serves. Slots are replaced about every 45 seconds (the 60-second lease, with a successor created when under 15 seconds remain), so a larger budget means more continuous `runsc` and containerd work. Size it as the number of images with checkpoints times slots per group, and do not raise it without a reason.

### Resource cost of idle slots

- **Declared resources.** A slot's container copies the Box configuration, so its limits equal the revision's limits, but its CPU and memory **requests are 0**. An idle slot reserves nothing from the scheduler, and the limits only cap what a Box may use after it claims the slot. The `resources` fields in the warm-pool status report these configured values; they are not usage.
- **Memory.** An idle slot holds no business program and no checkpoint data. Measured on one slot (2026-10-29, deferred mode, gVisor `cellbox-gvisor-20261029`), the processes belonging to it (containerd shim, the gVisor sandbox process and the waiting `runsc` process) used about 21-28 MiB of proportional set size, whatever the Box's memory limit. That is one sample on one machine; expect tens of MiB per slot, not hundreds.
- **CPU.** An idle slot used no measurable CPU over a 20-second window. The cost is in creating and retiring slots: each slot is replaced about every 45 seconds, which runs `runsc create` and containerd calls continuously. Keep the budget small.
- **Pod and IP.** Each slot is a real Pod and takes one Pod IP and one place under the node's Pod limit. During rotation the idle count can reach the budget plus one.
- **Disk.** Slots add no checkpoint or HOME data. Those are written when a Box suspends, not when a slot is created.

## 5. Slot lifecycle

1. The controller creates a Pod with a scheduling gate (`cellbox.jingyugao.github.io/prepared`), the label `cellbox.jingyugao.github.io/warm-pool=true`, and an annotation holding the compatibility fingerprint.
2. The node backend registers a lease (`registered`) and the controller removes the gate, so the Pod is scheduled.
3. The wrapper binds the lease when the root sandbox is created (`created`). It then starts waiting (`waiting`), with a **60-second lease**. In deferred mode the wait happens in the business container's `start`; in standard mode it happens in the root sandbox's `start`.
4. When under 15 seconds remain, the controller creates a successor. When under 5 seconds remain, the slot counts as expired and the whole Pod is deleted and its runtime state cleaned up. A Pod UID is never reused (`expired` / `retired`).
5. A resume claims the slot: the node prepares the checkpoint and ticket, the lease becomes `claimed` and then `restoring`, and the wrapper restores. From then on the Pod belongs to that Box.

A slot can be claimed when the Box is on this node, has a checkpoint, its fingerprint matches a `waiting` slot, and that slot has at least 5 seconds of lease left. The claim checks the owner, Pod UID and snapshot. A slot that is lost in a race, or whose identity changed, is skipped.

After a claim the node confirms the execution through the local CRI (`ListPodSandbox`, `ListContainers`, `PodSandboxStatus`) rather than waiting for kubelet to publish Pod status. The Pod IP is stored in the Cellbox custom resource under `status.execution`, and requests to the Box go straight to that Pod IP on port 40000, not through the Service.

When an idle slot is retired, its container may still be in the starting state (the start call has not returned from the wrapper). containerd rejects the delete with an error containing `is in starting state`; cleanup treats this as retryable and keeps trying inside the delete window. An event containing `OCI runtime start failed: warm slot retired` is the wrapper exiting normally after it was woken.

## 6. Relation to `warmSlots`

`defaultWarmSlots` and the revision's `warmSlots` can still be read and written, but they **no longer create warm demand**, and setting them to 0 does not disable warming. Their only remaining role is linking a revision to the pool that serves it:

- `desiredSlots` in `GET /v1/template-revisions/{id}/warm-pool` still shows the configured `warmSlots`. It is informational.
- `pool.requestedSlots` is the demand derived from checkpoints, and `pool.allocatedSlots` is the amount after the node budget is applied.
- `pool` has a value only when the revision's spec matches an existing pool. With no checkpoints, `pool` is `null`, which is normal.
- To disable warming, set `controller.warmPoolSize` to 0.

## 7. Observing and troubleshooting

| Observation | Meaning |
| --- | --- |
| No `cb-warm-*` Pods when there are no checkpoints | Normal; demand is 0 |
| `cb-warm-*` shows `ContainerCreating` | Normal; the slot is waiting for a claim. Use `readySlots` to judge readiness |
| `requestedSlots` is greater than `allocatedSlots` | The node budget is too small. Raise `warmPoolSize`, or check for groups you do not need |
| Resume is slower than expected | Check for a miss first: too few slots, under 5 seconds of lease left, a different fingerprint, or a different node all use the standard path |
| Log line `warm pool reconcile failed ... starting state` | A retirement race in older versions; the current version retries and it should not appear |

`failedSlots` counts warm Pods currently in a failed state, plus creation failures recorded in the last 15 seconds that have since been cleaned up. `stale` and `observedAt` show whether the controller's observation is fresh; it is marked stale after about 15 seconds.

## 8. Sample measurement

The numbers below are samples from a single machine (a single-node k3s cluster). They show order of magnitude only and are not a performance guarantee.

Serial resumes with a warm-pool hit, measured on 2026-10-30 (see [Resume on Kubernetes](resume-on-kubernetes.md#5-measurements)):

| Measured | Runs | Median | Range |
| --- | --- | --- | --- |
| Until the resume operation completes | 100 | 52 ms | 42-73 ms |
| Until the first HTTP response, polling from the start of the request | 30 | 74 ms | 61-90 ms |

With 1 GB in the Pod, measured on 2026-10-31 with memory that is not read after the resume:

| Measured | Runs | Median | Range |
| --- | --- | --- | --- |
| Until the resume operation completes | 16 | about 75 ms | 69-91 ms |
| Until a background process runs again | 16 | about 100 ms | 89-123 ms |
| Until a first command through the API finishes | 16 | 151 ms | 137-175 ms |

Concurrent resumes, measured on 2026-10-31 with `cellbox-gvisor-20261030`: 5 Boxes resuming at once with 2 slots, 8 rounds. Every resume succeeded, and each Box kept its background process and counter.

| Scenario | Runs | Median | Range |
| --- | --- | --- | --- |
| Hit a warm slot | 16 | 96 ms | 68-126 ms |
| Missed and took the standard path | 24 | 873 ms | 755-939 ms |

Disk speed had a large effect on an older version (2026-10-27): with the virtual disk on a spinning drive, hits took about 2.7 s and misses about 7.1 s, mostly from fsync latency. A warm hit now waits for the disk only once, so this effect should be smaller, but it has not been measured again. Measure in your own environment.

## 9. Known limits

- Only `Suspended` Boxes with a checkpoint are warmed; creating a new Box is not faster.
- Each group has at most 2 slots. Concurrent resumes beyond the slot count take the standard path.
- The budget is per node and shared by all groups. The pool does not rank workloads by importance.
- When upgrading to this behavior, the fingerprint of existing idle slots changes, so they are retired and recreated once.
- Slots are replaced about every 45 seconds, which continuously costs `runsc` CPU and containerd RPCs.
