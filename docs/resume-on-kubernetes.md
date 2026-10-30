# Resume on Kubernetes

A suspended Box resumes with its process memory intact. When a warm slot is ready, the process answers its first request about 75 ms after the resume request. This page explains what that figure means, why a restore is normally much slower, which parts of Cellbox remove the time, what it costs, and how to measure it yourself.

## 1. What "about 75 ms" means

The figure is the time from the request `POST /v1/boxes/{id}:resume` to the first successful response from the process inside the Box. It holds when all of these are true:

- the Box resumes on the node that took its checkpoint (there is no cross-node resume);
- a warm slot was ready and was claimed (`resumePath` is `warm_pool`);
- the node is set up as in [node installation](node-installation.md): the Cellbox gVisor build, the deferred-checkpoint settings and lazy page loading.

A resume that misses the pool is meant to preserve process memory but takes longer. It is not covered by this figure. **Known issue:** with `cellbox-gvisor-20261029`, this fallback crashes the sandbox and the resume fails, leaving the Box failed. All 5 fallback restores in the logs of the measured node hit this.

## 2. Why a restore is slow by default

Bringing a gVisor checkpoint back on Kubernetes normally means doing all of this after the request arrives:

1. create a Pod and wait for scheduling;
2. set up the Pod network and the gVisor sandbox;
3. check the checkpoint and bind the Box's HOME;
4. read all checkpoint memory before the application runs;
5. wait for the kubelet to publish Pod status before reporting success.

Several of these steps depend on the Kubernetes API server and the kubelet.

## 3. What Cellbox does

### Warm pool

Pods and gVisor root sandboxes are prepared before any request, based on the checkpoints that exist on the node. A resume claims a ready slot instead of creating a Pod. Steps 1 and 2 are already done; the claim itself takes under 1 ms. See [warm pool](warm-pool.md) for sizing, lifetime and cost.

### Deferred checkpoint

The warm sandbox starts without a checkpoint (`runsc restore --defer-checkpoint`). The business container is created ahead of time without Box-specific mounts and waits. When a Box claims the slot, the wrapper verifies the checkpoint, consumes it, binds the HOME volume and restores into the waiting sandbox. This is why one slot can serve any Box with a compatible runtime configuration, and why the Box's HOME is not part of the pool.

### Lazy page loading

After the restore, memory pages are read when the application touches them (`--background-load=lazy`), not all at once. The process runs before its memory is fully loaded. The cost moves to the first accesses. In one measurement with 500 MiB of memory (6 runs each), the first healthy response came after a median of 191 ms with lazy loading and 232 ms with eager loading (one eager run took 667 ms), while a request that touched all of the memory took a median of 578 ms with lazy and 513 ms with eager. That measurement predates the latency work described below and the samples are small, so read this as the trade-off, not as a measured gain. Set `CELLBOX_BACKGROUND_LOAD=eager` to load everything in the background instead.

### Local commit before Kubernetes

A resume transition is committed to a local journal and handed to the controller inside the same process. The Kubernetes write happens afterwards and is retried if it fails. The request does not wait for the API server. If no slot is ready, the transition falls back to the ordinary Kubernetes lifecycle.

### One wait for the disk

A warm resume waits for the disk once, for the marker that says the checkpoint was consumed. That marker must be durable before the guest runs, and it takes about 3-4 ms on the measured disk.

Everything else avoids the wait:

- Journal records live in memory. Each commit also appends a checksummed line to `runtime-journal.log` without syncing it. A background flush writes batches to SQLite after 100 ms and empties the log. On start, the server replays the log and stops at the first damaged line.
- The resume operation record and its completion are SQLite commits that do not sync.
- Before anything is published to Kubernetes, the journal is flushed and synced, so Kubernetes never sees state that the node could lose.

A process crash loses no committed record, because the page cache survives it. Power loss can drop the newest records; the consumed marker then refuses to replay the checkpoint, so the Box fails instead of running a second time from the same memory.

### Completion by port listening

The node does not wait for kubelet Pod status. It confirms the execution through the local CRI and detects that the Box's ports are listening with an eBPF monitor in the Pod's network namespace. The operation completes when the execution is confirmed and the ports are open.

By default that means the Guest's port 40000. Pass `waitPorts` to wait for your own service as well, for example `{"expectedGeneration": 3, "waitPorts": [8000]}`. In the test below this cost nothing measurable.

### After the resume: routing requests

Requests to a running Box go straight to its Pod IP on port 40000. The server keeps the address of each committed execution in memory, and the Guest rejects any request that carries another Box's identity, so a stale address cannot reach the wrong Box. The identity is not part of the Guest command line (that would make every Box's warm slot different): the server binds it once through `POST /v1/identity` on the first request to a fresh Guest, and the Guest keeps it in memory, so a checkpoint carries it. This does not shorten the resume, but it keeps the first requests after it fast: a request through the API proxy costs about 1.3 ms, against 8-40 ms when every request read Kubernetes.

## 4. Where the time goes

Trace of 30 warm-hit resumes with controller and wrapper timestamps, as median offsets from the request:

| Offset | Event |
| --- | --- |
| 2 ms | Warm slot claimed |
| 3 ms | Slot activated |
| 4 ms | The wrapper starts checking the ticket and checkpoint |
| 9 ms | Checkpoint consumed (the one disk wait), HOME bound, `runsc` restore begins |
| 50 ms | Ports confirmed listening |
| 53-54 ms | Checkpoint invalidated, service enabled, `Running` committed |
| 55 ms | Client sees the operation complete |
| 75 ms | First HTTP response (from the measurement below) |

About 40 ms of this lies between the start of the restore and the ports being confirmed, and about 20 ms is the first request after the operation completes. A later request through the same path takes about 3 ms.

## 5. Measurements

Conditions: one k3s node (8 cores, 15 GiB, virtual disk) with about 40 sandboxes, gVisor build `cellbox-gvisor-20261029`, deferred checkpoint, lazy loading, a warm slot ready before every resume, client on the same node calling the API, serial resumes, timing logs enabled in the wrapper. The workload is the Go HTTP test service in `test/counter` with 8 MiB of memory and an empty HOME. Requests go through the API proxy.

| How the client waits | Runs | Until the operation completes | Until the first HTTP 200 |
| --- | --- | --- | --- |
| `waitPorts: [8000]`, then one request | 30 | median 54 ms, range 44-65 ms | median 78 ms, p90 96 ms, range 60-98 ms |
| Polling the service from the start of the request | 30 | median 52 ms, range 46-64 ms | median 74 ms, p90 86 ms, range 61-90 ms |
| Operation only, suspending right after | 100 | median 52 ms, p90 64 ms, p99 73 ms, range 42-73 ms | not measured |

All 160 resumes succeeded, and the first two rows reported `warm_pool`. They exclude one earlier attempt whose first resume missed the pool and hit the known issue in section 1. In the first two rows the service's counter was unchanged across every resume, and over 10 separate cycles a background process kept its PID and a file in `/tmp` was preserved.

These are samples from one machine with a small workload. A Box with more memory or a larger HOME has not been measured with the current code. Do not assume this figure for a real workload; measure your own as described below. With lazy loading, a first request that touches a lot of memory also takes longer.

## 6. What is guaranteed

- **A checkpoint is used once.** A durable marker records that it was consumed before the guest runs, and it is invalidated before the Box is published as `Running`, so it cannot be replayed, including after power loss.
- **A slot is claimed only when it matches.** The Box must be on this node and have a checkpoint, the runtime fingerprint must match, the slot must have at least 5 seconds of lease left, and the claim checks the owner, Pod UID and checkpoint.
- **Failure is reported, not hidden.** If the execution is lost or the restore fails, the Box is marked failed. Cellbox does not start a fresh instance in its place.
- **A request reaches the Box it was meant for.** A request with another Box's identity is rejected and reported as a stale generation.

## 7. Costs and limits

- Resume works only on the node that took the checkpoint. Creating a Box is not faster.
- Each idle slot is a real Pod. One measured slot held about 21-28 MiB, and slots are replaced about every 45 seconds, which keeps the node doing sandbox creation work. See the cost section in [warm pool](warm-pool.md).
- The fast path needs the Cellbox gVisor build and the wrapper settings; without them warm slots do not start.
- Suspend is not optimized. On the measured Box it took about 2.8-3.0 s because the checkpoint has to be written.
- With lazy loading, the first accesses to memory that was not yet loaded are slower than later ones.
- After `kill -9` of the server during a resume (4 tests), the Boxes came back and the next cycle worked, but the API took about 1-2 minutes to return, mainly because the leader lease is not released on a crash. Recovery from a real power loss follows from the design above and has not been tested.
- Each journal record stays in server memory, about 7 KB per record.

## 8. Measuring it yourself

1. Create a Box and a workload that answers on a port, then suspend it.
2. Wait until the warm pool reports `readySlots > 0` for the revision (`GET /v1/template-revisions/{id}/warm-pool`) and a couple of seconds more.
3. Start a timer and send `POST /v1/boxes/{id}:resume`, with `waitPorts` if you want the operation to wait for your service. Poll the operation until it ends and, in parallel, poll your service through `/v1/boxes/{id}/services/{port}/...` until it answers.
4. Repeat many times and report the median and a high percentile together with the conditions above. Run on the node itself to keep network time out of the result.

Use a cheap probe for readiness. A health check that touches all of the Box's memory measures that memory, not the resume.
