# Resume on Kubernetes

A suspended Box resumes with its process memory intact. When a warm slot is ready, the process is serving requests again in about 200 ms. This page explains what that figure means, why a restore is normally much slower, which parts of Cellbox remove the time, what it costs, and how to measure it yourself.

## 1. What "about 200 ms" means

The figure is the time from the request `POST /v1/boxes/{id}:resume` to the first successful response from the process inside the Box. It holds when all of these are true:

- the Box resumes on the node that took its checkpoint (there is no cross-node resume);
- a warm slot was ready and was claimed (`resumePath` is `warm_pool`);
- the node is set up as in [node installation](node-installation.md): the Cellbox gVisor build, the deferred-checkpoint settings and lazy page loading.

A resume that misses the pool still preserves process memory but takes longer. It is not covered by this figure.

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

Pods and gVisor root sandboxes are prepared before any request, based on the checkpoints that exist on the node. A resume claims a ready slot instead of creating a Pod. Steps 1 and 2 are already done; the claim itself takes about 5 ms. See [warm pool](warm-pool.md) for sizing, lifetime and cost.

### Deferred checkpoint

The warm sandbox starts without a checkpoint (`runsc restore --defer-checkpoint`). The business container is created ahead of time without Box-specific mounts and waits. When a Box claims the slot, the wrapper verifies the checkpoint, consumes it, binds the HOME volume and restores into the waiting sandbox. This is why one slot can serve any Box with a compatible runtime configuration, and why the Box's HOME is not part of the pool.

### Lazy page loading

After the restore, memory pages are read when the application touches them (`--background-load=lazy`), not all at once. The process runs before its memory is fully loaded. The cost moves to the first accesses. In one measurement with 500 MiB of memory (6 runs each), the first healthy response came after a median of 191 ms with lazy loading and 232 ms with eager loading (one eager run took 667 ms), while a request that touched all of the memory took a median of 578 ms with lazy and 513 ms with eager. The samples are small, so read this as the trade-off, not as a measured gain. Set `CELLBOX_BACKGROUND_LOAD=eager` to load everything in the background instead.

### Local commit before Kubernetes

A resume transition is committed to a local durable journal and handed to the controller inside the same process. The Kubernetes write happens afterwards and is retried if it fails. The request does not wait for the API server. If no slot is ready, the transition falls back to the ordinary Kubernetes lifecycle.

### Completion by port listening

The node does not wait for kubelet Pod status. It confirms the execution through the local CRI and detects that the Box's ports are listening with an eBPF monitor in the Pod's network namespace. The operation completes when the execution is confirmed and the ports are open.

### After the resume: routing requests

Requests to a running Box go straight to its Pod IP on port 40000. The server keeps the address of each committed execution in memory, and the Guest rejects any request that carries another Box's identity, so a stale address cannot reach the wrong Box. The identity is not part of the Guest command line (that would make every Box's warm slot different): the server binds it once through `POST /v1/identity` on the first request to a fresh Guest, and the Guest keeps it in memory, so a checkpoint carries it. This does not shorten the resume, but it keeps the first requests after it fast: a request through the API proxy costs about 1.3 ms, against 8-40 ms when every request read Kubernetes.

## 4. Where the time goes

Server-side trace of 35 warm-hit resumes (before the address cache above), medians:

| Step | Time |
| --- | --- |
| Request accepted and stored | about 4 ms |
| Prepare the execution: claim a warm slot (about 5 ms of it), persist the assignment, prepare ticket and checkpoint | about 50 ms |
| The rest: restore, wait for the ports, invalidate the used checkpoint (17-18 ms of it), commit `Running` | about 115 ms (the remainder) |
| Total | about 170 ms |

The last row is the remainder after the two measured rows and is not broken down further. A finer breakdown needs more spans in the restore path and timestamps from the wrapper. The current numbers are enough to say that claiming a slot is cheap and that restoring and waiting for the process dominate.

## 5. Measurements

Conditions: one k3s node (8 cores, 7.9 GB), gVisor build `cellbox-gvisor-20261029`, deferred checkpoint, lazy loading, a warm slot ready before every resume, client on the same node calling the API, serial resumes.

| Workload | Runs | Measured | Result |
| --- | --- | --- | --- |
| Test program, 8 MiB memory, empty HOME | 30 | to the first healthy response through the API | median 173 ms, p95 205 ms, min 141 ms, max 211 ms |
| Real template Box, 109 MiB HOME | 5 | until the resume operation completes | median 199 ms, range 157-213 ms |

In the first row the process state (PID, memory hash, counter) was identical after every resume. In the second row the first command executed 24-40 ms after the operation completed; this shows the Guest is ready, not that the application inside is. These are small samples on one machine. The `POST` itself accounts for about 5 ms of each figure.

## 6. What is guaranteed

- **A checkpoint is used once.** It is invalidated before the Box is published as `Running`, so it cannot be replayed.
- **A slot is claimed only when it matches.** The Box must be on this node and have a checkpoint, the runtime fingerprint must match, the slot must have at least 5 seconds of lease left, and the claim checks the owner, Pod UID and checkpoint.
- **Failure is reported, not hidden.** If the execution is lost or the restore fails, the Box is marked failed. Cellbox does not start a fresh instance in its place.
- **A request reaches the Box it was meant for.** A request with another Box's identity is rejected and reported as a stale generation.

## 7. Costs and limits

- Resume works only on the node that took the checkpoint. Creating a Box is not faster.
- Each idle slot is a real Pod. One measured slot held about 21-28 MiB, and slots are replaced about every 45 seconds, which keeps the node doing sandbox creation work. See the cost section in [warm pool](warm-pool.md).
- The fast path needs the Cellbox gVisor build and the wrapper settings; without them warm slots do not start.
- Suspend is not optimized. On the measured Box it took about 2.8-3.0 s because the checkpoint has to be written.
- With lazy loading, the first accesses to memory that was not yet loaded are slower than later ones.

## 8. Measuring it yourself

1. Create a Box and a workload that answers on a port, then suspend it.
2. Wait until the warm pool reports `readySlots > 0` for the revision (`GET /v1/template-revisions/{id}/warm-pool`) and a couple of seconds more.
3. Start a timer and send `POST /v1/boxes/{id}:resume`. Poll the operation until it ends and, in parallel, poll your service through `/v1/boxes/{id}/services/{port}/...` until it answers.
4. Repeat many times and report the median and a high percentile together with the conditions above. Run on the node itself to keep network time out of the result.

Use a cheap probe for readiness. A health check that touches all of the Box's memory measures that memory, not the resume.
