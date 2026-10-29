# End-to-end validation

The acceptance entry point is [`test/scripts/e2e-full.sh`](../test/scripts/e2e-full.sh). It builds templates and operates Boxes through the public REST API, and checks the real Pods, Services and warm-pool claims with read-only Kubernetes queries. It can also run the Go SDK lifecycle example. It does not create diagnostic Pods.

## Environment and deployment boundaries

- Use a **dedicated test namespace** with its own Helm release. The script refuses the namespaces `cell-box`, `co-cell`, `default` and `kube-system`. The chart's default namespace name is `cell-box`, so pick another one for testing.
- Deploy a fresh `cellbox-server` pinned to one node, with BuildKit enabled, S3-compatible object storage configured for the archive cases, and a warm-pool budget of at most two slots. The service data directory must be on persistent storage.
- The namespace must contain no Boxes before the run. With `--restart-server` the script restarts the server Deployment you name; before doing so it checks again that every Box belongs to this test run.
- Do not mix test data with an existing deployment. A new server version uses a new data directory; templates and records from an older layout are not migrated.
- Components shared with other workloads on the node (the cluster-wide CRD, RuntimeClass, the installed gVisor and anything outside the test namespace) are not upgraded by a test run. Compare the UIDs, images and state of those workloads before and after.

## What the suite checks

| Case | Key assertions and evidence |
| --- | --- |
| Server shape and capabilities | A single `cellbox-server` runs; the profile advertises same-node checkpoints and `stopped-workspace` archives |
| Removed APIs | The old image, upgrade, rebuild, ensure-running, reconcile, lease and routing endpoints return 404 |
| Template build | `buildCommand` installs an executable tool; images resolve to digests; the revision and build log are readable; the revision inherits the template's default warm settings |
| No idempotency handling | Two creates with the same `Idempotency-Key` produce different Boxes and operations; two execs with it run their side effects twice |
| Basic use | The custom tool runs; file write, read, list, range download and HTTP service proxy work |
| Request boundaries | Reading another client's templates, revisions, Boxes, operations, execs or archives returns 404; path traversal is rejected; archiving a running Box is rejected |
| Suspend and warm resume | Files are not served from a suspended Box and a file request does not resume it; after resume the Pod UID belongs to a warm candidate that existed before the request and keeps its warm label; the Service UID and IP do not change |
| Process continuity | Before and after the checkpoint, the counter, PID, start time, random instance value, a 32 MiB memory SHA-256 and a workspace marker are identical |
| Standard resume | With the revision's warm demand set to 0, another suspend and resume does not claim a warm Pod, and process state is still continuous |
| Generation | An exec with the pre-resume generation returns `STALE_GENERATION` |
| Server restart | A long command with one file side effect and a long build are running when the test server is rolled; both end as `UNKNOWN_OUTCOME`, the exec status is `unknown`, nothing is replayed and no new revision appears; the business Pod and the counter memory stay as they were |
| SQLite persistence | After the server Pod UID changes, templates, the stable pointer, revisions, operations, exec status and warm demand are still readable |
| Multiple versions | The same base image with a changed tool version yields `1.01-r1` and `1.01-r2`; stable only affects new Boxes; an explicit revision pins the old version; different images cannot share a pool |
| Runtime statistics | The template runtime endpoint reports the real Box counts and fresh observations for both revisions |
| Build cancellation | An explicit cancel moves the build to `cancelled`, the operation list shows it, and no revision is produced |
| Archive | The source Box stays suspended; the downloaded archive's size and SHA-256 match the metadata; the workspace proof file in the tar is byte-identical |
| Restore and version migration | Restoring across images is rejected by default; restoring with the same image creates a new `staged` Box that does not start the business program; after an explicit activate the workspace is kept and the program starts again; an explicit `acceptImageChange` migrates to the new tool image |
| Go SDK | A real create, files, suspend, resume, handle update, exec and destroy run end to end |
| Cleanup | The run's Boxes and archives are deleted; revisions are retired and all warm demand withdrawn; build metadata and registry images that cannot be deleted are listed in the report |

Pools with the same runtime fingerprint are covered by controller unit tests. The end-to-end run does not assume that images with different digests are compatible, and it does not treat `readySlots > 0` alone as evidence of a warm hit. Warm and standard resume timings recorded by the run are functional-acceptance samples, not statistics.

The custom tool is run by absolute path (`/usr/local/bin/cellbox-e2e-tool`); the exec endpoint's default `PATH` is `/usr/bin:/bin`, and the test does not rely on the image's `PATH`.

`UNKNOWN_OUTCOME` does not mean the command inside the guest has stopped. The restart case uses a command with an explicit timeout and waits out that boundary before suspending for the archive step, so a `BUSY` result from a still-running command is not mistaken for a suspend failure.

## Build and run

You need `go`, `uv`, `kubectl`, `helm`, a working Docker, and an image registry reachable from the node and from BuildKit. The test source image contains the `/server` counter and Alpine's shell; an old scratch-based counter image will not work.

```bash
mkdir -p tmp/e2e-counter-image
CGO_ENABLED=0 GOOS=linux go build -o tmp/e2e-counter-image/server ./test/counter
cp test/e2e/Dockerfile tmp/e2e-counter-image/Dockerfile
docker build -t <registry>/cellbox-e2e-counter:acceptance tmp/e2e-counter-image
docker push <registry>/cellbox-e2e-counter:acceptance
```

Put the immutable digest that `docker push` prints into `--source-image` below. Deploy as described in the [deployment guide](deployment.md): run `go test -race`, `go vet` and chart validation first, publish and verify the server image digest, then run Helm upgrade.

```bash
test/scripts/e2e-full.sh \
  --context <kube-context> --namespace <test-namespace> --profile-id <profile-id> \
  --source-image <registry>/cellbox-e2e-counter@sha256:<digest> \
  --restart-server --sdk \
  --evidence-dir tmp/validation/full-e2e
```

The script manages a port-forward bound to `127.0.0.1` and reconnects after the restart. Talking to the server directly needs no token; behind an authenticating gateway set `CELLBOX_CLIENT_TOKEN`. Each run uses its own client namespace and unique template names, and every request with side effects is submitted once.

Without `--restart-server` or `--sdk` the matching part is skipped. The report marks skipped items, and such a run does not replace a full acceptance. Do not enable the restart test on a production service.

Docker has its own, smaller check:

```bash
CELLBOX_DOCKER_SMOKE=1 go test -count=1 ./test/cellbox-smoke
```

It covers create, exec, files, the HTTP proxy and destroy, and checks that unsupported complex requests return `UNSUPPORTED_CAPABILITY`. Service-layer and provider unit tests cover the other rejection branches.

## Evidence and pass criteria

`report.json` records every checkpoint, all accepted resource IDs, timings, errors, cleanup results and the build metadata that was kept. The evidence directory also holds the revisions, build logs, terminal operations, the CR, Pod and Service before and after resume, the warm candidate UIDs, the server Pod before and after the restart, and the SDK log. On failure the script keeps `failure.txt` and the runtime evidence, then cleans up.

A full acceptance requires exit code 0, `report.status == "passed"`, no skipped items and no `cleanupErrors`, plus confirmation that the UIDs and images of workloads outside the test namespace did not change during deployment. Fix a failure and rerun the whole suite; keep the failed report, and never record a missing result as a pass.

Successfully built revisions cannot be deleted, so the test retires them, clears the stable pointer and sets `warmSlots` to 0. Template, revision and operation metadata and the registry build images remain; the report lists them, and they are not counted as "everything deleted".
