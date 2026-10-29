# Cellbox

Cellbox gives you isolated sandboxes over an HTTP API, and lets you **suspend a running sandbox and resume it in about 200 ms with its process memory intact**.

A sandbox runs in [gVisor](https://gvisor.dev) on Kubernetes. You create it from a versioned template, run time-limited commands in it, read and write its files, proxy HTTP to services inside it, and suspend and resume it. Resume continues the same processes where they stopped; it does not start them again.

> **Status: alpha.** The API is `v1alpha1` and may change. Cellbox currently targets a single Kubernetes node on linux/amd64, and needs a patched gVisor build (see [Requirements](#requirements)). Read [Limitations](#limitations) before relying on it.

## Why resume is fast

Restoring a gVisor checkpoint normally means creating a Pod, setting up its network and sandbox, and loading all memory before the application runs again. Cellbox moves most of that out of the request:

- **Warm pool.** Pods and gVisor sandboxes are prepared ahead of time, based on the checkpoints that exist on the node. A resume claims a ready slot instead of creating a Pod.
- **Deferred checkpoint.** A warm sandbox starts without a checkpoint; the checkpoint and the persistent HOME are bound only when a sandbox claims the slot.
- **Lazy checkpoint load.** After restore, memory pages are read when the application touches them, not in one pass up front.

With a warm-pool hit, a suspended sandbox is running again and serving requests in about **200 ms**. The figure applies to a resume on the same node that finds a ready warm slot. A resume that misses the pool falls back to a normal checkpoint restore, which keeps process memory but takes longer. How the number is measured and where the time goes is described in [Resume on Kubernetes](docs/resume-on-kubernetes.md); the warm pool is explained in [warm pool](docs/warm-pool.md).

## What you can do

- Build **templates** and immutable **revisions** from a base image and a build command, then create sandboxes from a template or a pinned revision.
- Run **commands** with a time limit, read and write **files**, and **proxy HTTP** to ports inside a sandbox.
- **Suspend and resume** a sandbox on the same node, with process memory preserved.
- Keep state in **persistent HOME volumes**, with backups and exports, and move a stopped sandbox's workspace into a new one with **archives** (needs S3-compatible storage).
- Drive everything from the [REST API](api/openapi.yaml) or the [Go SDK](docs/go-sdk.md); trace requests with OpenTelemetry.

The Kubernetes provider supports all of this. A Docker provider offers only create, query, commands, files, service proxying and destroy; everything else returns `UNSUPPORTED_CAPABILITY`.

## How it works

```text
client / Go SDK
      | HTTP
      v
+------------------ cellbox-server (one process, pinned to one node) ------------------+
|  API + operations -> controller + warm pool -> node runtime (CRI, checkpoint, HOME)  |
|        |                    |                                                         |
|     SQLite           Kubernetes API (Cellbox custom resource, Pods, Service)          |
+---------------------------------------------------------------------------------------+
      |                                      |
      v                                      v
 BuildKit (image builds)        containerd -> gVisor (runsc) -> guest agent -> your program
```

`cellbox-server` serves the API, reconciles Kubernetes objects and drives the node runtime in one process. Template, revision and operation records live in a local SQLite database; each sandbox's desired and observed state lives in a Cellbox custom resource that owns its Pod and Service. See [concepts](docs/concepts.md) for the vocabulary and [architecture](docs/architecture.md) for the full request flows.

## Requirements

- A Kubernetes node on **linux/amd64** with containerd (tested: k3s v1.36, containerd 2.3, Ubuntu 24.04), and root access to it.
- A **Cellbox gVisor build**. Upstream gVisor does not yet have the options the fast path uses, so install the build published at [`jingyugao/gvisor`](https://github.com/jingyugao/gvisor/releases/tag/cellbox-gvisor-20261029). The [node installation guide](docs/node-installation.md) covers it.
- BuildKit, if you build templates from a base image (the Helm chart can deploy it).
- S3-compatible object storage, only if you use archives.

## Getting started

1. **Prepare the node.** Install the gVisor build, the Cellbox runsc wrapper and a containerd handler: [Node installation](docs/node-installation.md).
2. **Deploy.** Build the server image, configure `values.yaml` and install the Helm chart: [Deployment](docs/deployment.md).
3. **Try it.** Create a template, build a revision, create a sandbox, run a command, then suspend and resume it: [REST quick start](docs/rest-quickstart.md) or [Go SDK guide](docs/go-sdk.md).

No cluster? The [local quick start](docs/local-quickstart.md) runs Cellbox on one Linux host with the `local-runsc` provider. It has no warm pool, so the resume figure above does not apply to it.

## Documentation

| Guide | Contents |
| --- | --- |
| [Concepts](docs/concepts.md) | Terms used everywhere: Box, template, revision, generation, checkpoint, warm pool |
| [Node installation](docs/node-installation.md) | gVisor build, wrapper, containerd handler |
| [Local quick start](docs/local-quickstart.md) | Run on one Linux host without Kubernetes |
| [Deployment](docs/deployment.md) | Image, Helm values, upgrade |
| [Usage](docs/cellbox-usage.md) | Templates, revisions and the sandbox lifecycle |
| [REST quick start](docs/rest-quickstart.md) | `curl` examples |
| [Go SDK](docs/go-sdk.md) | Client usage and examples |
| [Resume on Kubernetes](docs/resume-on-kubernetes.md) | What the 200 ms figure means, how it is achieved, measurements and limits |
| [Warm pool](docs/warm-pool.md) | How warm slots are sized, claimed and reclaimed |
| [Architecture](docs/architecture.md) | Components and the create and resume flows |
| [End-to-end validation](docs/e2e-validation.md) | The acceptance suite and what it proves |
| [OpenAPI](api/openapi.yaml) | The full HTTP contract |

## Limitations

- **Single node.** `cellbox-server` is pinned to one node. A suspended sandbox resumes only on the node that took its checkpoint; there is no cross-node migration and no scheduler across nodes.
- **No built-in authentication.** The API does not authenticate callers, and the `X-Cellbox-Client-ID` header only selects a data namespace. Put an authenticating gateway in front of it.
- **Privileged server.** The server Pod runs privileged with `hostPID` and host mounts. Run it only on a node you trust, and do not treat it as an unprivileged API deployment.
- **Alpha API and data.** `v1alpha1` can change. Upgrading across the API-group rename (`cellbox.local` to `cellbox.jingyugao.github.io`) does not migrate existing sandboxes.
- **linux/amd64 only**, and one gVisor build.

## Build and test

Building needs Go 1.26 or later.

```sh
make check    # unit tests (with -race), go vet, gofmt check, script syntax
make build    # binaries, chart, docs and SDK source into dist/release
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
