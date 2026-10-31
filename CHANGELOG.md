# Changelog

All notable changes are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Cellbox uses [semantic versioning](https://semver.org/) once it leaves alpha. Until then, minor versions may contain breaking changes; they are called out below.

## [Unreleased]

### Breaking

- The Go module is now `github.com/jingyugao/cell-box`, and the Kubernetes API group, labels and annotations changed from `cellbox.local` to `cellbox.jingyugao.github.io`. A server of this version does not see objects stored under the old group and needs a fresh data directory; existing Boxes are not migrated. The old CRD is kept by Helm (`helm.sh/resource-policy: keep`) but is no longer used.
- The default server image is `ghcr.io/jingyugao/cellbox-server`.
- Nodes need the Cellbox gVisor build (`cellbox-gvisor-20261030` or later); upstream gVisor releases are not enough. The node's wrapper script must set `CELLBOX_NO_ROOT_CONTAINER=true` and `CELLBOX_DEFER_CHECKPOINT=true` for the warm resume path. See [node installation](docs/node-installation.md).
- Boxes on Kubernetes and local-runsc profiles now carry an instance identity. The server sends it with every request to the Guest, and the Guest rejects requests meant for another Box. **Revisions built before this change contain a Guest that cannot confirm the identity, so creating a Box from them fails, either with "Guest cannot confirm Box identity" or, if the Guest rejects the identity, by timing out after 180 s; rebuild those templates.** Existing Boxes are not affected.

### Changed

- Requests to a running Box are routed with an in-process cache of the committed execution's address instead of reading Kubernetes on every request. The identity check above makes a stale entry harmless.
- Restores that bind a checkpoint now load pages lazily by default (`--background-load=lazy`). Set `CELLBOX_BACKGROUND_LOAD=eager` in the wrapper script to restore the previous behavior.
- Resume completes when the process listens on its ports, and health is monitored independently.
- A warm resume waits for the disk only once, for the checkpoint-consumed marker. Resume records are kept in memory with an unsynced log (`runtime-journal.log` in the data directory) and written to SQLite in the background. They survive a process crash, and power loss can drop the newest ones. The consumed marker then refuses the replay, and local state is synced before it is published to Kubernetes.
- Exec operations, runtime observations and checkpoint inventory publication no longer wait for the disk. Each exec previously synced the database three times, about 11 ms on the measured disk. Power loss can drop the newest of these records; see [Resume on Kubernetes](docs/resume-on-kubernetes.md).
- Warm pool demand is derived from the checkpoints that exist on the node. `warmSlots` and `defaultWarmSlots` are kept for compatibility and no longer create demand.
- `make test`, `vet` and `fmt` are scoped to the source directories; `make check` also verifies formatting.

### Fixed

- A resume that misses the warm pool no longer crashes the sandbox and fails the Box. This needs `cellbox-gvisor-20261030`. Suspended Boxes and Boxes running on `cellbox-gvisor-20261029` cannot restore their process memory on the new build; recreate them. HOME volumes are not affected.
- Suspending a Box right after it resumed no longer fails the Box. The image check before a checkpoint now reads the container from CRI instead of the Pod status, which kubelet can publish after the guest is already serving. A container whose start call has not returned is retried.
- The Box instance identity is no longer part of the Guest command line. It is stored in a Custom Resource annotation and bound to the Guest once through `POST /v1/identity`. Warm slots built from one Box can again be claimed by any Box of the same revision. Boxes created by the previous release keep working but should be recreated to share warm slots.

- Warm resume failed with `"Args" does not match across checkpoint restore` when a Box claimed a warm slot that had been built from another Box of the same revision, which is the normal case once a revision has more than one Box. The instance identity added to the Guest arguments made them differ. The wrapper now gives the pre-created container the claiming Box's command line, carried in the ticket, and refuses a slot whose arguments differ in anything but the instance marker.

### Documentation

- Documentation is in English, with a new [concepts](docs/concepts.md) page.
