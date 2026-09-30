Cellbox
*******

Cellbox is a Go service for creating and controlling isolated coding-agent
workspaces. It exposes a versioned REST API to product clients. The local
Docker provider supports freeze and unfreeze. The Kubernetes provider uses
the imported same-node ResumablePod operator for checkpoint suspend and
resume. Archive capture is a best-effort workspace file copy, not an
application-consistent snapshot.

Runtime and behavior are independent dimensions: ``docker`` or ``k8s``,
and ``normal`` or ``resumable``. Only ``docker-normal`` and
``k8s-resumable`` are implemented; ``docker-resumable`` and ``k8s-normal``
are unsupported. Profile discovery reports all three fields (``runtime``,
``behavior``, ``kind``). Internal provider IDs remain ``docker`` and
``resumable-k8s-pod`` respectively.

Build
-----

Use Go 1.26 or later. ``make build`` writes Linux binaries into
``dist/release/``: ``cellbox``, ``cellbox-guest``, the
ResumablePod controller and runtime adapter. ``make check``
runs Go race tests, vet, and shell syntax checks.

Configuration and local start
-----------------------------

Use ``config/sample.json`` as the configuration template. Replace its zero
image digest with the prepared image ID. Set
client token environment variables named by each ``clients[].tokenEnv``;
provide distinct random tokens of at least 32 bytes. Configure at least one
profile and select an immutable prepared image reference. Docker profiles use
a local ``sha256:<64-hex>`` image ID or ``repository@sha256:<64-hex>``. The
The service defaults to ``127.0.0.1:8090`` and stores state under ``data/``.

After filling the sample configuration and token environment, start the API
with::

    ./dist/release/cellbox --config config/sample.json

Only configured providers are initialized. A Docker-only configuration does
not contact Kubernetes. ``--docker`` selects a Docker CLI binary. Kubernetes
profiles need a node name, namespace, and preloaded prepared image referenced
as ``repository@sha256:<64-hex>``. The API uses in-cluster Kubernetes
credentials by default; ``--kubeconfig`` selects an explicit file when run
outside the cluster. The operator and node runtime require separate
installation. The operator, API, and scoped RBAC are packaged in the
``charts/cellbox`` Helm chart.

The local Docker quickstart is in `REST quickstart <tmp/docs/rest-quickstart.md>`_.
To run the opt-in local Docker integration test, use::

    CELLBOX_DOCKER_SMOKE=1 go test -v ./test/cellbox-smoke

It builds scratch-based test images, uses no host credentials, binds the
private guest port only to IPv4 loopback, and removes its own containers/images.

Source and scope
----------------

The `implementation plan <tmp/docs/implementation-plan.md>`_ describes the work.
The `Kubernetes provider notes <tmp/docs/kubernetes-provider.md>`_ document
requirements and permissions. The public contract is in
`api/openapi.yaml <api/openapi.yaml>`_. CoCell is a separate product; this repository contains
the reusable Cellbox runtime service and its imported operator.

The three boundary documents are `Cellbox duties <tmp/docs/cellbox-boundary.md>`_,
`CoCell duties <tmp/docs/cocell-boundary.md>`_, and
`integration contracts <tmp/docs/contracts.md>`_. Use the Cellbox service gateway
for authenticated product previews.

The Kubernetes provider has fake-client and controller tests but has not been
validated end to end on a live checkpoint-capable cluster. Cross-node restore,
PTY execution, and application-consistent backups are outside this release's
claims.
