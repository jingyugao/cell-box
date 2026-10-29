# Contributing to Cellbox

Thanks for helping. This page covers how to build and test, what a good change looks like, and how changes are reviewed.

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), as described in section 5 of that license. There is no separate CLA or sign-off requirement.

## Before you start

- For anything larger than a small fix, open an issue first so we can agree on the approach.
- Report security problems privately; see [SECURITY.md](SECURITY.md).
- Read [concepts](docs/concepts.md) for the vocabulary and [architecture](docs/architecture.md) for how the pieces fit.

## Build and test

You need Go 1.26 or later.

```sh
make check    # unit tests with -race, go vet, gofmt check, shell syntax check
make build    # binaries, chart, docs and SDK source into dist/release
make fmt      # gofmt the Go sources
```

`make check` needs no cluster and no Docker. The end-to-end suite does: it needs a Kubernetes node prepared as in [node installation](docs/node-installation.md), a registry, `kubectl`, `helm`, `uv` and Docker. See [end-to-end validation](docs/e2e-validation.md). Run it for changes to the controller, the node runtime, the wrapper or the guest agent, and say in the pull request whether you did.

The Go SDK in `sdk/go` is a separate module with its own tests; `make check` covers it.

## Making a change

1. Branch from the latest `master`: `git switch -c <type>/<short-name> origin/master`.
2. Keep the change focused. Unrelated cleanups go in their own pull request.
3. Add tests that would fail without your change. Prefer a few tests that guard real behavior (lifecycle rules, identity checks, failure handling) over many that restate the code.
4. Update the documentation when behavior changes. Docs are in English, under `docs/`; keep terms consistent with [concepts](docs/concepts.md). If you change the HTTP API, change `api/openapi.yaml` too; the contract test checks it against the code.
5. Do not commit build output (`dist/`), scratch files (`tmp/`), local values files or credentials. Do not put secrets, tokens or private registry addresses in code, docs or examples.

### Commit messages and pull requests

Use a short prefix that says what kind of change it is, then an imperative summary:

```text
feat: load checkpoint pages lazily on restore by default
fix: reclaim warm child tasks and isolate cleanup failures
docs: describe the checkpoint-driven warm pool
test: scope warm fixture lookup to its Box owner
chore: trim the build recipe
perf: ...
```

Use the body to explain why. Pull requests are squash-merged, so the pull request title becomes the commit subject on `master`.

In the pull request description say what changed, why, how you verified it, and what you did not verify.

## Code style

- Format with `gofmt` (`make fmt`). `make check` fails on unformatted files.
- Match the surrounding code: naming, error handling and comment density. Comments explain why, not what.
- Errors returned to API callers must not leak internal addresses, paths or credentials.
- Anything that touches the node, the wrapper or the guest agent runs with high privilege. Check inputs, keep paths inside their base directory, and fail closed.

## Reviews

A maintainer reviews every pull request. Expect questions about failure cases and compatibility: what happens if the server restarts halfway, if a request is repeated, or if an older guest agent or gVisor build is in use. Changes to the API or to stored state should say how existing deployments upgrade.
