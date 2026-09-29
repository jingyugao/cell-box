SHELL := /bin/bash
.SHELLFLAGS := -eo pipefail -c
.ONESHELL:

OUT ?= dist/release

.PHONY: build check test vet fmt package

build:
	@set -u
	out=$$(realpath -m -- "$(OUT)")
	version=$$(cat VERSION)
	[[ $$version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][a-zA-Z0-9.-]+)?$$ ]] || { echo 'Invalid VERSION'; exit 1; }
	revision=$$(git rev-parse --short=12 HEAD 2>/dev/null || printf uncommitted)
	module=$$(go list -m)
	if [[ -n $$(git status --porcelain) ]]; then revision=$$revision-dirty; fi
	mkdir -p "$$out"
	if [[ -f "$$out/cellbox-image" ]]; then rm -- "$$out/cellbox-image"; fi
	ldflags="-s -w -X $$module/internal/version.Version=$$version -X $$module/internal/version.Revision=$$revision"
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$out/cellbox" ./cmd/cellbox
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$$out/cellbox-guest" ./cmd/cellbox-guest
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$out/resumablepod-controller" ./cmd/resumablepod-controller
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$out/resumablepod-runtime" ./cmd/resumablepod-runtime
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$$out/server" ./test/counter
	cp test/counter/Dockerfile "$$out/"
	cp -r charts examples images "$$out/"
	mkdir -p "$$out/test" "$$out/config" "$$out/api"
	cp -r test/scripts "$$out/test/"
	cp config/sample.json "$$out/config/"
	cp api/openapi.yaml "$$out/api/"
	cp LICENSE VERSION README.rst "$$out/"
	docs_out="$$out/tmp/docs"
	[[ ! -L $$docs_out ]] || { echo "Refusing to replace symlinked documentation directory: $$docs_out" >&2; exit 1; }
	mkdir -p "$$docs_out"
	for old_doc in "$$docs_out"/*.md; do if [[ -f $$old_doc ]]; then rm -- "$$old_doc"; fi; done
	if [[ -d tmp/docs ]]; then cp -r tmp/docs/. "$$docs_out/"; fi
	printf 'Release directory: %s\n' "$$out"

test:
	go test -race ./...

vet:
	go vet ./...

check: test vet
	@for script in test/scripts/*.sh; do bash -n "$$script"; done

fmt:
	gofmt -w api cmd internal pkg test

package:
	@set -u
	[[ $$(git rev-parse --is-inside-work-tree) == true ]]
	[[ -z $$(git status --porcelain) ]] || { echo 'Commit source changes before packaging'; exit 1; }
	version=$$(cat VERSION)
	arch=$$(go env GOARCH)
	name=cellbox-v$$version-linux-$$arch
	staging=$$(mktemp -d)
	trap 'rm -rf -- "$$staging"' EXIT
	$(MAKE) build OUT="$$staging/$$name"
	epoch=$${SOURCE_DATE_EPOCH:-$$(git log -1 --format=%ct)}
	mkdir -p dist
	(cd "$$staging/$$name" && sha256sum cellbox cellbox-guest resumablepod-controller resumablepod-runtime server > SHA256SUMS)
	tar --sort=name --mtime="@$$epoch" --owner=0 --group=0 --numeric-owner -C "$$staging" -cf - "$$name" | gzip -n > "dist/$$name.tar.gz"
	(cd dist && sha256sum "$$name.tar.gz" > "$$name.tar.gz.sha256")
	printf 'Package: dist/%s.tar.gz\n' "$$name"
