SHELL := /bin/bash
.SHELLFLAGS := -eo pipefail -c
.ONESHELL:

OUT ?= dist/release
LOCAL_ARCH ?= $(shell go env GOARCH)
LOCAL_PACKAGE := dist/cellbox-local-runsc-linux-$(LOCAL_ARCH).tar.gz

.PHONY: build check test vet fmt package local-package

build:
	@set -u
	out=$$(realpath -m -- "$(OUT)")
	version=$$(cat VERSION)
	[[ $$version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][a-zA-Z0-9.-]+)?$$ ]] || { echo 'Invalid VERSION'; exit 1; }
	revision=$$(git rev-parse --short=12 HEAD 2>/dev/null || printf uncommitted)
	module=$$(go list -m)
	if [[ -n $$(git status --porcelain) ]]; then revision=$$revision-dirty; fi
	mkdir -p "$$out"
	for obsolete in cellbox-runtime-config box-service cellbox cellbox-guest cellbox-image cellbox-gateway resumablepod-controller resumablepod-runtime resumablepod-gateway server Dockerfile GatewayDockerfile README.rst cellbox-api cellbox-node-controller; do
		if [[ -f "$$out/$$obsolete" ]]; then rm -- "$$out/$$obsolete"; fi
	done
	ldflags="-s -w -X $$module/internal/version.Version=$$version -X $$module/internal/version.Revision=$$revision"
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$out/cellbox-server" ./cmd/cellbox-server
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o "$$out/cellbox-container-agent" ./cmd/cellbox-container-agent
	CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$out/cellbox-runsc-wrapper" ./cmd/cellbox-runsc-wrapper
	cp -r charts examples images "$$out/"
	rm -f -- "$$out/charts/cellbox/values_local.yaml" "$$out/charts/cellbox/values-k3s.yaml" "$$out/images/controller/install-k8s-node.sh"
	rm -f -- "$$out/images/cellbox-api.Dockerfile" "$$out/images/controller.Dockerfile"
	rm -f -- "$$out/charts/cellbox/templates/controller.yaml" "$$out/charts/cellbox/templates/controller-access.yaml"
	rm -f -- "$$out/charts/cellbox/templates/gateway.yaml" "$$out/charts/cellbox/templates/gateway-access.yaml"
	rm -f -- "$$out/images/controller/install-k3s-node.sh" "$$out/images/controller/runtime.toml" "$$out/images/controller/install-node.sh" "$$out/images/controller/install-gvisor.sh" "$$out/images/controller/runsc.toml"
	rm -f -- "$$out/test/scripts/test-node-install.sh" "$$out/test/scripts/e2e-bootstrap.sh"
	mkdir -p "$$out/test" "$$out/config" "$$out/api"
	cp -r test/scripts "$$out/test/"
	cp -r test/fixtures "$$out/test/"
	mkdir -p "$$out/test/e2e"
	cp test/e2e/full.py test/e2e/Dockerfile "$$out/test/e2e/"
	cp config/sample.json config/local-runsc.sample.json "$$out/config/"
	cp api/openapi.yaml "$$out/api/"
	cp LICENSE VERSION README.md "$$out/"
	if [[ -d doc ]]; then cp -r doc "$$out/"; fi
	mkdir -p "$$out/sdk"
	cp -r sdk/go "$$out/sdk/"
	# Remove Markdown left by older builds; never publish local scratch notes.
	docs_out="$$out/tmp/docs"
	[[ ! -L $$docs_out ]] || { echo "Refusing to replace symlinked documentation directory: $$docs_out" >&2; exit 1; }
	for old_doc in "$$docs_out"/*.md; do if [[ -f $$old_doc ]]; then rm -- "$$old_doc"; fi; done
	if [[ -d $$docs_out ]]; then rmdir --ignore-fail-on-non-empty "$$docs_out"; fi
	if [[ -d $$out/tmp ]]; then rmdir --ignore-fail-on-non-empty "$$out/tmp"; fi
	printf 'Release directory: %s\n' "$$out"

test:
	go test -race ./...
	cd sdk/go && go test -race ./...

vet:
	go vet ./...
	cd sdk/go && go vet ./...

check: test vet
	@for script in test/scripts/*.sh; do bash -n "$$script"; done

fmt:
	gofmt -w api cmd internal sdk/go test

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
	(cd "$$staging/$$name" && sha256sum cellbox-server cellbox-container-agent cellbox-runsc-wrapper > SHA256SUMS)
	tar --sort=name --mtime="@$$epoch" --owner=0 --group=0 --numeric-owner -C "$$staging" -cf - "$$name" | gzip -n > "dist/$$name.tar.gz"
	(cd dist && sha256sum "$$name.tar.gz" > "$$name.tar.gz.sha256")
	printf 'Package: dist/%s.tar.gz\n' "$$name"

local-package:
	@set -u
	version=$$(cat VERSION)
	[[ $$version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][a-zA-Z0-9.-]+)?$$ ]] || { echo 'Invalid VERSION'; exit 1; }
	revision=$$(git rev-parse --short=12 HEAD 2>/dev/null || printf uncommitted)
	module=$$(go list -m)
	if [[ -n $$(git status --porcelain) ]]; then revision=$$revision-dirty; fi
	ldflags="-s -w -X $$module/internal/version.Version=$$version -X $$module/internal/version.Revision=$$revision"
	staging=$$(mktemp -d)
	trap 'rm -rf -- "$$staging"' EXIT
	bundle="$$staging/cellbox-local-runsc"
	mkdir -p "$$bundle"
	CGO_ENABLED=0 GOOS=linux GOARCH="$(LOCAL_ARCH)" go build -trimpath -buildvcs=false -ldflags "$$ldflags" -o "$$bundle/cellbox-server" ./cmd/cellbox-server
	CGO_ENABLED=0 GOOS=linux GOARCH="$(LOCAL_ARCH)" go build -trimpath -buildvcs=false -o "$$bundle/cellbox-container-agent" ./cmd/cellbox-container-agent
	cp scripts/install-local-runsc.sh "$$bundle/install-local-runsc.sh"
	cp config/local-runsc.sample.json "$$bundle/local-runsc.sample.json"
	chmod 0755 "$$bundle/install-local-runsc.sh" "$$bundle/cellbox-server" "$$bundle/cellbox-container-agent"
	(cd "$$bundle" && sha256sum cellbox-server cellbox-container-agent install-local-runsc.sh local-runsc.sample.json > SHA256SUMS)
	epoch=$${SOURCE_DATE_EPOCH:-$$(git log -1 --format=%ct)}
	mkdir -p dist
	tar --sort=name --mtime="@$$epoch" --owner=0 --group=0 --numeric-owner -C "$$staging" -cf - cellbox-local-runsc | gzip -n > "$(LOCAL_PACKAGE)"
	printf 'Package: %s\n' "$(LOCAL_PACKAGE)"
