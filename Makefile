.PHONY: build check test vet fmt package

build:
	bash hack/build.sh

test:
	go test -race ./...

vet:
	go vet ./...

check: test vet
	@for script in hack/*.sh; do bash -n "$$script" || exit 1; done

fmt:
	gofmt -w api cmd internal pkg test

package:
	bash hack/package.sh
