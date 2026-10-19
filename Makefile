VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

.PHONY: build test lint fmt clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o strata-diskcache ./cmd/strata-diskcache

test:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "run: make fmt"; exit 1; }
	go vet ./...
	go test -race -count=1 ./...

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	gofmt -w .

clean:
	rm -rf strata-diskcache dist
