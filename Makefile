VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/alberttwong/redis-apache-arrow-flight/server.Version=$(VERSION)
REDIS_PORT ?= 6379
REDIS_URI ?= redis://localhost:$(REDIS_PORT)/0

.PHONY: build test test-integration run redis-up redis-down docker example clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/redis-flight-server ./cmd/redis-flight-server

# Unit tests; the integration tests skip themselves without REDIS_URI.
test:
	go test ./...

# Integration tests against the compose Redis (started if needed).
test-integration: redis-up
	REDIS_URI=$(REDIS_URI) go test -count=1 -race ./...

run: build
	REDIS_URI=$(REDIS_URI) ./bin/redis-flight-server

redis-up:
	REDIS_PORT=$(REDIS_PORT) docker compose up --detach --wait redis

redis-down:
	docker compose down

docker:
	docker build --build-arg VERSION=$(VERSION) -t redis-flight-server:$(VERSION) .

example:
	uv run examples/python/quickstart.py

clean:
	rm -rf bin
