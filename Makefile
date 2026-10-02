VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/alberttwong/redis-apache-arrow-flight/server.Version=$(VERSION)
REDIS_PORT ?= 6379
REDIS_URI ?= redis://localhost:$(REDIS_PORT)/0

JDBC_VERSION ?= 19.0.0
JDBC_JAR := bin/flight-sql-jdbc-driver-$(JDBC_VERSION).jar
JDBC_BASE := https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/$(JDBC_VERSION)
JDBC_URL ?= jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
JAVA ?= java

.PHONY: build test test-integration test-cluster run redis-up redis-cluster-up redis-down docker example jdbc-smoke clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/redis-flight-server ./cmd/redis-flight-server

# Unit tests; the integration tests skip themselves without REDIS_URI.
test:
	go test ./...

# Integration tests against the compose Redis (started if needed).
test-integration: redis-up
	REDIS_URI=$(REDIS_URI) go test -count=1 -race ./...

# Integration tests against a 3-shard Redis Cluster (OSS Cluster API).
test-cluster: redis-cluster-up
	REDIS_URI=redis://localhost:7001/0 go test -count=1 -race ./...

run: build
	REDIS_URI=$(REDIS_URI) ./bin/redis-flight-server

redis-up:
	REDIS_PORT=$(REDIS_PORT) docker compose up --detach --wait redis

redis-cluster-up:
	docker compose --profile cluster up --detach --wait redis-cluster

redis-down:
	docker compose --profile cluster down

docker:
	docker build --build-arg VERSION=$(VERSION) -t redis-flight-server:$(VERSION) .

example:
	uv run examples/python/quickstart.py

# The Arrow Flight SQL JDBC driver, checked against Maven Central's SHA-1.
$(JDBC_JAR):
	mkdir -p bin
	curl -fsSL -o $@ $(JDBC_BASE)/flight-sql-jdbc-driver-$(JDBC_VERSION).jar
	echo "$$(curl -fsSL $(JDBC_BASE)/flight-sql-jdbc-driver-$(JDBC_VERSION).jar.sha1 | cut -c1-40)  $@" | shasum -a 1 -c - || (rm -f $@; exit 1)

# JDBC smoke test against a running server (make run, in another terminal).
jdbc-smoke: $(JDBC_JAR)
	$(JAVA) --add-opens=java.base/java.nio=ALL-UNNAMED -cp $(JDBC_JAR) examples/jdbc/JdbcSmoke.java "$(JDBC_URL)"

clean:
	rm -rf bin
