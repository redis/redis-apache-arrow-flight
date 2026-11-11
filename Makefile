VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/redis/redis-apache-arrow-flight/server.Version=$(VERSION)
REDIS_PORT ?= 6379
FLIGHT_URI ?= grpc://localhost:8815
RSCRIPT ?= Rscript
FLIGHT_SQL_DRIVER ?=

JDBC_VERSION ?= 19.0.0
JDBC_JAR := bin/flight-sql-jdbc-driver-$(JDBC_VERSION).jar
JDBC_BASE := https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/$(JDBC_VERSION)
JDBC_URL ?= jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
JAVA ?= java
# Arrow needs java.nio opened. Java 23 and later also need sun.misc.Unsafe's
# memory access allowed (Netty's allocator uses it); Java 17 to 22 reject that
# option. Evaluated only by jdbc-smoke.
JAVA_MAJOR = $(shell $(JAVA) -XshowSettings:properties -version 2>&1 | awk '$$1 == "java.specification.version" {print $$3}')
JAVA_OPTS = --add-opens=java.base/java.nio=ALL-UNNAMED $(if $(shell [ "$(JAVA_MAJOR)" -ge 23 ] 2>/dev/null && echo y),--sun-misc-unsafe-memory-access=allow)

# Platforms of the release archives (GOOS/GOARCH).
PLATFORMS ?= linux/amd64 linux/arm64 darwin/arm64

.PHONY: build dist test test-integration test-cluster run redis-up redis-cluster-up redis-down docker example clients-smoke r-smoke jdbc-smoke clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/redis-flight-server ./cmd/redis-flight-server

# Release archives in dist/: for each platform, a static binary and the
# README in redis-flight-server-$(VERSION)-<os>-<arch>.tar.gz, and its
# SHA-256 checksum.
dist:
	rm -rf dist
	mkdir -p dist
	set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; name=redis-flight-server-$(VERSION)-$$os-$$arch; \
		mkdir -p dist/$$name; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" \
			-o dist/$$name/redis-flight-server ./cmd/redis-flight-server; \
		cp README.md dist/$$name/; \
		tar -C dist -czf dist/$$name.tar.gz $$name; \
		rm -r dist/$$name; \
		(cd dist && shasum -a 256 $$name.tar.gz > $$name.tar.gz.sha256); \
	done

# Unit tests; the integration tests skip themselves without REDIS_URI.
test:
	env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test ./...

# Supply REDIS_URI in the environment; never echo credentials in recipes.
# Paid-feature tests also need their qualified enterprise composition (see docs/build-profiles.md).
export REDIS_URI
test-integration:
	@test -n "$$REDIS_URI" || (echo 'Set REDIS_URI to a Redis Enterprise TLS endpoint with username and password'; exit 1)
	@REDIS_INTEGRATION_REQUIRED=1 python3 scripts/run-integration.py -- go test -count=1 -p 1 -race ./...

# Compatibility alias: all integration tests now use Redis Enterprise.
test-cluster: test-integration

run: build
	@REDIS_URI="$${REDIS_URI:-redis://localhost:$(REDIS_PORT)/0}" ./bin/redis-flight-server

redis-up:
	REDIS_PORT=$(REDIS_PORT) docker compose up --detach --wait redis

redis-cluster-up:
	docker compose --profile cluster up --detach --wait redis-cluster

redis-down:
	docker compose --profile cluster down

docker:
	docker build --build-arg VERSION=$(VERSION) -t redis-flight-server:$(VERSION) .

example:
	uv run --with-requirements examples/python/requirements-smoke.txt examples/python/quickstart.py
	uv run examples/python/dataset_to_sql.py

clients-smoke:
	uv run --with-requirements examples/python/requirements-clients.txt python examples/python/client_compatibility.py --uri "$(FLIGHT_URI)"

r-smoke:
	@test -n "$(FLIGHT_SQL_DRIVER)" || (echo 'Set FLIGHT_SQL_DRIVER to the Flight SQL shared library path'; exit 1)
	$(RSCRIPT) examples/r/flight_sql.R "$(FLIGHT_URI)" "$(FLIGHT_SQL_DRIVER)"

# The Arrow Flight SQL JDBC driver, checked against Maven Central's SHA-1.
$(JDBC_JAR):
	mkdir -p bin
	curl -fsSL -o $@ $(JDBC_BASE)/flight-sql-jdbc-driver-$(JDBC_VERSION).jar
	echo "$$(curl -fsSL $(JDBC_BASE)/flight-sql-jdbc-driver-$(JDBC_VERSION).jar.sha1 | cut -c1-40)  $@" | shasum -a 1 -c - || (rm -f $@; exit 1)

# JDBC smoke test against a running server (make run, in another terminal).
jdbc-smoke: $(JDBC_JAR)
	$(JAVA) $(JAVA_OPTS) -cp $(JDBC_JAR) examples/jdbc/JdbcSmoke.java "$(JDBC_URL)"

clean:
	rm -rf bin dist

# Isolated local TLS/ACL Enterprise cluster and qualified enterprise composition.
.PHONY: enterprise-env-up enterprise-env-test enterprise-env-down
enterprise-env-up:
	python3 scripts/enterprise-env.py up

enterprise-env-test:
	python3 scripts/enterprise-env.py test

enterprise-env-down:
	python3 scripts/enterprise-env.py down

# All 27 inventoried Flight SQL routes; see docs/integrations/testing.md.
INTEGRATION_RESULTS ?= /tmp/redis-flight-integrations-$(shell date -u +%Y%m%dT%H%M%SZ)
INTEGRATION_ARGS ?= --setup --uri "$(FLIGHT_URI)" --results "$(INTEGRATION_RESULTS)"
.PHONY: integrations-test
integrations-test:
	python3 scripts/test-integrations.py $(INTEGRATION_ARGS)

# Requires a TLS/authenticated gateway backed by disposable Redis Enterprise; never native ADBC.
.PHONY: test-shared-snapshot
test-shared-snapshot:
	python3 scripts/run-integration.py -- python3 -m pytest -v tests/integrations/test_shared_snapshot.py
