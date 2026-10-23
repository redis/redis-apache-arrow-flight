# syntax=docker/dockerfile:1
# The build stage runs on the builder's platform and cross-compiles (the
# server needs no cgo), so a multi-platform build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS build
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
COPY scripts/ scripts/
RUN --mount=type=secret,id=redis_adbc_token \
    REDIS_ADBC_TOKEN_FILE=/run/secrets/redis_adbc_token ./scripts/with-private-modules.sh go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w -X github.com/alberttwong/redis-apache-arrow-flight/server.Version=${VERSION}" \
    -o /out/redis-flight-server ./cmd/redis-flight-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/redis-flight-server /redis-flight-server
ENV FLIGHT_LISTEN=0.0.0.0:8815
EXPOSE 8815
# Community backend monitoring is unavailable; do not install a failing paid probe.
HEALTHCHECK NONE
ENTRYPOINT ["/redis-flight-server"]
