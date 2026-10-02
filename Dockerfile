# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/alberttwong/redis-apache-arrow-flight/server.Version=${VERSION}" \
    -o /out/redis-flight-server ./cmd/redis-flight-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/redis-flight-server /redis-flight-server
ENV FLIGHT_LISTEN=0.0.0.0:8815
EXPOSE 8815
ENTRYPOINT ["/redis-flight-server"]
