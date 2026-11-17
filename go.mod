module github.com/redis/redis-apache-arrow-flight

go 1.27.4

require (
	github.com/adbc-drivers/redis/go v0.0.0-20261022161448-9cc8e25c0f01
	github.com/apache/arrow-adbc/go/adbc v1.13.0
	github.com/apache/arrow-go/v18 v18.9.0
	github.com/prometheus/client_golang v1.25.0
	github.com/redis/go-redis/v9 v9.23.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/adbc-drivers/driverbase-go/driverbase v0.0.0-20261115080055-809dff5b62b1 // indirect
	github.com/andybalholm/brotli v1.3.0 // indirect
	github.com/apache/thrift v0.25.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dlclark/regexp2/v2 v2.13.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-json v0.12.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.34.0 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/klauspost/cpuid/v2 v2.4.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pandatix/go-abnf v0.5.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.74.0 // indirect
	github.com/prometheus/procfs v0.23.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/exp v0.0.0-20261117202309-7a49493bfd90 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/tools v0.53.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261116203732-fd2c7689a808 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261116203732-fd2c7689a808 // indirect
)

replace github.com/adbc-drivers/redis/go => github.com/redis/redis-adbc-driver/go v0.0.0-20261117215830-30a9eefbf13d
