package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"os"
	"strings"
)

// clusterOption is the Redis ADBC driver's database option that selects
// its client; the server's own client follows it too.
const clusterOption = "adbc.redis.cluster"

// newRedisClient uses the community implementation; see the build-profile contract.
func newRedisClient(ctx context.Context, opts *goredis.Options, mode string) (goredis.UniversalClient, error) {
	switch mode {
	case "", "auto", "true", "false":
	default:
		return nil, fmt.Errorf("invalid %s %q (want auto, true or false)", clusterOption, mode)
	}
	if mode != "true" {
		single := goredis.NewClient(opts)
		if err := single.Ping(ctx).Err(); err != nil {
			_ = single.Close()
			return nil, err
		}
		if mode == "false" {
			return single, nil
		}
		// Proxied databases report cluster_enabled:0 or reject the section,
		// and an ACL user may not have INFO; all mean a single endpoint.
		info, err := single.Info(ctx, "cluster").Result()
		if err != nil || !strings.Contains(info, "cluster_enabled:1") {
			return single, nil
		}
		_ = single.Close()
	}
	if opts.DB != 0 {
		return nil, fmt.Errorf("redis Cluster only supports database 0 (got %d)", opts.DB)
	}
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:           []string{opts.Addr},
		Username:        opts.Username,
		Password:        opts.Password,
		TLSConfig:       opts.TLSConfig,
		Protocol:        opts.Protocol,
		DisableIdentity: opts.DisableIdentity,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
	})
	if err := cluster.Ping(ctx).Err(); err != nil {
		_ = cluster.Close()
		return nil, err
	}
	return cluster, nil
}

// clientOptions preserves verified TLS and applies the request's Redis identity.
func clientOptions(uri string, id identity) (*goredis.Options, error) {
	return clientOptionsWithCA(uri, id, "")
}

func clientOptionsWithCA(uri string, id identity, caFile string) (*goredis.Options, error) {
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		return nil, errors.New("invalid Redis URI")
	}
	if id.Username != "" || id.Password != "" {
		opts.Username, opts.Password = id.Username, id.Password
	}
	if opts.TLSConfig != nil {
		if opts.TLSConfig.InsecureSkipVerify {
			return nil, errors.New("Redis TLS certificate verification must remain enabled")
		}
		opts.TLSConfig.MinVersion = tls.VersionTLS12
	} else if opts.Username != "" || opts.Password != "" || caFile != "" {
		return nil, errors.New("Redis credentials and custom trust require a verified rediss:// connection")
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, errors.New("cannot read Redis TLS CA file")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("Redis TLS CA file contains no valid certificates")
		}
		opts.TLSConfig.RootCAs = roots
	}
	opts.DisableIdentity = true
	return opts, nil
}

// Use the same explicit trust bundle for gateway clients and native ADBC.
func (c *Config) redisOptions(id identity) (*goredis.Options, error) {
	return clientOptionsWithCA(c.RedisURI, id, c.ADBCOptions["adbc.redis.tls_ca_file"])
}
