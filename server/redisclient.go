package server

import (
	"context"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
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

// clientOptions uses the community implementation; see the build-profile contract.
func clientOptions(uri string, id identity) (*goredis.Options, error) {
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URI: %w", err)
	}
	if id.Username != "" || id.Password != "" {
		opts.Username = id.Username
		opts.Password = id.Password
	}
	if opts.TLSConfig != nil || opts.Username != "" || opts.Password != "" {
		return nil, errFeatureUnavailable
	}
	opts.DisableIdentity = true
	return opts, nil
}
