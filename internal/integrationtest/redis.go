// Package integrationtest defines the backend contract shared by integration suites.
package integrationtest

import (
	"net/url"
	"os"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// RedisURI requires a disposable Redis Enterprise cluster database endpoint.
// The database must have Search and TLS enabled and require an ACL username and
// password. Enterprise provisioning is an operator prerequisite: the OSS Cluster
// API is not a reliable edition check for a proxied Enterprise database.
func RedisURI(t testing.TB) string {
	t.Helper()
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		if os.Getenv("REDIS_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("REDIS_URI is required: use a Redis Enterprise cluster with TLS, username and password")
		}
		t.Skip("set REDIS_URI to a Redis Enterprise cluster with TLS, username and password to run integration tests")
	}
	if err := validateURI(uri); err != "" {
		t.Fatal(err)
	}
	return uri
}

func validateURI(uri string) string {
	// Never return parser errors: URLs can contain credentials.
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "rediss" || u.Hostname() == "" {
		return "integration REDIS_URI must use rediss:// and a Redis Enterprise database hostname"
	}
	password, hasPassword := u.User.Password()
	if u.User.Username() == "" || !hasPassword || password == "" {
		return "integration REDIS_URI must include a nonempty username and password"
	}
	opts, err := goredis.ParseURL(uri)
	if err != nil || opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify {
		return "integration REDIS_URI must be valid and enable verified TLS"
	}
	return ""
}
