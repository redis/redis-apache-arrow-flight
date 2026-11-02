package integrationtest

import (
	"strings"
	"testing"
)

func TestValidateURI(t *testing.T) {
	for _, uri := range []string{
		"", "redis://user:secret@localhost:6379/0", "rediss://localhost:6379/0",
		"rediss://:secret@localhost:6379/0", "rediss://user@localhost:6379/0",
		"rediss://user:@localhost:6379/0", "rediss://user:secret@/0",
		"rediss://user:secret@localhost:6379/0?unknown=secret",
		"rediss://user:secret@localhost:bad/0",
		"rediss://user:secret@localhost:6379/0?skip_verify=true",
	} {
		if msg := validateURI(uri); msg == "" || strings.Contains(msg, "secret") {
			t.Errorf("invalid URI must be rejected without disclosing credentials (message %q)", msg)
		}
	}
	for _, uri := range []string{
		"rediss://user:secret@redis.example.com:10000/0",
		"rediss://default:p%40ss%3Aword@redis.example.com:10000/0",
	} {
		if msg := validateURI(uri); msg != "" {
			t.Error(msg)
		}
	}
}
