package server

import (
	"errors"
	redis "github.com/adbc-drivers/redis/go"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildProfileSecurityAndTelemetry(t *testing.T) {
	if redis.FeatureCapabilities()["enterprise"] != enterpriseBuild {
		t.Fatal("Flight and driver build profiles do not match")
	}
	if FeatureCapabilities()["security"] != enterpriseBuild {
		t.Fatal("wrong capability")
	}
	if enterpriseBuild {
		return
	}
	for _, cfg := range []Config{{Auth: AuthRedisACL}, {TLSCertFile: "missing.crt", TLSKeyFile: "missing.key"}, {RedisURI: "rediss://localhost:1"}, {RedisURI: "redis://user:profile-canary@localhost:1"}} {
		if _, err := New(cfg); !errors.Is(err, errFeatureUnavailable) {
			t.Fatalf("security request did not fail closed: %v", err)
		}
	}
	recorder := httptest.NewRecorder()
	newMetrics().handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
		t.Fatal("community telemetry is not empty")
	}
}
func TestBuildBasicConnectionsDoNotReuse(t *testing.T) {
	if enterpriseBuild {
		return
	}
	db := &fakeDB{}
	pool := newConnPool(db, 2, 2)
	defer pool.close()
	ctx := t.Context()
	first, err := pool.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pool.put(first, true)
	second, err := pool.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pool.put(second, true)
	if len(db.conns) != 2 || !db.conns[0].closed || !db.conns[1].closed {
		t.Fatal("basic connection lifecycle is incorrect")
	}
}

func TestBuildIntegrationRoundTrip(t *testing.T) {
	clients := newClients(t, Config{RedisURI: redisURI(t)})
	c := clients.sql
	ctx := t.Context()
	table := uniqueName("it_build_profile")
	dropTable(t, c, table)
	update(t, ctx, c, "CREATE TABLE "+table+" (id BIGINT, label TEXT)")
	if n := update(t, ctx, c, "INSERT INTO "+table+" VALUES (1,'a'),(2,NULL)"); n != 2 {
		t.Fatal("incorrect mutation count", n)
	}
	wantRows(t, query(t, ctx, c, "SELECT SUM(id),COUNT(label) FROM "+table), "3|1")
	update(t, ctx, c, "UPDATE "+table+" SET label='b' WHERE id=2")
	wantRows(t, query(t, ctx, c, "SELECT id,label FROM "+table+" ORDER BY id"), "1|a", "2|b")
}
