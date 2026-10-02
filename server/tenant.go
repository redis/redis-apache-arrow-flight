package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"sync"
	"time"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/flight"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// identity is who a request runs as: the Redis credentials, from a bearer
// token (AuthRedisACL) or the server's URI (AuthNone, where both are empty).
type identity struct {
	Username string `json:"u"`
	Password string `json:"p"`
}

// key identifies the identity's tenant without keeping the password in
// clear text as a map key.
func (id identity) key() string {
	h := sha256.Sum256([]byte(id.Password))
	return id.Username + "\x00" + hex.EncodeToString(h[:])
}

// tenant holds what the requests of one identity use: an ADBC database and
// its connection pool for SQL, and a go-redis client for datasets.
type tenant struct {
	id   identity
	db   adbc.DatabaseWithContext
	pool *connPool
	rdb  goredis.UniversalClient

	// Guarded by tenantCache.mu.
	active   int
	lastUsed time.Time
}

func (t *tenant) close() {
	t.pool.close()
	_ = t.db.Close(context.Background())
	_ = t.rdb.Close()
}

// withConn runs fn on a pooled connection. sql is the statement fn runs, if
// any, so the pool can tell whether the connection may be reused.
func (t *tenant) withConn(ctx context.Context, sql string, fn func(adbc.ConnectionWithContext) error) error {
	conn, err := t.pool.get(ctx)
	if err != nil {
		return err
	}
	err = fn(conn)
	t.pool.put(conn, reusable(sql, err))
	return err
}

// tenantCache opens tenants on first use and closes them when idle.
type tenantCache struct {
	cfg    *Config
	driver databaseOpener

	mu       sync.Mutex
	tenants  map[string]*tenant
	creating map[string]chan struct{}
	stop     chan struct{}
	done     chan struct{}
}

func newTenantCache(cfg *Config) *tenantCache {
	c := &tenantCache{
		cfg:      cfg,
		driver:   redis.NewDriver(cfg.Allocator),
		tenants:  map[string]*tenant{},
		creating: map[string]chan struct{}{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.evictLoop()
	return c
}

// databaseOpener is the part of the Redis ADBC driver the server uses.
type databaseOpener interface {
	NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error)
}

// identityFrom returns the identity a request runs as.
func (c *tenantCache) identityFrom(ctx context.Context) (identity, error) {
	if c.cfg.Auth == AuthNone {
		return identity{}, nil
	}
	if id, ok := flight.AuthFromContext(ctx).(identity); ok {
		return id, nil
	}
	return identity{}, status.Error(codes.Unauthenticated, "not authenticated")
}

// acquire returns the tenant of the request's identity, and a function to
// call when the request is done with it.
func (c *tenantCache) acquire(ctx context.Context) (*tenant, func(), error) {
	id, err := c.identityFrom(ctx)
	if err != nil {
		return nil, nil, err
	}
	key := id.key()
	for {
		c.mu.Lock()
		if t, ok := c.tenants[key]; ok {
			t.active++
			c.mu.Unlock()
			return t, func() { c.release(t) }, nil
		}
		if wait, ok := c.creating[key]; ok {
			c.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		wait := make(chan struct{})
		c.creating[key] = wait
		c.mu.Unlock()

		t, err := c.open(ctx, id)

		c.mu.Lock()
		delete(c.creating, key)
		close(wait)
		if err != nil {
			c.mu.Unlock()
			return nil, nil, err
		}
		t.active = 1
		c.tenants[key] = t
		c.mu.Unlock()
		return t, func() { c.release(t) }, nil
	}
}

func (c *tenantCache) release(t *tenant) {
	c.mu.Lock()
	t.active--
	t.lastUsed = time.Now()
	c.mu.Unlock()
}

func (c *tenantCache) open(ctx context.Context, id identity) (*tenant, error) {
	opts, err := clientOptions(c.cfg.RedisURI, id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rdb, err := newRedisClient(ctx, opts, c.cfg.ADBCOptions[clusterOption])
	if err != nil {
		return nil, connectStatus(err)
	}
	if c.cfg.redisHook != nil {
		rdb.AddHook(c.cfg.redisHook)
	}
	dbOpts := maps.Clone(c.cfg.ADBCOptions)
	if dbOpts == nil {
		dbOpts = map[string]string{}
	}
	dbOpts[adbc.OptionKeyURI] = c.cfg.RedisURI
	if id.Username != "" || id.Password != "" {
		dbOpts[adbc.OptionKeyUsername] = id.Username
		dbOpts[adbc.OptionKeyPassword] = id.Password
	}
	db, err := c.driver.NewDatabaseWithContext(ctx, dbOpts)
	if err != nil {
		_ = rdb.Close()
		return nil, toStatus(err)
	}
	return &tenant{
		id:   id,
		db:   db,
		pool: newConnPool(db, c.cfg.PoolMaxIdle, c.cfg.PoolMaxOpen),
		rdb:  rdb,
	}, nil
}

func (c *tenantCache) evictLoop() {
	defer close(c.done)
	interval := min(c.cfg.TenantIdleTimeout/2, time.Minute)
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-tick.C:
			var idle []*tenant
			c.mu.Lock()
			for k, t := range c.tenants {
				if t.active == 0 && now.Sub(t.lastUsed) >= c.cfg.TenantIdleTimeout {
					idle = append(idle, t)
					delete(c.tenants, k)
				}
			}
			c.mu.Unlock()
			for _, t := range idle {
				t.close()
			}
		}
	}
}

// close closes every tenant. Requests still running finish on their
// borrowed connections, which are closed when put back.
func (c *tenantCache) close() {
	close(c.stop)
	<-c.done
	c.mu.Lock()
	all := c.tenants
	c.tenants = map[string]*tenant{}
	c.mu.Unlock()
	for _, t := range all {
		t.close()
	}
}
