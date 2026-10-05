package server

import (
	"context"
	"errors"
	"strings"
	"sync"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-adbc/go/adbc"
)

// connPool keeps open ADBC connections for reuse. Opening one is expensive
// with the Redis driver (PING, INFO, FT._LIST, a probe query, and metadata
// and temporary-object sweeps), so each request borrows one instead.
//
// A connection keeps session state: SET parameters such as search_path and
// TimeZone, an open BEGIN, and temporary tables and views. A statement that
// may have changed that state gets its connection closed rather than
// returned (see reusable), so no request sees another's session.
type connPool struct {
	db  adbc.DatabaseWithContext
	sem chan struct{} // one token per connection in use

	mu     sync.Mutex
	idle   []adbc.ConnectionWithContext
	max    int
	closed bool
}

func newConnPool(db adbc.DatabaseWithContext, maxIdle, maxOpen int) *connPool {
	return &connPool{db: db, sem: make(chan struct{}, maxOpen), max: maxIdle}
}

var errPoolClosed = errors.New("the connection pool is closed")

// get returns an idle connection, or opens one. It waits while maxOpen
// connections are in use.
func (p *connPool) get(ctx context.Context) (adbc.ConnectionWithContext, error) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.sem
		return nil, errPoolClosed
	}
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	c, err := p.db.Open(ctx)
	if err != nil {
		<-p.sem
		return nil, err
	}
	return c, nil
}

// put returns a connection that get gave out: to the idle list if reuse is
// true and there is room, else it closes it.
func (p *connPool) put(c adbc.ConnectionWithContext, reuse bool) {
	defer func() { <-p.sem }()
	p.mu.Lock()
	if reuse && !p.closed && len(p.idle) < p.max {
		p.idle = append(p.idle, c)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	_ = c.Close(context.Background())
}

// close closes the idle connections; connections in use are closed when
// they are put back.
func (p *connPool) close() {
	p.mu.Lock()
	idle := p.idle
	p.idle, p.closed = nil, true
	p.mu.Unlock()
	for _, c := range idle {
		_ = c.Close(context.Background())
	}
}

// reusable reports whether a connection can go back to the pool after
// running sql with the result err.
func reusable(sql string, err error) bool {
	if err != nil && !isStatementError(err) {
		// The connection itself may be broken (I/O, timeout, cancellation).
		return false
	}
	return !changesSession(sql)
}

// isStatementError reports whether err is about the statement (bad SQL, a
// missing table, a permission, an index out of cursors, a result too big
// for ResultMemory) rather than the connection.
func isStatementError(err error) bool {
	if isCursorLimit(err) || errors.Is(err, errResultMemory) {
		return true
	}
	var ae adbc.Error
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.Code {
	case adbc.StatusInvalidArgument, adbc.StatusInvalidState, adbc.StatusInvalidData,
		adbc.StatusNotFound, adbc.StatusAlreadyExists, adbc.StatusNotImplemented,
		adbc.StatusIntegrity, adbc.StatusUnauthorized:
		return true
	}
	return false
}

// tempSchema is where the driver keeps a connection's temporary tables
// and views: a table or view created in it is temporary even without TEMP.
const tempSchema = "pg_temp"

// isTempSchemaName reports whether schema names the temporary schema, as
// the driver decides it (case-insensitively).
func isTempSchemaName(schema string) bool { return strings.EqualFold(schema, tempSchema) }

// changesSession reports whether sql may leave state on its connection: SET
// / RESET, BEGIN and the other transaction statements, and temporary tables
// and views (see createsTemp).
func changesSession(sql string) bool {
	sets, temp := sessionEffects(sql)
	return sets || temp
}

// createsTemp reports whether sql may make a temporary table or view (CREATE
// TEMP TABLE / VIEW, CREATE TABLE … AS, or a name in pg_temp).
func createsTemp(sql string) bool {
	_, temp := sessionEffects(sql)
	return temp
}

// sessionEffects reports whether sql sets session state (SET / RESET, BEGIN
// and the other transaction statements) and whether it makes temporary
// objects. It parses sql with the driver's own parser; SQL that doesn't
// parse never ran.
func sessionEffects(sql string) (sets, temp bool) {
	if sql == "" {
		return false, false
	}
	stmts, err := redis.ParseScript(sql)
	if err != nil {
		return false, false
	}
	for _, ps := range stmts {
		switch s := ps.Stmt.(type) {
		case *redis.SetStmt, *redis.TransactionStmt:
			sets = true
		case *redis.CreateTableStmt:
			temp = temp || s.Temporary || isTempSchemaName(s.Table.Schema)
		case *redis.CreateViewStmt:
			temp = temp || s.Temporary || isTempSchemaName(s.Name.Schema)
		}
	}
	return sets, temp
}
