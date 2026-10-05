package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Sessions keep a client's session state (SET parameters such as
// search_path and TimeZone, and temporary tables and views) from one request
// to the next, on a dedicated ADBC connection. They are Flight's cookie
// sessions: every response to a request without a session cookie sets one,
// and a client that keeps cookies (the JDBC driver does by default, ADBC's
// Flight SQL driver with adbc.flight.sql.rpc.with_cookie_middleware) sends
// it back.
//
// The server keeps nothing for a cookie it hands out until a request
// carries it back (or calls SetSessionOptions), so clients that drop
// cookies cost nothing. A session's connection is opened by its first SQL
// request. Sessions are closed by CloseSession or after being idle, and
// live in this server's memory: with several servers, route a client to
// the same one.

const (
	sessionCookie = "arrow_flight_session_id"
	// maxSessions caps the sessions a server keeps; past it, requests use
	// the pool.
	maxSessions = 10_000
	// unconfirmedIdle closes sooner a session made by SetSessionOptions for
	// a cookie the client never sent back: it may not keep cookies.
	unconfirmedIdle = 30 * time.Second
)

// sessionOption is one of the session options a client may set.
type sessionOption struct {
	key   string // the driver's connection option that reads it
	set   func(ctx context.Context, conn adbc.ConnectionWithContext, v string) error
	reset string // the statement that erases it ("" for none)
}

var sessionOptions = map[string]sessionOption{
	"catalog": {key: adbc.OptionKeyCurrentCatalog, set: optionSetter(adbc.OptionKeyCurrentCatalog)},
	"schema": {key: adbc.OptionKeyCurrentDbSchema, set: optionSetter(adbc.OptionKeyCurrentDbSchema),
		reset: "RESET search_path"},
	// SET, not the connection option: the option would become what RESET
	// goes back to.
	"timezone": {key: "adbc.redis.time_zone", reset: "RESET TIME ZONE",
		set: func(ctx context.Context, conn adbc.ConnectionWithContext, v string) error {
			return execOn(ctx, conn, "SET TIME ZONE '"+strings.ReplaceAll(v, "'", "''")+"'")
		}},
}

func optionSetter(key string) func(context.Context, adbc.ConnectionWithContext, string) error {
	return func(ctx context.Context, conn adbc.ConnectionWithContext, v string) error {
		return setConnOption(ctx, conn, key, v)
	}
}

type session struct {
	token, owner string
	store        *sessionStore

	mu      sync.Mutex // held by the request using the session
	conn    adbc.ConnectionWithContext
	release func() // the tenant hold, while conn is open
	opts    map[string]string
	closed  bool
	// temp is set once conn may have temporary tables or views, until it
	// closes. They shadow other tables' unqualified names, so the
	// session's results are then read on conn (see scanSettings).
	temp bool

	// Guarded by store.mu.
	lastUsed  time.Time
	inUse     int
	confirmed bool // a request carried its cookie
}

// run runs fn on the session's connection, opening it from t on first use.
// If fn fails because of the connection, or panics, the connection is
// closed; the next request opens another and sets the session's options
// on it again (its temporary objects are gone).
func (s *session) run(ctx context.Context, t *tenant, fn func(adbc.ConnectionWithContext) error) (err error) {
	s.store.begin(s)
	defer s.store.end(s)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return status.Error(codes.Aborted, "the session was closed; start a new one")
	}
	if s.conn == nil {
		if err := s.open(ctx, t); err != nil {
			return err
		}
	}
	ok := false
	defer func() {
		if !ok {
			s.dropConn()
		}
	}()
	err = fn(s.conn)
	ok = err == nil || isStatementError(err)
	return err
}

// open opens the session's connection and sets its options. s.mu is held.
func (s *session) open(ctx context.Context, t *tenant) error {
	_, release, err := s.store.tenants.acquire(ctx)
	if err != nil {
		return err
	}
	conn, err := t.db.Open(ctx)
	if err != nil {
		release()
		return toStatus(err)
	}
	for name, v := range s.opts {
		if err := sessionOptions[name].set(ctx, conn, v); err != nil {
			_ = conn.Close(context.Background())
			release()
			return status.Errorf(codes.FailedPrecondition, "failed to restore session option %s: %v", name, err)
		}
	}
	s.conn, s.release = conn, release
	s.store.log.Debug("session connection opened", "session", s.token[:8])
	return nil
}

// dropConn closes the session's connection, if open. s.mu is held.
func (s *session) dropConn() {
	if s.conn == nil {
		return
	}
	_ = s.conn.Close(context.Background())
	s.release()
	s.conn, s.release, s.temp = nil, nil, false
}

// markTemp records that the request's session connection may now have
// temporary objects. Call it while running on that connection.
func markTemp(ctx context.Context) {
	if s := sessionFrom(ctx); s != nil {
		s.temp = true
	}
}

// scanOptions are the session options that change what a plain scan of a
// table returns: of the driver's SET parameters, only search_path and
// TimeZone do (the others accept one value, or are only shown). A session's
// scan streams on a pooled connection with these set as the session has
// them (see scanSettings).
var scanOptions = []string{"schema", "timezone"}

// scanSettings carries a session's scanOptions to a pooled connection for
// a scan, and puts the connection's own back afterwards.
type scanSettings struct {
	want   map[string]string // the session's, by option name
	orig   map[string]string // the pooled connection's, once applied
	resets []string          // what puts the pooled connection back
}

// settingsError is a failure to apply a session's settings to a pooled
// connection. It wraps nothing, so the connection isn't reused.
type settingsError struct{ err error }

func (e settingsError) Error() string {
	return "failed to apply the session's settings: " + e.err.Error()
}

// scanSettings reads the session's scanOptions, as of now (SET LOCAL
// included), for a scan to stream with on a pooled connection. It returns
// nil if the session's results must be read on its own connection: it may
// have temporary objects, which a pooled connection doesn't see, or its
// settings can't be read.
func (s *session) scanSettings(ctx context.Context, t *tenant) (*scanSettings, error) {
	var ss *scanSettings
	err := s.run(ctx, t, func(conn adbc.ConnectionWithContext) error {
		if s.temp {
			return nil
		}
		want := make(map[string]string, len(scanOptions))
		for _, name := range scanOptions {
			v, err := connOption(ctx, conn, sessionOptions[name].key)
			if err != nil {
				s.store.log.Debug("can't read a session setting: reading the result on the session's connection",
					"session", s.token[:8], "option", name, "error", err)
				return nil
			}
			want[name] = v
		}
		ss = &scanSettings{want: want}
		return nil
	})
	return ss, err
}

// apply sets the session's settings on conn, a pooled connection.
func (ss *scanSettings) apply(ctx context.Context, conn adbc.ConnectionWithContext) error {
	ss.orig = make(map[string]string, len(scanOptions))
	for _, name := range scanOptions {
		opt := sessionOptions[name]
		cur, err := connOption(ctx, conn, opt.key)
		if err != nil {
			return settingsError{err}
		}
		ss.orig[name] = cur
		if cur == ss.want[name] {
			continue
		}
		// Before setting: a SET that fails part way is reset too.
		ss.resets = append(ss.resets, opt.reset)
		if err := opt.set(ctx, conn, ss.want[name]); err != nil {
			return settingsError{err}
		}
		if got, err := connOption(ctx, conn, opt.key); err != nil || got != ss.want[name] {
			if err == nil {
				err = fmt.Errorf("%s is %q, not %q", name, got, ss.want[name])
			}
			return settingsError{err}
		}
	}
	return nil
}

// undo puts conn's own settings back, before it goes back to the pool. An
// error closes it instead, so the session's settings never stay in the
// pool.
func (ss *scanSettings) undo(ctx context.Context, conn adbc.ConnectionWithContext) error {
	ctx = context.WithoutCancel(ctx)
	for _, reset := range ss.resets {
		if err := execOn(ctx, conn, reset); err != nil {
			return err
		}
	}
	for name, v := range ss.orig {
		if got, err := connOption(ctx, conn, sessionOptions[name].key); err != nil || got != v {
			return fmt.Errorf("%s is %q after the reset, not %q: %v", name, got, v, err)
		}
	}
	return nil
}

func setConnOption(ctx context.Context, conn adbc.ConnectionWithContext, key, value string) error {
	o, ok := conn.(adbc.GetSetOptionsWithContext)
	if !ok {
		return adbc.Error{Code: adbc.StatusNotImplemented, Msg: "the connection has no options"}
	}
	return o.SetOption(ctx, key, value)
}

func connOption(ctx context.Context, conn adbc.ConnectionWithContext, key string) (string, error) {
	o, ok := conn.(adbc.GetSetOptionsWithContext)
	if !ok {
		return "", adbc.Error{Code: adbc.StatusNotImplemented, Msg: "the connection has no options"}
	}
	return o.GetOption(ctx, key)
}

// sessionStore holds a server's sessions, by token.
type sessionStore struct {
	tenants *tenantCache
	idle    time.Duration
	log     *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	stop     chan struct{}
	done     chan struct{}
}

func newSessionStore(tenants *tenantCache, idle time.Duration, log *slog.Logger) *sessionStore {
	st := &sessionStore{
		tenants:  tenants,
		idle:     idle,
		log:      log,
		sessions: map[string]*session{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go st.reapLoop()
	return st
}

// get returns the session of token for owner, making it if the server
// doesn't know token (the client kept a cookie this server, or a restarted
// one, handed out). It returns nil when the session belongs to someone
// else, or the store is full.
func (st *sessionStore) get(token, owner string, confirmed bool) *session {
	st.mu.Lock()
	defer st.mu.Unlock()
	if s, ok := st.sessions[token]; ok {
		if s.owner != owner {
			return nil
		}
		s.lastUsed = time.Now()
		s.confirmed = s.confirmed || confirmed
		return s
	}
	if len(st.sessions) >= maxSessions {
		st.log.Warn("too many sessions; this request uses the pool", "max", maxSessions)
		return nil
	}
	s := &session{token: token, owner: owner, store: st, opts: map[string]string{},
		lastUsed: time.Now(), confirmed: confirmed}
	st.sessions[token] = s
	st.log.Debug("session started", "session", token[:8])
	return s
}

func (st *sessionStore) begin(s *session) {
	st.mu.Lock()
	s.inUse++
	s.lastUsed = time.Now()
	st.mu.Unlock()
}

func (st *sessionStore) end(s *session) {
	st.mu.Lock()
	s.inUse--
	s.lastUsed = time.Now()
	st.mu.Unlock()
}

// remove closes a session and forgets it.
func (st *sessionStore) remove(s *session) {
	st.mu.Lock()
	if st.sessions[s.token] == s {
		delete(st.sessions, s.token)
	}
	st.mu.Unlock()
	s.mu.Lock()
	s.closed = true
	s.dropConn()
	s.mu.Unlock()
	st.log.Debug("session closed", "session", s.token[:8])
}

func (st *sessionStore) reapLoop() {
	defer close(st.done)
	interval := min(st.idle/2, unconfirmedIdle/2, time.Minute)
	tick := time.NewTicker(max(interval, 10*time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-st.stop:
			return
		case now := <-tick.C:
			st.reap(now)
		}
	}
}

// reap closes the sessions idle for longer than they may be.
func (st *sessionStore) reap(now time.Time) {
	var idle []*session
	st.mu.Lock()
	for token, s := range st.sessions {
		limit := st.idle
		if !s.confirmed {
			limit = min(limit, unconfirmedIdle)
		}
		if s.inUse == 0 && now.Sub(s.lastUsed) >= limit {
			delete(st.sessions, token)
			idle = append(idle, s)
		}
	}
	st.mu.Unlock()
	for _, s := range idle {
		s.mu.Lock()
		s.closed = true
		s.dropConn()
		s.mu.Unlock()
		st.log.Debug("idle session closed", "session", s.token[:8])
	}
}

// close closes every session.
func (st *sessionStore) close() {
	close(st.stop)
	<-st.done
	st.mu.Lock()
	all := st.sessions
	st.sessions = map[string]*session{}
	st.mu.Unlock()
	for _, s := range all {
		s.mu.Lock()
		s.closed = true
		s.dropConn()
		s.mu.Unlock()
	}
}

// ---- request context ----

type sessionCtxKey struct{}

// sessionInfo is what the session middleware puts in a request's context.
type sessionInfo struct {
	store *sessionStore
	owner string
	token string   // the request's cookie's, or the one its response sets
	sess  *session // nil until the request's cookie, or SetSessionOptions, names one
}

func sessionInfoFrom(ctx context.Context) *sessionInfo {
	si, _ := ctx.Value(sessionCtxKey{}).(*sessionInfo)
	return si
}

// sessionFrom returns the request's session, or nil.
func sessionFrom(ctx context.Context) *session {
	if si := sessionInfoFrom(ctx); si != nil {
		return si.sess
	}
	return nil
}

var errSessionsOff = status.Error(codes.Unimplemented, "sessions are turned off on this server")

// ensureSession returns the request's session, making one for the cookie
// its response sets if it has none.
func ensureSession(ctx context.Context) (*session, error) {
	si := sessionInfoFrom(ctx)
	if si == nil {
		return nil, errSessionsOff
	}
	if si.sess == nil {
		if si.sess = si.store.get(si.token, si.owner, false); si.sess == nil {
			return nil, status.Error(codes.ResourceExhausted, "the server has too many sessions")
		}
	}
	return si.sess, nil
}

func newSessionToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validSessionToken(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// cookieToken returns the session token in a request's cookies.
func cookieToken(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, line := range md.Get("cookie") {
		cookies, err := http.ParseCookie(line)
		if err != nil {
			continue
		}
		for _, c := range cookies {
			if c.Name == sessionCookie && validSessionToken(c.Value) {
				return c.Value, true
			}
		}
	}
	return "", false
}

// middleware attaches each request's session, and sets a session cookie on
// responses to requests without one. It runs after authentication, so a
// session belongs to the identity that made it.
func (st *sessionStore) middleware() flight.ServerMiddleware {
	start := func(ctx context.Context, setHeader func(metadata.MD) error) context.Context {
		id, err := st.tenants.identityFrom(ctx)
		if err != nil {
			return ctx // not authenticated (the Handshake): no session yet
		}
		si := &sessionInfo{store: st, owner: id.key()}
		if token, ok := cookieToken(ctx); ok {
			if si.sess = st.get(token, si.owner, true); si.sess != nil {
				si.token = token
				return context.WithValue(ctx, sessionCtxKey{}, si)
			}
		}
		si.token = newSessionToken()
		_ = setHeader(metadata.Pairs("set-cookie", sessionCookie+"="+si.token))
		return context.WithValue(ctx, sessionCtxKey{}, si)
	}
	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(start(ctx, func(md metadata.MD) error { return grpc.SetHeader(ctx, md) }), req)
		},
		Stream: func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, &contextStream{ServerStream: ss, ctx: start(ss.Context(), ss.SetHeader)})
		},
	}
}

// contextStream is a server stream with another context.
type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

// ---- Flight SQL session RPCs ----

// SetSessionOptions sets the session options catalog, schema and timezone
// on the session's connection, starting a session if the request has
// none.
func (s *sqlServer) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	sess, err := ensureSession(ctx)
	if err != nil {
		return nil, err
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	res := &flight.SetSessionOptionsResult{Errors: map[string]*flight.SetSessionOptionsResultError{}}
	fail := func(name string, v pb.SetSessionOptionsResult_ErrorValue) {
		res.Errors[name] = &flight.SetSessionOptionsResultError{Value: v}
	}
	err = sess.run(ctx, t, func(conn adbc.ConnectionWithContext) error {
		for name, v := range req.GetSessionOptions() {
			opt, ok := sessionOptions[name]
			if !ok {
				fail(name, flight.SetSessionOptionsResultErrorInvalidName)
				continue
			}
			if v.GetOptionValue() == nil { // erase
				if opt.reset != "" {
					if err := execOn(ctx, conn, opt.reset); err != nil {
						fail(name, flight.SetSessionOptionsResultErrorError)
						continue
					}
				}
				delete(sess.opts, name)
				continue
			}
			sv, ok := v.GetOptionValue().(*pb.SessionOptionValue_StringValue)
			if !ok {
				fail(name, flight.SetSessionOptionsResultErrorInvalidValue)
				continue
			}
			if err := opt.set(ctx, conn, sv.StringValue); err != nil {
				fail(name, flight.SetSessionOptionsResultErrorInvalidValue)
				continue
			}
			if name != "catalog" {
				sess.opts[name] = sv.StringValue
			}
		}
		return nil
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return res, nil
}

// GetSessionOptions returns catalog, schema and timezone: the session's,
// or a pooled connection's defaults without a session.
func (s *sqlServer) GetSessionOptions(ctx context.Context, _ *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error) {
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	res := &flight.GetSessionOptionsResult{SessionOptions: map[string]*flight.SessionOptionValue{}}
	err = t.withConn(ctx, "", func(conn adbc.ConnectionWithContext) error {
		for name, opt := range sessionOptions {
			v, err := connOption(ctx, conn, opt.key)
			if err != nil {
				continue
			}
			res.SessionOptions[name] = &flight.SessionOptionValue{
				OptionValue: &pb.SessionOptionValue_StringValue{StringValue: v}}
		}
		return nil
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return res, nil
}

// CloseSession closes the request's session, if it has one, and clears
// its cookie.
func (s *sqlServer) CloseSession(ctx context.Context, _ *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	if si := sessionInfoFrom(ctx); si != nil && si.sess != nil {
		si.store.remove(si.sess)
		si.sess = nil
		_ = grpc.SetHeader(ctx, metadata.Pairs("set-cookie", sessionCookie+"=; Max-Age=0"))
	}
	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

// execOn runs a statement that returns no rows on conn.
func execOn(ctx context.Context, conn adbc.ConnectionWithContext, sql string) error {
	stmt, err := conn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer stmt.Close(ctx)
	if err := stmt.SetSqlQuery(ctx, sql); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
