package server

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	snapshotOption         = "redis.read_snapshot"
	snapshotStatusOption   = "redis.read_snapshot_status"
	snapshotProtocolOption = "redis.read_snapshot_protocol"
	snapshotHeader         = "x-redis-read-snapshot"
)

var errSnapshotLost = status.Error(codes.FailedPrecondition,
	"shared read snapshot unavailable; close the session and restart discovery and all table reads")

type readSnapshot struct {
	State      string    `json:"state"`
	Generation string    `json:"generation,omitempty"`
	Deadline   time.Time `json:"deadline"`
	// Handle is random gateway state, never the native diagnostic generation.
	Handle string `json:"handle,omitempty"`
}

func snapshotHeaderValue(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(snapshotHeader)
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || !validSessionToken(values[0]) {
		return "", status.Error(codes.InvalidArgument, "invalid shared read snapshot header")
	}
	return values[0], nil
}

// snapshotGuard requires BOTH the owned cookie and an explicit attempt handle.
// Losing either cannot turn a snapshot request into a pooled/live request.
func snapshotGuard(ctx context.Context) error {
	handle, err := snapshotHeaderValue(ctx)
	if err != nil {
		return err
	}
	sess := sessionFrom(ctx)
	if sess == nil {
		if handle != "" {
			return errSnapshotLost
		}
		return nil
	}
	if !sess.mu.TryLock() {
		return status.Error(codes.ResourceExhausted, "session is busy; close its Arrow reader before another request")
	}
	defer sess.mu.Unlock()
	if sess.snapshot == nil {
		if handle != "" {
			return errSnapshotLost
		}
		return nil
	}
	if handle == "" || handle != sess.snapshot.Handle || sess.closed || sess.snapshot.State != "active" {
		return errSnapshotLost
	}
	return sess.checkSnapshot(ctx)
}

// checkSnapshot is called with sess.mu held before every use of the connection.
func (sess *session) checkSnapshot(ctx context.Context) error {
	if sess.snapshot == nil {
		return nil
	}
	if sess.snapshot.State != "active" || sess.conn == nil {
		return errSnapshotLost
	}
	handle, err := snapshotHeaderValue(ctx)
	if err != nil {
		return err
	}
	if handle != sess.snapshot.Handle {
		return errSnapshotLost
	}
	if !time.Now().Before(sess.snapshot.Deadline) {
		sess.snapshot.State = "expired"
		sess.dropConn()
		return errSnapshotLost
	}
	raw, err := connOption(ctx, sess.conn, redis.OptionStringReadSnapshotStatus)
	var current readSnapshot
	if err == nil {
		err = json.Unmarshal([]byte(raw), &current)
	}
	if err != nil || current.State != "active" || current.Generation != sess.snapshot.Generation || !current.Deadline.Equal(sess.snapshot.Deadline) {
		sess.snapshot.State = "failed"
		sess.dropConn()
		return errSnapshotLost
	}
	return nil
}

// setReadSnapshot is a single-option Flight SetSessionOptions extension. A new
// attempt always needs a new session; release/failure never enables live reads.
func (s *sqlServer) setReadSnapshot(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	cfg := s.tenants.cfg
	if cfg.Auth != AuthRedisACL || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, status.Error(codes.Unimplemented, "shared read snapshots require an authenticated TLS Flight SQL deployment")
	}
	if len(req.GetSessionOptions()) != 1 {
		return nil, status.Error(codes.InvalidArgument, "set redis.read_snapshot alone")
	}
	value, ok := req.GetSessionOptions()[snapshotOption].GetOptionValue().(*pb.SessionOptionValue_StringValue)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "redis.read_snapshot requires a JSON string")
	}
	command := struct {
		Action    string `json:"action"`
		Retention int64  `json:"retention_ms"`
	}{Retention: 900000}
	dec := json.NewDecoder(strings.NewReader(value.StringValue))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&command); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot command")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot command")
	}
	if command.Action != "acquire" && command.Action != "release" {
		return nil, status.Error(codes.InvalidArgument, "snapshot action must be acquire or release")
	}
	if command.Retention < 120000 || command.Retention > 3600000 {
		return nil, status.Error(codes.InvalidArgument, "snapshot retention_ms must be 120000–3600000")
	}
	sess, err := ensureSession(ctx)
	if err != nil {
		return nil, err
	}
	sess.store.begin(sess)
	defer sess.store.end(sess)
	if !sess.mu.TryLock() {
		return nil, status.Error(codes.ResourceExhausted, "close the active Arrow reader before snapshot control")
	}
	defer sess.mu.Unlock()
	if sess.closed {
		return nil, errSnapshotLost
	}
	if hasSnapshotHeader(ctx) {
		handle, err := snapshotHeaderValue(ctx)
		if err != nil {
			return nil, err
		}
		if sess.snapshot == nil || handle != sess.snapshot.Handle {
			return nil, errSnapshotLost
		}
	}
	if command.Action == "release" {
		// Closing the dedicated connection also cancels readers and releases its pin.
		if sess.snapshot != nil {
			sess.snapshot.State = "released"
		}
		sess.dropConn()
		return &flight.SetSessionOptionsResult{}, nil
	}
	if sess.snapshot != nil {
		return nil, errSnapshotLost
	}
	// Mark failed BEFORE opening tenant/native resources, including ambiguous
	// replies and cancellation. This session can never fall back to live reads.
	sess.snapshot = &readSnapshot{State: "failed", Handle: newSessionToken()}
	success := false
	defer func() {
		if !success {
			sess.dropConn()
		}
	}()
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if sess.conn == nil {
		if err := sess.open(ctx, t); err != nil {
			return nil, err
		}
	}
	if err := setConnOption(ctx, sess.conn, redis.OptionIntReadSnapshotRetention, strconv.FormatInt(command.Retention, 10)); err != nil {
		return nil, toStatus(err)
	}
	if err := setConnOption(ctx, sess.conn, redis.OptionStringReadSnapshotAction, "acquire"); err != nil {
		return nil, toStatus(err)
	}
	raw, err := connOption(ctx, sess.conn, redis.OptionStringReadSnapshotStatus)
	if err != nil {
		return nil, toStatus(err)
	}
	var view readSnapshot
	if err := json.Unmarshal([]byte(raw), &view); err != nil || view.State != "active" || view.Generation == "" || !time.Now().Before(view.Deadline) {
		return nil, errSnapshotLost
	}
	view.Handle = sess.snapshot.Handle
	sess.snapshot = &view
	success = true
	return &flight.SetSessionOptionsResult{}, nil
}

// snapshotQuery holds the dedicated connection until streamRecords releases the
// Arrow reader. It never uses the pooled stream or settings-copy paths.
func (s *sqlServer) snapshotQuery(ctx context.Context, t *tenant, sess *session, h statementHandle) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sess.store.begin(sess)
	if !sess.mu.TryLock() {
		sess.store.end(sess)
		return nil, nil, status.Error(codes.ResourceExhausted, "session has an active request")
	}
	live := false
	defer func() {
		if !live {
			if sess.snapshot != nil {
				sess.snapshot.State = "failed"
				sess.dropConn()
			}
			sess.mu.Unlock()
			sess.store.end(sess)
		}
	}()
	if err := sess.checkSnapshot(ctx); err != nil {
		return nil, nil, err
	}
	if !t.tryStream() {
		return nil, nil, status.Error(codes.ResourceExhausted, "no snapshot stream available")
	}
	kept := false
	defer func() {
		if !kept {
			t.endStream()
		}
	}()
	stmt, err := s.newStatement(ctx, sess.conn, h, true)
	if err != nil {
		return nil, nil, toStatus(err)
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close(context.WithoutCancel(ctx))
		return nil, nil, toStatus(err)
	}
	live, kept = true, true
	guarded := &snapshotReader{RecordReader: rdr, ctx: ctx, sess: sess}
	return rdr.Schema(), streamRecords(ctx, guarded, &liveResult{
		stall:    s.tenants.cfg.StreamStallTimeout,
		deadline: sess.snapshot.Deadline,
		validate: func() error { return sess.checkSnapshot(ctx) },
		stalled:  s.tenants.metrics.resultStalled,
		done: func(err error) {
			_ = stmt.Close(context.WithoutCancel(ctx))
			// Any failed/cancelled extraction terminates the whole attempt.
			if err != nil || ctx.Err() != nil {
				sess.snapshot.State = "failed"
				sess.dropConn()
			}
			t.endStream()
			sess.mu.Unlock()
			sess.store.end(sess)
		},
	}), nil
}

func bindSnapshotHandle(ctx context.Context, h *statementHandle) error {
	handle, err := snapshotHeaderValue(ctx)
	if err != nil {
		return err
	}
	if h.Snapshot != "" && h.Snapshot != handle {
		return errSnapshotLost
	}
	h.Snapshot = handle
	return nil
}

func validateSnapshotHandle(ctx context.Context, h statementHandle) error {
	if h.Snapshot == "" {
		return nil
	}
	handle, err := snapshotHeaderValue(ctx)
	if err != nil {
		return err
	}
	if handle != h.Snapshot || sessionFrom(ctx) == nil {
		return errSnapshotLost
	}
	return nil
}

func hasSnapshotHeader(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	return len(md.Get(snapshotHeader)) != 0
}

// Even a native materialized result must not hand out a new batch after the
// gateway attempt expires or loses authority. sess.mu remains held by the stream.
type snapshotReader struct {
	array.RecordReader
	ctx  context.Context
	sess *session
	err  error
}

func (r *snapshotReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.err = r.sess.checkSnapshot(r.ctx); r.err != nil {
		return false
	}
	if !r.RecordReader.Next() {
		return false
	}
	r.err = r.sess.checkSnapshot(r.ctx)
	return r.err == nil
}
func (r *snapshotReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.RecordReader.Err()
}
