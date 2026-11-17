package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Source-exact native Go error (VendorCode is zero, not the Flight client's
// vendor14): redis-adbc-driver 640dbf1fa880, snapshot.go publishPreparedSnapshot
// and sqlstate.go codeStateErrorf. Keep this fixture independent of the matcher.
func publicationRefusal() adbc.Error {
	return adbc.Error{Code: adbc.StatusIO, SqlState: [5]byte{'4', '0', '0', '0', '1'},
		Msg: "[redis] snapshot catalog changed concurrently; retry the statement"}
}

func TestPublicationRefusalClassification(t *testing.T) {
	refusal := publicationRefusal()
	for _, tc := range []struct {
		name string
		err  error
		keep bool
	}{
		{"native refusal", refusal, true},
		{"NotFound", adbc.Error{Code: adbc.StatusNotFound}, true},
		{"message only", errors.New(refusal.Msg), false},
		{"IO without SQLSTATE", adbc.Error{Code: adbc.StatusIO, Msg: refusal.Msg}, false},
		{"other SQLSTATE", adbc.Error{Code: adbc.StatusIO, SqlState: [5]byte{'0', '8', '0', '0', '6'}, Msg: refusal.Msg}, false},
		{"other vendor", adbc.Error{Code: refusal.Code, SqlState: refusal.SqlState, VendorCode: 14, Msg: refusal.Msg}, false},
		{"SQLSTATE only", adbc.Error{Code: adbc.StatusIO, SqlState: refusal.SqlState, Msg: "connection reset (40001)"}, false},
		{"unknown outcome", adbc.Error{Code: adbc.StatusIO, SqlState: refusal.SqlState, Msg: "[redis] snapshot publication outcome is unknown (operation 40001); do not blindly retry"}, false},
		{"extra diagnostic", adbc.Error{Code: refusal.Code, SqlState: refusal.SqlState, Msg: refusal.Msg + ": unexpected EOF"}, false},
		{"cancelled status", adbc.Error{Code: adbc.StatusCancelled, SqlState: refusal.SqlState, Msg: refusal.Msg}, false},
		{"timeout status", adbc.Error{Code: adbc.StatusTimeout, SqlState: refusal.SqlState, Msg: refusal.Msg}, false},
		{"wrapped", fmt.Errorf("execution failed: %w", refusal), false},
		{"joined IO", errors.Join(refusal, io.ErrUnexpectedEOF), false},
		{"joined cancellation", errors.Join(refusal, context.Canceled), false},
		{"joined deadline", errors.Join(refusal, context.DeadlineExceeded), false},
		{"transport status", toStatus(refusal), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isStatementError(tc.err); got != tc.keep {
				t.Fatalf("isStatementError(%v) = %v, want %v", tc.err, got, tc.keep)
			}
			if got := reusable("SELECT 1", tc.err); got != tc.keep {
				t.Fatalf("reusable = %v, want %v", got, tc.keep)
			}
		})
	}
	if got := status.Code(toStatus(refusal)); got != codes.Unavailable {
		t.Fatalf("refusal status = %v, want Unavailable", got)
	}
	if reusable("SET TIME ZONE 'America/New_York'", refusal) {
		t.Fatal("a refused script that may have changed settings must not enter the pool")
	}
}

func TestSessionPublicationRefusal(t *testing.T) {
	for worker := range 4 {
		t.Run(fmt.Sprint(worker), func(t *testing.T) {
			t.Parallel()
			db := &fakeDB{}
			tn := &tenant{db: db}
			owner := identity{}.key()
			tc := &tenantCache{cfg: &Config{Auth: AuthNone}, tenants: map[string]*tenant{owner: tn}}
			st := &sessionStore{tenants: tc, log: testLogger(t), sessions: map[string]*session{}}
			sess := st.get(newSessionToken(), owner, true)
			t.Cleanup(func() { st.remove(sess) })
			ctx := context.WithValue(context.Background(), sessionCtxKey{}, &sessionInfo{store: st, sess: sess})
			if err := tn.withConn(ctx, "CREATE TEMP TABLE local (id BIGINT)", func(adbc.ConnectionWithContext) error { return nil }); err != nil {
				t.Fatal(err)
			}
			original, token := sess.conn, sess.token
			for _, failure := range []error{adbc.Error{Code: adbc.StatusNotFound}, publicationRefusal()} {
				calls := 0
				// Stand in for an execution that has consumed input or committed a
				// script prefix. Flight must return the error without calling it again.
				err := tn.withConn(ctx, "INSERT INTO local VALUES (1)", func(adbc.ConnectionWithContext) error {
					calls++
					return failure
				})
				if !reflect.DeepEqual(err, failure) || calls != 1 {
					t.Fatalf("execution replayed or error changed: calls=%d err=%v", calls, err)
				}
				if sess.conn != original || sess.token != token || !sess.temp || db.conns[0].closed || len(db.conns) != 1 {
					t.Fatalf("session lost after %v: conn=%v temp=%v opens=%d", failure, sess.conn, sess.temp, len(db.conns))
				}
				// A fresh caller-owned attempt uses the original connection.
				if err := sess.run(ctx, tn, func(c adbc.ConnectionWithContext) error {
					if c != original {
						t.Fatal("retry opened another connection")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReadSnapshotPublicationRefusalRemainsTerminal(t *testing.T) {
	srv, tn, sess, conn, ctx := snapshotHarness(t)
	ctx = acquireSnapshot(t, srv, sess, ctx)
	err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { return publicationRefusal() })
	if err == nil || conn.closed.Load() != 1 || sess.snapshot.State != "failed" {
		t.Fatalf("snapshot survived failure: err=%v state=%s", err, sess.snapshot.State)
	}
	if err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { t.Fatal("snapshot reopened"); return nil }); err == nil {
		t.Fatal("failed snapshot accepted another operation")
	}
}
