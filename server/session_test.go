package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestCookieToken(t *testing.T) {
	token := newSessionToken()
	if !validSessionToken(token) {
		t.Fatalf("new token %q isn't valid", token)
	}
	for header, want := range map[string]string{
		sessionCookie + "=" + token:                       token,
		"a=b; " + sessionCookie + "=" + token + "; c=d":   token,
		sessionCookie + "=not-a-token":                    "",
		sessionCookie + "=" + strings.ToUpper(token)[:31]: "",
		"other=" + token:                                  "",
	} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("cookie", header))
		got, ok := cookieToken(ctx)
		if got != want || ok != (want != "") {
			t.Errorf("cookieToken(%q) = %q, %v; want %q", header, got, ok, want)
		}
	}
	if _, ok := cookieToken(context.Background()); ok {
		t.Error("cookieToken found a token without metadata")
	}
}

func TestSessionStoreReap(t *testing.T) {
	st := &sessionStore{idle: time.Hour, log: slog.New(slog.NewTextHandler(io.Discard, nil)), sessions: map[string]*session{}}
	confirmed := st.get(newSessionToken(), "owner", true)
	unconfirmed := st.get(newSessionToken(), "owner", false)
	busy := st.get(newSessionToken(), "owner", true)
	st.begin(busy)

	st.reap(time.Now().Add(unconfirmedIdle + time.Second))
	if !unconfirmed.closed || st.sessions[unconfirmed.token] != nil {
		t.Error("an unconfirmed session idle past unconfirmedIdle wasn't closed")
	}
	if confirmed.closed {
		t.Error("a confirmed session was closed before its idle timeout")
	}
	st.reap(time.Now().Add(2 * time.Hour))
	if !confirmed.closed {
		t.Error("an idle confirmed session wasn't closed")
	}
	if busy.closed {
		t.Error("a session in use was closed")
	}
	if got := st.get(busy.token, "someone else", true); got != nil {
		t.Error("another identity got the session")
	}
	if got := st.get(busy.token, "owner", true); got != busy {
		t.Error("the owner didn't get its session back")
	}
}
