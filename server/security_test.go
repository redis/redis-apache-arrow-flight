package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func securityCertificate(t *testing.T) (string, string, tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private})
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return certFile, keyFile, cert, roots
}

// A TLS RESP authentication fixture, not a Redis/SQL implementation. It exercises
// real go-redis handshakes, certificate validation and identity propagation.
func securityBackend(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	conns := map[net.Conn]bool{}
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[conn] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(conns, conn); mu.Unlock() }()
				reader := bufio.NewReader(conn)
				authed := false
				for {
					args, err := securityRESP(reader)
					if err != nil {
						return
					}
					response := "-ERR unsupported command\r\n"
					switch strings.ToLower(args[0]) {
					case "auth":
						authed = len(args) == 3 && (args[1] == "alice" || args[1] == "bob") && args[2] == "test-password"
						if authed {
							response = "+OK\r\n"
						} else {
							response = "-WRONGPASS invalid credentials\r\n"
						}
					case "ping":
						if authed {
							response = "+PONG\r\n"
						} else {
							response = "-NOAUTH authentication required\r\n"
						}
					case "select", "client":
						if authed {
							response = "+OK\r\n"
						}
					case "eval":
						response = "-NOPERM no cancellation metadata writes\r\n"
					}
					if _, err := io.WriteString(conn, response); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-done
		mu.Lock()
		for conn := range conns {
			conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return "rediss://" + listener.Addr().String() + "/0"
}
func securityRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[0] != '*' {
		return nil, fmt.Errorf("expected array")
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || n < 1 || n > 32 {
		return nil, fmt.Errorf("invalid array")
	}
	args := make([]string, n)
	for i := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 3 || line[0] != '$' {
			return nil, fmt.Errorf("expected string")
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 || size > 1<<20 {
			return nil, fmt.Errorf("invalid string")
		}
		raw := make([]byte, size+2)
		if _, err = io.ReadFull(reader, raw); err != nil {
			return nil, err
		}
		args[i] = string(raw[:size])
	}
	return args, nil
}

func TestSecurityTokens(t *testing.T) {
	sealer, err := newTokenSealer(strings.Repeat("s", 32))
	if err != nil {
		t.Fatal(err)
	}
	id := identity{Username: "alice", Password: "do-not-expose"}
	now := time.Now()
	token, err := sealer.seal(id, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := base64.RawURLEncoding.DecodeString(token)
	if strings.Contains(string(decoded), id.Password) {
		t.Fatal("credentials were not encrypted")
	}
	if got, err := sealer.open(token, now); err != nil || got != id {
		t.Fatal("identity did not round-trip", err)
	}
	second, _ := sealer.seal(id, now.Add(time.Minute))
	if second == token {
		t.Fatal("nonce reused")
	}
	other, _ := newTokenSealer(strings.Repeat("x", 32))
	for _, bad := range []string{"", "not-base64!", token[:len(token)/2], strings.Repeat("x", 17000)} {
		if _, err := sealer.open(bad, now); err == nil {
			t.Fatal("bad token accepted")
		}
	}
	decoded[len(decoded)-1] ^= 1
	if _, err := sealer.open(base64.RawURLEncoding.EncodeToString(decoded), now); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := sealer.open(token, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired token accepted")
	}
	if _, err := other.open(token, now); err == nil {
		t.Fatal("foreign key accepted")
	}
	if _, err := newTokenSealer("short"); err == nil {
		t.Fatal("weak configured secret accepted")
	}
}

func TestSecurityRedisTLSAndCredentials(t *testing.T) {
	certFile, _, cert, _ := securityCertificate(t)
	uri := securityBackend(t, cert)
	sealer, _ := newTokenSealer("")
	validator := &aclValidator{uri: uri, caFile: certFile, sealer: sealer, ttl: time.Minute, now: time.Now}
	token, err := validator.Validate("alice", "test-password")
	if err != nil {
		t.Fatal(err)
	}
	got, err := validator.IsValid(token)
	if err != nil || got.(identity).Username != "alice" {
		t.Fatal("wrong principal", err)
	}
	for _, id := range []identity{{Username: "alice", Password: "wrong"}, {Username: "alice"}, {Password: "test-password"}} {
		if _, err := validator.Validate(id.Username, id.Password); status.Code(err) != codes.Unauthenticated {
			t.Fatal("bad credentials accepted", err)
		}
	}
	validator.caFile = ""
	if _, err := validator.Validate("alice", "test-password"); status.Code(err) != codes.Unavailable {
		t.Fatal("untrusted Redis certificate accepted", err)
	}
	if _, err := clientOptionsWithCA(uri+"?skip_verify=true", identity{}, certFile); err == nil {
		t.Fatal("disabled TLS verification accepted")
	}
	if _, err := clientOptions("redis://alice:password@localhost", identity{}); err == nil {
		t.Fatal("plaintext credential transport accepted")
	}
	opts, err := clientOptionsWithCA(uri, identity{Username: "bob", Password: "test-password"}, certFile)
	if err != nil || opts.Username != "bob" || opts.TLSConfig.InsecureSkipVerify || opts.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("lost identity or verified TLS", err)
	}
}

type securitySnapshotDB struct{ adbc.DatabaseWithContext }

func (*securitySnapshotDB) Open(context.Context) (adbc.ConnectionWithContext, error) {
	return &snapshotTestConn{}, nil
}
func (*securitySnapshotDB) Close(context.Context) error { return nil }
func (*snapshotTestStmt) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil), nil
}

func TestSecurityFlightTLSAuthAndSnapshotOwnership(t *testing.T) {
	certFile, keyFile, cert, roots := securityCertificate(t)
	uri := securityBackend(t, cert)
	cfg := Config{Auth: AuthRedisACL, TLSCertFile: certFile, TLSKeyFile: keyFile, RedisURI: uri, ADBCOptions: map[string]string{"adbc.redis.tls_ca_file": certFile}, Logger: testLogger(t)}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Native snapshot semantics have separate real-Redis transport qualification.
	// Here only the native driver is a double; TLS, Redis login, cookies, bearer
	// authentication and Flight RPC routing are the production implementations.
	srv.tenants.open = func(ctx context.Context, id identity) (*tenant, error) {
		opts, err := cfg.redisOptions(id)
		if err != nil {
			return nil, err
		}
		db := &securitySnapshotDB{}
		return &tenant{id: id, db: db, pool: newConnPool(db, 1, 4), rdb: goredis.NewClient(opts), streams: make(chan struct{}, 2)}, nil
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Shutdown)
	connect := func(trust *x509.CertPool, hostname string) *flightsql.Client {
		client, err := flight.NewClientWithMiddlewareCtx(context.Background(), lis.Addr().String(), nil, []flight.ClientMiddleware{flight.CreateClientMiddleware(flight.NewCookieMiddleware())}, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: trust, ServerName: hostname})))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		return &flightsql.Client{Client: client, Alloc: memory.DefaultAllocator}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := connect(roots, "127.0.0.1")
	if _, err := owner.GetXdbcTypeInfo(ctx, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("anonymous Flight request accepted", err)
	}
	if _, err := owner.Client.AuthenticateBasicToken(ctx, "alice", "wrong"); status.Code(err) != codes.Unauthenticated {
		t.Fatal("wrong password accepted", err)
	}
	authed, err := owner.Client.AuthenticateBasicToken(ctx, "alice", "test-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.GetXdbcTypeInfo(authed, nil); err != nil {
		t.Fatal(err)
	}
	command := snapshotCommand("acquire")
	if result, err := owner.SetSessionOptions(authed, command); err != nil || len(result.Errors) > 0 {
		t.Fatal("snapshot acquire over TLS failed", err)
	}
	options, err := owner.GetSessionOptions(authed, &flight.GetSessionOptionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	raw := options.SessionOptions[snapshotStatusOption].GetStringValue()
	var view readSnapshot
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		t.Fatal(err)
	}
	bound := metadata.AppendToOutgoingContext(authed, snapshotHeader, view.Handle)
	info, err := owner.Execute(bound, "SELECT v FROM t")
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := owner.DoGet(bound, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	rows := int64(0)
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	err = rdr.Err()
	rdr.Release()
	if err != nil || rows != 3 {
		t.Fatal("snapshot read failed", rows, err)
	}
	stranger := connect(roots, "127.0.0.1")
	strangerCtx, err := stranger.Client.AuthenticateBasicToken(ctx, "bob", "test-password")
	if err != nil {
		t.Fatal(err)
	}
	strangerCtx = metadata.AppendToOutgoingContext(strangerCtx, snapshotHeader, view.Handle)
	if _, err := stranger.GetXdbcTypeInfo(strangerCtx, nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("foreign handle accepted", err)
	}
	if _, err := owner.SetSessionOptions(bound, snapshotCommand("release")); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.GetXdbcTypeInfo(bound, nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("released view allowed live fallback", err)
	}
	plain, err := flight.NewClientWithMiddlewareCtx(context.Background(), lis.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	short, stop := context.WithTimeout(context.Background(), time.Second)
	_, err = plain.AuthenticateBasicToken(short, "alice", "test-password")
	stop()
	if err == nil {
		t.Fatal("plaintext Flight authentication accepted")
	}
	for name, client := range map[string]*flightsql.Client{"untrusted": connect(x509.NewCertPool(), "127.0.0.1"), "wrong hostname": connect(roots, "wrong.invalid")} {
		short, stop := context.WithTimeout(context.Background(), time.Second)
		_, err := client.Client.AuthenticateBasicToken(short, "alice", "test-password")
		stop()
		if err == nil {
			t.Fatalf("%s TLS accepted", name)
		}
	}
}
