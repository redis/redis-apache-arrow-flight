package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// refusalGate forces definite native publication CAS refusals. It substitutes
// the native seal operation (the real pre-CAS side effect) and returns zero only
// after a successful seal. Skipping seal would create an impossible schedule:
// the driver's metadata-only rebase requires the original workspace sealed.
// The driver itself emits the IO/40001 after exhausting its metadata-only rebase
// attempts. SQL, TLS, authentication, storage and all other replies are real.
type refusalGate struct {
	mu            sync.Mutex
	armed         bool
	skip, refused int
	workspace     string
}

func (g *refusalGate) refuse(args []string) bool {
	if len(args) < 4 || (args[0] != "eval" && args[0] != "evalsha") {
		return false
	}
	n, err := strconv.Atoi(args[2])
	i := 3 + n
	if err != nil || n < 1 || len(args) <= i+2 || args[3] != "adbc:{meta}:snapshot:catalog" || args[i] != "publish" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return false
	}
	if g.skip > 0 {
		g.skip--
		return false
	}
	if g.workspace == "" {
		g.workspace = args[i+2]
	}
	if g.workspace != args[i+2] {
		return false
	}
	g.refused++
	args[i] = "seal"
	return true
}

func (g *refusalGate) arm(skip int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed, g.skip, g.refused, g.workspace = true, skip, 0, ""
}

func (g *refusalGate) disarm(t *testing.T) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = false
	if g.refused == 0 {
		t.Fatal("native publication fault was not exercised")
	}
	t.Logf("injected %d definite publication refusals for one workspace", g.refused)
}

// copyRESP preserves RESP2/3 reply frames, including native errors and nested
// search results. The proxy serializes commands even when the client pipelines.
func copyRESP(dst io.Writer, src *bufio.Reader) error {
	line, err := src.ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) < 3 {
		return fmt.Errorf("short RESP frame")
	}
	if _, err := io.WriteString(dst, line); err != nil {
		return err
	}
	switch line[0] {
	case '$', '!', '=':
		n, err := strconv.ParseInt(strings.TrimSpace(line[1:]), 10, 64)
		if err != nil {
			return err
		}
		if n >= 0 {
			_, err = io.CopyN(dst, src, n+2)
			return err
		}
	case '*', '~', '>', '%', '|':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return err
		}
		if line[0] == '%' || line[0] == '|' {
			n *= 2
		}
		for range max(0, n) {
			if err := copyRESP(dst, src); err != nil {
				return err
			}
		}
	}
	return nil
}

func publicationProxy(t *testing.T, uri string) (string, string, *refusalGate) {
	t.Helper()
	opts, err := clientOptionsWithCA(uri, identity{}, os.Getenv("REDIS_TLS_CA_FILE"))
	if err != nil {
		t.Fatal("cannot parse backend configuration")
	}
	certFile, _, cert, _ := securityCertificate(t)
	lis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	gate := &refusalGate{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	connections := map[net.Conn]bool{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			client, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[client] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				defer func() { mu.Lock(); delete(connections, client); mu.Unlock() }()
				backend, err := tls.Dial("tcp", opts.Addr, opts.TLSConfig)
				if err != nil {
					t.Logf("publication proxy backend TLS connection: %v", err)
					return
				}
				defer backend.Close()
				requests, replies := bufio.NewReader(client), bufio.NewReader(backend)
				for {
					args, err := securityRESP(requests)
					if err != nil {
						return
					}
					args[0] = strings.ToLower(args[0])
					refuse := gate.refuse(args)
					var command bytes.Buffer
					fmt.Fprintf(&command, "*%d\r\n", len(args))
					for _, arg := range args {
						fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(arg), arg)
					}
					if _, err := backend.Write(command.Bytes()); err != nil {
						return
					}
					if refuse {
						var reply bytes.Buffer
						if err := copyRESP(&reply, replies); err != nil {
							return
						}
						if reply.String() == ":1\r\n" {
							reply.Reset()
							reply.WriteString(":0\r\n")
						}
						if _, err := client.Write(reply.Bytes()); err != nil {
							return
						}
					} else if err := copyRESP(client, replies); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		<-done
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	u, _ := url.Parse(uri)
	u.Host = lis.Addr().String()
	return u.String(), certFile, gate
}

func TestIntegrationSessionPublicationRefusal(t *testing.T) {
	uri, ca, gate := publicationProxy(t, redisURI(t))
	ctx := context.Background()
	srv, lis := startServer(t, Config{RedisURI: uri, ADBCOptions: map[string]string{"adbc.redis.tls_ca_file": ca}})
	c := plainClient(t, lis)
	token := askSessionToken(t, ctx, c)
	ctx = withSession(ctx, token)
	name := uniqueName("it_flight_refusal_temp")
	schema := uniqueName("it_flight_refusal_schema")
	// A permanent relation establishes an owned schema for SET SCHEMA.
	qualified := schema + ".anchor"
	update(t, ctx, c, "CREATE TABLE "+qualified+" (id BIGINT)")
	t.Cleanup(func() { _, _ = c.ExecuteUpdate(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	update(t, ctx, c, "SET TIME ZONE 'America/New_York'")
	update(t, ctx, c, "SET SCHEMA '"+schema+"'")
	update(t, ctx, c, "CREATE TEMP TABLE "+name+" (id BIGINT)")
	update(t, ctx, c, "INSERT INTO "+name+" VALUES (1)")
	srv.sessions.mu.Lock()
	sess := srv.sessions.sessions[token]
	srv.sessions.mu.Unlock()
	sess.mu.Lock()
	original := sess.conn
	sess.mu.Unlock()
	checkState := func(want ...string) {
		t.Helper()
		wantRows(t, query(t, ctx, c, "SHOW TIME ZONE"), "America/New_York")
		wantRows(t, query(t, ctx, c, "SHOW search_path"), schema)
		wantRows(t, query(t, ctx, c, "SELECT id FROM "+name+" ORDER BY id"), want...)
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.conn != original || sess.token != token || !sess.temp {
			t.Fatal("session identity or dedicated connection changed")
		}
	}
	// Expected NotFound alone must preserve the same live SQL session.
	_, err := c.Execute(ctx, "SELECT * FROM "+uniqueName("it_flight_missing"))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing table: %v", err)
	}
	checkState("1")
	assertRefusal := func(err error) {
		t.Helper()
		gate.disarm(t)
		if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), publicationRefusal().Msg) || !strings.Contains(err.Error(), "40001") {
			t.Fatalf("not the native refusal: %v", err)
		}
	}
	gate.arm(0)
	_, err = c.ExecuteUpdate(ctx, "INSERT INTO "+name+" VALUES (2)")
	assertRefusal(err)
	checkState("1")
	// Caller retries a fresh statement; no server-side replay.
	var headers metadata.MD
	if _, err := c.ExecuteUpdate(ctx, "INSERT INTO "+name+" VALUES (2)", grpc.Header(&headers)); err != nil {
		t.Fatal(err)
	}
	if sessionToken(headers) != "" {
		t.Fatal("retry rotated the session cookie")
	}
	checkState("1", "2")
	// A later script failure must never replay its already committed prefix.
	gate.arm(1)
	_, err = c.ExecuteUpdate(ctx, "INSERT INTO "+name+" VALUES (3); INSERT INTO "+name+" VALUES (4)")
	assertRefusal(err)
	checkState("1", "2", "3")
	update(t, ctx, c, "INSERT INTO "+name+" VALUES (4)")
	checkState("1", "2", "3", "4")
	// Arrow input was consumed before publication; the failed ingest must not
	// be replayed. A fresh reader is required for the explicit next attempt.
	ingest := func() error {
		rec := int64Record(t, []string{"id"}, []int64{5})
		defer rec.Release()
		rdr, _ := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
		defer rdr.Release()
		_, err := c.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{Table: name, Temporary: true,
			TableDefinitionOptions: &flightsql.TableDefinitionOptions{IfExists: flightsql.TableDefinitionOptionsTableExistsOptionAppend,
				IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionFail}})
		return err
	}
	gate.arm(0)
	err = ingest()
	assertRefusal(err)
	checkState("1", "2", "3", "4")
	if err := ingest(); err != nil {
		t.Fatal(err)
	}
	checkState("1", "2", "3", "4", "5")
}
