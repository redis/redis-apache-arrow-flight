package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProbeAddr(t *testing.T) {
	for _, tc := range []struct{ listen, want string }{
		{"localhost:8815", "localhost:8815"},
		{"0.0.0.0:8815", "127.0.0.1:8815"},
		{":8815", "127.0.0.1:8815"},
		{"[::]:8815", "[::1]:8815"},
		{"10.1.2.3:9000", "10.1.2.3:9000"},
		{"[fd00::1]:8815", "[fd00::1]:8815"},
	} {
		got, err := probeAddr(tc.listen)
		if err != nil || got != tc.want {
			t.Errorf("probeAddr(%q) = %q, %v; want %q", tc.listen, got, err, tc.want)
		}
	}
	for _, bad := range []string{"8815", "127.0.0.1:0", ""} {
		if got, err := probeAddr(bad); err == nil {
			t.Errorf("probeAddr(%q) = %q, want an error", bad, got)
		}
	}
}

// healthcheck runs the command with -healthcheck and returns its output
// and error (an *exec.ExitError for a non-zero exit).
func healthcheck(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-healthcheck"}, args...)...)
	cmd.Env = append(os.Environ(), "REDIS_FLIGHT_SERVER_TEST_MAIN=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// wantExit1 checks that a -healthcheck failed with output containing want.
func wantExit1(t *testing.T, out string, err error, want string) {
	t.Helper()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
		t.Errorf("-healthcheck: %v, want exit status 1; output:\n%s", err, out)
	}
	if !strings.Contains(out, want) {
		t.Errorf("-healthcheck output %q doesn't mention %q", out, want)
	}
}

func TestHealthcheckFlag(t *testing.T) {
	down := []string{"REDIS_URI=redis://127.0.0.1:1/0"} // nothing listens on port 1

	t.Run("not serving while Redis is down", func(t *testing.T) {
		c := startCommand(t, down)
		out, err := healthcheck(t, "-listen", c.addr)
		wantExit1(t, out, err, "NOT_SERVING")
	})

	t.Run("nothing listening", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		_ = lis.Close()
		start := time.Now()
		out, err := healthcheck(t, "-listen", addr)
		wantExit1(t, out, err, addr)
		if d := time.Since(start); d > 10*time.Second {
			t.Errorf("-healthcheck took %v", d)
		}
	})

	t.Run("TLS, pinned to -tls-cert", func(t *testing.T) {
		dir := t.TempDir()
		cert, key := writeCert(t, dir, "server")
		c := startCommand(t, down, "-tls-cert", cert, "-tls-key", key)
		// The handshake succeeds, so the check gets as far as the status.
		out, err := healthcheck(t, "-listen", c.addr, "-tls-cert", cert)
		wantExit1(t, out, err, "NOT_SERVING")
		// Another certificate is refused.
		other, _ := writeCert(t, dir, "other")
		out, err = healthcheck(t, "-listen", c.addr, "-tls-cert", other)
		wantExit1(t, out, err, "certificate")
		// Without the certificate, a plaintext check of a TLS server fails.
		out, err = healthcheck(t, "-listen", c.addr)
		if err == nil {
			t.Errorf("a plaintext -healthcheck of a TLS server passed: %s", out)
		}
	})
}

// With Redis up, -healthcheck exits 0.
func TestIntegrationHealthcheckFlag(t *testing.T) {
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run the integration tests")
	}
	c := startCommand(t, []string{"REDIS_URI=" + uri})
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := healthcheck(t, "-listen", c.addr)
		if err == nil {
			if strings.TrimSpace(out) != "SERVING" {
				t.Errorf("-healthcheck printed %q, want SERVING", out)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("-healthcheck: %v\n%s", err, out)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// writeCert writes a self-signed certificate for 127.0.0.1 and its key to
// dir, and returns their paths.
func writeCert(t *testing.T, dir, name string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	write := func(path, typ string, b []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(certFile, "CERTIFICATE", der)
	write(keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}
