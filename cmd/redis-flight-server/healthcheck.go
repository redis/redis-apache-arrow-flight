package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// healthCheckTimeout bounds a -healthcheck, dial included.
const healthCheckTimeout = 3 * time.Second

// checkHealth asks the server listening on listen whether it is serving,
// with the gRPC health service. certFile is the server's TLS certificate,
// if it has one: the check trusts that certificate alone, since it dials
// the loopback address, which the certificate usually doesn't name.
func checkHealth(listen, certFile string) error {
	addr, err := probeAddr(listen)
	if err != nil {
		return err
	}
	creds := insecure.NewCredentials()
	if certFile != "" {
		if creds, err = pinnedCreds(certFile); err != nil {
			return err
		}
	}
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()
	res, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check of %s: %w", addr, err)
	}
	if st := res.GetStatus(); st != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("%s is %s", addr, st)
	}
	fmt.Println("SERVING")
	return nil
}

// probeAddr is the address to check a server listening on listen at: an
// unspecified host (all interfaces) is checked on the loopback address.
func probeAddr(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("invalid -listen address %q: %w", listen, err)
	}
	if port == "0" {
		return "", fmt.Errorf("-listen %q has no fixed port to check", listen)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	}
	return net.JoinHostPort(host, port), nil
}

// pinnedCreds are TLS credentials that accept only the certificate in
// certFile (its first, the server's own).
func pinnedCreds(certFile string) (credentials.TransportCredentials, error) {
	b, err := os.ReadFile(certFile)
	if err != nil {
		return nil, err
	}
	var pinned []byte
	for len(b) > 0 {
		var block *pem.Block
		if block, b = pem.Decode(b); block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			pinned = block.Bytes
			break
		}
	}
	if pinned == nil {
		return nil, fmt.Errorf("no certificate in %s", certFile)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		// Verified below, against the pinned certificate instead of a CA
		// and a name.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 || !bytes.Equal(raw[0], pinned) {
				return errors.New("the server's certificate isn't the one in -tls-cert")
			}
			return nil
		},
	}), nil
}
