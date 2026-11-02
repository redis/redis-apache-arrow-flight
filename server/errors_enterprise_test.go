package server

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestSoftwareCommandPermissionError(t *testing.T) {
	for _, message := range []string{"ERR command 'ping' is not allowed", "reading Redis: ERR command 'set' is not allowed"} {
		if code := status.Code(toStatus(errors.New(message))); code != codes.PermissionDenied {
			t.Fatalf("Software ACL denial mapped to %v", code)
		}
	}
	if code := status.Code(toStatus(errors.New("SQL operation is not allowed"))); code == codes.PermissionDenied {
		t.Fatal("unrelated SQL text was classified as an ACL denial")
	}
}
