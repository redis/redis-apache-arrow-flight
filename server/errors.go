package server

import (
	"context"
	"errors"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus turns an error from the ADBC driver, go-redis or the server
// into a gRPC status error. Errors that already are status errors pass
// through.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	msg := err.Error()
	// Redis's ACL errors, from the driver or go-redis.
	if strings.Contains(msg, "NOPERM") {
		return status.Error(codes.PermissionDenied, msg)
	}
	if strings.Contains(msg, "WRONGPASS") || strings.Contains(msg, "NOAUTH") {
		return status.Error(codes.Unauthenticated, msg)
	}
	if isCursorLimit(err) {
		return status.Error(codes.ResourceExhausted, msg)
	}
	var ae adbc.Error
	if errors.As(err, &ae) {
		return status.Error(adbcCode(ae.Code), msg)
	}
	return status.Error(codes.Internal, msg)
}

// isCursorLimit reports whether err is RediSearch refusing a query because
// its index has search-index-cursor-limit cursors open (128 by default,
// across every client). The driver reads every table through a cursor, so
// any query on the table fails until some are closed: clients can retry.
func isCursorLimit(err error) bool {
	return err != nil && strings.Contains(err.Error(), "INDEX_CURSOR_LIMIT")
}

// connectStatus is the status for a failure to connect to Redis. Bad or
// changed credentials, missing permissions and cancellation keep their
// codes, so a client logs in again rather than retrying. Anything else
// (connection refused, a timeout) means Redis is unavailable.
func connectStatus(err error) error {
	code := codes.Unavailable
	switch c := status.Code(toStatus(err)); c {
	case codes.Unauthenticated, codes.PermissionDenied, codes.Canceled, codes.DeadlineExceeded:
		code = c
	}
	return status.Errorf(code, "failed to connect to Redis: %v", err)
}

func adbcCode(c adbc.Status) codes.Code {
	switch c {
	case adbc.StatusNotImplemented:
		return codes.Unimplemented
	case adbc.StatusNotFound:
		return codes.NotFound
	case adbc.StatusAlreadyExists:
		return codes.AlreadyExists
	case adbc.StatusInvalidArgument, adbc.StatusInvalidData:
		return codes.InvalidArgument
	case adbc.StatusInvalidState, adbc.StatusIntegrity:
		return codes.FailedPrecondition
	case adbc.StatusIO:
		return codes.Unavailable
	case adbc.StatusCancelled:
		return codes.Canceled
	case adbc.StatusTimeout:
		return codes.DeadlineExceeded
	case adbc.StatusUnauthenticated:
		return codes.Unauthenticated
	case adbc.StatusUnauthorized:
		return codes.PermissionDenied
	case adbc.StatusUnknown:
		return codes.Unknown
	}
	return codes.Internal
}
