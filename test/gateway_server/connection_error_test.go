package gateway_server_test

import (
	"errors"
	"fmt"
	"io"
	gw "mixture/src/gateway_server"
	"net"
	"os"
	"testing"
)

// TestCheckIsReadTimeOut 区分真正的超时与普通网络错误，并覆盖带包装层的错误。
func TestCheckIsReadTimeOut(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"EOF", io.EOF, false},
		{"ordinary error", errors.New("read failed"), false},
		{"deadline exceeded", os.ErrDeadlineExceeded, true},
		{"wrapped timeout", fmt.Errorf("read: %w", os.ErrDeadlineExceeded), true},
		{"network timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		{"network error without timeout", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, false},
		{"closed connection", &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}, false},
		{"wrapped non-timeout", fmt.Errorf("read failed: %w", &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gw.CheckIsReadTimeOut(tc.err); got != tc.want {
				t.Fatalf("CheckIsReadTimeOut(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
