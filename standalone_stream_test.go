package rueidis

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// A Valkey REDIRECT reply is only seen once the caller reads, so following it
// means re-pointing the primary and re-issuing from inside WriteTo, the same
// way clusterClient streams follow MOVED.
func TestStandaloneDoStreamFollowsRedirect(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("REDIRECT 127.0.0.1:1")
		mock.Expect("GET", "a").ReplyBlobString("from new primary")
	}()

	var dials []string
	opt := &ClientOption{InitAddress: []string{":0"}}
	opt.Standalone.EnableRedirect = true
	s, err := newStandaloneClient(opt,
		func(dst string, _ *ClientOption) conn {
			dials = append(dials, dst)
			return &mockConn{
				DoStreamFn: func(cmd Completed) RedisResultStream {
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newStandaloneClient: %v", err)
	}
	defer s.Close()

	st := s.DoStream(context.Background(), s.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := st.WriteTo(&buf); werr != nil {
		t.Fatalf("REDIRECT should have been followed, got %v", werr)
	}
	if buf.String() != "from new primary" {
		t.Fatalf("got %q", buf.String())
	}
	if len(dials) != 2 || dials[1] != "127.0.0.1:1" {
		t.Fatalf("the new primary named by REDIRECT should have been dialed, got %v", dials)
	}
}

// Errors that are not REDIRECT surface after one attempt, and the connection
// stays usable for the next command.
func TestStandaloneDoStreamOrdinaryError(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("WRONGTYPE nope")
		mock.Expect("GET", "b").ReplyBlobString("after")
	}()

	var streams int64
	opt := &ClientOption{InitAddress: []string{":0"}}
	opt.Standalone.EnableRedirect = true
	s, err := newStandaloneClient(opt,
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newStandaloneClient: %v", err)
	}
	defer s.Close()

	st := s.DoStream(context.Background(), s.B().Get().Key("a").Build())
	var buf bytes.Buffer
	_, werr := st.WriteTo(&buf)
	if werr == nil || !strings.Contains(werr.Error(), "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE, got %v", werr)
	}
	if got := atomic.LoadInt64(&streams); got != 1 {
		t.Fatalf("an ordinary error must not be re-issued, got %d attempts", got)
	}

	st2 := s.DoStream(context.Background(), s.B().Get().Key("b").Build())
	var buf2 bytes.Buffer
	if _, werr := st2.WriteTo(&buf2); werr != nil || buf2.String() != "after" {
		t.Fatalf("connection unusable after error: %q %v", buf2.String(), werr)
	}
}

// Without EnableRedirect the path is byte for byte the old one: no pinning, no
// hook, inner client recycles the command.
func TestStandaloneDoStreamWithoutRedirectMode(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyBlobString("plain") }()

	opt := &ClientOption{InitAddress: []string{":0"}}
	s, err := newStandaloneClient(opt,
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoStreamFn: func(cmd Completed) RedisResultStream {
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newStandaloneClient: %v", err)
	}
	defer s.Close()

	st := s.DoStream(context.Background(), s.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := st.WriteTo(&buf); werr != nil || buf.String() != "plain" {
		t.Fatalf("got %q %v", buf.String(), werr)
	}
}

// A REDIRECT whose re-issue comes back errored must surface that error and
// recycle the pinned command, rather than leak it: WriteTo returns on the
// errored stream without calling the hook again.
func TestStandaloneDoStreamRecyclesOnErroredRedirectHop(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyError("REDIRECT 127.0.0.1:1") }()

	hopErr := errors.New("redirect target unreachable")
	var streams int64
	opt := &ClientOption{InitAddress: []string{":0"}}
	opt.Standalone.EnableRedirect = true
	s, err := newStandaloneClient(opt,
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoStreamFn: func(cmd Completed) RedisResultStream {
					if atomic.AddInt64(&streams, 1) == 1 {
						return p.DoStream(context.Background(), pl, cmd)
					}
					return NewErrorResultStream(hopErr)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newStandaloneClient: %v", err)
	}
	defer s.Close()

	st := s.DoStream(context.Background(), s.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := st.WriteTo(&buf); werr != hopErr {
		t.Fatalf("WriteTo should surface the errored redirect hop, got %v", werr)
	}
	if got := atomic.LoadInt64(&streams); got != 2 {
		t.Fatalf("expected the redirect target to be dialed, got %d streams", got)
	}
}
