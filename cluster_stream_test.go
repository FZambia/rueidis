package rueidis

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// A MOVED reply is only seen once the caller reads, so following it means
// re-issuing the command from inside WriteTo. streamTo reports an error
// response before writing anything, which is what makes that safe.
func TestClusterDoStreamFollowsMoved(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("MOVED 0 127.0.0.1:1")
		mock.Expect("GET", "a").ReplyBlobString("value")
	}()

	var streams int64
	var dsts []string
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(dst string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					dsts = append(dsts, dst)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	n, werr := s.WriteTo(&buf)

	if werr != nil {
		t.Fatalf("MOVED should have been followed, got %v after %d streams", werr, streams)
	}
	if buf.String() != "value" {
		t.Fatalf("got %q, want the reply from the redirected node", buf.String())
	}
	if n != 5 {
		t.Fatalf("wrote %d bytes", n)
	}
	if got := atomic.LoadInt64(&streams); got != 2 {
		t.Fatalf("expected two attempts, got %d", got)
	}
	if len(dsts) != 2 || dsts[1] != "127.0.0.1:1" {
		t.Fatalf("second attempt should go where MOVED pointed, got %v", dsts)
	}
}

// Without a redirect the stream behaves as before, and the command is still
// recycled even though the hook now holds it for a possible retry.
func TestClusterDoStreamWithoutRedirect(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyBlobString("plain") }()

	var streams int64
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != nil {
		t.Fatalf("unexpected error: %v", werr)
	}
	if buf.String() != "plain" {
		t.Fatalf("got %q", buf.String())
	}
	if got := atomic.LoadInt64(&streams); got != 1 {
		t.Fatalf("a reply that is not a redirect must not be re-issued, got %d", got)
	}
}

// ASKING is only meaningful on the connection carrying the command it
// precedes, so the two go out together and ASKING's reply is dropped before the
// stream is handed back positioned at the one the caller wants.
func TestClusterDoStreamFollowsAsk(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("ASK 0 127.0.0.1:1")
		mock.Expect("ASKING").ReplyString("OK")
		mock.Expect("GET", "a").ReplyBlobString("asked")
	}()

	var dsts []string
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(dst string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					dsts = append(dsts, dst)
					return p.DoStream(context.Background(), pl, cmd)
				},
				DoMultiStreamFn: func(multi ...Completed) MultiRedisResultStream {
					dsts = append(dsts, dst)
					if len(multi) != 2 || multi[0].Commands()[0] != "ASKING" {
						t.Errorf("ASKING must precede the command on one connection, got %v", multi)
					}
					return p.DoMultiStream(context.Background(), pl, multi...)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != nil {
		t.Fatalf("ASK should have been followed, got %v", werr)
	}
	if buf.String() != "asked" {
		t.Fatalf("got %q, want the reply that followed ASKING", buf.String())
	}
	if len(dsts) != 2 || dsts[1] != "127.0.0.1:1" {
		t.Fatalf("the retry should go where ASK pointed, got %v", dsts)
	}
}

// A node that keeps redirecting must not loop forever.
func TestClusterDoStreamBoundsRedirects(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	// MaxMovedRedirections 2 allows the first attempt and two more.
	const want = 3
	go func() {
		for i := 0; i < want; i++ {
			mock.Expect("GET", "a").ReplyError("MOVED 0 127.0.0.1:1")
		}
	}()

	var streams int64
	opt := &ClientOption{InitAddress: []string{":0"}}
	opt.ClusterOption.MaxMovedRedirections = 2
	client, err := newClusterClient(opt,
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	_, werr := s.WriteTo(&buf)

	if got := atomic.LoadInt64(&streams); got != want {
		t.Fatalf("expected %d attempts before giving up, got %d", want, got)
	}
	rerr, ok := IsRedisErr(werr)
	if !ok {
		t.Fatalf("the last redirect should surface, got %v", werr)
	}
	if _, moved := rerr.IsMoved(); !moved {
		t.Fatalf("expected the MOVED to surface, got %v", werr)
	}
}

// TRYAGAIN and friends go through the retry handler rather than the redirect
// counter, and the command is re-issued after a fresh pick.
func TestClusterDoStreamRetriesTryAgain(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("TRYAGAIN nope")
		mock.Expect("GET", "a").ReplyBlobString("eventually")
	}()

	var streams int64
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != nil {
		t.Fatalf("TRYAGAIN should have been retried, got %v", werr)
	}
	if buf.String() != "eventually" {
		t.Fatalf("got %q", buf.String())
	}
	if got := atomic.LoadInt64(&streams); got != 2 {
		t.Fatalf("expected one retry, got %d attempts", got)
	}
}

// A rejected ASKING is ignored, exactly as clusterClient.do ignores the ASKING
// reply: the command reply right behind it is what drives the outcome. If the
// target really cannot serve the slot, that reply is a redirect or an error of
// its own and the loop handles it.
func TestClusterDoStreamAskingRejectedIsIgnored(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("ASK 0 127.0.0.1:1")
		mock.Expect("ASKING").ReplyError("ERR asking rejected")
		mock.Expect("GET", "a").ReplyBlobString("asked anyway")
	}()

	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					return p.DoStream(context.Background(), pl, cmd)
				},
				DoMultiStreamFn: func(multi ...Completed) MultiRedisResultStream {
					return p.DoMultiStream(context.Background(), pl, multi...)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != nil {
		t.Fatalf("a rejected ASKING must not abort the command, got %v", werr)
	}
	if buf.String() != "asked anyway" {
		t.Fatalf("the command reply should reach the caller, got %q", buf.String())
	}
}

// streamTo cannot stream aggregate replies and reports a client side error for
// them. That error must surface immediately: it is not a redis error, so
// re-issuing the command would fetch the same unsupported reply forever.
func TestClusterDoStreamUnsupportedReplyNotRetried(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").Reply(slicemsg('*', []RedisMessage{
			strmsg('$', "not"), strmsg('$', "streamable"),
		}))
	}()

	var streams int64
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	// GET is retryable, which is exactly what makes an unbounded retry loop
	// reachable if the redirect gate accepts client side errors.
	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	_, werr := s.WriteTo(&buf)

	if werr == nil || !strings.Contains(werr.Error(), "unsupported") {
		t.Fatalf("the client side error should surface, got %v", werr)
	}
	if got := atomic.LoadInt64(&streams); got != 1 {
		t.Fatalf("a client side error must not be retried, attempts=%d", got)
	}
	if buf.Len() != 0 {
		t.Fatalf("nothing should have been written, got %q", buf.String())
	}
}

// When a retry cannot be routed at all, the reason has to reach the caller.
// clusterClient.do returns the pick error rather than the reply that prompted
// the retry; the stream path has to do the same, or the caller is left looking
// at a TRYAGAIN when the real problem is that the slot has no node.
func TestClusterDoStreamSurfacesPickError(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyError("TRYAGAIN nope") }()

	// The topology empties out between the first attempt and the retry, so the
	// retry's pick finds no node for the slot.
	var emptied atomic.Bool
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult {
					if emptied.Load() {
						return NewResult(slicemsg('*', []RedisMessage{}), nil)
					}
					return slotsResp
				},
				DoStreamFn: func(cmd Completed) RedisResultStream {
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	// The command goes out against the topology as it stands.
	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())

	// Then the slot map empties, so the retry's pick has nowhere to route to.
	// Refreshed synchronously rather than left to lazyRefresh, which is delayed
	// and would make the outcome a race.
	emptied.Store(true)
	if err := client.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	var buf bytes.Buffer
	_, werr := s.WriteTo(&buf)
	if !errors.Is(werr, ErrNoSlot) {
		t.Fatalf("the pick error should surface, got %v", werr)
	}
}

// A MOVED whose re-issue comes back errored (the target failed before the
// command reached the wire) must surface that error and recycle the command the
// hook was holding for the retry, rather than leak it: WriteTo returns on the
// errored stream without calling the hook again.
func TestClusterDoStreamRecyclesOnErroredMoveHop(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyError("MOVED 0 127.0.0.1:1") }()

	hopErr := errors.New("redirect target unreachable")
	var streams int64
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
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
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != hopErr {
		t.Fatalf("WriteTo should surface the errored redirect hop, got %v", werr)
	}
	if got := atomic.LoadInt64(&streams); got != 2 {
		t.Fatalf("expected the MOVED target to be dialed, got %d streams", got)
	}
}

// Same as above for the TRYAGAIN retry branch: an errored retry hop surfaces and
// the command is recycled.
func TestClusterDoStreamRecyclesOnErroredRetryHop(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() { mock.Expect("GET", "a").ReplyError("TRYAGAIN nope") }()

	hopErr := errors.New("retry target unreachable")
	var streams int64
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(_ string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
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
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	if _, werr := s.WriteTo(&buf); werr != hopErr {
		t.Fatalf("WriteTo should surface the errored retry hop, got %v", werr)
	}
	if got := atomic.LoadInt64(&streams); got != 2 {
		t.Fatalf("expected the retry to re-issue, got %d streams", got)
	}
}

// TestClusterDoStreamFollowsMultipleMoved exercises a two-hop MOVED chain
// (A -> B -> C -> value), which the single-hop tests never cover. Because the
// mock pool has size 1 and a nil make func, a hop that fails to return its wire
// would make the next Acquire panic — so a clean pass also proves the wire is
// balanced across every hop, not just recycled at the end.
func TestClusterDoStreamFollowsMultipleMoved(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, _, closeConn := setup(t, ClientOption{})
	defer closeConn()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "a").ReplyError("MOVED 0 127.0.0.1:1")
		mock.Expect("GET", "a").ReplyError("MOVED 0 127.0.0.1:2")
		mock.Expect("GET", "a").ReplyBlobString("value")
	}()

	var streams int64
	var dsts []string
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(dst string, _ *ClientOption) conn {
			return &mockConn{
				DoFn: func(_ Completed) RedisResult { return slotsResp },
				DoStreamFn: func(cmd Completed) RedisResultStream {
					atomic.AddInt64(&streams, 1)
					dsts = append(dsts, dst)
					return p.DoStream(context.Background(), pl, cmd)
				},
			}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	defer client.Close()

	s := client.DoStream(context.Background(), client.B().Get().Key("a").Build())
	var buf bytes.Buffer
	n, werr := s.WriteTo(&buf)

	if werr != nil {
		t.Fatalf("both MOVEDs should have been followed, got %v after %d streams", werr, streams)
	}
	if buf.String() != "value" {
		t.Fatalf("got %q, want the reply from the twice-redirected node", buf.String())
	}
	if n != 5 {
		t.Fatalf("wrote %d bytes", n)
	}
	if got := atomic.LoadInt64(&streams); got != 3 {
		t.Fatalf("expected three attempts, got %d", got)
	}
	if len(dsts) != 3 || dsts[1] != "127.0.0.1:1" || dsts[2] != "127.0.0.1:2" {
		t.Fatalf("attempts should chase each MOVED in turn, got %v", dsts)
	}
	// The wire must be back in the pool, balanced across all three hops.
	if got := s.HasNext(); got {
		t.Fatalf("stream should be exhausted after the value")
	}
}
