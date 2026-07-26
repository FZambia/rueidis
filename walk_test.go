package rueidis

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/rueidis/internal/cmds"
)

// collectWalk walks a reply, collecting the values fn selects by returning
// true for a string, and reports the stream error.
func collectWalk(t *testing.T, p *pipe, pl *pool, cmd Completed, fn WalkFunc) ([]string, error) {
	t.Helper()
	s := p.DoStream(context.Background(), pl, cmd)
	var got []string
	err := s.Walk(func(i WalkInfo) (bool, error) {
		keep, err := fn(i)
		if err != nil {
			return false, err
		}
		if keep && i.IsString() {
			got = append(got, string(i.Peek))
		}
		return keep, nil
	}, nil)
	if err == nil {
		err = s.Error() // io.EOF once the reply has been read
	}
	return got, err
}

func TestWalk(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	t.Run("descends selectively and yields chosen strings", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			// [ "meta", [ ["a","1"], ["b","2"] ] ]
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				strmsg('$', "meta"),
				slicemsg('*', []RedisMessage{
					slicemsg('*', []RedisMessage{strmsg('$', "a"), strmsg('$', "1")}),
					slicemsg('*', []RedisMessage{strmsg('$', "b"), strmsg('$', "2")}),
				}),
			}))
		}()

		got, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(i WalkInfo) (bool, error) {
			switch i.Depth {
			case 0, 2:
				return i.IsAggregate(), nil
			case 1: // skip "meta", descend into the pairs
				return i.Index == 1 && i.IsAggregate(), nil
			case 3: // values only
				return i.Index%2 == 1, nil
			}
			return false, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if strings.Join(got, ",") != "1,2" {
			t.Fatalf("got %v, want [1 2]", got)
		}
	})

	t.Run("declined subtrees are still consumed and the connection is reusable", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				slicemsg('*', []RedisMessage{strmsg('$', "deep"), {typ: ':', intlen: 7}}),
				strmsg('$', "tail"),
			}))
			mock.Expect("GET", "next").ReplyBlobString("after")
		}()

		// Decline everything below the top level.
		got, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(i WalkInfo) (bool, error) {
			return i.Depth == 0 && i.IsAggregate(), nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("nothing should have been yielded, got %v", got)
		}
		if p.Error() != nil {
			t.Fatalf("connection should stay healthy, got %v", p.Error())
		}

		// The nested subtree must have been drained, leaving the stream aligned
		// for the next command on the same connection.
		next, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "next"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("follow-up failed: %v", err)
		}
		if strings.Join(next, ",") != "after" {
			t.Fatalf("connection desynced: got %v", next)
		}
	})

	t.Run("reports depth and index correctly", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				strmsg('$', "x"),
				slicemsg('*', []RedisMessage{strmsg('$', "y")}),
			}))
		}()

		var trace []string
		_, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(i WalkInfo) (bool, error) {
			trace = append(trace, string(i.Type)+":"+itoa(i.Depth)+":"+itoa(i.Index))
			return true, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		want := "*:0:0 $:1:0 *:1:1 $:2:0"
		if strings.Join(trace, " ") != want {
			t.Fatalf("trace = %q, want %q", strings.Join(trace, " "), want)
		}
	})

	t.Run("out of band push before the reply is not mistaken for it", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			// A BCAST invalidation landing between the command and its reply.
			mock.Expect("GET", "k").
				Reply(slicemsg('>', []RedisMessage{
					strmsg('+', "invalidate"),
					slicemsg('*', []RedisMessage{strmsg('$', "k")}),
				})).
				Reply(slicemsg('*', []RedisMessage{strmsg('$', "real")}))
		}()

		got, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if strings.Join(got, ",") != "real" {
			t.Fatalf("got %v, want [real]; the push must not reach the callback", got)
		}
	})

	t.Run("attribute before an element is skipped", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k")
			// *2 where the second element is decorated with an attribute.
			_, _ = mock.conn.Write([]byte("*2\r\n$1\r\na\r\n|1\r\n$3\r\nttl\r\n:10\r\n$1\r\nb\r\n"))
		}()

		got, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if strings.Join(got, ",") != "a,b" {
			t.Fatalf("got %v, want [a b]", got)
		}
	})

	t.Run("nulls report identically whatever the wire encoding", func(t *testing.T) {
		// The same semantic null arrives as _ under RESP3 and as $-1 or *-1
		// under RESP2. The walker normalises them, so a callback passed as the
		// resp2 argument sees the same WalkInfo whichever encoding arrived.
		for _, tc := range []struct{ name, wire string }{
			{"resp3 null", "*3\r\n_\r\n$1\r\na\r\n_\r\n"},
			{"resp2 null bulk", "*3\r\n$-1\r\n$1\r\na\r\n$-1\r\n"},
			{"resp2 null array", "*3\r\n*-1\r\n$1\r\na\r\n*-1\r\n"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				p, mock, cancel, _ := setup(t, ClientOption{})
				defer cancel()
				pl := newPool(1, nil, 0, 0, nil)

				go func() {
					mock.Expect("GET", "k")
					_, _ = mock.conn.Write([]byte(tc.wire))
				}()

				var nulls []WalkInfo
				s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
				if err := s.Walk(func(i WalkInfo) (bool, error) {
					if i.IsNull() {
						nulls = append(nulls, i)
					}
					return true, nil
				}, nil); err != nil {
					t.Fatalf("Walk: %v", err)
				}
				if len(nulls) != 2 {
					t.Fatalf("expected 2 nulls, got %d", len(nulls))
				}
				for _, n := range nulls {
					if n.IntVal != 0 || n.Peek != nil {
						t.Fatalf("null must report the same regardless of encoding, got %+v", n)
					}
				}
			})
		}
	})

	t.Run("same shape declared: fn serves RESP2 too", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)
		p.version = 5 // what _newPipe records when RESP2 is negotiated

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				strmsg('$', "a"), strmsg('$', "b"),
			}))
		}()

		var got []string
		fn := func(i WalkInfo) (bool, error) {
			if i.IsString() {
				got = append(got, string(i.Peek))
			}
			return true, nil
		}
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		if err := s.Walk(fn, fn); err != nil {
			t.Fatalf("Walk(fn, fn) must run on RESP2: %v", err)
		}
		if strings.Join(got, ",") != "a,b" {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("reshaping declared: the resp2 callback is the one that runs", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)
		p.version = 5

		go func() {
			mock.Expect("GET", "k").ReplyBlobString("x")
		}()

		var ran string
		resp3 := func(_ WalkInfo) (bool, error) { ran = "resp3"; return false, nil }
		resp2 := func(_ WalkInfo) (bool, error) { ran = "resp2"; return false, nil }
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		if err := s.Walk(resp3, resp2); err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if ran != "resp2" {
			t.Fatalf("the resp2 callback should have run, got %q", ran)
		}
	})

	t.Run("a RESP2 connection is refused with a clear error", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)
		p.version = 5 // exactly what _newPipe records when RESP2 is negotiated

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				strmsg('$', "a"), strmsg('$', "b"),
			}))
			mock.Expect("GET", "next").ReplyBlobString("after")
		}()

		called := false
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		// nil resp2 is the explicit declaration that this parser is RESP3 only.
		err := s.Walk(func(_ WalkInfo) (bool, error) { called = true; return true, nil }, nil)
		if !errors.Is(err, ErrWalkRESP3Required) {
			t.Fatalf("expected ErrWalkRESP3Required, got %v", err)
		}
		if called {
			t.Fatal("no callback may run when the resp2 slot is nil")
		}
		if p.Error() != nil {
			t.Fatalf("refusing must not harm the connection: %v", p.Error())
		}

		// The reply was drained, so the connection serves the next command.
		p.version = 6
		next, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "next"}), func(_ WalkInfo) (bool, error) { return true, nil })
		if !errors.Is(err, io.EOF) || strings.Join(next, ",") != "after" {
			t.Fatalf("connection unusable after refusal: %v %v", next, err)
		}
	})

	t.Run("an error element inside an aggregate is reported with IsError", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			// RESP3 allows error elements inside aggregates, e.g. EXEC results.
			mock.Expect("GET", "k")
			_, _ = mock.conn.Write([]byte("*2\r\n$2\r\nok\r\n-ERR partial\r\n"))
		}()

		var errText string
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		if err := s.Walk(func(i WalkInfo) (bool, error) {
			if i.IsError() {
				errText = string(i.Peek)
			}
			return true, nil
		}, nil); err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if errText != "ERR partial" {
			t.Fatalf("nested error should be reported through IsError, got %q", errText)
		}
		if p.Error() != nil {
			t.Fatalf("a nested error is data, not a failure: %v", p.Error())
		}
	})

	t.Run("error reply surfaces without visiting", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k").ReplyError("ERR nope")
		}()

		called := false
		_, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(_ WalkInfo) (bool, error) {
			called = true
			return true, nil
		})
		var rerr *RedisError
		if !errors.As(err, &rerr) || rerr.string() != "nope" {
			t.Fatalf("expected RedisError, got %v", err)
		}
		if called {
			t.Fatal("callback must not run for an error reply")
		}
	})

	t.Run("an error reply leaves the connection usable", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			// Long enough to exceed the test harness's read buffer, which is
			// how a real MOVED arrives.
			mock.Expect("GET", "k").ReplyError("MOVED 1234 127.0.0.1:7002")
			mock.Expect("GET", "next").ReplyBlobString("after")
		}()

		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		called := false
		err := s.Walk(func(_ WalkInfo) (bool, error) { called = true; return true, nil }, nil)

		rerr, ok := IsRedisErr(err)
		if !ok {
			t.Fatalf("expected a RedisError, got %v", err)
		}
		if addr, moved := rerr.IsMoved(); !moved || addr != "127.0.0.1:7002" {
			t.Fatalf("MOVED must survive intact, got moved=%v addr=%q", moved, addr)
		}
		if called {
			t.Fatal("an error reply must not reach the callback")
		}
		// An error reply is an ordinary reply that was read in full, so the
		// connection has to stay usable; a redirect would otherwise cost one.
		if p.Error() != nil {
			t.Fatalf("connection should stay healthy, got %v", p.Error())
		}
		next, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "next"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, io.EOF) || strings.Join(next, ",") != "after" {
			t.Fatalf("connection unusable after an error reply: %v %v", next, err)
		}
	})

	t.Run("a simple string longer than the read buffer is read whole", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		want := strings.Repeat("x", 300) // the harness buffers far less than this
		go func() {
			mock.Expect("GET", "k").ReplyString(want)
		}()

		var got string
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		if err := s.Walk(func(i WalkInfo) (bool, error) {
			if i.IsString() {
				got = string(i.Peek)
			}
			return true, nil
		}, nil); err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if got != want {
			t.Fatalf("got %d bytes, want %d", len(got), len(want))
		}
	})

	t.Run("null reply surfaces as Nil", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k")
			_, _ = mock.conn.Write([]byte("_\r\n"))
		}()

		_, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if err != Nil {
			t.Fatalf("expected Nil, got %v", err)
		}
	})

	t.Run("a second Walk reports rather than doing nothing", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{strmsg('$', "a")}))
		}()

		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		seen := 0
		if err := s.Walk(func(_ WalkInfo) (bool, error) { seen++; return true, nil }, nil); err != nil {
			t.Fatalf("first Walk: %v", err)
		}
		if seen == 0 {
			t.Fatal("first Walk should have visited the reply")
		}
		again := 0
		err := s.Walk(func(_ WalkInfo) (bool, error) { again++; return true, nil }, nil)
		if !errors.Is(err, ErrStreamConsumed) {
			t.Fatalf("second Walk should report ErrStreamConsumed, got %v", err)
		}
		if again != 0 {
			t.Fatal("second Walk should not visit anything")
		}
	})

	t.Run("an error from the callback still drains the reply", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k").Reply(slicemsg('*', []RedisMessage{
				strmsg('$', "a"), strmsg('$', "b"), strmsg('$', "c"),
			}))
			mock.Expect("GET", "next").ReplyBlobString("after")
		}()

		boom := errors.New("boom")
		seen := 0
		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		err := s.Walk(func(i WalkInfo) (bool, error) {
			if i.IsString() {
				seen++
				return false, boom // give up on the first value
			}
			return true, nil
		}, nil)
		if !errors.Is(err, boom) {
			t.Fatalf("expected the callback error back, got %v", err)
		}
		if seen != 1 {
			t.Fatalf("reporting should stop at the first error, saw %d", seen)
		}
		if p.Error() != nil {
			t.Fatalf("a callback error must not kill the connection, got %v", p.Error())
		}

		// The remaining two values must have been drained, leaving the stream
		// aligned for the next command.
		next, err := collectWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "next"}), func(_ WalkInfo) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("follow-up failed: %v", err)
		}
		if strings.Join(next, ",") != "after" {
			t.Fatalf("connection desynced after a callback error: got %v", next)
		}
	})

	// An unknown RESP type has to be reportable. The error is wrapped with the
	// offending type byte so the message can name it, which means callers need
	// errors.Is and therefore need the sentinel exported.
	t.Run("an unknown type is reported as ErrWalkUnsupportedType", func(t *testing.T) {
		p, mock, cancel, _ := setup(t, ClientOption{})
		defer cancel()
		pl := newPool(1, nil, 0, 0, nil)

		go func() {
			mock.Expect("GET", "k")
			// Written raw: the harness's reply writer only knows real RESP
			// types, and an unknown one is exactly what this exercises.
			_, _ = mock.conn.Write([]byte("@nonsense\r\n"))
		}()

		s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
		err := s.Walk(func(_ WalkInfo) (bool, error) { return true, nil }, nil)
		if !errors.Is(err, ErrWalkUnsupportedType) {
			t.Fatalf("expected ErrWalkUnsupportedType, got %v", err)
		}
		if !strings.Contains(err.Error(), `'@'`) {
			t.Fatalf("the error should name the type it met, got %v", err)
		}
	})

	// A multi command stream carries several responses on one connection, and
	// only the last of them may release it. An error that leaves the connection
	// aligned — a redis error reply, a null, an error from the callback — must
	// therefore not end the stream: the responses behind it are still on the
	// wire, and releasing early hands the connection back to the pool with them
	// unread, so the next command to borrow it reads someone else's reply. That
	// trips the ring's ordering check and takes the process down with
	// "protocol bug, message handled out of order".
	t.Run("a non fatal error does not end a multi command stream", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			first func(*redisExpect)
			check func(*testing.T, error)
		}{
			{
				name:  "error reply",
				first: func(e *redisExpect) { e.ReplyError("WRONGTYPE first failed") },
				check: func(t *testing.T, err error) {
					if rerr, ok := IsRedisErr(err); !ok || rerr.string() != "WRONGTYPE first failed" {
						t.Fatalf("expected the redis error back, got %v", err)
					}
				},
			},
			{
				name:  "null reply",
				first: func(e *redisExpect) { e.Reply(RedisMessage{typ: typeNull}) },
				check: func(t *testing.T, err error) {
					if !errors.Is(err, Nil) {
						t.Fatalf("expected Nil, got %v", err)
					}
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				p, mock, cancel, _ := setup(t, ClientOption{})
				defer cancel()
				pl := newPool(1, nil, 0, 0, nil)

				go func() {
					tc.first(mock.Expect("GET", "a"))
					mock.Expect("GET", "b").ReplyBlobString("second")
				}()

				s := p.DoMultiStream(context.Background(), pl,
					cmds.NewCompleted([]string{"GET", "a"}),
					cmds.NewCompleted([]string{"GET", "b"}))

				tc.check(t, s.Walk(func(_ WalkInfo) (bool, error) { return true, nil }, nil))
				if !s.HasNext() {
					t.Fatal("the second response is still unread, so the stream must continue")
				}

				var got []string
				if err := s.Walk(func(i WalkInfo) (bool, error) {
					if i.IsString() {
						got = append(got, string(i.Peek))
					}
					return true, nil
				}, nil); err != nil {
					t.Fatalf("second Walk: %v", err)
				}
				if strings.Join(got, ",") != "second" {
					t.Fatalf("the second response was lost, got %v", got)
				}
			})
		}

		// The callback error variant needs its own reply shape: something with
		// values in it for the callback to trip over.
		t.Run("callback error", func(t *testing.T) {
			p, mock, cancel, _ := setup(t, ClientOption{})
			defer cancel()
			pl := newPool(1, nil, 0, 0, nil)

			go func() {
				mock.Expect("GET", "a").Reply(slicemsg('*', []RedisMessage{
					strmsg('$', "x"), strmsg('$', "y"),
				}))
				mock.Expect("GET", "b").ReplyBlobString("second")
			}()

			s := p.DoMultiStream(context.Background(), pl,
				cmds.NewCompleted([]string{"GET", "a"}),
				cmds.NewCompleted([]string{"GET", "b"}))

			boom := errors.New("boom")
			if err := s.Walk(func(i WalkInfo) (bool, error) {
				if i.IsString() {
					return false, boom
				}
				return true, nil
			}, nil); !errors.Is(err, boom) {
				t.Fatalf("expected the callback error back, got %v", err)
			}
			if !s.HasNext() {
				t.Fatal("the second response is still unread, so the stream must continue")
			}

			var got []string
			if err := s.Walk(func(i WalkInfo) (bool, error) {
				if i.IsString() {
					got = append(got, string(i.Peek))
				}
				return true, nil
			}, nil); err != nil {
				t.Fatalf("second Walk: %v", err)
			}
			if strings.Join(got, ",") != "second" {
				t.Fatalf("the second response was lost, got %v", got)
			}
		})
	})
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// walkRec is a flattened WalkInfo, captured so a whole reply can be asserted in
// one comparison.
type walkRec struct {
	Type   byte
	IntVal int64
	Depth  int
	Index  int
	Peek   string
}

// recordWalk descends the entire reply, capturing every visited message.
func recordWalk(t *testing.T, p *pipe, pl *pool, cmd Completed) ([]walkRec, error) {
	t.Helper()
	s := p.DoStream(context.Background(), pl, cmd)
	var recs []walkRec
	err := s.Walk(func(i WalkInfo) (bool, error) {
		recs = append(recs, walkRec{i.Type, i.IntVal, i.Depth, i.Index, string(i.Peek)})
		return true, nil // descend into every aggregate
	}, nil)
	if err == nil {
		err = s.Error()
	}
	return recs, err
}

// replyRaw answers the next command with exact RESP bytes, so a test can send
// reply shapes the message builder does not construct: doubles, booleans, big
// numbers, sets and streamed aggregates.
func replyRaw(mock *redisMock, raw string, expected ...string) {
	mock.Expect(expected...)
	_, _ = io.WriteString(mock.conn, raw)
}

// TestWalkValueTypes covers the RESP3 reply types that ship with Walk but had
// no direct coverage: maps, sets, doubles, booleans, big numbers and verbatim
// strings. Each is fed as raw bytes and the whole visit is asserted at once.
func TestWalkValueTypes(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	for _, tc := range []struct {
		name string
		raw  string
		want []walkRec
	}{
		{
			name: "map reports pair count then key/value children",
			raw:  "%1\r\n$1\r\nk\r\n$1\r\nv\r\n",
			want: []walkRec{
				{typeMap, 1, 0, 0, ""},
				{typeBlobString, 1, 1, 0, "k"},
				{typeBlobString, 1, 1, 1, "v"},
			},
		},
		{
			name: "set reports element count then children",
			raw:  "~2\r\n$1\r\na\r\n$1\r\nb\r\n",
			want: []walkRec{
				{typeSet, 2, 0, 0, ""},
				{typeBlobString, 1, 1, 0, "a"},
				{typeBlobString, 1, 1, 1, "b"},
			},
		},
		{
			name: "double is a string message carrying the digits",
			raw:  ",3.14\r\n",
			want: []walkRec{{typeFloat, 4, 0, 0, "3.14"}},
		},
		{
			name: "boolean true is an int message with value 1",
			raw:  "#t\r\n",
			want: []walkRec{{typeBool, 1, 0, 0, ""}},
		},
		{
			name: "boolean false is an int message with value 0",
			raw:  "#f\r\n",
			want: []walkRec{{typeBool, 0, 0, 0, ""}},
		},
		{
			name: "big number keeps its digits as the payload",
			raw:  "(12345\r\n",
			want: []walkRec{{typeBigNumber, 5, 0, 0, "12345"}},
		},
		{
			name: "verbatim string keeps its format prefix",
			raw:  "=15\r\ntxt:hello world\r\n",
			want: []walkRec{{typeVerbatimString, 15, 0, 0, "txt:hello world"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, mock, cancel, _ := setup(t, ClientOption{})
			defer cancel()
			pl := newPool(1, nil, 0, 0, nil)

			go replyRaw(mock, tc.raw, "GET", "k")
			got, err := recordWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}))
			if !errors.Is(err, io.EOF) {
				t.Fatalf("expected io.EOF, got %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("walk mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// TestWalkOversizedBlobCopyPath exercises the branch where a blob string is
// larger than the read buffer and so is copied rather than borrowed. The whole
// value must come back intact, not a short view.
func TestWalkOversizedBlobCopyPath(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	p, mock, cancel, _ := setup(t, ClientOption{ReadBufferEachConn: 1 << 10})
	defer cancel()
	pl := newPool(1, nil, 0, 0, nil)

	big := strings.Repeat("x", 100*1024) // far larger than the read buffer
	go func() { mock.Expect("GET", "k").Reply(strmsg('$', big)) }()

	got, err := recordWalk(t, p, pl, cmds.NewCompleted([]string{"GET", "k"}))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	want := []walkRec{{typeBlobString, int64(len(big)), 0, 0, big}}
	if !reflect.DeepEqual(got, want) {
		var gotLen, gotType int
		if len(got) == 1 {
			gotLen, gotType = len(got[0].Peek), int(got[0].Type)
		}
		t.Fatalf("oversized blob not returned intact: got %d records (peek len %d, type %c), want 1 blob of len %d",
			len(got), gotLen, byte(gotType), len(big))
	}
}

// TestWalkReportsIntAndBool covers integer and boolean elements and the IsInt
// predicate, including its negative cases.
func TestWalkReportsIntAndBool(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	p, mock, cancel, _ := setup(t, ClientOption{})
	defer cancel()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "k")
		_, _ = mock.conn.Write([]byte("*2\r\n:42\r\n#t\r\n")) // [ int 42, bool true ]
	}()

	var ints []WalkInfo
	s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
	err := s.Walk(func(i WalkInfo) (bool, error) {
		if i.Depth == 1 {
			if !i.IsInt() {
				t.Errorf("depth-1 element %d should be IsInt, type %c", i.Index, i.Type)
			}
			ints = append(ints, i)
		}
		return true, nil
	}, nil)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(ints) != 2 || ints[0].IntVal != 42 || ints[1].IntVal != 1 {
		t.Fatalf("got %+v, want [42, bool-true(1)]", ints)
	}
	// IsInt is false for non-numeric types.
	if (WalkInfo{Type: typeBlobString}).IsInt() || (WalkInfo{Type: typeArray}).IsInt() {
		t.Fatal("a string or aggregate must not report IsInt")
	}
}

// TestWalkTopLevelBlobError covers the depth-0 blob error (!) reply, exercising
// readErrorAt: the error surfaces from Walk and the callback never runs.
func TestWalkTopLevelBlobError(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	p, mock, cancel, _ := setup(t, ClientOption{})
	defer cancel()
	pl := newPool(1, nil, 0, 0, nil)

	go func() {
		mock.Expect("GET", "k")
		_, _ = mock.conn.Write([]byte("!12\r\nERR my error\r\n"))
	}()

	called := false
	s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
	err := s.Walk(func(WalkInfo) (bool, error) {
		called = true
		return true, nil
	}, nil)
	if called {
		t.Fatal("callback must not run for a top-level error reply")
	}
	if err == nil || !strings.Contains(err.Error(), "my error") {
		t.Fatalf("want the blob error surfaced, got %v", err)
	}
	if _, ok := IsRedisErr(err); !ok {
		t.Fatalf("error should be a redis error, got %T", err)
	}
}

// TestWalkTopLevelNull covers a null as the whole reply, in all three wire
// encodings: Walk returns Nil and the callback never runs.
func TestWalkTopLevelNull(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	for _, tc := range []struct{ name, wire string }{
		{"resp3 null", "_\r\n"},
		{"resp2 null bulk", "$-1\r\n"},
		{"resp2 null array", "*-1\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, mock, cancel, _ := setup(t, ClientOption{})
			defer cancel()
			pl := newPool(1, nil, 0, 0, nil)

			go func() {
				mock.Expect("GET", "k")
				_, _ = mock.conn.Write([]byte(tc.wire))
			}()

			called := false
			s := p.DoStream(context.Background(), pl, cmds.NewCompleted([]string{"GET", "k"}))
			err := s.Walk(func(WalkInfo) (bool, error) {
				called = true
				return true, nil
			}, nil)
			if called {
				t.Fatal("callback must not run for a top-level null reply")
			}
			if !errors.Is(err, Nil) {
				t.Fatalf("want Nil, got %v", err)
			}
		})
	}
}
