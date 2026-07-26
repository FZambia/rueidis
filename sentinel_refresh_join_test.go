package rueidis

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSentinelRefreshRetrySurvivesJoinedFailingFlight: refreshRetry must keep
// retrying when the refresh it waited on was started by someone else and
// failed.
//
// refresh() is deduplicated: if a refresh is already running, a second caller
// just waits for it. The singleflight used to hand such waiters nil even when
// the refresh failed. refreshRetry retries refresh() until it returns nil, so
// after waiting on someone else's failed refresh it stopped retrying — as if
// the topology were fixed.
//
// How this plays out during a Sentinel failover:
//
//  1. The master dies. The dropped sentinel connection spawns refreshRetry —
//     the mechanism responsible for re-resolving the master.
//  2. Every refresh fails until the sentinels elect a new master, which takes
//     seconds.
//  3. Inside that window a TopologyRefreshInterval tick starts a refresh.
//     Unlike refreshRetry, runTopologyRefreshment does not retry a failed
//     refresh — it waits for its next tick.
//  4. refreshRetry calls refresh(), waits on that failing refresh, gets nil,
//     and exits. Now nobody is retrying.
//
// The client then stays connected to the old master until a later tick
// succeeds: recovery waits for the ticker instead of finishing the moment
// the election ends. Until then commands keep going to the demoted node, and
// PUBLISH in particular succeeds on a replica while the message is silently
// lost. Before TopologyRefreshInterval existed this bug was latent: every
// runtime refresh caller was itself a retry loop, so whoever started the
// refresh saw the real error and kept retrying.
//
// The test performs the tick by calling client.refresh() directly — the same
// call runTopologyRefreshment makes — instead of running a real ticker.
// This keeps the timing deterministic, and it keeps the regression visible:
// with a real ticker the NEXT tick would eventually fix the topology and the
// test could not tell whether refreshRetry or the ticker did it.
func TestSentinelRefreshRetrySurvivesJoinedFailingFlight(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	sentinelDown := errors.New("sentinel unavailable")

	// phase 0: initial resolution, master is :1.
	// phase 1: outage — the flight blocks (so a second caller can join), then fails.
	// phase 2: recovered — master is :2.
	var phase atomic.Int32
	var startedOnce sync.Once
	flightStarted := make(chan struct{})
	release := make(chan struct{})

	masterReply := func(addr string) *redisresults {
		return &redisresults{s: []RedisResult{
			{val: slicemsg('*', []RedisMessage{})},
			{val: slicemsg('*', []RedisMessage{strmsg('+', ""), strmsg('+', addr)})},
		}}
	}

	s0 := &mockConn{
		DoFn: func(cmd Completed) RedisResult { return RedisResult{} },
		DoMultiFn: func(multi ...Completed) *redisresults {
			switch phase.Load() {
			case 0:
				return masterReply("1")
			case 1:
				startedOnce.Do(func() { close(flightStarted) })
				<-release
				return &redisresults{s: []RedisResult{{err: sentinelDown}, {err: sentinelDown}}}
			default:
				return masterReply("2")
			}
		},
	}
	node := func() *mockConn {
		return &mockConn{DoFn: func(cmd Completed) RedisResult {
			return RedisResult{val: slicemsg('*', []RedisMessage{strmsg('+', "master")})}
		}}
	}
	m1, m2 := node(), node()

	client, err := newSentinelClient(
		&ClientOption{InitAddress: []string{":0"}},
		func(dst string, opt *ClientOption) conn {
			switch dst {
			case ":0":
				return s0
			case ":1":
				return m1
			case ":2":
				return m2
			}
			return nil
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer client.Close()

	if got := client.mAddr.Load().(string); got != ":1" {
		t.Fatalf("expected initial master :1, got %v", got)
	}

	phase.Store(1)

	// Step 3 of the story above: a tick starts a refresh and will not retry
	// it. It blocks inside listWatch until released, then fails.
	tickDone := make(chan error, 1)
	go func() { tickDone <- client.refresh() }()
	select {
	case <-flightStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("tick refresh never started")
	}

	// refreshRetry arrives while that refresh is still running, so its own
	// refresh() call waits on it instead of starting a new one.
	retryDone := make(chan struct{})
	go func() {
		client.refreshRetry()
		close(retryDone)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for client.sc.suppressing() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("refreshRetry never joined the in-flight refresh")
		}
		time.Sleep(time.Millisecond)
	}

	// Let the shared refresh fail. Any refresh AFTER this point succeeds and
	// finds the new master :2 — so if refreshRetry keeps retrying, it fixes
	// the topology almost immediately.
	phase.Store(2)
	close(release)

	if err := <-tickDone; !errors.Is(err, sentinelDown) {
		t.Fatalf("tick refresh should fail with the sentinel error, got %v", err)
	}

	// refreshRetry must have seen the failure and retried. Before the fix it
	// got nil from the failed refresh it waited on, exited, and mAddr stayed
	// :1 forever.
	deadline = time.Now().Add(5 * time.Second)
	for client.mAddr.Load().(string) != ":2" {
		if time.Now().After(deadline) {
			t.Fatal("refreshRetry stopped retrying after waiting on a failed refresh — master never re-resolved to :2")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-retryDone:
	case <-time.After(3 * time.Second):
		t.Fatal("refreshRetry did not return after successful refresh")
	}
}
