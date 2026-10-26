package upstream

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// blockServer is a fake node whose reported block height and availability can be changed mid-test.
type blockServer struct {
	*httptest.Server
	block atomic.Uint64
	down  atomic.Bool
	hits  atomic.Int32
}

func newBlockServer(t *testing.T, block uint64) *blockServer {
	t.Helper()
	s := &blockServer{}
	s.block.Store(block)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		if s.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, s.block.Load())
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *blockServer) upstream(name string) *Upstream { return New(name, s.URL, s.Client()) }

func names(ups []*Upstream) []string {
	out := make([]string, len(ups))
	for i, u := range ups {
		out[i] = u.Name
	}
	return out
}

func TestDoReturnsBodyOnSuccess(t *testing.T) {
	s := newBlockServer(t, 10)
	u := s.upstream("a")

	data, err := u.Do(context.Background(), blockNumberRequest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"jsonrpc":"2.0","id":1,"result":"0xa"}` {
		t.Fatalf("unexpected body %s", data)
	}
	if u.Requests.Load() != 1 || u.Failures.Load() != 0 {
		t.Fatalf("requests=%d failures=%d", u.Requests.Load(), u.Failures.Load())
	}
}

func TestDoTreatsNon200AsRetryable(t *testing.T) {
	s := newBlockServer(t, 10)
	s.down.Store(true)
	u := s.upstream("a")

	_, err := u.Do(context.Background(), blockNumberRequest)

	var re *RetryableError
	if !errors.As(err, &re) || re.Status != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want RetryableError with status 503", err)
	}
	if u.Failures.Load() != 1 {
		t.Fatalf("failures = %d, want 1", u.Failures.Load())
	}
}

func TestCancelledCallerIsNotCountedAsUpstreamFailure(t *testing.T) {
	s := newBlockServer(t, 10)
	u := s.upstream("a")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := u.Do(ctx, blockNumberRequest); err == nil {
		t.Fatal("expected an error")
	}
	if u.Failures.Load() != 0 {
		t.Fatalf("failures = %d, a cancelled caller shouldn't count", u.Failures.Load())
	}
}

func TestProbeRecordsBlockHeight(t *testing.T) {
	s := newBlockServer(t, 12345)
	u := s.upstream("a")
	p := NewPool([]*Upstream{u}, 2, 5, time.Second)

	p.CheckAll(context.Background())

	if u.BlockNumber() != 12345 || !u.Healthy() {
		t.Fatalf("block=%d healthy=%v", u.BlockNumber(), u.Healthy())
	}
}

func TestMarksDownAfterConsecutiveFailuresAndRecovers(t *testing.T) {
	s := newBlockServer(t, 100)
	u := s.upstream("a")
	p := NewPool([]*Upstream{u}, 2, 5, time.Second)

	s.down.Store(true)
	p.CheckAll(context.Background())
	if !u.Healthy() {
		t.Fatal("one failure shouldn't mark it down yet")
	}
	p.CheckAll(context.Background())
	if u.Healthy() {
		t.Fatal("two failures in a row should mark it down")
	}

	s.down.Store(false)
	p.CheckAll(context.Background())
	if !u.Healthy() {
		t.Fatal("a good probe should bring it back")
	}
}

func TestIsolatedFailureDoesNotAccumulate(t *testing.T) {
	s := newBlockServer(t, 100)
	u := s.upstream("a")
	p := NewPool([]*Upstream{u}, 2, 5, time.Second)

	for i := 0; i < 3; i++ {
		s.down.Store(true)
		p.CheckAll(context.Background()) // fail
		s.down.Store(false)
		p.CheckAll(context.Background()) // succeed, resets the streak
	}
	if !u.Healthy() {
		t.Fatal("alternating failures should never reach the threshold")
	}
}

func TestLaggingUpstreamIsTakenOutOfRotation(t *testing.T) {
	fast := newBlockServer(t, 1000)
	slow := newBlockServer(t, 990)
	a, b := fast.upstream("fast"), slow.upstream("slow")
	p := NewPool([]*Upstream{b, a}, 2, 5, time.Second)

	p.CheckAll(context.Background())

	if b.Healthy() {
		t.Fatal("10 blocks behind with maxLag 5 should be unhealthy")
	}
	if !a.Healthy() {
		t.Fatal("the leader should stay healthy")
	}

	slow.block.Store(997) // catches up to within the limit
	p.CheckAll(context.Background())
	if !b.Healthy() {
		t.Fatal("within maxLag it should return to rotation")
	}
}

func TestCandidatesKeepPriorityAndPutUnhealthyLast(t *testing.T) {
	a := New("a", "http://a", nil)
	b := New("b", "http://b", nil)
	c := New("c", "http://c", nil)
	p := NewPool([]*Upstream{a, b, c}, 1, 5, time.Second)

	if got := names(p.Candidates()); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("all healthy: %v", got)
	}

	a.RecordFailure(1)
	if got := names(p.Candidates()); fmt.Sprint(got) != "[b c a]" {
		t.Fatalf("a down: %v", got)
	}

	b.RecordFailure(1)
	c.RecordFailure(1)
	if got := names(p.Candidates()); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("all down should still list everyone in priority order: %v", got)
	}
}

func TestPassiveSuccessDoesNotRevive(t *testing.T) {
	u := New("a", "http://a", nil)
	u.RecordFailure(1)
	u.RecordSuccess()
	if u.Healthy() {
		t.Fatal("only a probe should mark an unhealthy upstream healthy")
	}
}

func TestRunProbesImmediatelyAndStopsOnCancel(t *testing.T) {
	s := newBlockServer(t, 7)
	p := NewPool([]*Upstream{s.upstream("a")}, 2, 5, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx, 10*time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for s.hits.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d probes happened", s.hits.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
