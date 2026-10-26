package ratelimit

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestLimiter(rate float64, burst int) (*Limiter, *clock) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	l := New(rate, burst)
	l.now = clk.now
	return l, clk
}

func TestAllowsBurstThenBlocks(t *testing.T) {
	l, _ := newTestLimiter(1, 3)

	for i := 0; i < 3; i++ {
		if !l.Allow("ip") {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if l.Allow("ip") {
		t.Fatal("4th request should be blocked")
	}
}

func TestRefillsOverTime(t *testing.T) {
	l, clk := newTestLimiter(2, 2) // 2 tokens per second

	l.Allow("ip")
	l.Allow("ip")
	if l.Allow("ip") {
		t.Fatal("bucket should be empty")
	}

	clk.advance(500 * time.Millisecond) // +1 token
	if !l.Allow("ip") {
		t.Fatal("should have refilled one token")
	}
	if l.Allow("ip") {
		t.Fatal("only one token should have refilled")
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	l, clk := newTestLimiter(10, 2)
	l.Allow("ip")

	clk.advance(time.Hour)
	allowed := 0
	for i := 0; i < 5; i++ {
		if l.Allow("ip") {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed %d after a long idle, want burst of 2", allowed)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := newTestLimiter(1, 1)

	if !l.Allow("a") || l.Allow("a") {
		t.Fatal("key a should allow exactly one")
	}
	if !l.Allow("b") {
		t.Fatal("key b should be unaffected by key a")
	}
}

func TestZeroRateDisablesLimiting(t *testing.T) {
	l, _ := newTestLimiter(0, 1)
	for i := 0; i < 1000; i++ {
		if !l.Allow("ip") {
			t.Fatal("disabled limiter blocked a request")
		}
	}
}

func TestIdleBucketsAreSwept(t *testing.T) {
	l, clk := newTestLimiter(1, 1)
	l.Allow("old")

	clk.advance(11 * time.Minute)
	l.Allow("new") // triggers the sweep

	if l.size() != 1 {
		t.Fatalf("size = %d, want only the fresh bucket", l.size())
	}
}
