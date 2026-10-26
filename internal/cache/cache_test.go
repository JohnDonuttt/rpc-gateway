package cache

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestCache(max int) (*Cache, *clock) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	c := New(max)
	c.now = clk.now
	return c, clk
}

func TestGetAfterSet(t *testing.T) {
	c, _ := newTestCache(10)
	c.Set("a", []byte("1"), time.Minute)

	if v, ok := c.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("got %q, %v", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("unexpected hit")
	}
}

func TestEntriesExpire(t *testing.T) {
	c, clk := newTestCache(10)
	c.Set("a", []byte("1"), time.Minute)

	clk.advance(59 * time.Second)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expired too early")
	}

	clk.advance(2 * time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatal("should have expired")
	}
	if c.Len() != 0 {
		t.Fatalf("expired entry should be removed, len = %d", c.Len())
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	c, _ := newTestCache(2)
	c.Set("a", []byte("1"), time.Hour)
	c.Set("b", []byte("2"), time.Hour)

	c.Get("a") // a is now more recent than b
	c.Set("c", []byte("3"), time.Hour)

	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should still be cached")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be cached")
	}
}

func TestSetOverwritesAndRefreshesTTL(t *testing.T) {
	c, clk := newTestCache(10)
	c.Set("a", []byte("old"), time.Minute)
	clk.advance(50 * time.Second)
	c.Set("a", []byte("new"), time.Minute)
	clk.advance(50 * time.Second)

	if v, ok := c.Get("a"); !ok || string(v) != "new" {
		t.Fatalf("got %q, %v", v, ok)
	}
	if c.Len() != 1 {
		t.Fatalf("len = %d, want 1", c.Len())
	}
}

func TestDisabledCacheStoresNothing(t *testing.T) {
	c := New(0)
	c.Set("a", []byte("1"), time.Minute)
	if _, ok := c.Get("a"); ok {
		t.Fatal("disabled cache returned a value")
	}
}

func TestZeroTTLIsNotStored(t *testing.T) {
	c, _ := newTestCache(10)
	c.Set("a", []byte("1"), 0)
	if c.Len() != 0 {
		t.Fatal("zero-ttl entry was stored")
	}
}
