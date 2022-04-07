package jamcache

import (
	"testing"
	"time"
)

func sleepUntilRotates(c *Cache) {
	time.Sleep(c.head.Sub(time.Now()))
}

func TestCache_rotate1(t *testing.T) {
	c := New(1, 100*time.Millisecond)

	c.Set(1, 10)

	sleepUntilRotates(c)

	if _, ok := c.Get(1); ok {
		t.Fatal("shouldn't be there anymore")
	}

	c.Set(1, 20)

	if _, ok := c.Get(1); !ok {
		t.Fatal("should be there")
	}

	sleepUntilRotates(c)

	if _, ok := c.Get(1); ok {
		t.Fatal("shouldn't be there anymore")
	}
}

func TestCache_rotate10(t *testing.T) {
	c := New(10, 100*time.Millisecond)
	c.Set(1, 100)
	c.Set(2, 200)

	for n := 0; n < 2*len(c.gens); n++ {
		// expected value for key 1
		ev := 100 + n

		if v, ok := c.Get(1); !ok || v != ev {
			t.Fatal(n, ok, v, ev)
		}

		// key 2 is only present for cache duration
		if n < len(c.gens) {
			// checks size (key 1 in all generations + key 2)
			if size := c.Len(); size != (n+1)+1 {
				t.Error("invalid size", n, size)
			}
			if v, ok := c.Get(2); !ok || v != 200 {
				t.Fatal(n, ok, v)
			}
		} else {
			// now size only contains key 1 in every generation
			if size := c.Len(); size != len(c.gens) {
				t.Error("invalid size", n, size, c.gens)
			}
			if v, ok := c.Get(2); ok || v != nil {
				t.Fatal(n, ok, v)
			}
		}

		sleepUntilRotates(c)

		// Update key 1
		c.Set(1, 100+n+1)
	}
}

func TestCache_maxItems(t *testing.T) {
	c := New(2, 100*time.Millisecond)
	c.MaxItems = 1
	c.Set(1, 100)
	c.Set(2, 200)
	c.Set(3, 300)

	if _, ok := c.Get(1); ok {
		t.Error("item is there")
	}
	if _, ok := c.Get(2); ok {
		t.Error("item is there")
	}
	if _, ok := c.Get(3); !ok {
		t.Error("item is not there")
	}
	if size := c.Len(); size != c.MaxItems {
		t.Error("invalid size", size)
	}
}

func TestCache_expireAll(t *testing.T) {
	c := New(3, time.Millisecond)

	c.Set(1, 10)

	time.Sleep(10 * time.Millisecond)

	if _, ok := c.Get(1); ok {
		t.Fatal("shouldn't be there anymore")
	}

	c.Set(1, 20)

	if v, _ := c.Get(1); v != 20 {
		t.Fatal(v)
	}
}
