package jamcache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sleepUntilRotates[K comparable, V any](c *Cache[K, V]) {
	time.Sleep(c.head.Sub(time.Now()))
}

func TestCache_rotate1(t *testing.T) {
	c := New[int, int](1, 100*time.Millisecond)

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
	c := New[int, int](10, 100*time.Millisecond)
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
			if v, ok := c.Get(2); ok || v != 0 {
				t.Fatal(n, ok, v)
			}
		}

		sleepUntilRotates(c)

		// Update key 1
		c.Set(1, 100+n+1)
	}
}

func countNonEmptyGens[K comparable, V any](c *Cache[K, V]) int {
	res := 0

	for _, g := range c.gens {
		if len(g) > 0 {
			res++
		}
	}

	return res
}

func TestCache_numOfGens(t *testing.T) {
	c := New[int, *int](3, 100*time.Millisecond)

	checkGens := func(n int) error {
		if got := countNonEmptyGens(c); got != n {
			return fmt.Errorf("found %d generations, expected %d >> %v", got, n, c.gens)
		}
		return nil
	}

	if err := checkGens(0); err != nil {
		t.Error(err)
	}

	c.Set(1, nil)

	if err := checkGens(1); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)
	c.Set(2, nil)

	if err := checkGens(2); err != nil {
		t.Error(err)
	}

	c.Set(3, nil)

	if err := checkGens(2); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)
	c.Set(4, nil)

	if err := checkGens(3); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)
	c.Set(5, nil)

	if err := checkGens(3); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)
	// this time run Get, we should also run GC
	_, _ = c.Get(0)

	if err := checkGens(2); err != nil {
		t.Error(err)
	}

	_, _ = c.Get(0)

	if err := checkGens(2); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)

	_, _ = c.Get(0)
	if err := checkGens(1); err != nil {
		t.Error(err)
	}

	sleepUntilRotates(c)
	_, _ = c.Get(0)

	if err := checkGens(0); err != nil {
		t.Error(err)
	}

	// Add again

	for n := 0; n < 5; n++ {
		sleepUntilRotates(c)
		c.Set(1, nil)
	}

	if err := checkGens(3); err != nil {
		t.Error(err)
	}

	// sleep until should be empty
	time.Sleep(c.genDur * 3)

	// make sure we clear the cache on an operation
	c.Get(0)
	if err := checkGens(0); err != nil {
		t.Error(err)
	}
}

func TestCache_maxItems(t *testing.T) {
	c := New[int, int](2, 100*time.Millisecond)
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
	c := New[int, int](3, time.Millisecond)

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

func TestCache_GetOrSetOnce_oneByOne(t *testing.T) {
	c := New[int, int](1, time.Hour)

	for k := 0; k < 10; k++ {
		v, err := c.GetOrSetOnce(nil, 1, func() (int, error) { return 10 + k, nil })
		if err != nil {
			t.Fatal(err)
		}
		// we should only get the very first value
		if v != 10 {
			t.Errorf("[%d] unexpected value: %v", k, v)
		}
	}
}

func TestCache_GetOrSetOnce_oneByOneMultipleRoutines(t *testing.T) {
	c := New[int, int](1, time.Hour)

	for r := 0; r < 10; r++ {
		r := r
		t.Run(fmt.Sprintf("r=%d", r), func(t *testing.T) {
			t.Parallel()

			for k := 0; k < 1000; k++ {
				v, err := c.GetOrSetOnce(nil, r, func() (int, error) { return 1000 + r + k, nil })
				if err != nil {
					t.Fatal(err)
				}
				// we should only get the very first value
				if v != 1000+r {
					t.Errorf("[%d] unexpected value: %v", k, v)
				}
			}
		})
	}
}

func TestCache_GetOrSetOnce_random(t *testing.T) {
	c := New[int, int](1, time.Hour)

	size := 100
	access := make([]int64, size)

	var wg sync.WaitGroup
	for r := 0; r < size*100; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := rand.Intn(size)
			got, err := c.GetOrSetOnce(nil, key, func() (int, error) {
				atomic.AddInt64(&access[key], 1)
				return -key, nil
			})
			if err != nil {
				panic(err)
			}
			if got != -key {
				t.Errorf("got %d, expected %d", got, -key)
			}
		}()
	}

	wg.Wait()

	// make sure we called every func once
	for n := range access {
		if access[n] != 1 {
			t.Errorf("unpexted number of access at %d: %d", n, access[n])
		}
	}

	if s := c.Len(); s != size {
		t.Errorf("unexpected cache size: %d", s)
	}

	// check the values
	for key := 0; key < size; key++ {
		v, ok := c.Get(key)
		if ok == false || v != -key {
			t.Errorf("%d: expected %d, got %v [%v]", key, -key, v, ok)
		}
	}
}

func TestCache_GetOrSetOnce_error(t *testing.T) {
	c := New[int, int](1, time.Hour)
	someError := errors.New("some error")

	for n := 0; n < 100; n++ {
		n := n

		// Set value for n=10, otherwise error. The function shouln't be called
		// after the value was returned (as it will be in cache).
		v, err := c.GetOrSetOnce(nil, 1, func() (int, error) {
			if n != 10 {
				return 0, someError
			}
			if n > 10 {
				t.Errorf("unexpected call for n=%d", n)
			}
			return n, nil
		})

		// check we get error for the first 10 calls, then the value
		if n < 10 {
			if err != someError {
				t.Errorf("[%d] expected error, got: %v [with value: %v]", n, err, v)
			}
		} else {
			if v != 10 || err != nil {
				t.Errorf("expected value 10, got %d [err=%v]", v, err)
			}
		}
	}
}

func TestCache_GetOrSetOnce_errorWithWaiter(t *testing.T) {
	c := New[int, int](1, time.Hour)
	someError := errors.New("some error")

	res := make(chan interface{})

	v, err := c.GetOrSetOnce(nil, 1, func() (int, error) {
		// run another setter in the background
		go func() {
			v, err := c.GetOrSetOnce(nil, 1, func() (int, error) {
				return 20, nil
			})
			if err != nil {
				t.Error(err)
			}
			res <- v
		}()

		time.Sleep(time.Millisecond)

		// return error from the first call
		return 10, someError
	})

	// outer call returns error
	if err != someError {
		t.Error(err)
	}
	if v != 10 {
		t.Error(v)
	}

	// make sure we get the valid value from the waiter (inner call)
	if v := <-res; v.(int) != 20 {
		t.Errorf("unexpected value: %v", v)
	}
}

func TestCache_GetOrSetOnce_cancelWaiter(t *testing.T) {
	c := New[int, int](1, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())

	waiterError := make(chan error)
	sleep := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		v, err := c.GetOrSetOnce(ctx, 1, func() (int, error) {
			go func() {
				v, err := c.GetOrSetOnce(ctx, 1, func() (int, error) {
					t.Error("this shouldn't be called")
					return 0, nil
				})
				if v != 0 {
					t.Error(v)
				}
				waiterError <- err
			}()

			<-sleep
			return 10, nil
		})

		if v != 10 || err != nil {
			t.Error(v, err)
		}
	}()

	// cancel context (should affect waiter only)
	cancel()

	if err := <-waiterError; err != context.Canceled {
		t.Error(err)
	}

	// wake up outer call and wait for it to finish
	sleep <- struct{}{}
	wg.Wait()
}

func BenchmarkCache_GetOrSetOnce_uniqueKeys(b *testing.B) {
	c := New[int64, int](3, time.Hour)

	var key int64

	for n := 0; n < b.N; n++ {
		var wg sync.WaitGroup
		for r := 0; r < 10; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 100; k++ {
					// generate ubnique key for all operations
					atomic.AddInt64(&key, 1)
					_, err := c.GetOrSetOnce(nil, key, func() (int, error) {
						return 0, nil
					})
					if err != nil {
						panic(err)
					}
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkCache_GetOrSetOnce_10keys(b *testing.B) {
	var key int64

	for n := 0; n < b.N; n++ {
		c := New[int64, int](10, time.Microsecond)

		var wg sync.WaitGroup
		for r := 0; r < 10; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 1000; k++ {
					// choose from 10 keys
					atomic.AddInt64(&key, 1)
					key = key % 10
					_, err := c.GetOrSetOnce(nil, key, func() (int, error) {
						return 0, nil
					})
					if err != nil {
						panic(err)
					}
				}
			}()
		}
		wg.Wait()
	}
}

func TestCache_default(t *testing.T) {
	testFunc := func(t *testing.T, c *Cache[string, string]) {
		if v, ok := c.Get("key"); v != "" || ok {
			t.Error(v, ok)
		}

		c.Set("key", "val")

		// Set shouldn't set anything
		if v, ok := c.Get("key"); v != "" || ok {
			t.Error(v, ok)
		}

		// GetOrSetOnce returns loaded value
		if v, err := c.GetOrSetOnce(nil, "key2", func() (string, error) {
			return "val2", nil
		}); err != nil || v != "val2" {
			t.Error(v, err)
		}
	}

	t.Run("nil", func(t *testing.T) {
		testFunc(t, nil)
	})
	t.Run("empty", func(t *testing.T) {
		testFunc(t, &Cache[string, string]{})
	})
	t.Run("new-0-0", func(t *testing.T) {
		testFunc(t, New[string, string](0, 0))
	})
	t.Run("new-0-1", func(t *testing.T) {
		testFunc(t, New[string, string](0, 1))
	})
}

func TestCache_noDuration(t *testing.T) {
	testFunc := func(t *testing.T, c *Cache[string, string]) {
		if v, ok := c.Get("key"); v != "" || ok {
			t.Error(v, ok)
		}

		c.Set("key", "val")

		if v, ok := c.Get("key"); !ok || v != "val" {
			t.Error(v, ok)
		}

		if v, err := c.GetOrSetOnce(nil, "key2", func() (string, error) {
			return "val2", nil
		}); err != nil || v != "val2" {
			t.Error(v, err)
		}
	}

	t.Run("new-1-0", func(t *testing.T) {
		testFunc(t, New[string, string](1, 0))
	})
	t.Run("new-2-0", func(t *testing.T) {
		testFunc(t, New[string, string](2, 0))
	})
}

func TestCache_Delete(t *testing.T) {
	c := New[int, any](10, 100*time.Millisecond)

	// run Delete on empty Cache
	c.Delete(0)

	c.Set(1, nil)
	c.Set(2, nil)
	sleepUntilRotates(c)
	c.Set(1, nil)
	c.Set(3, nil)
	sleepUntilRotates(c)
	c.Set(1, nil)
	c.Set(3, nil)
	c.Set(4, nil)
	c.Set(5, nil)
	c.Set(6, nil)
	sleepUntilRotates(c)

	if len := c.Len(); len != 9 {
		t.Fatal(len)
	}

	// cases
	toremove := []struct {
		Remove       []int
		ExpectedSize int
	}{
		{[]int{6}, 8},
		{[]int{3, 4}, 5},
		{[]int{1, 2}, 1},
		{[]int{6}, 1}, // no-op
		{[]int{5}, 0},
	}

	for _, tc := range toremove {
		c.Delete(tc.Remove...)
		if len := c.Len(); len != tc.ExpectedSize {
			t.Fatalf("size is %d, expected %d", len, tc.ExpectedSize)
		}
		for _, v := range tc.Remove {
			if _, ok := c.Get(v); ok {
				t.Errorf("key %d removed, but still there", v)
			}
		}
	}

	// perform some other action at the end
	c.Set(1, nil)
	if _, ok := c.Get(1); !ok {
		t.Error("where is the one?")
	}
}
