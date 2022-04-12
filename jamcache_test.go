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

func countNonEmptyGens(c *Cache) int {
	res := 0

	for _, g := range c.gens {
		if len(g) > 0 {
			res++
		}
	}

	return res
}

func TestCache_numOfGens(t *testing.T) {
	c := New(3, 100*time.Millisecond)

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

func TestCache_GetOrSet_oneByOne(t *testing.T) {
	c := New(1, time.Hour)

	for k := 0; k < 10; k++ {
		v, err := c.GetOrSet(nil, 1, func() (interface{}, error) { return 10 + k, nil })
		if err != nil {
			t.Fatal(err)
		}
		// we should only get the very first value
		if v != 10 {
			t.Errorf("[%d] unexpected value: %v", k, v)
		}
	}
}

func TestCache_GetOrSet_oneByOneMultipleRoutines(t *testing.T) {
	c := New(1, time.Hour)

	for r := 0; r < 10; r++ {
		r := r
		t.Run(fmt.Sprintf("r=%d", r), func(t *testing.T) {
			t.Parallel()

			for k := 0; k < 1000; k++ {
				v, err := c.GetOrSet(nil, r, func() (interface{}, error) { return 1000 + r + k, nil })
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

func TestCache_GetOrSet_random(t *testing.T) {
	c := New(1, time.Hour)

	size := 100
	access := make([]int64, size)

	var wg sync.WaitGroup
	for r := 0; r < size*100; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := rand.Intn(size)
			got, err := c.GetOrSet(nil, key, func() (interface{}, error) {
				atomic.AddInt64(&access[key], 1)
				return -key, nil
			})
			if err != nil {
				panic(err)
			}
			if v := got.(int); v != -key {
				t.Errorf("got %d, expected %d", v, -key)
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
		if ok == false || v.(int) != -key {
			t.Errorf("%d: expected %d, got %v [%v]", key, -key, v, ok)
		}
	}
}

func TestCache_GetOrSet_error(t *testing.T) {
	c := New(1, time.Hour)
	someError := errors.New("some error")

	for n := 0; n < 100; n++ {
		n := n

		// Set value for n=10, otherwise error. The function shouln't be called
		// after the value was returned (as it will be in cache).
		v, err := c.GetOrSet(nil, 1, func() (interface{}, error) {
			if n != 10 {
				return nil, someError
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
			if got := v.(int); got != 10 || err != nil {
				t.Errorf("expected value 10, got %d [err=%v]", got, err)
			}
		}
	}
}

func TestCache_GetOrSet_errorWithWaiter(t *testing.T) {
	c := New(1, time.Hour)
	someError := errors.New("some error")

	res := make(chan interface{})

	v, err := c.GetOrSet(nil, 1, func() (interface{}, error) {
		// run another setter in the background
		go func() {
			v, err := c.GetOrSet(nil, 1, func() (interface{}, error) {
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
	if v.(int) != 10 {
		t.Error(v)
	}

	// make sure we get the valid value from the waiter (inner call)
	if v := <-res; v.(int) != 20 {
		t.Errorf("unexpected value: %v", v)
	}
}

func TestCache_GetOrSet_cancelWaiter(t *testing.T) {
	c := New(1, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())

	waiterError := make(chan error)
	sleep := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		v, err := c.GetOrSet(ctx, 1, func() (interface{}, error) {
			go func() {
				v, err := c.GetOrSet(ctx, 1, func() (interface{}, error) {
					t.Error("this shouldn't be called")
					return nil, nil
				})
				if v != nil {
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

func BenchmarkCache_GetOrSet_uniqueKeys(b *testing.B) {
	c := New(3, time.Hour)

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
					_, err := c.GetOrSet(nil, key, func() (interface{}, error) {
						return nil, nil
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

func BenchmarkCache_GetOrSet_10keys(b *testing.B) {
	var key int64

	for n := 0; n < b.N; n++ {
		c := New(10, time.Microsecond)

		var wg sync.WaitGroup
		for r := 0; r < 10; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 1000; k++ {
					// choose from 10 keys
					atomic.AddInt64(&key, 1)
					key = key % 10
					_, err := c.GetOrSet(nil, key, func() (interface{}, error) {
						return nil, nil
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
