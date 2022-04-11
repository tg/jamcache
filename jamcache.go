// jamcache == [j]ust [a] [m]ap [cache]
// jamcache aims at low overhead and hence uses only a single structure to cache
// items. Instead of tracking LRU etc. it keeps items in generations and drops
// whole generations once they expire.
package jamcache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	// MaxItems specifies maximum number of items in cache (across all generations).
	MaxItems int

	lock   sync.RWMutex
	gens   []items
	genDur time.Duration
	// head points to the end of current generation
	head time.Time
	size int

	// rungc is a flag used in Get() to run rotate
	rungc int64

	keyLockLock sync.Mutex
	keyLock     map[interface{}]chan struct{}
}

type items map[interface{}]interface{}

const DefaultNumOfGenerations = 3

// New creates new cache with n generations, each holding specified duration.
// Returned cache is nil iff any of the arguments is invalid.
func New(n int, dur time.Duration) *Cache {
	if n <= 0 || dur <= 0 {
		return nil
	}

	return &Cache{
		gens:    make([]items, n),
		genDur:  dur,
		keyLock: make(map[interface{}]chan struct{}),
	}
}

// NewAtLeast returns cache which stores elements for at least specified duration.
// It will use default number of generations.
func NewAtLeast(dur time.Duration) *Cache {
	genDur := dur / time.Duration(DefaultNumOfGenerations-1)
	return New(DefaultNumOfGenerations, genDur)
}

// NewAtMost returns cache which stores elements for at most specified duration.
// It will use default number of generations.
func NewAtMost(dur time.Duration) *Cache {
	genDur := dur / time.Duration(DefaultNumOfGenerations)
	return New(DefaultNumOfGenerations, genDur)
}

// Set adds (key, val) to the set. It is always placed in the newest generation.
func (c *Cache) Set(key, val interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// check if we need rotation
	if now := time.Now(); now.After(c.head) {
		c.rotate(now)
	}

	// delete one element if at or over the size limit
	if c.MaxItems > 0 && c.size >= c.MaxItems {
		c.deleteOne()
	}

	// take the current size, so we know if it changed after setting the key
	prevSize := len(c.gens[0])
	c.gens[0][key] = val
	c.size += len(c.gens[0]) - prevSize
	return
}

// Get returns the most recent value for the key.
// If key is not in cache then (nil, false) is returned, otherwise (value, true).
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	now := time.Now()
	var (
		val   interface{}
		found bool
		rungc bool
	)

	c.lock.RLock()

	// Get number of valid generations
	ng := c.vgens(now)

	// Mark for rotation if not all generations are valid and cache has been
	// initialized. We use CompareAndSwap to make sure only one Get() runs rotation.
	// TODO: as we need to grab a lock later for this operation and we do it in Set
	// anyway (which is prefarable), we could add some slack here in case Set is called regularly.
	if ng != len(c.gens) {
		if !c.head.IsZero() {
			rungc = atomic.CompareAndSwapInt64(&c.rungc, 0, 1)
		}
	}

	// Find first occurence of key
	for _, g := range c.gens[:ng] {
		if v, ok := g[key]; ok {
			val = v
			found = true
			break
		}
	}

	c.lock.RUnlock()

	// Rotate cache if GC flag set
	if rungc {
		c.lock.Lock()
		c.rotate(now)
		c.rungc = 0
		c.lock.Unlock()
	}

	return val, found
}

// GetOrSet gets the values from the cache and if it's not present then loads it by calling loadValue.
// When there are multiple simultaneous calls for the same key, only one will load the value and other
// will wait for it to finish. Waiting can be aborted by cancelling the context.
// Retruns the value and error passed from the call to loadValue.
func (c *Cache) GetOrSet(ctx context.Context, key interface{}, loadValue func() (interface{}, error)) (interface{}, error) {
	for {
		// check in the cache first
		if v, ok := c.Get(key); ok {
			return v, nil
		}

		// fetch will be set to true if we're responsible for fetching the value
		var fetch bool

		// grab the key lock (so there is only one fetcher)
		c.keyLockLock.Lock()
		done := c.keyLock[key]
		if done == nil {
			done = make(chan struct{})
			c.keyLock[key] = done
			fetch = true
		}
		c.keyLockLock.Unlock()

		if fetch {
			// once we fetch let others know and remove the done channel
			defer func() {
				close(done)
				c.keyLockLock.Lock()
				delete(c.keyLock, key)
				c.keyLockLock.Unlock()
			}()

			// check again in cache in case it was added in the meantime
			// (it's possible when done channel is removed just after the first Get)
			if v, ok := c.Get(key); ok {
				return v, nil
			}

			// fetch the value
			v, err := loadValue()
			if err != nil {
				return v, err
			}

			// set value in the cache
			c.Set(key, v)
			return v, nil
		} else {
			// use default context if missing
			if ctx == nil {
				ctx = context.Background()
			}

			// Wait for the fetcher to finish. We will grab the value from the
			// cache in the next iteration.
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

// vgens returns valid (non-expired) generations for the timestamp
func (c *Cache) vgens(ts time.Time) int {
	td := ts.Sub(c.head)
	if td > 0 {
		n := len(c.gens) - (int(td/c.genDur) + 1)
		if n < 0 {
			n = 0
		}
		return n
	}
	return len(c.gens)
}

// Len returns number of items in cache.
func (c *Cache) Len() int {
	// TODO: this can be implemented using atomic
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.size
}

func (c *Cache) rotate(ts time.Time) {
	// get valid generations
	gens := c.gens[:c.vgens(ts)]

	// skip if nothing has changed;
	if len(gens) == len(c.gens) {
		return
	}

	c.size = 0
	wn := len(c.gens) - 1
	rn := len(gens) - 1

	for ; rn >= 0; rn-- {
		c.gens[wn] = gens[rn]
		c.size += len(gens[rn])
		wn--
	}

	for ; wn >= 0; wn-- {
		c.gens[wn] = nil
	}

	// make sure current generation is initialized
	if c.gens[0] == nil {
		c.gens[0] = make(items)
	}

	c.head = ts.Truncate(c.genDur).Add(c.genDur)
}

// deleteOne deletes a random element from the oldest generation
func (c *Cache) deleteOne() {
	for n := len(c.gens) - 1; n >= 0; n-- {
		for k := range c.gens[n] {
			delete(c.gens[n], k)
			c.size--
			return
		}
	}
}
