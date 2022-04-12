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

	// gens keep the generations, the newest (current) comes first
	gens []items

	// genDur specifies duration of a single generation
	genDur time.Duration

	// lock protects generations
	genLock sync.RWMutex

	// head points to the end of current generation
	head time.Time

	// size tracks number of elements in the cache. It doesn't necessary track unique
	// elements in cache as one key can be present in multiple generations.
	size int

	// rungc is set to 1 in Get() to indicate gc is needed
	rungc int64

	// keySetChan holds channels for syncing GetOrSet method
	keySetChan     map[interface{}]chan struct{}
	keySetChanLock sync.Mutex
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
		gens:       make([]items, n),
		genDur:     dur,
		keySetChan: make(map[interface{}]chan struct{}),
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
	c.genLock.Lock()
	defer c.genLock.Unlock()

	// check if we need rotation
	if now := time.Now(); now.After(c.head) {
		c.gc(now)
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
		val      interface{}
		found    bool
		gcCaller bool
	)

	c.genLock.RLock()

	// Get number of valid generations
	ng := c.vgens(now)

	// Mark for rotation if not all generations are valid and cache has been
	// initialized. We use CompareAndSwap to pick only one caller (we don't want to
	// run GC in every Get). rungc flag will be cleared in gc().
	if ng != len(c.gens) {
		if !c.head.IsZero() {
			gcCaller = atomic.CompareAndSwapInt64(&c.rungc, 0, 1)
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

	c.genLock.RUnlock()

	// Run GC if we're a gc caller and nobody (Set) run it in a meantime
	if gcCaller && (atomic.LoadInt64(&c.rungc) == 1) {
		c.genLock.Lock()
		c.gc(now)
		c.genLock.Unlock()
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

		// grab the key channel (so there is only one fetcher)
		c.keySetChanLock.Lock()
		done := c.keySetChan[key]
		if done == nil {
			done = make(chan struct{})
			c.keySetChan[key] = done
			fetch = true
		}
		c.keySetChanLock.Unlock()

		if fetch {
			// once we fetch let others know and remove the done channel
			defer func() {
				close(done)
				c.keySetChanLock.Lock()
				delete(c.keySetChan, key)
				c.keySetChanLock.Unlock()
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
	c.genLock.RLock()
	defer c.genLock.RUnlock()
	return c.size
}

// gc rotates generations by removing expired and making space for the new ones.
// It must be called within a genLock. Passed time indicates latest timestamp.
func (c *Cache) gc(ts time.Time) {
	c.rungc = 0

	// get valid generations
	gens := c.gens[:c.vgens(ts)]

	// skip if nothing has changed;
	if len(gens) == len(c.gens) {
		return
	}

	// move valid generations to the end
	c.size = 0
	wn := len(c.gens) - 1
	rn := len(gens) - 1

	for ; rn >= 0; rn-- {
		c.gens[wn] = gens[rn]
		c.size += len(gens[rn])
		wn--
	}

	// clear new generations
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
