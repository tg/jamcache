// jamcache == [j]ust [a] [m]ap [cache]
//
// jamcache is a key-value cache which aims at low memory overhead. Every cached item is kept only once
// (in a map) without any additional metadata. Instead of tracking LRU etc. jamcache keeps multiple
// maps (generations) and drops them once they expire – this makes the garbage collection very efficient,
// but forces all the items to share the same TTL.
//
// Number of generations is defined at cache creation. The more generation we have, the better
// accuracy of TTL, but in order to check the key we need to query more maps. If there is only one
// generation defined, the cache behaves like a single map which is cleared periodically.
package jamcache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Cache implements key-value cache using one or more maps and with a single (shared) TTL.
//
// An empty or nil Cache is a valid object, but they differ slightly in GetOrSet method.
type Cache[K comparable, V any] struct {
	// MaxItems specifies maximum number of items in cache (across all generations).
	MaxItems int

	// gens keep the generations, the newest (current) comes first
	gens []map[K]V

	// genDur specifies duration of a single generation for automatic garbage-collecting;
	// if zero or less then there is no gc.
	genDur time.Duration

	// head points to the end of current generation (only set when genDur > 0)
	head time.Time

	// lock protects generations
	genLock sync.RWMutex

	// size tracks number of elements in the cache. It doesn't necessary track unique
	// elements in cache as one key can be present in multiple generations.
	size int

	// rungc is set to 1 in Get() to indicate gc is needed
	rungc int64

	// keySetChan holds channels for syncing GetOrSet method
	keySetChan     map[K]*getOrSetDone[V]
	keySetChanLock sync.Mutex
}

type getOrSetDone[V any] struct {
	// done will be closed by the setter
	done chan struct{}
	// ok will be set to true if the value was fetched
	ok bool
	// value will hold fetched value
	value V
}

const DefaultNumOfGenerations = 3

// New creates new cache with n generations, each holding specified duration.
//
// If number of generations is zero or less then returned cache is equivalent to an non-initialized
// cache. In this case cache doesn't store anything, Get returns false and GetOrSet performs only
// a synchronization to avoid multiple loads in-flight.
//
// If duration is zero or less then no automatic garbage collection is performed (no TTL).
func New[K comparable, V any](n int, dur time.Duration) *Cache[K, V] {
	if n <= 0 {
		return &Cache[K, V]{}
	}

	return &Cache[K, V]{
		gens:       make([]map[K]V, n),
		genDur:     dur,
		keySetChan: make(map[K]*getOrSetDone[V]),
	}
}

// NewAtLeast returns cache which stores elements for at least specified duration.
// It will use default number of generations.
func NewAtLeast[K comparable, V any](dur time.Duration) *Cache[K, V] {
	genDur := dur / time.Duration(DefaultNumOfGenerations-1)
	return New[K, V](DefaultNumOfGenerations, genDur)
}

// NewAtMost returns cache which stores elements for at most specified duration.
// It will use default number of generations.
func NewAtMost[K comparable, V any](dur time.Duration) *Cache[K, V] {
	genDur := dur / time.Duration(DefaultNumOfGenerations)
	return New[K, V](DefaultNumOfGenerations, genDur)
}

// Set adds (key, val) to the set. It is always placed in the newest generation.
func (c *Cache[K, V]) Set(key K, val V) {
	if c == nil || len(c.gens) == 0 {
		return
	}

	c.genLock.Lock()
	defer c.genLock.Unlock()

	// run GC if needed
	c.tryGC()

	// delete one element if at or over the size limit
	if c.MaxItems > 0 && c.size >= c.MaxItems {
		c.deleteOne()
	}

	// initialize map if nil
	if c.gens[0] == nil {
		c.gens[0] = make(map[K]V)
	}

	// take the current size, so we know if it changed after setting the key
	prevSize := len(c.gens[0])
	c.gens[0][key] = val
	c.size += len(c.gens[0]) - prevSize
	return
}

// Get returns the most recent value for the key.
// If key is not in cache then (V{}, false) is returned, otherwise (value, true).
func (c *Cache[K, V]) Get(key K) (V, bool) {
	if c == nil || len(c.gens) == 0 {
		var zero V
		return zero, false
	}

	now := time.Now()
	var (
		val      V
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

	// Find first occurrence of key
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

// GetOrSetOnce gets the values from the cache and if it's not present then loads it by calling loadValue.
// When there are multiple simultaneous calls for the same key, only one will load the value and other
// will wait for it to finish. Waiting can be aborted by cancelling the context.
//
// This function only makes sense when there are possibly multiple routines running Set on the same key.
// If there is only one routine (at given time) setting a given key then there is better to use
// Get and Set separately as we don't need any coordination between setters.
//
// There is no synchronization when Cache is nil and the call is equivalent to calling loadValue.
//
// Returns the value and error passed from the call to loadValue.
func (c *Cache[K, V]) GetOrSetOnce(ctx context.Context, key K, loadValue func() (val V, err error)) (V, error) {
	if c == nil {
		return loadValue()
	}

	// check in the cache first
	if v, ok := c.Get(key); ok {
		return v, nil
	}

	for {
		// doLoad will be set to true if we're responsible for loading the value
		var doLoad bool

		// grab the key channel (so there is only one fetcher)
		c.keySetChanLock.Lock()
		done := c.keySetChan[key]
		if done == nil {
			done = &getOrSetDone[V]{done: make(chan struct{})}
			if c.keySetChan == nil {
				c.keySetChan = make(map[K]*getOrSetDone[V])
			}
			c.keySetChan[key] = done
			doLoad = true
		}
		c.keySetChanLock.Unlock()

		if doLoad {
			// once we fetch let others know and remove the done channel
			defer func() {
				close(done.done)
				c.keySetChanLock.Lock()
				delete(c.keySetChan, key)
				c.keySetChanLock.Unlock()
			}()

			// check again in cache in case it was added in the meantime
			// (it's possible when done channel is removed just after the first Get)
			if v, ok := c.Get(key); ok {
				done.ok = true
				done.value = v
				return v, nil
			}

			// load the value
			v, err := loadValue()

			// if all god, set value in the cache and save it for the waiters
			if err == nil {
				c.Set(key, v)
				done.ok = true
				done.value = v
			}

			return v, err
		} else {
			// use default context if missing
			if ctx == nil {
				ctx = context.Background()
			}

			// Wait for the fetcher to finish
			select {
			case <-done.done:
				// if the value was fetched by the setter then return it;
				// otherwise (due to an error) we will try to fetch it again
				// in the next iteration.
				if done.ok {
					return done.value, nil
				}
			case <-ctx.Done():
				var zero V
				return zero, ctx.Err()
			}
		}
	}
}

// Delete removes specified keys from the cache.
func (c *Cache[K, V]) Delete(keys ...K) {
	if c == nil || len(keys) == 0 {
		return
	}

	c.genLock.Lock()
	defer c.genLock.Unlock()

	// run GC if needed
	c.tryGC()

	newSize := 0

	// delete keys from all the generations
	for _, g := range c.gens {
		for n := range keys {
			delete(g, keys[n])
		}
		newSize += len(g)
	}

	c.size = newSize
}

// vgens returns valid (non-expired) generations for the timestamp
func (c *Cache[K, V]) vgens(ts time.Time) int {
	td := ts.Sub(c.head)
	if td > 0 && c.genDur > 0 {
		n := len(c.gens) - (int(td/c.genDur) + 1)
		if n < 0 {
			n = 0
		}
		return n
	}
	return len(c.gens)
}

// Len returns number of items in cache.
func (c *Cache[K, V]) Len() int {
	// TODO: this can be implemented using atomic (but can't be zeroed in gc/Delete)
	c.genLock.RLock()
	defer c.genLock.RUnlock()
	return c.size
}

// tryGC run GC if needed.
// It must be called within a genLock.
func (c *Cache[K, V]) tryGC() {
	if c.genDur > 0 {
		if now := time.Now(); now.After(c.head) {
			c.gc(now)
		}
	}
}

// gc rotates generations by removing expired and making space for the new ones.
// It must be called within a genLock. Passed time indicates latest timestamp.
func (c *Cache[K, V]) gc(ts time.Time) {
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

	c.head = ts.Truncate(c.genDur).Add(c.genDur)
}

// deleteOne deletes a random element from the oldest generation.
// It must be called within a genLock.
func (c *Cache[K, V]) deleteOne() {
	for n := len(c.gens) - 1; n >= 0; n-- {
		for k := range c.gens[n] {
			delete(c.gens[n], k)
			c.size--
			return
		}
	}
}
