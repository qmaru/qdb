package lrubloom

import (
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type LRUBloom[K comparable, V any] struct {
	LRUEnable         bool
	BloomEnable       bool
	lruCache          *lru.Cache[K, V]
	lruCacheExpirable *expirable.LRU[K, V]
	bloomCache        *bloom.BloomFilter
	bloomN            uint
	bloomFP           float64
	bloomMu           sync.RWMutex
}

type LRUOptions struct {
	Enable bool
	Size   int
	TTL    time.Duration
}

type BloomOptions struct {
	Enable bool
	N      uint
	FP     float64
}

const (
	defaultBloomFP = 0.01
	defaultBloomN  = 10000
	defaultLRUSize = 1000
	defaultLRUTTL  = 5 * time.Minute
)

func toBytes[K comparable](k K) []byte {
	return []byte(fmt.Sprint(k))
}

func NewDefault[K comparable, V any](enableTTL bool) (*LRUBloom[K, V], error) {
	ttl := time.Duration(0)
	if enableTTL {
		ttl = defaultLRUTTL
	}

	lruOpt := LRUOptions{
		Enable: true,
		Size:   defaultLRUSize,
		TTL:    ttl,
	}
	bloomOpt := BloomOptions{
		Enable: true,
		N:      uint(defaultBloomN),
		FP:     defaultBloomFP,
	}
	return New[K, V](lruOpt, bloomOpt)
}

func New[K comparable, V any](lruOpt LRUOptions, bloomOpt BloomOptions) (*LRUBloom[K, V], error) {
	if !lruOpt.Enable && !bloomOpt.Enable {
		return nil, fmt.Errorf("either LRU or Bloom must be enabled")
	}

	var (
		lruCache          *lru.Cache[K, V]
		lruCacheExpirable *expirable.LRU[K, V]
		bloomFilter       *bloom.BloomFilter
		err               error
	)

	if lruOpt.Enable {
		if lruOpt.Size <= 0 {
			lruOpt.Size = defaultLRUSize
		}

		if lruOpt.TTL > 0 {
			lruCacheExpirable = expirable.NewLRU[K, V](lruOpt.Size, nil, lruOpt.TTL)
		} else {
			lruCache, err = lru.New[K, V](lruOpt.Size)
			if err != nil {
				return nil, err
			}
		}
	}

	var bn uint
	var bfp float64
	if bloomOpt.Enable {
		if bloomOpt.N == 0 {
			bloomOpt.N = defaultBloomN
		}
		if bloomOpt.FP <= 0 {
			bloomOpt.FP = defaultBloomFP
		}

		bn = bloomOpt.N
		bfp = bloomOpt.FP
		bloomFilter = bloom.NewWithEstimates(bn, bfp)
	}

	return &LRUBloom[K, V]{
		LRUEnable:         lruOpt.Enable,
		BloomEnable:       bloomOpt.Enable,
		lruCache:          lruCache,
		lruCacheExpirable: lruCacheExpirable,
		bloomCache:        bloomFilter,
		bloomN:            bn,
		bloomFP:           bfp,
	}, nil
}

func (c *LRUBloom[K, V]) ResetBloom() {
	if !c.BloomEnable {
		return
	}

	n := c.bloomN
	if n == 0 {
		n = defaultBloomN
	}
	fp := c.bloomFP
	if fp <= 0 {
		fp = defaultBloomFP
	}

	c.bloomMu.Lock()
	c.bloomCache = bloom.NewWithEstimates(n, fp)
	c.bloomMu.Unlock()
}

func (c *LRUBloom[K, V]) RebuildBloomFromLRU() {
	if !c.BloomEnable {
		return
	}

	n := c.bloomN
	if n == 0 {
		n = defaultBloomN
	}
	fp := c.bloomFP
	if fp <= 0 {
		fp = defaultBloomFP
	}

	newBF := bloom.NewWithEstimates(n, fp)
	if newBF == nil {
		return
	}

	if c.lruCacheExpirable != nil {
		for _, k := range c.lruCacheExpirable.Keys() {
			newBF.Add(toBytes(k))
		}
	} else if c.lruCache != nil {
		for _, k := range c.lruCache.Keys() {
			newBF.Add(toBytes(k))
		}
	}

	c.bloomMu.Lock()
	c.bloomCache = newBF
	c.bloomMu.Unlock()
}

func (c *LRUBloom[K, V]) Set(key K, val V) {
	if c.LRUEnable {
		if c.lruCacheExpirable != nil {
			c.lruCacheExpirable.Add(key, val)
		} else if c.lruCache != nil {
			c.lruCache.Add(key, val)
		}
	}
	if c.BloomEnable && c.bloomCache != nil {
		c.bloomCache.Add(toBytes(key))
	}
}

func (c *LRUBloom[K, V]) Get(key K) (V, bool) {
	var zero V

	if c.BloomEnable && c.bloomCache != nil {
		if !c.bloomCache.Test(toBytes(key)) {
			return zero, false
		}
	}

	if c.LRUEnable {
		if c.lruCacheExpirable != nil {
			if v, ok := c.lruCacheExpirable.Get(key); ok {
				return v, true
			}
			return zero, false
		}
		if c.lruCache != nil {
			if v, ok := c.lruCache.Get(key); ok {
				return v, true
			}
			return zero, false
		}
	}

	return zero, false
}

func (c *LRUBloom[K, V]) Exists(key K) bool {
	if c.LRUEnable {
		if c.lruCacheExpirable != nil {
			if _, ok := c.lruCacheExpirable.Get(key); ok {
				return true
			}
		} else if c.lruCache != nil {
			if _, ok := c.lruCache.Get(key); ok {
				return true
			}
		}
	}
	if c.BloomEnable && c.bloomCache != nil {
		return c.bloomCache.Test(toBytes(key))
	}
	return false
}

// LRUClient
func (c *LRUBloom[K, V]) LRUClient() (*lru.Cache[K, V], *expirable.LRU[K, V]) {
	return c.lruCache, c.lruCacheExpirable
}

// BloomClient
func (c *LRUBloom[K, V]) BloomClient() *bloom.BloomFilter {
	return c.bloomCache
}
