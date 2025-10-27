package lrubloom

import (
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/karlseguin/ccache/v3"
)

type LRUBloom[K any] struct {
	LRUEnable   bool
	BloomEnable bool
	lruCache    *ccache.Cache[K]
	lruTTL      time.Duration
	bloomCache  *bloom.BloomFilter
	bloomN      uint
	bloomFP     float64
	bloomMu     sync.RWMutex
	keys        map[string]struct{}
	keysMu      sync.RWMutex
}

type LRUOptions struct {
	Enable bool
	Size   int64
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

func toKeyString(key any) string {
	return fmt.Sprint(key)
}

func NewDefault[K any](enableTTL bool) (*LRUBloom[K], error) {
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
	return New[K](lruOpt, bloomOpt)
}

func NewDefaultLRU[K any](enableTTL bool) (*LRUBloom[K], error) {
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
		Enable: false,
	}
	return New[K](lruOpt, bloomOpt)
}

func NewDefaultBloom[K any]() (*LRUBloom[K], error) {
	lruOpt := LRUOptions{
		Enable: false,
	}
	bloomOpt := BloomOptions{
		Enable: true,
		N:      uint(defaultBloomN),
		FP:     defaultBloomFP,
	}
	return New[K](lruOpt, bloomOpt)
}

func New[K any](lruOpt LRUOptions, bloomOpt BloomOptions) (*LRUBloom[K], error) {
	if !lruOpt.Enable && !bloomOpt.Enable {
		return nil, fmt.Errorf("either LRU or Bloom must be enabled")
	}

	var cache *ccache.Cache[K]
	var lruTTL time.Duration

	if lruOpt.Enable {
		if lruOpt.Size <= 0 {
			lruOpt.Size = defaultLRUSize
		}

		cfg := ccache.Configure[K]().MaxSize(lruOpt.Size)
		cache = ccache.New(cfg)
		lruTTL = lruOpt.TTL
	}

	var bloomFilter *bloom.BloomFilter
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

	return &LRUBloom[K]{
		LRUEnable:   lruOpt.Enable,
		BloomEnable: bloomOpt.Enable,
		lruCache:    cache,
		lruTTL:      lruTTL,
		bloomCache:  bloomFilter,
		bloomN:      bn,
		bloomFP:     bfp,
		keys:        make(map[string]struct{}),
	}, nil
}

func (c *LRUBloom[K]) ResetBloom() {
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

func (c *LRUBloom[K]) RebuildBloomFromLRU() {
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

	c.keysMu.RLock()
	keys := make([]string, 0, len(c.keys))
	for k := range c.keys {
		keys = append(keys, k)
	}
	c.keysMu.RUnlock()

	var toDelete []string
	for _, k := range keys {
		if c.lruCache != nil {
			item := c.lruCache.Get(k)
			if item != nil && !item.Expired() {
				newBF.Add([]byte(k))
				continue
			}
		}
		toDelete = append(toDelete, k)
	}

	if len(toDelete) > 0 {
		c.keysMu.Lock()
		for _, k := range toDelete {
			delete(c.keys, k)
		}
		c.keysMu.Unlock()
	}

	c.keysMu.RLock()
	for k := range c.keys {
		newBF.Add([]byte(k))
	}
	c.keysMu.RUnlock()

	c.bloomMu.Lock()
	c.bloomCache = newBF
	c.bloomMu.Unlock()
}

func (c *LRUBloom[K]) Set(key any, val K, ttl ...time.Duration) {
	keyStr := toKeyString(key)
	if c.LRUEnable && c.lruCache != nil {
		var useTTL time.Duration
		if len(ttl) > 0 {
			useTTL = ttl[0]
		} else {
			useTTL = c.lruTTL
		}
		c.lruCache.Set(keyStr, val, useTTL)
		c.keysMu.Lock()
		c.keys[keyStr] = struct{}{}
		c.keysMu.Unlock()
	}
	if c.BloomEnable && c.bloomCache != nil {
		c.bloomMu.Lock()
		c.bloomCache.Add([]byte(keyStr))
		c.bloomMu.Unlock()
	}
}

func (c *LRUBloom[K]) GetOrExist(key any) (value K, ok bool, probable bool) {
	var zero K
	keyStr := toKeyString(key)

	if c.LRUEnable && c.lruCache != nil {
		item := c.lruCache.Get(keyStr)
		if item != nil && !item.Expired() {
			return item.Value(), true, true
		}
	}

	if c.BloomEnable && c.bloomCache != nil {
		c.bloomMu.RLock()
		probable = c.bloomCache.Test([]byte(keyStr))
		c.bloomMu.RUnlock()
	}

	return zero, false, probable
}

func (c *LRUBloom[K]) Delete(key any) {
	keyStr := toKeyString(key)
	if c.LRUEnable && c.lruCache != nil {
		c.lruCache.Delete(keyStr)
	}

	c.keysMu.Lock()
	delete(c.keys, keyStr)
	c.keysMu.Unlock()
}

func (c *LRUBloom[K]) Clear() {
	if c.LRUEnable && c.lruCache != nil {
		c.lruCache.Clear()
	}
	c.keysMu.Lock()
	c.keys = make(map[string]struct{})
	c.keysMu.Unlock()
	if c.BloomEnable {
		c.ResetBloom()
	}
}

// LRUClient
func (c *LRUBloom[K]) LRUClient() *ccache.Cache[K] {
	return c.lruCache
}

// BloomClient
func (c *LRUBloom[K]) BloomClient() *bloom.BloomFilter {
	return c.bloomCache
}
