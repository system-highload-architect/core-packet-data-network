package lru

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache_SetGet(t *testing.T) {
	cache := NewCache[string, int](time.Minute)
	cache.Set("key1", 42)
	val, ok := cache.Get("key1")
	if !ok || val != 42 {
		t.Errorf("expected 42, got %d, ok=%v", val, ok)
	}
	_, ok = cache.Get("key2")
	if ok {
		t.Error("expected false for missing key")
	}
}

func TestCache_TTL(t *testing.T) {
	now := time.Now()
	fixedNow := func() time.Time { return now }
	cache := NewCache[string, int](50*time.Millisecond, WithNowFunc[string, int](fixedNow))
	cache.Set("key1", 100)
	val, ok := cache.Get("key1")
	if !ok || val != 100 {
		t.Errorf("expected 100, got %d", val)
	}
	now = now.Add(100 * time.Millisecond)
	_, ok = cache.Get("key1")
	if ok {
		t.Error("expected key to expire")
	}
}

func TestCache_Delete(t *testing.T) {
	cache := NewCache[string, int](time.Minute)
	cache.Set("key1", 10)
	cache.Delete("key1")
	_, ok := cache.Get("key1")
	if ok {
		t.Error("expected key to be deleted")
	}
}

func TestCache_Finalizer(t *testing.T) {
	var deleted int32
	fn := func(key string, val int) {
		atomic.AddInt32(&deleted, 1)
	}
	cache := NewCache[string, int](50*time.Millisecond, WithFinalizer(fn))
	cache.Set("key1", 1)
	cache.Set("key2", 2)
	cache.Delete("key1")
	cache.Delete("key2")
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&deleted) != 2 {
		t.Errorf("expected finalizer called 2 times, got %d", deleted)
	}
}

func TestLayerCache_Promote(t *testing.T) {
	configs := []LayerConfig{
		{TTL: time.Hour, MaxAttempt: 1},
		{TTL: time.Hour, MaxAttempt: 2},
		{TTL: time.Hour, MaxAttempt: 3},
	}
	lc := NewLayerCache[string, int](configs, nil)
	lc.Set("key1", 100)

	if !lc.Promote("key1") {
		t.Error("first Promote should return true")
	}
	val, ok := lc.Get("key1")
	if !ok || val != 100 {
		t.Errorf("expected value 100 after first promote, got %d, ok=%v", val, ok)
	}

	if !lc.Promote("key1") {
		t.Error("second Promote should return true")
	}
	val, ok = lc.Get("key1")
	if !ok || val != 100 {
		t.Errorf("expected value 100 after second promote, got %d, ok=%v", val, ok)
	}

	if lc.Promote("key1") {
		t.Error("third Promote should return false (last layer)")
	}
	_, ok = lc.Get("key1")
	if ok {
		t.Error("expected key to be deleted after third promote")
	}
}

func TestLayerCache_GetAcrossLayers(t *testing.T) {
	configs := []LayerConfig{
		{TTL: time.Hour, MaxAttempt: 1},
		{TTL: time.Hour, MaxAttempt: 2},
	}
	lc := NewLayerCache[string, int](configs, nil)
	lc.Set("key1", 10)
	val, ok := lc.Get("key1")
	if !ok || val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
	lc.Promote("key1")
	val, ok = lc.Get("key1")
	if !ok || val != 10 {
		t.Errorf("expected 10 after promote, got %d", val)
	}
}

func TestCache_Concurrent(t *testing.T) {
	cache := NewCache[int, int](time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cache.Set(id, j)
				val, ok := cache.Get(id)
				if !ok || val != j {
					t.Errorf("concurrent: expected %d, got %d", j, val)
				}
			}
		}(i)
	}
	wg.Wait()
}
