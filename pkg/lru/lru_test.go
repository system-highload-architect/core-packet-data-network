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

// RU: Тестируем выталкивание элементов по слоям при наступлении таймаута (Timing Wheel)
// EN: Validate sequential layered promotion routines upon deadline breach conditions
func TestLayerCache_PromoteLayer(t *testing.T) {
	configs := []LayerConfig{
		{TTL: 10 * time.Millisecond}, // Слой 0
		{TTL: 20 * time.Millisecond}, // Слой 1
		{TTL: 30 * time.Millisecond}, // Слой 2
	}

	// RU: Используем явный указатель на время для защиты от рассинхронизации
	// EN: Use an explicit time pointer to prevent closure desynchronization
	currentTime := time.Now()
	fixedNow := func() time.Time { return currentTime }

	// RU: ИСХОДНОЕ ИСПРАВЛЕНИЕ: используем Линейный Backoff с шагом 10мс, чтобы интервалы
	// RU: слоев в тесте идеально совпадали с расчетом дедлайнов (10мс, 20мс, 30мс)!
	// EN: CRITICAL FIX: Use LinearBackoff with a 10ms step to match layer intervals
	// EN: perfectly with deadline calculations (10ms, 20ms, 30ms) inside this test!
	backoffStrategy := &LinearBackoff{Interval: 10 * time.Millisecond}

	lc := NewLayerCache[string, int](configs, backoffStrategy, WithNowFunc[string, int](fixedNow))
	lc.Set("key1", 100)

	// RU: Шаг 1: Сдвигаем время за дедлайн Слоя 0 (10мс)
	// EN: Step 1: Advance time past Layer 0 deadline (10ms)
	currentTime = currentTime.Add(15 * time.Millisecond)

	key, val, layerIdx, found := lc.PeekExpiredScan()
	if !found || layerIdx != 0 || val != 100 {
		t.Fatalf("expected expired key1 on layer 0, got layerIdx=%d, found=%v", layerIdx, found)
	}

	// Выталкиваем на Слой 1
	_, alive := lc.PromoteLayer(key, layerIdx)
	if !alive {
		t.Error("packet should be alive when moving from layer 0 to layer 1")
	}

	// RU: Шаг 2: Элемент на Слое 1. LinearBackoff.Next(1) вернет ровно 20мс!
	// RU: Чтобы гарантированно просрочить его, сдвигаем время на 25мс вперед.
	// EN: Step 2: Item is on Layer 1. LinearBackoff.Next(1) returns exactly 20ms.
	// EN: Advance time by 25ms to trigger expiration.
	currentTime = currentTime.Add(25 * time.Millisecond)

	key, val, layerIdx, found = lc.PeekExpiredScan()
	if !found || layerIdx != 1 {
		t.Fatalf("expected to find expired key1 on layer 1, layerIdx=%d, found=%v", layerIdx, found)
	}

	// Выталкиваем на Слой 2 (крайний слой)
	_, alive = lc.PromoteLayer(key, layerIdx)
	if !alive {
		t.Error("packet should be alive when moving from layer 1 to layer 2")
	}

	// RU: Шаг 3: Элемент на Слое 2. LinearBackoff.Next(2) вернет ровно 30мс!
	// RU: Чтобы гарантированно просрочить Слой 2, сдвигаем время на 35мс вперед.
	// EN: Step 3: Item is on Layer 2. LinearBackoff.Next(2) returns exactly 30ms.
	// EN: Advance time by 35ms to trigger expiration.
	currentTime = currentTime.Add(35 * time.Millisecond)

	key, val, layerIdx, found = lc.PeekExpiredScan()
	if !found || layerIdx != 2 {
		t.Fatalf("expected to find expired key1 on terminal layer 2, layerIdx=%d, found=%v", layerIdx, found)
	}

	// Пытаемся вытолкнуть дальше последнего слоя — должен вернуться статус мертв (LOST)
	_, alive = lc.PromoteLayer(key, layerIdx)
	if alive {
		t.Error("PromoteLayer on terminal layer should return alive=false (LOST)")
	}

	// Проверяем, что пакет полностью удален из памяти
	if _, ok := lc.Get("key1"); ok {
		t.Error("packet should be completely deleted from all layers after exhausting retries")
	}
}

func TestLayerCache_GetAcrossLayers(t *testing.T) {
	configs := []LayerConfig{
		{TTL: time.Hour},
		{TTL: time.Hour},
	}
	lc := NewLayerCache[string, int](configs, nil)
	lc.Set("key1", 10)

	val, ok := lc.Get("key1")
	if !ok || val != 10 {
		t.Errorf("expected 10, got %d", val)
	}

	// Вручную перекидываем из слоя 0 в слой 1
	lc.PromoteLayer("key1", 0)

	val, ok = lc.Get("key1")
	if !ok || val != 10 {
		t.Errorf("expected 10 after promotion, got %d", val)
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
