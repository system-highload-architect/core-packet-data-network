package lru

import (
	"sync"
	"time"
)

// LayerConfig описывает один слой кэша.
type LayerConfig struct {
	TTL time.Duration
}

// LayerCache — многослойный кэш, который перемещает элементы между слоями по истечении TTL.
type LayerCache[K comparable, V any] struct {
	mu      sync.Mutex
	layers  []*Cache[K, V]
	backoff Backoff
	now     func() time.Time
}

// NewLayerCache создаёт многослойный кэш с заданной конфигурацией слоёв и стратегией backoff.
func NewLayerCache[K comparable, V any](configs []LayerConfig, backoff Backoff, opts ...CacheOption[K, V]) *LayerCache[K, V] {
	if backoff == nil {
		backoff = NewExponentialBackoff()
	}
	lc := &LayerCache[K, V]{
		backoff: backoff,
		now:     time.Now,
	}

	dummyCache := &Cache[K, V]{now: time.Now}
	for _, opt := range opts {
		opt(dummyCache)
	}
	lc.now = dummyCache.now

	for _, cfg := range configs {
		cache := NewCache[K, V](cfg.TTL, opts...)
		lc.layers = append(lc.layers, cache)
	}
	return lc
}

// Set добавляет элемент в первый (самый быстрый) слой.
func (lc *LayerCache[K, V]) Set(key K, value V) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.layers) > 0 {
		lc.layers[0].Set(key, value)
	}
}

// Get возвращает элемент, проверяя слои по порядку.
func (lc *LayerCache[K, V]) Get(key K) (V, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, layer := range lc.layers {
		if val, ok := layer.Get(key); ok {
			return val, true
		}
	}
	var zero V
	return zero, false
}

// Delete удаляет элемент из всех слоёв (вызывается при получении ACK).
func (lc *LayerCache[K, V]) Delete(key K) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, layer := range lc.layers {
		layer.Delete(key)
	}
}

// PeekExpiredScan сканирует хвосты всех слоев и возвращает элемент, у которого ИСТЁК таймаут.
// Дополнительно возвращает индекс слоя, в котором этот элемент лежит.
func (lc *LayerCache[K, V]) PeekExpiredScan() (K, V, int, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	now := lc.now()

	for i, layer := range lc.layers {
		key, val, expiresAt, has := layer.PeekTail()
		if has && now.After(expiresAt) {
			return key, val, i, true // Нашли просроченный элемент!
		}
	}

	var zeroK K
	var zeroV V
	return zeroK, zeroV, -1, false
}

// GetEarliestDeadline возвращает время ближайшего дедлайна среди ВСЕХ хвостов всех слоев.
// Используется демоном, чтобы рассчитать точное время сна до микросекунды.
func (lc *LayerCache[K, V]) GetEarliestDeadline() (time.Time, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var earliest time.Time
	found := false

	for _, layer := range lc.layers {
		_, _, expiresAt, has := layer.PeekTail()
		if has {
			if !found || expiresAt.Before(earliest) {
				earliest = expiresAt
				found = true
			}
		}
	}
	return earliest, found
}

// PromoteLayer выталкивает элемент из текущего слоя на следующий уровень с расчетом Backoff TTL.
// Если это был последний слой — возвращает false (пакет окончательно потерян).
func (lc *LayerCache[K, V]) PromoteLayer(key K, currentLayerIndex int) (V, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var zero V
	if currentLayerIndex < 0 || currentLayerIndex >= len(lc.layers) {
		return zero, false
	}

	// Извлекаем из текущего слоя
	_, val, has := lc.layers[currentLayerIndex].PopTail()
	if !has {
		return zero, false
	}

	nextLayer := currentLayerIndex + 1
	if nextLayer >= len(lc.layers) {
		return val, false
	}

	nextTTL := lc.backoff.Next(nextLayer)

	lc.layers[nextLayer].SetWithTTL(key, val, nextTTL)
	return val, true
}

func (lc *LayerCache[K, V]) Close() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, layer := range lc.layers {
		layer.Close()
	}
}

// Len возвращает суммарное количество элементов, удерживаемых во всех слоях кэша.
func (lc *LayerCache[K, V]) Len() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	total := 0
	for _, layer := range lc.layers {
		total += layer.Len()
	}
	return total
}
