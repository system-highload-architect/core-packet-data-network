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

	// RU: Извлекаем функцию Now из опций, если она там есть, для самого LayerCache
	// EN: Extract Now function from options if present for LayerCache itself
	dummyCache := &Cache[K, V]{now: time.Now}
	for _, opt := range opts {
		opt(dummyCache)
	}
	lc.now = dummyCache.now

	for _, cfg := range configs {
		// RU: Исправлено: передаем opts дальше в каждый слой, чтобы WithNowFunc применился везде!
		// EN: Fixed: forward opts down into each individual layer instance explicitly!
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
// PeekExpiredScan checks tails across all layers and returns the absolute oldest expired item.
func (lc *LayerCache[K, V]) PeekExpiredScan() (K, V, int, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	now := lc.now()

	// RU: Проверяем слои последовательно. Слои с меньшим индексом приоритетнее.
	// EN: Scan layers sequentially. Lower layer indices yield higher processing priority.
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
// GetEarliestDeadline aggregates and returns the closest deadline timestamp observed across all tails.
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
// PromoteLayer upgrades an item out of its current layer context up into the next backoff stage.
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
		// RU: Достигнут крайний слой — выталкивать больше некуда, пакет мертв
		// EN: Terminal layer boundary reached — eviction limits exhausted, package dead
		return val, false
	}

	// RU: Рассчитываем экспоненциальный TTL для следующего слоя на основе стратегии Backoff
	// EN: Compute exponential TTL duration bounds for next layer using the Backoff strategy
	nextTTL := lc.backoff.Next(nextLayer)

	// RU: Помещаем на следующий слой с расширенным окном ожидания
	// EN: Push item into the next storage tier bound by expanded TTL windows
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
// Len returns the cumulative count of elements retained across all storage tiers.
func (lc *LayerCache[K, V]) Len() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	total := 0
	for _, layer := range lc.layers {
		total += layer.Len() // Используем готовый метод Len() базового Cache
	}
	return total
}
