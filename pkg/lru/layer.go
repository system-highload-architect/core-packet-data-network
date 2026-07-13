package lru

import (
	"sync"
	"time"
)

// LayerConfig описывает один слой кэша.
type LayerConfig struct {
	TTL        time.Duration
	MaxAttempt int // максимальное количество попыток в этом слое
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
	for _, cfg := range configs {
		// Создаём слой с заданным TTL и передаём опции
		cache := NewCache[K, V](cfg.TTL, opts...)
		lc.layers = append(lc.layers, cache)
	}
	return lc
}

// Set добавляет элемент в первый слой.
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

// Promote перемещает элемент на следующий уровень (например, после неудачной попытки).
// Возвращает true, если элемент был перемещён на следующий слой, false, если это был последний слой.
func (lc *LayerCache[K, V]) Promote(key K) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for i := 0; i < len(lc.layers)-1; i++ {
		val, ok := lc.layers[i].Get(key)
		if ok {
			lc.layers[i].Delete(key)
			lc.layers[i+1].Set(key, val)
			return true
		}
	}
	if len(lc.layers) > 0 {
		if _, ok := lc.layers[len(lc.layers)-1].Get(key); ok {
			lc.layers[len(lc.layers)-1].Delete(key)
		}
	}
	return false
}

// Delete удаляет элемент из всех слоёв.
func (lc *LayerCache[K, V]) Delete(key K) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, layer := range lc.layers {
		layer.Delete(key)
	}
}

// Close завершает работу всех слоёв.
func (lc *LayerCache[K, V]) Close() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, layer := range lc.layers {
		layer.Close()
	}
}
