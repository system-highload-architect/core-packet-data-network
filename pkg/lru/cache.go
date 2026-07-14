package lru

import (
	"sync"
	"time"
)

// node представляет элемент в двусвязном списке, упорядоченном по expiresAt (голова = самое позднее, хвост = самое раннее).
type node[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
	prev      *node[K, V]
	next      *node[K, V]
}

// Cache — обобщённый кэш с TTL, безопасный для конкурентного использования.
type Cache[K comparable, V any] struct {
	mu               sync.Mutex
	ttl              time.Duration
	items            map[K]*node[K, V]
	head             *node[K, V] // самый поздний (будет удалён позже)
	tail             *node[K, V] // самый ранний (будет удалён раньше)
	finalizer        func(key K, value V)
	finalizerWorkers int
	finalizerBuf     int
	finalizeCh       chan *node[K, V]
	stopCh           chan struct{}
	wakeCh           chan struct{}
	stopped          bool
	now              func() time.Time // для тестирования
}

// NewCache создаёт новый TTL-кэш.
func NewCache[K comparable, V any](ttl time.Duration, opts ...CacheOption[K, V]) *Cache[K, V] {
	c := &Cache[K, V]{
		ttl:              ttl,
		items:            make(map[K]*node[K, V]),
		finalizerWorkers: 1,
		finalizerBuf:     100,
		now:              time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.finalizer != nil {
		c.finalizeCh = make(chan *node[K, V], c.finalizerBuf)
		c.stopCh = make(chan struct{})
		c.wakeCh = make(chan struct{}, 1)
		for i := 0; i < c.finalizerWorkers; i++ {
			go c.finalizerWorker()
		}
		go c.cleanupLoop()
	}
	return c
}

// CacheOption — функциональная опция для настройки кэша.
type CacheOption[K comparable, V any] func(*Cache[K, V])

// WithFinalizer задаёт функцию, которая будет вызываться при удалении элемента.
func WithFinalizer[K comparable, V any](fn func(key K, value V)) CacheOption[K, V] {
	return func(c *Cache[K, V]) {
		c.finalizer = fn
	}
}

// WithFinalizerWorkers задаёт количество горутин для финализации.
func WithFinalizerWorkers[K comparable, V any](n int) CacheOption[K, V] {
	return func(c *Cache[K, V]) {
		if n > 0 {
			c.finalizerWorkers = n
		}
	}
}

// WithFinalizerBuffer задаёт размер буфера для финализации.
func WithFinalizerBuffer[K comparable, V any](n int) CacheOption[K, V] {
	return func(c *Cache[K, V]) {
		if n > 0 {
			c.finalizerBuf = n
		}
	}
}

// WithNowFunc задаёт функцию получения текущего времени (для тестов).
func WithNowFunc[K comparable, V any](fn func() time.Time) CacheOption[K, V] {
	return func(c *Cache[K, V]) {
		if fn != nil {
			c.now = fn
		}
	}
}

// Set добавляет или обновляет элемент в кэше.
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}
	// Если уже есть, удаляем старый узел
	if old, ok := c.items[key]; ok {
		c.removeNode(old)
	}
	// Создаём новый узел
	now := c.now()
	node := &node[K, V]{
		key:       key,
		value:     value,
		expiresAt: now.Add(c.ttl),
	}
	c.items[key] = node
	// Вставляем в голову (самый поздний)
	c.insertHead(node)
}

// Get возвращает значение и true, если элемент существует и не истёк.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	if c.now().After(node.expiresAt) {
		// Истёк — удаляем и возвращаем false
		c.removeNode(node)
		delete(c.items, key)
		c.scheduleFinalize(node)
		var zero V
		return zero, false
	}
	return node.value, true
}

// Delete удаляет элемент из кэша.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.items[key]
	if !ok {
		return
	}
	c.removeNode(node)
	delete(c.items, key)
	c.scheduleFinalize(node)
}

// Len возвращает текущее количество элементов в кэше.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Close завершает работу кэша (останавливает горутины очистки).
func (c *Cache[K, V]) Close() {
	if c.finalizer != nil && c.stopCh != nil {
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		close(c.stopCh)
		close(c.finalizeCh)
	}
}

// insertHead вставляет узел в голову (самый поздний).
func (c *Cache[K, V]) insertHead(n *node[K, V]) {
	if c.head == nil {
		c.head = n
		c.tail = n
		return
	}
	n.next = c.head
	c.head.prev = n
	c.head = n
}

// removeNode удаляет узел из списка (предполагается, что мьютекс уже захвачен).
func (c *Cache[K, V]) removeNode(n *node[K, V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		c.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		c.tail = n.prev
	}
	n.prev = nil
	n.next = nil
}

// scheduleFinalize отправляет узел в канал финализации (если финализатор задан).
func (c *Cache[K, V]) scheduleFinalize(n *node[K, V]) {
	if c.finalizer != nil && c.finalizeCh != nil {
		select {
		case c.finalizeCh <- n:
		default:
			// Буфер переполнен — вызываем синхронно (чтобы не блокировать)
			c.finalizer(n.key, n.value)
		}
	}
}

// finalizerWorker обрабатывает финализацию узлов.
func (c *Cache[K, V]) finalizerWorker() {
	for n := range c.finalizeCh {
		c.finalizer(n.key, n.value)
	}
}

// cleanupLoop периодически удаляет истёкшие элементы.
func (c *Cache[K, V]) cleanupLoop() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.cleanup()
		case <-c.wakeCh:
			c.cleanup()
		}
	}
}

// cleanup удаляет все истёкшие элементы.
func (c *Cache[K, V]) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	// Идём с хвоста (самые ранние)
	for c.tail != nil && now.After(c.tail.expiresAt) {
		n := c.tail
		c.removeNode(n)
		delete(c.items, n.key)
		c.scheduleFinalize(n)
	}
	// Если после очистки остались элементы, пробуем запланировать следующую очистку
	if c.tail != nil {
		timeUntil := c.tail.expiresAt.Sub(now)
		if timeUntil < 0 {
			timeUntil = 0
		}
		// Не блокируем — просто отправляем в канал wakeCh, если он не заполнен
		select {
		case c.wakeCh <- struct{}{}:
		default:
		}
	}
}

// PeekTail возвращает ключ, значение и время истечения самого старого элемента из хвоста (без удаления).
// PeekTail returns the oldest item from the tail along with its expiration time without evicting it.
func (c *Cache[K, V]) PeekTail() (K, V, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tail == nil {
		var zeroK K
		var zeroV V
		return zeroK, zeroV, time.Time{}, false
	}
	return c.tail.key, c.tail.value, c.tail.expiresAt, true
}

// PopTail извлекает (удаляет и возвращает) самый старый элемент из хвоста очереди.
// PopTail evicts and returns the absolute oldest item from the queue tail.
func (c *Cache[K, V]) PopTail() (K, V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tail == nil {
		var zeroK K
		var zeroV V
		return zeroK, zeroV, false
	}
	n := c.tail
	c.removeNode(n)
	delete(c.items, n.key)

	// RU: НЕ вызываем scheduleFinalize, так как узел извлекается для ретрансмиссии/переноса слоев
	// EN: Explicitly bypass finalization since node is popped for layer promotion routines
	return n.key, n.value, true
}

// SetWithTTL принудительно устанавливает элемент с кастомным TTL (для точного экспоненциального backoff)
// SetWithTTL overrides and forces a specific expiration window duration onto an item
func (c *Cache[K, V]) SetWithTTL(key K, value V, customTTL time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}
	if old, ok := c.items[key]; ok {
		c.removeNode(old)
	}
	node := &node[K, V]{
		key:       key,
		value:     value,
		expiresAt: c.now().Add(customTTL),
	}
	c.items[key] = node
	c.insertHead(node)
}
