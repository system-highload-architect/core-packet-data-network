package order

import (
	"sync"
)

// RingBuffer хранит элементы в кольцевом буфере.
type RingBuffer[V any] struct {
	mu      sync.Mutex
	buf     []V
	filled  []bool
	size    uint64
	startID uint64
	nextID  uint64
	count   uint64
}

// NewRingBuffer создаёт кольцевой буфер с заданным размером и начальным ID.
func NewRingBuffer[V any](size uint64, startID uint64) *RingBuffer[V] {
	return &RingBuffer[V]{
		buf:     make([]V, size),
		filled:  make([]bool, size),
		size:    size,
		startID: startID,
		nextID:  startID,
	}
}

// Insert добавляет элемент, если его ID в диапазоне.
func (rb *RingBuffer[V]) Insert(id uint64, value V) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if id < rb.startID || id >= rb.startID+rb.size {
		return false
	}
	offset := (id - rb.startID) % rb.size
	rb.buf[offset] = value
	if !rb.filled[offset] {
		rb.filled[offset] = true
		rb.count++
	}
	return true
}

// GetNext возвращает следующий элемент по порядку.
func (rb *RingBuffer[V]) GetNext() (V, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == 0 || rb.nextID >= rb.startID+rb.size {
		var zero V
		return zero, false
	}
	offset := (rb.nextID - rb.startID) % rb.size
	if !rb.filled[offset] {
		var zero V
		return zero, false
	}
	val := rb.buf[offset]
	var zero V
	rb.buf[offset] = zero
	rb.filled[offset] = false
	rb.count--
	rb.nextID++
	return val, true
}

// Len возвращает количество элементов в буфере.
func (rb *RingBuffer[V]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return int(rb.count)
}
