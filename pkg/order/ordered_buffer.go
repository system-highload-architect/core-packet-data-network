package order

import (
	"sync"
)

type OrderedBuffer[V any] struct {
	mu     sync.Mutex
	nextID uint64
	buffer map[uint64]V
}

func NewOrderedBuffer[V any](startID uint64) *OrderedBuffer[V] {
	return &OrderedBuffer[V]{
		nextID: startID,
		buffer: make(map[uint64]V),
	}
}

func (ob *OrderedBuffer[V]) Insert(id uint64, value V) []V {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if id < ob.nextID {
		return nil
	}
	ob.buffer[id] = value

	var result []V
	for {
		val, ok := ob.buffer[ob.nextID]
		if !ok {
			break
		}
		result = append(result, val)
		delete(ob.buffer, ob.nextID)
		ob.nextID++
	}
	return result
}

func (ob *OrderedBuffer[V]) Len() int {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return len(ob.buffer)
}
