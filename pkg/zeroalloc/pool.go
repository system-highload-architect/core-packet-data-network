package zeroalloc

import "sync"

// Pool is a type-safe wrapper around sync.Pool.
type Pool[T any] struct {
	pool sync.Pool
}

// NewPool creates a new Pool with the given constructor function.
func NewPool[T any](newFn func() T) *Pool[T] {
	return &Pool[T]{
		pool: sync.Pool{
			New: func() any {
				return newFn()
			},
		},
	}
}

// Get returns an item from the pool.
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put returns an item to the pool.
func (p *Pool[T]) Put(item T) {
	p.pool.Put(item)
}
