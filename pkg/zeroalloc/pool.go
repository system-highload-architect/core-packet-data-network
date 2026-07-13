package zeroalloc

import (
	"sync"
)

// Pool предоставляет типизированный sync.Pool.
type Pool[T any] struct {
	pool sync.Pool
	New  func() T
}

// NewPool создаёт новый пул с фабричной функцией.
func NewPool[T any](newFn func() T) *Pool[T] {
	p := &Pool[T]{
		New: newFn,
	}
	p.pool.New = func() interface{} {
		return p.New()
	}
	return p
}

// Get возвращает объект из пула.
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put возвращает объект в пул.
func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}
