package zeroalloc

import "bytes"

// Buffer is a reusable buffer that wraps bytes.Buffer and resets on Get.
type Buffer struct {
	pool *Pool[*bytes.Buffer]
}

// NewBuffer creates a new buffer pool.
func NewBuffer() *Buffer {
	return &Buffer{
		pool: NewPool(func() *bytes.Buffer {
			return &bytes.Buffer{}
		}),
	}
}

// Get returns a cleared buffer.
func (b *Buffer) Get() *bytes.Buffer {
	buf := b.pool.Get()
	buf.Reset()
	return buf
}

// Put returns the buffer to the pool.
func (b *Buffer) Put(buf *bytes.Buffer) {
	b.pool.Put(buf)
}
