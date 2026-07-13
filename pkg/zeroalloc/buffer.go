package zeroalloc

// Buffer — переиспользуемый байтовый буфер.
type Buffer struct {
	buf []byte
}

// NewBuffer создаёт буфер с начальной ёмкостью.
func NewBuffer(cap int) *Buffer {
	return &Buffer{
		buf: make([]byte, 0, cap),
	}
}

// Reset обнуляет длину, но сохраняет ёмкость.
func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
}

// Write добавляет байты в буфер.
func (b *Buffer) Write(p []byte) {
	b.buf = append(b.buf, p...)
}

// WriteByte добавляет один байт.
func (b *Buffer) WriteByte(c byte) {
	b.buf = append(b.buf, c)
}

// Bytes возвращает текущий срез.
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// Len возвращает длину.
func (b *Buffer) Len() int {
	return len(b.buf)
}

// Cap возвращает ёмкость.
func (b *Buffer) Cap() int {
	return cap(b.buf)
}

// Grow увеличивает ёмкость буфера.
func (b *Buffer) Grow(n int) {
	if cap(b.buf)-len(b.buf) < n {
		newCap := cap(b.buf) * 2
		if newCap < len(b.buf)+n {
			newCap = len(b.buf) + n
		}
		newBuf := make([]byte, len(b.buf), newCap)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
}
