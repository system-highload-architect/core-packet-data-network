package zeroalloc

import (
	"testing"
)

func TestPool(t *testing.T) {
	type testObj struct {
		val int
	}
	pool := NewPool(func() testObj {
		return testObj{val: 42}
	})
	obj1 := pool.Get()
	if obj1.val != 42 {
		t.Errorf("expected 42, got %d", obj1.val)
	}
	// Не изменяем объект, чтобы он остался в исходном состоянии.
	pool.Put(obj1)

	obj2 := pool.Get()
	if obj2.val != 42 {
		t.Errorf("expected 42, got %d", obj2.val)
	}
}

func TestBuffer(t *testing.T) {
	b := NewBuffer(10)
	b.Write([]byte("hello"))
	if b.Len() != 5 {
		t.Errorf("expected length 5, got %d", b.Len())
	}
	b.WriteByte(' ')
	b.Write([]byte("world"))
	if b.Len() != 11 {
		t.Errorf("expected length 11, got %d", b.Len())
	}
	if string(b.Bytes()) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(b.Bytes()))
	}
	b.Reset()
	if b.Len() != 0 {
		t.Errorf("expected length 0 after reset, got %d", b.Len())
	}
}
