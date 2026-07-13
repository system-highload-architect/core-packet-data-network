package order

import (
	"testing"
)

func TestOrderedBuffer(t *testing.T) {
	buf := NewOrderedBuffer[int](1)

	// Вставляем в разнобой
	buf.Insert(3, 30)
	buf.Insert(5, 50)
	buf.Insert(4, 40)
	buf.Insert(2, 20)

	// Ничего не должно выдаться, т.к. нет 1
	if buf.Len() != 4 {
		t.Errorf("expected buffer len 4, got %d", buf.Len())
	}

	// Вставляем 1 — должно выдать все пять
	res := buf.Insert(1, 10)
	expected := []int{10, 20, 30, 40, 50}
	if len(res) != 5 {
		t.Errorf("expected 5 results, got %d", len(res))
	}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("expected %d, got %d at index %d", expected[i], v, i)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("expected buffer empty, got %d", buf.Len())
	}
}

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](5, 1)
	if !rb.Insert(2, 20) {
		t.Error("failed to insert 2")
	}
	if !rb.Insert(4, 40) {
		t.Error("failed to insert 4")
	}
	if !rb.Insert(1, 10) {
		t.Error("failed to insert 1")
	}
	if !rb.Insert(3, 30) {
		t.Error("failed to insert 3")
	}
	val, ok := rb.GetNext()
	if !ok || val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
	val, ok = rb.GetNext()
	if !ok || val != 20 {
		t.Errorf("expected 20, got %d", val)
	}
	val, ok = rb.GetNext()
	if !ok || val != 30 {
		t.Errorf("expected 30, got %d", val)
	}
	val, ok = rb.GetNext()
	if !ok || val != 40 {
		t.Errorf("expected 40, got %d", val)
	}
	_, ok = rb.GetNext()
	if ok {
		t.Error("expected no more elements")
	}
}
