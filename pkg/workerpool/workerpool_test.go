package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	t.Run("executes tasks correctly", func(t *testing.T) {
		pool := New(2, 10)
		defer pool.Close(context.Background())

		var counter int32
		task := TaskFunc(func(ctx context.Context) error {
			atomic.AddInt32(&counter, 1)
			return nil
		})

		for i := 0; i < 100; i++ {
			if err := pool.Submit(task); err != nil {
				t.Fatalf("Submit failed: %v", err)
			}
		}

		if err := pool.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}

		if atomic.LoadInt32(&counter) != 100 {
			t.Errorf("expected 100 tasks executed, got %d", counter)
		}
	})

	t.Run("handles errors", func(t *testing.T) {
		pool := New(2, 10)
		defer pool.Close(context.Background())

		expectedErr := errors.New("task error")
		task := TaskFunc(func(ctx context.Context) error {
			return expectedErr
		})

		if err := pool.Submit(task); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
		if err := pool.Submit(task); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}

		if err := pool.Close(context.Background()); err == nil {
			t.Error("expected error, got nil")
		} else if !errors.Is(err, expectedErr) {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("graceful shutdown with context cancellation", func(t *testing.T) {
		pool := New(2, 10)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var executed int32
		if err := pool.Submit(TaskFunc(func(ctx context.Context) error {
			// Задача должна успеть выполниться до отмены
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&executed, 1)
			return nil
		})); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}

		// Даём задаче время начать выполнение
		time.Sleep(10 * time.Millisecond)
		cancel() // отменяем контекст, но задача уже выполняется

		// Close должен дождаться завершения задачи
		if err := pool.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Close error: %v", err)
		}

		if atomic.LoadInt32(&executed) != 1 {
			t.Errorf("expected 1 task executed, got %d", executed)
		}
	})

	t.Run("Close blocks until tasks finish", func(t *testing.T) {
		pool := New(1, 10)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		var executed int32
		if err := pool.Submit(TaskFunc(func(ctx context.Context) error {
			time.Sleep(150 * time.Millisecond)
			atomic.AddInt32(&executed, 1)
			return nil
		})); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}

		start := time.Now()
		if err := pool.Close(ctx); err != nil {
			t.Fatalf("Close error: %v", err)
		}
		elapsed := time.Since(start)

		if elapsed < 150*time.Millisecond {
			t.Errorf("Close returned too early, expected at least 150ms, got %v", elapsed)
		}
		if atomic.LoadInt32(&executed) != 1 {
			t.Errorf("expected 1 task executed, got %d", executed)
		}
	})

	t.Run("Close with timeout", func(t *testing.T) {
		pool := New(1, 10)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		if err := pool.Submit(TaskFunc(func(ctx context.Context) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		})); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}

		if err := pool.Close(ctx); err == nil {
			t.Error("expected timeout error, got nil")
		}
	})

	t.Run("Submit after Close returns error", func(t *testing.T) {
		pool := New(1, 10)
		if err := pool.Close(context.Background()); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if err := pool.Submit(TaskFunc(func(ctx context.Context) error { return nil })); err == nil {
			t.Error("expected error on Submit after Close, got nil")
		}
	})
}
