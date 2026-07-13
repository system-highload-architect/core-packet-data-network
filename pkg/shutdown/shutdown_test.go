package shutdown

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestShutdowner(t *testing.T) {
	t.Run("closes in priority order", func(t *testing.T) {
		s := New()
		var order []string
		var mu sync.Mutex

		// регистрируем компоненты с разными приоритетами
		s.RegisterFunc("high", func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "high")
			mu.Unlock()
			return nil
		}, PriorityHigh)

		s.RegisterFunc("medium", func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "medium")
			mu.Unlock()
			return nil
		}, PriorityMedium)

		s.RegisterFunc("low", func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "low")
			mu.Unlock()
			return nil
		}, PriorityLow)

		_ = s.Shutdown(context.Background())
		// ожидаем [high, medium, low]
		if !reflect.DeepEqual(order, []string{"high", "medium", "low"}) {
			t.Errorf("unexpected order: %v", order)
		}
	})
}
