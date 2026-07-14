package shutdown

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdowner(t *testing.T) {
	t.Run("closes in priority order", func(t *testing.T) {
		s := New()

		// Используем атомарные флаги состояния для детерминированной проверки барьеров
		var highDone int32
		var mediumDone int32
		var lowDone int32

		s.RegisterFunc("high", func(ctx context.Context) error {
			// Симулируем микро-задержку ввода-вывода сокета
			time.Sleep(5 * time.Millisecond)
			atomic.StoreInt32(&highDone, 1)
			return nil
		}, PriorityHigh)

		s.RegisterFunc("medium", func(ctx context.Context) error {
			// Слой Medium обязан выполняться только когда High уже закрыт!
			if atomic.LoadInt32(&highDone) == 0 {
				t.Error("PriorityMedium executed before PriorityHigh completed!")
			}
			time.Sleep(5 * time.Millisecond)
			atomic.StoreInt32(&mediumDone, 1)
			return nil
		}, PriorityMedium)

		s.RegisterFunc("low", func(ctx context.Context) error {
			// Слой Low обязан выполняться только когда Medium уже закрыт!
			if atomic.LoadInt32(&mediumDone) == 0 {
				t.Error("PriorityLow executed before PriorityMedium completed!")
			}
			atomic.StoreInt32(&lowDone, 1)
			return nil
		}, PriorityLow)

		// Запускаем боевой Shutdown
		_ = s.Shutdown(context.Background())

		// Финальная верификация, что все слои вообще отработали
		if atomic.LoadInt32(&lowDone) == 0 {
			t.Error("Shutdown sequence failed to reach the terminal Low-priority tier")
		}
	})
}
