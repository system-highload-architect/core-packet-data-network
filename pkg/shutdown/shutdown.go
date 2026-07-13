package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Priority определяет порядок закрытия.
// Чем выше значение, тем раньше компонент будет закрыт.
type Priority int

const (
	PriorityLowest  Priority = 0
	PriorityLow     Priority = 10
	PriorityMedium  Priority = 20
	PriorityHigh    Priority = 30
	PriorityHighest Priority = 40
)

// Closer — интерфейс для компонентов, которые нужно закрыть.
type Closer interface {
	Close(ctx context.Context) error
}

// CloserFunc — функция-замыкание для упрощения.
type CloserFunc func(ctx context.Context) error

func (f CloserFunc) Close(ctx context.Context) error {
	return f(ctx)
}

// Option — функциональная опция для Shutdowner.
type Option func(*Shutdowner)

// WithTimeout задаёт таймаут для закрытия каждого компонента.
func WithTimeout(timeout time.Duration) Option {
	return func(s *Shutdowner) {
		s.timeout = timeout
	}
}

// WithOnError задаёт функцию для обработки ошибок закрытия.
func WithOnError(fn func(error)) Option {
	return func(s *Shutdowner) {
		s.onError = fn
	}
}

// WithOnClose задаёт функцию для логирования закрытия каждого компонента.
func WithOnClose(fn func(name string, err error)) Option {
	return func(s *Shutdowner) {
		s.onClose = fn
	}
}

// Shutdowner управляет порядком завершения компонентов.
type Shutdowner struct {
	mu      sync.RWMutex
	closers []struct {
		priority Priority
		closer   Closer
		name     string
	}
	timeout time.Duration
	onError func(error)
	onClose func(name string, err error)
}

// New создаёт новый Shutdowner с таймаутом по умолчанию.
func New(opts ...Option) *Shutdowner {
	s := &Shutdowner{
		timeout: 30 * time.Second,
		onError: func(err error) {},
		onClose: func(name string, err error) {},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Register добавляет компонент в список закрываемых.
func (s *Shutdowner) Register(name string, closer Closer, priority Priority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closers = append(s.closers, struct {
		priority Priority
		closer   Closer
		name     string
	}{priority, closer, name})
}

// RegisterFunc — упрощённая регистрация функции.
func (s *Shutdowner) RegisterFunc(name string, fn func(ctx context.Context) error, priority Priority) {
	s.Register(name, CloserFunc(fn), priority)
}

// Shutdown закрывает все компоненты в порядке приоритета (от высокого к низкому).
func (s *Shutdowner) Shutdown(ctx context.Context) error {
	s.mu.RLock()
	closers := make([]struct {
		priority Priority
		closer   Closer
		name     string
	}, len(s.closers))
	copy(closers, s.closers)
	s.mu.RUnlock()

	// Сортируем по убыванию приоритета (высший — первый)
	sort.Slice(closers, func(i, j int) bool {
		return closers[i].priority > closers[j].priority
	})

	var wg sync.WaitGroup
	errCh := make(chan error, len(closers))

	for _, c := range closers {
		wg.Add(1)
		go func(c struct {
			priority Priority
			closer   Closer
			name     string
		}) {
			defer wg.Done()
			// Создаём дочерний контекст с таймаутом для каждого компонента
			ctx, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			err := c.closer.Close(ctx)
			if err != nil {
				s.onError(err)
				errCh <- fmt.Errorf("closing %s: %w", c.name, err)
			}
			s.onClose(c.name, err)
		}(c)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}

// WaitForSignal ожидает сигнала завершения и запускает Shutdown.
func (s *Shutdowner) WaitForSignal(ctx context.Context) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
		return s.Shutdown(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}
