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

// Priority определяет порядок закрытия (чем меньше число, тем выше приоритет).
type Priority int

const (
	PriorityHighest Priority = 1
	PriorityHigh    Priority = 2
	PriorityMedium  Priority = 3
	PriorityLow     Priority = 4
	PriorityLowest  Priority = 5
)

// Closer — интерфейс для компонентов, которые нужно закрыть.
type Closer interface {
	Close(ctx context.Context) error
}

// CloserFunc — функция-замыкание.
type CloserFunc func(ctx context.Context) error

func (f CloserFunc) Close(ctx context.Context) error {
	return f(ctx)
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

// Option — функция-опция.
type Option func(*Shutdowner)

// WithTimeout устанавливает таймаут для завершения (используется, если контекст не имеет дедлайна).
func WithTimeout(d time.Duration) Option {
	return func(s *Shutdowner) {
		s.timeout = d
	}
}

// WithOnError устанавливает обработчик ошибок.
func WithOnError(fn func(error)) Option {
	return func(s *Shutdowner) {
		s.onError = fn
	}
}

// WithOnClose устанавливает обработчик для каждого закрытого компонента.
func WithOnClose(fn func(name string, err error)) Option {
	return func(s *Shutdowner) {
		s.onClose = fn
	}
}

// New создаёт новый Shutdowner с опциями.
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

// Shutdown закрывает все компоненты в порядке приоритета (от меньшего числа к большему).
func (s *Shutdowner) Shutdown(ctx context.Context) error {
	// TODO: заглушка
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.RLock()
	closers := make([]struct {
		priority Priority
		closer   Closer
		name     string
	}, len(s.closers))
	copy(closers, s.closers)
	s.mu.RUnlock()

	// Сортируем по возрастанию приоритета (меньше число = раньше)
	sort.Slice(closers, func(i, j int) bool {
		return closers[i].priority < closers[j].priority
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
			// Используем переданный контекст (с возможным общим таймаутом)
			err := c.closer.Close(ctx)
			if err != nil {
				s.onError(err)
				errCh <- fmt.Errorf("closing %s: %w", c.name, err)
			}
			s.onClose(c.name, err)
		}(c)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(errCh)
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			return fmt.Errorf("shutdown errors: %v", errs)
		}
		return nil
	case <-ctx.Done():
		// Контекст истёк — возвращаем ошибку
		return ctx.Err()
	}
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
