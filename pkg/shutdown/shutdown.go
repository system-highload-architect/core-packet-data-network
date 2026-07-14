package shutdown

import (
	"context"
	"sync"
)

type Priority int

const (
	PriorityHigh   Priority = 3
	PriorityMedium Priority = 2
	PriorityLow    Priority = 1
)

// ContextCloser — расширенный интерфейс закрытия с поддержкой контекста отмены/таймаута
type ContextCloser interface {
	Close(ctx context.Context) error
}

type task struct {
	name string
	fn   func(ctx context.Context) error
}

type Shutdowner struct {
	mu    sync.Mutex
	tasks map[Priority][]task
}

func New() *Shutdowner {
	return &Shutdowner{
		tasks: make(map[Priority][]task),
	}
}

func (s *Shutdowner) RegisterFunc(name string, fn func(ctx context.Context) error, p Priority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[p] = append(s.tasks[p], task{name: name, fn: fn})
}

// Register принимает наш новый интерфейс ContextCloser и прокидывает контекст дальше
func (s *Shutdowner) Register(name string, closer ContextCloser, p Priority) {
	s.RegisterFunc(name, func(ctx context.Context) error {
		return closer.Close(ctx)
	}, p)
}

// Shutdown последовательно с жесткими барьерами закрывает слои, передавая контекст таймаута
func (s *Shutdowner) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	highTasks := s.tasks[PriorityHigh]
	mediumTasks := s.tasks[PriorityMedium]
	lowTasks := s.tasks[PriorityLow]
	s.mu.Unlock()

	// --- БАРЬЕР 1: PriorityHigh (Сетевые сокеты) ---
	if len(highTasks) > 0 {
		var wg sync.WaitGroup
		for _, t := range highTasks {
			wg.Add(1)
			go func(tk task) {
				defer wg.Done()
				_ = tk.fn(ctx) // Передаем контекст таймаута!
			}(t)
		}
		wg.Wait()
	}

	// --- БАРЬЕР 2: PriorityMedium (Воркерпулы) ---
	if len(mediumTasks) > 0 {
		var wg sync.WaitGroup
		for _, t := range mediumTasks {
			wg.Add(1)
			go func(tk task) {
				defer wg.Done()
				_ = tk.fn(ctx)
			}(t)
		}
		wg.Wait()
	}

	// --- БАРЬЕР 3: PriorityLow (Логгеры, Метрики) ---
	if len(lowTasks) > 0 {
		var wg sync.WaitGroup
		for _, t := range lowTasks {
			wg.Add(1)
			go func(tk task) {
				defer wg.Done()
				_ = tk.fn(ctx)
			}(t)
		}
		wg.Wait()
	}

	return nil
}
