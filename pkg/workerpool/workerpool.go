package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Task — интерфейс задачи.
type Task interface {
	Execute(ctx context.Context) error
}

// TaskFunc — адаптер функции к интерфейсу Task.
type TaskFunc func(ctx context.Context) error

func (f TaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

// Pool — пул воркеров с graceful shutdown.
type Pool struct {
	workers int
	taskCh  chan Task
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	errOnce sync.Once
	err     error
	closed  atomic.Bool
}

// New создаёт новый пул воркеров.
func New(workers int, queueSize int) *Pool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 10
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		workers: workers,
		taskCh:  make(chan Task, queueSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

// worker обрабатывает задачи из канала до его закрытия.
func (p *Pool) worker() {
	defer p.wg.Done()
	for task := range p.taskCh {
		if err := task.Execute(p.ctx); err != nil {
			p.errOnce.Do(func() {
				p.err = err
			})
		}
	}
}

// Submit добавляет задачу в очередь.
func (p *Pool) Submit(task Task) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		select {
		case p.taskCh <- task:
			return nil
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}
}

// Close завершает работу пула, дожидаясь выполнения всех задач.
func (p *Pool) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return ErrPoolClosed
	}
	// Закрываем канал — воркеры дочитают все оставшиеся задачи
	close(p.taskCh)
	// Отменяем внутренний контекст (чтобы новые задачи не принимались)
	p.cancel()

	// Определяем таймаут для ожидания завершения воркеров
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return ctx.Err() // контекст уже истёк
		}
	} else {
		timeout = 30 * time.Second // таймаут по умолчанию
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return p.err
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// ErrPoolClosed возвращается при попытке Submit после Close.
var ErrPoolClosed = &poolClosedError{}

type poolClosedError struct{}

func (e *poolClosedError) Error() string {
	return "workerpool: pool already closed"
}
