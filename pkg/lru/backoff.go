package lru

import "time"

// Backoff — интерфейс для стратегии задержки между попытками.
type Backoff interface {
	// Next возвращает задержку для следующей попытки.
	Next(attempt int) time.Duration
}

// ExponentialBackoff — экспоненциальная стратегия с ограничением.
type ExponentialBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// NewExponentialBackoff создаёт стратегию по умолчанию.
func NewExponentialBackoff() *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     5 * time.Second,
		Multiplier:      2.0,
	}
}

// Next возвращает задержку для попытки с номером attempt (начиная с 0).
func (b *ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := b.InitialInterval
	for i := 0; i < attempt; i++ {
		d = time.Duration(float64(d) * b.Multiplier)
		if d > b.MaxInterval {
			return b.MaxInterval
		}
	}
	return d
}

// LinearBackoff — линейная стратегия (просто интервал * попытка).
type LinearBackoff struct {
	Interval time.Duration
}

// Next возвращает задержку для попытки.
func (b *LinearBackoff) Next(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return b.Interval * time.Duration(attempt+1)
}
