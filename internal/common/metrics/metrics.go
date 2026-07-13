package metrics

import (
	"sync/atomic"
)

// Counter — атомарный счётчик.
type Counter struct {
	value uint64
}

// Inc увеличивает счётчик на 1.
func (c *Counter) Inc() {
	atomic.AddUint64(&c.value, 1)
}

// Add увеличивает счётчик на n.
func (c *Counter) Add(n uint64) {
	atomic.AddUint64(&c.value, n)
}

// Value возвращает текущее значение.
func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.value)
}

// Gauge — атомарный датчик (текущее значение).
type Gauge struct {
	value uint64
}

// Set устанавливает значение.
func (g *Gauge) Set(v uint64) {
	atomic.StoreUint64(&g.value, v)
}

// Add прибавляет n к значению.
func (g *Gauge) Add(n int64) {
	atomic.AddUint64(&g.value, uint64(n))
}

// Value возвращает текущее значение.
func (g *Gauge) Value() uint64 {
	return atomic.LoadUint64(&g.value)
}

// Histogram — простая гистограмма (минимальное, максимальное, среднее).
type Histogram struct {
	min   uint64
	max   uint64
	sum   uint64
	count uint64
}

// Observe записывает наблюдение.
func (h *Histogram) Observe(v uint64) {
	if v < h.min || h.count == 0 {
		h.min = v
	}
	if v > h.max || h.count == 0 {
		h.max = v
	}
	h.sum += v
	h.count++
}

// Min возвращает минимальное значение.
func (h *Histogram) Min() uint64 {
	return h.min
}

// Max возвращает максимальное значение.
func (h *Histogram) Max() uint64 {
	return h.max
}

// Avg возвращает среднее значение.
func (h *Histogram) Avg() uint64 {
	if h.count == 0 {
		return 0
	}
	return h.sum / h.count
}

// Count возвращает количество наблюдений.
func (h *Histogram) Count() uint64 {
	return h.count
}

// Registry хранит все метрики.
type Registry struct {
	Counters   map[string]*Counter
	Gauges     map[string]*Gauge
	Histograms map[string]*Histogram
}

// NewRegistry создаёт новый реестр метрик.
func NewRegistry() *Registry {
	return &Registry{
		Counters:   make(map[string]*Counter),
		Gauges:     make(map[string]*Gauge),
		Histograms: make(map[string]*Histogram),
	}
}

// Counter возвращает счётчик по имени, создаёт новый, если не существует.
func (r *Registry) Counter(name string) *Counter {
	if c, ok := r.Counters[name]; ok {
		return c
	}
	c := &Counter{}
	r.Counters[name] = c
	return c
}

// Gauge возвращает датчик по имени.
func (r *Registry) Gauge(name string) *Gauge {
	if g, ok := r.Gauges[name]; ok {
		return g
	}
	g := &Gauge{}
	r.Gauges[name] = g
	return g
}

// Histogram возвращает гистограмму по имени.
func (r *Registry) Histogram(name string) *Histogram {
	if h, ok := r.Histograms[name]; ok {
		return h
	}
	h := &Histogram{}
	r.Histograms[name] = h
	return h
}
