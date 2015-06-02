package context

// Per request metrics

import (
	"sync"
)

type metrics struct {
	data map[string]int
	mu   sync.Mutex
}

func NewMetrics() metrics {
	return metrics{data: make(map[string]int)}
}

func GetMetrics(ctx Context) map[string]int {
	res := map[string]int{}
	metrics := ctx.Value("metrics").(metrics)
	for k, v := range metrics.data {
		res[k] = v
	}

	return res
}

func IncrementMetric(ctx Context, key string) {
	m := ctx.Value("metrics").(metrics)
	m.increment(key)
}

func (m metrics) increment(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = m.data[key] + 1
}
