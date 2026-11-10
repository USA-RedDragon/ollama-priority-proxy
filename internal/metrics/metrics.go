// Package metrics defines the Prometheus metrics of the proxy.
package metrics

import (
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace  = "ollama_priority_proxy"
	labelClass = "class"
	labelRoute = "route"
)

// Label values for the class label.
const (
	ClassHigh = "high"
	ClassLow  = "low"
)

// Metrics holds the request metrics.
type Metrics struct {
	Requests        *prometheus.CounterVec
	Errors          *prometheus.CounterVec
	QueueWait       *prometheus.HistogramVec
	UpstreamLatency *prometheus.HistogramVec
}

// New creates the metrics, registers them with reg, and registers gauges
// that read the in-flight counts and queue depth from sched.
func New(reg prometheus.Registerer, sched *scheduler.Scheduler) (*Metrics, error) {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "Requests handled, by priority class, route and response status code.",
		}, []string{labelClass, labelRoute, "code"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Requests that failed, by priority class, route and reason: upstream, max_wait, canceled or bad_request.",
		}, []string{labelClass, labelRoute, "reason"}),
		QueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "queue_wait_seconds",
			Help:      "Time generation requests waited before they were sent to the upstream, by priority class.",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800},
		}, []string{labelClass}),
		UpstreamLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "upstream_duration_seconds",
			Help:      "Time from sending a request to the upstream until its response body was fully copied, by priority class and route.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{labelClass, labelRoute}),
	}

	inFlight := func(class string, get func(scheduler.Stats) int) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "in_flight",
			Help:        "Generation requests sent to the upstream and not yet finished, by priority class.",
			ConstLabels: prometheus.Labels{labelClass: class},
		}, func() float64 { return float64(get(sched.Stats())) })
	}
	collectors := []prometheus.Collector{
		m.Requests,
		m.Errors,
		m.QueueWait,
		m.UpstreamLatency,
		inFlight(ClassHigh, func(s scheduler.Stats) int { return s.HighInFlight }),
		inFlight(ClassLow, func(s scheduler.Stats) int { return s.LowInFlight }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Low priority generation requests waiting in the queue.",
		}, func() float64 { return float64(sched.Stats().LowQueued) }),
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}
