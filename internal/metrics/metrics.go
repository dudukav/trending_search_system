package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	KafkaMessagesTotal      *prometheus.CounterVec
	KafkaProcessingDuration *prometheus.HistogramVec
	DLQMessagesTotal        *prometheus.CounterVec

	SnapshotRebuildTotal    prometheus.Counter
	SnapshotRebuildDuration prometheus.Histogram
}

func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trending_search_http_requests_total",
			Help: "Total HTTP requests handled by the service.",
		}, []string{"method", "path", "status"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "trending_search_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path", "status"}),
		KafkaMessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trending_search_kafka_messages_total",
			Help: "Kafka messages processed by result.",
		}, []string{"result"}),
		KafkaProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "trending_search_kafka_processing_duration_seconds",
			Help:    "Kafka message processing duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
		DLQMessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trending_search_dlq_messages_total",
			Help: "Messages published to DLQ by result.",
		}, []string{"result"}),
		SnapshotRebuildTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trending_search_snapshot_rebuild_total",
			Help: "Total snapshot rebuilds.",
		}),
		SnapshotRebuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trending_search_snapshot_rebuild_duration_seconds",
			Help:    "Snapshot rebuild duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	m.registry.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.KafkaMessagesTotal,
		m.KafkaProcessingDuration,
		m.DLQMessagesTotal,
		m.SnapshotRebuildTotal,
		m.SnapshotRebuildDuration,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveHTTP(method string, path string, status int, duration time.Duration) {
	statusLabel := strconv.Itoa(status)
	m.HTTPRequestsTotal.WithLabelValues(method, path, statusLabel).Inc()
	m.HTTPRequestDuration.WithLabelValues(method, path, statusLabel).Observe(duration.Seconds())
}

func (m *Metrics) ObserveKafkaMessage(result string, duration time.Duration) {
	m.KafkaMessagesTotal.WithLabelValues(result).Inc()
	m.KafkaProcessingDuration.WithLabelValues(result).Observe(duration.Seconds())
}

func (m *Metrics) ObserveDLQ(result string) {
	m.DLQMessagesTotal.WithLabelValues(result).Inc()
}

func (m *Metrics) ObserveSnapshotRebuild(duration time.Duration) {
	m.SnapshotRebuildTotal.Inc()
	m.SnapshotRebuildDuration.Observe(duration.Seconds())
}
