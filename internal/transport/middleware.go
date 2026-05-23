package transport

import (
	"net/http"
	"search_trend/internal/metrics"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func MetricsMiddleware(appMetrics *metrics.Metrics, next http.Handler) http.Handler {
	if appMetrics == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		next.ServeHTTP(recorder, r)
		appMetrics.ObserveHTTP(r.Method, r.URL.Path, recorder.status, time.Since(startedAt))
	})
}
