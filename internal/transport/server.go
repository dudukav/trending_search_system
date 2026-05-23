package transport

import (
	"net/http"
	"search_trend/internal/metrics"
	"time"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 5 * time.Second
	idleTimeout  = 60 * time.Second
)

func NewServer(addr string, handler *Handler, appMetrics *metrics.Metrics) *http.Server {
	mux := http.NewServeMux()

	handler.RegisterRoutes(mux, appMetrics)

	return &http.Server{
		Addr:         addr,
		Handler:      MetricsMiddleware(appMetrics, mux),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux, appMetrics *metrics.Metrics) {
	mux.HandleFunc("GET /v1/trends", h.Top)
	mux.HandleFunc("GET /v1/stoplist", h.List)
	mux.HandleFunc("POST /v1/stoplist", h.Add)
	mux.HandleFunc("DELETE /v1/stoplist/{id}", h.Remove)
	mux.HandleFunc("GET /healthz", h.Health)
	if appMetrics != nil {
		mux.Handle("GET /metrics", appMetrics.Handler())
	}
}
