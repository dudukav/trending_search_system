package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"search_trend/internal/aggregator"
	"search_trend/internal/config"
	"search_trend/internal/transport"
	"time"
)

const shutdownTimeout = 10 * time.Second

type App struct {
	aggregator      aggregator.Aggregator
	logger          *slog.Logger
	server          *http.Server
	rebuildInterval time.Duration
}

func Build(cfg config.Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	agg := aggregator.NewAggregator(
		cfg.WindowSize,
		cfg.BucketSize,
		cfg.SnapshotTopSize,
		cfg.MaxPerIdentity,
	)

	handler, err := transport.New(agg, cfg.MaxLimit, logger)
	if err != nil {
		return nil, fmt.Errorf("create handler: %w", err)
	}

	return &App{
		aggregator:      agg,
		logger:          logger,
		server:          transport.NewServer(cfg.HTTPAddr, handler),
		rebuildInterval: cfg.BucketSize,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	rebuildInterval := a.snapshotRebuildInterval()
	go rebuildSnapshots(ctx, a.aggregator, rebuildInterval)

	errCh := make(chan error, 1)
	go func() {
		a.logger.Info("http server listening", "addr", a.server.Addr)
		errCh <- a.server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		a.logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("http server failed", "error", err)
			return fmt.Errorf("http server failed: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.logger.Error("http server shutdown failed", "error", err)
		return fmt.Errorf("shutdown http server: %w", err)
	}

	a.logger.Info("http server stopped")
	return nil
}

func (a *App) snapshotRebuildInterval() time.Duration {
	if a.rebuildInterval <= 0 {
		return time.Second
	}

	return a.rebuildInterval
}

func rebuildSnapshots(ctx context.Context, agg aggregator.Aggregator, interval time.Duration) {
	agg.RebuildSnapshot(time.Now().UTC())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			agg.RebuildSnapshot(now.UTC())
		}
	}
}
