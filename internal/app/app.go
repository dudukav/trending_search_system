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
	"search_trend/internal/consumer"
	"search_trend/internal/metrics"
	"search_trend/internal/schema"
	"search_trend/internal/transport"
	"time"
)

const shutdownTimeout = 10 * time.Second

type App struct {
	aggregator      aggregator.Aggregator
	consumer        *consumer.Consumer
	dlqPublisher    *consumer.KafkaDLQPublisher
	kafkaClient     *consumer.KafkaClient
	logger          *slog.Logger
	metrics         *metrics.Metrics
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
	appMetrics := metrics.New()

	handler, err := transport.New(agg, cfg.MaxLimit, logger)
	if err != nil {
		return nil, fmt.Errorf("create handler: %w", err)
	}

	kafkaClient, err := consumer.NewKafkaClient(consumer.KafkaConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	dlqPublisher, err := consumer.NewKafkaDLQPublisher(consumer.DLQConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaDLQTopic,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka dlq publisher: %w", err)
	}

	avroCodec, err := loadAvroCodec(context.Background(), cfg, logger)
	if err != nil {
		return nil, err
	}

	eventConsumer := consumer.New(
		kafkaClient,
		kafkaClient,
		consumer.NewDecoder(avroCodec),
		consumer.NewHandler(agg, logger),
		dlqPublisher,
		appMetrics,
		logger,
	)

	return &App{
		aggregator:      agg,
		consumer:        eventConsumer,
		dlqPublisher:    dlqPublisher,
		kafkaClient:     kafkaClient,
		logger:          logger,
		metrics:         appMetrics,
		server:          transport.NewServer(cfg.HTTPAddr, handler, appMetrics),
		rebuildInterval: cfg.BucketSize,
	}, nil
}

func loadAvroCodec(ctx context.Context, cfg config.Config, logger *slog.Logger) (*schema.AvroCodec, error) {
	if cfg.SchemaRegistry == "" || cfg.SchemaSubject == "" {
		logger.Info("schema registry disabled")
		return nil, nil
	}

	registry := schema.NewRegistryClient(cfg.SchemaRegistry)
	latest, err := registry.Latest(ctx, cfg.SchemaSubject)
	if err != nil {
		return nil, fmt.Errorf("load avro schema subject=%s: %w", cfg.SchemaSubject, err)
	}

	codec, err := schema.NewAvroCodec(latest.ID, latest.Schema)
	if err != nil {
		return nil, err
	}

	logger.Info(
		"avro schema loaded",
		"subject", latest.Subject,
		"version", latest.Version,
		"id", latest.ID,
	)

	return codec, nil
}

func (a *App) Run(ctx context.Context) error {
	rebuildInterval := a.snapshotRebuildInterval()
	go a.rebuildSnapshots(ctx, rebuildInterval)
	go func() {
		if err := a.consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("consumer stopped with error", "error", err)
		}
	}()

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
	if err := a.kafkaClient.Close(); err != nil {
		a.logger.Error("kafka client close failed", "error", err)
		return fmt.Errorf("close kafka client: %w", err)
	}
	if err := a.dlqPublisher.Close(); err != nil {
		a.logger.Error("kafka dlq publisher close failed", "error", err)
		return fmt.Errorf("close kafka dlq publisher: %w", err)
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

func (a *App) rebuildSnapshots(ctx context.Context, interval time.Duration) {
	a.rebuildSnapshot()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.rebuildSnapshot()
		}
	}
}

func (a *App) rebuildSnapshot() {
	startedAt := time.Now()
	a.aggregator.RebuildSnapshot(startedAt.UTC())
	a.metrics.ObserveSnapshotRebuild(time.Since(startedAt))
}
