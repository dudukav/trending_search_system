package consumer

import (
	"context"
	"log/slog"
	"os"

	"search_trend/internal/aggregator"
	"search_trend/internal/model"
)

type Handler struct {
	aggregator aggregator.Aggregator
	logger     *slog.Logger
}

func NewHandler(aggregator aggregator.Aggregator, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	return &Handler{
		aggregator: aggregator,
		logger:     logger,
	}
}

func (h *Handler) Handle(ctx context.Context, event model.SearchEvent, meta KafkaMetadata) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	h.aggregator.Add(event)
	return nil
}
