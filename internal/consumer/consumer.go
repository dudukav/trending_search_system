package consumer

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"search_trend/internal/metrics"
	"time"

	"search_trend/internal/model"
)

type MessageReader interface {
	ReadMessage(ctx context.Context) (*KafkaMessage, error)
}

type OffsetCommitter interface {
	CommitMessage(ctx context.Context, msg *KafkaMessage) error
}

type EventDecoder interface {
	Decode(data []byte) (model.SearchEvent, error)
}

type EventHandler interface {
	Handle(ctx context.Context, event model.SearchEvent, meta KafkaMetadata) error
}

type DLQPublisher interface {
	Publish(ctx context.Context, msg *KafkaMessage, cause error) error
}

type Consumer struct {
	reader    MessageReader
	committer OffsetCommitter
	decoder   EventDecoder
	handler   EventHandler
	dlq       DLQPublisher
	logger    *slog.Logger
	metrics   *metrics.Metrics
}

func New(
	reader MessageReader,
	committer OffsetCommitter,
	decoder EventDecoder,
	handler EventHandler,
	dlq DLQPublisher,
	appMetrics *metrics.Metrics,
	logger *slog.Logger,
) *Consumer {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	return &Consumer{
		reader:    reader,
		committer: committer,
		decoder:   decoder,
		handler:   handler,
		dlq:       dlq,
		logger:    logger,
		metrics:   appMetrics,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("consumer stopped", "reason", ctx.Err())
			return ctx.Err()
		default:
		}

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("consumer stopped", "reason", err)
				return err
			}

			c.logger.Error("failed to read kafka message", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Error(
				"failed to process kafka message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *KafkaMessage) error {
	startedAt := time.Now()

	event, err := c.decoder.Decode(msg.Value)
	if err != nil {
		c.logger.Warn(
			"invalid kafka message skipped",
			"error", err,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
		if c.metrics != nil {
			c.metrics.ObserveKafkaMessage("invalid", time.Since(startedAt))
		}
		return c.publishToDLQAndCommit(ctx, msg, err)
	}

	meta := KafkaMetadata{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Time:      msg.Time,
	}

	if err := c.handler.Handle(ctx, event, meta); err != nil {
		if c.metrics != nil {
			c.metrics.ObserveKafkaMessage("handler_error", time.Since(startedAt))
		}
		return err
	}

	if err := c.committer.CommitMessage(ctx, msg); err != nil {
		if c.metrics != nil {
			c.metrics.ObserveKafkaMessage("commit_error", time.Since(startedAt))
		}
		return err
	}

	if c.metrics != nil {
		c.metrics.ObserveKafkaMessage("processed", time.Since(startedAt))
	}

	c.logger.Info(
		"kafka event processed",
		"event_id", event.EventID.String(),
		"query", event.Query,
		"partition", meta.Partition,
		"offset", meta.Offset,
	)

	return nil
}

func (c *Consumer) publishToDLQAndCommit(ctx context.Context, msg *KafkaMessage, cause error) error {
	if c.dlq != nil {
		if err := c.dlq.Publish(ctx, msg, cause); err != nil {
			if c.metrics != nil {
				c.metrics.ObserveDLQ("publish_error")
			}
			return err
		}
	}

	if c.metrics != nil {
		c.metrics.ObserveDLQ("published")
	}

	return c.committer.CommitMessage(ctx, msg)
}
