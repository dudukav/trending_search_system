package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type KafkaMessage struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int
	Offset    int64
	Time      time.Time
}

type KafkaMetadata struct {
	Topic     string
	Partition int
	Offset    int64
	Time      time.Time
}

type KafkaClient struct {
	reader *kafka.Reader
}

func NewKafkaClient(cfg KafkaConfig) (*KafkaClient, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are empty")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic is empty")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka group id is empty")
	}

	return &KafkaClient{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			Topic:          cfg.Topic,
			GroupID:        cfg.GroupID,
			CommitInterval: 0,
			StartOffset:    kafka.LastOffset,
		}),
	}, nil
}

func (c *KafkaClient) ReadMessage(ctx context.Context) (*KafkaMessage, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch kafka message: %w", err)
	}

	return &KafkaMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Time:      msg.Time,
	}, nil
}

func (c *KafkaClient) CommitMessage(ctx context.Context, msg *KafkaMessage) error {
	if err := c.reader.CommitMessages(ctx, kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}); err != nil {
		return fmt.Errorf("commit kafka message topic=%s partition=%d offset=%d: %w", msg.Topic, msg.Partition, msg.Offset, err)
	}

	return nil
}

func (c *KafkaClient) Close() error {
	return c.reader.Close()
}
