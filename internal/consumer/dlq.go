package consumer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQConfig struct {
	Brokers []string
	Topic   string
}

type KafkaDLQPublisher struct {
	writer *kafka.Writer
}

type DLQMessage struct {
	OriginalEventBase64 string      `json:"original_event_base64"`
	OriginalKeyBase64   string      `json:"original_key_base64,omitempty"`
	ErrorReason         string      `json:"error_reason"`
	FailedAt            time.Time   `json:"failed_at"`
	KafkaMetadata       DLQMetadata `json:"kafka_metadata"`
}

type DLQMetadata struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Time      time.Time `json:"time"`
}

func NewKafkaDLQPublisher(cfg DLQConfig) (*KafkaDLQPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka dlq brokers are empty")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka dlq topic is empty")
	}

	return &KafkaDLQPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.Topic,
			Balancer: &kafka.LeastBytes{},
		},
	}, nil
}

func (p *KafkaDLQPublisher) Publish(ctx context.Context, msg *KafkaMessage, cause error) error {
	payload, err := json.Marshal(newDLQMessage(msg, cause))
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: payload,
		Time:  time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("publish dlq message: %w", err)
	}

	return nil
}

func (p *KafkaDLQPublisher) Close() error {
	return p.writer.Close()
}

func newDLQMessage(msg *KafkaMessage, cause error) DLQMessage {
	dlqMessage := DLQMessage{
		OriginalEventBase64: base64.StdEncoding.EncodeToString(msg.Value),
		ErrorReason:         cause.Error(),
		FailedAt:            time.Now().UTC(),
		KafkaMetadata: DLQMetadata{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Time:      msg.Time,
		},
	}

	if len(msg.Key) > 0 {
		dlqMessage.OriginalKeyBase64 = base64.StdEncoding.EncodeToString(msg.Key)
	}

	return dlqMessage
}
