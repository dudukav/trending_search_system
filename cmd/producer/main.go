package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"search_trend/internal/model"
	"search_trend/internal/schema"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

var queries = []string{
	"iphone 15",
	"iphone 15",
	"iphone 15",
	"lego",
	"lego",
	"samsung",
	"nike sneakers",
	"airpods",
	"coffee machine",
	"gaming laptop",
	"winter jacket",
	"baby stroller",
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	var (
		brokersRaw = flag.String("brokers", envString("KAFKA_BROKERS", "localhost:9092"), "comma-separated kafka brokers")
		topic      = flag.String("topic", envString("KAFKA_TOPIC", "search-events"), "kafka topic")
		registry   = flag.String("schema-registry", envString("SCHEMA_REGISTRY_URL", ""), "schema registry URL; empty means JSON payload")
		subject    = flag.String("schema-subject", envString("SCHEMA_SUBJECT", "search-events-value"), "schema registry subject")
		rate       = flag.Int("rate", envInt("PRODUCER_RATE", 10), "events per second")
		count      = flag.Int("count", envInt("PRODUCER_COUNT", 0), "events to produce; 0 means forever")
	)
	flag.Parse()

	brokers := splitCSV(*brokersRaw)
	if len(brokers) == 0 {
		logger.Error("kafka brokers are empty")
		os.Exit(1)
	}
	if *topic == "" {
		logger.Error("kafka topic is empty")
		os.Exit(1)
	}
	if *rate <= 0 {
		*rate = 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer func() {
		_ = writer.Close()
	}()

	encoder, err := newPayloadEncoder(ctx, *registry, *subject, logger)
	if err != nil {
		logger.Error("create payload encoder failed", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"producer started",
		"brokers", brokers,
		"topic", *topic,
		"rate", *rate,
		"count", *count,
	)

	if err := produce(ctx, writer, encoder, *rate, *count, logger); err != nil && ctx.Err() == nil {
		logger.Error("producer failed", "error", err)
		os.Exit(1)
	}

	logger.Info("producer stopped")
}

type payloadEncoder interface {
	Encode(event model.SearchEvent) ([]byte, error)
}

type jsonPayloadEncoder struct{}

func (e jsonPayloadEncoder) Encode(event model.SearchEvent) ([]byte, error) {
	return json.Marshal(event)
}

func newPayloadEncoder(ctx context.Context, registryURL string, subject string, logger *slog.Logger) (payloadEncoder, error) {
	if registryURL == "" {
		logger.Info("producer using json payloads")
		return jsonPayloadEncoder{}, nil
	}

	registry := schema.NewRegistryClient(registryURL)
	latest, err := registry.Latest(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("load schema subject=%s: %w", subject, err)
	}

	codec, err := schema.NewAvroCodec(latest.ID, latest.Schema)
	if err != nil {
		return nil, err
	}

	logger.Info("producer using avro payloads", "schema_id", latest.ID, "subject", subject)
	return codec, nil
}

func produce(ctx context.Context, writer *kafka.Writer, encoder payloadEncoder, rate int, count int, logger *slog.Logger) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for produced := 0; count == 0 || produced < count; produced++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		event := randomEvent(rng)
		payload, err := encoder.Encode(event)
		if err != nil {
			return err
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.EventID.String()),
			Value: payload,
			Time:  event.Timestamp,
		}); err != nil {
			return err
		}

		if produced == 0 || (produced+1)%rate == 0 {
			logger.Info("events produced", "count", produced+1)
		}
	}

	return nil
}

func randomEvent(rng *rand.Rand) model.SearchEvent {
	userID := uuid.New()

	return model.SearchEvent{
		EventID:   uuid.New(),
		Query:     queries[rng.Intn(len(queries))],
		UserID:    &userID,
		IPHash:    "ip-" + strconv.Itoa(rng.Intn(1000)),
		Timestamp: time.Now().UTC(),
		Source:    "WEB",
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			result = append(result, item)
		}
	}

	return result
}

func envString(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

func envInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return parsed
}
