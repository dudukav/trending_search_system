package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr        = ":8080"
	defaultKafkaBroker     = "localhost:9092"
	defaultKafkaTopic      = "search-events"
	defaultKafkaGroupID    = "trending-search-service"
	defaultWindowSize      = 5 * time.Minute
	defaultBucketSize      = time.Second
	defaultSnapshotTopSize = 1000
	defaultMaxLimit        = 100
	defaultMaxPerIdentity  = 2
)

type Config struct {
	HTTPAddr        string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaGroupID    string
	WindowSize      time.Duration
	BucketSize      time.Duration
	SnapshotTopSize int
	MaxLimit        int
	MaxPerIdentity  int
}

func Load() Config {
	kafkaBrokers := parseStringToSlice(getEnvString("KAFKA_BROKERS", defaultKafkaBroker))

	return Config{
		HTTPAddr:        getEnvString("HTTP_ADDR", defaultHTTPAddr),
		KafkaBrokers:    kafkaBrokers,
		KafkaTopic:      getEnvString("KAFKA_TOPIC", defaultKafkaTopic),
		KafkaGroupID:    getEnvString("KAFKA_GROUP_ID", defaultKafkaGroupID),
		WindowSize:      getEnvDuration("WINDOW_SIZE", defaultWindowSize),
		BucketSize:      getEnvDuration("BUCKET_SIZE", defaultBucketSize),
		SnapshotTopSize: getEnvInt("SNAPSHOT_TOP_SIZE", defaultSnapshotTopSize),
		MaxLimit:        getEnvInt("MAX_LIMIT", defaultMaxLimit),
		MaxPerIdentity:  getEnvInt("MAX_PER_IDENTITY", defaultMaxPerIdentity),
	}
}

func getEnvString(key string, defaultValue string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}

	return v
}

func parseStringToSlice(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

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

func getEnvInt(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue
	}

	return i
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(v)
	if err == nil {
		return duration
	}

	seconds, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue
	}

	return time.Duration(seconds) * time.Second
}
