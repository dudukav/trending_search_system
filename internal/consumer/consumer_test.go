package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"search_trend/internal/model"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConsumerProcessMessagePublishesInvalidMessageToDLQAndCommits(t *testing.T) {
	committer := &fakeCommitter{}
	dlq := &fakeDLQ{}
	consumer := New(
		nil,
		committer,
		NewJSONDecoder(),
		&fakeHandler{},
		dlq,
		nil,
		testLogger(),
	)
	msg := &KafkaMessage{
		Value:     []byte("{"),
		Topic:     "search-events",
		Partition: 1,
		Offset:    10,
	}

	if err := consumer.processMessage(context.Background(), msg); err != nil {
		t.Fatalf("processMessage() error = %v", err)
	}

	if dlq.published != 1 {
		t.Fatalf("DLQ published = %d, want 1", dlq.published)
	}
	if committer.committed != 1 {
		t.Fatalf("committed = %d, want 1", committer.committed)
	}
}

func TestConsumerProcessMessageHandlesValidEventAndCommits(t *testing.T) {
	committer := &fakeCommitter{}
	handler := &fakeHandler{}
	consumer := New(
		nil,
		committer,
		NewJSONDecoder(),
		handler,
		&fakeDLQ{},
		nil,
		testLogger(),
	)
	payload := mustMarshalSearchEvent(t)
	msg := &KafkaMessage{
		Value:     payload,
		Topic:     "search-events",
		Partition: 1,
		Offset:    10,
	}

	if err := consumer.processMessage(context.Background(), msg); err != nil {
		t.Fatalf("processMessage() error = %v", err)
	}

	if handler.handled != 1 {
		t.Fatalf("handled = %d, want 1", handler.handled)
	}
	if committer.committed != 1 {
		t.Fatalf("committed = %d, want 1", committer.committed)
	}
}

func TestConsumerProcessMessageDoesNotCommitHandlerError(t *testing.T) {
	committer := &fakeCommitter{}
	handlerErr := errors.New("handler failed")
	consumer := New(
		nil,
		committer,
		NewJSONDecoder(),
		&fakeHandler{err: handlerErr},
		&fakeDLQ{},
		nil,
		testLogger(),
	)
	msg := &KafkaMessage{
		Value: mustMarshalSearchEvent(t),
	}

	err := consumer.processMessage(context.Background(), msg)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("processMessage() error = %v, want %v", err, handlerErr)
	}
	if committer.committed != 0 {
		t.Fatalf("committed = %d, want 0", committer.committed)
	}
}

func mustMarshalSearchEvent(t *testing.T) []byte {
	t.Helper()

	userID := uuid.New()
	payload, err := json.Marshal(model.SearchEvent{
		EventID:   uuid.New(),
		Query:     "iphone",
		UserID:    &userID,
		Timestamp: time.Now().UTC(),
		Source:    "WEB",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	return payload
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeCommitter struct {
	committed int
}

func (f *fakeCommitter) CommitMessage(ctx context.Context, msg *KafkaMessage) error {
	f.committed++
	return nil
}

type fakeDLQ struct {
	published int
	err       error
}

func (f *fakeDLQ) Publish(ctx context.Context, msg *KafkaMessage, cause error) error {
	if f.err != nil {
		return f.err
	}

	f.published++
	return nil
}

type fakeHandler struct {
	handled int
	err     error
}

func (f *fakeHandler) Handle(ctx context.Context, event model.SearchEvent, meta KafkaMetadata) error {
	if f.err != nil {
		return f.err
	}

	f.handled++
	return nil
}
