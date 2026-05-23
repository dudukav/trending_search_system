package consumer

import (
	"encoding/json"
	"errors"
	"os"
	"search_trend/internal/model"
	"search_trend/internal/schema"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJSONDecoderDecodeValidEvent(t *testing.T) {
	decoder := NewJSONDecoder()
	userID := uuid.New()
	event := model.SearchEvent{
		EventID:   uuid.New(),
		Query:     "iphone",
		UserID:    &userID,
		Timestamp: time.Now().UTC(),
		Source:    "WEB",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	got, err := decoder.Decode(payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.EventID != event.EventID {
		t.Fatalf("Decode() event id = %s, want %s", got.EventID, event.EventID)
	}
}

func TestEventDecoderDecodeAvroEvent(t *testing.T) {
	schemaText, err := os.ReadFile("../../schema/markerplace_search.avsc")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	codec, err := schema.NewAvroCodec(1, string(schemaText))
	if err != nil {
		t.Fatalf("NewAvroCodec() error = %v", err)
	}

	userID := uuid.New()
	event := model.SearchEvent{
		EventID:   uuid.New(),
		Query:     "iphone",
		UserID:    &userID,
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		Source:    "WEB",
	}
	payload, err := codec.Encode(event)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := NewDecoder(codec).Decode(payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.EventID != event.EventID {
		t.Fatalf("EventID = %s, want %s", got.EventID, event.EventID)
	}
}

func TestJSONDecoderDecodeInvalidJSON(t *testing.T) {
	decoder := NewJSONDecoder()

	if _, err := decoder.Decode([]byte("{")); err == nil {
		t.Fatal("Decode() error = nil, want error")
	}
}

func TestJSONDecoderDecodeValidationError(t *testing.T) {
	decoder := NewJSONDecoder()
	event := model.SearchEvent{
		EventID:   uuid.New(),
		Query:     "",
		IPHash:    "ip-1",
		Timestamp: time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	_, err = decoder.Decode(payload)
	if !errors.Is(err, model.ErrQueryRequired) {
		t.Fatalf("Decode() error = %v, want %v", err, model.ErrQueryRequired)
	}
}
