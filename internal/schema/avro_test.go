package schema

import (
	"os"
	"search_trend/internal/model"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAvroCodecEncodeDecode(t *testing.T) {
	schemaText, err := os.ReadFile("../../schema/markerplace_search.avsc")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	codec, err := NewAvroCodec(7, string(schemaText))
	if err != nil {
		t.Fatalf("NewAvroCodec() error = %v", err)
	}

	userID := uuid.New()
	event := model.SearchEvent{
		EventID:   uuid.New(),
		Query:     "iphone 15",
		UserID:    &userID,
		IPHash:    "ip-1",
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		Source:    "WEB",
	}

	payload, err := codec.Encode(event)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if !IsConfluentAvro(payload) {
		t.Fatal("IsConfluentAvro() = false, want true")
	}

	got, err := codec.Decode(payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.EventID != event.EventID {
		t.Fatalf("EventID = %s, want %s", got.EventID, event.EventID)
	}
	if got.Query != event.Query {
		t.Fatalf("Query = %q, want %q", got.Query, event.Query)
	}
	if got.UserID == nil || *got.UserID != userID {
		t.Fatalf("UserID = %v, want %s", got.UserID, userID)
	}
	if got.IPHash != event.IPHash {
		t.Fatalf("IPHash = %q, want %q", got.IPHash, event.IPHash)
	}
	if !got.Timestamp.Equal(event.Timestamp) {
		t.Fatalf("Timestamp = %s, want %s", got.Timestamp, event.Timestamp)
	}
	if got.Source != event.Source {
		t.Fatalf("Source = %q, want %q", got.Source, event.Source)
	}
}
