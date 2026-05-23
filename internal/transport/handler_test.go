package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"search_trend/internal/aggregator"
	"search_trend/internal/model"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHandlerTop(t *testing.T) {
	now := time.Now().UTC()
	agg := aggregator.NewAggregator(5*time.Minute, time.Second, 100, 10)
	agg.Add(searchEvent("iphone", now))
	agg.RebuildSnapshot(now)
	handler := newTestHandler(t, agg)

	req := httptest.NewRequest(http.MethodGet, "/v1/trends?limit=10", nil)
	rec := httptest.NewRecorder()

	handler.Top(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var response topResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(response.Items) != 1 || response.Items[0].Query != "iphone" {
		t.Fatalf("items = %+v, want iphone", response.Items)
	}
}

func TestHandlerTopInvalidLimit(t *testing.T) {
	handler := newTestHandler(t, aggregator.NewAggregator(5*time.Minute, time.Second, 100, 10))
	req := httptest.NewRequest(http.MethodGet, "/v1/trends?limit=bad", nil)
	rec := httptest.NewRecorder()

	handler.Top(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlerAddListRemoveStopRule(t *testing.T) {
	handler := newTestHandler(t, aggregator.NewAggregator(5*time.Minute, time.Second, 100, 10))
	body := bytes.NewBufferString(`{"value":"казино","match_type":"phrase"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/stoplist", body)
	rec := httptest.NewRecorder()

	handler.Add(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("add status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var created struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if created.ID == uuid.Nil {
		t.Fatal("created id is nil")
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/stoplist", nil)
	listRec := httptest.NewRecorder()
	handler.List(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
	}

	removeReq := httptest.NewRequest(http.MethodDelete, "/v1/stoplist/"+created.ID.String(), nil)
	removeReq.SetPathValue("id", created.ID.String())
	removeRec := httptest.NewRecorder()
	handler.Remove(removeRec, removeReq)
	if removeRec.Code != http.StatusNoContent {
		t.Fatalf("remove status = %d, want %d; body=%s", removeRec.Code, http.StatusNoContent, removeRec.Body.String())
	}
}

func newTestHandler(t *testing.T, agg aggregator.Aggregator) *Handler {
	t.Helper()

	handler, err := New(agg, 100, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return handler
}

func searchEvent(query string, timestamp time.Time) model.SearchEvent {
	userID := uuid.New()
	return model.SearchEvent{
		EventID:   uuid.New(),
		Query:     query,
		UserID:    &userID,
		Timestamp: timestamp,
		Source:    "WEB",
	}
}
