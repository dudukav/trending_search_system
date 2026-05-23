package transport

import (
	"encoding/json"
	"errors"
	"net/http"
	"search_trend/internal/aggregator"
	stoplist "search_trend/internal/stop_list"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const (
	defaultLimit    = 10
	defaultMaxLimit = 100
)

type Handler struct {
	aggregator aggregator.Aggregator
	maxLimit   int
}

func New(aggregator aggregator.Aggregator, maxLimit int) (*Handler, error) {
	if aggregator == nil {
		return nil, ErrAggregatorNil
	}
	if maxLimit <= 0 {
		maxLimit = defaultMaxLimit
	}

	return &Handler{
		aggregator: aggregator,
		maxLimit:   maxLimit,
	}, nil
}

func (h *Handler) Add(w http.ResponseWriter, r *http.Request) {
	var httpReq AddStopRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&httpReq); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if httpReq.MatchType == "" {
		httpReq.MatchType = string(stoplist.MatchPhrase)
	}

	if httpReq.MatchType != string(stoplist.MatchExact) && httpReq.MatchType != string(stoplist.MatchPhrase) {
		writeJSONError(w, http.StatusBadRequest, "invalid match type")
		return
	}

	rule, err := h.aggregator.StopList().Add(httpReq.Value, stoplist.MatchType(httpReq.MatchType))
	if err != nil {
		if errors.Is(err, stoplist.ErrEmptyStopRule) {
			writeJSONError(w, http.StatusBadRequest, "invalid stop rule")
			return
		}

		writeJSONError(w, http.StatusInternalServerError, "failed to add stop rule")
		return
	}

	h.aggregator.RebuildSnapshot(time.Now().UTC())
	writeJSON(w, http.StatusCreated, rule)
}

func (h *Handler) Top(w http.ResponseWriter, r *http.Request) {
	limit := defaultLimit

	rawLimit := r.URL.Query().Get("limit")
	if rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid limit")
			return
		}

		limit = parsedLimit
	}

	if limit <= 0 {
		writeJSONError(w, http.StatusBadRequest, "limit must be positive")
		return
	}

	if limit > h.maxLimit {
		limit = h.maxLimit
	}

	items := h.aggregator.Top(limit)

	writeJSON(w, http.StatusOK, topResponse{
		GeneratedAt: time.Now().UTC(),
		Items:       items,
	})
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	rules := h.aggregator.StopList().List()

	writeJSON(w, http.StatusOK, stopListResponse{
		Items: rules,
	})
}

func (h *Handler) Remove(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")

	id, err := uuid.Parse(rawID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid stop rule id")
		return
	}

	removed := h.aggregator.StopList().Remove(id)
	if !removed {
		writeJSONError(w, http.StatusNotFound, "stop rule not found")
		return
	}

	h.aggregator.RebuildSnapshot(time.Now().UTC())

	writeJSON(w, http.StatusNoContent, nil)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}
