package transport

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
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
	logger     *slog.Logger
	maxLimit   int
}

func New(aggregator aggregator.Aggregator, maxLimit int, logger *slog.Logger) (*Handler, error) {
	if aggregator == nil {
		return nil, ErrAggregatorNil
	}
	if maxLimit <= 0 {
		maxLimit = defaultMaxLimit
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	return &Handler{
		aggregator: aggregator,
		logger:     logger,
		maxLimit:   maxLimit,
	}, nil
}

func (h *Handler) Add(w http.ResponseWriter, r *http.Request) {
	var httpReq AddStopRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&httpReq); err != nil {
		h.logger.Warn("invalid add stop rule request body", "error", err)
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if httpReq.MatchType == "" {
		httpReq.MatchType = string(stoplist.MatchPhrase)
	}

	if httpReq.MatchType != string(stoplist.MatchExact) && httpReq.MatchType != string(stoplist.MatchPhrase) {
		h.logger.Warn("invalid stop rule match type", "match_type", httpReq.MatchType)
		writeJSONError(w, http.StatusBadRequest, "invalid match type")
		return
	}

	rule, err := h.aggregator.StopList().Add(httpReq.Value, stoplist.MatchType(httpReq.MatchType))
	if err != nil {
		if errors.Is(err, stoplist.ErrEmptyStopRule) {
			h.logger.Warn("empty stop rule rejected")
			writeJSONError(w, http.StatusBadRequest, "invalid stop rule")
			return
		}

		h.logger.Error("add stop rule failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to add stop rule")
		return
	}

	h.aggregator.RebuildSnapshot(time.Now().UTC())
	h.logger.Info(
		"stop rule added",
		"id", rule.ID.String(),
		"value", rule.Value,
		"match_type", rule.MatchType,
	)
	writeJSON(w, http.StatusCreated, rule)
}

func (h *Handler) Top(w http.ResponseWriter, r *http.Request) {
	limit := defaultLimit

	rawLimit := r.URL.Query().Get("limit")
	if rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil {
			h.logger.Warn("invalid trends limit", "limit", rawLimit)
			writeJSONError(w, http.StatusBadRequest, "invalid limit")
			return
		}

		limit = parsedLimit
	}

	if limit <= 0 {
		h.logger.Warn("non-positive trends limit", "limit", limit)
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
		h.logger.Warn("invalid stop rule id", "id", rawID)
		writeJSONError(w, http.StatusBadRequest, "invalid stop rule id")
		return
	}

	removed := h.aggregator.StopList().Remove(id)
	if !removed {
		h.logger.Warn("stop rule not found", "id", id.String())
		writeJSONError(w, http.StatusNotFound, "stop rule not found")
		return
	}

	h.aggregator.RebuildSnapshot(time.Now().UTC())
	h.logger.Info("stop rule removed", "id", id.String())

	writeJSON(w, http.StatusNoContent, nil)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}
