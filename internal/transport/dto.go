package transport

import (
	"search_trend/internal/aggregator"
	stoplist "search_trend/internal/stop_list"
	"time"
)

type AddStopRuleRequest struct {
	Value     string `json:"value"`
	MatchType string `json:"match_type"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type TrendsResponse struct {
	Items []aggregator.TopItem `json:"items"`
}

type topResponse struct {
	GeneratedAt time.Time            `json:"generated_at"`
	Items       []aggregator.TopItem `json:"items"`
}

type stopListResponse struct {
	Items []stoplist.StopRule `json:"items"`
}
