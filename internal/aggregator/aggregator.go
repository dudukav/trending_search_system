package aggregator

import (
	"search_trend/internal/model"
	"time"
)

type Aggregator interface {
	Add(event model.SearchEvent)
	Top(limit int) []TopItem
	RebuildSnapshot(now time.Time)
}