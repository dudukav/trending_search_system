package aggregator

import (
	"search_trend/internal/model"
	"search_trend/internal/stop_list"
	"time"
)

type Aggregator interface {
	Add(event model.SearchEvent)
	Top(limit int) []TopItem
	RebuildSnapshot(now time.Time)
	StopList() *stoplist.StopList
}
