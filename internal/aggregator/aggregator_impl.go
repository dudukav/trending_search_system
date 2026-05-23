package aggregator

import (
	"search_trend/internal/model"
	"search_trend/internal/stop_list"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type aggregator struct {
	mu         sync.RWMutex
	windowSize time.Duration
	bucketSize time.Duration
	buckets    []Bucket
	stopList   *stoplist.StopList
	antiFraud  *AntiFraud
	snapshot   atomic.Value
	topSize    int
}

type Bucket struct {
	Start  time.Time
	Counts map[string]int64
}

func NewAggregator(
	windowSize time.Duration,
	bucketSize time.Duration,
	topSize int,
	maxPerIdentity int,
) Aggregator {
	stopList := stoplist.NewStoplist()
	return NewAggregatorWithStopList(windowSize, bucketSize, topSize, maxPerIdentity, stopList)
}

func NewAggregatorWithStopList(
	windowSize time.Duration,
	bucketSize time.Duration,
	topSize int,
	maxPerIdentity int,
	stopList *stoplist.StopList,
) Aggregator {
	if windowSize <= 0 {
		windowSize = defaultWindowSize
	}
	if bucketSize <= 0 {
		bucketSize = time.Second
	}
	if topSize <= 0 {
		topSize = defaultTopSize
	}
	if stopList == nil {
		stopList = stoplist.NewStoplist()
	}

	antifraud := NewAntifraud(maxPerIdentity, windowSize)
	agg := &aggregator{
		mu:         sync.RWMutex{},
		windowSize: windowSize,
		bucketSize: bucketSize,
		buckets:    make([]Bucket, 0),
		stopList:   stopList,
		antiFraud:  antifraud,
		topSize:    topSize,
	}
	agg.snapshot.Store([]TopItem{})

	return agg
}

func (a *aggregator) StopList() *stoplist.StopList {
	return a.stopList
}

func (a *aggregator) Add(event model.SearchEvent) {
	query := model.Normalize(event.Query)
	if query == "" {
		return
	}

	if a.stopList.Contains(query) {
		return
	}

	eventTime := event.Timestamp
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
		event.Timestamp = eventTime
	}

	if !a.antiFraud.Allow(event, query) {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	bucket := a.bucketFor(eventTime)
	bucket.Counts[query]++
}

func (a *aggregator) Top(limit int) []TopItem {
	if limit <= 0 {
		return []TopItem{}
	}

	value := a.snapshot.Load()
	if value == nil {
		return []TopItem{}
	}

	items := value.([]TopItem)
	if limit > len(items) {
		limit = len(items)
	}

	result := make([]TopItem, limit)
	copy(result, items[:limit])

	return result
}

func (a *aggregator) RebuildSnapshot(now time.Time) {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	counts := make(map[string]int64)
	cutoff := now.Add(-a.windowSize)

	a.mu.Lock()
	a.cleanupLocked(cutoff)
	for _, bucket := range a.buckets {
		if bucket.Start.Before(cutoff) || bucket.Start.After(now) {
			continue
		}

		for query, count := range bucket.Counts {
			if count <= 0 || a.stopList.Contains(query) {
				continue
			}

			counts[query] += count
		}
	}
	a.mu.Unlock()

	items := make([]TopItem, 0, len(counts))
	for query, count := range counts {
		items = append(items, TopItem{
			Query: query,
			Count: count,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Query < items[j].Query
		}

		return items[i].Count > items[j].Count
	})

	if len(items) > a.topSize {
		items = items[:a.topSize]
	}

	for i := range items {
		items[i].Rank = i + 1
	}

	a.snapshot.Store(items)
}

func (a *aggregator) bucketFor(eventTime time.Time) *Bucket {
	start := eventTime.Truncate(a.bucketSize)
	for i := range a.buckets {
		if a.buckets[i].Start.Equal(start) {
			return &a.buckets[i]
		}
	}

	a.buckets = append(a.buckets, Bucket{
		Start:  start,
		Counts: make(map[string]int64),
	})

	return &a.buckets[len(a.buckets)-1]
}

func (a *aggregator) cleanupLocked(cutoff time.Time) {
	kept := a.buckets[:0]
	for _, bucket := range a.buckets {
		if !bucket.Start.Before(cutoff) {
			kept = append(kept, bucket)
		}
	}

	a.buckets = kept
}
