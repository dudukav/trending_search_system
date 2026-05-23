package aggregator

import (
	"fmt"
	"search_trend/internal/model"
	"sync"
	"time"
)

type AntiFraud struct {
	mu             sync.Mutex
	maxPerIdentity int
	windowSize     time.Duration
	hits           map[string][]time.Time
}

func NewAntifraud(
	maxPerIdentity int,
	windowSize time.Duration) *AntiFraud {
	if maxPerIdentity <= 0 {
		maxPerIdentity = defaultMaxPerIdentity
	}
	if windowSize <= 0 {
		windowSize = defaultWindowSize
	}

	return &AntiFraud{
		mu:             sync.Mutex{},
		maxPerIdentity: maxPerIdentity,
		windowSize:     windowSize,
		hits:           make(map[string][]time.Time),
	}
}

func (a *AntiFraud) Allow(event model.SearchEvent, query string) bool {
	query = model.Normalize(query)
	if query == "" {
		return false
	}

	identity := eventIdentity(event)
	if identity == "" {
		return true
	}

	eventTime := event.Timestamp
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}

	cutoff := eventTime.Add(-a.windowSize)
	key := fmt.Sprintf("%s:%s", query, identity)

	a.mu.Lock()
	defer a.mu.Unlock()

	times := a.hits[key]
	kept := times[:0]
	for _, hitTime := range times {
		if !hitTime.Before(cutoff) {
			kept = append(kept, hitTime)
		}
	}

	if len(kept) >= a.maxPerIdentity {
		a.hits[key] = kept
		return false
	}

	kept = append(kept, eventTime)
	a.hits[key] = kept

	return true
}

func eventIdentity(event model.SearchEvent) string {
	if event.UserID != nil {
		return "user:" + event.UserID.String()
	}

	if event.IPHash != "" {
		return "ip:" + event.IPHash
	}

	return ""
}
