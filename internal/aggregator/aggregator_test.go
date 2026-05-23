package aggregator

import (
	"search_trend/internal/model"
	stoplist "search_trend/internal/stop_list"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAggregatorTopReturnsEmptySliceByDefault(t *testing.T) {
	agg := NewAggregator(5*time.Minute, time.Second, 10, 3)

	got := agg.Top(10)
	if got == nil {
		t.Fatal("Top() = nil, want empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("Top() length = %d, want 0", len(got))
	}
}

func TestAggregatorBuildsSortedTop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := NewAggregator(5*time.Minute, time.Second, 10, 10)

	agg.Add(searchEvent("iphone", now, nil, "ip-1"))
	agg.Add(searchEvent("lego", now.Add(time.Second), nil, "ip-2"))
	agg.Add(searchEvent("iphone", now.Add(2*time.Second), nil, "ip-3"))
	agg.Add(searchEvent("samsung", now.Add(3*time.Second), nil, "ip-4"))
	agg.RebuildSnapshot(now.Add(4 * time.Second))

	got := agg.Top(10)
	want := []TopItem{
		{Query: "iphone", Count: 2, Rank: 1},
		{Query: "lego", Count: 1, Rank: 2},
		{Query: "samsung", Count: 1, Rank: 3},
	}

	assertTopItems(t, got, want)
}

func TestAggregatorTopAppliesLimit(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := NewAggregator(5*time.Minute, time.Second, 10, 10)

	agg.Add(searchEvent("iphone", now, nil, "ip-1"))
	agg.Add(searchEvent("lego", now, nil, "ip-2"))
	agg.RebuildSnapshot(now)

	got := agg.Top(1)
	if len(got) != 1 {
		t.Fatalf("Top(1) length = %d, want 1", len(got))
	}

	got = agg.Top(0)
	if len(got) != 0 {
		t.Fatalf("Top(0) length = %d, want 0", len(got))
	}

	got = agg.Top(-1)
	if len(got) != 0 {
		t.Fatalf("Top(-1) length = %d, want 0", len(got))
	}
}

func TestAggregatorIgnoresEventsOutsideWindow(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := NewAggregator(5*time.Minute, time.Second, 10, 10)

	agg.Add(searchEvent("old query", now.Add(-6*time.Minute), nil, "ip-1"))
	agg.Add(searchEvent("fresh query", now.Add(-time.Minute), nil, "ip-2"))
	agg.RebuildSnapshot(now)

	got := agg.Top(10)
	want := []TopItem{
		{Query: "fresh query", Count: 1, Rank: 1},
	}

	assertTopItems(t, got, want)
}

func TestAggregatorFiltersStopListOnAdd(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	list := stoplist.NewStoplist()
	if _, err := list.Add("казино", stoplist.MatchPhrase); err != nil {
		t.Fatalf("Add stop rule error = %v", err)
	}
	agg := NewAggregatorWithStopList(5*time.Minute, time.Second, 10, 10, list)

	agg.Add(searchEvent("онлайн казино", now, nil, "ip-1"))
	agg.Add(searchEvent("iphone", now, nil, "ip-2"))
	agg.RebuildSnapshot(now)

	got := agg.Top(10)
	want := []TopItem{
		{Query: "iphone", Count: 1, Rank: 1},
	}

	assertTopItems(t, got, want)
}

func TestAggregatorFiltersStopListOnRebuild(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	list := stoplist.NewStoplist()
	agg := NewAggregatorWithStopList(5*time.Minute, time.Second, 10, 10, list)

	agg.Add(searchEvent("iphone", now, nil, "ip-1"))
	agg.Add(searchEvent("lego", now, nil, "ip-2"))
	if _, err := list.Add("iphone", stoplist.MatchExact); err != nil {
		t.Fatalf("Add stop rule error = %v", err)
	}
	agg.RebuildSnapshot(now)

	got := agg.Top(10)
	want := []TopItem{
		{Query: "lego", Count: 1, Rank: 1},
	}

	assertTopItems(t, got, want)
}

func TestAggregatorLimitsSameIdentity(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	userID := uuid.New()
	agg := NewAggregator(5*time.Minute, time.Second, 10, 2)

	agg.Add(searchEvent("iphone", now, &userID, ""))
	agg.Add(searchEvent("iphone", now.Add(time.Second), &userID, ""))
	agg.Add(searchEvent("iphone", now.Add(2*time.Second), &userID, ""))
	agg.Add(searchEvent("iphone", now.Add(3*time.Second), nil, "ip-2"))
	agg.RebuildSnapshot(now.Add(4 * time.Second))

	got := agg.Top(10)
	want := []TopItem{
		{Query: "iphone", Count: 3, Rank: 1},
	}

	assertTopItems(t, got, want)
}

func assertTopItems(t *testing.T, got []TopItem, want []TopItem) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("Top() length = %d, want %d; got %+v", len(got), len(want), got)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Top()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func searchEvent(query string, timestamp time.Time, userID *uuid.UUID, ipHash string) model.SearchEvent {
	return model.SearchEvent{
		EventID:   uuid.New(),
		Query:     query,
		UserID:    userID,
		IPHash:    ipHash,
		Timestamp: timestamp,
		Source:    "WEB",
	}
}
