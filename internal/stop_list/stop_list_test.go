package stoplist

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestStopListAddNormalizesRule(t *testing.T) {
	stopList := NewStoplist()

	rule, err := stopList.Add("  IPHONE   15  ", MatchExact)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if rule.ID == uuid.Nil {
		t.Fatal("Add() generated nil rule ID")
	}

	if rule.Value != "iphone 15" {
		t.Fatalf("Add() rule value = %q, want %q", rule.Value, "iphone 15")
	}

	if rule.MatchType != MatchExact {
		t.Fatalf("Add() match type = %q, want %q", rule.MatchType, MatchExact)
	}

	if rule.CreatedAt.IsZero() {
		t.Fatal("Add() CreatedAt is zero")
	}
}

func TestStopListAddRejectsEmptyRule(t *testing.T) {
	stopList := NewStoplist()

	_, err := stopList.Add(" \t\n ", MatchExact)
	if !errors.Is(err, ErrEmptyStopRule) {
		t.Fatalf("Add() error = %v, want %v", err, ErrEmptyStopRule)
	}
}

func TestStopListContainsExactMatch(t *testing.T) {
	stopList := NewStoplist()

	if _, err := stopList.Add("iphone 15", MatchExact); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "same normalized query",
			query: "iphone 15",
			want:  true,
		},
		{
			name:  "normalizes query before matching",
			query: "  IPHONE   15 ",
			want:  true,
		},
		{
			name:  "does not match query containing extra tokens",
			query: "купить iphone 15",
			want:  false,
		},
		{
			name:  "empty query",
			query: " ",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stopList.Contains(tt.query); got != tt.want {
				t.Fatalf("Contains(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestStopListContainsPhraseMatch(t *testing.T) {
	stopList := NewStoplist()

	if _, err := stopList.Add("iphone 15", MatchPhrase); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "matches exact phrase",
			query: "iphone 15",
			want:  true,
		},
		{
			name:  "matches phrase inside query",
			query: "купить iphone 15 дешево",
			want:  true,
		},
		{
			name:  "does not match different token",
			query: "купить iphone 14 дешево",
			want:  false,
		},
		{
			name:  "does not match joined token",
			query: "купить iphone15 дешево",
			want:  false,
		},
		{
			name:  "does not match wrong token order",
			query: "15 iphone",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stopList.Contains(tt.query); got != tt.want {
				t.Fatalf("Contains(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestStopListRemove(t *testing.T) {
	stopList := NewStoplist()

	rule, err := stopList.Add("казино", MatchPhrase)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if !stopList.Contains("онлайн казино") {
		t.Fatal("Contains() = false before removing rule, want true")
	}

	if removed := stopList.Remove(rule.ID); !removed {
		t.Fatal("Remove() = false, want true")
	}

	if stopList.Contains("онлайн казино") {
		t.Fatal("Contains() = true after removing rule, want false")
	}

	if removed := stopList.Remove(rule.ID); removed {
		t.Fatal("Remove() existing removed rule = true, want false")
	}
}

func TestStopListList(t *testing.T) {
	stopList := NewStoplist()

	first, err := stopList.Add("iphone", MatchExact)
	if err != nil {
		t.Fatalf("Add(first) error = %v", err)
	}

	second, err := stopList.Add("казино онлайн", MatchPhrase)
	if err != nil {
		t.Fatalf("Add(second) error = %v", err)
	}

	got := stopList.List()
	if len(got) != 2 {
		t.Fatalf("List() length = %d, want 2", len(got))
	}

	byID := make(map[uuid.UUID]StopRule, len(got))
	for _, rule := range got {
		byID[rule.ID] = rule
	}

	if byID[first.ID] != first {
		t.Fatalf("List() first rule = %+v, want %+v", byID[first.ID], first)
	}

	if byID[second.ID] != second {
		t.Fatalf("List() second rule = %+v, want %+v", byID[second.ID], second)
	}
}
