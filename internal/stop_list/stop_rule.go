package stoplist

import (
	"time"

	"github.com/google/uuid"
)

type StopRule struct {
	ID        uuid.UUID `json:"id"`
	Value     string    `json:"value"`
	MatchType MatchType `json:"match_type"`
	CreatedAt time.Time `json:"created_at"`
}

type MatchType string

const (
	MatchExact  MatchType = "exact"
	MatchPhrase MatchType = "phrase"
)
