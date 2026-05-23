package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrEventIDRequired  = errors.New("event_id is required")
	ErrQueryRequired    = errors.New("query is required")
	ErrTimestampMissing = errors.New("timestamp is required")
	ErrTimestampTooOld  = errors.New("timestamp is too old")
	ErrTimestampFuture  = errors.New("timestamp is too far in future")
	ErrIdentityRequired = errors.New("user_id or ip_hash is required")
)

func (e SearchEvent) Validate(now time.Time, maxAge time.Duration, maxFutureSkew time.Duration) error {
	if e.EventID == uuid.Nil {
		return ErrEventIDRequired
	}

	if Normalize(e.Query) == "" {
		return ErrQueryRequired
	}

	if e.Timestamp.IsZero() {
		return ErrTimestampMissing
	}

	if maxAge > 0 && e.Timestamp.Before(now.Add(-maxAge)) {
		return ErrTimestampTooOld
	}

	if maxFutureSkew > 0 && e.Timestamp.After(now.Add(maxFutureSkew)) {
		return ErrTimestampFuture
	}

	if e.UserID == nil && e.IPHash == "" {
		return ErrIdentityRequired
	}

	return nil
}
