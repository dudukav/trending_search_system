package model

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSearchEventValidate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	userID := uuid.New()

	tests := []struct {
		name  string
		event SearchEvent
		want  error
	}{
		{
			name: "valid",
			event: SearchEvent{
				EventID:   uuid.New(),
				Query:     "iphone",
				UserID:    &userID,
				Timestamp: now,
			},
		},
		{
			name: "missing event id",
			event: SearchEvent{
				Query:     "iphone",
				UserID:    &userID,
				Timestamp: now,
			},
			want: ErrEventIDRequired,
		},
		{
			name: "missing query",
			event: SearchEvent{
				EventID:   uuid.New(),
				UserID:    &userID,
				Timestamp: now,
			},
			want: ErrQueryRequired,
		},
		{
			name: "missing timestamp",
			event: SearchEvent{
				EventID: uuid.New(),
				Query:   "iphone",
				UserID:  &userID,
			},
			want: ErrTimestampMissing,
		},
		{
			name: "too old",
			event: SearchEvent{
				EventID:   uuid.New(),
				Query:     "iphone",
				UserID:    &userID,
				Timestamp: now.Add(-11 * time.Minute),
			},
			want: ErrTimestampTooOld,
		},
		{
			name: "future",
			event: SearchEvent{
				EventID:   uuid.New(),
				Query:     "iphone",
				UserID:    &userID,
				Timestamp: now.Add(time.Minute),
			},
			want: ErrTimestampFuture,
		},
		{
			name: "missing identity",
			event: SearchEvent{
				EventID:   uuid.New(),
				Query:     "iphone",
				Timestamp: now,
			},
			want: ErrIdentityRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate(now, 10*time.Minute, 30*time.Second)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.want)
			}
		})
	}
}
