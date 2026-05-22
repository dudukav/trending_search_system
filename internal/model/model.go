package model

import (
	"time"

	"github.com/google/uuid"
)

type SearchEvent struct {
	EventID 	uuid.UUID 	`json:"event_id"`
	Query 		string		`json:"query"`
	UserID 		*uuid.UUID	`json:"user_id"`
	IPHash 		string		`json:"ip_hash"`
	Timestamp 	time.Time	`json:"timestamp"`
	Source 		string		`json:"source"`
}