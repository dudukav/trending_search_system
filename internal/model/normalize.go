package model

import "strings"

const MaxQueryLen = 150

func Normalize(query string) string {
	query = strings.TrimSpace(query)
	query = strings.ToLower(query)
	fields := strings.Fields(query)
	query = strings.Join(fields, " ")

	if len(query) > MaxQueryLen {
		query = query[:MaxQueryLen]
	}

	return query
}
