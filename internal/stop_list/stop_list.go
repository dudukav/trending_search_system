package stoplist

import (
	"errors"
	"search_trend/internal/model"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrEmptyStopRule = errors.New("stop rule value could not be empty")
)

type StopList struct {
	mu    sync.RWMutex
	rules map[uuid.UUID]StopRule
}

func NewStoplist() *StopList {
	return &StopList{
		mu:    sync.RWMutex{},
		rules: make(map[uuid.UUID]StopRule),
	}
}

func (s *StopList) Add(value string, matchType MatchType) (StopRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value = model.Normalize(value)
	if value == "" {
		return StopRule{}, ErrEmptyStopRule
	}

	rule := StopRule{
		ID:        uuid.New(),
		Value:     value,
		MatchType: matchType,
		CreatedAt: time.Now().UTC(),
	}

	s.rules[rule.ID] = rule

	return rule, nil
}

func (s *StopList) Remove(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.rules[id]; !ok {
		return false
	}

	delete(s.rules, id)
	return true
}

func (s *StopList) Contains(query string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query = model.Normalize(query)
	if query == "" {
		return false
	}

	for _, rule := range s.rules {
		switch rule.MatchType {
		case MatchExact:
			if query == rule.Value {
				return true
			}

		case MatchPhrase:
			if phraseMatch(query, rule.Value) {
				return true
			}
		}
	}

	return false
}

func (s *StopList) List() []StopRule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stopRules []StopRule
	for _, rule := range s.rules {
		stopRules = append(stopRules, rule)
	}

	return stopRules
}

func phraseMatch(query string, phrase string) bool {
	queryTokens := strings.Fields(query)
	phraseTokens := strings.Fields(phrase)

	if len(phraseTokens) == 0 || len(phraseTokens) > len(queryTokens) {
		return false
	}

	for i := 0; i <= len(queryTokens)-len(phraseTokens); i++ {
		matched := true
		for j := range phraseTokens {
			if queryTokens[i+j] != phraseTokens[j] {
				matched = false
				break
			}
		}

		if matched {
			return true
		}
	}

	return false
}
