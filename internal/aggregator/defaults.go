package aggregator

import "time"

const (
	defaultWindowSize     = 5 * time.Minute
	defaultMaxPerIdentity = 2
	defaultBucketSize     = time.Second
	defaultTopSize        = 100
)
