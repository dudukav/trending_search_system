package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type result struct {
	duration time.Duration
	status   int
	err      error
}

func main() {
	var (
		url         = flag.String("url", "http://localhost:8080/v1/trends?limit=10", "target URL")
		duration    = flag.Duration("duration", 30*time.Second, "test duration")
		concurrency = flag.Int("concurrency", 200, "number of concurrent workers")
		timeout     = flag.Duration("timeout", 3*time.Second, "per-request timeout")
		failErrors  = flag.Bool("fail-on-errors", false, "exit with non-zero code when errors or non-2xx responses are observed")
	)
	flag.Parse()

	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "concurrency must be positive")
		os.Exit(1)
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be positive")
		os.Exit(1)
	}

	summary := run(*url, *duration, *concurrency, *timeout)
	printSummary(*url, *duration, *concurrency, summary)

	if *failErrors && summary.errors > 0 {
		os.Exit(1)
	}
}

type summary struct {
	total      int64
	errors     int64
	latencies  []time.Duration
	statusCode map[int]int64
}

func run(url string, duration time.Duration, concurrency int, timeout time.Duration) summary {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
		},
	}

	results := make(chan result, concurrency*2)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					results <- request(ctx, client, url)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	statusCode := make(map[int]int64)
	latencies := make([]time.Duration, 0, concurrency*1024)
	var total int64
	var errors int64

	for res := range results {
		total++
		latencies = append(latencies, res.duration)
		if res.err != nil {
			errors++
			continue
		}
		statusCode[res.status]++
		if res.status < 200 || res.status >= 300 {
			errors++
		}
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	return summary{
		total:      total,
		errors:     errors,
		latencies:  latencies,
		statusCode: statusCode,
	}
}

func request(ctx context.Context, client *http.Client, url string) result {
	startedAt := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return result{duration: time.Since(startedAt), err: err}
	}

	resp, err := client.Do(req)
	if err != nil {
		return result{duration: time.Since(startedAt), err: err}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return result{duration: time.Since(startedAt), status: resp.StatusCode, err: err}
	}

	return result{duration: time.Since(startedAt), status: resp.StatusCode}
}

func printSummary(url string, duration time.Duration, concurrency int, summary summary) {
	fmt.Printf("URL: %s\n", url)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Requests: %d\n", summary.total)
	fmt.Printf("Errors/non-2xx: %d\n", summary.errors)
	fmt.Printf("Requests/sec: %.2f\n", float64(summary.total)/duration.Seconds())
	fmt.Printf("Status codes: %v\n", summary.statusCode)

	if len(summary.latencies) == 0 {
		return
	}

	fmt.Printf("Latency avg: %s\n", average(summary.latencies))
	fmt.Printf("Latency p50: %s\n", percentile(summary.latencies, 0.50))
	fmt.Printf("Latency p95: %s\n", percentile(summary.latencies, 0.95))
	fmt.Printf("Latency p99: %s\n", percentile(summary.latencies, 0.99))
	fmt.Printf("Latency max: %s\n", summary.latencies[len(summary.latencies)-1])
}

func average(values []time.Duration) time.Duration {
	var total time.Duration
	for _, value := range values {
		total += value
	}

	return total / time.Duration(len(values))
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}

	index := int(float64(len(values)-1) * p)
	return values[index]
}
