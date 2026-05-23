#!/usr/bin/env bash
set -euo pipefail

URL="${URL:-http://localhost:8080/v1/trends?limit=10}"
DURATION="${DURATION:-30s}"
CONCURRENCY="${CONCURRENCY:-200}"
TIMEOUT="${TIMEOUT:-3s}"

go run ./cmd/loadtest \
  -url "$URL" \
  -duration "$DURATION" \
  -concurrency "$CONCURRENCY" \
  -timeout "$TIMEOUT"
