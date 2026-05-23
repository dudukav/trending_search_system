FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/search ./cmd/search
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/producer ./cmd/producer

FROM alpine:3.20

RUN adduser -D -H appuser
USER appuser

COPY --from=builder /out/search /bin/search
COPY --from=builder /out/producer /bin/producer

EXPOSE 8080

ENTRYPOINT ["/bin/search"]
