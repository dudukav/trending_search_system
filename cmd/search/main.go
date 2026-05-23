package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"search_trend/internal/app"
	"search_trend/internal/config"
	"syscall"
)

func main() {
	cfg := config.Load()
	application, err := app.Build(cfg)
	if err != nil {
		log.Fatalf("build app: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatalf("run app: %v", err)
	}
}
