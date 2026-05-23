package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"search_trend/internal/app"
	"search_trend/internal/config"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := config.Load()
	application, err := app.Build(cfg, logger)
	if err != nil {
		logger.Error("build app failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		logger.Error("run app failed", "error", err)
		os.Exit(1)
	}
}
