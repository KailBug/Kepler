package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KailBug/Kepler/internal/api"
	"github.com/KailBug/Kepler/internal/config"
	"github.com/KailBug/Kepler/internal/module"
)

func main() {
	cfg := config.LoadFromEnv()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := module.NewRegistry()
	srv := api.NewServer(cfg.HTTPAddr, reg.Names, logger)
	if err := reg.Register(srv); err != nil {
		logger.Error("register api module failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := reg.StartAll(ctx); err != nil {
		logger.Error("keplerd startup failed", "error", err)
		os.Exit(1)
	}
	logger.Info("keplerd started", "modules", reg.Names(), "sandbox_runtime", cfg.SandboxRuntime)

	<-ctx.Done()
	logger.Info("shutting down keplerd...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := reg.StopAll(shutdownCtx); err != nil {
		logger.Error("keplerd shutdown completed with errors", "error", err)
		os.Exit(1)
	}
	logger.Info("keplerd stopped cleanly")
}
