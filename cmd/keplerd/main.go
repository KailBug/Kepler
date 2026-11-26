package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KailBug/Kepler/gen/go/kepler/v1/keplerv1connect"
	"github.com/KailBug/Kepler/internal/api"
	"github.com/KailBug/Kepler/internal/config"
	"github.com/KailBug/Kepler/internal/module"
	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/memory"
	"github.com/KailBug/Kepler/internal/runtime/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runtimeModule manages the Durable Runtime store lifecycle and mounts WorkerService.
type runtimeModule struct {
	cfg    config.Config
	srv    *api.Server
	logger *slog.Logger
	pool   *pgxpool.Pool
	store  runtime.Store
	worker *runtime.WorkerService
}

func (m *runtimeModule) Name() string { return "runtime" }

func (m *runtimeModule) Start(ctx context.Context) error {
	switch m.cfg.StoreBackend {
	case "memory":
		m.store = memory.New()
		m.logger.Info("runtime store initialized", "backend", "memory")
	case "postgres":
		pool, err := pgxpool.New(ctx, m.cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return fmt.Errorf("ping postgres: %w", err)
		}
		pgStore := postgres.New(pool)
		if err := pgStore.Migrate(ctx); err != nil {
			pool.Close()
			return fmt.Errorf("migrate postgres: %w", err)
		}
		m.pool = pool
		m.store = pgStore
		m.logger.Info("runtime store initialized", "backend", "postgres")
	default:
		return fmt.Errorf("unsupported KEPLER_STORE %q (expected postgres or memory)", m.cfg.StoreBackend)
	}

	m.worker = runtime.NewWorkerService(m.store, runtime.WorkerServiceOptions{})
	keplerv1connect.RegisterWorkerServiceHandler(m.srv.Connect(), m.worker)
	return nil
}

func (m *runtimeModule) Stop(_ context.Context) error {
	if m.pool != nil {
		m.pool.Close()
	}
	return nil
}

func main() {
	cfg := config.LoadFromEnv()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := module.NewRegistry()
	srv := api.NewServer(cfg.HTTPAddr, reg.Names, logger)
	rt := &runtimeModule{cfg: cfg, srv: srv, logger: logger}

	if err := reg.Register(rt); err != nil {
		logger.Error("register runtime module failed", "error", err)
		os.Exit(1)
	}
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
