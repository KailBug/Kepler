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
	"github.com/KailBug/Kepler/internal/gateway"
	"github.com/KailBug/Kepler/internal/module"
	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/memory"
	"github.com/KailBug/Kepler/internal/runtime/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runtimeModule manages the Durable Runtime store lifecycle and mounts Worker/Session services.
type runtimeModule struct {
	cfg         config.Config
	srv         *api.Server
	logger      *slog.Logger
	pool        *pgxpool.Pool
	store       runtime.Store
	broadcaster *runtime.Broadcaster
	worker      *runtime.WorkerService
	sessions    *runtime.SessionService
}

func (m *runtimeModule) Name() string        { return "runtime" }
func (m *runtimeModule) Pool() *pgxpool.Pool { return m.pool }

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

	m.broadcaster = runtime.NewBroadcaster()
	m.worker = runtime.NewWorkerService(m.store, runtime.WorkerServiceOptions{
		Broadcaster: m.broadcaster,
	})
	m.sessions = runtime.NewSessionService(m.store, runtime.SessionServiceOptions{
		Broadcaster:   m.broadcaster,
		OnTaskCreated: m.worker.NotifyNewTask,
	})

	keplerv1connect.RegisterWorkerServiceHandler(m.srv.Connect(), m.worker)
	keplerv1connect.RegisterSessionServiceHandler(m.srv.Connect(), m.sessions)
	api.MountSessionRoutes(m.srv.Mux(), api.SessionRoutesOptions{
		Store:         m.store,
		Broadcaster:   m.broadcaster,
		OnTaskCreated: m.worker.NotifyNewTask,
	})
	return nil
}

func (m *runtimeModule) Stop(_ context.Context) error {
	if m.pool != nil {
		m.pool.Close()
	}
	return nil
}

func buildServer(cfg config.Config, logger *slog.Logger) (*module.Registry, *api.Server, error) {
	reg := module.NewRegistry()
	srv := api.NewServer(cfg.HTTPAddr, reg.Names, logger)
	rt := &runtimeModule{cfg: cfg, srv: srv, logger: logger}
	gw := gateway.NewModule(gateway.ModuleOptions{
		Mux:            srv.Mux(),
		Logger:         logger,
		PoolProvider:   rt.Pool,
		DockerCmd:      cfg.DockerCmd,
		SandboxRuntime: cfg.SandboxRuntime,
		SandboxImage:   cfg.SandboxImage,
		ProxyTargets:   cfg.ProxyTargets,
	})

	for _, mod := range []module.Module{rt, gw, srv} {
		if err := reg.Register(mod); err != nil {
			return nil, nil, fmt.Errorf("register module %q: %w", mod.Name(), err)
		}
	}
	return reg, srv, nil
}

func main() {
	cfg := config.LoadFromEnv()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg, _, err := buildServer(cfg, logger)
	if err != nil {
		logger.Error("keplerd assemble failed", "error", err)
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
