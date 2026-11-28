package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/KailBug/Kepler/internal/sandbox"
	"github.com/KailBug/Kepler/internal/sandbox/docker"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ModuleOptions configures the pluggable MCP Gateway module for keplerd.
type ModuleOptions struct {
	Mux            *http.ServeMux
	Logger         *slog.Logger
	PoolProvider   func() *pgxpool.Pool // optional shared PostgreSQL pool (nil -> memory store)
	Store          Store                // optional explicit Store override
	Sandbox        sandbox.Provider     // optional SandboxProvider override
	DockerCmd      string               // e.g. "docker" or "wsl -e docker"
	SandboxRuntime string               // "runc" | "runsc"
	SandboxImage   string               // default "python:3.12-alpine"
	ProxyTargets   string               // optional JSON array of CredentialBinding
}

// Module adapts Gateway into a lifecycle-managed keplerd module (implements module.Module).
type Module struct {
	opts    ModuleOptions
	logger  *slog.Logger
	gateway *Gateway
}

func NewModule(opts ModuleOptions) *Module {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Module{
		opts:   opts,
		logger: logger,
	}
}

func (m *Module) Name() string { return "gateway" }

// Gateway exposes the initialized Gateway instance after Start.
func (m *Module) Gateway() *Gateway { return m.gateway }

func (m *Module) Start(ctx context.Context) error {
	store := m.opts.Store
	backend := "custom"
	if store == nil {
		var pool *pgxpool.Pool
		if m.opts.PoolProvider != nil {
			pool = m.opts.PoolProvider()
		}
		if pool != nil {
			pgStore := NewPostgresStore(pool)
			if err := pgStore.Migrate(ctx); err != nil {
				return fmt.Errorf("migrate gateway postgres tables: %w", err)
			}
			store = pgStore
			backend = "postgres"
		} else {
			store = NewMemoryStore()
			backend = "memory"
		}
	}

	sb := m.opts.Sandbox
	if sb == nil {
		sb = docker.New(docker.Options{
			CommandPrefix:  docker.ParseCommandPrefix(m.opts.DockerCmd),
			DefaultRuntime: m.opts.SandboxRuntime,
			DefaultImage:   m.opts.SandboxImage,
		})
	}

	spec := sandbox.DefaultSpec(m.opts.SandboxRuntime)
	if m.opts.SandboxImage != "" {
		spec.Image = m.opts.SandboxImage
	}

	gw := New(store)
	if err := gw.Register(NewPythonExecTool(sb, spec)); err != nil {
		return fmt.Errorf("register python_exec tool: %w", err)
	}

	if raw := strings.TrimSpace(m.opts.ProxyTargets); raw != "" {
		var bindings []CredentialBinding
		if err := json.Unmarshal([]byte(raw), &bindings); err != nil {
			return fmt.Errorf("parse KEPLER_PROXY_TARGETS: %w", err)
		}
		if len(bindings) > 0 {
			requireApproval := false
			for _, b := range bindings {
				if b.RequireApproval {
					requireApproval = true
					break
				}
			}
			if err := RegisterCredentialProxyTool(gw, CredentialProxyOptions{
				Bindings:        bindings,
				RequireApproval: requireApproval,
			}); err != nil {
				return fmt.Errorf("register http_proxy tool: %w", err)
			}
		}
	}

	if m.opts.Mux != nil {
		m.opts.Mux.Handle("/mcp", gw.MCPHandler())
		m.opts.Mux.Handle("GET /api/v1/audit", gw.AuditHandler())
	}

	m.gateway = gw
	toolNames := make([]string, 0, len(gw.ListTools()))
	for _, t := range gw.ListTools() {
		toolNames = append(toolNames, t.Name)
	}
	m.logger.Info("mcp gateway initialized",
		"store", backend,
		"sandbox_runtime", spec.Runtime,
		"tools", toolNames,
	)
	return nil
}

func (m *Module) Stop(_ context.Context) error {
	return nil
}
