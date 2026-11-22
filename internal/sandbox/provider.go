package sandbox

import (
	"context"
	"time"
)

// Spec defines resource and isolation constraints when acquiring a sandbox instance.
type Spec struct {
	Image       string
	Runtime     string // "runc" | "runsc"
	NetworkMode string // default: "none"
	ReadOnly    bool
	MemoryMB    int64
	CPUMilli    int64
}

// Lease represents an acquired sandbox instance.
type Lease struct {
	ID        string
	Runtime   string
	CreatedAt time.Time
}

// ExecRequest describes a command executed inside an isolated sandbox.
type ExecRequest struct {
	Command []string
	WorkDir string
	Env     map[string]string
	Timeout time.Duration
}

// ExecResult captures bounded output and exit status from sandbox execution.
type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
}

// Provider decouples the runtime and MCP Gateway from concrete sandbox technologies
// (Docker + runc, Docker + gVisor runsc, Warm Pool decorator, Firecracker/E2B).
type Provider interface {
	Acquire(ctx context.Context, spec Spec) (Lease, error)
	Exec(ctx context.Context, lease Lease, req ExecRequest) (ExecResult, error)
	SyncWorkspace(ctx context.Context, lease Lease, hostDir string) error
	Release(ctx context.Context, lease Lease) error
}
