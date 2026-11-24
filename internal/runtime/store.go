package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrSessionBusy      = errors.New("session is busy")
	ErrNotWaitingReview = errors.New("session is not waiting for approval")
	ErrStaleEpoch       = errors.New("stale lease epoch")
	ErrSeqConflict      = errors.New("event sequence conflict")
)

const (
	SessionStatusIdle            = "idle"
	SessionStatusRunning         = "running"
	SessionStatusWaitingApproval = "waiting_approval"

	TaskStatusPending   = "pending"
	TaskStatusLeased    = "leased"
	TaskStatusCompleted = "completed"
	TaskStatusSuspended = "suspended"
	TaskStatusFailed    = "failed"
	TaskStatusCancelled = "cancelled"
)

// Session mirrors kepler.v1.SessionInfo.
type Session struct {
	ID             string `json:"id"`
	AgentID        string `json:"agentId"`
	Title          string `json:"title"`
	Status         string `json:"status"`
	LastSeq        int64  `json:"lastSeq"`
	ActiveWorkerID string `json:"activeWorkerId"`
	LeaseEpoch     int64  `json:"leaseEpoch"`
	CreatedAtMs    int64  `json:"createdAtMs"`
	UpdatedAtMs    int64  `json:"updatedAtMs"`
}

// Event mirrors kepler.v1.SessionEvent for storage backends (PostgreSQL / Memory).
type Event struct {
	ID        string          `json:"id"`
	Seq       int64           `json:"seq"`
	SessionID string          `json:"sessionId"`
	TurnID    string          `json:"turnId"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	TsMs      int64           `json:"tsMs"`
}

// Lease represents a fencing token granted to a worker for a task.
type Lease struct {
	LeaseID     string `json:"leaseId"`
	Epoch       int64  `json:"epoch"`
	ExpiresAtMs int64  `json:"expiresAtMs"`
}

// Task represents a unit of work queued for a Harness Worker.
type Task struct {
	ID          string `json:"id"`
	SessionID   string `json:"sessionId"`
	TurnID      string `json:"turnId"`
	AgentID     string `json:"agentId"`
	Status      string `json:"status"`
	WorkerID    string `json:"workerId"`
	Lease       Lease  `json:"lease"`
	Traceparent string `json:"traceparent"`
	Error       string `json:"error"`
	CreatedAtMs int64  `json:"createdAtMs"`
	UpdatedAtMs int64  `json:"updatedAtMs"`
}

// AcquiredTask is returned to a worker when it claims a task.
type AcquiredTask struct {
	Task   Task
	Events []Event
}

// WorkerRegistration records an online Harness Worker.
type WorkerRegistration struct {
	WorkerID     string
	Runtime      string
	Version      string
	Capabilities []string
}

// EventStore is the minimal event-sourcing persistence interface.
type EventStore interface {
	AppendEvents(ctx context.Context, sessionID, leaseID string, epoch int64, events []Event) (lastSeq int64, err error)
	ListEvents(ctx context.Context, sessionID string, sinceSeq int64) ([]Event, error)
}

// Store is the full pluggable persistence & scheduling contract for Durable Runtime.
type Store interface {
	EventStore

	// Worker lifecycle & task scheduling
	RegisterWorker(ctx context.Context, reg WorkerRegistration) error
	HeartbeatWorker(ctx context.Context, workerID string, activeTasks int32) error
	AcquireTask(ctx context.Context, workerID string, leaseTTL time.Duration) (*AcquiredTask, error)
	RenewLease(ctx context.Context, taskID, leaseID string, epoch int64, leaseTTL time.Duration) (Lease, error)
	CompleteTask(ctx context.Context, taskID, leaseID string, epoch int64) error
	SuspendTask(ctx context.Context, taskID, leaseID string, epoch int64, reason string) error
	FailTask(ctx context.Context, taskID, leaseID string, epoch int64, errMsg string, retryable bool) error

	// User-facing session operations
	CreateSession(ctx context.Context, agentID string) (Session, error)
	ListSessions(ctx context.Context, agentID string, limit int) ([]Session, error)
	GetSession(ctx context.Context, sessionID string) (Session, []Event, error)
	SubmitUserMessage(ctx context.Context, sessionID, text string) (turnID string, ev Event, task Task, err error)
	ResolveApproval(ctx context.Context, sessionID, toolCallID string, approved bool, reason string) (Event, Task, error)
	ResumeSession(ctx context.Context, sessionID string) (Task, error)
	CancelSession(ctx context.Context, sessionID string) error
}
