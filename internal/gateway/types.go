package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrToolNotFound          = errors.New("tool not found")
	ErrToolAlreadyRegistered = errors.New("tool already registered")
	ErrIdempotencyInProgress = errors.New("idempotent tool call is already in progress")
)

// ToolMetadata describes an MCP tool exposed by the Gateway.
type ToolMetadata struct {
	Name            string          `json:"name"`
	Description     string          `json:"description"`
	InputSchema     json.RawMessage `json:"inputSchema"`
	RequireApproval bool            `json:"requireApproval,omitempty"`
}

// CallContext carries governance, tracing, and idempotency context into tool handlers.
type CallContext struct {
	SessionID      string
	TurnID         string
	ToolCallID     string
	IdempotencyKey string
}

// CallResult is the normalized MCP tool output stored in idempotency cache and returned to callers.
type CallResult struct {
	Content string `json:"content"`
	IsError bool   `json:"isError,omitempty"`
}

// ToolHandler executes a registered tool with validated JSON arguments.
type ToolHandler func(ctx context.Context, call CallContext, args json.RawMessage) (CallResult, error)

// Tool bundles MCP metadata with its server-side execution handler.
type Tool struct {
	Metadata ToolMetadata
	Handler  ToolHandler
}

// AuditEntry records every tool invocation passing through the MCP Gateway.
type AuditEntry struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"sessionId,omitempty"`
	TurnID         string          `json:"turnId,omitempty"`
	ToolCallID     string          `json:"toolCallId,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	ToolName       string          `json:"toolName"`
	Arguments      json.RawMessage `json:"arguments"`
	Status         string          `json:"status"` // "ok" | "error" | "cached" | "denied"
	OutputPreview  string          `json:"outputPreview,omitempty"`
	Error          string          `json:"error,omitempty"`
	DurationMs     int64           `json:"durationMs"`
	CreatedAtMs    int64           `json:"createdAtMs"`
}

// AuditFilter controls audit log listing queries.
type AuditFilter struct {
	SessionID string
	ToolName  string
	Limit     int
}

// Store persists MCP Gateway idempotency state and audit logs.
type Store interface {
	// BeginIdempotency reserves key. If already completed, returns (cached, false, nil).
	// If currently in progress, returns (nil, false, ErrIdempotencyInProgress).
	BeginIdempotency(ctx context.Context, key, toolName, sessionID, turnID string) (cached *CallResult, acquired bool, err error)
	FinishIdempotency(ctx context.Context, key string, result CallResult) error
	AbortIdempotency(ctx context.Context, key string) error

	RecordAudit(ctx context.Context, entry AuditEntry) error
	ListAuditLogs(ctx context.Context, filter AuditFilter) ([]AuditEntry, error)
}

// Redact replaces every non-empty secret value with "[REDACTED]" to prevent credential leakage.
func Redact(text string, secrets []string) string {
	out := text
	for _, s := range secrets {
		if len(s) >= 4 {
			out = strings.ReplaceAll(out, s, "[REDACTED]")
		}
	}
	return out
}
