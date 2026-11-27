package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

const maxAuditPreviewBytes = 1024

// Gateway coordinates tool registration, secret redaction, idempotency, and audit logging.
type Gateway struct {
	store   Store
	mu      sync.RWMutex
	tools   map[string]Tool
	secrets []string
}

func New(store Store) *Gateway {
	if store == nil {
		store = NewMemoryStore()
	}
	return &Gateway{
		store: store,
		tools: make(map[string]Tool),
	}
}

// AddSecret registers a secret value that will be scrubbed from all tool outputs and audit logs.
func (g *Gateway) AddSecret(secret string) {
	if len(secret) < 4 {
		return
	}
	g.mu.Lock()
	g.secrets = append(g.secrets, secret)
	g.mu.Unlock()
}

func (g *Gateway) snapshotSecrets() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]string(nil), g.secrets...)
}

// Register adds a tool to the gateway. Duplicate names are rejected.
func (g *Gateway) Register(tool Tool) error {
	if tool.Metadata.Name == "" {
		return fmt.Errorf("tool name is required")
	}
	if tool.Handler == nil {
		return fmt.Errorf("tool %q handler is required", tool.Metadata.Name)
	}
	if len(tool.Metadata.InputSchema) == 0 {
		tool.Metadata.InputSchema = json.RawMessage(`{"type":"object","properties":{}}`)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.tools[tool.Metadata.Name]; exists {
		return fmt.Errorf("%w: %s", ErrToolAlreadyRegistered, tool.Metadata.Name)
	}
	g.tools[tool.Metadata.Name] = tool
	return nil
}

// ListTools returns registered tool metadata sorted alphabetically by name.
func (g *Gateway) ListTools() []ToolMetadata {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]ToolMetadata, 0, len(g.tools))
	for _, t := range g.tools {
		out = append(out, t.Metadata)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ListAuditLogs queries persisted audit records.
func (g *Gateway) ListAuditLogs(ctx context.Context, filter AuditFilter) ([]AuditEntry, error) {
	return g.store.ListAuditLogs(ctx, filter)
}

// CallTool executes a registered tool with idempotency protection, secret redaction, and audit logging.
func (g *Gateway) CallTool(ctx context.Context, toolName string, call CallContext, args json.RawMessage) (CallResult, error) {
	start := time.Now()
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}

	g.mu.RLock()
	tool, ok := g.tools[toolName]
	g.mu.RUnlock()
	if !ok {
		return CallResult{}, fmt.Errorf("%w: %s", ErrToolNotFound, toolName)
	}

	secrets := g.snapshotSecrets()
	redactedArgs := json.RawMessage(Redact(string(args), secrets))
	if !json.Valid(redactedArgs) {
		redactedArgs = json.RawMessage(`{}`)
	}

	// 1. Idempotency check & reservation
	if call.IdempotencyKey != "" {
		cached, acquired, err := g.store.BeginIdempotency(ctx, call.IdempotencyKey, toolName, call.SessionID, call.TurnID)
		if err != nil {
			return CallResult{}, err
		}
		if !acquired && cached != nil {
			res := *cached
			res.Content = Redact(res.Content, secrets)
			_ = g.store.RecordAudit(ctx, AuditEntry{
				ID:             uuid.NewString(),
				SessionID:      call.SessionID,
				TurnID:         call.TurnID,
				ToolCallID:     call.ToolCallID,
				IdempotencyKey: call.IdempotencyKey,
				ToolName:       toolName,
				Arguments:      redactedArgs,
				Status:         "cached",
				OutputPreview:  truncatePreview(res.Content),
				DurationMs:     time.Since(start).Milliseconds(),
				CreatedAtMs:    time.Now().UnixMilli(),
			})
			return res, nil
		}
	}

	// 2. Execute tool handler
	res, execErr := tool.Handler(ctx, call, args)
	durationMs := time.Since(start).Milliseconds()

	if execErr != nil {
		if call.IdempotencyKey != "" {
			_ = g.store.AbortIdempotency(context.WithoutCancel(ctx), call.IdempotencyKey)
		}
		errMsg := Redact(execErr.Error(), secrets)
		_ = g.store.RecordAudit(context.WithoutCancel(ctx), AuditEntry{
			ID:             uuid.NewString(),
			SessionID:      call.SessionID,
			TurnID:         call.TurnID,
			ToolCallID:     call.ToolCallID,
			IdempotencyKey: call.IdempotencyKey,
			ToolName:       toolName,
			Arguments:      redactedArgs,
			Status:         "error",
			Error:          truncatePreview(errMsg),
			DurationMs:     durationMs,
			CreatedAtMs:    time.Now().UnixMilli(),
		})
		return CallResult{}, fmt.Errorf("%s", errMsg)
	}

	// 3. Scrub secrets from result, commit idempotency cache, and persist audit entry
	res.Content = Redact(res.Content, secrets)
	if call.IdempotencyKey != "" {
		_ = g.store.FinishIdempotency(context.WithoutCancel(ctx), call.IdempotencyKey, res)
	}

	status := "ok"
	if res.IsError {
		status = "error"
	}
	_ = g.store.RecordAudit(context.WithoutCancel(ctx), AuditEntry{
		ID:             uuid.NewString(),
		SessionID:      call.SessionID,
		TurnID:         call.TurnID,
		ToolCallID:     call.ToolCallID,
		IdempotencyKey: call.IdempotencyKey,
		ToolName:       toolName,
		Arguments:      redactedArgs,
		Status:         status,
		OutputPreview:  truncatePreview(res.Content),
		DurationMs:     durationMs,
		CreatedAtMs:    time.Now().UnixMilli(),
	})
	return res, nil
}

func truncatePreview(s string) string {
	if len(s) <= maxAuditPreviewBytes {
		return s
	}
	return s[:maxAuditPreviewBytes] + "...[truncated]"
}
