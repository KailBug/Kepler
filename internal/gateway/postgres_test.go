package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/KailBug/Kepler/internal/gateway"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresGatewayStore(t *testing.T) {
	dsn := os.Getenv("KEPLER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set KEPLER_TEST_DATABASE_URL to run PostgreSQL gateway store tests")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	store := gateway.NewPostgresStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE tool_idempotency, tool_audit_logs`); err != nil {
		t.Fatalf("TRUNCATE: %v", err)
	}

	// 1. Reserve key -> in_progress conflict -> finish -> cache hit
	cached, acquired, err := store.BeginIdempotency(ctx, "pg-key-1", "python_exec", "s1", "t1")
	if err != nil || !acquired || cached != nil {
		t.Fatalf("BeginIdempotency(1): cached=%v acquired=%v err=%v", cached, acquired, err)
	}
	if _, _, err := store.BeginIdempotency(ctx, "pg-key-1", "python_exec", "s1", "t1"); !errors.Is(err, gateway.ErrIdempotencyInProgress) {
		t.Fatalf("expected ErrIdempotencyInProgress, got %v", err)
	}
	if err := store.FinishIdempotency(ctx, "pg-key-1", gateway.CallResult{Content: "42"}); err != nil {
		t.Fatalf("FinishIdempotency: %v", err)
	}
	cached, acquired, err = store.BeginIdempotency(ctx, "pg-key-1", "python_exec", "s1", "t1")
	if err != nil || acquired || cached == nil || cached.Content != "42" {
		t.Fatalf("BeginIdempotency(hit): cached=%+v acquired=%v err=%v", cached, acquired, err)
	}

	// 2. Abort releases key for retry
	_, acquired, err = store.BeginIdempotency(ctx, "pg-key-abort", "python_exec", "s1", "t1")
	if err != nil || !acquired {
		t.Fatalf("BeginIdempotency(abort): acquired=%v err=%v", acquired, err)
	}
	if err := store.AbortIdempotency(ctx, "pg-key-abort"); err != nil {
		t.Fatalf("AbortIdempotency: %v", err)
	}
	_, acquired, err = store.BeginIdempotency(ctx, "pg-key-abort", "python_exec", "s1", "t1")
	if err != nil || !acquired {
		t.Fatalf("BeginIdempotency(retry): acquired=%v err=%v", acquired, err)
	}

	// 3. Audit log write + filtered read
	if err := store.RecordAudit(ctx, gateway.AuditEntry{
		SessionID:      "s1",
		TurnID:         "t1",
		ToolCallID:     "c1",
		IdempotencyKey: "pg-key-1",
		ToolName:       "python_exec",
		Arguments:      json.RawMessage(`{"code":"print(42)"}`),
		Status:         "ok",
		OutputPreview:  "42",
		DurationMs:     120,
	}); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	entries, err := store.ListAuditLogs(ctx, gateway.AuditFilter{SessionID: "s1", Limit: 10})
	if err != nil || len(entries) != 1 || entries[0].OutputPreview != "42" {
		t.Fatalf("ListAuditLogs: entries=%+v err=%v", entries, err)
	}
}
