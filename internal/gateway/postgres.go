package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KailBug/Kepler/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore persists MCP Gateway idempotency records and audit logs in PostgreSQL.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, migrations.GatewaySQL)
	return err
}

func (s *PostgresStore) BeginIdempotency(ctx context.Context, key, toolName, sessionID, turnID string) (*CallResult, bool, error) {
	now := time.Now().UnixMilli()
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO tool_idempotency (idempotency_key, tool_name, session_id, turn_id, status, created_at_ms, updated_at_ms)
		VALUES ($1, $2, $3, $4, 'in_progress', $5, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, key, toolName, sessionID, turnID, now)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 1 {
		return nil, true, nil
	}

	var (
		status string
		raw    []byte
	)
	err = s.pool.QueryRow(ctx, `
		SELECT status, result FROM tool_idempotency WHERE idempotency_key = $1
	`, key).Scan(&status, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		// Rare race with AbortIdempotency; retry once.
		return s.BeginIdempotency(ctx, key, toolName, sessionID, turnID)
	}
	if err != nil {
		return nil, false, err
	}
	if status == "in_progress" {
		return nil, false, ErrIdempotencyInProgress
	}
	var cached CallResult
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cached); err != nil {
			return nil, false, fmt.Errorf("decode cached idempotency result: %w", err)
		}
	}
	return &cached, false, nil
}

func (s *PostgresStore) FinishIdempotency(ctx context.Context, key string, result CallResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE tool_idempotency SET status = 'completed', result = $1, updated_at_ms = $2
		WHERE idempotency_key = $3
	`, raw, time.Now().UnixMilli(), key)
	return err
}

func (s *PostgresStore) AbortIdempotency(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM tool_idempotency WHERE idempotency_key = $1 AND status = 'in_progress'
	`, key)
	return err
}

func (s *PostgresStore) RecordAudit(ctx context.Context, entry AuditEntry) error {
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if entry.CreatedAtMs == 0 {
		entry.CreatedAtMs = time.Now().UnixMilli()
	}
	args := entry.Arguments
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tool_audit_logs (
			id, session_id, turn_id, tool_call_id, idempotency_key,
			tool_name, arguments, status, output_preview, error, duration_ms, created_at_ms
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`, entry.ID, entry.SessionID, entry.TurnID, entry.ToolCallID, entry.IdempotencyKey,
		entry.ToolName, args, entry.Status, entry.OutputPreview, entry.Error, entry.DurationMs, entry.CreatedAtMs)
	return err
}

func (s *PostgresStore) ListAuditLogs(ctx context.Context, filter AuditFilter) ([]AuditEntry, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	var (
		clauses []string
		args    []any
	)
	if filter.SessionID != "" {
		args = append(args, filter.SessionID)
		clauses = append(clauses, fmt.Sprintf("session_id = $%d", len(args)))
	}
	if filter.ToolName != "" {
		args = append(args, filter.ToolName)
		clauses = append(clauses, fmt.Sprintf("tool_name = $%d", len(args)))
	}
	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT id, session_id, turn_id, tool_call_id, idempotency_key,
		       tool_name, arguments, status, output_preview, error, duration_ms, created_at_ms
		FROM tool_audit_logs %s
		ORDER BY created_at_ms DESC, id DESC
		LIMIT $%d
	`, where, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(
			&e.ID, &e.SessionID, &e.TurnID, &e.ToolCallID, &e.IdempotencyKey,
			&e.ToolName, &e.Arguments, &e.Status, &e.OutputPreview, &e.Error, &e.DurationMs, &e.CreatedAtMs,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
