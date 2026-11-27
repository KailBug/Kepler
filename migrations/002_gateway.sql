-- Kepler MCP Gateway 幂等与审计表结构（W3）

CREATE TABLE IF NOT EXISTS tool_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    tool_name       TEXT NOT NULL,
    session_id      TEXT NOT NULL DEFAULT '',
    turn_id         TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL,
    result          JSONB,
    created_at_ms   BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS tool_audit_logs (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL DEFAULT '',
    turn_id         TEXT NOT NULL DEFAULT '',
    tool_call_id    TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    tool_name       TEXT NOT NULL,
    arguments       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL,
    output_preview  TEXT NOT NULL DEFAULT '',
    error           TEXT NOT NULL DEFAULT '',
    duration_ms     BIGINT NOT NULL DEFAULT 0,
    created_at_ms   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_audit_created_at ON tool_audit_logs (created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_tool_audit_session ON tool_audit_logs (session_id, created_at_ms DESC);
