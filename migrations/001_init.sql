-- Kepler Durable Runtime 初始表结构（W2）
-- 四张核心表：sessions / events / tasks / workers

CREATE TABLE IF NOT EXISTS sessions (
    id               TEXT PRIMARY KEY,
    agent_id         TEXT NOT NULL DEFAULT '',
    title            TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'idle',
    last_seq         BIGINT NOT NULL DEFAULT 0,
    active_worker_id TEXT NOT NULL DEFAULT '',
    lease_epoch      BIGINT NOT NULL DEFAULT 0,
    created_at_ms    BIGINT NOT NULL,
    updated_at_ms    BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions (updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq        BIGINT NOT NULL,
    turn_id    TEXT NOT NULL,
    type       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    ts_ms      BIGINT NOT NULL,
    UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_session_seq ON events (session_id, seq);

CREATE TABLE IF NOT EXISTS tasks (
    id                  TEXT PRIMARY KEY,
    session_id          TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    turn_id             TEXT NOT NULL,
    agent_id            TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL,
    worker_id           TEXT NOT NULL DEFAULT '',
    lease_id            TEXT NOT NULL DEFAULT '',
    lease_epoch         BIGINT NOT NULL DEFAULT 0,
    lease_expires_at_ms BIGINT NOT NULL DEFAULT 0,
    traceparent         TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    created_at_ms       BIGINT NOT NULL,
    updated_at_ms       BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_claimable ON tasks (status, lease_expires_at_ms, created_at_ms);
CREATE INDEX IF NOT EXISTS idx_tasks_session_status ON tasks (session_id, status);

CREATE TABLE IF NOT EXISTS workers (
    id                TEXT PRIMARY KEY,
    runtime           TEXT NOT NULL,
    version           TEXT NOT NULL DEFAULT '',
    capabilities      JSONB NOT NULL DEFAULT '[]'::jsonb,
    active_tasks      INT NOT NULL DEFAULT 0,
    last_heartbeat_ms BIGINT NOT NULL,
    created_at_ms     BIGINT NOT NULL
);
