package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const NotifyChannel = "kepler_events"

// Store implements runtime.Store backed by PostgreSQL (ACID + SKIP LOCKED + Epoch Fencing).
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool: pool,
		now:  time.Now,
	}
}

// WithClock overrides the clock (useful in tests for lease expiration).
func (s *Store) WithClock(fn func() time.Time) *Store {
	s.now = fn
	return s
}

func (s *Store) nowMs() int64 { return s.now().UnixMilli() }

// Migrate applies the embedded initial schema idempotently.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, migrations.InitSQL)
	return err
}

func (s *Store) RegisterWorker(ctx context.Context, reg runtime.WorkerRegistration) error {
	caps, err := json.Marshal(reg.Capabilities)
	if err != nil {
		return err
	}
	now := s.nowMs()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workers (id, runtime, version, capabilities, active_tasks, last_heartbeat_ms, created_at_ms)
		VALUES ($1, $2, $3, $4, 0, $5, $5)
		ON CONFLICT (id) DO UPDATE SET
			runtime = EXCLUDED.runtime,
			version = EXCLUDED.version,
			capabilities = EXCLUDED.capabilities,
			last_heartbeat_ms = EXCLUDED.last_heartbeat_ms
	`, reg.WorkerID, reg.Runtime, reg.Version, caps, now)
	return err
}

func (s *Store) HeartbeatWorker(ctx context.Context, workerID string, activeTasks int32) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE workers SET active_tasks = $1, last_heartbeat_ms = $2 WHERE id = $3
	`, activeTasks, s.nowMs(), workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return runtime.ErrNotFound
	}
	return nil
}

func (s *Store) CreateSession(ctx context.Context, agentID string) (runtime.Session, error) {
	now := s.nowMs()
	sess := runtime.Session{
		ID:          uuid.NewString(),
		AgentID:     agentID,
		Status:      runtime.SessionStatusIdle,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sessions (id, agent_id, title, status, last_seq, active_worker_id, lease_epoch, created_at_ms, updated_at_ms)
		VALUES ($1, $2, '', $3, 0, '', 0, $4, $4)
	`, sess.ID, sess.AgentID, sess.Status, now)
	if err != nil {
		return runtime.Session{}, err
	}
	return sess, nil
}

func (s *Store) ListSessions(ctx context.Context, agentID string, limit int) ([]runtime.Session, error) {
	if limit <= 0 {
		limit = 100
	}
	var (
		rows pgx.Rows
		err  error
	)
	if agentID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, agent_id, title, status, last_seq, active_worker_id, lease_epoch, created_at_ms, updated_at_ms
			FROM sessions WHERE agent_id = $1
			ORDER BY updated_at_ms DESC, id DESC LIMIT $2
		`, agentID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, agent_id, title, status, last_seq, active_worker_id, lease_epoch, created_at_ms, updated_at_ms
			FROM sessions
			ORDER BY updated_at_ms DESC, id DESC LIMIT $1
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []runtime.Session
	for rows.Next() {
		var sess runtime.Session
		if err := rows.Scan(
			&sess.ID, &sess.AgentID, &sess.Title, &sess.Status, &sess.LastSeq,
			&sess.ActiveWorkerID, &sess.LeaseEpoch, &sess.CreatedAtMs, &sess.UpdatedAtMs,
		); err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

func (s *Store) GetSession(ctx context.Context, sessionID string) (runtime.Session, []runtime.Event, error) {
	var sess runtime.Session
	err := s.pool.QueryRow(ctx, `
		SELECT id, agent_id, title, status, last_seq, active_worker_id, lease_epoch, created_at_ms, updated_at_ms
		FROM sessions WHERE id = $1
	`, sessionID).Scan(
		&sess.ID, &sess.AgentID, &sess.Title, &sess.Status, &sess.LastSeq,
		&sess.ActiveWorkerID, &sess.LeaseEpoch, &sess.CreatedAtMs, &sess.UpdatedAtMs,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.Session{}, nil, runtime.ErrNotFound
	}
	if err != nil {
		return runtime.Session{}, nil, err
	}
	evs, err := s.ListEvents(ctx, sessionID, 0)
	if err != nil {
		return runtime.Session{}, nil, err
	}
	return sess, evs, nil
}

func (s *Store) ListEvents(ctx context.Context, sessionID string, sinceSeq int64) ([]runtime.Event, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sessions WHERE id = $1)`, sessionID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, runtime.ErrNotFound
	}
	return queryEvents(ctx, s.pool, sessionID, sinceSeq)
}

func (s *Store) SubmitUserMessage(ctx context.Context, sessionID, text string) (string, runtime.Event, runtime.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}
	defer tx.Rollback(ctx)

	var (
		agentID string
		title   string
		status  string
		lastSeq int64
	)
	err = tx.QueryRow(ctx, `
		SELECT agent_id, title, status, last_seq FROM sessions WHERE id = $1 FOR UPDATE
	`, sessionID).Scan(&agentID, &title, &status, &lastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", runtime.Event{}, runtime.Task{}, runtime.ErrNotFound
	}
	if err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}
	if status != runtime.SessionStatusIdle {
		return "", runtime.Event{}, runtime.Task{}, runtime.ErrSessionBusy
	}

	now := s.nowMs()
	turnID := uuid.NewString()
	payload, _ := json.Marshal(map[string]string{"text": text})
	ev := runtime.Event{
		ID:        uuid.NewString(),
		Seq:       lastSeq + 1,
		SessionID: sessionID,
		TurnID:    turnID,
		Type:      "user",
		Payload:   payload,
		TsMs:      now,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO events (id, session_id, seq, turn_id, type, payload, ts_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, ev.ID, ev.SessionID, ev.Seq, ev.TurnID, ev.Type, ev.Payload, ev.TsMs); err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}

	if title == "" {
		runes := []rune(text)
		if len(runes) > 40 {
			runes = runes[:40]
		}
		title = string(runes)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET title = $1, status = $2, last_seq = $3, updated_at_ms = $4 WHERE id = $5
	`, title, runtime.SessionStatusRunning, ev.Seq, now, sessionID); err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}

	task := runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     agentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, session_id, turn_id, agent_id, status, created_at_ms, updated_at_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
	`, task.ID, task.SessionID, task.TurnID, task.AgentID, task.Status, now); err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}

	if err := notifyEvents(ctx, tx, sessionID, ev.Seq); err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", runtime.Event{}, runtime.Task{}, err
	}
	return turnID, ev, task, nil
}

func (s *Store) ResolveApproval(ctx context.Context, sessionID, toolCallID string, approved bool, reason string) (runtime.Event, runtime.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	defer tx.Rollback(ctx)

	var (
		agentID string
		status  string
		lastSeq int64
	)
	err = tx.QueryRow(ctx, `
		SELECT agent_id, status, last_seq FROM sessions WHERE id = $1 FOR UPDATE
	`, sessionID).Scan(&agentID, &status, &lastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.Event{}, runtime.Task{}, runtime.ErrNotFound
	}
	if err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	if status != runtime.SessionStatusWaitingApproval {
		return runtime.Event{}, runtime.Task{}, runtime.ErrNotWaitingReview
	}

	var turnID string
	err = tx.QueryRow(ctx, `
		SELECT turn_id FROM events WHERE session_id = $1 ORDER BY seq DESC LIMIT 1
	`, sessionID).Scan(&turnID)
	if errors.Is(err, pgx.ErrNoRows) {
		turnID = uuid.NewString()
	} else if err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}

	now := s.nowMs()
	payload, _ := json.Marshal(map[string]any{
		"toolCallId": toolCallID,
		"approved":   approved,
		"reason":     reason,
	})
	ev := runtime.Event{
		ID:        uuid.NewString(),
		Seq:       lastSeq + 1,
		SessionID: sessionID,
		TurnID:    turnID,
		Type:      "approval-resolved",
		Payload:   payload,
		TsMs:      now,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO events (id, session_id, seq, turn_id, type, payload, ts_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, ev.ID, ev.SessionID, ev.Seq, ev.TurnID, ev.Type, ev.Payload, ev.TsMs); err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET status = $1, last_seq = $2, updated_at_ms = $3 WHERE id = $4
	`, runtime.SessionStatusRunning, ev.Seq, now, sessionID); err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}

	task := runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     agentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, session_id, turn_id, agent_id, status, created_at_ms, updated_at_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
	`, task.ID, task.SessionID, task.TurnID, task.AgentID, task.Status, now); err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	if err := notifyEvents(ctx, tx, sessionID, ev.Seq); err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return runtime.Event{}, runtime.Task{}, err
	}
	return ev, task, nil
}

func (s *Store) ResumeSession(ctx context.Context, sessionID string) (runtime.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return runtime.Task{}, err
	}
	defer tx.Rollback(ctx)

	var agentID, status string
	err = tx.QueryRow(ctx, `
		SELECT agent_id, status FROM sessions WHERE id = $1 FOR UPDATE
	`, sessionID).Scan(&agentID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.Task{}, runtime.ErrNotFound
	}
	if err != nil {
		return runtime.Task{}, err
	}
	if status != runtime.SessionStatusIdle {
		return runtime.Task{}, runtime.ErrSessionBusy
	}

	var turnID string
	err = tx.QueryRow(ctx, `
		SELECT turn_id FROM events WHERE session_id = $1 ORDER BY seq DESC LIMIT 1
	`, sessionID).Scan(&turnID)
	if errors.Is(err, pgx.ErrNoRows) {
		turnID = uuid.NewString()
	} else if err != nil {
		return runtime.Task{}, err
	}

	now := s.nowMs()
	if _, err := tx.Exec(ctx, `UPDATE sessions SET status = $1, updated_at_ms = $2 WHERE id = $3`,
		runtime.SessionStatusRunning, now, sessionID); err != nil {
		return runtime.Task{}, err
	}
	task := runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     agentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, session_id, turn_id, agent_id, status, created_at_ms, updated_at_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
	`, task.ID, task.SessionID, task.TurnID, task.AgentID, task.Status, now); err != nil {
		return runtime.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return runtime.Task{}, err
	}
	return task, nil
}

func (s *Store) CancelSession(ctx context.Context, sessionID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := s.nowMs()
	tag, err := tx.Exec(ctx, `
		UPDATE sessions SET status = $1, active_worker_id = '', updated_at_ms = $2 WHERE id = $3
	`, runtime.SessionStatusIdle, now, sessionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return runtime.ErrNotFound
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status = $1, updated_at_ms = $2
		WHERE session_id = $3 AND status IN ($4, $5)
	`, runtime.TaskStatusCancelled, now, sessionID, runtime.TaskStatusPending, runtime.TaskStatusLeased); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AcquireTask claims one pending or lease-expired task using FOR UPDATE SKIP LOCKED.
func (s *Store) AcquireTask(ctx context.Context, workerID string, leaseTTL time.Duration) (*runtime.AcquiredTask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	now := s.nowMs()
	var task runtime.Task
	err = tx.QueryRow(ctx, `
		SELECT id, session_id, turn_id, agent_id, lease_epoch, traceparent, created_at_ms
		FROM tasks
		WHERE status = $1
		   OR (status = $2 AND lease_expires_at_ms <= $3)
		ORDER BY created_at_ms ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, runtime.TaskStatusPending, runtime.TaskStatusLeased, now).Scan(
		&task.ID, &task.SessionID, &task.TurnID, &task.AgentID, &task.Lease.Epoch, &task.Traceparent, &task.CreatedAtMs,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	task.Status = runtime.TaskStatusLeased
	task.WorkerID = workerID
	task.Lease.LeaseID = uuid.NewString()
	task.Lease.Epoch++
	task.Lease.ExpiresAtMs = now + leaseTTL.Milliseconds()
	task.UpdatedAtMs = now

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = $1, worker_id = $2, lease_id = $3, lease_epoch = $4, lease_expires_at_ms = $5, updated_at_ms = $6
		WHERE id = $7
	`, task.Status, task.WorkerID, task.Lease.LeaseID, task.Lease.Epoch, task.Lease.ExpiresAtMs, now, task.ID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions
		SET status = $1, active_worker_id = $2, lease_epoch = $3, updated_at_ms = $4
		WHERE id = $5
	`, runtime.SessionStatusRunning, workerID, task.Lease.Epoch, now, task.SessionID); err != nil {
		return nil, err
	}

	evs, err := queryEvents(ctx, tx, task.SessionID, 0)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &runtime.AcquiredTask{Task: task, Events: evs}, nil
}

func (s *Store) RenewLease(ctx context.Context, taskID, leaseID string, epoch int64, leaseTTL time.Duration) (runtime.Lease, error) {
	now := s.nowMs()
	expiresAt := now + leaseTTL.Milliseconds()
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET lease_expires_at_ms = $1, updated_at_ms = $2
		WHERE id = $3 AND status = $4 AND lease_id = $5 AND lease_epoch = $6
	`, expiresAt, now, taskID, runtime.TaskStatusLeased, leaseID, epoch)
	if err != nil {
		return runtime.Lease{}, err
	}
	if tag.RowsAffected() == 0 {
		return runtime.Lease{}, runtime.ErrStaleEpoch
	}
	return runtime.Lease{LeaseID: leaseID, Epoch: epoch, ExpiresAtMs: expiresAt}, nil
}

// AppendEvents enforces Write-Ahead monotonic seq + lease/epoch fencing inside one ACID transaction.
func (s *Store) AppendEvents(ctx context.Context, sessionID, leaseID string, epoch int64, events []runtime.Event) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var lastSeq int64
	err = tx.QueryRow(ctx, `SELECT last_seq FROM sessions WHERE id = $1 FOR UPDATE`, sessionID).Scan(&lastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, runtime.ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	now := s.nowMs()
	var (
		activeLeaseID string
		activeEpoch   int64
		expiresAtMs   int64
	)
	err = tx.QueryRow(ctx, `
		SELECT lease_id, lease_epoch, lease_expires_at_ms
		FROM tasks
		WHERE session_id = $1 AND status = $2
		FOR UPDATE
	`, sessionID, runtime.TaskStatusLeased).Scan(&activeLeaseID, &activeEpoch, &expiresAtMs)
	if errors.Is(err, pgx.ErrNoRows) || activeLeaseID != leaseID || activeEpoch != epoch || expiresAtMs <= now {
		return 0, runtime.ErrStaleEpoch
	}
	if err != nil {
		return 0, err
	}

	nextSeq := lastSeq
	for _, ev := range events {
		nextSeq++
		if ev.Seq != 0 && ev.Seq != nextSeq {
			return 0, runtime.ErrSeqConflict
		}
		if ev.ID == "" {
			ev.ID = uuid.NewString()
		}
		if ev.TsMs == 0 {
			ev.TsMs = now
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO events (id, session_id, seq, turn_id, type, payload, ts_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, ev.ID, sessionID, nextSeq, ev.TurnID, ev.Type, ev.Payload, ev.TsMs); err != nil {
			return 0, err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET last_seq = $1, updated_at_ms = $2 WHERE id = $3
	`, nextSeq, now, sessionID); err != nil {
		return 0, err
	}
	if err := notifyEvents(ctx, tx, sessionID, nextSeq); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return nextSeq, nil
}

func (s *Store) CompleteTask(ctx context.Context, taskID, leaseID string, epoch int64) error {
	return s.finishTask(ctx, taskID, leaseID, epoch, runtime.TaskStatusCompleted, runtime.SessionStatusIdle, "")
}

func (s *Store) SuspendTask(ctx context.Context, taskID, leaseID string, epoch int64, reason string) error {
	return s.finishTask(ctx, taskID, leaseID, epoch, runtime.TaskStatusSuspended, runtime.SessionStatusWaitingApproval, reason)
}

func (s *Store) FailTask(ctx context.Context, taskID, leaseID string, epoch int64, errMsg string, retryable bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := s.nowMs()
	var sessionID string
	if retryable {
		err = tx.QueryRow(ctx, `
			UPDATE tasks
			SET status = $1, worker_id = '', lease_id = '', lease_expires_at_ms = 0, error = $2, updated_at_ms = $3
			WHERE id = $4 AND status = $5 AND lease_id = $6 AND lease_epoch = $7
			RETURNING session_id
		`, runtime.TaskStatusPending, errMsg, now, taskID, runtime.TaskStatusLeased, leaseID, epoch).Scan(&sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return runtime.ErrStaleEpoch
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sessions SET active_worker_id = '', updated_at_ms = $1 WHERE id = $2
		`, now, sessionID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status = $1, error = $2, updated_at_ms = $3
		WHERE id = $4 AND status = $5 AND lease_id = $6 AND lease_epoch = $7
		RETURNING session_id
	`, runtime.TaskStatusFailed, errMsg, now, taskID, runtime.TaskStatusLeased, leaseID, epoch).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.ErrStaleEpoch
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET status = $1, active_worker_id = '', updated_at_ms = $2 WHERE id = $3
	`, runtime.SessionStatusIdle, now, sessionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) finishTask(ctx context.Context, taskID, leaseID string, epoch int64, taskStatus, sessionStatus, note string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := s.nowMs()
	var sessionID string
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status = $1, error = $2, updated_at_ms = $3
		WHERE id = $4 AND status = $5 AND lease_id = $6 AND lease_epoch = $7
		RETURNING session_id
	`, taskStatus, note, now, taskID, runtime.TaskStatusLeased, leaseID, epoch).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.ErrStaleEpoch
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET status = $1, active_worker_id = '', updated_at_ms = $2 WHERE id = $3
	`, sessionStatus, now, sessionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type eventQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryEvents(ctx context.Context, q eventQuerier, sessionID string, sinceSeq int64) ([]runtime.Event, error) {
	rows, err := q.Query(ctx, `
		SELECT id, seq, session_id, turn_id, type, payload, ts_ms
		FROM events
		WHERE session_id = $1 AND seq > $2
		ORDER BY seq ASC
	`, sessionID, sinceSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []runtime.Event
	for rows.Next() {
		var ev runtime.Event
		if err := rows.Scan(&ev.ID, &ev.Seq, &ev.SessionID, &ev.TurnID, &ev.Type, &ev.Payload, &ev.TsMs); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func notifyEvents(ctx context.Context, tx pgx.Tx, sessionID string, lastSeq int64) error {
	payload := fmt.Sprintf(`{"sessionId":%q,"lastSeq":%d}`, sessionID, lastSeq)
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, payload)
	return err
}
