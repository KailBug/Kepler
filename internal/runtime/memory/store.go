package memory

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/google/uuid"
)

// Store is an in-memory implementation of runtime.Store for fast unit tests.
type Store struct {
	mu       sync.Mutex
	now      func() time.Time
	sessions map[string]*runtime.Session
	events   map[string][]runtime.Event
	tasks    map[string]*runtime.Task
	taskSeq  []string
	workers  map[string]workerRecord
}

type workerRecord struct {
	reg         runtime.WorkerRegistration
	activeTasks int32
	heartbeatMs int64
	createdMs   int64
}

func New() *Store {
	return &Store{
		now:      time.Now,
		sessions: make(map[string]*runtime.Session),
		events:   make(map[string][]runtime.Event),
		tasks:    make(map[string]*runtime.Task),
		workers:  make(map[string]workerRecord),
	}
}

// WithClock overrides the clock (useful in tests for lease expiration).
func (s *Store) WithClock(fn func() time.Time) *Store {
	s.now = fn
	return s
}

func (s *Store) nowMs() int64 { return s.now().UnixMilli() }

func (s *Store) RegisterWorker(_ context.Context, reg runtime.WorkerRegistration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMs()
	rec := s.workers[reg.WorkerID]
	if rec.createdMs == 0 {
		rec.createdMs = now
	}
	rec.reg = reg
	rec.heartbeatMs = now
	s.workers[reg.WorkerID] = rec
	return nil
}

func (s *Store) HeartbeatWorker(_ context.Context, workerID string, activeTasks int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.workers[workerID]
	if !ok {
		return runtime.ErrNotFound
	}
	rec.activeTasks = activeTasks
	rec.heartbeatMs = s.nowMs()
	s.workers[workerID] = rec
	return nil
}

func (s *Store) CreateSession(_ context.Context, agentID string) (runtime.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMs()
	sess := &runtime.Session{
		ID:          uuid.NewString(),
		AgentID:     agentID,
		Status:      runtime.SessionStatusIdle,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	s.sessions[sess.ID] = sess
	return *sess, nil
}

func (s *Store) ListSessions(_ context.Context, agentID string, limit int) ([]runtime.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtime.Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		if agentID != "" && sess.AgentID != agentID {
			continue
		}
		out = append(out, *sess)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAtMs == out[j].UpdatedAtMs {
			return out[i].ID > out[j].ID
		}
		return out[i].UpdatedAtMs > out[j].UpdatedAtMs
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) GetSession(_ context.Context, sessionID string) (runtime.Session, []runtime.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return runtime.Session{}, nil, runtime.ErrNotFound
	}
	evs := append([]runtime.Event(nil), s.events[sessionID]...)
	return *sess, evs, nil
}

func (s *Store) ListEvents(_ context.Context, sessionID string, sinceSeq int64) ([]runtime.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[sessionID]; !ok {
		return nil, runtime.ErrNotFound
	}
	var out []runtime.Event
	for _, ev := range s.events[sessionID] {
		if ev.Seq > sinceSeq {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (s *Store) SubmitUserMessage(_ context.Context, sessionID, text string) (string, runtime.Event, runtime.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return "", runtime.Event{}, runtime.Task{}, runtime.ErrNotFound
	}
	if sess.Status != runtime.SessionStatusIdle {
		return "", runtime.Event{}, runtime.Task{}, runtime.ErrSessionBusy
	}

	now := s.nowMs()
	turnID := uuid.NewString()
	payload, _ := json.Marshal(map[string]string{"text": text})
	sess.LastSeq++
	ev := runtime.Event{
		ID:        uuid.NewString(),
		Seq:       sess.LastSeq,
		SessionID: sessionID,
		TurnID:    turnID,
		Type:      "user",
		Payload:   payload,
		TsMs:      now,
	}
	s.events[sessionID] = append(s.events[sessionID], ev)

	if sess.Title == "" {
		runes := []rune(text)
		if len(runes) > 40 {
			runes = runes[:40]
		}
		sess.Title = string(runes)
	}
	sess.Status = runtime.SessionStatusRunning
	sess.UpdatedAtMs = now

	task := &runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     sess.AgentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	s.tasks[task.ID] = task
	s.taskSeq = append(s.taskSeq, task.ID)
	return turnID, ev, *task, nil
}

func (s *Store) ResolveApproval(_ context.Context, sessionID, toolCallID string, approved bool, reason string) (runtime.Event, runtime.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return runtime.Event{}, runtime.Task{}, runtime.ErrNotFound
	}
	if sess.Status != runtime.SessionStatusWaitingApproval {
		return runtime.Event{}, runtime.Task{}, runtime.ErrNotWaitingReview
	}

	evs := s.events[sessionID]
	turnID := ""
	if len(evs) > 0 {
		turnID = evs[len(evs)-1].TurnID
	} else {
		turnID = uuid.NewString()
	}

	now := s.nowMs()
	payload, _ := json.Marshal(map[string]any{
		"toolCallId": toolCallID,
		"approved":   approved,
		"reason":     reason,
	})
	sess.LastSeq++
	ev := runtime.Event{
		ID:        uuid.NewString(),
		Seq:       sess.LastSeq,
		SessionID: sessionID,
		TurnID:    turnID,
		Type:      "approval-resolved",
		Payload:   payload,
		TsMs:      now,
	}
	s.events[sessionID] = append(s.events[sessionID], ev)
	sess.Status = runtime.SessionStatusRunning
	sess.UpdatedAtMs = now

	task := &runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     sess.AgentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	s.tasks[task.ID] = task
	s.taskSeq = append(s.taskSeq, task.ID)
	return ev, *task, nil
}

func (s *Store) ResumeSession(_ context.Context, sessionID string) (runtime.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return runtime.Task{}, runtime.ErrNotFound
	}
	if sess.Status != runtime.SessionStatusIdle {
		return runtime.Task{}, runtime.ErrSessionBusy
	}
	evs := s.events[sessionID]
	turnID := uuid.NewString()
	if len(evs) > 0 {
		turnID = evs[len(evs)-1].TurnID
	}
	now := s.nowMs()
	sess.Status = runtime.SessionStatusRunning
	sess.UpdatedAtMs = now
	task := &runtime.Task{
		ID:          uuid.NewString(),
		SessionID:   sessionID,
		TurnID:      turnID,
		AgentID:     sess.AgentID,
		Status:      runtime.TaskStatusPending,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}
	s.tasks[task.ID] = task
	s.taskSeq = append(s.taskSeq, task.ID)
	return *task, nil
}

func (s *Store) CancelSession(_ context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return runtime.ErrNotFound
	}
	now := s.nowMs()
	for _, task := range s.tasks {
		if task.SessionID == sessionID && (task.Status == runtime.TaskStatusPending || task.Status == runtime.TaskStatusLeased) {
			task.Status = runtime.TaskStatusCancelled
			task.UpdatedAtMs = now
		}
	}
	sess.Status = runtime.SessionStatusIdle
	sess.ActiveWorkerID = ""
	sess.UpdatedAtMs = now
	return nil
}

func (s *Store) AcquireTask(_ context.Context, workerID string, leaseTTL time.Duration) (*runtime.AcquiredTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMs()
	for _, id := range s.taskSeq {
		task := s.tasks[id]
		claimable := task.Status == runtime.TaskStatusPending ||
			(task.Status == runtime.TaskStatusLeased && task.Lease.ExpiresAtMs <= now)
		if !claimable {
			continue
		}
		task.Status = runtime.TaskStatusLeased
		task.WorkerID = workerID
		task.Lease = runtime.Lease{
			LeaseID:     uuid.NewString(),
			Epoch:       task.Lease.Epoch + 1,
			ExpiresAtMs: now + leaseTTL.Milliseconds(),
		}
		task.UpdatedAtMs = now

		if sess, ok := s.sessions[task.SessionID]; ok {
			sess.Status = runtime.SessionStatusRunning
			sess.ActiveWorkerID = workerID
			sess.LeaseEpoch = task.Lease.Epoch
			sess.UpdatedAtMs = now
		}
		evs := append([]runtime.Event(nil), s.events[task.SessionID]...)
		return &runtime.AcquiredTask{Task: *task, Events: evs}, nil
	}
	return nil, nil
}

func (s *Store) RenewLease(_ context.Context, taskID, leaseID string, epoch int64, leaseTTL time.Duration) (runtime.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok || task.Status != runtime.TaskStatusLeased || task.Lease.LeaseID != leaseID || task.Lease.Epoch != epoch {
		return runtime.Lease{}, runtime.ErrStaleEpoch
	}
	now := s.nowMs()
	task.Lease.ExpiresAtMs = now + leaseTTL.Milliseconds()
	task.UpdatedAtMs = now
	return task.Lease, nil
}

func (s *Store) AppendEvents(_ context.Context, sessionID, leaseID string, epoch int64, events []runtime.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return 0, runtime.ErrNotFound
	}
	now := s.nowMs()
	var activeTask *runtime.Task
	for _, task := range s.tasks {
		if task.SessionID == sessionID && task.Status == runtime.TaskStatusLeased {
			activeTask = task
			break
		}
	}
	if activeTask == nil || activeTask.Lease.LeaseID != leaseID || activeTask.Lease.Epoch != epoch || activeTask.Lease.ExpiresAtMs <= now {
		return 0, runtime.ErrStaleEpoch
	}

	nextSeq := sess.LastSeq
	staged := make([]runtime.Event, 0, len(events))
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
		ev.Seq = nextSeq
		ev.SessionID = sessionID
		staged = append(staged, ev)
	}
	s.events[sessionID] = append(s.events[sessionID], staged...)
	sess.LastSeq = nextSeq
	sess.UpdatedAtMs = now
	return nextSeq, nil
}

func (s *Store) CompleteTask(_ context.Context, taskID, leaseID string, epoch int64) error {
	return s.finishTask(taskID, leaseID, epoch, runtime.TaskStatusCompleted, runtime.SessionStatusIdle, "")
}

func (s *Store) SuspendTask(_ context.Context, taskID, leaseID string, epoch int64, reason string) error {
	return s.finishTask(taskID, leaseID, epoch, runtime.TaskStatusSuspended, runtime.SessionStatusWaitingApproval, reason)
}

func (s *Store) FailTask(_ context.Context, taskID, leaseID string, epoch int64, errMsg string, retryable bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok || task.Status != runtime.TaskStatusLeased || task.Lease.LeaseID != leaseID || task.Lease.Epoch != epoch {
		return runtime.ErrStaleEpoch
	}
	now := s.nowMs()
	task.Error = errMsg
	task.UpdatedAtMs = now
	if retryable {
		task.Status = runtime.TaskStatusPending
		task.WorkerID = ""
		task.Lease.LeaseID = ""
		task.Lease.ExpiresAtMs = 0
		if sess, ok := s.sessions[task.SessionID]; ok {
			sess.ActiveWorkerID = ""
			sess.UpdatedAtMs = now
		}
		return nil
	}
	task.Status = runtime.TaskStatusFailed
	if sess, ok := s.sessions[task.SessionID]; ok {
		sess.Status = runtime.SessionStatusIdle
		sess.ActiveWorkerID = ""
		sess.UpdatedAtMs = now
	}
	return nil
}

func (s *Store) finishTask(taskID, leaseID string, epoch int64, taskStatus, sessionStatus, note string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok || task.Status != runtime.TaskStatusLeased || task.Lease.LeaseID != leaseID || task.Lease.Epoch != epoch {
		return runtime.ErrStaleEpoch
	}
	now := s.nowMs()
	task.Status = taskStatus
	task.Error = note
	task.UpdatedAtMs = now
	if sess, ok := s.sessions[task.SessionID]; ok {
		sess.Status = sessionStatus
		sess.ActiveWorkerID = ""
		sess.UpdatedAtMs = now
	}
	return nil
}
