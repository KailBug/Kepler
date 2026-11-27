package gateway

import (
	"context"
	"sync"
	"time"
)

type idempotencyRecord struct {
	status string
	result *CallResult
}

// MemoryStore is an in-memory implementation of Store for unit tests and KEPLER_STORE=memory mode.
type MemoryStore struct {
	mu    sync.Mutex
	idem  map[string]idempotencyRecord
	audit []AuditEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		idem: make(map[string]idempotencyRecord),
	}
}

func (s *MemoryStore) BeginIdempotency(_ context.Context, key, _, _, _ string) (*CallResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, exists := s.idem[key]; exists {
		if rec.status == "in_progress" {
			return nil, false, ErrIdempotencyInProgress
		}
		if rec.result != nil {
			cp := *rec.result
			return &cp, false, nil
		}
	}
	s.idem[key] = idempotencyRecord{status: "in_progress"}
	return nil, true, nil
}

func (s *MemoryStore) FinishIdempotency(_ context.Context, key string, result CallResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := result
	s.idem[key] = idempotencyRecord{status: "completed", result: &cp}
	return nil
}

func (s *MemoryStore) AbortIdempotency(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.idem[key]; ok && rec.status == "in_progress" {
		delete(s.idem, key)
	}
	return nil
}

func (s *MemoryStore) RecordAudit(_ context.Context, entry AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.CreatedAtMs == 0 {
		entry.CreatedAtMs = time.Now().UnixMilli()
	}
	s.audit = append(s.audit, entry)
	return nil
}

func (s *MemoryStore) ListAuditLogs(_ context.Context, filter AuditFilter) ([]AuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	var out []AuditEntry
	for i := len(s.audit) - 1; i >= 0; i-- {
		e := s.audit[i]
		if filter.SessionID != "" && e.SessionID != filter.SessionID {
			continue
		}
		if filter.ToolName != "" && e.ToolName != filter.ToolName {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
