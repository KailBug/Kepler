package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrStaleEpoch is returned when a worker attempts to write with an outdated lease epoch.
var ErrStaleEpoch = errors.New("stale lease epoch")

// Event mirrors kepler.v1.SessionEvent for storage backends (PostgreSQL / Memory).
type Event struct {
	ID        string          `json:"id"`
	Seq       int64           `json:"seq"`
	SessionID string          `json:"sessionId"`
	TurnID    string          `json:"turnId"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"ts"`
}

// EventStore is the pluggable persistence contract for Durable Runtime.
type EventStore interface {
	AppendEvents(ctx context.Context, sessionID, leaseID string, epoch int64, events []Event) (lastSeq int64, err error)
	ListEvents(ctx context.Context, sessionID string, sinceSeq int64) ([]Event, error)
}
