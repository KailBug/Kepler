package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
)

// SessionRoutesOptions wires user-facing REST + SSE routes to the Runtime Store & Broadcaster.
type SessionRoutesOptions struct {
	Store         runtime.Store
	Broadcaster   *runtime.Broadcaster
	OnTaskCreated func()
	Heartbeat     time.Duration
}

// MountSessionRoutes registers user-facing HTTP + SSE endpoints aligned with @kepler/protocol:
//   - GET  /api/sessions
//   - POST /api/sessions
//   - GET  /api/sessions/{id}
//   - GET  /api/sessions/{id}/events?since=seq (SSE + Last-Event-ID replay)
//   - POST /api/sessions/{id}/messages
//   - POST /api/sessions/{id}/approvals/{toolCallId}
//   - POST /api/sessions/{id}/resume
//   - POST /api/sessions/{id}/cancel
func MountSessionRoutes(mux *http.ServeMux, opts SessionRoutesOptions) {
	bc := opts.Broadcaster
	if bc == nil {
		bc = runtime.NewBroadcaster()
	}
	heartbeat := opts.Heartbeat
	if heartbeat <= 0 {
		heartbeat = 15 * time.Second
	}
	notifyTask := func() {
		if opts.OnTaskCreated != nil {
			opts.OnTaskCreated()
		}
	}

	mux.HandleFunc("GET /api/sessions", func(w http.ResponseWriter, r *http.Request) {
		agentID := r.URL.Query().Get("agentId")
		limit := 0
		if raw := r.URL.Query().Get("limit"); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 {
				writeError(w, http.StatusBadRequest, "bad_request", "limit must be a non-negative integer")
				return
			}
			limit = v
		}
		sessions, err := opts.Store.ListSessions(r.Context(), agentID, limit)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		out := make([]wireSessionInfo, len(sessions))
		for i, s := range sessions {
			out[i] = toWireSession(s)
		}
		writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
	})

	mux.HandleFunc("POST /api/sessions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AgentID string `json:"agentId"`
		}
		if err := readOptionalJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		sess, err := opts.Store.CreateSession(r.Context(), req.AgentID)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"session": toWireSession(sess)})
	})

	mux.HandleFunc("GET /api/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		sess, evs, err := opts.Store.GetSession(r.Context(), id)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		wireEvents := make([]map[string]any, len(evs))
		for i, ev := range evs {
			wireEvents[i] = toWireEvent(ev)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"session": toWireSession(sess),
			"events":  wireEvents,
		})
	})

	mux.HandleFunc("POST /api/sessions/{id}/messages", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req struct {
			Text string `json:"text"`
		}
		if err := readOptionalJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if strings.TrimSpace(req.Text) == "" {
			writeError(w, http.StatusBadRequest, "bad_request", "text must be a non-empty string")
			return
		}
		turnID, _, _, err := opts.Store.SubmitUserMessage(r.Context(), id, req.Text)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		bc.Notify(id)
		notifyTask()
		writeJSON(w, http.StatusAccepted, map[string]string{"turnId": turnID})
	})

	mux.HandleFunc("POST /api/sessions/{id}/approvals/{toolCallId}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		toolCallID := r.PathValue("toolCallId")
		var req struct {
			Approved *bool  `json:"approved"`
			Reason   string `json:"reason"`
		}
		if err := readOptionalJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if req.Approved == nil {
			writeError(w, http.StatusBadRequest, "bad_request", "approved must be a boolean")
			return
		}
		if _, _, err := opts.Store.ResolveApproval(r.Context(), id, toolCallID, *req.Approved, req.Reason); err != nil {
			writeStoreError(w, err)
			return
		}
		bc.Notify(id)
		notifyTask()
		writeJSON(w, http.StatusAccepted, map[string]any{})
	})

	mux.HandleFunc("POST /api/sessions/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := opts.Store.ResumeSession(r.Context(), id); err != nil {
			writeStoreError(w, err)
			return
		}
		bc.Notify(id)
		notifyTask()
		writeJSON(w, http.StatusAccepted, map[string]any{})
	})

	mux.HandleFunc("POST /api/sessions/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := opts.Store.CancelSession(r.Context(), id); err != nil {
			writeStoreError(w, err)
			return
		}
		bc.Notify(id)
		writeJSON(w, http.StatusAccepted, map[string]any{})
	})

	mux.HandleFunc("GET /api/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		rawSince := r.URL.Query().Get("since")
		if rawSince == "" {
			rawSince = r.Header.Get("Last-Event-ID")
		}
		var since int64
		if rawSince != "" {
			v, err := strconv.ParseInt(rawSince, 10, 64)
			if err != nil || v < 0 {
				writeError(w, http.StatusBadRequest, "bad_request", "since must be a non-negative integer")
				return
			}
			since = v
		}

		// Subscribe BEFORE initial query so no event is missed between replay and live stream.
		notifyCh, unsub := bc.Subscribe(id)
		defer unsub()

		sess, _, err := opts.Store.GetSession(r.Context(), id)
		if err != nil {
			writeStoreError(w, err)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "internal", "streaming unsupported")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "retry: 2000\n\n")
		flusher.Flush()

		lastSeq := since
		lastStatus := ""
		writeFrame := func(event string, data any, idSeq *int64) error {
			raw, err := json.Marshal(data)
			if err != nil {
				return err
			}
			if idSeq != nil {
				if _, err := fmt.Fprintf(w, "id: %d\n", *idSeq); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, raw); err != nil {
				return err
			}
			flusher.Flush()
			return nil
		}

		flush := func(current *runtime.Session) error {
			if current == nil {
				s, _, err := opts.Store.GetSession(r.Context(), id)
				if err != nil {
					return err
				}
				current = &s
			}
			evs, err := opts.Store.ListEvents(r.Context(), id, lastSeq)
			if err != nil {
				return err
			}
			for _, ev := range evs {
				seq := ev.Seq
				if err := writeFrame("event", toWireEvent(ev), &seq); err != nil {
					return err
				}
				lastSeq = seq
			}
			if current.Status != lastStatus {
				lastStatus = current.Status
				if err := writeFrame("status", map[string]string{"status": current.Status}, nil); err != nil {
					return err
				}
			}
			return nil
		}

		if err := flush(&sess); err != nil {
			return
		}
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-notifyCh:
				if err := flush(nil); err != nil {
					return
				}
			case <-ticker.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})
}

// wireSessionInfo matches @kepler/protocol SessionInfo plus Kepler runtime fields.
type wireSessionInfo struct {
	ID             string `json:"id"`
	AgentID        string `json:"agentId,omitempty"`
	Title          string `json:"title"`
	Status         string `json:"status"`
	CreatedAt      int64  `json:"createdAt"`
	UpdatedAt      int64  `json:"updatedAt"`
	LastSeq        int64  `json:"lastSeq"`
	ActiveWorkerID string `json:"activeWorkerId,omitempty"`
	LeaseEpoch     int64  `json:"leaseEpoch,omitempty"`
}

func toWireSession(s runtime.Session) wireSessionInfo {
	return wireSessionInfo{
		ID:             s.ID,
		AgentID:        s.AgentID,
		Title:          s.Title,
		Status:         s.Status,
		CreatedAt:      s.CreatedAtMs,
		UpdatedAt:      s.UpdatedAtMs,
		LastSeq:        s.LastSeq,
		ActiveWorkerID: s.ActiveWorkerID,
		LeaseEpoch:     s.LeaseEpoch,
	}
}

// toWireEvent flattens payload fields into the top-level event object to match @kepler/protocol SessionEvent.
func toWireEvent(ev runtime.Event) map[string]any {
	out := make(map[string]any, 8)
	if len(ev.Payload) > 0 {
		_ = json.Unmarshal(ev.Payload, &out)
	}
	out["id"] = ev.ID
	out["seq"] = ev.Seq
	out["sessionId"] = ev.SessionID
	out["turnId"] = ev.TurnID
	out["type"] = ev.Type
	out["ts"] = ev.TsMs
	return out
}

func readOptionalJSON(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil
	}
	return json.Unmarshal([]byte(trimmed), dst)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, runtime.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, runtime.ErrSessionBusy), errors.Is(err, runtime.ErrNotWaitingReview), errors.Is(err, runtime.ErrStaleEpoch):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
	}
}
