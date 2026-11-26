package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/api"
	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/memory"
)

type sseFrame struct {
	ID    string
	Event string
	Data  map[string]any
}

func readSSEUntil(t *testing.T, url, lastEventID string, stop func(sseFrame) bool) []sseFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("SSE GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200", resp.StatusCode)
	}

	var frames []sseFrame
	var cur sseFrame
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if cur.Event != "" {
				frames = append(frames, cur)
				if stop(cur) {
					return frames
				}
				cur = sseFrame{}
			}
			continue
		}
		if strings.HasPrefix(line, "id: ") {
			cur.ID = strings.TrimPrefix(line, "id: ")
		} else if strings.HasPrefix(line, "event: ") {
			cur.Event = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			var m map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m); err != nil {
				t.Fatalf("unmarshal SSE data: %v", err)
			}
			cur.Data = m
		}
	}
	return frames
}

func TestSessionRESTAndSSE(t *testing.T) {
	store := memory.New()
	bc := runtime.NewBroadcaster()
	srv := api.NewServer("127.0.0.1:0", func() []string { return []string{"runtime", "api"} }, nil)
	api.MountSessionRoutes(srv.Mux(), api.SessionRoutesOptions{
		Store:       store,
		Broadcaster: bc,
	})
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// 1. POST /api/sessions
	resp, err := http.Post(ts.URL+"/api/sessions", "application/json", bytes.NewBufferString(`{"agentId":"demo"}`))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/sessions: resp=%v err=%v", resp, err)
	}
	var createBody struct {
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&createBody)
	resp.Body.Close()
	sessID := createBody.Session.ID

	// 2. POST /api/sessions/{id}/messages -> 202
	resp, err = http.Post(ts.URL+"/api/sessions/"+sessID+"/messages", "application/json", bytes.NewBufferString(`{"text":"hello sse"}`))
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST messages: resp=%v err=%v", resp, err)
	}
	resp.Body.Close()

	// 3. Simulate worker appending seq=2 while SSE client is connected with Last-Event-ID=1
	go func() {
		time.Sleep(40 * time.Millisecond)
		ctx := context.Background()
		acq, err := store.AcquireTask(ctx, "w1", 10*time.Second)
		if err != nil || acq == nil {
			return
		}
		_, _ = store.AppendEvents(ctx, sessID, acq.Task.Lease.LeaseID, acq.Task.Lease.Epoch, []runtime.Event{{
			TurnID:  acq.Task.TurnID,
			Type:    "assistant",
			Payload: json.RawMessage(`{"text":"world"}`),
		}})
		bc.Notify(sessID)
		_ = store.CompleteTask(ctx, acq.Task.ID, acq.Task.Lease.LeaseID, acq.Task.Lease.Epoch)
		bc.Notify(sessID)
	}()

	frames := readSSEUntil(t, ts.URL+"/api/sessions/"+sessID+"/events", "1", func(f sseFrame) bool {
		return f.Event == "status" && f.Data["status"] == "idle"
	})

	var gotAssistant bool
	for _, f := range frames {
		if f.Event == "event" {
			if f.ID == "1" {
				t.Fatalf("Last-Event-ID=1 should not replay seq 1, got %+v", f)
			}
			if f.ID == "2" && f.Data["type"] == "assistant" && f.Data["text"] == "world" {
				gotAssistant = true
			}
		}
	}
	if !gotAssistant {
		t.Fatalf("expected live assistant event seq=2, got frames: %+v", frames)
	}
}
