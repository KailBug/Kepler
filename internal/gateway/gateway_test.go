package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KailBug/Kepler/internal/gateway"
)

func postRPC(t *testing.T, url string, headers map[string]string, body any) (*http.Response, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal rpc: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	var decoded map[string]any
	if resp.StatusCode == http.StatusOK || resp.StatusCode >= 400 {
		_ = json.NewDecoder(resp.Body).Decode(&decoded)
	}
	return resp, decoded
}

func TestMCPStreamableHTTPIdempotencyAndAudit(t *testing.T) {
	gw := gateway.New(gateway.NewMemoryStore())
	gw.AddSecret("sk-super-secret-9999")

	var executions atomic.Int32
	if err := gw.Register(gateway.Tool{
		Metadata: gateway.ToolMetadata{
			Name:            "charge_credit",
			Description:     "模拟有副作用的扣费工具",
			InputSchema:     json.RawMessage(`{"type":"object","properties":{"amount":{"type":"number"}},"required":["amount"]}`),
			RequireApproval: true,
		},
		Handler: func(_ context.Context, call gateway.CallContext, args json.RawMessage) (gateway.CallResult, error) {
			n := executions.Add(1)
			return gateway.CallResult{
				Content: "charged#" + string(rune('0'+n)) + " leak=sk-super-secret-9999 session=" + call.SessionID,
			}, nil
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/mcp", gw.MCPHandler())
	mux.Handle("GET /api/v1/audit", gw.AuditHandler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. initialize handshake
	resp, body := postRPC(t, ts.URL+"/mcp", nil, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]any{"protocolVersion": "2025-03-26"},
	})
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("initialize failed: status=%d headers=%v body=%v", resp.StatusCode, resp.Header, body)
	}

	// 2. notifications/initialized -> 202 Accepted
	resp, _ = postRPC(t, ts.URL+"/mcp", nil, map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("notification status = %d, want 202", resp.StatusCode)
	}

	// 3. tools/list includes requireApproval metadata
	_, listBody := postRPC(t, ts.URL+"/mcp", nil, map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
	})
	tools := listBody["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("expected 1 tool, got %v", tools)
	}
	toolMeta := tools[0].(map[string]any)
	if toolMeta["name"] != "charge_credit" || toolMeta["_meta"].(map[string]any)["requireApproval"] != true {
		t.Fatalf("unexpected tool metadata: %v", toolMeta)
	}

	// 4. First tools/call executes handler once and redacts secret
	headers := map[string]string{
		"Idempotency-Key":      "sess-1:call-1",
		"X-Kepler-Session-Id":  "sess-1",
		"X-Kepler-Turn-Id":     "turn-1",
		"X-Kepler-Tool-Call-Id": "call-1",
	}
	_, call1 := postRPC(t, ts.URL+"/mcp", headers, map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params":  map[string]any{"name": "charge_credit", "arguments": map[string]any{"amount": 100}},
	})
	text1 := call1["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if strings.Contains(text1, "sk-super-secret-9999") || !strings.Contains(text1, "[REDACTED]") {
		t.Fatalf("secret not redacted in tool output: %q", text1)
	}
	if executions.Load() != 1 {
		t.Fatalf("executions = %d, want 1", executions.Load())
	}

	// 5. Duplicate tools/call with same Idempotency-Key MUST return cached result without re-executing!
	_, call2 := postRPC(t, ts.URL+"/mcp", headers, map[string]any{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params":  map[string]any{"name": "charge_credit", "arguments": map[string]any{"amount": 100}},
	})
	text2 := call2["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if text2 != text1 || executions.Load() != 1 {
		t.Fatalf("idempotency failed: executions=%d text1=%q text2=%q", executions.Load(), text1, text2)
	}

	// 6. Audit logs contain both the "ok" and "cached" entries with zero plaintext secrets
	auditResp, err := http.Get(ts.URL + "/api/v1/audit?sessionId=sess-1")
	if err != nil || auditResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/audit: resp=%v err=%v", auditResp, err)
	}
	defer auditResp.Body.Close()
	var auditBody struct {
		Entries []gateway.AuditEntry `json:"entries"`
	}
	if err := json.NewDecoder(auditResp.Body).Decode(&auditBody); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if len(auditBody.Entries) != 2 {
		t.Fatalf("expected 2 audit entries, got %d", len(auditBody.Entries))
	}
	if auditBody.Entries[0].Status != "cached" || auditBody.Entries[1].Status != "ok" {
		t.Fatalf("unexpected audit statuses: %+v", auditBody.Entries)
	}
	for _, e := range auditBody.Entries {
		if strings.Contains(e.OutputPreview, "sk-super-secret-9999") {
			t.Fatalf("secret leaked in audit entry: %+v", e)
		}
	}
}

func TestGatewayIdempotencyInProgressAndAbort(t *testing.T) {
	store := gateway.NewMemoryStore()
	gw := gateway.New(store)

	started := make(chan struct{})
	release := make(chan struct{})
	_ = gw.Register(gateway.Tool{
		Metadata: gateway.ToolMetadata{Name: "slow_op"},
		Handler: func(_ context.Context, _ gateway.CallContext, _ json.RawMessage) (gateway.CallResult, error) {
			close(started)
			<-release
			return gateway.CallResult{Content: "done"}, nil
		},
	})

	done := make(chan error, 1)
	go func() {
		_, err := gw.CallTool(context.Background(), "slow_op", gateway.CallContext{IdempotencyKey: "k-concurrent"}, nil)
		done <- err
	}()
	<-started

	// Concurrent duplicate with same key must get ErrIdempotencyInProgress.
	if _, err := gw.CallTool(context.Background(), "slow_op", gateway.CallContext{IdempotencyKey: "k-concurrent"}, nil); !errors.Is(err, gateway.ErrIdempotencyInProgress) {
		t.Fatalf("expected ErrIdempotencyInProgress, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first call failed: %v", err)
	}
}
