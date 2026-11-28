package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/KailBug/Kepler/internal/config"
)

func TestKeplerdAssemblesRuntimeGatewayAndAPI(t *testing.T) {
	const secret = "kepler-e2e-secret-4321"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"auth":"` + r.Header.Get("Authorization") + `"}`))
	}))
	defer upstream.Close()

	targets, _ := json.Marshal([]map[string]any{{
		"name":            "demo_svc",
		"baseUrl":         upstream.URL,
		"secret":          secret,
		"allowedMethods":  []string{"GET"},
		"requireApproval": true,
	}})

	cfg := config.Config{
		HTTPAddr:       "127.0.0.1:0",
		StoreBackend:   "memory",
		SandboxRuntime: "runc",
		SandboxImage:   "python:3.12-alpine",
		DockerCmd:      "docker",
		ProxyTargets:   string(targets),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg, srv, err := buildServer(cfg, logger)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	if err := reg.StartAll(context.Background()); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	t.Cleanup(func() {
		if err := reg.StopAll(context.Background()); err != nil {
			t.Errorf("StopAll: %v", err)
		}
	})

	baseURL := "http://" + srv.Addr()

	// 1. /api/v1/status lists all 3 modules in startup order
	resp, err := http.Get(baseURL + "/api/v1/status")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/status: resp=%v err=%v", resp, err)
	}
	var statusBody struct {
		Modules []string `json:"modules"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&statusBody)
	resp.Body.Close()
	if !slices.Equal(statusBody.Modules, []string{"runtime", "gateway", "api"}) {
		t.Fatalf("modules = %v, want [runtime gateway api]", statusBody.Modules)
	}

	// 2. POST /mcp tools/list exposes both python_exec and http_proxy
	listReq := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	resp, err = http.Post(baseURL+"/mcp", "application/json", strings.NewReader(listReq))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /mcp tools/list: resp=%v err=%v", resp, err)
	}
	var listBody struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&listBody)
	resp.Body.Close()
	var names []string
	for _, tool := range listBody.Result.Tools {
		names = append(names, tool.Name)
	}
	if !slices.Equal(names, []string{"http_proxy", "python_exec"}) {
		t.Fatalf("MCP tools = %v, want [http_proxy python_exec]", names)
	}

	// 3. POST /mcp tools/call on http_proxy injects secret, redacts output, and writes audit
	callPayload := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "http_proxy",
			"arguments": map[string]any{"target": "demo_svc", "method": "GET", "path": "/ping"},
		},
	}
	rawCall, _ := json.Marshal(callPayload)
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(rawCall))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "keplerd-test:1")
	req.Header.Set("X-Kepler-Session-Id", "s-keplerd")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /mcp tools/call: resp=%v err=%v", resp, err)
	}
	rawResp, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(rawResp), secret) || !strings.Contains(string(rawResp), "[REDACTED]") {
		t.Fatalf("expected redacted credential response, got %s", rawResp)
	}

	// 4. GET /api/v1/audit returns the recorded entry without leaking secret
	resp, err = http.Get(baseURL + "/api/v1/audit?sessionId=s-keplerd")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/audit: resp=%v err=%v", resp, err)
	}
	rawAudit, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(rawAudit), `"toolName":"http_proxy"`) || strings.Contains(string(rawAudit), secret) {
		t.Fatalf("unexpected audit response: %s", rawAudit)
	}
}
