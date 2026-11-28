package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/gateway"
	"github.com/KailBug/Kepler/internal/sandbox"
	"github.com/KailBug/Kepler/internal/sandbox/docker"
)

type stubSandbox struct {
	execFn   func(ctx context.Context, req sandbox.ExecRequest) (sandbox.ExecResult, error)
	released atomic.Int32
}

func (s *stubSandbox) Acquire(_ context.Context, spec sandbox.Spec) (sandbox.Lease, error) {
	return sandbox.Lease{ID: "stub-sb-1", Runtime: spec.Runtime, CreatedAt: time.Now()}, nil
}

func (s *stubSandbox) Exec(ctx context.Context, _ sandbox.Lease, req sandbox.ExecRequest) (sandbox.ExecResult, error) {
	return s.execFn(ctx, req)
}

func (s *stubSandbox) SyncWorkspace(_ context.Context, _ sandbox.Lease, _ string) error { return nil }

func (s *stubSandbox) Release(_ context.Context, _ sandbox.Lease) error {
	s.released.Add(1)
	return nil
}

func TestPythonExecToolUnit(t *testing.T) {
	sb := &stubSandbox{
		execFn: func(_ context.Context, req sandbox.ExecRequest) (sandbox.ExecResult, error) {
			if req.Stdin == "timeout" {
				return sandbox.ExecResult{Stdout: "partial"}, sandbox.ErrTimeout
			}
			if req.Stdin == "fail" {
				return sandbox.ExecResult{ExitCode: 1, Stderr: "ZeroDivisionError"}, nil
			}
			return sandbox.ExecResult{ExitCode: 0, Stdout: "42\n"}, nil
		},
	}

	gw := gateway.New(gateway.NewMemoryStore())
	if err := gw.Register(gateway.NewPythonExecTool(sb, sandbox.DefaultSpec("runc"))); err != nil {
		t.Fatalf("Register python_exec: %v", err)
	}

	// 1. Success
	res, err := gw.CallTool(context.Background(), "python_exec", gateway.CallContext{SessionID: "s1"}, json.RawMessage(`{"code":"print(42)"}`))
	if err != nil || res.IsError || res.Content != "42" {
		t.Fatalf("python_exec ok: res=%+v err=%v", res, err)
	}

	// 2. Non-zero exit code returns tool error (IsError=true) without RPC failure
	res, err = gw.CallTool(context.Background(), "python_exec", gateway.CallContext{SessionID: "s1"}, json.RawMessage(`{"code":"fail"}`))
	if err != nil || !res.IsError || !strings.Contains(res.Content, "ZeroDivisionError") {
		t.Fatalf("python_exec fail: res=%+v err=%v", res, err)
	}

	// 3. Timeout returns bounded error result and still releases sandbox
	res, err = gw.CallTool(context.Background(), "python_exec", gateway.CallContext{SessionID: "s1"}, json.RawMessage(`{"code":"timeout","timeout_seconds":2}`))
	if err != nil || !res.IsError || !strings.Contains(res.Content, "timed out") {
		t.Fatalf("python_exec timeout: res=%+v err=%v", res, err)
	}
	if sb.released.Load() != 3 {
		t.Fatalf("sandbox released %d times, want 3", sb.released.Load())
	}
}

func TestCredentialProxyToolIsolationAndIdempotency(t *testing.T) {
	const secret = "kepler-prod-secret-token-7788"

	var upstreamHits atomic.Int32
	var receivedAuth string
	var receivedIdemKey string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		receivedAuth = r.Header.Get("Authorization")
		receivedIdemKey = r.Header.Get("Idempotency-Key")
		// Intentionally echo the received auth header back to prove Gateway redacts leaks!
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"echoAuth":"` + receivedAuth + `"}`))
	}))
	defer upstream.Close()

	gw := gateway.New(gateway.NewMemoryStore())
	if err := gateway.RegisterCredentialProxyTool(gw, gateway.CredentialProxyOptions{
		RequireApproval: true,
		Bindings: []gateway.CredentialBinding{{
			Name:           "billing_api",
			BaseURL:        upstream.URL,
			Secret:         secret,
			AllowedMethods: []string{"POST"},
		}},
	}); err != nil {
		t.Fatalf("RegisterCredentialProxyTool: %v", err)
	}

	callCtx := gateway.CallContext{
		SessionID:      "sess-cred",
		TurnID:         "turn-1",
		ToolCallID:     "call-1",
		IdempotencyKey: "sess-cred:call-1",
	}
	args := json.RawMessage(`{"target":"billing_api","method":"POST","path":"/v1/charges","body":{"amount":99}}`)

	// 1. Upstream receives real Bearer secret, but Agent response MUST scrub it to [REDACTED].
	res1, err := gw.CallTool(context.Background(), "http_proxy", callCtx, args)
	if err != nil || res1.IsError {
		t.Fatalf("http_proxy call 1: res=%+v err=%v", res1, err)
	}
	if receivedAuth != "Bearer "+secret {
		t.Fatalf("upstream auth = %q, want Bearer secret", receivedAuth)
	}
	if receivedIdemKey != "sess-cred:call-1" {
		t.Fatalf("upstream Idempotency-Key = %q, want sess-cred:call-1", receivedIdemKey)
	}
	if strings.Contains(res1.Content, secret) || !strings.Contains(res1.Content, "[REDACTED]") {
		t.Fatalf("secret leaked in agent tool output: %q", res1.Content)
	}

	// 2. Duplicate call with same Idempotency-Key returns cached output; upstream hit count stays 1!
	res2, err := gw.CallTool(context.Background(), "http_proxy", callCtx, args)
	if err != nil || res2.Content != res1.Content || upstreamHits.Load() != 1 {
		t.Fatalf("idempotency failed: hits=%d res2=%+v err=%v", upstreamHits.Load(), res2, err)
	}

	// 3. SSRF / host-escape path and disallowed method MUST be blocked before hitting upstream.
	for _, badArgs := range []string{
		`{"target":"billing_api","method":"GET","path":"/v1/charges"}`,
		`{"target":"billing_api","method":"POST","path":"//evil.example/steal"}`,
		`{"target":"billing_api","method":"POST","path":"http://evil.example/steal"}`,
		`{"target":"unknown_svc","method":"POST","path":"/v1/charges"}`,
	} {
		badRes, err := gw.CallTool(context.Background(), "http_proxy", gateway.CallContext{SessionID: "sess-cred"}, json.RawMessage(badArgs))
		if err != nil || !badRes.IsError {
			t.Fatalf("expected blocked call for %s, got res=%+v err=%v", badArgs, badRes, err)
		}
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("blocked requests should not hit upstream, got %d hits", upstreamHits.Load())
	}

	// 4. Audit logs must NEVER contain the plaintext secret.
	logs, err := gw.ListAuditLogs(context.Background(), gateway.AuditFilter{SessionID: "sess-cred"})
	if err != nil || len(logs) == 0 {
		t.Fatalf("ListAuditLogs: len=%d err=%v", len(logs), err)
	}
	for _, entry := range logs {
		if strings.Contains(entry.OutputPreview, secret) || strings.Contains(string(entry.Arguments), secret) || strings.Contains(entry.Error, secret) {
			t.Fatalf("plaintext secret leaked in audit log: %+v", entry)
		}
	}
}

func TestPythonExecToolRealSandboxIntegration(t *testing.T) {
	dockerCmd := os.Getenv("KEPLER_TEST_DOCKER_CMD")
	if dockerCmd == "" {
		t.Skip("set KEPLER_TEST_DOCKER_CMD to test python_exec tool against real Docker/gVisor")
	}
	rt := os.Getenv("KEPLER_TEST_SANDBOX_RUNTIME")
	if rt == "" {
		rt = "runsc"
	}

	provider := docker.New(docker.Options{
		CommandPrefix:  docker.ParseCommandPrefix(dockerCmd),
		DefaultRuntime: rt,
	})
	gw := gateway.New(gateway.NewMemoryStore())
	if err := gw.Register(gateway.NewPythonExecTool(provider, sandbox.DefaultSpec(rt))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	res, err := gw.CallTool(context.Background(), "python_exec", gateway.CallContext{
		SessionID:      "sess-py-real",
		IdempotencyKey: "sess-py-real:call-1",
	}, json.RawMessage(`{"code":"import math; print(math.factorial(10))"}`))
	if err != nil || res.IsError || res.Content != "3628800" {
		t.Fatalf("real python_exec failed: res=%+v err=%v", res, err)
	}
}
