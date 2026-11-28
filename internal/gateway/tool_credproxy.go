package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	defaultProxyTimeout  = 15 * time.Second
	maxProxyResponseSize = 64 * 1024
)

// CredentialBinding binds a logical service name to an upstream BaseURL and a gateway-managed secret.
// The Agent only selects the target name and relative path; it never sees the plaintext secret
// and cannot redirect the credential to an arbitrary host.
type CredentialBinding struct {
	Name            string   `json:"name"`
	BaseURL         string   `json:"baseUrl"`
	HeaderName      string   `json:"headerName,omitempty"`   // default: Authorization
	HeaderPrefix    string   `json:"headerPrefix,omitempty"` // default: "Bearer "
	Secret          string   `json:"secret"`
	AllowedMethods  []string `json:"allowedMethods,omitempty"` // default: ["GET", "POST"]
	RequireApproval bool     `json:"requireApproval,omitempty"`
}

// CredentialProxyOptions configures the http_proxy credential-injecting MCP tool.
type CredentialProxyOptions struct {
	Bindings        []CredentialBinding
	HTTPClient      *http.Client
	RequireApproval bool // tool-level HITL hint (default true for external side-effecting calls)
}

// RegisterCredentialProxyTool registers the http_proxy tool on gw and automatically
// registers every bound secret with gw.AddSecret so outputs and audit logs are always redacted.
func RegisterCredentialProxyTool(gw *Gateway, opts CredentialProxyOptions) error {
	if gw == nil {
		return errors.New("gateway is required")
	}

	byName := make(map[string]CredentialBinding, len(opts.Bindings))
	targetNames := make([]string, 0, len(opts.Bindings))
	for _, b := range opts.Bindings {
		if b.Name == "" || b.BaseURL == "" {
			return fmt.Errorf("credential binding requires non-empty name and baseUrl")
		}
		parsed, err := url.Parse(b.BaseURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("credential binding %q has invalid baseUrl %q", b.Name, b.BaseURL)
		}
		if b.HeaderName == "" {
			b.HeaderName = "Authorization"
			if b.HeaderPrefix == "" {
				b.HeaderPrefix = "Bearer "
			}
		}
		if len(b.AllowedMethods) == 0 {
			b.AllowedMethods = []string{http.MethodGet, http.MethodPost}
		} else {
			upper := make([]string, len(b.AllowedMethods))
			for i, m := range b.AllowedMethods {
				upper[i] = strings.ToUpper(strings.TrimSpace(m))
			}
			b.AllowedMethods = upper
		}
		b.BaseURL = strings.TrimRight(b.BaseURL, "/")
		byName[b.Name] = b
		targetNames = append(targetNames, b.Name)
		if b.Secret != "" {
			gw.AddSecret(b.Secret)
		}
	}
	sort.Strings(targetNames)

	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultProxyTimeout}
	}

	schemaObj := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target": map[string]any{
				"type":        "string",
				"description": "网关托管凭证的目标服务名（可用目标：" + strings.Join(targetNames, ", ") + "）",
			},
			"method": map[string]any{
				"type":        "string",
				"enum":        []string{"GET", "POST", "PUT", "PATCH", "DELETE"},
				"description": "HTTP 请求方法",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "以 / 开头的相对路径，例如 /v1/items",
			},
			"body": map[string]any{
				"description": "可选的 JSON 请求体",
			},
		},
		"required": []string{"target", "method", "path"},
	}
	schemaRaw, _ := json.Marshal(schemaObj)

	return gw.Register(Tool{
		Metadata: ToolMetadata{
			Name:            "http_proxy",
			Description:     "通过 MCP Gateway 向受控上游服务发起 HTTP 请求；业务密钥由网关自动注入，Agent 无法接触明文凭证。",
			InputSchema:     schemaRaw,
			RequireApproval: opts.RequireApproval,
		},
		Handler: func(ctx context.Context, call CallContext, rawArgs json.RawMessage) (CallResult, error) {
			var args struct {
				Target string          `json:"target"`
				Method string          `json:"method"`
				Path   string          `json:"path"`
				Body   json.RawMessage `json:"body,omitempty"`
			}
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return CallResult{Content: fmt.Sprintf("invalid arguments: %v", err), IsError: true}, nil
			}

			binding, ok := byName[args.Target]
			if !ok {
				return CallResult{Content: fmt.Sprintf("unknown target %q (allowed: %s)", args.Target, strings.Join(targetNames, ", ")), IsError: true}, nil
			}
			method := strings.ToUpper(strings.TrimSpace(args.Method))
			if !slices.Contains(binding.AllowedMethods, method) {
				return CallResult{Content: fmt.Sprintf("method %s is not allowed for target %q", method, args.Target), IsError: true}, nil
			}
			fullURL, err := resolveSafeTargetURL(binding.BaseURL, args.Path)
			if err != nil {
				return CallResult{Content: err.Error(), IsError: true}, nil
			}

			var reqBody io.Reader
			if len(args.Body) > 0 && string(bytes.TrimSpace(args.Body)) != "null" {
				reqBody = bytes.NewReader(args.Body)
			}
			req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
			if err != nil {
				return CallResult{Content: fmt.Sprintf("build request: %v", err), IsError: true}, nil
			}
			if reqBody != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			// Inject gateway-managed credential ONLY on the server side.
			if binding.Secret != "" {
				req.Header.Set(binding.HeaderName, binding.HeaderPrefix+binding.Secret)
			}
			if call.IdempotencyKey != "" {
				req.Header.Set("Idempotency-Key", call.IdempotencyKey)
			}

			resp, err := client.Do(req)
			if err != nil {
				return CallResult{Content: fmt.Sprintf("upstream request failed: %v", err), IsError: true}, nil
			}
			defer resp.Body.Close()

			limited, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyResponseSize+1))
			if err != nil {
				return CallResult{Content: fmt.Sprintf("read upstream response: %v", err), IsError: true}, nil
			}
			truncated := len(limited) > maxProxyResponseSize
			if truncated {
				limited = limited[:maxProxyResponseSize]
			}
			respText := fmt.Sprintf("HTTP %d\n%s", resp.StatusCode, strings.TrimSpace(string(limited)))
			if truncated {
				respText += "\n...[truncated]"
			}
			return CallResult{
				Content: respText,
				IsError: resp.StatusCode >= 400,
			}, nil
		},
	})
}

// resolveSafeTargetURL ensures the relative path cannot escape the bound upstream host/scheme (prevents SSRF).
func resolveSafeTargetURL(baseURL, relPath string) (string, error) {
	if !strings.HasPrefix(relPath, "/") || strings.HasPrefix(relPath, "//") {
		return "", fmt.Errorf("path must start with a single '/': %q", relPath)
	}
	if strings.Contains(relPath, "@") || strings.Contains(relPath, "\\") {
		return "", fmt.Errorf("invalid characters in path: %q", relPath)
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	joined, err := url.Parse(baseURL + relPath)
	if err != nil {
		return "", fmt.Errorf("invalid path %q: %w", relPath, err)
	}
	if joined.Scheme != base.Scheme || joined.Host != base.Host {
		return "", fmt.Errorf("path %q attempts to escape target host %q", relPath, base.Host)
	}
	return joined.String(), nil
}
