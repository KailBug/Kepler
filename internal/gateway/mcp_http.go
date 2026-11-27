package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	mcpProtocolVersion = "2025-03-26"
	mcpServerName      = "kepler-mcp-gateway"
	mcpServerVersion   = "0.1.0"
)

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MCPHandler returns an http.Handler implementing the MCP Streamable HTTP transport (JSON-RPC 2.0).
func (g *Gateway) MCPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			// Client closing MCP session.
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodGet:
			// Standalone server-push SSE stream is optional in MCP Streamable HTTP;
			// 405 tells @modelcontextprotocol/sdk to stay on request/response POST.
			w.Header().Set("Allow", "POST, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		case http.MethodPost:
		default:
			w.Header().Set("Allow", "POST, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			writeRPCError(w, http.StatusBadRequest, nil, -32700, "failed to read request body")
			return
		}

		var req jsonRPCRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeRPCError(w, http.StatusBadRequest, nil, -32700, "invalid JSON-RPC payload")
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, http.StatusBadRequest, req.ID, -32600, "invalid JSON-RPC 2.0 request")
			return
		}

		// JSON-RPC notifications have no "id" and expect 202 Accepted per MCP Streamable HTTP spec.
		if len(req.ID) == 0 || string(req.ID) == "null" {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		switch req.Method {
		case "initialize":
			var params struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(req.Params, &params)
			protoVer := mcpProtocolVersion
			if params.ProtocolVersion != "" {
				protoVer = params.ProtocolVersion
			}
			w.Header().Set("Mcp-Session-Id", uuid.NewString())
			writeRPCResult(w, req.ID, map[string]any{
				"protocolVersion": protoVer,
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]string{
					"name":    mcpServerName,
					"version": mcpServerVersion,
				},
			})

		case "ping":
			writeRPCResult(w, req.ID, map[string]any{})

		case "tools/list":
			tools := g.ListTools()
			wireTools := make([]map[string]any, len(tools))
			for i, t := range tools {
				item := map[string]any{
					"name":        t.Name,
					"description": t.Description,
					"inputSchema": t.InputSchema,
				}
				if t.RequireApproval {
					item["annotations"] = map[string]any{
						"destructiveHint": true,
					}
					item["_meta"] = map[string]any{
						"requireApproval": true,
					}
				}
				wireTools[i] = item
			}
			writeRPCResult(w, req.ID, map[string]any{"tools": wireTools})

		case "tools/call":
			var params struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
				Meta      struct {
					IdempotencyKey string `json:"idempotencyKey"`
					SessionID      string `json:"sessionId"`
					TurnID         string `json:"turnId"`
					ToolCallID     string `json:"toolCallId"`
				} `json:"_meta"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
				writeRPCError(w, http.StatusOK, req.ID, -32602, "tools/call requires a valid tool name")
				return
			}

			callCtx := CallContext{
				IdempotencyKey: firstNonEmpty(r.Header.Get("Idempotency-Key"), params.Meta.IdempotencyKey),
				SessionID:      firstNonEmpty(r.Header.Get("X-Kepler-Session-Id"), params.Meta.SessionID),
				TurnID:         firstNonEmpty(r.Header.Get("X-Kepler-Turn-Id"), params.Meta.TurnID),
				ToolCallID:     firstNonEmpty(r.Header.Get("X-Kepler-Tool-Call-Id"), params.Meta.ToolCallID),
			}

			res, err := g.CallTool(r.Context(), params.Name, callCtx, params.Arguments)
			if err != nil {
				switch {
				case errors.Is(err, ErrToolNotFound):
					writeRPCError(w, http.StatusOK, req.ID, -32602, err.Error())
				case errors.Is(err, ErrIdempotencyInProgress):
					writeRPCError(w, http.StatusConflict, req.ID, -32001, err.Error())
				default:
					writeRPCError(w, http.StatusOK, req.ID, -32000, err.Error())
				}
				return
			}

			writeRPCResult(w, req.ID, map[string]any{
				"content": []map[string]string{
					{"type": "text", "text": res.Content},
				},
				"isError": res.IsError,
			})

		default:
			writeRPCError(w, http.StatusOK, req.ID, -32601, "method not found: "+req.Method)
		}
	})
}

// AuditHandler returns an http.Handler serving GET /api/v1/audit?sessionId=&toolName=&limit=.
func (g *Gateway) AuditHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v <= 0 {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]string{"code": "bad_request", "message": "limit must be a positive integer"},
				})
				return
			}
			limit = v
		}
		entries, err := g.ListAuditLogs(r.Context(), AuditFilter{
			SessionID: r.URL.Query().Get("sessionId"),
			ToolName:  r.URL.Query().Get("toolName"),
			Limit:     limit,
		})
		if err != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]string{"code": "internal", "message": err.Error()},
			})
			return
		}
		if entries == nil {
			entries = []AuditEntry{}
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
	})
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	})
}

func writeRPCError(w http.ResponseWriter, httpStatus int, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &jsonRPCError{Code: code, Message: message},
	})
}
