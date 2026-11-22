package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBaseRoutes(t *testing.T) {
	srv := NewServer("127.0.0.1:0", func() []string { return []string{"api"} }, nil)

	t.Run("healthz", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		srv.Mux().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode json: %v", err)
		}
		if body["status"] != "ok" {
			t.Fatalf("body = %v, want status=ok", body)
		}
	})

	t.Run("status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
		rec := httptest.NewRecorder()
		srv.Mux().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body struct {
			Service string   `json:"service"`
			Status  string   `json:"status"`
			Modules []string `json:"modules"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode json: %v", err)
		}
		if body.Service != "keplerd" || len(body.Modules) != 1 || body.Modules[0] != "api" {
			t.Fatalf("unexpected status payload: %+v", body)
		}
	})

	// Also verify Stop is safe when Start was never called.
	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}
