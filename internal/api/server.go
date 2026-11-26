package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
)

// Server is the HTTP/Connect-RPC transport module for keplerd.
type Server struct {
	addr        string
	moduleNames func() []string
	logger      *slog.Logger
	mux         *http.ServeMux
	connect     *connect.Server
	mountOnce   sync.Once
	httpServer  *http.Server
	listener    net.Listener
}

func NewServer(addr string, moduleNames func() []string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		addr:        addr,
		moduleNames: moduleNames,
		logger:      logger,
		mux:         http.NewServeMux(),
		connect:     connect.NewServer(),
	}
	s.registerBaseRoutes()
	return s
}

func (s *Server) Name() string { return "api" }

// Mux exposes the underlying ServeMux so other modules can mount REST/SSE handlers.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// Connect exposes the shared Connect-RPC server for registering service handlers.
func (s *Server) Connect() *connect.Server { return s.connect }

// MountConnect mounts registered Connect-RPC procedures onto the HTTP mux once.
func (s *Server) MountConnect() {
	s.mountOnce.Do(func() {
		connecthttp.Mount(s.mux, s.connect)
	})
}

// Addr returns the actual bound address after Start (useful when configured with ":0").
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) Start(_ context.Context) error {
	s.MountConnect()
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.httpServer = &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http server stopped unexpectedly", "error", err)
		}
	}()
	s.logger.Info("keplerd listening", "addr", ln.Addr().String())
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) registerBaseRoutes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /api/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		var modules []string
		if s.moduleNames != nil {
			modules = s.moduleNames()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"service": "keplerd",
			"status":  "running",
			"modules": modules,
		})
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
