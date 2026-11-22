package module

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Module represents a pluggable subsystem inside the keplerd modular monolith.
type Module interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Registry manages module registration, ordered startup, and reverse-order graceful shutdown.
type Registry struct {
	mu      sync.RWMutex
	modules []Module
	byName  map[string]Module
	started []Module
}

func NewRegistry() *Registry {
	return &Registry{
		byName: make(map[string]Module),
	}
}

// Register adds a module to the registry. Duplicate names are rejected.
func (r *Registry) Register(m Module) error {
	if m == nil {
		return errors.New("module cannot be nil")
	}
	name := m.Name()
	if name == "" {
		return errors.New("module name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("module %q already registered", name)
	}
	r.byName[name] = m
	r.modules = append(r.modules, m)
	return nil
}

// Names returns registered module names in startup order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, len(r.modules))
	for i, m := range r.modules {
		names[i] = m.Name()
	}
	return names
}

// StartAll starts modules in registration order. If any module fails to start,
// already-started modules are stopped in reverse order before returning the error.
func (r *Registry) StartAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.started = r.started[:0]
	for _, m := range r.modules {
		if err := m.Start(ctx); err != nil {
			rollbackErr := r.stopLocked(ctx)
			if rollbackErr != nil {
				return fmt.Errorf("start module %q: %w (rollback errors: %v)", m.Name(), err, rollbackErr)
			}
			return fmt.Errorf("start module %q: %w", m.Name(), err)
		}
		r.started = append(r.started, m)
	}
	return nil
}

// StopAll stops all started modules in reverse startup order.
func (r *Registry) StopAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopLocked(ctx)
}

func (r *Registry) stopLocked(ctx context.Context) error {
	var errs []error
	for i := len(r.started) - 1; i >= 0; i-- {
		m := r.started[i]
		if err := m.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop module %q: %w", m.Name(), err))
		}
	}
	r.started = r.started[:0]
	return errors.Join(errs...)
}
