package module

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type stubModule struct {
	name     string
	startErr error
	log      *[]string
}

func (s *stubModule) Name() string { return s.name }

func (s *stubModule) Start(context.Context) error {
	*s.log = append(*s.log, "start:"+s.name)
	return s.startErr
}

func (s *stubModule) Stop(context.Context) error {
	*s.log = append(*s.log, "stop:"+s.name)
	return nil
}

func TestRegistryLifecycleOrder(t *testing.T) {
	reg := NewRegistry()
	var log []string

	if err := reg.Register(&stubModule{name: "runtime", log: &log}); err != nil {
		t.Fatalf("register runtime: %v", err)
	}
	if err := reg.Register(&stubModule{name: "api", log: &log}); err != nil {
		t.Fatalf("register api: %v", err)
	}
	if err := reg.Register(&stubModule{name: "api", log: &log}); err == nil {
		t.Fatal("expected duplicate module registration error")
	}

	if err := reg.StartAll(context.Background()); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	if err := reg.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: %v", err)
	}

	want := []string{"start:runtime", "start:api", "stop:api", "stop:runtime"}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("lifecycle log = %v, want %v", log, want)
	}
}

func TestRegistryRollbackOnStartFailure(t *testing.T) {
	reg := NewRegistry()
	var log []string

	_ = reg.Register(&stubModule{name: "store", log: &log})
	_ = reg.Register(&stubModule{name: "gateway", startErr: errors.New("bind failed"), log: &log})
	_ = reg.Register(&stubModule{name: "api", log: &log})

	if err := reg.StartAll(context.Background()); err == nil {
		t.Fatal("expected StartAll error")
	}
	want := []string{"start:store", "start:gateway", "stop:store"}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("rollback log = %v, want %v", log, want)
	}
}
