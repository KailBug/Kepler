package docker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/sandbox"
)

type recordedCall struct {
	args  []string
	stdin string
}

type fakeRunner struct {
	calls   []recordedCall
	handler func(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error)
}

func (f *fakeRunner) Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	var in string
	if stdin != nil {
		b, _ := io.ReadAll(stdin)
		in = string(b)
	}
	f.calls = append(f.calls, recordedCall{args: append([]string(nil), args...), stdin: in})
	if f.handler != nil {
		return f.handler(ctx, args, strings.NewReader(in), stdout, stderr)
	}
	return 0, nil
}

func TestProviderArgBuildingAndOutputBound(t *testing.T) {
	fake := &fakeRunner{
		handler: func(ctx context.Context, args []string, _ io.Reader, stdout, _ io.Writer) (int, error) {
			switch args[0] {
			case "run":
				_, _ = io.WriteString(stdout, "cid-12345\n")
				return 0, nil
			case "exec":
				_, _ = io.WriteString(stdout, strings.Repeat("A", 100))
				return 0, nil
			default:
				return 0, nil
			}
		},
	}

	p := New(Options{
		DefaultRuntime: "runsc",
		MaxOutputBytes: 32,
		Runner:         fake,
	})

	lease, err := p.Acquire(context.Background(), sandbox.DefaultSpec(""))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.ID != "cid-12345" || lease.Runtime != "runsc" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	runArgs := fake.calls[0].args
	for _, want := range []string{
		"--network=none",
		"--runtime=runsc",
		"--read-only",
		"--memory=256m",
		"--memory-swap=256m",
		"--cpus=0.500",
		"--pids-limit=64",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
	} {
		if !slices.Contains(runArgs, want) {
			t.Fatalf("runArgs missing %q: %v", want, runArgs)
		}
	}

	res, err := p.Exec(context.Background(), lease, sandbox.ExecRequest{
		Command: []string{"python3", "-"},
		Stdin:   "print('hi')",
		Env:     map[string]string{"B": "2", "A": "1"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !res.Truncated || !strings.HasSuffix(res.Stdout, "...[truncated]") {
		t.Fatalf("expected truncated output, got %+v", res)
	}
	if fake.calls[1].stdin != "print('hi')" {
		t.Fatalf("stdin not forwarded: %q", fake.calls[1].stdin)
	}
	if !slices.Contains(fake.calls[1].args, "A=1") || !slices.Contains(fake.calls[1].args, "B=2") {
		t.Fatalf("env args missing: %v", fake.calls[1].args)
	}

	if err := p.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !slices.Equal(fake.calls[2].args, []string{"rm", "-f", "cid-12345"}) {
		t.Fatalf("unexpected release args: %v", fake.calls[2].args)
	}
}

func TestProviderExecTimeout(t *testing.T) {
	fake := &fakeRunner{
		handler: func(ctx context.Context, _ []string, _ io.Reader, _ io.Writer, _ io.Writer) (int, error) {
			<-ctx.Done()
			return -1, ctx.Err()
		},
	}
	p := New(Options{Runner: fake})
	_, err := p.Exec(context.Background(), sandbox.Lease{ID: "cid-1"}, sandbox.ExecRequest{
		Command: []string{"sleep", "10"},
		Timeout: 30 * time.Millisecond,
	})
	if !errors.Is(err, sandbox.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

// TestDockerSandboxIntegration verifies real container isolation (runc and gVisor runsc).
// Enable via: KEPLER_TEST_DOCKER_CMD="wsl -e docker" (or "docker" inside Linux/WSL2)
// and optional KEPLER_TEST_SANDBOX_RUNTIME="runsc" (default tests both runc and runsc if available).
func TestDockerSandboxIntegration(t *testing.T) {
	dockerCmd := os.Getenv("KEPLER_TEST_DOCKER_CMD")
	if dockerCmd == "" {
		t.Skip("set KEPLER_TEST_DOCKER_CMD (e.g. 'wsl -e docker' or 'docker') to run real Docker/gVisor sandbox tests")
	}
	runtimes := []string{"runc", "runsc"}
	if rt := os.Getenv("KEPLER_TEST_SANDBOX_RUNTIME"); rt != "" {
		runtimes = []string{rt}
	}

	for _, rt := range runtimes {
		t.Run("runtime="+rt, func(t *testing.T) {
			ctx := context.Background()
			p := New(Options{
				CommandPrefix:  ParseCommandPrefix(dockerCmd),
				DefaultRuntime: rt,
			})

			lease, err := p.Acquire(ctx, sandbox.DefaultSpec(rt))
			if err != nil {
				t.Fatalf("Acquire(%s): %v", rt, err)
			}
			t.Cleanup(func() {
				if err := p.Release(context.Background(), lease); err != nil {
					t.Errorf("Release(%s): %v", rt, err)
				}
			})

			// 1. Basic Python execution via stdin
			res, err := p.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-"},
				Stdin:   "print(sum(range(1, 101)))",
				Timeout: 10 * time.Second,
			})
			if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "5050" {
				t.Fatalf("python exec failed: res=%+v err=%v", res, err)
			}

			// 2. SyncWorkspace copies local host files into /workspace
			tmpDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(tmpDir, "data.csv"), []byte("x\n10\n20\n"), 0o644); err != nil {
				t.Fatalf("write csv: %v", err)
			}
			if err := p.SyncWorkspace(ctx, lease, tmpDir); err != nil {
				t.Fatalf("SyncWorkspace: %v", err)
			}
			res, err = p.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-c", "print(open('/workspace/data.csv').read().strip().replace('\\n', ','))"},
				Timeout: 10 * time.Second,
			})
			if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "x,10,20" {
				t.Fatalf("workspace read failed: res=%+v err=%v", res, err)
			}

			// 3. Isolation check A: root filesystem MUST be read-only
			res, err = p.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-c", "open('/etc/kepler_escape', 'w').write('x')"},
				Timeout: 10 * time.Second,
			})
			if err != nil || res.ExitCode == 0 {
				t.Fatalf("expected read-only rootfs failure, got res=%+v err=%v", res, err)
			}

			// 4. Isolation check B: external network MUST be blocked (--network=none)
			res, err = p.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-c", "import socket; s=socket.create_connection(('1.1.1.1', 80), timeout=2)"},
				Timeout: 10 * time.Second,
			})
			if err != nil || res.ExitCode == 0 {
				t.Fatalf("expected network isolation failure, got res=%+v err=%v", res, err)
			}

			// 5. Isolation check C: host secrets/env MUST NOT leak into container
			res, err = p.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-c", "import os; print(os.environ.get('DEEPSEEK_API_KEY', ''))"},
				Timeout: 10 * time.Second,
			})
			if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "" {
				t.Fatalf("expected clean env, got res=%+v err=%v", res, err)
			}
		})
	}
}
