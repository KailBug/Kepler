package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KailBug/Kepler/internal/sandbox"
	"github.com/google/uuid"
)

const (
	defaultImage          = "python:3.12-alpine"
	defaultNetworkMode    = "none"
	defaultMemoryMB       = int64(256)
	defaultCPUMilli       = int64(500)
	defaultPIDsLimit      = int64(64)
	defaultTmpfsMB        = int64(64)
	defaultMaxOutputBytes = 64 * 1024
	defaultExecTimeout    = 30 * time.Second
	defaultReleaseTimeout = 10 * time.Second
	workspaceDir          = "/workspace"
)

// CommandRunner abstracts subprocess execution so the provider can be unit-tested without Docker.
type CommandRunner interface {
	Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (exitCode int, err error)
}

// Options configures the Docker/gVisor SandboxProvider.
type Options struct {
	// CommandPrefix specifies how to invoke docker, e.g. ["docker"] or ["wsl", "-e", "docker"].
	CommandPrefix  []string
	DefaultRuntime string // "runc" | "runsc"
	DefaultImage   string
	MaxOutputBytes int
	DefaultTimeout time.Duration
	Runner         CommandRunner
}

// Provider implements sandbox.Provider on top of Docker Engine with optional gVisor (runsc) runtime.
type Provider struct {
	runner         CommandRunner
	defaultRuntime string
	defaultImage   string
	maxOutputBytes int
	defaultTimeout time.Duration
}

func New(opts Options) *Provider {
	prefix := opts.CommandPrefix
	if len(prefix) == 0 {
		prefix = []string{"docker"}
	}
	runner := opts.Runner
	if runner == nil {
		runner = &execRunner{prefix: append([]string(nil), prefix...)}
	}
	img := opts.DefaultImage
	if img == "" {
		img = defaultImage
	}
	maxOut := opts.MaxOutputBytes
	if maxOut <= 0 {
		maxOut = defaultMaxOutputBytes
	}
	timeout := opts.DefaultTimeout
	if timeout <= 0 {
		timeout = defaultExecTimeout
	}
	return &Provider{
		runner:         runner,
		defaultRuntime: opts.DefaultRuntime,
		defaultImage:   img,
		maxOutputBytes: maxOut,
		defaultTimeout: timeout,
	}
}

// ParseCommandPrefix splits a space-separated command string (e.g. "wsl -e docker") into argv.
func ParseCommandPrefix(raw string) []string {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return []string{"docker"}
	}
	return fields
}

func (p *Provider) Acquire(ctx context.Context, spec sandbox.Spec) (sandbox.Lease, error) {
	resolved := p.resolveSpec(spec)
	name := "kepler-sb-" + uuid.NewString()[:12]
	args := buildRunArgs(name, resolved)

	var stdout, stderr bytes.Buffer
	code, err := p.runner.Run(ctx, args, nil, &stdout, &stderr)
	if err != nil {
		return sandbox.Lease{}, fmt.Errorf("docker acquire: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	if code != 0 {
		return sandbox.Lease{}, fmt.Errorf("docker acquire exited %d: %s", code, strings.TrimSpace(stderr.String()))
	}

	containerID := strings.TrimSpace(stdout.String())
	if containerID == "" {
		containerID = name
	}
	return sandbox.Lease{
		ID:        containerID,
		Runtime:   resolved.Runtime,
		CreatedAt: time.Now(),
	}, nil
}

func (p *Provider) Exec(ctx context.Context, lease sandbox.Lease, req sandbox.ExecRequest) (sandbox.ExecResult, error) {
	if lease.ID == "" {
		return sandbox.ExecResult{}, errors.New("sandbox lease ID is required")
	}
	if len(req.Command) == 0 {
		return sandbox.ExecResult{}, errors.New("sandbox exec command is required")
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = p.defaultTimeout
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := buildExecArgs(lease.ID, req)
	var stdin io.Reader
	if req.Stdin != "" {
		stdin = strings.NewReader(req.Stdin)
	}
	stdout := newBoundedBuffer(p.maxOutputBytes)
	stderr := newBoundedBuffer(p.maxOutputBytes)

	start := time.Now()
	code, err := p.runner.Run(execCtx, args, stdin, stdout, stderr)
	duration := time.Since(start)

	res := sandbox.ExecResult{
		ExitCode:  code,
		Stdout:    stdout.String(),
		Stderr:    stderr.String(),
		Truncated: stdout.truncated || stderr.truncated,
		Duration:  duration,
	}
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		res.ExitCode = -1
		return res, sandbox.ErrTimeout
	}
	if err != nil {
		return res, fmt.Errorf("docker exec: %w", err)
	}
	return res, nil
}

// SyncWorkspace streams hostDir into the container's /workspace using tar over stdin.
// This works cleanly with --read-only rootfs + tmpfs /workspace and across Windows/WSL2 boundaries.
func (p *Provider) SyncWorkspace(ctx context.Context, lease sandbox.Lease, hostDir string) error {
	if lease.ID == "" {
		return errors.New("sandbox lease ID is required")
	}
	info, err := os.Stat(hostDir)
	if err != nil {
		return fmt.Errorf("stat workspace %q: %w", hostDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("workspace %q is not a directory", hostDir)
	}

	pr, pw := io.Pipe()
	tarErrCh := make(chan error, 1)
	go func() {
		err := writeDirTar(hostDir, pw)
		_ = pw.CloseWithError(err)
		tarErrCh <- err
	}()

	var stderr bytes.Buffer
	code, runErr := p.runner.Run(ctx, []string{"exec", "-i", lease.ID, "tar", "-xf", "-", "-C", workspaceDir}, pr, io.Discard, &stderr)
	_ = pr.Close()
	tarErr := <-tarErrCh

	if tarErr != nil {
		return fmt.Errorf("archive workspace: %w", tarErr)
	}
	if runErr != nil {
		return fmt.Errorf("sync workspace: %w (stderr: %s)", runErr, strings.TrimSpace(stderr.String()))
	}
	if code != 0 {
		return fmt.Errorf("sync workspace exited %d: %s", code, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func (p *Provider) Release(ctx context.Context, lease sandbox.Lease) error {
	if lease.ID == "" {
		return nil
	}
	// Always attempt cleanup even if caller's context was canceled or timed out.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultReleaseTimeout)
	defer cancel()

	var stderr bytes.Buffer
	code, err := p.runner.Run(cleanupCtx, []string{"rm", "-f", lease.ID}, nil, io.Discard, &stderr)
	if err != nil {
		return fmt.Errorf("docker release %s: %w (stderr: %s)", lease.ID, err, strings.TrimSpace(stderr.String()))
	}
	if code != 0 && !strings.Contains(stderr.String(), "No such container") {
		return fmt.Errorf("docker release %s exited %d: %s", lease.ID, code, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func (p *Provider) resolveSpec(spec sandbox.Spec) sandbox.Spec {
	if spec.Image == "" {
		spec.Image = p.defaultImage
	}
	if spec.Runtime == "" {
		spec.Runtime = p.defaultRuntime
	}
	if spec.NetworkMode == "" {
		spec.NetworkMode = defaultNetworkMode
	}
	if spec.MemoryMB <= 0 {
		spec.MemoryMB = defaultMemoryMB
	}
	if spec.CPUMilli <= 0 {
		spec.CPUMilli = defaultCPUMilli
	}
	if spec.PIDsLimit <= 0 {
		spec.PIDsLimit = defaultPIDsLimit
	}
	if spec.TmpfsMB <= 0 {
		spec.TmpfsMB = defaultTmpfsMB
	}
	return spec
}

func buildRunArgs(name string, spec sandbox.Spec) []string {
	args := []string{
		"run", "-d",
		"--name", name,
		"--label", "kepler.managed=true",
		"--network=" + spec.NetworkMode,
	}
	if spec.Runtime != "" {
		args = append(args, "--runtime="+spec.Runtime)
	}
	if spec.ReadOnly {
		args = append(args, "--read-only")
	}
	tmpfsOpt := fmt.Sprintf("rw,noexec,nosuid,size=%dm", spec.TmpfsMB)
	args = append(args,
		"--tmpfs", "/tmp:"+tmpfsOpt,
		"--tmpfs", workspaceDir+":"+tmpfsOpt,
		"--workdir", workspaceDir,
		"--memory="+strconv.FormatInt(spec.MemoryMB, 10)+"m",
		"--memory-swap="+strconv.FormatInt(spec.MemoryMB, 10)+"m",
		"--cpus="+formatCPUCores(spec.CPUMilli),
		"--pids-limit="+strconv.FormatInt(spec.PIDsLimit, 10),
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		spec.Image,
		"sh", "-c", "trap 'exit 0' TERM INT; while true; do sleep 60 & wait $!; done",
	)
	return args
}

func buildExecArgs(containerID string, req sandbox.ExecRequest) []string {
	args := []string{"exec"}
	if req.Stdin != "" {
		args = append(args, "-i")
	}
	workDir := req.WorkDir
	if workDir == "" {
		workDir = workspaceDir
	}
	args = append(args, "-w", workDir)

	if len(req.Env) > 0 {
		keys := make([]string, 0, len(req.Env))
		for k := range req.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			args = append(args, "-e", k+"="+req.Env[k])
		}
	}
	args = append(args, containerID)
	args = append(args, req.Command...)
	return args
}

func formatCPUCores(milli int64) string {
	return strconv.FormatFloat(float64(milli)/1000.0, 'f', 3, 64)
}

func writeDirTar(root string, dst io.Writer) error {
	tw := tar.NewWriter(dst)
	defer tw.Close()

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	return filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil || rel == "." {
			return err
		}
		// Skip .git metadata and symlinks to prevent host path escape.
		if d.Name() == ".git" && d.IsDir() {
			return filepath.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

type execRunner struct {
	prefix []string
}

func (r *execRunner) Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	full := make([]string, 0, len(r.prefix)-1+len(args))
	full = append(full, r.prefix[1:]...)
	full = append(full, args...)

	cmd := exec.CommandContext(ctx, r.prefix[0], full...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

type boundedBuffer struct {
	limit     int
	buf       bytes.Buffer
	truncated bool
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if b.buf.Len() >= b.limit {
		b.truncated = true
		return n, nil
	}
	rem := b.limit - b.buf.Len()
	if len(p) > rem {
		b.buf.Write(p[:rem])
		b.truncated = true
		return n, nil
	}
	b.buf.Write(p)
	return n, nil
}

func (b *boundedBuffer) String() string {
	if b.truncated {
		return b.buf.String() + "\n...[truncated]"
	}
	return b.buf.String()
}
