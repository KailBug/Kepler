package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KailBug/Kepler/internal/sandbox"
)

const (
	defaultPythonTimeoutSec = 15
	maxPythonTimeoutSec     = 60
)

var pythonExecSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"code": {
			"type": "string",
			"description": "要执行的 Python 3 代码"
		},
		"timeout_seconds": {
			"type": "integer",
			"description": "超时时间（秒），默认 15，最大 60",
			"minimum": 1,
			"maximum": 60
		}
	},
	"required": ["code"]
}`)

// NewPythonExecTool constructs the sandboxed python_exec MCP tool backed by sandbox.Provider.
func NewPythonExecTool(provider sandbox.Provider, spec sandbox.Spec) Tool {
	return Tool{
		Metadata: ToolMetadata{
			Name:            "python_exec",
			Description:     "在隔离的安全沙箱（断网、只读根文件系统、资源受限）中执行 Python 3 代码，返回标准输出、标准错误和退出码。",
			InputSchema:     pythonExecSchema,
			RequireApproval: false,
		},
		Handler: func(ctx context.Context, _ CallContext, rawArgs json.RawMessage) (CallResult, error) {
			if provider == nil {
				return CallResult{}, errors.New("sandbox provider is not configured")
			}
			var args struct {
				Code           string `json:"code"`
				TimeoutSeconds int    `json:"timeout_seconds"`
			}
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return CallResult{Content: fmt.Sprintf("invalid arguments: %v", err), IsError: true}, nil
			}
			if strings.TrimSpace(args.Code) == "" {
				return CallResult{Content: "code must be a non-empty string", IsError: true}, nil
			}

			timeoutSec := args.TimeoutSeconds
			if timeoutSec <= 0 {
				timeoutSec = defaultPythonTimeoutSec
			}
			if timeoutSec > maxPythonTimeoutSec {
				timeoutSec = maxPythonTimeoutSec
			}

			lease, err := provider.Acquire(ctx, spec)
			if err != nil {
				return CallResult{}, fmt.Errorf("acquire sandbox: %w", err)
			}
			defer func() {
				_ = provider.Release(ctx, lease)
			}()

			res, err := provider.Exec(ctx, lease, sandbox.ExecRequest{
				Command: []string{"python3", "-"},
				Stdin:   args.Code,
				Timeout: time.Duration(timeoutSec) * time.Second,
			})
			if errors.Is(err, sandbox.ErrTimeout) {
				msg := fmt.Sprintf("execution timed out after %ds", timeoutSec)
				if out := formatExecOutput(res); out != "" {
					msg += "\n" + out
				}
				return CallResult{Content: msg, IsError: true}, nil
			}
			if err != nil {
				return CallResult{}, err
			}

			body := formatExecOutput(res)
			if res.ExitCode != 0 {
				if body == "" {
					body = fmt.Sprintf("process exited with code %d", res.ExitCode)
				} else {
					body = fmt.Sprintf("exit code %d\n%s", res.ExitCode, body)
				}
				return CallResult{Content: body, IsError: true}, nil
			}
			if body == "" {
				body = "(no output)"
			}
			return CallResult{Content: body, IsError: false}, nil
		},
	}
}

func formatExecOutput(res sandbox.ExecResult) string {
	stdout := strings.TrimRight(res.Stdout, "\r\n")
	stderr := strings.TrimRight(res.Stderr, "\r\n")
	switch {
	case stdout != "" && stderr != "":
		return stdout + "\n[stderr]\n" + stderr
	case stdout != "":
		return stdout
	case stderr != "":
		return "[stderr]\n" + stderr
	default:
		return ""
	}
}
