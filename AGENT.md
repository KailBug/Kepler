# AGENT.md

本文件为在 `Kepler` 仓库中工作的 AI Coding Agent 提供项目上下文、命令手册与核心架构红线。详细架构背景与演进路线请参阅 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

---

## 1. 项目概览与双仓边界

**Kepler**（`github.com/KailBug/Kepler`）是面向生产环境的 **AI Agent 持久化运行时（Durable Runtime）、安全沙箱与治理底座**，采用模块化单体（`keplerd`）架构编写（Go `1.26+`）。

- **双项目联动分工**：与同级仓库 `../Kepler-Harness`（TypeScript 微内核执行引擎）配合，采用 **「Worker 领任务 + Harness 跑循环 + Go 端存状态」** 模式：
  - **Kepler（本仓库，Go）**：负责控制/用户面 API、会话与事件溯源持久化（PostgreSQL / Memory）、基于 `SKIP LOCKED` + Lease Epoch Fencing 的分布式任务调度、SSE 实时广播，以及后续的安全沙箱预热池与 MCP Gateway。
  - **Kepler-Harness（TypeScript）**：作为无状态 Worker 通过 Connect-RPC 主动拉取 `WorkerTask`、恢复历史事件并执行 Agent Loop。
- **协议唯一事实来源（SSOT）**：所有 `.proto` 契约统一维护在本仓库 `proto/kepler/v1/` 下；Go 桩代码生成至 `gen/go/`，TypeScript ESM 客户端桩通过 `buf.gen.ts.yaml` 生成至 `../Kepler-Harness/packages/protocol/src/gen`。
- **当前落地进度 vs. 规划边界**：
  - **已实现**：模块化生命周期注册表（`internal/module`）、Durable Runtime 存储契约及 `memory`/`postgres` 双实现（`internal/runtime`）、Connect-RPC `WorkerService` & `SessionService`、兼容 `@kepler/protocol` 的 REST + SSE 会话路由（`internal/api`）、沙箱抽象接口（`internal/sandbox`）。
  - **规划中（勿假定代码已存在）**：Docker + gVisor (`runsc`) 沙箱实现与 Warm Pool、MCP Gateway、OpenViking 上下文适配、控制台前端（`/web`）、OTel/Mock LLM 压测组件。

---

## 2. 常用开发命令

### 构建与本地运行

```bash
# 编译守护进程二进制
go build -o bin/keplerd ./cmd/keplerd

# 零外部依赖快速启动（使用内存存储后端，默认监听 127.0.0.1:8080）
# Bash:
KEPLER_STORE=memory go run ./cmd/keplerd
# PowerShell:
$env:KEPLER_STORE="memory"; go run ./cmd/keplerd
```

### 本地基础设施（Docker Compose Profiles）

```bash
# 启动常驻核心依赖（PostgreSQL 16 + Redis 7）
docker compose -f deploy/compose.yaml --profile core up -d

# 按需启用可观测性组件（Jaeger OTLP: UI 16686 / gRPC 4317 / HTTP 4318）
docker compose -f deploy/compose.yaml --profile core --profile observability up -d
```

### 测试与校验

```bash
# 运行全部单元测试（默认跳过需外部 PG 的集成测试）
go test ./...
go test -race ./...

# 运行 PostgreSQL 存储契约集成测试（注意：测试会执行 TRUNCATE CASCADE，严禁指向非测试库！）
# Bash:
KEPLER_TEST_DATABASE_URL="postgres://kepler:kepler_dev_password@127.0.0.1:5432/kepler?sslmode=disable" \
  go test -v ./internal/runtime/postgres
# PowerShell:
$env:KEPLER_TEST_DATABASE_URL="postgres://kepler:kepler_dev_password@127.0.0.1:5432/kepler?sslmode=disable"
go test -v ./internal/runtime/postgres
```

### Protobuf 与 Connect-RPC 代码生成（Buf v2）

```bash
# 协议静态检查与破坏性变更检测
buf lint
buf breaking --against '.git#branch=main'

# 生成 Go protobuf 与 Connect-RPC v2 代码（输出至 gen/go/）
buf generate

# 生成跨仓 TypeScript ESM + .d.ts 桩代码（输出至 ../Kepler-Harness/packages/protocol/src/gen）
buf generate --template buf.gen.ts.yaml
```

---

## 3. 目录结构与模块职责

```text
cmd/keplerd/                # 守护进程入口：装配 Config、module.Registry、runtimeModule 与 api.Server
proto/kepler/v1/            # Protobuf 契约源（events.proto 事件模型、runtime.proto Worker/Session RPC）
gen/go/kepler/v1/           # Buf 生成的 Go protobuf 与 Connect v2 桩代码（严禁手改）
migrations/                 # PostgreSQL 初始表结构（001_init.sql，通过 go:embed 嵌入进程启动自动迁移）
deploy/compose.yaml         # 本地开发依赖编排（core / observability profiles）
internal/
  config/                   # 环境变量配置加载（KEPLER_HTTP_ADDR / KEPLER_STORE / KEPLER_DATABASE_URL 等）
  module/                   # 模块化单体生命周期管理（按序 StartAll、启动失败自动回滚、逆序 StopAll）
  api/                      # HTTP/Connect-RPC 传输层：/healthz、/api/v1/status 及 /api/sessions/* REST+SSE 路由
  runtime/                  # Durable Runtime 核心领域层：
    store.go                #   领域模型、状态枚举、哨兵错误与 Store/EventStore 接口定义
    worker_service.go       #   Worker 面 Connect-RPC 实现（注册、心跳、长轮询领任务、续租、追加事件、结单）
    session_service.go      #   用户面 Connect-RPC 实现（会话管理、发消息、HITL 审批、流式事件）
    proto_conv.go           #   存储层 JSONB Event 与 Protobuf oneof SessionEvent 双向转换
    broadcaster.go          #   进程内非阻塞会话级唤醒通知器（驱动 SSE 与 StreamEvents 实时推送）
    memory/                 #   内存版 Store 实现（支持 WithClock 注入，用于毫秒级确定性单测）
    postgres/               #   PostgreSQL 版 Store 实现（pgx/v5 事务 + FOR UPDATE SKIP LOCKED + pg_notify）
    storetest/              #   跨后端共享行为契约测试套件（storetest.Run）
  sandbox/                  # 安全沙箱 Provider 接口定义（Acquire / Exec / SyncWorkspace / Release）
```

### 环境变量速查（`internal/config/config.go`）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `KEPLER_HTTP_ADDR` | `127.0.0.1:8080` | HTTP / Connect-RPC / SSE 共用监听地址 |
| `KEPLER_STORE` | `postgres` | 存储后端：`postgres` 或 `memory` |
| `KEPLER_DATABASE_URL` | `postgres://kepler:kepler_dev_password@127.0.0.1:5432/kepler?sslmode=disable` | PostgreSQL 连接串（启动时自动执行幂等 `Migrate`） |
| `KEPLER_SANDBOX_RUNTIME` | `runc` | 沙箱容器运行时标识（如 `runc` / `runsc`） |
| `KEPLER_LOG_LEVEL` | `info` | 日志级别 |
| `KEPLER_TEST_DATABASE_URL` | _（空）_ | 仅测试使用；设置后启用 `internal/runtime/postgres` 集成测试 |

---

## 4. 核心架构不变量与开发红线（必读）

### 4.1 严禁手改生成代码 & 注意 Connect-RPC v2 签名

1. **禁止手动修改 `gen/go/**`**：任何字段或接口变更必须先修改 `proto/kepler/v1/*.proto` 并执行 `buf generate`。
2. **使用 `connectrpc.com/connect/v2`（非 v1）**：
   - 本项目依赖 **Connect v2**（`connectrpc.com/connect/v2` + `connecthttp`），**不使用** v1 的泛型包装类型 `connect.Request[T]` / `connect.Response[T]`。
   - Handler 方法直接接收并返回原生 Protobuf 指针：
     `func(ctx context.Context, req *keplerv1.FooRequest) (*keplerv1.FooResponse, error)`。
   - 服务通过 `keplerv1connect.Register*ServiceHandler(srv.Connect(), impl)` 注册到 `*connect.Server`，并由 `api.Server` 在启动时通过 `connecthttp.Mount(mux, connectServer)` 一次性挂载。
   - **模块启动顺序约束**：向 `srv.Connect()` 或 `srv.Mux()` 注册路由的业务模块（如 `runtimeModule`）必须在 `api.Server` 之前注册到 `module.Registry`，确保在 `api.Server.Start()` 调用 `MountConnect()` 前完成路由挂载。

### 4.2 Durable Runtime 状态机与 Fencing 红线

1. **只追加事件流与严格单调 `seq`**：
   - `events` 表是不可变 Append-only 日志，受 `(session_id, seq)` 唯一约束保护。
   - 每次写入必须在同一事务内锁定会话行、递增 `sessions.last_seq`；若传入非零 `ev.Seq` 且与期望序号不一致，必须返回 `runtime.ErrSeqConflict`。
2. **Lease 与 Epoch Fencing（防脑裂 / 防僵尸 Worker）**：
   - `AcquireTask` 通过 `FOR UPDATE SKIP LOCKED` 认领 `pending` 或租约已过期（`status = 'leased' AND lease_expires_at_ms <= now`）的任务，单调递增 `lease_epoch` 并签发新 `lease_id`。
   - 所有 Worker 写操作（`AppendEvents`、`RenewLease`、`CompleteTask`、`SuspendTask`、`FailTask`）**必须严格校验**当前活跃任务的 `status == leased`、`lease_id` 与 `lease_epoch`（`AppendEvents` 还需校验租约未过期）；不匹配时一律返回 `runtime.ErrStaleEpoch`（映射为 Connect `CodeFailedPrecondition` / HTTP `409 Conflict`），绝不允许旧代际僵尸 Worker 污染状态。
3. **双存储后端语义对齐与契约测试**：
   - 修改或扩展 `runtime.Store` 接口时，**必须同步实现** `internal/runtime/memory` 与 `internal/runtime/postgres`，并在 `internal/runtime/storetest/suite.go` 中补充契约用例。
   - 测试租约超时与故障接管时，使用 `WithClock` + `advance(duration)` 推进虚拟时钟，**禁止在单测中使用 `time.Sleep` 等待真实租约过期**。
4. **流式订阅「先订阅、后查库」防漏消息模式**：
   - 在 REST SSE（`GET /api/sessions/{id}/events`）与 Connect `StreamEvents` 中，必须在首次查询 `Store` 历史快照/增量事件**之前**调用 `broadcaster.Subscribe(sessionID)`，防止快照读取与进入事件循环之间提交的事件丢失，并保持对 `since` / `Last-Event-ID` / `since_seq` 断线续传的支持。

### 4.3 双传输面协议同源（Connect-RPC 与 REST/SSE）

用户面同时暴露 Connect-RPC（`SessionService`）与对齐 `@kepler/protocol` 的 HTTP/SSE 路由（`internal/api/sessions.go`）。新增或修改 `SessionEvent` 事件类型时，**必须三处同步更新**：

1. `proto/kepler/v1/events.proto`（更新 `SessionEvent.payload` 的 `oneof` 定义并重新 `buf generate`）。
2. `internal/runtime/proto_conv.go`（同步更新 `EventToProto` 与 `EventFromProto`；注意存储层 `ev.Type` 使用 kebab-case，如 `"user"`、`"assistant"`、`"tool-result"`、`"approval-requested"`、`"approval-resolved"`、`"turn-ended"`，JSONB 字段使用 camelCase）。
3. `internal/api/sessions.go`（确认 `toWireEvent` 扁平化输出与前端 `@kepler/protocol` 的 JSON 结构保持一致）。

---

## 5. 代码风格与提交规范

- **克制引入第三方依赖**：优先使用 Go 标准库（路由使用 Go 1.22+ `http.ServeMux` 方法路径模式如 `"POST /api/sessions/{id}/messages"` 与 `r.PathValue`，日志使用 `log/slog`，SQL 嵌入使用 `embed`）。
- **统一错误映射**：领域层返回 `internal/runtime/store.go` 中的哨兵错误并用 `errors.Is` 判断；传输层统一通过 `mapStoreError`（Connect-RPC）与 `writeStoreError`（HTTP REST）转换为标准状态码，勿在业务层硬编码 HTTP/RPC 错误码。
- **Git 提交规范**：遵循 Conventional Commits 格式 `<type>(<scope>): <summary>`（例如 `feat(runtime): ...`、`fix(api): ...`、`test(runtime): ...`、`build(deps): ...`）。提交前确保 `go test ./...` 全部通过。
