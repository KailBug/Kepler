# Kepler： 面向生产环境的 AI Agent 执行与治理基础设施

> **项目定位**：一套面向规模化生产环境的 **Agent 运行时、安全沙箱与全生命周期治理底座**。

> **开发策略**：「1 个极简内核 + 1~2 个硬核基建深水区」。本项目与独立的 **Agent Harness（TypeScript）** 形成双项目联动，天然验证**跨语言、异构 Agent 统一纳管（Managed / BYO）**的工程架构。

## 1. 背景与核心痛点 (Why This Project?)

当 AI Agent 从 Demo 阶段进入真实生产环境时，决定其最终效果与可靠性的已不再仅仅是底层大模型，而是 **Model、Harness、Context、Tool、Data 与 Execution Architecture** 的协同。本项目旨在解决生产级 Agent 落地过程中的五大工程难题：

1. **长周期任务极易中断与丢失状态**：多步推理和外部工具调用耗时可能长达数分钟甚至数小时，进程重启、网络抖动或实例缩容会导致上下文和执行进度全部丢失，且缺乏幂等保障。
2. **执行逻辑与模型/框架高度耦合**：Agent Loop、上下文组装、工具调用、人工审批（HITL）与可观测性混杂在一起，难以灵活切换 ReAct、Planner-Executor 或 Multi-Agent 等执行范式。
3. **上下文窗口与成本约束**：长程任务中 Token 爆炸与信息衰减并存，缺乏系统化的 Context Engineering 与分层记忆管理。
4. **工具执行与权限安全边界缺失**：Agent 执行代码或调用 MCP Server 时缺乏隔离沙箱、细粒度凭证委托（Delegated Auth）与网络/文件访问控制，存在容器逃逸与密钥泄露风险。
5. **非确定性系统的发布与演进困难**：修改 Prompt、模型版本或工具定义后，缺乏统一的 Trace、自动化回归评测与上线门禁（Release Gate），无法形成持续优化的闭环。

## 2. 系统总体架构 (Architecture Overview)

系统采用**控制面（Control Plane）与数据/执行面（Data/Execution Plane）分离**的设计，覆盖从 `Build -> Evaluate -> Gate -> Release -> Run -> Observe -> Evolve` 的完整闭环：

```other
+-------------------------------------------------------------------------+
|                   Agent Control Plane (Golang)                          |
|  [Artifact Registry]  [Lifecycle & Release]  [IAM & Policy]  [Cost/Quota]|
+------------------------------------+------------------------------------+
                                     | (Unified Protocol: gRPC / HTTP / MCP)
+------------------------------------v------------------------------------+
|                      Execution & Runtime Plane                          |
|                                                                         |
|  +-------------------------------+   +-------------------------------+  |
|  |   Harness Kernel (Project A)  |   |   Context & Memory (Pluggable)|  |
|  | - TypeScript + Zod + MCP SDK  |<->| - OpenViking (viking:// VFS)  |  |
|  | - Pluggable Loop (ReAct/Plan) |   | - L0/L1/L2 On-Demand Loading  |  |
|  | - HITL Approval & OTel Hooks  |   | - Grounding & RAG Pipeline    |  |
|  +---------------+---------------+   +-------------------------------+  |
|                  |                                                      |
|  +---------------v---------------+   +-------------------------------+  |
|  | Managed Durable Runtime (Go)  |   | MCP Gateway & Secure Sandbox  |  |
|  | - PostgreSQL Event Sourcing   |<->| - MCP Gateway (Credential Proxy) |
|  | - Session/Turn/Event State    |   | - SandboxProvider Interface   |  |
|  | - Skip-Locked Queue & Replay  |   | - Docker + gVisor Warm Pool   |  |
|  | - Tool Idempotency & Leasing  |   | - Network/FS ACL & Isolation  |  |
|  +---------------+---------------+   +-------------------------------+  |
+------------------+------------------------------------------------------+
                   | (OpenTelemetry OTLP - GenAI Semantic Conventions)
+------------------v------------------------------------------------------+
|                   Evaluation & Evolution Platform                       |
|  [Langfuse / Jaeger]  [Dataset & Evaluators]  [Release Gate]  [BadCase] |
+-------------------------------------------------------------------------+
```

### 双项目联动设计 (Dual-Repo Synergy)

- **Project A — Agent Harness（TypeScript）**：专注于 AI 原生执行循环、Schema 校验（Zod）、流式输出与多范式编排（ReAct / Plan-and-Execute），可独立运行，也可作为被托管的执行引擎接入底座。
- **Project B — AgentSphere-Lite（Golang）**：专注于高并发调度、持久化状态机、容器/沙箱隔离池与 MCP 安全网关。通过标准协议（gRPC / HTTP / MCP）托管 Project A，实战验证**异构、跨语言 Agent 纳管**能力。

## 3. 核心模块拆解 (Core Modules)

### 模块一：Harness Kernel（微内核执行引擎 — Project A）

将 Agent 的核心推理循环抽象为模块化、可组合、可扩展的微内核：

- **能力解耦**：将 Model Adapter、Agent Loop、Context Assembly、Tool Dispatcher、HITL Approval（人工审批挂起/恢复）和 Telemetry Hooks 彻底解耦。
- **多范式支持**：通过插件化策略支持 `ReAct`、`Workflow`、`Planner-Executor`、`Multi-Agent` 等不同执行架构，避免被单一框架绑死。
- **模型演进适配**：屏蔽底层不同模型厂商在 Tool Calling、Reasoning Token、Structured Output 上的协议差异。

### 模块二：Managed Runtime（持久化长任务运行时 — 核心深水区一）

借鉴 **Durable Execution（如 Temporal / Restate）** 思想，保证“计算实例可随时被替换，但任务状态与结果永不丢失”：

- **核心状态模型**：定义标准化的 `Session`（会话）、`Turn`（轮次）、`Event`（不可变事件流）、`Deployment` 与 `Workspace` 状态机。
- **事件溯源与轻量调度**：以 PostgreSQL 作为强一致状态底座（Append-only `events` 表 + `JSONB` 载荷），结合 `SELECT ... FOR UPDATE SKIP LOCKED` 与租约（Lease）机制实现轻量级分布式任务调度与 Worker 故障自动接管。
- **断点恢复与幂等执行**：持久化每一步模型输出与工具调用结果，Worker 遭遇 `kill -9` 宕机后可在其他节点无缝 Replay 并继续执行；为具有外部副作用的工具调用提供 `Idempotency Key` 机制，防止重试引发重复写入或扣费。
- **实时事件广播**：通过 Redis Streams（或 PostgreSQL `LISTEN/NOTIFY`）向控制面与前端推送 SSE 实时执行状态。

### 模块三：Context, Memory & Knowledge（上下文与记忆引擎）

解决有限上下文窗口与成本约束下的信息获取与保留问题，**与 Runtime 强一致状态库解耦**：

- **分层按需加载**：接入 **OpenViking** 作为可插拔的上下文数据库后端，基于 `viking://` 虚拟文件系统范式统一组织 Resources、Memory 与 Skills，支持 L0/L1/L2 分层按需加载以降低 Token 消耗。
- **Context Engineering**：实现动态上下文预算分配、历史消息压缩/摘要（Compaction）以及 KV-Cache 友好的 Prompt 组装策略。
- **Grounding 与 Vectorless RAG with PageIndex**：提供可插拔的检索增强接口，确保外部知识注入可溯源、可验证。

### 模块四：MCP Gateway & Secure Sandbox（工具治理与安全沙箱 — 核心深水区二）

为代码执行和外部工具调用建立可隔离、可审计、可治理的安全边界：

- **统一 MCP 网关（MCP Gateway）**：
   - **双传输协议适配**：同时兼容沙箱内运行的本地 `stdio` 模式与远程 `Streamable HTTP / SSE` 模式。
   - **凭证隔离与代理注入（Credential Isolation）**：业务 API Key 与 OAuth Token 统一托管在网关层，**Agent 与沙箱内部永远无法接触明文密钥**，鉴权后由网关代理注入请求头转发，从根源阻断 Prompt 注入导致的密钥泄露。
   - **细粒度治理**：提供 Tool/Skill 注册与版本管理、RBAC 权限控制、高危操作拦截（触发 HITL 挂起审批）与全量审计日志。
- **强隔离沙箱与预热池（Secure Sandbox & Warm Pool）**：
   - **接口抽象**：定义统一的 `SandboxProvider` 接口（`Acquire` / `Exec` / `SyncWorkspace` / `Release`），解耦底层隔离技术。
   - **内核级隔离**：首期采用 **Docker + gVisor (`runsc`, `systrap` 模式)**，在无需嵌套 KVM 的环境下提供用户态内核隔离，并强制叠加 `--network=none`、`--read-only` 与 cgroups 资源配额；后续可扩展支持 Firecracker / E2B。
   - **预热池优化**：在 Go 侧实现沙箱 Warm Pool（预先维持空闲轻量容器池，按需绑定 Workspace、用完销毁重置），将单次代码执行的冷启动延迟从秒级压低至百毫秒以内。

### 模块五：Agent Control Plane（控制面与异构纳管）

- **统一资产协议**：定义标准的 `Agent Artifact`（声明模型、Prompt、工具依赖、权限策略与运行配置），支持灰度发布、版本回滚与成本限流。
- **异构纳管（Managed / BYO）**：不仅管理平台原生托管的 Agent，还可通过统一网关与接入协议纳管外部自研框架（BYO Agent），在保留业务创新空间的同时实现统一的权限、观测与治理。

### 模块六：Evaluation & Continuous Evolution（评测与持续进化）

- **供应商中立的全链路可观测**：遵循 **OpenTelemetry GenAI Semantic Conventions**，通过 OTLP 协议导出至 **Langfuse**（或本地 Jaeger）。强制将同一 `trace_id` 透传贯穿 `Harness Loop -> Runtime Checkpoint -> MCP Gateway -> Sandbox Exec`，精准定位长任务故障根因。
- **自动化评测与上线 Gate**：支持数据集管理、规则/LLM-as-a-Judge 评估器与回归对比分析，在模型、Prompt 或工具变更前设置自动化质量门禁。
- **线上闭环**：支持线上流量抽样与 Bad Case 一键回流至评测集，驱动系统持续迭代。

## 4. 技术选型与架构决策 (Tech Stack & ADR)

| **层级 / 模块**                   | **确定选型**                                | **核心决策理由 (Why)**                                                                                           |
| ----------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Infra 底座 (Project B)**      | **Golang**                              | 云原生/Docker/K8s 原生语言，高并发调度、网络代理（MCP Gateway）与分布式状态机开发体验极佳，契合大厂 Infra 技术栈。                                   |
| **Agent Harness (Project A)** | **TypeScript + Zod**                    | MCP 官方 SDK 第一梯队支持，JSON Schema 校验与流式事件处理丝滑，同时与 Go 底座构成跨语言异构纳管闭环。                                            |
| **持久化与事件溯源**                  | **PostgreSQL**                          | 强 ACID 保证状态机与幂等键一致性；`JSONB` + Append-only Event 表 + `SKIP LOCKED` 队列即可支撑生产级 Durable Execution，架构极简无重型组件负担。 |
| **实时事件流**                     | **Redis Streams**（或 PG `LISTEN/NOTIFY`） | 轻量高效，负责 Runtime 向控制面/前端的 SSE 实时状态广播与异步通知。                                                                  |
| **上下文与记忆层**                   | **OpenViking**（可插拔）                     | 专注语义检索与 L0/L1/L2 分层上下文管理，与 PG 负责的执行态持久化职责正交解耦。                                                             |
| **安全沙箱**                      | **Docker + gVisor (`runsc`)**           | 相比原生 Docker 提供用户态内核隔离（防容器逃逸）；`systrap` 模式不强依赖 KVM，WSL2 原生可跑，配合自研 Warm Pool 解决冷启动延迟。                        |
| **工具协议**                      | **MCP (Model Context Protocol)**        | 行业标准协议；在其之上自建 MCP Gateway 实现凭证隔离、权限校验与审计。                                                                  |
| **可观测性与评测**                   | **OpenTelemetry (OTLP) + Langfuse**     | 通过 OTel 保持供应商中立，Langfuse 提供开箱即用的 LLM Trace 视图与评测集管理（支持 Cloud 免费版或本地自托管）。                                   |

## 5. 本地开发环境与零成本压测方案 (Local Dev & Testing)

本项目专为**单机本地高配环境（WSL2 Linux + 32GB 内存）**进行了轻量化与工程化适配，无需租用昂贵云服务器即可完成全链路开发与高并发压测：

1. **原生 Docker + gVisor 环境**：在 WSL2 内开启 `systemd`，直接安装原生 `docker-ce` 与 `runsc`（避开 Windows Docker Desktop 的额外虚拟化开销），以极简基础镜像（如 `python:3.12-alpine`）验证沙箱隔离与 Warm Pool 性能。
2. **按需编排（Docker Compose Profiles）**：
   - `core` profile（常驻）：仅启动 PostgreSQL + Redis，极低资源占用。
   - `memory` / `observability` profile（按需）：按需拉起 OpenViking、本地 Jaeger 或自托管 Langfuse（也可直接对接 Langfuse Cloud 免费版实现零本地内存开销）。
3. **内置 Mock / Replay LLM Server（零成本混沌压测利器）**：
   - 在测试套件中使用 Go 实现一个兼容 OpenAI HTTP 协议的 **Mock LLM Server**，支持按剧本精准返回 Tool Call、注入网络延迟、模拟 429 限流或返回畸形 JSON。
   - **工程价值**：以 **0 API 费用**在本地跑通高并发任务压测（如 50+ 并发长任务）、Worker 进程 `kill -9` 混沌测试（验证断点恢复率与工具幂等防重入）以及沙箱预热池吞吐量基准测试。

## 6. 渐进式开发路线图 (Implementation Roadmap)

按四个阶段逐步交付可验证的里程碑，避免前期过度设计：

- [ ] **Phase 1: Minimal Harness Kernel & Mock LLM (MVP)**
   - 在 Project A (TS) 中实现解耦的 Agent Loop、Model Adapter 与基础 ReAct 执行器。
   - 在 Project B (Go) 中编写 Mock LLM Server，接入 OTel Trace 导出至 Langfuse，跑通本地确定性多步调用。
- [ ] **Phase 2: Durable Runtime (核心亮点一)**
   - 在 PostgreSQL 中落地 `Session / Turn / Event` 状态机与事件溯源表结构。
   - 实现基于 `SKIP LOCKED` 与 Lease 的 Worker 调度、工具调用 `Idempotency Key` 校验。
   - **验收指标**：并发压测下随机 `kill -9` 杀掉执行 Worker，验证任务 100% 断点恢复且副作用工具零重复执行。
- [ ] **Phase 3: MCP Gateway & gVisor Sandbox Warm Pool (核心亮点二)**
   - 搭建 Go MCP Gateway，实现 `stdio`/HTTP 路由、网关层凭证注入与 RBAC 鉴权。
   - 实现 `SandboxProvider` 接口与 Docker + gVisor Warm Pool，配置网络隔离与资源配额。
   - **验收指标**：压测对比冷启动 vs. 预热池分配耗时，验证沙箱内无法访问外部网络与宿主机密钥。
- [ ] **Phase 4: Context/Memory, Control Plane & Eval Gate (闭环完善)**
   - 通过插件接口接入 OpenViking，实现分层上下文加载与记忆管理。
   - 实现 `Agent Artifact` 版本管理与离线回归评测 CLI/Pipeline，支持不同版本间的成功率、延迟与 Token 成本对比。

