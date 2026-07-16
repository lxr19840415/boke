# Hermes Agent v0.16 Kanban Swarm 深度解析：看板即协议的多智能体协作新范式

> **摘要：** 当多智能体系统从单轮对话进化到持久化协作时，状态持久性、失败恢复能力和人工可介入性成为刚需。本文深入解析 Hermes Agent 在 v0.16 系列中引入的 Kanban Swarm——一个基于 SQLite 持久化的多 Profile 任务协作框架。文章从架构设计、双表面交互、五角色协作模式、可观测性等维度展开分析，并与 AutoGen、CrewAI、LangGraph 等主流框架进行横向对比。核心论点：「看板即协议（Kanban as Protocol）」——看板既是任务的状态存储，也是 Agent 间的交流契约。本文本身即由 Kanban Swarm 流水线生产，是 Nous Research Dogfooding 实践的第一手案例。

---

## 1. 引言：从单智能体到多智能体协作拓扑

多智能体系统的协作拓扑在过去两年经历了三次范式跃迁。

第一范式是**管道式（Pipeline）**：Agent 按固定顺序执行，前一个 Agent 的输出是后一个 Agent 的输入。CrewAI 的 Sequential Process 是典型代表，简单直观，但缺乏灵活性——一旦中间环节失败，整个流水线需要重置。

第二范式是**市场式（Marketplace）**：Agent 通过某种中介机制竞标任务。Microsoft AutoGen 的 GroupChat 接近这种模式——Agent 围绕对话轮流发言，由 Manager 裁决。但市场式的代价是协调开销随 Agent 数量呈超线性增长。

第三范式是**看板式（Kanban）**：所有 Agent 围绕一个共享的持久化工作板读写任务，状态的变迁由看板自身的事务保证。这就是 Hermes Agent 在 v0.16 引入的 **Kanban Swarm**。

在 Hermes 的架构版图中，Kanban Swarm 位于 Gateway 与 Worker Profile 之间——它不是替代 `delegate_task`，而是填补了「函数调用」无法覆盖的持久化工作队列场景。一句话定义：**Kanban Swarm 是一个基于 SQLite 持久化的多 Profile 任务协作板**，每个 Worker 是独立 OS 进程，看板既是它们的通讯协议，也是状态的唯一真理源。

---

## 2. 架构设计：看板即协议（Kanban as Protocol）

### 2.1 为什么是 SQLite？

在选择协作媒介时，Kanban Swarm 的设计者做了一个看似「反潮流」的决定：不用 Kafka，不用 RabbitMQ，不用 Redis，而是用了 Python 标准库自带的 **SQLite**。

这个决定的背后是一组清晰的取舍：

| 维度 | SQLite (Kanban Swarm) | 消息队列 (Kafka/RabbitMQ) |
|------|----------------------|---------------------------|
| **状态存储** | 任务状态 + 事件全量持久化 | 消息通常是短暂的（消费即删除） |
| **查询能力** | SQL 任意查询（按 status, assignee, session_id 等字段） | 有限（通常只支持按 topic/queue 消费） |
| **审计追踪** | task_events + task_runs 表原生支持 | 需要额外建存储系统 |
| **部署复杂度** | 零额外服务 | 需要独立部署/运维消息队列 |
| **跨进程协调** | 文件锁 + CAS 字段 | 原生消息传递 |

**核心论点：一板对多 Agent。** 就像车间里的物理看板——所有人都能看到它，但只有一个人在它面前操作——SQLite 文件就是那个「物理看板」。工位上的黑板只有一个，但在它面前的所有人都能看到最新状态。零依赖、事务性（WAL 模式 + `BEGIN IMMEDIATE` 保证写原子性）、CAS（Compare-And-Swap）原子认领——这些特性让 SQLite 在多 Agent 场景下意外地合适。

当然，代价是清晰的：单主机限制（不支持跨网络 board 共享），写并发瓶颈（WAL 锁序列化写操作），以及需要轮询而非事件驱动推送。这是特意为之的取舍，不是缺陷。

### 2.2 双表设计

Kanban Swarm 的存储层由两个核心表构成：

- **`tasks` 表**：存储任务的当前状态。每个任务一行，包含标题、描述、状态、Assignee、Claim Lock、优先级、依赖关系等字段。这是系统的「工作目录」——任何对任务当前状态的查询都直接读取此表。
- **`task_runs` 表**：记录每次 Worker 尝试执行任务的历史。每次 Worker 认领任务并开始执行时，都会在 `task_runs` 中创建一条记录，记录开始时间、结束时间、执行结果等。这是系统的「运行日志」。

### 2.3 任务状态机

一个任务在其生命周期中经历以下状态：

```
triage → [scheduled →] todo → ready → running → [review →] blocked/done → archived
```

（方括号表示可选分支）

- **triage**：新创建的任务，尚未被拆解或指定细节
- **todo**：已经规划但尚未就绪
- **ready**：可以开始执行，等待 Worker 认领
- **running**：已被 Worker 认领并正在执行
- **blocked**：遇到阻塞（依赖未满足、需要人工输入等）
- **done**：完成
- **archived**：归档

此外还有 `scheduled`（定时任务）和 `review`（需要审校）两个中间状态。九种有效状态构成了完整的任务生命周期。

Block 类型有五种：`dependency`（依赖阻塞，自动路由到 todo）、`needs_input`（需人工输入）、`capability`（超出 Worker 能力）、`transient`（临时异常）、`legacy/untyped`（向后兼容）。

### 2.4 轻量事件溯源：task_events

Kanban Swarm 包含一张 `task_events` 表，记录了任务生命周期中的所有关键事件：

```sql
CREATE TABLE IF NOT EXISTS task_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT NOT NULL,
    run_id     INTEGER,
    kind       TEXT NOT NULL,
    payload    TEXT,
    created_at INTEGER NOT NULL
);
```

事件类型包括：`completed`、`blocked`、`gave_up`、`crashed`、`timed_out`、`status`（拖拽变更）、`archived`、`unblocked` 等。

**但这不是完整的事件溯源（Event Sourcing）。**

与 EventStoreDB / Axon 等框架不同，Kanban Swarm 没有选择「事件作为唯一真理源」的架构。状态不从事件流中重建，而是直接从 `tasks` 表读取。事件表只用于三个目的：**审计追踪**（谁在什么时候做了什么）、**通知**（驱动 Dashboard WebSocket 和 Gateway 通知）、**诊断**（排查失败原因）。

**类比 Git**：`tasks` 表是工作目录（你看到的最新状态），`task_events` 表是 commit log（你知道谁在什么时候做了什么）。这种「轻量级事件溯源」足够支撑看板的可观测性需求，同时避免了 Event Sourcing 架构的复杂性负担——没有事件版本控制、没有事件升级（Upcasting）、没有投影（Projection）、没有快照（Snapshot）。因为 `tasks` 表本身就是物化状态。

**工程取舍的判断标准**：看板的主要价值是「任务当前的即时状态」，而非历史重建。事件的最大用途是「通知 UI 更新」和「诊断问题」。为了审计日志引入一套完整的事件溯源框架（如 Axon 需要 Java 生态 + Axon Server），对于这个场景来说是过度设计。

---

## 3. 双表面交互：人走 CLI，模型走工具

Kanban Swarm 提供了两个完全不同的交互表面：

### CLI 表面（人类操作）

通过 `hermes kanban` 子命令暴露给用户：

```
hermes kanban create        # 创建任务
hermes kanban list          # 列出任务（支持 filter/status/assignee）
hermes kanban show          # 查看任务详情
hermes kanban assign        # 指派任务
hermes kanban complete      # 完成任务
hermes kanban block         # 阻塞任务
hermes kanban unblock       # 解阻塞
hermes kanban tail          # 单任务事件流
hermes kanban watch         # 全板事件流
hermes kanban stats         # 聚合统计
hermes kanban dispatch      # 主动调度
hermes kanban daemon        # 常驻调度器
```

CLI 的定位是「人介入看板」——用户通过 CLI 创建任务、修改状态、查看进度、处理异常。这不是 Worker 的工作方式。

### 工具表面（Worker/Agent 操作）

Worker Profile 通过 Hermes 的工具系统与看板交互：

- `kanban_show` — 读取任务详情和上下文
- `kanban_complete` — 标记任务完成，附带摘要
- `kanban_block` — 标记任务阻塞，附带原因
- `kanban_heartbeat` — 发送心跳，延长 Claim TTL
- `kanban_comment` — 在任务上添加评论

Worker **只能通过工具操作看板**——它们没有 CLI 的权限。这个「双表面」设计保证了人类始终在控制回路中，而 Worker 只能在其被分配的任务上操作。

### Dispatcher 机制

Dispatcher 是 Kanban Swarm 的「大脑」——它内嵌于 Gateway 进程中，默认每 **60 秒** 执行一次 Tick。每个 Tick 完成以下工作：

1. 扫描 `ready` 状态的任务
2. 检查并发预算（`max_spawn`、`max_in_progress`、`max_in_progress_per_profile`）
3. 对于超 budget 的任务，暂缓调度
4. 分配新 Worker 进程（shell spawn 独立 OS 进程）
5. 回收超时的 Claim（TTL 过期或被放弃的任务）
6. 对失败次数达到 `failure_limit` 的任务自动阻塞（断路器）

### Claim 机制

任务认领（Claim）是 Kanban Swarm 协作的核心原语：

```
原子性加锁（CAS on claim_lock）
    → Worker 进程启动
    → 定期 Heartbeat（TTL 保活）
    → 任务完成（kanban_complete）或阻塞（kanban_block）
    → Claim 释放
    → 超时自动回收（Dispatcher 扫描 stale claims）
```

- **Claim TTL**：默认 900 秒（15 分钟）。Worker 必须在 TTL 内完成或发送心跳。
- **Heartbeat 过期阈值**：默认 1 小时无心跳视为 Worker 僵死。
- **Crash 检测宽限期**：新 spawn 的 Worker 有 30 秒保护期。
- **断路器阈值**：`failure_limit` 默认 2——连续 2 次失败后自动阻塞任务，防止重试风暴。

**协议违规检测**：如果 Worker 进程崩溃退出但没有调用 `kanban_complete` 或 `kanban_block`，Dispatcher 会在下一个 Tick 中检测到 Claim 无响应，将任务状态回退到 `ready` 并允许其他 Worker 重新认领。

---

## 4. 五角色协作模式与博客自动化流水线

Kanban Swarm 最成熟的 Dogfooding 实践是 Nous Research 内部的博客自动化流水线。这条流水线由五个角色协作完成：

```
Orchestrator（拆解）
    ↓
Researcher（调研）
    ↓
Writer（撰写）
    ↓
Reviewer（审校）
    ↓
Publisher（发布到 GitHub）
```

### parent/children 依赖链

流水线的推进不靠硬编码，而是通过 **任务依赖链** 自动实现：

1. Orchestrator 创建博客主题任务，拆解为多个子任务
2. 子任务通过 `parent_id` 关联到父任务
3. Researcher 子任务完成后，自动 promote 其依赖的下游任务（Writer）
4. Writer 子任务完成后，自动 promote Reviewer
5. Reviewer 完成后，自动 promote Publisher

每个角色都是独立的 Hermes Profile，运行在独立的 OS 进程中，通过看板（SQLite 文件）交换信息。不存在「接力棒传递」——每个 Worker 启动时读取看板上的任务描述，完成后在看板上写入结果，下一个 Worker 只需查询看板即可获得所有上下文。

### 活体案例：你正在阅读的本文

你正在阅读的这篇文章本身就是这条流水线的产出。这是一个真实的 Dogfooding 案例——Nous Research 没有为 Kanban Swarm 写一篇「外部 PR 稿」，而是让 Kanban Swarm 自己生产了一篇深度技术文章来描述自己。这种「用自己造的刀削自己的把」的实践，在工程上是最有说服力的验证。

从 Kafka 看板的实际状态来看：
- **Board slug**: `blog`（技术博客专用板）
- **当前工作流**: Orchestrator（规划）→ Researcher（你已读完调研笔记）→ Writer（当前角色）→ Reviewer → Publisher
- **五角色协作模式**: 通过 Kanban Swarm 的 parent/children 依赖链自动推进

### 竞品对比：Kanban Swarm vs AutoGen vs CrewAI vs LangGraph

这是本文的核心卖点——一张完整的横向对比表：

| 维度 | AutoGen | CrewAI | LangGraph | Kanban Swarm |
|------|---------|--------|-----------|--------------|
| **协作范式** | 对话式（GroupChat） | 任务接棒式（Sequential） | 有状态图编排（StateGraph） | **看板式（持久化队列）** |
| **状态持久化** | 无（对话上下文） | 无 | SQLite/PostgreSQL checkpoint | **SQLite 全量持久化** |
| **失败恢复** | 重试整轮对话 | 重试整个 Pipeline | Checkpoint 恢复 | **Claim 超时/崩溃自动回收，可自动重试** |
| **人工介入** | 无原生支持 | 无原生支持 | interrupt 机制 | **Comment/Unblock 全生命周期介入** |
| **跨进程** | 单进程 | 单进程 | 单进程 | **多进程（每个 Worker 独立 OS 进程）** |
| **审计追踪** | 对话日志 | 无 | Checkpointer | **task_events + task_runs 全量审计** |
| **动态拓扑** | 静态（运行前定义） | 静态 | 静态（编译时） | **动态（运行时通过 parent/child 链接）** |
| **前端可观测** | 无 | 无 | LangSmith | **Dashboard + CLI tail/watch/stats** |

**定位（选型核心论点）**：如果用工厂流水线来类比多智能体协作：
- **AutoGen** 是「会议室的 Roundtable」——大家一起讨论，但没有会议室以外的记录
- **CrewAI** 是「接力棒接力赛」——棒子从一个人传给下一个人，中间没有存档
- **LangGraph** 是「带状态机的自动化产线」——每一步都有 Checkpoint，但产线不能跨车间
- **Kanban Swarm** 是「带看板的车间」——每个工人独立工作，看板既是状态存储也是交流协议，车间主任（人类）随时可以过来查看和干预

---

## 5. Dashboard、REST API 与可观测性

Kanban Swarm 不只是一个后端框架——它提供了完整的前端可观测性工具链。

### Dashboard Plugin

作为 Hermes Desktop 的插件，Kanban Dashboard 是一个 React 组件，提供：
- **拖拽排序**：任务卡片在状态列之间拖拽，状态变更自动写入 SQLite
- **WebSocket 事件流**：新事件实时推送，UI 即时更新
- **多 Board 切换**：切换不同的 Board slug

Dashboard 是对 CLI 的补充——CLI 适合快速操作，Dashboard 适合全局概览。

### REST API

Kanban Swarm 暴露了一组 REST API，挂载在 Hermes Gateway 下：

```
GET    /api/plugins/kanban/tasks              # 任务列表
GET    /api/plugins/kanban/tasks/:id          # 任务详情
POST   /api/plugins/kanban/tasks              # 创建任务
PATCH  /api/plugins/kanban/tasks/:id          # 更新任务
DELETE /api/plugins/kanban/tasks/:id          # 删除任务
```

这意味着任何 HTTP 客户端（包括外部服务、SDK、CI/CD 管道）都可以与 Kanban Swarm 交互。

### 可观测性工具链

除了 Dashboard，CLI 提供了强大的可观测性能力：

- **`tail <task_id>`**：查看单个任务的事件流（类似 `tail -f`），实时追踪任务的生命周期
- **`watch`**：监听全板事件流，任何状态变更都会实时显示
- **`stats`**：聚合统计（各状态任务数、各 Profile 任务分配、完成率、失败率）
- **`runs`**：查看 Attempt 历史，追溯每次 Worker 执行的详细信息

### Gateway 通知

Gateway 内置了 notifier watcher，每 5 秒轮询 `task_events` 表中的新事件，并将事件推送到配置的通知渠道（Telegram / Discord 等）。通知失败超过 3 次（`MAX_SEND_FAILURES`）后自动取消订阅。

---

## 6. 适用场景、局限性与实践指南

### 最佳实践

Kanban Swarm 在以下场景中表现最佳：

1. **内容生产流水线**：博客文章、技术文档、新闻稿——多角色接力协作，人工审核介入
2. **代码审查与工程流水线**：Decompose → 并行 Worktree 实现 → Review → Iterate → PR
3. **研究任务**：并行 Researcher + Analyst + Writer，可随时人工介入调整方向
4. **批量数据处理**：多个 Worker 并行处理独立的数据分片，Dispatcher 统筹并发
5. **定时运维**：每日简报、系统检查——跨周累积为日志

### 局限性

- **单主机 SQLite**：不支持跨主机 board 共享。Worker 和 Dispatcher 必须在同一台机器上共享文件系统
- **写并发瓶颈**：WAL 模式虽然提高了读并发，但写操作仍然被序列化。高并发场景（尤其是多 Gateway 同时写入）需要谨慎设计
- **没有消息推送**：Worker 需要通过轮询来发现新任务，而非事件驱动推送

### 配置清单与调优建议

| 场景 | 参数 | 建议值 | 说明 |
|------|------|--------|------|
| **断路器阈值** | `failure_limit` | 2（默认） | 连续 2 次失败后自动阻塞任务，防止重试风暴 |
| **Worker 超时回收** | `dispatch_stale_timeout_seconds` | 14400（4 小时） | Worker 无进度超时，超时后回收任务 |
| **并发 Worker 上限** | `max_spawn` | 10（建议值） | 全局并发 Worker 数量上限 |
| **全板运行任务上限** | `max_in_progress` | 50 | 全板同时处于 `running` 状态的任务上限 |
| **单 Profile 并发限制** | `max_in_progress_per_profile` | 5 | 防止某个 Profile 占满所有 Worker 槽位 |
| **自动拆解** | `auto_decompose` | true | Dispatcher 自动对 triage 任务执行拆解 |
| **每 Tick 拆解上限** | `auto_decompose_per_tick` | 3 | 防止批量导入时爆发式 LLM 调用 |
| **Claim TTL** | `DEFAULT_CLAIM_TTL_SECONDS` | 900（15 分钟） | Worker 认领任务的有效时间 |
| **Heartbeat 过期阈值** | `DEFAULT_CLAIM_HEARTBEAT_MAX_STALE_SECONDS` | 3600（1 小时） | 超过此时间无心跳视为 Worker 僵死 |

**调优建议**：
- **高并发 Worker 农场**：`max_spawn` 调至 20-50，但注意机器 CPU/内存
- **长时间研究任务**：`DEFAULT_CLAIM_TTL_SECONDS` 调至 3600+，避免任务中途被回收
- **断路器敏感的生产任务**：`failure_limit` 设为 1，一次失败即阻塞，人工审查后放行
- **容忍偶发失败的批处理**：`failure_limit` 设为 3-5，允许更多自动重试
- **快速故障检测**：`dispatch_stale_timeout_seconds` 设为 1800（30 分钟）

---

## 7. 结论

### 核心价值：三根支柱

Kanban Swarm 的核心价值可以归结为三个关键词：

1. **持久化（Persistence）**：任务状态、运行历史、审计事件全部持久化在 SQLite 中，Agent 重启、崩溃、跨会话都不会丢失进度。这是与 `delegate_task`（函数调用）最本质的区别。
2. **可观测（Observability）**：从 Dashboard 到 CLI tail/watch/stats，从 REST API 到 Gateway 通知——Kanban Swarm 提供了完整的多视角可观测性工具链。
3. **事件驱动（Event-Driven）**：事件表驱动了通知、Dashboard 实时更新、以及未来所有扩展的入口。

### `delegate_task` vs Kanban：函数调用 vs 工作队列

这是 Hermes 中两个协作原语的明确分工：

| 维度 | `delegate_task` | Kanban |
|------|----------------|--------|
| 形态 | RPC 调用（fork → join） | 持久化消息队列 + 状态机 |
| 父进程 | 阻塞直到子进程返回 | Fire-and-forget（创建后即返回） |
| 子进程身份 | 匿名子 Agent | 命名 Profile，带持久化记忆 |
| 可恢复性 | 无——失败即失败 | Block → unblock → 重执行；崩溃 → 回收 |
| 人工介入 | 不支持 | 随时 Comment/Unblock |
| 审计轨迹 | 随上下文压缩消失 | SQLite 中持久化，永不丢失 |
| 协调模式 | 层级式（调用者 → 被调用者） | 对等式——任何 Profile 可读写任何任务 |

**一句话区分**：`delegate_task` 是函数调用；Kanban 是工作队列。每个交接都是一行记录，任何 Profile（或人类）都能看到和编辑。

### 未来展望

Kanban Swarm 是 Hermes Agent 多智能体协作的第一版实现。从架构设计和代码结构中可以预见的发展方向：

- **工作流模板（v2）**：将八种规范协作模式（官方 Spec 定义）抽象为可复用的工作流模板，让用户通过 YAML 定义协作拓扑
- **多主机支持**：目前 SQLite 的单机限制是最明显的短板，未来可能引入同步层或迁移到嵌入式网络协议
- **更丰富的事件驱动集成**：事件表作为统一消息总线，与外部系统（CI/CD、Slack、邮件）深度集成
- **可视化工作流编辑器**：在 Dashboard 中拖拽搭建协作拓扑，而非通过代码/配置定义

---

## 参考资料

| 来源 | 链接 |
|------|------|
| Hermes Agent 官方文档 | https://hermes-agent.nousresearch.com/docs/ |
| Kanban 特性文档 | https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban |
| Kanban 教程 | https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban-tutorial |
| Hermes Agent GitHub 仓库 | https://github.com/NousResearch/hermes-agent |
| Nous Research Discord | https://discord.gg/NousResearch |
| 本地源码路径 | `C:\Users\admin\AppData\Local\hermes\hermes-agent` |
| 设计文档 | `docs/hermes-kanban-v1-spec.pdf`（本地 Spec） |

---

*本文由 Hermes Agent Kanban Swarm 博客流水线自动生成。当前会话即为 Dogfooding 活体案例：Orchestrator → Researcher → Writer → Reviewer → Publisher。*
