# Agent OS Runtime

Agent 执行数据域的独立服务，负责任务状态、领取租约/fence、模型与工具回执、DSH 交互证据、工作状态、笔记和恢复。Worker 独立部署并消费固定 `execution` 模块；Runtime 不复制 Registry、Knowledge、Storage 或 Sandbox 的数据库逻辑。

## 分层与事务边界

| 包/进程 | 职责 |
| --- | --- |
| `runtime` | 提交/管理 API、工具治理、执行效果、笔记、事件投递、唯一迁移入口 |
| `internal/runstate` | 执行事务、证据确认、检查点；没有 HTTP 服务或工具实现 |
| `execution` | `runtime-execution-v1` 窄接口；不暴露数据库、事务、SQL、DDL 或本地工具调用 |
| 独立 Worker | 领取、Node 管道、模型/ToolHub HTTP 与历史引擎兼容 |

`Exchange` 将规范化意图和首批事件一起提交，在有效 fence 下确认不可变证据后才执行外部模型/工具调用。归档屏障失败阻止外部动作；已提交回执重放不重复效果，回执已提交但检查点失败可恢复。模型账本必须提交后才能返回成功。

笔记、子任务、工具回执和事件保持原子性。远端配置缺失时拒绝执行，不回退跨服务 SQL；外部写入未知时必须核对，丢回复不代表未执行。

## 工作状态与固定 Harness

原始任务和当前用户约束保存在 `runs`，与推导的工作集分开。`run_working_states` 追加记录问题、观察、计划、第三方预测、冲突和下一步，旧约束产生的工作集明确标为过期。

工作状态更新要求活跃租约、owner/fence、匹配约束修订、预期版本 CAS 和任务内操作 ID。状态、稳定回复和元数据审计原子提交；同 ID 重试返回原回执，换载荷拒绝。工作集最多 32 KiB，拒绝重复/未知 JSON 字段，每项观察关联同任务 1..8 条已提交工具回执。关联与哈希证明可回查，不证明观察正确。

固定源码引擎 `dsh-source-639ed015-state-v1` 经 Worker 身份调用 `POST /v1/source-state`；Runtime 冻结每次读、成功或确定性拒绝的回复，核验 staged intent 和已确认归档。取消/过期 owner 不得再写，新 owner 可重放已提交回复。历史引擎保持原协议。

resident-v2 是独立候选，需明确版本选择、匹配 Worker 编译 pin、完整发布/回滚门槛；不能因源码可编译就替换未结束任务。`execution.New` 仅认历史引擎，`NewSourceEnabled` 仅加入 state-v1，`NewSourceVersions` 接受经过固定产物校验的明确源码版本。新根由部署配置选择，幂等重试和子任务保留最初引擎。

中台只读 `GET /v1/runs/{id}/working-state` 可查询当前/历史状态或操作回执。源码恢复由 control 身份读取 `/source-recovery`，再调用 `/resume-source-state`；动作会重新加锁校验 `waiting_unknown`、最新已确认状态交互、无取消和无其他未决调用。预览许可不能越过随后发生的取消。

## 回执、证据与 MCP

新增 budget-v3 候选提供持久化的预发送上下文拒绝、最终请求与快照完整绑定，以及
从模型原回执派生的用量 envelope。空/异常 usage 保留预算预留，家族总数不会整数
回绕；这一计量纠错也作用于旧任务重新统计，但不改旧回执和 v1/v2 回复。
候选尚未安装，契约、兼容影响和验收边界见 [budget-v3](docs/budget-engine-v3.md)。

- 中台 `/v1/runs/{id}/calls/page` 默认每页 25、最多 50 条摘要，未决调用独立过滤；游标绑定任务和过滤条件，列表不是数据库快照。
- `/calls/record` 按操作、字段、Unicode 偏移和内容版本读取，默认 8,192、最多 16,384 字符；版本改变返回 409，SQL NULL 与 JSON null 分开。旧完整 Calls 接口保留兼容。
- 应用 `/v1/apps/{app}/runs/{id}/evidence` 仅列该任务及子任务的成功 `document_read` 回执，最多 128 个可见任务，默认 20/最多 30 条；单条正文最多 8,192 字符。来源是当时实际读取片段，不是当前 Knowledge 查询；外部采集工具暂不纳入此列表。
- `run_recall` 列表最多 10 条，响应公开实际 `page_size` 和游标；操作正文保持 1,200 字符上限，非零偏移要求操作 ID。
- MCP 握手使用短预算，执行传输响应头上限 35 秒、总计 40 秒，使 Sources 的 30 秒采集可返回；保留取消、网络策略和 4 MiB 响应界限。业务 `isError` 作为工具回执保存，传输失败不能伪造“无效果”回执。

回执分页能力 `call-receipts-v1` 要求先升级 Runtime，再升级依赖它的 Console；回滚 Console 在先。读回执不证明事实正确、完整来源已阅读或研究验收通过。

## 构建、迁移与部署

需要 Go 1.27.2、PostgreSQL 18，SDK 与 `vendor` 固定，不依赖兄弟仓库。

```powershell
$env:GOWORK='off'; $env:GOPROXY='off'
go test -mod=vendor ./...
go vet -mod=vendor ./...
./scripts/Build.ps1
```

生成 `runtime`、`migrate`、`retire-access`、`healthcheck`。按 [配置示例](deploy/service.example.json) 在仓库外提供运行配置；Docker 非 root。连接池默认 3、最少 2，就绪检查只验证本域，不证明模型或下游可用。

先创建 Runtime/Worker 两个不同应用账号，用独立 DDL 连接 `MIGRATION_DATABASE_URL` 执行 `migrate --role <runtime账号> --worker-role <worker账号>`。迁移保留 public 表位置和锁 8843921，本域结构、触发器与授权原子提交；正常启动不做 DDL，没有自动反向迁移或旧授权撤销。

Worker 新账号只访问六张执行表和事件序列，不读写工作状态表、笔记或其他数据域。旧单仓 Worker readiness 曾读更广的 Runtime 表，回滚窗口内需保留其 SELECT，不能把新最小权限账号当作旧程序兼容证明。

`retire-access --role <runtime账号> --worker-role <worker账号> --foreign-roles <逗号分隔账号>` 是单独协调的权限收缩作业。先验证可还原备份、阻断新连接、停消费者并核对真实程序/数据库绑定；事务用 RESTRICT 处理本域，拒绝危险继承、PUBLIC、默认 ACL、活跃会话等情况。COMMIT 回复未知时保持停机、只读核对实际授权；不把客户端错误当成事务回滚，也不自动恢复广泛权限。

## 验证与未完成项

真实隔离 PostgreSQL 检查覆盖并发领取、fencing、UNKNOWN、约束修订、家族预算、笔记/回执原子性、工作状态 CAS、迁移和权限拒绝。实际 Windows Worker/Node 中断接力、丢 HTTP 回复、归档超过 4 MiB 后回查也有通过记录；这些场景的模型、ToolHub 或 Storage 部分使用明确夹具，不等于付费模型或存储落盘验收。

`TEST_DATABASE_URL` 指向可创建隔离夹具的测试连接；旧/新 Worker 可通过 `TEST_OLD_WORKER_BINARY`、`TEST_NEW_WORKER_BINARY`、对应 DSH runner 与 `AGENTOS_NODE` 执行可选接力测试。详情、原始失败、通过数与跳过项保留在 [历史验收记录](docs/history/service-notes-20261014.en.md)。

Windows 正式安装以 [集成计划](https://github.com/euler-gaozl/agent-os/blob/main/docs/windows-single-machine-plan.md) 为准。当前仍需修复真实研究中的上下文拒绝、预算及输出上限问题，完成内存边界、UI、剩余权限退役和最终故障验收。容量与跨主机高可用尚未验收，Actions 日常关闭。
