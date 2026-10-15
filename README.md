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

新增 [native-v4 原生长度候选](docs/native-engine-v4.md)：模型能力与任务快照绑定，
供应商上下文拒绝和成功回执按新协议落盘；旧引擎保留。匹配 Worker 的独立包联调已通过，正式安装仍待完成。

新增 budget-v3 候选提供持久化的预发送上下文拒绝、最终请求与快照完整绑定，以及
从模型原回执派生的用量 envelope。空/异常 usage 保留预算预留，家族总数不会整数
回绕；这一计量纠错也作用于旧任务重新统计，但不改旧回执和 v1/v2 回复。
候选尚未安装，契约、兼容影响和验收边界见 [budget-v3](docs/budget-engine-v3.md)。

- 中台 `/v1/runs/{id}/calls/page` 默认每页 25、最多 50 条摘要，未决调用独立过滤；游标绑定任务和过滤条件，列表不是数据库快照。
- `/calls/record` 按操作、字段、Unicode 偏移和内容版本读取，默认 8,192、最多 16,384 字符；版本改变返回 409，SQL NULL 与 JSON null 分开。旧完整 Calls 接口保留兼容。
- 应用 `/v1/apps/{app}/runs/{id}/evidence` 仅列该任务及子任务的成功 `document_read` 回执，最多 128 个可见任务，默认 20/最多 30 条；单条正文最多 8,192 字符。来源是当时实际读取片段，不是当前 Knowledge 查询；外部采集工具暂不纳入此列表。
- 部署配置 `APP_RECEIPT_EXPORTS` 可明确允许应用读取特定工具的两个字符串字段。默认关闭；每项指定 app、id、tool、input_path、output_path，客户端不能更改路径或工具。`/v1/apps/{app}/runs/{id}/receipts/{profile}` 仅列本任务树已完成的工具回执；`/record` 按 input/output、操作、Unicode 偏移与版本分页。配置及输入/输出共同绑定版本，分别给出原字符串 SHA-256；不开放其他参数、模型消息或凭证，也不改变任务完成状态。业务解释由应用负责。历史 DSH/Worker 执行协议不变。
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

新增 Mac 原生故障测试 `runtime/disk_full_darwin_test.go`，仅由集成仓库的有界卷控制器显式启用。它在新 PostgreSQL 集群中通过 Owner 迁移与 NOLOGIN/SET ROLE 角色池，核对笔记及 native 模型回执在真实 ENOSPC 下的原子性、同 PGDATA 恢复前留存和自然 fence 接管；模型为合成回执，归档为内存夹具，没有独立 Runtime/Worker/Node/Storage 进程。执行版本、首轮失败、实际结果与清理见 [Mac Owner 故障记录](https://github.com/euler-gaozl/agent-os/blob/feat/end-to-end-delivery/docs/runtime-enospc-macos-20261015.md)。此切片不删除旧覆盖，不证明独立登录、Compact、完整生命周期、Windows 或全平台资源验收。

Windows 正式安装以 [集成计划](https://github.com/euler-gaozl/agent-os/blob/main/docs/windows-single-machine-plan.md) 为准。当前仍需修复真实研究中的上下文拒绝、预算及输出上限问题，完成内存边界、UI、剩余权限退役和最终故障验收。容量与跨主机高可用尚未验收，Actions 日常关闭。

新增 `runtime/telemetry_delivery_test.go` 将事件投递的 Runtime Owner 覆盖留在本仓：使用新建隔离 PostgreSQL 数据库、Owner 迁移与 NOLOGIN/SET ROLE 的 Runtime 角色，经真实 loopback HTTP 和正式服务身份策略，检查低序号事件晚提交不会漏投、活跃租约不被抢占、原有 30 秒租约自然到期后 fence 接管、旧确认拒绝、整批确认原子性及重复确认幂等。返回只含事件身份、类型、时间和 fence，不导出 payload；原事件内容始终保留。测试不 SQL 改写租约，不导入 Telemetry 实现，也不验证其 inbox、导出器、真实收集端或独立账号登录；旧集成覆盖在对应验收完成前继续保留。

定向入口为 `go test -mod=vendor -race -count=1 ./runtime -run '^TestTelemetryOwnerHTTP' -timeout 90s`，需要显式提供 `TEST_DATABASE_URL`；未提供时按既有夹具跳过，不能记为数据库验收通过。自然到期测试约需 30 秒。

2026-10-15 维护者 macOS 15.3/arm64 使用 Go 1.27.2、真实隔离 PostgreSQL 18.6 运行上述两项，race 检查通过、零跳过，包耗时 31.795 秒；自然租约场景 30.15 秒。`go vet ./runtime` 和 `go build ./...`（均 `-mod=vendor`）通过。证据位于本机 `local/evidence/macos-telemetry-owner-20261015`，不代表真实 Telemetry/Langfuse 联调；Runtime 生产代码和已安装程序未改变。

新增 `runtime/notes_native_test.go` 将笔记修订/撤回的 native-v4 Owner 组合留在本仓。测试通过正常 Submit/Claim 创建任务，用 Owner 迁移后的 Runtime/Worker NOLOGIN/SET ROLE 连接池执行真实 PostgreSQL 事务；每个任务使用独立内存归档和合成 native 模型回执建立计划，不改引擎、任务快照、租约或笔记 SQL 状态来促成恢复。

生命周期场景检查带未来 `valid_until` 的修订先可用、自然过期后不让旧版本重新生效；修订已提交但回复丢失后，经正常 Heartbeat 和自然到期 Claim 取得新 fence，重放模型/工具回执保持当时的有效视图，新 `document_read` 则显示当前已过期。随后显式续订和撤回，核对原文、来源、metadata、版本链、操作审计、完成回执及旧 fence/篡改重放拒绝。撤回后的新修订是已完成的 conflict，不能隐式复活。

并发场景让两个同 scope 的不同 native 任务分别执行 revise/revise 和 revise/retire；只持锁的事务与 `pg_blocking_pids` 证明两方实际等待同一根锁，再释放竞争。必须恰好一方修改，另一方收到 `receipt.committed=true` 的明确 conflict；回放两方都不再调用效果或增加文档、审计、回执。测试不把“一个 goroutine 报错”当成已知冲突验收。

定向入口：`go test -mod=vendor -race -count=1 ./runtime -run '^TestNativeNote(LifecycleReplyLossExpiryAndRetirement|ConcurrentHeadConflicts)$' -timeout 120s`。需要显式提供 `TEST_DATABASE_URL`；未提供时按既有夹具跳过，不能记为数据库通过。两个顶层测试，其中并发测试有两个子场景；自然有效期等待约 8 秒。2026-10-15 在独立 macOS 15.3 arm64 / Go 1.27.2 / PostgreSQL 18.6 上实际执行：2 个顶层测试、2 个并发子场景全部通过，零 skip，race 包耗时 10.533 秒，命令 exit=0；`go vet -mod=vendor ./runtime` 也通过。详细版本、故障范围及清理证据归主仓本次 Mac 增量记录。

这是 Owner/native-v4 协议及真实 PostgreSQL 事务覆盖；合成模型 usage 不是供应商消耗或费用，内存归档不是实际 Storage 持久化，SET ROLE 不是独立账号登录。没有执行官方 Harness/独立 Worker 进程、管理浏览器或 Windows 验收。旧生命周期、权限、历史分叉及集成故障覆盖继续保留，不能据此关闭完整 W09/P2.5。
