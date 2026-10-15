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

新增 `runtime/source_state_native_recovery_test.go` 验证现有 native-v4 工作状态恢复 HTTP 边界。通过正常 Submit/Claim、真实隔离 PostgreSQL 与正式 HTTP 身份策略，检查工作状态读/更新已提交但调用方丢弃成功回复后进入 `waiting_unknown`；只有 control 可以预览和显式恢复。约束修订后，旧回执仍重放当时视图，接管后的新读取看到当前约束；重复恢复、旧 fence、取消后的恢复及未完成调用均被拒绝。模型回执与归档使用合成/内存夹具，不 SQL 改写运行状态来促成恢复。

定向入口：`go test -mod=vendor -race -count=1 ./runtime -run '^TestNativeSourceStateHTTP(Recovery|CancelAfterPreview|UnresolvedAndArchive)$' -timeout 120s`。必须提供本机隔离测试的 `TEST_DATABASE_URL`；未提供时的 skip 不算通过。2026-10-15 在维护者 macOS 15.3 arm64 / Go 1.27.2 / PostgreSQL 18.6 实际执行，3 个顶层测试、4 个子场景全部通过，零 skip，race 包耗时 2.427 秒，exit=0；`go vet -mod=vendor ./runtime` 通过。清理后夹具数据库、角色及连接均为 0。

此测试的回复丢失由 HTTP 成功返回后主动丢弃回调结果模拟，不是 TCP 丢包或独立 Worker 重试。归档失败场景实际停在 `failed/not_waiting`，未覆盖 `archive_unconfirmed` 原因。没有执行官方 Node/独立 Worker/实际 Storage、真实供应商、浏览器或 Windows 验收，也没有实现新的通用续跑策略；生产代码、固定引擎及已安装 Runtime 均未改变。

## 失败任务只读接续预览

新增 control 专用 `GET /v1/runs/{id}/continuation`，协议为 `failed-run-continuation-preview-v1`。响应固定 `read_only=true`、`action_available=false`；`same_run_candidate` 只是本次持久化记录符合已知条件，不是续跑许可。此接口没有执行动作，不调用旧 `/resume`，不改变已有任务、固定预算、Snapshot、引擎、Worker pin 或 execution 协议。

首版只正向评估 native-v4 根任务：任务为 failed、没有取消或族内未决工具/模型、子任务已终态；整个冻结 Snapshot 的 native 能力和有限预算有效；exchange 序列完整，较早帧都有冻结回复，所有帧已有归档确认；最后帧是未冻结回复的 model，规则修订一致。该帧的 call 和当前 attempt 必须均为 failed，请求与回执一致，保留的完整 `NativeReceipt` 为 HTTP 503，原请求与冻结 input/模型配置一致。重试仍是同一帧，所以 `model_rounds <= MaxSteps`；全族全部状态的模型尝试数必须小于根任务原 `MaxModelAttempts`，不增加旧额度。

取消和 completed 优先分类。failed/waiting_unknown 会检查真实根及全族的 unknown/inflight 工具效果，其后 waiting_unknown 返回核对要求；其余活动状态直接返回 `not_applicable/not_failed`。历史/未知引擎、失败子任务、格式异常、未确认归档、非白名单失败等按证据返回 blocked 或 manual_review。manual_review 不表示必须新建任务，也不表示旧引擎不可恢复。业务不适用结果均为 HTTP 200；缺少运行为 404，内部读取失败/超时为通用 503，不回显 SQL/回执。仅 control GET 可访问；参数覆盖和未知查询均拒绝，响应禁止缓存。

读取使用一个 10 秒有界 `REPEATABLE READ READ ONLY` 事务；元数据聚合不加载整族模型正文。Snapshot、最后冻结 input 和最后 call request 在 SQL CASE 中按 native 32 MiB 限额控制传输，receipt envelope 最多 64 MiB 且解码后的 body 仍按 native 32 MiB 校验；超限返回人工核对。JSONB 文本格式按保守字节界限判断，极限记录可能需要人工核对。任何后续执行接口都必须重新加锁核验当前状态和预算；不能把 observed_at/assessment 当授权或可靠费用估计。

本预览只验证已保存的归档确认元数据，不读取实际 Storage 字节，不探测当前 Registry、工具治理、供应商或费用。在只读预览提交时，旧 `/resume` 语义保持不变，Console/Futures 尚未接入；显式动作及 UI 的后续源码进展见下节。预览本身仍不执行续跑，自动接续、配置变更后的新任务承接和完整 UI 验收仍未完成。此处实现说明与下面实际验证结果应分开记录，不能把可编译候选记作真实业务验收。

定向入口：`go test -mod=vendor -race -count=1 -timeout 180s ./runtime -run '^(TestContinuationPreview|TestRuntimePeerBoundaries$)'`，必须提供本机隔离的 `TEST_DATABASE_URL`。2026-10-15 在维护者 macOS 15.3 arm64 / Go 1.27.2 / PostgreSQL 18.6 上实际执行：6 个新增顶层测试及 1 个既有身份策略测试、17 个子场景全部通过，0 fail/skip，race 包完成事件耗时 4.327 秒，命令 exit=0；`go vet -mod=vendor ./...` 与 `go build -mod=vendor ./...` 均 exit=0。

覆盖同帧最后轮次、全族尝试预算、真实未知工具路径、取消、未确认归档、约束修订、非 503 回执、未决模型、活动子任务、非模型末帧，以及明确标记的损坏账本反例。并发测试在第一次读取建立快照后，通过既有显式 resume/Claim 与合成失败回执推进另一方，证明首次读取保持一致快照，随后新读取看到预算耗尽；预览自身不执行 resume。测试还比较 12 张表的内容摘要及调用计数，拒绝外部 HTTP/归档访问。

以上是正式 Owner HTTP、真实 PostgreSQL 事务与合成模型回执测试，使用 NOLOGIN/SET ROLE 和内存归档；不等于独立账号登录、官方 Node/独立 Worker/实际 Storage 或真实供应商验收。没有穷举所有旧引擎、超大记录和损坏亲缘组合。夹具清理后数据库、角色及连接均为 0，10 个已有服务在测试前后身份与文件摘要不变。代码及执行结果已有独立评审；安装结果另记主仓本次 Mac 增量。未运行浏览器、Windows 或 GitHub Actions，未新增付费模型调用。

## Native 模型尝试与约束修订

native-v4 的 BeginModel 在运行行锁内、已完成回执缓存返回之后、预算检查和新 attempt 之前，比较冻结 model exchange 与当前运行的约束修订。旧修订的未完成请求返回普通错误，任务按既有失败路径落盘，不新增尝试、usage 或 model_started 事件；不会把规则变化伪装成租约丢失。原请求、快照、历史回执与官方 DSH 协议不变。

成功回执尚未冻结时仍可从缓存恢复；旧结果保留旧 PlanRevision，不能据此完成新约束下的任务。约束更新先于 BeginModel 准入提交时，旧请求不再获准；调用已获准后才更新约束，仍须保留可能在途的发送、费用与回执。

2026-10-15 独立 Mac（macOS 15.3 / arm64，Go 1.27.2，隔离 PostgreSQL 18.6）用正常 API、内存归档和合成回执复现了两个旧代码反例，并验证修复、成功缓存重放及同修订重试；相邻 native、预算、约束、取消和回执回归通过。它们不是供应商或业务验收。详细 Mac 证据见主仓维护知识库的后续增量。

Worker 直接运行自己编译的固定 execution 模块，因此仅更新 Runtime HTTP 服务不能覆盖本检查。现有 rc.5 Worker 需显式更新不可变模块 pin、vendor 和实际二进制后再做联合验证；不得保持 rc.5 名称而静默修改 vendor。在该执行库修复提交时，新 continuation 写动作及 UI 仍未实现。本段提交时记录的是执行库修复，Worker 部署结果须以独立后续证据为准。

新增可选的 `TestIndependentNativeWorkerRevisionGuard`（macOS/Linux）使用独立旧、新 Worker 二进制、固定官方 native-v4 Node runner、真实 Runtime HTTP 与隔离 PostgreSQL。显式提供 `TEST_DATABASE_URL`、`TEST_OLD_WORKER_BINARY`、`TEST_NEW_WORKER_BINARY`、新包绑定的 `TEST_NEW_WORKER_SHA256`/`TEST_NEW_WORKER_RUNTIME_VERSION`、`TEST_NATIVE_DSH_RUNNER` 和 `AGENTOS_NODE` 后运行 `go test -mod=vendor -race -count=1 -timeout 240s ./runtime -run '^TestIndependentNativeWorkerRevisionGuard$'`；未配置时的跳过不算通过。

2026-10-15 在上述 Mac 环境实际执行，三个子场景全部通过、零跳过：旧 rc.5 在约束修订后发送两次；固定到 `1f6c4c29c62bac3ad6d674b7797a7410f373bf00` 的新 Worker 只保留首次 503、拒绝新增尝试；同修订的新 Worker 重试正常完成，首次未知用量仍保留。首次测试因 JSONB 读回改变对象键序而有两项原始字节比较失败，原记录保留；修订测试比较完整 JSON 值与冻结意图、调用及所有尝试，并另存原始字节哈希，没有更改生产代码或二进制来通过测试。

模型为有界 loopback TLS 服务，归档为 HTTP 夹具，未调用真实供应商或实际 Storage。六个临时 Worker 正常退出，夹具数据库、角色和连接均已清理；十个既有服务在测试前后保持不变。此测试不证明 Windows、真实业务、完整发布包或后续安装成功，实际部署组合另记主仓 Mac 增量。

## 显式接续请求与持久化回执

control 可读取 `GET /v1/runs/{id}/continuation/request-context`，协议 `failed-run-continuation-action-context-v1`。只有本次判定为 `same_run_candidate` 时才返回 `failure_version`；它绑定任务、引擎、规则修订、fence、最后帧和当前 attempt，不是授权令牌或预算预留。原 `GET /continuation` 的 `failed-run-continuation-preview-v1`、`read_only=true` 和 `action_available=false` 保持不变。

显式动作是 `POST /v1/runs/{id}/continuation`，请求仅包含 `request_id`、`expected_failure_version` 两个字符串。客户端必须在第一次发送前保存请求 ID 和请求体，在结果未知时保留同一对值。请求 ID 为 1–128 位 ASCII 字母、数字、`_` 或 `-`，版本为 64 位小写十六进制；JSON 最大 1 KiB，拒绝重复、未知、大小写别名、空值和多个 JSON 值。新管理接口要求命名的 control 身份；共享单 token 模式不授予这些动作。HEAD、查询参数及其他服务身份均拒绝。

Owner 事务先锁请求 ID，先找旧回执；同 ID、同目标和版本重放原结果，同 ID、不同内容返回 409 `request_id_conflict`，不改变旧回执。新请求按父先顺序锁完整原生根任务族，允许正向处理最多 128 个任务；检测容量溢出可锁到第 129 个哨兵行，返回已记录的人工复核拒绝。读取并关闭全部行后取得模型预算锁 35，再在 READ COMMITTED 新语句快照中复用原只读判据。只允许完整 503 回执、原轮次内、原全族尝试预算未耗尽、规则与冻结请求一致且没有未知效果的失败根任务。子任务和历史引擎的动作返回明确人工复核；缺迁移表、SQL/连接故障或超时返回 503；缺失或不可信的业务证据则按原预览判据记录人工复核拒绝。

成功在同一事务中把 failed 变为 queued、清空旧 owner/lease，并追加 `continuation_queued` 事件及回执，返回 202。预算、快照、协议、任务状态正文、轮次、既有调用及尝试均不改；不会在该请求中调用模型。确定拒绝返回持久化的 409。首次返回和重放均取数据库保存的同一 JSONB 响应，避免键序差异。回执协议 `failed-run-continuation-receipt-v1` 的 queued/rejected 描述历史决定，不描述任务当前状态。

`GET /v1/runs/{id}/continuation/requests/{request_id}` 返回 200 和原回执正文，即使原 POST 为 409；404 仅说明本次未观察到匹配回执，不能证明并发提交没有成功。内部错误或 COMMIT 回复未知均为通用 503，不生成虚假的“已拒绝”回执。客户端应查询回执或使用同一请求 ID 和原请求体重发，不能换新 ID 猜测补偿。三个新处理器返回禁止缓存的响应，不回显冻结请求、供应商回执或 SQL。

原生引擎的旧 POST `/resume` 现在锁内返回 409 `explicit_continuation_request_required`；历史引擎的旧行为保持。`/reconcile`、`/resume-source-state` 和执行协议不变。Console 同期源码加入原生任务的显式动作、浏览器请求指针及回执恢复；Futures 尚未接入此管理动作。实际迁移部署、浏览器交互及自动接续仍未完成，不能把这组 Owner 接口算作完整自动接续。

新增 Owner 表 `run_continuation_receipts`：全局请求 ID 主键，任务外键，请求哈希、预期失败版本、原 HTTP 状态、最多 8 KiB 的 JSONB 回执及时间，另有任务查询索引。Runtime 仅被授予 SELECT/INSERT，Worker 无新增权限；普通迁移保持只扩展，不回收已有异常授权，不做 TTL 清理。表加入迁移和 readiness，也加入原 `runtimeTables` 权限退役清单；`workerAccess` 无此表，因此现有显式退役路径撤销并验证 Worker/foreign 无权限。没有新增退役执行或权限收缩步骤。回执保留且任务外键阻止绕开回执直接删除任务，后续存储生命周期需把该永久历史一并纳入。

启用前必须确认所有能领取原生队列的 Worker 已含 `1f6c4c29c62bac3ad6d674b7797a7410f373bf00` 的新尝试规则守卫，完成 Owner 迁移、独立数据库/并发/权限测试和实际二进制验证。仅更新 Runtime HTTP 不会替换 Worker 内执行库。约束变化的线性化点仍是 BeginModel 锁内准入：准入前已提交的修订阻止旧意图新增尝试；准入后发生修订不能承诺消除已经在途的供应商请求，必须留存账单和回执。本切片不另改已明确升级的 Worker pin。

2026-10-15 Mac 源码验证：macOS 15.3 arm64、Go 1.27.2、隔离 PostgreSQL 18.6。新增动作及关联预览/规则守卫定向测试 `-race` 通过 19 顶层、42 子场景，零跳过；其中新增 Owner/HTTP/权限事务组为 7 顶层、11 子场景。正常提交/领取/失败构造覆盖最后轮次重试、旧效果不重复、丢 HTTP 回复、同/不同 ID 竞争、取消、约束、Claim、锁超时、回滚、族预算和只读表摘要。没有用 SQL 改状态或租约制造成功。

独立编译的旧/新 Worker、固定官方 Node Harness、实际 Runtime HTTP/PG 与合成 TLS 模型夹具通过 1 顶层、3 子场景：旧 pin 在约束变更后仍有 2 次请求，新 pin 仅 1 次；约束不变时新 pin 合法重试并完成，共 2 次。此处动作先在 R1 排队，再改 R2 后启动第二进程。完整请求按 JSON 语义与冻结意图/每次账本绑定，原始字节差异仍保留。实际供应商调用为 0；这不是丢 PostgreSQL COMMIT 回复、真实供应商、业务研究质量或浏览器验收。安装中的 Runtime HTTP、现存任务与数据库尚未切换。


同轮扩大回归因新增迁移/权限表与旧管理入口变化而执行一次：Runtime 全包测试选择 `^Test`，112 顶层、195 子场景通过，27 项按各自配置条件明确跳过，零失败；其中独立 Worker 的 3 场景已在上述单独配置的运行中通过，不能把剩余专项跳过算通过。internal/runstate 的离线纯测试、全模块 vet/build 也通过。10 个现有实例的 PID、记录、二进制及配置哈希与基线完全一致；临时数据库、角色、连接均为 0，6 个夹具 Worker PID 已退出。
