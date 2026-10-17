# 回执引用交付 final-v5 候选

2026-10-17：Owner 事务实现、本机隔离 PostgreSQL 及独立 Worker 完整包联调通过，**尚未正式安装或启用**。固定 SDK `v0.1.0-rc.4`，使用标准 Go 模块下载和 vendor；不依赖相邻源码或 local replace。当前正式任务仍按原固定引擎执行。

后续已补齐 final-v5 模型 HTTP 503 的显式接续：沿用同一冻结任务、步骤/尝试预算、父优先家族锁、原生模型意图核验和永久接续回执。策略树必须仍有合法策略且工具绑定有效，不能用当前 Registry 替换旧快照；允许根无策略而子树有策略。取消、约束变化、预算耗尽、非 503 或半包响应不能获得重试许可。旧 native-v4 的评估规则保留，legacy resume 仍不能用于 v5。

本轮真实隔离 PostgreSQL + 管理 HTTP 回归及实际搬迁 Worker 包联合，84 个测试事件通过、零 skip。补充整树策略丢失/合法子树策略及半包拒绝后，只重跑受影响的 v5 20 个事件，全部通过；内部 Owner 单元 13 项及全仓 vet、命令原生构建通过。新 model503 场景在检查工具执行后失败，停止第一个实际 Worker，再通过 HTTP 预览/POST/重复 POST/回执 GET 入队，由第二个 Worker 恢复并交付同一 312,021 字节原文：3 次模型请求、1 次检查、1 条入队回执/事件、1 条完成事件，保留原 503 尝试和 1 次未知用量。既有 native-v4 六场景回归仍通过。

此场景证明失败后的显式接续和进程替换，不是执行中宕机接管；供应商、检查工具和归档为 HTTP 夹具。管理 Owner 方法变化没有改变 Worker execution 调用协议，联调继续使用已固定的 Worker `c828bbcc` 完整包及其 execution `7805a48`，未因管理方法而漂移 Worker 依赖。独立静态复核未发现 P1/P2，没有代跑数据库。私有证据 `ops/final-continuation.path`，首轮 `final-continuation-f5e32c87926a4bd9ac7fa9d31ad27729`、定向补验 `final-continuation-7e244422c6f445e39deb604afe9c2107`；隔离数据库已停止，未改正式库、调用付费模型或运行 Actions。

Worker `c828bbccbb9903ff20c47e6b0e5201124d810f2b` 固定本服务 execution `7805a48` 与 Harness `696c019`。`TestIndependentFinalWorkerOriginalAndRecovery` 启动独立 Worker exe、真实 Node/DSH、Runtime HTTP 和受限 PostgreSQL，验证 312,021 字节 Unicode/CRLF/尾空白原文、选择后强杀、结束事件落盘后强杀、选择/完成 SQL 失败后的显式恢复、取消和供应商半包。7 个新场景及旧 native-v4 的 6 个回归通过，零测试跳过；以实际搬迁完整包复验，原包目录不存在。正常与恢复场景均两次合成模型请求、一次检查，同原文摘要及唯一完成事件。

该联调的供应商、检查工具和归档服务为明确 HTTP 夹具。接管测试只对测试任务手动缩短租约，没有证明真实租约等待时长、付费供应商或正式 Storage。独立评审修复了结果落盘处的测试注入定位，未发现新的集成阻断；独立 Agent 未运行 PostgreSQL。证据由集成仓 Windows 记录和本机 `ops/final-worker-joint.path` 索引。

## 保证与边界

`dsh-source-639ed015-final-v5` 的 `Agent.final_output` 属于不可变快照，绑定已存在的候选工具、原始输入字符串字段和就绪布尔路径。策略必须匹配引擎及工具；旧引擎不能接受新策略。新默认配置下，无策略的原生任务仍选 native-v4，全旧能力任务保持 budget-v3，子任务继承原引擎。候选没有默认启用。

候选工具回执额外提供原字符串 SHA-256。就绪字段必须恰为 JSON `true`，缺失/null/字符串/错误 MCP 结果均不能成稿。原字符串不 trim、不重排 JSON、不转换单位；结构就绪和回执哈希不代表研究事实正确。

`final_output_select` 只接受当前任务的最新候选，核对实际工具请求、已提交结果、原文字段、策略摘要、规则修订、归档、租约/fence，以及最新已提交模型的唯一 `final_output_commit` 调用。出现更新但未就绪的候选时不能退回旧稿。未决调用或未结束子任务阻止交付。

v5 checkpoint 在首次意图中冻结小型结束事件投影；原事件仍走已有不可变归档。选择后的真实成功工具结果和 completed 轮次必须确认落盘，`finish_ref` 才能完成。重新生成的重放 Events 不能改变首次投影。

`finish_ref` 在一次事务里保存原字符串、`completed` 状态、完成响应和引用事件。已提交但丢回复时返回原结果，不重复模型；旧 owner、取消、约束变化、旧选择和新模型步骤均不能误完成。普通 `finish` 和调用者篡改 Run 后的 `Save(completed)` 都受数据库冻结策略限制。

内部 checkpoint/state/select/finish 交换出现普通数据库/归档错误时，真实 Worker 使用的 `CheckpointFailure` 将 v5 归入可核对的 `waiting_unknown`。现有 control source-recovery/resume 入口在锁内检查取消、未决外部调用及原始归档/暂存批次后恢复原交换；不会猜测外部工具是否执行。取消可正确收敛，完成后的迟到失败回报不会覆盖结果。v5 模型失败现通过上述明确回执判定的接续入口；legacy resume 已明确拒绝 v5，不能绕过接续审计。

## 本机证据

- 实际 PostgreSQL、Owner DDL、独立 Runtime/Worker 权限：27 顶层 / 45 子场景通过，零 skip；其中新最终化为 10 顶层 / 24 子场景。
- 覆盖长 Unicode/CRLF/尾空格逐字交付、错误布尔/候选/摘要/模型批次/修订、未知调用和子任务、成功结束证明、事务内故障回滚、选择/完成丢回复、fence 接管、首次事件投影、篡改 Run、取消及失败分类→control 恢复→新 Worker 领取。
- 原生模型/源码状态的定向回归通过；`internal/runstate` 单元测试、全仓 vet、服务 build 通过。
- 原始证据仅在原 Windows 的 `ops/final-output.path` 指向目录内，含 JSONL、构建和停止测试数据库的日志。数据库已停止；没有正式库写入、付费模型或 GitHub Actions。
- 归档与供应商是内存/协议夹具；这里的 Worker 通过实际 execution facade 与独立 DB 角色调用，不是新 Worker exe + Node + HTTP 全链路。真实 Harness 进程的 19 项检查在 Harness 仓分别记录，不能拼成尚未运行的联合验收。

独立评审指出并关闭：模型参数错误永久锁死（Harness）、调用者篡改策略、legacy 接续绕过、普通数据库错误后的恢复入口、checkpoint 失败后的取消卡住。评审为静态检查及 Harness 定向复测；数据库结果来自本仓实际运行。

## 启用前仍需完成

1. 已完成 Worker 固定 execution/Harness 与完整包联合、Registry 固定绑定校验、Console 策略编辑及 Futures 结构就绪。接下来用实际固定业务资源贯通注册→读取→快照→Worker；各域测试不自动等于整条已部署链路。
2. v5 模型失败接续和 Console 管理入口源码已接通，浏览器流程与实际安装仍须验收。
3. 建立受管升级/回滚门槛，包含新旧 Registry/Console 策略能力、全部 Worker 与 Runtime 的 final-v5 能力、未完成任务及接续回执保留；不能仅设置新默认引擎。
4. 完成正式 Windows 发布与有界真实业务验收；内容错误、全平台剩余验收和一次统一 CI 保持开放。
