# budget-v3 模型回执候选

2026-10-14。保持既有 `runtime-execution-v1` 窄接口兼容，新增显式 opt-in 的
`dsh-source-639ed015-budget-v3`。历史 New/NewSourceEnabled 领取范围不变，
v1/v2 的 model Exchange 回复形状不变。此候选未安装，完整平台目标未完成。

## 效果与事务

`BudgetModelEffects.CompleteBudgetModel` 返回最终供应商请求字节。供应商成功
必须先经 BeginModel/EndModel 保存回执；回调不负责提供“权威 usage”。Runtime
在原租约/fence 行锁内，从 calls 已提交原文派生 completion、message 和计量。

预检拒绝必须同时满足：固定配置有效、最终完整请求超过上下文启发式上限、
同 operation 没有任何 calls。拒绝作为 `format:1,kind:context_rejected` 的正常
Exchange 响应提交，带最终请求哈希、估算输入、输出上限和窗口。
BeginModel 也在同一行锁下核对冻结拒绝，拒绝提交与登记调用双向互斥。
拒绝消耗有界模型步骤，但不登记供应商 attempt、不推进 PlanRevision。

最终请求以公共纯函数 BudgetModelRequest 构造，并与冻结 intent 和数据库中的
原始 Agent 快照核对：模型、工具/schema、thinking、reasoning、输出上限、stream
及完整消息均绑定。当前约束和子任务结果在首次 intent 提交时加入；旧 intent 的
恢复不会被随后规则改变重新拼接。新约束需由新的模型步骤采用。

网络、401、数据库、归档、预算、取消或 fence 错误仍是错误，不会变成“未发送的
上下文拒绝”。已登记效果、其他调用占用同一 ID 或缺少已提交 completion 时拒绝
构造成功 envelope。并发回调不得改写已冻结响应。

## 计量修正及兼容影响

旧实现把 `usage:{}` 中不存在的字段解码为零，可能低估费用。本次要求 prompt 和
completion 字段实际存在且为非负整数；缓存为可选非负整数且不超过 prompt，
供应商提供 total 时必须等于两者之和。无效/缺失计量继续保留保守预留。
家族累计和下一次预留采用饱和加法，不能整数回绕后重新开放预算。

这项账本纠错适用于旧任务的重新统计：不改历史回执或旧 Harness 回复，但不完整
历史 usage 可能提高统计预留并阻止后续调用。预算错误会明确列出已计量值、下一次
预留、上限及未知次数，不把所有拒绝都解释为未知调用。

送往 Node 的计量只使用可精确表示的整数；超出其整数范围的计量不下发，int64
家族账本仍按自己的有效计量和饱和规则保护预算。它不是金额估算或供应商计费凭证。

## 验收范围

本机测试使用实际隔离 PostgreSQL、受限 Runtime/Worker 角色和 HTTP 状态端点；
模型、归档字节服务及 effects 回调是明确夹具。覆盖提交拒绝前故障、成功模型回执
之后 Exchange 提交失败、冻结回复后接管、旧 fence、同 fence 并发登记/拒绝、规则
更新、篡改最终参数、未知操作冲突、空/异常 usage 和家族累计溢出。

源码 Node 的独立 15 项测试另见 Harness 仓库。两类测试均不等于真实 Worker+Node
联合强杀、正式供应商或完整研究验收。后续仍须发布固定 Runtime 模块、Worker
独立第三套 Harness pin、包/配置/领取和回退守卫，再执行联合故障与有界付费验收。

Actions 保持关闭，不因提交触发 CI。
