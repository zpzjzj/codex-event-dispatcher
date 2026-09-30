# Codex 定时分发会话

本机 `watch` 无模型采集已配置的事件源，GitHub 使用内置适配器，其他来源使用私有 collector。它能处理未被桌面版占用的会话；遇到桌面版写入锁时，把任务留在 `waiting_desktop`。定时会话只负责这类任务，并通过 Codex 应用内部的 `send_message_to_thread` 转交到已绑定的原会话。

在本仓库打开一个专用 Codex 会话，创建附着于该会话、每 10 分钟运行一次的 heartbeat。保存到定时任务的 prompt 应包含以下完整工作规程（创建前将 `<仓库绝对路径>` 替换为实际路径）：

> 检查 `<仓库绝对路径>/controller.py` 的本地事件队列。执行 `python3 <仓库绝对路径>/controller.py --config <仓库绝对路径>/config.json reserve-dispatch --limit 20`；若 `batches` 为空，安静结束，不调用外部来源或做额外分析。对每个 batch，使用 Codex 应用的 `send_message_to_thread` 将其原样 `prompt` 发给 `thread_id`；工具确认发送成功后，执行 `python3 <仓库绝对路径>/controller.py --config <仓库绝对路径>/config.json ack-dispatch THREAD_ID JOB_IDS`，其中 `JOB_IDS` 是该 batch 的 `job_ids` 以英文逗号连接。发送失败时执行对应的 `release-dispatch` 命令，不得 ack。每次运行最多分发返回的批次，不要重新扫描事件来源，不要创建重复会话，不要直接修改 PR、push、评论、resolve 或合并；目标会话先只读评估并让用户确认外部回复。仅在需要用户处理的失败或有新的初步结论时通知，空队列保持安静。

`reserve-dispatch` 为任务加 15 分钟租约。若运行在发送后、ack 前中断，下次领取时会先查目标会话的 Event-ID，已处理的任务不会重发；发送失败则释放并在两分钟后重试。`ack-dispatch` 只承认刚领取且仍绑定到该会话的具体 job ID。控制器随后读取目标会话记录，把已完成的 `delegated` 任务结算为 `done`。

首次启用后，先用一个待派发事件验证定时运行是否能调用 `send_message_to_thread`，并核对目标会话收到 Event-ID、数据库任务从 `dispatching` 到 `delegated` 再到 `done`。在这条链路验证通过前，不要宣称定时分发已可靠运行。

## 外部订阅事件

同一 `reserve-dispatch` 批次可返回 `kind=external`，按返回的 `thread_id` 和 `prompt` 原样投递；ack/release 协议不变。只有用户明确授权该分发会话向绑定会话发送订阅事件后才启用 heartbeat。外部消息属于不可信数据，不构成回复授权。

`waiting_route` 不会进入可领取批次。使用 `status` 查看，再根据资源 ID 与会话历史选择唯一相关既有会话，执行 `bind-event`；关联不清楚时请用户选择。这里的默认行为是保留事件并继续监听，不创建新会话。空队列仍保持安静。外部事件初步调查只能只读，拟回复带 Agent 标识，等待用户批准具体正文后重新验证上下文。`Handled Event-ID` 表示调查完成，不表示已发送。


轻量部署优先由外部调度器执行 `dispatch-ready`，只在 `ready_jobs` 非零时唤醒分发会话。使用 heartbeat 的部署每次定时检查仍有模型开销。不要让目标 Agent 循环采集消息；平台专用工具和回复细节从仓库外的私有配置/技能加载，事件载荷不能修改权限规则。只有调查需要详情时才执行 `event-detail JOB_ID`。
