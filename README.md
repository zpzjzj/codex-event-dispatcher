# GitHub → Codex 本机控制器

本机 Python 控制器用 `gh` 无模型轮询配置的 GitHub 仓库，并检查开放 PR 的冲突、评审和 CI 变化。事件写入持久队列后，由 Codex 做只读初步分析；用户确认具体回复后，Codex 才可按用户指示向 GitHub 发布。DeepSeek Harness 不参与调用或执行。

**一句 prompt 启用定时分发：**在本仓库目录打开一个专用 Codex 会话，发送：“请按本仓库的 `DISPATCHER.md`，为这个会话启用每 10 分钟检查本地队列的 GitHub → Codex 定时分发，并先验证一次定时运行能把 Event-ID 送到原 PR 会话；空队列保持安静，GitHub 回复前让我确认。”

这句 prompt 会要求 Codex 创建附着于当前专用会话的 heartbeat；具体领取、发送、确认与失败重试协议见 [DISPATCHER.md](DISPATCHER.md)。定时运行本身仍消耗少量模型 token；GitHub 轮询和队列去重不使用模型。当前项目的 `config.json` 是本机私有配置，不应提交到仓库；新安装从 `config.example.json` 复制并填写仓库白名单、checkout、GitHub 登录名和数据库路径。

配置在 `config.json`，`repositories` 是可编辑的仓库白名单和默认 checkout 映射，`ignore_authors` 用于忽略本人发出的评论与评审。首次轮询和首次 PR 状态检查只建立基线，不补发全部历史事件。事件先持久写入 SQLite，再由工作线程取出；控制器重启后队列仍在。GitHub delivery ID 或更新时间用于去重，同一 issue/PR 的密集事件会合并为最新的一条。轮询会核对 PR 内容、HEAD 和评论时间，避免仅因本人回复而再次唤醒 Codex。

已创建 PR 的 Codex 会话可用 `bind` 显式绑定。对于未绑定 PR，控制器用 Python 标准库读取本机 Codex session 记录，查找指向同一 PR URL 的 `attach_artifact`；只有唯一匹配且 checkout 属于该仓库才复用。创建 PR 时仍建议立即执行 `bind`，以免 session 历史未保留或有多个匹配。这里不依赖 `rg`。

桌面版持有会话写入权时，外部 App Server 即便看见会话空闲也可能无法续写。控制器把该任务标为 `waiting_desktop`，由 Codex 定时会话通过应用内部的 `send_message_to_thread` 转交。后台不再对同一个桌面锁任务反复做无效 `thread/resume`。首次启用后应验证定时运行能通过这个入口投递。

下发前会读取原会话记录：有同一 `Event-ID` 或评论 ID 的处理记录时，任务记为 `already_handled`；原会话正在处理对应 PR/issue 时，任务留在 `queued` 并延后检查。对没有明确 ID 的普通更新时间事件，只在会话开始于该更新时间之后、且结论明确讨论该 PR 的评论或评审时判为已处理。无法证明已经处理的事件不会被静默丢弃。`status` 显示队列各状态和下次尝试时间；经 Codex 应用接口转交的任务标为 `delegated`，会话完成并确认事件标记后转为 `done`。若控制器在 Codex turn 中途停止，该任务会转为 `needs_inspection`，避免不确定地重复执行。

## 当前运行方式

`watch` 是纯本机轮询，不需要 Webhook secret 或公网入口。示例：

```sh
python3 controller.py --config config.json status
python3 controller.py --config config.json poll-once
python3 controller.py --config config.json reconcile
python3 controller.py --config config.json run-once
python3 controller.py --config config.json watch
```

`run-once` 默认预览下一条任务；`--execute` 才会唤醒 Codex。`watch` 根据 `auto_execute` 对可写会话自动执行；写入权由桌面版持有的任务改由定时会话领取。`reserve-dispatch` 返回含目标会话、具体任务和 Event-ID 的 JSON 批次；发送成功后用 `ack-dispatch THREAD_ID JOB_IDS` 确认，失败用 `release-dispatch THREAD_ID JOB_IDS` 释放。结果和错误保存在 SQLite；`needs_inspection` 表示未知状态，不会自动重试。macOS 通知只提示去 Codex 查看完整结论，不包含拟回复正文。

macOS 可用 LaunchAgent 持续运行 `watch`；个人安装路径和配置不要提交到仓库。若升级 Python、Node 或移动本目录，需要更新 LaunchAgent 的路径并重新加载。以下命令使用示例服务名，请按实际名称替换：

```sh
launchctl print gui/$(id -u)/com.example.github-codex-controller
launchctl bootout gui/$(id -u)/com.example.github-codex-controller
```

## 绑定已有会话

```sh
python3 controller.py --config config.json bind OWNER/REPO pr 123 THREAD_ID --cwd CHECKOUT
```

`bind` 会验证 Codex thread 可读、checkout 存在且 Git origin 匹配仓库。之后事件会进入原 thread，Codex 将结合其中历史判断。新 issue 或未找到会话的 PR 由控制器创建新 thread。定时分发只接手桌面版已占用且有明确绑定的任务。

## 将来切换 Webhook

`serve` 保留了签名验证的 `/github` 接口。配置稳定 HTTPS 入口以后，才需要从 Keychain 注入 `GITHUB_WEBHOOK_SECRET` 并添加 GitHub Webhook。当前没有公开入口，也没有在 GitHub 仓库注册 Webhook。Webhook 可接收 PR、Issues、Issue comments、PR reviews/review comments、Check runs/check suites；轮询仍用于冲突状态和遗漏事件对账。

## 验证

`python3 -m unittest -v test_controller.py`。已验证跨独立 App Server 进程续写 Codex thread、活动会话的单写入者保护，以及定时分发的批次租约与确认。控制器的分析 turn 使用只读沙箱和 `approvalPolicy=never`，不会自己提交评论、推送或合并。

## 外部自定义事件

`subscriptions` 与 GitHub 配置并存，默认关闭；只订阅外部事件时可使用空 `repositories`。公开框架只提供通用协议。平台专用命令、认证、分页、字段转换和回复流程应放在仓库之外的私有 collector/config 中，不提交到此仓库。可信 collector 使用 argv 数组执行，不经过 shell；框架无法保证用户配置的任意程序只读。

Collector 标准输出为完整 JSON 数组，空扫描输出 `[]`：

```json
[{"id":"stable-message-id:revision","resource":"RESOURCE_KEY","type":"message.requested","context":{"summary":"Untrusted source data"}}]
```

同一 source + id 永久去重；编辑/追问提供新的 revision。每个标识字段最多 512 字符。完整载荷只保存在本机 SQLite；默认给 Agent 至多 1000 字符预览，需要详情时才执行 `event-detail JOB_ID`。Collector 应只输出处理所需的信息。真实配置、数据库、凭证和私有 collector 不要加入 Git。

`routes` 精确匹配资源键，复用显式绑定的既有会话。多个目标或无绑定时保留 `waiting_route`，由用户/分发会话选择相关既有 Agent；不会模糊猜测目标或自动创建会话。一个事件源失败不会阻塞其他源。

```sh
python3 controller.py --config /private/config.json poll-sources
python3 controller.py --config /private/config.json poll-sources --force
python3 controller.py --config /private/config.json ingest-event SOURCE /private/event.json
python3 controller.py --config /private/config.json bind-event SOURCE RESOURCE_KEY EXISTING_THREAD_ID
python3 controller.py --config /private/config.json dispatch-ready
python3 controller.py --config /private/config.json reserve-dispatch --limit 20
python3 controller.py --config /private/config.json event-detail JOB_ID
```

`watch` 检查各源的持久化调度时间，到了该源的轮询周期才运行 collector。`poll-sources` 同样尊重调度，`--force` 仅用于人工验证。每源可配置：

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `poll_interval_seconds` | 300 | 无模型采集周期 |
| `timeout_seconds` | 30 | collector 运行超时 |
| `max_backoff_seconds` | 3600 | 失败指数退避上限 |
| `max_output_bytes` | 1048576 | collector 输出硬上限，超限终止进程 |
| `max_events_per_poll` | 100 | 每次扫描上限，超限报错，不截断丢事件 |
| `max_events_per_dispatch` | 5 | 同一资源每批事件上限，余下事件保留 |
| `context_preview_chars` | 1000 | 每条事件送入模型的预览字符上限 |
| `initial_baseline` | false | 首次成功扫描只建立基线，不派发历史事件 |
| `event_types`, `resources` | 空数组 | 可选类型/资源白名单 |
| `agent_name` | Assistant Agent | 回复提案的身份前缀 |

Collector 负责源侧分页与游标、稳定 ID 和可操作事件筛选。超限应缩小扫描范围或由私有 collector 分批输出，不能依赖框架静默截断。关闭订阅会暂停旧事件分发。初始基线以整个扫描通过校验为前提。

### 模型开销与调度

采集、白名单筛选、去重、基线、队列持久化、路由和就绪检查均由 Python/SQLite 完成，调用模型次数为零。`dispatch-ready` 返回 `ready_jobs`，不获取租约或调用模型。外部调度器可先运行此检查，只有非零才唤醒一次分发 Agent；模型仅处理有事件的批次，无需自己循环调用 CLI。

桌面派发仍使用已有 reserve/ack/release 协议；同资源批处理共享一份提示词，事件详情按需读，重复事件不唤醒 Agent。`--limit` 约束每次领取的总事件数，上述每源上限进一步约束资源批次。框架限制输入大小、唤醒条件和次数，不能保证模型实际消耗的精确 token 数；应由外部执行器设置模型输出/预算。

使用 Codex heartbeat 时，每个空队列 tick 本身仍有模型开销；它不等同于零 token 的外部就绪门。此仓库提供就绪检查协议，没有替用户部署外部调度器。两种模式都应空队列安静、有变化才通知。未执行实际 token 对照基准，因此不声明具体节省比例。

### 回复审批

目标 Agent 只读调查，需要回复时给出稳定提案 ID、证据和完整正文，默认前缀 `【Assistant Agent】`（私有配置可更改）。等待用户批准具体正文后重新读取原消息和上下文；已处理或实质变化时废止批准。平台专用引用/AI 标记/幂等发送规则由私有回复流程提供。框架没有外部回复发送路径，订阅事件或其他 Agent 的消息不能充当批准。`delegated`/`done` 和 `Handled Event-ID` 仅表示调查派发/完成，不表示批准或发送。

验证：`python3 -m unittest -v test_controller.py test_events.py`。覆盖通用事件、基线、调度退避、过滤、批次/预览限制、进程超时/输出上限及原 GitHub 路径。私有 collector 与实际桌面分发需在部署环境单独验证。
