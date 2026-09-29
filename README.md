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
