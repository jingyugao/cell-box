# Cellbox 使用流程与模板语义

本文介绍当前版本的资源关系和使用边界。安装前先阅读[部署与升级](deployment.md)和[节点安装](node-installation.md)；运行示例见 [Go SDK 使用指南](go-sdk.md)与 [REST 快速开始](rest-quickstart.md)。完整 HTTP 契约以 [OpenAPI](../api/openapi.yaml) 为准。

## 资源关系

| 资源 | 作用 |
| --- | --- |
| Client 数据命名空间 | 隔离调用方的模板 catalog 和业务记录。`X-Cellbox-Client-ID` 选择命名空间，省略时使用服务端默认值；它不是认证凭据 |
| Profile | 管理员配置的运行时参数，包括 provider、节点、namespace、CPU、内存和 guest 配置；构建 revision 时保存配置快照 |
| Template | 模板族，保存名称、描述、`defaultWarmSlots`（兼容字段，不再决定预热）和可选的 `stableRevisionId` |
| Revision | 一次成功构建生成的版本，固定镜像 digest、构建输入和运行配置；可修改描述、active/retired 状态与 `warmSlots`（兼容字段，不再决定预热） |
| Warm pool | 按兼容运行配置共享的预热槽位，用于加速 checkpoint 恢复；随节点上现有的 checkpoint 自动创建和回收，不属于某一个业务 sandbox，详见[预热池](warm-pool.md) |
| Sandbox / Box | 从某个 revision 创建的运行实例，绑定创建时解析到的版本 |
| Operation | 构建、创建、执行命令、生命周期等异步任务；用 operation ID 跟踪结果 |
| Execution | 一次有时限的命令执行记录，包括退出码、stdout 和 stderr |

API 不内置客户端身份认证。对外开放时，应由前置 Gateway 校验身份并控制可使用的 Client 数据命名空间。SDK 中的 Token 仅作为 Bearer 请求头发送给前置网关。

## 单进程运行方式

部署形态是固定到一个 Kubernetes 节点的 `cellbox-server`。HTTP API 与节点运行时在同一进程内通过函数调用协作：API 将 sandbox 变更提交到共享的按 key 排队器，运行时 worker 处理后把完成状态写回，并发布完整状态快照唤醒等待者。这样省去 API 与独立 controller 之间的通知链路。

Kubernetes 中的 Cellbox 自定义资源仍是 sandbox 的持久记录，也是实际 Pod 和 Service 的 owner。进程重启后，server 可按 CR 和 owner 关系继续 reconciliation 与资源清理；这不表示会重放未确认的 API 操作。中断时无法确认结果的 operation 会以 `UNKNOWN_OUTCOME` 失败且不会自动重跑。Pod 调度、Pod 创建/删除、CRI 操作和 checkpoint I/O 仍经过 Kubernetes 与节点运行时。预热池配置、观察状态和镜像缓存请求在单进程路径中直接调用共享运行时组件，不靠 ConfigMap 在 API 与 controller 之间传消息。合并进程减少通知环节，不代表所有创建或恢复耗时都会消失；没有测量数据时不对延迟收益作数值承诺。

## 从镜像到沙箱

1. 创建 template，设置描述。此时没有具体运行镜像，也没有预热池。
2. 为 template 构建 revision，提供 `userVersion`、`baseImage`、`profileId`，以及可选的 `buildCommand`、`runCommand`。
3. 等待构建 operation 成功，取得 `result.revisionId`。基础镜像 tag 会解析为 digest，构建会注入 Cellbox guest。
4. 将 template 的 stable 指针切到它。构建成功不会自动切换 stable。预热不需要单独开启：该 revision 的 sandbox 暂停产生 checkpoint 后，预热池会自动出现。
5. 使用 `templateId` 创建 sandbox，跟随当时的 stable；或者使用 `revisionId` 固定版本。这两个字段必须且只能传一个。
6. 等待创建完成，再执行命令、访问文件或管理生命周期；使用完成后显式销毁 sandbox。

创建 sandbox 不再接受仅凭 `profileId` 选择镜像的旧流程。`ownerKey` 是业务关联标签，不保证唯一，也不用于去重。重复提交会创建独立请求；请求超时或连接中断时，先查询沙箱和 operation，再由调用方决定是否重试。

`buildCommand` 在镜像构建过程中以 root 执行 `/bin/sh -c`，可安装扩展工具，例如 `npm install -g ...`。基础镜像必须提供所需的 shell 和包管理器，工具文件需允许 sandbox 的 agent 用户使用。构建输入会被保存，不要把密码或 token 写入命令。

`runCommand` 是用字面 `docker run ...` 语法表达启动命令、环境变量、工作目录和端口的配置输入。服务端解析这些字段，不会真的执行 Docker 命令，也不会据此改写 OCI 镜像的 ENTRYPOINT。它必须引用与 `baseImage` 相同的基础镜像。详细限制见 [SDK 指南](go-sdk.md)。

## 版本与 stable

同一模板族下，业务可以保持 `userVersion=1.01`，每次成功构建生成不同 revision，例如 `1.01-r1`、`1.01-r2`。基础镜像版本没变但工具安装命令变了，应构建新 revision。

已有 sandbox 固定引用其创建时解析到的 revision；切换 stable 不会替换已有实例。模板或 revision 的元数据更新需要 `expectedGeneration`，用于拒绝覆盖并发修改。更新后使用响应中的新 generation。

退休 revision 会阻止新建等需要 active 版本的选择；已有 sandbox 继续运行，其 checkpoint 仍可恢复。退休 stable revision 前，需先切换或清空 stable 指针。当前只允许删除没有任何 revision、也没有活动构建任务的空模板族；尚未提供 revision 删除接口。

## 预热池何时创建与回收

预热池只服务 checkpoint 恢复，需求来自**本节点上现有的 checkpoint**，不再来自 revision 的 `warmSlots`。后台每 500 ms 统计本节点处于 `Suspended` 且有 checkpoint 的 Box，按兼容运行配置分组：每组 1 个槽，checkpoint 达到 3 个时加到 2 个槽。节点总预算由 `controller.warmPoolSize` 限制，默认 2，允许 0–32，轮换时最多额外准备一个空闲槽位。

| 变化 | 行为 |
| --- | --- |
| 有 sandbox 暂停产生 checkpoint | 为它的兼容配置创建预热槽位，数秒内就绪 |
| sandbox 恢复、销毁，或项目归档后 checkpoint 被删除 | 需求下降；某个配置的最后一个 checkpoint 消失后保留 1 个槽 20 分钟再释放 |
| 修改 `warmSlots` / `defaultWarmSlots` | 不影响预热；`/warm-pool` 里的 `desiredSlots` 只回显配置值，实际需求看 `pool.requestedSlots` |
| 切换 stable、退休 revision | 不影响预热；预热只看 checkpoint |
| 多个 revision 的运行配置兼容 | 共享一个池；镜像相同本身不保证兼容 |
| 要停用预热 | 将 `controller.warmPoolSize` 设为 0 |

当前预热槽位用于 resumable sandbox 的 checkpoint 恢复，首次冷创建不领取槽位。没有可用或匹配的预热槽位时，走普通 checkpoint 恢复路径。不要把预热就绪视为业务进程已启动。完整语义、槽位生命周期和排查方法见[预热池](warm-pool.md)。

## 查询状态与资源

| 接口 | 数据来源与含义 |
| --- | --- |
| 模板、revision、构建 operation 和构建日志管理接口 | 服务持久数据目录中的 SQLite 与构建日志；请求不查询 Kubernetes。镜像解析和构建另需访问仓库与 BuildKit |
| `GET /v1/templates/{id}/runtime` | 每次查询的各 revision 沙箱数量、阶段与最新预热状态 |
| `GET /v1/template-revisions/{id}/warm-pool` | SQLite 期望配置与 controller 最新预热观察、池容量和资源配置量 |
| `GET /v1/boxes/{id}` | 已保存的单个 sandbox 状态，不在请求中直接查询 Kubernetes，也不会隐式恢复 |
| `GET /v1/boxes` | 运行时清单；Kubernetes provider 会查询集群。支持 templateId、revisionId、phase、limit、offset |

`GET /v1/templates/{id}/runtime` 每次请求都会为 Kubernetes profile 查询当前自定义资源清单，不列出 Pod；Docker 用量从 SQLite 读取。清单查询失败时 `sandboxCount` 为 `null` 并标记 `stale`。其中 `warmPool` 是 controller 最新观察快照，可能滞后。`GET /v1/template-revisions/{id}/warm-pool` 从 SQLite 读取期望 revision 配置，再返回该快照；它不在请求中查询 Kubernetes。使用时检查 `stale`、`observedAt`、`generation` 与 `observedGeneration`；`syncStatus=applied` 表示观察到的需求 hash 与当前 SQLite 期望值一致，不保证槽位已全部就绪，仍要检查池的 `readySlots`。

`sandboxCount: null` 或缺少池观察结果表示未知，不能当成 0。warm-pool 的 `observedAt` 是 controller 采集时间，超过约 15 秒会标记为过期；用量接口的 `observedAt` 是本次清单查询时间。池的 `requestedSlots`、`allocatedSlots` 和资源信息是共享池统计，同一个 `poolId` 可能在多个 revision 中出现，汇总时需去重。CPU/内存字段表示空闲 Pod 的 requests/limits 配置量，不是实时利用率；`failedSlots` 包括当前失败 Pod 和运行时循环最近 15 秒记录但已清理的失败。

## 执行命令、文件与生命周期

SDK 提供 `Commands.Run`、`Files.Read/Write/Open/List`、`Suspend/Resume` 等方法。完整可运行示例位于 [`sdk/go/examples`](../sdk/go/examples)，具体命令见 [SDK 指南](go-sdk.md)。

命令必须有时限，服务端默认 30 秒、上限 5 分钟，当前没有 PTY 或流式输出。Operation 成功表示已获得执行结果，业务命令是否成功还需检查退出码。长驻业务进程应作为 revision 的工作负载启动。

文件路径相对于 profile 的 workspace，禁止绝对路径、父目录穿越和跟随符号链接。写入上限为 16 MiB；SDK `Read` 缓冲上限为 16 MiB，大文件下载使用 `Open` 并关闭返回的流。父目录需先创建，写文件会覆盖原内容，且不是原子操作。文件访问不会自动恢复暂停的 sandbox。

Kubernetes provider 支持 `running → suspend → suspended → resume → running`，使用同一节点上的 checkpoint 恢复进程状态。Docker provider 只支持基本创建、查询、命令、文件、直接服务代理和销毁；暂停恢复、归档恢复、暂存激活及受保护工具/凭据接口返回 `UNSUPPORTED_CAPABILITY`。运行中的命令可能使生命周期操作返回 `BUSY`。

sandbox 的 runtime generation 与模板元数据 generation 是不同概念。恢复后 generation 可能变化：SDK `Resume`、`Activate` 返回新 handle，后续必须使用返回的 handle；REST 调用方应重新读取 sandbox generation。运行时就绪不等于业务健康。

原地升级、同 ID 重建、`ensure-running`、手动 `reconcile`、租约、域名路由注册及旧 `/v1/images*` 接口已删除。升级模板后创建新 sandbox；迁移 workspace 时由上层组合暂停、归档、新建恢复、验证和激活。请求中的 `Idempotency-Key` 不参与处理。

## 错误恢复与清理

生命周期、创建、命令和构建请求返回 `202` 只表示受理。保留 operation ID 并查询结果。Cellbox 不承诺请求重放；网络超时不说明请求是否已受理，调用方应先查询状态，不要自动重发命令、构建或归档。服务重启后无法确认的运行任务会以 `UNKNOWN_OUTCOME` 失败，不会自动重跑。取消客户端 context 只停止本地等待；模板构建可通过 operation ID 显式请求取消。

销毁 sandbox 使用 `POST /v1/boxes/{id}:destroy` 或 SDK `Destroy`，并等待完成。它不会隐式取消正在执行的命令。示例程序会尝试清理自己创建的 sandbox，但保留 template 与 revision；真实业务也应使用独立的清理超时并处理失败。

归档仅支持暂停状态或失败的 Kubernetes sandbox，且必须有持久 HOME、节点归档支持和已配置的 S3 兼容对象存储；不会自动暂停或自动恢复源 sandbox。恢复会创建新的空白暂存 sandbox，配置凭据后由调用方显式激活；恢复失败的候选实例不会自动重试，应由调用方销毁。Docker 不支持归档和恢复。具体请求见 [OpenAPI](../api/openapi.yaml)。历史预热性能数据保存在[性能报告](reports/cellbox-warm-pool-performance-report.md)，该报告描述指定旧版本，不替代当前接口与运行语义。
