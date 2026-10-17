# Cellbox 使用流程与模板语义

本文介绍当前版本的资源关系和使用边界。安装前先阅读[部署与升级](deployment.md)和[节点安装](node-installation.md)；运行示例见 [Go SDK 使用指南](go-sdk.md)与 [REST 快速开始](rest-quickstart.md)。完整 HTTP 契约以 [OpenAPI](../api/openapi.yaml) 为准。

## 资源关系

| 资源 | 作用 |
| --- | --- |
| Client 数据命名空间 | 隔离调用方的 catalog 和运行记录。`X-Cellbox-Client-ID` 选择命名空间，省略时使用服务端默认值；它不是认证凭据 |
| Profile | 管理员配置的运行时参数，包括 provider、节点、namespace、CPU、内存和 guest 配置；构建 revision 时保存配置快照 |
| Template | 模板族，保存名称、描述、`defaultWarmSlots` 和可选的 `stableRevisionId` |
| Revision | 一次成功构建生成的版本，固定镜像 digest、构建输入和运行配置；可修改描述、active/retired 状态与 `warmSlots` |
| Warm pool | 按兼容运行配置共享的预热槽位，用于加速 checkpoint 恢复；不属于某一个业务 sandbox |
| Sandbox / Box | 从某个 revision 创建的运行实例，绑定创建时解析到的版本 |
| Operation | 构建、创建、执行命令、生命周期等异步任务；用 operation ID 跟踪结果 |
| Execution | 一次有时限的命令执行记录，包括退出码、stdout 和 stderr |

API 不内置客户端身份认证。对外开放时，应由前置 Gateway 校验身份并控制可使用的 Client 数据命名空间。SDK 中的 Token 仅作为 Bearer 请求头发送给前置网关。

## 单进程运行方式

部署形态是固定到一个 Kubernetes 节点的 `cellbox-server`。HTTP API 与节点运行时在同一进程内通过函数调用协作：API 将 sandbox 变更提交到共享的按 key 排队器，运行时 worker 处理后把完成状态写回，并发布完整状态快照唤醒等待者。这样省去 API 与独立 controller 之间的通知链路。

Kubernetes 中的 Cellbox 自定义资源仍是 sandbox 的持久记录，也是实际 Pod 和 Service 的 owner。进程重启后可根据 CR 和其 owner 关系继续 reconcile、清理或恢复；Pod 调度、Pod 创建/删除、CRI 操作和 checkpoint I/O 仍经过 Kubernetes 与节点运行时。预热池配置、观察状态和镜像缓存请求在单进程路径中直接调用共享运行时组件，不靠 ConfigMap 在 API 与 controller 之间传消息。合并进程减少通知环节，不代表所有创建或恢复耗时都会消失；没有测量数据时不对延迟收益作数值承诺。

## 从镜像到沙箱

1. 创建 template，设置描述和默认预热数。此时没有具体运行镜像，也没有预热池。
2. 为 template 构建 revision，提供 `userVersion`、`baseImage`、`profileId`，以及可选的 `buildCommand`、`runCommand`、`warmSlots`。
3. 等待构建 operation 成功，取得 `result.revisionId`。基础镜像 tag 会解析为 digest，构建会注入 Cellbox guest。
4. 如需预热，等待该 revision 的预热状态就绪，然后将 template 的 stable 指针切到它。构建成功不会自动切换 stable。
5. 使用 `templateId` 创建 sandbox，跟随当时的 stable；或者使用 `revisionId` 固定版本。这两个字段必须且只能传一个。
6. 等待创建完成，再执行命令、访问文件或管理生命周期；使用完成后显式销毁 sandbox。

创建 sandbox 不再接受仅凭 `profileId` 选择镜像的旧流程。`ownerKey` 是业务关联标签，不保证唯一；操作幂等由 `Idempotency-Key` 控制。

`buildCommand` 在镜像构建过程中以 root 执行 `/bin/sh -c`，可安装扩展工具，例如 `npm install -g ...`。基础镜像必须提供所需的 shell 和包管理器，工具文件需允许 sandbox 的 agent 用户使用。构建输入会被保存，不要把密码或 token 写入命令。

`runCommand` 是用字面 `docker run ...` 语法表达启动命令、环境变量、工作目录和端口的配置输入。服务端解析这些字段，不会真的执行 Docker 命令，也不会据此改写 OCI 镜像的 ENTRYPOINT。它必须引用与 `baseImage` 相同的基础镜像。详细限制见 [SDK 指南](go-sdk.md)。

## 版本与 stable

同一模板族下，业务可以保持 `userVersion=1.01`，每次成功构建生成不同 revision，例如 `1.01-r1`、`1.01-r2`。基础镜像版本没变但工具安装命令变了，应构建新 revision。

已有 sandbox 固定引用其创建时解析到的 revision；切换 stable 不会替换已有实例。模板或 revision 的元数据更新需要 `expectedGeneration`，用于拒绝覆盖并发修改。更新后使用响应中的新 generation。

退休 revision 会阻止新建等需要 active 版本的选择，并撤销其预热需求；已有 sandbox 继续运行。退休 stable revision 前，需先切换或清空 stable 指针。当前只允许删除没有任何 revision、也没有活动构建任务的空模板族；尚未提供 revision 删除接口。

## 预热池何时创建与回收

构建成功且 active 的 revision，如果 `warmSlots > 0`，后台会自动发布预热需求。server 内的节点运行时循环在配置节点准备槽位，不需要先创建 sandbox，也不需要手动调用“应用配置”。需求持久保存在 SQLite catalog；server 重启后从 catalog 恢复。节点 Pod 等实际运行时操作仍依赖 Kubernetes 可用。

`defaultWarmSlots` 是构建新 revision 时的默认值。构建请求省略 `warmSlots` 时复制该默认值，显式传 `0` 则关闭该 revision 的预热。修改 template 默认值不会追溯修改已有 revision。

| 变化 | 行为 |
| --- | --- |
| 增大 revision 的 `warmSlots` | 请求增加容量，受节点总预算限制 |
| 减小或设为 `0` | 撤销多余需求，回收相应空闲槽位 |
| revision 退休 | 有效需求变为 0，不删除其已有 sandbox |
| 切换 stable | 旧 revision 的预热配置保持不变，仍可按需保留旧版本容量 |
| 多个 revision 的运行配置兼容 | 共享一个池，合并需求；镜像相同本身不保证兼容 |

节点总预算由 `controller.warmPoolSize` 配置，默认 2，允许 0–32；轮换时最多额外准备一个空闲槽位。各兼容配置的需求按节点预算分配，因此实际分配量可能小于期望量。已被 sandbox 领用的 Pod 不再属于空闲池，缩容或撤销预热不会删除它。

当前预热槽位用于 resumable sandbox 的 checkpoint 恢复，首次冷创建不领取槽位。没有可用或匹配的预热槽位时，走普通 checkpoint 恢复路径。不要把预热就绪视为业务进程已启动。

## 查询状态与资源

| 接口 | 数据来源与含义 |
| --- | --- |
| 模板、revision、构建 operation 和构建日志管理接口 | 本地 SQLite catalog 与构建日志；请求不查询 Kubernetes。镜像解析和构建另需访问仓库与 BuildKit |
| `GET /v1/templates/{id}/runtime` | 后台采集的各 revision 沙箱数量、阶段与预热状态快照 |
| `GET /v1/template-revisions/{id}/warm-pool` | 后台采集的预热同步状态、池容量与资源配置量 |
| `GET /v1/boxes/{id}` | 已保存的单个 sandbox 状态，不在请求中直接查询 Kubernetes，也不会隐式恢复 |
| `GET /v1/boxes` | 运行时清单；Kubernetes provider 会查询集群。支持 templateId、revisionId、phase、limit、offset |

两个模板运行状态接口在 HTTP 请求中不直接查询 Kubernetes。单进程后台约每 2 秒刷新运行时快照，采集时间超过约 15 秒会标记为过期。使用时检查 `stale`、`observedAt`、`generation` 与 `observedGeneration`；`syncStatus=applied` 表示需求已应用到进程内运行时，不保证槽位已全部就绪，仍要检查池的 `readySlots`。

`sandboxCount: null` 或缺少池观察结果表示未知，不能当成 0。池的 `requestedSlots`、`allocatedSlots` 和资源信息是共享池统计，同一个 `poolId` 可能在多个 revision 中出现，汇总时需去重。CPU/内存字段表示空闲 Pod 的 requests/limits 配置量，不是实时利用率；`failedSlots` 包括当前失败 Pod 和运行时循环最近 15 秒记录但已清理的失败。

## 执行命令、文件与生命周期

SDK 提供 `Commands.Run`、`Files.Read/Write/Open/List`、`Suspend/Resume` 等方法。完整可运行示例位于 [`sdk/go/examples`](../sdk/go/examples)，具体命令见 [SDK 指南](go-sdk.md)。

命令必须有时限，服务端默认 30 秒、上限 5 分钟，当前没有 PTY 或流式输出。Operation 成功表示已获得执行结果，业务命令是否成功还需检查退出码。长驻业务进程应作为 revision 的工作负载启动。

文件路径相对于 profile 的 workspace，禁止绝对路径、父目录穿越和跟随符号链接。写入上限为 16 MiB；SDK `Read` 缓冲上限为 16 MiB，大文件下载使用 `Open` 并关闭返回的流。父目录需先创建，写文件会覆盖原内容，且不是原子操作。文件访问不会自动恢复暂停的 sandbox。

Kubernetes resumable provider 的典型流程是 `running → suspend → suspended → resume → running`，使用同一节点上的 checkpoint 恢复进程状态；Docker provider 使用 `freeze/unfreeze`。以 sandbox 的 capabilities 判断支持的操作。活动命令或使用租约可能使生命周期操作返回 `BUSY`。

sandbox 的 runtime generation 与模板元数据 generation 是不同概念。恢复后 generation 可能变化：SDK `Resume`、`EnsureRunning`、`Activate` 返回新 handle，后续必须使用返回的 handle；REST 调用方应重新读取 sandbox generation。运行时就绪不等于业务健康。

## 错误恢复与清理

异步请求返回 `202` 只表示受理。保留 operation ID；请求超时且提交结果不确定时，使用原幂等键和完全相同的输入恢复原请求。取消客户端 context 只停止请求或等待，不会自动取消已受理任务。模板构建提供显式取消接口；日志读到 EOF 后仍需检查最终任务状态。

销毁 sandbox 使用 `POST /v1/boxes/{id}:destroy` 或 SDK `Destroy`，并等待完成。它不会隐式取消正在执行的命令。示例程序会尝试清理自己创建的 sandbox，但保留 template 与 revision；真实业务也应使用独立的清理超时并处理失败。

归档、内部服务代理等进阶能力及其限制见 [OpenAPI](../api/openapi.yaml)。历史预热性能数据保存在[性能报告](reports/cellbox-warm-pool-performance-report.md)，该报告描述指定旧版本，不替代当前接口与运行语义。
