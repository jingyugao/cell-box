# REST 快速开始

本指南演示创建模板族、构建 revision、将其设为 stable、创建沙箱并执行命令，并说明预热状态查询。Go 客户端请参阅 [SDK 使用指南](go-sdk.md)。完整请求和响应结构见 [`api/openapi.yaml`](../api/openapi.yaml)。

## 连接服务

连接当前 k3s 测试部署时，将 `cell-box-v2` 命名空间中的 API Service 转发到本机：

```sh
kubectl --context k3s -n cell-box-v2 port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

在另一个终端设置 API 地址：

```sh
export CELLBOX_API_URL=http://127.0.0.1:18092
```

上面的 k3s context 和 namespace 是当前测试环境配置；在其他集群请替换成目标 context、namespace 和端口。Cellbox API 本身不提供客户端身份认证；只有前置网关启用了认证时才需要 `Authorization: Bearer ...`，并由 Gateway 校验。`X-Cellbox-Client-ID` 用来选择 client 数据命名空间，不是认证凭据。省略时使用服务端配置的默认命名空间。

## 创建模板族

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/templates" \
  -H 'Content-Type: application/json' \
  -d '{"name":"coding-agent","description":"Agent workspace","defaultWarmSlots":0}'
```

将响应中的 `id` 保存为 `TEMPLATE_ID`。模板族包含可变元数据、`defaultWarmSlots`（兼容字段，不再决定预热），以及可选的 stable revision 指针。创建 template 时尚无可运行镜像，因此不会创建预热池。模板数据保存在服务持久数据目录的 `cellbox.db` 中。

## 构建 revision

将 `profileId` 替换为当前 Cellbox 部署中 `api.config.profiles` 已配置的 profile ID。构建还需要可用的 BuildKit 配置，以及拉取 `baseImage` 和发布构建镜像的权限。

```sh
export TEMPLATE_ID='<上一步返回的模板 ID>'
export CELLBOX_PROFILE_ID='<api.config.profiles 中配置的 profile ID>'

curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID/revisions" \
  -H 'Content-Type: application/json' \
  --data-binary @- <<JSON
{
  "userVersion": "1.01",
  "baseImage": "docker.io/library/ubuntu:24.04",
  "profileId": "$CELLBOX_PROFILE_ID",
  "buildCommand": "apt-get update && apt-get install -y git ca-certificates",
  "platform": "linux/amd64"
}
JSON
```

响应是异步构建 operation。将其 `id` 保存为 `BUILD_ID`。构建会将基础镜像解析为 digest，并记录构建输入。`userVersion` 为 `1.01` 的构建会创建编号不可变 revision，例如 `1.01-r1`；后续成功构建可生成 `1.01-r2`。每次提交都会创建独立构建任务，不做幂等处理。

查询构建 operation 状态。每次请求最多等待 10 秒：

```sh
export BUILD_ID='<operation id returned above>'
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/operations/$BUILD_ID?waitMs=10000"
```

当 `status` 为 `queued` 或 `running` 时重复查询。完成后从 `result.revisionId` 读取并保存：

```sh
export REVISION_ID='<成功 operation 的 result.revisionId>'
```

失败时检查 operation 的 `error`，并从 `/v1/operations/$BUILD_ID/logs?offset=0&limit=65536` 分块读取日志；后续 offset 使用响应中的 `nextOffset`。日志以有大小上限的 JSON 块返回，不支持流式跟踪。构建成功不会自动切换 stable revision。构建成功且 active 的 revision 若 `warmSlots > 0`，后台会发布预热需求并重试失败发布；预热准备和构建是两个独立状态。`warmSlots` 为 0 或 revision 退休时，后台会撤回需求。

模板、revision、构建 operation 和日志接口使用服务本地 `cellbox.db`，不查询 Kubernetes。解析或构建镜像时会访问镜像仓库和 BuildKit。选择 profile 后，系统会把当时配置快照保存到 revision。构建成功不会自动切换 stable revision。

切换 stable 只影响之后新建的沙箱。预热需求来自节点上现有的 checkpoint，不来自 revision 的 `warmSlots`；兼容的 revision 共用预热池，每个节点有共享容量上限（默认最多 2 个，配置上限 32 个），详见[预热池](warm-pool.md)。同一 `poolId` 的槽位数是共享池汇总值，会在关联 revision 的状态中重复出现；节点容量可能使 `allocatedSlots` 小于 `requestedSlots`。汇总多 revision 时按 `poolId` 去重。撤回需求只清理空闲预热 Pod；已被 sandbox 领用的 Pod 不会因此删除。当前预热 Pod 服务于 resumable sandbox 的恢复路径，不能假定它会加速首次冷创建。

## 查询预热与模板运行状态

预热状态接口从 SQLite 读取 revision 期望配置，并返回 controller 保存在 provider 中的最新观察快照；这个请求不查询 Kubernetes。模板运行状态接口则每次为 Kubernetes profile 查询当前 Cellbox 自定义资源清单（不列出 Pod），Docker 用量从 SQLite 读取。查看某个 revision 的预热同步和池状态：

```sh
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/template-revisions/$REVISION_ID/warm-pool"
```

响应包括 `desiredSlots`、`syncStatus`、`generation` / `observedGeneration`、`observedAt`、`stale` 和可空的 `pool`。`stale` 表示 controller 观察过期或与当前 SQLite 期望配置不一致；`pool: null` 表示当前没有关联到预热池。池中的 `requestedSlots`、`allocatedSlots`、`readySlots`、`preparingSlots`、`failedSlots` 用于区分需求和实际准备进度。`failedSlots` 包含当前仍处于失败态的 Pod，以及运行时循环最近 15 秒内记录但已清理的创建失败。

查询模板全部 revision 的沙箱数量、生命周期阶段和预热状态：

```sh
curl --fail-with-body -sS "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID/runtime"
```

`sandboxCount: null` 表示清单查询失败、数量未知，不是 0。用量 `observedAt` 是本次查询时间；warm-pool 的 `observedAt` 是 controller 采集时间。`resources` 是该池已分配空闲 Pod 的 requests/limits 配置值，不是实时 CPU 或内存用量；共享池可能映射多个 revision，汇总时按 `poolId` 去重，避免重复计算资源。

## 设置 stable revision

使用创建模板或最近一次 GET 返回的 `generation`。更新时必须传入 `expectedGeneration`；更新成功后，下一次操作要使用响应返回的新 generation。

```sh
curl --fail-with-body -sS -X PATCH "$CELLBOX_API_URL/v1/templates/$TEMPLATE_ID" \
  -H 'Content-Type: application/json' \
  -d "{\"stableRevisionId\":\"$REVISION_ID\",\"expectedGeneration\":1}"
```

Stable revision 选择保存在 SQLite 元数据中，必须指向同一模板族中的 active revision。如需让沙箱固定使用指定版本，创建时传 `revisionId`，不要传模板族的 `templateId`。

## 创建沙箱并执行命令

使用模板族当前的 stable revision 创建沙箱。`ownerKey` 是调用方填写的业务关联标签，不保证唯一，也不用于认证。Cellbox 不提供请求重放承诺；如果提交结果不确定，先按 operation 或业务信息查询，再决定是否重新提交。

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes" \
  -H 'Content-Type: application/json' \
  -d "{\"templateId\":\"$TEMPLATE_ID\",\"ownerKey\":\"job-42\"}"
```

将响应中的 operation `id` 保存为 `CREATE_OP_ID`。轮询 `/v1/operations/$CREATE_OP_ID?waitMs=10000` 直到任务结束，再从 `box` 读取沙箱 `id` 和 `generation`。也可以使用终态 operation 的 `targetId`，再 GET `/v1/boxes/{id}`。运行时创建由配置的 provider 执行；如果使用 Kubernetes profile，provider 会查询 Kubernetes。

显式传入 `/bin/sh -c` 来执行有时限的 shell 命令。先从创建结果中保存 sandbox ID 和 generation：

```sh
export BOX_ID='<box id>'
export BOX_GENERATION='<generation>'

curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes/$BOX_ID/execs" \
  -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"/bin/sh\",\"-c\",\"git --version && pwd\"],\"timeoutMs\":30000,\"expectedGeneration\":$BOX_GENERATION}"
```

响应中包含 operation ID 和 `result.execId`。轮询 operation 直到结束，再 GET `/v1/execs/{execId}` 读取最终的 `stdout`、`stderr` 和 `exitCode`。命令默认超时为 30 秒，最长 5 分钟；结束后一次性返回输出，不支持 PTY、stdin 或流式输出。每次提交都会执行一条新命令；超时后先查询 operation 和执行记录，避免盲目重发。

测试结束后显式销毁沙箱。使用创建结果里的 generation：

```sh
curl --fail-with-body -sS -X POST "$CELLBOX_API_URL/v1/boxes/$BOX_ID:destroy" \
  -H 'Content-Type: application/json' \
  -d "{\"expectedGeneration\":$BOX_GENERATION}"
```

该请求返回 HTTP 202 和 operation；轮询其 ID 直到终态。客户端断开等待不会取消已受理的销毁操作。

## 行为说明

- 模板与 revision 元数据操作使用服务持久数据目录中的 `cellbox.db`；单进程 server 的运行时循环根据节点上现有的 checkpoint 直接协调预热池。预热状态查询读取后台快照，不直接查询集群。沙箱创建和生命周期操作会写入 Kubernetes Cellbox CR，再通过进程内队列通知 runtime worker；Cellbox CR 继续作为持久期望/运行状态并拥有实际 Pod/Service。命令、文件和清单操作仍按所选 runtime provider 执行。
- 模板 stable revision 只影响之后新建的沙箱。已有沙箱继续使用创建时绑定的不可变 revision。
- 预热需求只由 `Suspended` 且有 checkpoint 的 Box 决定，与 `warmSlots`、stable 指针无关；`desiredSlots` 只回显配置值。状态接口返回后台快照，`stale` / `observedAt` 用于判断新鲜度。
- 只有没有 revision 且没有待处理构建的空模板族才能删除。本期不提供 revision 删除接口；将 revision 标记为 retired 可阻止新选择，同时保留历史记录。
- 客户端断开连接或取消本地等待不会取消已受理的服务端 operation。用 operation ID 查询状态。网络失败后不要自动重新提交有副作用的操作。服务重启后无法确认的任务会以 `UNKNOWN_OUTCOME` 失败且不会自动重跑。构建需要显式调用 `POST /v1/operations/{id}:cancel` 请求取消。

## 归档与恢复

归档只接受已暂停或失败、并使用持久 HOME 的 Kubernetes sandbox，且需要节点归档支持和 S3 兼容对象存储。未配置对象存储时 capture、download、restore、delete 返回 `UNSUPPORTED_CAPABILITY`。Cellbox 不会为了归档自动暂停或恢复源 sandbox。提交 `POST /v1/boxes/{id}/archives` 后等待 operation，成功结果给出 `archiveId`；可用 `GET /v1/archives/{id}` 查询元数据，`GET /v1/archives/{id}/content` 下载归档，`DELETE /v1/archives/{id}` 删除归档。

恢复时调用 `POST /v1/boxes:restore`，请求包含 `archiveId`、`ownerKey`，以及且仅有一个 `templateId` 或 `revisionId`。服务创建新的暂存 sandbox，等待恢复 operation 成功后由业务写入凭据，再调用 `POST /v1/boxes/{id}:activate` 启动工作负载。恢复失败的候选 sandbox 不会自动重试，应由业务显式销毁。Docker provider 不支持归档、恢复或暂存激活。
