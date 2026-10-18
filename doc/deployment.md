# 单机 Kubernetes 部署与升级

本指南面向 Kubernetes / k3s 单机部署。节点上的 gVisor、Cellbox wrapper 和 containerd handler 需先按[节点安装](node-installation.md)准备；Helm Chart 不负责安装这些宿主机组件。应用侧使用方法见[使用流程](cellbox-usage.md)。

## 构建并发布镜像

仓库后端需要 Go 1.26 或更高版本。在仓库根目录运行：

```sh
make check
make build
```

`make build` 输出到 `dist/release`，包含二进制、Chart、`doc/`、OpenAPI 和 `sdk/go/` 源码。只构建输出目录不会升级集群。临时实验文档不会被打包。

`make build` 会生成 `dist/release/cellbox-server`、guest agent 和 wrapper。Cellbox API 与节点运行时合并为一个进程和镜像：

```sh
docker build -f images/cellbox-server.Dockerfile -t registry.example.com/team/cellbox-server:v1 dist/release
docker push registry.example.com/team/cellbox-server:v1
```

推送成功后，将镜像 digest 写入 `server.image`。Chart 不会自动使用刚构建的镜像。宿主机的 gVisor、containerd handler 和 wrapper 仍按[节点安装指南](node-installation.md)单独维护。

## 配置存储、节点和构建器

以 [`charts/cellbox/values.yaml`](../charts/cellbox/values.yaml) 为配置参考，将环境覆盖值保存在被 Git 忽略的 `charts/cellbox/values_local.yaml`。升级已有环境时维护原来的完整配置，保留仍在使用的 profile、存储路径和 Secret 引用。

| 配置 | 单机部署要求 |
| --- | --- |
| `server.nodeName` | 必填；填写实际 Kubernetes 节点名，单进程 server 固定运行在该节点 |
| `api.replicas` | 保持 1；单机运行不扩容，由 Kubernetes Lease 选出唯一活动进程 |
| `storage.hostPath` | 单一持久数据目录，默认 `/var/lib/cellbox-data`；保存 `cellbox.db`、构建日志和归档下载校验时的临时文件 |
| `storage.existingClaim` | 可选；提供已有 PVC 后，数据目录改用该 PVC。未提供时使用 `storage.hostPath` |
| `api.config.dataDir` | 保持 `/var/lib/cellbox-data`，它是唯一服务数据目录，不要覆盖为其他容器路径 |
| `objectStorage.endpoint`、`bucket`、`region`、`prefix`、`pathStyle` | S3 兼容归档存储；未配置时归档 capture、download、restore、delete 返回 `UNSUPPORTED_CAPABILITY`。模板、operation 等元数据始终保存在 SQLite |
| `objectStorage.credentialsSecret` | 引用同 namespace 中的已有 Secret；默认字段为 `OSS_ACCESS_KEY`、`OSS_SECRET_KEY`，可通过 `accessKeyKey` / `secretKeyKey` 修改字段名 |
| `api.config.profiles` | 配置 profile ID、provider、镜像、CPU、内存、guest；Kubernetes profile 的 namespace 必须是 release namespace，`nodeName` 必须匹配 `server.nodeName`。示例结构见 [`config/sample.json`](../config/sample.json) |
| `controller.nodeSelector` | 为单进程 server 提供节点平台约束；实际节点还会由 `server.nodeName` 固定。默认 Linux amd64 |
| `controller.criDirectory` / `criSocket` | server 直接访问节点 CRI；普通 containerd 默认 `/run/containerd` / `/run/containerd/containerd.sock`，k3s 使用 `/run/k3s/containerd` / `/run/k3s/containerd/containerd.sock` |
| `controller.warmPoolSize` | 单进程后台运行时循环的空闲预热槽位总预算，默认 2、允许 0–32；各 revision 的需求由 `warmSlots` 决定 |
| `buildkit.enabled`、`repository` | 构建模板 revision 时需启用 BuildKit，并指定可推送的完整目标镜像仓库 |
| `buildkit.registryCredentialsSecret` | 私有镜像仓库需要时，引用已有的 `kubernetes.io/dockerconfigjson` Secret |
| `buildkit.registryHost` / `registryHTTP` | 本地 HTTP 测试仓库需显式配置；普通 HTTPS 仓库保持 HTTP 选项关闭 |

服务元数据和模板 catalog 共用 `storage` 挂载中的 `cellbox.db`。库内只有 `templates`、`template_revisions`、`operations`（构建任务）和 `runtime_records` 四张业务表。`runtime_records` 只保存 sandbox、运行操作、执行记录、执行结果、归档元数据和 checkpoint 索引；查询直接读库，事务内使用的临时对象不保留为进程级数据副本。归档 payload 仅保存在配置的 S3 兼容对象存储中；本地 dataDir 只用于 SQLite、构建日志和下载校验的临时文件。

旧的 `state.json`、对象元数据账本和独立 `catalog.db` 不做自动转换。部署新版时使用新的数据目录；这里的改造没有迁移现存集群数据。

Kubernetes profile 的 sandbox 期望状态和运行状态仍保存在 Cellbox 自定义资源中；gVisor checkpoint 文件仍放在节点的 `/var/lib/cellbox` 下。

创建 template 时不创建预热池。成功构建的 active revision 在 `warmSlots > 0` 时自动触发后台预热，且受节点总预算限制；详细语义见[使用说明](cellbox-usage.md)。

API 不内置客户端身份认证。外部访问应由前置 Gateway 校验身份并限制 Client 数据命名空间；`X-Cellbox-Client-ID` 本身不是认证凭据。Secret 正文不应写入版本控制中的 values 或文档。

## 单进程与运行时状态

Helm 只部署一个 node-pinned `cellbox-server` Deployment；`cellbox-api` Service 名称保持不变，SDK 和现有端口转发继续使用它。server 内部通过函数调用和按 sandbox key 排队，把 API 生命周期写入与节点 reconcile 串起来；状态提交后，完成快照直接通知等待中的请求。Warm pool 和镜像预热请求也由进程内共享组件交接，不使用 ConfigMap 作为这些功能的进程间消息通道。

Kubernetes 仍保存 Cellbox 自定义资源，作为 sandbox 的持久期望状态、运行状态和 Pod/Service ownership 记录；实际 Pod 创建、删除及 CRI/gVisor checkpoint 操作仍由 Kubernetes 和节点运行时完成。因此合并缩短的是本地通知链路，不会消除 Kubernetes 持久化写入、Pod 生命周期操作或它们的等待时间。本文描述目标部署结构，不代表该 Chart 已部署到任何环境。

由于 API 与节点运行时处于同一个进程容器，`cellbox-server` Pod 使用 `hostPID`、特权容器和 containerd、gVisor、`/var/lib/cellbox` 等宿主机挂载。只应把 `server.nodeName` 指向已准备并受信任的单机运行节点；不要按普通无特权 API Deployment 的安全边界部署。

## 应用配置

以下命令使用 Bash。先明确目标环境；当前仓库的独立测试环境为 `k3s` context、`cell-box-v2` namespace、`cellbox-v2` release，实际部署请替换为自己的值：

```sh
export CELLBOX_CONTEXT=k3s
export CELLBOX_NAMESPACE=cell-box-v2
export CELLBOX_RELEASE=cellbox-v2
export CELLBOX_VALUES=charts/cellbox/values_local.yaml

helm lint charts/cellbox -f "$CELLBOX_VALUES"
helm upgrade --install "$CELLBOX_RELEASE" charts/cellbox \
  --kube-context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" --create-namespace \
  -f "$CELLBOX_VALUES" --wait --timeout 10m
```

升级时保持 `storage.hostPath` 或 `storage.existingClaim` 不变，这决定 SQLite 数据和构建日志的持久位置。S3 prefix 只影响归档对象的路径。

## 检查部署与调用

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" get pods
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  rollout status deploy/cellbox-server --timeout=180s
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs deploy/cellbox-server -c server --tail=100
```

在单独终端运行并保持端口转发：

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

另一个终端设置 `CELLBOX_API_URL=http://127.0.0.1:18092`，按 [REST 快速开始](rest-quickstart.md)或 [Go SDK 指南](go-sdk.md)验证构建、创建、执行、文件访问和 suspend/resume。Pod 更新后，旧 port-forward 可能断开，需要重新建立。

`cellbox-server` Running 不能替代完整功能验证。预热 Pod 的业务容器尚未启动时，Kubernetes 可能显示 `ContainerCreating`；以 revision 的 `/warm-pool` 状态中本进程维护的新鲜 `readySlots` 判断预热就绪。该就绪只用于恢复准备，不是业务应用健康。
