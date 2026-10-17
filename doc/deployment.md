# 单机 Kubernetes 部署与升级

本指南面向 Kubernetes / k3s 单机部署。节点上的 gVisor、Cellbox wrapper 和 containerd handler 需先按[节点安装](node-installation.md)准备；Helm Chart 不负责安装这些宿主机组件。应用侧使用方法见[使用流程](cellbox-usage.md)。

## 构建并发布镜像

仓库后端需要 Go 1.26 或更高版本。在仓库根目录运行：

```sh
make check
make build
```

`make build` 输出到 `dist/release`，包含二进制、Chart、`doc/`、OpenAPI 和 `sdk/go/` 源码。只构建输出目录不会升级集群。临时实验文档不会被打包。

将下面的镜像仓库和版本替换为实际值：

```sh
docker build -f images/cellbox-api.Dockerfile -t registry.example.com/team/cellbox-api:v1 dist/release
docker build -f images/controller.Dockerfile -t registry.example.com/team/cellbox-controller:v1 dist/release
docker push registry.example.com/team/cellbox-api:v1
docker push registry.example.com/team/cellbox-controller:v1
```

推送成功后，把完整的 `repository@sha256:...` 写入 `api.image` 和 `controller.image`，便于核对部署版本。Chart 中的默认镜像是固定版本，不会自动指向本地刚构建的代码。节点 wrapper 需按节点安装指南单独维护，更新 Controller 镜像不会同步更新宿主机 wrapper。

## 配置存储、节点和构建器

以 [`charts/cellbox/values.yaml`](../charts/cellbox/values.yaml) 为配置参考，将环境覆盖值保存在被 Git 忽略的 `charts/cellbox/values_local.yaml`。升级已有环境时维护原来的完整配置，保留仍在使用的 profile、存储路径和 Secret 引用。

| 配置 | 单机部署要求 |
| --- | --- |
| `api.replicas` | 保持 1；API 通过 Kubernetes Lease 控制实例接替，catalog 使用单进程锁 |
| `localMetadata.enabled`、`nodeName`、`hostPath` | 推荐启用；API 和 Controller 固定在同一节点并共享专用 hostPath，保存元数据及 SQLite catalog |
| `localMetadata.initialize` | 全新元数据目录首次部署设为 true，初始化完成后改回 false；已有目录正常升级使用 false |
| `api.catalogExistingClaim` | 不启用 localMetadata 时，为 catalog 指定已有持久本地或块存储 PVC；不能依赖临时容器目录 |
| `objectStorage.endpoint`、`bucket`、`region`、`prefix`、`pathStyle` | 配置 S3 兼容对象存储，用于 checkpoint、归档等内容；独立环境使用独立 prefix |
| `objectStorage.credentialsSecret` | 引用同 namespace 中的已有 Secret；默认字段为 `OSS_ACCESS_KEY`、`OSS_SECRET_KEY`，可通过 `accessKeyKey` / `secretKeyKey` 修改字段名 |
| `api.config.profiles` | 配置 profile ID、provider、镜像、CPU、内存、guest；Kubernetes profile 还需匹配实际 `nodeName` 和 release `namespace`。示例结构见 [`config/sample.json`](../config/sample.json) |
| `controller.nodeSelector` | 仅选择已完成节点准备的节点；默认 Linux amd64 |
| `controller.criDirectory` / `criSocket` | 普通 containerd 默认 `/run/containerd` / `/run/containerd/containerd.sock`；k3s 使用 `/run/k3s/containerd` / `/run/k3s/containerd/containerd.sock` |
| `controller.warmPoolSize` | 节点空闲预热槽位总预算，默认 2、允许 0–32；各 revision 的需求由 `warmSlots` 决定 |
| `buildkit.enabled`、`repository` | 构建模板 revision 时需启用 BuildKit，并指定可推送的完整目标镜像仓库 |
| `buildkit.registryCredentialsSecret` | 私有镜像仓库需要时，引用已有的 `kubernetes.io/dockerconfigjson` Secret |
| `buildkit.registryHost` / `registryHTTP` | 本地 HTTP 测试仓库需显式配置；普通 HTTPS 仓库保持 HTTP 选项关闭 |

创建 template 时不创建预热池。成功构建的 active revision 在 `warmSlots > 0` 时自动触发后台预热，且受节点总预算限制；详细语义见[使用说明](cellbox-usage.md)。

API 不内置客户端身份认证。外部访问应由前置 Gateway 校验身份并限制 Client 数据命名空间；`X-Cellbox-Client-ID` 本身不是认证凭据。Secret 正文不应写入版本控制中的 values 或文档。

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

首次初始化使用了 `localMetadata.initialize=true` 时，确认初始化成功后将该字段改回 false，再执行同一条升级命令。元数据目录、对象存储 prefix 和 catalog PVC 属于持久身份配置；正常升级保持原值。

## 检查部署与调用

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" get pods
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  rollout status deploy/cellbox-api --timeout=180s
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  rollout status ds/cellbox-controller --timeout=180s
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs deploy/cellbox-api -c api --tail=100
```

在单独终端运行并保持端口转发：

```sh
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

另一个终端设置 `CELLBOX_API_URL=http://127.0.0.1:18092`，按 [REST 快速开始](rest-quickstart.md)或 [Go SDK 指南](go-sdk.md)验证构建、创建、执行、文件访问和 suspend/resume。Pod 更新后，旧 port-forward 可能断开，需要重新建立。

API 和 Controller Running 不能替代完整功能验证。预热 Pod 的业务容器尚未启动时，Kubernetes 可能显示 `ContainerCreating`；以 revision 的 `/warm-pool` 状态中的新鲜 `readySlots` 判断预热就绪。该就绪只用于恢复准备，不是业务应用健康。
