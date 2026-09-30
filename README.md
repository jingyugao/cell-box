# Cellbox

Cellbox 是一个通过 REST API 创建和管理隔离工作空间的通用平台，提供执行命令、文件访问、生命周期控制、归档和受控预览访问。产品应用与沙箱镜像由使用方配置。

安装后的操作步骤与 API 调用示例见 [使用说明](doc/cellbox-usage.md)。

| 运行方式 | 生命周期能力 |
| --- | --- |
| `docker-normal` | 本地 Docker 工作空间，支持 freeze / unfreeze |
| `k8s-resumable` | Kubernetes 工作空间，使用 gVisor checkpoint 实现同节点 suspend / resume |

`k8s-normal` 和 `docker-resumable` 尚未实现。跨节点恢复、PTY 和应用一致性备份不在当前支持范围内。归档是工作空间文件的尽力复制；`running` 表示运行时阶段，不代表应用健康或 guest 已就绪。

## Kubernetes 安装与升级

主要部署方式是 Kubernetes + Helm；k3s 是本机测试环境。

### 前提

- 节点使用 systemd 管理的 Linux / containerd，安装器支持 containerd 配置版本 2 和 3。Chart 默认选择 Linux amd64 节点。
- 已安装兼容的 gVisor，节点上存在可执行的 `/usr/local/bin/runsc` 和 `/usr/local/bin/containerd-shim-runsc-v1`。默认不会安装或替换 gVisor。
- Kubernetes 凭证允许创建 Chart 中的 CRD、RuntimeClass、RBAC、Deployment、DaemonSet 和存储资源；集群允许 Controller 使用 `privileged`、`hostPID` 和 `hostPath`。
- 有可用的默认 StorageClass，或在 values 中指定 `api.storageClassName`。
- 节点能拉取所配置的 API、Controller、初始化容器及沙箱镜像。

部署只使用 Kubernetes API，不需要 SSH 或宿主机登录凭证。首次注册运行时可能重启 containerd；在 k3s 上对应重启 `k3s` 或 `k3s-agent` 服务。

### 配置 values

公共默认配置在 [charts/cellbox/values.yaml](charts/cellbox/values.yaml)。集群地址、镜像、节点、存储类和凭证写入 `charts/cellbox/values_local.yaml`；该文件已被 Git 忽略。包含 Token 时应设置权限为 `0600`。

下面是使用自有镜像仓库、配置一个 Kubernetes profile 的示例。将仓库地址、镜像版本、节点和沙箱 digest 替换为实际值：

```yaml
imageRegistry: registry.example.com/team

controller:
  image: cellbox-controller:v0.1.0
  runtimeInstaller:
    enabled: true
    mode: auto
    gvisor:
      enabled: false

api:
  image: cellbox-api:v0.1.0
  storageClassName: standard
  config:
    profiles:
      - id: coding-k8s
        provider: resumable-k8s-pod
        # 替换零 digest；镜像需要已包含 Cellbox guest 和所需工具。
        image: registry.example.com/team/sandbox@sha256:0000000000000000000000000000000000000000000000000000000000000000
        namespace: cell-box
        nodeName: worker-01
        cpu: 1
        memoryMiB: 1024
        clients: [default]
        guest:
          workspace: /workspace
```

默认 API 使用客户端 `default`，Helm 自动生成其 Token。默认 profiles 为空，服务可以启动；配置 profile 后才能创建工作空间。Kubernetes profile 的 namespace 应使用 Controller 管理的 Helm release namespace。

| 配置 | 用途 |
| --- | --- |
| `imageRegistry` | 给相对镜像名添加仓库前缀；完整镜像地址保持原样 |
| `controller.image` / `api.image` | 平台镜像，支持 tag 或 `@sha256:` digest，生产部署建议固定 digest |
| `controller.adapterImage` | 默认使用 `controller.image`，升级 Controller 时同步升级节点安装镜像 |
| `controller.criDirectory` / `controller.criSocket` | 节点 containerd 的 CRI 目录和 socket |
| `controller.runtimeInstaller` | 自动安装 wrapper 和运行时配置；已有完整 Cellbox 节点适配时可设置 `enabled: false` |
| `api.namespace` | 默认使用 Helm release namespace |
| `api.config` | API JSON 配置的覆盖项，字段参考 [config/sample.json](config/sample.json) |
| `api.clientTokens` | `tokenEnv: token` 映射；Token 至少 32 字节，省略时自动生成并在升级时复用 |
| `api.manageSecrets` | 默认 `true`，由 Helm 管理配置和 Token；设为 `false` 可引用已有 Secret |
| `api.ossCredentialsSecret` | 可选的外部 Secret，提供 `OSS_ACCESS_KEY` 和 `OSS_SECRET_KEY` |
| `api.storageClassName` / `api.storageSize` | API 状态和归档的 PVC 存储配置 |

初始化容器和 BuildKit 的默认镜像是完整的 Docker Hub 地址，因此不会随 `imageRegistry` 改写。需要镜像代理时，分别覆盖 `api.initImage` 和 `buildkit.image`。

### 执行部署

安装和以后升级都使用同一条命令。将 `<context>` 替换为目标 Kubernetes context：

```bash
helm upgrade --install cellbox ./charts/cellbox \
  --kube-context <context> -n cell-box --create-namespace \
  -f charts/cellbox/values_local.yaml \
  --reset-values --wait --timeout 10m
```

修改 values 中的配置或镜像后，重新执行即可。保持 release 名称和 namespace 一致，并维护完整的本地覆盖文件。

当前生命周期字段为 `phase`，运行阶段为 `running`。持久化状态使用 schema 2；schema 1 文件会被拒绝读取，当前没有自动迁移。保留 PVC 的 Helm 升级不会执行状态格式迁移。

Chart 会部署 CRD、`runsc-recoverable` RuntimeClass、Controller DaemonSet、API Deployment / Service、RBAC、配置与 Token Secret，以及 API PVC。API 默认监听容器内的 `0.0.0.0:8090`，数据目录为 `/var/lib/cellbox`。

Controller 的特权 init container 将 wrapper 复制到节点 `/usr/local/libexec/cellbox-runsc-wrapper`，写入 `/var/lib/cellbox/runsc.toml`，并为 containerd 配置 `runsc-recoverable` handler。节点安装器保留原有配置；运行时 handler 需要激活时才重启节点服务，单独更新 wrapper 或 shim 配置不需要重启。

使用与当前源码一致的 Controller 镜像才能获得对应的安装器修复。Chart 中的 Docker Hub 默认镜像是已发布构建；自建的新版本可发布到任意仓库并通过 values 指定。

迁移此前手工创建的配置和 Token Secret 时，先将原值写入本地 values，再给升级命令临时添加 `--take-ownership`。完成归属迁移后，后续升级不再需要此参数。本机已完成该迁移。

### 本机 k3s 配置

k3s 使用以下覆盖项；普通 containerd 集群沿用公共 CRI 默认路径：

```yaml
controller:
  criDirectory: /run/k3s/containerd
  criSocket: /run/k3s/containerd/containerd.sock
  runtimeInstaller:
    mode: k3s

api:
  storageClassName: local-path
```

### 检查与访问

```bash
kubectl --context <context> -n cell-box get pods
kubectl --context <context> -n cell-box logs ds/cellbox-controller -c install-runtime-adapter
kubectl --context <context> -n cell-box port-forward \
  --address 127.0.0.1 svc/cellbox-api 8090:8090
```

另一个终端检查 API。默认客户端 Token 从 Secret 读取，不要提交到 Git：

```bash
export CELLBOX_CLIENT_TOKEN="$(kubectl --context <context> -n cell-box \
  get secret cellbox-api-client-tokens \
  -o jsonpath='{.data.CELLBOX_CLIENT_TOKEN}' | base64 -d)"
curl --noproxy '*' http://127.0.0.1:8090/healthz
curl --noproxy '*' -H "Authorization: Bearer $CELLBOX_CLIENT_TOKEN" \
  http://127.0.0.1:8090/v1/profiles
```

完整 REST 契约见 [api/openapi.yaml](api/openapi.yaml)。预览路由、grant 和授权代理由 API 服务提供。

启用 BuildKit 后，可调用 `POST /v1/images:import`，提交镜像仓库地址（tag 或 digest）及可选 `runCommand`。平台解析并固定源 digest、加入 guest、发布准备好的镜像；创建工作空间时传入返回的 `importedImageId`，无需修改 Profile 的默认镜像。导入支持 Linux amd64，具体参数和限制见 [使用说明](doc/cellbox-usage.md#动态导入用户镜像)。

可选 `buildCommand` 在 BuildKit 内以 root 执行 `/bin/sh -c`，用于安装依赖或打包平台文件，然后再注入 guest。源镜像需要包含 shell；构建命令参与缓存身份，`runCommand` 仍决定沙箱启动方式。

### 可选 BuildKit

需要通过 `POST /v1/images` 构建准备好的沙箱镜像时，启用 BuildKit：

```yaml
buildkit:
  enabled: true
  repository: registry.example.com/team/cellbox-prepared
  registryHost: registry.example.com
  registryHTTP: false
```

`repository` 是构建产物的完整仓库路径，`imageRegistry` 不会自动给它添加前缀。平台镜像发布包含 API 和 Controller；BuildKit 默认使用上游镜像，沙箱镜像由使用方准备。

私有仓库可通过 `buildkit.registryCredentialsSecret` 挂载 `kubernetes.io/dockerconfigjson` Secret，供 API 和 buildctl 读取认证配置。单次导入也可通过 `registryAuth` 提交源仓库的用户名和密码；该凭证不写入持久化状态。

集群访问外部仓库需要代理时，通过 `api.extraEnv` 和 `buildkit.extraEnv` 配置 `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY`。代理地址放在本地覆盖文件；`NO_PROXY` 应包含集群 Service / Pod 网段、`.svc`、`.cluster.local` 和直接访问的仓库地址。

BuildKit 使用 overlayfs 和 GC 预算，避免 native snapshotter 对多层镜像进行完整复制。可根据节点磁盘调整 `buildkit.cacheSize`、`buildkit.gc` 和临时存储资源配额。

### 卸载范围

当前重点是安装与升级，没有节点卸载 hook。`helm uninstall` 不会自动移除节点上的 wrapper 和 containerd 配置。API PVC、CRD 和 RuntimeClass 标记为保留；已有工作空间也不会由 Helm 自动删除。卸载前应先处理工作空间生命周期。

## 构建与自建镜像

需要 Go 1.26 或更高版本：

```bash
make check
make build
```

构建产物在 `dist/release/`，包括 `cellbox-api`、`cellbox-container-agent`、`cellbox-node-controller`、`cellbox-runsc-wrapper` 和 `cellbox-runtime-config`。

自建镜像使用构建产物目录作为 Docker context：

```bash
CELLBOX_REGISTRY=registry.example.com/team
CELLBOX_IMAGE_TAG=v0.1.0
docker build -f images/cellbox-api.Dockerfile \
  -t "$CELLBOX_REGISTRY/cellbox-api:$CELLBOX_IMAGE_TAG" dist/release
docker build -f images/controller.Dockerfile \
  -t "$CELLBOX_REGISTRY/cellbox-controller:$CELLBOX_IMAGE_TAG" dist/release
docker push "$CELLBOX_REGISTRY/cellbox-api:$CELLBOX_IMAGE_TAG"
docker push "$CELLBOX_REGISTRY/cellbox-controller:$CELLBOX_IMAGE_TAG"
```

将生成的 tag 或 digest 写入 values，再执行 Helm 升级。`make package` 要求工作区干净，会生成包含 Chart、镜像构建文件和测试脚本的发布包，排除本地 values 和 `config/local.json`。

## 本地 Docker 运行

使用 [config/sample.json](config/sample.json) 配置 Docker profile：替换沙箱镜像的零 digest，为 `clients[].tokenEnv` 设置至少 32 字节且互不相同的 Token，然后启动：

```bash
./dist/release/cellbox-api --config config/sample.json
```

本地默认监听 `127.0.0.1:8090`，数据目录为 `data/`。Docker profile 接受本地 `sha256:<digest>` 或 `repository@sha256:<digest>`。仅初始化配置中用到的 provider；Docker-only 配置不会连接 Kubernetes。Kubernetes profile 在集群内使用 ServiceAccount 凭证，集群外可指定 `--kubeconfig`。

## 测试

`make check` 执行 Go race tests、vet 和 Shell 语法检查。以下是可选的集成测试，需要相应环境：

| 测试 | 命令与要求 |
| --- | --- |
| Docker REST | `CELLBOX_DOCKER_SMOKE=1 go test -v ./test/cellbox-smoke`；需要本地 Docker，会移除自己创建的容器和镜像 |
| 镜像导入 | `CELLBOX_CLIENT_TOKEN=... test/scripts/e2e-image-import.sh <API-URL> <profile-id> <source-image>`；API 已启用 BuildKit，源镜像包含 sh / sleep；验证导入、创建、执行、暂停恢复和归档恢复，清理本次沙箱与归档，保留导入镜像记录 |
| Counter 镜像导入 | `CELLBOX_CLIENT_TOKEN=... test/scripts/e2e-counter-import.sh <API-URL> <profile-id> <counter-image>`；使用 `test/counter` 构建的普通镜像，验证计数、32 MiB 内存暂停恢复及文件归档恢复；保留源沙箱暂停、恢复沙箱运行。可设置 `CELLBOX_K8S_CONTEXT` 检查 Pod 更换和 Service 保留 |
| Helm install / upgrade | `test/scripts/e2e-helm.sh <context> <test-values-file>`；已有 CRD、RuntimeClass 和节点适配，在独立命名空间安装、升级并清理测试资源 |
| 节点重复安装 | `test/scripts/e2e-bootstrap.sh <context> [node-name]`；已部署 `cell-box` 中的 Controller，重建 DaemonSet Pod 并验证节点服务启动时间不变 |
| suspend / resume | `test/scripts/e2e-k8s.sh <context>`；`cell-box` 中已有 Running 的 `counter` CR 及计数器测试镜像，会挂起并恢复该工作空间 |
| 安装器回归 | `test/scripts/test-node-install.sh <controller-image>`；在隔离 Docker 容器中模拟 systemd，验证首次安装、重复安装及中断恢复 |

Helm E2E 的 values 文件只需提供测试集群的镜像、CRI 路径和存储类，API 配置与 Token 使用测试默认值。该脚本使用 `uv` 运行 Python 检查。测试证据默认写入被 Git 忽略的 `tmp/validation/`。

已经在真实 k3s 环境验证 Helm 首次安装、配置升级、重复升级，以及同节点 suspend / resume 后的进程内存和可写根文件系统恢复。
