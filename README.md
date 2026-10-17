# Cellbox

Cellbox 通过 HTTP API 提供隔离沙箱。沙箱从模板或指定修订版本创建，可执行有时限的命令、访问工作区文件，并使用运行时 profile 支持的生命周期操作。

## 快速开始

- [Go SDK 使用指南](tmp/docs/go-sdk.md)：从本地源码引用尚未发布的 SDK，创建模板和修订版本、创建沙箱，并使用生命周期与文件接口。
- [REST 快速开始](tmp/docs/rest-quickstart.md)：使用 `curl` 调用模板、构建任务和沙箱接口。
- [OpenAPI 接口契约](api/openapi.yaml)：完整的 HTTP 请求和响应结构。

模板、修订版本和构建任务保存在 API 服务本地 SQLite catalog 中。模板管理接口使用本地 catalog，不查询 Kubernetes。沙箱创建和运行时操作由配置的 runtime provider 执行。

## 构建与检查

仓库后端使用 Go 1.26 或更高版本。在仓库根目录运行：

```sh
make check
make build
```

## 部署前提

Helm Chart 位于 [`charts/cellbox`](charts/cellbox)。Kubernetes 节点需准备 Linux/containerd 和 [Cellbox gVisor 节点组件](doc/node-installation.md)；Chart 默认调度 Linux amd64。API 单副本运行，SQLite catalog 必须使用持久本地/块存储：配置 `localMetadata.enabled=true` 与 `localMetadata.nodeName`，让 API 和 Controller 固定在同一节点并共享 hostPath；或给 `api.catalogExistingClaim` 配置已有持久 PVC。

首次使用全新的节点元数据目录时，设置 `localMetadata.initialize=true`；初始化完成后改回 `false`。环境配置保存在被 Git 忽略的 `charts/cellbox/values_local.yaml` 中。

沙箱 profile 在 `api.config.profiles` 中配置，包含唯一的 profile ID、provider、镜像、资源和 guest/runtime 配置；Kubernetes profile 还需设置 `nodeName` 与 `namespace`。归档和 checkpoint 内容要求配置 `objectStorage.bucket`；私有对象存储凭据通过已有 Secret 的 `objectStorage.credentialsSecret` 注入，Secret 默认包含 `OSS_ACCESS_KEY` 和 `OSS_SECRET_KEY`。

模板构建需启用 `buildkit.enabled` 并配置完整的 `buildkit.repository`；私有镜像仓库可通过已有 `kubernetes.io/dockerconfigjson` Secret 配置 `buildkit.registryCredentialsSecret`。Cellbox API 不内置客户端身份认证；需要认证时，应在 API 前部署 Gateway 并由 Gateway 校验请求。`ClientID` 只是数据命名空间，不代表用户身份；SDK 的 `CELLBOX_CLIENT_TOKEN` 仅作为 Bearer token 发给前置 Gateway。

自建 API/Controller 镜像：

```sh
make build
docker build -f images/cellbox-api.Dockerfile -t registry.example.com/team/cellbox-api:v1 dist/release
docker build -f images/controller.Dockerfile -t registry.example.com/team/cellbox-controller:v1 dist/release
docker push registry.example.com/team/cellbox-api:v1
docker push registry.example.com/team/cellbox-controller:v1
```

将镜像地址写入 `api.image` 和 `controller.image`，并按环境维护 Chart values。SDK 和接口调用入口见上方快速开始；配置字段详见 [`charts/cellbox/values.yaml`](charts/cellbox/values.yaml) 与 [`config/sample.json`](config/sample.json)。
