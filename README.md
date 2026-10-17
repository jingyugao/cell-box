# Cellbox

Cellbox 通过 HTTP API 提供隔离沙箱。单机 Kubernetes 部署使用一个固定到指定节点的 `cellbox-server` 进程，提供 API 并管理该节点的 sandbox runtime。沙箱从模板或指定修订版本创建，可执行有时限的命令、访问工作区文件，并使用运行时 profile 支持的生命周期操作。

## 快速开始

- [使用流程与模板语义](doc/cellbox-usage.md)：模板、版本、预热池和沙箱生命周期的关系。
- [Go SDK 使用指南](doc/go-sdk.md)：从本地源码引用尚未发布的 SDK，创建模板和修订版本、创建沙箱，并使用生命周期与文件接口。
- [REST 快速开始](doc/rest-quickstart.md)：使用 `curl` 调用模板、构建任务和沙箱接口。
- [OpenAPI 接口契约](api/openapi.yaml)：完整的 HTTP 请求和响应结构。

模板、修订版本和构建任务保存在 server 本地 SQLite catalog 中。Kubernetes profile 的 sandbox 仍由 Cellbox 自定义资源持久化并拥有其 Pod/Service；API 和节点运行时在进程内通过队列与完成快照直接协作。这样减少一个进程间通知链路，仍保留 Kubernetes 对象写入和 Pod/CRI 操作。

## 构建与检查

仓库后端使用 Go 1.26 或更高版本。在仓库根目录运行：

```sh
make check
make build
```

## 部署

参阅[单机 Kubernetes 部署与升级](doc/deployment.md)，配置持久 catalog、对象存储、运行时 profile 和镜像构建器，再应用 Helm Chart。节点组件需先按[节点安装指南](doc/node-installation.md)准备。
