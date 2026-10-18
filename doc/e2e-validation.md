# 单机版部署与端到端验收

验收入口为 [`test/scripts/e2e-full.sh`](../test/scripts/e2e-full.sh)。测试通过公开 REST API 构建模板和操作 sandbox，通过只读 Kubernetes 查询核对实际 Pod、Service 和预热领用。可额外执行 Go SDK 生命周期示例。测试不创建诊断 Pod。

## 环境和部署边界

- 使用独立测试 namespace；当前本地环境为 `k3s` context、`cell-box-v2` namespace、`cellbox-v2` Helm release。脚本拒绝 `cell-box`、`co-cell`、`default` 和 `kube-system`。
- 部署新版 `cellbox-server`，固定单节点，启用 BuildKit、S3 归档和最多两个预热槽位。服务数据目录挂载持久存储。
- 测试开始前要求该 namespace 没有业务 sandbox。`--restart-server` 会滚动重启指定测试 Deployment；重启前再次检查所有 sandbox 都属于本次测试。
- 旧双进程版的 catalog/对象元数据不自动迁移。升级测试环境前导出历史模板记录和 Helm 配置，保留旧宿主机目录，另用持久目录承载新版 SQLite。
- 共用节点上的现有 CoCell、旧 `cell-box`、全局 CRD、RuntimeClass 和 gVisor 不随本次测试升级。测试前后核对其 Pod UID、镜像和状态。

## 自动验收用例

| 用例 | 关键断言与证据 |
| --- | --- |
| 服务形态与能力 | 运行单个 `cellbox-server`，profile 宣告同节点 checkpoint 和 `stopped-workspace` 归档 |
| 删除的 API | 旧镜像、升级、重建、ensure-running、reconcile、租约、路由接口返回 404 |
| 模板构建 | `buildCommand` 安装可执行工具；镜像解析为 digest；revision 与构建日志可读取；继承模板默认预热配置 |
| 无幂等处理 | 同一 `Idempotency-Key` 的两次创建产生不同 sandbox/operation；两次 exec 的副作用发生两次 |
| 基础使用 | 实际执行自定义工具；文件写入、读取、列表、Range 下载与 HTTP 服务代理可用 |
| 请求边界 | 跨 client 读取模板、版本、sandbox、operation、execution、archive 返回 404；路径穿越拒绝；运行态归档拒绝 |
| 暂停与预热恢复 | 暂停后文件请求不隐式恢复；resume 后 Pod UID 属于请求前已存在的预热候选，保留 warm label；Service UID/IP 不变 |
| 进程连续性 | checkpoint 前后计数、PID、启动时间、实例随机值、32 MiB 内存 SHA256 和 workspace marker 完全一致 |
| 普通恢复 | 将 revision 预热需求设为 0 后执行另一轮暂停恢复，确认未领取 warm Pod，且进程状态仍连续 |
| generation | 恢复后使用旧 generation 的 exec 返回 `STALE_GENERATION` |
| 服务重启 | 先启动带一次文件副作用的长命令和长构建，再滚动重启测试 server；二者返回 `UNKNOWN_OUTCOME`，exec 状态为 unknown，没有重放或新增 revision；业务 Pod 和计数器内存保持原状 |
| SQLite 持久化 | server Pod UID 变化后，模板、stable 指针、revision、operation、执行状态及预热需求仍可读取 |
| 多版本 | 相同基础镜像、工具版本变化生成 `1.01-r1` / `1.01-r2`；stable 只影响新实例；显式 revision 固定旧版；不同镜像不能共用池 |
| 运行统计 | template runtime 返回两个 revision 的实际实例数和非过期观察结果 |
| 构建取消 | 显式取消构建进入 cancelled，操作清单可查，没有生成 revision |
| 归档 | 源实例保持暂停；下载归档的大小和 SHA256 与元数据一致，tar 中 workspace 证明文件逐字节一致 |
| 恢复与版本迁移 | 默认跨镜像恢复拒绝；同镜像恢复创建全新 staged 实例且不启动业务；显式 activate 后工作区保留、业务进程重新启动；显式 `acceptImageChange` 可迁移到新版工具镜像 |
| Go SDK | 实际 create、files、suspend、resume、更新 handle、exec、destroy 全流程成功 |
| 清理 | 本次 sandbox 和归档删除，revision 退休并撤回全部预热需求；不能删除的构建元数据和 registry 镜像在报告中列明 |

相同运行 fingerprint 共享池由控制器单元测试覆盖。端到端测试不假设不同 digest 的镜像兼容，也不把 `readySlots > 0` 单独当作预热命中证据。记录的 warm/cold resume 时间只是本次功能验收样本，不用于统计性能结论。

自定义工具以 `/usr/local/bin/cellbox-e2e-tool` 绝对路径执行；命令接口的默认 PATH 为 `/usr/bin:/bin`，测试不依赖镜像的 PATH。

`UNKNOWN_OUTCOME` 也不表示 guest 内的命令已停止。重启用例的命令有明确超时边界，测试等待该边界后才继续归档所需的暂停，避免把仍在运行的命令产生的 `BUSY` 误判为暂停故障。

## 构建和运行

需要 `go`、`uv`、`kubectl`、`helm`、可用 Docker，以及节点和 BuildKit 可访问的镜像仓库。测试源码镜像包含 `/server` 计数器和 Alpine 的 shell，不能用不含 shell 的旧 scratch counter 镜像代替。

```bash
mkdir -p tmp/e2e-counter-image
CGO_ENABLED=0 GOOS=linux go build -o tmp/e2e-counter-image/server ./test/counter
cp test/e2e/Dockerfile tmp/e2e-counter-image/Dockerfile
docker build -t REGISTRY/cellbox-e2e-counter:acceptance tmp/e2e-counter-image
docker push REGISTRY/cellbox-e2e-counter:acceptance
```

将 push 返回的不可变 digest 填入下面的 `--source-image`。部署流程见[部署文档](deployment.md)；部署前先完成 `go test -race`、`go vet`、Chart 校验，发布并验证 server 镜像 digest，然后执行 Helm upgrade。

```bash
test/scripts/e2e-full.sh \
  --context k3s --namespace cell-box-v2 --profile-id basic-k3s \
  --source-image REGISTRY/cellbox-e2e-counter@sha256:DIGEST \
  --restart-server --sdk \
  --evidence-dir tmp/validation/full-e2e
```

脚本管理绑定 `127.0.0.1` 的 port-forward，重启后自动重新连接。直接访问 server 不需要 token；经过认证网关时可设置 `CELLBOX_CLIENT_TOKEN`。每次运行使用独立 client namespace 和唯一模板名；有副作用的请求只提交一次。

省略 `--restart-server` 或 `--sdk` 时不执行对应部分。报告会标记未执行项，这种运行不能代替完整验收。生产服务不要启用重启测试。

Docker 的独立验收使用：

```bash
CELLBOX_DOCKER_SMOKE=1 go test -count=1 ./test/cellbox-smoke
```

Docker 冒烟验证基础创建、exec、文件、HTTP 代理、销毁，以及不支持的复杂请求明确返回 `UNSUPPORTED_CAPABILITY`；服务层和 provider 单元测试补充其他能力拒绝分支。

## 证据和通过标准

`report.json` 保存每个检查点、所有受理的资源 ID、计时、错误、清理结果和保留的构建元数据。目录还包含 revision、构建日志、terminal operation、恢复前后 CR/Pod/Service、预热候选 UID、重启前后 server Pod 和 SDK 日志。失败时保留 `failure.txt` 和运行时证据，再执行清理。

完整验收要求退出码为 0、`report.status == "passed"`、无 skipped 项、无 cleanupErrors；同时确认旧环境 Pod UID 和镜像未因部署发生变化。失败应修复后重新运行完整测试，保留失败报告，不把缺失结果记为通过。

构建成功的 revision 没有删除接口，因此测试只退休这些 revision、清空 stable 并将 warmSlots 设为 0；模板/revision/operation 元数据和 registry 构建镜像会保留。它们会在报告中列明，不作为“全部资源已删除”处理。
