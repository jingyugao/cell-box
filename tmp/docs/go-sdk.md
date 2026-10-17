# Go SDK 使用指南

Go SDK 位于 [`sdk/go`](../../sdk/go)，使用 Go 1.22，模块路径为 `github.com/jingyugao/cell-box/sdk/go`。当前版本随 Cellbox 源码开发，尚未发布为 Go module。开发时通过本地 `replace` 引用此仓库。

## 在应用中添加 SDK

在应用的 Go module 目录中，将 SDK 替换为本地绝对路径，再添加依赖：

```sh
go mod edit -replace=github.com/jingyugao/cell-box/sdk/go=/home/wsl/Code/cell-box/sdk/go
go get github.com/jingyugao/cell-box/sdk/go@v0.0.0
```

请将路径换成你本机 Cellbox checkout 的绝对路径。使用此未发布版本期间，本地替换会保留在应用的 `go.mod` 中。需要时运行 `go mod tidy` 更新 `go.sum`。

## 连接 Cellbox

如果 `Config` 中对应字段为空，SDK 会读取以下环境变量：

| 变量 | 作用 |
| --- | --- |
| `CELLBOX_API_URL` | Cellbox API 地址，例如 `http://127.0.0.1:18092` |
| `CELLBOX_CLIENT_ID` | 模板及其他 client 所有数据的命名空间，可选 |
| `CELLBOX_CLIENT_TOKEN` | 仅当 Cellbox 前有认证网关时使用的 Bearer token；Cellbox API 本身不将它用于 catalog 认证 |

`ClientID` 用于隔离 catalog 命名空间，不是密码、唯一业务 ID 或认证身份。`OwnerKey` 只是沙箱的业务关联标签，不保证唯一，也不是幂等键或访问控制。省略 `ClientID` 时使用服务端配置的默认命名空间。可以通过 `HTTPClient` 配置 TLS 和代理。SDK 禁用 HTTP 重定向，避免将 token 转发到其他主机。

连接当前 k3s 测试部署时，将 `cell-box-v2` 命名空间里的 Service 转发到本机：

```sh
kubectl --context k3s -n cell-box-v2 port-forward --address 127.0.0.1 svc/cellbox-api 18092:8090
```

下面的模板 ID 只属于该部署，不能用于其他 Cellbox catalog：

```sh
export CELLBOX_API_URL=http://127.0.0.1:18092
export CELLBOX_TEMPLATE_ID=tpl-44312d31c4a0d898fcc9f585b9c01537
```

然后在仓库根目录运行示例：

```sh
go -C sdk/go run ./examples/basic
```

示例会使用模板族当前的 stable revision 创建沙箱、执行短命令、打印结果并用独立的 60 秒 context 尝试清理；清理失败会写入日志。若要固定到某个不可变修订版本，设置 `CELLBOX_REVISION_ID` 并省略 `CELLBOX_TEMPLATE_ID`。两个变量必须且只能设置一个。

## 创建模板并构建 revision

**Template** 表示模板族及其可变配置；**revision** 是构建生成的不可变运行镜像和 profile 快照。例如，用户版本 `1.01` 连续构建成功后会得到 `1.01-r1`、`1.01-r2`。已创建的沙箱仍固定使用创建时选中的修订版本。

```go
package main

import (
	"context"
	"fmt"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

func build(ctx context.Context, c *cellbox.Client) error {
	template, err := c.CreateTemplate(ctx, cellbox.CreateTemplateOptions{
		Name: "coding-agent",
		DefaultWarmSlots: 0,
	})
	if err != nil {
		return err
	}

	op, err := c.BuildRevision(ctx, template.ID, cellbox.CreateRevisionOptions{
		UserVersion: "1.01",
		BaseImage:   "docker.io/library/ubuntu:24.04",
		ProfileID:   "coding-k8s",
		BuildCommand: "apt-get update && apt-get install -y git ca-certificates",
		// Optional. This literal docker run form describes sandbox workload
		// settings and must reference BaseImage; Cellbox does not run Docker here.
		RunCommand: "docker run ubuntu:24.04 /bin/sh -c 'sleep infinity'",
	}, "coding-agent-1.01-build-1")
	if err != nil {
		return err
	}

	revision, err := c.WaitBuildRevision(ctx, op)
	if err != nil {
		return err
	}
	fmt.Printf("built %s (%s)\n", revision.Version, revision.ID)

	// Stable selection is an explicit metadata update using optimistic
	// concurrency. A successful build never publishes itself automatically.
	_, err = c.UpdateTemplate(ctx, template.ID, cellbox.UpdateTemplateOptions{
		StableRevisionID:   &revision.ID,
		ExpectedGeneration: template.Generation,
	})
	return err
}

func main() {
	client, err := cellbox.NewClient(cellbox.Config{})
	if err != nil { panic(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := build(ctx, client); err != nil { panic(err) }
}
```

构建需要一个可用的 profile，并配置好镜像构建器和仓库。构建前会将 `baseImage` 解析为不可变镜像。`buildCommand` 在镜像构建过程中以 root 执行；`runCommand` 是用字面 `docker run ...` 语法描述沙箱工作负载配置的输入，Cellbox 会解析其中支持的 command、entrypoint、环境变量、工作目录和端口，不会真的执行 `docker run`，也不会据此改写 OCI 镜像的 ENTRYPOINT。它必须引用相同的基础镜像。本期构建支持 `linux/amd64`。构建输入会保存在 revision 和 operation 记录中，不要把密码或 token 写进 `buildCommand`。

构建以异步任务提交。SDK 返回持久化的 operation，`WaitBuildRevision` 会等待完成并读取生成的 revision。应用可使用 `GetOperation`、`ListOperations` 和 `GetOperationLogs` 查看进度，或在进程重启后恢复状态。如果提交响应丢失，使用相同输入和相同幂等键重试即可取回原任务；相同幂等键配上不同输入会发生冲突。`CancelOperation` 会请求取消构建，但不能保证清理已经发布的镜像产物。

恢复已受理的构建时，持久化 operation ID。等待超时只停止客户端等待，不会取消服务端任务；使用新的 context 后，可用 `WaitOperation` 继续等待，也可显式调用 `CancelOperation`。提交失败时 `RequestError` 会保留幂等键；如果底层是网络错误、请求结果不确定且还没有 operation ID，才用新的 context、相同幂等键和完全相同的输入重试。确定的 `APIError`（例如无效请求）应先修正原因，不要盲目重试：

```go
import (
	"context"
	"errors"
	"fmt"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

op, err := client.BuildRevision(ctx, templateID, buildInput, buildKey)
if err != nil {
	var requestErr *cellbox.RequestError
	var apiErr *cellbox.APIError
	if errors.As(err, &requestErr) && !errors.As(requestErr.Err, &apiErr) {
		// 原 context 可能已超时；恢复请求使用新 context，保持输入和键不变。
		recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 30*time.Second)
		op, err = client.BuildRevision(recoveryCtx, templateID, buildInput, requestErr.IdempotencyKey)
		cancelRecovery()
	}
}
if err != nil { return err }

waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Minute)
defer cancelWait()
revision, err := client.WaitBuildRevision(waitCtx, op)
if err != nil {
	var operationErr *cellbox.OperationError
	if errors.As(err, &operationErr) && operationErr.Operation != nil {
		// 保存 ID；之后可以用新的 context 调用 WaitOperation。
		fmt.Println("operation:", operationErr.Operation.ID)
	}
	return err
}
_ = revision
```

日志读取可按 `NextOffset` 分页，SDK 单次最多读取 1 MiB：

```go
offset := int64(0)
for {
	chunk, err := client.GetOperationLogs(ctx, op.ID, offset, 64*1024)
	if err != nil { return err }
	fmt.Print(chunk.Text)
	if chunk.EOF { break }
	if chunk.NextOffset == offset {
		// 构建尚未产生新日志时暂停轮询，避免空转请求。
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	offset = chunk.NextOffset
}
```

`GetOperation` / `WaitOperation` 同时支持构建和运行时任务；`ListOperations`、日志和取消接口当前只支持模板构建。日志读到 EOF 后，仍需检查任务状态以区分成功、失败和取消。

SDK 的主要错误类型：`APIError` 表示 HTTP/API 错误，包含状态码和错误码；`RequestError` 表示一次提交请求失败并保留幂等键；`OperationError` 携带最近一次 operation 快照和底层错误；命令非零退出会返回 `CommandExitError`，同时保留 `CommandResult` 中的 stdout/stderr/exit code。使用 `errors.As` 提取这些类型。命令非零退出时，`Run` 会同时返回结果和 `CommandExitError`。

需要取消仍在运行的构建时，用 operation ID 显式请求取消；这只适用于可取消的模板构建任务：

```go
_, err := client.CancelOperation(ctx, operationID)
if err != nil { return err }
```

修改 `DefaultWarmSlots` 或 revision 的 `WarmSlots` 只会将期望配置保存到 SQLite catalog。本期不会据此创建、发布或统计 Kubernetes 预热池。构建 revision 时，`WarmSlots == nil` 会复制模板默认值；显式传入 `WarmSlots`（包括 `0`）则使用该值。

模板名创建后不可修改；`description`、`defaultWarmSlots` 和 `stableRevisionId` 可通过带 `ExpectedGeneration` 的 `UpdateTemplate` 修改。可用 `SetTemplateStableRevision(ctx, templateID, "", generation)` 清空 stable 指针。Revision 的镜像、基础镜像、构建命令、运行配置和 profile 快照不可变；`description`、`status`（`active`/`retired`）和 `warmSlots` 可更新。Retire stable revision 前必须先切换或清空 stable 指针；retired 只阻止后续选择，不影响已固定该 revision 的沙箱。revision 删除和运行引用统计接口尚未提供。只有没有任何 revision、也没有排队或运行中构建任务的空模板族可删除。

常用方法按资源整理如下：

| 能力 | SDK 方法 | 说明 |
| --- | --- | --- |
| 模板族 | `CreateTemplate`、`ListTemplates`、`GetTemplate`、`UpdateTemplate`、`DeleteTemplate` | 删除仅适用于空模板族；更新使用 generation 防止覆盖并发修改 |
| Revision | `BuildRevision`、`WaitBuildRevision`、`ListTemplateRevisions`、`GetTemplateRevision`、`UpdateTemplateRevision`、`GetRevisionWarmPool` | 构建成功后生成不可变 revision；可改描述、状态和预热配置 |
| 构建任务 | `GetOperation`、`WaitOperation`、`ListOperations`、`GetOperationLogs`、`CancelOperation` | 查看/等待/取消构建，按 offset 读取日志；不会自动重放构建命令 |
| Profile / 执行记录 | `ListProfiles`、`GetExecution` | Profile 是服务端配置；执行记录用于读取已完成命令的结果 |
| 沙箱 | `Create`、`CreateAsync`、`Connect`、`GetSandbox`、`ListSandboxes`、`GetTemplateRuntime`、`Sandbox.Destroy` | 清单和运行时快照属于运行时接口；服务端后台观察 Kubernetes，查询接口读取快照 |
| 命令 | `Commands.Run`、`RunArgv`、`Start`、`StartArgv`、`Command.Wait` | 有时限的单次命令，不提供 PTY 或流式输出 |
| 文件 | `Files.Open`、`Read`、`Write`、`List` | workspace 相对路径；不自动恢复沙箱 |
| 生命周期 | `Suspend`、`Resume`、`Freeze`、`Unfreeze`、`EnsureRunning`、`Activate` | 依 profile 能力使用；`Resume`、`EnsureRunning`、`Activate` 返回新 handle，需接收返回值 |

## 创建并使用沙箱

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
defer cancel()

sandbox, err := client.Create(ctx, cellbox.CreateOptions{
	TemplateID: templateID, // follows the current stable revision
	OwnerKey:   "job-42",
})
if err != nil {
	return err
}
defer func() {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Minute)
	defer cancelCleanup()
	if err := sandbox.Destroy(cleanupCtx, nil); err != nil {
		log.Printf("destroy sandbox %s: %v", sandbox.ID, err)
	}
}()

result, err := sandbox.Commands.Run(ctx, "git --version && pwd", &cellbox.CommandOptions{
	Timeout: 15 * time.Second,
})
if result != nil {
	fmt.Print(result.Stdout)
}
if err != nil {
	return err // nonzero exit returns both result and *CommandExitError
}
```

如需固定到特定 revision，传入 `RevisionID` 而不是 `TemplateID`，二者必须且只能设置一个。`OwnerKey` 是调用方填写的业务关联标签，不保证唯一，也不用于幂等或认证。`Sandbox` handle 保存创建或连接时观察到的 runtime generation；其他调用方更改沙箱后，应通过 `Client.Connect` 重新连接。

`Run`/`Start` 使用 `/bin/sh -c` 执行命令；`RunArgv`/`StartArgv` 则直接调用绝对路径程序。命令执行有时限，不提供交互式终端或无限期后台进程。服务端默认超时为 30 秒，可设置 1 毫秒至 5 分钟；`Start` 只是先返回已受理的有时限命令 handle，不会让命令脱离服务端超时限制。命令结束后一次性返回输出，不提供流式输出。

默认 HTTP client 对每个请求设置 30 秒总超时（包含读取响应体）；调用方传入的 context 可再设置更短的整体截止时间。长时间等待构建时请给 context 留足时间。若要下载耗时较长的大文件，可通过 `Config.HTTPClient` 提供合适的超时时间。取消 context 只停止客户端请求/等待，不会自动取消已受理的服务端 operation。

## 生命周期和文件

`Sandbox` 提供的生命周期方法取决于所选 profile：`k8s-resumable` 支持 `Suspend`/`Resume`，`docker-normal` 支持 `Freeze`/`Unfreeze`。这些方法接受包含幂等键的 `LifecycleOptions`。`Resume`、`EnsureRunning` 和 `Activate` 会返回一个**新的 sandbox handle**；后续执行命令前请改用新 handle。请先检查 `SandboxInfo.Capabilities`，确认当前 profile 支持所需操作。`Client.ListSandboxes` 返回沙箱清单快照，不会连接或恢复沙箱；这是运行时清单接口，可能会查询 Kubernetes。

`Sandbox.Files` 可在配置的 workspace 下读取、写入和列出文件，路径相对于 workspace。文件访问不会自动恢复已挂起的沙箱。`Read` 最大读取 16 MiB；`Write` 最大写入 16 MiB，会覆盖目标文件（非原子操作，父目录必须已存在）；`Open` 流式下载并返回 `io.ReadCloser`，调用方必须关闭它。文件请求作用于当前沙箱，不使用命令和生命周期接口的 generation compare-and-swap。网络错误导致写入结果不确定时，不要自动重试。

仓库还提供完整的模板构建和生命周期示例：

```sh
# 创建/复用模板，构建 revision，将它设为 stable，然后创建沙箱执行工具
export CELLBOX_PROFILE_ID='<当前部署中已配置的 profile ID>'
export CELLBOX_BASE_IMAGE='docker.io/library/ubuntu:24.04'
go -C sdk/go run ./examples/template

# 在模板的 stable revision 上测试文件接口以及 suspend/resume 或 freeze/unfreeze
export CELLBOX_TEMPLATE_ID='<上一步创建或复用的模板 ID>'
go -C sdk/go run ./examples/lifecycle
```

模板示例支持以下环境变量：

| 环境变量 | 是否必需 | 作用 |
| --- | --- | --- |
| `CELLBOX_API_URL` | 必需 | API 地址；也可通过 `Config.BaseURL` 指定 |
| `CELLBOX_PROFILE_ID`、`CELLBOX_BASE_IMAGE` | 必需 | 构建使用的 profile 与基础镜像 |
| `CELLBOX_TEMPLATE_ID` | 可选 | 复用现有模板；不设置时创建新模板 |
| `CELLBOX_TEMPLATE_NAME`、`CELLBOX_USER_VERSION` | 可选 | 新模板名和用户版本；默认随机名和 `1.01` |
| `CELLBOX_BUILD_COMMAND`、`CELLBOX_RUN_COMMAND` | 可选 | 自定义镜像构建命令和 workload 配置 |
| `CELLBOX_VERIFY_COMMAND` | 可选 | revision 构建后在沙箱中执行的检查命令 |
| `CELLBOX_BUILD_KEY`、`CELLBOX_PLATFORM` | 可选 | 构建幂等键和平台；平台默认为 `linux/amd64` |
| `CELLBOX_CLIENT_ID`、`CELLBOX_CLIENT_TOKEN` | 可选 | 数据命名空间；仅有前置认证 Gateway 时设置 token |

默认构建命令会安装示例工具 `template-tool`；如果自定义了 `CELLBOX_BUILD_COMMAND`，示例默认只打印提示并查看工作目录，不会假设自定义镜像里有该工具。可用 `CELLBOX_VERIFY_COMMAND` 指定实际验证命令。模板示例和生命周期示例创建的 sandbox 都会尝试自动销毁；模板和构建出的 revision 会保留。生命周期示例设置 `CELLBOX_TEMPLATE_ID` 或 `CELLBOX_REVISION_ID` 其中之一即可。其他示例代码位于 [`sdk/go/examples`](../../sdk/go/examples)。HTTP 契约和各 provider 的能力字段见 [OpenAPI 文档](../../api/openapi.yaml)。SDK 当前不提供 PTY/流式交互，也没有封装所有归档、路由或内部服务接口。

## 预热池和运行时状态

Revision 构建成功且状态为 `active`、`warmSlots > 0` 时，服务端后台会发布预热需求；`warmSlots` 为 `0` 或 revision 退休时会撤回需求。创建 template 只保存默认值，不会单独创建预热池；构建 revision 时 `warmSlots == nil` 才继承模板默认值。stable 指针切换不会改变旧 revision 的预热需求。后台发布失败会继续重试。

预热 Pod 按兼容的运行配置共享池，节点总容量受 Controller 限额约束（默认最多 2 个，配置上限 32 个）；因此各 revision 的 `requestedSlots` 可能因共享和预算而小于各自需求，不能把每个 revision 的资源字段相加当作节点池总量。`resources` 是当前分配的空闲预热 Pod 的 Kubernetes requests/limits 配置，不是实际 CPU/内存使用量。预热 Pod 用于 resumable sandbox 的恢复路径，不能据此承诺首次冷创建会更快。撤回需求只清理空闲预热 Pod，不会杀掉已经领用的 sandbox。

```go
runtime, err := client.GetTemplateRuntime(ctx, templateID)
if err != nil { return err }
for _, revision := range runtime.Revisions {
	if revision.Stale || revision.ObservedAt == nil {
		log.Printf("revision %s runtime status is not current", revision.RevisionID)
		continue // SandboxCount nil means unknown, not zero.
	}
	log.Printf("revision %s sandboxes=%d phases=%v", revision.RevisionID, *revision.SandboxCount, revision.Phases)
	if revision.WarmPool.Pool != nil {
		log.Printf("pool=%s ready=%d/%d stale=%t", revision.WarmPool.Pool.PoolID, revision.WarmPool.Pool.ReadySlots, revision.WarmPool.Pool.RequestedSlots, revision.WarmPool.Pool.Stale)
	}
}

warm, err := client.GetRevisionWarmPool(ctx, revisionID)
if err != nil { return err }
log.Printf("sync=%s desired=%d applied-generation=%d current=%t", warm.SyncStatus, warm.DesiredSlots, warm.ObservedGeneration, !warm.Stale)
```

服务端约每 2 秒在后台同步需求并读取 Controller 快照；查询不会在请求内调用 Kubernetes。`observedAt` 是快照采集时间，超过约 15 秒或未完成首次同步时 `stale` 为 true。同步失败时检查 `error` / `syncStatus`；`sandboxCount == nil` 表示未知。预热池不存在时 `pool` 为 null。
