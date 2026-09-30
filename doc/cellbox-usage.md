# Cellbox 安装后使用说明

本文面向已通过 Helm 部署 Cellbox 的使用者，按“连接 API → 选择配置 → 创建沙箱 → 执行命令和访问文件 → 暂停恢复 → 销毁”的顺序介绍操作。适用于 Kubernetes，包括本机 k3s。

部署方法见 [README](../README.md)，完整请求与响应见 [OpenAPI](../api/openapi.yaml)。镜像导入功能需要部署包含 `POST /v1/images:import` 的新版本 API；此前 master `6f59a29` 不包含该接口。

## 1. 使用前需要知道的对象

| 对象 | 用途 |
| --- | --- |
| Client | 调用方身份，通过 Bearer Token 认证；不同 Client 的工作空间相互隔离 |
| Profile | 管理员预先配置的沙箱模板，包含镜像、CPU、内存、节点、工作目录及允许使用的 Client |
| Box | 一个隔离工作空间，创建后获得 `boxId` |
| Operation | 创建、执行命令、生命周期操作、归档和镜像构建的异步任务 |
| Execution | 一次命令执行，保存退出码、标准输出和标准错误 |
| Archive | 工作目录的文件归档，可用于创建新的工作空间 |
| Route / Grant | 沙箱内服务的访问入口及有时效的访问凭证 |

日常操作调用 API；Controller 和节点 wrapper 由平台负责运行。调用方不需要登录宿主机，也不需要直接编辑沙箱 Pod 或 CR。

## 2. 连接 API 并取得 Token

以下命令在 Linux 的 **Bash** 中执行，需要 `kubectl`、`curl`、`jq`；配置升级另外需要 `helm`。示例使用默认 release `cellbox`、namespace `cell-box` 和默认客户端 Token。自定义部署请替换这些名称。

在终端 A 中运行并保持连接：

```bash
export CELLBOX_CONTEXT=k3s  # 其他集群替换为实际 context
export CELLBOX_NAMESPACE=cell-box

kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" get pods
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" port-forward \
  --address 127.0.0.1 svc/cellbox-api 18090:8090
```

在终端 B 中设置调用参数：

```bash
export CELLBOX_CONTEXT=k3s
export CELLBOX_NAMESPACE=cell-box
export CELLBOX_URL=http://127.0.0.1:18090
export CELLBOX_TOKEN_ENV=CELLBOX_CLIENT_TOKEN

# 只列字段名称，不输出 Token；自定义部署将上面的变量改为实际 tokenEnv。
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  get secret cellbox-api-client-tokens -o json | jq -r '.data | keys[]'

CELLBOX_CLIENT_TOKEN="$(
  kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
    get secret cellbox-api-client-tokens -o json | \
    jq -er --arg key "$CELLBOX_TOKEN_ENV" '.data[$key]' | base64 -d
)"
export CELLBOX_CLIENT_TOKEN
test -n "$CELLBOX_CLIENT_TOKEN"

curl --noproxy '*' --fail-with-body "$CELLBOX_URL/healthz"
curl --noproxy '*' --fail-with-body \
  -H "Authorization: Bearer $CELLBOX_CLIENT_TOKEN" \
  "$CELLBOX_URL/v1/profiles" | jq .
```

`/healthz` 无需认证，其余管理 API 使用 Client Token。`CELLBOX_CLIENT_TOKEN` 是本地调用变量，Secret 字段名称由 `clients[].tokenEnv` 决定；本机部署保留了自定义 Client 配置，应按列出的字段取值。Token 不要写入文档、提交到 Git 或交给沙箱内的应用。

集群内调用方可使用 `http://cellbox-api.cell-box.svc.cluster.local:8090`。外部长期访问可在集群入口配置 HTTPS 并转发到 API Service，再将 `CELLBOX_URL` 改为实际地址。

## 3. 定义调用与等待函数

后续示例在终端 B 的同一个 Bash 会话中执行，先加载这些函数：

```bash
api() {
  local method="$1" endpoint="$2"
  shift 2
  curl --noproxy '*' --silent --show-error --fail-with-body \
    --connect-timeout 10 --max-time 60 \
    -H "Authorization: Bearer $CELLBOX_CLIENT_TOKEN" \
    -X "$method" "$CELLBOX_URL$endpoint" "$@"
}

new_key() {
  cat /proc/sys/kernel/random/uuid
}

wait_operation() {
  local op="$1" id status deadline
  id=$(jq -er '.id' <<< "$op") || return 1
  deadline=$((SECONDS + 900))
  while true; do
    status=$(jq -er '.status' <<< "$op") || return 1
    case "$status" in
      succeeded) printf '%s\n' "$op"; return 0 ;;
      failed) printf '%s\n' "$op" | jq . >&2; return 1 ;;
      queued|running) ;;
      *) printf '未知 operation 状态：%s\n' "$status" >&2; return 1 ;;
    esac
    if (( SECONDS >= deadline )); then
      printf '等待超时，operationId=%s；任务可能仍在执行\n' "$id" >&2
      return 1
    fi
    sleep 1
    op=$(api GET "/v1/operations/$id") || return 1
  done
}
```

创建等异步请求返回 HTTP `202` 和 Operation，表示请求已受理。必须等 `status=succeeded` 才进入依赖它的下一步；`failed` 时查看 `error.code` 和 `error.message`。本文的 `api` 函数会让 HTTP 错误返回非零退出码，执行示例时遇到错误应停止后续步骤。

异步写操作需要 `Idempotency-Key`。每个新的业务操作生成一个 Key；网络超时后重试同一请求时，使用**原 Key 和完全相同的参数**，可取回原 Operation。不要为一次不确定是否完成的操作生成新 Key，尤其是创建和执行命令。同 Key 修改参数会返回 `409`；已失败的 Operation 也不会因重复请求而自动重跑。

## 4. 选择或配置 Profile

```bash
api GET /v1/profiles | jq '.[] | {id, kind, image, workspace, capabilities}'
export PROFILE_ID=coding-k8s  # 替换为返回列表中允许使用的 id
```

Kubernetes profile 的 `kind` 为 `k8s-resumable`，`capabilities.suspend` 为 `same-node-checkpoint`。以响应中的 capabilities 判断可用功能。

### 如果列表为空

Chart 默认不绑定产品或沙箱镜像，profiles 为空。管理员需要准备含 Cellbox guest 的镜像，再配置 Profile。

已有准备好的镜像可直接使用，必须是完整的 `repository@sha256:<64 位 digest>`。普通 Ubuntu / Alpine 基础镜像不能直接作为 Profile 镜像；它们还没有 `/opt/cellbox/bin/cellbox-container-agent`。

如果部署时启用了 BuildKit，并配置了可推送的 `buildkit.repository`，可以通过 API 给基础镜像注入 guest：

```bash
# 替换为实际 Linux amd64 基础镜像；使用真实 digest，不接受 tag。
export BASE_IMAGE='registry.example.com/team/base@sha256:<64位真实digest>'
BUILD_KEY=$(new_key)
BUILD_OP=$(api POST /v1/images \
  -H "Idempotency-Key: $BUILD_KEY" \
  -F "baseImage=$BASE_IMAGE" \
  -F 'platform=linux/amd64')
BUILD_DONE=$(wait_operation "$BUILD_OP")
PREPARED_IMAGE=$(jq -er '.result.image' <<< "$BUILD_DONE")
printf '%s\n' "$PREPARED_IMAGE"
```

基础镜像需要包含后续命令使用的工具；本文示例依赖 `/bin/sh`、`/bin/cat`。构建接口可选上传未压缩的 `product` tar，将文件放入 `/opt/product/`，以及 JSON 字段 `manifest`。它不会自动安装 Python、Node.js 或业务应用，也不会自动注册 Profile。未启用镜像构建时该接口返回 `422`。

将准备好的镜像加入 `charts/cellbox/values_local.yaml` 的 `api.config.profiles`。下面只展示要合入的字段，保留文件中现有镜像仓库、存储、凭证等配置：

```yaml
api:
  config:
    profiles:
      - id: coding-k8s
        provider: resumable-k8s-pod
        image: registry.example.com/team/prepared@sha256:<64位真实digest>
        namespace: cell-box
        nodeName: worker-01
        cpu: 1
        memoryMiB: 1024
        clients: [default]
        guest:
          workspace: /workspace
```

替换镜像和节点名称；本机节点可通过 `kubectl --context k3s get nodes` 查看。`namespace` 使用 Controller 管理的 release namespace，`clients` 与 API 配置中的 Client ID 一致。Helm 会替换整个 profiles 列表，增加 Profile 时要保留仍需使用的条目。

在仓库根目录执行升级，然后重新查询 `/v1/profiles`：

```bash
helm upgrade --install cellbox ./charts/cellbox \
  --kube-context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  -f charts/cellbox/values_local.yaml --reset-values --wait --timeout 10m
```

API Pod 更新后，终端 A 的 port-forward 可能断开，重新执行即可。Profile 配置变更作用于随后创建的 Box；已有 Box 保留创建时的 Profile 配置。

### 动态导入用户镜像

启用 BuildKit 后，用户可以直接提交 `docker pull` 使用的镜像引用。支持 tag，例如 `docker.io/team/app:v1`；导入时解析为固定 digest，原 tag 随后变化不会影响已导入镜像。这里只接收镜像仓库引用，不接收 HTTP 下载地址。

```bash
IMPORT_KEY=$(new_key)
IMPORT_OP=$(api POST /v1/images:import \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IMPORT_KEY" \
  --data '{
    "url":"docker.io/team/app:v1",
    "runCommand":"docker run -e MODE=demo -w /workspace -p 127.0.0.1:18080:8080 docker.io/team/app:v1 /bin/sh -c '\''echo ready; sleep 3600'\''"
  }')
IMPORT_DONE=$(wait_operation "$IMPORT_OP")
IMPORTED_IMAGE_ID=$(jq -er '.result.importedImageId' <<< "$IMPORT_DONE")
api GET "/v1/images/$IMPORTED_IMAGE_ID" | jq .
api GET /v1/images | jq .
```

将示例镜像替换为实际镜像；`url` 与 `runCommand` 中的镜像必须相同。不传 `runCommand` 时，沿用镜像中的 ENTRYPOINT 和 CMD。平台保留镜像的 ENV 和 WorkingDir，命令中的 `-e` / `-w` 可以覆盖它们。工作目录不会因 `-w` 改变；文件 API 仍以 Profile 的 workspace 为根。

需要在镜像里安装依赖或打包平台内容时，增加可选的 `buildCommand` 字符串。例如以下请求会先把平台文件写入镜像，再注入 Cellbox guest：

```json
{
  "url": "docker.io/library/alpine:3.22",
  "buildCommand": "mkdir -p /opt/product && printf '%s' 'platform-ready' > /opt/product/platform.txt",
  "runCommand": "docker run -w /workspace docker.io/library/alpine:3.22 /bin/sh -c 'cat /opt/product/platform.txt; sleep 3600'"
}
```

`buildCommand` 在 BuildKit 构建容器内以 root 执行 `/bin/sh -c`，可使用管道、重定向和多行脚本；源镜像必须包含 `/bin/sh` 及命令需要的工具。构建使用源镜像原有的 ENV 和 WorkingDir，`runCommand` 的 `-e` / `-w` 只影响沙箱启动。构建命令最多 65536 字节，会保存在导入记录中并参与缓存标识；修改它会产生不同的镜像身份。打包出的文件需允许 Profile 的 agent UID/GID 读取或执行。构建失败时 Operation 为 `failed`，不会注册可用的导入镜像。未传该字段时保持原有导入流程。

导入不会运行用户提交的 Docker 命令。支持的参数为 `-e` / `--env`、`-w` / `--workdir`、`--entrypoint` 和 TCP `-p` / `--publish`。端口只作为导入元数据保存，访问服务仍需创建 Route；不会在宿主机绑定命令里的端口。`--name`、`-d` 和 `--rm` 的差异在响应 `warnings` 中说明，生命周期由 Cellbox 管理。主机目录挂载、特权、host network、交互终端及其他未支持参数会返回错误；外层 shell 的变量展开、命令替换、管道和重定向也不接受，带引号的 `/bin/sh -c` 参数作为业务命令保存。

当前导入支持 `linux/amd64`，可选 `platform` 字段也只接受该值。镜像 USER 由 Profile 的 agent UID/GID 替代，相关说明在 `warnings` 中返回；需要 root 启动的应用应先适配为非 root。镜像的 ONBUILD 指令不支持。

私有源仓库可以通过额外字段 `registryAuth` 提交 `username` 和 `password`；不要将凭证拼入 URL。凭证只用于本次解析和构建，不会出现在导入记录或持久化状态中。管理员也可使用 `buildkit.registryCredentialsSecret` 配置平台仓库认证。

创建时引用这个 Client 自己导入的镜像：

```bash
CREATE_IMPORTED_KEY=$(new_key)
CREATE_IMPORTED_BODY=$(jq -nc --arg profile "$PROFILE_ID" \
  --arg imported "$IMPORTED_IMAGE_ID" \
  '{profileId:$profile, ownerKey:"import-demo", importedImageId:$imported}')
CREATE_IMPORTED_OP=$(api POST /v1/boxes \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $CREATE_IMPORTED_KEY" --data "$CREATE_IMPORTED_BODY")
CREATE_IMPORTED_DONE=$(wait_operation "$CREATE_IMPORTED_OP")
IMPORTED_BOX_ID=$(jq -er '.targetId' <<< "$CREATE_IMPORTED_DONE")
api GET "/v1/boxes/$IMPORTED_BOX_ID" | jq .
```

Profile 继续决定资源、节点、workspace 和 agent 身份，镜像与默认业务启动命令由导入记录提供。Profile 显式配置的环境变量优先于导入环境。用户镜像创建的 Box 不继承 Profile 的受控 debug tools 或 debug 宿主机目录挂载，`protectedTools` 为 false。导入记录在 API 重启后保留，只能由所属 Client 查询和引用。归档恢复不传 `importedImageId` 时，自动沿用源 Box 的导入镜像。构建可以复用 BuildKit 缓存；再次创建同一导入镜像的 Box 不需要再次构建。

## 5. 创建工作空间

```bash
CREATE_KEY=$(new_key)
CREATE_BODY=$(jq -nc --arg profile "$PROFILE_ID" \
  '{profileId:$profile, ownerKey:"usage-demo"}')
CREATE_OP=$(api POST /v1/boxes \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $CREATE_KEY" \
  --data "$CREATE_BODY")
CREATE_DONE=$(wait_operation "$CREATE_OP")
BOX_ID=$(jq -er '.targetId' <<< "$CREATE_DONE")
api GET "/v1/boxes/$BOX_ID" | jq .
```

保存 `BOX_ID`。`ownerKey` 是调用方的业务关联字段，例如用户 ID 或会话 ID，不是认证凭证，也不会限制同一 ownerKey 只能创建一个 Box。避免重复创建依靠 Idempotency-Key。

创建成功会等待 guest 可连接。查询返回的 `phase=running` 只代表运行时阶段，不能单独证明 guest 或业务应用健康；应用就绪需要执行自己的检查。

查看当前 Client 的工作空间，或只查询指定 ID：

```bash
api GET /v1/boxes | jq .
api GET /v1/boxes --get --data-urlencode "id=$BOX_ID" | jq .
```

查询不会隐式恢复暂停的 Box。请通过 API 管理生命周期，以保持平台记录与运行时状态一致。

## 6. 执行命令并读取结果

每次执行前读取当前 generation，作为请求的 `expectedGeneration`：

```bash
GENERATION=$(api GET "/v1/boxes/$BOX_ID" | jq -er '.generation')
EXEC_KEY=$(new_key)
EXEC_BODY=$(jq -nc --argjson generation "$GENERATION" '{
  argv:["/bin/sh","-c","printf \"hello from cellbox\\n\"; pwd; id"],
  cwd:".",
  env:{DEMO:"cellbox"},
  timeoutMs:30000,
  expectedGeneration:$generation
}')
EXEC_OP=$(api POST "/v1/boxes/$BOX_ID/execs" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $EXEC_KEY" --data "$EXEC_BODY")
EXEC_DONE=$(wait_operation "$EXEC_OP")
EXEC_ID=$(jq -er '.result.execId' <<< "$EXEC_DONE")
api GET "/v1/execs/$EXEC_ID" | jq '{state, result}'
```

返回结果包含 `stdout`、`stderr`、`exitCode`，输出被截断时还有 `truncated=true`。Operation 成功表示取得执行结果，业务命令是否成功还需检查 `result.exitCode == 0`。

使用规则：

- `argv[0]` 必须是镜像中存在的可执行文件的绝对路径。管道、重定向和 shell 变量使用显式的 `/bin/sh -c`。
- `cwd` 相对 Profile 的 workspace；默认在工作目录根部。执行使用 agent 非 root 身份，默认 UID/GID 为 `11000/11000`。
- `timeoutMs` 默认 30 秒，上限 300 秒；stdout、stderr 各最多保存 1 MiB。当前不提供 PTY 或可重连的交互终端。
- 恢复或运行时实例变化后重新读取 generation。遇到 `STALE_GENERATION` 时先确认旧操作结果，再决定是否提交新操作。
- 单次 exec 用于有界任务。持续运行的业务服务通过 Profile 的 `guest.command` 配置，或由镜像中的应用负责启动。

## 7. 上传、下载和列出工作目录文件

```bash
mkdir -p tmp
printf 'hello file\n' > tmp/cellbox-hello.txt

# 上传原始文件字节；成功返回 204。
api PUT "/v1/boxes/$BOX_ID/files?path=hello.txt" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @tmp/cellbox-hello.txt

# 下载；path 使用 URL 编码。
api GET "/v1/boxes/$BOX_ID/files" --get \
  --data-urlencode 'path=hello.txt' -o tmp/cellbox-downloaded.txt
cat tmp/cellbox-downloaded.txt

# 列出工作目录根部。
api GET "/v1/boxes/$BOX_ID/files" --get \
  --data-urlencode 'path=.' --data-urlencode 'list=1' | jq .
```

路径相对 workspace，例如 `hello.txt` 对应 `/workspace/hello.txt`。禁止绝对路径、`..` 和跟随符号链接。API 单次文件传输上限为 **16 MiB**。上传子目录文件前，先通过 exec 创建父目录。写文件会覆盖原内容，权限归属为 agent。

文件接口是同步调用，不返回 Operation。`suspended` 或 `frozen` 状态不能直接访问文件，先完成恢复。

## 8. 暂停与恢复 Kubernetes 工作空间

```bash
SUSPEND_KEY=$(new_key)
SUSPEND_OP=$(api POST "/v1/boxes/$BOX_ID:suspend" \
  -H "Idempotency-Key: $SUSPEND_KEY")
wait_operation "$SUSPEND_OP" | jq .
api GET "/v1/boxes/$BOX_ID" | jq '{id, phase, generation}'

RESUME_KEY=$(new_key)
RESUME_OP=$(api POST "/v1/boxes/$BOX_ID:resume" \
  -H "Idempotency-Key: $RESUME_KEY")
wait_operation "$RESUME_OP" | jq .
api GET "/v1/boxes/$BOX_ID" | jq '{id, phase, generation}'
```

暂停后阶段为 `suspended`，恢复后为 `running`。Kubernetes 模式通过 gVisor checkpoint 保存进程内存和可写根文件系统，在**同一节点**恢复；当前不支持跨节点恢复。checkpoint 占用节点磁盘，不能代替异地备份。

活动 exec、平台正在处理的服务请求和未过期的 usage lease 会阻止生命周期变更，返回 `BUSY`。先等待活动任务完成、结束访问或释放自己的 lease，再暂停。暂停只能从 `running` 发起，恢复只能从 `suspended` 发起。

Docker 模式使用 `:freeze` / `:unfreeze`，对应 `frozen` / `running`；Kubernetes 模式不支持这两个动作，Docker 不支持 `:suspend` / `:resume`。不要混用。

## 9. 归档与恢复到新工作空间

归档保存 **workspace 文件**，不保存进程内存、整个根文件系统或凭证槽。它是 `workspace-best-effort` 文件复制；应用自行写入时不保证一致性，重要数据应先由应用完成刷盘和静止写入。

先在可访问的 Box 上创建归档并下载：

```bash
ARCHIVE_KEY=$(new_key)
ARCHIVE_OP=$(api POST "/v1/boxes/$BOX_ID/archives" \
  -H "Idempotency-Key: $ARCHIVE_KEY")
ARCHIVE_DONE=$(wait_operation "$ARCHIVE_OP")
ARCHIVE_ID=$(jq -er '.result.archiveId' <<< "$ARCHIVE_DONE")
api GET "/v1/archives/$ARCHIVE_ID" | jq .
api GET "/v1/archives/$ARCHIVE_ID/content" --max-time 300 \
  -o "tmp/$ARCHIVE_ID.tar.gz"
```

从归档创建候选工作空间：

```bash
RESTORE_KEY=$(new_key)
RESTORE_BODY=$(jq -nc --arg profile "$PROFILE_ID" --arg archive "$ARCHIVE_ID" \
  '{profileId:$profile, ownerKey:"usage-demo-restored", archiveId:$archive}')
RESTORE_OP=$(api POST /v1/boxes:restore \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $RESTORE_KEY" --data "$RESTORE_BODY")
RESTORE_DONE=$(wait_operation "$RESTORE_OP")
RESTORED_BOX_ID=$(jq -er '.targetId' <<< "$RESTORE_DONE")
api GET "/v1/boxes/$RESTORED_BOX_ID" | jq '{id, phase}'
api GET "/v1/boxes/$RESTORED_BOX_ID/files" --get \
  --data-urlencode 'path=hello.txt'

# 验证文件后激活，启动配置的业务 command。
ACTIVATE_KEY=$(new_key)
ACTIVATE_OP=$(api POST "/v1/boxes/$RESTORED_BOX_ID:activate" \
  -H "Idempotency-Key: $ACTIVATE_KEY")
wait_operation "$ACTIVATE_OP" | jq .
```

恢复成功先进入 `staged`，可以检查文件，也可以执行验证命令；显式 `:activate` 后才启动 Profile 中配置的业务 command。新 Box 不替换原 Box。目标 Profile 的 agent UID/GID 必须与归档一致；使用与原来相同的 Profile 最方便。

失败或中断的恢复候选应销毁后重新创建，`:reconcile` 不会续传恢复过程。归档保存在 API 数据 PVC，销毁源 Box 后归档仍保留。

## 10. 访问沙箱中的 HTTP 服务

先确保应用已经在沙箱内启动并监听目标端口。平台可代理沙箱内 `127.0.0.1` 上的服务，下面假设应用端口是 `8080`：

```bash
ROUTE_BODY=$(jq -nc --arg box "$BOX_ID" '{boxId:$box, port:8080}')
ROUTE=$(api POST /v1/routes -H "Content-Type: application/json" \
  --data "$ROUTE_BODY")
ROUTE_ID=$(jq -er '.id' <<< "$ROUTE")

GRANT=$(api POST "/v1/routes/$ROUTE_ID/grants" \
  -H "Content-Type: application/json" \
  --data '{"subject":"usage-demo","ttlSeconds":900}')
GRANT_ID=$(jq -er '.grant.id' <<< "$GRANT")
GRANT_TOKEN=$(jq -er '.token' <<< "$GRANT")

# 本地转发使用路径形式；不依赖配置中的 publicUrl 能从本机访问。
curl --noproxy '*' --fail-with-body \
  -H "Authorization: Bearer $GRANT_TOKEN" \
  "$CELLBOX_URL/s/$ROUTE_ID/"

# 停止授权，成功返回 204。
api DELETE "/v1/grants/$GRANT_ID"
unset GRANT_TOKEN GRANT
```

访问 `/s/...` 使用 **Grant Token**，管理 `/v1/...` 使用 **Client Token**。Grant 绑定一个 route，有效期最多 3600 秒；创建响应中的明文 Token 只返回一次。给其他人访问时发放短期 Grant，不共享 Client Token。

相同 Box 和端口重复注册返回原 Route。端口 `40000` 为 guest 控制端口，不可公开。`route.url` 来自 `api.config.publicUrl` 或 `serviceDomain`；部署默认地址是集群内部 Service 地址。

浏览器登录与完整的 Web 应用预览需要单独配置 `serviceDomain`、每个 route 的 HTTPS 子域名入口，以及对应 Client 的 `authorizeUrl` 授权流程。上面的 curl 验证不要求配置浏览器登录。暂停中的 Box 不会因访问而自动恢复。

## 11. 销毁工作空间

确认不再需要 Box 中的数据后执行；需要保存 workspace 时先完成归档：

```bash
DESTROY_KEY=$(new_key)
DESTROY_OP=$(api POST "/v1/boxes/$BOX_ID:destroy" \
  -H "Idempotency-Key: $DESTROY_KEY")
wait_operation "$DESTROY_OP" | jq .
api GET "/v1/boxes/$BOX_ID" | jq '{id, phase}'
```

销毁后阶段为 `deleted`，元数据仍可查询。沙箱运行资源和它的 checkpoint 由平台处理，归档不会随源 Box 自动删除。

若还创建了恢复 Box，需要对 `RESTORED_BOX_ID` 单独调用 `:destroy`。归档不再需要时，显式删除：

```bash
api GET /v1/archives | jq .
# 确认 ARCHIVE_ID 是要删除的归档，再执行；成功返回 204。
api DELETE "/v1/archives/$ARCHIVE_ID"
```

## 12. 接入业务应用时的约定

- 由业务后端持有 Client Token，浏览器或最终用户使用业务授权流程和短期 Grant。
- 保存业务对象与 `boxId` 的关联，以及每次写操作的 Idempotency-Key 和 Operation ID。进程重启或网络中断后先查询原 Operation。
- 租约适合保护一段业务使用期：`POST /v1/boxes/{id}/leases`，请求如 `{"purpose":"editing","ttlSeconds":60}`，返回 `lease.id`。使用 `PATCH /v1/leases/{id}` 续期、`DELETE /v1/leases/{id}` 释放；单次 TTL 最多 3600 秒。lease 不会自动创建、恢复或销毁 Box。
- 可用 `GET /v1/operations/{id}/events` 订阅 SSE operation 状态快照；它不是命令 stdout 的流式接口。
- 需要外部凭证的受控操作，可由管理员在 Profile 配置 tools，使用 `PUT /v1/boxes/{id}/credentials/{slot}` 写入凭证槽，再调用 `POST /v1/boxes/{id}/tools/{tool}`。该工具接口同步返回执行结果，不支持幂等重放；普通 exec 使用 agent 身份，受控工具使用单独的 debug 身份。

## 13. 常见问题与检查命令

| 现象 | 检查与处理 |
| --- | --- |
| `401 UNAUTHENTICATED` | `/v1/...` 检查 Client Token；`/s/...` 检查 Grant Token、有效期和是否已撤销 |
| Profiles 为空或 `403 FORBIDDEN` | 检查 `api.config.profiles`，以及 Profile 的 `clients` 是否包含当前 Client ID |
| `409 BUSY` | 等待活动操作、exec、请求完成；释放自己的 lease 或等待其过期 |
| `409 STALE_GENERATION` | 重新查询 Box；确认旧命令是否完成，再使用当前 generation 提交必要的新请求 |
| `409 CONFLICT` | 检查生命周期前置状态，以及是否用同一个幂等 Key 提交了不同参数 |
| `422 UNSUPPORTED_CAPABILITY` | 检查 Profile capabilities；镜像构建还需启用 BuildKit |
| HTTP `202` 后 Operation 失败 | 查询 Operation 的 error；不能仅凭 HTTP 状态认定业务成功 |
| 创建超时、拉取失败 | 检查节点可访问镜像仓库、镜像包含 guest、节点安装完成、资源足够 |
| `RUNTIME_LOST` | 底层运行时丢失；平台不会隐式冷启动重建，检查节点和 CR，必要时从文件归档创建新 Box |
| 恢复失败 | 检查原节点、checkpoint 和运行时兼容性；当前没有跨节点恢复能力 |
| 修改配置后 API 无法连接 | 检查 API Pod 日志；Pod 更新后重新启动 port-forward |

排查使用明确的 Kubernetes context：

```bash
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" get pods -o wide
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs deploy/cellbox-api -c api --tail=100
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs ds/cellbox-controller -c controller --tail=100
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  logs ds/cellbox-controller -c install-runtime-adapter --tail=100
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  get cellboxes -l "cellbox.local/box-id=$BOX_ID" -o yaml
kubectl --context "$CELLBOX_CONTEXT" -n "$CELLBOX_NAMESPACE" \
  get events --sort-by=.metadata.creationTimestamp
```

使用结束后，在终端 A 按 Ctrl-C 结束本地转发；终端 B 可执行 `unset CELLBOX_CLIENT_TOKEN GRANT_TOKEN` 清除当前会话的凭证变量。这不会停止 Cellbox 或删除工作空间。
