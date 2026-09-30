# Cellbox 节点手工安装

在每个承载 `k8s-resumable` 工作空间的节点上完成本文步骤，再部署 Helm Chart。Chart 只创建 Kubernetes 资源，不安装 gVisor、wrapper，不修改宿主机运行时配置，也不重启节点服务。Controller 仍需特权和 hostPID 来执行 checkpoint / restore。

以下节点命令使用 Bash 和 sudo。普通 containerd 与 K3s 的配置步骤二选一；路径、节点名、服务名及版本需按实际环境确认。不要在已有工作空间运行或暂停时直接替换 gVisor；已有 checkpoint 的版本兼容性需要先验证。

## 1. 确认环境并备份

Chart 默认选择 Linux amd64。确认节点架构、运行时实际配置路径和 systemd 服务：

```bash
uname -m
sudo systemctl cat containerd.service   # 普通 containerd
# K3s server 使用 k3s.service，agent 使用 k3s-agent.service
sudo systemctl cat k3s.service
```

普通 containerd 通常使用 `/etc/containerd/config.toml`，但应以服务启动参数为准。K3s 的默认生成配置是 `/var/lib/rancher/k3s/agent/etc/containerd/config.toml`。阅读配置中的 `version`、`imports` 及导入文件，确认 CRI 未禁用，并检查是否已有 `runsc-recoverable` handler。已有配置应人工合并，避免重复定义。

在修改前将实际配置文件、K3s 自定义模板或 drop-in、已有 wrapper 和 `/var/lib/cellbox/runsc.toml` 备份到运维指定目录，记录原权限。记录哪些文件原本不存在，方便回滚时移除新文件。保留原 gVisor 的完整安装目录；不要仅备份 runsc。

## 2. 安装完整 gVisor

已有完整且兼容的安装可跳过。本例沿用项目此前固定的 `20260914.0`，应先在测试节点确认 checkpoint / restore 功能。不要使用浮动的 `latest`。

完整发行包包含 runsc、shim 和 `gvisor-bin/`；后者必须与 runsc 相邻。不要仅下载两个可执行文件。发行包布局及校验方式参考 [gVisor 官方安装文档](https://gvisor.dev/docs/user_guide/install/)。

在待安装节点执行下载和检查：

```bash
(
  set -euo pipefail
  gvisor_version=20260914.0
  gvisor_arch=$(uname -m)
  case "$gvisor_arch" in x86_64|aarch64) ;; *) exit 1 ;; esac
  gvisor_url="https://storage.googleapis.com/gvisor/releases/release/$gvisor_version/$gvisor_arch"
  gvisor_work=$(mktemp -d)
  trap 'rm -rf -- "$gvisor_work"' EXIT
  cd "$gvisor_work"
  curl --fail --location "$gvisor_url/gvisor.tar.bz2" -o gvisor.tar.bz2
  curl --fail --location "$gvisor_url/gvisor.tar.bz2.sha512" -o gvisor.tar.bz2.sha512
  sha512sum -c gvisor.tar.bz2.sha512
  mkdir unpacked
  tar -xjf gvisor.tar.bz2 -C unpacked
  test -x unpacked/runsc
  test -x unpacked/containerd-shim-runsc-v1
  test -x unpacked/gvisor-bin/gvisor_sentry
  # 仅在确认备份且允许替换节点组件后执行下面的安装命令。
  sudo install -d -m 755 /usr/local/bin /usr/local/bin/gvisor-bin
  sudo cp -a unpacked/gvisor-bin/. /usr/local/bin/gvisor-bin/
  sudo install -m 755 unpacked/containerd-shim-runsc-v1 /usr/local/bin/containerd-shim-runsc-v1
  sudo install -m 755 unpacked/runsc /usr/local/bin/runsc
)
/usr/local/bin/runsc --version
```

需要 curl、CA 证书、tar、bzip2 和 sha512sum。确认运行时服务的 PATH 包含 `/usr/local/bin`，能找到 `containerd-shim-runsc-v1`；仅登录 Shell 的 PATH 正确还不够。

## 3. 安装 Cellbox wrapper 和 shim 配置

在构建机执行 `make build`，将对应节点架构的 `dist/release/cellbox-runsc-wrapper` 复制到节点。交叉构建可用 `GOARCH=arm64 make build`；默认 Chart 选择 amd64，其他架构还需配套的平台及沙箱镜像。

在节点上，从包含已复制 wrapper 的目录执行：

```bash
sudo install -d -m 755 /usr/local/libexec
sudo install -m 755 cellbox-runsc-wrapper /usr/local/libexec/cellbox-runsc-wrapper
sudo install -d -m 700 /var/lib/cellbox \
  /var/lib/cellbox/tickets /var/lib/cellbox/requests /var/lib/cellbox/claims \
  /var/lib/cellbox/workloads /var/lib/cellbox/logs
sudo /usr/local/libexec/cellbox-runsc-wrapper --adapter-version
```

将下面完整内容保存为节点的 `/var/lib/cellbox/runsc.toml`，文件属主 root、权限 `0600`。已有文件先备份并检查差异。

```toml
binary_name = "/usr/local/libexec/cellbox-runsc-wrapper"
log_path = "/var/lib/cellbox/logs/%ID%/shim.log"
log_level = "info"
[runsc_config]
  restore-spec-validation = "enforce"
  platform = "systrap"
```

```bash
sudo chown root:root /var/lib/cellbox/runsc.toml
sudo chmod 600 /var/lib/cellbox/runsc.toml
```

默认使用 systrap。如明确选择 KVM，将 `platform` 改为 `kvm`，并先确认节点 `/dev/kvm` 可用。shim 配置项参考 [gVisor containerd 配置文档](https://gvisor.dev/docs/user_guide/containerd/configuration/)；wrapper 和 recovery annotation 是 Cellbox 的适配要求。

## 4A. 普通 containerd

人工编辑运行时实际使用的配置文件，保留所有原有设置。在 `version = 2` 的配置中增加：

```toml
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
```

`version = 3` 使用下面的插件路径，其他内容相同：

```toml
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
```

如使用已有的 imports/drop-in 体系，将片段放入实际被导入的文件并遵循该集群的配置规范。不要直接覆盖整个配置，也不要修改默认 runtime。合并完成后，使用节点安装的 containerd 检查配置解析和合并结果：

```bash
sudo containerd --config /etc/containerd/config.toml config dump
```

解析通过后，再按第 5 节安排人工重启。

## 4B. K3s

不要直接编辑 K3s 自动生成的 `config.toml`，重启会重新生成。使用对应版本的模板，参考 [K3s 官方 containerd 模板说明](https://docs.k3s.io/advanced#configuring-containerd)。

| 生成配置版本 | 默认模板路径 | runtime 插件路径 |
| --- | --- | --- |
| `version = 2` | `/var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl` | `io.containerd.grpc.v1.cri` |
| `version = 3` | `/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl` | `io.containerd.cri.v1.runtime` |

若节点尚无自定义模板，版本 3 模板的完整内容如下：

```gotemplate
{{ template "base" . }}

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
```

版本 2 模板保留首行，将两处 `io.containerd.cri.v1.runtime` 改为 `io.containerd.grpc.v1.cri`，保存到版本 2 对应路径。

已有模板时保留原内容，人工合并 handler；不要再次添加 base 调用。已有 `config-v3.toml.d/91-cellbox.toml` 等 drop-in 时，确认生成配置确实导入该文件；可保留并人工维护该布局，不要在模板中重复定义。不要把自动生成的 `config.toml` 整份复制为模板。

## 5. 人工重启和检查

选择维护窗口，先处理节点上的业务和已有 Cellbox 工作空间。下面以普通 worker 为例，命令在管理机执行；drain 应遵循集群维护规范，PDB 或本地数据阻止 drain 时先处理原因，不要直接强制删除。

```bash
kubectl --context <context> cordon <node>
kubectl --context <context> drain <node> --ignore-daemonsets
```

在节点上只执行实际服务对应的一组命令：

```bash
# 普通 containerd
sudo systemctl restart containerd.service
sudo systemctl is-active containerd.service
sudo journalctl -u containerd.service -n 100 --no-pager

# 或 K3s server（会影响本机控制面，需单独安排维护窗口）
sudo systemctl restart k3s.service
sudo systemctl is-active k3s.service
sudo journalctl -u k3s.service -n 100 --no-pager

# 或 K3s agent
sudo systemctl restart k3s-agent.service
sudo systemctl is-active k3s-agent.service
sudo journalctl -u k3s-agent.service -n 100 --no-pager
```

确认日志无配置解析或 CRI 加载错误。K3s 还需检查重新生成的 `config.toml` 中 handler 与 ConfigPath 正确。使用已安装的 crictl 检查 CRI（普通 containerd）：

```bash
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock info
```

K3s 可用 `sudo k3s crictl info`；实际 CRI socket 通常为 `/run/k3s/containerd/containerd.sock`。

在管理机确认节点 Ready 后解除调度限制：

```bash
kubectl --context <context> wait --for=condition=Ready node/<node> --timeout=180s
kubectl --context <context> uncordon <node>
```

按 [README](../README.md#kubernetes-安装与升级) 部署 Helm。K3s values 需设置 `controller.criDirectory: /run/k3s/containerd` 和 `controller.criSocket: /run/k3s/containerd/containerd.sock`。只让已完成节点准备的节点匹配 Controller 的 nodeSelector，并让 profile 的 nodeName 指向已准备节点。

```bash
kubectl --context <context> get runtimeclass runsc-recoverable -o yaml
kubectl --context <context> -n cell-box rollout status ds/cellbox-controller --timeout=180s
kubectl --context <context> -n cell-box logs ds/cellbox-controller -c controller
```

RuntimeClass 的 handler 应为 `runsc-recoverable`。服务 active、节点 Ready 和 Controller Running 只能证明基础状态；最后使用项目测试工作空间确认创建、执行及同节点 suspend / resume。已有 Running 的 counter 测试 CR 时，可使用 `test/scripts/e2e-k8s.sh <context>`。

## 升级、已有安装迁移和回滚

- 已由旧安装器配置好的节点无需重新安装。升级新 Chart 后将不再有安装 init container；旧 `runtimeInstaller` 和 `adapterImage` values 应从本地覆盖文件删除。已有 wrapper、配置和 checkpoint 保留。
- Controller 镜像升级不再同步升级 wrapper。需要更新适配器时，从同版本发布包取 wrapper，核对 `--adapter-version`，按第 3 节人工安装。单独更新 wrapper 或 shim TOML 不要求重启 containerd，但已有 shim 可能继续使用旧配置；需验证新建工作空间，避免假定存量工作空间已更新。
- 更换 gVisor 或 containerd handler 按节点维护流程处理，先验证已有 checkpoint 的兼容性。
- 配置重启失败时保持节点 cordon，恢复备份的实际配置、模板或 drop-in；移除此次新增且原本不存在的配置文件，恢复原 wrapper、shim TOML 及完整 gVisor 安装，再由运维重启对应服务并验证。K3s 应恢复输入模板/drop-in，不能仅恢复生成文件。
- 不要删除 `/var/lib/cellbox` 下的 tickets、requests、claims、workloads 或运行时状态来“重装”。`helm uninstall` 不移除节点组件；正式卸载前先处理已有工作空间，再人工撤销 handler 和组件。
