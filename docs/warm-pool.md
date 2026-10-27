# 预热池

预热池提前准备好 Pod 和 gVisor root sandbox，让 checkpoint 恢复（resume）不必在请求内创建 Pod。本文说明预热需求从哪里来、槽位怎么创建和回收、哪些请求会命中，以及怎么观察。接口字段见 [REST 快速开始](rest-quickstart.md)和 [Go SDK 指南](go-sdk.md)，调用链见[整体架构](architecture.md)。

## 1. 作用与边界

- 预热只服务 **resumable sandbox 的 checkpoint 恢复**。首次创建 sandbox 不领取预热槽，没有 checkpoint 的 Box 也用不上。
- 空闲槽位里 `runsc create` 已完成，wrapper 在 root sandbox 的 `start` 路径等待领用。此时没有业务容器或 guest 程序在运行，`readySlots` 只表示槽位进入了这种等待状态，不表示业务就绪。
- 因为 wrapper 一直阻塞在 `StartContainer` 里，Kubernetes 会把空闲槽位显示为 `ContainerCreating`，这是正常的。
- 没有可用或匹配的槽位时，走普通 checkpoint 恢复路径，进程内存同样保持，只是慢一些。

## 2. 预热需求从哪里来

需求由**本节点上现有的 checkpoint** 决定，不由 revision 上的 `warmSlots` 决定。预热池循环每 500 ms 列出本节点的 Cellbox 自定义资源：

| 计入条件 | 说明 |
| --- | --- |
| `spec.nodeName` 是本节点 | checkpoint 在节点本地，没有跨节点恢复 |
| `status.phase` 是 `Suspended` | 正在暂停、正在恢复的不计入 |
| `status.snapshot` 非空 | 恢复完成后旧 checkpoint 会被撤销，这样的 Box 不再计入 |
| 未在删除 | |

满足条件的 Box 按**兼容运行配置**分组。兼容指纹包含镜像、命令、环境变量、资源、Service 端口等，**忽略持久 HOME 和挂载**，所以挂载不同 HOME 的 Box 可以共用一个池。镜像相同本身不保证兼容。

每个分组的槽位数：

| 该分组的 checkpoint 数 | 槽位 |
| --- | --- |
| 1–2 | 1 |
| 3 个及以上 | 2（每组上限） |

槽位对应的是**并发恢复**能力，不是 checkpoint 库存：2 个槽只能让同时到达的 2 个恢复命中，第 3 个会走普通恢复。

池的模板直接取自这些 Box 自己的规格（清除 HOME 和挂载后），所以池与要恢复的 Box 的指纹天然一致。

## 3. 回收

- 项目归档、Box 被回收或恢复完成后，checkpoint 消失，需求随之下降，不需要手工改配置。
- 一个分组的最后一个 checkpoint 消失后，**保留 1 个槽 20 分钟**再释放。恢复后的 Box 空闲一段时间会再次暂停，保留期避免每次 resume/suspend 都重建 Pod。保留计时只在内存里，server 重启后按现有 checkpoint 重算。
- 已被 Box 领用的 Pod 不再属于池，回收预热需求不会删除它。

## 4. 总预算

| 项 | 值 |
| --- | --- |
| 参数 | `controller.warmPoolSize`（`--warm-pool-size`），0–32，默认 2，0 关闭预热 |
| 范围 | 每个节点一个预算，所有分组共享，按最大最小公平分配，每个分组不超过自己的需求 |
| 轮换 | 为先建后继再退休老槽，空闲 Pod 数最多到预算加 1 |
| 创建速度 | 每轮对账最多创建 1 个 Pod；刚有恢复请求（约 2 秒内）时暂缓创建，避免和恢复抢资源 |

预算只数个数，不看 CPU/内存。每个槽位是完整的 container 配置，Pod 的 requests/limits 与它服务的 revision 相同。**每个槽位大约每 45 秒整体轮换一次**，所以预算越大，持续的 `runsc create` 开销越大，预算应按"有 checkpoint 的镜像数 × 每组槽数"估计，不要无故调大。

## 5. 槽位生命周期

1. 控制器创建带调度门控的 Pod（标签 `cellbox.local/warm-pool=true`，注解里记录兼容指纹）。
2. 节点后端登记租约（`registered`），控制器去掉门控，Pod 被调度。
3. wrapper 在 root sandbox 的 `start` 里绑定租约（`created`），开始等待（`waiting`），**租约 60 秒**。
4. 剩余不足 15 秒时先建后继；不足 5 秒视为过期，整个 Pod 被删除并清理 runtime，Pod UID 不复用（`expired` / `retired`）。
5. 恢复请求领用槽位：准备 checkpoint 和 ticket，租约变为 `claimed`，再变为 `restoring`，wrapper 执行 `restore`。此后该 Pod 属于这个 Box。

领用条件：Box 在本节点、有 checkpoint、指纹与某个 `waiting` 槽位一致、该槽位剩余租期不少于 5 秒，并且领用时校验 owner、Pod UID 和 snapshot。竞争失败或身份变化时跳过该槽位。

回收空闲槽位时，容器可能仍处于"正在启动"状态（`StartContainer` 还没有从 wrapper 返回）。containerd 会拒绝此时的删除，错误里带 `is in starting state`，清理逻辑把它当作可重试的暂态，在删除窗口内继续等待。事件里出现 `OCI runtime start failed: warm slot retired` 是 wrapper 被叫醒后的正常退出。

## 6. 与 `warmSlots` 的关系

`defaultWarmSlots` 和 revision 的 `warmSlots` 字段仍可读写，但**不再产生预热需求**，把它改成 0 也不会关闭预热。现在它们的作用只是把某个 revision 关联到服务它的池：

- `GET /v1/template-revisions/{id}/warm-pool` 的 `desiredSlots` 仍是配置的 `warmSlots`，只用于回显。
- `pool.requestedSlots` 才是由 checkpoint 推导出的需求，`pool.allocatedSlots` 是受节点预算限制后的分配量。
- revision 的规格与某个现有池一致时，`pool` 才有值；没有 checkpoint 时 `pool` 为 `null`，这是正常的。
- 要停用预热，把 `controller.warmPoolSize` 设为 0。

## 7. 观察与排查

| 现象 | 含义 |
| --- | --- |
| 没有任何 checkpoint 时没有 `cb-warm-*` Pod | 正常，需求为 0 |
| `cb-warm-*` 显示 `ContainerCreating` | 正常，wrapper 在等待领用；以 `readySlots` 判断就绪 |
| `requestedSlots` 大于 `allocatedSlots` | 节点预算不够，调大 `warmPoolSize` 或确认是否有无用的分组 |
| 恢复比预期慢 | 先看是否命中：槽位数不足、租约不足 5 秒、指纹不一致或节点不同都会走普通恢复 |
| 日志里 `warm pool reconcile failed ... starting state` | 旧版本的回收竞争，当前版本会重试，不应再出现 |

`failedSlots` 统计当前失败态的预热 Pod，以及最近 15 秒内记录、但已清理的创建失败。`stale` 与 `observedAt` 用于判断 controller 观察是否新鲜，超过约 15 秒会标记为过期。

## 8. 性能参考

下面是 2026-10-27 在 perf 环境（k3s 单节点，节点磁盘在 NVMe）上的实测，每组样本很少，只用于说明量级，不是性能承诺：

| 场景 | resume 延迟 |
| --- | --- |
| 单个 Box 串行 resume，命中预热（8 次） | 176–224 ms |
| 4 个 Box 并发 resume，2 个槽：命中的 2 个（4 轮） | 263–341 ms |
| 同一轮里未命中的另外 2 个（4 轮） | 876–967 ms |

节点磁盘对结果影响很大：同一环境里，虚拟磁盘放在机械盘上时命中约 2.7 s、未命中约 7.1 s，主要被 fsync 拖慢。生产云盘的写延迟在毫秒级，建议在目标环境里自行测量。

## 9. 已知限制

- 只对 `Suspended` 且有 checkpoint 的 Box 预热，首次创建不加速。
- 每个分组最多 2 个槽，同一镜像的并发恢复超过槽数时，多出的请求走普通恢复。
- 预算是节点级的，所有分组共享；预热不区分业务重要性。
- 升级到这套行为时，已有空闲槽的指纹会变化，会被回收后重建一次。
- 槽位大约每 45 秒轮换一次，持续消耗 `runsc create` 的 CPU 和 containerd 的 RPC。
