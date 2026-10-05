# Cellbox 预热池优化报告

报告日期：2026-10-05。范围：节点预热池及其 checkpoint 恢复路径。实现来自 [Cellbox #21](https://github.com/jingyugao/cell-box/pull/21)，合并提交 `daf8ccd`；代码核对基准为 `cf8a740`。性能数据来自 2026-10-04 的已有实测，本次文档修改没有重新压测。

## 1. 优化目标与结果

预热池把恢复请求中的 Pod 调度、网络和部分 runtime 准备提前执行。请求到达后，兼容模板的 Box 领取已准备的 Pod，再恢复原 checkpoint 中的进程内存。默认每节点保留 **2 个未领取槽位**，轮换时最多额外准备 **1 个槽位**。

| 测试组 | 样本 | 客户端端到端结果 | 说明 |
| --- | --- | --- | --- |
| 初版预热恢复 | 3 组双并发 / 6 次 | 中位数 **1,761.823 ms**，范围 1,684.805–1,856.466 ms | 已使用预热池的开发中间版本 |
| 最终普通预热恢复 | 5 组双并发 / 10 次 | **P50 936.130 ms**，范围 824.292–1,036.835 ms | 预热 Pod 和节点 checkpoint 缓存均命中；8/10 请求低于 1 秒 |
| 最终长历史、运行时压力 | 20 组双并发 / 40 次 | **P50 1,123.023 ms、P95 1,233.903 ms**，最大 1,398.551 ms | 每个 Box 先执行 200 次命令；全部请求超过 1 秒 |
| 节点 checkpoint 缓存丢失 | 1 组双并发 / 2 次 | **1,987.715 / 1,975.036 ms** | 从 OSS 回源，原进程内存保持 |

普通热恢复已接近秒内，尚不能保证双并发全部请求低于 1 秒。初版与最终版之间还包含同轮的 API 提交和通知优化，因此这张表反映预热恢复路径的整体结果，不能把全部降幅归因于池本身。现有记录也没有同节点、同镜像、同负载下“关闭预热池与开启预热池”的端到端对照。[E1]、[E2]、[E3]

## 2. 优化前的路径与耗时

引入预热池前，checkpoint 恢复要在请求到达后创建新 Pod，推进调度、网络与 runtime 准备，再执行恢复。旧确认路径还需要等待 kubelet 异步上报 Pod 状态和就绪信息。这些环节串在用户请求内，且双并发会竞争节点资源。

| 请求中的开销 | 我们实施的处理 | 收益边界 |
| --- | --- | --- |
| 新 Pod 调度、网络、sandbox 准备 | 提前准备待领取的 runtime sandbox | 把准备成本移到请求之前，仍消耗节点资源 |
| 候选发现和并发领取 | 候选 LIST 使用 RV=0/NotOlderThan，resourceVersion CAS 与节点租约认领 | 竞争、删除、租约过期或模板不匹配时跳过候选 |
| checkpoint 下载和重复内容校验 | 使用已验证的节点缓存，文件指纹变化时完整校验 | 缓存丢失仍需 OSS 回源 |
| 已恢复执行等待异步状态传播 | 通过节点本地 CRI 确认执行身份及运行状态 | Pod Ready 继续由真实探测决定 |
| 旧 checkpoint 的物理清理 | 同步撤销重放资格，精确文件删除放到后台 | 必要的持久化失效仍在完成边界内 |

初版已经使用预热池，但 6 次恢复仍耗时约 1.7–1.9 秒，说明提前准备 Pod 后仍需优化恢复和确认路径。该批只有总耗时留档，没有足够阶段日志，无法给每项改动分配独立的加速数字。底层小工作负载的独立缓存实验中，校验阶段中位数由 73.092 ms 降至 0.110 ms；它从 claim 校验前计时，排除了池准备，不是完整 API 恢复耗时。[E1]、[E4]

## 3. 已实现的预热池机制

### 3.1 槽位按工作负载模板准备

池从当前节点已有的有效 Box 中取模板，候选生命周期包括 Creating、Running、Checkpointing、Suspending、Suspended、Restoring。模板按最近的状态转换时间优先分配节点容量；多个 Box 模板相同时共享对应槽位。

槽位 Pod 直接复制候选 Box 的完整 container 配置，镜像来自这个 Box 的模板。兼容性指纹包含节点、完整 container 配置（镜像、命令、环境变量、资源等）、Service ports 和相关挂载配置。池没有“只接受系统镜像”的过滤：系统镜像和导入镜像都可随有效 Box 成为候选，但仅导入镜像、尚无对应 Box 时不会自动建立预热槽。

因此池按**完整模板**匹配，容量在节点上共享。5 个镜像不会自动得到 5 个槽；即使把容量设为 5，也不能保证每个镜像各分到一个。相同镜像的命令或配置不同，也可能无法共享槽位。[模板与候选分配](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/controller/warm_pool.go#L55)、[模板指纹](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/controller/reconciler.go#L83)

### 3.2 有界准备、领取和轮换

Controller 默认参数 `--warm-pool-size=2`，Helm 配置为 `warmPoolSize`；设为 0 可关闭，参数允许 0–32。容量约束针对未领取的池槽位，已领取并承载业务的 Pod 另计资源。

预热时先创建 sandbox，adapter 在 sandbox start 阶段等待领取；此时业务 Guest 尚未启动，也没有预先运行一份新业务进程。槽位租期为 60 秒，按整 Pod 轮换，Pod UID 不复用。领取只接受处于 waiting、剩余租期至少 5 秒且模板匹配的槽位，通过 resourceVersion CAS 设置归属，并绑定节点租约；竞争失败或身份变化时拒绝继续使用。

候选 LIST 通过直接客户端设置 `ResourceVersion=0`、`ResourceVersionMatch=NotOlderThan`，不要求 quorum read；最终领取仍由本地租约校验和带资源版本的 PATCH 仲裁。维护、清理使用新状态，避免只凭候选列表回收槽位。

轮换允许最多额外 1 个未领取槽位，先准备后继再退休老槽。近期存在活跃恢复时，暂缓推测性的补池，减少小节点上与当前请求的 CPU/runtime 竞争。池耗尽、模板不匹配或关闭时走普通 checkpoint restore，继续恢复原内存。首次创建 Box 不领取预热槽。[容量配置](https://github.com/jingyugao/cell-box/blob/cf8a740/cmd/cellbox-node-controller/main.go#L35)、[轮换与领取](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/controller/warm_pool.go#L195)、[节点租约](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/node/warm.go#L94)、[adapter 等待点](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/adapter/adapter.go#L125)

### 3.3 节点缓存减少恢复前的重复工作

checkpoint 保留 OSS 持久副本和节点本地副本。领取后仍核对 owner UID、Pod UID、snapshot 和模板身份；节点缓存的验证记录保存 manifest 及文件 device/inode、size、mtime、ctime 等指纹，复用此前的完整内容校验。

指纹变化或验证记录缺失时重新完整校验；本地副本丢失时从 OSS 下载、解包、验证后再恢复。缓存优化只减少重复下载和重哈希，不省略恢复授权及快照完整性边界。[缓存校验](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/node/prepared.go#L61)、[领取后准备 checkpoint](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/node/warm.go#L137)

### 3.4 用实际执行证明缩短恢复确认

节点直接查询本地 CRI，核对业务 container Running、Pod UID、container ID、attempt、runtime handler 和有效 Pod IP，并向 CR 发布执行证明。仅活跃恢复进行 20 ms 间隔的有界查询，单次最多等待 1 秒；慢恢复由后续 reconcile 继续推进。

恢复完成仍遵循以下顺序：

```mermaid
flowchart LR
    A[领取兼容槽位] --> B[校验 checkpoint 并恢复]
    B --> C[CRI 确认实际执行]
    C --> D[持久化撤销旧快照重放资格]
    D --> E[开放 serving 并发布 Running]
    E --> F[Guest unquiesce]
    F --> G[持久化操作完成]
```

直接执行证明减少对 kubelet 异步状态上报的等待，连接使用核验后的 Pod IP；Pod Ready 保留真实探测语义。旧 checkpoint 的重放资格先同步失效，物理文件随后按精确 owner/snapshot 身份后台删除，避免延迟清理误删后续快照。[CRI 确认](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/node/execution.go#L14)、[完成顺序](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/controller/reconciler.go#L332)、[快照失效与清理](https://github.com/jingyugao/cell-box/blob/cf8a740/internal/node/garbage.go#L31)

## 4. 当前性能分析

### 4.1 测量口径

最终运行测试在 **2 vCPU、约 8 GiB 内存的 k3s 节点**上执行，两个 resume HTTP 请求通过 barrier 同时提交。端到端计时止于恢复后的首次业务 HTTP 响应；普通热样本要求预热 Pod 和本地 checkpoint 缓存双命中。此前 checkpoint、缓存和预热池准备不在请求计时内，仍有真实资源成本。

P50/P95 使用 nearest-rank，取排序后第 `ceil(p × N)` 个值；“中位数”取标准中位数。普通 10 次的标准中位数为 936.300 ms，长历史 40 次为 1,123.949 ms。长历史组还包含运行时压力，两组不能作为只改变历史量的严格 A/B。上述数据对应 #21 最终测试版本，后续修复后的代码基准未单独重测。[#21 验证记录](https://github.com/jingyugao/cell-box/pull/21)

### 4.2 当前主要耗时仍在运行时与状态观察

最终长历史 40 次的阶段数据如下。占比按阶段平均值除以服务端总耗时平均值计算，阶段中位数不能相加。

| 阶段 | 标准中位数 | 平均值 | 平均耗时占比 |
| --- | ---: | ---: | ---: |
| 接受 resume 并提交元数据 | 84.5 ms | 87.0 ms | 8.1% |
| 运行时推进与状态观察 | **843.9 ms** | **859.4 ms** | **80.0%** |
| Guest unquiesce | 28.0 ms | 26.9 ms | 2.5% |
| 完成持久化 | 92.5 ms | 100.0 ms | 9.3% |
| 服务端总耗时 | 1,058.7 ms | 1,074.8 ms | 100% |

运行时阶段包含 Controller 推进、槽位领取、恢复和 API 观察，不能全部归为 gVisor。更细的节点时间线中，sandbox 返回至业务 StartContainer 返回的中位数为 **543.8 ms**，StartContainer 返回至 API 观察 Running 为 **138.4 ms**。两者与上表边界不同，不能叠加。客户端平均端到端耗时为 1,142.6 ms，比服务端平均值多约 67.9 ms。[E3]

下一轮分析应优先拆分实际容器恢复及恢复后的发布路径，并补充模板不匹配、池耗尽和混合模板下的命中率。现在的数据证明命中后的恢复性能，尚未量化真实流量中池命中率对整体延迟的影响。

### 4.3 预热的资源成本

独立容量 PoC 在同类 2 vCPU、约 7.75 GiB 节点上，以相同镜像和模板的小型 Node HTTP 工作负载测量 2/3 个空闲槽，各采样约 10 秒、11 次。以下是实验进程的聚合 PSS（按比例分摊共享页），包含实验 runner；不是生产预热池的通用成本，也不是节点总内存增量。[E5]

| 空闲槽数 | 聚合进程 PSS 范围 | 平均空闲 CPU | Pod cgroup 内存合计 |
| --- | ---: | ---: | ---: |
| 2 | 112.76–118.23 MiB | 0.0319 核 | 约 26.22 MiB |
| 3 | 138.03–142.39 MiB | 0.0408 核 | 约 39.12 MiB |

进程采样可见 runtime shim、runsc/gVisor 相关进程和实验 runner，其内存包括堆栈与共享映射。代码中 adapter 确实在 sandbox start 等待领取，但采样未保存完整命令行，无法给各运行角色或生产 adapter 单独分账。业务 Guest 尚未运行；已有业务 checkpoint 的磁盘缓存另计。只看 Pod cgroup 会遗漏其外部或共享计费的 runtime 进程成本。

测量排除了 checkpoint 缓存、Pod cgroup 外的内核内存和轮换 CPU；缺少同条件的零槽对照，也没有 5 个异构镜像的容量实测。因此不能把 2/3 槽差值当通用单槽成本，再乘以 5 作为资源承诺。当前容量策略控制节点总槽位数，不随镜像数无限增长；代价是低频模板可能无法命中。

## 5. 验证与证据

最终普通和长历史恢复均验证了内存中的启动 UUID、递增计数器保持，且恢复后立即 exec 成功；API/Controller 重启后能够重建状态；删除本地 checkpoint 缓存后从 OSS 回源仍保持内存。#21 的验证覆盖竞争领取、失效租约、执行身份变化、损坏缓存及精确快照清理。本文引用已有结果，本次只修改文档。[#21 验证记录](https://github.com/jingyugao/cell-box/pull/21)

### 5.1 原始测量来源

临时路径是已有测试输出的位置，不是重跑命令。核心请求样本在下节摘录，避免临时目录清理后只剩汇总数字。

| 编号 | 本机记录 | 版本 / 用途 |
| --- | --- | --- |
| E1 | `/tmp/cellbox-timed-20261004/result.json` | #21 开发中间版本，commit 未留档；6 次初版预热恢复 |
| E2 | `/tmp/cellbox-restart-20261004/{result,summary}.json` | #21 最终版本；10 次普通预热、重启验证及 2 次缓存回源 |
| E3 | `/tmp/cellbox-compressed-20261004/{result,summary,timings,cri-analysis}.json` | #21 最终压缩版本；40 次长历史压力及阶段数据 |
| E4 | `/tmp/cellbox-restore-opt-20261004/summary.json` | 底层缓存校验 A/B，各 6 次；不含完整 API 路径 |
| E5 | `/tmp/cellbox-pool-capacity-20261004/{summary.json,idle/idle-2-resources.json,idle/idle-3-resources.json}` | 独立同模板容量 PoC；实验进程内存和 CPU |

### 5.2 请求样本摘录

以下均为毫秒，每行是同组两个并发请求；不同场景分别统计。

| 初版预热组 | 请求 A | 请求 B |
| --- | ---: | ---: |
| 1 | 1856.466 | 1829.943 |
| 2 | 1762.241 | 1761.405 |
| 3 | 1684.805 | 1686.305 |

| 最终普通预热组 | 请求 A | 请求 B |
| --- | ---: | ---: |
| 1 | 1024.463 | 1036.835 |
| 2 | 974.626 | 936.130 |
| 3 | 838.396 | 844.891 |
| 4 | 829.740 | 824.292 |
| 5 | 942.145 | 936.469 |

| 最终长历史组 | 请求 A | 请求 B |
| --- | ---: | ---: |
| 1 | 1383.031 | 1398.551 |
| 2 | 1053.328 | 1038.361 |
| 3 | 1211.888 | 1139.880 |
| 4 | 1187.394 | 1173.981 |
| 5 | 1094.125 | 1078.432 |
| 6 | 1069.803 | 1089.693 |
| 7 | 1180.355 | 1075.438 |
| 8 | 1097.253 | 1034.397 |
| 9 | 1233.903 | 1220.247 |
| 10 | 1123.023 | 1115.827 |
| 11 | 1220.447 | 1181.385 |
| 12 | 1086.662 | 1072.499 |
| 13 | 1124.874 | 1084.089 |
| 14 | 1069.845 | 1084.660 |
| 15 | 1167.554 | 1151.393 |
| 16 | 1153.832 | 1133.210 |
| 17 | 1215.615 | 1212.701 |
| 18 | 1089.785 | 1075.664 |
| 19 | 1095.274 | 1081.919 |
| 20 | 1198.057 | 1206.624 |

缓存回源两次：1987.715、1975.036 ms。最终普通组平均值为 918.799 ms，长历史组平均值为 1,142.625 ms。

[E1]: #51-原始测量来源
[E2]: #51-原始测量来源
[E3]: #51-原始测量来源
[E4]: #51-原始测量来源
[E5]: #51-原始测量来源
