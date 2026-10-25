# Ozone Container Balancer 设计与实现详解

## 1. 整体定位与目标

### 1.1 解决的问题

Container Balancer（容器均衡器）是运行在 SCM（Storage Container Manager）内部的一个后台服务，用于**在 datanode 之间搬移 CLOSED 状态的 container 副本，从而拉平集群中各节点的磁盘利用率**（used / capacity）。典型场景：新扩容的节点利用率远低于老节点、异构容量机器导致利用率不均、扩缩容后数据分布倾斜。

- 它只搬移**已经 CLOSED（或放宽模式下 QUASI_CLOSED/OVER_REPLICATED）的容器副本**，不碰处于写入状态的 OPEN 容器，因此不会干扰正在进行的 I/O（`hadoop-hdds/docs/content/feature/ContainerBalancer.md:30`）。
- 它不是独立进程，而是 `StorageContainerManager` 内部的一个 `SCMService`（`ContainerBalancer.java:55`），本质上是"决策者"：决定"谁是 source、谁是 target、搬哪个 container"，而真正的数据复制/删除委托给 `ReplicationManager`/`MoveManager` 完成。

### 1.2 与 ReplicationManager、SCM 的关系

- `ContainerBalancer` 持有 `StorageContainerManager scm` 引用，通过它拿到 `NodeManager`、`ContainerManager`、`ReplicationManager`、`MoveManager`、`SCMContext`（`ContainerBalancerTask.java:143-162`）。
- `ReplicationManager` 负责判断容器的"健康状态"（是否欠复制/过复制/错误放置）并发送真正的 add/delete replica 命令；balancer 自己**不直接**发命令给 datanode，而是通过 `MoveManager` 这一层转发给 `ReplicationManager`。
- balancer 与 `ReplicationManager` 之间有明确的职责边界：balancer 只会挑选**健康的**（非 under/over/mis-replicated）容器去搬，如果容器本身有复制问题，balancer 会主动放弃，把问题留给 `ReplicationManager` 常规修复链路处理（见第 9 节）。

### 1.3 线程模型

- `ContainerBalancer` 本身是单例（每个 SCM 进程一个实例，注册到 `scm.getSCMServiceManager()`，`ContainerBalancer.java:94`），只负责生命周期管理（start/stop/状态持久化），自己不跑循环逻辑。
- 真正跑均衡算法的是 `ContainerBalancerTask`（实现 `Runnable`），每次 `start()` 都会 new 一个新的 `ContainerBalancerTask` 实例并丢到一个新建的后台 daemon 线程里运行（`ContainerBalancer.java:329-340`，`startBalancingThread`）。
- `ContainerBalancerTask` 内部是一个**单线程的、分 iteration（轮次）驱动的状态机**：`Status` 枚举为 `RUNNING -> STOPPING -> STOPPED`（`ContainerBalancerTask.java:1351-1355`）。容器移动的异步完成通过 `CompletableFuture` 处理（见第 8 节），但"决策"过程本身是单线程串行的，没有线程池并行选容器。

## 2. 启动/停止/生命周期管理

### 2.1 StatefulService：状态持久化机制

`ContainerBalancer extends StatefulService<ContainerBalancerConfigurationProto>`（`ContainerBalancer.java:55`）。`StatefulService` 把配置以 protobuf 序列化后存入 SCM 的 RocksDB（通过 `StatefulServiceStateManager`，key 是服务名 `ContainerBalancer`）。这意味着：

- balancer 的"应该运行吗"(`shouldRun`)、"下一轮迭代从第几轮开始"(`nextIterationIndex`)、完整配置都被持久化。
- **SCM 重启或 HA 场景下 leader 切换后，新 leader 可以从 RocksDB 里读出上次保存的配置，决定是否要自动恢复 balancer 运行**，并从中断的那个 iteration 继续（而不是从头开始）。由 `startBalancer`/`start`/`saveConfiguration` 三者共同实现（`ContainerBalancer.java:251-287`, `301-324`, `452-460`）。

### 2.2 Leader/SafeMode 联动

`ContainerBalancer` 实现了 `notifyStatusChanged()`（`ContainerBalancer.java:103-141`），这是 SCM 的 `SCMServiceManager` 在 Raft 角色或安全模式变化时广播的回调：

- 如果当前 SCM **不是 leader 或处于安全模式**：若 balancer 正在运行则调用 `stop()` 强制停止（`ContainerBalancer.java:105-117`）。
- 如果当前 SCM **变成 leader 且退出安全模式**：检查持久化状态 `shouldRun()`（读 RocksDB 中 `proto.getShouldRun()`，`ContainerBalancer.java:148-165`），如果之前配置为"应该运行"，就调用 `start()` 自动续跑（`ContainerBalancer.java:120-141`）。

`start()` 内部传入 `delayStart=true`（`startBalancingThread(proto.getNextIterationIndex(), true)`，`ContainerBalancer.java:283`）会使 `ContainerBalancerTask.run()` 先 `sleep` `hdds.scm.wait.time.after.safemode.exit` 秒，这正是文档中"SCM failover 后延迟启动"的实现（`ContainerBalancerTask.java:184-194`）。而用户通过 CLI 主动 `startBalancer()` 不延迟（`delayStart=false`，`ContainerBalancer.java:320`）。

### 2.3 start/stop API 与异常

两条调用路径：
- `start()`（SCMService 接口方法，由框架在状态变化时回调）——从持久化配置恢复运行。
- `startBalancer(ContainerBalancerConfiguration)`（给 CLI/Admin API）——使用用户传入的新配置启动，并立即 `saveConfiguration(configuration, true, 0)` 持久化（`ContainerBalancer.java:301-324`）。

两条路径共用 `validateState()` 做前置校验：

```java
// ContainerBalancer.java:370-383
private void validateState(boolean expectedRunning) throws IllegalContainerBalancerStateException {
  validateEligibility();              // leader-ready 且不在安全模式
  if (!expectedRunning && !canBalancerStart()) throw ...;  // 期望未运行但其实在运行
  if (expectedRunning && !canBalancerStop()) throw ...;    // 期望运行但其实没在运行
}
```

- `IllegalContainerBalancerStateException`：状态机层面的非法调用（SCM 不是 leader/在安全模式/balancer 已经在跑却又 start/没在跑却 stop）。
- `InvalidContainerBalancerConfigurationException`：**配置内容**不合法（见第 3.1 节具体校验规则）。两者都继承自 `SCMServiceException`，是可预期的"用户/配置错误"，不是系统缺陷。

`stop()`/`stopBalancer()` 都会调用 `blockTillTaskStop()`，用自旋 `interrupt()` + `join(5)` 的方式同步等待后台线程真正退出（`ContainerBalancer.java:409-424`），保证 `stop` 调用返回时任务已经彻底停掉。

## 3. 配置体系（`ContainerBalancerConfiguration`，前缀 `hdds.container.balancer`）

| 配置项 | 默认值 | 作用 |
|---|---|---|
| `hdds.container.balancer.utilization.threshold` | 10（%） | 阈值，决定 upper/lower limit 的宽度 |
| `hdds.container.balancer.datanodes.involved.max.percentage.per.iteration` | 20（%） | 单次 iteration 中最多允许多少比例的健康 in-service datanode 参与（source+target 合计） |
| `hdds.container.balancer.size.moved.max.per.iteration` | 500GB | 单次 iteration 总共最多搬多少数据 |
| `hdds.container.balancer.size.entering.target.max` | 26GB | 单次 iteration 中单个 target 节点最多能"流入"多少 |
| `hdds.container.balancer.size.leaving.source.max` | 26GB | 单次 iteration 中单个 source 节点最多能"流出"多少 |
| `hdds.container.balancer.iterations` | 10 | 总共跑多少轮；-1 表示无限轮 |
| `hdds.container.balancer.exclude.containers` / `include.containers` | 空 | 容器级黑白名单 |
| `hdds.container.balancer.move.timeout` | 65m | 单个容器一次 move（复制+删除）允许的总超时 |
| `hdds.container.balancer.move.replication.timeout` | 50m | 其中分配给"复制"阶段的子超时（必须小于 move.timeout） |
| `hdds.container.balancer.balancing.iteration.interval` | 3m | 两轮 iteration 之间的等待间隔 |
| `hdds.container.balancer.include.datanodes` / `exclude.datanodes` | 空 | 节点级黑白名单（按 hostname/IP） |
| `hdds.container.balancer.move.networkTopology.enable` | false | 选 target 时是否按网络拓扑优化（跨机架优先） |
| `hdds.container.balancer.trigger.du.before.move.enable` | false | 每轮开始前是否主动触发所有健康节点跑 `du` 刷新磁盘用量 |
| `hdds.container.balancer.include.non.standard.containers` | false | 是否放宽到允许搬 OVER_REPLICATED 的 CLOSED/QUASI_CLOSED 容器 |

### 3.1 配置间的强校验规则（`ContainerBalancer.java:467-563`，均在 `validateConfiguration()` 中）

1. `size.entering.target.max` 和 `size.leaving.source.max` **必须大于** `ozone.scm.container.size`（否则一个容器都塞不进去）。
2. 两者都必须 **小于等于** `size.moved.max.per.iteration`。
3. `balancing.iteration.interval` 应大于 `hdds.node.report.interval`（否则下一轮可能还在用过时的磁盘用量数据），不满足时只告警不阻断。
4. `move.replication.timeout` 必须**严格小于** `move.timeout`。
5. `(move.timeout - move.replication.timeout - hdds.scm.replication.event.timeout.datanode.offset)` 必须 **≥ 9 分钟**——为删除阶段预留最小窗口，不满足会抛 `InvalidContainerBalancerConfigurationException`（`ContainerBalancer.java:541-556`）。
6. include/exclude 节点列表、include/exclude 容器列表不能互相完全覆盖导致无节点/无容器可用。
7. include-containers 中列出的容器 ID 必须真实存在于 SCM。
8. 过滤后，**健康 IN_SERVICE 节点数至少 2 个**，且按比例换算后每轮至少能凑出 1 对 source+target（`validateEligibleDatanodePool`，`ContainerBalancer.java:653-694`）。

配置支持通过 protobuf（`ContainerBalancerConfigurationProto`）与持久化状态互转（`ContainerBalancerConfiguration.java:538-617`）。

还有一组**节流档位** `ContainerBalancerProfile`（SLOW/MEDIUM/FAST），预置了不同的 `datanodesMaxPercentage` 和流入/流出上限（10%/10GB、20%/26GB、40%/100GB），供 Advisor 做"预估"时选用，不是运行时的强制档位。

## 4. 集群快照与不均衡度量

### 4.1 核心公式（`ContainerBalancerClusterAnalyzer.java`）

```
utilization = (capacity - remaining) / capacity
```
（`calculateAvgUtilization`，第 43-48 行；单节点版本在 221-227 行）

集群平均利用率是把所有参与节点的 capacity/remaining **先聚合求和再相除**，而不是节点利用率的算术平均（`ContainerBalancerTask.calculateAvgUtilization`，`ContainerBalancerTask.java:1132-1147`）。

```
upperLimit = clusterAvgUtilization + threshold
lowerLimit = clusterAvgUtilization - threshold
```

- 利用率 **> upperLimit** → over-utilized（source 候选）
- 利用率 **< lowerLimit** → under-utilized（target 候选）
- 其余节点视为已均衡

### 4.2 两个数据结构的分工

- `ContainerBalancerClusterAnalyzer`：纯函数工具类（无状态，全 static 方法），既被真正跑迭代的 `ContainerBalancerTask.initializeIteration()` 复用（`isOverUtilized`/`isUnderUtilized`/`overUtilizedBytes`/`underUtilizedBytes` 等），也被 `ContainerBalancerAdvisor`（离线预估工具）调用其 `analyze()` 方法一次性产出完整快照。
- `ContainerBalancerClusterSnapshot`：不可变 Builder 模式的数据类，携带"某一时刻"的汇总指标——`totalEligibleDatanodes`、`clusterAvgUtilization`、`upperLimit/lowerLimit`、`sourceCount/targetCount`、`totalOverUtilizedBytes/totalUnderUtilizedBytes`、`imbalance`（max-min 利用率差）、以及 Top-5 的 source/target 节点列表。这是一份只读的"体检报告"，服务于 `ContainerBalancerAdvisor` 预估/CLI 展示，不被真正的均衡循环使用。

`ContainerBalancerTask` 自己维护 `overUtilizedNodes`/`underUtilizedNodes`（`ContainerBalancerTask.java:92-93`），是真正驱动迭代算法的数据，和 `ContainerBalancerClusterSnapshot` 是两条平行的代码路径——算法逻辑共享但数据结构不同，这样"离线预估"就不依赖正在跑的 task 状态。

## 5. 核心迭代算法（`ContainerBalancerTask`）

### 5.1 顶层循环：`balance()`（`ContainerBalancerTask.java:225-345`）

```
for (i = nextIterationIndex; i < iterations && isBalancerRunning(); i++) {
    resetState();
    (可选) triggerDu + sleep 等待刷新;
    if (!initializeIteration()) { 根据失败原因决定 return 或 stop; }
    result = doIteration();
    记录 iterationStatistic;
    if (result == CAN_NOT_BALANCE_ANY_MORE) { stop; return; }
    if (result == ITERATION_COMPLETED) { 持久化 nextIterationIndex = i+1; }
    sleep(balancingInterval);  // 最后一轮不睡
}
tryStopWithSaveConfiguration(COMPLETED_ALL_ITERATIONS);
```

### 5.2 `initializeIteration()`：划分 over/under-utilized 节点（`ContainerBalancerTask.java:534-640`）

1. 校验 SCM 仍是 leader 且不在安全模式，否则标记 `INIT_SCM_NOT_READY` 失败。
2. 从 `NodeManager.getMostOrLeastUsedDatanodes(true)` 拿到全部节点按利用率排序的快照，按 include/exclude 过滤。
3. 计算 `clusterAvgUtilisation`、`upperLimit`、`lowerLimit`。
4. 遍历每个节点分类进 `overUtilizedNodes` / `underUtilizedNodes`，同时累加指标。
5. `underUtilizedNodes` 最后被 `Collections.reverse()`——因为节点列表原本按"从多到少"排序，under-utilized 子集需反转为"利用率从低到高"，便于后续贪心策略优先选最闲的节点。
6. 若两个列表都为空 → 集群已均衡，返回 `false`（`INIT_NO_UNBALANCED_DATANODES`），上层据此直接 stop balancer（而非重试）。
7. 成功时才构造本轮的 `ContainerBalancerSelectionCriteria`。

### 5.3 `doIteration()`：source-target 配对主循环（`ContainerBalancerTask.java:654-715`）

```java
while (true) {
  if (!isBalancerRunning()) { ITERATION_INTERRUPTED; break; }
  if (reachedMaxSizeToMovePerIteration()) break;
  adaptWhenNearingIterationLimits();   // 临近 datanode 限额时收紧 target 候选
  adaptOnReachingIterationLimits();    // 达到限额时同时收紧 source+target 候选
  source = findSourceStrategy.getNextCandidateSourceDataNode();
  if (source == null) break;           // 没有更多 source 了
  moveSelection = matchSourceWithTarget(source);
  if (moveSelection != null) processMoveSelection(source, moveSelection);
  else findSourceStrategy.removeCandidateSourceDataNode(source);
}
checkIterationResults(isMoveGeneratedInThisIteration);
```

这是一个**贪心的、逐个 source 处理**的循环：每次从 `FindSourceGreedy`（内部用 `PriorityQueue` 按"考虑了本轮已流出数据后的当前利用率"降序排列）弹出利用率最高的候选 source，为它找一个合适的 target+container 组合，成功则继续处理下一个 source（source 若后续还有余量可被重新入队），失败则彻底剔除该 source。

### 5.4 选 container：`ContainerBalancerSelectionCriteria.shouldBeExcluded()`（第 191-236 行）

对一个 source 节点上的候选容器集合（按已用字节数**从大到小**排序，优先搬大容器以更快拉平利用率，见 `getCandidateContainers`，429-451 行），排除规则包括：

- 不在 `include-containers`（若非空）内；或在 `exclude-containers` / 本轮失败排除集 / 永久排除集内；
- 已经在本轮被选过；
- `canSizeLeaveSource` 返回 false（会突破 `size.leaving.source.max` 或导致 source 利用率跌破 lowerLimit）；
- 会突破本轮全局 `size.moved.max.per.iteration`；
- 容器未 CLOSED，或该副本在该节点上的状态不是 CLOSED；
- `replicationManager.getContainerReplicationHealth(...)` 判定非 HEALTHY（欠复制/过复制/错放置），或存在在途的复制/删除动作。

若开启 `include.non.standard.containers`，则换用放宽版本，允许 OVER_REPLICATED 的 CLOSED 容器（需满足"CLOSED 副本数 ≥ 所需副本数"）以及全员 QUASI_CLOSED 且源副本非空的容器参与搬迁。

### 5.5 选 target：`FindTargetStrategy`（贪心策略，两种实现）

- `FindTargetGreedyByUsageInfo`：`TreeSet` 按"当前利用率加上本轮已流入量后的利用率"升序排列，始终优先选利用率最低的 target。
- `FindTargetGreedyByNetworkTopology`：在贪心基础上额外考虑拓扑距离（跨机架优先），由 `hdds.container.balancer.move.networkTopology.enable` 开关控制。

公共逻辑在 `AbstractFindTargetGreedy.findTargetForContainerMove()`：遍历 potentialTargets，要求该容器在 target 上还没有副本、把副本集合中 source 换成 target 后仍满足 `PlacementPolicyValidateProxy`（机架感知等放置策略）、以及 `canSizeEnterTarget`（不突破 `size.entering.target.max` 且不超过 upperLimit）。第一个满足条件的 target 即被选中（贪心，非全局最优）。

### 5.6 限流与自适应收紧

- `reachedMaxSizeToMovePerIteration()`：全局已调度字节数达到上限直接跳出循环。
- `adaptWhenNearingIterationLimits()`（941-960 行）：涉及节点数距上限只差 1 时，把 target 候选池收紧为"已经被选中过的 target"，避免引入新节点导致超限。
- `adaptOnReachingIterationLimits()`（968-986 行）：真正达到上限时，source 和 target 候选池都收紧为已选中集合，后续只能在已涉及节点之间继续搭配。

### 5.7 发起真正的移动：`moveContainer()`（998-1095 行）

```java
future = moveManager.move(containerID, source, target);
future.whenComplete((result, ex) -> { 更新 metrics / moveFailureTracker; });
```

根据返回结果做不同补救：`ContainerNotFoundException`/`NodeNotFoundException` → source 退回候选池，容器加入永久排除集；`ContainerReplicaNotFoundException` → 仅退回 source；各类"可重试"失败（`REPLICATION_FAIL_*`、`FAIL_CONTAINER_ALREADY_BEING_MOVED` 等）→ 退回 source 换容器再试；`REPLICATION_NOT_HEALTHY_AFTER_MOVE` → 退回 source 同时把该容器加入**本轮**失败排除集。

### 5.8 Iteration 收尾（770-870 行）

- 若本轮一个 move 都没发起成功，直接判定 `CAN_NOT_BALANCE_ANY_MORE`，上层据此彻底停止 balancer。
- 否则用 `CompletableFuture.allOf(...).get(moveTimeout, ...)` 阻塞等待本轮所有 move 完成，超时则取消未完成的 future 并记为 `ITERATION_MOVE_TIMEOUT`——代码注释明确指出：**如果复制/删除命令已下发给 datanode，SCM 侧无法真正撤回，只能依赖 datanode 侧命令自身超时**（833-841 行注释）。

### 5.9 停止原因汇总（`ContainerBalancerStopReason`）

| 枚举值 | 触发场景 |
|---|---|
| `USER_REQUESTED` | CLI stop |
| `SCM_STATE_CHANGE` | 失去 leader 或进入安全模式 |
| `COMPLETED_ALL_ITERATIONS` | 跑满配置的 iterations 数 |
| `CAN_NOT_BALANCE_ANY_MORE` | 某轮一个 move 都生成不出来 |
| `INITIALIZATION_FAILED` | SCM 未就绪 / 节点列表为空 / 无不均衡节点 |
| `ERROR` | run() 中捕获未预期异常 |
| `UNKNOWN` | 兜底 |

## 6. 度量与可观测性

**JMX 指标**（`ContainerBalancerMetrics`）：分"最近一轮"和"累计"两套平行计数器——`dataSizeMovedGB(InLatestIteration)`、`numContainerMovesScheduled/Completed/Timeout/Failed(InLatestIteration)`、`numIterations`、`numDatanodesInvolvedInLatestIteration`、`dataSizeUnbalancedGB`、`numDatanodesUnbalanced`。每轮开始的 `resetState()` 清零"InLatestIteration"计数器；`incrementCurrentIterationContainerMoveMetric()` 按 `MoveManager.MoveResult` 枚举分流（192-226 行）。

**状态快照类**（服务于 `ozone admin containerbalancer status`）：
- `ContainerBalancerStatusInfo`：顶层状态——启动时间、当前配置、每轮迭代统计列表，及停止时间/原因/消息。
- `ContainerBalancerTaskIterationStatusInfo`：单轮统计——迭代号、结果、耗时、各类容器数、计划/实际搬运字节数、各节点流入/流出字节 Map，以及**失败明细**（按原因+节点分组，供 `-v --history` 展示）。
- `ContainerMoveFailureTracker`：每轮失败归因簿记，同时接收 `MoveManager.MoveResult` 与 balancer 自定义失败原因（`PRE_MOVE_CONTAINER_NOT_FOUND` 等）。

这些状态类都提供 `toProto()`，走 SCM RPC 协议对外暴露。

## 7. `ContainerBalancerAdvisor` / `ContainerBalancerEstimation`：离线预估工具

两者位于 `common` 模块，**不参与运行时真正的均衡循环**，是纯函数式"假设分析"工具：

- `ContainerBalancerAdvisor.estimate()`：给定节点用量快照和参数覆盖，先调用 `analyze()` 得到 `ContainerBalancerClusterSnapshot`，再对 SLOW/MEDIUM/FAST 档位分别估算**每轮能搬多少字节**（取全局/流出/流入三个上限与待搬总量的最小正值）、**需要多少轮**（ceil(待搬总量/每轮搬运量)）、**预计总耗时**。
- `ContainerBalancerEstimation`：单档位估算结果（成功时含字节数/轮数/耗时；失败时含失败原因，如"没有 over-utilized 节点"）。

**调用方**：给 `ozone admin containerbalancer` 的"评估/预览"类操作使用，在不实际启动 balancer 的前提下做容量规划，由 CLI 按需触发一次性计算，不被 SCM 内部定时任务自动调用。

## 8. 并发与线程安全

- `ContainerBalancer` 用一个 `ReentrantLock` 保护所有生命周期方法，防止并发的 leader 变更回调和用户 CLI 调用互相竞争。
- `ContainerBalancerTask` 决策主循环**单线程**，不存在并发选 source/target 的竞争；但容器搬移本身是**异步**的：`moveManager.move()` 立即返回 `CompletableFuture<MoveResult>`，真正的网络 IO 由 `ReplicationManager` 异步完成。Task 把这些 future 收集进 `ConcurrentHashMap`（`moveSelectionToFutureMap`），一轮所有 source 处理完后才用 `CompletableFuture.allOf(...).get(timeout)` 一次性阻塞等待。
- `MoveManager` 内部用 `ConcurrentHashMap<ContainerID, MoveOperation> pendingMoves` 跟踪在途 move，实现 `ContainerReplicaPendingOpsSubscriber` 接口，以**回调**方式被 `ReplicationManager` 的副本操作完成/超时事件驱动，推进 move 状态机——这是整个系统最核心的异步协作点。关键临界区用 `synchronized (containerInfo)` 保护"检查副本状态+决定是否可发起 move"的原子性，避免同一容器被并发调度两次。
- `ContainerBalancerTask` 用 `volatile` 字段和 `AtomicBoolean` 保证跨线程（balancer 线程 vs 调用 stop/查询状态的线程）可见性。

## 9. 容器迁移的底层机制：MoveManager 与 ReplicationManager 的边界

`ContainerBalancerTask` 只负责决策，真正"搬家"的两阶段流程封装在 `MoveManager.move()`（156-274 行）：

1. **前置校验**（全部在 `synchronized(containerInfo)` 块中）：source/target 必须 HEALTHY 且 IN_SERVICE；source 上必须有副本、target 上不能已有副本；移动前容器必须 HEALTHY（欠/过/错放置都拒绝，除非放宽模式）；无正在进行的 add/delete；容器必须 CLOSED；模拟搬完之后的副本集合再次校验放置策略，防止破坏副本分布规则。
2. **阶段一：复制**——校验通过后注册 `MoveOperation`，调用 `replicationManager.sendLowPriorityReplicateContainerCommand(...)`。这只是向 `ReplicationManager` 提交一条"低优先级复制"指令，真正的指令下发、datanode 执行、心跳/增量报告确认全部走 `ReplicationManager` 既有的常规复制链路（和修复欠复制容器是同一套机制）。
3. **回调驱动**：target 上的 ADD 完成后触发 `handleSuccessfulAdd()`，再次确认 source 健康、再次校验"删除 source 副本之后"仍然健康。
4. **阶段二：删除**——调用 `replicationManager.sendDeleteCommand(...)`，同样委托给常规删除链路。
5. **完成**：source 上的 DELETE 完成回调触发 `completeMove(cid, COMPLETED)`，`CompletableFuture` complete，Task 更新 metrics。
6. 任一阶段超时（由 `ReplicationManager` 的 pending-op 过期机制驱动）分别产生 `REPLICATION_FAIL_TIME_OUT` 或 `DELETION_FAIL_TIME_OUT`。

**职责划分**：balancer（`ContainerBalancerTask`+`MoveManager`）决定"要不要搬、搬哪个、搬去哪"并做搬运前后的健康/放置策略校验；`ReplicationManager` 负责"真正怎么复制数据、怎么确认完成、怎么判断超时"——这是 Ozone 所有副本增减共用的底层执行引擎，balancer 只是它的一个调用方，优先级标记为"low priority"以避免和紧急的欠复制修复抢资源。

## 10. 已知限制 / 设计取舍

1. **只搬 CLOSED 容器**（默认），避免干扰写入路径；放宽模式才允许部分 QUASI_CLOSED/OVER_REPLICATED 容器参与。
2. **贪心算法，非全局最优**：source/target 选择都是局部贪心，不做全局规划求解，换取实现简单和可预测开销，代价是极端分布下可能需要更多轮次才能收敛。
3. **move 一旦下发无法真正撤销**：复制/删除指令发给 datanode 后，SCM 端无法追回，只能等待 datanode 侧命令自身超时。
4. **iteration 间的磁盘用量新鲜度依赖心跳**：`balancing.iteration.interval` 若小于 `node.report.interval`，下一轮可能用过时利用率数据决策（只告警不阻断）。
5. **`CAN_NOT_BALANCE_ANY_MORE` 是保守终止条件**：某一轮一个 move 都生成不出来就彻底停止整个 balancer，而非跳过重试——认为"找不到可行移动"大概率意味着约束过紧或集群已尽力拉平。
6. **永久排除集 `excludeContainersNotFound` 有容量上限**（`MAX_NOT_FOUND_CONTAINERS = 10_000`），超过后找不到的容器不再加入，可能被重复尝试。
7. **不做跨磁盘（同节点多卷）粒度均衡**：节点粒度均衡是 Container Balancer 的职责，节点内多盘均衡是独立组件 **DiskBalancer** 的职责，两者互补但完全独立，不共享代码路径。
8. **Advisor/Estimation 是启发式预估，非精确保证**：用"50/50 对半分配"简化假设估算 source/target 节点数，实际贪心算法分配未必对半，预估轮数/耗时仅供参考。

## 核心类关系图

```
StorageContainerManager (SCM)
   └─ registers ─▶ ContainerBalancer (SCMService / StatefulService)
                      │  管理生命周期：start/stop/状态持久化(RocksDB)
                      │  对接 SCMContext (leader/safemode 状态)
                      │
                      └─ 每次 start() 创建并驱动 ──▶ ContainerBalancerTask (Runnable, 单线程)
                                                         │
                                                         │ 每轮 iteration:
                                                         ├─ initializeIteration()
                                                         │     └─ ContainerBalancerClusterAnalyzer (静态工具)
                                                         │           判定 over/under-utilized 节点
                                                         │
                                                         ├─ doIteration() 贪心主循环
                                                         │     ├─ FindSourceStrategy (FindSourceGreedy)
                                                         │     │     选 source（利用率最高且还能流出）
                                                         │     ├─ ContainerBalancerSelectionCriteria
                                                         │     │     在 source 上挑候选 container
                                                         │     │     （健康/CLOSED/未占用/不超限额）
                                                         │     ├─ FindTargetStrategy
                                                         │     │     (FindTargetGreedyByUsageInfo /
                                                         │     │      FindTargetGreedyByNetworkTopology)
                                                         │     │     选 target（利用率最低/拓扑优先，
                                                         │     │     满足 PlacementPolicy 与流入限额）
                                                         │     └─ moveContainer()
                                                         │           └─▶ MoveManager.move(container, src, tgt)
                                                         │                  │ 前置校验(健康/放置策略)
                                                         │                  ├─ sendLowPriorityReplicateContainerCommand
                                                         │                  │     ──▶ ReplicationManager (真正执行副本复制)
                                                         │                  ├─ (回调) handleSuccessfulAdd → 再校验
                                                         │                  └─ sendDeleteCommand
                                                         │                        ──▶ ReplicationManager (真正执行副本删除)
                                                         │                  返回 CompletableFuture<MoveResult>
                                                         │
                                                         ├─ ContainerBalancerMetrics (JMX 累计/当轮指标)
                                                         ├─ ContainerMoveFailureTracker (按原因/节点的失败归因)
                                                         └─ ContainerBalancerTaskIterationStatusInfo
                                                               └─ 汇总进 ContainerBalancerStatusInfo
                                                                     （供 `ozone admin containerbalancer status` 查询）

(独立、离线侧) ContainerBalancerAdvisor ──使用──▶ ContainerBalancerClusterAnalyzer / ContainerBalancerProfile
                     │
                     └─ 产出 ContainerBalancerEstimation（预估每轮搬运量/轮数/总耗时，不参与实际运行）
```
