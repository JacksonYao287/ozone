<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Ozone Container Balancer 设计与实现分析

本文根据当前源码树说明 Ozone SCM 中 Container Balancer 的架构、调度逻辑、移动状态机、EC/Ratis 行为差异、限流、安全约束和可观测性。

## 1. 定位

Container Balancer 是 SCM 的后台服务，按 datanode 磁盘空间利用率迁移已关闭 container 的 replica，以减少容量偏斜：

```text
source DN 的一个 container replica
  -> 复制到 target DN
  -> SCM 确认 target replica
  -> 删除 source DN 的旧 replica
```

它不移动 key namespace，不重写用户数据，不创建新 pipeline，也不负责修复欠副本/错误放置 container。后者属于 `ReplicationManager`。Container Balancer 的职责是：在 container 已健康、replication/placement 正确的前提下，安全迁移 existing replica。

主要源码：

```text
hadoop-hdds/common/src/main/java/org/apache/hadoop/hdds/scm/container/balancer/
  ContainerBalancerConfiguration.java
  ContainerBalancerClusterAnalyzer.java

hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/balancer/
  ContainerBalancer.java
  ContainerBalancerTask.java
  MoveManager.java
  ContainerBalancerSelectionCriteria.java
  FindSourceGreedy.java
  AbstractFindTargetGreedy.java
  FindTargetGreedyByUsageInfo.java
  FindTargetGreedyByNetworkTopology.java
```

用户文档：

```text
hadoop-hdds/docs/content/feature/ContainerBalancer.md
```

## 2. 架构与调用链

```text
管理员 CLI / SCM RPC
  -> SCMClientProtocolServer.startContainerBalancer(...)
  -> ContainerBalancer.startBalancer(...)
  -> StatefulService 持久化配置
  -> ContainerBalancerTask 后台线程
  -> 每轮 iteration:
       计算 DN usage、source 和 target
       筛选 source 上可移动 container
       找到 placement 合法的 target
       MoveManager.move(...)
       ReplicationManager 发送低优先级 ReplicateContainerCommand
       等待 target ADD 完成
       ReplicationManager 发送 source DeleteContainerCommand
       等待 source DELETE 完成
```

职责分工：

| 组件 | 职责 |
|---|---|
| `ContainerBalancer` | 生命周期、HA/safe mode、状态持久化、启动停止 |
| `ContainerBalancerTask` | iteration、利用率分类、配对、并发 move 管理 |
| `FindSourceGreedy` | 选择最满 source DN |
| `FindTargetStrategy` | 选择合法 target DN |
| `ContainerBalancerSelectionCriteria` | 筛选可搬 container |
| `MoveManager` | 复制后删除的 move 状态机 |
| `ReplicationManager` | 发 DN 命令、pending ADD/DELETE、健康检查 |
| `ContainerBalancerMetrics` | 移动、字节、节点、iteration metrics |

## 3. Service 生命周期与 SCM HA

`ContainerBalancer` 继承：

```java
StatefulService<ContainerBalancerConfigurationProto>
```

因此它会持久化运行状态和配置，而不是仅存在于当前 JVM 内存。持久化信息包括：

```text
shouldRun
nextIterationIndex
threshold
iterations
移动限制
超时
include/exclude nodes
include/exclude containers
network topology 开关
```

这支持：

```text
start balancer
  -> SCM leader failover / restart
  -> 新 leader leader-ready
  -> 读取持久化配置
  -> shouldRun=true 时重新启动
  -> 从 nextIterationIndex 继续
```

不过上一任 leader 的 Java `CompletableFuture` 不会被恢复；真实复制状态仍由 ReplicationManager pending ops、DN reports 和超时机制决定。

### 3.1 运行资格

Balancer 只能在：

```text
SCM leader-ready
且 SCM 不在 safe mode
```

时运行。`notifyStatusChanged()` 在以下情况停止 task：

```text
SCM 变 follower
SCM 进入 safe mode
```

原因是 Balancer 会下发 replica copy/delete，必须依赖 SCM 对 container、replica、node state 的权威视图。

### 3.2 Failover 后延迟

从持久化状态自动恢复时，`ContainerBalancerTask` 会按：

```text
hdds.scm.wait.time.after.safe.mode.exit
```

延迟启动。该等待给新 leader 时间接收 container report、usage report、pending-op 状态及 ReplicationManager 状态，避免基于过期 usage 立刻迁移。

### 3.3 启动与停止

管理员使用：

```bash
ozone admin containerbalancer start
ozone admin containerbalancer stop
ozone admin containerbalancer status -v --history
```

启动时写入：

```text
shouldRun = true
nextIterationIndex = 0
```

成功完成一个 iteration 后，尝试持久化：

```text
nextIterationIndex = 当前轮次 + 1
```

用户停止时写入：

```text
shouldRun = false
nextIterationIndex = 0
```

停止原因包含：

```text
USER_REQUESTED
SCM_STATE_CHANGE
CAN_NOT_BALANCE_ANY_MORE
COMPLETED_ALL_ITERATIONS
INITIALIZATION_FAILED
ERROR
UNKNOWN
```

## 4. 利用率模型

核心实现：`ContainerBalancerClusterAnalyzer`。

平均利用率为容量加权平均：

```text
clusterAvg = (totalCapacity - totalRemaining) / totalCapacity
           = totalUsed / totalCapacity
```

它不是每个 DN 利用率的简单算术平均，因此适用于异构容量集群。

阈值配置：

```text
hdds.container.balancer.utilization.threshold
```

默认 `10%`。设阈值 ratio 为 `t`：

```text
upperLimit = clusterAvg + t
lowerLimit = clusterAvg - t
```

分类规则：

```text
usage > upperLimit -> over-utilized source
usage < lowerLimit -> under-utilized target
其他               -> 位于平衡带
```

例如：

```text
clusterAvg=60%, threshold=10%
lowerLimit=50%, upperLimit=70%

90% -> source
75% -> source
45% -> target
30% -> target
```

使用率来自 NodeManager 维护的 datanode usage reports。若 iteration interval 小于 `hdds.node.report.interval`，下一轮可能仍使用旧 usage；代码会给出警告。

可选配置：

```text
hdds.container.balancer.trigger.du.before.move.enable
```

启用后，Balancer 每轮请求 healthy/in-service DN 立即刷新 `du`，再等待约 `3 × node report interval` 后开始调度。它能提高 usage 精度，但会产生较高 DN 磁盘 I/O，默认关闭。

## 5. 每轮 iteration

`ContainerBalancerTask#balance()` 的主流程：

```text
resetState
  -> 可选刷新 DN usage
  -> initializeIteration
  -> 计算 source/target
  -> 贪心产生多个 move
  -> 等待 moves 完成或 timeout
  -> 汇总 metrics/history
  -> 保存 next iteration index
  -> sleep balancing interval
```

`initializeIteration()`：

1. 验证 SCM 仍 leader-ready 且不在 safe mode；
2. 从 NodeManager 获取 usage nodes；
3. 依 include/exclude DN 过滤；
4. 计算 weighted cluster average；
5. 计算 lower/upper utilization band；
6. 分类 over-utilized 与 under-utilized nodes；
7. 创建 `ContainerBalancerSelectionCriteria`；
8. 记录不平衡字节数和节点数 metrics。

若没有任何 source 和 target，会停止，原因通常为已平衡；但也可能是可参与节点被配置过滤完。

## 6. Greedy source/target 算法

### 6.1 Source

`FindSourceGreedy` 使用 priority queue，按预测利用率倒序：

```text
sourceEffectiveUsage = utilization(usedBytes - scheduledLeavingBytes)
```

最满 source 优先。source 每计划移出一个 container，其预测利用率下降并会重新入队排序。

### 6.2 Target

默认策略 `FindTargetGreedyByUsageInfo` 使用 TreeSet，按预测利用率升序：

```text
targetEffectiveUsage = utilization(usedBytes + scheduledEnteringBytes)
```

即最空 target 优先。

若启用：

```text
hdds.container.balancer.move.networkTopology.enable=true
```

则使用 `FindTargetGreedyByNetworkTopology`：

```text
1. source 到 target 的 topology distance 最小优先
2. 距离相同，预测利用率低者优先
```

网络拓扑策略不会绕过 placement policy；它只在合法 target 之间改变优先级。

## 7. 每轮限制与节流

Balancer 同时受以下限制：

### 7.1 总计划迁移量

```text
hdds.container.balancer.size.moved.max.per.iteration
```

默认 `500GB`。候选需满足：

```text
scheduledBytes + containerUsedBytes <= maxSizeToMovePerIteration
```

限制的是已调度字节，不只是最终成功字节，以防并发调度超过预算。

### 7.2 Source 流出限制

```text
hdds.container.balancer.size.leaving.source.max
```

默认 `26GB`。移动后必须满足：

```text
scheduledLeaving(source) + containerSize <= maxSizeLeavingSource
utilization(sourceUsed - scheduledLeavingAfterMove) >= lowerLimit
```

这避免 source 被过度搬空。

### 7.3 Target 流入限制

```text
hdds.container.balancer.size.entering.target.max
```

默认 `26GB`。移动后必须满足：

```text
scheduledEntering(target) + containerSize <= maxSizeEnteringTarget
utilization(targetUsed + scheduledEnteringAfterMove) <= upperLimit
```

这避免 target 被搬成新的 source。

### 7.4 参与 DN 数限制

```text
hdds.container.balancer.datanodes.involved.max.percentage.per.iteration
```

默认 `20%`。设 eligible DN 数为 `N`：

```text
maxInvolved = floor(N × ratio)
```

临近限制时，只允许已经参与过的 target；达到限制时，source 和 target 都仅能从已参与节点中选择。这样单轮不会把迁移压力扩散到全体 DN。

## 8. Candidate container 筛选

核心类：`ContainerBalancerSelectionCriteria`。

每个 source 的 containers 被缓存为 `NavigableSet`，按 `usedBytes` 降序，因此大 container 优先。但真正候选必须通过全部安全筛选。

### 8.1 include/exclude

```text
includeContainers 非空 -> 只有 include 中 container 可选
excludeContainers 非空 -> exclude 中 container 不可选
```

同一 container 在一轮只允许移动一次。Task 使用：

```text
containerToSourceMap
containerToTargetMap
```

防止重复选择。

### 8.2 容量预算

候选 container 必须：

```text
usedBytes > 0
满足 source leaving 上限
满足 iteration total move 上限
```

### 8.3 默认只允许 CLOSED、HEALTHY

默认必须满足：

```text
container lifecycle state = CLOSED
source replica state      = CLOSED
ReplicationManager health = HEALTHY
无 inflight replication/deletion
```

因此默认不会移动：

```text
OPEN
CLOSING
QUASI_CLOSED
UNDER_REPLICATED
OVER_REPLICATED
MIS_REPLICATED
MISSING
UNHEALTHY
```

原因是 Balancer 不应和 ReplicationManager 的副本修复、EC reconstruction、decommission 竞争。

### 8.4 双重 pending-op 检查

选择期调用：

```java
replicationManager.isContainerReplicatingOrDeleting(containerID)
```

MoveManager 真正调度前还会遍历 pending ops，再拒绝：

```text
ADD    -> REPLICATION_FAIL_INFLIGHT_REPLICATION
DELETE -> REPLICATION_FAIL_INFLIGHT_DELETION
```

这是针对选择与调度之间并发状态变化的双层保护。

### 8.5 non-standard containers

配置：

```text
hdds.container.balancer.include.non.standard.containers
```

默认关闭。启用后可放宽至部分：

```text
OVER_REPLICATED CLOSED
HEALTHY QUASI_CLOSED
```

但仍要求：

* 足够的 CLOSED replicas；
* QUASI_CLOSED source replica 非空；
* QUASI_CLOSED container 的 replicas 满足一致条件；
* move 前后 projected replica set 仍满足安全规则。

不会因此允许 UNDER_REPLICATED 或 MIS_REPLICATED container。

### 8.6 not-found container

`ContainerNotFoundException` 会让 container 进入跨 iteration 的 `excludeContainersNotFound`，避免无限重试。该集合上限为 `10,000`；普通暂态失败不会永久排除。

## 9. Target 的合法性检查

`AbstractFindTargetGreedy` 为 `(source, container)` 遍历 target candidates，要求：

1. target 不已有该 container replica；
2. 用 target 替换 source 后 placement policy 仍满足；
3. target 的 entering bytes 不超限；
4. target 预测利用率不超 upper limit。

placement 检查构造：

```text
current replica DNs
  - source
  + target
```

并调用：

```java
placementPolicyValidateProxy.validateContainerPlacement(...)
```

这意味着 Balancer 不会为了空间均衡破坏 rack/domain placement。

## 10. `MoveManager` 状态机

`MoveManager` 是实际 move 的控制器，并实现：

```java
ContainerReplicaPendingOpsSubscriber
```

它依靠 ReplicationManager 的 ADD/DELETE 完成事件收敛，而非仅根据“命令已发出”判定成功。

内部状态：

```text
pendingMoves:
  ContainerID -> MoveOperation

MoveOperation:
  CompletableFuture<MoveResult>
  source/target pair
  moveStartTime
```

同一 container 最多有一个 pending Balancer move。

### 10.1 move 前最终校验

`MoveManager.move(containerID, source, target)` 在 `synchronized(containerInfo)` 中检查：

```text
source HEALTHY + IN_SERVICE
target HEALTHY + IN_SERVICE
source 有 replica
target 没有 replica
container 健康
没有 pending ADD/DELETE
container CLOSED（或显式允许的 non-standard 情形）
projected replica set 健康
```

projected replica set 为：

```text
existing replicas - source replica + target replica
```

若 projected state 违反 replication health，则返回：

```text
REPLICATION_NOT_HEALTHY_AFTER_MOVE
```

因此 target 选择时已有 placement check，真正发命令前仍会做 replication-health 最终检查。

### 10.2 第一阶段：复制

MoveManager 调用：

```java
replicationManager.sendLowPriorityReplicateContainerCommand(...)
```

这是低优先级复制，不应抢占故障修复等正常 replication 工作。

命令带有：

```text
container
source
target
replicaIndex
replication deadline
```

### 10.3 第二阶段：target ADD

target DN 完成复制并上报 replica 后，ReplicationManager 完成 pending ADD。MoveManager 收到：

```text
PendingOpType.ADD
目标是 move target
```

然后调用 `handleSuccessfulAdd`，再次检查：

```text
source replica 仍存在？
source 仍 HEALTHY、IN_SERVICE？
删除 source 后 projected health 是否仍安全？
```

只有通过后，才发 delete command。复制成功不自动意味着可以删除 source。

### 10.4 第三阶段：删除 source

MoveManager 调用：

```java
replicationManager.sendDeleteCommand(...)
```

source DELETE pending op 完成后：

```text
MoveResult.COMPLETED
```

因此一个 move 的成功定义是：

```text
target ADD 已确认
且 source DELETE 已确认
```

而不只是 target 出现了额外副本。

## 11. EC 与 Ratis 的 move 语义

Balancer 同时支持 Ratis 与 EC container。

### Ratis

```text
source 的完整 replica
  -> target 的完整 replica
  -> 删除 source replica
```

### EC

以 `RS-6-3` 为例，container replicas 有 index：

```text
1..6  : data fragments
7..9  : parity fragments
```

MoveManager 从 source replica 读取其 `replicaIndex`，并将相同 index 交给 ReplicationManager：

```text
source: EC replicaIndex=8
  -> target: EC replicaIndex=8
  -> delete source index=8
```

因此 Balancer 是移动已有 EC fragment，而不是：

```text
读取 d 个 fragments
解码
重新编码
生成不同 index
```

真正缺失 fragment 的 reconstruction 属于 EC under-replication/reconstruction 流程，不属于 Balancer。

EC 场景仍需 projected health/placement 检查，因为安全性依赖不同 replica indexes 的正确集合，而非仅物理 replica 总数。

## 12. 超时、失败和取消

配置：

```text
hdds.container.balancer.move.replication.timeout
hdds.container.balancer.move.timeout
```

默认：

```text
replication timeout = 50m
move timeout        = 65m
```

语义：

```text
replication timeout: target ADD 必须在此期限前完成
move timeout:        ADD、状态传播、健康检查、source DELETE 的总期限
```

失败类型包括：

```text
REPLICATION_FAIL_NOT_EXIST_IN_SOURCE
REPLICATION_FAIL_EXIST_IN_TARGET
REPLICATION_FAIL_CONTAINER_NOT_CLOSED
REPLICATION_FAIL_INFLIGHT_DELETION
REPLICATION_FAIL_INFLIGHT_REPLICATION
REPLICATION_FAIL_TIME_OUT
REPLICATION_FAIL_NODE_NOT_IN_SERVICE
REPLICATION_FAIL_NODE_UNHEALTHY
DELETION_FAIL_TIME_OUT
DELETION_FAIL_NODE_NOT_IN_SERVICE
DELETION_FAIL_NODE_UNHEALTHY
DELETE_FAIL_POLICY
REPLICATION_NOT_HEALTHY_BEFORE_MOVE
REPLICATION_NOT_HEALTHY_AFTER_MOVE
FAIL_CONTAINER_ALREADY_BEING_MOVED
FAIL_UNEXPECTED_ERROR
```

Task 还记录 pre-move failures：

```text
PRE_MOVE_CONTAINER_NOT_FOUND
PRE_MOVE_NODE_NOT_FOUND
PRE_MOVE_REPLICA_NOT_FOUND
ITERATION_MOVE_TIMEOUT
```

iteration 等待所有 futures 至多 `moveTimeout`。超时后会取消未完成的 Java future，但这不代表已发送到 DN 的命令一定停止；DN 命令和 ReplicationManager pending-op timeout 仍会继续收敛实际 cluster state。

失败后的安全性质：

```text
复制尚未确认 -> source 仍保留，数据安全
复制成功但删除失败 -> source + target 均存在，可能临时 over-replicated
```

后者比提前删除 source 安全，后续由 ReplicationManager/GC/下一次调度收敛。

## 13. iteration 结束条件

一轮会在以下情况结束：

```text
达到 iteration max moved size
没有更多 source
source 上没有合格 container
找不到 placement 合法 target
达到 DN involved 限制且不能继续
任务被 stop 或 interrupt
```

若整轮没有产生任何 move：

```text
CAN_NOT_BALANCE_ANY_MORE
```

它不必严格表示“所有节点已经平衡”。还可能表示：

* containers 都不是 CLOSED/HEALTHY；
* containers 正在 replication/deletion；
* target 都已有 replica；
* placement policy 拒绝所有 target；
* include/exclude 限制太严；
* per-node/per-iteration bytes 限制过低；
* source/target 状态不稳定；
* EC/Ratis projected health 不允许迁移。

Balancer 在这些情况下停止，而不是强行破坏 durability 或 placement。

## 14. Metrics 和状态

`ContainerBalancerMetrics` 维护累计与 latest iteration 指标：

```text
numIterations
numDatanodesUnbalanced
numDatanodesInvolved
dataSizeUnbalancedGB

numContainerMovesScheduled
numContainerMovesCompleted
numContainerMovesTimeout
numContainerMovesFailed

dataSizeMoved
dataSizeMovedGB
```

每轮 status 还包括：

```text
iteration number
iteration result
iteration duration
scheduled/completed/failed/timeout moves
planned move bytes
actual moved bytes
每个 source 流出字节
每个 target 流入字节
失败原因
失败按 source/target DN 分布
```

`ContainerMoveFailureTracker` 按 failure reason/source/target 聚合，因此 verbose history 可以定位失败集中于哪批 datanode。

## 15. 关键配置

| 配置 | 默认值 | 含义 |
|---|---:|---|
| `hdds.container.balancer.utilization.threshold` | `10%` | 平衡带阈值 |
| `hdds.container.balancer.datanodes.involved.max.percentage.per.iteration` | `20%` | 每轮参与 DN 比例 |
| `hdds.container.balancer.size.moved.max.per.iteration` | `500GB` | 每轮总计划移动量 |
| `hdds.container.balancer.size.entering.target.max` | `26GB` | 单 target 单轮最大流入 |
| `hdds.container.balancer.size.leaving.source.max` | `26GB` | 单 source 单轮最大流出 |
| `hdds.container.balancer.iterations` | `10` | 运行轮数；`-1` 为无限 |
| `hdds.container.balancer.move.timeout` | `65m` | 单 move 总超时 |
| `hdds.container.balancer.move.replication.timeout` | `50m` | 复制阶段超时 |
| `hdds.container.balancer.balancing.iteration.interval` | 当前代码 `3m` | iteration 间等待 |
| `hdds.container.balancer.include.datanodes` | 空 | participating DN allow-list |
| `hdds.container.balancer.exclude.datanodes` | 空 | DN deny-list |
| `hdds.container.balancer.include.containers` | 空 | 只允许移动指定 containers |
| `hdds.container.balancer.exclude.containers` | 空 | 禁止移动指定 containers |
| `hdds.container.balancer.move.networkTopology.enable` | `false` | target 按 topology distance 优先 |
| `hdds.container.balancer.trigger.du.before.move.enable` | `false` | 每轮前要求 DN 刷新 usage |
| `hdds.container.balancer.include.non.standard.containers` | `false` | 放宽默认 CLOSED/HEALTHY 约束 |

配置校验还确保：

```text
per-source/per-target byte cap > SCM container size
per-source/per-target cap <= iteration total cap
replication timeout < move timeout
move timeout 留足复制完成后的删除与 report 传播时间
include/exclude 不会消除全部 participant
```

## 16. 设计取舍

Balancer 的算法是贪心近似：

```text
最满 source
  -> 最大合格 container
  -> 最空（或最近）且 placement 合格 target
```

它不是全局最优求解器，也不试图一次性最小化总网络流量或计算最佳 multi-hop migration plan。它通过多轮迭代、重新读取 usage、逐轮预算限制来逐步收敛。

安全策略则明显保守：只要 container health、placement、pending ops、DN 状态或 lifecycle 不满足要求，Balancer 宁可跳过该 container 并可能停止，也不冒数据风险。

## 17. 推荐源码阅读顺序

1. `ContainerBalancerConfiguration.java`：配置、默认值、限制。
2. `ContainerBalancerClusterAnalyzer.java`：usage 公式和 source/target 阈值。
3. `ContainerBalancer.java`：StatefulService、HA、启动停止。
4. `ContainerBalancerTask.java`：iteration 主循环、限流、metrics。
5. `ContainerBalancerSelectionCriteria.java`：container 过滤。
6. `FindSourceGreedy.java`：source 选择。
7. `AbstractFindTargetGreedy.java`：target、placement、预测 usage。
8. `FindTargetGreedyByUsageInfo.java`、`FindTargetGreedyByNetworkTopology.java`：target 排序。
9. `MoveManager.java`：复制/删除两阶段状态机。
10. `ReplicationManager.java`：低优先级复制、pending op、report 驱动完成。

## 18. 总结

Container Balancer 是一个：

```text
按容量利用率驱动
  + 受 placement、replication health、DN 状态严格约束
  + 按 iteration 限流
  + 先复制后删除
  + 通过 ReplicationManager pending-op 回调确认结果
  + 支持 Ratis replica 和 EC fragment 原 index 搬运
```

的 SCM 后台 replica 迁移器。

最关键的不变量是：**只有 target replica 已确认存在，并且删除 source 后 projected replica set 仍满足健康和 placement 要求时，才允许删除 source replica。**
