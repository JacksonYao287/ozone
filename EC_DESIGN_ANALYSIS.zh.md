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

# Apache Ozone EC 设计与实现分析

本文根据当前源码树梳理 Apache Ozone 的 Erasure Coding（EC）实现，重点包括数据模型、写路径、正常读、在线恢复（online recovery / degraded read）与离线恢复（offline recovery / persistent reconstruction）。

## 1. 总览

Ozone 的 EC 是**客户端编码、客户端读/解码、datanode 保存独立 fragment、SCM 负责放置、健康检查和持久修复调度**的架构。

```text
key
  -> 一个或多个 EC block groups
       -> 每个 group 有 d 个 data fragments 和 p 个 parity fragments
       -> 每个 fragment 位于一个 datanode，以 replicaIndex 标识
```

* `d`：一个 stripe 的 data cell 数；`p`：parity cell 数；`d+p`：完整 group 的 fragment 数。
* 写入时 client 收集 `d` 个 data cells，在 client 生成 `p` 个 parity cells，并分别写到 `d+p` 个 datanode。
* 正常读取仅访问 data fragments；fragment 缺失或读失败时，client 从至少 `d` 个可用 fragments 读取并解码。
* 在线恢复只服务于当前读取，不创建新 replica；SCM 检测到欠副本后触发离线恢复，创建持久的新 fragment。

## 2. EC 数据模型

### 2.1 `ECReplicationConfig`

源码：`hadoop-hdds/common/src/main/java/org/apache/hadoop/hdds/client/ECReplicationConfig.java`。

EC policy 格式：

```text
<codec>-<data>-<parity>-<ecChunkSize>
```

例如 `RS-6-3-1024k`：

| 字段 | 含义 |
|---|---|
| `codec` | 当前支持 Reed-Solomon (`RS`) 与 `XOR` |
| `data` / `d` | 每个 stripe 中 data cell 数 |
| `parity` / `p` | 每个 stripe 中 parity cell 数 |
| `ecChunkSize` | 一个 cell 的字节数，也是 EC 读写粒度 |
| `requiredNodes` | `d+p`，完整 EC group 所需节点数 |
| `minimumNodes` | `d`，可解码恢复的最少不同 index 数 |

以 `RS-3-2-1MiB` 为例：

```text
逻辑 stripe 容量 = 3 × 1MiB = 3MiB
物理 stripe 容量 = 5 × 1MiB = 5MiB
存储放大 = (3 + 2) / 3 = 1.67 倍
```

只要还能取得 `d=3` 个不同、有效的 fragment indexes，就可以恢复至多 `p=2` 个丢失 fragments。

### 2.2 Stripe、内部 block 和 block group

对于 `RS-3-2-1MiB`：

```text
stripe 0:
  data[0] = 用户数据 0 ~ 1MiB
  data[1] = 用户数据 1 ~ 2MiB
  data[2] = 用户数据 2 ~ 3MiB
  parity[0], parity[1] = encode(data[0..2])
```

同一 index 的 cells 在同一个内部 block 中连续存储：

```text
replicaIndex 1: data[0] stripe0, data[0] stripe1, ...
replicaIndex 2: data[1] stripe0, data[1] stripe1, ...
replicaIndex 3: data[2] stripe0, data[2] stripe1, ...
replicaIndex 4: parity[0] stripe0, parity[0] stripe1, ...
replicaIndex 5: parity[1] stripe0, parity[1] stripe1, ...
```

一个 block group 是一个逻辑 Ozone block 在 `d+p` 个 datanode 上的所有内部 blocks。它们使用相同 container ID 和逻辑/local block ID；通过 1-based `replicaIndex` 区分 fragment 身份。它不是 Ratis 的多份相同副本。

`ECBlockOutputStreamEntry` 的一个 entry 管理 `d+p` 个 `ECBlockOutputStream`，每个 stream 是指向单个 datanode 的内部 fragment writer：

```text
hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/ECBlockOutputStreamEntry.java
```

### 2.3 容量

设 `C=ecChunkSize`、`D=data`、`B=SCM block size`：

```text
一个 stripe 的逻辑容量       = D × C
一个 EC block group 的逻辑容量 ≈ D × B
```

`ECBlockOutputStreamEntry` 将单个内部 block 的长度映射为逻辑 group 长度：

```java
this.length = replicationConfig.getData() * b.getLength();
```

OM 预分配 blocks 时也考虑 `data`：

```text
numBlocks = ceil(requestedSize / (scmBlockSize × data))
```

相关代码：`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyRequest.java`。

## 3. 控制面：OM、SCM 和 Pipeline

EC replication config 可由 key 创建请求显式指定，或从 bucket/集群默认 replication config 继承；创建后被持久化在 key metadata 中，bucket policy 的后续修改不会重写已有 key。

`ECPipelineProvider` 一次选择 `d+p` 个 datanode，并顺序分配固定的 replica indexes：

```text
node[0] -> index 1
node[1] -> index 2
...
node[d+p-1] -> index d+p
```

源码：`hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/pipeline/ECPipelineProvider.java`。

EC pipeline 与 Ratis pipeline 的差别：

| Ratis | EC |
|---|---|
| 多个节点存储同一 block | 各节点存储不同 fragment |
| leader/follower 复制 | client 直写各 fragment DN |
| 可读任意健康副本 | 正常读 data；故障读 data+parity 解码 |

对写缓存，OM 要求 EC pipeline 至少具备全部 data indexes `1..d`；否则缓存失效，避免继续向不完整 pipeline 写入。实现位于 `ScmClient#shouldInvalidate`。

## 4. 编码器层

编码器模块：

```text
hadoop-hdds/erasurecode/
  CodecRegistry
  rawcoder/RawErasureEncoder
  rawcoder/RawErasureDecoder
  rawcoder/RSRawEncoder, RSRawDecoder
  rawcoder/XORRawEncoder, XORRawDecoder
  rawcoder/NativeRS*, NativeXOR*
  rawcoder/util/CodecUtil
```

`CodecUtil.createRawEncoderWithFallback` 和 `createRawDecoderWithFallback` 通过 `ServiceLoader` 选择 coder factory，优先 native coder，不可用时回退 Java coder。encoder/decoder 关闭时调用 `release()` 释放可能的 native 资源。

同一个 raw coder 层由三条路径复用：

```text
EC 写入       -> encoder.encode(...)
在线恢复读    -> decoder.decode(...)
离线恢复      -> decoder.decode(...)
```

## 5. EC 写路径

### 5.1 调用链

```text
RpcClient
  -> ECKeyOutputStream
     -> ECBlockOutputStreamEntryPool
        -> ECBlockOutputStreamEntry
           -> d+p 个 ECBlockOutputStream
              -> 单节点 pipeline
                 -> datanode WriteChunk / PutBlock
```

核心源码：

```text
hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/ECKeyOutputStream.java
hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/ECBlockOutputStreamEntry.java
hadoop-hdds/client/src/main/java/org/apache/hadoop/hdds/scm/storage/ECBlockOutputStream.java
```

### 5.2 client 编码的原因

client 是唯一自然拥有完整 stripe 的 `d` 个 data cells 的角色；每个 datanode 仅拥有自己的单一 fragment。若在 DN 侧编码，就需要额外的 DN 间汇聚、同步和传输协议。

当前写入模型为：

```text
client -> data DN 0: data cell 0
client -> data DN 1: data cell 1
...
client -> parity DN 0: parity cell 0
client -> parity DN 1: parity cell 1
```

不是 primary DN 转发的复制写模型。

### 5.3 Stripe buffer 和异步 flush

`ECKeyOutputStream` 维护 data/parity `ByteBuffer[]` 与一个有界 `ArrayBlockingQueue<ECChunkBuffers>`。

应用线程按顺序填充 data cells。当第 `d` 个 data cell 填满：

1. `generateParityCells()` 填充/补零并调用 raw encoder；
2. 形成完整 stripe，放入 flush queue；
3. 为下一 stripe 分配 buffers；
4. 后台 flush worker 写出上一 stripe。

worker 对每个 stripe 的步骤：

```text
writeDataCells
  -> 写 replica indexes 1..d
writeParityCells
  -> 写 replica indexes d+1..d+p
commitStripeWrite
  -> 验证全部 WriteChunk future
  -> 对全部 fragments 执行 PutBlock
  -> 验证全部 PutBlock future
  -> 更新已 ACK 的逻辑 group 长度
```

`ozone.client.ec.stripe.queue.size` 是 producer/worker 的背压点；flush thread 的异常通过 `flushFuture` 传播回调用者。

### 5.4 Partial stripe

假设 `RS-3-2-1MiB` 的最后 stripe 只包含 1.5MiB：

```text
data[0] = 1MiB
data[1] = 0.5MiB
data[2] = 0
```

`generateParityCells()` 找到第一个未满 cell，对编码需要的部分补零，编码后恢复 data cells 的真实 limits：实际不存在用户数据的后续 data cells limit 为 0。padding 是编码输入，不被当作真实用户数据写入这些 data fragments；parity 的长度由首个有效 data cell 决定。

### 5.5 内部单节点 streams

`ECBlockOutputStreamEntry` 对每个 index 创建仅含一个 DN 的 pipeline。它保留 EC config 和 replica index，但真实 WriteChunk 是 client 到对应 DN 的直接请求。

### 5.6 `PutBlock`、逻辑长度和 checksum

每个内部 block 的 `PutBlock` 包含：

```text
BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK
```

即逻辑 block group length。最后 partial stripe 中各内部 blocks 的物理长度不同，不能只靠单个内部 block 长度推导用户可见长度。

当前实现将 stripe checksum 主要保存于第一个 data replica 和 parity replicas，避免每个 data fragment 重复保存。离线恢复会从 source `BlockData` 取得并写回正确 checksum metadata。

### 5.7 写失败和重试

任一 fragment 的 WriteChunk 或 PutBlock 失败，整个 stripe 失败：

1. 收集失败 streams；
2. 排除失败 pipeline；适用时也排除失败 DN；
3. 回滚本 stripe 对逻辑 offset 的贡献；
4. 重置到最后 ACK 的位置；
5. 丢弃旧 pipeline 上无价值的预分配 block groups；
6. 关闭失败 entry；
7. 在新 pipeline/block group 重写完整 stripe。

最大重试由 `ozone.client.ec.stripe.write.max.retries` 控制。

必须以 stripe 重试，而非只补一个 fragment：parity、chunk 边界、checksum、PutBlock metadata 和提交边界都以完整 stripe 为单位；部分成功写可能留下 orphan/dirty data，不能当作已提交数据。

### 5.8 close 和同步语义

`ECKeyOutputStream#close()` 对最后 partial stripe 编码、发送 EOF 给 worker、等待所有 stripe 成功、校验接收字节数与已确认字节数一致，再向 OM commit key。

当前实现：

```text
flush()  不提供 durable EC flush 语义
hflush() 不支持
hsync()  不支持
```

这是有意的语义边界：可靠提交单位是完整、编码完成并在全部 fragments 得到确认的 stripe。任意位置同步需要额外定义 partial stripe 的 parity、padding 和崩溃恢复语义。

## 6. 正常 EC 读取

### 6.1 Reader 选择

`ECBlockInputStreamProxy` 选择：

```text
所有预期 data locations 可用，且无已知失败 DN
  -> ECBlockInputStream

缺少预期 data location，或正常读失败
  -> ECBlockReconstructedInputStream
     -> ECBlockReconstructedStripeInputStream
```

对于短 block group，预期 data location 数不是固定 `d`，而是：

```text
min(ceil(blockLength / ecChunkSize), d)
```

所以一个只有 1.2 cells 数据的 `RS-3-2` group 只预期前两个 data indexes；第三个是逻辑 padding，不应被误判为缺失。

### 6.2 逻辑 offset 到内部 block offset

正常读当前 data index：

```text
currentDataIndex = (position / cellSize) mod d
```

逻辑 position 在该内部 block 的偏移：

```text
internalOffset = floor(position / stripeSize) × cellSize
                 + (position mod cellSize)
```

EC client 因此必须自行实现 stripe-interleaved 逻辑 key 到连续内部 block stream 的映射。

### 6.3 复用普通 BlockInputStream

`ECBlockInputStream#getOrOpenStream()` 为某个 replica index 建立临时 standalone pipeline：

```text
EC fragment DN -> StandaloneReplicationConfig(ONE) -> 普通 BlockInputStream
```

它复用普通 DN block read 协议，同时将 EC index/stripe 映射逻辑留在 client。

### 6.4 顺序读、并行读与 spare locations

正常路径只读取 data fragments：

```text
data[0] -> data[1] -> ... -> data[d-1] -> 下一 stripe
```

启用 `ozone.client.ec.parallel.read.enabled` 后，一个跨越多个 cells 的 read 可按 data index 并行：同一 index 内 cells 保序，不同 index 并行，最多 `d` 个任务；失败时只推进到第一个失败 cell 前的连续成功数据。

若同一个 replica index 存在多个 locations，reader 维护当前 source 与备用 source 队列。读失败先切换同 index spare DN；所有 spare 耗尽才抛 `BadDataLocationException` 给 proxy，进入 reconstruction。

## 7. Online Recovery：在线降级读取

在线恢复是 read path 的临时解码能力：不创建新 fragment，不更新 SCM metadata。

### 7.1 触发与 failover

触发条件：

1. 初始 pipeline 中缺少预期 data location；
2. 正常读取中，某 index 的当前和备用 source 均失败。

`ECBlockInputStreamProxy` 在失败时：

```text
记录 failed locations
关闭 normal reader
创建 reconstruction reader
seek 回失败前 position
重置调用者 ByteBuffer
用 reconstruction reader 重读
```

对应用透明。如果 reconstruction reader 自身失败，异常直接向上返回。

### 7.2 Stripe reader 和 decoder 合约

`ECBlockReconstructedStripeInputStream` 是核心类：

```text
readStripe(ByteBuffer[] dataBuffers)       # 在线读完整 data stripe
recoverChunks(ByteBuffer[] outputBuffers) # 离线恢复指定 indexes
```

decoder 输入数组固定为 `d+p`，采用 0-based index：

```text
0..d-1       data
 d..d+p-1    parity
```

container 协议使用 1-based `replicaIndex`，因此：

```text
decoderIndex = replicaIndex - 1
```

可读 fragment 的 ByteBuffer 放在其 decoder index；不可读或不需要的输入为 `null`；`erasedIndexes` 表明需要生成的 indexes：

```java
decoder.decode(decoderInputBuffers, erasedIndexes, decoderOutputBuffers);
```

### 7.3 输入选择与重试

在线恢复并不读取全部 `d+p` fragments。它先使用可用 data indexes，只在 data 不足时选择恰好所需数量的 parity inputs，目标是得到 `d` 个独立 inputs。

source read 也可能失败。reader 将失败 index 记入 `failedDataIndexes`，回到当前 stripe 起点，重新选择 data/parity inputs 后重试；最终可选 inputs 少于所需 data count 时抛 `InsufficientLocationsException`。

### 7.4 Partial stripe

最后 partial stripe 中后续 data cells 天然不存在。reconstructed reader 根据 block group length：

1. 设置实际 read limits；
2. 为 decoder 所需的未写 data cells 注入零；
3. 保持 parity buffer 与首 data cell 的有效长度一致；
4. decode；
5. 去除 padding 对调用者可见长度的影响。

核心方法：`setBufferReadLimits`、`padBuffers`、`zeroFill`、`decodeStripe`。

在线恢复的性质：

| 维度 | 行为 |
|---|---|
| 目标 | 当前请求可读 |
| 集群副作用 | 无 |
| 网络/CPU | 读取多个 inputs 并解码，成本高于正常读 |
| 可用条件 | 至少 `d` 个不同、可读 indexes |
| 持久修复 | 不负责，由 SCM offline recovery 负责 |

## 8. Offline Recovery：持久化重建

端到端链路：

```text
DN container report / node status
  -> SCM EC health check
  -> ReplicationManager 队列
  -> ECUnderReplicationHandler
  -> target placement、source 过滤与限流
  -> ReconstructECContainersCommand
  -> 某个 target DN 作为 coordinator
  -> ECReconstructionCoordinator
  -> 创建 RECOVERING containers
  -> 逐 block group、逐 stripe 解码并写入 targets
  -> PutBlock、close containers、DN report
  -> SCM 观察到完整 replicas
```

### 8.1 按 index 的健康判断

实现：

```text
hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/replication/ECContainerReplicaCount.java
hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/replication/health/ECReplicationCheckHandler.java
```

以 `RS-3-2` 为例：

```text
1 2 _ 4 5
```

尽管有四个物理 replicas，index 3 缺失，仍是 under-replicated。反之，index 1 有两份、index 5 缺失时，可同时 over-replicated 和 under-replicated。

`ECContainerReplicaCount` 分别追踪 healthy、unhealthy、decommission、maintenance、pending add、pending delete 等 index 集。

### 8.2 可恢复不代表完全健康

```text
可恢复：至少 d 个不同 indexes 可用
完全健康：全部 d+p indexes 都存在并满足状态/placement 要求
```

`isUnrecoverable()` 在健康不同 index 数小于 `d` 时为真；`isSufficientlyReplicated()` 则要求完整 `d+p` index 集。因此某 container 可以仍可 online read，却处于 under-replicated 并等待 offline repair。

### 8.3 不同修复场景

`ECUnderReplicationHandler` 区分：

1. **真正缺失 index**：该 index 不存在可用副本，必须 EC reconstruction。
2. **decommission index**：原副本仍可读，通常用 1:1 copy；若 source 高负载且有足够其他 sources，也可 reconstruction。
3. **maintenance-only index**：按 maintenance redundancy 决定是否复制到 in-service node。

所以需要一个新 index 并不必然需要 decode；原 index 尚可读时直接复制更低成本。

### 8.4 source、target 和限流

source 必须是 CLOSED 且健康可达的 replica；排除 pending delete、stale/dead nodes；同 index 多副本时优先 IN_SERVICE。重建前提为：

```text
可用不同 source indexes >= d
```

target 由 placement policy 选择，排除现有 replica nodes 和过载/不可用 nodes。一个重建命令可以携带多个 missing indexes 与对应 targets。

`ReplicationManager#sendThrottledReconstructionCommand` 同时限制：

* 全集群 reconstruction slots；
* target DN 的 replication/reconstruction queue capacity。

它从 targets 中选择负载较低的 DN 接收命令。该 DN 是 coordinator：从所有 sources 读取，向全部 targets 写入，并非只修复命令接收者。

### 8.5 DN 侧执行

```text
ReconstructECContainersCommandHandler
  -> ECReconstructionCommandInfo
  -> ECReconstructionCoordinatorTask
  -> ReplicationSupervisor.addTask
  -> ECReconstructionCoordinator
```

恢复在 replication supervisor 后台任务中执行，而不是 heartbeat 线程。

### 8.6 Coordinator 完整算法

`ECReconstructionCoordinator` 的流程：

```text
1. 对 source replicas ListBlock
2. 按 local block ID 聚合为 BlockData[d+p]
3. 计算每个 block group 的安全逻辑长度
4. 在 target DNs 创建 RECOVERING containers
5. 对每个有效 block group：
     a. 确定真正需要恢复的 indexes
     b. 创建 ECBlockReconstructedStripeInputStream
     c. 设置 recovery indexes
     d. 每个 stripe 读 source、decode
     e. 将 recovered cells 写入 targets
     f. 在各 target 上 PutBlock
6. 关闭所有 recovered containers
```

#### Orphan/abandoned block 过滤

写失败时部分 DN 可能拥有未被完整确认的 WriteChunk/PutBlock。`getBlockDataMap()` 要求每个候选 block group 在每一个选定 source index 都有 `BlockData` 条目；否则当作 orphan/abandoned stripe，跳过重建，避免把未提交数据放大为有效数据。

#### 安全长度

sources 报告的 `blockGroupLength` 可能不同；恢复使用所有可见值的最小值：

```text
safeBlockGroupLength = min(source blockGroupLength)
```

这样不会将任一 source 的脏尾部复制到 recovered replica。

#### `RECOVERING` 生命周期

target 先通过 `createRecoveringContainer` 创建恢复容器，成功写完所有 blocks 后再 `closeContainer`。若失败，coordinator 仅删除本任务创建的 `RECOVERING` 或 `UNHEALTHY` containers，避免半成品被当作正常副本或误删已有健康副本。

#### 解码和写回

coordinator 使用与 online recovery 相同的 `ECBlockReconstructedStripeInputStream`，但调用：

```java
sis.setRecoveryIndexes(toReconstructIndexes - 1);
sis.recoverChunks(bufs);
```

减一是从 1-based replica index 转换为 0-based decoder index。每个恢复出的 cell 被写到其对应 target 的 `ECBlockOutputStream`。

对于短 block group，某些 data indexes 本就是 padding。它们不需要 decode 或 WriteChunk，但仍需建立 stream 并执行空 `PutBlock`，以使 fragment metadata 完整一致。

恢复 PutBlock 会传播 block group length 和适当 stripe checksum；成功后 DN report 新 CLOSED replica，SCM 完成 pending ADD 并重新判定 container 健康。

## 9. Online 与 Offline Recovery 的比较

| 维度 | Online recovery | Offline recovery |
|---|---|---|
| 目标 | 当前读请求成功 | 补齐持久冗余 |
| 触发者 | client read path | SCM ReplicationManager |
| 时机 | location 缺失或读失败 | health scan、节点失效、decommission/maintenance |
| 算法 | 读 d 个 inputs 并 decode | 相同 reader/decoder |
| 输出 | 应用读取 buffer | target DN 的持久 fragment |
| 创建 replica | 否 | 是 |
| SCM 状态 | 不变 | 经 pending ops 和 DN reports 更新 |
| 资源成本 | 降级读延迟和带宽高 | 后台读、decode 和写负载 |
| 失败边界 | 少于 d 个有效 inputs | 少于 d 个健康 source indexes |

## 10. 典型状态：RS-3-2

```text
完整：      1 2 3 4 5
丢 data 2： 1 _ 3 4 5  -> 可 online decode，SCM offline rebuild 2
丢 2 和 5： 1 _ 3 4 _  -> 仍有 3 个 inputs，可同时重建多个 indexes
丢 1、2、5：_ _ 3 4 _  -> 仅 2 个 inputs，少于 d=3，不可恢复
```

## 11. 性能和运维含义

* 正常读仅访问 data fragments，开销最低；降级读需要多源读取和 decode，因此更慢。
* 每 `d × cellSize` 用户数据都需额外生成/写入 `p × cellSize` parity；写必须确认 `d+p` 个 fragments。
* EC 适合大对象、吞吐导向、冷/温数据；不适合高频小随机写、极端低延迟负载、依赖任意位置 hsync 的写入模式。
* 集群至少需 `d+p` 个 datanodes；生产上应跨足够多机架分布 fragments，避免单机架失效损失超过 `p` 个 indexes。
* `ECPipelineProvider` 仍有 EC 专用 rack-aware placement 的 TODO，因此实际拓扑和 placement policy 配置很重要。

## 12. 关键配置和源码阅读顺序

常用配置：

| 配置 | 作用 |
|---|---|
| `ozone.client.ec.stripe.queue.size` | client 到 flush worker 的 stripe 队列大小 |
| `ozone.client.ec.stripe.write.max.retries` | 单 stripe 最大重试次数 |
| `ozone.client.ec.parallel.read.enabled` | 正常 EC read 是否并行读多个 data indexes |
| `ozone.client.ec.reconstruct.stripe.read.pool.limit` | reconstruction read 线程池上限 |
| `ozone.client.ec.reconstruct.stripe.write.pool.limit` | reconstruction write 线程池上限 |
| `ozone.client.read.timeout` | source DN 读取通信超时 |

推荐源码阅读顺序：

1. `ECReplicationConfig`：策略和基本公式。
2. `ECPipelineProvider`：DN 选择和 replica indexes。
3. `ECKeyOutputStream`：stripe buffer、编码、写、重试与 commit。
4. `ECBlockOutputStreamEntry`、`ECBlockOutputStream`：内部 fragment 写和 PutBlock。
5. `ECBlockInputStreamProxy`、`ECBlockInputStream`：正常读、spare、failover。
6. `ECBlockReconstructedStripeInputStream`：在线/离线共用的 stripe 解码核心。
7. `ECContainerReplicaCount`、`ECReplicationCheckHandler`：按 index 的健康模型。
8. `ECUnderReplicationHandler`、`ReplicationManager`：SCM 的恢复调度和节流。
9. `ECReconstructionCoordinator`：DN 侧持久重建全过程。
