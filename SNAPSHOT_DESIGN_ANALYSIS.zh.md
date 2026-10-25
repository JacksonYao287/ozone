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

# Apache Ozone Snapshot 设计与实现分析

本文依据当前源码树，说明 Ozone 用户级 Snapshot 的数据模型、创建、读取、删除、垃圾回收、SnapshotDiff 与 defragmentation 实现。

> 本文的 **Ozone Snapshot** 是 bucket 的用户数据快照。它不同于 OM HA 中用于 follower 同步状态的 **Ratis snapshot**。

## 1. 总览

Ozone Snapshot 是 bucket 级、时间点一致、只读的 namespace 视图：

```text
/volume/bucket
  ├── key-a
  ├── dir/key-b
  └── .snapshot/
       ├── snap-1/
       └── snap-2/
```

访问 Snapshot 的逻辑路径是：

```text
/volume/bucket/.snapshot/<snapshot-name>/<key-path>
```

其核心设计不是复制 bucket 数据，而是：

```text
immutable datanode blocks 不复制
  + OM RocksDB checkpoint 冻结 bucket metadata
  + Snapshot chain 和 deleted-table 迁移保护旧 blocks
  + 后台服务异步清理无引用的 metadata 与 blocks
```

所以：

* 创建 Snapshot 通常是 O(1) 级别的 checkpoint/hard-link 操作；
* Snapshot 保留的是 key/file/directory metadata 的历史视图；
* datanode block 不因创建 Snapshot 而复制；
* live bucket 删除或覆盖的数据，只要仍被某个 active Snapshot 引用，就不能真正回收；
* Snapshot 删除是异步流程，不是立刻删除 checkpoint 和 blocks；
* Snapshot 没有自动 rollback；恢复通常是从 `.snapshot` 路径手动 copy，或使用 DistCp/SnapshotDiff。

主要源码目录：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/
  OmSnapshotManager.java
  OmSnapshot.java
  SnapshotChainManager.java
  snapshot/
  request/snapshot/
  response/snapshot/
  service/SnapshotDeletingService.java
```

## 2. Snapshot 的边界和用户语义

Snapshot 的粒度是 **bucket**，而不是任意目录。用户可以：

```bash
ozone sh snapshot create /vol1/bucket1 snap-1
ozone sh snapshot list /vol1/bucket1
ozone sh snapshot delete /vol1/bucket1 snap-1
ozone sh key get /vol1/bucket1/.snapshot/snap-1/report.csv ./report.csv
```

Snapshot 的保证：

1. Snapshot 创建点可见的 key/file/directory metadata 在 Snapshot 内保持可读。
2. 此后 live bucket 的写、删、覆盖和 rename 不会改变该 Snapshot 的 namespace 视图。
3. `.snapshot` 下的对象只读。
4. Snapshot 适用于 Ratis 和 EC bucket，因为它冻结的是 OM 元数据；key 的 block location metadata 仍描述原有 replication config。
5. bucket ACL、quota 等 bucket 属性并不是 Snapshot 中的历史版本对象；核心版本化对象是 namespace 和 key metadata。

限制：

* `.snapshot` 是 bucket 根目录的保留名称；
* Snapshot 管理 API 不经 S3/s3a 暴露；
* Snapshot 读取可以通过 `.snapshot/<name>/...` 路径访问；
* Snapshot 数量过多会增加 OM 的 RocksDB、FD、内存、清理和 listing 压力；
* Snapshot 删除后空间回收异步发生。

## 3. 元数据模型：`SnapshotInfo`

核心类：

```text
hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/helpers/SnapshotInfo.java
```

SnapshotInfo 存在 active OM RocksDB 的 `snapshotInfoTable`。其 table key 是：

```text
/volume/bucket/snapshot-name
```

关键字段：

| 字段 | 含义 |
|---|---|
| `snapshotId` | UUID；内部稳定身份 |
| `name` | 用户可见 Snapshot 名称 |
| `volumeName`、`bucketName` | 所属 bucket |
| `snapshotStatus` | `SNAPSHOT_ACTIVE` 或 `SNAPSHOT_DELETED` |
| `creationTime`、`deletionTime` | 创建、逻辑删除时间 |
| `snapshotPath` | `/volume/bucket`，即 path chain 键 |
| `pathPreviousSnapshotId` | 同一 bucket 的前一个 Snapshot |
| `globalPreviousSnapshotId` | 全局创建序列中的前一个 Snapshot |
| `createTransactionInfo` | 创建事务的 OM Ratis term/index |
| `lastTransactionInfo` | 最近状态改变的 OM Ratis term/index |
| `deepClean` | 是否需要重新 key deep clean |
| `deepCleanedDeletedDir` | deleted-directory deep clean 状态 |
| `sstFiltered` | Snapshot SST filtering 状态 |
| `referencedSize` | 创建时引用的估算逻辑数据大小 |
| `referencedReplicatedSize` | 创建时引用的复制后数据大小 |
| `exclusiveSize` | key deep clean 发现的 Snapshot 独占数据 |
| `exclusiveReplicatedSize` | 上述数据的复制后大小 |
| `exclusiveSizeDeltaFromDirDeepCleaning` | directory deep clean 独立累计的独占大小 |
| `exclusiveReplicatedSizeDeltaFromDirDeepCleaning` | 对应复制后大小 |

真实独占空间为：

```text
exclusiveSize + exclusiveSizeDeltaFromDirDeepCleaning
```

目录 deep cleaning 使用独立 delta 字段，避免它与 key deep cleaning 这两个异步服务并发更新同一 SnapshotInfo 时产生覆盖。

## 4. Snapshot chain

实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/SnapshotChainManager.java
```

系统维护两套内存双向链，均从持久化的 `snapshotInfoTable` 重建。

### 4.1 Global chain

按全局创建顺序：

```text
S1(bucket-A) -> S2(bucket-B) -> S3(bucket-A) -> S4(bucket-C)
```

用途：

* 全局顺序后台处理；
* Snapshot deletion service 的遍历；
* Snapshot defragmentation 顺序扫描；
* 全局最新/最旧 Snapshot 定位。

持久化前驱字段为 `globalPreviousSnapshotId`。

### 4.2 Path chain

按单个 bucket：

```text
bucket-A: S1 -> S3 -> S5
bucket-B: S2 -> S4
```

用途：

* 查找同 bucket 前后 Snapshot；
* Snapshot deletion 时转移 delete/rename metadata；
* deep clean；
* SnapshotDiff；
* incremental defragmentation。

持久化前驱字段为 `pathPreviousSnapshotId`。

### 4.3 完整性

OM 启动会遍历 SnapshotInfo，验证：

* Snapshot ID 无重复；
* 前驱存在；
* 一个节点不会有两个不同后继；
* global/path chain membership 一致；
* 所有 Snapshot 都可通过链遍历加入。

如果重建失败，`snapshotChainCorrupted` 被置为真，依赖该链的操作会失败而不是继续做危险 GC。

## 5. 为什么创建很快：RocksDB checkpoint

创建 checkpoint 的核心方法：

```text
OmSnapshotManager.createOmSnapshotCheckpoint(...)
```

它通过 active OM RocksDB 的 `RDBStore#getSnapshot(...)` 建立 checkpoint。初始 checkpoint 通常通过 SST hard links 共享文件，而不是复制整个 DB。

```text
Active OM RocksDB SST files
  ├── hard link -> Snapshot S1 checkpoint
  ├── hard link -> Snapshot S2 checkpoint
  └── 持续写入/compaction -> 新 SST
```

后续 active DB compaction 会使长期 Snapshot 与 AOS 使用不同 SST 集。这个特性同时解释了：

| 现象 | 原因 |
|---|---|
| 创建 Snapshot 快 | 基于 checkpoint 和 hard link |
| 长期 Snapshot 占用增长 | compaction/write 造成 SST 分叉 |
| Snapshot 不能任意自动 compact | 会影响 SnapshotDiff 的 SST/历史分析 |

## 6. 创建 Snapshot 的完整路径

处理器：

```text
OMSnapshotCreateRequest
  -> OMSnapshotCreateResponse
  -> snapshotInfoTable + RocksDB checkpoint
```

源码：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/snapshot/OMSnapshotCreateRequest.java
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/response/snapshot/OMSnapshotCreateResponse.java
```

### 6.1 `preExecute`

leader 执行：

1. 校验 Snapshot 名称；
2. 将 linked bucket 解析成真实 volume/bucket；
3. 校验调用者是 Ozone admin 或 bucket owner；
4. 检查 Snapshot 数量限制；
5. 写入 leader 生成的 UUID 和 creation time。

UUID、时间戳和真实 bucket 名称被写进 Ratis 请求，以确保 follower 重放的是确定性命令。

默认名称形如：

```text
sYYYYMMDD-HHmmss.SSS
```

### 6.2 锁

创建时获取：

```text
BUCKET_LOCK    : read lock
SNAPSHOT_LOCK  : volume/bucket/snapshotName 的 write lock
```

前者阻止 bucket 在创建中被删除；后者序列化同名 Snapshot 的创建、删除、rename。

### 6.3 初始化 SnapshotInfo

创建时写入：

```text
createTransactionInfo = 当前 OM Ratis term/index
lastTransactionInfo   = 当前 OM Ratis term/index
```

`referencedReplicatedSize` 直接取 bucket 的 `usedBytes`。`referencedSize` 是根据 bucket 默认 replication config 由 used bytes 反推的估算值，不扫描所有 key；这是创建 Snapshot 保持 O(1) 的设计取舍。每个 key 可有不同 replication config，因此该逻辑大小并不保证精确。

### 6.4 原子更新 chain 和 cache

创建过程在 `SnapshotChainManager` 对象上同步：

1. 当前 bucket 最新 Snapshot 成为 `pathPreviousSnapshotId`；
2. 当前全局最新 Snapshot 成为 `globalPreviousSnapshotId`；
3. 新 Snapshot 加入内存 global/path chains；
4. SnapshotInfo 加入 `snapshotInfoTable` cache。

若 cache 更新失败，代码删除刚加入的 chain 节点。否则可能出现后继指向一个不在 SnapshotInfo table 中的前驱，导致 OM 重启后链无法重建。

### 6.5 double buffer、持久化和 checkpoint

OM 请求路径概念上是：

```text
Ratis apply
  -> validateAndUpdateCache
  -> OM double buffer
  -> active OM RocksDB batch
```

`OMSnapshotCreateResponse#addToDBBatch`：

1. 向 active DB 的 `snapshotInfoTable` batch 写 SnapshotInfo；
2. 创建 Snapshot checkpoint；
3. 清理 active DB 中该 bucket 的部分 deleted/rename tracking entries。

源码注释强调 SnapshotInfo 写入和 checkpoint 创建的顺序，避免 checkpoint differ 在边界 compaction 时遗漏 DAG tracking。最坏情况下影响的是 Diff 优化性能，不是 Snapshot 内容正确性。

### 6.6 创建后删除 AOS 部分 deleted tables

checkpoint 建成后，`createOmSnapshotCheckpoint` 会针对该 bucket 清理 AOS 的：

```text
deletedTable
deletedDirTable
snapshotRenamedTable
```

原因不是直接丢弃这些信息：它们已经被 checkpoint 捕获，并将由 Snapshot chain 的迁移机制继续保护。后续 Snapshot 删除时，删除/rename history 会移动到下一个 active Snapshot 或 AOS，直至安全回收。

## 7. Snapshot 的物理目录和本地 YAML

checkpoint 常位于：

```text
db.snapshots/checkpointState/
```

目录名：

```text
om.db-<snapshot-id>             # version 0，初始 checkpoint
om.db-<snapshot-id>-<version>   # version > 0，defrag 后的 checkpoint
```

每个 Snapshot 还有本地 YAML：

```text
om.db-<snapshot-id>.yaml
```

相关实现：

```text
OmSnapshotLocalData.java
OmSnapshotLocalDataYaml.java
OmSnapshotLocalDataManager.java
```

YAML 不是 Ratis replicated `SnapshotInfo` 的一部分，而是每个 OM 的本地状态。它记录：

```text
snapshotId
version
previousSnapshotId
versionSstFileInfos
dbTxSequenceNumber
needsDefrag
lastDefragTime
transactionInfo
isSSTFiltered
checksum
```

使用本地 YAML 的原因：

* defrag 版本是每个 OM 本地磁盘状态；
* 各 OM 不一定在相同时间执行 defrag；
* Snapshot RocksDB 目录版本不是用户可见的集群逻辑状态；
* SnapshotDiff 和旧版本清理需要本地 SST dependency 信息。

`OmSnapshotLocalDataManager` 将 `(snapshotId, version)` 维护为内存 dependency DAG：

```text
(S2, v4) -> (S1, v3)
```

这个关系用于防止仍被后继 Snapshot version 依赖的旧 checkpoint version 被删除。

## 8. Snapshot 读路径

入口：

```text
OmSnapshotManager.getActiveFsMetadataOrSnapshot(...)
```

它检查 key path 是否以：

```text
.snapshot/<snapshotName>/...
```

开头。

```text
普通 key path
  -> active OM metadata reader

.snapshot/<name>/path
  -> 校验 Snapshot ACTIVE
  -> SnapshotCache 获取 OmSnapshot
  -> Snapshot checkpoint DB reader
```

### 8.1 `OmSnapshot`

实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OmSnapshot.java
```

`OmSnapshot` 基于 Snapshot checkpoint 创建：

```text
Snapshot OMMetadataManager
  -> Snapshot KeyManagerImpl
  -> Snapshot PrefixManagerImpl
  -> OmMetadataReader
```

因此复用了常规 OM metadata 查询逻辑：

* lookup key；
* get key info；
* list keys；
* list status；
* get file status；
* ACL 查询；
* key/bucket tagging；
* FSO namespace 查询。

区别只在于 metadata manager 使用 Snapshot RocksDB，而不是 active OM RocksDB。

### 8.2 路径转换

外部路径：

```text
.snapshot/snap-1/dir/file
```

Snapshot DB 内部使用的相对路径：

```text
dir/file
```

`OmSnapshot` 会：

```text
请求 -> normalize：去掉 .snapshot/<name>/
结果 -> denormalize：加回 .snapshot/<name>/
```

这让 Snapshot 对 OFS/FS 客户端表现为真实的只读目录树，而无需改变快照数据库中的 key names。

## 9. SnapshotCache 和资源管理

实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/snapshot/SnapshotCache.java
```

打开一个 Snapshot RocksDB 会占用 RocksDB handle、SST、file descriptors 和 OM 内存。SnapshotCache 维护：

```text
UUID -> ReferenceCounted<OmSnapshot>
```

工作方式：

1. `get(snapshotId)` 原子加载或复用 Snapshot DB；
2. 引用计数加一；
3. 调用者关闭返回的 supplier 后计数减一；
4. 引用计数为零的对象进入 pending eviction queue；
5. 定期或超过 soft cache limit 时清理无引用的 Snapshot DB；
6. 清理前可选 compact 非 SnapshotDiff DAG 追踪的 tables；
7. 不会关闭仍被引用的 Snapshot DB。

因此所有调用方必须 close 通过 cache 获取的 `OmSnapshot` handle。否则会造成：

```text
Snapshot DB 永不释放
  -> FD 持续增长
  -> cache 无法回收
  -> defrag/purge 受阻
  -> OM 可能耗尽 nofile
```

`OmSnapshot` 自己还挂接 leak detector，GC 时发现未 close 会告警。

## 10. 并发控制

Snapshot 相关锁有不同职责：

| 锁 | 用途 |
|---|---|
| `SNAPSHOT_LOCK` | SnapshotInfo 创建、删除、rename 等逻辑操作 |
| `SNAPSHOT_DB_LOCK` | Snapshot DB handle 打开、关闭、失效、目录删除 |
| `SNAPSHOT_DB_CONTENT_LOCK` | 修改 Snapshot DB 内容，如 tombstone move、defrag |
| `SNAPSHOT_GC_LOCK` | Snapshot GC 与 chain 移动 |
| `SNAPSHOT_LOCAL_DATA_LOCK` | YAML 与本地 version dependency DAG |
| `BOOTSTRAP_LOCK` | OM bootstrap 与 Snapshot 内容一致性 |

例如，SnapshotCache 的读取在整个 DB handle 生命周期内持有该 Snapshot 的 `SNAPSHOT_DB_LOCK` read lock。删除 checkpoint directory 或切换 defrag version 必须取得 write lock 并 drain cache，从而不删除仍在被读线程使用的目录。

## 11. Snapshot 删除：逻辑删除与物理 purge 分离

完整流程：

```text
DeleteSnapshot
  -> SnapshotInfo 标为 SNAPSHOT_DELETED
  -> SnapshotDeletingService 移动 deleted/rename entries
  -> SnapshotPurge 更新 chain、移除 SnapshotInfo
  -> 删除 checkpoint/YAML/旧 version 目录
  -> 无 Snapshot/live 引用的 blocks 才能真正回收
```

### 11.1 逻辑删除

处理器：`OMSnapshotDeleteRequest`。

它获取 bucket write lock 和 Snapshot write lock，检查 Snapshot 为 `SNAPSHOT_ACTIVE`，然后设置：

```text
snapshotStatus = SNAPSHOT_DELETED
deletionTime = leader 确定的时间
lastTransactionInfo = 当前 Ratis term/index
```

此后用户不能读取该 Snapshot，也不能以它计算新的 Diff；但 checkpoint 和 block 引用尚不能立即删。

### 11.2 为什么要记录 `lastTransactionInfo`

后台 `SnapshotDeletingService` 只处理：

```text
status == SNAPSHOT_DELETED
且 SnapshotInfo 的最后改动已经 flush 到 active OM DB
```

它通过：

```java
OmSnapshotManager.areSnapshotChangesFlushedToDB(...)
```

比较 active OM DB 持久化事务与 SnapshotInfo 的 `lastTransactionInfo`。

这防止以下危险情况：

```text
内存 cache 已显示 Snapshot deleted
磁盘 snapshotInfoTable 仍显示 Snapshot active
后台若提前 purge 并回收 block
OM 重启后从磁盘恢复 active Snapshot
但它引用的数据已被删
```

## 12. 删除时为什么要搬运 deleted entries

这是 Snapshot GC 的关键。

假设同 bucket 的链：

```text
S1 -> S2 -> S3
```

某 key 在 S1 中存在，之后在 live bucket 被删除。为保护该 key 的旧 block，相关 delete/rename metadata 需要持续存在于：

```text
deletedTable
deletedDirTable
snapshotRenamedTable
```

若 S2 被删除，不能直接删掉其 checkpoint，因为其中待处理的删除记录可能仍是证明 blocks 被后续历史视图引用的唯一信息。

解决方案：将 deleted/rename entries 前移到下一个 active Snapshot；若没有下一个 Snapshot，则移回 AOS：

```text
S2(deleted)
  -> S3(active)
  -> ...
  -> AOS
  -> 正常 block deletion 流程
```

因此中间 Snapshot 删除不会丢失历史引用保护。

## 13. `SnapshotDeletingService`

实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/service/SnapshotDeletingService.java
```

特点：

* 单线程执行，避免多个线程扫描同表并对同一 Snapshot 重复提交操作；
* 反向遍历 global Snapshot chain；
* 每轮受 Snapshot 数、key 数与 Ratis request byte size 限制；
* 持有当前与 next Snapshot 的 `SNAPSHOT_GC_LOCK`；
* 仅处理已逻辑删除且状态已落盘的 Snapshot。

每轮对一个 Snapshot：

1. 读取其 deleted keys、deleted dirs、rename entries；
2. 若有 entries，构造 `SnapshotMoveTableKeys` 内部 OM 请求；
3. 为避免超过 Ratis appender queue byte limit，将请求批处理；
4. 若 entries 为空，提交 `SnapshotPurge`。

若下一个 path Snapshot 仍是 deleted，服务会跳过当前 Snapshot，避免做无效的多次拷贝；等链中下一个 active Snapshot 或 AOS 成为最终承接者。

## 14. `SnapshotMoveTableKeys` 的一致性

请求：

```text
OMSnapshotMoveTableKeysRequest
OMSnapshotMoveTableKeysResponse
```

请求会验证：

* deleted key、rename key、deleted dir 必须在对应 bucket prefix 下；
* 请求内不能有重复 key；
* deleted dir 的 keyinfo 数量符合预期。

响应真正执行时：

1. 取得 source/target Snapshot 的 `SNAPSHOT_DB_CONTENT_LOCK`；
2. 打开 source Snapshot DB；
3. 若有 next active Snapshot：写入其 Snapshot DB；
4. 若无后继：写入 active OM DB batch；
5. 从 source Snapshot DB 删除已迁移 records；
6. flush source/target Snapshot RocksDB WAL 与 DB；
7. 更新关联 SnapshotInfo。

这使 delete/rename history 能沿 Snapshot chain 安全迁移。

## 15. SnapshotPurge

处理器：

```text
OMSnapshotPurgeRequest
OMSnapshotPurgeResponse
```

当 Snapshot 的 delete/rename entries 全部迁移完成，`SnapshotDeletingService` 提交 `SnapshotPurge`。

对每个待 purge Snapshot：

1. 标记下一个 Snapshot（必要时还有 next-next）需要重新 deep clean；
2. 修改并重连 global/path Snapshot chain；
3. 从 `snapshotInfoTable` cache 和最终 DB 中移除当前 SnapshotInfo；
4. 记录 purge transaction，以支持本地 YAML/版本清理的安全判断。

例如：

```text
S1 -> S2 -> S3
purge S2
S1 -> S3
```

S3 的 previous IDs 更新为 S2 原来的前驱。之后本地 Snapshot checkpoint、YAML 和旧 version directory 会由本地清理流程移除；并非 purge RPC 返回时立即全部删除。

## 16. Key/Directory Deep Cleaning 与 block 回收

Snapshot 是否真正保留 blocks，由以下服务协作完成：

```text
KeyDeletingService
DirectoryDeletingService
SnapshotDirectoryCleaningService
SnapshotDeletingService
```

Key deep clean 需要判定：

```text
一个待删 key 的 blocks 是否仍被任一 active Snapshot 引用？
```

* 若仍被引用：不发送真实 block delete；删除历史与 key metadata 继续按 Snapshot chain 保留。
* 若仅被单个 Snapshot 引用：更新该 Snapshot 的 `exclusiveSize` / `exclusiveReplicatedSize`。
* 若没有 active Snapshot 或 AOS 引用：进入普通 block 删除流程。

FSO directory delete 需要异步递归处理大量子项。目录 deep clean 对应 `deletedDirTable`，并单独更新：

```text
exclusiveSizeDeltaFromDirDeepCleaning
exclusiveReplicatedSizeDeltaFromDirDeepCleaning
```

## 17. SnapshotDiff

核心实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/snapshot/SnapshotDiffManager.java
```

相关辅助：

```text
snapshot/diff/
snapshot/diff/delta/
CompositeDeltaDiffComputer
RDBDifferComputer
FullDiffComputer
FileLinkDeltaFileComputer
TableMergeIterator
SnapshotDiffValueParser
SnapDiffPathResolver
```

Diff 比较 `fromSnapshot -> toSnapshot`，结果类型：

| 标记 | 类型 |
|---|---|
| `+` | CREATE |
| `-` | DELETE |
| `M` | MODIFY |
| `R` | RENAME |

### 17.1 Job 模型

SnapshotDiff 采用异步 job。job key 是：

```text
<fromSnapshotId>-<toSnapshotId>
```

同一 Snapshot pair 的并发请求会复用同一个 job。状态包括：

```text
QUEUED
IN_PROGRESS
DONE
FAILED
REJECTED
CANCELLED
NOT_FOUND
```

job 和 report 存在独立的 SnapshotDiff RocksDB 中，路径由：

```text
ozone.om.snapshot.diff.db.dir
```

控制。线程池有界；超过并发能力时 job 会进入 `REJECTED` 或允许后续重试。

### 17.2 输出顺序与分页

Diff report 的存储 key 按如下顺序编码：

```text
1. DELETE
2. RENAME
3. CREATE
4. MODIFY
```

顺序是可应用性要求。例如：旧状态有 A，B；新状态删除 A，并将 B rename 为 A。必须先 DELETE A，才能 RENAME B -> A。

report key 包含 job ID、类型前缀和固定宽度 index，支持 pagination token。最后一页会校验返回的最大 key 与 job 记录的 `largestEntryKey` 一致；失败则将 job 标为 FAILED，防止返回不完整 report。

### 17.3 快速 diff：compaction DAG

RocksDB compaction 会将 SST 重写。全表扫描比较两个 Snapshot 成本很高，因此快速路径会：

1. 追踪 Diff 相关 column family 的 SST compaction DAG；
2. 找到两个 checkpoint 间可能变更的 SST；
3. hard-link 候选 SST，保证 job 运行期间源 SST 不会被 prune；
4. 使用 native raw SST reader 读取 keys/tombstones；
5. 用 `TableMergeIterator` 对两端 Snapshot table 进行 merge compare；
6. 仅生成真实变化记录。

重点 column families 是：

```text
keyTable
fileTable
directoryTable
```

compaction DAG 只用于性能优化。若 DAG 不完整、历史过旧、native library 不可用、调用者指定 full diff，系统应走 FullDiff，不以牺牲 correctness 换性能。

### 17.4 FullDiff

`FullDiffComputer` 比较 Snapshot RocksDB 中实际 table 数据，成本更高但语义正确。`--forceFullDiff` 和相关配置可主动选择此路径。

## 18. Snapshot Defragmentation

文档：

```text
hadoop-hdds/docs/content/feature/SnapshotDefragmentation.md
```

实现：

```text
hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/snapshot/defrag/SnapshotDefragService.java
```

### 18.1 问题

初始 Snapshot checkpoint 使用 hard links，短期高效；但 Active OM RocksDB 持续 compaction 后，多个长期 Snapshot 可能保留大量不同历史 SST，元数据盘占用增长。

Snapshot RocksDB 自动 compaction 被有意关闭，因为它会破坏或复杂化 SnapshotDiff 对 SST 历史的利用。

### 18.2 Defrag 逻辑

```text
同 bucket 第一个 Snapshot：
  以当前 Snapshot checkpoint 为基础
  -> 仅保留 bucket prefix 数据
  -> tracked tables 执行 full defrag

后续 Snapshot：
  以前一个 path Snapshot 当前 version checkpoint 为基础
  -> 计算前后 Snapshot 的 logical delta
  -> 生成 changed values / tombstones SST
  -> ingest 到新 checkpoint
```

tracked tables：

```text
keyTable
fileTable
directoryTable
```

不参与增量重写的 tables 会在 version switch 前从当前 Snapshot 导出并重新 ingest。

### 18.3 原子版本切换

defrag 在 `tmp_defrag` 创建 checkpoint，成功后：

1. 获取当前 Snapshot 的 `SNAPSHOT_DB_CONTENT_LOCK`；
2. 重新装入 non-incremental tables；
3. 关闭临时 DB handle；
4. 移动 checkpoint 到 `om.db-<id>-<nextVersion>`；
5. 更新 YAML 的 version、SST 元信息和依赖；
6. 清除 `needsDefrag`；
7. 在 SnapshotCache 已 drain 时删除无依赖旧 version。

这样不会让读取者看到内容不完整的 Snapshot DB，也避免删除仍被打开的旧 version。

## 19. Feature gate、HA 和 bootstrap

Snapshot 请求受以下约束：

```java
@DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
@RequireSnapshotFeatureState(true)
```

即 OM layout version 和 feature state 都必须允许 Snapshot。

若管理员禁用 filesystem Snapshot，但 `snapshotInfoTable` 仍有任何 Snapshot——包括已标 deleted 但尚未 purge 的 Snapshot——OM 会拒绝启动。原因是停掉 Snapshot 服务后仍忽略这些历史引用会带来数据回收风险。

OM bootstrap 必须感知 Snapshot：它不只复制 active OM DB，还可能需要一致地处理所有 Snapshot RocksDB directories、YAML 和相关 SST 数据。bootstrap/defrag 会使用 Snapshot cache lock、Snapshot DB content lock 与 bootstrap lock，避免拷贝到活跃修改中的 Snapshot DB。

## 20. 关键配置

| 配置 | 用途 |
|---|---|
| `ozone.filesystem.snapshot.enabled` | 启用 filesystem Snapshot |
| `ozone.om.fs.snapshot.max.limit` | Snapshot 总数安全上限 |
| `ozone.om.snapshot.cache.max.size` | 已打开 Snapshot DB cache soft limit |
| `ozone.om.snapshot.db.max.open.files` | Snapshot RocksDB 最大打开文件数 |
| `ozone.om.snapshot.checkpoint.dir.creation.poll.timeout` | 新 checkpoint 可加载前的轮询等待时间 |
| `ozone.om.snapshot.rename.allowed` | 是否允许 Snapshot rename |
| `ozone.om.snapshot.diff.db.dir` | Diff job/report DB 路径 |
| `ozone.om.snapshot.diff.thread.pool.size` | Diff 并行度 |
| `ozone.om.snapshot.force.full.diff` | 强制 FullDiff |
| `ozone.om.snapshot.diff.disable.native.libs` | 禁用 native SST diff |
| `ozone.snapshot.deleting.service.interval` | SnapshotDeletingService 运行周期 |
| `ozone.snapshot.deleting.limit.per.task` | 单轮处理 Snapshot 数量 |
| `ozone.snapshot.key.deleting.limit.per.task` | 单轮处理/迁移 key 数量 |
| `ozone.snapshot.deep.cleaning.enabled` | 启用 Snapshot deep clean |
| `ozone.om.snapshot.compaction.dag.max.time.allowed` | 快速 DAG diff 的有效历史窗口 |
| `ozone.om.snapshot.compact.non.snapshot.diff.tables` | eviction 时可选 compact 非 Diff 跟踪表 |

## 21. 性能与运维含义

* Snapshot 创建快，但不是零成本：长期 Snapshot 会增加 OM SST 版本与 datanode 旧 block 保留量。
* Snapshot 删除不等于立刻释放空间；需等待 deleted metadata 沿 chain 迁移、purge、deep clean 和正常 key/block GC。
* 高 Snapshot 数量需要更高的 OM `nofile`、更快 metadata disk、更充足内存，以及合理的 Snapshot retention policy。
* SnapshotDiff 的效率主要取决于变更量、SST 历史、native library 可用性和并发 Diff 数量，而非仅 Snapshot 数量。
* compaction DAG 或 native SST 分析不可用时，系统可回退 FullDiff；正确性优先于优化。
* Snapshot checkpoint 文件依赖 hard links；文件系统 hard-link 上限会约束极端高频、长期保留 Snapshot 的规模。

## 22. 推荐源码阅读顺序

1. `SnapshotInfo.java`：模型和持久化字段。
2. `SnapshotChainManager.java`：global/path 双链与重启重建。
3. `OMSnapshotCreateRequest.java`、`OMSnapshotCreateResponse.java`：创建事务。
4. `OmSnapshotManager#createOmSnapshotCheckpoint`：checkpoint、deleted-table cleanup。
5. `OmSnapshotManager#getActiveFsMetadataOrSnapshot`、`OmSnapshot.java`：Snapshot 读路径。
6. `SnapshotCache.java`：RocksDB handle 生命周期与 refcount。
7. `OMSnapshotDeleteRequest.java`：逻辑删除。
8. `SnapshotDeletingService.java`、`OMSnapshotMoveTableKeysResponse.java`：deleted/rename metadata 前移。
9. `OMSnapshotPurgeRequest.java`：链重连和最终 purge。
10. `KeyDeletingService.java`、`DirectoryDeletingService.java`、`snapshot/filter/`：block 保留和 deep cleaning。
11. `SnapshotDiffManager.java`、`snapshot/diff/delta/`：Diff job 和 SST 优化。
12. `OmSnapshotLocalDataManager.java`、`SnapshotDefragService.java`：本地版本 DAG 与规模优化。

## 23. 总结

Ozone Snapshot 的本质是：

```text
OM RocksDB checkpoint 冻结 bucket metadata
  + immutable blocks 不复制
  + Snapshot chain 维护时间依赖
  + deleted/rename metadata 沿链迁移
  + deep cleaning 延迟物理 block 回收
  + SnapshotCache 管理打开的 checkpoint DB
  + SnapshotDiff 使用 DAG/SST 优化并可回退 FullDiff
  + defragmentation 控制长期 Snapshot 的元数据盘膨胀
```

最核心的安全不变量是：**只要 active Snapshot 或 live AOS 仍可能引用某个 key 的旧 blocks，对应删除历史和 block metadata 就不得被提前丢弃。** Ozone 通过 Snapshot chain、transaction flush 检查、内部 Ratis move/purge 请求、内容锁和后台 GC 服务共同维持这个不变量。
