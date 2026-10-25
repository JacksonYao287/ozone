# Ozone 快照（Snapshot）设计与实现详解

## 0. 先厘清概念：这是哪个"快照"

Ozone 代码库里"snapshot"这个词被用在两个完全不相关的地方，必须先区分开：

- **本文讲的"bucket/volume 快照"**（`ozone sh snapshot ...`）：用户可见的、**OM 命名空间层面**的功能，对某个 bucket 的 key 空间做一次只读的时间点视图，概念上类似 HDFS 快照。核心类是 `SnapshotInfo`、`SnapshotChainManager`、`OmSnapshotManager` 等，都在 `hadoop-ozone/ozone-manager` 和 `hadoop-ozone/common` 下。
- **Ratis/SCM-HA 的"state machine snapshot"**（`RDBSnapshotProvider`、`SCMSnapshotProvider`/`SCMSnapshotDownloader`、`OmRatisSnapshotProvider`、`ReconRDBSnapshotProvider`）：这是 Raft 日志压缩/状态安装机制，把某个服务**整个** RocksDB 的全量 checkpoint 打包传给落后的 Ratis follower 或新启动的 HA 节点（Recon 也复用这套协议做全量镜像同步）。它与 `SnapshotInfo`/`snapshotInfoTable` 毫无关系，只是恰好都用 RocksDB checkpoint 做底层机制、又都叫"snapshot"。两者的 bug/安全问题不能混为一谈。

下文全部围绕第一种（bucket 快照）展开。

---

## 1. 元数据模型：`SnapshotInfo`

`hadoop-ozone/common/.../om/helpers/SnapshotInfo.java` 是快照的核心元数据对象，持久化在 OM 的 `snapshotInfoTable`（key 为 `/vol/bucket/snapName`，见 `getTableKey()`，SnapshotInfo.java:599-603）。关键字段：

- `snapshotId`（UUID，创建时随机生成，终身不变）、`name`（可改名）、`volumeName`/`bucketName`。
- `snapshotStatus`：`SNAPSHOT_ACTIVE` / `SNAPSHOT_DELETED` 两态枚举（SnapshotInfo.java:818-847），"删除"只是软删除状态转换，不等于立刻物理清除。
- `pathPreviousSnapshotId` / `globalPreviousSnapshotId`：两条独立链表的"上一个节点"指针——**path 链**（同一个 bucket 内快照按创建顺序排列）和**global 链**（跨所有 bucket 的全局创建顺序），由 `SnapshotChainManager` 维护。
- `referencedSize`/`referencedReplicatedSize`：创建时刻该快照引用的数据量估算（见下文 §2）。
- `exclusiveSize`/`exclusiveReplicatedSize`：只有这一个快照独占引用（删掉它就能真正释放）的数据量，由后台 deep-clean 服务异步计算填充（非创建时就有）。
- `exclusiveSizeDeltaFromDirDeepCleaning`/`…Replicated…`：目录级 deep-clean（`SnapshotDirectoryCleaningService`）单独发现的增量独占大小，故意和 key 级的 `exclusiveSize` 分开存储，注释明确写着"避免两个异步服务互相覆盖写"（SnapshotInfo.java:94-103）。
- `deepClean`/`deepCleanedDeletedDir`：标记该快照的 deletedTable/deletedDirTable 是否已被 `KeyDeletingService` 深度清理过一轮，避免重复扫描。
- `sstFiltered`：是否已做过 SST 过滤（配合 SnapDiff 的压缩追踪）。
- `createTransactionInfo`/`lastTransactionInfo`：记录该快照元数据最近一次变更对应的 Raft `TermIndex`，是判断"这条变更是否已经被 double buffer 落盘"的依据（下文会反复用到）。

`SnapshotInfo.newInstance(...)`（SnapshotInfo.java:716-737）在创建时把 `pathPreviousSnapshotId`/`globalPreviousSnapshotId` 先设成一个哨兵值 `INITIAL_SNAPSHOT_ID`（每个 JVM 随机生成一次），后面请求处理逻辑里会把它们正确赋值成链表中真正的前驱。

---

## 2. 创建快照：`OMSnapshotCreateRequest` / `OMSnapshotCreateResponse`

这是一个标准的 OM 双缓冲（double buffer）写请求，分 `preExecute`（leader 单独预处理，生成确定性字段）+ `validateAndUpdateCache`（Ratis 状态机 apply 阶段，leader/follower 都执行）两段：

**`preExecute`**（OMSnapshotCreateRequest.java:104-132）：
- `@DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)` + `@RequireSnapshotFeatureState(true)` 两个注解把开关检查做成 AOP 切面（`RequireSnapshotFeatureStateAspect`），不用在每个方法里手写 if。
- 校验快照名合法性、解析 link bucket 的真实 volume/bucket、权限检查（只有 bucket owner 或 Ozone admin 能创建）、调用 `snapshotLimitCheck()`（避免一次性堆积过多未完成的创建请求）。
- **把随机生成的 `snapshotId` 和 `creationTime` 写回 request**（`setSnapshotId(UUID.randomUUID())`、`setCreationTime(Time.now())`），这样这些非确定性值只在 leader 生成一次，再通过 Ratis 日志复制给所有副本，保证所有 OM 节点对同一个快照算出完全相同的 ID 和时间戳——这是 OM HA 下写请求的标准模式。

**`validateAndUpdateCache`**（OMSnapshotCreateRequest.java:135-233）：
1. 依次获取 bucket 读锁（防止创建期间 bucket 被删）和该快照名的写锁。
2. 检查 `snapshotInfoTable` 里同名 key 是否已存在（`FILE_ALREADY_EXISTS`）。
3. **referencedSize 的计算是 O(1) 估算而非精确扫描**：直接取 bucket 当前 `usedBytes`（`referencedReplicatedSize`），再除以默认副本因子得到未复制大小的估算值（`estimateBucketDataSize`，OMSnapshotCreateRequest.java:335-345）。代码注释明确说明这是设计目标（"CreateSnapshot 必须是 O(1) 操作"），不会对 keyTable 做全表扫描。
4. 调用 `addSnapshotInfoToSnapshotChainAndCache`（OMSnapshotCreateRequest.java:252-294），**把"挂链表"和"写 table cache"包在同一把 `synchronized(snapshotChainManager)` 锁里原子完成**——方法上的长注释解释了为什么必须原子：如果两个线程分别创建两个不同 bucket 的快照，一个先挂链表后续写 cache 失败被回滚，另一个却已经把前驱指向了被回滚的那个，链表就会指向一个实际上不存在于 `snapshotInfoTable` 的快照 ID，导致 OM 重启时 `SnapshotChainManager` 加载链表直接崩溃。失败时会显式调用 `removeSnapshotInfoFromSnapshotChainManager` 把刚加的节点摘回去。
5. 写入 `createTransactionInfo`/`lastTransactionInfo`（当前 Raft `TermIndex`）。

**`OMSnapshotCreateResponse.addToDBBatch`**（OMSnapshotCreateResponse.java:51-71）是实际落盘的地方，顺序很关键，代码注释专门强调：**先把 `SnapshotInfo` 写入 `snapshotInfoTable`，再创建 RocksDB checkpoint**。原因：`RocksDBCheckpointDiffer` 靠监听 `snapshotInfoTable` 是否非空来决定要不要追踪压缩事件（见 §8），顺序反了的话，checkpoint 刚建好、紧接着发生一次压缩，而 `snapshotInfoTable` 此时还没写入，就可能漏记一条压缩 DAG 边——注释说这最坏只是影响 SnapDiff 性能（退化为全量 diff），不影响正确性。

真正创建 RocksDB checkpoint 的代码是 `OmSnapshotManager.createOmSnapshotCheckpoint`（OmSnapshotManager.java:540-594）：
- 调用 `RDBStore.getSnapshot(checkpointDirName)`，底层走 RocksDB 原生的 checkpoint API（本质是对当前 SST 文件做**硬链接**，不拷贝数据，所以是轻量操作）。
- checkpoint 目录命名为 `.snapshot/<snapshotId>`（版本 0 时，见 `getCheckpointDirName`，SnapshotInfo.java:577-583），之后 defrag 产生新版本时会变成 `.snapshot/<snapshotId>/<version>`。
- 同时调用 `OmSnapshotLocalDataManager.createNewOmSnapshotLocalDataFile` 生成该快照的本地 YAML 元数据文件（见 §4）。
- **清理活跃 DB 的 `deletedTable`/`deletedDirTable`/`snapshotRenamedTable` 中属于该 bucket 的条目**（`deleteKeysFromTableWithBucketPrefix`）——因为这个新快照的 checkpoint 已经把这些"待删除但还没真正删"的 key 原样复制过去了，继续留在活跃 DB 里只会造成状态分叉，必须统一挪到快照自己的表里，由快照自己的生命周期去管（对应 §6 的"责任下移"机制）。

---

## 3. `SnapshotChainManager`：双链表结构与一致性校验

`hadoop-ozone/ozone-manager/.../om/SnapshotChainManager.java` 维护两条逻辑上独立的**双向链表**（用 `Map<UUID, SnapshotChainInfo>` 实现，`SnapshotChainInfo` 只是一个 `{snapshotId, previousSnapshotId, nextSnapshotId}` 的节点）：

- **全局链**（`globalSnapshotChain`）：跨所有 bucket，按创建顺序排列所有快照。
- **路径链**（`snapshotChainByPath`，`Map<bucketPath, LinkedHashMap<UUID, SnapshotChainInfo>>`）：每个 bucket 一条独立链。

OM 启动时 `loadFromSnapshotInfoTable`（SnapshotChainManager.java:305-363）会**全表扫描 `snapshotInfoTable`**，按 `globalPreviousSnapshotId` 重建出一条单向链表，并严格校验：链表长度必须等于表中记录总数，否则抛出"snapshot chain corruption"异常——这是 OM 启动时唯一一次对链完整性的强校验，之后运行期全靠增量维护。

插入/删除都有配套的 `validateAddSnapshot*`/`validateDeleteSnapshot*` 做前置校验（比如：要插入的前驱必须存在、前驱不能已经有 next、要删除的节点的 prev/next 指向必须自洽），任何不一致都抛 `IOException` 并打印详细诊断信息——这套校验逻辑本质上是整个快照系统正确性的地基，一旦链损坏，`SnapshotDeletingService`、`SnapshotDiffManager` 的"找上一个快照"逻辑全部失效，这也正是 §9 提到的 `SnapshotChainRepair` 运维工具存在的原因。

`iterator(boolean reverse)`（SnapshotChainManager.java:446-480）提供按全局链正向/反向遍历的迭代器，供 `SnapshotDeletingService`、`SnapshotDefragService` 使用；它在 `next()` 内部对 `SnapshotChainManager.this` 加锁，并且能容忍"遍历到一半、节点已被并发 purge 掉"的情况（遇到 `NoSuchElementException` 就当作链走到头）。

---

## 4. 运行时访问与缓存：`OmSnapshotManager` / `SnapshotCache` / `OmSnapshot`

一个快照的 checkpoint 目录要变成可查询的对象，经过这几层：

**`OmSnapshotManager.getSnapshot(...)`** → `SnapshotCache.get(snapshotId)` → 缓存未命中时触发 Guava `CacheLoader`（OmSnapshotManager.java:339-427）：
1. 从内存里的 `SnapshotChainManager` 反查 `snapshotId -> tableKey`。
2. 从 `snapshotInfoTable` 读 `SnapshotInfo`（用 `getSkipCache` 绕过未提交的写缓存，确保 follower 节点也能看到已经 flush 到磁盘的快照）。
3. **轮询等待** `isSnapshotFlushedToDB` 变 true（受 `ozone.snapshot.checkpoint.dir.creation.poll.timeout` 控制）——因为创建快照的 Raft 事务 apply 和 checkpoint 目录真正落盘之间存在时间差，必须等确认后才能去开这个目录。
4. 用 `OmMetadataManagerImpl` 的专用构造器打开这个 checkpoint 目录，**复用和活跃 DB 完全相同的 `OMDBDefinition`**（即同一套 column family 定义），所以快照 DB 暴露的表结构和活跃 DB 一模一样。
5. 值得注意的细节：快照 DB 虽然语义上只读，但**物理上是以读写模式打开**（`setOpenReadOnly(false)`）——这是为了让后台服务（defrag、move-keys）能够修改它，"只读"约束完全靠应用层锁（`OmReadOnlyLock`）而不是 RocksDB 本身的打开模式来保证。禁用了自动压缩和 DAG 追踪（快照 DB 不需要参与 SnapDiff 的压缩 DAG）。
6. 每个快照 DB 都是**完全独立的 RocksDB 实例**，各自的 options/缓存互不共享，也不与活跃 DB 共享 block cache。

**`SnapshotCache`** 的淘汰策略是"引用计数 + 软上限"，不是严格 LRU：`ConcurrentHashMap<UUID, ReferenceCounted<OmSnapshot>>`，`get()` 返回一个 `AutoCloseable`，调用方 try-with-resources 用完即 `close()` 减引用计数。超过 `ozone.om.snapshot.cache.max.size` 软上限只会打印节流告警日志，**绝不会强制关掉正在被使用的快照**；真正的关闭只发生在引用计数归零后，由定时任务（`ozone.om.snapshot.cache.cleanup.service.run.interval`）或者 `invalidate()` 触发。关闭前如果开启了 `ozone.om.snapshot.compact.non.snapshot.diff.tables`，还会顺手压缩一遍该快照 DB 里和 SnapDiff 无关的表，回收空间。

**`OmSnapshotLocalData`/`OmSnapshotLocalDataYaml`**：每个快照 checkpoint 目录旁边有一个 YAML sidecar 文件，存**节点本地的、不走 Raft 复制**的物理布局信息——SST 文件清单、defrag 版本号（`version`/`versionSstFileInfos`）、是否需要 defrag、RocksDB 内部 `dbTxSequenceNumber`、以及一个 SHA-256 校验和防止写一半损坏（模仿 datanode 的 `ContainerDataYaml` 模式）。之所以不能放进受 Raft 复制的 `SnapshotInfo` 里，是因为 defrag 会**物理重写**某台 OM 节点上该快照的 SST 布局，这是天然的节点本地状态，不需要（也不能）达成跨节点共识。

**并发控制**：`MultiSnapshotLocks` 用于一次性锁住多个快照 ID（比如 GC 要同时锁住一个快照和它的下一个邻居），避免两个并发调用以不同顺序加锁造成死锁——它本身不排序，真正的防死锁保证来自底层 `OzoneManagerLock` 用 Guava `Striped.bulkGet`，该方法**保证按 stripe 索引排序**后再返回，天然让任意两组重叠的锁请求都以全局一致的顺序获取。

---

## 5. 重命名与属性设置

**`OMSnapshotRenameRequest`**（OMSnapshotRenameRequest.java）：先检查 `ozone.om.snapshot.rename.allowed` 开关（可整集群禁用改名功能），校验新名不冲突、旧快照必须处于 `SNAPSHOT_ACTIVE`，然后**直接在内存对象上改名**（`snapshotOldInfo.setName(newName)`），同时在 `snapshotInfoTable` 里做"旧 key 删除 + 新 key 写入"的 cache 操作（两条 `addCacheEntry`，一条值为空代表删除），并调用 `SnapshotChainManager.updateSnapshot()` 同步更新 `snapshotIdToTableKey` 反查表——**链表拓扑本身不变**，只是 ID 对应的表 key 变了。

**`OMSnapshotSetPropertyRequest`**：这是一个**纯内部/系统请求**（不是普通用户 API），用来批量更新 `exclusiveSize`、`exclusiveSizeDeltaFromDirDeepCleaning`、`deepClean`/`deepCleanedDeletedDir` 这几个由后台服务异步计算出来的字段。支持一次请求携带多个快照的更新（`setSnapshotPropertyRequestsList`），用一个专门的 `OMSYSTEMLOGGER` 审计日志记录，和普通用户操作的审计流分开。

---

## 6. 删除快照的完整生命周期：软删除 → 责任下移 → 物理清除

这是整个快照系统里最精巧、也最容易踩坑的部分，核心矛盾是：**快照的 checkpoint 是对某一时刻 RocksDB 状态的引用，但底层数据块是在 datanode 上按引用共享的，不能因为某个 key 在活跃命名空间里被删了就立刻回收它的块**——只要还有快照的 checkpoint 里"看得见"这个 key，它的块就必须留着。

**第一步，`OMKeyDeleteRequest` 对快照毫无感知**——它无条件把被删 key 挪进活跃 DB 的 `deletedTable`（`RepeatedOmKeyInfo`，支持同名多代），不管这个 bucket 有没有快照。真正的"安不安全删"完全是 GC 侧的事。

**第二步，`OMSnapshotDeleteRequest`（软删除）**（OMSnapshotDeleteRequest.java:118-247）：把 `SnapshotStatus` 从 `ACTIVE` 改成 `DELETED`，记录 `deletionTime`，并且**特意盖上 `lastTransactionInfo` 时间戳**——代码里有一段很说明问题的注释：这个时间戳是让 `areSnapshotChangesFlushedToDB()` 能正确判断"这次删除有没有真正落盘"，`SnapshotDeletingService.shouldIgnoreSnapshot()` 依赖这个信号来推迟后续的 move/purge 操作；如果没有这个戳，purge 可能在内存链表已经清空、但磁盘上 `snapshotInfoTable` 还是 ACTIVE 状态时就被执行，导致 `KeyDeletingService` 错误地回收了磁盘快照仍然引用的数据块。这一步**不会**立刻删除 checkpoint 目录——快照进入"已软删除、等待后台回收"状态。

**第三步，`KeyDeletingService` + `ReclaimableKeyFilter` 做 deep-clean 判断**：处理某个快照（或活跃 DB）`deletedTable` 里的条目时，去查**上一个快照**的 keyTable/fileTable：
- 如果上一个快照里也没这个 key → 真正可回收，发起 SCM 块删除。
- 如果上一个快照里还有 → 暂不回收；同时顺便计算"独占大小"（`exclusiveSize`）：如果这份数据在上一个快照里存在、但在上上个快照里不存在，说明它是**只被上一个快照独占引用**的数据，这个大小会通过 `OMSnapshotSetPropertyRequest` 写回 `SnapshotInfo.exclusiveSize`——注意这只是一个**展示/估算用的运维指标**（"删掉这个快照大概能回收多少空间"），并不是真正驱动回收与否的判断依据，真正的 gate 是上面那个"上一个快照里有没有"的检查。

**第四步，`SnapshotDeletingService` 把"责任"沿着链条下移**：对每个处于 `SNAPSHOT_DELETED` 且变更已落盘的快照 S，读出它自己 `deletedTable`/`deletedDirTable`/`snapshotRenamedTable` 里的条目，通过 `OMSnapshotMoveTableKeysRequest`/`Response`（当前实际使用的协议；还有一套功能相近但已不再被任何代码路径调用的旧协议 `OMSnapshotMoveDeletedKeysRequest`/`Response`，疑似只为 Ratis 日志重放兼容性保留）**合并挪到链上的下一个活跃快照**（如果 S 是链尾则直接挪到 AOS，即活跃对象存储命名空间本身）。两个快照各自是独立的 RocksDB 实例，这次合并需要分别对两边各自 `commitBatchOperation` 落盘。

**第五步，只有当 S 自己的三张表全部清空后**，才会提交 `OMSnapshotPurgeRequest` 真正摘除这个快照：
1. 把下游（next 和 next-next）快照的 `deepClean`/`deepCleanedDeletedDir` 重置为 false——因为 S 消失后，原来被 S "挡住"的 key 现在可能在下一个快照里变成可回收的了，必须让 `KeyDeletingService` 重新扫一遍，否则会因为"已经 deep-clean 过"的标记被错误跳过。
2. **链表拼接**（`updateSnapshotChainAndCache`）：对 path 链和 global 链分别做类似双向链表删除中间节点的操作——把 S 的后继节点的 `previousSnapshotId` 直接指向 S 的前驱，相当于把 S 从两条链里摘掉。
3. 从 `snapshotInfoTable` 里清掉这条记录。

**`OMSnapshotPurgeResponse.addToDBBatch`** 才是真正的物理删除：关闭/驱逐 `SnapshotCache` 里这个快照的 RocksDB 句柄、从 `SnapshotChainManager` 的反查表里移除、**删除磁盘上的 checkpoint 目录**（`deleteSnapshotCheckpointDirectories(snapshotId, -1)`，参数 `-1` 表示删除该快照所有版本），并从表里删掉这条 `SnapshotInfo` 记录。

---

## 7. `SnapshotDefragService`：快照存储格式优化（不是 GC）

这个服务解决的是另一个问题：快照 checkpoint 是对活跃 DB 当时 SST 文件的硬链接，随着活跃 DB 后续不断压缩重写，一个很早创建的快照可能长期挂着一堆早已过时、零散、互相重叠的小 SST 文件，既浪费磁盘又拖慢该快照的读性能。

- 对一个 bucket path 链上的**第一个**快照做**全量 defrag**：建一个只保留该 bucket 前缀键范围的新 checkpoint，强制压缩清掉墓碑。
- 对后续快照做**增量 defrag**：复用 SnapDiff 同一套压缩 DAG/SST-diff 机制算出相对上一个已 defrag 快照的增量，只把变化部分写入新 SST。
- 用"创建新版本目录 → 原子切换 → 删除旧版本"的方式做无锁定/低锁定的原地升级（`atomicSwitchSnapshotDB`），版本号记在前面提到的 YAML sidecar 文件里。
- 受 `OMLayoutFeature.SNAPSHOT_DEFRAG` 升级完成标志和 RocksDB 原生库可用性共同门控；也可以用 `ozone admin om snapshot defrag` 手动触发一次。

---

## 8. SnapshotDiff 引擎：基于压缩 DAG 的增量差异计算

这是整个快照功能里工程含量最高的部分，目标是让"diff 两个快照"的开销只跟**变化的数据量**成正比，而不是跟 bucket 里 key 的总数成正比。

**请求/持久化流程**：客户端 `submitSnapshotDiff` → `OmSnapshotManager`/`SnapshotDiffManager` 用 `fromSnapshotId+toSnapshotId` 组装 job key（按 ID 不按名字，防止改名导致任务找不到）→ 写入独立的 RocksDB（`snapDiffJobTable`/`snapDiffReportTable`/`snapDiffPurgedJobTable`）→ 提交到有界线程池异步执行。任务状态机 `QUEUED → IN_PROGRESS → {DONE|FAILED}`，还有 `CANCELLED`/`REJECTED`（线程池背压时）。OM 重启时，`IN_PROGRESS` 的任务会清掉半成品报告重新跑，`QUEUED` 的不会重新提交（因为提交响应从未送达客户端，预期客户端会重新发起）。**注意这套逻辑目前在每个 OM 节点（包括 follower）上都独立跑一遍，没有走 Ratis 复制**，这是代码里标注的已知局限。

**核心算法**（`RocksDBCheckpointDiffer` + `CompactionDag`）：
- OM 对活跃 DB 的 `keyTable`/`directoryTable`/`fileTable` 注册 RocksDB 压缩事件监听器，每次压缩发生时把**输入文件先硬链接保存**（防止 RocksDB 把它们真删了），再记一条"输出文件 → 输入文件"的边到一张压缩 DAG 里（`CompactionDag`，本质是一个 Guava `MutableGraph<CompactionNode>`，节点按文件名唯一标识，携带创建时的 DB 序列号）。
- Diff 两个快照时，**不需要扫描任意一方的全部 key**：对新快照里的每个 SST 文件，沿着压缩 DAG 往"祖先"方向广度优先搜索，如果能找到一个文件名也出现在老快照 SST 集合里的节点，就说明这两个快照在这部分数据上**同源无差异**，直接跳过；只有搜不到共同祖先的文件才需要真正打开读取——这把 diff 开销从"全量扫描"降到"实际发生变化附近的少量 SST 文件"。
- 从"有差异的 SST 文件集合"到具体 key 差异：读取这些增量 SST 文件（而不是整表扫描），对每个 key 记下它在两个快照里的 **object ID**（Ozone 的 key/目录记录天生携带不随路径变化的 `objectID`）。
- **重命名检测靠的就是这个 object ID**：同一个 object ID 在旧快照和新快照里都存在、但对应的路径名不同 → 判定为 RENAME（而不是误判成一对无关的 DELETE+CREATE）；如果内容也变了，还会额外追加一条 MODIFY。CREATE/DELETE 则分别对应"仅在新快照存在"/"仅在旧快照存在"。
- FSO（目录树）和 OBS（扁平）桶处理方式不同：OBS 直接按 key 全路径做对象 ID 映射；FSO 下目录本身也是带 object ID 的对象，需要额外对 `directoryTable` 做同样的 diff，并且要沿着 `parentObjectId` 链一级级把路径解析出来；为了不在删除一个有百万文件的目录时生成百万条 DELETE，还做了"祖先已删除则子孙的删除被隐式吸收"的折叠优化（但对祖先被**重命名**的情况不做折叠，因为重命名不蕴含子孙被删）。

**安全回退**：如果 DAG 遍历证明不了足够的覆盖（某些中间 SST 已经被更早的压缩 DAG 剪枝任务物理删除），或者原生库不可用，会自动退化成 `FullDiffComputer`——直接对比两个快照当前各自存活的 SST 文件集合做全量差异扫描，正确性优先，牺牲一些 I/O。还有 `ozone.om.snapshot.force.full.diff` 开关和单次请求级别的 `forceFullDiff` 参数可以手动强制走这条保守路径（CLI 里对应 `--force-full-diff`/`--disable-native-libs-diff`）。

**一个值得注意的发现**：仓库里其实还放着**另一套尚未接入生产路径的新一代 diff 引擎草案**（`SnapshotDiffDBDefinition`、`SnapshotDiffMetadataManager(Impl)`、`SnapshotDiffObjectInfo` 等，对应设计文档 `hadoop-hdds/docs/content/design/efficient-snapdiff.md`，目标是用顺序归并读取代替随机 `db.get()` 以进一步提速），但 `SnapshotDiffMetadataManagerImpl` 目前在代码库里**没有任何地方被实例化**，属于未启用的 scaffolding，不代表当前实际行为。

---

## 9. 客户端 API、CLI 与运维工具

- **Java 客户端**：`ObjectStore`（`hadoop-ozone/client`）暴露 `createSnapshot`/`renameSnapshot`/`deleteSnapshot`/`getSnapshotInfo`/`listSnapshot`/`snapshotCount`/`submitSnapshotDiff`/`cancelSnapshotDiff`/`listSnapshotDiffJobs` 等方法，全部是普通 Hadoop RPC 调用到 `OzoneManagerProtocol`，与 Ratis 无直接关系。
- **CLI**：`ozone sh snapshot {create|delete|info|list|rename|diff|listDiff|count}`，其中 `diff` 子命令同时承担提交/轮询报告/取消三种模式，并带 `--force-full-diff`/`--disable-native-libs-diff` 逃生舱口；`ozone admin om snapshot defrag` 用独立的 OM admin 协议手动触发碎片整理。
- **运维修复工具**：`ozone repair om snapshot chain` 可以在 OM 离线状态下直接打开 RocksDB，手工改写某个快照的 `pathPreviousSnapshotId`/`globalPreviousSnapshotId`——这是链表损坏时的最后手段，需要操作员自己先诊断出正确的前驱 ID 再手工打补丁，工具本身不做自动修复推导。
- `SnapshotListJSONServlet` 是挂在 OM 内嵌 HTTP 服务上的轻量 JSON 接口，供 Web UI/监控脚本直接拉取快照列表，不走 RPC。

---

## 10. 设计亮点与已知权衡小结

1. **创建是 O(1) 操作**：RocksDB checkpoint 硬链接 + 估算式 referencedSize，不扫全表。
2. **回收用"责任下移"而非"立即计算全局引用"**：每个快照只需要知道紧邻的前驱快照状态即可判断能不能回收，避免了维护全局引用计数的复杂度，代价是回收有延迟、需要后台服务持续推进链条。
3. **SnapDiff 用压缩 DAG 做增量比较，而不是逐 key diff**，是这套系统里复杂度最高但性价比也最高的设计，配有明确的全量扫描兜底路径保证正确性优先。
4. **两条独立链表（path + global）+ 严格的一致性校验**，是整个系统的核心不变量；配套的 `SnapshotChainRepair` 运维工具和链加载时的全表校验，说明链损坏是社区认真对待过的故障模式。
5. 几个目前代码里明确标注为**尚不完善/已知局限**的点：legacy 的 `OMSnapshotMoveDeletedKeysRequest` 协议疑似死代码但仍可被 Ratis 日志重放；SnapDiff 任务调度目前每个 OM 节点各跑一份，没有 leader-only 复制；新一代 SnapDiff 存储引擎已经写了代码和单测但尚未接入。
