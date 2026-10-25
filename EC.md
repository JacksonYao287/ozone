Ozone EC（纠删码）设计与实现详解

  0. 总体概念

  EC 把一个 key 的数据切分成 stripe（条带），每个 stripe 再切成固定大小（默认
  1MB，ecChunkSize）的 cell，按 RS(d, p) 这样的编码（ECReplicationConfig，如 rs-3-2-1024k）：d
  个数据 cell + p 个奇偶校验（parity）cell 组成一个 stripe，一次写入的整组 d+p
  个物理副本称为一个 block group，每个物理副本在组内有 1..d+p 的 EC index（index 1..d
  是数据块，d+1..d+p 是校验块）。与 Ratis 三副本不同，EC 的每个 datanode
  上存的是不同的数据，不是同一份数据的拷贝，所以必须靠纠删码数学关系（而不是简单的多数派读/写
  ）来保证可用性和一致性。

  EC 的编解码实现在 hadoop-hdds/erasurecode 模块：CodecRegistry 通过 ServiceLoader 加载
  RawErasureCoderFactory，原生 ISA-L 实现永远排在最前面（updateCoders 把 Native 工厂插到下标
  0），如果 native 库加载失败（LinkageError/Exception）则回退到纯 Java 的
  RSRawEncoder/RSRawDecoder（基于 Cauchy 矩阵 + Galois Field
  查表实现，RSUtil.genCauchyMatrix/initTables），两者编码产物在 wire format 上兼容。这个
  CodecUtil.createRawEncoderWithFallback/createRawDecoderWithFallback 是整个 EC
  读写路径、以及后面离线重建都复用的同一套编解码核心。

  ---

  1. EC 写路径（客户端驱动）

  入口判定：EC vs 副本写入的选择不是客户端临时决定的，而是在 bucket/key 创建时就已经写入了
  ReplicationConfig（EC 还是 RATIS）。RpcClient.createKeyOutputStream 只是读取 OM 返回的
  ReplicationConfig，如果是 EC 类型就构造 ECKeyOutputStream 而不是普通的 KeyOutputStream。

  分 cell / 攒 stripe：ECKeyOutputStream.write() 不断调用 handleWrite，把数据拷入
  ECChunkBuffers.dataBuffers[chunkIndex]（每个 buffer 大小 = ecChunkSize），填满一个 cell
  就移到下一个，直到 d 个数据 cell 全部填满（凑齐一个完整 stripe）才触发 generateParityCells()
  计算校验块，然后把整组 buffer 塞进一个
  ArrayBlockingQueue（ecStripeQueue）。真正的网络写入发生在一个独立的后台 flush
  线程里，数据摄入（writeOffset）和落盘确认（offset）是解耦的两个游标。

  奇偶校验计算与边界 stripe 处理（generateParityCells，这是整个写路径里最精巧的部分）：
  - parityCellSize 取第一个数据 cell 已写入的长度——这样可以正确处理最后一个不满的 stripe。
  - 扫描找到第一个"没填满"的 cell 下标 firstNonFullIndex，并断言它之后的 cell
    必须是完全空的（stripe 必须严格从左到右填满，不允许出现 cell[i] 空但 cell[i+1]
    有数据的情况）。
  - 把 firstNonFullIndex 之后的所有数据 cell 零填充到 parityCellSize，调用
    encoder.encode(dataBuffers, parityBuffers) 算出校验块。
  - 编码完成后再把零填充去掉，恢复数据 cell
    的真实长度——这样零填充只在编码数学计算中短暂存在，真正发送到 datanode 的数据 cell
    不含填充字节；但 parity cell 必须按完整长度发送（RS 数学要求如此）。

  写入 d+p 个 datanode（ECBlockOutputStreamEntry/ECBlockOutputStream）：一个 EC block group
  对应一个原始 Pipeline（含 d+p 个节点及各自的 replicaIndexes），客户端把它拆成 d+p 个"单节点
  pipeline"，每个对应一个 ECBlockOutputStream，只跟一个 DN 通信。一次 stripe
  提交的顺序是：先写 d 个数据 cell（writeDataCells，跳过空 cell），再写 p 个 parity
  cell（writeParityCells），然后对所有 d+p 个流统一发起 PutBlock（executePutBlock，这是 EC
  写入的"水位/提交点"，是按 stripe 粒度而非按 chunk 粒度）。注意：只有 replica 1 和 parity
  副本会携带完整 checksum 校验信息（代码注释 "Write checksum only to parity and 1st
  Replica"），但每个节点的 PutBlock 元数据里都会带上
  BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK，这是后续纠删恢复时判断"这个 block
  组的真实逻辑长度"的关键依据。

  SCM 侧 pipeline 分配：客户端走标准的 allocateBlock RPC，SCM 端由 WritableECContainerProvider
  决定复用已有的 open EC pipeline还是新建；新建走 ECPipelineProvider.create()，调用 placement
  policy 一次性选出 d+p 个互不相同的节点，按节点列表顺序分配 1..d+p 的 EC index，存在
  Pipeline.replicaIndexes 这个 map 上（EC index 不是通过 locationInfo
  列表顺序决定的，而是通过这个 map）。EC 假设一个 pipeline 只服务一个 container（不像 Ratis
  pipeline 可长期复用）。

  失败处理——没有"写时在线重建"：写入是按 stripe 粒度整体成败的。commitStripeWrite 在所有 d+p
  流的写请求都发出之后，统一检查 future 结果；只要有任何一个 DN 写失败，就把整个 pipeline
  和失败的 DN 加入 ExcludeList（EC 对"只排除 container 还是连 DN
  一起排除"比普通副本写更严格，要求必须是明确的 ContainerNotOpenException
  才算良性失败），回滚整个 stripe，重新向 OM/SCM 申请一个全新的 d+p 节点 pipeline 重试整个
  stripe，最多重试 maxECStripeWriteRetries 次，超限则直接抛异常让整个 key
  写入失败。也就是说：EC 写时不存在"接受部分写入，之后再离线补齐"的路径；一次提交点（stripe
  commit）必须是全量 d+p 都成功，否则整条 stripe
  作废重写。真正的"坏了之后再修"是后面介绍的离线重建，是写完之后才会发生的事。

  ---

  2. EC 读路径（含在线降级读 / Online Recovery）

  统一入口：这个版本代码库里没有单独的 ECKeyInputStream 类，通用的 KeyInputStream 对 EC
  和副本是无感的，它通过 BlockInputStreamFactoryImpl.create() 按 ReplicationType == EC 分流出
  ECBlockInputStreamProxy，每个 block group 对应一个 proxy 实例。

  健康路径 vs 重建路径的切换（ECBlockInputStreamProxy）：构造时先算出 expected（这个 block
  理论上应该有多少个数据 index 在用，取决于 block 长度）和 available（pipeline
  里实际报告了多少个不同 index 的健康节点），如果 available < expected 或已知某些位置失败，就
  直接走重建读；否则走健康路径。运行时故障切换才是"在线恢复"真正的触发点：健康路径读到
  BadDataLocationException（由下层 IOException——无论是网络错误、容器找不到，还是 checksum
  校验失败，统统被当作"这个 index 坏了"）时，failoverToReconstructionRead() 把坏节点加入
  failedLocations，关闭旧 reader，切换成重建 reader，从上次读到的位置继续读。这个切换每个
  proxy 实例只发生一次（健康→重建），如果重建 reader
  自己再失败，直接把异常抛给调用者，不会再往上找别的补救路径。

  健康路径的 stripe 定位数学（ECBlockInputStream）：stripeSize = ecChunkSize *
  d，当前偏移量对应的数据 index = (position / ecChunkSize) % d，直接打开一个指向该 index 所在
  DN 的单节点 STANDALONE pipeline 读（和普通 Ratis/Standalone
  读没有本质区别），没有任何解码开销。还支持跨多 cell 的并行读（readCellsInParallel，受
  OzoneClientConfig#isEcParallelReadEnabled() 控制）、以及"spare 副本"重试（如果同一个 EC
  index 报告了多个物理位置，先换个位置重试，再决定是否真的要升级到降级重建）。

  在线重建的核心算法（ECBlockReconstructedStripeInputStream，这是整个"在线恢复"最关键的一个类
  ，完全是客户端内存里的事情，不会往任何 datanode 写入新副本）：
  - EC index 0-based：数据 index [0, d)，校验 index [d, d+p)。
  - selectIndexes()：先找出真正缺失的数据 index（missingIndexes），健康的数据 cell 直接加入
    selectedIndexes（不需要解码），再从可用的校验 cell 里随机挑选 n = missingIndexes.size()
    个加入——核心不变式是：恢复 n 个缺失数据块，永远只需要读取恰好 d 个输入（(d-n) 个健康数据
    cell + n 个校验 cell），这正好满足 Reed-Solomon 需要 k=d 个符号才能解出任意缺失量（≤p
    个）的数学要求。如果可选 index 总数 < d，直接抛
    InsufficientLocationsException（硬失败，恢复不了，因为丢失数据超过了纠删码容错能力 p）。
  - 缓冲区装配（assignBuffers）：调用方传入 d 个 buffer（每个数据 index 一个），对应缺失 index
    的 buffer 直接变成 decoderOutputBuffers（解码结果直接写进调用方的 buffer，零拷贝），健康
    index 的 buffer 变成 decoderInputBuffers。
  - 从各个源节点并行读入输入 buffer（线程池 EC-reconstruction-%d，和健康路径一样是单节点
    STANDALONE pipeline 直接读），调用 RawErasureDecoder.decode(decoderInputBuffers,
    erasedIndexes, decoderOutputBuffers) 完成解码。
  - checksum 失败和"位置不可达"在 EC 层是无差别处理的：下层 ChunkInputStream 的 checksum
    校验失败抛出的 OzoneChecksumException 本质上也是个 IOException，EC 层统一包装成
    BadDataLocationException，一律触发"排除该 index，换用校验块重建"的逻辑。
  - 三层重试：① 同 index 的 spare 节点切换；② proxy 层从健康 reader 切到重建
    reader（仅一次）；③ 重建 reader 内部，如果选中的某个校验块本身也读不到，重新 init()
    换一个校验块再试，直到 hasSufficientLocations() 不满足为止（硬失败）。

  ---

  3. SCM 端：离线恢复的"检测与调度"

  这是后台/离线恢复的大脑，职责被清晰地分成两层：判断状态（纯只读分类）和决定动作（下发命令）
  。

  3.1 健康状态判定（ECContainerReplicaCount）

  把 container 的每个 EC index 分到 healthyIndexes / unHealthyIndexes / decommissionIndexes /
  maintenanceIndexes 四个桶里，派生出几个关键判定：
  - unrecoverable：健康+退役中+维护中的 index 数 < d（即使努力也凑不出 d 个能解码的源了）。
  - missing（比 unrecoverable 更糟）：连不健康的副本都算上还 < d，意味着数据彻底没了。
  - under-replicated：没能凑齐完整的 d+p 个健康 index（或者算上维护节点的冗余预算仍不够）。
  - over-replicated：某个 index 有超过 1 份健康拷贝。

  ECReplicationCheckHandler 基于此生成
  UnderReplicatedHealthResult/OverReplicatedHealthResult，再喂进
  ReplicationQueue。ECMisReplicationCheckHandler（检查机架布局是否违反
  SCMContainerPlacementRackScatter 策略）作为更低优先级的检查链接在后面——一个 container
  不会同时被标记为 under-replicated 和
  mis-replicated，数量不对时优先修数量，数量对了才检查布局。

  3.2 Pipeline/Placement：EC 专用的机架打散策略

  ECPipelineProvider 每次都是为一个 container/一次写入新建一组 d+p 节点的一次性 pipeline（不像
  Ratis pipeline 可长期复用）。EC 专用的 placement policy 默认是
  SCMContainerPlacementRackScatter（而非 Ratis 默认的
  SCMContainerPlacementRackAware），目标是用尽可能多的机架（min(机架数, 副本数)）来打散 d+p
  个副本，以降低单机架故障打掉超过 p 个副本的概率；选不满机架时有宽松 fallback（允许退化到已用
  机架，只要不比当前更差就不报错），避免在小集群里直接卡死恢复流程。写入时和重建时用的是同一个
  placement policy 实例，保证两处逻辑一致。

  3.3 三个处理器：Under / Over / Mis Replication Handler

  - ECUnderReplicationHandler（离线重建的触发核心）：区分三种子情况分别处理——①真正缺失的
    index（processReconstruction，发 ReconstructECContainersCommand）；②只存在于"退役中"节点的
    index（优先直接 1 对 1 拷贝，只有在源节点负载过高且配置开启时才改用多源纠删重建以降低源节
    点读压力）；③只存在于"维护中"节点且违反冗余预算的 index。processReconstruction 要求至少有
    d 个可用源才会尝试；有一个"关键恢复"判断——如果这是最后一次能救这个 container
    的机会（missingIndexes.size() ==
    parity），即使目标节点过载也硬着头皮发；否则宁可推迟，避免往已经高负载的节点继续加压。
  - ECOverReplicationHandler：只在 HEALTHY+IN_SERVICE 副本里选多余的删，并且删除前二次确认该
    index 至少还剩 2 份，防止误删导致瞬间变成 under-replicated。
  - ECMisReplicationHandler：只在"数量没问题、只是布局不对"且没有任何其它 pending
    操作时才触发，修复方式是"先拷贝到更合适的机架，再靠 over-replication handler
    把旧的多余副本删掉"，并且会模拟拷贝后的效果，如果布局并不会变好就直接跳过（防止在小集群上
    无意义地反复搬运）。

  3.4 调度节奏

  ReplicationManager 每 300s 做一次全量 container 扫描（processAll），把结果塞进
  ReplicationQueue（under-replicated 用按"加权冗余度"排序的优先队列，over-replicated 用普通
  FIFO）；两个独立的后台线程 UnderReplicatedProcessor/OverReplicatedProcessor 各自以 30s
  周期从队列里取出任务下发命令，和全量扫描的节奏解耦。节点状态变化（比如开始退役）会调用
  notifyNodeStateChange() 提前唤醒扫描线程，不用等满 300s。

  3.5 限流

  - 单个 DN 上排队的命令有配额（datanode.replication.limit 默认 20），一个 EC 重建命令按权重 3
    倍计数（因为更吃 CPU/IO）。
  - 删除命令单独限流（默认 40）。
  - 全集群层面有个基于节点数估算的"inflight 复制总量上限"（节点数 × 单节点配额 × 0.75）。
  - 可选的全局纠删重建数量硬上限（GlobalReconstructionTracker，默认关闭），按"命令"而不是按"缺
    失 index 个数"计数一个槽位。
  - 退役场景下专门的负载因子（默认 0.9）决定是否把 1 对 1 拷贝切换成多源重建。

  3.6 退役/维护复用同一套逻辑

  节点进入 DECOMMISSIONING 或 ENTERING_MAINTENANCE 后，它上面的副本会被
  ECContainerReplicaCount 归到 decommissionIndexes/maintenanceIndexes 桶，自然就让该 container
  被判定为 under-replicated，走的是完全相同的 ECUnderReplicationHandler 路径——SCM
  没有为退役单独写一套流程，DatanodeAdminMonitorImpl
  只是只读地轮询健康检查结果来决定"退役能不能继续往下走"，真正的补副本动作还是常规的
  ReplicationManager 扫描/队列机制在做。

  ---

  4. Datanode 端：离线重建的实际执行

  SCM 下发的 ReconstructECContainersCommand（携带源节点+index 列表、目标节点+缺失 index
  列表、EC 配置）经由标准的 CommandDispatcher 路由到
  ReconstructECContainersCommandHandler，包装成 ECReconstructionCoordinatorTask
  丢进和普通容器复制共用的 ReplicationSupervisor 线程池（按 containerId 去重，防止同一个
  container 被并发重建两次）。

  ECReconstructionCoordinator 的完整算法（这是离线恢复真正的执行引擎）：
  1. 构造一个临时的（不注册到 SCM）输入 pipeline，向所有源节点 ListBlock，组装出 blockID →
     BlockData[d+p] 的映射；丢弃"孤儿 stripe"——如果某个 block
     不是在所有存活源节点上都有记录（可能是客户端写到一半崩了，PutBlock
     没发全），就整个丢弃不重建，避免用垃圾数据重建。
  2. 对每个 block 取所有源节点上报的 BlockGroupLength
     的最小值作为有效长度，避免恢复出超过各方公认的"实际写入量"的数据。
  3. 在每个目标节点上创建 RECOVERING 状态的空 container（立即发送一次 ICR，让 SCM
     马上知道有个正在重建的新副本，即便还是空的）。
  4. 逐 block 重建：复用和客户端在线降级读完全相同的 ECBlockReconstructedStripeInputStream
     类（通过 setRecoveryIndexes() 切到离线模式），强制开启 checksum
     校验；一次命令里要重建的所有缺失 index，在同一次 stripe 解码里一次性全部算出来（不是一个
     index 读一遍源数据），读出来的恢复结果流式写给各自的目标
     ECBlockOutputStream；完成后给每个目标发 PutBlock，并把原始的 BlockData（含原始 chunk
     checksum）带过去，这样新副本的元数据和其它副本保持一致。
  5. 全部 block 处理完后，对所有目标 container 执行 Close：RECOVERING → CLOSING →
     CLOSED，每次状态转换都发 ICR；只有从 RECOVERING 转 Close 时才会额外触发一次异步的后台
     ContainerDataScanner 扫描（和日常的 container checksum 巡检复用同一套机制，不是 EC
     专属的校验，也不会阻塞任务判定为完成）。
  6. 任何一步失败（源读失败、目标写失败、源数量不足 d 等），都会把该命令下已经创建成功的所有
     RECOVERING 目标 container 删除干净，不留半成品，然后让任务失败——SCM
     之后会在下一轮扫描里发现副本依旧缺失，重新下发命令（datanode 侧本身不做重试）。

  资源控制上，ECReconstructionCoordinator 内部有两个独立于 ReplicationSupervisor
  的专用线程池（读池/写池），用 SynchronousQueue + CallerRunsPolicy 做天然背压；跨 container
  的并发度则受 ReplicationSupervisor 共享线程池大小限制。

  ---

  5. 总结：几个贯穿全局的设计思想

  1. 解码引擎只有一套：RawErasureEncoder/RawErasureDecoder（ISA-L 优先，纯 Java
     兜底）被写路径、在线读恢复、离线重建三处统一复用，没有重复实现。
  2. 在线恢复和离线恢复是完全不同量级的两件事：在线恢复（ECBlockReconstructedStripeInputStream
     被客户端直接使用）只是"这次读请求凑合算出来给你"，纯内存操作，不持久化、不通知
     SCM；离线恢复（同一个类被 datanode 端的 ECReconstructionCoordinator
     复用）才是真正"造出一个新副本、写盘、关闭 container、上报 SCM"的动作。
  3. 写时严格，读/修时宽容：写入要求 d+p 全部成功才提交（失败即整 stripe 重做换新
     pipeline），但读取和后台修复都容忍"先凑合用，边读边发现问题边换"的渐进式降级策略。
  4. EC index 的身份是通过 Pipeline.replicaIndexes 这个 map
     显式携带的，而不是隐含在数据结构的顺序里，这个设计贯穿写入提交、SCM
     副本计数、以及重建命令的所有环节。
  5. SCM 侧刻意把"退役/维护"当成"under-replication
     的一种"而不是单开一套流程，大幅简化了状态机，所有不健康状态最终都收敛到同一个
     ECUnderReplicationHandler。

  如果你想继续深入某一块（比如具体某个配置项的默认值来源、某个 proto 定义、或者集成测试
  TestECContainerRecovery 里验证的具体场景），我可以让对应的 agent 继续挖。


⏺ Teammate @ec-scm-explorer finished
  I've sent the complete structured analysis to team-lead covering all 7 requested areas:
  ECPipelineProvider's pipeline creation, the SCMContainerPlacementRackScatter placement
  policy, EC health checkin…

⏺ This is just the idle confirmation for the SCM-side report that I already received and
  synthesized into the full explanation above. No further action needed — all four research
  threads are complete and the comprehensive write-up has been delivered.

※ recap: I researched and explained Apache Ozone's Erasure Coding design (write, read,
  online/offline recovery) in detail, pulling from four parallel code-exploration reports.
  That explanation is complete; next step is answering any follow-up questions you have.

❯ 更加细致地讲解一下，尽最大可能深入实现细节

⏺ 我将直接阅读几个最核心的源码文件，提取精确的实现细节（算法、数据结构、边界条件），在此基础上
  写一份更深入的讲解。

  Read 8 files (ctrl+o to expand)

⏺ 基于刚才直接阅读的核心源码（ECKeyOutputStream、ECBlockOutputStreamEntry、RSRawEncoder、ECBlo
  ckInputStreamProxy、ECBlockReconstructedStripeInputStream、ECContainerReplicaCount、ECRecons
  tructionCoordinator、ECUnderReplicationHandler），我把讲解下钻到具体变量、分支条件和边界情况
  这一层。

  Ozone EC 实现深度解析（代码级）

  一、写路径：逐行拆解 ECKeyOutputStream

  1.1 双游标模型：writeOffset vs offset

  ECKeyOutputStream 维护两个长度计数器，这是理解整个写路径异步性的关键：
  - writeOffset：调用方往 write() 塞进来的总字节数，在 write() 方法末尾 writeOffset += len
    更新（第160行），是同步的，调用方写完就立即增加。
  - offset：真正已经确认写到 DN 上的字节数，只在 writeToOutputStream()（373行）里当 !isParity
    时 offset += writeLen，这个调用发生在后台 flush 线程里。

  close() 方法里有一个硬断言（480-482行）：Preconditions.checkArgument(writeOffset == offset,
  ...)——即关闭时两个计数器必须完全相等，这保证了"调用方以为写完的数据"和"后台线程确认落盘的数
  据"完全一致，没有任何中间丢失或重复计数的字节。

  1.2 生产者-消费者解耦：为什么要用队列和独立线程

  构造函数里 (120-123行) 启动了一个专门的 flushFuture 线程跑 flushStripeFromQueue()，write()
  调用方线程只负责把数据拷进 ecChunkBufferCache，攒够一个 stripe 后 addStripeToQueue() 塞进
  ArrayBlockingQueue<ECChunkBuffers> ecStripeQueue（容量 = config.getEcStripeQueueSize()）。

  addStripeToQueue()（508-531行）有个很讲究的细节：它用 offer(stripe, 1, TimeUnit.SECONDS)
  轮询塞入，每次塞入失败就先检查
  flushFuture.isDone()——如果后台线程已经提前退出（说明它遇到异常崩溃了），就调用
  flushFuture.get() 把异常重新抛出来传给调用方线程，而不是让调用方在一个已经没人消费的队列上死
  等。这是一个典型的"生产者检测消费者已死"模式。

  EOFDummyStripe 和 CheckpointDummyStripe 是两个哨兵对象（继承 ECChunkBuffers 但内部 buffer 为
  null），通过队列传递控制信号（EOF 结束、checkpoint
  版本号记录），而不是额外开一个控制通道——复用同一个 BlockingQueue 保证了顺序性（checkpoint
  必须排在它之前所有数据 stripe 之后被处理）。

  1.3 Cell 填充与 Stripe 边界检测（handleWrite，336-356行）

  int bufferRem = ecChunkBufferCache.dataBuffers[chunkIndex].remaining();
  final int writeLen = Math.min(len, Math.min(bufferRem, ecChunkSize));
  int pos = ecChunkBufferCache.addToDataBuffer(chunkIndex, b, off, writeLen);
  if (pos == ecChunkSize) {
    chunkIndex++;
    if (chunkIndex == numDataBlks) {
      generateParityCells();
      addStripeToQueue(ecChunkBufferCache);
      ecChunkBufferCache = new ECChunkBuffers(...); // 全新对象，不是复用！
      chunkIndex = 0;
    }
  }
  注意每次攒满一个 stripe 后，ecChunkBufferCache 是重新 new 出来的，不是 clear
  旧的复用——因为旧的 ECChunkBuffers 对象（连同里面装好的 d+p 个
  ByteBuffer）已经被放进队列，后台线程还要用它继续处理（ECChunkBuffers.release() 才是真正把
  buffer 还给 ByteBufferPool，发生在 flushStripeFromQueue 消费完之后，548行
  stripe.release()）。这是典型的"所有权转移"模式，避免了并发修改同一组 buffer 的 race。

  1.4 generateParityCells() 的精确数学（265-310行）

  这是整条写路径里最精妙的一段，逐行翻译其含义：

  final int parityCellSize = dataBuffers[0].position();
  关键洞察：代码注释写明 parityCellSize = min(ecChunkSize, stripeSize) =
  dataBuffers[0].position()。这依赖一个不变式——写入永远严格按 cell 0, 1, 2... 顺序填满，所以
  cell[0] 的当前写入量就代表了这个 stripe 里"每个 parity cell 应该编码出多大"。如果 cell[0]
  写满了（等于 ecChunkSize），说明整个 stripe 都是满的；如果只写了一部分（close()
  触发的最后一个不完整 stripe），parityCellSize 就是这部分长度。

  int firstNonFullIndex = dataBuffers.length; // 默认值=d，表示"全部满"
  for (int i = 0; i < dataBuffers.length; i++) {
    if (dataBuffers[i].position() != ecChunkSize) {
      firstNonFullIndex = i;
      firstNonFullLength = dataBuffers[i].position();
      break;
    }
  }
  for (int i = firstNonFullIndex + 1; i < dataBuffers.length; i++) {
    Preconditions.checkState(dataBuffers[i].position() == 0, ...);
  }
  这个断言是防御性的不变式校验：一旦发现某个 cell 不满，它后面所有 cell
  必须严格为空——不允许出现"cell[1] 写了一半，cell[2] 却有数据"这种乱序，因为 handleWrite
  本身就是严格顺序填充的，这个断言只是用来在出现 bug 时 fail-fast。

  for (int i = firstNonFullIndex; i < dataBuffers.length; i++) {
    padBufferToLimit(dataBuffers[i], parityCellSize);
  }
  for (ByteBuffer b : parityBuffers) { b.limit(parityCellSize); }
  for (ByteBuffer b : dataBuffers) { b.flip(); }
  encoder.encode(dataBuffers, parityBuffers);
  // 编码完之后,把刚才的零填充"擦掉"(只是改limit,不是真的清零):
  if (firstNonFullIndex < dataBuffers.length) {
    dataBuffers[firstNonFullIndex].limit(firstNonFullLength);
  }
  for (int i = firstNonFullIndex + 1; i < dataBuffers.length; i++) {
    dataBuffers[i].limit(0);
  }
  padBufferToLimit（568-575行）用 Arrays.fill(buf.array(), pos, limit, (byte)0)
  真实写入零字节，然后 encoder.encode() 用包含零填充的完整数据计算出
  parity。编码完成后，仅仅是把 limit()
  调回真实长度——底层数组里那些零字节依然存在，只是不会被后续的 writeDataCells()
  读取/发送（ByteBuffer.limit() 决定了
  get()/写出操作能读到哪里）。这是一个零拷贝的"伪装"技巧：既要用零填充满足 RS
  编码算法要求所有输入等长，又不想把这些零字节真的发给 DN 浪费带宽。

  1.5 写入提交的精确顺序与 PutBlock 的 checksum 分配

  flushStripeToDatanodes（552-566行）：
  for (int i = 0; i <= maxRetry; i++) {
    writeDataCells(stripe);
    writeParityCells(stripe);
    if (commitStripeWrite(stripe) == SUCCESS) return;
    rollbackAndReset(stripe);
  }
  throw new IOException("Completed max allowed retries...");

  commitStripeWrite（204-241行）里有个容易被忽略的细节——两阶段失败检测：
  1. 先检查 streamsWithWriteFailure()（WriteChunk 阶段的 future），失败就直接返回
     FAILED，不会去调用 executePutBlock。
  2. 只有 WriteChunk 全部成功，才继续调用 executePutBlock，然后再检查
     streamsWithPutBlockFailure()。

  isLastStripe 的判断条件很讲究：
  final boolean isLastStripe = streamEntry.getRemaining() <= 0 ||
      stripe.getLastDataCell().limit() < ecChunkSize;
  两个条件取或：① 这个 block group 的总容量已经用完（getRemaining() <= 0，说明这个 stripe
  之后还会开一个新的 block group）；② 当前 stripe 本身就是不满的最后一个
  stripe（getLastDataCell().limit() < ecChunkSize，对应 close() 触发的
  flush）。两种情况都要告诉 DN"这是这个 block 的最后一次 PutBlock，可以关闭了"。

  getFailedStreams（306-330行）里有个很微妙的跳过逻辑：
  if (!forPutBlock && stream.getWrittenDataLength() <= 0) {
    continue; // 跳过未写过数据的流的WriteChunk失败检查,但PutBlock检查不跳过
  }
  注释解释得很清楚：最后一个不满的 stripe 里，有些数据 cell
  根本没数据（dataCells[i].limit()==0），对应的流压根没发
  WriteChunk，这种情况不该被当成"写失败"。但即便某个流没写数据，它依然要收到 PutBlock（这是
  HDDS-6794 修复的问题——为了在 DN 上也创建出一个空的 container/block 记录，保证 d+p
  个节点的元数据一致）。

  1.6 Checksum 只写到部分副本：ECBlockOutputStreamEntry.calculateChecksum()

  int currentIdx = blockOutputStreams[0].getContainerBlockData().getChunksCount();
  for (ECBlockOutputStream stream: blockOutputStreams) {
    if (stream.getContainerBlockData().getChunksCount() > currentIdx - 1) {
      chunkInfos.add(stream.getContainerBlockData().getChunksList().get(currentIdx - 1));
    }
  }
  它以流 0（第一个数据副本）已经写入的 chunk 数量作为基准索引 currentIdx，然后遍历所有 d+p
  个流，只收集那些"chunk 数量达到了这个基准"的流的最后一个 chunk的 checksum，拼接成一个总的
  ByteString。换句话说，这个 checksum 是"当前这次 stripe 提交，在每个有写数据的流上新增的那个
  chunk 的 checksum 的拼接"，用来在 executePutBlock
  时统一下发，而具体哪些节点会真正采用它取决于 ECBlockOutputStream.executePutBlock
  内部逻辑（README 注释提到"仅对第一个节点和 parity 节点设置
  checksum"）——目的是避免对每个数据副本重复存储同一份 checksum 数据，因为纠删恢复时真正需要
  checksum 校验的是读取路径（ChunkInputStream），而各副本的 chunk
  内容本身在正常情况下已经一致。

  1.7 失败回滚：rollbackAndReset（163-179行）

  offset -= Arrays.stream(dataBuffers).mapToInt(Buffer::limit).sum();
  failedStreamEntry.resetToFirstEntry();
  failedStreamEntry.resetToAckedPosition();
  blockOutputStreamEntryPool.discardPreallocatedBlocks(-1,
  failedStreamEntry.getPipeline().getId());
  failedStreamEntry.close();
  注意 offset 回滚量是按数据 cell 的 limit 求和（不含 parity），这正好对应之前
  writeToOutputStream 里 offset += writeLen（仅对非 parity
  累加）的逆操作——两处必须精确对称，否则前面提到的 close() 里 writeOffset == offset
  断言就会失败。discardPreallocatedBlocks(-1, pipelineId) 把同一个失败 pipeline 下所有预分配的
  block（不仅仅是当前这一个）都丢弃——因为整个 pipeline 已经被加入
  ExcludeList，继续用它预分配的其它 block 毫无意义。

  ---

  二、RS 编解码数学内核：RSRawEncoder/RSUtil

  encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
  RSUtil.genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
  gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
  RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
      getNumDataUnits() * getNumDataUnits(), gfTables);
  这是标准的 Cauchy-RS 构造：先生成一个 (d+p) × d 的 Cauchy 矩阵（矩阵任意子方阵都可逆，这是
  RS 码可以从任意 d 个幸存符号恢复出原始 d 个符号的数学基础），再把矩阵系数通过 initTables
  展开成 Galois Field GF(2^8) 的预计算乘法表（gfTables，大小 d × (d+p) × 32），这样实际编码时
  RSUtil.encodeData 就是纯粹的查表+异或，不需要在热路径上做昂贵的 GF 乘法运算——这是 ISA-L 和
  Intel 优化版本能做到高吞吐的关键技巧，纯 Java 版本复用了同样的算法结构（代码注释说比
  HDFS-RAID 原版快 5 倍）。

  解码时（RawErasureDecoder.decode）本质上是从编码矩阵里抽取幸存行、求逆，构造出一个新的解码矩
  阵，再用同样的 GF 表运算机制把 d 个幸存符号（数据+parity
  混合）乘回去解出丢失的符号——这也是为什么重建时"必须凑够恰好 d 个输入"的数学原因：RS
  解码等价于解一个 d 元线性方程组，少于 d 个方程无解，多于 d 个是冗余的。

  encoder.release()（ECKeyOutputStream.close()
  487行）、decoder.release()（ECBlockReconstructedStripeInputStream.close()
  741-745行）专门用来释放 ISA-L native coder 可能持有的本地（堆外）资源——这是 JNI
  封装常见的显式生命周期管理需求，纯 Java 版本这个方法通常是空操作。

  ---

  三、读路径：ECBlockInputStreamProxy 与重建流的精确状态机

  3.1 "是否需要重建"的判定公式

  public static int expectedDataLocations(ECReplicationConfig repConfig, long blockLength) {
    return (int)Math.min(Math.ceil((double)blockLength / repConfig.getEcChunkSize()),
  repConfig.getData());
  }
  public static int availableDataLocations(Pipeline pipeline, int expectedLocs) {
    Set<Integer> locations = new HashSet<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      int index = pipeline.getReplicaIndex(dn);
      if (index > 0 && index <= expectedLocs) locations.add(index);
    }
    return locations.size();
  }
  expectedDataLocations 处理了"短 block"的情况：如果一个 block 总共只有 0.5 个 ecChunkSize
  那么大，它理论上只用到了 1 个数据 index，不需要 d 个全部存在。reconstructionReader =
  available < expected || !failedLocations.isEmpty()——即便 available ==
  expected（看起来够用），只要 failedLocations 里已经记录过坏节点（来自上一次 readPositioned
  共享的黑名单），也要走重建路径，不会二次尝试已知失败的位置。

  3.2 read() 的异常处理：只允许一次 failover

  } catch (IOException e) {
    if (reconstructionReader) {
      XceiverClientManager.getXceiverClientMetrics().incECReconstructionFailsTotal();
      throw e;  // 已经在重建模式了,没有再退路
    }
    if (e instanceof BadDataLocationException) {
      failoverToReconstructionRead(((BadDataLocationException) e).getFailedLocations(),
  lastPosition);
      buf.reset();
      totalRead += read(buf);  // 递归重试,这次会进入reconstructionReader==true分支
    } else {
      throw e;  // 非BadDataLocationException(比如非IO层面的bug)直接抛
    }
  }
  buf.mark()/buf.reset()（176、201行）的配对非常关键：在每次内层 blockReader.read(buf)
  调用前标记 buffer 位置，一旦这次 read 抛异常，reset() 把 buffer
  指针退回到这次失败读取开始前的位置，再带着 lastPosition（在抛异常前记录的流位置）调用
  failoverToReconstructionRead，新 reader 会 seek(lastPosition)
  后重新读——保证了"已经成功读到调用方 buffer
  里的字节不受影响，只重新读这次失败的部分"，而不是把整个 read 请求推倒重来。

  3.3 readPositioned：每次定位读都是全新 proxy 实例，但共享"黑名单"

  protected int readPositioned(long position, ByteBuffer buffer) throws IOException {
    try (ECBlockInputStreamProxy reader = new ECBlockInputStreamProxy(repConfig, blockInfo,
        xceiverClientFactory, refreshFunction, ecBlockInputStreamFactory, config,
  failedLocations)) {
      reader.seek(position);
      return reader.read(buffer);
    }
  }
  这里把自己的 failedLocations（一个 CopyOnWriteArrayList）传给新建的 reader，意味着并发的
  positional read（常见于预读/多线程随机读场景）共享同一份"已知失败节点"黑名单——一旦某次 pread
  发现某个 DN 坏了，所有其它并发的 pread 下次构造 reader 时 setReaderType()
  就会直接判定需要重建读，不用每个线程各自踩一次坑才发现。

  3.4 ECBlockReconstructedStripeInputStream 的索引选择算法（selectIndexes，788-835行）

  在线读（非 offline recovery）分支：
  missingIndexes.addAll(failedDataIndexes);
  missingIndexes.retainAll(dataIndexes);  // 只保留"数据"index里失败的那些

  SortedSet<Integer> dataAvailable = new TreeSet<>(dataIndexes);
  dataAvailable.removeAll(failedDataIndexes);
  SortedSet<Integer> parityAvailable = new TreeSet<>(parityIndexes);
  parityAvailable.removeAll(failedDataIndexes);

  selectedIndexes.addAll(dataAvailable);  // 健康数据cell直接选中,不需要解码
  candidates = parityAvailable;
  required = dataIndexes.size() - dataAvailable.size();  // 需要补几个parity
  再调用 selectInternalInputs(candidates,
  required)（523-551行），这个方法有个增量复用的巧妙设计：
  SortedSet<Integer> selected = new TreeSet<>();
  for (int i : available) {
    if (decoderInputBuffers[i] != null) {
      selected.add(i);  // 复用上一次init()已经打开过的parity流
    }
  }
  List<Integer> candidates = new ArrayList<>(available);
  candidates.removeAll(selected);
  Collections.shuffle(candidates);
  selected.addAll(candidates.stream().limit(count - selected.size()).collect(toSet()));
  只有在需要补充新的 parity 流时才随机 shuffle 挑选——如果某个 parity
  流已经打开过（decoderInputBuffers[i] != null，说明是重试场景，之前 init
  过），优先沿用它，不重新开流，减少不必要的网络连接churn。随机挑选（而不是固定总挑第一个
  parity）是为了负载均衡——避免集群里所有的 EC 重建读都集中打到 parity 块 index d+1
  所在的那组节点上。

  hasSufficientLocations()（707-729行）的判定是 selectedIndexes.size() >=
  dataIndexes.size()——注意这里比较的是候选集合的大小和 d，而不是直接检查 missingIndexes.size()
  <= parity，这是因为 selectIndexes() 已经把"能凑够多少"体现在 selectedIndexes
  里了，如果凑不够 d 个（说明连累加了所有 parity 都不够补齐缺的数据块），这个布尔值自然为
  false。

  3.5 read() 的重试循环与"为什么只对齐到整 stripe"

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos % getStripeSize() != 0) {
      throw new IOException("Requested position " + pos + " does not align with a stripe
  offset");
    }
    super.seek(pos);
  }
  这个类只允许按 stripe 边界 seek——因为它的读取单位就是"一整个
  stripe"（readStripe/recoverChunks 一次性填满调用方传入的 d 个 buffer），不支持 stripe
  内部的字节级随机定位，这是设计上刻意简化的边界：粒度更细的字节级定位由上层的
  ECBlockInputStream（健康路径）负责，重建路径只负责"按 stripe 批量重建"。

  read(ByteBuffer[] bufs)（365-421行）的 try/catch 重试逻辑：
  while (true) {
    try {
      assignBuffers(bufs);
      clearInternalBuffers();
      setBufferReadLimits(toRead);
      loadDataBuffersFromStream();
      break;
    } catch (IOException e) {
      seek(getPos());               // 把流位置倒回本次stripe开头(重新打开必要的streams)
      for (ByteBuffer b : bufs) b.position(0);  // 重置调用方buffer
      init();                        //
  重新选择index集合(此时坏的index已经进了failedDataIndexes)
    } catch (InterruptedException ie) { ... }
  }
  loadDataBuffersFromStream 内部某个 index 读失败后，会在 readIntoBuffer（638-660行）里先尝试
  shouldRetryFailedRead(ind)（继承自 ECBlockInputStream，检查有没有 spare
  位置）就地重试，实在不行才抛 BadDataLocationException 导致外层 IOException，触发这里的"重新
  init() 换一组 index"。init() 内部的 selectIndexes()
  每次都会重新跑一遍随机挑选逻辑，这就是为什么"第一次选中的 parity
  块本身也读失败时，会自动换另一个 parity 块重试"的实现基础。

  3.6 padBuffers/setBufferReadLimits：处理非整 stripe 大小的 block 尾部

  这两个方法专门处理一个 block 的最后一个不完整 stripe（比如一个 10MB 的
  block，ecChunkSize=1MB，d=3，最后一个 stripe 可能只有 1MB 数据）：setReadLimits 根据
  fullChunks（完整填满的 cell 数）精确计算每个 index 的 buffer limit——对于下标恰好等于
  fullChunks 的那个 cell 设为 partialChunkSize，之后的数据 cell 直接设
  limit(0)（相当于空），这样保证解码器不会因为某些"根本不存在"的尾部数据 cell 是空 buffer
  而算错。

  ---

  四、SCM 判定逻辑：ECContainerReplicaCount 的集合代数

  这个类本质上是在玩集合运算，所有判定方法都建立在四个 Map<Integer /*index*/, Integer
  /*count*/> 上：healthyIndexes、unHealthyIndexes、decommissionIndexes、maintenanceIndexes。

  4.1 为什么 isUnrecoverable 和 isMissing 要分开两级

  public boolean isUnrecoverable() {
    Set<Integer> distinct = healthyReplicas();  // healthy ∪ decommission ∪ maintenance
    return distinct.size() < repConfig.getData();
  }
  public boolean isMissing() {
    Set<Integer> distinct = healthyReplicas();
    distinct.addAll(unHealthyIndexes.keySet());  // 再并上unhealthy
    return distinct.size() < repConfig.getData();
  }
  isUnrecoverable只统计"能提供有效数据的"副本（healthy/decommission/maintenance
  节点上的，这些节点虽然状态特殊但数据是完好的，CLOSED 状态可读），不算
  UNHEALTHY（数据本身被标记损坏，不可信）。如果连这些加起来都 <
  d，说明哪怕排除坏数据，剩下的好数据也凑不够 d 份来解码——真正不可恢复。而 isMissing
  更悲观：即使把"状态为 UNHEALTHY 但好歹还存在于某处"的副本也算进去都凑不够 d，说明这个
  container 在集群里物理上存在的拷贝数都不够，属于最坏情况。两者的差值就是"有没有价值去尝试一
  次读取看看能不能用那些 UNHEALTHY 副本抢救"的操作空间。

  4.2 isSufficientlyReplicated 的维护冗余公式

  if (hasFullSetOfIndexes(onlineIndexes)) return true;
  Map<Integer, Integer> healthy = new HashMap<>(onlineIndexes);
  maintenanceIndexes.forEach((k, v) -> healthy.merge(k, v, Integer::sum));
  return hasFullSetOfIndexes(healthy)
      && onlineIndexes.size() >= repConfig.getData() + remainingMaintenanceRedundancy;
  第二个条件除了"健康+维护凑齐 d+p 个不同 index"之外，还要求纯健康（不含维护）的 index 数量
  onlineIndexes.size() 至少达到 d + remainingMaintenanceRedundancy。举例
  RS(3,2)，remainingMaintenanceRedundancy=1：即使 5 个 index 凑齐了（3 healthy + 2
  maintenance），onlineIndexes.size()=3，需要 >= 3+1=4才算"足够"——3 不满足，所以会被判定为
  under-replicated，触发 processMaintenanceOnlyIndexes
  去多做一份拷贝。这保证了"维护节点随时可能真的消失"的情况下，还能保留至少 1
  份冗余容忍度（remainingMaintenanceRedundancy
  默认值语义上是"允许多少个维护副本在不触发额外拷贝的前提下同时存在"）。

  4.3 getMaxMaintenance() 与 additionalMaintenanceCopiesNeeded

  private int getMaxMaintenance() {
    return Math.max(0, repConfig.getParity() - remainingMaintenanceRedundancy);
  }
  public int additionalMaintenanceCopiesNeeded(boolean includePendingAdd) {
    Set<Integer> maintenanceOnly = maintenanceOnlyIndexes(includePendingAdd);
    return Math.max(0, maintenanceOnly.size() - getMaxMaintenance());
  }
  getMaxMaintenance 的含义是"最多允许多少个 index 只存在于维护节点上而不触发补拷贝"——用 parity
  - remainingMaintenanceRedundancy 表示：parity
  本身就是冗余度，拿出其中一部分（remainingMaintenanceRedundancy）作为"保留给维护场景的缓冲"，
  剩下的部分才是维护节点可以安全占用的名额。比如
  RS(3,2)，parity=2，remainingMaintenanceRedundancy=1，则 getMaxMaintenance()=1——最多允许 1 个
  index 仅存在于维护节点，一旦有 2 个 index 都只在维护节点上，就需要补 1 份拷贝。

  ---

  五、离线重建：ECReconstructionCoordinator 的精确流程

  5.1 孤儿 Stripe 剔除（getBlockDataMap，483-541行）

  for (Map.Entry<Long, BlockData[]> entry : resultMap.entrySet()) {
    BlockData[] blockDataArr = entry.getValue();
    for (Map.Entry<Integer, DatanodeDetails> e : sourceNodeMap.entrySet()) {
      if (blockDataArr[e.getKey() - 1] == null) {
        LOG.warn("...making it an orphan block / stripe. It will not be reconstructed", ...);
        resultIterator.remove();
        break;
      }
    }
  }
  注释说得很明白：PutBlock 是对 stripe 内所有 d+p 节点广播发送的（即便某节点这次没数据也要收到
  PutBlock，见前面第 1.5 节），所以只要某个源节点的 ListBlock 结果里缺了这个 block
  的记录，就说明当初写入时这个节点没收到 PutBlock ack（客户端判定为失败，整个 stripe
  被放弃重写了），这个 block 是"写了一半被放弃的幽灵记录"，不应该被当作真实数据去恢复。

  5.2 安全长度计算（calcEffectiveBlockGroupLen，555-572行）

  long blockGroupLen = Long.MAX_VALUE;
  for (int i = 0; i < replicaCount; i++) {
    if (blockGroup[i] == null) continue;
    long putBlockLen = blockGroup[i].getBlockGroupLength();
    blockGroupLen = Math.min(putBlockLen, blockGroupLen);
  }
  取所有幸存源节点上报的 BlockGroupLength
  的最小值。注释解释了为什么不能做到绝对精确：如果最后一个 stripe
  写入失败，不同节点上记录的长度可能不一致（有的节点收到了这个失败 stripe
  的部分写入痕迹，有的没收到），取最小值"最多多恢复一个额外的
  stripe"，但不会让恢复出来的数据超出客户端实际认可的长度，保证数据视图不会被污染。

  5.3 "哪些 index 真正需要解码 vs 只需要空占位"（reconstructECBlockGroup，246-259行）

  int dataLocs = ECBlockInputStreamProxy.expectedDataLocations(repConfig,
  safeBlockGroupLength);
  for (Integer index : missingContainerIndexes) {
    if (index <= dataLocs || index > repConfig.getData()) {
      toReconstructIndexes.add(index);      // 需要真正解码
    } else {
      notReconstructIndexes.add(index);     // 只需要PutBlock占位,不需要写数据
    }
  }
  这处理的是"短 block"场景：如果这个具体 block 比较短，实际只用到了 dataLocs < d 个数据
  index，那么缺失的、下标在 dataLocs 和 d 之间的那些 index（本来就不该有数据，是超出这个 block
  实际长度的"虚位"）不需要解码——只要在新建的 container 上发一个空的 PutBlock
  让元数据对齐即可；而缺失的 parity index（index > d，parity
  永远是满长度参与计算的）和缺失的"实际用到的"数据 index（index <=
  dataLocs）才需要真正走解码流程。

  5.4 一次解码出多个缺失 index：零拷贝写出循环

  sis.setRecoveryIndexes(toReconstructIndexes.stream().map(i -> (i -
  1)).collect(Collectors.toSet()));
  long length = safeBlockGroupLength;
  while (length > 0) {
    int readLen = sis.recoverChunks(bufs);   // 一次stripe,同时恢复所有缺失index
    for (int i = 0; i < bufs.length; i++) {
      if (bufs[i].remaining() != 0) {
        CompletableFuture<...> future = targetBlockStreams[i].write(bufs[i]);
        checkFailures(targetBlockStreams[i], future);
      }
      bufs[i].clear();
    }
    length -= readLen;
  }
  bufs[i].remaining() != 0 这个判断是为了跳过"这个 stripe 里这个目标 index 根本没有数据"（短
  block 尾部场景）的空写，注释解释：否则会在 block 末尾产生一个多余的空
  chunk。每轮循环读出一个 stripe 的量（readLen），length -= readLen 推进，直到整个 block 的
  safeBlockGroupLength 处理完。

  5.5 失败即整体清理，无"部分成功"状态

  reconstructECContainerGroup 的 try/catch（161-219行）用 recoveringContainersCreatedDNs 这个
  List 按时间顺序累积已经成功创建的 RECOVERING
  container，一旦任何阶段（创建、重建写入、关闭）抛异常，catch
  块只对这个列表里已创建成功的目标做 deleteContainerInState(..., {UNHEALTHY,
  RECOVERING})——限定了只能删除还处于 UNHEALTHY 或 RECOVERING 状态的
  container，这是一个乐观并发控制：如果在清理之前，这个 container
  不知为何已经被别的流程转换成了其它状态（比如已经被成功 CLOSE
  了），删除请求会被拒绝，避免误删一个其实已经恢复成功的 container。

  5.6 recoveryIsCritical：SCM 端"要不要为了赶紧抢救而无视过载"的决策

  final int expectedTargetCount = missingIndexes.size();
  boolean recoveryIsCritical = expectedTargetCount == repConfig.getParity();
  ...
  if (hasOverloaded && 0 < targetCount && targetCount < expectedTargetCount &&
  !recoveryIsCritical) {
    // 还没到最后关头,且存在过载节点,且"去掉过载节点限制后能选够全部目标"——
    // 说明唯一卡住的原因就是过载保护,那就宁可推迟也不去冲撞过载节点
    final List<DatanodeDetails> targetsMaybeOverloaded = getTargetDatanodes(container,
  expectedTargetCount, usedNodes, excludedNodes);
    if (targetsMaybeOverloaded.size() == expectedTargetCount) {
      LOG.info("Deferring reconstruction...");
      throw new InsufficientDatanodesException(expectedTargetCount, targetCount);
    }
  }
  recoveryIsCritical = (missingIndexes.size() == parity)——这是说"当前缺失的 index
  数量已经等于奇偀校验容量的上限"，再丢一个就彻底不可恢复了，这种情况下宁可让某个节点超载也要
  立刻恢复；但如果缺的没那么多（比如 RS(3,2) 只缺 1 个，parity=2，还有一次容错余量），SCM
  选择"推迟"（抛 InsufficientDatanodesException 被外层捕获、计入
  requeueHealthResult，下次扫描再试），避免把负载进一步集中到已经高负载的节点上造成雪崩。这是
  一个很典型的"风险分级—紧急程度决定是否绕过保护机制"的工程判断。

  5.7 源节点优先级排序：避免从即将离开的节点读取

  List<Pair<ContainerReplica, NodeStatus>> sortedSources = sources.values().stream()
      .sorted((p1, p2) -> {
        boolean p1InService = p1.getRight().getOperationalState() == IN_SERVICE;
        boolean p2InService = p2.getRight().getOperationalState() == IN_SERVICE;
        if (p1InService && !p2InService) return -1;
        if (!p1InService && p2InService) return 1;
        return 0;
      }).collect(Collectors.toList());
  ...
  for (Pair<ContainerReplica, NodeStatus> src : sortedSources) {
    if (inServiceCount >= repConfig.getData() && src.getRight().getOperationalState() !=
  IN_SERVICE) {
      continue;  // 已经有足够IN_SERVICE源了,跳过非IN_SERVICE的(退役中/维护中)源
    }
    sourceDatanodesWithIndex.add(...);
  }
  只有当 IN_SERVICE 的源数量不足以凑够 d 个时，才会退而求其次用 decommissioning/maintenance
  节点作为读源——这是为了在条件允许的情况下，尽量不去打扰即将离线的节点（它们本身的网络/IO
  资源应该优先服务于它们自己正在进行的数据搬迁）。

  ---

  六、几个容易被忽视但很关键的工程细节

  1. ECKeyOutputStream 不支持 flush()/hflush()/hsync()（416-428行，后两者直接
     NotImplementedException）——因为 EC 写入的"durability checkpoint"天然就是整 stripe
     粒度的（必须凑齐 d 个数据 cell 才能算出 parity），没法做到比 stripe
     更细粒度的中途刷盘语义。insertFlushCheckpoint/flushCheckpoint 这套机制（92-99行）是专门为
     测试或某些需要"确认到某个版本号已经落盘"的场景设计的旁路机制，塞一个
     CheckpointDummyStripe 进队列，后台线程处理到它时记录版本号。
  2. ECBlockOutputStreamEntry.incCurrentPosition/getTotalAckDataLength 对 parity
     的特殊处理（143-157、193-201行）：isWritingParity()（currentStreamIdx >= d）为真时直接
     return，不调用父类的位置累加逻辑——因为"块组的逻辑长度"只由数据部分决定，parity
     的写入不应该影响这个计数,否则长度会被错误地放大 (d+p)/d 倍。
  3. ECContainerOperationClient.createRecoveringContainer 在 datanode
     侧走的是一条绕过常规"container已存在校验"的特殊建容器路径（containerSet.addContainerByOve
     rwriteMissingContainer），允许直接覆盖掉一个"SCM 认为丢失"的 container
     记录，这是因为正常的建容器流程有"不能对一个已知 containerID
     重复创建"的保护，但重建场景恰恰就是要对一个本地确实不存在（或者本地有残留但状态错误）的
     containerID 重新创建，必须有专门的旁路。
  4. 离线重建读的 checksum
     强制开启（clientConfig.setChecksumVerify(true)）在代码里是直接对传入的
     ozoneClientConfig对象做 setter 修改，而不是克隆一份——这意味着这个配置对象的其它用途（如果
     被共享）也会被这次调用永久改成"校验开启"，从代码结构看 ECReconstructionCoordinator 持有的
     this.ozoneClientConfig 是构造时通过 conf.getObject(OzoneClientConfig.class)
     单独取出来的专属实例，不会误伤到别处，但这种"直接改字段而不是传参覆盖"的写法算是一个需要
     留意的隐含假设。
