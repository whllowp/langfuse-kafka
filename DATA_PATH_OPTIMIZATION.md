# 数据路径优化:ClickHouse 写入组提交 + UI 表查询合并

> 基于 `feat/data-path-perf` 分支,实施提交 `d1352dce745c54438cdc0abd30b35af794eb175d`(10 文件,+908/−200,`git show --stat` 已核)。
> 结论先行:**Kafka 内联摄取落地后(见 [SCALING_OPTIMIZATION_DESIGN.md](SCALING_OPTIMIZATION_DESIGN.md) §2.8 的预警),瓶颈移到 ClickHouse 写入侧的固定小批与 UI 查询侧的重复执行。本方案以纯应用层改动回应:写入侧 per-table 单飞 + 组提交排空,查询侧结构化 key 的单飞合并 + count/metrics 短 TTL 结果缓存。**
> 硬约束:不引入新存储系统、不要求部署新基础设施;所有改动限于 `worker` 与 `packages/shared`。

---

## 1. 背景与目标

Kafka 摄取后端(dc4195391)把事件体内联进消息,S3 已退出摄取热路径。SCALING_OPTIMIZATION_DESIGN.md §2.8 当时即指出:"换 Kafka 只是让队列不再是第一瓶颈——下一步瓶颈会移到 CH 写入,需同时评估 ClickhouseWriter 批量参数"。本报告处理这"下一步":

| 目标       | 度量                             | 手段                                                 |
| ---------- | -------------------------------- | ---------------------------------------------------- |
| 写入吞吐   | 高积压时 INSERT 行数/请求        | 单飞排空循环 + `maxBatchSize`(默认 2000)放大批次     |
| 查询去重   | 相同逻辑 UI 表查询的 CH 执行数   | 结构化 key 单飞合并;count/metrics 加 2s TTL 结果缓存 |
| 语义不回退 | at-least-once、失败重试/丢弃策略 | 失败 requeue 后停止本轮 drain,由下一 tick 接力       |

## 2. 现状读写路径与瓶颈(勘察证据)

### 2.1 写路径(Kafka → ClickHouse)

```text
Kafka langfuse.ingestion.v1(事件体内联)
  → kafkaBackend eachBatch(packages/shared/src/server/queueBackends/kafkaBackend.ts:252)
      分区内 key 亲和并行;offset 只在"连续已处理前缀"提交(kafkaBackend.ts:347-356)
  → worker/src/queues/ingestionQueue.ts:65 ClickhouseWriter.getInstance()
  → IngestionService process*Event 逐条 addToQueue
      (worker/src/services/IngestionService/index.ts:513,604,783,864,896,1102,1106,1127)
  → ClickhouseWriter 内存队列 → 定时/阈值触发 flush → INSERT JSONEachRow
```

**改造前的瓶颈**(`git show d1352dce7^:worker/src/services/ClickhouseWriter/index.ts`):

| #   | 事实                                                                                     | 证据                                                                             | 后果                                                                                                                                                   |
| --- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| W1  | `flush` 每次调用只 `splice(0, batchSize)` 一批,无排空循环                                | 旧版 index.ts:363-369                                                            | interval tick(默认 1000ms,`worker/src/env.ts:214-217`)每表每秒至多消化一批 1000 行(`worker/src/env.ts:206-209`);高积压时要么等多个 tick,要么靠 W2 补偿 |
| W2  | `addToQueue` 每次越过 `batchSize` 就再触发一次 `flush`                                   | 旧版 index.ts:586-590                                                            | 高积压时 INSERT 请求数随积压线性增长,但**批大小恒为 batchSize 不放大**——吞吐靠"更多请求"而非"更大批"                                                   |
| W3  | 无 in-flight 保护,interval tick、addToQueue 触发、shutdown 的 fullQueue flush 可并发交错 | 旧版 flush 全文无锁(363 起)                                                      | 同表多批并发写放大 CH merge 压力,且 fullQueue 语义(排空)无法保证                                                                                       |
| W4  | 每消息 `getClickhouseRecord` 回读(SELECT 回读旧值供 merge)                               | `worker/src/services/IngestionService/index.ts:1769`(定义),调用点 :647,:834,:970 | 已知但本次未解决(见 §4 放弃项 2)                                                                                                                       |

### 2.2 读路径(traces UI 表)

```text
浏览器 traces 列表 → tRPC 三个独立 procedure:
  rows   → getTracesTable        (web/src/server/api/routers/traces.ts:159)
  count  → getTracesTableCount   (web/src/server/api/routers/traces.ts:193)
  metrics→ getTracesTableMetrics (web/src/server/api/routers/traces.ts:251)
→ 均落入 getTracesTableGeneric(packages/shared/src/server/services/traces-ui-table-service.ts:226)
→ 复杂 FINAL/JOIN 查询(该文件 :543-548 的自注释),默认排序走 toDate+timestamp DESC+LIMIT 1 BY 非 FINAL 快路径(:496-511,:531-535)
```

| #   | 事实                                                                                                                      | 证据                                                                                                                                        | 后果                                               |
| --- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| R1  | 一次列表加载至少 3 条独立 CH 查询;多标签、多用户同项目、多 web 副本、React Query 重挂载会**并发发出逻辑上完全相同的查询** | traces.ts:159/193/251 三个 procedure 各自直达 CH;coalescer 接入前无任何进程内去重(该文件 d1352dce7 前 `getTracesTableGeneric` 为单函数直查) | 相同重查询在仪表盘轮询/多人查看时成倍打到 CH       |
| R2  | filter 渲染每次生成**随机参数名**,相同逻辑请求的渲染 SQL 文本永不相等                                                     | traces-ui-table-service.ts:227-231 注释(改造时写入的依据)                                                                                   | 任何"以渲染 SQL 文本为 key"的缓存/合并方案天然失效 |
| R3  | count/metrics 对短陈旧不敏感,rows/identifiers 必须反映当前表状态(分页/删除批)                                             | traces-ui-table-service.ts:251-253 注释                                                                                                     | 两类查询应区别对待:前者可缓存,后者只能合并         |

## 3. GreptimeDB 可借鉴技术(来源逐条给出)

GreptimeDB 与本管线同构:高吞吐写入 + WAL(Kafka)+ 列存物化 + 分布式查询。以下机制均引自其源码/文档,按"写入/查询"归类;每条标注本仓库的对应决策。

### 3.1 写入侧

| #   | 机制(原文摘要)                                                                                                                                                                                                                                                                     | 来源                                                                                                                                                                                                             | 本仓库对应                                                                                                                                        |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1  | **RegionWorker 专属写线程 + 自适应攒批(group commit)**:Datanode 把 region 哈希到 N 个 worker 线程(默认 `num_workers=CPU/2`,通道容量 128);worker 阻塞收到第一条后用 `try_recv` 非阻塞排空至 `worker_request_batch_size`(默认 64)条合为一个批次——低延迟退化为单条,高并发自动放大批次 | https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/worker.rs(run loop L1300-1410)及 config.rs L74-80                                                                                             | **已采纳(思想)**:消费端"首条阻塞、后续非阻塞排空到批量上限"→ ClickhouseWriter 的 drainQueue 循环 + maxBatchSize(§5.1)                             |
| G2  | **跨 region WAL 批量 append + 每 region entry-id 水位**:`write_wal()` 把批次内所有 region 的 WAL 条目合成一次 `append_batch`;Kafka LogStore 按 region→topic 分桶、各 producer 并行 produce,join 回执后返回每 region 的 last_entry_id(Kafka offset 即 entry id)                     | https://github.com/GreptimeTeam/greptimedb/blob/main/src/log-store/src/kafka/log_store.rs(append_batch L237-305)与 src/mito2/src/worker/handle_write.rs                                                          | 部分对应:多 project/多 shard 事件已合成尽量少的 INSERT;**分区级水位管理未做**(§4 放弃项 3)                                                        |
| G3  | **WAL-first 提交路径 + 原子 sequence/entry-id 发布**:WAL append → memtable 写入 → 一次性 publish committed sequence;读者以 version+sequence 原子快照读取,WAL 失败不写 memtable                                                                                                     | https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/region_write_ctx.rs 与 https://docs.greptime.com/contributor-guide/datanode/memtable/                                                         | **不采纳(§4 放弃项 3)**:offset 提交与 CH flush 绑定会放大重试代价                                                                                 |
| G4  | **表级 skip_wal(可重放源替代 WAL)**:写入绕过 WAL 直进 memtable,持久性依赖外部可重放源(如 Kafka);崩溃恢复时这类内存数据被丢弃而非回放                                                                                                                                               | https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/region_write_ctx.rs(is_skip_wal_write/mark_skip_wal_writes)与 https://greptime.com/blogs/2025-03-26-greptimedb-storage-architecture           | **正是本管线模式**:Kafka 即 WAL,CH 写失败靠 offset 未提交重放;重放窗口 = Kafka retention(部署参数需与之绑定,未在本次代码内强制)                   |
| G5  | **WAL 压力驱动的 flush(flush_trigger_size)**:周期选出未释放条目过多的 region,worker 触发 `FlushReason::WalPressure` flush;Metasrv 默认 512MB 阈值,仅 SST 上传确认后 DeleteRecords 截断 topic                                                                                       | https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/worker/handle_flush.rs(flush_for_wal_pressure)与 https://docs.greptime.com/user-guide/deployments-administration/wal/remote-wal/configuration | **未采纳(§4 放弃项 4)**:两套后端确认语义牵动面大;Kafka 消费暂停本身已提供背压                                                                     |
| G6  | **分级背压阶梯**:region 级 memtable ≥ write_buffer_size/2 flush、≥ limit 暂存(stall)、≥ 2×limit 拒绝;engine 级全局 1GiB/2GiB 同构阶梯;被 stall 的请求进队列由 flush 完成唤醒                                                                                                       | https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/worker/handle_flush.rs(region_write_buffer_status_from_usage L339-355,flush_regions_on_engine_full)与 handle_write.rs、config.rs L145-156     | **未采纳(§4 放弃项 4)**;`ingestion_clickhouse_insert_queue_length` 指标(worker/src/services/ClickhouseWriter/index.ts:588-595)已可观测,边际收益小 |

### 3.2 查询侧

| #   | 机制(原文摘要)                                                                                                                                               | 来源                                                                                                                                                      | 本仓库对应                                                                                                                      |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| G7  | **Frontend 分布式查询规划与 MergeScan**:planner 把 TableScan 替换为 MergeScan,按 region 拆子查询下发 Datanode(Flight 流式 Arrow),Frontend 执行剩余算子并合并 | https://docs.greptime.com/contributor-guide/frontend/overview ;https://github.com/GreptimeTeam/greptimedb/blob/main/src/query/src/dist_plan/merge_scan.rs | 架构参照:等价于 CH Distributed 表;应用层保证过滤/聚合下沉扫描节点(现状已如此,无改动)                                            |
| G8  | **分区裁剪 ConstraintPruner**:提取过滤条件中的分区表达式,区间求交判定保留 region;任何环节失败保守返回全部 region                                             | https://github.com/GreptimeTeam/greptimedb/blob/main/src/query/src/dist_plan/region_pruner.rs 与 planner.rs                                               | "裁剪保守失败"原则适用;可用 CH `EXPLAIN` 验证 partition pruning(后续观测项)                                                     |
| G9  | **时间范围提取 + 粗到细多层裁剪**:planner 内建从过滤条件静态提取时间范围→跳过不相交 SST/memtable→row group 统计裁剪→索引裁剪                                 | https://docs.greptime.com/contributor-guide/datanode/storage-engine ;https://github.com/GreptimeTeam/greptimedb/blob/main/src/table/src/predicate.rs      | 本管线表已按日分区 + 主键;时间窗过滤已强制存在,无即时改动                                                                       |
| G10 | **统一谓词对象 Predicate{exprs, dyn_filters} 与下推**:已知坑——Join 阻断谓词下推(GitHub issue #8338),谓词须附着到 scan                                        | https://github.com/GreptimeTeam/greptimedb/blob/main/src/table/src/predicate.rs ;https://github.com/GreptimeTeam/greptimedb/issues/8338                   | 对应 CH PREWHERE/子查询;traces 查询的 observations/scores CTE 已先过滤再 join(traces-ui-table-service.ts:543-546 注释),维持现状 |
| G11 | **动态谓词下推(TopK 运行时收紧界)**:50-60 亿行表 ORDER BY DESC LIMIT 10 从 ~28.9s 到 ~0.21s                                                                  | https://greptime.com/blogs/2026-05-15-greptimedb-topk-dynamic-filter ;merge_scan.rs                                                                       | **不采纳(§4 放弃项 6)**:CH 无原生等价物,复现需两段式查询改变语义                                                                |
| G12 | **多级缓存体系**:内存对象缓存按 OS 内存比例(sst_meta 1/8、vector 1/16 等)+ 磁盘 write/read cache                                                             | https://github.com/GreptimeTeam/greptimedb/blob/main/config/datanode.example.toml ;https://greptime.com/blogs/2025-03-26-greptimedb-storage-architecture  | **部分采纳(应用层)**:count/metrics 进程内短 TTL 结果缓存(§5.2);CH 服务器侧缓存调优属部署侧,未动                                 |

## 4. 候选方案与取舍(含放弃项及理由)

| 方案                                                                                        | 结论                 | 理由                                                                                                                                                                                                                                                                              |
| ------------------------------------------------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **写入:per-table 单飞 + 组提交排空(G1 思想)**                                               | **采纳**             | 纯应用层;低延迟退化单条、高积压自动放大到 maxBatchSize;不改变失败/重试语义                                                                                                                                                                                                        |
| **查询:结构化 key 单飞 + count/metrics TTL 缓存**                                           | **采纳**             | 消除 R1 重复执行;结构化 key 从输入侧根治 R2 随机参数名问题;rows/identifiers 不缓存规避 R3 风险                                                                                                                                                                                    |
| 1. 存储直接替换为 GreptimeDB                                                                | 放弃                 | 违反硬约束(不引入新存储/新基础设施);查询层深度绑定 CH 方言(FINAL、LIMIT 1 BY、argMax、sumMap、arrayExists、SAMPLE BY、ReplacingMergeTree(event_ts) 去重、events_full→events_core 物化视图),迁移=查询层全重写+双写+无法原地回滚;目标收益在现有 CH 管线上以应用层改动即可获得大部分 |
| 2. 写入:消除每消息 CH 回读(批级去重 getClickhouseRecord / 进程内实体状态缓存,类比 memtable) | 放弃                 | 瓶颈属实(§2.1 W4),但进程内实体缓存在多 worker 实例下有跨实例陈旧风险,而 merge 正确性依赖读到最新行;按批去重回读需侵入 4 条 process\*EventList 路径,effort large。Kafka inline 路径已消除 S3 读放大,此项留待独立评估                                                               |
| 3. 写入:Kafka offset 提交与 CH 落盘绑定为原子检查点(G3 WAL-first 思想)                      | 放弃                 | ClickhouseWriter 是跨消息/跨表内存聚合队列,批次无法归属单条消息;推迟 offset 提交会把 flush 失败升级为整批消息重试(重复写放大)并显著降低消费吞吐;effort large                                                                                                                      |
| 4. 写入:分级背压阶梯(region 级 flush/stall/reject,G5/G6)                                    | 放弃                 | 需同时改 Kafka eachBatch(暂停分区/延迟 ack)与 BullMQ 两套后端确认语义;上游 Kafka 消费暂停已提供背压,队列长度已有指标可观测,边际收益小;effort large                                                                                                                                |
| 5. 写入:时间对齐的攒批分组(time-partitioned memtable 思想)                                  | 放弃                 | CH 表 PARTITION BY toYYYYMM/主键 toDate(timestamp) 已按日组织,client 层 async_insert 服务端已合批,客户端再按时间桶分组对 part 数与 merge 压力收益不明确,收益/成本比差于自适应批放大                                                                                               |
| 6. 查询:动态谓词下推/两段式 TopK(G11 思想)                                                  | 放弃                 | CH 无原生等价物;复现需应用层两段式查询(粗界 top 候选再二次过滤),改变查询语义与 UI 契约;traces 默认排序已走 toDate+timestamp DESC+LIMIT 1 BY 优化读路径(traces-ui-table-service.ts:496-511 与 :547-548 注释),收益不确定;effort large                                               |
| 7. 查询:legacy UI 表查询显式路由 ReadOnly 副本                                              | 放弃                 | 收益依赖多副本部署(单副本时逐级回退 CLICKHOUSE_URL 无收益);副本滞后引入"刚入库 trace 刷新不可见"的读己之写回归;宜作部署侧决策单独评估                                                                                                                                             |
| 8. 查询:合并 count+rows 为单查询或调整 FINAL/去重策略                                       | 放弃                 | 牵动 UI 分页契约与 ReplacingMergeTree 去重正确性;FINAL/LIMIT 1 BY 策略已多级可调(env + Redis OTel 标记),v4 events 路径去 FINAL 化在持续演进,重复改动风险大;effort large                                                                                                           |
| 9. 查询备选:渲染文本 key + 随机参数名正则规范化                                             | 放弃(终审明示不推荐) | 对 `stringFilter[A-Za-z]{5}` 类参数名做正则归一——脆弱:参数名模式分散在 10+ 个 filter 类与 CTE 生成器,新增 filter 类型或改命名模式会静默错误命中(不同查询归一为同 key→缓存串数据,正确性风险高于收益);且仍需序列化整个大 query 文本。结构化 key 从输入侧根治                        |

## 5. 已实现变更(提交 d1352dce7)

### 5.1 写入:ClickhouseWriter 组提交(worker)

文件:`worker/src/env.ts`、`worker/src/services/ClickhouseWriter/index.ts`、`worker/src/services/ClickhouseWriter/ClickhouseWriter.unit.test.ts`

- **新 env**:`LANGFUSE_INGESTION_CLICKHOUSE_WRITE_MAX_BATCH_SIZE` 默认 2000(`worker/src/env.ts:210-213`);`maxBatchSize` 恒 ≥ `batchSize` 构造保证(index.ts:53-56)。
- **per-table 单飞**:`flushInFlight: Partial<Record<TableName, Promise<void>>>`(index.ts:47);`flush()` 发现 in-flight 即返回,靠在飞 drain 或下一 tick 接管;`fullQueue`(shutdown)改为等待在飞 flush 后接管排空(index.ts:374-398,占位 promise 在首个 await 前同步登记,index.ts:389-390)。
- **排空循环 `drainQueue`**:每轮 `splice` 至多 `maxBatchSize` 条(index.ts:420-424),非 fullQueue 模式排到队列 `< batchSize` 为止(index.ts:643-647);fullQueue 只排空启动时的水位快照 `pendingRowCount`(index.ts:417),排空期间新入队行留给下一轮,保证循环有限。
- **失败路径不变**:批次失败整批 requeue(attempts+1),超限丢弃并计数,本轮 drain 终止由下一 tick 重触(index.ts:596-641)——持久失败的 CH 不会陷入 busy loop。
- **size-error 二分拆批**:截断后仍 size error 判定为"批整体过大(行数×体积)"→ 拆半重试,后半 unshift 回队(index.ts:492-518;`handleStringLengthError` index.ts:182-216,单条时退化为截断防死循环)。
- **addToQueue 触发去抖**:队列越阈值但已有 in-flight flush 时跳过触发(index.ts:664-667),避免 W2 的重复 flush 风暴。
- **偏离计划一处**:`fullQueue` 的循环继续条件从计划的 `queue.length > 0` 改为水位快照。原因:旧单测中 `insert` mock 向队列回灌,`queue.length > 0` 条件下无限循环直至 OOM——开发中已插桩实证并修复。

### 5.2 查询:queryRequestCoalescer(packages/shared)

文件:`packages/shared/src/server/cache/queryRequestCoalescer.ts`(新)、`queryRequestCoalescer.test.ts`(新)、`cache/index.ts`(barrel 追加导出)、`packages/shared/src/env.ts`、`traces-ui-table-service.ts`、`traces-ui-table-service.test.ts`

- **coalescer**(`queryRequestCoalescer.ts`):in-flight Map 单飞(:9,:48-52,占位在首个 await 前同步注册)+ `lru-cache` TTL 结果缓存(:10-16,上限 1000 条封顶内存);成功且 `ttlMs>0` 才缓存(:57-59),错误永不缓存、所有订阅者收到同一 rejection(:64-69);`__resetForTests` 清态(:152-155)。
- **key 双保险**:`stableStringify` 复用 `JSON.stringify` 值语义(Date 经 toJSON→ISO,故不同时间窗不会碰撞成同 key;:92-98 的注释明确禁止手写递归替代);`normalizeFilterForKey` 再对输入做深度 Date→ISO 规范化(:107-122)。
- **接入**:`getTracesTableGeneric` 拆为「结构化 key + coalescer 包裹」(traces-ui-table-service.ts:226-264,key 由 select/projectId/filter/search/orderBy/limit/page/cursor 等结构化输入构成 :233-249)与 `loadTracesTableGeneric` 原查询体不动(:266 起);count/metrics 走 `LANGFUSE_UI_TABLE_QUERY_CACHE_TTL_MS`(默认 2000ms,`packages/shared/src/env.ts:649-653`),rows/identifiers 仅单飞不缓存(:254-257)。
- **共享行的防串改**:合并命中返回同一行对象,`convertToUiTableRows` 对唯一被消费方原地排序的数组字段 `tags` 做浅拷贝(traces-ui-table-service.ts:106-108)。

### 5.3 复核

无(ask 复核环节为空;本报告自身完成了引用证据与测试的独立重验,见 §6)。

## 6. 验证结果

| 门禁                  | 命令(实际执行)                                                                                                                                                                         | 结果                                             |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| 写入侧单测            | `cd worker && set -a && source ../.env.dev.example && set +a && pnpm --config.verify-deps-before-run=false run test ClickhouseWriter`                                                  | `Test Files 1 passed (1)`,`Tests 48 passed (48)` |
| 查询侧单测(coalescer) | `cd packages/shared && set -a && source ../../.env.dev.example && set +a && pnpm --config.verify-deps-before-run=false exec vitest run src/server/cache/queryRequestCoalescer.test.ts` | `Test Files 1 passed (1)`,`Tests 10 passed (10)` |
| 查询侧单测(traces 表) | 同上,`vitest run src/server/services/traces-ui-table-service.test.ts`                                                                                                                  | `Test Files 1 passed (1)`,`Tests 7 passed (7)`   |
| 提交核验              | `git show --stat d1352dce745c54438cdc0abd30b35af794eb175d`                                                                                                                             | 10 文件,+908/−200,与 §5 文件清单一致             |

环境说明(如实记录):

- `pnpm --filter worker run test`(AGENTS.md 标准命令)在本机触发 pnpm 依赖校验→ postinstall 失败(`ERR_PNPM_EXECUTOR_LIFECYCLE_SCRIPT_FAILED`),故改在包目录内直跑并加 `--config.verify-deps-before-run=false`;env 经 `source .env.dev.example` 进程级注入,与 CI 的 `cp .env.dev.example .env` 等价(CI 证据:.github/workflows/pipeline.yml:966-969),**未创建或修改任何 .env 文件**。
- worker 测试首次运行因缺 `CLICKHOUSE_URL/DATABASE_URL` 等 env 失败(ZodError),补齐后通过——失败与通过两次输出均已亲见。
- **未跑**:全量 `pnpm run lint` / `typecheck`(本次任务产出仅为本文档,不改代码;且 `--filter` 路径在本机同样会触发失败的 postinstall)。提交 d1352dce7 已包含全部代码变更,本文档为新增文件,不属该提交。

## 7. 未覆盖与后续

1. **真实 ClickHouse 实测未做**:§6 全部为 mock client 的单元测试;`maxBatchSize=2000` 的吞吐收益、INSERT 行数分布、CH merge 压力变化需在预发环境用真实流量压测。观测抓手:已有 `ingestion_clickhouse_insert_queue_length`(index.ts:588-595)、`langfuse.queue.clickhouse_writer.processing_time` 与新增 `langfuse.query.coalesced`(outcome=ttl_hit/shared,queryRequestCoalescer.ts:43,:50)。
2. **每消息 CH 回读去重**(§4 放弃项 2):多实例部署下的跨实例一致性方案(如共享缓存或批级 SELECT IN)需独立设计。
3. **分区级水位管理**(G2/G5):当 CH 持续不可写时的消费暂停策略,与 Kafka retention 的绑定关系,建议作为部署 runbook 项。
4. **CH 服务器侧调优**(G12):mark cache/query cache 按内存比例分层属部署侧决策,与本次应用层改动解耦。
5. **EXPLAIN 审计**(G8/G10):对热查询逐条核对 partition pruning 与 join 对下推的阻断,可纳入日常性能巡检。
