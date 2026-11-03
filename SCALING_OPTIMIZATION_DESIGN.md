# Langfuse 高流量优化设计：Kafka 替换消息队列 + S3 写路径优化

> 基于 v4.35.0 源码的工程设计方案。所有文件路径、机制描述均以当前代码为准。
> 结论先行：**Kafka 只替换"高吞吐事件流"（摄取与派生队列）；cron 调度、延迟重试、分布式锁、限流、缓存留在 Redis。**
> **事件体内联进 Kafka 后，S3 从摄取热路径上消失，热路径瓶颈随之解除**——两个问题一个方案解决大半。

---

## 1. 现状与瓶颈定位

### 1.1 当前消息路径（代码事实）

```text
SDK 批量事件
  → web: processEventBatch (packages/shared/src/server/ingestion/processEventBatch.ts)
      ① 按 eventBodyId 分组，每组一个 JSON 对象 PUT 到 S3（阻塞式，失败即整批报错）
      ② 每组入队一个 BullMQ job，shardingKey = "projectId-eventBodyId"
  → Redis (BullMQ ingestion-queue[-N]，分片数 env 可配，默认 1)
  → worker: ingestionQueue.ts
      ③ Redis 去重缓存(5min) → ④ LIST S3 前缀 + 并发下载 → ⑤ IngestionService 合并 → ⑥ 批量写 CH
```

关键接缝（替换工作的着力点）：

| 接缝    | 位置                                                                    | 说明                                                                                 |
| ----- | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| 唯一生产者 | `packages/shared/src/server/ingestion/processEventBatch.ts`           | S3 上传 + 入队都在此；OTel 路径在 `packages/shared/src/server/otel/OtelIngestionProcessor.ts` |
| 队列工厂  | `packages/shared/src/server/redis/*.ts`（约 45 个文件）                     | 每队列一个类，静态单例 + 分片命名，包装 BullMQ `Queue`                                               |
| 消费注册  | `worker/src/app.ts` + `worker/src/queues/workerManager.ts`            | 每队列一个 processor，带指标/OTel 包装                                                        |
| 队列契约  | `packages/shared/src/server/queues.ts`                                | ≈40 队列名 + Zod payload（**保持不变，是两套后端的公共契约**）                                         |
| 运维工具  | `web/src/pages/api/admin/bullmq/index.ts`、`admin/ingestion-replay.ts` | 队列内省与重放依赖 BullMQ API，需同步适配                                                         |

### 1.2 为什么 Redis + BullMQ 在极高吞吐下会成为瓶颈

| 约束            | 机制                                                                                  | 量级说明                                                |
| ------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------- |
| Redis 单线程 CPU | BullMQ 每个 job ≈ 4–6 次 Redis 调用（HSET + LPUSH + ZADD + PUBLISH…）                      | 单 Redis 节点 ~10 万 ops/s 上限 → 折算约 2 万 job/s；再叠加缓存/锁流量 |
| Cluster 下队列同槽 | hash-tag `{prefix:queueName}` 保证队列键落同槽（`packages/shared/src/server/redis/redis.ts`） | **单队列吞吐上限 = 单分片上限**，分片数是静态配置                        |
| 内存即 backlog   | `noeviction` 是硬要求（队列不允许逐出）                                                          | 消费停滞时 backlog 深度受内存约束，无磁盘缓冲层                        |
| 无回放能力         | 消费后即删除                                                                              | 事故后无法重放历史事件，只能靠 S3 侧文件补救                            |
| 多租户公平性        | 靠 secondary 队列分流热点项目（S3 SlowDown 触发）                                                | 被动、粗粒度                                              |

**先量化再动手**（避免为不存在的问题引入新系统）：切换前先用现有指标确认瓶颈——
`WorkerManager` 已有 wait/processing 时间直方图；再补三个探针：Redis 单分片 CPU、
各队列 lag（`zcard` + waiting 数）、摄取端到端 P99（事件 timestamp → ClickHouse `created_at`）。

### 1.3 S3 / MinIO 热路径的真实成本

- **每个 eventBodyId 一次 PUT**，JSON 未压缩；上传阻塞摄取响应，失败整批 429/500（SlowDown 会标记项目转 secondary 队列）。
- **worker 端 LIST**：按 `bucketPrefix` 列出前缀下对象再逐个下载（`worker/src/queues/ingestionQueue.ts:185`）——LIST 是 O(前缀下对象数)，MinIO 上比 S3 更贵。
  代码已支持"直传 fileKey 跳过 LIST"（`ingestionQueue.ts:161-174`），但只对 dataset_run_item、OTel 项目、`LANGFUSE_SKIP_S3_LIST_FOR_OBSERVATIONS_PROJECT_IDS` 白名单生效。
- **写读放大**：同一份事件数据，热路径上写一次 S3、读一次 S3，只为绕开 Redis 大 payload——这是把存储成本换队列成本的折中，Kafka 内联后此折中不再必要。

---

## 2. Kafka 替换设计

### 2.1 范围切分：哪些换、哪些不换

**换（事件流性质，高吞吐，Kafka 的主场）：**

| 现队列                                                       | 新 Topic                                             | 分区键                | 说明                       |
| --------------------------------------------------------- | --------------------------------------------------- | ------------------ | ------------------------ |
| ingestion-queue(+secondary, 分片)                           | `langfuse.ingestion.v1`                             | `eventBodyId`      | 热路径；secondary 机制由分区+配额取代 |
| otel-ingestion(+secondary)                                | `langfuse.ingestion.otel.v1`                        | `eventBodyId`      | 建议第一个切换（入口单一）            |
| trace-upsert-queue(分片)                                    | `langfuse.trace-upsert.v1`                          | `traceId`          | 摄取派生的评估触发                |
| evaluation-execution(+secondary)、llm-as-a-judge、code-eval | `langfuse.eval-execution.v1`（header 区分子类型）或三个 topic | `traceId`          | 外部 LLM 调用耗时大，吞吐中等但量大     |
| dataset-run-item-upsert                                   | `langfuse.dataset-run-item.v1`                      | `datasetRunItemId` |                          |
| entity-change-queue                                       | `langfuse.entity-change.v1`                         | 实体 id              |                          |

**不换（保留 Redis/BullMQ，控制面性质）：**

| 用途                                                        | 依赖的 BullMQ/Redis 能力 | Kafka 不提供   |
| --------------------------------------------------------- | ------------------- | ----------- |
| cron 调度（`scheduleRecurringJob.ts` 的 `upsertJobScheduler`） | 定时/重复 job           | Kafka 无延迟投递 |
| 延迟重试（`RetryBaggage`、`LANGFUSE_INGESTION_QUEUE_DELAY_MS`）  | delayed job         | 同上          |
| 周期任务互斥（`PeriodicExclusiveRunner`）                         | Redis 分布式锁          | —           |
| 限流（`RateLimitService`）、删除队列限速（BullMQ `limiter`）           | 计数器/令牌桶             | —           |
| 低频控制队列（删除、batch action/export、集成、云计费，约 25 个）              | 队列内省、动态增删           | 迁移收益低       |

### 2.2 语义设计

- **投递语义：at-least-once + 幂等合并**。现有链路天然适配：Redis 去重缓存 + IngestionService 按时间排序合并（creates-before-updates）已把重复/乱序吸收掉；Kafka 重平衡期间的重复投递不引入新问题。**不需要事务/exactly-once。**
- **顺序性：分区键保证同实体有序**。现状 shardingKey = `projectId-eventBodyId` 哈希进分片；Kafka 用 `eventBodyId` 做键即等价或更强（同实体必然同分区）。不用 `projectId` 做键——大租户会打出热点分区；租户维度的 lag/流量监控放 message header。
- **offset 提交**：处理完成后手动提交（`enable.auto.commit=false`），与 BullMQ 的 job ack 语义对齐；ClickhouseWriter 的内存批在优雅停机时冲刷（复用现有 `drainAndClose` 流程，先 flush writer 再提交 offset）。

### 2.3 消息设计：内联事件体，S3 退居溢出通道

```text
Kafka 消息 value = 现有 IngestionEvent payload 直接内联（JSON + zstd）
  - 典型事件 1–4KB，zstd 后 0.3–1KB；远低于 Kafka 默认 1MB 上限
  - 阈值 LANGFUSE_KAFKA_INLINE_MAX_BYTES（默认 512KB）：
      ≤ 阈值 → 内联（热路径 99%+ 的消息）
      >  阈值 → 走现有 S3 引用通道（保留 StorageService 路径，仅大事件使用）
  - header: projectId（监控/配额）、schemaVersion、traceId（排查）
```

收益：摄取热路径的 S3 PUT + LIST + GET 全部消失（见 §3.2）；S3 仅服务媒体上传、批量导出、大事件溢出。

### 2.4 代码改造地图

```text
packages/shared/src/server/queues/backend/        【新增】
  ├─ types.ts            EventPublisher / EventConsumer 接口（produce/consume/ack/nack）
  ├─ bullmqBackend.ts    包装现有 redis/*.ts 队列类（默认实现，行为不变）
  └─ kafkaBackend.ts     kafkajs 适配（producer 池 + consumer group）

processEventBatch.ts     生产端：S3 上传块整体移入 kafkaBackend 分支；
                         内联 payload ≤ 阈值直发，超阈值回落现有 S3 路径
worker/src/app.ts        注册表按 QUEUE_BACKEND 选择后端；Kafka 模式下
                         每队列一个 consumer group（group=worker-<queue>），
                         分区并行度取代分片数；现有 metrics/OTel 包装函数复用
workerManager.ts         Worker 生命周期 → consumer pause/resume/stop 映射
queues.ts                契约不变；新增队列→topic 映射表
env.ts                   QUEUE_BACKEND=redis|kafka（按队列粒度覆盖：
                         QUEUE_BACKEND_INGESTION=kafka），LANGFUSE_KAFKA_*
```

BullMQ 能力 → Kafka 替代对照：

| BullMQ 能力                        | Kafka 替代                                                                 |
| -------------------------------- | ------------------------------------------------------------------------ |
| 分片队列（9 个）                        | 分区数（动态再平衡，免静态配置）                                                         |
| secondary 队列热点分流                 | 租户级配额（producer 侧）或专属分区/独立 consumer group                                 |
| lockDuration / stalled 检测        | `max.poll.interval.ms` + 重平衡（语义变化：处理超时触发重平衡而非单 job 重派）                   |
| limiter（删除限速）                    | consumer 侧令牌桶（这些队列本来就不迁）                                                 |
| failed jobs + DLQ 重试服务（每 10min）  | 分层重试 topic：`*.retry.10m` / `*.retry.1d` → `langfuse.dlq.v1`，定时消费器搬运      |
| admin/bullmq 内省、ingestion-replay | admin API 增加 Kafka 变体（lag、offset、按时间戳重放）——Kafka 天生支持按 offset/时间回放，能力反而增强 |

### 2.5 生产/消费参数基线

```text
producer: acks=all, enable.idempotence=true, compression.type=zstd,
          linger.ms=50, batch.size=64KB, delivery.timeout=30s
consumer: group=worker-<queue>, enable.auto.commit=false,
          max.poll.records≈分区内批大小(1000 对齐 ClickhouseWriter 批量),
          session.timeout=15s（对齐 lockDuration 量级）
broker:   RF=3, min.insync.replicas=2, retention=3–7 天（热 topic），
          partition 数=目标并行度（替代 9 分片起步，可 24/48）
本地开发: docker-compose 增加 KRaft 单节点 kafka（bitnami/redpanda 均可），
          QUEUE_BACKEND 默认仍为 redis，compose 覆盖为 kafka
```

### 2.6 容量估算（示例，需按实测替换）

```text
假设 5 万事件/s，均值 2KB/条：
  入口流量 = 100MB/s；zstd 压缩比按 5×  → 网络与磁盘 ≈ 20MB/s
  保留 3 天 × RF3 → 集群磁盘 ≈ 20MB/s × 259200s × 3 ≈ 15.5TB（集群总量）
  3 台 broker（NVMe）× 48 分区即可承载，CPU 余量 >50%
对比：同流量下 BullMQ ≈ 25 万 Redis ops/s（每 job 5 次调用）——单分片远不可行，
     需 8+ 专属分片且失去回放能力。
```

### 2.7 迁移路线（每步带回滚开关）

| 阶段  | 内容                                                               | 回滚                                   |
| --- | ---------------------------------------------------------------- | ------------------------------------ |
| 0   | 指标先行：Redis CPU / 队列 lag / 端到端延迟基线                                | —                                    |
| 1   | 落地 backend 抽象层，BullMQ 走新接口（行为零变化）                                | 直接 revert                            |
| 2   | 影子双写：Kafka 与 BullMQ 同时生产，worker 消费 Kafka 但**不写库**，对比消息数与 payload | 关双写                                  |
| 3   | **OTel 摄取切流**（入口单一、量可控），观察 1–2 周                                 | `QUEUE_BACKEND_INGESTION_OTEL=redis` |
| 4   | v3 ingestion 切流 + 内联生效（S3 热路径退出）                                 | 同上                                   |
| 5   | 派生队列：trace-upsert → eval-execution                               | 按队列开关                                |
| 6   | admin 工具与重放、DLQ 服务 Kafka 化收尾                                     | —                                    |

### 2.8 风险与代价（明确列出，不做单向推销）

- **多一套有状态系统**：Kafka 的运维（再平衡、磁盘、版本）是真实成本；团队若无 Kafka 经验，优先考虑托管（MSK / Confluent / Upstash）再考虑自建 KRaft。
- **fork 维护成本**：此改造偏离上游 Langfuse 主干（上游明确选择了 S3+Redis 架构，见其 "Simplifying Langfuse for Scale"）。后续合并上游版本时，抽象层是唯一的隔离带——**这也是必须先做 §2.4 接口层、而不是散改 45 个队列文件的原因**。
- **延迟重试语义变化**：BullMQ delayed job 精确到毫秒；Kafka 重试 topic 粒度固定（10m/1d 档）。摄取链路对重试延迟不敏感，可接受。
- **最小收益场景**：若实测瓶颈其实是 ClickHouse 写入或合并引擎读放大（每事件一次 getClickhouseRecord 读），换 Kafka 只是让队列不再是第一瓶颈——下一步瓶颈会移到 CH 写入，需同时评估 ClickhouseWriter 批量参数与异步插入。

---

## 3. S3 / MinIO 写路径优化

### 3.1 不动架构、立即可做（按 ROI 排序）

1. **全量消灭 LIST**：把"直传 fileKey 跳过 LIST"从 OTel 白名单推广到全部事件类型（生产端 `processEventBatch.ts` 已持有精确 key，worker 端 `ingestionQueue.ts:161-174` 分支已存在，只需放开条件并让 job payload 携带 `fileKeys[]`）。
   LIST 是 O(前缀对象数) 的请求，MinIO 上尤贵；这一步消除 worker 端最大的延迟方差来源。
2. **上传前压缩**：`uploadJson` 的 body 先 zstd/gzip 再 PUT（`Content-Encoding` 标注）。JSON 压缩比 5–10×，直接降 MinIO 磁盘写带宽与容量，PUT 时延同步下降。
3. **微批聚合**：现按 eventBodyId 分组（同实体同调用合并）；可再跨实体聚合——内存缓冲按 `projectId` 攒批，N 条或 T ms 刷成一个对象（`uploadJson` 本就接受数组，worker 端解析处按 `entityType` 拆分即可）。PUT 次数可再降 5–20×。
4. **MinIO 侧配置**：事件桶与媒体桶隔离；对象键前缀加时间片+hash 摊开（避免顺序前缀热点）；纠删码 EC:4 或 NVMe 池承载事件桶；确认 `LANGFUSE_S3_CONCURRENT_READS` 与连接池上限匹配 MinIO 的 API 并发预算。
5. **背压显式化**：现有 S3 SlowDown → secondary 队列是被动信号；加上 producer 侧熔断（连续失败率阈值）主动降速，避免整批同步失败放大为 SDK 重试风暴。

### 3.2 若执行 §2 的 Kafka 内联方案

摄取热路径不再写/读 S3（仅 >512KB 的溢出事件走对象存储），§3.1 的 1–3 项自动失效为兜底路径；
S3/MinIO 回归其擅长的角色：媒体 blob、批量导出、事件溢出、合规冷存。

### 3.3 验证方案

- 压测：`pnpm run seed` 造基线数据 + 自定义 producer（或多 worker 并发 SDK 回放）打流量；
- 对比指标：摄取 API P99（S3 PUT 路径占比）、worker 端到端处理直方图（WorkerManager 现成）、ClickHouse 落库延迟（事件 timestamp → `created_at`）、Redis ops/s、MinIO API QPS/带宽；
- 验收线示例：切换后摄取端到端 P99 不劣化、队列 lag 在 2× 流量下有界、重放 1 小时窗口数据与 BullMQ 路径逐条 diff 为零差异。

---

## 4. 决策清单（动手前需要拍板）

1. Kafka 形态：托管（MSK/Confluent）vs 自建 KRaft vs Redpanda（API 兼容、运维更轻）。
2. 上游策略：纯 fork 维护，还是把 backend 抽象层作为 PR 推给上游（决定长期合并成本）。
3. 多租户公平性：分区级隔离（大租户专属分区）vs producer 配额，二者选一先做。
4. 保留在 Redis 上的控制面队列清单确认（本文 §2.1 的划分是否符合实际流量画像）。
5. 压缩算法：zstd（CPU 换带宽，推荐）vs lz4（更低延迟）。
