/**
 * Queue-layer performance benchmark: BullMQ (redis) vs Kafka backend for the
 * ingestion event stream (SCALING_OPTIMIZATION_DESIGN.md §3.3).
 *
 * Measures only the queue layer — produce throughput, consume throughput and
 * produce→consume latency — with a no-op processor, so the numbers isolate
 * the backend difference from S3/ClickHouse costs.
 *
 * Scenarios:
 *   - bullmq-ref:    current production shape — small S3-reference payload
 *                    (the S3 PUT itself is out of scope here)
 *   - bullmq-inline: event bodies inlined into the BullMQ job (shows why the
 *                    S3 detour exists: redis memory/ops cost per large job)
 *   - kafka-inline:  new path — event bodies inlined into Kafka messages
 *
 * Usage:
 *   pnpm --filter @langfuse/shared run benchmark:queue-backend \
 *     [--messages 20000] [--concurrency 20] [--event-bytes 2048] \
 *     [--handler-ms 5] [--kafka-handler-concurrency 4]
 *     [--backends bullmq-ref,bullmq-inline,kafka-inline] \
 *     [--redis-url redis://:myredissecret@localhost:6379] \
 *     [--kafka-brokers localhost:9094]
 */

// Env bootstrap must run before any @langfuse/shared module parses env.
const args = parseArgs(process.argv.slice(2));
const setDefault = (key: string, value: string) => {
  if (process.env[key] === undefined) {
    process.env[key] = value;
  }
};
setDefault(
  "REDIS_CONNECTION_STRING",
  args["redis-url"] ?? "redis://:myredissecret@localhost:6379",
);
setDefault("LANGFUSE_KAFKA_BROKERS", args["kafka-brokers"] ?? "localhost:9094");
// Placeholders only to satisfy shared env validation; the benchmark touches
// neither ClickHouse nor Postgres.
setDefault("CLICKHOUSE_URL", "http://localhost:8123");
setDefault("CLICKHOUSE_USER", "clickhouse");
setDefault("CLICKHOUSE_PASSWORD", "clickhouse");
setDefault(
  "DATABASE_URL",
  "postgresql://postgres:postgres@localhost:5432/postgres",
);
setDefault("LANGFUSE_S3_EVENT_UPLOAD_BUCKET", "benchmark-placeholder");
// Isolate benchmark topics from dev data; partitions follow --concurrency.
process.env.LANGFUSE_KAFKA_TOPIC_PREFIX = "langfuse-bench";

const MESSAGES = numberArg(args, "messages", 20_000);
const CONCURRENCY = numberArg(args, "concurrency", 20);
const EVENT_BYTES = numberArg(args, "event-bytes", 2_048);
// Simulated per-message processing latency (ms) applied to BOTH backends'
// consume handlers - models the S3/ClickHouse IO cost of real ingestion
// jobs, the regime where handler parallelism decides throughput.
const HANDLER_MS = numberArg(args, "handler-ms", 0);
// Key-affinity lanes per partition on the kafka backend (1 = sequential).
const KAFKA_HANDLER_CONCURRENCY = numberArg(
  args,
  "kafka-handler-concurrency",
  1,
);
const PRODUCE_BATCH = 100; // mirrors a large SDK ingestion batch
const SCENARIOS = (
  args["backends"] ?? "bullmq-ref,bullmq-inline,kafka-inline"
).split(",");

function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    if (!argv[i].startsWith("--")) continue;
    const key = argv[i].slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[key] = next;
      i++;
    } else {
      out[key] = "true";
    }
  }
  return out;
}

function numberArg(
  args: Record<string, string>,
  key: string,
  def: number,
): number {
  const v = args[key];
  if (v === undefined) return def;
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`--${key} must be > 0`);
  return Math.floor(n);
}

type ScenarioResult = {
  scenario: string;
  messages: number;
  payloadBytesPerMessage: number;
  produceMs: number;
  produceRatePerS: number;
  produceMbPerS: number;
  consumeMs: number;
  consumeRatePerS: number;
  publishP50Ms: number;
  publishP95Ms: number;
  publishP99Ms: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  latencyP99Ms: number;
};

const percentile = (values: number[], p: number): number => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[idx];
};

function printTable(results: ScenarioResult[]) {
  console.log(
    `\n| scenario | msg/s produce | MB/s produce | msg/s consume | pub p50/p95/p99 ms | e2e p50/p95/p99 ms |`,
  );
  console.log(`|---|---|---|---|---|---|`);
  for (const r of results) {
    console.log(
      `| ${r.scenario} | ${Math.round(r.produceRatePerS).toLocaleString()} | ${r.produceMbPerS.toFixed(1)} | ${Math.round(r.consumeRatePerS).toLocaleString()} | ${r.publishP50Ms.toFixed(1)} / ${r.publishP95Ms.toFixed(1)} / ${r.publishP99Ms.toFixed(1)} | ${r.latencyP50Ms.toFixed(1)} / ${r.latencyP95Ms.toFixed(1)} / ${r.latencyP99Ms.toFixed(1)} |`,
    );
  }
}

function makeQueuePayload(i: number, inline: boolean, bytes: number) {
  const filler = "x".repeat(Math.max(0, bytes - 512));
  const event = {
    type: "span-create",
    id: `bench-event-${i}`,
    timestamp: new Date().toISOString(),
    body: {
      id: `bench-span-${i}`,
      traceId: `bench-trace-${i % 1_000}`,
      name: "queue-benchmark-span",
      startTime: new Date().toISOString(),
      endTime: new Date().toISOString(),
      environment: "default",
      input: JSON.stringify({
        prompt: filler.slice(0, Math.floor(filler.length / 2)),
      }),
      output: JSON.stringify({
        completion: filler.slice(Math.floor(filler.length / 2)),
      }),
      metadata: { bench: true },
    },
  };
  return {
    data: {
      type: "span-create" as const,
      eventBodyId: `bench-span-${i}`,
      ...(inline
        ? { inlineEvents: [event] }
        : { fileKey: `bench-event-${i}`, skipS3List: true }),
      bucketPrefix: `bench/project-${i % 10}/span/bench-span-${i}/`,
      ingestionApiKey: "bench-key",
      ingestionSdkName: "benchmark",
      ingestionSdkVersion: "1.0.0",
    },
    authCheck: {
      validKey: true as const,
      scope: { projectId: `bench-project-${i % 10}` },
    },
  };
}

async function main() {
  const { QueueJobs, QueueName } = await import("../src/server/queues.js");
  const {
    ingestionBullMQPublisher,
    KafkaEventConsumer,
    KafkaEventPublisher,
    ensureTopics,
    getKafkaClient,
    topicForQueue,
  } = await import("../src/server/queueBackends/index.js");
  const { createBullMQWorkerOptionsWithRedis } =
    await import("../src/server/redis/redis.js");
  const { IngestionQueue } =
    await import("../src/server/redis/ingestionQueue.js");
  const { Worker } = await import("bullmq");
  const fs = await import("fs");
  const path = await import("path");

  const results: ScenarioResult[] = [];
  const runId = Date.now();

  for (const scenario of SCENARIOS) {
    if (!["bullmq-ref", "bullmq-inline", "kafka-inline"].includes(scenario)) {
      throw new Error(`Unknown scenario: ${scenario}`);
    }
    console.log(`\n=== ${scenario} ===`);
    const inline = scenario.endsWith("inline");
    const isKafka = scenario.startsWith("kafka");

    const payloads = Array.from({ length: MESSAGES }, (_, i) =>
      makeQueuePayload(i, inline, EVENT_BYTES),
    );
    const payloadBytes = Buffer.byteLength(JSON.stringify(payloads[0]));
    const publishDurations: number[] = [];
    let consumedCount = 0;

    if (isKafka) {
      // Recreate the topic so the partition count always matches
      // --concurrency for this run.
      process.env.LANGFUSE_KAFKA_TOPIC_PARTITIONS = String(CONCURRENCY);
      const admin = getKafkaClient().admin();
      await admin.connect();
      await admin
        .deleteTopics({ topics: [topicForQueue(QueueName.IngestionQueue)] })
        .catch(() => {});
      await admin.disconnect();
      await ensureTopics([QueueName.IngestionQueue]);
    }

    // --- Produce -----------------------------------------------------------
    const publisher = isKafka
      ? new KafkaEventPublisher(QueueName.IngestionQueue)
      : ingestionBullMQPublisher();

    // Warm up connections (redis client dial / kafka metadata fetch) so the
    // first scenario is not penalized by setup cost.
    await Promise.all(
      Array.from({ length: 100 }, (_, j) =>
        publisher.publish(
          {
            id: `bench-warmup-${j}-${runId}`,
            name: QueueJobs.IngestionJob,
            payload: payloads[j % payloads.length],
            timestamp: new Date(),
          },
          { shardingKey: `bench-warmup-${j}` },
        ),
      ),
    );

    // Producer pattern mirrors production: concurrent API-sized batches
    // (100 eventBodyId groups each), 5 batches in flight — a busy web pod.
    // The kafka backend publishes each batch as one produce call
    // (publishBatch); the redis backend publishes per group, like the
    // processEventBatch redis path. Kafka publish durations are therefore
    // per-batch acks, redis ones per-message acks.
    const SDK_BATCH = 100;
    const CONCURRENT_API_BATCHES = 5;
    const totalBatches = Math.ceil(MESSAGES / SDK_BATCH);
    const produceStart = performance.now();
    let nextBatch = 0;
    const batchWorker = async () => {
      while (true) {
        const b = nextBatch++;
        if (b >= totalBatches) return;
        const slice = payloads.slice(b * SDK_BATCH, (b + 1) * SDK_BATCH);
        const t0 = performance.now();
        if (isKafka) {
          await publisher.publishBatch(
            slice.map((payload, j) => ({
              message: {
                id: `bench-job-${b * SDK_BATCH + j}-${runId}`,
                name: QueueJobs.IngestionJob,
                payload,
                timestamp: new Date(),
              },
              shardingKey: `bench-project-${(b * SDK_BATCH + j) % 10}-bench-span-${b * SDK_BATCH + j}`,
            })),
          );
          publishDurations.push(performance.now() - t0);
        } else {
          await Promise.all(
            slice.map((payload, j) => {
              const t1 = performance.now();
              return publisher
                .publish(
                  {
                    id: `bench-job-${b * SDK_BATCH + j}-${runId}`,
                    name: QueueJobs.IngestionJob,
                    payload,
                    timestamp: new Date(),
                  },
                  {
                    shardingKey: `bench-project-${(b * SDK_BATCH + j) % 10}-bench-span-${b * SDK_BATCH + j}`,
                  },
                )
                .then(() => publishDurations.push(performance.now() - t1));
            }),
          );
        }
      }
    };
    await Promise.all(
      Array.from({ length: CONCURRENT_API_BATCHES }, batchWorker),
    );
    const produceMs = performance.now() - produceStart;

    // --- Consume -----------------------------------------------------------
    const consumedLatencies: number[] = [];
    const consumeStart = performance.now();
    let consumeDone!: () => void;
    const consumeDonePromise = new Promise<void>((r) => (consumeDone = r));

    if (isKafka) {
      const groupId = `bench-${scenario}-${runId}`;
      const consumer = new KafkaEventConsumer(QueueName.IngestionQueue, {
        partitionsConsumedConcurrently: CONCURRENCY,
        handlerConcurrencyPerPartition: KAFKA_HANDLER_CONCURRENCY,
        groupId,
      });
      await consumer.subscribe(async (message) => {
        // Warmup messages share the topic; only measured ones count.
        if (!message.id.startsWith("bench-job-")) return;
        if (HANDLER_MS > 0) {
          await new Promise((r) => setTimeout(r, HANDLER_MS));
        }
        consumedCount++;
        consumedLatencies.push(Date.now() - message.timestamp.getTime());
        if (consumedCount >= MESSAGES) consumeDone();
      });
      await consumeDonePromise;
      await consumer.stop();
      const admin = getKafkaClient().admin();
      await admin.connect();
      // Best-effort cleanup; a missing group (already expired) is fine.
      await admin.deleteGroups([groupId]).catch(() => {});
      await admin.disconnect();
    } else {
      const worker = new Worker(
        QueueName.IngestionQueue,
        async (job) => {
          // Warmup jobs share the queue; only measured ones count.
          if (!job.data.id.startsWith("bench-job-")) return;
          if (HANDLER_MS > 0) {
            await new Promise((r) => setTimeout(r, HANDLER_MS));
          }
          consumedCount++;
          consumedLatencies.push(Date.now() - job.timestamp);
          if (consumedCount >= MESSAGES) consumeDone();
        },
        {
          ...createBullMQWorkerOptionsWithRedis(QueueName.IngestionQueue)!,
          concurrency: CONCURRENCY,
        },
      );
      await consumeDonePromise;
      await worker.close();
      // Reset the queue so reruns start from a clean slate.
      await IngestionQueue.getInstance({})?.obliterate({ force: true });
    }
    const consumeMs = performance.now() - consumeStart;

    const result: ScenarioResult = {
      scenario,
      messages: MESSAGES,
      payloadBytesPerMessage: payloadBytes,
      produceMs,
      produceRatePerS: (MESSAGES / produceMs) * 1000,
      produceMbPerS: (MESSAGES * payloadBytes) / produceMs / 1000,
      consumeRatePerS: (consumedCount / consumeMs) * 1000,
      consumeMs,
      publishP50Ms: percentile(publishDurations, 50),
      publishP95Ms: percentile(publishDurations, 95),
      publishP99Ms: percentile(publishDurations, 99),
      latencyP50Ms: percentile(consumedLatencies, 50),
      latencyP95Ms: percentile(consumedLatencies, 95),
      latencyP99Ms: percentile(consumedLatencies, 99),
    };
    results.push(result);
    console.log(
      `produced ${MESSAGES} msgs (${payloadBytes} B each) in ${(produceMs / 1000).toFixed(1)}s; consumed ${consumedCount} in ${(consumeMs / 1000).toFixed(1)}s`,
    );
  }

  printTable(results);

  const resultsDir = path.resolve(process.cwd(), "scripts/results");
  fs.mkdirSync(resultsDir, { recursive: true });
  const outPath = path.join(resultsDir, `queue-backend-${runId}.json`);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        config: {
          messages: MESSAGES,
          concurrency: CONCURRENCY,
          eventBytes: EVENT_BYTES,
          handlerMs: HANDLER_MS,
          kafkaHandlerConcurrency: KAFKA_HANDLER_CONCURRENCY,
          produceBatch: PRODUCE_BATCH,
          kafkaBrokers: process.env.LANGFUSE_KAFKA_BROKERS,
          redis: process.env.REDIS_CONNECTION_STRING,
        },
        results,
      },
      null,
      2,
    ),
  );
  console.log(`\nResults written to ${outPath}`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
