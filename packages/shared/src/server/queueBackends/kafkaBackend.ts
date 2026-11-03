import {
  CompressionTypes,
  Kafka,
  logLevel,
  type Consumer,
  type EachBatchPayload,
  type Producer,
} from "kafkajs";
import { z } from "zod";

import { env } from "../../env";
import { recordIncrement } from "../instrumentation";
import { logger } from "../logger";
import { QueueName } from "../queues";
import { consumerGroupIdForQueue, topicForQueue } from "./topics";
import type {
  ConsumedMessage,
  EventConsumer,
  EventPublisher,
  QueueMessage,
} from "./types";

/**
 * Wire envelope for Kafka messages. Carries the BullMQ job fields so the
 * consumer can synthesize a job object and run the same processors.
 * Timestamps are ISO strings — BullMQ JSON-serializes `new Date()` payload
 * fields the same way, so processors see an identical shape either way.
 */
export const KafkaEnvelopeSchema = z.object({
  version: z.literal(1),
  id: z.string(),
  name: z.string(),
  timestamp: z.iso.datetime(),
  payload: z.unknown(),
});

export type KafkaEnvelope = z.infer<typeof KafkaEnvelopeSchema>;

/**
 * Shared kafkajs client and producer per process. kafkajs producers are safe
 * for concurrent use and multiplex over one connection pool per broker, so
 * all queue publishers share a single producer.
 */
const globalForKafka = globalThis as unknown as {
  __langfuseKafkaClient?: Kafka;
  __langfuseKafkaProducer?: Promise<Producer>;
};

export const getKafkaClient = (): Kafka => {
  if (!globalForKafka.__langfuseKafkaClient) {
    globalForKafka.__langfuseKafkaClient = new Kafka({
      clientId: env.LANGFUSE_KAFKA_CLIENT_ID,
      brokers: env.LANGFUSE_KAFKA_BROKERS ?? [],
      // kafkajs logs to its own console logger; we route issues through our
      // logger from the error events below instead.
      logLevel: logLevel.NOTHING,
    });
  }
  return globalForKafka.__langfuseKafkaClient;
};

export const getKafkaProducer = (): Promise<Producer> => {
  if (!globalForKafka.__langfuseKafkaProducer) {
    const producer = getKafkaClient().producer({
      idempotent: true,
      allowAutoTopicCreation: false,
      transactionTimeout: 30_000,
    });
    const connected = producer.connect().then(() => {
      producer.on("producer.disconnect", (e) =>
        logger.warn("Kafka producer disconnected", e),
      );
      producer.on("producer.network.request_timeout", (e) =>
        logger.error("Kafka producer request timed out", e),
      );
      return producer;
    });
    // Allow a later publish to retry the connection.
    connected.catch(() => {
      if (globalForKafka.__langfuseKafkaProducer === connected) {
        globalForKafka.__langfuseKafkaProducer = undefined;
      }
    });
    globalForKafka.__langfuseKafkaProducer = connected;
  }
  return globalForKafka.__langfuseKafkaProducer;
};

/**
 * Create topics if absent (dev + first-deploy convenience). The partition
 * count of an existing topic is never modified — grow partitions
 * deliberately with an ops runbook, because repartitioning changes
 * key→partition assignment mid-stream.
 */
export const ensureTopics = async (queueNames: QueueName[]): Promise<void> => {
  const admin = getKafkaClient().admin();
  try {
    await admin.connect();
    const topics = Array.from(
      new Set(queueNames.map((queueName) => topicForQueue(queueName))),
    );
    const existing = new Set(
      (await admin.listTopics()).map((t) => t.toLowerCase()),
    );
    const missing = topics.filter((t) => !existing.has(t.toLowerCase()));
    if (missing.length > 0) {
      await admin.createTopics({
        topics: missing.map((topic) => ({
          topic,
          numPartitions: env.LANGFUSE_KAFKA_TOPIC_PARTITIONS,
          replicationFactor: env.LANGFUSE_KAFKA_TOPIC_REPLICATION_FACTOR,
        })),
      });
      logger.info("Created Kafka topics", { topics: missing });
    }
  } finally {
    await admin.disconnect().catch(() => {});
  }
};

export class KafkaEventPublisher implements EventPublisher {
  readonly backend = "kafka" as const;

  constructor(readonly queueName: string) {}

  async publish(message: QueueMessage, options?: { shardingKey?: string }) {
    await this.publishBatch([{ message, shardingKey: options?.shardingKey }]);
  }

  async publishBatch(
    entries: { message: QueueMessage; shardingKey?: string }[],
    _options?: { delayMs?: number },
  ) {
    if (entries.length === 0) return;
    const producer = await getKafkaProducer();
    // acks/compression are per-produce-request in kafkajs 2.x, and the
    // idempotent producer pins acks to "all insync replicas" on its own.
    // There is no linger knob in this client version: batches are shaped
    // by naturally overlapping produce calls, which is why producers
    // should batch per API request instead of one call per event.
    await producer.send({
      topic: topicForQueue(this.queueName as QueueName),
      ...(env.LANGFUSE_KAFKA_COMPRESSION === "gzip"
        ? { compression: CompressionTypes.GZIP }
        : {}),
      messages: entries.map(({ message, shardingKey }) => {
        const envelope: KafkaEnvelope = {
          version: 1,
          id: message.id,
          name: message.name,
          timestamp: message.timestamp.toISOString(),
          payload: message.payload,
        };
        return {
          key: shardingKey,
          value: JSON.stringify(envelope),
          headers: { schemaVersion: "1" },
        };
      }),
    });
  }

  async close() {
    // The producer is process-shared; closing it here would break other
    // publishers. Process shutdown owns the lifecycle.
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** JSON.parse + schema parse in one step; null on any malformation. */
const parseEnvelope = (raw: string) => {
  try {
    return KafkaEnvelopeSchema.safeParse(JSON.parse(raw));
  } catch {
    return null;
  }
};

export type KafkaConsumerOptions = {
  /**
   * Number of partitions processed concurrently. Replaces the BullMQ worker
   * concurrency knob: per-partition processing stays sequential (ordering),
   * parallelism comes from partition count.
   */
  partitionsConsumedConcurrently?: number;
  /**
   * Messages processed concurrently within one partition while preserving
   * per-key order (messages of different entities sharing a partition run
   * in parallel; same-key messages stay strictly sequential). This is what
   * closes the gap to BullMQ's concurrency for IO-bound processors: a
   * sequential per-partition loop would serialize their latency. Offsets
   * commit at the batch's contiguous processed prefix, so a crash only
   * redelivers unprocessed or post-prefix messages (at-least-once).
   * Default 1 keeps the plain sequential behavior.
   */
  handlerConcurrencyPerPartition?: number;
  /**
   * Overrides the consumer group id (`worker-<queue>` by default). Used by
   * tooling that must read the stream independently — benchmarks and
   * timestamp-based replays — without disturbing worker offset positions.
   */
  groupId?: string;
};

export class KafkaEventConsumer implements EventConsumer {
  readonly backend = "kafka" as const;

  private consumer: Consumer | null = null;
  private stopped = false;

  constructor(
    readonly queueName: string,
    private options: KafkaConsumerOptions = {},
  ) {}

  async subscribe(handler: (message: ConsumedMessage) => Promise<void>) {
    if (this.consumer) {
      logger.warn(`Kafka consumer for ${this.queueName} already subscribed`);
      return;
    }

    const consumer = getKafkaClient().consumer({
      groupId:
        this.options.groupId ??
        consumerGroupIdForQueue(this.queueName as QueueName),
      sessionTimeout: env.LANGFUSE_KAFKA_SESSION_TIMEOUT_MS,
      rebalanceTimeout: env.LANGFUSE_KAFKA_SESSION_TIMEOUT_MS * 2,
    });
    this.consumer = consumer;

    consumer.on(consumer.events.CRASH, (e) => {
      logger.error(
        `Kafka consumer for ${this.queueName} crashed`,
        e.payload.error,
      );
    });
    consumer.on(consumer.events.DISCONNECT, () =>
      logger.warn(`Kafka consumer for ${this.queueName} disconnected`),
    );

    await consumer.connect();
    await consumer.subscribe({
      topic: topicForQueue(this.queueName as QueueName),
      fromBeginning: true,
    });

    await consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently:
        this.options.partitionsConsumedConcurrently ?? 1,
      eachBatch: async (payload: EachBatchPayload) => {
        if (this.stopped) return;
        const concurrency = Math.max(
          1,
          this.options.handlerConcurrencyPerPartition ?? 1,
        );

        // One task per message in offset order. Malformed messages are
        // pre-marked done so they never block the commit water mark.
        type BatchTask = {
          message: { offset: string; key?: Buffer | string | null };
          consumed: ConsumedMessage | null;
          started: boolean;
          done: boolean;
          /** True when every earlier same-key message in this batch finished. */
          prevDone?: boolean;
          /** Index of the next message with the same key in this batch. */
          nextIndex?: number;
        };
        const tasks: BatchTask[] = payload.batch.messages.map((message) => {
          const parsed = parseEnvelope(message.value?.toString() ?? "");
          if (!parsed?.success) {
            logger.error(
              `Discarding malformed Kafka message on ${payload.batch.topic}/${payload.batch.partition}:${message.offset}`,
              parsed?.error,
            );
            return { message, consumed: null, started: true, done: true };
          }
          return {
            message,
            consumed: {
              id: parsed.data.id,
              name: parsed.data.name,
              payload: parsed.data.payload,
              timestamp: new Date(parsed.data.timestamp),
            } satisfies ConsumedMessage,
            started: false,
            done: false,
          };
        });

        if (concurrency === 1) {
          for (const task of tasks) {
            if (this.stopped) return;
            if (task.consumed) {
              await this.processTask(handler, task, payload.batch);
            }
            payload.resolveOffset(task.message.offset);
          }
          await payload.commitOffsetsIfNecessary();
          return;
        }

        // Parallel path: a task is eligible when its same-key predecessor
        // within this batch has finished; a pool of `concurrency` workers
        // picks eligible tasks in offset order.
        const nextOfKey = new Map<string, number>();
        for (let i = 0; i < tasks.length; i++) {
          const key = tasks[i].message.key?.toString() ?? `__nokey_${i}`;
          const prevIndex = nextOfKey.get(key);
          if (prevIndex !== undefined) {
            tasks[prevIndex].nextIndex = i;
          } else {
            // First message of its key: immediately eligible.
            tasks[i].prevDone = true;
          }
          nextOfKey.set(key, i);
        }

        let remaining = tasks.filter((t) => !t.done).length;
        const pickAndRun = async () => {
          while (remaining > 0 && !this.stopped) {
            const task = tasks.find((t) => !t.started && t.prevDone === true);
            if (!task) {
              // All unfinished tasks wait on same-key predecessors that are
              // being processed by other workers.
              await new Promise((r) => setTimeout(r, 0));
              continue;
            }
            task.started = true;
            await this.processTask(handler, task, payload.batch);
            task.done = true;
            remaining--;
            if (task.nextIndex !== undefined) {
              tasks[task.nextIndex].prevDone = true;
            }
          }
        };
        await Promise.all(
          Array.from(
            { length: Math.min(concurrency, tasks.length) },
            pickAndRun,
          ),
        );

        // Commit at the contiguous processed prefix so an unprocessed gap
        // keeps every later offset uncommitted (redelivery on restart).
        let prefixEnd = 0;
        while (prefixEnd < tasks.length && tasks[prefixEnd].done) {
          prefixEnd++;
        }
        if (prefixEnd > 0) {
          payload.resolveOffset(tasks[prefixEnd - 1].message.offset);
        }
        await payload.commitOffsetsIfNecessary();
      },
    });
  }

  private async processTask(
    handler: (message: ConsumedMessage) => Promise<void>,
    task: { consumed: ConsumedMessage | null; message: { offset: string } },
    batch: { topic: string; partition: number },
  ): Promise<void> {
    if (!task.consumed) return;
    const succeeded = await this.processWithRetries(
      handler,
      task.consumed,
      `${batch.topic}/${batch.partition}:${task.message.offset}`,
    );
    if (!succeeded) {
      recordIncrement("langfuse.queue.kafka.skipped_after_retries", 1, {
        queue: this.queueName,
      });
    }
  }

  /**
   * Bounded in-place retry replacing BullMQ's attempts/backoff semantics
   * (LANGFUSE_KAFKA_MAX_CONSUME_ATTEMPTS, ~1s linear backoff). Returns
   * whether the message eventually succeeded. A message that still fails is
   * skipped, not retried across restarts: without a durable retry topic the
   * redelivery would loop forever. The retry-topic service (design §2.4,
   * migration phase 6) will own durable dead-lettering; until then the skip
   * is logged and counted.
   */
  private async processWithRetries(
    handler: (message: ConsumedMessage) => Promise<void>,
    message: ConsumedMessage,
    logKey: string,
  ): Promise<boolean> {
    const maxAttempts = env.LANGFUSE_KAFKA_MAX_CONSUME_ATTEMPTS;
    let lastError: unknown;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await handler(message);
        return true;
      } catch (error) {
        lastError = error;
        if (attempt < maxAttempts) {
          await sleep(Math.min(1000 * attempt, 5000));
        }
      }
    }
    logger.error(
      `Kafka message failed after ${maxAttempts} attempts, skipping: ${logKey}`,
      lastError,
    );
    return false;
  }

  async pause() {
    // Omitting partitions pauses/resumes every currently assigned partition
    // of the topic; kafkajs 2.x exposes no assignments() accessor.
    this.consumer?.pause([
      { topic: topicForQueue(this.queueName as QueueName) },
    ]);
  }

  async resume() {
    this.consumer?.resume([
      { topic: topicForQueue(this.queueName as QueueName) },
    ]);
  }

  async stop() {
    this.stopped = true;
    await this.consumer?.disconnect();
    this.consumer = null;
  }
}
