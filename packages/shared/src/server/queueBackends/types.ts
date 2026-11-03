/**
 * Queue backend abstraction for event-stream queues.
 *
 * Two implementations exist behind these interfaces: the default BullMQ/Redis
 * path (behavior identical to the pre-abstraction code) and a Kafka backend
 * for high-throughput event ingestion. Backend selection is env-driven
 * (`QUEUE_BACKEND`, with per-queue overrides such as
 * `QUEUE_BACKEND_INGESTION`), so a single deployment can migrate queues one
 * at a time and roll back per queue.
 *
 * Control-plane queues (cron scheduling, delayed retries, distributed locks,
 * rate limiting) stay on BullMQ and are not expected to migrate — see
 * SCALING_OPTIMIZATION_DESIGN.md §2.1.
 */

export type QueueBackendName = "redis" | "kafka";

export type PublishOptions = {
  /**
   * Co-partitioning key. BullMQ hashes it into a shard; Kafka uses it as the
   * message key so same-entity messages land on the same partition in order.
   * Should be `projectId-<entityId>` for ingestion queues.
   */
  shardingKey?: string;
  /**
   * Delay in ms before the message becomes consumable. Honored by BullMQ
   * only — Kafka has no delayed delivery; ingestion's date-boundary delay is
   * unnecessary there because per-partition ordering replaces the
   * cross-shard merge window.
   */
  delayMs?: number;
};

/** Backend-agnostic job/message shape passed to publishers. */
export type QueueMessage = {
  id: string;
  name: string;
  payload: unknown;
  timestamp: Date;
};

export interface EventPublisher {
  readonly backend: QueueBackendName;
  readonly queueName: string;
  publish(message: QueueMessage, options?: PublishOptions): Promise<void>;
  /**
   * Publish a batch in one backend call (BullMQ addBulk / one Kafka produce
   * request). Per-entry keys still route entities to the same
   * shard/partition. Kafka throughput is call-granularity bound — many
   * single-message produce calls throttle to the linger cadence — so
   * producers with a natural batch (one API ingestion request) should use
   * this instead of N awaited publish() calls.
   */
  publishBatch(
    entries: { message: QueueMessage; shardingKey?: string }[],
    options?: PublishOptions,
  ): Promise<void>;
  close(): Promise<void>;
}

export type ConsumedMessage = QueueMessage;

export type MessageHandler = (message: ConsumedMessage) => Promise<void>;

export interface EventConsumer {
  readonly backend: QueueBackendName;
  readonly queueName: string;
  /**
   * Resolves once the subscription is running (BullMQ worker up / consumer
   * group joined). Delivery contract mirrors a BullMQ processor: a handler
   * that resolves acknowledges the message; a handler that rejects triggers
   * the backend's retry policy (BullMQ defaultJobOptions / bounded in-place
   * retries then skip on Kafka).
   */
  subscribe(handler: MessageHandler): Promise<void>;
  pause(): Promise<void>;
  resume(): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Minimal job shape consumed by worker processors. BullMQ `Job` satisfies
 * this structurally, which lets the same processor run behind either backend
 * (the Kafka consumer synthesizes one per message).
 */
export type QueueJobLike<TData = any> = {
  id: string;
  name: string;
  data: TData;
  /** Epoch ms when the job was produced (BullMQ `Job#timestamp`). */
  timestamp: number;
};
