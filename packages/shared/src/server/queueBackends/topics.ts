import { env } from "../../env";
import { logger } from "../logger";
import { QueueName } from "../queues";
import type { QueueBackendName } from "./types";

/**
 * Queue-name → Kafka-topic mapping. The queue-name contract in queues.ts is
 * the shared contract between both backends; topics are an internal detail of
 * the Kafka backend. Only queues selected for migration are listed — an
 * unmapped queue requested on the Kafka backend is a configuration error,
 * surfaced loudly instead of silently spawning a topic.
 */
const KAFKA_TOPICS: Partial<Record<QueueName, string>> = {
  [QueueName.IngestionQueue]: "ingestion.v1",
  [QueueName.IngestionSecondaryQueue]: "ingestion.secondary.v1",
};

export const topicForQueue = (queueName: QueueName): string => {
  const mapped = KAFKA_TOPICS[queueName];
  if (!mapped) {
    throw new Error(
      `Queue ${queueName} has no Kafka topic mapping; keep it on the redis backend`,
    );
  }
  return `${env.LANGFUSE_KAFKA_TOPIC_PREFIX}.${mapped}`;
};

/** Kafka consumer group id for a queue — one group per queue, stable across deploys. */
export const consumerGroupIdForQueue = (queueName: QueueName): string =>
  `worker-${queueName}`;

/**
 * Resolve the backend for a queue: per-queue override first, then the global
 * default. Kafka requires brokers to be configured; missing brokers downgrades
 * to redis with an error so a half-configured deployment fails visibly at the
 * queue layer instead of crashing env parsing.
 */
export const resolveQueueBackend = (
  override: QueueBackendName | undefined,
): QueueBackendName => {
  const backend = override ?? env.QUEUE_BACKEND;
  if (backend === "kafka" && !env.LANGFUSE_KAFKA_BROKERS) {
    logger.error(
      "QUEUE_BACKEND=kafka but LANGFUSE_KAFKA_BROKERS is not set; falling back to redis",
    );
    return "redis";
  }
  return backend;
};
