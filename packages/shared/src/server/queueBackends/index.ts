import { env } from "../../env";
import { QueueName } from "../queues";
import { ingestionBullMQPublisher } from "./bullmqBackend";
import { KafkaEventPublisher } from "./kafkaBackend";
import { resolveQueueBackend } from "./topics";
import type { EventPublisher } from "./types";

export * from "./types";
export * from "./topics";
export * from "./bullmqBackend";
export * from "./kafkaBackend";

/**
 * Publisher for the v3 ingestion queue on the configured backend. Kafka mode
 * is expected to inline event bodies (processEventBatch decides per event);
 * the publisher itself is backend-neutral.
 */
export const getIngestionEventPublisher = (): EventPublisher => {
  const backend = resolveQueueBackend(env.QUEUE_BACKEND_INGESTION);
  return backend === "kafka"
    ? new KafkaEventPublisher(QueueName.IngestionQueue)
    : ingestionBullMQPublisher();
};
