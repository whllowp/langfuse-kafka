import { Queue } from "bullmq";

import { QueueJobs, QueueName } from "../queues";
import { IngestionQueue } from "../redis/ingestionQueue";
import type { EventPublisher, QueueMessage } from "./types";

/**
 * BullMQ publisher wrapping the existing sharded queue classes. Payloads and
 * options pass through verbatim, so the redis path is byte-for-byte the
 * pre-abstraction behavior.
 */
export class BullMQEventPublisher implements EventPublisher {
  readonly backend = "redis" as const;

  constructor(
    readonly queueName: string,
    private readonly resolveQueue: (shardingKey?: string) => Queue | null,
  ) {}

  async publish(
    message: QueueMessage,
    options?: { shardingKey?: string; delayMs?: number },
  ) {
    await this.publishBatch([{ message, shardingKey: options?.shardingKey }], {
      delayMs: options?.delayMs,
    });
  }

  async publishBatch(
    entries: { message: QueueMessage; shardingKey?: string }[],
    options?: { delayMs?: number },
  ) {
    if (entries.length === 0) return;
    // Sharded deployments resolve different keys to different queue
    // instances; addBulk is per-queue, so group first. Queue instances are
    // cached per shard, so instance identity is the grouping key.
    const byQueue = new Map<Queue, { message: QueueMessage }[]>();
    for (const entry of entries) {
      const queue = this.resolveQueue(entry.shardingKey);
      if (!queue) {
        throw new Error(`Failed to instantiate queue ${this.queueName}`);
      }
      const list = byQueue.get(queue) ?? [];
      list.push({ message: entry.message });
      byQueue.set(queue, list);
    }
    await Promise.all(
      Array.from(byQueue).map(([queue, list]) =>
        queue.addBulk(
          list.map(({ message }) => ({
            name: message.name as QueueJobs,
            data: {
              id: message.id,
              timestamp: message.timestamp,
              name: message.name,
              payload: message.payload,
            },
            opts:
              options?.delayMs !== undefined ? { delay: options.delayMs } : {},
          })),
        ),
      ),
    );
  }

  async close() {
    // Queue instances are process-shared singletons; their connections are
    // released by disconnectAllRedisInstances on shutdown.
  }
}

export const ingestionBullMQPublisher = (): EventPublisher =>
  new BullMQEventPublisher(QueueName.IngestionQueue, (shardingKey?: string) =>
    IngestionQueue.getInstance(shardingKey ? { shardingKey } : {}),
  );
