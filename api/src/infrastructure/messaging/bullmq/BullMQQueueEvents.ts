import { QueueEvents } from 'bullmq';
import type { Redis } from 'ioredis';
import { DEFAULT_QUEUE_NAME } from './BullMQJobQueueService.js';

export type JobEventHandler = (jobId: string) => void;
export type JobFailedHandler = (jobId: string, failedReason: string) => void;

/**
 * Minimal interface for QueueEvents — allows injection in tests.
 */
export interface QueueEventsLike {
  on(event: 'active' | 'completed', handler: (args: { jobId: string }) => void): this;
  on(event: 'failed', handler: (args: { jobId: string; failedReason: string }) => void): this;
  off(event: string, handler: (...args: unknown[]) => void): this;
  close(): Promise<void>;
}

/**
 * Wrapper around BullMQ QueueEvents for listening to job lifecycle events.
 * Uses a separate Redis connection as required by BullMQ.
 */
export class BullMQQueueEvents {
  private readonly queueEvents: QueueEventsLike;

  constructor(redis: Redis, queueName = DEFAULT_QUEUE_NAME) {
    this.queueEvents = new QueueEvents(queueName, { connection: redis });
  }

  /**
   * For testing: inject a mock QueueEventsLike.
   */
  static withInstance(instance: QueueEventsLike): BullMQQueueEvents {
    const obj = Object.create(BullMQQueueEvents.prototype) as BullMQQueueEvents;
    (obj as unknown as { queueEvents: QueueEventsLike }).queueEvents = instance;
    return obj;
  }

  onJobActive(handler: JobEventHandler): void {
    this.queueEvents.on('active', ({ jobId }) => { handler(jobId); });
  }

  onJobCompleted(handler: JobEventHandler): void {
    this.queueEvents.on('completed', ({ jobId }) => { handler(jobId); });
  }

  onJobFailed(handler: JobFailedHandler): void {
    this.queueEvents.on('failed', ({ jobId, failedReason }) => { handler(jobId, failedReason); });
  }

  async close(): Promise<void> {
    await this.queueEvents.close();
  }
}
