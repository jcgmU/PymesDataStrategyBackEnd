import type { JobsOptions } from 'bullmq';
import { Queue } from 'bullmq';
import type { Redis } from 'ioredis';
import type {
  JobQueueService,
  JobPayload,
  JobResult,
} from '../../../domain/ports/services/JobQueueService.js';

/**
 * Queue interface for dependency injection and testing.
 */
export interface QueueLike {
  add(name: string, data: unknown, opts?: JobsOptions): Promise<unknown>;
  getJob(jobId: string): Promise<JobLike | undefined>;
  getJobCounts(): Promise<Record<string, number>>;
  close(): Promise<void>;
}

export interface JobLike {
  getState(): Promise<string>;
  remove(): Promise<void>;
}

/**
 * Configuration for BullMQ JobQueueService.
 */
export interface BullMQJobQueueConfig {
  redis: Redis;
  queueName?: string;
  defaultJobOptions?: JobsOptions;
  /** Optional queue instance for testing */
  queue?: QueueLike;
}

/**
 * Default queue name for ETL transformation jobs.
 */
export const DEFAULT_QUEUE_NAME = 'etl-transformations';

/**
 * BullMQ implementation of JobQueueService.
 * Handles enqueueing transformation jobs for the Python worker.
 */
export class BullMQJobQueueService implements JobQueueService {
  private readonly queue: QueueLike;
  private readonly defaultJobOptions: JobsOptions;

  constructor(config: BullMQJobQueueConfig) {
    // Use injected queue or create real BullMQ queue
    this.queue = config.queue ?? new Queue(config.queueName ?? DEFAULT_QUEUE_NAME, {
      connection: config.redis,
      defaultJobOptions: {
        removeOnComplete: {
          count: 1000, // Keep last 1000 completed jobs
          age: 24 * 3600, // Keep for 24 hours
        },
        removeOnFail: {
          count: 5000, // Keep last 5000 failed jobs for debugging
        },
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 1000,
        },
      },
    });

    this.defaultJobOptions = config.defaultJobOptions ?? {};
  }

  /**
   * Enqueue a transformation job.
   */
  async enqueue(payload: JobPayload): Promise<JobResult> {
    const jobOptions: JobsOptions = {
      ...this.defaultJobOptions,
      jobId: payload.jobId,
      priority: payload.priority ?? 5,
    };

    await this.queue.add(payload.transformationType, payload, jobOptions);

    return {
      jobId: payload.jobId,
      status: 'queued',
    };
  }

  /**
   * Get the status of a job.
   */
  async getStatus(jobId: string): Promise<JobResult | null> {
    const job = await this.queue.getJob(jobId);

    if (!job) {
      return null;
    }

    const state = await job.getState();
    let status: JobResult['status'];

    switch (state) {
      case 'completed':
        status = 'completed';
        break;
      case 'failed':
        status = 'failed';
        break;
      case 'active':
        status = 'processing';
        break;
      default:
        status = 'queued';
    }

    return {
      jobId,
      status,
    };
  }

  /**
   * Cancel a job.
   * Returns true if the job was successfully removed, false otherwise.
   */
  async cancel(jobId: string): Promise<boolean> {
    const job = await this.queue.getJob(jobId);

    if (!job) {
      return false;
    }

    const state = await job.getState();

    // Can only cancel jobs that are not active or completed
    if (state === 'active' || state === 'completed') {
      return false;
    }

    await job.remove();
    return true;
  }

  /**
   * Get the queue instance for advanced operations.
   */
  getQueue(): QueueLike {
    return this.queue;
  }

  /**
   * Close the queue connection gracefully.
   */
  async close(): Promise<void> {
    await this.queue.close();
  }

  /**
   * Check if the queue is healthy.
   */
  async healthCheck(): Promise<boolean> {
    try {
      await this.queue.getJobCounts();
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get job counts by state.
   */
  async getJobCounts(): Promise<Record<string, number>> {
    return this.queue.getJobCounts();
  }
}
