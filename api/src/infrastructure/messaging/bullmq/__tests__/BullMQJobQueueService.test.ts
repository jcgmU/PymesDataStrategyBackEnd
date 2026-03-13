import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Redis } from 'ioredis';
import {
  BullMQJobQueueService,
  DEFAULT_QUEUE_NAME,
  type QueueLike,
  type JobLike,
} from '../BullMQJobQueueService.js';
import type { JobPayload } from '../../../../domain/ports/services/JobQueueService.js';

describe('BullMQJobQueueService', () => {
  let service: BullMQJobQueueService;
  let mockRedis: Redis;
  let mockQueue: QueueLike;

  beforeEach(() => {
    vi.clearAllMocks();
    mockRedis = {} as Redis;

    // Create mock queue
    mockQueue = {
      add: vi.fn().mockResolvedValue({ id: 'test-job-id' }),
      getJob: vi.fn(),
      getJobCounts: vi.fn().mockResolvedValue({
        waiting: 0,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
      }),
      close: vi.fn().mockResolvedValue(undefined),
    };

    service = new BullMQJobQueueService({
      redis: mockRedis,
      queue: mockQueue,
    });
  });

  describe('constructor', () => {
    it('should use injected queue', () => {
      const queue = service.getQueue();
      expect(queue).toBe(mockQueue);
    });

    it('should use default queue name constant', () => {
      expect(DEFAULT_QUEUE_NAME).toBe('etl-transformations');
    });
  });

  describe('enqueue', () => {
    const samplePayload: JobPayload = {
      jobId: 'job-123',
      datasetId: 'dataset-456',
      userId: 'user-789',
      transformationType: 'CLEAN_NULLS',
      parameters: { action: 'clean' },
      sourceStorageKey: 'datasets/file.csv',
      priority: 10,
    };

    it('should enqueue a job successfully', async () => {
      const result = await service.enqueue(samplePayload);

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'queued',
      });
    });

    it('should call queue.add with correct parameters', async () => {
      await service.enqueue(samplePayload);

      expect(mockQueue.add).toHaveBeenCalledWith(
        'CLEAN_NULLS',
        samplePayload,
        expect.objectContaining({
          jobId: 'job-123',
          priority: 10,
        })
      );
    });

    it('should use default priority when not specified', async () => {
      const payloadWithoutPriority: JobPayload = {
        jobId: 'job-123',
        datasetId: 'dataset-456',
        userId: 'user-789',
        transformationType: 'CLEAN_NULLS',
        parameters: { action: 'clean' },
        sourceStorageKey: 'datasets/file.csv',
      };

      await service.enqueue(payloadWithoutPriority);

      expect(mockQueue.add).toHaveBeenCalledWith(
        'CLEAN_NULLS',
        payloadWithoutPriority,
        expect.objectContaining({
          priority: 5,
        })
      );
    });
  });

  describe('getStatus', () => {
    it('should return null for non-existent job', async () => {
      vi.mocked(mockQueue.getJob).mockResolvedValue(undefined);

      const result = await service.getStatus('non-existent');

      expect(result).toBeNull();
    });

    it('should return queued status for waiting job', async () => {
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('waiting'),
        remove: vi.fn(),
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.getStatus('job-123');

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'queued',
      });
    });

    it('should return processing status for active job', async () => {
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('active'),
        remove: vi.fn(),
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.getStatus('job-123');

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'processing',
      });
    });

    it('should return completed status for completed job', async () => {
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('completed'),
        remove: vi.fn(),
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.getStatus('job-123');

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'completed',
      });
    });

    it('should return failed status for failed job', async () => {
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('failed'),
        remove: vi.fn(),
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.getStatus('job-123');

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'failed',
      });
    });

    it('should return queued status for delayed job', async () => {
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('delayed'),
        remove: vi.fn(),
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.getStatus('job-123');

      expect(result).toEqual({
        jobId: 'job-123',
        status: 'queued',
      });
    });
  });

  describe('cancel', () => {
    it('should return false for non-existent job', async () => {
      vi.mocked(mockQueue.getJob).mockResolvedValue(undefined);

      const result = await service.cancel('non-existent');

      expect(result).toBe(false);
    });

    it('should return false for active job', async () => {
      const mockRemove = vi.fn();
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('active'),
        remove: mockRemove,
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.cancel('job-123');

      expect(result).toBe(false);
      expect(mockRemove).not.toHaveBeenCalled();
    });

    it('should return false for completed job', async () => {
      const mockRemove = vi.fn();
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('completed'),
        remove: mockRemove,
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.cancel('job-123');

      expect(result).toBe(false);
      expect(mockRemove).not.toHaveBeenCalled();
    });

    it('should remove and return true for waiting job', async () => {
      const mockRemove = vi.fn().mockResolvedValue(undefined);
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('waiting'),
        remove: mockRemove,
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.cancel('job-123');

      expect(result).toBe(true);
      expect(mockRemove).toHaveBeenCalled();
    });

    it('should remove and return true for delayed job', async () => {
      const mockRemove = vi.fn().mockResolvedValue(undefined);
      const mockJob: JobLike = {
        getState: vi.fn().mockResolvedValue('delayed'),
        remove: mockRemove,
      };
      vi.mocked(mockQueue.getJob).mockResolvedValue(mockJob);

      const result = await service.cancel('job-123');

      expect(result).toBe(true);
      expect(mockRemove).toHaveBeenCalled();
    });
  });

  describe('healthCheck', () => {
    it('should return true when queue is healthy', async () => {
      vi.mocked(mockQueue.getJobCounts).mockResolvedValue({
        waiting: 0,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
      });

      const result = await service.healthCheck();

      expect(result).toBe(true);
    });

    it('should return false when queue throws error', async () => {
      vi.mocked(mockQueue.getJobCounts).mockRejectedValue(new Error('Connection error'));

      const result = await service.healthCheck();

      expect(result).toBe(false);
    });
  });

  describe('getJobCounts', () => {
    it('should return job counts from queue', async () => {
      const counts = {
        waiting: 5,
        active: 2,
        completed: 100,
        failed: 3,
        delayed: 1,
      };
      vi.mocked(mockQueue.getJobCounts).mockResolvedValue(counts);

      const result = await service.getJobCounts();

      expect(result).toEqual(counts);
    });
  });

  describe('getQueue', () => {
    it('should return the queue instance', () => {
      const queue = service.getQueue();

      expect(queue).toBe(mockQueue);
    });
  });

  describe('close', () => {
    it('should close the queue', async () => {
      await service.close();

      expect(mockQueue.close).toHaveBeenCalled();
    });
  });
});
