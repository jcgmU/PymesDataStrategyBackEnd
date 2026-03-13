import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GetJobStatusUseCase } from '../GetJobStatusUseCase.js';
import type { JobQueueService } from '../../../domain/ports/services/JobQueueService.js';

describe('GetJobStatusUseCase', () => {
  let useCase: GetJobStatusUseCase;
  let mockJobQueueService: JobQueueService;

  beforeEach(() => {
    mockJobQueueService = {
      enqueue: vi.fn(),
      getStatus: vi.fn(),
      cancel: vi.fn(),
    };

    useCase = new GetJobStatusUseCase(mockJobQueueService);
  });

  it('should return job status when job exists', async () => {
    vi.mocked(mockJobQueueService.getStatus).mockResolvedValue({
      jobId: 'job-123',
      status: 'processing',
    });

    const result = await useCase.execute({ jobId: 'job-123' });

    expect(result).toEqual({ jobId: 'job-123', status: 'processing' });
    expect(mockJobQueueService.getStatus).toHaveBeenCalledWith('job-123');
  });

  it('should return null when job does not exist', async () => {
    vi.mocked(mockJobQueueService.getStatus).mockResolvedValue(null);

    const result = await useCase.execute({ jobId: 'non-existent' });

    expect(result).toBeNull();
  });

  it('should return completed status when job is done', async () => {
    vi.mocked(mockJobQueueService.getStatus).mockResolvedValue({
      jobId: 'job-456',
      status: 'completed',
    });

    const result = await useCase.execute({ jobId: 'job-456' });

    expect(result?.status).toBe('completed');
  });

  it('should return failed status when job failed', async () => {
    vi.mocked(mockJobQueueService.getStatus).mockResolvedValue({
      jobId: 'job-789',
      status: 'failed',
    });

    const result = await useCase.execute({ jobId: 'job-789' });

    expect(result?.status).toBe('failed');
  });
});
