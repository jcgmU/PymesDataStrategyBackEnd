import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { PrismaStatsRepository } from '../PrismaStatsRepository.js';
import type { PrismaClient } from '@prisma/client';

function createMockPrisma() {
  return {
    dataset: {
      count: vi.fn(),
    },
    transformationJob: {
      count: vi.fn(),
      findMany: vi.fn(),
    },
    anomaly: {
      count: vi.fn(),
    },
  } as unknown as PrismaClient & {
    dataset: { count: Mock };
    transformationJob: { count: Mock; findMany: Mock };
    anomaly: { count: Mock };
  };
}

describe('PrismaStatsRepository', () => {
  let repository: PrismaStatsRepository;
  let mockPrisma: ReturnType<typeof createMockPrisma>;

  beforeEach(() => {
    mockPrisma = createMockPrisma();
    repository = new PrismaStatsRepository(mockPrisma);
  });

  it('should return correct stats when user has data', async () => {
    const now = new Date();
    const createdAt = new Date(now.getTime() - 5000); // 5 seconds ago
    const completedAt = new Date(now.getTime() - 2000); // 2 seconds ago

    mockPrisma.dataset.count
      .mockResolvedValueOnce(10) // totalDatasets
      .mockResolvedValueOnce(3);  // datasetsThisMonth

    mockPrisma.transformationJob.count
      .mockResolvedValueOnce(8)  // totalJobs
      .mockResolvedValueOnce(7)  // jobsCompleted
      .mockResolvedValueOnce(1); // jobsFailed

    mockPrisma.transformationJob.findMany.mockResolvedValueOnce([
      { createdAt, completedAt },
      { createdAt, completedAt },
    ]);

    mockPrisma.anomaly.count.mockResolvedValueOnce(2); // pendingReviews

    const result = await repository.getStats('user-123');

    expect(result.totalDatasets).toBe(10);
    expect(result.datasetsThisMonth).toBe(3);
    expect(result.jobsCompleted).toBe(7);
    expect(result.jobsFailed).toBe(1);
    expect(result.avgProcessingTimeMs).toBe(3000); // 5000 - 2000
    expect(result.pendingReviews).toBe(2);
  });

  it('should return 0 avgProcessingTimeMs when no completed jobs', async () => {
    mockPrisma.dataset.count
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(0);

    mockPrisma.transformationJob.count
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(0);

    mockPrisma.transformationJob.findMany.mockResolvedValueOnce([]); // no completed jobs

    mockPrisma.anomaly.count.mockResolvedValueOnce(0);

    const result = await repository.getStats('user-new');

    expect(result.avgProcessingTimeMs).toBe(0);
    expect(result.totalDatasets).toBe(0);
  });

  it('should query dataset count with userId filter', async () => {
    mockPrisma.dataset.count.mockResolvedValue(5);
    mockPrisma.transformationJob.count.mockResolvedValue(0);
    mockPrisma.transformationJob.findMany.mockResolvedValueOnce([]);
    mockPrisma.anomaly.count.mockResolvedValueOnce(0);

    await repository.getStats('user-xyz');

    // First call is totalDatasets
    expect(mockPrisma.dataset.count).toHaveBeenCalledWith(
      expect.objectContaining({ where: { userId: 'user-xyz' } })
    );
  });

  it('should query anomalies by dataset owner userId', async () => {
    mockPrisma.dataset.count.mockResolvedValue(0);
    mockPrisma.transformationJob.count.mockResolvedValue(0);
    mockPrisma.transformationJob.findMany.mockResolvedValueOnce([]);
    mockPrisma.anomaly.count.mockResolvedValueOnce(4);

    const result = await repository.getStats('user-123');

    expect(mockPrisma.anomaly.count).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          dataset: { userId: 'user-123' },
          status: 'PENDING',
        },
      })
    );
    expect(result.pendingReviews).toBe(4);
  });

  it('should correctly round avgProcessingTimeMs', async () => {
    const now = new Date();
    // 3 jobs: 1000ms, 2000ms, 3000ms → avg = 2000ms
    const jobs = [
      { createdAt: new Date(now.getTime() - 1000), completedAt: now },
      { createdAt: new Date(now.getTime() - 2000), completedAt: now },
      { createdAt: new Date(now.getTime() - 3000), completedAt: now },
    ];

    mockPrisma.dataset.count.mockResolvedValue(0);
    mockPrisma.transformationJob.count.mockResolvedValue(3);
    mockPrisma.transformationJob.findMany.mockResolvedValueOnce(jobs);
    mockPrisma.anomaly.count.mockResolvedValueOnce(0);

    const result = await repository.getStats('user-123');

    expect(result.avgProcessingTimeMs).toBe(2000);
  });
});
