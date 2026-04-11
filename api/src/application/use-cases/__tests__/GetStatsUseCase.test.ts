import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GetStatsUseCase } from '../GetStatsUseCase.js';
import type { IStatsRepository, Stats } from '../../../domain/ports/repositories/StatsRepository.js';

const makeStats = (overrides?: Partial<Stats>): Stats => ({
  totalDatasets: 10,
  datasetsThisMonth: 3,
  totalJobs: 8,
  jobsCompleted: 7,
  jobsFailed: 1,
  avgProcessingTimeMs: 1500,
  pendingReviews: 2,
  ...overrides,
});

describe('GetStatsUseCase', () => {
  let useCase: GetStatsUseCase;
  let mockStatsRepository: IStatsRepository;

  beforeEach(() => {
    mockStatsRepository = {
      getStats: vi.fn(),
    };

    useCase = new GetStatsUseCase(mockStatsRepository);
  });

  it('should return stats from the repository', async () => {
    const stats = makeStats();
    vi.mocked(mockStatsRepository.getStats).mockResolvedValue(stats);

    const result = await useCase.execute('user-123');

    expect(result).toEqual(stats);
    expect(mockStatsRepository.getStats).toHaveBeenCalledWith('user-123');
    expect(mockStatsRepository.getStats).toHaveBeenCalledTimes(1);
  });

  it('should pass the userId to the repository', async () => {
    const stats = makeStats();
    vi.mocked(mockStatsRepository.getStats).mockResolvedValue(stats);

    await useCase.execute('user-abc-456');

    expect(mockStatsRepository.getStats).toHaveBeenCalledWith('user-abc-456');
  });

  it('should return zero values when no activity exists', async () => {
    const emptyStats = makeStats({
      totalDatasets: 0,
      datasetsThisMonth: 0,
      jobsCompleted: 0,
      jobsFailed: 0,
      avgProcessingTimeMs: 0,
      pendingReviews: 0,
    });
    vi.mocked(mockStatsRepository.getStats).mockResolvedValue(emptyStats);

    const result = await useCase.execute('user-new');

    expect(result.totalDatasets).toBe(0);
    expect(result.avgProcessingTimeMs).toBe(0);
  });

  it('should propagate errors from the repository', async () => {
    vi.mocked(mockStatsRepository.getStats).mockRejectedValue(new Error('DB error'));

    await expect(useCase.execute('user-123')).rejects.toThrow('DB error');
  });
});
