import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Request, Response, NextFunction } from 'express';
import { StatsController } from '../StatsController.js';
import type { Container } from '../../../config/container.js';
import type { Stats } from '../../../../domain/ports/repositories/StatsRepository.js';

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

describe('StatsController', () => {
  let controller: StatsController;
  let mockContainer: Container;
  let mockReq: Partial<Request>;
  let mockRes: Partial<Response>;
  let mockNext: NextFunction;

  const mockStatsRepository = {
    getStats: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();

    mockContainer = {
      statsRepository: mockStatsRepository,
    } as unknown as Container;

    controller = new StatsController(mockContainer);

    mockRes = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn().mockReturnThis(),
    };

    mockNext = vi.fn();
  });

  describe('getStats', () => {
    it('should return 200 with stats for the authenticated user', async () => {
      const stats = makeStats();
      mockStatsRepository.getStats.mockResolvedValue(stats);

      mockReq = { userId: 'user-123' };

      await controller.getStats(
        mockReq as Request,
        mockRes as Response,
        mockNext
      );

      expect(mockRes.status).toHaveBeenCalledWith(200);
      expect(mockRes.json).toHaveBeenCalledWith({
        success: true,
        data: stats,
      });
      expect(mockStatsRepository.getStats).toHaveBeenCalledWith('user-123');
    });

    it('should return correct stats structure', async () => {
      const stats = makeStats({
        totalDatasets: 5,
        datasetsThisMonth: 1,
        jobsCompleted: 3,
        jobsFailed: 0,
        avgProcessingTimeMs: 2000,
        pendingReviews: 4,
      });
      mockStatsRepository.getStats.mockResolvedValue(stats);

      mockReq = { userId: 'user-456' };

      await controller.getStats(
        mockReq as Request,
        mockRes as Response,
        mockNext
      );

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({
            totalDatasets: 5,
            jobsCompleted: 3,
            avgProcessingTimeMs: 2000,
          }),
        })
      );
    });

    it('should call next with error when repository throws', async () => {
      const error = new Error('Repository failure');
      mockStatsRepository.getStats.mockRejectedValue(error);

      mockReq = { userId: 'user-123' };

      await controller.getStats(
        mockReq as Request,
        mockRes as Response,
        mockNext
      );

      expect(mockNext).toHaveBeenCalledWith(error);
      expect(mockRes.status).not.toHaveBeenCalled();
    });

    it('should return zero values when user has no activity', async () => {
      const emptyStats = makeStats({
        totalDatasets: 0,
        datasetsThisMonth: 0,
        jobsCompleted: 0,
        jobsFailed: 0,
        avgProcessingTimeMs: 0,
        pendingReviews: 0,
      });
      mockStatsRepository.getStats.mockResolvedValue(emptyStats);

      mockReq = { userId: 'new-user' };

      await controller.getStats(
        mockReq as Request,
        mockRes as Response,
        mockNext
      );

      expect(mockRes.status).toHaveBeenCalledWith(200);
      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            totalDatasets: 0,
            avgProcessingTimeMs: 0,
          }),
        })
      );
    });
  });
});
