import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { GetStatsUseCase } from '../../../application/use-cases/GetStatsUseCase.js';

/**
 * Controller for stats-related HTTP endpoints.
 */
export class StatsController {
  constructor(private readonly container: Container) {}

  /**
   * GET /api/v1/stats
   * Returns aggregated statistics for the authenticated user.
   */
  async getStats(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const userId = req.userId ?? '';

      const useCase = new GetStatsUseCase(this.container.statsRepository);
      const stats = await useCase.execute(userId);

      res.status(200).json({
        success: true,
        data: stats,
      });
    } catch (error) {
      next(error);
    }
  }
}
