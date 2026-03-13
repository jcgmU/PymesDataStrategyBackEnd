import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { GetJobStatusUseCase } from '../../../application/use-cases/GetJobStatusUseCase.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';

/**
 * Controller for job-related HTTP endpoints.
 */
export class JobController {
  constructor(private readonly container: Container) {}

  /**
   * GET /api/v1/jobs/:jobId
   * Get the status of a transformation job.
   */
  async getStatus(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const jobId = req.params['jobId'] as string;

      if (!jobId) {
        throw new ValidationError('Job ID is required', 'jobId');
      }

      const useCase = new GetJobStatusUseCase(this.container.jobQueue);
      const result = await useCase.execute({ jobId });

      if (!result) {
        res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `Job with ID ${jobId} not found`,
          },
        });
        return;
      }

      res.json({
        success: true,
        data: {
          jobId: result.jobId,
          status: result.status,
        },
      });
    } catch (error) {
      next(error);
    }
  }
}
