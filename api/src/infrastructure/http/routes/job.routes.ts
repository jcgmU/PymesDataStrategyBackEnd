import { Router } from 'express';
import type { Container } from '../../config/container.js';
import { JobController } from '../controllers/JobController.js';

/**
 * Create job routes.
 * All routes are prefixed with /api/v1/jobs
 */
export function createJobRoutes(container: Container): Router {
  const router = Router();
  const controller = new JobController(container);

  // GET /api/v1/jobs/:jobId - Get job status
  router.get('/:jobId', (req, res, next) => {
    controller.getStatus(req, res, next).catch(next);
  });

  return router;
}
