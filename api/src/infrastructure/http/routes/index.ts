import { Router } from 'express';
import type { Container } from '../../config/container.js';
import { createHealthRoutes } from './health.routes.js';
import { createDatasetRoutes } from './dataset.routes.js';
import { createJobRoutes } from './job.routes.js';

/**
 * Create all application routes.
 */
export function createRoutes(container: Container): Router {
  const router = Router();

  // Mount health routes at root
  router.use(createHealthRoutes(container));

  // Dataset routes
  router.use('/api/v1/datasets', createDatasetRoutes(container));

  // Job routes
  router.use('/api/v1/jobs', createJobRoutes(container));

  return router;
}
