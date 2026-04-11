import { Router } from 'express';
import type { Container } from '../../config/container.js';
import { createHealthRoutes } from './health.routes.js';
import { createDatasetRoutes } from './dataset.routes.js';
import { createJobRoutes } from './job.routes.js';
import { createAuthRoutes } from './auth.routes.js';
import { createStatsRoutes } from './stats.routes.js';
import { createWebhookRoutes } from './webhook.routes.js';
import { createEventsRoutes } from './events.routes.js';
import { createAuthMiddleware } from '../middleware/AuthMiddleware.js';

/**
 * Create all application routes.
 */
export function createRoutes(container: Container): Router {
  const router = Router();
  const requireAuth = createAuthMiddleware(container.jwtService);

  // Mount health routes at root (public)
  router.use(createHealthRoutes(container));

  // Auth and user routes (public — login/register)
  router.use('/api/v1', createAuthRoutes(container));

  // Dataset routes (protected)
  router.use('/api/v1/datasets', requireAuth, createDatasetRoutes(container));

  // Job routes (protected)
  router.use('/api/v1/jobs', requireAuth, createJobRoutes(container));

  // Stats routes (protected)
  router.use('/api/v1/stats', requireAuth, createStatsRoutes(container));

  // Global workspace SSE events (protected)
  router.use('/api/v1/events', requireAuth, createEventsRoutes(container));

  // Webhook routes (public — protected by shared secret)
  router.use('/api/v1/webhooks', createWebhookRoutes(container));

  return router;
}
