import { Router } from 'express';
import type { Container } from '../../config/container.js';
import { EventsController } from '../controllers/EventsController.js';

/**
 * Create global workspace event routes.
 * All routes are prefixed with /api/v1/events
 */
export function createEventsRoutes(_container: Container): Router {
  const router = Router();
  const controller = new EventsController();

  /**
   * GET /api/v1/events
   * Opens a persistent SSE connection that pushes dataset lifecycle events
   * to the authenticated user in real time.
   *
   * Events emitted:
   *   - `dataset:status_changed` — dataset moved to READY or ERROR
   *   - `timeout`               — stream closed after 10-minute hard limit
   *   - `: keepalive`           — comment frame every 15 s
   */
  router.get('/', (req, res) => {
    controller.subscribe(req, res);
  });

  return router;
}
