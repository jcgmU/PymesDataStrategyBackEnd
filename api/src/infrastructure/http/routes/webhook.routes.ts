import { Router } from 'express';
import type { Container } from '../../config/container.js';
import { WebhookController } from '../controllers/WebhookController.js';

/**
 * Webhook routes for machine-to-machine integrations (e.g. n8n).
 * These routes are NOT protected by user auth — they use a shared secret instead.
 * All routes are prefixed with /api/v1/webhooks
 */
export function createWebhookRoutes(container: Container): Router {
  const router = Router();
  const controller = new WebhookController(container);

  // POST /api/v1/webhooks/n8n/suggestions
  // Called by n8n after Gemini generates AI suggestions for anomalies
  router.post('/n8n/suggestions', (req, res, next) => {
    controller.saveAiSuggestions(req, res, next).catch(next);
  });

  return router;
}
