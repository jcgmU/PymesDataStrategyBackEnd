import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { SaveAiSuggestionsUseCase } from '../../../application/use-cases/SaveAiSuggestionsUseCase.js';

export class WebhookController {
  constructor(private readonly container: Container) {}

  async saveAiSuggestions(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { secret, datasetId, suggestions } = req.body as {
        secret: string;
        datasetId: string;
        suggestions: Array<{ anomalyId: string; suggestion: string }>;
      };

      const webhookSecret = process.env['N8N_WEBHOOK_SECRET'] ?? '';

      const useCase = new SaveAiSuggestionsUseCase(
        this.container.anomalyRepository,
        webhookSecret
      );

      const result = await useCase.execute({ secret, datasetId, suggestions });

      res.status(200).json({ success: true, data: result });
    } catch (error) {
      if (error instanceof Error && error.message === 'Invalid webhook secret') {
        res.status(401).json({ success: false, error: 'Unauthorized' });
        return;
      }
      next(error);
    }
  }
}
