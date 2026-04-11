import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';

export interface AiSuggestionItem {
  anomalyId: string;
  suggestion: string;
}

export interface SaveAiSuggestionsInput {
  secret: string;
  datasetId: string;
  suggestions: AiSuggestionItem[];
}

export interface SaveAiSuggestionsOutput {
  saved: number;
}

/**
 * Use case for persisting AI-generated suggestions for anomalies.
 * Called by the n8n webhook after Gemini generates suggestions.
 */
export class SaveAiSuggestionsUseCase {
  constructor(
    private readonly anomalyRepository: AnomalyRepository,
    private readonly webhookSecret: string
  ) {}

  async execute(input: SaveAiSuggestionsInput): Promise<SaveAiSuggestionsOutput> {
    if (input.secret !== this.webhookSecret) {
      throw new Error('Invalid webhook secret');
    }

    let saved = 0;

    for (const item of input.suggestions) {
      if (!item.anomalyId || !item.suggestion) continue;

      const anomaly = await this.anomalyRepository.findById(item.anomalyId);
      if (!anomaly) continue;

      // Normalize suggestion to structured JSON.
      // If n8n sends valid JSON with a known actionType, store it as-is.
      // Otherwise (legacy free text) wrap it as KEEP for backward compatibility.
      const VALID_ACTIONS = ['FILL', 'DELETE', 'KEEP'] as const;
      let normalizedSuggestion: string;
      try {
        const parsed: unknown = JSON.parse(item.suggestion);
        if (
          parsed !== null &&
          typeof parsed === 'object' &&
          'actionType' in parsed &&
          VALID_ACTIONS.includes(
            (parsed as { actionType: string }).actionType as (typeof VALID_ACTIONS)[number]
          )
        ) {
          normalizedSuggestion = item.suggestion;
        } else {
          throw new Error('Invalid structure');
        }
      } catch {
        // Legacy free text — wrap as KEEP so existing data keeps working
        normalizedSuggestion = JSON.stringify({
          actionType: 'KEEP',
          value: null,
          reason: item.suggestion.slice(0, 100),
        });
      }

      anomaly.setAiSuggestion(normalizedSuggestion);
      await this.anomalyRepository.save(anomaly);
      saved++;
    }

    return { saved };
  }
}
