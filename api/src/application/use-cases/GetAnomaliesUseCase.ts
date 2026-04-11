import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { AnomalyType, AnomalyStatus, AnomalyDecision } from '../../domain/entities/Anomaly.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';

/**
 * Output DTO for a single anomaly.
 */
export interface AnomalyDto {
  id: string;
  datasetId: string;
  column: string;
  row: number | null;
  type: AnomalyType;
  description: string;
  originalValue: string | null;
  suggestedValue: string | null;
  aiSuggestion: string | null;
  aiActionType: 'FILL' | 'DELETE' | 'KEEP' | null;
  aiActionValue: string | null;
  status: AnomalyStatus;
  createdAt: string;
  updatedAt: string;
  decision: AnomalyDecision | null;
}

/**
 * Input DTO for getting anomalies.
 */
export interface GetAnomaliesInput {
  datasetId: string;
}

/**
 * Output DTO for getting anomalies.
 */
export interface GetAnomaliesOutput {
  anomalies: AnomalyDto[];
}

/**
 * Use case for retrieving all anomalies associated with a dataset.
 *
 * Flow:
 * 1. Verify the dataset exists
 * 2. Fetch anomalies by datasetId
 * 3. Return mapped DTOs
 */
export class GetAnomaliesUseCase {
  constructor(
    private readonly anomalyRepository: AnomalyRepository,
    private readonly datasetRepository: DatasetRepository
  ) {}

  async execute(input: GetAnomaliesInput): Promise<GetAnomaliesOutput> {
    // 1. Verify dataset exists
    const datasetId = DatasetId.fromString(input.datasetId);
    const dataset = await this.datasetRepository.findById(datasetId);

    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    // 2. Fetch anomalies
    const anomalies = await this.anomalyRepository.findByDatasetId(input.datasetId);

    // 3. Return mapped DTOs
    return {
      anomalies: anomalies.map((a) => {
        // Parse structured JSON stored in aiSuggestion to expose typed fields.
        // Falls back gracefully for legacy free-text values.
        let aiActionType: 'FILL' | 'DELETE' | 'KEEP' | null = null;
        let aiActionValue: string | null = null;
        let aiSuggestionText: string | null = a.aiSuggestion;

        if (a.aiSuggestion) {
          try {
            const parsed = JSON.parse(a.aiSuggestion) as Record<string, unknown>;
            aiActionType = (parsed['actionType'] as 'FILL' | 'DELETE' | 'KEEP') ?? null;
            aiActionValue = (parsed['value'] as string | null) ?? null;
            aiSuggestionText = (parsed['reason'] as string) ?? a.aiSuggestion;
          } catch {
            // Legacy free text — leave aiActionType as null
          }
        }

        return {
          id: a.id,
          datasetId: a.datasetId,
          column: a.column,
          row: a.row,
          type: a.type,
          description: a.description,
          originalValue: a.originalValue,
          suggestedValue: a.suggestedValue,
          aiSuggestion: aiSuggestionText,
          aiActionType,
          aiActionValue,
          status: a.status,
          createdAt: a.createdAt.toISOString(),
          updatedAt: a.updatedAt.toISOString(),
          decision: a.decision,
        };
      }),
    };
  }
}
