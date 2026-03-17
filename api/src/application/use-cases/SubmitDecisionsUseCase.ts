import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { DecisionAction } from '../../domain/entities/Anomaly.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../domain/errors/ValidationError.js';

/**
 * Input DTO for a single decision.
 */
export interface DecisionInput {
  anomalyId: string;
  action: DecisionAction;
  correction?: string;
}

/**
 * Input DTO for submitting decisions.
 */
export interface SubmitDecisionsInput {
  datasetId: string;
  userId: string;
  decisions: DecisionInput[];
}

/**
 * Output DTO for a single resolved anomaly decision.
 */
export interface DecisionResult {
  anomalyId: string;
  action: DecisionAction;
  decisionId: string;
}

/**
 * Output DTO for submitting decisions.
 */
export interface SubmitDecisionsOutput {
  resolved: number;
  results: DecisionResult[];
}

/**
 * Use case for submitting human decisions on anomalies (HITL flow).
 *
 * Flow:
 * 1. Verify the dataset exists
 * 2. For each decision, find the anomaly and validate it belongs to the dataset
 * 3. Apply the decision and mark anomaly as RESOLVED
 * 4. Persist each anomaly
 * 5. Return summary
 */
export class SubmitDecisionsUseCase {
  constructor(
    private readonly anomalyRepository: AnomalyRepository,
    private readonly datasetRepository: DatasetRepository
  ) {}

  async execute(input: SubmitDecisionsInput): Promise<SubmitDecisionsOutput> {
    // 1. Verify dataset exists
    const datasetId = DatasetId.fromString(input.datasetId);
    const dataset = await this.datasetRepository.findById(datasetId);

    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    // 2. Validate inputs
    if (input.decisions.length === 0) {
      throw new ValidationError('At least one decision is required', 'decisions');
    }

    const results: DecisionResult[] = [];

    // 3 & 4. Process each decision
    for (const decisionInput of input.decisions) {
      const anomaly = await this.anomalyRepository.findById(decisionInput.anomalyId);

      if (!anomaly) {
        throw new NotFoundError('Anomaly', decisionInput.anomalyId);
      }

      if (anomaly.datasetId !== input.datasetId) {
        throw new ValidationError(
          `Anomaly ${decisionInput.anomalyId} does not belong to dataset ${input.datasetId}`,
          'anomalyId'
        );
      }

      const decisionId = `dec-${decisionInput.anomalyId}-${String(Date.now())}`;

      anomaly.resolve({
        id: decisionId,
        anomalyId: decisionInput.anomalyId,
        action: decisionInput.action,
        correction: decisionInput.correction ?? null,
        userId: input.userId,
        createdAt: new Date(),
      });

      await this.anomalyRepository.save(anomaly);

      results.push({
        anomalyId: decisionInput.anomalyId,
        action: decisionInput.action,
        decisionId,
      });
    }

    // 5. Return summary
    return {
      resolved: results.length,
      results,
    };
  }
}
