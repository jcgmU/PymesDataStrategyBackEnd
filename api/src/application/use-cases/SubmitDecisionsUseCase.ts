import { randomUUID } from 'node:crypto';
import path from 'node:path';

function outputFormatFromFilename(filename: string): string {
  const ext = path.extname(filename).toLowerCase().replace('.', '');
  const map: Record<string, string> = { xlsx: 'xlsx', xls: 'xlsx', csv: 'csv', json: 'json', parquet: 'parquet', txt: 'csv' };
  return map[ext] ?? 'parquet';
}
import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { JobQueueService } from '../../domain/ports/services/JobQueueService.js';
import type { DecisionAction } from '../../domain/entities/Anomaly.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../domain/errors/ValidationError.js';
import { IRValidator } from '../../domain/ir/IRValidator.js';
import type { ColumnInfo } from '../../domain/ir/IRValidator.js';

/**
 * Input DTO for a single decision.
 */
export interface DecisionInput {
  anomalyId: string;
  action: DecisionAction;
  correction?: string;
  /** Validated IR tree (re-validated server-side; never trust the client) */
  correctionIr?: unknown;
  /** "rule" | "gemini" */
  irSource?: string;
  /** Original natural-language text typed by the user */
  irRawText?: string;
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
 * Extract ColumnInfo list from a dataset schema object.
 * Supports two common shapes produced by the worker:
 *   1. { columns: [{name, dtype}] }
 *   2. { col1: 'int64', col2: 'object', ... }
 */
function extractColumnsFromSchema(schema: Record<string, unknown>): ColumnInfo[] {
  if (Array.isArray(schema['columns'])) {
    return (schema['columns'] as unknown[])
      .filter(
        (c): c is { name: string; dtype: string } =>
          typeof c === 'object' &&
          c !== null &&
          typeof (c as Record<string, unknown>)['name'] === 'string' &&
          typeof (c as Record<string, unknown>)['dtype'] === 'string'
      )
      .map((c) => ({ name: c.name, dtype: c.dtype }));
  }
  const result: ColumnInfo[] = [];
  for (const [key, value] of Object.entries(schema)) {
    if (typeof value === 'string') {
      result.push({ name: key, dtype: value });
    }
  }
  return result;
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
    private readonly datasetRepository: DatasetRepository,
    private readonly jobQueueService: JobQueueService
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

    // Extract dataset columns for IR validation (empty = skip column existence check)
    const datasetColumns: ColumnInfo[] = extractColumnsFromSchema(dataset.schema);

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

      // B7: server-side IR validation — never trust the client
      let validatedIr: Record<string, unknown> | null = null;
      if (decisionInput.correctionIr !== undefined && decisionInput.correctionIr !== null) {
        const validation = IRValidator.validate(decisionInput.correctionIr, datasetColumns);
        if (!validation.valid) {
          throw new ValidationError(
            `Invalid correctionIr for anomaly ${decisionInput.anomalyId}: ${validation.error}`,
            'correctionIr'
          );
        }
        validatedIr = validation.node as unknown as Record<string, unknown>;
      }

      const decisionId = `dec-${decisionInput.anomalyId}-${String(Date.now())}`;

      anomaly.resolve({
        id: decisionId,
        anomalyId: decisionInput.anomalyId,
        action: decisionInput.action,
        correction: decisionInput.correction ?? null,
        correctionIr: validatedIr,
        irSource: decisionInput.irSource ?? null,
        irRawText: decisionInput.irRawText ?? null,
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

    // 5. Enqueue the final processing job
    const jobId = randomUUID();
    
    await this.jobQueueService.enqueue({
      jobId,
      datasetId: input.datasetId,
      userId: input.userId,
      transformationType: 'CLEAN_NULLS', // Or a relevant type indicating applying decisions
      parameters: {},
      sourceStorageKey: dataset.storageKey,
      sourceKey: dataset.storageKey,
      filename: dataset.originalFileName,
      outputFormat: outputFormatFromFilename(dataset.originalFileName),
      priority: 5,
    });

    // 6. Change dataset status to PROCESSING
    dataset.markProcessing();
    await this.datasetRepository.save(dataset);

    // 7. Return summary
    return {
      resolved: results.length,
      results,
    };
  }
}
