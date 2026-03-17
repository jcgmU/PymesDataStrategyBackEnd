import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import type { TransformationType } from '../../domain/entities/TransformationJob.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { JobQueueService, JobResult } from '../../domain/ports/services/JobQueueService.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../domain/errors/ValidationError.js';

/**
 * Input DTO for triggering a dataset transformation.
 */
export interface TransformDatasetInput {
  datasetId: string;
  userId: string;
  transformationType: TransformationType;
  parameters?: Record<string, unknown>;
  priority?: number;
}

/**
 * Output DTO for the transform operation.
 */
export interface TransformDatasetOutput {
  jobId: string;
  datasetId: string;
  status: JobResult['status'];
}

/**
 * Use case for triggering a transformation job on an existing dataset.
 *
 * Flow:
 * 1. Find dataset by ID
 * 2. Validate dataset is in READY state
 * 3. Build a unique job ID
 * 4. Enqueue the transformation job
 * 5. Mark dataset as PROCESSING
 * 6. Return job info
 */
export class TransformDatasetUseCase {
  constructor(
    private readonly datasetRepository: DatasetRepository,
    private readonly jobQueueService: JobQueueService
  ) {}

  async execute(input: TransformDatasetInput): Promise<TransformDatasetOutput> {
    // 1. Find dataset
    const datasetId = DatasetId.fromString(input.datasetId);
    const dataset = await this.datasetRepository.findById(datasetId);

    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    // 2. Validate state — only READY datasets can be transformed
    if (dataset.status !== 'READY') {
      throw new ValidationError(
        `Dataset must be in READY status to trigger a transformation, but is currently ${dataset.status}`,
        'status'
      );
    }

    // 3. Build unique job ID
    const jobId = `transform-${input.datasetId}-${input.transformationType.toLowerCase()}-${String(Date.now())}`;

    // 4. Enqueue job
    const result = await this.jobQueueService.enqueue({
      jobId,
      datasetId: input.datasetId,
      userId: input.userId,
      transformationType: input.transformationType,
      parameters: input.parameters ?? {},
      sourceStorageKey: dataset.storageKey,
      priority: input.priority ?? 5,
    });

    // 5. Mark dataset as processing
    dataset.markProcessing();
    await this.datasetRepository.save(dataset);

    // 6. Return result
    return {
      jobId: result.jobId,
      datasetId: input.datasetId,
      status: result.status,
    };
  }
}
