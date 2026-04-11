import type { Readable } from 'node:stream';
import { Dataset } from '../../domain/entities/Dataset.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { StorageService } from '../../domain/ports/services/StorageService.js';
import type { JobQueueService, JobPayload } from '../../domain/ports/services/JobQueueService.js';

import { randomUUID } from 'node:crypto';
import path from 'node:path';

/** Derive output format from a filename extension. Defaults to 'parquet'. */
function outputFormatFromFilename(filename: string): string {
  const ext = path.extname(filename).toLowerCase().replace('.', '');
  const map: Record<string, string> = {
    xlsx: 'xlsx',
    xls: 'xlsx',
    csv: 'csv',
    json: 'json',
    parquet: 'parquet',
    txt: 'csv',
  };
  return map[ext] ?? 'parquet';
}

/**
 * Input DTO for creating a dataset.
 */
export interface CreateDatasetInput {
  name: string;
  description?: string;
  originalFileName: string;
  mimeType: string;
  fileSizeBytes: number;
  fileContent: Buffer | Readable;
  userId: string;
  metadata?: Record<string, unknown>;
}

/**
 * Output DTO for create dataset operation.
 */
export interface CreateDatasetOutput {
  datasetId: string;
  storageKey: string;
  status: string;
  jobId?: string;
}

/**
 * Use case for creating a new dataset.
 * 
 * Flow:
 * 1. Generate dataset ID
 * 2. Upload file to storage (MinIO)
 * 3. Create dataset entity
 * 4. Save to database
 * 5. Enqueue parsing job (optional - if jobQueue is provided)
 * 6. Return dataset info
 */
export class CreateDatasetUseCase {
  constructor(
    private readonly datasetRepository: DatasetRepository,
    private readonly storageService: StorageService,
    private readonly jobQueueService?: JobQueueService
  ) {}

  async execute(input: CreateDatasetInput): Promise<CreateDatasetOutput> {
    // 1. Generate dataset ID
    const datasetId = DatasetId.generate();
    
    // 2. Generate storage key and upload file
    const storageKey = this.generateStorageKey(datasetId.value, input.originalFileName);
    
    await this.storageService.uploadToDatasets(storageKey, input.fileContent, {
      contentType: input.mimeType,
      customMetadata: {
        datasetId: datasetId.value,
        originalFileName: input.originalFileName,
        userId: input.userId,
      },
    });

    // 3. Create dataset entity
    const dataset = Dataset.create({
      id: datasetId,
      name: input.name,
      description: input.description ?? null,
      originalFileName: input.originalFileName,
      storageKey,
      fileSizeBytes: BigInt(input.fileSizeBytes),
      mimeType: input.mimeType,
      schema: {},
      metadata: input.metadata ?? {},
      statistics: {},
      userId: input.userId,
    });

    // 4. Save to database
    await this.datasetRepository.save(dataset);

    // 5. Enqueue parsing job (if job queue is available)
    let jobId: string | undefined;
    if (this.jobQueueService) {
      const jobPayload: JobPayload = {
        jobId: randomUUID(),
        datasetId: datasetId.value,
        userId: input.userId,
        transformationType: 'CLEAN_NULLS', // Initial parsing job
        parameters: {
          action: 'parse',
          detectSchema: true,
        },
        sourceStorageKey: storageKey,
        sourceKey: storageKey,
        filename: input.originalFileName,
        outputFormat: outputFormatFromFilename(input.originalFileName),
        priority: 10, // High priority for initial parsing
      };

      const result = await this.jobQueueService.enqueue(jobPayload);
      jobId = result.jobId;

      // Mark dataset as processing
      dataset.markProcessing();
      await this.datasetRepository.save(dataset);
    }

    // 6. Return result
    return {
      datasetId: datasetId.value,
      storageKey,
      status: dataset.status,
      ...(jobId !== undefined ? { jobId } : {}),
    };
  }

  /**
   * Generate a unique storage key for the dataset file.
   * Format: {datasetId}/{timestamp}_{originalFileName}
   */
  private generateStorageKey(datasetId: string, originalFileName: string): string {
    const timestamp = Date.now();
    const sanitizedFileName = originalFileName.replace(/[^a-zA-Z0-9._-]/g, '_');
    return `${datasetId}/${String(timestamp)}_${sanitizedFileName}`;
  }
}
