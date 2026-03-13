import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { StorageService } from '../../domain/ports/services/StorageService.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';

/**
 * Default signed URL expiry in seconds (1 hour).
 */
export const DEFAULT_DOWNLOAD_URL_EXPIRY_SECONDS = 3600;

/**
 * Input DTO for generating a download URL.
 */
export interface GetDownloadUrlInput {
  datasetId: string;
  expiresInSeconds?: number;
}

/**
 * Output DTO for a download URL.
 */
export interface GetDownloadUrlOutput {
  datasetId: string;
  downloadUrl: string;
  expiresIn: number;
  fileName: string;
}

/**
 * Use case for generating a signed download URL for a dataset file.
 *
 * Allows the client to download the file directly from object storage
 * without exposing credentials or routing large files through the API.
 */
export class GetDownloadUrlUseCase {
  constructor(
    private readonly datasetRepository: DatasetRepository,
    private readonly storageService: StorageService
  ) {}

  async execute(input: GetDownloadUrlInput): Promise<GetDownloadUrlOutput> {
    // 1. Find dataset
    const datasetId = DatasetId.fromString(input.datasetId);
    const dataset = await this.datasetRepository.findById(datasetId);

    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    const expiresIn = input.expiresInSeconds ?? DEFAULT_DOWNLOAD_URL_EXPIRY_SECONDS;

    // 2. Generate signed download URL
    const downloadUrl = await this.storageService.getDatasetDownloadUrl(
      dataset.storageKey,
      { expiresInSeconds: expiresIn }
    );

    return {
      datasetId: input.datasetId,
      downloadUrl,
      expiresIn,
      fileName: dataset.originalFileName,
    };
  }
}
