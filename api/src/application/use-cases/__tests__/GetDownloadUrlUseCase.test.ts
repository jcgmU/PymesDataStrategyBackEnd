import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GetDownloadUrlUseCase, DEFAULT_DOWNLOAD_URL_EXPIRY_SECONDS } from '../GetDownloadUrlUseCase.js';
import type { DatasetRepository } from '../../../domain/ports/repositories/DatasetRepository.js';
import type { StorageService } from '../../../domain/ports/services/StorageService.js';
import { Dataset } from '../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../../domain/errors/NotFoundError.js';

describe('GetDownloadUrlUseCase', () => {
  let useCase: GetDownloadUrlUseCase;
  let mockDatasetRepository: DatasetRepository;
  let mockStorageService: StorageService;

  const sampleDataset = Dataset.create({
    id: DatasetId.generate(),
    name: 'Test Dataset',
    description: null,
    originalFileName: 'ventas_q1.csv',
    storageKey: 'ds-id/ts_ventas_q1.csv',
    fileSizeBytes: BigInt(2048),
    mimeType: 'text/csv',
    schema: {},
    metadata: {},
    statistics: {},
    userId: 'user-123',
  });

  beforeEach(() => {
    mockDatasetRepository = {
      save: vi.fn(),
      findById: vi.fn(),
      findByUserId: vi.fn(),
      findAll: vi.fn(),
      delete: vi.fn(),
      exists: vi.fn(),
    };

    mockStorageService = {
      upload: vi.fn(),
      download: vi.fn(),
      delete: vi.fn(),
      exists: vi.fn(),
      getMetadata: vi.fn(),
      getSignedDownloadUrl: vi.fn(),
      getSignedUploadUrl: vi.fn(),
      uploadToDatasets: vi.fn(),
      downloadFromDatasets: vi.fn(),
      deleteFromDatasets: vi.fn(),
      existsInDatasets: vi.fn(),
      getDatasetMetadata: vi.fn(),
      getDatasetDownloadUrl: vi.fn(),
      uploadToResults: vi.fn(),
      uploadToTemp: vi.fn(),
      healthCheck: vi.fn(),
    };

    useCase = new GetDownloadUrlUseCase(mockDatasetRepository, mockStorageService);
  });

  it('should return a signed download URL for a valid dataset', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(sampleDataset);
    vi.mocked(mockStorageService.getDatasetDownloadUrl).mockResolvedValue(
      'https://minio.example.com/datasets/signed-url'
    );

    const result = await useCase.execute({ datasetId: sampleDataset.id.value });

    expect(result.downloadUrl).toBe('https://minio.example.com/datasets/signed-url');
    expect(result.datasetId).toBe(sampleDataset.id.value);
    expect(result.fileName).toBe('ventas_q1.csv');
    expect(result.expiresIn).toBe(DEFAULT_DOWNLOAD_URL_EXPIRY_SECONDS);
  });

  it('should use custom expiry when provided', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(sampleDataset);
    vi.mocked(mockStorageService.getDatasetDownloadUrl).mockResolvedValue('https://url');

    const result = await useCase.execute({
      datasetId: sampleDataset.id.value,
      expiresInSeconds: 7200,
    });

    expect(mockStorageService.getDatasetDownloadUrl).toHaveBeenCalledWith(
      sampleDataset.storageKey,
      { expiresInSeconds: 7200 }
    );
    expect(result.expiresIn).toBe(7200);
  });

  it('should throw NotFoundError when dataset does not exist', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(null);

    await expect(
      useCase.execute({ datasetId: 'non-existent-id' })
    ).rejects.toThrow(NotFoundError);
  });

  it('should call getDatasetDownloadUrl with the correct storage key', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(sampleDataset);
    vi.mocked(mockStorageService.getDatasetDownloadUrl).mockResolvedValue('https://url');

    await useCase.execute({ datasetId: sampleDataset.id.value });

    expect(mockStorageService.getDatasetDownloadUrl).toHaveBeenCalledWith(
      sampleDataset.storageKey,
      expect.objectContaining({ expiresInSeconds: DEFAULT_DOWNLOAD_URL_EXPIRY_SECONDS })
    );
  });
});
