import { describe, it, expect, vi, beforeEach } from 'vitest';
import { TransformDatasetUseCase } from '../TransformDatasetUseCase.js';
import type { DatasetRepository } from '../../../domain/ports/repositories/DatasetRepository.js';
import type { JobQueueService } from '../../../domain/ports/services/JobQueueService.js';
import { Dataset } from '../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';

describe('TransformDatasetUseCase', () => {
  let useCase: TransformDatasetUseCase;
  let mockDatasetRepository: DatasetRepository;
  let mockJobQueueService: JobQueueService;

  const makeReadyDataset = () => {
    const dataset = Dataset.create({
      id: DatasetId.generate(),
      name: 'Test Dataset',
      description: null,
      originalFileName: 'data.csv',
      storageKey: 'ds-id/123_data.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });
    // Manually set to READY for testing
    dataset.markProcessing();
    dataset.markReady({ rowCount: 100 });
    return dataset;
  };

  beforeEach(() => {
    mockDatasetRepository = {
      save: vi.fn(),
      findById: vi.fn(),
      findByUserId: vi.fn(),
      findAll: vi.fn(),
      delete: vi.fn(),
      exists: vi.fn(),
    };

    mockJobQueueService = {
      enqueue: vi.fn(),
      getStatus: vi.fn(),
      cancel: vi.fn(),
    };

    useCase = new TransformDatasetUseCase(mockDatasetRepository, mockJobQueueService);
  });

  it('should enqueue a transformation job for a READY dataset', async () => {
    const dataset = makeReadyDataset();
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockJobQueueService.enqueue).mockResolvedValue({
      jobId: expect.any(String) as unknown as string,
      status: 'queued',
    });

    const result = await useCase.execute({
      datasetId: dataset.id.value,
      userId: 'user-123',
      transformationType: 'NORMALIZE',
      parameters: { locale: 'es' },
    });

    expect(mockJobQueueService.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        datasetId: dataset.id.value,
        userId: 'user-123',
        transformationType: 'NORMALIZE',
        parameters: { locale: 'es' },
        sourceStorageKey: dataset.storageKey,
      })
    );
    expect(result.status).toBe('queued');
    expect(result.datasetId).toBe(dataset.id.value);
  });

  it('should throw NotFoundError when dataset does not exist', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(null);

    await expect(
      useCase.execute({
        datasetId: 'non-existent-id',
        userId: 'user-123',
        transformationType: 'CLEAN_NULLS',
      })
    ).rejects.toThrow(NotFoundError);
  });

  it('should throw ValidationError when dataset is not in READY status', async () => {
    const dataset = Dataset.create({
      id: DatasetId.generate(),
      name: 'Pending Dataset',
      description: null,
      originalFileName: 'data.csv',
      storageKey: 'key',
      fileSizeBytes: BigInt(100),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });
    // Dataset is in PENDING status by default

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);

    await expect(
      useCase.execute({
        datasetId: dataset.id.value,
        userId: 'user-123',
        transformationType: 'CLEAN_NULLS',
      })
    ).rejects.toThrow(ValidationError);
  });

  it('should mark dataset as PROCESSING after enqueueing', async () => {
    const dataset = makeReadyDataset();
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockJobQueueService.enqueue).mockResolvedValue({
      jobId: 'job-abc',
      status: 'queued',
    });

    await useCase.execute({
      datasetId: dataset.id.value,
      userId: 'user-123',
      transformationType: 'FILTER',
    });

    expect(dataset.status).toBe('PROCESSING');
    expect(mockDatasetRepository.save).toHaveBeenCalledWith(dataset);
  });

  it('should use default empty parameters when not provided', async () => {
    const dataset = makeReadyDataset();
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockJobQueueService.enqueue).mockResolvedValue({
      jobId: 'job-abc',
      status: 'queued',
    });

    await useCase.execute({
      datasetId: dataset.id.value,
      userId: 'user-123',
      transformationType: 'CLEAN_NULLS',
    });

    expect(mockJobQueueService.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({ parameters: {} })
    );
  });
});
