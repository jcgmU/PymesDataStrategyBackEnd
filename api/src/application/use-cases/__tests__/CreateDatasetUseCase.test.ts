import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { CreateDatasetUseCase, type CreateDatasetInput } from '../CreateDatasetUseCase.js';
import type { DatasetRepository } from '../../../domain/ports/repositories/DatasetRepository.js';
import type { StorageService, UploadResult } from '../../../domain/ports/services/StorageService.js';
import type { JobQueueService, JobResult } from '../../../domain/ports/services/JobQueueService.js';
import { Dataset } from '../../../domain/entities/Dataset.js';

function createMockDatasetRepository(): DatasetRepository & {
  save: Mock;
  findById: Mock;
  findByUserId: Mock;
  findAll: Mock;
  delete: Mock;
  exists: Mock;
} {
  return {
    save: vi.fn(),
    findById: vi.fn(),
    findByUserId: vi.fn(),
    findAll: vi.fn(),
    delete: vi.fn(),
    exists: vi.fn(),
  };
}

function createMockStorageService(): StorageService & {
  uploadToDatasets: Mock;
  upload: Mock;
  download: Mock;
  delete: Mock;
  exists: Mock;
  getMetadata: Mock;
  getSignedDownloadUrl: Mock;
  getSignedUploadUrl: Mock;
  downloadFromDatasets: Mock;
  deleteFromDatasets: Mock;
  existsInDatasets: Mock;
  getDatasetMetadata: Mock;
  getDatasetDownloadUrl: Mock;
  uploadToResults: Mock;
  uploadToTemp: Mock;
  healthCheck: Mock;
} {
  return {
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
}

function createMockJobQueueService(): JobQueueService & {
  enqueue: Mock;
  getStatus: Mock;
  cancel: Mock;
} {
  return {
    enqueue: vi.fn(),
    getStatus: vi.fn(),
    cancel: vi.fn(),
  };
}

function createValidInput(): CreateDatasetInput {
  return {
    name: 'Test Dataset',
    description: 'A test dataset',
    originalFileName: 'data.csv',
    mimeType: 'text/csv',
    fileSizeBytes: 1024,
    fileContent: Buffer.from('col1,col2\n1,2'),
    userId: 'user-123',
    metadata: { source: 'test' },
  };
}

describe('CreateDatasetUseCase', () => {
  let useCase: CreateDatasetUseCase;
  let mockDatasetRepository: ReturnType<typeof createMockDatasetRepository>;
  let mockStorageService: ReturnType<typeof createMockStorageService>;
  let mockJobQueueService: ReturnType<typeof createMockJobQueueService>;

  beforeEach(() => {
    mockDatasetRepository = createMockDatasetRepository();
    mockStorageService = createMockStorageService();
    mockJobQueueService = createMockJobQueueService();

    // Default mock implementations
    mockStorageService.uploadToDatasets.mockResolvedValue({
      key: 'test-key',
      bucket: 'datasets',
      etag: 'abc123',
    } satisfies UploadResult);

    mockJobQueueService.enqueue.mockResolvedValue({
      jobId: 'job-123',
      status: 'queued',
    } satisfies JobResult);

    useCase = new CreateDatasetUseCase(
      mockDatasetRepository,
      mockStorageService,
      mockJobQueueService
    );
  });

  describe('execute', () => {
    it('should create a dataset successfully', async () => {
      const input = createValidInput();

      const result = await useCase.execute(input);

      expect(result.datasetId).toBeDefined();
      expect(result.storageKey).toContain('data.csv');
      expect(result.status).toBe('PROCESSING');
      expect(result.jobId).toBe('job-123');
    });

    it('should upload file to storage', async () => {
      const input = createValidInput();

      await useCase.execute(input);

      expect(mockStorageService.uploadToDatasets).toHaveBeenCalledWith(
        expect.stringContaining('data.csv'),
        input.fileContent,
        expect.objectContaining({
          contentType: 'text/csv',
        })
      );
    });

    it('should save dataset to repository', async () => {
      const input = createValidInput();

      await useCase.execute(input);

      // Should be called twice: once for initial save, once after marking processing
      expect(mockDatasetRepository.save).toHaveBeenCalledTimes(2);
      expect(mockDatasetRepository.save).toHaveBeenCalledWith(expect.any(Dataset));
    });

    it('should enqueue parsing job', async () => {
      const input = createValidInput();

      await useCase.execute(input);

      expect(mockJobQueueService.enqueue).toHaveBeenCalledWith(
        expect.objectContaining({
          datasetId: expect.any(String),
          userId: 'user-123',
          transformationType: 'CLEAN_NULLS',
          parameters: expect.objectContaining({
            action: 'parse',
            detectSchema: true,
          }),
        })
      );
    });

    it('should work without job queue service', async () => {
      // Create use case without job queue
      const useCaseWithoutQueue = new CreateDatasetUseCase(
        mockDatasetRepository,
        mockStorageService
      );

      const input = createValidInput();
      const result = await useCaseWithoutQueue.execute(input);

      expect(result.status).toBe('PENDING');
      expect(result.jobId).toBeUndefined();
      expect(mockDatasetRepository.save).toHaveBeenCalledTimes(1);
    });

    it('should generate unique storage key', async () => {
      const input = createValidInput();

      const result1 = await useCase.execute(input);
      const result2 = await useCase.execute(input);

      expect(result1.storageKey).not.toBe(result2.storageKey);
    });

    it('should sanitize filename in storage key', async () => {
      const input = createValidInput();
      input.originalFileName = 'my file (1).csv';

      const result = await useCase.execute(input);

      expect(result.storageKey).toMatch(/my_file__1_.csv$/);
    });

    it('should handle description being undefined', async () => {
      const input = createValidInput();
      delete input.description;

      const result = await useCase.execute(input);

      expect(result.datasetId).toBeDefined();
    });

    it('should include metadata in storage upload', async () => {
      const input = createValidInput();

      await useCase.execute(input);

      expect(mockStorageService.uploadToDatasets).toHaveBeenCalledWith(
        expect.any(String),
        expect.anything(),
        expect.objectContaining({
          customMetadata: expect.objectContaining({
            userId: 'user-123',
            originalFileName: 'data.csv',
          }),
        })
      );
    });
  });

  describe('error handling', () => {
    it('should propagate storage errors', async () => {
      mockStorageService.uploadToDatasets.mockRejectedValue(new Error('Storage failed'));

      const input = createValidInput();

      await expect(useCase.execute(input)).rejects.toThrow('Storage failed');
    });

    it('should propagate repository errors', async () => {
      mockDatasetRepository.save.mockRejectedValue(new Error('Database error'));

      const input = createValidInput();

      await expect(useCase.execute(input)).rejects.toThrow('Database error');
    });

    it('should propagate job queue errors', async () => {
      mockJobQueueService.enqueue.mockRejectedValue(new Error('Queue error'));

      const input = createValidInput();

      await expect(useCase.execute(input)).rejects.toThrow('Queue error');
    });
  });
});
