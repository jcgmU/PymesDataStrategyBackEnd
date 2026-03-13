import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Request, Response, NextFunction } from 'express';
import { DatasetController } from '../DatasetController.js';
import type { Container } from '../../../config/container.js';
import { Dataset } from '../../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../../domain/value-objects/DatasetId.js';
import { ValidationError } from '../../../../domain/errors/ValidationError.js';
import { NotFoundError } from '../../../../domain/errors/NotFoundError.js';

describe('DatasetController', () => {
  let controller: DatasetController;
  let mockContainer: Container;
  let mockReq: Partial<Request>;
  let mockRes: Partial<Response>;
  let mockNext: NextFunction;

  const mockDatasetRepository = {
    save: vi.fn(),
    findById: vi.fn(),
    findByUserId: vi.fn(),
    findAll: vi.fn(),
    delete: vi.fn(),
    exists: vi.fn(),
  };

  const mockJobQueue = {
    enqueue: vi.fn(),
    getStatus: vi.fn(),
    cancel: vi.fn(),
  };

  const mockStorage = {
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

  beforeEach(() => {
    vi.clearAllMocks();

    mockContainer = {
      datasetRepository: mockDatasetRepository,
      storage: mockStorage,
      jobQueue: mockJobQueue,
    } as unknown as Container;

    controller = new DatasetController(mockContainer);

    mockRes = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
    };

    mockNext = vi.fn();
  });

  describe('create', () => {
    it('should return 201 on successful dataset creation', async () => {
      mockReq = {
        file: {
          originalname: 'test.csv',
          mimetype: 'text/csv',
          size: 1024,
          buffer: Buffer.from('col1,col2\n1,2'),
        } as Express.Multer.File,
        body: {
          name: 'Test Dataset',
          description: 'A test dataset',
        },
        headers: {
          'x-user-id': 'user-123',
        },
      };

      mockStorage.uploadToDatasets.mockResolvedValue(undefined);
      mockDatasetRepository.save.mockResolvedValue(undefined);
      mockJobQueue.enqueue.mockResolvedValue({ jobId: 'job-test-123', status: 'queued' });

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(201);
      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({
            id: expect.any(String),
            storageKey: expect.any(String),
          }),
        })
      );
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('should throw ValidationError when no file is provided', async () => {
      mockReq = {
        body: { name: 'Test' },
        headers: {},
      };

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
      const error = ((mockNext as ReturnType<typeof vi.fn>).mock.calls[0] as [ValidationError])[0];
      expect(error.message).toBe('File is required');
      expect(error.field).toBe('file');
    });

    it('should throw ValidationError for unsupported MIME type', async () => {
      mockReq = {
        file: {
          originalname: 'test.exe',
          mimetype: 'application/x-msdownload',
          size: 1024,
          buffer: Buffer.from('binary'),
        } as Express.Multer.File,
        body: { name: 'Test' },
        headers: {},
      };

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
      const error = ((mockNext as ReturnType<typeof vi.fn>).mock.calls[0] as [ValidationError])[0];
      expect(error.message).toContain('Unsupported file type');
    });

    it('should throw ValidationError when name is missing', async () => {
      mockReq = {
        file: {
          originalname: 'test.csv',
          mimetype: 'text/csv',
          size: 1024,
          buffer: Buffer.from('data'),
        } as Express.Multer.File,
        body: {},
        headers: {},
      };

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });

    it('should use anonymous as userId when x-user-id header is not provided', async () => {
      mockReq = {
        file: {
          originalname: 'test.csv',
          mimetype: 'text/csv',
          size: 1024,
          buffer: Buffer.from('data'),
        } as Express.Multer.File,
        body: { name: 'Test Dataset' },
        headers: {},
      };

      mockStorage.uploadToDatasets.mockResolvedValue(undefined);
      mockDatasetRepository.save.mockResolvedValue(undefined);
      mockJobQueue.enqueue.mockResolvedValue({ jobId: 'job-anon', status: 'queued' });

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockStorage.uploadToDatasets).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Buffer),
        expect.objectContaining({
          customMetadata: expect.objectContaining({
            userId: 'anonymous',
          }),
        })
      );
    });

    it('should parse metadata from JSON string', async () => {
      mockReq = {
        file: {
          originalname: 'test.csv',
          mimetype: 'text/csv',
          size: 1024,
          buffer: Buffer.from('data'),
        } as Express.Multer.File,
        body: {
          name: 'Test Dataset',
          metadata: JSON.stringify({ source: 'excel', version: 1 }),
        },
        headers: { 'x-user-id': 'user-123' },
      };

      mockStorage.uploadToDatasets.mockResolvedValue(undefined);
      mockDatasetRepository.save.mockResolvedValue(undefined);
      mockJobQueue.enqueue.mockResolvedValue({ jobId: 'job-meta', status: 'queued' });

      await controller.create(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(201);
    });
  });

  describe('getById', () => {
    const sampleDataset = Dataset.create({
      id: DatasetId.generate(),
      name: 'Test Dataset',
      description: 'A test',
      originalFileName: 'test.csv',
      storageKey: 'abc/123_test.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });

    it('should return dataset when found', async () => {
      mockReq = {
        params: { id: sampleDataset.id.value },
      };

      mockDatasetRepository.findById.mockResolvedValue(sampleDataset);

      await controller.getById(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({
            id: sampleDataset.id.value,
            name: 'Test Dataset',
          }),
        })
      );
    });

    it('should return 404 when dataset not found', async () => {
      mockReq = {
        params: { id: 'non-existent-id' },
      };

      mockDatasetRepository.findById.mockResolvedValue(null);

      await controller.getById(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(404);
      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: false,
          error: expect.objectContaining({
            code: 'NOT_FOUND',
          }),
        })
      );
    });

    it('should throw ValidationError when id is missing', async () => {
      mockReq = {
        params: {},
      };

      await controller.getById(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });
  });

  describe('list', () => {
    it('should return list of datasets', async () => {
      const datasets = [
        Dataset.create({
          id: DatasetId.generate(),
          name: 'Dataset 1',
          description: null,
          originalFileName: 'file1.csv',
          storageKey: 'key1',
          fileSizeBytes: BigInt(100),
          mimeType: 'text/csv',
          schema: {},
          metadata: {},
          statistics: {},
          userId: 'user-1',
        }),
        Dataset.create({
          id: DatasetId.generate(),
          name: 'Dataset 2',
          description: 'Second dataset',
          originalFileName: 'file2.csv',
          storageKey: 'key2',
          fileSizeBytes: BigInt(200),
          mimeType: 'text/csv',
          schema: {},
          metadata: {},
          statistics: {},
          userId: 'user-1',
        }),
      ];

      mockReq = {
        query: { limit: '10', offset: '0' },
      };

      mockDatasetRepository.findAll.mockResolvedValue(datasets);

      await controller.list(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.arrayContaining([
            expect.objectContaining({ name: 'Dataset 1' }),
            expect.objectContaining({ name: 'Dataset 2' }),
          ]),
          pagination: { limit: 10, offset: 0 },
        })
      );
    });

    it('should cap limit at 100', async () => {
      mockReq = {
        query: { limit: '500', offset: '0' },
      };

      mockDatasetRepository.findAll.mockResolvedValue([]);

      await controller.list(mockReq as Request, mockRes as Response, mockNext);

      expect(mockDatasetRepository.findAll).toHaveBeenCalledWith(
        expect.objectContaining({ limit: 100 })
      );
    });

    it('should use default values for limit and offset', async () => {
      mockReq = {
        query: {},
      };

      mockDatasetRepository.findAll.mockResolvedValue([]);

      await controller.list(mockReq as Request, mockRes as Response, mockNext);

      expect(mockDatasetRepository.findAll).toHaveBeenCalledWith(
        expect.objectContaining({ limit: 20, offset: 0 })
      );
    });

    it('should filter by userId when provided', async () => {
      mockReq = {
        query: { userId: 'user-123' },
      };

      mockDatasetRepository.findAll.mockResolvedValue([]);

      await controller.list(mockReq as Request, mockRes as Response, mockNext);

      expect(mockDatasetRepository.findAll).toHaveBeenCalledWith(
        expect.objectContaining({ userId: 'user-123' })
      );
    });
  });

  describe('delete', () => {
    const sampleDataset = Dataset.create({
      id: DatasetId.generate(),
      name: 'Test Dataset',
      description: null,
      originalFileName: 'test.csv',
      storageKey: 'abc/123_test.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });

    it('should delete dataset and return 204', async () => {
      mockReq = {
        params: { id: sampleDataset.id.value },
      };

      mockDatasetRepository.findById.mockResolvedValue(sampleDataset);
      mockStorage.deleteFromDatasets.mockResolvedValue(undefined);
      mockDatasetRepository.delete.mockResolvedValue(undefined);

      await controller.delete(mockReq as Request, mockRes as Response, mockNext);

      expect(mockStorage.deleteFromDatasets).toHaveBeenCalledWith('abc/123_test.csv');
      expect(mockDatasetRepository.delete).toHaveBeenCalledWith(
        expect.any(DatasetId)
      );
      expect(mockRes.status).toHaveBeenCalledWith(204);
      expect(mockRes.send).toHaveBeenCalled();
    });

    it('should return 404 when dataset not found', async () => {
      mockReq = {
        params: { id: 'non-existent-id' },
      };

      mockDatasetRepository.findById.mockResolvedValue(null);

      await controller.delete(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(404);
      expect(mockDatasetRepository.delete).not.toHaveBeenCalled();
    });

    it('should continue deletion even if storage delete fails', async () => {
      mockReq = {
        params: { id: sampleDataset.id.value },
      };

      mockDatasetRepository.findById.mockResolvedValue(sampleDataset);
      mockStorage.deleteFromDatasets.mockRejectedValue(new Error('Storage error'));
      mockDatasetRepository.delete.mockResolvedValue(undefined);

      await controller.delete(mockReq as Request, mockRes as Response, mockNext);

      // Should still delete from database
      expect(mockDatasetRepository.delete).toHaveBeenCalled();
      expect(mockRes.status).toHaveBeenCalledWith(204);
    });

    it('should throw ValidationError when id is missing', async () => {
      mockReq = {
        params: {},
      };

      await controller.delete(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });
  });

  describe('transform', () => {
    const readyDataset = Dataset.reconstitute({
      id: DatasetId.generate(),
      name: 'Ready Dataset',
      description: null,
      status: 'READY',
      originalFileName: 'data.csv',
      storageKey: 'ds/ts_data.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
      createdAt: new Date(),
      updatedAt: new Date(),
      processedAt: new Date(),
    });

    it('should enqueue a transformation and return 201', async () => {
      mockReq = {
        params: { id: readyDataset.id.value },
        body: { transformationType: 'NORMALIZE' },
        headers: { 'x-user-id': 'user-123' },
      };

      mockDatasetRepository.findById.mockResolvedValue(readyDataset);
      mockJobQueue.enqueue.mockResolvedValue({ jobId: 'job-norm-123', status: 'queued' });
      mockDatasetRepository.save.mockResolvedValue(undefined);

      await controller.transform(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(201);
      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({
            jobId: 'job-norm-123',
            status: 'queued',
          }),
        })
      );
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('should return 404 when dataset not found', async () => {
      mockReq = {
        params: { id: 'non-existent-id' },
        body: { transformationType: 'CLEAN_NULLS' },
        headers: {},
      };

      mockDatasetRepository.findById.mockResolvedValue(null);

      await controller.transform(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundError));
    });

    it('should throw ValidationError when id is missing', async () => {
      mockReq = {
        params: {},
        body: { transformationType: 'CLEAN_NULLS' },
        headers: {},
      };

      await controller.transform(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });

    it('should throw ValidationError for invalid transformationType', async () => {
      mockReq = {
        params: { id: readyDataset.id.value },
        body: { transformationType: 'INVALID_TYPE' },
        headers: {},
      };

      await controller.transform(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });
  });

  describe('download', () => {
    const sampleDataset = Dataset.create({
      id: DatasetId.generate(),
      name: 'Test Dataset',
      description: null,
      originalFileName: 'report.csv',
      storageKey: 'ds/ts_report.csv',
      fileSizeBytes: BigInt(512),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });

    it('should return a signed download URL', async () => {
      mockReq = {
        params: { id: sampleDataset.id.value },
        query: {},
      };

      mockDatasetRepository.findById.mockResolvedValue(sampleDataset);
      mockStorage.getDatasetDownloadUrl.mockResolvedValue('https://minio/signed-url');

      await controller.download(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({
            downloadUrl: 'https://minio/signed-url',
            fileName: 'report.csv',
            expiresIn: 3600,
          }),
        })
      );
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('should return 404 when dataset not found', async () => {
      mockReq = {
        params: { id: 'non-existent-id' },
        query: {},
      };

      mockDatasetRepository.findById.mockResolvedValue(null);

      await controller.download(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundError));
    });

    it('should throw ValidationError when id is missing', async () => {
      mockReq = {
        params: {},
        query: {},
      };

      await controller.download(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });

    it('should pass custom expiresIn to use case', async () => {
      mockReq = {
        params: { id: sampleDataset.id.value },
        query: { expiresIn: '7200' },
      };

      mockDatasetRepository.findById.mockResolvedValue(sampleDataset);
      mockStorage.getDatasetDownloadUrl.mockResolvedValue('https://minio/custom-url');

      await controller.download(mockReq as Request, mockRes as Response, mockNext);

      expect(mockStorage.getDatasetDownloadUrl).toHaveBeenCalledWith(
        sampleDataset.storageKey,
        expect.objectContaining({ expiresInSeconds: 7200 })
      );
    });
  });
});
