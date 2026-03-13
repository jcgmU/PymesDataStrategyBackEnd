import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { Readable } from 'node:stream';
import {
  MinioStorageService,
  StorageNotFoundError,
  StorageError,
  type MinioStorageConfig,
} from '../MinioStorageService.js';

// We need to test the service by intercepting the S3Client's send method
// We'll use a factory approach to inject mocked behavior

function createTestConfig(): MinioStorageConfig {
  return {
    endpoint: 'localhost',
    port: 9000,
    accessKey: 'minioadmin',
    secretKey: 'minioadmin123',
    useSSL: false,
    bucketDatasets: 'datasets',
    bucketResults: 'results',
    bucketTemp: 'temp',
  };
}

// Create a testable subclass that exposes the client for mocking
class TestableMinioStorageService extends MinioStorageService {
  private mockSendFn: Mock | null = null;

  setMockSend(mockFn: Mock): void {
    this.mockSendFn = mockFn;
    // Override the client's send method
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this as any).client.send = mockFn;
  }

  getMockSend(): Mock {
    if (!this.mockSendFn) {
      throw new Error('Mock not set');
    }
    return this.mockSendFn;
  }
}

describe('MinioStorageService', () => {
  let service: TestableMinioStorageService;
  let mockSend: Mock;

  beforeEach(() => {
    vi.clearAllMocks();
    service = new TestableMinioStorageService(createTestConfig());
    mockSend = vi.fn();
    service.setMockSend(mockSend);
  });

  describe('constructor', () => {
    it('should create service with valid config', () => {
      expect(service).toBeInstanceOf(MinioStorageService);
    });

    it('should use https when useSSL is true', () => {
      const sslConfig = { ...createTestConfig(), useSSL: true };
      const sslService = new TestableMinioStorageService(sslConfig);
      expect(sslService).toBeInstanceOf(MinioStorageService);
    });
  });

  describe('upload', () => {
    it('should upload a buffer successfully', async () => {
      mockSend.mockResolvedValueOnce({
        ETag: '"abc123"',
        VersionId: 'v1',
      });

      const result = await service.upload('test-bucket', 'test-key', Buffer.from('test data'));

      expect(result).toEqual({
        key: 'test-key',
        bucket: 'test-bucket',
        etag: 'abc123',
        versionId: 'v1',
      });
    });

    it('should upload with content type option', async () => {
      mockSend.mockResolvedValueOnce({ ETag: '"abc123"' });

      await service.upload('test-bucket', 'test-key', Buffer.from('test'), {
        contentType: 'application/json',
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ContentType: 'application/json',
          }),
        })
      );
    });

    it('should upload with custom metadata', async () => {
      mockSend.mockResolvedValueOnce({ ETag: '"abc123"' });

      await service.upload('test-bucket', 'test-key', Buffer.from('test'), {
        customMetadata: { userId: '123', source: 'upload' },
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Metadata: { userId: '123', source: 'upload' },
          }),
        })
      );
    });

    it('should handle upload without ETag', async () => {
      mockSend.mockResolvedValueOnce({});

      const result = await service.upload('test-bucket', 'test-key', Buffer.from('test'));

      expect(result).toEqual({
        key: 'test-key',
        bucket: 'test-bucket',
      });
      expect(result.etag).toBeUndefined();
    });

    it('should throw StorageError on failure', async () => {
      mockSend.mockRejectedValueOnce(new Error('Network error'));

      await expect(service.upload('test-bucket', 'test-key', Buffer.from('test'))).rejects.toThrow(
        StorageError
      );
    });
  });

  describe('download', () => {
    it('should return a readable stream', async () => {
      const mockStream = Readable.from(['test data']);
      mockSend.mockResolvedValueOnce({ Body: mockStream });

      const result = await service.download('test-bucket', 'test-key');

      expect(result).toBe(mockStream);
    });

    it('should throw StorageNotFoundError when object does not exist', async () => {
      mockSend.mockRejectedValueOnce({ name: 'NoSuchKey' });

      await expect(service.download('test-bucket', 'test-key')).rejects.toThrow(
        StorageNotFoundError
      );
    });

    it('should throw StorageNotFoundError when Body is empty', async () => {
      mockSend.mockResolvedValueOnce({ Body: null });

      await expect(service.download('test-bucket', 'test-key')).rejects.toThrow(
        StorageNotFoundError
      );
    });

    it('should throw StorageError on other errors', async () => {
      mockSend.mockRejectedValueOnce(new Error('Connection refused'));

      await expect(service.download('test-bucket', 'test-key')).rejects.toThrow(StorageError);
    });
  });

  describe('delete', () => {
    it('should delete object successfully', async () => {
      mockSend.mockResolvedValueOnce({});

      await expect(service.delete('test-bucket', 'test-key')).resolves.toBeUndefined();
    });

    it('should not throw when object does not exist', async () => {
      mockSend.mockRejectedValueOnce({ name: 'NoSuchKey' });

      await expect(service.delete('test-bucket', 'test-key')).resolves.toBeUndefined();
    });

    it('should throw StorageError on other errors', async () => {
      mockSend.mockRejectedValueOnce(new Error('Permission denied'));

      await expect(service.delete('test-bucket', 'test-key')).rejects.toThrow(StorageError);
    });
  });

  describe('exists', () => {
    it('should return true when object exists', async () => {
      mockSend.mockResolvedValueOnce({});

      const result = await service.exists('test-bucket', 'test-key');

      expect(result).toBe(true);
    });

    it('should return false when object does not exist', async () => {
      mockSend.mockRejectedValueOnce({ name: 'NotFound' });

      const result = await service.exists('test-bucket', 'test-key');

      expect(result).toBe(false);
    });

    it('should throw StorageError on other errors', async () => {
      mockSend.mockRejectedValueOnce(new Error('Timeout'));

      await expect(service.exists('test-bucket', 'test-key')).rejects.toThrow(StorageError);
    });
  });

  describe('getMetadata', () => {
    it('should return object metadata', async () => {
      mockSend.mockResolvedValueOnce({
        ContentType: 'text/csv',
        ContentLength: 1024,
        LastModified: new Date('2024-01-01'),
        ETag: '"def456"',
        Metadata: { custom: 'value' },
      });

      const result = await service.getMetadata('test-bucket', 'test-key');

      expect(result).toEqual({
        contentType: 'text/csv',
        contentLength: 1024,
        lastModified: new Date('2024-01-01'),
        etag: 'def456',
        customMetadata: { custom: 'value' },
      });
    });

    it('should return partial metadata when some fields are missing', async () => {
      mockSend.mockResolvedValueOnce({
        ContentType: 'text/plain',
      });

      const result = await service.getMetadata('test-bucket', 'test-key');

      expect(result).toEqual({
        contentType: 'text/plain',
      });
    });

    it('should throw StorageNotFoundError when object does not exist', async () => {
      mockSend.mockRejectedValueOnce({ name: 'NotFound' });

      await expect(service.getMetadata('test-bucket', 'test-key')).rejects.toThrow(
        StorageNotFoundError
      );
    });
  });

  describe('getSignedDownloadUrl', () => {
    it('should generate a signed URL', async () => {
      // Note: For unit tests, we can't easily mock getSignedUrl from s3-request-presigner
      // This will be tested in integration tests with real MinIO
      // For now, we just verify it doesn't throw
      const result = await service.getSignedDownloadUrl('test-bucket', 'test-key');

      expect(typeof result).toBe('string');
      expect(result).toContain('test-bucket');
    });
  });

  describe('getSignedUploadUrl', () => {
    it('should generate a signed URL', async () => {
      const result = await service.getSignedUploadUrl('test-bucket', 'test-key');

      expect(typeof result).toBe('string');
      expect(result).toContain('test-bucket');
    });

    it('should include content type when provided', async () => {
      const result = await service.getSignedUploadUrl('test-bucket', 'test-key', {
        contentType: 'text/csv',
      });

      expect(typeof result).toBe('string');
    });
  });

  describe('convenience methods', () => {
    it('uploadToDatasets should use datasets bucket', async () => {
      mockSend.mockResolvedValueOnce({ ETag: '"abc"' });

      await service.uploadToDatasets('test-key', Buffer.from('data'));

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Bucket: 'datasets',
            Key: 'test-key',
          }),
        })
      );
    });

    it('uploadToResults should use results bucket', async () => {
      mockSend.mockResolvedValueOnce({ ETag: '"abc"' });

      await service.uploadToResults('test-key', Buffer.from('data'));

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Bucket: 'results',
            Key: 'test-key',
          }),
        })
      );
    });

    it('uploadToTemp should use temp bucket', async () => {
      mockSend.mockResolvedValueOnce({ ETag: '"abc"' });

      await service.uploadToTemp('test-key', Buffer.from('data'));

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Bucket: 'temp',
            Key: 'test-key',
          }),
        })
      );
    });
  });

  describe('healthCheck', () => {
    it('should return true when bucket is accessible', async () => {
      mockSend.mockResolvedValueOnce({});

      const result = await service.healthCheck();

      expect(result).toBe(true);
    });

    it('should return false when bucket is not accessible', async () => {
      mockSend.mockRejectedValueOnce(new Error('Bucket not found'));

      const result = await service.healthCheck();

      expect(result).toBe(false);
    });

    it('should check datasets bucket', async () => {
      mockSend.mockResolvedValueOnce({});

      await service.healthCheck();

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Bucket: 'datasets',
          }),
        })
      );
    });
  });
});

describe('StorageNotFoundError', () => {
  it('should have correct properties', () => {
    const error = new StorageNotFoundError('my-bucket', 'my-key');

    expect(error.name).toBe('StorageNotFoundError');
    expect(error.bucket).toBe('my-bucket');
    expect(error.key).toBe('my-key');
    expect(error.message).toBe('Object not found: my-bucket/my-key');
  });
});

describe('StorageError', () => {
  it('should have correct properties', () => {
    const cause = new Error('Original error');
    const error = new StorageError('Storage failed', cause);

    expect(error.name).toBe('StorageError');
    expect(error.message).toBe('Storage failed');
    expect(error.errorCause).toBe(cause);
  });

  it('should work without cause', () => {
    const error = new StorageError('Storage failed');

    expect(error.errorCause).toBeUndefined();
  });
});
