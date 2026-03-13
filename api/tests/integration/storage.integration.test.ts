import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Readable } from 'node:stream';
import { createMinioContainer, MinioContainerFixture } from '../fixtures/containers.js';
import { MinioStorageService, StorageNotFoundError } from '../../src/infrastructure/storage/MinioStorageService.js';

describe('MinioStorageService Integration Tests', () => {
  let minioFixture: MinioContainerFixture;
  let storageService: MinioStorageService;

  const TEST_BUCKET = 'test-bucket';
  const DATASETS_BUCKET = 'datasets';
  const RESULTS_BUCKET = 'results';
  const TEMP_BUCKET = 'temp';

  beforeAll(async () => {
    // Start MinIO container
    minioFixture = createMinioContainer();
    await minioFixture.start();

    // Create test buckets
    await minioFixture.createBuckets([TEST_BUCKET, DATASETS_BUCKET, RESULTS_BUCKET, TEMP_BUCKET]);

    // Create storage service
    storageService = new MinioStorageService({
      endpoint: minioFixture.getHost(),
      port: minioFixture.getPort(),
      accessKey: minioFixture.getAccessKey(),
      secretKey: minioFixture.getSecretKey(),
      useSSL: false,
      bucketDatasets: DATASETS_BUCKET,
      bucketResults: RESULTS_BUCKET,
      bucketTemp: TEMP_BUCKET,
    });
  }, 60000); // 60s timeout for container startup

  afterAll(async () => {
    await minioFixture.stop();
  }, 30000);

  describe('upload and download', () => {
    it('should upload and download a small file', async () => {
      const testKey = 'test-files/small-file.txt';
      const testContent = 'Hello, MinIO!';
      const testBuffer = Buffer.from(testContent);

      // Upload
      const uploadResult = await storageService.upload(TEST_BUCKET, testKey, testBuffer, {
        contentType: 'text/plain',
      });

      expect(uploadResult.key).toBe(testKey);
      expect(uploadResult.bucket).toBe(TEST_BUCKET);
      expect(uploadResult.etag).toBeDefined();

      // Download
      const downloadStream = await storageService.download(TEST_BUCKET, testKey);
      const chunks: Buffer[] = [];

      for await (const chunk of downloadStream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }

      const downloadedContent = Buffer.concat(chunks).toString('utf-8');
      expect(downloadedContent).toBe(testContent);
    });

    it('should upload a large file (1MB)', async () => {
      const testKey = 'test-files/large-file.bin';
      const size = 1024 * 1024; // 1MB
      const testBuffer = Buffer.alloc(size, 'A');

      const uploadResult = await storageService.upload(TEST_BUCKET, testKey, testBuffer, {
        contentType: 'application/octet-stream',
      });

      expect(uploadResult.key).toBe(testKey);
      expect(uploadResult.etag).toBeDefined();

      // Verify metadata
      const metadata = await storageService.getMetadata(TEST_BUCKET, testKey);
      expect(metadata.contentLength).toBe(size);
      expect(metadata.contentType).toBe('application/octet-stream');
    });

    it('should upload content created from stream-like source', async () => {
      const testKey = 'test-files/stream-file.txt';
      const testContent = 'Streamed content!';
      // Note: For S3-compatible storage, we need to know content-length upfront
      // So we convert stream to buffer first in production code
      const testBuffer = Buffer.from(testContent);

      const uploadResult = await storageService.upload(TEST_BUCKET, testKey, testBuffer, {
        contentType: 'text/plain',
      });

      expect(uploadResult.key).toBe(testKey);

      // Verify content
      const downloadStream = await storageService.download(TEST_BUCKET, testKey);
      const chunks: Buffer[] = [];

      for await (const chunk of downloadStream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }

      expect(Buffer.concat(chunks).toString('utf-8')).toBe(testContent);
    });

    it('should preserve custom metadata', async () => {
      const testKey = 'test-files/with-metadata.txt';
      const testBuffer = Buffer.from('Content with metadata');
      const customMetadata = {
        'user-id': '12345',
        source: 'integration-test',
      };

      await storageService.upload(TEST_BUCKET, testKey, testBuffer, {
        contentType: 'text/plain',
        customMetadata,
      });

      const metadata = await storageService.getMetadata(TEST_BUCKET, testKey);
      expect(metadata.customMetadata).toBeDefined();
      expect(metadata.customMetadata?.['user-id']).toBe('12345');
      expect(metadata.customMetadata?.['source']).toBe('integration-test');
    });
  });

  describe('exists and delete', () => {
    it('should return true for existing object', async () => {
      const testKey = 'test-files/exists-check.txt';
      await storageService.upload(TEST_BUCKET, testKey, Buffer.from('exists'));

      const exists = await storageService.exists(TEST_BUCKET, testKey);
      expect(exists).toBe(true);
    });

    it('should return false for non-existing object', async () => {
      const exists = await storageService.exists(TEST_BUCKET, 'non-existent-key');
      expect(exists).toBe(false);
    });

    it('should delete an object', async () => {
      const testKey = 'test-files/to-delete.txt';
      await storageService.upload(TEST_BUCKET, testKey, Buffer.from('delete me'));

      // Verify exists
      expect(await storageService.exists(TEST_BUCKET, testKey)).toBe(true);

      // Delete
      await storageService.delete(TEST_BUCKET, testKey);

      // Verify deleted
      expect(await storageService.exists(TEST_BUCKET, testKey)).toBe(false);
    });

    it('should not throw when deleting non-existent object', async () => {
      await expect(
        storageService.delete(TEST_BUCKET, 'non-existent-delete-target')
      ).resolves.toBeUndefined();
    });
  });

  describe('getMetadata', () => {
    it('should return object metadata', async () => {
      const testKey = 'test-files/metadata-test.json';
      const testContent = JSON.stringify({ hello: 'world' });

      await storageService.upload(TEST_BUCKET, testKey, Buffer.from(testContent), {
        contentType: 'application/json',
      });

      const metadata = await storageService.getMetadata(TEST_BUCKET, testKey);

      expect(metadata.contentType).toBe('application/json');
      expect(metadata.contentLength).toBe(Buffer.from(testContent).length);
      expect(metadata.lastModified).toBeInstanceOf(Date);
      expect(metadata.etag).toBeDefined();
    });

    it('should throw StorageNotFoundError for non-existent object', async () => {
      await expect(storageService.getMetadata(TEST_BUCKET, 'non-existent-metadata')).rejects.toThrow(
        StorageNotFoundError
      );
    });
  });

  describe('signed URLs', () => {
    it('should generate a valid signed download URL', async () => {
      const testKey = 'test-files/signed-download.txt';
      await storageService.upload(TEST_BUCKET, testKey, Buffer.from('signed content'));

      const signedUrl = await storageService.getSignedDownloadUrl(TEST_BUCKET, testKey, {
        expiresInSeconds: 300,
      });

      expect(signedUrl).toContain(TEST_BUCKET);
      expect(signedUrl).toContain(testKey);
      expect(signedUrl).toContain('X-Amz-Signature');
    });

    it('should generate a valid signed upload URL', async () => {
      const testKey = 'test-files/signed-upload.txt';

      const signedUrl = await storageService.getSignedUploadUrl(TEST_BUCKET, testKey, {
        expiresInSeconds: 300,
        contentType: 'text/plain',
      });

      expect(signedUrl).toContain(TEST_BUCKET);
      expect(signedUrl).toContain(testKey);
      expect(signedUrl).toContain('X-Amz-Signature');
    });

    it('should allow uploading via signed URL', async () => {
      const testKey = 'test-files/uploaded-via-signed-url.txt';
      const testContent = 'Uploaded via signed URL';

      const signedUrl = await storageService.getSignedUploadUrl(TEST_BUCKET, testKey, {
        contentType: 'text/plain',
      });

      // Use fetch to upload via signed URL
      const response = await fetch(signedUrl, {
        method: 'PUT',
        body: testContent,
        headers: {
          'Content-Type': 'text/plain',
        },
      });

      expect(response.ok).toBe(true);

      // Verify the upload worked
      const downloadStream = await storageService.download(TEST_BUCKET, testKey);
      const chunks: Buffer[] = [];

      for await (const chunk of downloadStream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }

      expect(Buffer.concat(chunks).toString('utf-8')).toBe(testContent);
    });
  });

  describe('convenience methods', () => {
    it('uploadToDatasets should upload to datasets bucket', async () => {
      const testKey = 'dataset-file.csv';
      const result = await storageService.uploadToDatasets(testKey, Buffer.from('col1,col2\n1,2'));

      expect(result.bucket).toBe(DATASETS_BUCKET);
      expect(result.key).toBe(testKey);
      expect(await storageService.exists(DATASETS_BUCKET, testKey)).toBe(true);
    });

    it('uploadToResults should upload to results bucket', async () => {
      const testKey = 'result-file.json';
      const result = await storageService.uploadToResults(testKey, Buffer.from('{}'));

      expect(result.bucket).toBe(RESULTS_BUCKET);
      expect(result.key).toBe(testKey);
      expect(await storageService.exists(RESULTS_BUCKET, testKey)).toBe(true);
    });

    it('uploadToTemp should upload to temp bucket', async () => {
      const testKey = 'temp-file.tmp';
      const result = await storageService.uploadToTemp(testKey, Buffer.from('temporary'));

      expect(result.bucket).toBe(TEMP_BUCKET);
      expect(result.key).toBe(testKey);
      expect(await storageService.exists(TEMP_BUCKET, testKey)).toBe(true);
    });
  });

  describe('healthCheck', () => {
    it('should return true when MinIO is accessible', async () => {
      const result = await storageService.healthCheck();
      expect(result).toBe(true);
    });

    it('should return false when MinIO is not accessible', async () => {
      // Create a service with wrong credentials
      const badService = new MinioStorageService({
        endpoint: 'non-existent-host',
        port: 9999,
        accessKey: 'wrong',
        secretKey: 'wrong',
        useSSL: false,
        bucketDatasets: 'datasets',
        bucketResults: 'results',
        bucketTemp: 'temp',
      });

      const result = await badService.healthCheck();
      expect(result).toBe(false);
    });
  });

  describe('error handling', () => {
    it('should throw StorageNotFoundError when downloading non-existent object', async () => {
      await expect(storageService.download(TEST_BUCKET, 'non-existent-file')).rejects.toThrow(
        StorageNotFoundError
      );
    });
  });
});
