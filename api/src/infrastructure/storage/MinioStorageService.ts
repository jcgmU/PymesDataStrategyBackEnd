import { Readable } from 'node:stream';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  HeadBucketCommand,
  type PutObjectCommandInput,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import type {
  StorageService,
  ObjectMetadata,
  UploadOptions,
  UploadResult,
  SignedUrlOptions,
} from '../../domain/ports/services/StorageService.js';

/**
 * Configuration for MinioStorageService.
 */
export interface MinioStorageConfig {
  endpoint: string;
  port: number;
  accessKey: string;
  secretKey: string;
  useSSL: boolean;
  bucketDatasets: string;
  bucketResults: string;
  bucketTemp: string;
}

/**
 * Error thrown when an object is not found in storage.
 */
export class StorageNotFoundError extends Error {
  constructor(
    public readonly bucket: string,
    public readonly key: string
  ) {
    super(`Object not found: ${bucket}/${key}`);
    this.name = 'StorageNotFoundError';
  }
}

/**
 * Error thrown when a storage operation fails.
 */
export class StorageError extends Error {
  public readonly errorCause: Error | undefined;

  constructor(message: string, cause?: Error) {
    super(message);
    this.name = 'StorageError';
    this.errorCause = cause;
  }
}

/**
 * MinIO/S3 implementation of StorageService.
 * Uses AWS SDK v3 which is compatible with any S3-compatible storage.
 */
export class MinioStorageService implements StorageService {
  private readonly client: S3Client;
  private readonly bucketDatasets: string;
  private readonly bucketResults: string;
  private readonly bucketTemp: string;

  constructor(config: MinioStorageConfig) {
    const protocol = config.useSSL ? 'https' : 'http';
    const endpoint = `${protocol}://${config.endpoint}:${config.port}`;

    this.client = new S3Client({
      endpoint,
      region: 'us-east-1', // MinIO ignores this but SDK requires it
      credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
      },
      forcePathStyle: true, // Required for MinIO
    });

    this.bucketDatasets = config.bucketDatasets;
    this.bucketResults = config.bucketResults;
    this.bucketTemp = config.bucketTemp;
  }

  // ─────────────────────────────────────────────────────────────
  // Core operations
  // ─────────────────────────────────────────────────────────────

  async upload(
    bucket: string,
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult> {
    try {
      const input: PutObjectCommandInput = {
        Bucket: bucket,
        Key: key,
        Body: data,
        ContentType: options?.contentType,
        Metadata: options?.customMetadata,
      };

      const response = await this.client.send(new PutObjectCommand(input));

      const result: UploadResult = {
        key,
        bucket,
      };

      if (response.ETag) {
        result.etag = response.ETag.replace(/"/g, '');
      }
      if (response.VersionId) {
        result.versionId = response.VersionId;
      }

      return result;
    } catch (error) {
      throw new StorageError(
        `Failed to upload object: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  async download(bucket: string, key: string): Promise<Readable> {
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );

      if (!response.Body) {
        throw new StorageNotFoundError(bucket, key);
      }

      // AWS SDK v3 returns a web stream, convert to Node.js Readable
      return response.Body as Readable;
    } catch (error) {
      if (error instanceof StorageNotFoundError) {
        throw error;
      }
      if (this.isNotFoundError(error)) {
        throw new StorageNotFoundError(bucket, key);
      }
      throw new StorageError(
        `Failed to download object: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  async delete(bucket: string, key: string): Promise<void> {
    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );
    } catch (error) {
      // S3 delete is idempotent - doesn't throw if object doesn't exist
      // But we still want to catch actual errors
      if (!this.isNotFoundError(error)) {
        throw new StorageError(
          `Failed to delete object: ${bucket}/${key}`,
          error instanceof Error ? error : undefined
        );
      }
    }
  }

  async exists(bucket: string, key: string): Promise<boolean> {
    try {
      await this.client.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );
      return true;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        return false;
      }
      throw new StorageError(
        `Failed to check object existence: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  async getMetadata(bucket: string, key: string): Promise<ObjectMetadata> {
    try {
      const response = await this.client.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );

      const metadata: ObjectMetadata = {};

      if (response.ContentType) {
        metadata.contentType = response.ContentType;
      }
      if (response.ContentLength !== undefined) {
        metadata.contentLength = response.ContentLength;
      }
      if (response.LastModified) {
        metadata.lastModified = response.LastModified;
      }
      if (response.ETag) {
        metadata.etag = response.ETag.replace(/"/g, '');
      }
      if (response.Metadata) {
        metadata.customMetadata = response.Metadata;
      }

      return metadata;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        throw new StorageNotFoundError(bucket, key);
      }
      throw new StorageError(
        `Failed to get object metadata: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Signed URLs
  // ─────────────────────────────────────────────────────────────

  async getSignedDownloadUrl(
    bucket: string,
    key: string,
    options?: SignedUrlOptions
  ): Promise<string> {
    try {
      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      });

      return await getSignedUrl(this.client, command, {
        expiresIn: options?.expiresInSeconds ?? 3600,
      });
    } catch (error) {
      throw new StorageError(
        `Failed to generate signed download URL: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  async getSignedUploadUrl(
    bucket: string,
    key: string,
    options?: SignedUrlOptions
  ): Promise<string> {
    try {
      const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        ContentType: options?.contentType,
      });

      return await getSignedUrl(this.client, command, {
        expiresIn: options?.expiresInSeconds ?? 3600,
      });
    } catch (error) {
      throw new StorageError(
        `Failed to generate signed upload URL: ${bucket}/${key}`,
        error instanceof Error ? error : undefined
      );
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Convenience methods
  // ─────────────────────────────────────────────────────────────

  async uploadToDatasets(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult> {
    return this.upload(this.bucketDatasets, key, data, options);
  }

  async downloadFromDatasets(key: string): Promise<Readable> {
    return this.download(this.bucketDatasets, key);
  }

  async deleteFromDatasets(key: string): Promise<void> {
    return this.delete(this.bucketDatasets, key);
  }

  async existsInDatasets(key: string): Promise<boolean> {
    return this.exists(this.bucketDatasets, key);
  }

  async getDatasetMetadata(key: string): Promise<ObjectMetadata> {
    return this.getMetadata(this.bucketDatasets, key);
  }

  async getDatasetDownloadUrl(key: string, options?: SignedUrlOptions): Promise<string> {
    return this.getSignedDownloadUrl(this.bucketDatasets, key, options);
  }

  async uploadToResults(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult> {
    return this.upload(this.bucketResults, key, data, options);
  }

  async uploadToTemp(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult> {
    return this.upload(this.bucketTemp, key, data, options);
  }

  // ─────────────────────────────────────────────────────────────
  // Health check
  // ─────────────────────────────────────────────────────────────

  async healthCheck(): Promise<boolean> {
    try {
      // Check if the datasets bucket is accessible
      await this.client.send(
        new HeadBucketCommand({
          Bucket: this.bucketDatasets,
        })
      );
      return true;
    } catch {
      return false;
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Private helpers
  // ─────────────────────────────────────────────────────────────

  private isNotFoundError(error: unknown): boolean {
    if (error && typeof error === 'object' && 'name' in error) {
      const name = (error as { name: string }).name;
      return name === 'NotFound' || name === 'NoSuchKey' || name === '404';
    }
    return false;
  }
}
