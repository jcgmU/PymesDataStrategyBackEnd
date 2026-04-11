import type { Readable } from 'node:stream';

/**
 * Metadata for stored objects.
 */
export interface ObjectMetadata {
  contentType?: string;
  contentLength?: number;
  lastModified?: Date;
  etag?: string;
  customMetadata?: Record<string, string>;
}

/**
 * Options for upload operations.
 */
export interface UploadOptions {
  contentType?: string;
  customMetadata?: Record<string, string>;
}

/**
 * Result of a successful upload operation.
 */
export interface UploadResult {
  key: string;
  bucket: string;
  etag?: string;
  versionId?: string;
}

/**
 * Options for signed URL generation.
 */
export interface SignedUrlOptions {
  /** Expiration time in seconds. Default: 3600 (1 hour) */
  expiresInSeconds?: number;
  /** Content type for upload URLs */
  contentType?: string;
}

/**
 * Port for object storage operations.
 * Infrastructure layer (MinIO/S3) must implement this interface.
 */
export interface StorageService {
  // ─────────────────────────────────────────────────────────────
  // Core operations
  // ─────────────────────────────────────────────────────────────

  /**
   * Upload an object to storage.
   */
  upload(
    bucket: string,
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult>;

  /**
   * Download an object from storage.
   * @throws StorageNotFoundError if the object doesn't exist.
   */
  download(bucket: string, key: string): Promise<Readable>;

  /**
   * Delete an object from storage.
   * Does not throw if the object doesn't exist.
   */
  delete(bucket: string, key: string): Promise<void>;

  /**
   * Check if an object exists in storage.
   */
  exists(bucket: string, key: string): Promise<boolean>;

  /**
   * Get metadata for an object.
   * @throws StorageNotFoundError if the object doesn't exist.
   */
  getMetadata(bucket: string, key: string): Promise<ObjectMetadata>;

  // ─────────────────────────────────────────────────────────────
  // Signed URLs (for direct browser access)
  // ─────────────────────────────────────────────────────────────

  /**
   * Generate a signed URL for downloading an object.
   * Allows direct browser download without exposing credentials.
   */
  getSignedDownloadUrl(
    bucket: string,
    key: string,
    options?: SignedUrlOptions
  ): Promise<string>;

  /**
   * Generate a signed URL for uploading an object.
   * Allows direct browser upload without exposing credentials.
   */
  getSignedUploadUrl(
    bucket: string,
    key: string,
    options?: SignedUrlOptions
  ): Promise<string>;

  // ─────────────────────────────────────────────────────────────
  // Convenience methods for configured buckets
  // ─────────────────────────────────────────────────────────────

  /**
   * Upload to the datasets bucket.
   */
  uploadToDatasets(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult>;

  /**
   * Download from the datasets bucket.
   */
  downloadFromDatasets(key: string): Promise<Readable>;

  /**
   * Delete from the datasets bucket.
   */
  deleteFromDatasets(key: string): Promise<void>;

  /**
   * Check if an object exists in the datasets bucket.
   */
  existsInDatasets(key: string): Promise<boolean>;

  /**
   * Get metadata for an object in the datasets bucket.
   */
  getDatasetMetadata(key: string): Promise<ObjectMetadata>;

  /**
   * Generate a signed download URL for an object in the datasets bucket.
   */
  getDatasetDownloadUrl(key: string, options?: SignedUrlOptions): Promise<string>;

  /**
   * Generate a signed download URL for an object in the results bucket.
   */
  getResultDownloadUrl(key: string, options?: SignedUrlOptions): Promise<string>;

  /**
   * Upload to the results bucket.
   */
  uploadToResults(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult>;

  /**
   * Upload to the temp bucket.
   */
  uploadToTemp(
    key: string,
    data: Buffer | Readable,
    options?: UploadOptions
  ): Promise<UploadResult>;

  // ─────────────────────────────────────────────────────────────
  // Health check
  // ─────────────────────────────────────────────────────────────

  /**
   * Check if the storage service is healthy and accessible.
   */
  healthCheck(): Promise<boolean>;
}
