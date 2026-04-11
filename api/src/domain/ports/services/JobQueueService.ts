import type { TransformationType } from '../../entities/TransformationJob.js';

/**
 * Payload for enqueuing a transformation job.
 */
export interface JobPayload {
  jobId: string;
  datasetId: string;
  userId: string;
  transformationType: TransformationType;
  parameters: Record<string, unknown>;
  sourceStorageKey: string;
  sourceKey?: string;   // added for python worker compatibility
  filename?: string;    // added for python worker compatibility
  outputFormat?: string; // output format matching the original file (csv, xlsx, json, parquet)
  priority?: number;
}

/**
 * Result of a job operation.
 */
export interface JobResult {
  jobId: string;
  status: 'queued' | 'processing' | 'completed' | 'failed';
}

/**
 * Port for job queue operations.
 * Infrastructure layer (BullMQ) must implement this interface.
 */
export interface JobQueueService {
  enqueue(payload: JobPayload): Promise<JobResult>;
  getStatus(jobId: string): Promise<JobResult | null>;
  cancel(jobId: string): Promise<boolean>;
}
