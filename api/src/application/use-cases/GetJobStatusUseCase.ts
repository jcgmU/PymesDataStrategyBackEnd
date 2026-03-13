import type { JobQueueService, JobResult } from '../../domain/ports/services/JobQueueService.js';

/**
 * Input DTO for querying a job's status.
 */
export interface GetJobStatusInput {
  jobId: string;
}

/**
 * Output DTO for job status query.
 */
export interface GetJobStatusOutput {
  jobId: string;
  status: JobResult['status'];
}

/**
 * Use case for retrieving the current status of a transformation job.
 *
 * Returns null if the job is not found.
 */
export class GetJobStatusUseCase {
  constructor(private readonly jobQueueService: JobQueueService) {}

  async execute(input: GetJobStatusInput): Promise<GetJobStatusOutput | null> {
    const result = await this.jobQueueService.getStatus(input.jobId);

    if (!result) {
      return null;
    }

    return {
      jobId: result.jobId,
      status: result.status,
    };
  }
}
