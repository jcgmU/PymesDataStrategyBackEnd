/**
 * Domain type for aggregated user statistics.
 */
export interface Stats {
  totalDatasets: number;
  datasetsThisMonth: number;
  totalJobs: number;
  jobsCompleted: number;
  jobsFailed: number;
  avgProcessingTimeMs: number;
  pendingReviews: number;
}

/**
 * Port for Stats aggregation operations.
 * Infrastructure layer must implement this interface.
 */
export interface IStatsRepository {
  getStats(userId: string): Promise<Stats>;
}
