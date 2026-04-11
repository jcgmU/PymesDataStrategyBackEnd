import type { PrismaClient } from '@prisma/client';
import type { IStatsRepository, Stats } from '../../../domain/ports/repositories/StatsRepository.js';

/**
 * Prisma implementation of IStatsRepository.
 * Aggregates user metrics from the database.
 */
export class PrismaStatsRepository implements IStatsRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async getStats(userId: string): Promise<Stats> {
    const now = new Date();
    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);

    // Run all queries in parallel for performance
    // NOTE: TransformationJob records are never persisted to PostgreSQL (jobs live
    // only in BullMQ/Redis). We derive job metrics from the `datasets` table instead:
    //   totalJobs       → datasets that have been processed (status != PENDING)
    //   jobsCompleted   → datasets in READY status
    //   jobsFailed      → datasets in ERROR status
    //   avgProcessingTimeMs → mean of (processedAt - createdAt) for READY datasets
    const [
      totalDatasets,
      datasetsThisMonth,
      totalJobs,
      jobsCompleted,
      jobsFailed,
      processedDatasets,
      pendingReviews,
    ] = await Promise.all([
      // Total datasets owned by user
      this.prisma.dataset.count({
        where: { userId },
      }),

      // Datasets created in the current calendar month
      this.prisma.dataset.count({
        where: {
          userId,
          createdAt: { gte: startOfMonth },
        },
      }),

      // Datasets that have been submitted for processing (not still PENDING)
      this.prisma.dataset.count({
        where: {
          userId,
          status: { not: 'PENDING' },
        },
      }),

      // Datasets successfully processed
      this.prisma.dataset.count({
        where: { userId, status: 'READY' },
      }),

      // Datasets that failed processing
      this.prisma.dataset.count({
        where: { userId, status: 'ERROR' },
      }),

      // READY datasets with processedAt to compute average processing duration
      this.prisma.dataset.findMany({
        where: {
          userId,
          status: 'READY',
          processedAt: { not: null },
        },
        select: {
          createdAt: true,
          processedAt: true,
        },
      }),

      // Anomalies on user's datasets that are still PENDING
      this.prisma.anomaly.count({
        where: {
          dataset: { userId },
          status: 'PENDING',
        },
      }),
    ]);

    // Calculate average processing time in milliseconds
    let avgProcessingTimeMs = 0;
    if (processedDatasets.length > 0) {
      const totalMs = processedDatasets.reduce((sum, ds) => {
        const completed = ds.processedAt ?? new Date();
        return sum + (completed.getTime() - ds.createdAt.getTime());
      }, 0);
      avgProcessingTimeMs = Math.round(totalMs / processedDatasets.length);
    }

    return {
      totalDatasets,
      datasetsThisMonth,
      totalJobs,
      jobsCompleted,
      jobsFailed,
      avgProcessingTimeMs,
      pendingReviews,
    };
  }
}
