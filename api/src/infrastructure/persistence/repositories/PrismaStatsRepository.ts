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
    const [
      totalDatasets,
      datasetsThisMonth,
      totalJobs,
      jobsCompleted,
      jobsFailed,
      completedJobs,
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

      // Total jobs
      this.prisma.transformationJob.count({
        where: { userId },
      }),

      // Jobs with status COMPLETED
      this.prisma.transformationJob.count({
        where: { userId, status: 'COMPLETED' },
      }),

      // Jobs with status FAILED
      this.prisma.transformationJob.count({
        where: { userId, status: 'FAILED' },
      }),

      // Completed jobs with both createdAt and completedAt to compute duration
      this.prisma.transformationJob.findMany({
        where: {
          userId,
          status: 'COMPLETED',
          completedAt: { not: null },
        },
        select: {
          createdAt: true,
          completedAt: true,
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
    if (completedJobs.length > 0) {
      const totalMs = completedJobs.reduce((sum, job) => {
        const completed = job.completedAt ?? new Date();
        return sum + (completed.getTime() - job.createdAt.getTime());
      }, 0);
      avgProcessingTimeMs = Math.round(totalMs / completedJobs.length);
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
