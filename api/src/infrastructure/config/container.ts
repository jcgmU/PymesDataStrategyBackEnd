import { Redis } from 'ioredis';
import { type Env, getEnv } from './env.js';
import { getPrismaClient, disconnectPrisma } from '../persistence/prisma/client.js';
import type { PrismaClient } from '@prisma/client';
import { MinioStorageService } from '../storage/MinioStorageService.js';
import type { StorageService } from '../../domain/ports/services/StorageService.js';
import { PrismaDatasetRepository } from '../persistence/repositories/PrismaDatasetRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import { BullMQJobQueueService } from '../messaging/bullmq/BullMQJobQueueService.js';
import type { JobQueueService } from '../../domain/ports/services/JobQueueService.js';

/**
 * Simple dependency injection container.
 * Manages singleton instances of infrastructure services.
 */
export class Container {
  private readonly env: Env;
  private redisInstance: Redis | null = null;
  private storageInstance: StorageService | null = null;
  private datasetRepositoryInstance: DatasetRepository | null = null;
  private jobQueueInstance: JobQueueService | null = null;

  constructor() {
    this.env = getEnv();
  }

  get config(): Env {
    return this.env;
  }

  get prisma(): PrismaClient {
    return getPrismaClient();
  }

  get redis(): Redis {
    if (this.redisInstance === null) {
      this.redisInstance = new Redis({
        host: this.env.REDIS_HOST,
        port: this.env.REDIS_PORT,
        maxRetriesPerRequest: null, // Required for BullMQ
        enableReadyCheck: false,
      });
    }
    return this.redisInstance;
  }

  get storage(): StorageService {
    if (this.storageInstance === null) {
      this.storageInstance = new MinioStorageService({
        endpoint: this.env.MINIO_ENDPOINT,
        port: this.env.MINIO_PORT,
        accessKey: this.env.MINIO_ACCESS_KEY ?? 'minioadmin',
        secretKey: this.env.MINIO_SECRET_KEY ?? 'minioadmin123',
        useSSL: this.env.MINIO_USE_SSL,
        bucketDatasets: this.env.MINIO_BUCKET_DATASETS,
        bucketResults: this.env.MINIO_BUCKET_RESULTS,
        bucketTemp: this.env.MINIO_BUCKET_TEMP,
      });
    }
    return this.storageInstance;
  }

  get datasetRepository(): DatasetRepository {
    if (this.datasetRepositoryInstance === null) {
      this.datasetRepositoryInstance = new PrismaDatasetRepository(this.prisma);
    }
    return this.datasetRepositoryInstance;
  }

  get jobQueue(): JobQueueService {
    if (this.jobQueueInstance === null) {
      this.jobQueueInstance = new BullMQJobQueueService({
        redis: this.redis,
      });
    }
    return this.jobQueueInstance;
  }

  /**
   * Check if the database connection is healthy.
   */
  async checkDatabase(): Promise<boolean> {
    try {
      await this.prisma.$queryRaw`SELECT 1`;
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check if Redis connection is healthy.
   */
  async checkRedis(): Promise<boolean> {
    try {
      const result = await this.redis.ping();
      return result === 'PONG';
    } catch {
      return false;
    }
  }

  /**
   * Check if MinIO/S3 storage is healthy.
   */
  async checkStorage(): Promise<boolean> {
    try {
      return await this.storage.healthCheck();
    } catch {
      return false;
    }
  }

  /**
   * Gracefully shutdown all connections.
   */
  async shutdown(): Promise<void> {
    if (this.jobQueueInstance !== null) {
      const bullmqService = this.jobQueueInstance as BullMQJobQueueService;
      await bullmqService.close();
      this.jobQueueInstance = null;
    }
    if (this.redisInstance !== null) {
      await this.redisInstance.quit();
      this.redisInstance = null;
    }
    await disconnectPrisma();
  }
}

// Singleton container instance
let containerInstance: Container | null = null;

/**
 * Get the container singleton.
 */
export function getContainer(): Container {
  if (containerInstance === null) {
    containerInstance = new Container();
  }
  return containerInstance;
}
