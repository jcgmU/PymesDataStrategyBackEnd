import { Redis } from 'ioredis';
import { type Env, getEnv } from './env.js';
import { getPrismaClient, disconnectPrisma } from '../persistence/prisma/client.js';
import type { PrismaClient } from '@prisma/client';
import { MinioStorageService } from '../storage/MinioStorageService.js';
import type { StorageService } from '../../domain/ports/services/StorageService.js';
import { PrismaDatasetRepository } from '../persistence/repositories/PrismaDatasetRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import { PrismaAnomalyRepository } from '../persistence/repositories/PrismaAnomalyRepository.js';
import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import { PrismaUserRepository } from '../persistence/repositories/PrismaUserRepository.js';
import type { UserRepository } from '../../domain/ports/repositories/UserRepository.js';
import { PrismaStatsRepository } from '../persistence/repositories/PrismaStatsRepository.js';
import type { IStatsRepository } from '../../domain/ports/repositories/StatsRepository.js';
import { BullMQJobQueueService } from '../messaging/bullmq/BullMQJobQueueService.js';
import type { JobQueueService } from '../../domain/ports/services/JobQueueService.js';
import { BullMQQueueEvents } from '../messaging/bullmq/BullMQQueueEvents.js';
import { JwtServiceAdapter } from '../auth/JwtServiceAdapter.js';
import type { IJwtService } from '../../domain/ports/services/JwtService.js';
import { BcryptPasswordService } from '../auth/BcryptPasswordService.js';
import type { IPasswordService } from '../../domain/ports/services/PasswordService.js';

/**
 * Simple dependency injection container.
 * Manages singleton instances of infrastructure services.
 */
export class Container {
  private readonly env: Env;
  private redisInstance: Redis | null = null;
  private storageInstance: StorageService | null = null;
  private datasetRepositoryInstance: DatasetRepository | null = null;
  private anomalyRepositoryInstance: AnomalyRepository | null = null;
  private userRepositoryInstance: UserRepository | null = null;
  private statsRepositoryInstance: IStatsRepository | null = null;
  private jobQueueInstance: JobQueueService | null = null;
  private queueEventsInstance: BullMQQueueEvents | null = null;
  private jwtServiceInstance: IJwtService | null = null;
  private passwordServiceInstance: IPasswordService | null = null;

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
        keepAlive: 10000,
        retryStrategy(times) {
          return Math.min(times * 50, 2000);
        }
      });
      this.redisInstance.on('error', (err) => {
        console.error('Redis error (main):', err.message);
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
        publicEndpoint: this.env.MINIO_PUBLIC_ENDPOINT,
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

  get anomalyRepository(): AnomalyRepository {
    if (this.anomalyRepositoryInstance === null) {
      this.anomalyRepositoryInstance = new PrismaAnomalyRepository(this.prisma);
    }
    return this.anomalyRepositoryInstance;
  }

  get userRepository(): UserRepository {
    if (this.userRepositoryInstance === null) {
      this.userRepositoryInstance = new PrismaUserRepository(this.prisma);
    }
    return this.userRepositoryInstance;
  }

  get statsRepository(): IStatsRepository {
    if (this.statsRepositoryInstance === null) {
      this.statsRepositoryInstance = new PrismaStatsRepository(this.prisma);
    }
    return this.statsRepositoryInstance;
  }

  get jwtService(): IJwtService {
    if (this.jwtServiceInstance === null) {
      this.jwtServiceInstance = new JwtServiceAdapter(
        this.env.JWT_SECRET,
        this.env.JWT_EXPIRES_IN
      );
    }
    return this.jwtServiceInstance;
  }

  get passwordService(): IPasswordService {
    if (this.passwordServiceInstance === null) {
      this.passwordServiceInstance = new BcryptPasswordService();
    }
    return this.passwordServiceInstance;
  }

  get jobQueue(): JobQueueService {
    if (this.jobQueueInstance === null) {
      this.jobQueueInstance = new BullMQJobQueueService({
        redis: this.redis,
      });
    }
    return this.jobQueueInstance;
  }

  get queueEvents(): BullMQQueueEvents {
    if (this.queueEventsInstance === null) {
      // QueueEvents requires its own dedicated Redis connection
      const redisForEvents = new Redis({
        host: this.env.REDIS_HOST,
        port: this.env.REDIS_PORT,
        maxRetriesPerRequest: null,
        enableReadyCheck: false,
        keepAlive: 10000,
        retryStrategy(times) {
          return Math.min(times * 50, 2000);
        }
      });
      redisForEvents.on('error', (err) => {
        console.error('Redis error (events):', err.message);
      });
      this.queueEventsInstance = new BullMQQueueEvents(redisForEvents);
    }
    return this.queueEventsInstance;
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
      const result: string = await this.redis.ping();
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
    if (this.queueEventsInstance !== null) {
      await this.queueEventsInstance.close();
      this.queueEventsInstance = null;
    }
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
