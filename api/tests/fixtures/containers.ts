import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  ListBucketsCommand,
} from '@aws-sdk/client-s3';

/**
 * PostgreSQL container fixture for integration tests.
 * Uses testcontainers to spin up a real PostgreSQL instance.
 */
export class PostgresContainerFixture {
  private container: StartedPostgreSqlContainer | null = null;

  async start(): Promise<StartedPostgreSqlContainer> {
    this.container = await new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('test_db')
      .withUsername('test_user')
      .withPassword('test_password')
      .start();

    return this.container;
  }

  async stop(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  getConnectionString(): string {
    if (!this.container) {
      throw new Error('PostgreSQL container not started');
    }
    return this.container.getConnectionUri();
  }

  getContainer(): StartedPostgreSqlContainer | null {
    return this.container;
  }
}

/**
 * Redis container fixture for integration tests.
 * Uses testcontainers to spin up a real Redis instance.
 */
export class RedisContainerFixture {
  private container: StartedRedisContainer | null = null;

  async start(): Promise<StartedRedisContainer> {
    this.container = await new RedisContainer('redis:7-alpine').start();

    return this.container;
  }

  async stop(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  getHost(): string {
    if (!this.container) {
      throw new Error('Redis container not started');
    }
    return this.container.getHost();
  }

  getPort(): number {
    if (!this.container) {
      throw new Error('Redis container not started');
    }
    return this.container.getPort();
  }

  getConnectionUrl(): string {
    if (!this.container) {
      throw new Error('Redis container not started');
    }
    return this.container.getConnectionUrl();
  }

  getContainer(): StartedRedisContainer | null {
    return this.container;
  }
}

// Convenience factory functions
export function createPostgresContainer(): PostgresContainerFixture {
  return new PostgresContainerFixture();
}

export function createRedisContainer(): RedisContainerFixture {
  return new RedisContainerFixture();
}

/**
 * MinIO container fixture for integration tests.
 * Uses testcontainers to spin up a real MinIO instance.
 */
export class MinioContainerFixture {
  private container: StartedTestContainer | null = null;
  private s3Client: S3Client | null = null;

  static readonly DEFAULT_ACCESS_KEY = 'minioadmin';
  static readonly DEFAULT_SECRET_KEY = 'minioadmin123';
  static readonly S3_PORT = 9000;

  async start(): Promise<StartedTestContainer> {
    this.container = await new GenericContainer('minio/minio:RELEASE.2024-02-17T01-15-57Z')
      .withExposedPorts(MinioContainerFixture.S3_PORT)
      .withEnvironment({
        MINIO_ROOT_USER: MinioContainerFixture.DEFAULT_ACCESS_KEY,
        MINIO_ROOT_PASSWORD: MinioContainerFixture.DEFAULT_SECRET_KEY,
      })
      .withCommand(['server', '/data'])
      .withWaitStrategy(Wait.forHttp('/minio/health/ready', MinioContainerFixture.S3_PORT))
      .start();

    // Initialize S3 client
    this.s3Client = new S3Client({
      endpoint: this.getEndpoint(),
      region: 'us-east-1',
      credentials: {
        accessKeyId: MinioContainerFixture.DEFAULT_ACCESS_KEY,
        secretAccessKey: MinioContainerFixture.DEFAULT_SECRET_KEY,
      },
      forcePathStyle: true,
    });

    return this.container;
  }

  async stop(): Promise<void> {
    this.s3Client = null;
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  getHost(): string {
    if (!this.container) {
      throw new Error('MinIO container not started');
    }
    return this.container.getHost();
  }

  getPort(): number {
    if (!this.container) {
      throw new Error('MinIO container not started');
    }
    return this.container.getMappedPort(MinioContainerFixture.S3_PORT);
  }

  getEndpoint(): string {
    return `http://${this.getHost()}:${this.getPort()}`;
  }

  getAccessKey(): string {
    return MinioContainerFixture.DEFAULT_ACCESS_KEY;
  }

  getSecretKey(): string {
    return MinioContainerFixture.DEFAULT_SECRET_KEY;
  }

  getS3Client(): S3Client {
    if (!this.s3Client) {
      throw new Error('MinIO container not started');
    }
    return this.s3Client;
  }

  /**
   * Create a bucket in the MinIO container.
   */
  async createBucket(bucketName: string): Promise<void> {
    const client = this.getS3Client();

    try {
      // Check if bucket exists
      await client.send(new HeadBucketCommand({ Bucket: bucketName }));
    } catch {
      // Create if it doesn't exist
      await client.send(new CreateBucketCommand({ Bucket: bucketName }));
    }
  }

  /**
   * Create multiple buckets.
   */
  async createBuckets(bucketNames: string[]): Promise<void> {
    await Promise.all(bucketNames.map((name) => this.createBucket(name)));
  }

  /**
   * List all buckets.
   */
  async listBuckets(): Promise<string[]> {
    const client = this.getS3Client();
    const response = await client.send(new ListBucketsCommand({}));
    return response.Buckets?.map((b) => b.Name ?? '') ?? [];
  }

  getContainer(): StartedTestContainer | null {
    return this.container;
  }
}

export function createMinioContainer(): MinioContainerFixture {
  return new MinioContainerFixture();
}
