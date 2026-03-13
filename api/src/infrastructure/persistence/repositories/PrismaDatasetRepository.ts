import type { PrismaClient, Dataset as PrismaDataset, Prisma } from '@prisma/client';
import type { DatasetRepository, FindAllOptions } from '../../../domain/ports/repositories/DatasetRepository.js';
import { Dataset, type DatasetStatus } from '../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';

/**
 * Prisma implementation of DatasetRepository.
 */
export class PrismaDatasetRepository implements DatasetRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async save(dataset: Dataset): Promise<void> {
    const data = this.toPrismaData(dataset);

    await this.prisma.dataset.upsert({
      where: { id: dataset.id.value },
      create: data,
      update: data,
    });
  }

  async findById(id: DatasetId): Promise<Dataset | null> {
    const record = await this.prisma.dataset.findUnique({
      where: { id: id.value },
    });

    if (!record) {
      return null;
    }

    return this.toDomain(record);
  }

  async findByUserId(userId: string): Promise<Dataset[]> {
    const records = await this.prisma.dataset.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
    });

    return records.map((record) => this.toDomain(record));
  }

  async findAll(options: FindAllOptions): Promise<Dataset[]> {
    const records = await this.prisma.dataset.findMany({
      ...(options.userId !== undefined ? { where: { userId: options.userId } } : {}),
      orderBy: { createdAt: 'desc' },
      skip: options.offset,
      take: options.limit,
    });

    return records.map((record) => this.toDomain(record));
  }

  async delete(id: DatasetId): Promise<void> {
    await this.prisma.dataset.delete({
      where: { id: id.value },
    });
  }

  async exists(id: DatasetId): Promise<boolean> {
    const count = await this.prisma.dataset.count({
      where: { id: id.value },
    });
    return count > 0;
  }

  // ─────────────────────────────────────────────────────────────
  // Mapping helpers
  // ─────────────────────────────────────────────────────────────

  private toPrismaData(dataset: Dataset): Prisma.DatasetCreateInput {
    return {
      id: dataset.id.value,
      name: dataset.name,
      description: dataset.description,
      status: dataset.status,
      originalFileName: dataset.originalFileName,
      storageKey: dataset.storageKey,
      fileSizeBytes: dataset.fileSizeBytes,
      mimeType: dataset.mimeType,
      schema: dataset.schema as Prisma.InputJsonValue,
      metadata: dataset.metadata as Prisma.InputJsonValue,
      statistics: dataset.statistics as Prisma.InputJsonValue,
      user: { connect: { id: dataset.userId } },
      createdAt: dataset.createdAt,
      updatedAt: dataset.updatedAt,
      processedAt: dataset.processedAt,
    };
  }

  private toDomain(record: PrismaDataset): Dataset {
    return Dataset.reconstitute({
      id: DatasetId.fromString(record.id),
      name: record.name,
      description: record.description,
      status: record.status as DatasetStatus,
      originalFileName: record.originalFileName,
      storageKey: record.storageKey,
      fileSizeBytes: record.fileSizeBytes,
      mimeType: record.mimeType,
      schema: record.schema as Record<string, unknown>,
      metadata: record.metadata as Record<string, unknown>,
      statistics: record.statistics as Record<string, unknown>,
      userId: record.userId,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      processedAt: record.processedAt,
    });
  }
}
