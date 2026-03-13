import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { PrismaDatasetRepository } from '../PrismaDatasetRepository.js';
import { Dataset } from '../../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../../domain/value-objects/DatasetId.js';
import type { PrismaClient, Dataset as PrismaDataset } from '@prisma/client';

// Mock Prisma client
function createMockPrisma() {
  return {
    dataset: {
      upsert: vi.fn(),
      findUnique: vi.fn(),
      findMany: vi.fn(),
      delete: vi.fn(),
      count: vi.fn(),
    },
  } as unknown as PrismaClient & {
    dataset: {
      upsert: Mock;
      findUnique: Mock;
      findMany: Mock;
      delete: Mock;
      count: Mock;
    };
  };
}

function createTestDataset(overrides: Partial<{
  id: DatasetId;
  name: string;
  userId: string;
}> = {}): Dataset {
  return Dataset.create({
    id: overrides.id ?? DatasetId.generate(),
    name: overrides.name ?? 'Test Dataset',
    description: 'Test description',
    originalFileName: 'test.csv',
    storageKey: 'datasets/test.csv',
    fileSizeBytes: BigInt(1024),
    mimeType: 'text/csv',
    schema: { columns: [] },
    metadata: {},
    statistics: {},
    userId: overrides.userId ?? 'user-123',
  });
}

function createPrismaDataset(id: string): PrismaDataset {
  return {
    id,
    name: 'Test Dataset',
    description: 'Test description',
    status: 'PENDING',
    originalFileName: 'test.csv',
    storageKey: 'datasets/test.csv',
    fileSizeBytes: BigInt(1024),
    mimeType: 'text/csv',
    schema: { columns: [] },
    metadata: {},
    statistics: {},
    userId: 'user-123',
    createdAt: new Date(),
    updatedAt: new Date(),
    processedAt: null,
  };
}

describe('PrismaDatasetRepository', () => {
  let repository: PrismaDatasetRepository;
  let mockPrisma: ReturnType<typeof createMockPrisma>;

  beforeEach(() => {
    mockPrisma = createMockPrisma();
    repository = new PrismaDatasetRepository(mockPrisma);
  });

  describe('save', () => {
    it('should save a new dataset', async () => {
      const dataset = createTestDataset();
      mockPrisma.dataset.upsert.mockResolvedValueOnce(createPrismaDataset(dataset.id.value));

      await repository.save(dataset);

      expect(mockPrisma.dataset.upsert).toHaveBeenCalledWith({
        where: { id: dataset.id.value },
        create: expect.objectContaining({
          id: dataset.id.value,
          name: dataset.name,
          status: 'PENDING',
        }),
        update: expect.objectContaining({
          id: dataset.id.value,
          name: dataset.name,
        }),
      });
    });

    it('should update an existing dataset', async () => {
      const dataset = createTestDataset();
      dataset.markProcessing();
      mockPrisma.dataset.upsert.mockResolvedValueOnce(createPrismaDataset(dataset.id.value));

      await repository.save(dataset);

      expect(mockPrisma.dataset.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            status: 'PROCESSING',
          }),
        })
      );
    });
  });

  describe('findById', () => {
    it('should return dataset when found', async () => {
      const id = DatasetId.generate();
      mockPrisma.dataset.findUnique.mockResolvedValueOnce(createPrismaDataset(id.value));

      const result = await repository.findById(id);

      expect(result).toBeInstanceOf(Dataset);
      expect(result?.id.value).toBe(id.value);
    });

    it('should return null when not found', async () => {
      const id = DatasetId.generate();
      mockPrisma.dataset.findUnique.mockResolvedValueOnce(null);

      const result = await repository.findById(id);

      expect(result).toBeNull();
    });
  });

  describe('findByUserId', () => {
    it('should return datasets for user', async () => {
      const userId = 'user-123';
      mockPrisma.dataset.findMany.mockResolvedValueOnce([
        createPrismaDataset('id-1'),
        createPrismaDataset('id-2'),
      ]);

      const result = await repository.findByUserId(userId);

      expect(result).toHaveLength(2);
      expect(mockPrisma.dataset.findMany).toHaveBeenCalledWith({
        where: { userId },
        orderBy: { createdAt: 'desc' },
      });
    });

    it('should return empty array when no datasets', async () => {
      mockPrisma.dataset.findMany.mockResolvedValueOnce([]);

      const result = await repository.findByUserId('user-456');

      expect(result).toEqual([]);
    });
  });

  describe('delete', () => {
    it('should delete dataset by id', async () => {
      const id = DatasetId.generate();
      mockPrisma.dataset.delete.mockResolvedValueOnce(createPrismaDataset(id.value));

      await repository.delete(id);

      expect(mockPrisma.dataset.delete).toHaveBeenCalledWith({
        where: { id: id.value },
      });
    });
  });

  describe('exists', () => {
    it('should return true when dataset exists', async () => {
      const id = DatasetId.generate();
      mockPrisma.dataset.count.mockResolvedValueOnce(1);

      const result = await repository.exists(id);

      expect(result).toBe(true);
    });

    it('should return false when dataset does not exist', async () => {
      const id = DatasetId.generate();
      mockPrisma.dataset.count.mockResolvedValueOnce(0);

      const result = await repository.exists(id);

      expect(result).toBe(false);
    });
  });
});
