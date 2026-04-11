import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GetAnomaliesUseCase } from '../GetAnomaliesUseCase.js';
import type { AnomalyRepository } from '../../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../../domain/ports/repositories/DatasetRepository.js';
import { Anomaly } from '../../../domain/entities/Anomaly.js';
import { Dataset } from '../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../../domain/errors/NotFoundError.js';

describe('GetAnomaliesUseCase', () => {
  let useCase: GetAnomaliesUseCase;
  let mockAnomalyRepository: AnomalyRepository;
  let mockDatasetRepository: DatasetRepository;

  const makeDataset = () =>
    Dataset.create({
      id: DatasetId.generate(),
      name: 'Test Dataset',
      description: null,
      originalFileName: 'data.csv',
      storageKey: 'ds-id/data.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
    });

  const makeAnomaly = (datasetId: string) =>
    Anomaly.create({
      id: 'anomaly-1',
      datasetId,
      column: 'email',
      row: null,
      type: 'MISSING_VALUE',
      description: '10 missing values in email column',
      originalValue: null,
      suggestedValue: null,
    });

  beforeEach(() => {
    mockAnomalyRepository = {
      findByDatasetId: vi.fn(),
      findById: vi.fn(),
      save: vi.fn(),
      saveMany: vi.fn(),
    };

    mockDatasetRepository = {
      save: vi.fn(),
      findById: vi.fn(),
      findByUserId: vi.fn(),
      findAll: vi.fn(),
      delete: vi.fn(),
      exists: vi.fn(),
    };

    useCase = new GetAnomaliesUseCase(mockAnomalyRepository, mockDatasetRepository);
  });

  it('should return anomalies for an existing dataset', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly(dataset.id.value);

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findByDatasetId).mockResolvedValue([anomaly]);

    const result = await useCase.execute({ datasetId: dataset.id.value });

    expect(result.anomalies).toHaveLength(1);
    expect(result.anomalies[0]?.id).toBe('anomaly-1');
    expect(result.anomalies[0]?.column).toBe('email');
    expect(result.anomalies[0]?.type).toBe('MISSING_VALUE');
    expect(result.anomalies[0]?.status).toBe('PENDING');
    expect(result.anomalies[0]?.createdAt).toBeDefined();
  });

  it('should return empty array when no anomalies exist', async () => {
    const dataset = makeDataset();

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findByDatasetId).mockResolvedValue([]);

    const result = await useCase.execute({ datasetId: dataset.id.value });

    expect(result.anomalies).toHaveLength(0);
  });

  it('should throw NotFoundError when dataset does not exist', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(null);

    await expect(
      useCase.execute({ datasetId: 'non-existent' })
    ).rejects.toThrow(NotFoundError);
  });

  it('should include decision when anomaly has been resolved', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly(dataset.id.value);

    anomaly.resolve({
      id: 'dec-1',
      anomalyId: 'anomaly-1',
      action: 'APPROVED',
      correction: null,
      correctionIr: null,
      irSource: null,
      irRawText: null,
      userId: 'user-123',
      createdAt: new Date(),
    });

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findByDatasetId).mockResolvedValue([anomaly]);

    const result = await useCase.execute({ datasetId: dataset.id.value });

    expect(result.anomalies[0]?.decision).not.toBeNull();
    expect(result.anomalies[0]?.decision?.action).toBe('APPROVED');
    expect(result.anomalies[0]?.status).toBe('RESOLVED');
  });

  it('should map ISO date strings for createdAt and updatedAt', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly(dataset.id.value);

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findByDatasetId).mockResolvedValue([anomaly]);

    const result = await useCase.execute({ datasetId: dataset.id.value });

    expect(typeof result.anomalies[0]?.createdAt).toBe('string');
    expect(typeof result.anomalies[0]?.updatedAt).toBe('string');
  });
});
