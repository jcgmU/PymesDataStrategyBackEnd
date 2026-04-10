import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SubmitDecisionsUseCase } from '../SubmitDecisionsUseCase.js';
import type { AnomalyRepository } from '../../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../../domain/ports/repositories/DatasetRepository.js';
import { Anomaly } from '../../../domain/entities/Anomaly.js';
import { Dataset } from '../../../domain/entities/Dataset.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';

describe('SubmitDecisionsUseCase', () => {
  let useCase: SubmitDecisionsUseCase;
  let mockAnomalyRepository: AnomalyRepository;
  let mockDatasetRepository: DatasetRepository;

  const DATASET_ID = 'dataset-abc';

  const makeDataset = (id = DATASET_ID) => {
    const datasetId = DatasetId.fromString(id);
    return Dataset.reconstitute({
      id: datasetId,
      name: 'Test Dataset',
      description: null,
      status: 'READY',
      originalFileName: 'data.csv',
      storageKey: 'ds-id/data.csv',
      fileSizeBytes: BigInt(1024),
      mimeType: 'text/csv',
      schema: {},
      metadata: {},
      statistics: {},
      userId: 'user-123',
      createdAt: new Date(),
      updatedAt: new Date(),
      processedAt: null,
    });
  };

  const makeAnomaly = (id: string, datasetId = DATASET_ID) =>
    Anomaly.create({
      id,
      datasetId,
      column: 'email',
      row: null,
      type: 'MISSING_VALUE',
      description: 'Missing values detected',
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

    const mockJobQueueService = {
      enqueue: vi.fn().mockResolvedValue({ jobId: 'test-job', status: 'queued' }),
      getStatus: vi.fn(),
      cancel: vi.fn(),
    };

    useCase = new SubmitDecisionsUseCase(mockAnomalyRepository, mockDatasetRepository, mockJobQueueService);
  });

  it('should resolve anomalies and return results', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-1');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    const result = await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [{ anomalyId: 'anomaly-1', action: 'APPROVED' }],
    });

    expect(result.resolved).toBe(1);
    expect(result.results).toHaveLength(1);
    expect(result.results[0]?.anomalyId).toBe('anomaly-1');
    expect(result.results[0]?.action).toBe('APPROVED');
    expect(mockAnomalyRepository.save).toHaveBeenCalledWith(anomaly);
    expect(anomaly.status).toBe('RESOLVED');
  });

  it('should handle CORRECTED action with correction text', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-2');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    const result = await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [{ anomalyId: 'anomaly-2', action: 'CORRECTED', correction: 'N/A' }],
    });

    expect(result.resolved).toBe(1);
    expect(anomaly.decision?.action).toBe('CORRECTED');
    expect(anomaly.decision?.correction).toBe('N/A');
  });

  it('should handle DISCARDED action', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-3');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [{ anomalyId: 'anomaly-3', action: 'DISCARDED' }],
    });

    expect(anomaly.decision?.action).toBe('DISCARDED');
  });

  it('should process multiple decisions in one call', async () => {
    const dataset = makeDataset();
    const anomaly1 = makeAnomaly('anomaly-1');
    const anomaly2 = makeAnomaly('anomaly-2');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById)
      .mockResolvedValueOnce(anomaly1)
      .mockResolvedValueOnce(anomaly2);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    const result = await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [
        { anomalyId: 'anomaly-1', action: 'APPROVED' },
        { anomalyId: 'anomaly-2', action: 'DISCARDED' },
      ],
    });

    expect(result.resolved).toBe(2);
    expect(mockAnomalyRepository.save).toHaveBeenCalledTimes(2);
  });

  it('should throw NotFoundError when dataset does not exist', async () => {
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(null);

    await expect(
      useCase.execute({
        datasetId: 'non-existent',
        userId: 'user-123',
        decisions: [{ anomalyId: 'anomaly-1', action: 'APPROVED' }],
      })
    ).rejects.toThrow(NotFoundError);
  });

  it('should throw NotFoundError when anomaly does not exist', async () => {
    const dataset = makeDataset();
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(null);

    await expect(
      useCase.execute({
        datasetId: DATASET_ID,
        userId: 'user-123',
        decisions: [{ anomalyId: 'non-existent', action: 'APPROVED' }],
      })
    ).rejects.toThrow(NotFoundError);
  });

  it('should throw ValidationError when decisions array is empty', async () => {
    const dataset = makeDataset();
    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);

    await expect(
      useCase.execute({
        datasetId: DATASET_ID,
        userId: 'user-123',
        decisions: [],
      })
    ).rejects.toThrow(ValidationError);
  });

  it('should throw ValidationError when anomaly belongs to a different dataset', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-1', 'other-dataset-id');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);

    await expect(
      useCase.execute({
        datasetId: DATASET_ID,
        userId: 'user-123',
        decisions: [{ anomalyId: 'anomaly-1', action: 'APPROVED' }],
      })
    ).rejects.toThrow(ValidationError);
  });

  // ── IR fields extension ───────────────────────────────────────────────

  it('should persist correctionIr, irSource, irRawText when correctionIr is valid', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-ir-1');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    const validIr = { op: 'FILL_AGGREGATE', agg: 'median' };

    const result = await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [
        {
          anomalyId: 'anomaly-ir-1',
          action: 'CORRECTED',
          correctionIr: validIr,
          irSource: 'rule',
          irRawText: 'usa la mediana',
        },
      ],
    });

    expect(result.resolved).toBe(1);
    expect(anomaly.decision?.correctionIr).toEqual(validIr);
    expect(anomaly.decision?.irSource).toBe('rule');
    expect(anomaly.decision?.irRawText).toBe('usa la mediana');
  });

  it('should throw ValidationError when correctionIr is invalid (server-side re-validation)', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-ir-bad');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);

    const maliciousIr = { op: 'EXECUTE_CODE', code: 'rm -rf /' };

    await expect(
      useCase.execute({
        datasetId: DATASET_ID,
        userId: 'user-123',
        decisions: [
          {
            anomalyId: 'anomaly-ir-bad',
            action: 'CORRECTED',
            correctionIr: maliciousIr,
            irSource: 'gemini',
            irRawText: 'algo malicioso',
          },
        ],
      })
    ).rejects.toThrow(ValidationError);
  });

  it('should handle mixed batch — legacy decision + IR decision', async () => {
    const dataset = makeDataset();
    const legacyAnomaly = makeAnomaly('anomaly-legacy');
    const irAnomaly = makeAnomaly('anomaly-with-ir');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById)
      .mockResolvedValueOnce(legacyAnomaly)
      .mockResolvedValueOnce(irAnomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    const validIr = { op: 'DELETE_ROWS' };

    const result = await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [
        // Legacy: plain correction string
        { anomalyId: 'anomaly-legacy', action: 'CORRECTED', correction: '0' },
        // IR-based
        {
          anomalyId: 'anomaly-with-ir',
          action: 'CORRECTED',
          correctionIr: validIr,
          irSource: 'rule',
          irRawText: 'elimina las filas',
        },
      ],
    });

    expect(result.resolved).toBe(2);

    // Legacy anomaly should have correction string, no IR
    expect(legacyAnomaly.decision?.correction).toBe('0');
    expect(legacyAnomaly.decision?.correctionIr).toBeNull();

    // IR anomaly should have IR, no correction string
    expect(irAnomaly.decision?.correctionIr).toEqual(validIr);
    expect(irAnomaly.decision?.irSource).toBe('rule');
  });

  it('should pass null correctionIr fields for legacy decisions (no IR)', async () => {
    const dataset = makeDataset();
    const anomaly = makeAnomaly('anomaly-legacy-2');

    vi.mocked(mockDatasetRepository.findById).mockResolvedValue(dataset);
    vi.mocked(mockAnomalyRepository.findById).mockResolvedValue(anomaly);
    vi.mocked(mockAnomalyRepository.save).mockResolvedValue(undefined);

    await useCase.execute({
      datasetId: DATASET_ID,
      userId: 'user-123',
      decisions: [{ anomalyId: 'anomaly-legacy-2', action: 'CORRECTED', correction: 'N/A' }],
    });

    expect(anomaly.decision?.correctionIr).toBeNull();
    expect(anomaly.decision?.irSource).toBeNull();
    expect(anomaly.decision?.irRawText).toBeNull();
  });
});
