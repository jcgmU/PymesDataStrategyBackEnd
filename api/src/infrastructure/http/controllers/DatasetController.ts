import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { CreateDatasetUseCase } from '../../../application/use-cases/CreateDatasetUseCase.js';
import { TransformDatasetUseCase } from '../../../application/use-cases/TransformDatasetUseCase.js';
import { GetDownloadUrlUseCase } from '../../../application/use-cases/GetDownloadUrlUseCase.js';
import { GetAnomaliesUseCase } from '../../../application/use-cases/GetAnomaliesUseCase.js';
import { SubmitDecisionsUseCase } from '../../../application/use-cases/SubmitDecisionsUseCase.js';
import { ParseInstructionUseCase } from '../../../application/use-cases/ParseInstructionUseCase.js';
import {
  GenerateReportUseCase,
  GeminiUnavailableError,
} from '../../../application/use-cases/GenerateReportUseCase.js';
import {
  createDatasetSchema,
  isAllowedMimeType,
} from '../schemas/dataset.schema.js';
import { transformDatasetSchema } from '../schemas/transform.schema.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';

/**
 * Safely extract a string header value.
 * Express headers can be string | string[] | undefined.
 */
function getStringHeader(value: string | string[] | undefined, fallback: string): string {
  if (typeof value === 'string') return value;
  if (Array.isArray(value) && value.length > 0) return value[0] ?? fallback;
  return fallback;
}

/**
 * Controller for dataset-related HTTP endpoints.
 */
export class DatasetController {
  constructor(private readonly container: Container) {}

  /**
   * POST /api/v1/datasets
   * Upload a new dataset file.
   */
  async create(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      // Validate file presence
      const file = req.file;
      if (!file) {
        throw new ValidationError('File is required', 'file');
      }

      // Validate MIME type
      if (!isAllowedMimeType(file.mimetype)) {
        throw new ValidationError(
          'Unsupported file type. Allowed types: CSV, Excel, JSON, TXT',
          'file'
        );
      }

      // Validate and parse body fields
      const parseResult = createDatasetSchema.safeParse(req.body);
      if (!parseResult.success) {
        const firstError = parseResult.error.errors[0];
        throw new ValidationError(
          firstError?.message ?? 'Invalid request body',
          firstError?.path.join('.') ?? 'body'
        );
      }

      const { name, description, metadata } = parseResult.data;

      // Get user ID from JWT middleware or fallback to x-user-id header (legacy)
      const userId = req.userId ?? getStringHeader(req.headers['x-user-id'], 'anonymous');

      // Execute use case (with job queue for async processing)
      const useCase = new CreateDatasetUseCase(
        this.container.datasetRepository,
        this.container.storage,
        this.container.jobQueue
      );

      const result = await useCase.execute({
        name,
        ...(description !== undefined ? { description } : {}),
        originalFileName: file.originalname,
        mimeType: file.mimetype,
        fileSizeBytes: file.size,
        fileContent: file.buffer,
        userId,
        ...(metadata !== undefined ? { metadata } : {}),
      });

      res.status(201).json({
        success: true,
        data: {
          id: result.datasetId,
          storageKey: result.storageKey,
          status: result.status,
          jobId: result.jobId,
        },
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/datasets/:id
   * Get a dataset by ID.
   */
  async getById(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const dataset = await this.container.datasetRepository.findById(
        DatasetId.fromString(id)
      );

      if (!dataset) {
        res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `Dataset with ID ${id} not found`,
          },
        });
        return;
      }

      res.json({
        success: true,
        data: {
          id: dataset.id.value,
          name: dataset.name,
          description: dataset.description,
          status: dataset.status,
          originalFileName: dataset.originalFileName,
          storageKey: dataset.storageKey,
          fileSizeBytes: Number(dataset.fileSizeBytes),
          mimeType: dataset.mimeType,
          schema: dataset.schema,
          metadata: dataset.metadata,
          statistics: dataset.statistics,
          userId: dataset.userId,
          createdAt: dataset.createdAt.toISOString(),
          updatedAt: dataset.updatedAt.toISOString(),
        },
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/datasets
   * List datasets with pagination.
   */
  async list(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const userId = req.userId;
      const limit = Math.min(parseInt(req.query['limit'] as string) || 20, 100);
      const offset = parseInt(req.query['offset'] as string) || 0;

      const datasets = await this.container.datasetRepository.findAll({
        ...(userId !== undefined ? { userId } : {}),
        limit,
        offset,
      });

      res.json({
        success: true,
        data: datasets.map((dataset) => ({
          id: dataset.id.value,
          name: dataset.name,
          description: dataset.description,
          status: dataset.status,
          originalFileName: dataset.originalFileName,
          fileSizeBytes: Number(dataset.fileSizeBytes),
          mimeType: dataset.mimeType,
          userId: dataset.userId,
          createdAt: dataset.createdAt.toISOString(),
          updatedAt: dataset.updatedAt.toISOString(),
        })),
        pagination: {
          limit,
          offset,
        },
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * DELETE /api/v1/datasets/:id
   * Delete a dataset.
   */
  async delete(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      // First get the dataset to find the storage key
      const dataset = await this.container.datasetRepository.findById(
        DatasetId.fromString(id)
      );

      if (!dataset) {
        res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `Dataset with ID ${id} not found`,
          },
        });
        return;
      }

      // Delete from storage
      try {
        await this.container.storage.deleteFromDatasets(dataset.storageKey);
      } catch {
        // Log but don't fail if storage deletion fails
        console.warn(`Failed to delete storage key ${dataset.storageKey}`);
      }

      // Delete from database
      await this.container.datasetRepository.delete(DatasetId.fromString(id));

      res.status(204).send();
    } catch (error) {
      next(error);
    }
  }

  /**
   * POST /api/v1/datasets/:id/transform
   * Trigger a transformation job on an existing dataset.
   */
  async transform(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      // Validate body
      const parseResult = transformDatasetSchema.safeParse(req.body);
      if (!parseResult.success) {
        const firstError = parseResult.error.errors[0];
        throw new ValidationError(
          firstError?.message ?? 'Invalid request body',
          firstError?.path.join('.') ?? 'body'
        );
      }

      const { transformationType, parameters, priority } = parseResult.data;
      const userId = req.userId ?? getStringHeader(req.headers['x-user-id'], 'anonymous');

      const useCase = new TransformDatasetUseCase(
        this.container.datasetRepository,
        this.container.jobQueue
      );

      const result = await useCase.execute({
        datasetId: id,
        userId,
        transformationType,
        parameters,
        priority,
      });

      res.status(201).json({
        success: true,
        data: {
          jobId: result.jobId,
          datasetId: result.datasetId,
          status: result.status,
        },
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/datasets/:id/stream
   * Stream the dataset file directly through the API.
   * Accepts auth token via ?token= query param so window.open() works.
   */
  async stream(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const dataset = await this.container.datasetRepository.findById(
        DatasetId.fromString(id)
      );

      if (!dataset) {
        res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `Dataset with ID ${id} not found`,
          },
        });
        return;
      }

      const isProcessed = dataset.storageKey.startsWith('processed/');
      const bucket = isProcessed ? 'results' : 'datasets';

      const fileStream = await this.container.storage.download(bucket, dataset.storageKey);

      res.setHeader('Content-Type', dataset.mimeType);
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${dataset.originalFileName}"`
      );

      fileStream.pipe(res);

      fileStream.on('error', (err) => {
        next(err);
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/datasets/:id/download
   * Generate a signed download URL for a dataset file.
   */
  async download(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const expiresInRaw = req.query['expiresIn'];
      const expiresInSeconds = typeof expiresInRaw === 'string'
        ? parseInt(expiresInRaw)
        : undefined;

      const useCase = new GetDownloadUrlUseCase(
        this.container.datasetRepository,
        this.container.storage
      );

      const result = await useCase.execute({
        datasetId: id,
        ...(expiresInSeconds !== undefined ? { expiresInSeconds } : {}),
      });

      res.json({
        success: true,
        data: {
          datasetId: result.datasetId,
          downloadUrl: result.downloadUrl,
          expiresIn: result.expiresIn,
          fileName: result.fileName,
        },
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/datasets/:id/anomalies
   * Get all anomalies for a dataset.
   */
  async getAnomalies(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const useCase = new GetAnomaliesUseCase(
        this.container.anomalyRepository,
        this.container.datasetRepository
      );

      const result = await useCase.execute({ datasetId: id });

      res.json({
        success: true,
        data: result.anomalies,
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * POST /api/v1/datasets/:id/anomalies/:anomalyId/parse-instruction
   * Parse a natural language instruction into a validated IR tree (preview, no persist).
   */
  async parseInstruction(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const datasetId = req.params['id'] as string;
      const anomalyId = req.params['anomalyId'] as string;

      if (!datasetId) {
        throw new ValidationError('Dataset ID is required', 'id');
      }
      if (!anomalyId) {
        throw new ValidationError('Anomaly ID is required', 'anomalyId');
      }

      const body = req.body as { instruction?: unknown };
      const instruction = body.instruction;

      if (typeof instruction !== 'string' || instruction.trim() === '') {
        throw new ValidationError('instruction must be a non-empty string', 'instruction');
      }

      const userId = req.userId ?? getStringHeader(req.headers['x-user-id'], 'anonymous');

      const useCase = new ParseInstructionUseCase(
        this.container.anomalyRepository,
        this.container.datasetRepository
      );

      const result = await useCase.execute({
        datasetId,
        anomalyId,
        instruction,
        userId,
      });

      // isParseError check — errors have an 'error' field
      if ('error' in result) {
        const status = result.error === 'invalid_instruction' ? 400 : 422;
        res.status(status).json(result);
        return;
      }

      res.json(result);
    } catch (error) {
      next(error);
    }
  }

  /**
   * POST /api/v1/datasets/:id/report
   * Generate an AI-powered report for a dataset's cleaning process.
   */
  async generateReport(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const userId = req.userId ?? getStringHeader(req.headers['x-user-id'], 'anonymous');

      const useCase = new GenerateReportUseCase(
        this.container.anomalyRepository,
        this.container.datasetRepository
      );

      const result = await useCase.execute({ datasetId: id, userId });

      res.json({
        success: true,
        data: {
          stats: result.stats,
          narrative: result.narrative,
          datasetName: result.datasetName,
        },
      });
    } catch (error) {
      if (error instanceof GeminiUnavailableError) {
        res.status(503).json({
          success: false,
          error: {
            code: error.code,
            message: error.message,
          },
        });
        return;
      }
      next(error);
    }
  }

  /**
   * POST /api/v1/datasets/:id/decisions
   * Submit human decisions for anomalies (HITL flow).
   */
  async submitDecisions(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const id = req.params['id'] as string;

      if (!id) {
        throw new ValidationError('Dataset ID is required', 'id');
      }

      const { decisions } = req.body as { decisions: unknown };

      if (!Array.isArray(decisions)) {
        throw new ValidationError('decisions must be an array', 'decisions');
      }

      const userId = req.userId ?? getStringHeader(req.headers['x-user-id'], 'anonymous');

      const useCase = new SubmitDecisionsUseCase(
        this.container.anomalyRepository,
        this.container.datasetRepository,
        this.container.jobQueue
      );

      const result = await useCase.execute({
        datasetId: id,
        userId,
        decisions: decisions as {
          anomalyId: string;
          action: 'APPROVED' | 'CORRECTED' | 'DISCARDED';
          correction?: string;
          correctionIr?: unknown;
          irSource?: string;
          irRawText?: string;
        }[],
      });

      res.status(201).json({
        success: true,
        data: {
          resolved: result.resolved,
          results: result.results,
        },
      });
    } catch (error) {
      next(error);
    }
  }
}
