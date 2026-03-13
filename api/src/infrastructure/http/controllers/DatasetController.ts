import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { CreateDatasetUseCase } from '../../../application/use-cases/CreateDatasetUseCase.js';
import { TransformDatasetUseCase } from '../../../application/use-cases/TransformDatasetUseCase.js';
import { GetDownloadUrlUseCase } from '../../../application/use-cases/GetDownloadUrlUseCase.js';
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

      // Get user ID from auth (for now, use a placeholder)
      // TODO: Replace with actual auth when implemented
      const userId = getStringHeader(req.headers['x-user-id'], 'anonymous');

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
      const userId = req.query['userId'] as string | undefined;
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
      const userId = getStringHeader(req.headers['x-user-id'], 'anonymous');

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
}
