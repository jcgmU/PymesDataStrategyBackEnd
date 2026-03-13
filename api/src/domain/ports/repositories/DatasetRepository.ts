import type { Dataset } from '../../entities/Dataset.js';
import type { DatasetId } from '../../value-objects/DatasetId.js';

/**
 * Options for listing datasets with pagination.
 */
export interface FindAllOptions {
  userId?: string;
  limit: number;
  offset: number;
}

/**
 * Port for Dataset persistence operations.
 * Infrastructure layer must implement this interface.
 */
export interface DatasetRepository {
  save(dataset: Dataset): Promise<void>;
  findById(id: DatasetId): Promise<Dataset | null>;
  findByUserId(userId: string): Promise<Dataset[]>;
  findAll(options: FindAllOptions): Promise<Dataset[]>;
  delete(id: DatasetId): Promise<void>;
  exists(id: DatasetId): Promise<boolean>;
}
