import type { DatasetId } from '../value-objects/DatasetId.js';

export interface DatasetProps {
  id: DatasetId;
  name: string;
  description: string | null;
  status: DatasetStatus;
  originalFileName: string;
  storageKey: string;
  fileSizeBytes: bigint;
  mimeType: string;
  schema: Record<string, unknown>;
  metadata: Record<string, unknown>;
  statistics: Record<string, unknown>;
  userId: string;
  createdAt: Date;
  updatedAt: Date;
  processedAt: Date | null;
}

export type DatasetStatus = 'PENDING' | 'PROCESSING' | 'READY' | 'ERROR' | 'ARCHIVED';

export class Dataset {
  private constructor(private readonly props: DatasetProps) {}

  static create(
    props: Omit<DatasetProps, 'createdAt' | 'updatedAt' | 'processedAt' | 'status'>
  ): Dataset {
    return new Dataset({
      ...props,
      status: 'PENDING',
      createdAt: new Date(),
      updatedAt: new Date(),
      processedAt: null,
    });
  }

  static reconstitute(props: DatasetProps): Dataset {
    return new Dataset(props);
  }

  get id(): DatasetId {
    return this.props.id;
  }

  get name(): string {
    return this.props.name;
  }

  get description(): string | null {
    return this.props.description;
  }

  get status(): DatasetStatus {
    return this.props.status;
  }

  get originalFileName(): string {
    return this.props.originalFileName;
  }

  get storageKey(): string {
    return this.props.storageKey;
  }

  get fileSizeBytes(): bigint {
    return this.props.fileSizeBytes;
  }

  get mimeType(): string {
    return this.props.mimeType;
  }

  get schema(): Record<string, unknown> {
    return { ...this.props.schema };
  }

  get metadata(): Record<string, unknown> {
    return { ...this.props.metadata };
  }

  get statistics(): Record<string, unknown> {
    return { ...this.props.statistics };
  }

  get userId(): string {
    return this.props.userId;
  }

  get createdAt(): Date {
    return this.props.createdAt;
  }

  get updatedAt(): Date {
    return this.props.updatedAt;
  }

  get processedAt(): Date | null {
    return this.props.processedAt;
  }

  markProcessing(): void {
    this.props.status = 'PROCESSING';
    this.props.updatedAt = new Date();
  }

  markReady(statistics: Record<string, unknown>): void {
    this.props.status = 'READY';
    this.props.statistics = statistics;
    this.props.processedAt = new Date();
    this.props.updatedAt = new Date();
  }

  markError(): void {
    this.props.status = 'ERROR';
    this.props.updatedAt = new Date();
  }

  archive(): void {
    this.props.status = 'ARCHIVED';
    this.props.updatedAt = new Date();
  }

  updateMetadata(metadata: Record<string, unknown>): void {
    this.props.metadata = metadata;
    this.props.updatedAt = new Date();
  }

  updateStorageKey(key: string): void {
    this.props.storageKey = key;
    this.props.updatedAt = new Date();
  }
}
