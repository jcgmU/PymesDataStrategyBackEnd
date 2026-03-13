import { randomUUID } from 'node:crypto';

/**
 * Value object representing a Dataset identifier.
 * Ensures type safety and encapsulates ID generation logic.
 */
export class DatasetId {
  private constructor(private readonly _value: string) {}

  static generate(): DatasetId {
    return new DatasetId(randomUUID());
  }

  static fromString(id: string): DatasetId {
    if (!id || id.trim() === '') {
      throw new Error('DatasetId cannot be empty');
    }
    return new DatasetId(id);
  }

  get value(): string {
    return this._value;
  }

  toString(): string {
    return this._value;
  }

  equals(other: DatasetId): boolean {
    return this._value === other._value;
  }
}
