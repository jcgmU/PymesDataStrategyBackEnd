import { describe, it, expect } from 'vitest';
import {
  createDatasetSchema,
  isAllowedMimeType,
  ALLOWED_MIME_TYPES,
  MAX_FILE_SIZE,
} from '../dataset.schema.js';

describe('dataset.schema', () => {
  describe('createDatasetSchema', () => {
    it('should validate a valid dataset input', () => {
      const input = {
        name: 'My Dataset',
        description: 'A sample dataset',
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.name).toBe('My Dataset');
        expect(result.data.description).toBe('A sample dataset');
      }
    });

    it('should fail when name is empty', () => {
      const input = {
        name: '',
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(false);
    });

    it('should fail when name exceeds 255 characters', () => {
      const input = {
        name: 'a'.repeat(256),
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(false);
    });

    it('should allow description to be optional', () => {
      const input = {
        name: 'My Dataset',
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.description).toBeUndefined();
      }
    });

    it('should fail when description exceeds 1000 characters', () => {
      const input = {
        name: 'My Dataset',
        description: 'a'.repeat(1001),
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(false);
    });

    it('should parse valid JSON metadata string', () => {
      const input = {
        name: 'My Dataset',
        metadata: JSON.stringify({ source: 'api', version: 2 }),
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.metadata).toEqual({ source: 'api', version: 2 });
      }
    });

    it('should return undefined for invalid JSON metadata', () => {
      const input = {
        name: 'My Dataset',
        metadata: 'not-valid-json',
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.metadata).toBeUndefined();
      }
    });

    it('should return undefined for empty metadata', () => {
      const input = {
        name: 'My Dataset',
        metadata: '',
      };

      const result = createDatasetSchema.safeParse(input);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.metadata).toBeUndefined();
      }
    });
  });

  describe('isAllowedMimeType', () => {
    it('should allow text/csv', () => {
      expect(isAllowedMimeType('text/csv')).toBe(true);
    });

    it('should allow application/vnd.ms-excel', () => {
      expect(isAllowedMimeType('application/vnd.ms-excel')).toBe(true);
    });

    it('should allow Excel xlsx', () => {
      expect(
        isAllowedMimeType(
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
      ).toBe(true);
    });

    it('should allow application/json', () => {
      expect(isAllowedMimeType('application/json')).toBe(true);
    });

    it('should allow text/plain', () => {
      expect(isAllowedMimeType('text/plain')).toBe(true);
    });

    it('should reject application/pdf', () => {
      expect(isAllowedMimeType('application/pdf')).toBe(false);
    });

    it('should reject application/x-msdownload', () => {
      expect(isAllowedMimeType('application/x-msdownload')).toBe(false);
    });

    it('should reject image/png', () => {
      expect(isAllowedMimeType('image/png')).toBe(false);
    });
  });

  describe('constants', () => {
    it('should have 5 allowed MIME types', () => {
      expect(ALLOWED_MIME_TYPES).toHaveLength(5);
    });

    it('should have MAX_FILE_SIZE as 100MB', () => {
      expect(MAX_FILE_SIZE).toBe(100 * 1024 * 1024);
    });
  });
});
